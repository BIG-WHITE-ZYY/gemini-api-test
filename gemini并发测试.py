#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
并发扫节点：不切节点、不烧 API 额度，几秒钟扫完 54 个。

为什么能快
----------
Clash 一个代理组同时只能选中一个节点 —— "切节点"这件事本身没法并发，
这是 Clash 的设计，不是脚本懒得写。所以 gemini_api直连测试.py 只能一个
节点一个节点切过去问，54 个要三四分钟。

但 mihomo 有个接口，可以在**不切节点**的前提下单独测某个节点：

    GET /proxies/<节点名>/delay?url=<URL>&timeout=<毫秒>

把这个 URL 指成一个"只有地区被支持才通得过"的地址（Gemini 的 models 接口
+ 你的 key），再把所有节点同时扔进去 —— 几秒钟出全量结果，
不消耗 generate_content 的额度（那是免费档 15 次/分钟的瓶颈），
也不动你当前选中的那个节点。只有"预筛通过"的节点，才切过去用真正的
generate_content 确认一遍，所以总请求数从 54 降到个位数。

可靠性
------
这个办法成立的前提：那个接口对"地区不支持"和"成功"的反应不一样。
这取决于 mihomo 怎么判定成功、以及我们用的是哪个 URL，不是我能凭空保证的。
所以脚本会先**校准**：拿一个已知能用的节点和一个已知不能用的各测一次，
能区分才继续往下扫；区分不了会直接告诉你，别浪费时间。

依赖
----
本脚本和 gemini_api直连测试.py 必须放在同一个目录 —— 控制器发现、命名管道、
key 读取、真 API 调用都复用它，不重复一份。跑法：

    conda activate wsmx
    python gemini并发测试.py
"""

import glob
import importlib.util
import json
import os
import sys
import threading
import time
import urllib.parse
from concurrent.futures import ThreadPoolExecutor, as_completed

HERE = os.path.dirname(os.path.abspath(__file__))
DEFAULT_OUTDIR = HERE
BASE_FILE = "gemini_api直连测试.py"

# ============================================================================
#  配置区
# ============================================================================
CONFIG = {
    # 预筛用的 URL，{key} 会换成你的 API key。
    # 挑它的理由：地区不支持时它返回 400，支持时返回 200，正好能当判别器。
    "预筛URL": "https://generativelanguage.googleapis.com/v1beta/models?key={key}",

    # 同时测多少个节点。mihomo 就在本机，开太大没用，16~32 足够。
    "并发数": 16,

    # 单个节点的预筛超时（毫秒）
    "预筛超时毫秒": 8000,

    # 校准用的节点：一个你知道能用的，一个你知道不能用的。
    # 留空 = 自动从最近一份报告里挑（推荐）。
    "已知能用的节点": "",
    "已知不能用的节点": "",

    # 预筛通过的节点，再切过去用真 API 确认（强烈建议 True，否则只是"猜"）
    "通过后再用真API确认": True,

    # 最多确认几个（按预筛延迟从快到慢排）。有多个 key 轮换时不用吝啬，
    # 8 个 key 就是 120 次/分钟，够确认几十个了。
    "最多确认几个": 20,

    # 测哪个组；留空 = 自动挑节点最多的那个
    "测哪个组": "",

    # 只测这些地区；留空 = 全部
    "只测这些地区": [],

    # 确认阶段的切换等待（秒）
    "切换等待秒数": 0.5,

    # 确认阶段的请求超时（秒）
    "接口超时秒数": 20,
}


def load_base():
    """把 gemini_api直连测试.py 当模块加载，复用里面的控制器/管道/key 逻辑。"""
    p = os.path.join(HERE, BASE_FILE)
    if not os.path.exists(p):
        print(f"✗ 找不到 {BASE_FILE}。它得跟本脚本放在同一个目录。")
        raise SystemExit(2)
    spec = importlib.util.spec_from_file_location("gembase", p)
    m = importlib.util.module_from_spec(spec)
    sys.modules["gembase"] = m
    spec.loader.exec_module(m)
    return m


base = load_base()
log = base.log


# ----------------------------------------------------------------------------
# 核心：问 mihomo「这个节点访问这个 URL 通不通」
# ----------------------------------------------------------------------------

def delay_test(ctrl, node, url, timeout_ms):
    """测一个节点，不切节点、不影响当前选中的节点。

    返回 {"ok": bool, "delay": int|None, "msg": str}
    """
    path = (f"/proxies/{urllib.parse.quote(node, safe='')}/delay"
            f"?url={urllib.parse.quote(url, safe='')}"
            f"&timeout={int(timeout_ms)}")
    try:
        st, j = base.api(ctrl, path, timeout=max(20, timeout_ms / 1000 + 5))
    except Exception as e:
        return {"ok": False, "delay": None, "msg": f"{type(e).__name__}: {e}"}
    if not isinstance(j, dict):
        return {"ok": False, "delay": None, "msg": f"HTTP {st}: {str(j)[:90]}"}
    if "delay" in j:
        return {"ok": True, "delay": j.get("delay"), "msg": ""}
    msg = j.get("message") or j.get("error") or str(j)
    return {"ok": False, "delay": None, "msg": str(msg)[:110]}


def load_known_from_reports():
    """从最近一份报告里挑出"一个能用的 + 一个不能用的"，用来校准。"""
    good = bad = None
    files = sorted(glob.glob(os.path.join(HERE, "gemini直连报告_*.json")),
                   reverse=True)
    for f in files:
        try:
            with open(f, "r", encoding="utf-8") as fh:
                d = json.load(fh)
        except Exception:
            continue
        for r in (d.get("明细") or []):
            if good is None and r.get("ok"):
                good = r.get("node")
            if bad is None and r.get("kind") == "region":
                bad = r.get("node")
        if good and bad:
            break
    return good, bad


def calibrate(ctrl, url, good, bad, timeout_ms):
    """先确认这个办法在你机器上真的能区分好坏。"""
    log(f"  已知能用：{good or '（没找到）'}")
    log(f"  已知不能用：{bad or '（没找到）'}")
    if not good or not bad:
        return None, ("校准节点不全（要一个已知能用、一个已知不能用）。"
                      "可以在 CONFIG 的「已知能用的节点」/「已知不能用的节点」里手动填。")
    rg = delay_test(ctrl, good, url, timeout_ms)
    rb = delay_test(ctrl, bad, url, timeout_ms)
    log(f"  能用的那个 -> {'通，' + str(rg['delay']) + 'ms' if rg['ok'] else '不通：' + rg['msg']}")
    log(f"  不能用的那个 -> {'通，' + str(rb['delay']) + 'ms' if rb['ok'] else '不通：' + rb['msg']}")
    if rg["ok"] and not rb["ok"]:
        return True, "校准通过：能用的通、不能用的不通，这个口径可以拿来扫"
    if not rg["ok"] and rb["ok"]:
        return False, ("校准反了：已知能用的那个反而不通。可能是节点被换了/IP 变了，"
                       "或者这个 URL 不是按地区判的。先别扫。")
    if rg["ok"] and rb["ok"]:
        return False, ("两个都通 —— 说明这个 URL 对「地区不支持」没有反应"
                       "（或者 mihomo 把 400 也当成成功）。换个 URL 再试，"
                       "或者回去用 gemini_api直连测试.py 一个一个测。")
    return False, ("两个都不通 —— 说明这个 URL 谁都访问不了（key 不对？"
                   "或者这个地址本身连不上）。换个 URL 再试，"
                   "或者回去用 gemini_api直连测试.py。")


# ----------------------------------------------------------------------------
# 报告
# ----------------------------------------------------------------------------

def write_report(outdir, meta_lines, results, survivors, confirmed, winner):
    ts = time.strftime("%Y%m%d_%H%M%S")
    os.makedirs(outdir, exist_ok=True)
    md_path = os.path.join(outdir, f"gemini并发报告_{ts}.md")
    js_path = os.path.join(outdir, f"gemini并发报告_{ts}.json")

    with open(js_path, "w", encoding="utf-8") as f:
        json.dump({"时间": time.strftime("%Y-%m-%d %H:%M:%S"),
                   "预筛通过": survivors,
                   "确认过的": confirmed,
                   "结论_找到可用节点": winner["node"] if winner else None,
                   "明细": results}, f, ensure_ascii=False, indent=2)

    L = ["# 并发预筛：哪些 Clash 节点可能能直连 Gemini API\n"]
    L += meta_lines
    L.append("")
    L.append("## 预筛结果（不切节点、不占 API 额度）\n")
    L.append("| 节点 | 预筛 | 延迟 | 说明 |")
    L.append("|------|------|------|------|")
    for r in results:
        ok = r.get("pre_ok")
        L.append(f"| {r['node']} | {'✅ 过' if ok else '❌ 不过'} | "
                 f"{r.get('pre_delay') or '?'}ms | `{r.get('pre_msg') or ''}` |")
    L.append("")
    L.append("## 真 API 确认（只有预筛通过的才切过去问）\n")
    if not confirmed:
        L.append("没有节点进入确认阶段。\n")
    else:
        L.append("| 节点 | 结果 | Gemini 说 | 耗时 |")
        L.append("|------|------|-----------|------|")
        for r in confirmed:
            say = (r.get("text") or "").replace("\n", " ")[:60].replace("|", "/")
            L.append(f"| {r['node']} | {r.get('why')} | `{say}` | {r.get('elapsed')}s |")
    L.append("")
    L.append("## 结论\n")
    if winner:
        L.append(f"**✅ 能直连的节点：`{winner['node']}`**\n")
        L.append(f"Gemini 回的是：`{(winner.get('text') or '')[:200]}`\n")
        L.append("已经在 Clash 里选中，直接拿去用（脚本没有切回去）。\n")
    else:
        L.append("**❌ 预筛里没有节点通过真 API 确认。**\n")
        if not survivors:
            L.append("一个节点都没通过预筛 —— 这批节点在当前网络下都够不到那个地址。\n")
        else:
            L.append(f"{len(survivors)} 个节点过了预筛，但真 API 确认时都没成 —— "
                     "说明预筛这个口径和 Gemini API 的真实判定不完全一致，"
                     "以真 API 的结果为准。\n")
    with open(md_path, "w", encoding="utf-8") as f:
        f.write("\n".join(L))
    return md_path, js_path


# ----------------------------------------------------------------------------
# 主流程
# ----------------------------------------------------------------------------

def main():
    base._setup_stdout()

    t0 = time.time()
    log("=" * 70)
    log("  并发预筛：不切节点、不烧 API 额度，几秒钟扫完所有节点")
    log("=" * 70)

    # --- 1) key ---
    log("\n[1/5] 检查 API key ...")
    keys, src = base.load_api_keys()
    if not keys:
        log("  ✗ 没找到 API key（同目录 gemini_api_key.txt / 环境变量 GEMINI_API_KEY）。")
        return 2
    ring = base.KeyRing(keys)
    log(f"  key：{len(keys)} 个（来自 {src}）"
        + ("　→ 轮流用，撞到 429 就换一个" if len(keys) > 1 else ""))

    TEMPLATE = CONFIG["预筛URL"]
    if "{key}" not in TEMPLATE:
        log("  [注意] 「预筛URL」里没有 {key} 占位符，这个地址可能不带 key 就访问不了")

    def pre_url_for(key):
        return TEMPLATE.replace("{key}", urllib.parse.quote(key, safe=""))

    # --- 2) 控制器 + 节点 ---
    log("\n[2/5] 查找 Clash 控制器、枚举节点 ...")
    ctrl = base.discover_controller()
    if not ctrl:
        log("  ✗ 没找到 Clash 控制器。先跑 python gemini_api直连测试.py --diagnose 看看。")
        return 2

    _st, ccfg = base.api(ctrl, "/configs", timeout=10)
    mixed = None
    if isinstance(ccfg, dict):
        mixed = ccfg.get("mixed-port") or ccfg.get("mixed_port") or ccfg.get("port")
    if not mixed:
        mixed = base.read_verge_yaml_hints().get("mixed_port") or 7897
    proxy_url = f"http://127.0.0.1:{mixed}"

    _st, proxies = base.api(ctrl, "/proxies", timeout=15)
    if not isinstance(proxies, dict) or "proxies" not in proxies:
        log("  ✗ 读取 /proxies 失败")
        return 2
    proxies = proxies["proxies"]

    picked = base.pick_group(proxies, CONFIG["测哪个组"] or None)
    if not picked:
        log("  ✗ 没找到可用的代理组")
        return 2
    gname, _gt, nodes, gnow = picked

    sub_groups = {n for n, p in proxies.items()
                  if (p.get("all") or [])
                  and p.get("type") in ("Selector", "URLTest", "Fallback",
                                        "LoadBalance", "Relay")}
    nodes = [n for n in nodes if n not in sub_groups]
    JUNK = ("套餐", "到期", "剩余流量", "流量：", "官网", "续费", "订阅",
            "重置", "客服", "购买", "长期有效")
    junk = [n for n in nodes if any(k in n for k in JUNK)]
    nodes = [n for n in nodes if n not in junk]

    kw = CONFIG["只测这些地区"] or []
    if kw:
        nodes = [n for n in nodes if base.region_match(n, kw)]

    log(f"  控制器 {ctrl['label']}   代理端口 {mixed}")
    log(f"  组「{gname}」：{len(nodes)} 个节点待测"
        + (f"（跳过 {len(junk)} 条订阅信息）" if junk else ""))
    log(f"  当前选中的节点是「{gnow}」—— 预筛阶段不会动它")
    if not nodes:
        log("  ✗ 没有可测的节点")
        return 2

    # --- 3) 校准 ---
    log("\n[3/5] 校准：先确认这个办法在你机器上能区分好坏 ...")
    good = CONFIG["已知能用的节点"] or None
    bad = CONFIG["已知不能用的节点"] or None
    if not (good and bad):
        g2, b2 = load_known_from_reports()
        good = good or g2
        bad = bad or b2
    ok, why = calibrate(ctrl, pre_url_for(ring.acquire()), good, bad,
                        CONFIG["预筛超时毫秒"])
    log(f"  {why}")
    if not ok:
        log("\n  先别扫了。三个办法：")
        log("    1. 换「预筛URL」—— 找一个「地区不支持时不会成功」的地址")
        log("    2. 手动填 CONFIG 里的「已知能用的节点」/「已知不能用的节点」再试")
        log("    3. 回去用 gemini_api直连测试.py，慢但可靠")
        return 3

    # --- 4) 并发预筛 ---
    log(f"\n[4/5] 并发预筛 {len(nodes)} 个节点（{CONFIG['并发数']} 路并发）...")
    t_sweep = time.time()
    results = [None] * len(nodes)
    lock = threading.Lock()
    done = {"n": 0}

    def one(i, node):
        # 每个节点用一个轮换出来的 key —— 54 个并发请求挤在一个 key 上
        # 肯定会撞 15 次/分钟的免费档限额
        r = delay_test(ctrl, node, pre_url_for(ring.acquire()),
                       CONFIG["预筛超时毫秒"])
        with lock:
            done["n"] += 1
            n = done["n"]
        flag = "✅" if r["ok"] else "❌"
        extra = f"{r['delay']}ms" if r["ok"] else (r["msg"] or "")[:60]
        log(f"  [{n}/{len(nodes)}] {flag} {node[:44]}   {extra}")
        return i, {"node": node, "pre_ok": r["ok"], "pre_delay": r["delay"],
                   "pre_msg": r["msg"]}

    with ThreadPoolExecutor(max_workers=int(CONFIG["并发数"])) as ex:
        futs = [ex.submit(one, i, n) for i, n in enumerate(nodes)]
        for f in as_completed(futs):
            try:
                i, rec = f.result()
                results[i] = rec
            except Exception as e:
                log(f"  [警告] 某个节点测的时候抛异常：{type(e).__name__}: {e}")
    results = [r for r in results if r]

    sweep_secs = time.time() - t_sweep
    survivors = sorted([r for r in results if r["pre_ok"]],
                       key=lambda r: r["pre_delay"] or 99999)
    log(f"\n  预筛完成：{len(survivors)}/{len(results)} 个通过，用时 {sweep_secs:.1f}s"
        f"（对比：一个一个切着测要 3-4 分钟）")
    for r in survivors[:15]:
        log(f"    ✅ {r['node'][:50]}   {r['pre_delay']}ms")
    if len(survivors) > 15:
        log(f"    … 还有 {len(survivors) - 15} 个")

    # --- 5) 真 API 确认 ---
    confirmed = []
    winner = None
    if CONFIG["通过后再用真API确认"] and survivors:
        todo = survivors[:int(CONFIG["最多确认几个"])]
        log(f"\n[5/5] 切过去用真 API 确认前 {len(todo)} 个 ...")
        try:
            for k, s in enumerate(todo, 1):
                node = s["node"]
                log(f"  [{k}/{len(todo)}] {node[:50]}")
                try:
                    st, _j = base.api(
                        ctrl, f"/proxies/{urllib.parse.quote(gname, safe='')}",
                        method="PUT", body={"name": node}, timeout=10)
                    if st not in (200, 204):
                        log(f"        ✗ 切节点失败 HTTP {st}")
                        continue
                except Exception as e:
                    log(f"        ✗ 切节点失败 {e}")
                    continue

                time.sleep(float(CONFIG["切换等待秒数"]))

                # 用轮换的 key 打；撞到 429 / 坏 key 就换一个重试同一个节点
                r = okc = kind = whyc = None
                used_key = None
                for _try in range(len(ring)):
                    used_key = ring.acquire()
                    cli = base.make_gemini_client(used_key, proxy_url,
                                                  float(CONFIG["接口超时秒数"]))
                    try:
                        r = base.ask_gemini(cli, "gemini-3.5-flash-lite", "hi",
                                            float(CONFIG["接口超时秒数"]))
                    finally:
                        try:
                            cli.close()
                        except Exception:
                            pass
                    okc, kind, whyc = base.classify(r)
                    if kind == "ok":
                        ring.note(used_key, "ok")
                        break
                    if kind == "quota":
                        ring.cool(used_key,
                                  base.parse_retry_delay(r.get("text")))
                        continue
                    if kind == "key":
                        ring.cool(used_key, 3600, why="坏 key")
                        continue
                    ring.note(used_key, "err")
                    break

                rec = {"node": node, "ok": okc, "kind": kind, "why": whyc,
                       "status": r.get("status"), "text": r.get("text"),
                       "elapsed": r.get("elapsed"),
                       "key": base.mask_key(used_key) if used_key else None}
                confirmed.append(rec)
                log(f"        {whyc}   ({r.get('elapsed')}s)")
                if okc:
                    log(f"        Gemini 回：{(r.get('text') or '')[:120]}")
                    winner = rec
                    log("")
                    log("  " + "★" * 28)
                    log(f"  ★ 找到了：{node}")
                    log("  ★ 已经切过去并且不切回来了，直接用")
                    log("  " + "★" * 28)
                    break
        except KeyboardInterrupt:
            log("\n  已中断。")

    if not winner:
        # 没找到就把原来的节点切回去
        restored = False
        if gnow and gnow in proxies:
            try:
                st, _j = base.api(ctrl, f"/proxies/{urllib.parse.quote(gname, safe='')}",
                                  method="PUT", body={"name": gnow}, timeout=10)
                restored = st in (200, 204)
            except Exception:
                pass
        if restored:
            log(f"\n  已恢复原节点：{gnow}")

    meta = [
        f"- 时间：{time.strftime('%Y-%m-%d %H:%M:%S')}",
        f"- 控制器：`{ctrl['label']}`，代理端口：{mixed}，组：**{gname}**",
        f"- 预筛：`/proxies/<节点>/delay` 打 `{CONFIG['预筛URL']}` 的域名，"
        f"{CONFIG['并发数']} 路并发，{sweep_secs:.1f} 秒扫完 {len(results)} 个节点",
        f"- 预筛通过 {len(survivors)} 个，真 API 确认了 {len(confirmed)} 个",
        f"- 判据：预筛只是「可能行」，**以真 API 拿到回复为准**",
    ]
    md, js = write_report(DEFAULT_OUTDIR, meta, results, survivors, confirmed, winner)

    log("\n" + "=" * 70)
    log(f"  完成，总用时 {time.time() - t0:.1f}s（其中预筛 {sweep_secs:.1f}s）")
    if winner:
        log(f"  ✅ 能直连 Gemini API 的节点：{winner['node']}")
    else:
        log("  ❌ 没找到能直连的节点")
    log(f"  报告：{md}")
    log(f"  数据：{js}")
    log("=" * 70)
    return 0


if __name__ == "__main__":
    try:
        _code = main()
    except KeyboardInterrupt:
        print("\n已中断")
        _code = 130
    try:
        if sys.stdin is not None and sys.stdin.isatty():
            input("\n按回车键关闭 ...")
    except Exception:
        pass
    sys.exit(_code)
