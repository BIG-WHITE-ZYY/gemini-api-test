#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
逐个切 Clash 节点，直接打 Gemini API 发一句 hi，看哪个节点能拿到回复。

和 gemini节点并发测试.py 的区别
-------------------------------
那个脚本走的是网页那套：导出 cookie → 算 SAPISIDHASH → 请求 AI Studio 的
内部 RPC 接口，看 Google 说不说"Region not supported"。
这个是直接把节点切过去、用官方 SDK（google-genai）调 Gemini API 本体：

    client.models.generate_content(model=..., contents="hi")

拿到回复 = 这个节点真能直连 Gemini API 干活，不是"接口没报错"那种间接判断。
不用 cookie，也不会过期。

跑法（在用户本机，不是在这个沙箱里）
------------------------------------
    conda activate wsmx
    pip install google-genai        # 只需装这一次
    python gemini_api直连测试.py

key 的取法：环境变量 GEMINI_API_KEY → 同目录 gemini_api_key.txt → CONFIG 里的 "API_KEY"。
**支持多个 key**：文件里一行一个，环境变量里用逗号隔开。多个 key 会轮流用，
谁撞到 429（免费档 15 次/分钟）就把它冷一会儿换下一个 —— 不用为了躲限流干等。
（注意：AI Studio 的临时 token 形如 AQ.Ab8RN6…，会过期；长期用的是 AIza… 开头的 key）

参数都在下面的「配置区」，改完保存再按 ▶ 跑。
跑完写一份 Markdown + JSON 报告到同目录。找到能用的节点就停，并且不切回去。
"""

import json
import os
import re
import socket
import ssl
import sys
import threading
import time
import urllib.error
import urllib.parse
import urllib.request
from concurrent.futures import ThreadPoolExecutor, as_completed
from concurrent.futures import TimeoutError as FutTimeout

HERE = os.path.dirname(os.path.abspath(__file__))
DEFAULT_OUTDIR = HERE
KEY_FILES = ("gemini_api_key.txt", "gemini_api_key")

UA = ("Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
      "(KHTML, like Gecko) Chrome/140.0.0.0 Safari/537.36")

# ============================================================================
#  配置区 —— 想改参数就改这里，然后按 ▶ 运行
# ============================================================================
CONFIG = {
    # 发给 Gemini 的话
    "测试消息": "hi",

    # 模型名。要是报"模型不存在"，把它换成 AI Studio 里列出来的名字
    # （比如 gemini-2.5-flash / gemini-2.5-pro）
    "模型": "gemini-3.5-flash-lite",

    # 只测这些地区，其他地区直接跳过；留空列表 = 全部节点都测
    # 中英文都行；纯英文的按整词匹配，避免 US 误伤 RUS 这类
    "只测这些地区": [],

    # 报错不是"地区不支持"的节点（连不上之类），放到队尾再试几次
    "允许重试次数": 1,

    # 测哪个代理组；留空 = 自动挑节点最多的那个
    "测哪个组": "",

    # 切换节点后等多久再发请求（秒）
    # 多个 key 轮换时不用担心限流，0.5 秒就够；只有一个 key 时才需要调到 5 左右
    "切换等待秒数": 0.5,

    # 单次 API 请求超时（秒）
    "接口超时秒数": 20,

    # 顺便记录 Google 眼里这个节点在哪个地区（每个节点多约 1 秒）
    "顺便记录Google地区": True,

    # 记出口 IP（经同一个代理问 ip-api）。所有节点 IP 一样 = 切换根本没生效
    "记录出口IP": True,

    # 读控制器的 /connections，记下这次请求实际走的是哪个组 / 哪个节点
    # —— 这是"切换到底有没有生效"最硬的证据，不花额外网络时间
    "记录实际链路": True,

    # 没别的地方放 key 时才填这儿，推荐用环境变量或同目录 gemini_api_key.txt
    "API_KEY": "",
}

# 优先猜的控制器端口
CANDIDATE_PORTS = [
    9090, 9097, 9091, 9099, 9098, 9092, 9093, 9094, 9096, 6170, 63333,
    8000, 8080, 8888, 20171, 33210, 50000, 50001, 60000, 15600, 10000, 12345,
]

# 猜本机混合代理端口
PROXY_PORT_CANDIDATES = [7897, 7890, 7891, 7892, 1080, 10809, 10808, 2080,
                         8889, 8118, 20171, 33210, 7899, 7898, 7078, 1081]

# Clash Verge 在 Windows 上可能的数据目录名
VERGE_DIR_NAMES = [
    "io.github.clash-verge-rev.clash-verge-rev",
    "io.github.clash-verge-rev.clash-verge",
    "io.github.clash-verge-rev",
    "clash-verge",
    "clash-verge-rev",
    "Clash Verge",
]

PIPE_HINTS = ("verge-mihomo", "verge_mihomo", "mihomo", "clash", "verge")


# ----------------------------------------------------------------------------
# 基础设施
# ----------------------------------------------------------------------------

def _setup_stdout():
    """Windows 控制台默认 GBK，节点名里有 emoji 会崩，强制 UTF-8。"""
    try:
        sys.stdout.reconfigure(encoding="utf-8", errors="replace")
        sys.stderr.reconfigure(encoding="utf-8", errors="replace")
    except Exception:
        pass


def log(msg=""):
    print(msg, flush=True)


def http_request(url, method="GET", body=None, headers=None, timeout=8,
                 proxy=None, no_redirect=False):
    """极简 HTTP 客户端。返回 (status, headers, body_bytes)，出错抛异常。"""
    hdrs = {"User-Agent": UA, "Accept": "*/*"}
    if headers:
        hdrs.update(headers)

    data = None
    if body is not None:
        if isinstance(body, (dict, list)):
            data = json.dumps(body).encode("utf-8")
            hdrs.setdefault("Content-Type", "application/json")
        elif isinstance(body, str):
            data = body.encode("utf-8")
        else:
            data = body

    handlers = []
    if proxy:
        handlers.append(urllib.request.ProxyHandler({"http": proxy, "https": proxy}))
    else:
        handlers.append(urllib.request.ProxyHandler({}))  # 显式不走代理

    ctx = ssl.create_default_context()
    ctx.check_hostname = False
    ctx.verify_mode = ssl.CERT_NONE
    handlers.append(urllib.request.HTTPSHandler(context=ctx))

    if no_redirect:
        class _NoRedirect(urllib.request.HTTPRedirectHandler):
            def redirect_request(self, req, fp, code, msg, headers, newurl):
                return None
        handlers.append(_NoRedirect())

    opener = urllib.request.build_opener(*handlers)
    req = urllib.request.Request(url, data=data, headers=hdrs, method=method)
    try:
        with opener.open(req, timeout=timeout) as resp:
            return resp.status, dict(resp.headers), resp.read()
    except urllib.error.HTTPError as e:
        return e.code, dict(e.headers or {}), e.read()


def port_open(host, port, timeout=0.35):
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s.settimeout(timeout)
    try:
        return s.connect_ex((host, port)) == 0
    except Exception:
        return False
    finally:
        try:
            s.close()
        except Exception:
            pass


def find_proxy_port(verbose=True):
    """自己找出本机哪个端口是能出网的代理。"""
    for p in PROXY_PORT_CANDIDATES:
        if not port_open("127.0.0.1", p):
            continue
        try:
            _st, _h, body = http_request("http://ip-api.com/json",
                                         proxy=f"http://127.0.0.1:{p}",
                                         timeout=8)
            if json.loads(body.decode("utf-8", "replace")).get("query"):
                if verbose:
                    log(f"  [发现] 本机代理端口 -> {p}")
                return p
        except Exception:
            continue
    return None


# ----------------------------------------------------------------------------
# 第一步：找到 Clash 外部控制器
# ----------------------------------------------------------------------------

def find_verge_dirs():
    dirs = []
    for env in ("APPDATA", "LOCALAPPDATA", "USERPROFILE"):
        base = os.environ.get(env)
        if not base:
            continue
        for name in VERGE_DIR_NAMES:
            p = os.path.join(base, name)
            if os.path.isdir(p):
                dirs.append(p)
    seen, out = set(), []
    for d in dirs:
        if d.lower() not in seen:
            seen.add(d.lower())
            out.append(d)
    return out


def read_verge_yaml_hints():
    """从 Clash Verge 的配置里挖控制器地址 / 命名管道 / secret / mixed-port。"""
    hints = {"controller": None, "secret": None, "mixed_port": None,
             "pipes": [], "files": [], "dirs": []}
    for d in find_verge_dirs():
        hints["dirs"].append(d)
        for root, dirs, files in os.walk(d):
            if root.count(os.sep) - d.count(os.sep) > 4:
                dirs[:] = []
                continue
            dirs[:] = [x for x in dirs
                       if x.lower() not in ("logs", "node_modules", ".git",
                                            "cache", "temp")]
            for fn in files:
                if not fn.lower().endswith((".yaml", ".yml")):
                    continue
                fp = os.path.join(root, fn)
                try:
                    if os.path.getsize(fp) > 6 * 1024 * 1024:
                        continue
                    with open(fp, "r", encoding="utf-8", errors="ignore") as f:
                        txt = f.read()
                except Exception:
                    continue
                hints["files"].append(fp)

                for key in ("external-controller", "external_controller"):
                    m = re.search(r'^\s*' + key + r'\s*:\s*["\']?([^\s"\']+)',
                                  txt, re.MULTILINE)
                    if m and not hints["controller"]:
                        hints["controller"] = m.group(1)

                m = re.search(r'^\s*external-controller-pipe\s*:\s*["\']?([^\s"\']+)',
                              txt, re.MULTILINE)
                if m:
                    v = m.group(1).strip("'\"")
                    if v and v not in hints["pipes"]:
                        hints["pipes"].append(v)

                for pm in re.finditer(r'\\\\\.\\pipe\\[A-Za-z0-9_.\-]+', txt):
                    v = pm.group(0)
                    if v not in hints["pipes"]:
                        hints["pipes"].append(v)

                m = re.search(r'^\s*secret\s*:\s*["\']?([^\s"\']+)', txt, re.MULTILINE)
                if m and hints["secret"] is None:
                    val = m.group(1).strip("'\"")
                    if val:
                        hints["secret"] = val

                m = re.search(r'^\s*(?:verge_)?mixed[-_]port\s*:\s*(\d+)',
                              txt, re.MULTILINE)
                if m and not hints["mixed_port"]:
                    hints["mixed_port"] = int(m.group(1))

                m = re.search(r'^\s*verge_mixed_port\s*:\s*(\d+)', txt, re.MULTILINE)
                if m and not hints["mixed_port"]:
                    hints["mixed_port"] = int(m.group(1))
    return hints


def discover_controller(verbose=True):
    """找一个可用的 mihomo 控制器。先试命名管道（Verge Rev 默认），再试 TCP 端口。"""
    hints = read_verge_yaml_hints()
    secrets = []
    for s in (hints.get("secret"), None):
        if s not in secrets:
            secrets.append(s)

    # ---------- 1) 命名管道 ----------
    pipes, all_pipes = candidate_pipes()
    for p in hints.get("pipes", []):
        if p and p not in pipes:
            pipes.insert(0, p)

    if verbose:
        rel = [n for n in all_pipes
               if any(h in n.lower() for h in PIPE_HINTS)] if all_pipes else []
        if all_pipes:
            log(f"  本机命名管道总数 {len(all_pipes)}，其中疑似控制器的: {rel or '无'}")
        elif os.name != "nt":
            log("  (非 Windows 系统，跳过命名管道)")

    for pname in pipes:
        res = _probe_transport({"kind": "pipe", "target": pname, "secret": None})
        if res and res.get("_auth_required"):
            for sec in secrets:
                if not sec:
                    continue
                res2 = _probe_transport({"kind": "pipe", "target": pname,
                                         "secret": sec})
                if res2 and not res2.get("_auth_required"):
                    if verbose:
                        log(f"  [发现] Clash 控制器 -> 命名管道 {pname} "
                            f"(带 secret，内核 {res2.get('version','?')})")
                    return {"kind": "pipe", "target": pname, "secret": sec,
                            "label": f"pipe:{pname}"}
            if verbose:
                log(f"  [注意] 管道 {pname} 需要 secret，但配置文件里没读到")
            continue
        if res:
            if verbose:
                log(f"  [发现] Clash 控制器 -> 命名管道 {pname}  "
                    f"内核 {res.get('version','?')}")
            return {"kind": "pipe", "target": pname, "secret": None,
                    "label": f"pipe:{pname}"}

    # ---------- 2) TCP 候选端口 ----------
    ports = []
    c = hints.get("controller")
    if c and ":" in c and not c.startswith("\\\\"):
        tail = c.rsplit(":", 1)[-1]
        if tail.isdigit():
            ports.append(int(tail))
    ports += CANDIDATE_PORTS

    seen, ordered = set(), []
    for p in ports:
        if p not in seen:
            seen.add(p)
            ordered.append(p)

    if verbose:
        log(f"  管道没找到，试 {len(ordered)} 个候选 TCP 端口 ...")
    for p in ordered:
        if not port_open("127.0.0.1", p):
            continue
        for sec in secrets:
            res = _probe_transport({"kind": "tcp", "target": p, "secret": sec})
            if res and not res.get("_auth_required"):
                if verbose:
                    log(f"  [发现] Clash 控制器 -> 127.0.0.1:{p}  "
                        f"内核 {res.get('version','?')}")
                return {"kind": "tcp", "target": p, "secret": sec,
                        "label": f"127.0.0.1:{p}"}

    # ---------- 3) 全端口扫描兜底 ----------
    if verbose:
        log("  开始全端口扫描本机 127.0.0.1（这个要等一会儿）...")
    open_ports = []
    with ThreadPoolExecutor(max_workers=256) as ex:
        futs = {ex.submit(port_open, "127.0.0.1", p, 0.25): p
                for p in range(1, 65536)}
        for f in as_completed(futs):
            try:
                if f.result():
                    open_ports.append(futs[f])
            except Exception:
                pass
    open_ports.sort()
    if verbose:
        log(f"  本机开放端口: {open_ports}")

    for p in open_ports:
        for sec in secrets:
            res = _probe_transport({"kind": "tcp", "target": p, "secret": sec})
            if res and not res.get("_auth_required"):
                if verbose:
                    log(f"  [发现] Clash 控制器 -> 127.0.0.1:{p}")
                return {"kind": "tcp", "target": p, "secret": sec,
                        "label": f"127.0.0.1:{p}"}
    return None


def diagnose():
    """把环境信息全打出来，找不到控制器时跑这个。"""
    log("=" * 70)
    log("  环境诊断")
    log("=" * 70)
    log(f"  平台: {sys.platform}   Python: {sys.version.split()[0]}")

    log("\n[1] Clash Verge 数据目录")
    dirs = find_verge_dirs()
    if dirs:
        for d in dirs:
            log(f"    ✓ {d}")
    else:
        log("    ✗ 没找到。APPDATA / LOCALAPPDATA 下没有已知的 Verge 目录名")

    log("\n[2] 配置里的控制器线索")
    hints = read_verge_yaml_hints()
    log(f"    external-controller : {hints.get('controller')}")
    log(f"    external-controller-pipe : {hints.get('pipes')}")
    log(f"    secret              : {'有' if hints.get('secret') else '无'}")
    log(f"    mixed-port          : {hints.get('mixed_port')}")
    log(f"    扫到的 yaml 文件共 {len(hints.get('files', []))} 个")
    for f in hints.get("files", [])[:12]:
        log(f"      · {f}")

    log("\n[3] 命名管道")
    pipes, all_pipes = candidate_pipes()
    log(f"    本机管道总数: {len(all_pipes)}")
    rel = [n for n in all_pipes if any(h in n.lower() for h in PIPE_HINTS)]
    log(f"    疑似控制器的: {rel or '无'}")
    for p in rel:
        full = f"\\\\.\\pipe\\{p}"
        r = _probe_transport({"kind": "pipe", "target": full, "secret": None})
        log(f"      {full} -> {r if r else '连上但没响应 /version'}")
    if not all_pipes:
        log("    （非 Windows，或 os.listdir 读不了管道目录）")

    log("\n[4] TCP 控制器端口")
    found = False
    for p in CANDIDATE_PORTS + [7897, 7890]:
        if port_open("127.0.0.1", p):
            r = _probe_transport({"kind": "tcp", "target": p, "secret": None})
            log(f"    {p} 开放  -> {r if r else '不是 mihomo 控制器'}")
            found = True
    if not found:
        log("    候选端口一个都没开")

    log("\n[5] 代理端口能不能出网")
    pp = find_proxy_port()
    if pp:
        try:
            _s, _h, b = http_request("http://ip-api.com/json",
                                     proxy=f"http://127.0.0.1:{pp}", timeout=10)
            j = json.loads(b.decode("utf-8", "replace"))
            log(f"    出口: {j.get('country')} {j.get('city')} {j.get('query')}")
        except Exception as e:
            log(f"    端口 {pp} 在但出网失败: {e}")

    log("\n[6] google-genai 装没装")
    try:
        from google import genai  # noqa: F401
        log("    ✓ 已安装")
    except Exception as e:
        log(f"    ✗ 没装好：{e}")
        log("      装一下：pip install google-genai")

    log("\n[7] API key 从哪儿取的")
    _k, src = load_api_key()
    log(f"    {'✓ ' + str(src) if _k else '✗ 没找到 key'}")

    log("\n" + "=" * 70)
    log("  把上面全部内容复制给 Claude")
    log("=" * 70)
    return 0


# ----------------------------------------------------------------------------
# 控制器通道：TCP 端口 或 Windows 命名管道
# ----------------------------------------------------------------------------

# 同时打开的命名管道连接数上限
_PIPE_SEM = threading.Semaphore(20)


def list_windows_pipes():
    """列出本机所有命名管道名（Windows 专有，其它系统返回 []）。"""
    try:
        return sorted(os.listdir("\\\\.\\pipe\\"))
    except Exception:
        return []


def candidate_pipes():
    """猜控制器管道名：先看系统上实际存在的，再兜底几个常见名字。"""
    found = list_windows_pipes()
    hits = [n for n in found
            if any(h in n.lower() for h in PIPE_HINTS)]
    hits = [n for n in hits
            if not any(bad in n.lower() for bad in
                       ("crashpad", "chrome", "discord", "steam", "code",
                        "window", "nvagent", "splunk", "dropbox", "obs"))]
    out = [f"\\\\.\\pipe\\{n}" for n in hits]
    for guess in ("\\\\.\\pipe\\verge-mihomo", "\\\\.\\pipe\\clash-verge",
                  "\\\\.\\pipe\\mihomo", "\\\\.\\pipe\\clash"):
        if guess not in out:
            out.append(guess)
    return out, found


def _dechunk(buf):
    """极简 chunked 解码。"""
    out = b""
    while True:
        i = buf.find(b"\r\n")
        if i < 0:
            return out or buf
        try:
            n = int(buf[:i].split(b";")[0].strip(), 16)
        except Exception:
            return out or buf
        if n == 0:
            return out
        out += buf[i + 2:i + 2 + n]
        buf = buf[i + 2 + n + 2:]


def _pipe_raw(pipe_name, raw, timeout):
    """在命名管道上发 HTTP，按 Content-Length 读回响应，不依赖连接关闭。"""
    O_BINARY = getattr(os, "O_BINARY", 0)  # Windows 专有，其它平台为 0

    def _do():
        fd = os.open(pipe_name, os.O_RDWR | O_BINARY)
        try:
            os.write(fd, raw)
            buf = b""
            while b"\r\n\r\n" not in buf:
                c = os.read(fd, 65536)
                if not c:
                    break
                buf += c
            head, sep, rest = buf.partition(b"\r\n\r\n")
            if not sep:
                return buf
            hl = head.decode("latin-1").lower()
            cl, chunked = None, "transfer-encoding: chunked" in hl
            for ln in hl.split("\r\n"):
                if ln.startswith("content-length:"):
                    try:
                        cl = int(ln.split(":", 1)[1].strip())
                    except Exception:
                        cl = None
            if chunked:
                while not rest.endswith(b"0\r\n\r\n"):
                    try:
                        c = os.read(fd, 65536)
                    except OSError:
                        break
                    if not c:
                        break
                    rest += c
                rest = _dechunk(rest)
            elif cl is not None:
                while len(rest) < cl:
                    c = os.read(fd, 65536)
                    if not c:
                        break
                    rest += c
            return head + sep + rest
        finally:
            try:
                os.close(fd)
            except Exception:
                pass

    # 注意：不能用 with 语句——它退出时会 shutdown(wait=True)，
    # 万一管道读卡住，timeout 就形同虚设，整个脚本会永久挂死。
    last_err = None
    for attempt in range(5):
        def _guarded():
            # Windows 命名管道实例数有限，并发开太多会报
            # "All pipe instances are busy"（winerror 231）。这里限流。
            if not _PIPE_SEM.acquire(timeout=max(2, timeout - 1)):
                raise FutTimeout("等管道空闲超时")
            try:
                return _do()
            finally:
                _PIPE_SEM.release()

        ex = ThreadPoolExecutor(max_workers=1)
        try:
            return ex.submit(_guarded).result(timeout=timeout)
        except FutTimeout:
            raise
        except OSError as e:
            last_err = e
            busy = getattr(e, "winerror", None) == 231 or "busy" in str(e).lower()
            if not busy or attempt == 4:
                raise
            time.sleep(0.12 * (attempt + 1))
        finally:
            try:
                ex.shutdown(wait=False)
            except Exception:
                pass
    if last_err:
        raise last_err


def pipe_request(pipe_name, method, path, body=None, headers=None, timeout=15):
    hdrs = dict(headers or {})
    hdrs.setdefault("Host", "localhost")
    hdrs.setdefault("Connection", "close")
    data = b""
    if body is not None:
        if isinstance(body, (dict, list)):
            data = json.dumps(body).encode("utf-8")
            hdrs.setdefault("Content-Type", "application/json")
        elif isinstance(body, str):
            data = body.encode("utf-8")
        else:
            data = body
        hdrs["Content-Length"] = str(len(data))

    raw = (f"{method} {path} HTTP/1.1\r\n"
           + "".join(f"{k}: {v}\r\n" for k, v in hdrs.items())
           + "\r\n").encode("ascii", "replace") + data

    resp = _pipe_raw(pipe_name, raw, timeout)
    head, sep, bodyb = resp.partition(b"\r\n\r\n")
    lines = head.decode("latin-1").split("\r\n")
    try:
        status = int(lines[0].split()[1])
    except Exception:
        status = 0
    hout = {}
    for ln in lines[1:]:
        if ":" in ln:
            k, v = ln.split(":", 1)
            hout[k.strip().lower()] = v.strip()
    return status, hout, bodyb


def _norm(status, data):
    try:
        return status, json.loads(data.decode("utf-8", "replace"))
    except Exception:
        return status, data


def api(ctrl, path, method="GET", body=None, timeout=15):
    """对控制器发请求。ctrl = {"kind": "tcp"|"pipe", "target": ..., "secret": ...}"""
    headers = {}
    if ctrl.get("secret"):
        headers["Authorization"] = f"Bearer {ctrl['secret']}"
    if ctrl.get("kind") == "pipe":
        status, _h, data = pipe_request(ctrl["target"], method, path,
                                        body=body, headers=headers, timeout=timeout)
        return _norm(status, data)
    url = f"http://127.0.0.1:{ctrl['target']}{path}"
    status, _h, data = http_request(url, method=method, body=body,
                                    headers=headers, timeout=timeout)
    return _norm(status, data)


def _probe_transport(ctrl, timeout=6):
    """探测某个通道是不是可用的 mihomo 控制器。"""
    try:
        status, _h, data = (pipe_request(ctrl["target"], "GET", "/version",
                                         headers=({"Authorization":
                                                   f"Bearer {ctrl['secret']}"}
                                                  if ctrl.get("secret") else None),
                                         timeout=timeout)
                            if ctrl["kind"] == "pipe"
                            else http_request(
                                f"http://127.0.0.1:{ctrl['target']}/version",
                                timeout=timeout,
                                headers=({"Authorization":
                                          f"Bearer {ctrl['secret']}"}
                                         if ctrl.get("secret") else None)))
    except Exception:
        return None
    if status == 401:
        return {"_auth_required": True}
    if status != 200:
        return None
    try:
        j = json.loads(data.decode("utf-8", "replace"))
    except Exception:
        return None
    if isinstance(j, dict) and ("version" in j or "meta" in j):
        return j
    return None


# ----------------------------------------------------------------------------
# 第二步：挑出节点
# ----------------------------------------------------------------------------

def pick_group(proxies, want=None):
    """挑出装着真实节点的 Selector 组。"""
    groups = []
    for name, p in proxies.items():
        if p.get("type") in ("Selector", "Fallback", "URLTest", "LoadBalance"):
            alln = p.get("all") or []
            if alln:
                groups.append((name, p.get("type"), alln, p.get("now")))
    if not groups:
        return None
    if want:
        for g in groups:
            if g[0] == want:
                return g
        for g in groups:
            if want in g[0]:
                return g
        log(f"  [警告] 没找到组 '{want}'，改用默认规则挑")
    skip = {"GLOBAL", "DIRECT", "REJECT", "REJECT-DROP", "PASS", "COMPATIBLE"}
    sel = [g for g in groups if g[1] == "Selector" and g[0] not in skip]
    if not sel:
        sel = [g for g in groups if g[0] not in skip]
    pool = sel if sel else groups
    pool.sort(key=lambda g: len(g[2]), reverse=True)
    return pool[0]


def region_match(name, keywords):
    """节点名里有没有这些地区关键词。纯英文的按整词匹配，防 US 误伤 RUS。"""
    for k in keywords:
        if not k:
            continue
        if k.isascii():
            if re.search(r"(?<![a-z0-9])" + re.escape(k.lower()) + r"(?![a-z0-9])",
                         name.lower()):
                return True
        elif k in name:
            return True
    return False


# Google 的区域域名后缀 -> 国家码
_G_SUFFIX = {
    "": "US", "com": "US", "com.hk": "HK", "cn": "CN", "co.jp": "JP",
    "co.uk": "GB", "co.kr": "KR", "com.tw": "TW", "com.sg": "SG",
    "com.au": "AU", "co.in": "IN", "com.br": "BR", "de": "DE", "fr": "FR",
    "ca": "CA", "com.mx": "MX", "it": "IT", "es": "ES", "nl": "NL",
    "ru": "RU", "co.nz": "NZ", "com.my": "MY", "co.th": "TH",
    "com.vn": "VN", "co.id": "ID", "com.ph": "PH", "com.ar": "AR",
}


def exit_ip(proxy_url):
    """经同一个代理问 ip-api：这个节点真正的出口 IP 是啥。

    32 个节点如果全是同一个 IP，说明切换没生效 / 规则没走这个组。
    """
    try:
        _st, _h, body = http_request("http://ip-api.com/json",
                                     proxy=proxy_url, timeout=10)
        j = json.loads(body.decode("utf-8", "replace"))
        cc = j.get("countryCode") or j.get("country") or "?"
        city = j.get("city") or ""
        return j.get("query"), (f"{cc} {city}".strip() or None)
    except Exception:
        return None, None


def _conn_start(c):
    """这条连接是什么时候建的（mihomo 的 start 字段是 RFC3339），拿不到返回 0。"""
    s = (c or {}).get("start") or ""
    try:
        return time.mktime(time.strptime(s[:19], "%Y-%m-%dT%H:%M:%S"))
    except Exception:
        return 0.0


def current_chains(ctrl, host_kw=("googleapis.com", "generativelanguage")):
    """从控制器的 /connections 里捞出刚那条请求实际走的链路。

    返回 {"chains": ["节点", "组"], "rule": ..., "rulePayload": ..., "host": ...}，
    找不到返回 None。链路的第一个是真正干活的节点，最后一个是命中的组。
    """
    try:
        _st, j = api(ctrl, "/connections", timeout=10)
    except Exception:
        return None
    if not isinstance(j, dict):
        return None

    best = None
    for c in (j.get("connections") or []):
        meta = c.get("metadata") or {}
        host = (meta.get("host") or "").lower()
        payload = (c.get("rulePayload") or "").lower()
        # 优先认 host，其次认命中规则里的 google
        if any(k in host for k in host_kw) or (payload and "google" in payload):
            # 列表里常常还留着上一个节点的旧连接，挑错了就会误报
            # "实际走的不是刚切的那个"。按建立时间挑最新的一条。
            if best is None or _conn_start(c) >= _conn_start(best):
                best = c
    if not best:
        return None
    meta = best.get("metadata") or {}
    return {"chains": best.get("chains") or [],
            "rule": best.get("rule"),
            "rulePayload": best.get("rulePayload"),
            "host": meta.get("host") or meta.get("destinationIP")}


def google_geo(proxy_url):
    """问 Google「你觉得我在哪个地区」—— 看 www.google.com 往哪个区域域名跳。

    这是 Google 自己的地理定位，和 ip-api / Cloudflare 的结论经常不一致，
    而只有 Google 这个说了算（Google 的地区判定以它为准）。
    """
    for url in ("https://www.google.com/", "https://www.google.com/ncr"):
        try:
            _st, hd, _b = http_request(url, proxy=proxy_url, timeout=15,
                                       no_redirect=True)
        except Exception:
            continue
        loc = hd.get("Location") or hd.get("location") or url
        host = urllib.parse.urlparse(loc).netloc.lower() or "www.google.com"
        if "google." in host:
            suffix = host.split("google.", 1)[1].strip("/")
            return _G_SUFFIX.get(suffix, suffix.upper() or None), suffix
        return None, host
    return None, None


# ----------------------------------------------------------------------------
# 第三步：用 google-genai 直接打 Gemini API
# ----------------------------------------------------------------------------

def load_api_keys():
    """把所有能拿到的 key 都收进来（去重、保序）。返回 (keys, 来源说明)。

    来源，按顺序：
      1. 环境变量 GEMINI_API_KEY / GEMINI_API_KEYS
         （多个用逗号、分号或空格隔开）
      2. 同目录的 gemini_api_key.txt / gemini_api_key
         （一行一个，# 开头当注释）
      3. 脚本顶部 CONFIG 的 "API_KEY"
    """
    keys, srcs = [], []

    env = ((os.environ.get("GEMINI_API_KEY") or "") + ";"
           + (os.environ.get("GEMINI_API_KEYS") or ""))
    n_env = 0
    for part in re.split(r"[,;\s]+", env.strip()):
        if part:
            keys.append(part)
            n_env += 1
    if n_env:
        srcs.append(f"环境变量（{n_env} 个）")

    for fn in KEY_FILES:
        p = os.path.join(HERE, fn)
        if not os.path.exists(p):
            continue
        got = []
        try:
            with open(p, "r", encoding="utf-8", errors="ignore") as f:
                for line in f:
                    v = line.strip()
                    if v and not v.startswith("#"):
                        got.append(v)
        except Exception:
            continue
        if got:
            keys.extend(got)
            srcs.append(f"{fn}（{len(got)} 个）")

    k = (CONFIG.get("API_KEY") or "").strip()
    if k:
        keys.append(k)
        srcs.append("脚本配置区")

    seen, out = set(), []
    for k in keys:
        if k not in seen:
            seen.add(k)
            out.append(k)
    return out, ("、".join(srcs) if srcs else None)


def load_api_key():
    """（兼容只取一个 key 的老调用）"""
    ks, src = load_api_keys()
    return (ks[0], src) if ks else (None, None)


def parse_retry_delay(text, default=60.0):
    """从 429 的报错里把它要求的等待秒数抠出来。"""
    t = text or ""
    for pat in (r"retryDelay'?\s*:?\s*'?(\d+(?:\.\d+)?)s",
                r"retry in (\d+(?:\.\d+)?)s",
                r"retry after (\d+(?:\.\d+)?)"):
        m = re.search(pat, t, re.I)
        if m:
            try:
                return min(300.0, float(m.group(1)) + 2.0)
            except Exception:
                pass
    return float(default)


class KeyRing:
    """一堆 key 轮着用；谁撞到 429 就把它冷一会儿，换下一个接着打。

    有了它就不用为了躲限流在节点之间干等了 —— 免费档是每个 key 15 次/分钟，
    8 个 key 就是 120 次/分钟，逐个节点测根本用不满。
    """

    def __init__(self, keys, default_cool=60.0):
        self.keys = list(keys)
        self.default_cool = float(default_cool)
        self.dead_until = {}
        self.stats = {}
        self._i = 0
        self._lock = threading.Lock()

    def __len__(self):
        return len(self.keys)

    def _st(self, k):
        return self.stats.setdefault(
            k, {"发出": 0, "成功": 0, "限流": 0, "坏key": 0, "报错": 0})

    def cool_left(self, k):
        return max(0.0, self.dead_until.get(k, 0.0) - time.time())

    def acquire(self):
        """拿一个现在能用的 key；全都在冷却就返回最快解冻的那个。"""
        with self._lock:
            now = time.time()
            n = len(self.keys)
            for _ in range(n):
                k = self.keys[self._i % n]
                self._i += 1
                if self.dead_until.get(k, 0.0) <= now:
                    return k
            return min(self.keys, key=lambda x: self.dead_until.get(x, 0.0))

    def min_cool_left(self):
        if not self.keys:
            return 0.0
        return min(self.cool_left(k) for k in self.keys)

    def cool(self, k, seconds=None, why="429"):
        secs = float(seconds if seconds else self.default_cool)
        with self._lock:
            self.dead_until[k] = max(self.dead_until.get(k, 0.0),
                                     time.time() + secs)
            if why == "429":
                self._st(k)["限流"] += 1
            else:
                self._st(k)["坏key"] += 1
        log(f"        ⏳ {mask_key(k)} 冷却 {int(secs)}s（{why}），换下一个 key")

    def note(self, k, kind):
        with self._lock:
            st = self._st(k)
            st["发出"] += 1
            if kind == "ok":
                st["成功"] += 1
            elif kind == "err":
                st["报错"] += 1

    def alive(self):
        now = time.time()
        return [k for k in self.keys if self.dead_until.get(k, 0.0) <= now]

    def summary(self):
        live = len(self.alive())
        s = f"{live}/{len(self.keys)} 个 key 可用"
        hot = sorted(((k, self.cool_left(k)) for k in self.keys
                      if self.cool_left(k) > 0), key=lambda x: x[1])
        if hot:
            s += "；冷却中：" + "、".join(
                f"{mask_key(k)}({int(v)}s)" for k, v in hot[:4])
            if len(hot) > 4:
                s += f" 等 {len(hot)} 个"
        bad = [k for k, st in self.stats.items() if st.get("坏key")]
        if bad:
            s += f"；被判为坏 key：{'、'.join(mask_key(k) for k in bad[:4])}"
        return s


def mask_key(k):
    if not k:
        return "(空)"
    return k[:6] + "…" + k[-4:] if len(k) > 12 else "…"


def make_gemini_client(api_key, proxy_url, timeout_s):
    """建一个走指定代理的 genai client。

    代理两条路一起上：一是环境变量（httpx 默认 trust_env=True 会认），
    二是直接塞给 httpx 的 client_args。哪条生效都行。
    """
    from google import genai
    from google.genai import types

    for var in ("HTTP_PROXY", "HTTPS_PROXY", "http_proxy", "https_proxy"):
        os.environ[var] = proxy_url
    for var in ("ALL_PROXY", "all_proxy", "NO_PROXY", "no_proxy"):
        os.environ.pop(var, None)

    kwargs = {"api_key": api_key}
    try:
        kwargs["http_options"] = types.HttpOptions(
            timeout=int(timeout_s * 1000),
            client_args={"proxy": proxy_url, "trust_env": True},
            async_client_args={"proxy": proxy_url, "trust_env": True},
        )
    except Exception:
        # 老版本 SDK 没有 client_args，退回只靠环境变量
        try:
            kwargs["http_options"] = types.HttpOptions(timeout=int(timeout_s * 1000))
        except Exception:
            pass
    return genai.Client(**kwargs)


def ask_gemini(client, model, prompt, timeout_s):
    """发一句 prompt，返回记录用的字典。不抛异常。"""
    from google.genai import errors
    t = time.time()
    try:
        resp = client.models.generate_content(model=model, contents=prompt)
        try:
            txt = (getattr(resp, "text", None) or "").strip()
        except Exception:
            txt = ""
        if not txt:
            return {"ok": False, "status": 200, "empty": True,
                    "text": "有响应但没文本（可能被安全策略拦了 / 响应被截断）",
                    "elapsed": round(time.time() - t, 2)}
        return {"ok": True, "status": 200, "text": txt,
                "elapsed": round(time.time() - t, 2)}
    except Exception as e:
        code = getattr(e, "code", None) if isinstance(e, errors.APIError) else None
        if not isinstance(code, int):
            code = None
        return {"ok": False, "status": code, "text": f"{type(e).__name__}: {e}",
                "api_error": isinstance(e, errors.APIError),
                "elapsed": round(time.time() - t, 2)}


_REGION_HINTS = ("location is not supported", "user location",
                 "not available in your country", "region not supported",
                 "failed_precondition")
_KEY_HINTS = ("api key not valid", "api_key_invalid", "api key is invalid",
              "invalid api key", "api key expired", "api key not found",
              "unauthenticated", "unauthorized", "permission_denied")
_MODEL_HINTS = ("is not found for api version", "not_found", "not found",
                "does not exist", "invalid model", "unsupported model")
_QUOTA_HINTS = ("resource_exhausted", "quota", "rate limit", "too many requests")


def classify(r):
    """把一次调用的结果翻译成 (成功?, 类型, 人话)。"""
    txt = (r.get("text") or "")
    low = txt.lower()
    if r.get("ok"):
        return True, "ok", "✅ 拿到回复"
    if r.get("empty"):
        return False, "other", "⚠️ 有响应但没文本"
    if any(h in low for h in _REGION_HINTS):
        return False, "region", "❌ 地区不支持（User location is not supported）"
    if any(h in low for h in _KEY_HINTS):
        return False, "key", "⚠️ API key 有问题（跟节点无关）"
    if any(h in low for h in _MODEL_HINTS):
        return False, "model", "⚠️ 模型名可能不对（跟节点无关）"
    if any(h in low for h in _QUOTA_HINTS):
        return False, "quota", "⚠️ 限流 / 配额用完（跟节点无关）"
    if r.get("api_error"):
        return False, "api", f"❌ HTTP {r.get('status')}：{txt[:60]}"
    return False, "net", f"❌ 连不上：{txt[:60]}"


# ----------------------------------------------------------------------------
# 报告
# ----------------------------------------------------------------------------

def switch_warnings(results):
    """判断"切节点"这件事到底有没有生效 —— 这里最容易把人带沟里。"""
    out = []
    if len(results) < 2:
        return out
    ips = [r.get("exit_ip") for r in results if r.get("exit_ip")]
    chains = [tuple(r.get("chains") or []) for r in results if r.get("chains")]
    if ips and len(set(ips)) == 1:
        out.append(f"⚠️ **{len(results)} 个节点的出口 IP 全是 `{ips[0]}`** —— "
                   "说明切换没生效，或者你的分流规则没让这些流量走这个组。"
                   "这种情况下逐个节点的结果没有意义。")
    if chains and len(set(chains)) == 1:
        out.append(f"⚠️ **每次请求实际走的链路都是 `{' → '.join(chains[0])}`** —— "
                   "同上：切组没影响到这些请求，结果不能按节点解读。")
    # 链路格式是「节点 → 组」，所以"这条连接是不是刚切的那个节点"要看
    # 节点名在不在链路里，不能看链路最后一项（那是组名）。
    mism = [r for r in results if (r.get("chains") or [])
            and r["node"] not in r["chains"]]
    if mism and len(mism) == len(results):
        out.append("⚠️ 每次「实际走的节点」都和刚切的节点对不上 —— 大概率是分流规则"
                   "把 Google / Gemini 的流量交给了别的组，脚本切的这个组根本不在链路上。")
    glob = [r for r in results if "GLOBAL" in (r.get("chains") or [])]
    if glob and len(glob) == len(results):
        out.append("⚠️ 每条链路里都有 `GLOBAL` —— Clash 很可能在**全局模式**，所有流量都走 "
                   "GLOBAL 里选中的那个节点，切别的组完全不影响流量。把 Verge 的模式切回"
                   "「规则」，或者把 CONFIG 的「测哪个组」设成 `GLOBAL`。")
    return out


def write_report(outdir, meta_lines, results, winner):
    ts = time.strftime("%Y%m%d_%H%M%S")
    os.makedirs(outdir, exist_ok=True)
    md_path = os.path.join(outdir, f"gemini直连报告_{ts}.md")
    js_path = os.path.join(outdir, f"gemini直连报告_{ts}.json")

    with open(js_path, "w", encoding="utf-8") as f:
        json.dump({"时间": time.strftime("%Y-%m-%d %H:%M:%S"),
                   "结论_找到可用节点": winner["node"] if winner else None,
                   "明细": results}, f, ensure_ascii=False, indent=2)

    L = ["# 哪个 Clash 节点能直连 Gemini API\n"]
    L += meta_lines
    L.append("")
    L.append("## 逐个节点结果\n")
    L.append("| 节点 | 结果 | Gemini 说 | 出口 IP | 实际走的是 | Google 认为你在 | 耗时 |")
    L.append("|------|------|-----------|---------|------------|-----------------|------|")
    for r in results:
        gc = r.get("google_country") or "?"
        gs = r.get("google_suffix")
        label = f"{gc}" + (f" (google.{gs})" if gs else "")
        say = (r.get("text") or "").replace("\n", " ")[:60].replace("|", "/")
        iploc = r.get("exit_ip_loc")
        ip = r.get("exit_ip") or "?"
        ipcell = f"{ip}" + (f" ({iploc})" if iploc else "")
        ch = r.get("chains") or []
        chcell = " → ".join(ch) if ch else "?"
        L.append(f"| {r['node']} | {r.get('why')} | `{say}` | {ipcell} | "
                 f"{chcell} | {label} | {r.get('elapsed')}s |")
    L.append("")
    L.append("## 结论\n")
    warn = switch_warnings(results)
    if warn:
        for w in warn:
            L.append(w + "\n")
    if winner:
        L.append(f"**✅ 找到能直连的节点：`{winner['node']}`**\n")
        L.append(f"Gemini 回的是：`{(winner.get('text') or '')[:200]}`\n")
        L.append("这个节点已经在 Clash 里选中，直接拿去用就行"
                 "（脚本没有切回原来的节点）。\n")
    else:
        L.append("**❌ 这一轮没有能直连的节点。**\n")
        kinds = {}
        for r in results:
            kinds[r.get("kind")] = kinds.get(r.get("kind"), 0) + 1
        region = kinds.get("region", 0)
        if region and region >= max(2, len(results) // 2):
            L.append(f"注意：**{region}/{len(results)} 个节点都被判为地区不支持** —— "
                     "说明是这批 IP 段被 Google 划到了不支持的地区，"
                     "不是你选的节点不够好。换 VPN / 换 IP 段才可能有用。\n")
        elif kinds.get("net", 0):
            L.append(f"{kinds.get('net', 0)} 个节点连不上 —— "
                     "先确认 Clash 是通的、代理端口没变，再重跑一次。\n")
        else:
            L.append("节点地区看着没问题但一律失败，更可能是账号或 key 的问题。\n")
    with open(md_path, "w", encoding="utf-8") as f:
        f.write("\n".join(L))
    return md_path, js_path


# ----------------------------------------------------------------------------
# 主流程
# ----------------------------------------------------------------------------

def main():
    _setup_stdout()

    cfg = CONFIG
    model = cfg["模型"]
    prompt = cfg["测试消息"]
    timeout_s = float(cfg["接口超时秒数"])

    t0 = time.time()
    log("=" * 70)
    log("  逐个切节点，直连 Gemini API 发一句 hello —— 看哪个节点真能用")
    log("=" * 70)
    log(f"  模型：{model}　　消息：{prompt!r}")

    # --- 0) key / SDK ---
    log("\n[0/4] 检查 API key 和 google-genai ...")
    keys, src = load_api_keys()
    if not keys:
        log("  ✗ 没找到 API key。三种放法任选一种：")
        log("      1. 设环境变量 GEMINI_API_KEY（多个用逗号隔开）")
        log(f"      2. 写进 {os.path.join(HERE, 'gemini_api_key.txt')}，一行一个")
        log("      3. 填进脚本顶部 CONFIG 的 \"API_KEY\"")
        return 2
    ring = KeyRing(keys)
    log(f"  key：{len(keys)} 个（来自 {src}）")
    log(f"      {'、'.join(mask_key(k) for k in keys)}")
    if len(keys) > 1:
        log("      → 轮流用；谁撞到 429 就冷一会儿换下一个，不用在节点之间干等")
    try:
        from google import genai  # noqa: F401
    except Exception as e:
        log(f"  ✗ google-genai 没装好：{e}")
        log("    装一下：pip install google-genai")
        return 2
    try:
        import google.genai as _g
        log(f"  google-genai：{getattr(_g, '__version__', '?')}")
    except Exception:
        pass
    # SDK 会打一句 "Direct use of automatic function calling ..." 的废话，关掉
    import logging
    logging.getLogger("google_genai").setLevel(logging.ERROR)

    # --- 1) 找控制器 ---
    log("\n[1/4] 查找 Clash 控制器 ...")
    ctrl = discover_controller()
    if not ctrl:
        log("\n  ✗ 没找到 Clash 控制器。跑一次诊断看看：")
        log("      python gemini_api直连测试.py --diagnose")
        return 2

    log("\n[2/4] 读取内核配置、枚举代理组 ...")
    _st, ccfg = api(ctrl, "/configs", timeout=10)
    mixed = None
    if isinstance(ccfg, dict):
        mixed = ccfg.get("mixed-port") or ccfg.get("mixed_port")
        if not mixed:
            mixed = ccfg.get("port") or ccfg.get("socks-port")
    if not mixed:
        mixed = read_verge_yaml_hints().get("mixed_port") or 7897
    proxy_url = f"http://127.0.0.1:{mixed}"

    _st, proxies = api(ctrl, "/proxies", timeout=15)
    if not isinstance(proxies, dict) or "proxies" not in proxies:
        log("  ✗ 读取 /proxies 失败")
        return 2
    proxies = proxies["proxies"]

    picked = pick_group(proxies, cfg["测哪个组"] or None)
    if not picked:
        log("  ✗ 没找到可用的代理组")
        return 2
    gname, _gt, nodes, gnow = picked

    # 剔除本身是代理组的成员
    sub_groups = {n for n, p in proxies.items()
                  if (p.get("all") or [])
                  and p.get("type") in ("Selector", "URLTest", "Fallback",
                                        "LoadBalance", "Relay")}
    nodes = [n for n in nodes if n not in sub_groups]

    # 剔除订阅信息条目
    JUNK = ("套餐", "到期", "剩余流量", "流量：", "官网", "续费", "订阅",
            "重置", "客服", "购买", "长期有效")
    junk = [n for n in nodes if any(k in n for k in JUNK)]
    nodes = [n for n in nodes if n not in junk]

    kw = cfg["只测这些地区"] or []
    if kw:
        kept = [n for n in nodes if region_match(n, kw)]
        skipped = [n for n in nodes if n not in kept]
    else:
        kept, skipped = list(nodes), []

    log(f"  代理端口 {mixed}   组「{gname}」共 {len(nodes)} 个节点"
        + (f"（跳过 {len(junk)} 条订阅信息）" if junk else ""))
    log(f"  ★ 脚本要切的就是这个组：「{gname}」")
    log("    在 Clash Verge 里看的时候请打开这个组；首页 / 别的组不一定跟着变。")
    if kw:
        log(f"  按地区筛：-> {len(kept)} 个，跳过 {len(skipped)} 个")
    if not kept:
        log("  ✗ 没有可测的节点")
        return 2

    # --- 3) 预检：先用当前节点打一次，把 key / 模型 / SDK 的问题挡在前面 ---

    def probe(hook=None, key=None):
        """用指定的 key 打一次 API。

        每次新建一个 client：httpx 默认会复用长连接，而复用的那条连接
        还挂在切换前的节点上 —— 不新建的话，切完节点问的还是老节点。

        hook 在请求刚回来、连接还开着的时候调用，用来读 /connections
        看这条请求实际走的哪个节点（连接一关就从列表里消失了）。
        """
        cli = make_gemini_client(key or keys[0], proxy_url, timeout_s)
        try:
            r = ask_gemini(cli, model, prompt, timeout_s)
            if hook is not None:
                try:
                    hook()
                except Exception:
                    pass
            return r
        finally:
            try:
                cli.close()
            except Exception:
                pass

    def call_keys(hook=None, tries=None):
        """用轮换的 key 打一次。

        撞到 429 就让那个 key 冷一会儿、换下一个接着打 —— 节点不用重切，
        request 也不用重发到别的节点上，所以比"等一分钟再试"快得多。
        坏 key（无效/过期）直接冷藏一小时。
        """
        tries = tries or len(ring)
        last = None
        for _ in range(tries):
            key = ring.acquire()
            left = ring.cool_left(key)
            if 0 < left <= 30:      # 全在冷却、但快解冻了，等一下下就好
                time.sleep(min(left, 3.0))
            r = probe(hook, key)
            ok, kind, why = classify(r)
            if kind == "ok":
                ring.note(key, "ok")
                return r, key, ok, kind, why
            if kind == "quota":
                ring.cool(key, parse_retry_delay(r.get("text")))
                last = (r, key, ok, kind, why)
                continue
            if kind == "key":
                ring.cool(key, 3600, why="坏 key")
                last = (r, key, ok, kind, why)
                continue
            ring.note(key, "err")
            return r, key, ok, kind, why
        return last

    def switch_to(name):
        """切组里选中的节点，并读回来确认真的切过去了。返回 (成功?, 现在选中的)."""
        try:
            st, _j = api(ctrl, f"/proxies/{urllib.parse.quote(gname, safe='')}",
                         method="PUT", body={"name": name}, timeout=10)
            if st not in (200, 204):
                return False, f"HTTP {st}"
        except Exception as e:
            return False, str(e)
        cur = None
        try:
            _st2, now = api(ctrl, f"/proxies/{urllib.parse.quote(gname, safe='')}",
                            timeout=10)
            if isinstance(now, dict):
                cur = now.get("now")
        except Exception:
            pass
        return True, cur

    log("\n[3/4] 预检：先用当前节点打一次 API（验证 key、模型、SDK 走没走代理）...")
    r, used_key, ok, kind, why = call_keys()
    log(f"  当前节点：{gnow or '?'}")
    log(f"  {why}   ({r.get('elapsed')}s)"
        + (f"   [key {mask_key(used_key)}]" if len(ring) > 1 else ""))
    if ok:
        log(f"  Gemini 回：{(r.get('text') or '')[:120]}")
        log("  ✅ 预检通过 —— key / 模型 / 代理都正常，接着逐个节点测")
    else:
        log(f"  原始返回：{(r.get('text') or '')[:200]}")
        if kind == "key":
            log(f"  ✗ {len(ring)} 个 key 全被判为无效，跟节点无关。")
            log("    提示：AQ. 开头的是 AI Studio 的临时 token，会过期；")
            log("    长期用的是 https://aistudio.google.com/apikey 里 AIza… 开头的 key。")
            return 2
        if kind == "model":
            log(f"  ✗ 模型名 '{model}' 大概不对（也可能这个 key 不能访问它）。")
            log("    改 CONFIG 里的「模型」，比如 gemini-2.5-flash，再跑。")
            return 2
        if kind == "net":
            log("  ⚠️ 当前节点连不上 Gemini —— 这不代表别的节点不行，继续往下测。")
            log("    （如果所有节点都连不上，就要怀疑 SDK 没走代理：")
            log(f"      先在 PowerShell 里 set HTTPS_PROXY={proxy_url} 再重跑，")
            log("      或者 pip install -U google-genai）")
        else:
            log("  ⚠️ 预检没过，但不是 key/模型的问题，继续往下测。")

    # --- 4) 逐个节点 ---
    log(f"\n[4/4] 逐个节点切过去发 {prompt!r}（每个约 2-4 秒）...")
    log("      拿到回复 = 这个节点能直连 Gemini API\n")

    queue = list(kept)
    retries = {}
    results = []
    winner = None
    max_retry = int(cfg["允许重试次数"])
    want_geo = bool(cfg["顺便记录Google地区"])
    want_ip = bool(cfg["记录出口IP"])
    want_chain = bool(cfg["记录实际链路"])
    seen = 0
    current = gnow

    try:
        while queue:
            node = queue.pop(0)
            seen += 1
            log(f"  [{seen}] {node[:50]}   (队列还剩 {len(queue)} 个)")

            switched, cur = switch_to(node)
            if not switched:
                log(f"        ✗ 切节点失败：{cur}")
                continue
            current = node
            if cur and cur != node:
                log(f"        ⚠️ 切了，但组里现在选中的还是「{cur}」"
                    f"（这个组可能不支持手动切，比如 URLTest / LoadBalance）")

            time.sleep(float(cfg["切换等待秒数"]))

            chain = {}

            def _hook(_ch=chain):
                info = current_chains(ctrl)
                if info:
                    _ch.update(info)

            r, used_key, ok, kind, why = call_keys(_hook if want_chain else None)

            ip, iploc = exit_ip(proxy_url) if want_ip else (None, None)
            gc, gsuf = google_geo(proxy_url) if want_geo else (None, None)
            ch = chain.get("chains") or []
            rec = {"node": node, "ok": ok, "kind": kind, "why": why,
                   "status": r.get("status"), "text": r.get("text"),
                   "elapsed": r.get("elapsed"), "key": mask_key(used_key),
                   "exit_ip": ip, "exit_ip_loc": iploc,
                   "chains": ch, "rule": chain.get("rule"),
                   "rulePayload": chain.get("rulePayload"),
                   "google_country": gc, "google_suffix": gsuf}
            results.append(rec)

            if (want_ip or want_chain) and len(results) == 3 and switch_warnings(results):
                log("        ⚠️ 前 3 个节点的出口 / 链路完全一样 —— 切换可能根本没生效。")
                log("           可以先 Ctrl+C 停掉，按上面的提示去查分流规则，"
                    "不然测完 30 个也是白测。")

            if want_ip:
                log(f"        出口 IP：{ip or '?'}"
                    + (f"  ({iploc})" if iploc else "")
                    + ("" if ip else "  ← ip-api 没问到，可能这条流量被规则拦了"))
            if want_geo:
                log(f"        Google 认为你在：{gc or '?'}"
                    + (f"   (www.google.com -> google.{gsuf})" if gsuf else ""))
            if want_chain:
                if ch:
                    log(f"        实际链路：{' → '.join(ch)}"
                        + (f"   规则 {chain.get('rule')} {chain.get('rulePayload') or ''}"
                           if chain.get("rule") else ""))
                    # 链路的格式是「节点 → 组」：第一项是真正干活的节点，
                    # 最后一项是命中的那个组名。别拿最后一项去比节点名。
                    if node not in ch:
                        log(f"        ⚠️ 实际走的不是刚切的「{node}」"
                            f"（链路：{' → '.join(ch)}）")
                    elif gname not in ch:
                        log(f"        ⚠️ 这条连接没经过组「{gname}」"
                            f" —— 规则可能把它引到别的组了")
                    if "GLOBAL" in ch and gname not in ch:
                        log("        ⚠️ 链路里是 GLOBAL、没有刚切的这个组 —— Clash 可能在"
                            "全局模式，所有流量都走 GLOBAL 里选中的节点，切别的组没用")
                else:
                    log("        (连接列表里没找到这条请求，链路未知)")
            log(f"        {why}   ({r.get('elapsed')}s)")
            if ok:
                log(f"        Gemini 回：{(r.get('text') or '')[:120]}")
            elif kind in ("key", "model"):
                log(f"        原始返回：{(r.get('text') or '')[:160]}")

            if ok:
                winner = rec
                log("")
                log("  " + "★" * 28)
                log(f"  ★ 找到了：{node}")
                log("  ★ 已经切过去并且不切回来了，直接用")
                log("  " + "★" * 28)
                break

            # 不是"地区不支持"的（连不上之类）放队尾再试
            if kind in ("net", "api"):
                left = retries.get(node, max_retry)
                if left > 0:
                    retries[node] = left - 1
                    queue.append(node)
                    log("        不是地区问题 -> 放到队尾再试一次")
    except KeyboardInterrupt:
        log(f"\n  已中断。当前停在的节点是：{current}")

    # --- 先看"切节点"这件事本身有没有生效 ---
    warns = switch_warnings(results)
    if warns:
        log("")
        for w in warns:
            log("  " + w.replace("**", ""))
        log("  → 这属于分流/规则的问题，不是节点好不好的问题：")
        log("     打开 Clash Verge 的「连接」页，跑的时候看一眼 "
            "generativelanguage.googleapis.com")
        log("     那条连接走的是哪个组、命中了哪条规则。")

    # --- 没找到就把原来的节点切回去；找到了就留着不动 ---
    restored = False
    if not winner and gnow and gnow in proxies:
        try:
            st, _j = api(ctrl, f"/proxies/{urllib.parse.quote(gname, safe='')}",
                         method="PUT", body={"name": gnow}, timeout=10)
            restored = st in (200, 204)
        except Exception:
            pass
        if restored:
            log(f"\n  已恢复原节点：{gnow}")

    meta = [
        f"- 时间：{time.strftime('%Y-%m-%d %H:%M:%S')}",
        f"- 控制器：`{ctrl['label']}`，代理端口：{mixed}",
        f"- 组：**{gname}**，测了 {len(results)} 个节点"
        + (f"（按地区筛过，候选 {len(kept)} 个）" if kw else ""),
        f"- 模型：`{model}`，消息：`{prompt}`",
        f"- 判据：能拿到回复 = 能直连；"
        f"`User location is not supported` = 地区不支持",
        f"- key：{len(ring)} 个轮流用（{src}）",
        f"- key 用完的统计：{ring.summary()}",
        f"- 原节点已恢复：{'是' if restored else ('否（保留找到的节点）' if winner else '否')}",
    ]
    md, js = write_report(DEFAULT_OUTDIR, meta, results, winner)

    log("\n" + "=" * 70)
    log(f"  完成，用时 {time.time() - t0:.1f}s，测了 {len(results)} 个节点")
    if winner:
        log(f"  ✅ 能直连 Gemini API 的节点：{winner['node']}")
        log("     （已选中，没有切回去）")
    else:
        log("  ❌ 这一轮没有能直连的节点")
    log(f"  报告：{md}")
    log(f"  数据：{js}")
    log("=" * 70)
    return 0


if __name__ == "__main__":
    if "--diagnose" in sys.argv:
        _setup_stdout()
        sys.exit(diagnose())
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
