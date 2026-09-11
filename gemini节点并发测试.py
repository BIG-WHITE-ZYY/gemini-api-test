#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Clash 节点能不能进 Gemini / AI Studio —— 直接问 Google 的接口

原理
----
AI Studio 前端发现你所在地区不支持时，会先调一个后端接口，拿到
    403  [7,"Region not supported."]
之后才跳转到说明页。这个脚本跳过整个浏览器，直接调那个接口读返回值：

    POST https://alkalimakersuite-pa.clients6.google.com/$rpc/
         google.internal.alkali.applications.makersuite.v1.MakerSuiteService/
         GetUserPreferences

    200                                  -> 这个节点能用
    403 [7,"Region not supported."]      -> 地区不支持
    别的                                  -> 原样打出来给你看

请求头里的 authorization 是 SAPISIDHASH，用 cookie 文件里的 SAPISID
算一个 SHA1 就有了，所以不需要浏览器、不需要等页面跳转。
一个节点大约 2 秒。

全标准库，不用装任何东西。

用法：VS Code 里打开直接按 ▶ 就行，参数在下面的「配置区」里改。
"""

import hashlib
import http.cookiejar
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
DEFAULT_COOKIE = os.path.join(HERE, "aistudio.google.com_cookies.txt")
DEFAULT_OUTDIR = HERE

# 下面几个是从真实页面的网络请求里抓出来的，正常不用改
ORIGIN = "https://aistudio.google.com"
RPC_BASE = ("https://alkalimakersuite-pa.clients6.google.com/$rpc/"
            "google.internal.alkali.applications.makersuite.v1."
            "MakerSuiteService/")
RPC_METHODS = ["GetUserPreferences", "GetLoggingContext"]
X_GOOG_API_KEY = "AIzaSyDdP816MREB3SkjZO04QXbjsigfcI0GWOs"

UA = ("Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
      "(KHTML, like Gecko) Chrome/140.0.0.0 Safari/537.36")

# ============================================================================
#  配置区 —— 想改参数就改这里，然后按 ▶ 运行
# ============================================================================
CONFIG = {
    # 只测这些地区，其他地区（香港/台湾/新加坡等）直接跳过
    # 中英文都行；纯英文的按整词匹配，避免 US 误伤 RUS 这类
    "只测这些地区": [
        "英国", "伦敦", "美国", "洛杉矶", "圣何塞", "西雅图", "纽约", "硅谷",
        "日本", "东京", "大阪",
        "UK", "US", "USA", "JP", "London", "Japan", "Tokyo",
        "Los Angeles", "United Kingdom", "United States",
    ],

    # 报错不是"地区不支持"的节点（连不上之类），放到队尾再试几次
    "允许重试次数": 1,

    # 测哪个代理组；留空 = 自动挑节点最多的那个
    "测哪个组": "",

    # 切换节点后等多久再发请求（秒）
    "切换等待秒数": 0.3,

    # 单次接口请求超时
    "接口超时秒数": 20,
}

# 优先猜的控制器端口
CANDIDATE_PORTS = [
    9090, 9097, 9091, 9099, 9098, 9092, 9093, 9094, 9096, 6170, 63333,
    8000, 8080, 8888, 20171, 33210, 50000, 50001, 60000, 15600, 10000, 12345,
]

# 猜本机混合代理端口（--manual 模式用）
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
        # 有些版本直接放在 AppData\Roaming\<name>
    # 去重
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

                # external-controller: 127.0.0.1:9097  或  管道名
                for key in ("external-controller", "external_controller"):
                    m = re.search(r'^\s*' + key + r'\s*:\s*["\']?([^\s"\']+)',
                                  txt, re.MULTILINE)
                    if m and not hints["controller"]:
                        hints["controller"] = m.group(1)

                # external-controller-pipe: \\.\pipe\verge-mihomo
                m = re.search(r'^\s*external-controller-pipe\s*:\s*["\']?([^\s"\']+)',
                              txt, re.MULTILINE)
                if m:
                    v = m.group(1).strip("'\"")
                    if v and v not in hints["pipes"]:
                        hints["pipes"].append(v)

                # 任何形如 \\.\pipe\xxx 的值
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
    """找一个可用的 mihomo 控制器。

    返回 ctrl 字典，找不到返回 None：
        {"kind": "pipe"|"tcp", "target": 管道名或端口, "secret": ..., "label": ...}
    新版 Clash Verge Rev 默认只开命名管道，所以先试管道再试 TCP 端口。
    """
    hints = read_verge_yaml_hints()
    secrets = []
    for s in (hints.get("secret"), None):
        if s not in secrets:
            secrets.append(s)

    # ---------- 1) 命名管道（Clash Verge Rev 的默认方式）----------
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

    log("\n" + "=" * 70)
    log("  把上面全部内容复制给 Claude")
    log("=" * 70)
    return 0


# ----------------------------------------------------------------------------
# 第二步：并发测所有节点
# ----------------------------------------------------------------------------

# ----------------------------------------------------------------------------
# 控制器通道：TCP 端口 或 Windows 命名管道
# ----------------------------------------------------------------------------

PIPE_HINTS = ("verge-mihomo", "verge_mihomo", "mihomo", "clash", "verge")

# 同时打开的命名管道连接数上限（见 _pipe_raw 里的说明）
# 延迟测试时每条管道会被占用到该节点测完为止，所以这个值直接影响第 4 步的速度。
# 调高之后万一触发 "All pipe instances are busy"，_pipe_raw 会自动重试，不会误报。
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
    # 排掉明显不是控制器的
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
            # Windows 命名管道的实例数有限，并发开太多会报
            # "All pipe instances are busy"（winerror 231，实测 61 个节点时
            # 有 42 个因此误报为不通）。这里限流。
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
# 小工具
# ----------------------------------------------------------------------------

def cookie_jar(path):
    cj = http.cookiejar.MozillaCookieJar(path)
    try:
        cj.load(ignore_discard=True, ignore_expires=True)
    except Exception as e:
        log(f"  [警告] cookie 文件读取失败：{e}")
        return None
    return cj


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


# ----------------------------------------------------------------------------
# 核心：直接问 Google 的接口
# ----------------------------------------------------------------------------

def rpc_auth(cookie_path):
    """算出 SAPISIDHASH 请求头和 Cookie 头。"""
    cj = cookie_jar(cookie_path)
    if not cj:
        return None, None
    ck = {c.name: c.value for c in cj}

    def one(cookie_name):
        v = ck.get(cookie_name)
        if not v:
            return None
        ts = int(time.time())
        d = hashlib.sha1(f"{ts} {v} {ORIGIN}".encode()).hexdigest()
        return f"{ts}_{d}"

    parts = []
    for cookie_name, label in (("SAPISID", "SAPISIDHASH"),
                               ("__Secure-1PAPISID", "SAPISID1PHASH"),
                               ("__Secure-3PAPISID", "SAPISID3PHASH")):
        h = one(cookie_name)
        if h:
            parts.append(f"{label} {h}")

    cookie_hdr = "; ".join(f"{c.name}={c.value}" for c in cj)
    return (" ".join(parts) if parts else None), cookie_hdr


def ask_google(proxy_url, auth, cookie_hdr, cfg):
    """直接调那个接口，返回它说的话。"""
    timeout = float(cfg.get("接口超时秒数", 20))
    last = None
    for method in RPC_METHODS:
        headers = {
            "authorization": auth,
            "content-type": "application/json+protobuf",
            "x-user-agent": "grpc-web-javascript/0.1",
            "x-goog-api-key": X_GOOG_API_KEY,
            "x-goog-authuser": "0",
            "referer": ORIGIN + "/",
            "origin": ORIGIN,
            "cookie": cookie_hdr,
        }
        t = time.time()
        try:
            st, _h, data = http_request(RPC_BASE + method, method="POST",
                                        body=b"[]", headers=headers,
                                        timeout=timeout, proxy=proxy_url)
            txt = data.decode("utf-8", "replace").strip()
            r = {"method": method, "status": st, "body": txt,
                 "elapsed": round(time.time() - t, 2)}
            if st == 200 or "Region not supported" in txt:
                return r          # 有结论了，不用再试别的接口
            last = r
        except Exception as e:
            last = {"method": method, "status": None,
                    "body": f"{type(e).__name__}: {e}",
                    "elapsed": round(time.time() - t, 2)}
    return last


def judge(r):
    """把接口的返回值翻译成人话。"""
    if not r:
        return False, "没拿到响应"
    st, body = r.get("status"), (r.get("body") or "")
    if st is None:
        return False, f"连不上：{body[:70]}"
    if st == 200:
        return True, "✅ 接口正常返回 200"
    if "Region not supported" in body:
        return False, "❌ Region not supported（地区不支持）"
    if st in (401, 403):
        return False, f"❌ HTTP {st}：{body[:70]}"
    return False, f"❌ HTTP {st}：{body[:70]}"


# Google 的区域域名后缀 -> 国家码
_G_SUFFIX = {
    "": "US", "com": "US", "com.hk": "HK", "cn": "CN", "co.jp": "JP",
    "co.uk": "GB", "co.kr": "KR", "com.tw": "TW", "com.sg": "SG",
    "com.au": "AU", "co.in": "IN", "com.br": "BR", "de": "DE", "fr": "FR",
    "ca": "CA", "com.mx": "MX", "it": "IT", "es": "ES", "nl": "NL",
    "ru": "RU", "co.nz": "NZ", "com.my": "MY", "co.th": "TH",
    "com.vn": "VN", "co.id": "ID", "com.ph": "PH", "com.ar": "AR",
}

# 官方支持地区里，能确定国家的都收进来（够判断节点用的了）
_SUPPORTED = set("""
US GB JP KR TW SG AU CA DE FR IT ES NL NZ MY TH VN ID PH IN BR MX AR
AT BE CH DK FI GR HKX IE IL NO PL PT RU SE TR UA AE SA ZA EG NG KE
""".split())
_SUPPORTED.discard("HKX")


def google_geo(proxy_url):
    """问 Google「你觉得我在哪个地区」—— 看 www.google.com 往哪个区域域名跳。

    返回 (国家码 或 None, 原始域名后缀)。
    这是 Google 自己的地理定位，和 ip-api / Cloudflare 的结论经常不一致，
    而只有 Google 这个说了算。
    """
    for url in ("https://www.google.com/", "https://www.google.com/ncr"):
        try:
            st, hd, _b = http_request(url, proxy=proxy_url, timeout=15,
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
# 报告
# ----------------------------------------------------------------------------

def write_report(outdir, meta_lines, results, winner):
    ts = time.strftime("%Y%m%d_%H%M%S")
    os.makedirs(outdir, exist_ok=True)
    md_path = os.path.join(outdir, f"节点测试报告_{ts}.md")
    js_path = os.path.join(outdir, f"节点测试报告_{ts}.json")

    with open(js_path, "w", encoding="utf-8") as f:
        json.dump({"时间": time.strftime("%Y-%m-%d %H:%M:%S"),
                   "结论_找到可用节点": winner["node"] if winner else None,
                   "明细": results}, f, ensure_ascii=False, indent=2)

    L = ["# Clash 节点能不能进 Gemini / AI Studio\n"]
    L += meta_lines
    L.append("")
    L.append("## 逐个节点结果\n")
    L.append("| 节点 | Google 认为你在 | 接口返回 | 结果 | 耗时 |")
    L.append("|------|-----------------|----------|------|------|")
    for r in results:
        ok = bool(r.get("ok"))
        gc = r.get("google_country") or "?"
        gs = r.get("google_suffix")
        label = f"{gc}" + (f" (google.{gs})" if gs else "")
        L.append(f"| {r['node']} | {label} | "
                 f"`{(r.get('body') or '')[:52]}` | "
                 f"{'✅ 能进' if ok else '❌ 不能'} | {r.get('elapsed')}s |")
    L.append("")
    L.append("## 结论\n")
    if winner:
        L.append(f"**✅ 找到能用的节点：`{winner['node']}`**\n")
        L.append("接口返回 200，这个节点可以进 AI Studio。程序到此结束。\n")
    else:
        L.append("**❌ 这一轮没有能用的节点。**\n")
        hk = [r for r in results if (r.get("google_country") or "") in ("HK", "CN", "MO")]
        if hk and len(hk) >= max(2, len(results) // 2):
            L.append(f"注意：**{len(hk)}/{len(results)} 个节点，Google 都认为你在香港/中国**"
                     "—— 而香港不在支持列表里。这说明是 VPN 的 IP 段被 Google "
                     "划到了不支持的地区，不是你选的节点不够好。\n")
            L.append("换一家 VPN / 换一个 IP 段才可能有用。\n")
        else:
            L.append("节点地区看着没问题，但接口一律返回地区不支持 —— "
                     "那就更可能是账号本身的问题了（地区设置 / 年龄验证）。\n")
    with open(md_path, "w", encoding="utf-8") as f:
        f.write("\n".join(L))
    return md_path, js_path


# ----------------------------------------------------------------------------
# 主流程
# ----------------------------------------------------------------------------

def main():
    _setup_stdout()

    cfg = CONFIG
    cookie_path = DEFAULT_COOKIE if os.path.exists(DEFAULT_COOKIE) else None

    t0 = time.time()
    log("=" * 70)
    log("  节点能不能进 Gemini / AI Studio —— 直接问 Google 接口")
    log("=" * 70)

    if not cookie_path:
        log(f"\n  ✗ 找不到 cookie 文件：{DEFAULT_COOKIE}")
        log("    需要它来算 SAPISIDHASH。把浏览器导出的 cookie 放到同目录即可。")
        return 2
    auth, cookie_hdr = rpc_auth(cookie_path)
    if not auth:
        log(f"\n  ✗ cookie 里没有 SAPISID，算不出认证头：{cookie_path}")
        return 2
    log(f"\n  cookie：{os.path.basename(cookie_path)}（认证头已就绪）")

    # --- 找控制器 ---
    log("\n[1/3] 查找 Clash 控制器 ...")
    ctrl = discover_controller()
    if not ctrl:
        log("\n  ✗ 没找到 Clash 控制器。跑一次诊断看看：")
        log("      python gemini节点并发测试.py --diagnose")
        return 2
    base = ctrl["label"]

    # --- 拿配置 ---
    log("\n[2/3] 读取内核配置、枚举代理组 ...")
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

    # 只要英/美/日
    kw = cfg["只测这些地区"]
    kept = [n for n in nodes if region_match(n, kw)]
    skipped = [n for n in nodes if n not in kept]

    log(f"  代理端口 {mixed}   组「{gname}」共 {len(nodes)} 个节点"
        + (f"（跳过 {len(junk)} 条订阅信息）" if junk else ""))
    log(f"  按地区筛：只留英国/美国/日本 -> {len(kept)} 个，跳过 {len(skipped)} 个")
    if skipped:
        log(f"    跳过：{'、'.join(skipped[:5])}{' …' if len(skipped) > 5 else ''}")
    if not kept:
        log("  ✗ 没有英/美/日的节点可测")
        return 2

    # --- 逐个问接口 ---
    log(f"\n[3/3] 逐个节点问 Google 接口（每个约 2 秒）...")
    log(f"      {RPC_BASE.split('/$rpc/')[1][:70]}...")
    log(f"      看到 403 Region not supported = 这个节点进不去\n")

    queue = list(kept)
    retries = {}
    results = []
    winner = None
    max_retry = int(cfg["允许重试次数"])
    seen = 0

    while queue:
        node = queue.pop(0)
        seen += 1
        log(f"  [{seen}] {node[:50]}   (队列还剩 {len(queue)} 个)")
        try:
            st, _j = api(ctrl, f"/proxies/{urllib.parse.quote(gname, safe='')}",
                         method="PUT", body={"name": node}, timeout=10)
            if st not in (200, 204):
                log(f"        ✗ 切节点失败 HTTP {st}")
                continue
        except Exception as e:
            log(f"        ✗ 切节点失败 {e}")
            continue

        time.sleep(float(cfg["切换等待秒数"]))
        r = ask_google(proxy_url, auth, cookie_hdr, cfg) or {}
        ok, why = judge(r)
        gc, gsuf = google_geo(proxy_url)
        rec = {"node": node, "ok": ok, "status": r.get("status"),
               "body": r.get("body"), "elapsed": r.get("elapsed"),
               "method": r.get("method"), "why": why,
               "google_country": gc, "google_suffix": gsuf}
        results.append(rec)
        log(f"        Google 认为你在：{gc or '?'}"
            + (f"   (www.google.com -> google.{gsuf})" if gsuf else ""))
        log(f"        接口返回：{why}   ({r.get('elapsed')}s)")

        if ok:
            winner = rec
            log("")
            log("  " + "★" * 28)
            log("  ★ 找到了，程序结束")
            log("  " + "★" * 28)
            break

        # 不是"地区不支持"的（连不上之类）放队尾再试
        if r.get("status") is None or "Region not supported" not in (r.get("body") or ""):
            left = retries.get(node, max_retry)
            if left > 0:
                retries[node] = left - 1
                queue.append(node)
                log("        不是地区问题 -> 放到队尾再试一次")

    # --- 恢复原节点 ---
    restored = False
    if gnow and gnow in proxies:
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
        f"- 控制器：`{base}`，代理端口：{mixed}",
        f"- 组：**{gname}**，英/美/日节点 {len(kept)} 个",
        f"- 判据：接口 `{RPC_METHODS[0]}` 返回 200 = 能进；"
        f"403 `Region not supported` = 进不去",
        f"- 原节点已恢复：{'是' if restored else '否'}",
    ]
    md, js = write_report(DEFAULT_OUTDIR, meta, results, winner)

    log("\n" + "=" * 70)
    log(f"  完成，用时 {time.time() - t0:.1f}s")
    log(f"  报告：{md}")
    log(f"  数据：{js}")
    log("=" * 70)
    return 0


if __name__ == "__main__":
    if "--diagnose" in sys.argv:
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
