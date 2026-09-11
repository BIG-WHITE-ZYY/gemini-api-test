# clash-node-ai-studio-check

一个脚本：**在 Clash Verge 里逐个切节点，判断哪个节点能进 Google AI Studio / Gemini API。**

一个节点约 2 秒，纯标准库，不用装任何东西。

```
[1] 🇬🇧英国伦敦02|流媒体|0.1x
      Google 认为你在：HK   (www.google.com -> google.com.hk)
      接口返回：❌ Region not supported（地区不支持）   (1.61s)
```

## 它是怎么工作的

AI Studio 前端发现你所在地区不支持时，**会先调一个后端接口**，拿到报错之后才跳转到
[available-regions](https://ai.google.dev/gemini-api/docs/region-support) 说明页。
这个脚本跳过整个浏览器，直接调那个接口读返回值：

```
POST https://alkalimakersuite-pa.clients6.google.com/$rpc/
     google.internal.alkali.applications.makersuite.v1.MakerSuiteService/
     GetUserPreferences

200                               -> 这个节点能用
403  [7,"Region not supported."]  -> 地区不支持
别的                               -> 原样打出来
```

请求头里的 `authorization` 是 **SAPISIDHASH**，用 cookie 文件里的 `SAPISID`
算一个 SHA1 就有了：

```python
sha1(f"{unix_ts} {SAPISID} https://aistudio.google.com")
```

所以不需要浏览器、不需要等页面跳转、也不需要装 Playwright。

## 用法

1. 用浏览器插件（比如 Get cookies.txt）导出 `aistudio.google.com` 的 cookie，
   存成 `aistudio.google.com_cookies.txt` 放在脚本同目录
2. 打开 Clash Verge，确保它在运行
3. VS Code 里打开脚本，按 ▶

参数都在文件顶部的 `CONFIG` 里，改完保存再跑：

```python
CONFIG = {
    "只测这些地区": ["英国", "伦敦", "美国", "日本", ...],  # 其他地区直接跳过
    "允许重试次数": 1,        # 连不上的节点放队尾再试几次
    "测哪个组": "",           # 留空 = 自动挑节点最多的组
    "切换等待秒数": 0.3,
    "接口超时秒数": 20,
}
```

跑完会写一份 Markdown + JSON 报告到同目录，并把你原来的节点切回去。

## 关键发现：Google 的地理定位和第三方库不一致

同一个出口 IP：

| 来源 | 结论 |
|------|------|
| ip-api | 日本 东京 |
| Cloudflare (`cdn-cgi/trace`) | `loc=JP` `colo=NRT`（东京） |
| **Google** | **香港**（`www.google.com` → `www.google.com.hk`） |

只有 Google 那个说了算。所以脚本会**同时报告 Google 眼里这个节点属于哪个地区** ——
节点名写着"英国"、Google 却说香港，那这个节点就是白搭。

另外实测：**地区判定是在认证之后做的**。

```
不带完整认证  -> HTTP 401（只报认证错误，压根没做地区判断）
完整认证      -> HTTP 403 [7,"Region not supported."]
```

也就是说 Google 是认出你是谁之后才判的地区。

## 走过的弯路（供参考，别再踩）

这个脚本是被一点点打脸打出来的，下面每条都有实测数据：

**1. HTTP 抓页面判断地区 —— 不行。**
AI Studio 是前端 SPA，抓 HTML 永远返回 200，真正的判定是加载后由前端做的。
抓 HTML 只能证明"页面骨架能加载"，什么都不代表。

**2. 用浏览器等页面跳转 —— 时机波动太大。**
同一个进不去的节点连测 5 次，跳转分别发生在：

```
第 5.4 / 11.3 / 14.8 / 16.3 / 23.6 秒
```

跨度 4 倍多。取 15 秒窗口的话，5 次里有 3 次会把失败节点误判成成功。

**3. 「不跳转」当成功 —— 会撞上白屏。**
实测某个美国节点整整 40 秒完全不跳转，但页面是**纯白屏**（元素数只有 60、body 全空）。
"不跳转"和"能进"是两回事。

## 环境要求

- Python 3.8+
- **纯标准库，不需要 pip install 任何东西**
- Clash Verge（用了 mihomo 的外部控制器 API）
  - 新版 Clash Verge Rev 默认**不开 TCP 控制器**，把 API 挂在命名管道
    `\\.\pipe\verge-mihomo` 上。脚本会先找管道再试端口
  - 找不到控制器时跑 `python gemini节点并发测试.py --diagnose` 看环境

## 关于 cookie 文件

**`aistudio.google.com_cookies.txt` 是敏感文件，已经写进 `.gitignore`。**

它包含 `SID` / `HSID` / `SSID` / `SAPISID` / `__Secure-1PSID` 这些 —— 拿到它等于
拿到你 Google 账号的登录态。**绝对不要提交到任何仓库**（私有仓库也不行：将来转公开、
加协作者、或者 clone 到别的机器，凭据就跟着走了）。

脚本里出现的 `X_GOOG_API_KEY` 是 AI Studio 前端页面里内嵌的公开 key，
任何人打开页面都能看到，不是私有凭据。

## 已知限制

- 只能测英/美/日这类你配置里指定的地区节点
- 需要 cookie 文件，所以 cookie 过期后要重新导出
- 依赖 mihomo 的外部控制器 API，只适合 Clash/mihomo 系客户端
