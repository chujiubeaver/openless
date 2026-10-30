# 任务提示词：让百炼/DashScope 流量在 Clash Verge（TUN+fake-ip）下走国内直连

> 把本文件整段交给执行方模型即可。所有路径、现象、证据都已核实过；请严格按"安全约束"和"分步执行"操作，任何一步验证失败立即回滚，不要 improvisation。

## 0. 目标

用户机器上的 OpenLess（macOS 听写应用）依赖阿里云百炼 `dashscope.aliyuncs.com`（HTTPS 识别请求 + WebSocket 实时译文）。当前该域名的流量被 Clash Verge Rev（mihomo 内核，TUN 模式 + fake-ip）裹挟：DNS 经"美国代理节点"上的 1.1.1.1 DoH 解析、数据路径可能走代理节点或拿到海外边缘 IP。表现为：

- 间歇性**连接黑洞**：TCP 握手 8 秒无任何响应，HTTP 与 WebSocket 同时死 → 整段录音丢失（应用侧表现为"只有译文/没有中文/历史为空"或完全无结果）；
- **转写极慢**：60 秒录音的识别请求耗时 20–25 秒（健康时应为 2–8 秒）；
- 代理节点本身也间歇性全死（节点死时连 DNS 一起死，故障扩散到全站）。

目标：让 `*.aliyuncs.com` 的**路由**和 **DNS 解析**都完全绕开代理节点，走国内直连 + 国内 DNS；其余一切流量（含翻墙）保持原样。

## 1. 环境事实（已核实，勿重复探索）

- 应用：`/Applications/Clash Verge.app`；内核以**服务模式**运行：`verge-mihomo -d <配置目录> -f <配置目录>/clash-verge.yaml`。
- 配置目录：`$HOME/Library/Application Support/io.github.clash-verge-rev.clash-verge-rev/`
  - `profiles.yaml`：profile 链。当前激活 profile 的 `option:` 字段指向各增强文件：
    - rules 增强：`profiles/rdWA1EnLK3zC.yaml`（内容形如 `prepend: [] / append: [] / delete: []`）
    - script 增强：`profiles/sCmB7G8GoOms.js`（一个 `main(config, profileName)` 函数）
    - merge 增强：`profiles/m0ShV9tEJxTl.yaml`
  - 订阅本体（约 4500 行规则，含 `GEOSITE,cn,DIRECT`、`GEOIP,CN,DIRECT,no-resolve`、末尾 `MATCH,Final`）：**不要改**，订阅刷新会覆盖。
- `sCmB7G8GoOms.js` 现状关键点：强制 `tun.enable=true, strict-route=true, dns-hijack=[any:53, tcp://any:53]`；`dns.enhanced-mode=fake-ip`；`dns.nameserver=["https://1.1.1.1/dns-query#Proxies"]`（**所有 DNS 走代理组**）；并 `delete` 了 `nameserver-policy`、`direct-nameserver`、`direct-nameserver-follow-policy`。
- 当前手动选中的节点：美国 DMIT GIAo（`🚀 手动切换` / `Proxies`）。
- mihomo RESTful API：`http://127.0.0.1:9097`，`Authorization: Bearer set-your-secret`（`GET /rules`、`GET /configs` 可用于验证；该 API 偶发不可用，不作为唯一验证手段）。
- 重启方式（会断网数秒）：`osascript -e 'quit app "Clash Verge"'; sleep 4; open -a "Clash Verge"; sleep 12`。内核日志在配置目录 `logs/` 下按时间戳命名，**脚本/配置错误会在那里留痕**。

## 2. 安全约束（硬性）

1. 改任何文件前先在同目录做带时间戳的备份：`cp X X.bak-$(date +%Y%m%d-%H%M%S)`。回滚 = 用备份覆盖 + 重启 Clash Verge。
2. 一次只改一个文件；改完先做语法/结构校验，再重启，再做第 4 节的全部验证。
3. **禁止**修改：`profiles.yaml`、订阅本体 yaml、`clash-verge.yaml`（运行时生成）、TUN 全局字段、dns 的 `nameserver`/`default-nameserver` 等全局键。
4. Script.js 里**禁止**使用数组/对象 spread（`...x`）、可选链、模板字符串等较新语法——Clash Verge 内嵌 JS 引擎对语法异常会导致整份配置生成失败，表现为**重启后全站断网**（本任务上一次尝试正是因此失败并回滚）。只用 `concat`、`||`、普通字符串拼接。
5. 验证失败或用户报告断网：**立即回滚并重启**，不要在同一坏状态上继续调试。
6. 每步结束后向用户汇报：改了什么、验证结果（带数字）、如何回滚。

## 3. 分步执行

### Step 1 — 路由直连（纯 YAML，风险最低）

编辑 `profiles/rdWA1EnLK3zC.yaml`，把 `prepend: []` 改为：

```yaml
prepend:
  # 百炼/DashScope 为国内服务：绕开代理节点直连（OpenLess 听写与实时译文依赖）。
  - DOMAIN-SUFFIX,aliyuncs.com,DIRECT
```

校验：`ruby -ryaml -e "YAML.load_file('<该文件>'); puts 'yaml ok'"`（或 python3 + PyYAML）。
重启 Clash Verge。执行第 4 节验证。

> 说明：此步只解决"数据路径"。由于 DNS 仍走 `1.1.1.1#Proxies`，当代理节点死时，DIRECT 域名的真实 IP 解析仍可能失败——所以节点健康时本步已足够，节点死时残余风险由 Step 2 消除。若用户只接受最小改动，可止步于此。

### Step 2 — DNS 独立于代理（改 script 增强的 dns 块）

编辑 `profiles/sCmB7G8GoOms.js`，在 `delete config.dns["direct-nameserver-follow-policy"];` 之后、`return config;` 之前插入（**注意：无 spread，用 concat**）：

```js
  // 百炼/DashScope：不进 fake-ip（拿真实国内 IP），并用国内 DNS 解析，
  // 使该域名的路由与解析都不依赖代理节点是否存活。
  var ff = config.dns["fake-ip-filter"] || [];
  config.dns["fake-ip-filter"] = ff.concat(["*.aliyuncs.com"]);
  config.dns["nameserver-policy"] = {
    "+.aliyuncs.com": "223.5.5.5",
  };
```

校验：`node --check <该文件>`。
重启 Clash Verge。执行第 4 节全部验证，**额外**验证 DNS 独立性：
`nslookup dashscope.aliyuncs.com 127.0.0.1` 应返回真实国内 IP（阿里云网段），**不应**是 `198.18.x.x`（fake-ip 段）。

## 4. 验证清单（每步重启后都要跑全）

```bash
# 1) 国内直连路径（必须 200 且 <1s）
curl -sS -m 10 -o /dev/null -w "yuanbao %{http_code} %{time_total}s\n" https://yuanbao.tencent.com/
# 2) 目标域名（必须 connect <0.5s；400 属正常响应码，说明链路通）
for i in 1 2 3; do curl -sS -m 12 -o /dev/null -w "dashscope %{http_code} total=%{time_total}s connect=%{time_connect}s\n" \
  https://dashscope.aliyuncs.com/compatible-mode/v1/chat/completions; done
# 3) 代理路径（注意：该用户的美国节点本身间歇性死亡，此项失败属既有问题，
#    不作为本次改动成败判据；仅当"改动前通、改动后不通"才算回归）
curl -sS -m 12 -o /dev/null -w "google %{http_code} %{time_total}s\n" https://www.google.com/
# 4) 内核日志无脚本/配置错误
tail -30 "$HOME/Library/Application Support/io.github.clash-verge-rev.clash-verge-rev/logs/$(ls -t "$HOME/Library/Application Support/io.github.clash-verge-rev.clash-verge-rev/logs/" | head -1)"
# 5) 规则生效（API 偶发不可用时跳过）
curl -s -m 5 http://127.0.0.1:9097/rules -H "Authorization: Bearer set-your-secret" | head -c 400
```

应用侧验收（可选，需用户配合说一句话）：OpenLess 日志
`$HOME/Library/Logs/OpenLess/openless.log` 中不再出现
`[llm] connect-phase timeout ... one direct (no-proxy) attempt`；
一次约 60 秒听写的 `[omni] POST` → `[omni] stream done` 间隔回到个位数秒。

## 5. 回滚

```bash
D="$HOME/Library/Application Support/io.github.clash-verge-rev.clash-verge-rev/profiles"
cp "$D/rdWA1EnLK3zC.yaml.bak-<时间戳>" "$D/rdWA1EnLK3zC.yaml"
cp "$D/sCmB7G8GoOms.js.bak-<时间戳>"   "$D/sCmB7G8GoOms.js"
osascript -e 'quit app "Clash Verge"'; sleep 4; open -a "Clash Verge"
```

## 6. 对用户的影响（执行方需原样转述）

- 只有 `*.aliyuncs.com`（百炼/DashScope，含 OpenLess 的识别与实时译文）改为国内直连 + 国内 DNS（223.5.5.5 AliDNS）。
- 其余所有域名与流量（包括翻墙流量、其他国内站点）行为完全不变；订阅更新不会覆盖本次改动（改的是增强配置文件，不是订阅本体）。
- 代价：该域名不再经过代理节点——对国内云服务这是期望行为；若某天处于"必须全流量走代理"的网络（极少数公司网络），删除 Step 1 的 prepend 一行并重启即可完全恢复原状。
- 收益：消除"整段录音丢失"和"转写 20–25 秒"两类问题的网络根因；代理节点死亡时不再连带拖垮听写。
