# Binance / OKX 历史行情对齐验证报告

验证时间：2026-07-24（Asia/Shanghai）

验证环境：`ticket`，生产机器 `hn3` 未访问、未修改。

## 验收口径

本报告的通过标准是：

> 对同一个 `exchange / symbol / timeframe / start_ms`，我们服务已完成 K 线的 OHLCV 与交易所当前官方目标口径一致。OKX 股票永续以官方前复权口径为准。

使用的官方接口：

- Binance USD-M：`GET /fapi/v1/klines`
- OKX：`GET /api/v5/market/candles` 和 `GET /api/v5/market/history-candles`，股票永续均传 `adjust=forward`

系统不再自行计算复权因子或提供 adjusted 视图。API 中的 `price_mode=raw` 表示未经过本地因子加工；OKX 股票永续的实际存储值来自官方 `adjust=forward` 响应。

## 样本

| 交易所 | 品种 | 官方动作比例 | 官方边界（UTC） | 代表周期 | raw 对齐结果 |
| --- | --- | ---: | --- | --- | --- |
| OKX | OPENAI-USDT-SWAP | 10 | 2026-06-30 07:00 | 1H | PASS |
| OKX | ANTHROPIC-USDT-SWAP | 10 | 2026-06-30 08:06 | 1H | PASS |
| OKX | SPACEX-USDT-SWAP -> SPCX-USDT-SWAP | 12.52 | 2026-06-02 07:10 | 2H | PASS |
| Binance | SPCXUSDT | 1.1 | 2026-06-10 09:10 | 2H | PASS |
| Binance | CRWDUSDT | 4 | 2026-07-02 13:35 | 1H | PASS |
| Binance | KORUUSDT | 20 | 2026-07-15 09:35 | 15m | PASS |

每个品种都检查了 `1m / 5m / 15m / 30m / 1H / 2H / 4H / 6H / 12H / 1D`，每个周期检查边界前后各 2 桶，共 300 根 K 线、约 2400 个字段比较。所有六个动作品种均 PASS。

## KORU 说明

Binance 当前官方 15m 历史接口返回：

| 位置 | start_ms | Official OHLC | Local raw OHLC |
| --- | ---: | --- | --- |
| 边界前一桶 | 1784106900000 | 481.11 / 481.11 / 481.11 / 481.11 | 481.11 / 481.11 / 481.11 / 481.11 |
| 跨边界桶 | 1784107800000 | 22.68 / 25.20 / 22.68 / 23.71 | 22.68 / 25.20 / 22.68 / 23.71 |
| 边界后一桶 | 1784108700000 | 23.71 / 23.77 / 23.34 / 23.39 | 23.71 / 23.77 / 23.34 / 23.39 |

因此 KORU 的正确结论是：我们的 raw 服务与 Binance 官方服务一致。官方公共历史接口没有把边界前所有 K 线统一除以 20，系统也不再自行构造这种复权价格。

## 验证中发现并修复的问题

### 1. 只重建边界桶会漏掉官方回写

OPENAI 边界前一根 1H：

- OKX 官方 open：`1380.85`
- 原本本地 open：`1380.46`

虽然本地已经覆盖了官方 1m，但 OKX 官方 1m 聚合结果与官方 1H 仍可能不同，说明交易所不同周期会独立维护或回写。

修复后，历史回填会分别拉取官方 `1m / 5m / 15m / 30m / 1H / 2H / 4H / 6H / 12H / 1D`，并按相同 `timeframe/start_ms` 覆盖 raw 数据。官方目标周期优先，只有官方该周期缺行时才回退到 1m 聚合。

修复后 OPENAI 该 1H 的本地 raw open 已更新为 `1380.85`。

### 2. Binance 停牌零成交占位 K 线

Binance 在部分停牌窗口返回 `volume=0` 的旧尺度 1m 占位行，但官方高周期可能忽略这些占位行。现在 raw 直接使用官方目标周期，不再依赖本地聚合猜测官方口径。

### 3. OKX 限流

逐周期回补会增加请求数。回填器已加入请求间隔和最多四次退避重试，避免 `429 Too Many Requests` 导致半途失败。

### 4. OKX 历史查询游标的 confirm 行为

OKX `history-candles` 在带 `after` 的查询中，可能把游标下第一根零成交历史 K 线标为 `confirm=0`；把游标再向后移动一个周期后，同一根 K 线会返回 `confirm=1`。如果直接使用区间末尾作为游标，会造成本地回补和验证结果不稳定。

现在 OKX 拉取器会向请求结束位置后探测一个完整周期，再按原始 `start_ms/end_ms` 过滤；新增回归测试已覆盖该行为。六个动作样本已使用该逻辑重新回填。

### 5. OKX 股票永续必须显式请求前复权

OKX K 线接口不传 `adjust` 时默认返回不复权数据；股票永续需要传 `adjust=forward` 才是本系统的验收口径。边界定位仍临时读取未复权 `1m`，利用价格跳变确认动作时间；写库回补、通用历史回填、guardian 校验和 verifier 均使用官方前复权数据。

KORU `1W start_ms=1783900800000` 修复前为 `536.81 / 554.75 / 15.49 / 18.61`，修复后与官方前复权一致，为 `26.84 / 27.74 / 15.49 / 18.61`。四根已完成周线逐字段比较均通过。`confirm=0` 的当前周线会实时变化，不纳入历史覆盖验收。

## 扩大样本结果

### 动作品种

六个动作品种的 10 周期矩阵全部通过：

- OKX：OPENAI、ANTHROPIC、SPCX。
- Binance：SPCX、CRWD、KORU。

每个周期均为 `official=5, compared=5, missing_local=0, missing_official=0, mismatches=0`。

### 非动作品种对照

| 交易所 | 品种 | 周期 | 样本 | 结果 |
| --- | --- | --- | ---: | --- |
| OKX | ZHIPU-USDT-SWAP | 1m | 5 | PASS |
| OKX | ZHIPU-USDT-SWAP | 5m/15m/30m/1H/2H/4H/1D | 5/周期 | FAIL，存在高周期字段差异 |
| OKX | BTC-USDT-SWAP | 10 个周期 | 5/周期 | PASS |
| Binance | BTCUSDT | 10 个周期 | 5/周期 | PASS |

ZHIPU 扩大到前后各 10 桶后，仍观察到 23 个字段差异：5m 11 个、15m 5 个、1H 4 个、1D 3 个。差异主要是 open，另有少量 high/low；没有缺行。

该结果不是除权因子错误：ZHIPU 的本地 1m 与 OKX 官方 1m 对齐，但本地高周期是由本地 1m 聚合，OKX 官方高周期是交易所独立维护的目标周期，二者并不必然相同。例如 `5m start_ms=1784548200000`，本地 open 为 `115.62`，官方 open 为 `115.34`，而两边 1m 数据一致。这说明“普通品种高周期”仍需要独立拉取官方目标周期才能保证全量历史与官方一致。

## 运行检查

- ZHIPU-USDT-SWAP：扫描范围内没有公司行动公告；其高周期 raw 对照未全部通过，详见上文。
- OKX BTC-USDT-SWAP、Binance BTCUSDT：作为普通品种对照，10 个周期均通过。
- 六个动作重复执行都会重新拉取并覆盖官方历史 raw 数据，审计事件保持幂等。
- 扩展周期验证：OKX OPENAI、ANTHROPIC、SPCX 按 `adjust=forward` 验证 12 个周期均 PASS；OKX KORU 已完成 `1W 4/4 PASS`；Binance KORU 的 `1W` 为 `3/3 PASS`。
- 本地 `go test ./...`、`go vet ./...`、`git diff --check` 全部通过。
- `ticket` 已部署代码提交 `521fe51`，`crypto-ticket.service` 为 active；`hn3` 未操作。

## 高周期实时 HTTP / WebSocket 验证（2026-07-24）

提交 `521fe51` 将 `5m` 及以上的当前未完成 K 线改为直接读取官方目标周期接口。OKX SWAP 固定使用 `adjust=forward`；Binance 使用 `/fapi/v1/klines` 的官方唯一口径。公开 WS 只对实际订阅的高周期每 2 秒轮询，字段未变化时不重复推送；旧的本地 `1m` live rollup 不再混入高周期 WS。

| 交易所 | 品种 | 周期 | HTTP 当前 K 线 | WS 当前 K 线 | 结果 |
| --- | --- | --- | --- | --- | --- |
| OKX | KORU-USDT-SWAP | 1W | `adjust=forward` 对齐 | `source=official_rest_live`，`18.6 / 23.4 / 17.43` 对齐 | PASS |
| OKX | KORU-USDT-SWAP | 1D | `adjust=forward` 对齐 | `source=official_rest_live` | PASS |
| OKX | OPENAI-USDT-SWAP | 1W | `adjust=forward` 对齐 | `120.81 / 121.2 / 103.66 / 117.38` 对齐 | PASS |
| OKX | ZHIPU-USDT-SWAP | 1D | `adjust=forward` 对齐 | `145.77 / 163.63 / 143.27` 对齐 | PASS |
| Binance | KORUUSDT | 1W / 1D | 官方 futures kline 对齐 | `source=official_rest_live` | PASS |
| Binance | BTCUSDT | 1W | 官方 futures kline 对齐 | `64694.8 / 66924.1 / 63736.1` 对齐 | PASS |

实时 close、volume 和 quote volume 会在 WS 消息与随后执行的官方 HTTP 查询之间继续变化，因此验收以时间戳和 open/high/low 等稳定字段完全一致、close/volume 仅单向实时推进为准。KORU `1W` 的关键错误字段已从本地合成的 `low=17.62` 修正为 OKX 官方前复权的 `low=17.43`。

## 当前覆盖范围

- raw 官方对齐：OKX 动作品种覆盖 `1m` 到 `1D`、`2D` 和 `1W`；Binance 动作品种覆盖 `1m` 到 `1D` 和 `1W`。Binance 官方不提供 `2D`。
- API 仅提供 raw；旧 adjusted 请求会明确返回 `400`。
- rename + rebase：SPACEX 历史使用后继品种 `SPCX-USDT-SWAP` 对齐。

当前未覆盖：

- 月线的公司行动官方逐周期 raw 回补，以及 Binance 官方未提供的 `2D`。
- 如果交易所在动作日之外继续回写更早历史，需要扩大回补窗口后再次扫描。
- 普通品种的高周期历史全量官方回补；ZHIPU 对照已证明本地 1m 聚合不能保证与 OKX 官方 5m 以上周期完全一致。

## 结论

按“我们的 raw 服务与交易所当前官方同周期历史接口一致”的标准，六个典型除权/合约规模调整样本均通过，OKX/Binance BTC 普通品种对照也通过。测试同时发现：普通品种并非天然全周期对齐，ZHIPU 的 1m 通过但高周期存在官方目标周期差异。因此当前实现已经覆盖“历史复权动作窗口”的官方同周期修复，但还不能宣称所有品种、所有历史高周期都与官方完全一致；下一步应增加普通品种的官方目标周期回补或定期校验。
