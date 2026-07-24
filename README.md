# crypto-ticket

`crypto-ticket` 当前主链路是 Go 版实时行情服务 `cmd/marketd` 加 React dashboard。它直接订阅 Binance / OKX 官方 `1m` kline WebSocket，把未完成的 `1m` K 线保存在进程内存，把已完成的官方 `1m` K 线写入 store，并用已完成的 `1m` K 线合成更高周期。

旧的 Python 模块、归档脚本和 Redis kline cache 清理工具仍在仓库中，但 `marketd` 的实时主链路不再使用 Redis Streams，也不再用 tick 自行聚合 `1m`。

## 当前数据流

```text
Binance / OKX official 1m kline WebSocket
  -> internal/collector.Runner
  -> MarketService.IngestKline
      -> non-final 1m:
           保存到进程内 liveBars
           每次收到官方推送都转发 1m kline WS event
           同时推送 ticker WS event，price = 当前 1m close
           如果有高周期 WS 订阅者，实时合成并推送对应高周期 live kline
      -> final 1m:
           同步写入 store，MySQL 模式下写 bar_history
           推送 final 1m kline + ticker WS event
           查询已落库 final 1m，合成已完成的高周期 K 线
           写入高周期 final bar 并推送高周期 kline WS event
      -> kline guardian:
           记录 final 1m 水位
           如果发现水位跳过，使用交易所 REST official 1m kline 补洞
           定时用近窗口 REST official 1m kline 校验/修复 MySQL
           修复后复用 MarketService 入库、推送和级联 rollup

HTTP /api/v1/klines?include_live=true
  -> 读取 store 中已持久化的目标周期 bars
  -> 如有当前 live 1m，则把 live bar 合并进去
  -> 对高周期请求，会按级联源周期合成当前未完成高周期
```

关键结论：

- `marketd` 订阅的是交易所官方 `1m` kline/candle，不是 trade tick。
- `marketd` 当前没有“收到推送后插 Redis，再消费 Redis 入 MySQL”的链路。
- Redis 只在 `cmd/backfill_klines` 中用于清理旧的 `kline:*` / `livebar:*` cache key。
- MySQL 中存的是 final bar。实时未完成 `1m` 只在当前 Go 进程内。
- HTTP 可以读不同 timeframe 的行情；默认会包含当前未完成行情。
- WS 会推 `1m` live，也会对当前有订阅者的高周期实时合成并推送 live bar。
- 高周期 final/live rollup 使用级联源周期，不再所有高周期都直接扫描 `1m`：`30m <- 15m`，`1H <- 30m`，`1D <- 1H`，`1M/3M <- 1D`。
- Kline guardian 不从 trade 重算 OHLCV；它只用交易所官方 REST kline 校验和修复官方 WS 可能漏掉的 final `1m`。

## 品种分类与公司行动修复

symbol 元数据会写入 `symbol_registry`，并带上以下分类字段：

- `source_market`：交易所和市场类型，例如 `binance:um_futures`、`okx:SWAP`。
- `instrument_type`：交易所原始合约/品种类型，例如 `PERPETUAL`、`TRADIFI_PERPETUAL`、`SWAP`。
- `asset_class`：归一化资产类别，例如 `crypto`、`equity`、`commodity`、`index`、`pre_market`。
- `rule_type`：交易规则类型，当前支持 `normal` / `pre_market`。
- `lifecycle_phase`：生命周期状态，例如 `continuous`、`pre_market`、`preopen`、`suspend`、`expired`。

当前分类来源：

- Binance `exchangeInfo`：读取 `contractType`、`underlyingType`、`underlyingSubType`、`status/contractStatus`。`underlyingType=EQUITY` 会标为 `equity`，`underlyingType=PREMARKET` 会标为 `pre_market`。
- OKX `public/instruments`：读取 `instType`、`instCategory`、`ruleType`、`state`。`instCategory=3` 会标为 `equity`，`ruleType=pre_market` 会标为 `pre_market`。

行情接口只提供交易所官方 `raw` 口径。`price_mode` 省略或传 `raw` 均可；旧的 `backward_adjusted`、`forward_adjusted` 参数会返回 `400 unsupported price mode`。系统不再计算复权因子，也不再提供动态或物化复权视图。

### 历史公司行动修复

`cmd/backfill_adjustments` 支持 Binance 和 OKX。它分页扫描官方公告、读取公告比例，使用公告窗口内的官方 `1m` K 线定位精确动作边界，并把审计记录幂等写入 `corporate_action_event`。

确认边界后，命令分别拉取交易所官方目标周期 K 线，按 `exchange / symbol / timeframe / start_ms` 覆盖动作所在 UTC 日及边界上下文的 `bar_history`。OKX 修复 `1m / 5m / 15m / 30m / 1H / 2H / 4H / 6H / 12H / 1D / 2D / 1W`；Binance 修复相同周期中的 `1m` 到 `1D` 以及 `1W`，因为 Binance 官方不提供 `2D`。每次执行都会重新拉取并覆盖，因此可以吸收交易所后续对历史 K 线的回写。

```bash
# 先预览，不写数据库
go run ./cmd/backfill_adjustments \
	-exchange binance \
  -symbols KORUUSDT \
  -start 2026-07-01 \
  -end 2026-07-31 \
  -dry-run

# 确认结果后写入
go run ./cmd/backfill_adjustments \
	-exchange binance \
  -symbols KORUUSDT \
  -start 2026-07-01 \
  -end 2026-07-31

# OKX rebase / rename；先 dry-run，再去掉 -dry-run 写入
go run ./cmd/backfill_adjustments \
	-exchange okx \
	-symbols OPENAI-USDT-SWAP,ANTHROPIC-USDT-SWAP,SPCX-USDT-SWAP,ZHIPU-USDT-SWAP \
	-start 2026-06-01 \
	-end 2026-07-01 \
	-dry-run
```

不传 `-symbols` 时会处理日期范围内扫描到的全部公司行动。`-continue-on-error` 可跳过无法确认 K 线边界的公告。扫描范围内没有公告的品种不会改写数据。

## 支持的周期

运行时 timeframe 支持：

```text
1m 5m 15m 30m 1H 2H 4H 6H 12H 1D 2D 3D 5D 1W 2W 1M 3M
```

时间桶按 UTC 计算。`1W` / `2W` 以 UTC Monday 为周起点，`1M` / `3M` 以 UTC 月初为起点。

HTTP 和 WS 参数是大小写敏感的，必须使用上面的规范值，例如 `1H`，不是 `1h`。

REST backfill 使用交易所官方历史 K 线接口，支持范围受交易所限制：

- Binance REST: `1m 5m 15m 30m 1H 2H 4H 6H 12H 1D 3D 1W 1M`
- OKX REST: `1m 5m 15m 30m 1H 2H 4H 6H 12H 1D 2D 3D 1W 1M 3M`
- 运行时 rollup 可以基于 final `1m` 合成完整的本地支持周期。

## 启动和配置

先构建前端：

```bash
cd web
npm install
npm run build
cd ..
```

内存 demo 模式：

```bash
USE_MEMORY_STORE=true \
DASHBOARD_DIR=./web/dist \
HTTP_ADDR=127.0.0.1:8088 \
go run ./cmd/marketd
```

MySQL 实时采集模式：

```bash
USE_MEMORY_STORE=false \
ENABLE_COLLECTOR=true \
ENABLE_MOCK_SYMBOLS=false \
MYSQL_DSN='root:root123@tcp(127.0.0.1:3306)/crypto_ticket?parseTime=true' \
DASHBOARD_DIR=./web/dist \
HTTP_ADDR=127.0.0.1:8088 \
go run ./cmd/marketd
```

常用环境变量：

| 变量 | 默认值 | 说明 |
| --- | --- | --- |
| `HTTP_ADDR` | `127.0.0.1:8088` | HTTP / WS / dashboard 监听地址 |
| `DASHBOARD_DIR` | `./web/dist` | 静态 dashboard 目录 |
| `USE_MEMORY_STORE` | `true` | `true` 使用进程内 store；`false` 使用 MySQL |
| `MYSQL_DSN` | 从 `MYSQL_USER` / `MYSQL_PASSWORD` / `MYSQL_HOST` / `MYSQL_PORT` / `MYSQL_DATABASE` 拼出 | MySQL DSN |
| `ENABLE_COLLECTOR` | `false` | 是否启动 Binance / OKX WebSocket collector |
| `ENABLE_KLINE_GUARDIAN` | 跟随 `ENABLE_COLLECTOR` | 是否启动轻量 K 线守护者 |
| `KLINE_GUARDIAN_AUDIT_INTERVAL_SECONDS` | `60` | 近窗口 REST 校验周期 |
| `KLINE_GUARDIAN_WINDOW_MINUTES` | `30` | 每次校验最近多少分钟的 final `1m` |
| `KLINE_GUARDIAN_DELAY_SECONDS` | `120` | 只校验已结束并延迟足够久的 bar，避免刚 final 的边缘差异 |
| `KLINE_GUARDIAN_SYMBOLS_PER_RUN` | `50` | 每轮最多校验多少个 active symbol，按轮询 cursor 分批 |
| `KLINE_GUARDIAN_REQUEST_DELAY_MS` | `100` | symbol 之间 REST 请求节流 |
| `KLINE_GUARDIAN_SYMBOL_MAX_AGE_SECONDS` | `600` | 只审计最近被 collector 刷新过的 active symbol，避免旧脏 symbol 进入 REST 校验 |
| `ENABLE_MOCK_SYMBOLS` | collector 关闭时默认 `true` | 是否插入 demo symbols |
| `MARKET_TIMEFRAMES` | 全部支持周期 | final 高周期 rollup 入库的目标周期列表；会自动补齐级联 rollup 依赖；live WS 会扫描全部支持周期并只为有订阅者的 channel 合成 |
| `RECENT_CACHE_LIMIT` | `300` | 默认 HTTP K 线条数 |
| `ENABLED_EXCHANGES` | `binance,okx` | 启用哪些交易所 |
| `SYMBOL_REFRESH_INTERVAL_SECONDS` | `120` | OKX 订阅刷新间隔；Binance static streams 只在重连时重新拉 symbols |
| `BINANCE_UM_ENABLED` | `true` | 是否启用 Binance USDT-M futures |
| `BINANCE_COIN_ENABLED` | `true` | 是否启用 Binance COIN-M futures |
| `OKX_ENABLED` | `true` | 是否启用 OKX |
| `REDIS_URL` | `redis://127.0.0.1:6379/0` | 仅 backfill 清理旧 Redis kline cache 时使用 |

运行测试：

```bash
go test ./...
```

## 交易所订阅逻辑

`cmd/marketd` 根据配置创建 exchange adapter：

- Binance USDT-M futures: `https://fapi.binance.com`, `wss://fstream.binance.com/market`
- Binance COIN-M futures: `https://dapi.binance.com`, `wss://dstream.binance.com/ws`
- OKX swap: `https://www.okx.com`, `wss://ws.okx.com:8443/ws/v5/public`

启动 collector 后，每个 enabled exchange runtime 会：

1. 通过 REST 拉取 symbol 列表。
2. 把 symbol 元数据 upsert 到 store 的 `symbol_registry`。
3. 只保留 active symbol 进行订阅。
4. 建立 WebSocket，读取官方 `1m` kline/candle 消息。
5. 每条消息解析为 `market.Bar` 后调用 `MarketService.IngestKline`。

Binance 订阅：

- Binance adapter 实现了 `StaticStreamURL`。
- collector 会按 `BINANCE_SUBSCRIPTION_CHUNK_SIZE` / `BINANCE_COIN_SUBSCRIPTION_CHUNK_SIZE` 分 chunk，默认每个 chunk 50 个 symbol。
- 每个 chunk 打开一个 combined stream，例如：

```text
wss://fstream.binance.com/market/stream?streams=btcusdt@kline_1m/ethusdt@kline_1m
```

- Binance static stream 连接存活期间不做增量 subscribe/unsubscribe；symbols 会在重连时重新拉取。

OKX 订阅：

- OKX 使用一个 public WebSocket 连接。
- 按 `OKX_SUBSCRIPTION_CHUNK_SIZE` 分批发送 subscribe payload，默认每批 120 个 symbol。
- 订阅参数是 `channel=candle1m` 和 `instId=<symbol>`。
- OKX 连接存活期间会按 `SYMBOL_REFRESH_INTERVAL_SECONDS` 重新拉 symbol 列表，并对差异做 subscribe/unsubscribe。

## Ingest 逻辑

`MarketService.IngestKline` 会先规范化和校验 bar：

- `exchange` 转小写。
- `symbol` 转大写。
- `timeframe` 必须是支持的规范值。
- `margin_type` 规范化为 `umargin` / `coinmargin` 等。
- 自动补齐 `end_ms`、`last_tick_ms`、`updated_at_ms`、`source`、`reason`。
- OHLC 必须大于 0，`start_ms` / `end_ms` 必须有效。

然后计算派生字段：

- 查询同周期上一根 final bar 的 close 作为 `prev_close`。
- `chg = (close - prev_close) / prev_close * 100`。
- `amp = (high - low) / low * 100`。

非 final `1m`：

- 保存到进程内 `liveBars`，key 为 `exchange:symbol:timeframe`。
- 每次收到官方 kline update 都推送 `1m` `kline` event。
- 对 `1m` bar 还会推送 `ticker` event，ticker 的 price 来自当前 bar 的 close。
- 如果该 symbol 有 `5m` / `1H` / `1D` 等高周期 kline WS 订阅者，会查询当前周期内已完成 `1m`，加上这根 live `1m`，实时合成高周期 live bar 并推送。
- 不写 MySQL。

final `1m`：

- 先从 `liveBars` 删除对应 live bar。
- 同步 `UpsertBars` 到 store。
- MySQL 模式下写入 `bar_history`。
- 写库成功后推送 final `kline` event 和 `ticker` event。
- 再触发高周期 rollup。
- 通知 kline guardian 更新该 symbol 的 final `1m` 水位。

## Kline Guardian

`marketd` 内置轻量 K 线守护者，用来处理 WS 断流、进程重启、写入失败后可能造成的 final `1m` 缺口或字段不一致。

守护者有两条路径：

1. 实时水位检测：每次 final `1m` 成功入库后，检查 `(exchange, symbol, 1m)` 的上一根 final start。若新 final start 跳过了一个或多个分钟桶，则立即用交易所 REST official `1m` kline 拉取缺口区间并修复。
2. 近窗口 REST 校验：按 `KLINE_GUARDIAN_AUDIT_INTERVAL_SECONDS` 定时轮询 active symbols，只检查 `now - KLINE_GUARDIAN_WINDOW_MINUTES` 到 `now - KLINE_GUARDIAN_DELAY_SECONDS` 之间的 final `1m`，比较本地 MySQL 与官方 REST 的 OHLCV。

为避免历史测试数据或旧脏 symbol 触发无效 REST 请求，近窗口校验只会选择最近 `KLINE_GUARDIAN_SYMBOL_MAX_AGE_SECONDS` 内被 collector 刷新过的 active symbol。实时水位检测不受这个限制，因为它来自真实 WS final bar。

校验字段：

- `open_price` / `high_price` / `low_price` / `close_price`
- `volume` / `quote_volume` / `contract_volume`
- Binance 会校验 `trade_count`；OKX REST candles 当前没有 trade count，跳过该字段。

修复逻辑：

1. 缺失或字段不一致时，以交易所 REST official bar 为准。
2. 调用 `MarketService.RepairFinalBars` upsert final `1m`。
3. 重新计算包含该 `1m` 的高周期 bucket，并继续级联到更高周期。
4. 修复后的 final `1m` 和受影响高周期 final bar 都会走正常 WS 推送路径。

守护者只校验 final `1m`，不校验当前未完成 live bar。live 行情仍以官方 WS 最新推送为准。

## 高周期合并逻辑

高周期 final 合并由 final 源周期触发。源周期是级联的，不是所有目标周期都直接扫 `1m`：

```text
5m / 15m <- 1m
30m      <- 15m
1H       <- 30m
2H/4H/6H/12H/1D <- 1H
2D/3D/5D/1W/2W/1M/3M <- 1D
```

对每个配置的 timeframe：

1. 找出目标周期的级联源周期。
2. 用当前 final 源 bar 的 `start_ms` 计算目标周期 `target_start` 和 `target_end`。
3. 正常实时入库时，如果当前源 bar 还没覆盖到目标周期结束，跳过。
4. 修复模式下，即使被修复的是 bucket 中间的源 bar，也会尝试重算它所在的完整目标 bucket。
5. 从 store 查询 `[target_start, target_end]` 范围内的 final 源周期 bars。
6. 严格检查源周期 bars 是否从 `target_start` 开始连续覆盖到 `target_end`；中间缺任何一根都不生成 final rollup。
7. 使用这些源周期 bars 合成目标周期：
   - open = 第一根源 bar 的 open
   - high = 所有源 bar 的 high 最大值
   - low = 所有源 bar 的 low 最小值
   - close = 最后一根源 bar 的 close
   - volume / quote_volume / contract_volume / trade_count 求和
   - last_tick_ms 取最大值
   - `is_final=true`
   - `source=rollup`
   - `reason=rollup`
8. 计算派生字段后写入 store。
9. 推送该高周期 final `kline` event。

## WebSocket 推送

入口：

```text
GET /api/v1/ws
```

客户端发送订阅消息：

```json
{
  "op": "subscribe",
  "req_id": "1",
  "channels": [
    { "type": "ticker", "exchange": "binance", "symbol": "BTCUSDT" },
    { "type": "kline", "exchange": "binance", "symbol": "BTCUSDT", "timeframe": "1m" }
  ]
}
```

服务端返回：

```json
{
  "op": "subscribed",
  "req_id": "1",
  "channels": [
    { "type": "ticker", "exchange": "binance", "symbol": "BTCUSDT" },
    { "type": "kline", "exchange": "binance", "symbol": "BTCUSDT", "timeframe": "1m" }
  ]
}
```

`ticker` event：

```json
{
  "type": "ticker",
  "seq": 12,
  "exchange": "binance",
  "symbol": "BTCUSDT",
  "tick": {
    "exchange": "binance",
    "symbol": "BTCUSDT",
    "ts_ms": 1779340060000,
    "price": 105,
    "size": 2.5,
    "event_type": "kline",
    "source": "exchange_kline",
    "recv_ms": 1779340060123
  }
}
```

`kline` event：

```json
{
  "type": "kline",
  "seq": 13,
  "exchange": "binance",
  "symbol": "BTCUSDT",
  "timeframe": "1m",
  "bar": {
    "exchange": "binance",
    "symbol": "BTCUSDT",
    "margin_type": "umargin",
    "timeframe": "1m",
    "start_ms": 1779340000000,
    "end_ms": 1779340059999,
    "open_price": 100,
    "high_price": 110,
    "low_price": 95,
    "close_price": 105,
    "volume": 2.5,
    "quote_volume": 260,
    "trade_count": 12,
    "last_tick_ms": 1779340060000,
    "is_final": true,
    "source": "exchange_kline",
    "reason": "final",
    "updated_at_ms": 1779340060123
  }
}
```

WS 时效性：

- 非 final `1m` kline 不再做服务端 1 秒节流；每次收到交易所官方 kline update 都会转发给订阅者。
- final `1m` 会先写 store，写成功后再推送。
- `ticker` 基于 `1m` kline 的 close，不是逐笔 trade tick。
- 高周期 final bar 在周期结束且对应源周期 final bar 入库后级联生成并推送，例如 `1D <- 1H`、`1M/3M <- 1D`。
- 高周期 live bar 会在有人订阅对应 `exchange/symbol/timeframe` 时实时级联合成并通过 WS 推送。live 合成覆盖全部支持周期，不受 `MARKET_TIMEFRAMES` 限制；为避免全市场无意义 DB 查询，没有订阅者的高周期不会主动合成。
- 服务端每 15 秒发送一次 `{"op":"ping"}`；客户端发送 `{"op":"ping"}` 时服务端返回 `{"op":"pong"}`。
- 每个 subscriber 的事件队列大小是 256；客户端消费太慢时，新事件会被丢弃，不做阻塞和重放。因此前端应该先 HTTP 拉快照，再接 WS 增量。
- 当前 WS 只有 subscribe，没有 unsubscribe。前端切换 symbol/timeframe 时会关闭旧连接并重新连接。

## HTTP API

健康检查：

```bash
curl 'http://127.0.0.1:8088/healthz'
```

最新 ticker：

```bash
curl 'http://127.0.0.1:8088/api/v1/ticker/latest?exchange=binance&symbol=BTCUSDT'
```

返回逻辑：

- 如果当前进程里有该 symbol 的 live `1m`，返回 live `1m.close` 构造的 ticker。
- 如果没有 live `1m`，返回 store 中最近一根 `1m` bar 构造的 ticker。
- 这里的“最新”是官方 `1m` kline 最新 close，不是逐笔成交最新价。

K 线：

```bash
curl 'http://127.0.0.1:8088/api/v1/klines?exchange=binance&symbol=BTCUSDT&timeframe=1m&limit=300&include_live=true'
curl 'http://127.0.0.1:8088/api/v1/klines?exchange=binance&symbol=BTCUSDT&timeframe=1H&limit=300&include_live=true'
```

参数：

| 参数 | 必填 | 默认值 | 说明 |
| --- | --- | --- | --- |
| `exchange` | 是 | 无 | `binance` 或 `okx` |
| `symbol` | 是 | 无 | 例如 `BTCUSDT` 或 `BTC-USDT-SWAP` |
| `timeframe` | 是 | 无 | 支持的规范 timeframe |
| `limit` | 否 | `300` | 服务端最大限制为 `1000` |
| `include_live` | 否 | `true` | `false` / `0` 时只返回已持久化 bars |

`include_live=true` 时的返回逻辑：

- `timeframe=1m`：读取 store 中最近 final `1m`，再合并当前进程内 live `1m`。如果 live bar 是新一根，返回数量可能是 `limit + 1`。
- `timeframe>1m`：读取 store 中该 timeframe 的 final bars，再按级联源周期临时合成一根未完成高周期 bar 并合并返回。
- 如果当前进程刚启动且还没收到 live `1m`，则只能返回 store 中已有 final bars。
- `include_live=false` 时，只返回 store 中已持久化 bars。

现在访问能拿到什么：

- `/api/v1/ticker/latest`：能拿到当前进程最后收到的 live `1m` close；没有 live 时拿最近 final `1m`。
- `/api/v1/klines&timeframe=1m&include_live=true`：能拿到历史 final `1m` + 当前未完成 `1m`。
- `/api/v1/klines&timeframe=5m/1H/...&include_live=true`：能拿到历史 final 高周期 + 当前未完成高周期，当前未完成高周期是在请求时按级联源周期合成的。
- `/api/v1/klines&include_live=false`：只能拿到已经入库的 final bars，最新数据至少落后到最近一次周期收盘。

Symbols：

```bash
curl 'http://127.0.0.1:8088/api/v1/symbols?exchange=binance&active=true'
curl 'http://127.0.0.1:8088/api/v1/symbols?exchange=okx&active=all'
```

Dashboard：

```text
http://127.0.0.1:8088/
```

前端启动时会：

1. HTTP 拉 Binance + OKX active symbols，watchlist 和搜索框支持全市场筛选。
2. HTTP 拉最新 ticker。
3. HTTP 拉 `include_live=true` 的 K 线快照。
4. 打开 `/api/v1/ws`。
5. 订阅当前 symbol 的 ticker 和当前 timeframe 的 kline。
6. 显示官方推送距今、交易所事件距今、浏览器收到距今、collector span 等延迟指标。
7. 用户缩放 K 线后，后续 live 更新不会自动 `fitContent()` 拉回全量；只有切换 exchange / symbol / timeframe 或首次加载数据时才自动适配全图。

## 数据库逻辑

`marketd` 支持两种 store：

- `USE_MEMORY_STORE=true`：所有 symbols 和 bars 存在 Go 进程内，重启丢失。
- `USE_MEMORY_STORE=false`：使用 MySQL。

MySQL 模式下，Go 会确保存在这些表：

- `symbol_registry`
- `bar_history`
- `kline_guardian_state`
- `kline_guardian_event`

`symbol_registry`：

- 主键：`exchange, symbol`
- 存交易所 symbol 元数据、active 状态、最近看到时间和原始状态。
- collector 每次拉 symbol list 时 upsert。

`bar_history`：

- 主键：`exchange, symbol, timeframe, start_ms`
- 存 OHLCV、quote volume、contract volume、trade count、derived fields、`last_tick_ms`、`is_final`。
- `UpsertBars` 使用 `INSERT ... ON DUPLICATE KEY UPDATE`，同一根 K 线可覆盖更新。
- `BarsInRange` 只返回 `is_final = 1` 的 bars，rollup 依赖这个查询。
- `RecentBars` 按 `start_ms DESC LIMIT ?` 查最近 bars，再反转成时间升序返回。

实时写库路径：

```text
final 1m received
  -> MarketService.persistFinalBars
  -> store.UpsertBars
  -> MySQL bar_history upsert
  -> publish final 1m
  -> rollup higher timeframes
  -> upsert final higher timeframe bars
  -> publish final higher timeframe bars
```

如果 final `1m` 写库失败，collector 会收到 error，当前 exchange worker 会重连；该 final bar 不会被推送，也不会触发高周期 rollup。

`kline_guardian_state`：

- 主键：`exchange, symbol, timeframe`
- 存每个 symbol 的 final `1m` 水位、最近校验窗口、最近 gap 范围和状态。

`kline_guardian_event`：

- 自增主键：`id`
- 存 `gap_detected`、`missing_repair`、`mismatch_repair`、`rest_error`、`repair_error` 等事件。
- `old_value_json` / `new_value_json` 用来记录修复前后的 bar 快照或错误信息。

### Retention 和分区维护

默认历史保留策略：

| 周期 | 保留 |
| --- | --- |
| `1m` | 15 天 |
| `5m` / `15m` / `30m` | 90 天 |
| `1H` / `2H` / `4H` / `6H` / `12H` | 180 天 |
| `1D` 及以上 | 全部保留 |

清理命令默认是 dry-run，只统计将删除的行数：

```bash
USE_MEMORY_STORE=false \
MYSQL_DSN='root:root123@tcp(127.0.0.1:3306)/crypto_ticket?parseTime=true' \
go run ./cmd/maintain_klines -mode=retention
```

真实分批删除：

```bash
USE_MEMORY_STORE=false \
MYSQL_DSN='root:root123@tcp(127.0.0.1:3306)/crypto_ticket?parseTime=true' \
go run ./cmd/maintain_klines -mode=retention -dry-run=false -batch-size=10000
```

`sql/schema.sql` 的 `bar_history` 使用 `PARTITION BY RANGE COLUMNS(timeframe, start_ms)`。会过期的周期按 timeframe + month 分区，`1D` 及以上只保留 future 分区。生成迁移 SQL：

```bash
go run ./cmd/maintain_klines -mode=partition-create-sql -partition-start=2026-01 -partition-months=12 > /tmp/bar_history_timeframe_partition.sql
```

生成按 retention 可直接 drop 的整月过期分区 SQL：

```bash
go run ./cmd/maintain_klines -mode=partition-drop-sql -partition-start=2026-01 -partition-months=12
```

把现有 `p_tf_*_future` 分区扩展成新的月分区：

```bash
go run ./cmd/maintain_klines -mode=partition-add-sql -partition-start=2029-01 -partition-months=12
```

## Backfill 和 Redis

历史回填：

```bash
USE_MEMORY_STORE=false \
MYSQL_DSN='root:root123@tcp(127.0.0.1:3306)/crypto_ticket?parseTime=true' \
REDIS_URL='redis://127.0.0.1:6379/0' \
go run ./cmd/backfill_klines -exchanges=binance,okx -timeframes=1m,5m,15m,30m,1H -limit=300
```

`cmd/backfill_klines` 会：

1. 读取 / 刷新 symbols。
2. 调用交易所 REST kline 接口拉历史 official final bars。
3. 批量 upsert 到 MySQL `bar_history`。
4. 默认清理旧 Redis kline recent/live cache key。

如果当前环境没有 Redis，回填时加 `-clear-redis=false`：

```bash
USE_MEMORY_STORE=false \
MYSQL_DSN='root:root123@tcp(127.0.0.1:3306)/crypto_ticket?parseTime=true' \
go run ./cmd/backfill_klines -exchanges=binance,okx -timeframes=1m,5m,15m,30m,1H -limit=300 -clear-redis=false
```

测试环境清空数据：

```bash
go run ./cmd/backfill_klines -clear-all-bar-history -clear-all-redis-kline -limit=300
```

Redis 当前定位：

- `marketd` 实时服务不连接 Redis。
- `REDIS_URL` 只被 `cmd/backfill_klines` 用来清理旧 key。
- 清理的 key pattern 包括：

```text
kline:idx:{exchange}:{symbol}:{timeframe}
kline:bar:{exchange}:{symbol}:{timeframe}
livebar:{exchange}:{symbol}:{timeframe}
```

## 当前限制

- 没有 Redis / Kafka 这样的实时缓冲队列；collector、入库、rollup、推送都在 `marketd` 进程内完成。
- 没有 tick-level 最新价；ticker 来自 `1m` kline close。
- 高周期 live WS 合成只对当前有订阅者的 channel 执行，避免全市场每次 kline update 都触发高周期查询；final 高周期入库仍由 `MARKET_TIMEFRAMES` 控制。
- 进程重启后 live bar 丢失，直到下一条交易所 kline update 到达；final 历史仍在 MySQL。
- Kline guardian 会修复近窗口 final `1m` 缺口；超过窗口的历史需要用 backfill 或单独维护命令修复。
- Binance static stream 的 symbol 刷新只发生在重连时；OKX 会在连接内定期刷新并同步订阅差异。
- 如果没有先 backfill，HTTP 历史只包含服务启动后成功写入的 final bars。
