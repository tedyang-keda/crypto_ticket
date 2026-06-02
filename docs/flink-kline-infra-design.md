# Flink K 线基础设施开发方案

> 日期：2026-06-01  
> 目标：先保证 1m K 线准确、低延迟、不漏、可恢复、可重放，再基于 completed 1m K 线生成高级别 K 线。

## 1. 背景与核心目标

当前 Go 实时行情服务把 tick 直接聚合到多个 timeframe，包括 `1m/5m/15m/1H/1D` 等。这会让状态维护、迟到数据、重启恢复、数据修复都变复杂，也不符合“1m 是事实源，高级别从 1m 拼接”的基础设施设计。

新的目标是把链路收敛为：

```text
Exchange WebSocket
  -> Go Collector 标准化 tick
  -> Kafka / Redpanda 原始 tick 日志
  -> Flink 1m event-time 聚合
  -> completed 1m K 线入库
  -> completed 1m K 线 rollup 高级别 K 线
  -> Go API 查询 completed bars + Redis live 1m 拼接实时结果
```

第一阶段只追求一个核心能力：

```text
每个 symbol 的每一分钟 1m K 线都能按 tick.ts_ms 精准聚合、稳定入库、不重复、不漏、可恢复。
```

## 2. 非目标

第一阶段不做以下事情：

- 不直接从 tick 维护所有高级别 livebar。
- 不把 MySQL 当实时状态缓存。
- 不在 API 层消费 tick。
- 不做复杂 Dashboard 功能。
- 不先做全量历史 tick 入 MySQL。
- 不把 Redis recent cache 作为正确性依赖。

这些可以后续优化，但不能进入核心正确性链路。

## 3. 总体架构

推荐生产架构：

```text
Binance / OKX WS
        |
        v
Go Collector
  - connect/reconnect
  - subscribe symbols
  - normalize tick
  - write Kafka/Redpanda
        |
        v
Kafka/Redpanda topic: ticks.raw
  - partition key: exchange + symbol
  - retention: 7-30 days
  - replay source of truth for raw ticks
        |
        v
Flink Job A: 1m Aggregator
  - event time = tick.ts_ms
  - watermark
  - dedupe by trade_id
  - live 1m -> Redis
  - final 1m -> Kafka bars.1m.final
  - final 1m -> DB bars_1m
        |
        v
Flink Job B: Rollup Aggregator
  - consume bars.1m.final
  - aggregate 5m/15m/30m/1H/1D...
  - final higher bars -> DB
        |
        v
Go API
  - HTTP klines
  - WebSocket realtime kline snapshot
  - query DB final bars
  - query Redis live 1m
  - stitch latest unfinished bar on read
```

## 4. 组件职责

### 4.1 Go Collector

职责：

- 拉交易所 symbol 列表。
- 建立并维护 WebSocket 连接。
- 订阅 active symbols。
- 重连后恢复订阅。
- 解析 Binance/OKX WS 消息。
- 标准化为内部 Tick。
- 写入 Kafka/Redpanda。

Collector 不做：

- 不聚合 K 线。
- 不写 K 线表。
- 不维护 timeframe 状态。
- 不做数据查询。

Collector 写 Kafka 成功后，才认为 tick 进入系统。

### 4.2 Kafka / Redpanda

职责：

- 原始 tick 可重放日志。
- 承接交易所 WS 和 Flink 之间的流量峰值。
- 为 Flink checkpoint 提供 offset 恢复能力。
- 为后续重算、审计、修复提供原始数据。

推荐 topic：

```text
ticks.raw
bars.1m.final
bars.1m.late
bars.rollup.final
```

`ticks.raw` 分区 key：

```text
exchange + "|" + symbol
```

这样同一个 `(exchange, symbol)` 尽量进入同一个 Kafka partition，便于 Flink `keyBy(exchange, symbol)` 后保持状态局部性。

### 4.3 Flink Job A：1m Aggregator

这是最核心的任务。

职责：

- 读取 `ticks.raw`。
- 使用 `tick.ts_ms` 作为 event time。
- 使用 watermark 处理乱序和延迟。
- 按 `(exchange, symbol)` keyBy。
- 按 1m 窗口维护 OHLCV。
- 对 tick 去重。
- 持续写 Redis live 1m。
- 窗口 final 后写 DB 和 `bars.1m.final`。
- 对迟到 tick 做 upsert 修正或 side output。

建议优先使用 `KeyedProcessFunction` 实现，而不是纯 SQL 窗口。

原因：

- 需要每个 tick 或按小间隔输出 live 1m。
- 需要自定义 dedupe。
- 需要自定义迟到修正。
- 需要同时写 Redis live 和 final sink。
- 需要清晰控制状态 TTL。

### 4.4 Flink Job B：Rollup Aggregator

职责：

- 读取 `bars.1m.final`。
- 基于 completed 1m K 线生成高级别 final K 线。
- 输出到 DB。

高级别 K 线不直接从 tick 算。

高级别聚合规则：

```text
open         = 第一根 1m.open
high         = max(1m.high)
low          = min(1m.low)
close        = 最后一根 1m.close
volume       = sum(1m.volume)
quote_volume = sum(1m.quote_volume)
trade_count  = sum(1m.trade_count)
start_ms     = timeframe floor
end_ms       = next_start_ms - 1
```

### 4.5 Go API

职责：

- 查询 completed bars。
- 查询 Redis live 1m。
- 按请求 timeframe 临时拼接最新未完成 bar。
- 提供 HTTP 和 WebSocket。

API 不做：

- 不消费原始 tick。
- 不维护长期状态。
- 不写 final K 线。

## 5. Tick 数据模型

推荐先用 JSON，生产阶段可以切到 Protobuf/Avro 降低体积和解析成本。

```json
{
  "exchange": "binance",
  "symbol": "BTCUSDT",
  "ts_ms": 1779340001000,
  "price": 100000.12,
  "size": 0.01,
  "side": "buy",
  "trade_id": "123456",
  "event_type": "trade",
  "source": "ws",
  "recv_ms": 1779340001033
}
```

字段要求：

- `exchange`：小写，例如 `binance`、`okx`。
- `symbol`：交易所原始 symbol 规范化大写。
- `ts_ms`：交易所成交时间，Flink event time 使用它。
- `price`：成交价。
- `size`：基础币数量。OKX 合约张数需要在 collector 中转换为 base size。
- `trade_id`：用于去重。没有 trade_id 时需要生成 fallback id。
- `recv_ms`：collector 收到消息的本地时间，用于监控链路延迟。

## 6. 1m K 线窗口语义

窗口归属只由 `tick.ts_ms` 决定：

```text
start_ms = floor(tick.ts_ms / 60000) * 60000
end_ms   = start_ms + 60000 - 1
```

窗口更新规则：

第一笔 tick：

```text
open = high = low = close = price
volume = size
quote_volume = price * size
trade_count = 1
last_tick_ms = tick.ts_ms
```

后续 tick：

```text
high = max(high, price)
low = min(low, price)
close = tick with greatest event-time ordering
volume += size
quote_volume += price * size
trade_count += 1
last_tick_ms = max(last_tick_ms, tick.ts_ms)
```

关于 close：

- 如果 tick 在同一窗口内乱序到达，`close` 应该按最大 `tick.ts_ms` 的 tick 计算。
- 如果两个 tick `ts_ms` 相同，可使用 `trade_id` 或 Kafka offset 作为 tie-breaker。

## 7. Watermark 与迟到策略

推荐配置从保守值开始：

```text
watermark_out_of_orderness = 2s
allowed_lateness = 30s
live_emit_interval = 100ms - 500ms
```

含义：

- live 1m：tick 到达后马上更新 Redis，可低延迟。
- first final：watermark 超过窗口结束后输出。
- 允许迟到：窗口 final 后 30s 内迟到 tick 仍可修正该 1m bar。
- 超过 allowed lateness：写 `bars.1m.late`，进入审计/修复流程。

示例：

```text
窗口: [10:00:00, 10:01:00)
watermark_out_of_orderness = 2s

当 Flink watermark >= 10:01:00 时输出 first final。
实际大约在观察到 10:01:02 之后触发。
```

如果 10:01:10 收到属于 10:00 窗口的 tick：

- 若在 allowed lateness 内：更新窗口结果，重新 upsert DB。
- 若超过 allowed lateness：写 late topic，后续修复。

## 8. 去重设计

去重是保证“不重复聚合”的核心。

去重 key：

```text
exchange + "|" + symbol + "|" + trade_id
```

如果交易所没有稳定 `trade_id`，fallback：

```text
exchange + "|" + symbol + "|" + ts_ms + "|" + price + "|" + size + "|" + side
```

Flink state：

```text
MapState<dedupe_key, seen_marker>
TTL = 5m - 30m
```

去重流程：

```text
if dedupe_key already seen:
    drop tick
else:
    mark seen
    update 1m window
```

注意：

- Flink checkpoint 可以处理失败重放下的状态一致性。
- dedupe 主要防交易所重复消息、collector 重连补发、应用层重复发送。

## 9. Flink 状态设计

Key：

```text
exchange + "|" + symbol
```

State：

```text
MapState<window_start_ms, OneMinuteBarAccumulator>
MapState<dedupe_key, Boolean>
ValueState<last_live_emit_ms>
```

`OneMinuteBarAccumulator`：

```text
exchange
symbol
timeframe = "1m"
start_ms
end_ms
open_price
high_price
low_price
close_price
close_tick_ts_ms
close_tie_breaker
volume
quote_volume
trade_count
last_tick_ms
updated_at_ms
is_final
revision
```

定时器：

```text
first-final event-time timer: window_end_ms
cleanup event-time timer: window_end_ms + allowed_lateness + cleanup_grace
```

说明：

- Flink 的 watermark 已经包含 out-of-orderness 延迟。
- 定时器注册在 `window_end_ms`，实际触发时间由 watermark 推进决定。
- 不要再把 `watermark_delay` 手动加到 timer timestamp 上，否则会重复延迟。

## 10. Redis 设计

Redis 只放实时状态，不作为 final K 线事实源。

推荐 key：

```text
live_1m:{exchange}:{symbol}
```

value：

```json
{
  "exchange": "binance",
  "symbol": "BTCUSDT",
  "timeframe": "1m",
  "start_ms": 1779340020000,
  "end_ms": 1779340079999,
  "open_price": 100000.12,
  "high_price": 100020.00,
  "low_price": 99990.50,
  "close_price": 100010.00,
  "volume": 12.34,
  "quote_volume": 1234567.89,
  "trade_count": 302,
  "last_tick_ms": 1779340068123,
  "is_final": false,
  "updated_at_ms": 1779340068150
}
```

TTL：

```text
5m - 10m
```

Redis live 更新频率：

- 极低延迟场景：每个 tick 写一次。
- 常规场景：同一 symbol 100ms-500ms 节流写。

如果交易量极大，建议节流写 Redis，但 Flink 内部 state 必须每个 tick 更新。

## 11. DB 表设计

可以先沿用 `bar_history`，但建议明确主键和 timeframe 语义。

```sql
CREATE TABLE IF NOT EXISTS bar_history (
  exchange VARCHAR(16) NOT NULL,
  symbol VARCHAR(64) NOT NULL,
  timeframe VARCHAR(8) NOT NULL,
  start_ms BIGINT NOT NULL,
  end_ms BIGINT NOT NULL,
  open_price DECIMAL(28, 12) NOT NULL,
  high_price DECIMAL(28, 12) NOT NULL,
  low_price DECIMAL(28, 12) NOT NULL,
  close_price DECIMAL(28, 12) NOT NULL,
  volume DECIMAL(30, 12) NOT NULL DEFAULT 0,
  quote_volume DECIMAL(30, 12) NOT NULL DEFAULT 0,
  trade_count BIGINT NOT NULL DEFAULT 0,
  last_tick_ms BIGINT NOT NULL,
  is_final TINYINT(1) NOT NULL DEFAULT 1,
  revision BIGINT NOT NULL DEFAULT 0,
  source VARCHAR(16) NOT NULL DEFAULT 'flink',
  updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
  PRIMARY KEY (exchange, symbol, timeframe, start_ms),
  KEY idx_lookup (exchange, symbol, timeframe, start_ms)
);
```

写入方式：

```sql
INSERT INTO bar_history (...)
VALUES (...)
ON DUPLICATE KEY UPDATE
  end_ms = VALUES(end_ms),
  open_price = VALUES(open_price),
  high_price = VALUES(high_price),
  low_price = VALUES(low_price),
  close_price = VALUES(close_price),
  volume = VALUES(volume),
  quote_volume = VALUES(quote_volume),
  trade_count = VALUES(trade_count),
  last_tick_ms = VALUES(last_tick_ms),
  is_final = VALUES(is_final),
  revision = GREATEST(revision, VALUES(revision));
```

说明：

- DB sink 不强依赖 XA exactly-once。
- 通过主键和幂等 upsert 保证最终可观测结果正确。
- Flink 到 Kafka sink 可以使用 transactional exactly-once。

## 12. 高级别 K 线生成

高级别 K 线全部从 completed 1m 生成。

支持 timeframe：

```text
5m 15m 30m 1H 2H 4H 6H 12H 1D 1W 1M
```

Rollup 输入：

```text
bars.1m.final
```

Rollup 状态 key：

```text
exchange + "|" + symbol + "|" + target_timeframe
```

窗口归属：

```text
target_start_ms = floor_according_to_timeframe(one_minute_bar.start_ms)
```

final 条件：

- 目标 timeframe 的所有 1m 子窗口已结束。
- watermark 超过目标窗口结束时间。
- 或由 1m final topic 的 event time 推动。

迟到 1m 修正：

- 如果 1m bar 因迟到 tick 被修正，`bars.1m.final` 再发一条同主键 upsert 事件。
- Rollup job 收到修正后重新计算对应高级别窗口并 upsert DB。

## 13. 查询拼接逻辑

### 13.1 查询 1m

请求：

```text
GET /api/v1/klines?exchange=binance&symbol=BTCUSDT&timeframe=1m&limit=100
```

流程：

```text
1. 查 DB latest completed 1m bars，limit=100
2. 查 Redis live_1m:{exchange}:{symbol}
3. 如果 live.start_ms > last_final.start_ms，append live
4. 如果 live.start_ms == last_final.start_ms，使用 live 替换最后一根
5. 返回
```

### 13.2 查询高级别

请求：

```text
GET /api/v1/klines?exchange=binance&symbol=BTCUSDT&timeframe=1H&limit=100
```

流程：

```text
1. 查 DB latest completed 1H bars，limit=100
2. 读取 Redis live 1m
3. 根据 live_1m.start_ms 算 current_1H_start_ms
4. 查 DB completed 1m bars:
     start_ms >= current_1H_start_ms
     start_ms < live_1m.start_ms
5. 把这些 completed 1m + live 1m 临时 rollup 成一根 is_final=false 的 1H bar
6. 如果该 partial 1H start_ms 大于 DB 最后一根 1H，则 append
7. 如果等于 DB 最后一根，则替换
8. 返回
```

WebSocket 实时推送同理：

```text
1. 客户端订阅 kline:binance:BTCUSDT:1H
2. API 监听 live_1m 更新或从 Redis pub/sub 接收更新
3. API 临时拼出当前 1H partial bar
4. 推送给客户端
```

API 不需要维护 1H live 状态。

## 14. 准确性保障

### 14.1 不漏

保证链路：

```text
Collector 写 Kafka 成功
Flink checkpoint Kafka offset + window state
Flink final bar 幂等 upsert DB
```

只要 tick 进入 Kafka，就可以重放。

### 14.2 不重复

依赖：

- Flink checkpoint。
- trade_id dedupe。
- DB primary key upsert。

### 14.3 乱序正确

依赖：

- event time = `tick.ts_ms`。
- watermark out-of-orderness。
- allowed lateness。

### 14.4 重启恢复

依赖：

- Kafka retained raw ticks。
- Flink checkpoint。
- Flink RocksDB state backend。
- DB upsert 幂等。

### 14.5 审计与修复

建议第二阶段增加官方 REST K 线对账：

```text
每根 1m final 后延迟 2-5 分钟
  -> 拉交易所官方 1m kline
  -> 比对 OHLCV
  -> 差异写 audit 表
  -> 必要时修复 bar_history
```

这个不是未来花活，而是行情基础设施的正确性兜底。

## 15. 资源用量评估

下面是估算，实际资源需要以真实 tick/sec 压测为准。

### 15.1 关键变量

```text
S = symbol 数量
R = 平均 tick/sec
P = 峰值 tick/sec
B = 单条 tick 序列化后大小，JSON 约 250B-500B，Protobuf 约 80B-180B
D = Kafka retention 天数
N = Kafka replication factor
C = 压缩比，JSON+lz4/zstd 通常 2x-5x
```

Kafka 磁盘估算：

```text
disk_bytes = R * B * 86400 * D * N / C
```

示例：`R=5000/s, B=350B, D=7, N=3, C=3`

```text
5000 * 350 * 86400 * 7 * 3 / 3
= 1.06 TB
```

因为 replication 和 compression 可能互相抵消，所以可以粗略按：

```text
5000 tick/s, 350B, 7 days ~= 1 TB Kafka disk
```

### 15.2 小规模规格

适用：

```text
50-100 symbols
平均 200-1000 tick/s
峰值 2000-5000 tick/s
retention 3-7 days
```

建议：

```text
Redpanda/Kafka: 1-3 nodes, each 2-4 vCPU, 8-16 GB RAM, 500GB-1TB SSD
Flink JobManager: 1 vCPU, 1-2 GB RAM
Flink TaskManager: 2-4 vCPU, 4-8 GB RAM
Redis: 1-2 vCPU, 2-4 GB RAM
MySQL: 2-4 vCPU, 8 GB RAM, SSD
Go Collector/API: 1-2 vCPU, 1-2 GB RAM
```

如果只有单机部署，可以用 Redpanda 单节点 + Flink standalone，但生产可靠性弱于 3 节点。

### 15.3 中规模规格

适用：

```text
300-1000 symbols
平均 2000-10000 tick/s
峰值 20000-50000 tick/s
retention 7-14 days
```

建议：

```text
Kafka/Redpanda: 3 nodes, each 4-8 vCPU, 16-32 GB RAM, 2-4 TB NVMe
Flink JobManager: 2 vCPU, 2-4 GB RAM
Flink TaskManager: 3-6 nodes, each 4-8 vCPU, 8-16 GB RAM
Redis: 2-4 vCPU, 8-16 GB RAM
MySQL: 8 vCPU, 32 GB RAM, NVMe
Go Collector: 2-4 vCPU per exchange
Go API: 2-4 vCPU
```

说明：

- MySQL 可以扛 K 线写入，因为 1m final bar 写入量是 `symbol_count / minute`，远小于 tick 写入量。
- 高级别 bars 写入更少。
- 真正大流量压力在 Kafka 和 Flink。

### 15.4 大规模规格

适用：

```text
全量主流交易所永续/现货
平均 10000-50000+ tick/s
峰值 100000+ tick/s
retention 14-30 days
```

建议：

```text
Kafka/Redpanda: 5+ nodes, each 8-16 vCPU, 32-64 GB RAM, 多块 NVMe
Flink TaskManager: 8+ nodes, each 8-16 vCPU, 16-32 GB RAM
Redis: cluster 或主从，按 live symbol 数量和推送压力扩容
DB: MySQL 分区表或 ClickHouse/StarRocks 用于历史查询
Collector: 按 exchange/market/symbol shard 拆分
API: 横向扩容
```

大规模下建议历史查询库考虑 ClickHouse/StarRocks。MySQL 仍可保留作为元数据和小规模热数据存储。

## 16. Flink 配置建议

基础配置：

```text
state.backend = rocksdb
checkpoint.interval = 10s - 30s
checkpoint.timeout = 2m
min.pause.between.checkpoints = 5s
restart.strategy = fixed-delay or failure-rate
parallelism.default = 按 Kafka partition 和 tick/sec 设置
```

Kafka：

```text
acks = all
enable.idempotence = true
compression.type = zstd or lz4
linger.ms = 5-20
batch.size = 64KB-256KB
```

Flink sink：

```text
bars.1m.final Kafka sink: exactly-once transactional
DB sink: idempotent upsert
Redis live sink: best effort + periodic overwrite
```

## 17. 监控指标

Collector：

```text
ws_connected
ws_reconnect_count
ws_message_rate
kafka_write_latency_ms
kafka_write_error_count
tick_recv_lag_ms = now_ms - tick.ts_ms
```

Kafka/Redpanda：

```text
topic_bytes_in
topic_messages_in
consumer_lag
under_replicated_partitions
disk_usage
```

Flink：

```text
numRecordsInPerSecond
numRecordsOutPerSecond
busyTimeMsPerSecond
backPressuredTimeMsPerSecond
checkpoint_duration
checkpoint_failed_count
watermark_lag_ms
state_size
rocksdb_block_cache_usage
late_tick_count
dedupe_drop_count
```

K 线质量：

```text
bar_1m_final_delay_ms = final_emit_ms - window_end_ms
missing_1m_bar_count
duplicate_tick_count
late_correction_count
rest_audit_mismatch_count
```

API：

```text
http_klines_latency_ms
ws_subscriber_count
redis_live_read_latency_ms
db_query_latency_ms
```

## 18. 验收标准

第一阶段必须满足：

1. 单 symbol 连续跑 24 小时，DB 中 `1m` 每分钟有且只有一根 final bar。
2. `start_ms` 严格按 60000 递增，无重复、无缺口。
3. Flink 重启后不丢当前分钟数据。
4. Kafka 重放同一段 tick 后，DB final bar 不重复、不翻倍。
5. 同一 trade_id 重复发送，成交量和 trade_count 不翻倍。
6. 乱序 tick 能按 `tick.ts_ms` 归入正确窗口。
7. 迟到 tick 在 allowed lateness 内能修正 final bar。
8. `/klines?timeframe=1m` 能返回 completed bars + 当前 live 1m。
9. `/klines?timeframe=1H` 能返回 completed 1H + 当前由 1m 拼出的 partial 1H。
10. 峰值 tick/sec 压测下 Flink 无持续 backpressure，Kafka consumer lag 可回落。

## 19. 开发里程碑

### Milestone 1：Collector 到 Kafka

- 新增 Kafka/Redpanda writer。
- Collector 只写 `ticks.raw`。
- 完成 Binance/OKX tick 标准化。
- 增加 collector 基础指标。

产出：

```text
ticks.raw topic 中稳定写入 normalized tick
```

### Milestone 2：Flink 1m Aggregator

- 实现 Flink job。
- event-time + watermark。
- 1m state 聚合。
- trade_id dedupe。
- Redis live 1m sink。
- DB final 1m upsert sink。
- `bars.1m.final` Kafka sink。

产出：

```text
1m final bars 稳定入库
Redis live 1m 实时更新
```

### Milestone 3：API 查询拼接

- Go API 改成查 DB final bars + Redis live 1m。
- 支持 `1m` 查询。
- 支持高级别查询时临时拼 partial bar。
- WebSocket 推送 partial bar snapshot。

产出：

```text
HTTP/WS 返回结果符合 1m 事实源模型
```

### Milestone 4：Rollup

- Flink job 消费 `bars.1m.final`。
- 生成 5m/15m/30m/1H/1D completed bars。
- 支持迟到 1m 修正导致高级别 upsert。

产出：

```text
高级别 final bars 来自 completed 1m
```

### Milestone 5：审计与修复

- 定时拉官方 REST 1m K 线。
- 与本地 bars_1m 比对。
- 记录 mismatch。
- 支持手动或自动修复。

产出：

```text
有外部基准验证 K 线准确性
```

## 20. 当前 Go 项目迁移建议

保留：

- `internal/exchange` 中 Binance/OKX 的 symbol 拉取和消息解析逻辑。
- Go HTTP API 框架。
- MySQL `bar_history` upsert 思路。
- Redis live 查询能力。

重写或下线：

- 当前 Go `aggregator.Engine` 多 timeframe tick 聚合。
- 当前每个 timeframe 的 livebar 写入。
- 当前 stream consumer 直接调用 Go 聚合器。
- 未闭环的 checkpoint 状态恢复。

新增：

- Kafka/Redpanda writer。
- Flink 1m aggregator job。
- Flink rollup job。
- API 查询拼接逻辑。

## 21. 推荐技术栈

基础版：

```text
Go Collector/API
Redpanda single node or 3-node cluster
Apache Flink standalone or Kubernetes
Redis
MySQL
Prometheus + Grafana
```

更标准生产版：

```text
Go Collector/API
Kafka 3+ brokers or Redpanda 3+ nodes
Flink on Kubernetes
Redis Sentinel/Cluster
MySQL partitioned tables or ClickHouse for history
S3/MinIO/HDFS for Flink checkpoints
Prometheus + Grafana + Alertmanager
```

## 22. 推荐第一版参数

```text
tick raw retention: 7 days
Kafka partitions ticks.raw: 24 or 48
replication factor: 3
watermark out-of-orderness: 2s
allowed lateness: 30s
live Redis emit interval: 200ms
Flink checkpoint interval: 10s
dedupe state TTL: 10m
Redis live key TTL: 10m
```

这些参数先用于上线压测，后续根据真实 `tick_recv_lag_ms` 和 `late_tick_count` 调整。

## 23. 最终结论

如果系统定位是行情基础设施，优先级应该是：

```text
准确性 > 可恢复 > 低延迟 > 扩展性 > 功能丰富度
```

推荐最终形态：

```text
Go 负责接入和查询
Kafka/Redpanda 负责可重放日志
Flink 负责 event-time 聚合和状态一致性
Redis 负责 live 1m
DB 负责 final bars
高级别 K 线全部基于 completed 1m 生成
```

这样主链路简单、边界清楚、可压测、可恢复，也符合“先把每一分钟 K 线做准”的核心要求。

## 24. 当前单机资源评估：69.5.22.0

实际机器信息：

```text
OS: Ubuntu 24.04 LTS
CPU: 4 vCPU
Memory: 7.8 GiB
Disk: 40 GB system disk, about 22 GB free
Docker: not installed
Java: OpenJDK 21 installed
Redis: redis-server 7.0.15 installed
MySQL client/server package exists
```

结论：

```text
这台机器不适合跑完整生产版 Kafka/Redpanda + Flink + Redis + MySQL。
```

主要瓶颈不是 CPU，而是：

- 内存只有 8GB，Flink、Kafka/Redpanda、MySQL、Redis 都是常驻服务，空间很紧。
- 磁盘只有 40GB，无法保存有意义的 raw tick retention。
- 没有 Docker，部署和隔离成本更高。
- 单机没有 Kafka/Flink HA，不能算生产高可用。

### 24.1 这台机器可以做什么

适合做：

```text
小规模功能验证
少量 symbol 的 1m K 线稳定性测试
Flink event-time 聚合逻辑验证
Go Collector/API 原型
Redis live 1m + MySQL final 1m 验证
```

不适合做：

```text
全量交易所 tick 接入
长时间 Kafka retention
多 exchange 大量 symbol 高峰流量
生产级 Flink/Kafka HA
大规模历史查询
```

### 24.2 单机精简版部署建议

如果必须只用这台机器，可以跑一个“单机精简版”：

```text
Go Collector
Redpanda single node 或 Kafka single broker
Flink standalone mini cluster
Redis
MySQL
Go API
```

建议资源上限：

```text
symbols: 20-50 个活跃交易对
avg tick/sec: 200-500
peak tick/sec: 1000-2000
raw tick retention: 1-6 小时
watermark delay: 2s
allowed lateness: 30s
```

建议内存分配：

```text
OS: 1.0 GB
Flink JobManager: 512 MB - 1 GB
Flink TaskManager: 2 GB - 3 GB
Redpanda/Kafka: 1 GB - 2 GB
MySQL: 1 GB - 1.5 GB
Redis: 256 MB - 512 MB
Go services: 256 MB - 512 MB
```

这已经比较紧，不能开太大的 Kafka retention，也不能接太多 symbol。

### 24.3 更现实的单机方案

如果目标是先把“1m K 线准确、不漏、低延迟”做出来，而不是验证 Flink 运维，当前机器更适合：

```text
Go Collector
Redis Streams
Go 1m event-time aggregator
Redis durable rolling 1m state
MySQL final 1m bars
Go API
```

这个方案没有 Flink 完整的 checkpoint/watermark 生态，但资源占用低很多，也可以把核心语义做对：

- tick 写 Redis Stream。
- consumer group 消费。
- 用 `tick.ts_ms` 分 1m 窗口。
- Redis 保存 open window rolling state。
- MySQL 幂等 upsert final 1m。
- 高级别后续从 completed 1m rollup。

适合当前 4C8G40G 单机先落地。

### 24.4 推荐升级规格

如果要上生产版 Flink/Kafka 架构，最低建议：

```text
单机低配生产验证:
8 vCPU
32 GB RAM
500 GB NVMe
```

更推荐：

```text
3 台 broker/compute 节点
每台 8 vCPU / 32 GB RAM / 1 TB NVMe
```

这样 Kafka/Redpanda retention、Flink checkpoint、MySQL/ClickHouse、Redis 才有比较健康的空间。
