import { useEffect, useMemo, useState, type CSSProperties, type ReactNode } from "react";
import { QueryClient, QueryClientProvider, useQuery } from "@tanstack/react-query";
import {
  Activity,
  Bell,
  Brush,
  Crosshair,
  Lock,
  Magnet,
  Minus,
  PencilRuler,
  Ruler,
  Search,
  TrendingUp,
  Type,
  Wifi,
} from "lucide-react";
import { ChartPanel } from "./ChartPanel";
import { getAllSymbols, getKlines, getLatestTicker } from "./api";
import {
  formatCompact,
  formatLatency,
  formatPct,
  formatPrice,
  formatQty,
  formatSignedPrice,
  formatTime,
  formatTimeRange,
  latencyFromTimestamp,
  metricsForBar,
  quoteAmountForBar,
} from "./marketMetrics";
import { useMarketSocket } from "./useMarketSocket";
import { useMarketStore } from "./store";
import type { Bar, SymbolInfo, Tick } from "./types";
import "./styles.css";

const queryClient = new QueryClient();
const timeframes = ["1m", "5m", "15m", "30m", "1H", "4H", "1D"];
const exchanges = ["binance", "okx"];
const modes = ["Candles", "Depth", "Replay"];
const tools = [
  ["crosshair", "Crosshair", <Crosshair size={16} />],
  ["trend", "Trend line", <TrendingUp size={16} />],
  ["horizontal", "Horizontal line", <Minus size={16} />],
  ["brush", "Brush", <Brush size={16} />],
  ["text", "Text", <Type size={16} />],
  ["measure", "Measure", <Ruler size={16} />],
  ["fib", "Fib retracement", <PencilRuler size={16} />],
  ["magnet", "Magnet", <Magnet size={16} />],
  ["lock", "Lock", <Lock size={16} />],
] as const;

type Indicators = {
  ema: boolean;
  volume: boolean;
  momentum: boolean;
};

export default function App() {
  return (
    <QueryClientProvider client={queryClient}>
      <MarketTerminal />
    </QueryClientProvider>
  );
}

function MarketTerminal() {
  const exchange = useMarketStore((state) => state.exchange);
  const symbol = useMarketStore((state) => state.symbol);
  const timeframe = useMarketStore((state) => state.timeframe);
  const latestTick = useMarketStore((state) => state.latestTick);
  const bars = useMarketStore((state) => state.bars);
  const connection = useMarketStore((state) => state.connection);
  const setExchange = useMarketStore((state) => state.setExchange);
  const setSymbol = useMarketStore((state) => state.setSymbol);
  const setTimeframe = useMarketStore((state) => state.setTimeframe);
  const setLatestTick = useMarketStore((state) => state.setLatestTick);
  const setBars = useMarketStore((state) => state.setBars);
  const [mode, setMode] = useState("Candles");
  const [activeTool, setActiveTool] = useState("crosshair");
  const [symbolQuery, setSymbolQuery] = useState("");
  const [selectedBar, setSelectedBar] = useState<Bar | null>(null);
  const [recentTicks, setRecentTicks] = useState<Tick[]>([]);
  const [indicators, setIndicators] = useState<Indicators>({ ema: true, volume: true, momentum: true });
  const nowMS = useNow();

  const symbolsQuery = useQuery({ queryKey: ["symbols", "all"], queryFn: () => getAllSymbols(exchanges), staleTime: 30_000 });
  const barsQuery = useQuery({
    queryKey: ["klines", exchange, symbol, timeframe],
    queryFn: () => getKlines(exchange, symbol, timeframe),
    refetchInterval: 30_000,
  });
  const tickerQuery = useQuery({
    queryKey: ["ticker", exchange, symbol],
    queryFn: () => getLatestTicker(exchange, symbol),
    refetchInterval: 5_000,
  });

  useEffect(() => {
    if (barsQuery.data) setBars(barsQuery.data);
  }, [barsQuery.data, setBars]);

  useEffect(() => {
    if (tickerQuery.data) setLatestTick(tickerQuery.data);
  }, [tickerQuery.data, setLatestTick]);

  useEffect(() => {
    setSelectedBar(null);
    setRecentTicks([]);
  }, [exchange, symbol, timeframe]);

  useEffect(() => {
    if (!latestTick) return;
    setRecentTicks((items) => {
      const key = tickKey(latestTick);
      if (items.some((item) => tickKey(item) === key)) return items;
      return [latestTick, ...items].slice(0, 9);
    });
  }, [latestTick]);

  useMarketSocket();

  const lastBar = bars[bars.length - 1] ?? null;
  const activeBar = selectedBar ?? lastBar;
  const activeMetrics = metricsForBar(activeBar);
  const lastMetrics = metricsForBar(lastBar);
  const symbols = symbolsQuery.data ?? [];
  const filteredSymbols = useMemo(() => filterSymbols(symbols, symbolQuery), [symbols, symbolQuery]);
  const watchlist = useMemo(() => makeWatchlist(symbols, filteredSymbols, exchange, symbol), [exchange, filteredSymbols, symbol, symbols]);
  const latestPrice = latestTick?.price ?? lastBar?.close_price ?? null;
  const officialPushAge = latencyFromTimestamp(latestTick?.recv_ms ?? lastBar?.updated_at_ms, nowMS);
  const exchangeEventAge = latencyFromTimestamp(latestTick?.ts_ms ?? lastBar?.last_tick_ms, nowMS);
  const browserReceiveAge = latencyFromTimestamp(latestTick?.client_recv_ms ?? lastBar?.client_recv_ms, nowMS);
  const selectMarketSymbol = (item: SymbolInfo) => {
    const nextExchange = item.exchange.toLowerCase();
    if (nextExchange !== exchange) setExchange(nextExchange);
    setSymbol(item.symbol);
    setSymbolQuery("");
  };

  const changeLabel = lastMetrics ? `${formatPct(lastMetrics.changePct)} ${formatSignedPrice(lastMetrics.change)}` : "waiting for live bar";
  const subtitle = lastBar ? `${lastBar.is_final ? "final" : "live"} bar | ${formatTime(lastBar.start_ms)} | ${bars.length} rows` : "waiting for live cache";

  return (
    <main className="terminal">
      <header className="topbar">
        <div className="symbol-strip">
          <div className="symbol-card">
            <strong>{symbolDisplay(symbol)}</strong>
            <span>{exchange.toUpperCase()}</span>
          </div>
          <div className="price-stack">
            <strong className={`last-price ${lastMetrics?.direction ?? "flat"}`}>{formatPrice(latestPrice)}</strong>
            <span className={lastMetrics?.direction ?? "flat"}>{changeLabel}</span>
          </div>
        </div>

        <div className="market-switchboard">
          <SegmentedControl values={exchanges} active={exchange} onSelect={(item) => setExchange(item)} labelFormatter={(item) => item.toUpperCase()} />
          <SegmentedControl values={timeframes} active={timeframe} onSelect={setTimeframe} />
          <button className={`live-button ${connection === "open" ? "active" : ""}`} type="button">
            LIVE
          </button>
          <SegmentedControl values={modes} active={mode} onSelect={setMode} />
        </div>

        <div className="top-actions">
          <label className="terminal-search">
            <Search size={15} />
            <input value={symbolQuery} onChange={(event) => setSymbolQuery(event.target.value)} placeholder="Search all symbols" />
          </label>
          <IconButton active={indicators.ema} label="Indicators" onClick={() => setIndicators((value) => ({ ...value, ema: !value.ema }))}>
            <Activity size={16} />
          </IconButton>
          <IconButton label="Alerts">
            <Bell size={16} />
          </IconButton>
          <div className={`connection ${connection}`}>
            <Wifi size={15} />
            <span>{connection}</span>
          </div>
        </div>
      </header>

      <section className="terminal-workspace">
        <nav className="drawbar" aria-label="Drawing tools">
          {tools.map(([id, label, icon]) => (
            <button className={id === activeTool ? "active" : ""} key={id} title={label} type="button" onClick={() => setActiveTool(id)}>
              {icon}
            </button>
          ))}
        </nav>

        <section className="chart-column">
          <ChartReadout bar={activeBar} metrics={activeMetrics} />
          <ChartPanel
            bars={bars}
            resetKey={`${exchange}:${symbol}:${timeframe}`}
            onSelectBar={setSelectedBar}
            showMovingAverages={indicators.ema}
            showVolume={indicators.volume}
          />
          {indicators.momentum ? <MomentumStrip bars={bars} /> : <div className="indicator-strip empty" />}
        </section>

        <aside className="sidebar">
          <Panel title="Watchlist" meta={`${filteredSymbols.length}/${symbols.length} all`}>
            <Watchlist items={watchlist} activeExchange={exchange} activeSymbol={symbol} latestTick={latestTick} onSelect={selectMarketSymbol} />
          </Panel>
          <Panel title="Indicators" meta="active set">
            <IndicatorToggle label="EMA 20 / 50" active={indicators.ema} onClick={() => setIndicators((value) => ({ ...value, ema: !value.ema }))} />
            <IndicatorToggle label="Volume" active={indicators.volume} onClick={() => setIndicators((value) => ({ ...value, volume: !value.volume }))} />
            <IndicatorToggle label="Momentum" active={indicators.momentum} onClick={() => setIndicators((value) => ({ ...value, momentum: !value.momentum }))} />
          </Panel>
          <Panel title="Depth Feed" meta={mode === "Depth" ? "selected" : "standby"}>
            <MetricRows
              rows={[
                ["Best bid", "-"],
                ["Best ask", "-"],
                ["Spread", "-"],
                ["Status", "not wired"],
              ]}
            />
          </Panel>
          <Panel title="System Metrics" meta="live path">
            <MetricRows
              rows={[
                ["Exchange", exchange.toUpperCase()],
                ["Browser WS", connection],
                ["Latest source", latestTick?.source || lastBar?.source || "stream"],
                ["Rows", String(bars.length)],
                ["Official push age", formatLatency(officialPushAge)],
                ["Exchange event age", formatLatency(exchangeEventAge)],
                ["Browser recv age", formatLatency(browserReceiveAge)],
              ]}
            />
          </Panel>
        </aside>
      </section>

      <BottomDock activeBar={activeBar} latestBar={lastBar} metrics={activeMetrics} latestTick={latestTick} nowMS={nowMS} recentTicks={recentTicks} subtitle={subtitle} />
    </main>
  );
}

function SegmentedControl<T extends string>({
  values,
  active,
  onSelect,
  labelFormatter = (value: T) => value,
}: {
  values: readonly T[];
  active: string;
  onSelect: (value: T) => void;
  labelFormatter?: (value: T) => string;
}) {
  return (
    <div className="segmented">
      {values.map((item) => (
        <button className={item === active ? "active" : ""} key={item} type="button" onClick={() => onSelect(item)}>
          {labelFormatter(item)}
        </button>
      ))}
    </div>
  );
}

function IconButton({ active, label, children, onClick }: { active?: boolean; label: string; children: ReactNode; onClick?: () => void }) {
  return (
    <button className={active ? "icon-action active" : "icon-action"} title={label} type="button" onClick={onClick}>
      {children}
    </button>
  );
}

function ChartReadout({ bar, metrics }: { bar: Bar | null; metrics: ReturnType<typeof metricsForBar> }) {
  return (
    <div className="chart-head">
      <div className="ohlc">
        <span>
          O <strong>{formatPrice(bar?.open_price)}</strong>
        </span>
        <span>
          H <strong>{formatPrice(bar?.high_price)}</strong>
        </span>
        <span>
          L <strong>{formatPrice(bar?.low_price)}</strong>
        </span>
        <span>
          C <strong>{formatPrice(bar?.close_price)}</strong>
        </span>
        <span>
          V <strong>{formatCompact(bar?.volume)}</strong>
        </span>
        <span>
          Amt <strong>{formatCompact(quoteAmountForBar(bar))}</strong>
        </span>
        <span>
          T <strong>{formatCompact(bar?.trade_count)}</strong>
        </span>
        <span className={metrics?.direction ?? "flat"}>{metrics ? formatPct(metrics.changePct) : "-"}</span>
      </div>
      <div className="legend">
        <span className="legend-pill amber">EMA 20</span>
        <span className="legend-pill blue">EMA 50</span>
        <span className="legend-pill green">Volume</span>
      </div>
    </div>
  );
}

function MomentumStrip({ bars }: { bars: Bar[] }) {
  const items = bars.slice(-34).map((bar) => ({
    id: `${bar.start_ms}`,
    value: Number(bar.close_price) - Number(bar.open_price),
  }));
  const max = Math.max(1, ...items.map((item) => Math.abs(item.value)));
  return (
    <div className="indicator-strip">
      <div className="indicator-label">Momentum</div>
      <div className="mini-histogram">
        {items.map((item) => {
          const height = Math.max(6, (Math.abs(item.value) / max) * 100);
          const style: CSSProperties = { height: `${height}%` };
          return <span className={item.value >= 0 ? "positive" : "negative"} key={item.id} style={style} />;
        })}
      </div>
    </div>
  );
}

function Panel({ title, meta, children }: { title: string; meta?: string; children: ReactNode }) {
  return (
    <section className="terminal-panel">
      <h2>
        <span>{title}</span>
        {meta ? <small>{meta}</small> : null}
      </h2>
      {children}
    </section>
  );
}

function Watchlist({
  items,
  activeExchange,
  activeSymbol,
  latestTick,
  onSelect,
}: {
  items: SymbolInfo[];
  activeExchange: string;
  activeSymbol: string;
  latestTick: Tick | null;
  onSelect: (item: SymbolInfo) => void;
}) {
  if (!items.length) return <div className="empty-state">No symbols</div>;
  return (
    <div className="watchlist">
      {items.map((item) => {
        const itemExchange = item.exchange.toLowerCase();
        const isActive = itemExchange === activeExchange.toLowerCase() && item.symbol === activeSymbol;
        const hasLivePrice = isActive && latestTick && latestTick.exchange.toLowerCase() === itemExchange && latestTick.symbol === item.symbol;
        const livePrice = hasLivePrice && latestTick ? latestTick.price : null;
        return (
          <button className={isActive ? "watch-row active" : "watch-row"} key={`${item.exchange}:${item.symbol}`} type="button" onClick={() => onSelect(item)}>
            <span className={item.is_active ? "status-dot on" : "status-dot"} />
            <span className="watch-symbol">
              <strong>{symbolDisplay(item.symbol)}</strong>
              <small>{item.exchange.toUpperCase()}</small>
            </span>
            <strong>{livePrice !== null ? formatPrice(livePrice) : item.status || "-"}</strong>
          </button>
        );
      })}
    </div>
  );
}

function IndicatorToggle({ label, active, onClick }: { label: string; active: boolean; onClick: () => void }) {
  return (
    <button className="indicator-row" type="button" onClick={onClick}>
      <span>{label}</span>
      <span className={active ? "switch on" : "switch"} />
      <strong className={active ? "positive" : ""}>{active ? "On" : "Off"}</strong>
    </button>
  );
}

function MetricRows({ rows }: { rows: Array<[string, string]> }) {
  return (
    <div className="metric-list">
      {rows.map(([key, value]) => (
        <div className="metric-row" key={key}>
          <span>{key}</span>
          <strong>{value}</strong>
        </div>
      ))}
    </div>
  );
}

function BottomDock({
  activeBar,
  latestBar,
  metrics,
  latestTick,
  nowMS,
  recentTicks,
  subtitle,
}: {
  activeBar: Bar | null;
  latestBar: Bar | null;
  metrics: ReturnType<typeof metricsForBar>;
  latestTick: Tick | null;
  nowMS: number;
  recentTicks: Tick[];
  subtitle: string;
}) {
  const collectorLatency = latestTick?.recv_ms && latestTick.ts_ms ? Math.max(0, latestTick.recv_ms - latestTick.ts_ms) : null;
  const officialPushAge = latencyFromTimestamp(latestBar?.updated_at_ms ?? latestTick?.recv_ms, nowMS);
  const exchangeEventAge = latencyFromTimestamp(latestBar?.last_tick_ms ?? latestTick?.ts_ms, nowMS);
  const browserReceiveAge = latencyFromTimestamp(latestBar?.client_recv_ms ?? latestTick?.client_recv_ms, nowMS);
  return (
    <footer className="bottom-dock">
      <section className="dock-panel">
        <h2>
          Selected K-Line <small>{subtitle}</small>
        </h2>
        <div className="kline-stats">
          <StatBox label="Time" value={formatTimeRange(activeBar)} />
          <StatBox label="Range" value={metrics ? formatPrice(metrics.range) : "-"} />
          <StatBox label="Body" value={metrics ? formatPrice(metrics.body) : "-"} />
          <StatBox label="Signal" value={metrics?.signal ?? "-"} tone={metrics?.direction} />
        </div>
      </section>
      <section className="dock-panel">
        <h2>
          Realtime Route <small>end-to-end</small>
        </h2>
        <div className="route">
          <StatBox label="Official push" value={formatLatency(officialPushAge)} tone={latencyTone(officialPushAge)} />
          <StatBox label="Exchange event" value={formatLatency(exchangeEventAge)} tone={latencyTone(exchangeEventAge)} />
          <StatBox label="Browser recv" value={formatLatency(browserReceiveAge)} tone={latencyTone(browserReceiveAge)} />
          <StatBox label="Collector span" value={formatLatency(collectorLatency)} tone={latencyTone(collectorLatency)} />
        </div>
      </section>
      <section className="dock-panel">
        <h2>
          Tape <small>latest trades</small>
        </h2>
        <div className="tape">
          {recentTicks.length ? (
            recentTicks.slice(0, 4).map((tick) => (
              <div className="trade-row" key={tickKey(tick)}>
                <span>{formatTime(tick.ts_ms)}</span>
                <strong className={tick.side === "sell" ? "negative" : "positive"}>{tick.side || "tick"}</strong>
                <span>{formatPrice(tick.price)}</span>
                <span>{formatQty(tick.size, 5)}</span>
              </div>
            ))
          ) : (
            <div className="empty-state">No ticks</div>
          )}
        </div>
      </section>
    </footer>
  );
}

function StatBox({ label, value, tone }: { label: string; value: string; tone?: string }) {
  return (
    <div className="stat-box">
      <span>{label}</span>
      <strong className={tone ?? ""}>{value}</strong>
    </div>
  );
}

function filterSymbols(symbols: SymbolInfo[], query: string) {
  const trimmed = query.trim().toLowerCase();
  if (!trimmed) return symbols;
  return symbols.filter(
    (item) =>
      item.symbol.toLowerCase().includes(trimmed) ||
      item.exchange.toLowerCase().includes(trimmed) ||
      item.market_type.toLowerCase().includes(trimmed) ||
      item.status.toLowerCase().includes(trimmed),
  );
}

function makeWatchlist(symbols: SymbolInfo[], filteredSymbols: SymbolInfo[], activeExchange: string, activeSymbol: string) {
  const active = symbols.find((item) => item.exchange.toLowerCase() === activeExchange.toLowerCase() && item.symbol === activeSymbol);
  const merged = active
    ? [active, ...filteredSymbols.filter((item) => item.exchange.toLowerCase() !== active.exchange.toLowerCase() || item.symbol !== active.symbol)]
    : filteredSymbols;
  return merged.slice(0, 18);
}

function symbolDisplay(value: string) {
  if (value.endsWith("USDT") && !value.includes("-")) return `${value}.P`;
  return value;
}

function tickKey(tick: Tick) {
  return tick.trade_id || `${tick.exchange}:${tick.symbol}:${tick.ts_ms}:${tick.price}:${tick.size}`;
}

function useNow(intervalMS = 1_000) {
  const [now, setNow] = useState(Date.now());
  useEffect(() => {
    const timer = window.setInterval(() => setNow(Date.now()), intervalMS);
    return () => window.clearInterval(timer);
  }, [intervalMS]);
  return now;
}

function latencyTone(value: number | null | undefined) {
  if (value === null || value === undefined || value > 60_000) return "flat";
  return "positive";
}
