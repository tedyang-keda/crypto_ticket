import { useEffect, useMemo } from "react";
import { QueryClient, QueryClientProvider, useQuery } from "@tanstack/react-query";
import { Activity, Database, Radio, Search, Wifi } from "lucide-react";
import { ChartPanel } from "./ChartPanel";
import { getKlines, getLatestTicker, getSymbols } from "./api";
import { useMarketSocket } from "./useMarketSocket";
import { useMarketStore } from "./store";
import "./styles.css";

const queryClient = new QueryClient();
const timeframes = ["1m", "5m", "15m", "30m", "1H", "4H", "1D"];
const exchanges = ["binance", "okx"];

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

  const symbolsQuery = useQuery({ queryKey: ["symbols", exchange], queryFn: () => getSymbols(exchange) });
  const barsQuery = useQuery({ queryKey: ["klines", exchange, symbol, timeframe], queryFn: () => getKlines(exchange, symbol, timeframe), refetchInterval: 30_000 });
  const tickerQuery = useQuery({ queryKey: ["ticker", exchange, symbol], queryFn: () => getLatestTicker(exchange, symbol), refetchInterval: 5_000 });

  useEffect(() => {
    if (barsQuery.data) setBars(barsQuery.data);
  }, [barsQuery.data, setBars]);

  useEffect(() => {
    if (tickerQuery.data) setLatestTick(tickerQuery.data);
  }, [tickerQuery.data, setLatestTick]);

  useMarketSocket();

  const lastBar = bars[bars.length - 1];
  const symbols = symbolsQuery.data ?? [];
  const subtitle = useMemo(() => {
    if (!lastBar) return "waiting for live cache";
    return `${lastBar.is_final ? "final" : "live"} bar · ${formatTime(lastBar.start_ms)} · ${bars.length} rows`;
  }, [bars.length, lastBar]);

  return (
    <main className="terminal-shell">
      <header className="topbar">
        <div>
          <div className="eyebrow">Crypto Ticket Realtime</div>
          <h1>{exchange.toUpperCase()} / {symbol} / {timeframe}</h1>
        </div>
        <div className={`connection ${connection}`}>
          <Wifi size={16} />
          {connection}
        </div>
      </header>

      <section className="workspace">
        <aside className="sidebar">
          <div className="segmented">
            {exchanges.map((item) => (
              <button className={item === exchange ? "active" : ""} key={item} onClick={() => setExchange(item)}>
                {item.toUpperCase()}
              </button>
            ))}
          </div>
          <label className="search">
            <Search size={15} />
            <input placeholder="Filter symbols" />
          </label>
          <div className="symbol-list">
            {symbols.map((item) => (
              <button className={item.symbol === symbol ? "symbol active" : "symbol"} key={item.symbol} onClick={() => setSymbol(item.symbol)}>
                <span className="dot" />
                <span>
                  <strong>{item.symbol}</strong>
                  <small>{item.market_type || "market"} · {item.status || "live"}</small>
                </span>
              </button>
            ))}
          </div>
        </aside>

        <section className="main-panel">
          <div className="tool-row">
            <div className="timeframes">
              {timeframes.map((item) => (
                <button className={item === timeframe ? "active" : ""} key={item} onClick={() => setTimeframe(item)}>
                  {item}
                </button>
              ))}
            </div>
            <div className="hint">{subtitle}</div>
          </div>
          <ChartPanel bars={bars} />
        </section>

        <aside className="inspector">
          <InfoCard icon={<Radio size={17} />} title="Latest Tick" rows={[
            ["Price", latestTick ? price(latestTick.price) : "-"],
            ["Size", latestTick ? qty(latestTick.size) : "-"],
            ["Side", latestTick?.side || "-"],
            ["Time", latestTick ? formatTime(latestTick.ts_ms) : "-"],
          ]} />
          <InfoCard icon={<Activity size={17} />} title="Live Bar" rows={[
            ["Open", lastBar ? price(lastBar.open_price) : "-"],
            ["High", lastBar ? price(lastBar.high_price) : "-"],
            ["Low", lastBar ? price(lastBar.low_price) : "-"],
            ["Close", lastBar ? price(lastBar.close_price) : "-"],
            ["Volume", lastBar ? qty(lastBar.volume) : "-"],
            ["Trades", lastBar ? String(lastBar.trade_count) : "-"],
          ]} />
          <InfoCard icon={<Database size={17} />} title="Cache Path" rows={[
            ["HTTP", "Redis first"],
            ["History", "MySQL fallback"],
            ["Live", "WebSocket"],
            ["Limit", "300 bars"],
          ]} />
        </aside>
      </section>
    </main>
  );
}

function InfoCard({ icon, title, rows }: { icon: React.ReactNode; title: string; rows: Array<[string, string]> }) {
  return (
    <section className="info-card">
      <h2>{icon}{title}</h2>
      {rows.map(([key, value]) => (
        <div className="kv" key={key}>
          <span>{key}</span>
          <strong>{value}</strong>
        </div>
      ))}
    </section>
  );
}

function price(value: number) {
  return Number(value).toLocaleString("en-US", { maximumFractionDigits: value > 1 ? 4 : 8 });
}

function qty(value: number) {
  return Number(value).toLocaleString("en-US", { maximumFractionDigits: 6 });
}

function formatTime(ms: number) {
  return new Intl.DateTimeFormat("zh-CN", { hour: "2-digit", minute: "2-digit", second: "2-digit", hour12: false }).format(new Date(ms));
}
