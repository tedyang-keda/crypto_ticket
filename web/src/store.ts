import { create } from "zustand";
import type { Bar, Tick } from "./types";

type MarketState = {
  exchange: string;
  symbol: string;
  timeframe: string;
  latestTick: Tick | null;
  bars: Bar[];
  connection: "idle" | "connecting" | "open" | "closed";
  setExchange: (exchange: string) => void;
  setSymbol: (symbol: string) => void;
  setTimeframe: (timeframe: string) => void;
  setLatestTick: (tick: Tick | null) => void;
  setBars: (bars: Bar[]) => void;
  updateBar: (bar: Bar) => void;
  setConnection: (connection: MarketState["connection"]) => void;
};

export const useMarketStore = create<MarketState>((set) => ({
  exchange: "binance",
  symbol: "BTCUSDT",
  timeframe: "1m",
  latestTick: null,
  bars: [],
  connection: "idle",
  setExchange: (exchange) => set({ exchange, symbol: exchange === "okx" ? "BTC-USDT-SWAP" : "BTCUSDT" }),
  setSymbol: (symbol) => set({ symbol }),
  setTimeframe: (timeframe) => set({ timeframe }),
  setLatestTick: (latestTick) =>
    set((state) => {
      if (!latestTick) return { latestTick };
      const exchange = latestTick.exchange.toLowerCase();
      const symbol = latestTick.symbol.toUpperCase();
      if (exchange !== state.exchange.toLowerCase() || symbol !== state.symbol.toUpperCase()) return state;
      if (
        state.latestTick &&
        state.latestTick.exchange.toLowerCase() === exchange &&
        state.latestTick.symbol.toUpperCase() === symbol &&
        latestTick.ts_ms < state.latestTick.ts_ms
      ) {
        return state;
      }
      return { latestTick };
    }),
  setBars: (bars) => set({ bars }),
  updateBar: (bar) =>
    set((state) => {
      const bars = [...state.bars];
      const index = bars.findIndex((item) => item.start_ms === bar.start_ms);
      if (index >= 0) {
        bars[index] = bar;
      } else {
        bars.push(bar);
      }
      bars.sort((a, b) => a.start_ms - b.start_ms);
      return { bars: bars.slice(-301) };
    }),
  setConnection: (connection) => set({ connection }),
}));
