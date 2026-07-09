import { create } from "zustand";
import type { Bar, PriceMode, Tick } from "./types";

type MarketState = {
  exchange: string;
  symbol: string;
  timeframe: string;
  priceMode: PriceMode;
  latestTick: Tick | null;
  bars: Bar[];
  connection: "idle" | "connecting" | "open" | "closed";
  setExchange: (exchange: string) => void;
  setSymbol: (symbol: string) => void;
  setTimeframe: (timeframe: string) => void;
  setPriceMode: (priceMode: PriceMode) => void;
  setLatestTick: (tick: Tick | null) => void;
  setBars: (bars: Bar[]) => void;
  updateBar: (bar: Bar) => void;
  setConnection: (connection: MarketState["connection"]) => void;
};

export const useMarketStore = create<MarketState>((set) => ({
  exchange: "binance",
  symbol: "BTCUSDT",
  timeframe: "1m",
  priceMode: "raw",
  latestTick: null,
  bars: [],
  connection: "idle",
  setExchange: (exchange) => set({ exchange, symbol: exchange === "okx" ? "BTC-USDT-SWAP" : "BTCUSDT", latestTick: null, bars: [] }),
  setSymbol: (symbol) => set({ symbol, latestTick: null, bars: [] }),
  setTimeframe: (timeframe) => set({ timeframe, bars: [] }),
  setPriceMode: (priceMode) => set({ priceMode, latestTick: null, bars: [] }),
  setLatestTick: (latestTick) =>
    set((state) => {
      if (!latestTick) return { latestTick };
      if ((latestTick.price_mode || "raw") !== state.priceMode) return state;
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
      if (
        bar.exchange.toLowerCase() !== state.exchange.toLowerCase() ||
        bar.symbol.toUpperCase() !== state.symbol.toUpperCase() ||
        bar.timeframe !== state.timeframe ||
        (bar.price_mode || "raw") !== state.priceMode
      ) {
        return state;
      }
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
