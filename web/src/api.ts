import type { Bar, PriceMode, SymbolInfo, Tick } from "./types";

const json = async <T>(url: string): Promise<T> => {
  const response = await fetch(url, { headers: { Accept: "application/json" } });
  if (!response.ok) {
    throw new Error(await response.text());
  }
  return response.json() as Promise<T>;
};

export const getSymbols = async (exchange: string): Promise<SymbolInfo[]> => {
  const payload = await json<{ symbols: SymbolInfo[] }>(`/api/v1/symbols?exchange=${encodeURIComponent(exchange)}&active=true`);
  return payload.symbols ?? [];
};

export const getAllSymbols = async (exchanges: string[]): Promise<SymbolInfo[]> => {
  const rows = await Promise.all(exchanges.map((exchange) => getSymbols(exchange)));
  return rows.flat().sort((a, b) => `${a.exchange}:${a.symbol}`.localeCompare(`${b.exchange}:${b.symbol}`));
};

export const getLatestTicker = async (exchange: string, symbol: string, priceMode: PriceMode = "raw"): Promise<Tick | null> => {
  try {
    const tick = await json<Tick>(
      `/api/v1/ticker/latest?exchange=${encodeURIComponent(exchange)}&symbol=${encodeURIComponent(symbol)}&price_mode=${encodeURIComponent(priceMode)}`
    );
    return { ...tick, client_recv_ms: Date.now() };
  } catch {
    return null;
  }
};

export const getKlines = async (exchange: string, symbol: string, timeframe: string, priceMode: PriceMode = "raw", limit = 300): Promise<Bar[]> => {
  const payload = await json<{ bars: Bar[] }>(
    `/api/v1/klines?exchange=${encodeURIComponent(exchange)}&symbol=${encodeURIComponent(symbol)}&timeframe=${encodeURIComponent(timeframe)}&limit=${limit}&include_live=true&price_mode=${encodeURIComponent(priceMode)}`
  );
  return payload.bars ?? [];
};
