import type { Bar, SymbolInfo, Tick } from "./types";

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

export const getLatestTicker = async (exchange: string, symbol: string): Promise<Tick | null> => {
  try {
    return await json<Tick>(`/api/v1/ticker/latest?exchange=${encodeURIComponent(exchange)}&symbol=${encodeURIComponent(symbol)}`);
  } catch {
    return null;
  }
};

export const getKlines = async (exchange: string, symbol: string, timeframe: string, limit = 300): Promise<Bar[]> => {
  const payload = await json<{ bars: Bar[] }>(
    `/api/v1/klines?exchange=${encodeURIComponent(exchange)}&symbol=${encodeURIComponent(symbol)}&timeframe=${encodeURIComponent(timeframe)}&limit=${limit}&include_live=true`
  );
  return payload.bars ?? [];
};
