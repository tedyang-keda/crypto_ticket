export type Tick = {
  exchange: string;
  symbol: string;
  ts_ms: number;
  price: number;
  size: number;
  side?: string;
  trade_id?: string;
  event_type?: string;
  source?: string;
  recv_ms?: number;
};

export type Bar = {
  exchange: string;
  symbol: string;
  timeframe: string;
  start_ms: number;
  end_ms: number;
  open_price: number;
  high_price: number;
  low_price: number;
  close_price: number;
  volume: number;
  quote_volume: number;
  trade_count: number;
  last_tick_ms: number;
  is_final: boolean;
  source?: string;
  reason?: string;
  updated_at_ms?: number;
};

export type SymbolInfo = {
  exchange: string;
  symbol: string;
  market_type: string;
  status: string;
  is_active: boolean;
  first_seen_at_ms?: number;
  last_seen_at_ms?: number;
};

export type RealtimeEvent = {
  type: "ticker" | "kline";
  seq: number;
  exchange: string;
  symbol: string;
  timeframe?: string;
  tick?: Tick;
  bar?: Bar;
};
