import type { Bar, Tick } from "./types";

export type Direction = "positive" | "negative" | "flat";

export type BarMetrics = {
  range: number;
  body: number;
  change: number;
  changePct: number;
  direction: Direction;
  signal: string;
};

export function metricsForBar(bar: Bar | null | undefined): BarMetrics | null {
  if (!bar) return null;
  const open = Number(bar.open_price);
  const high = Number(bar.high_price);
  const low = Number(bar.low_price);
  const close = Number(bar.close_price);
  const change = close - open;
  const changePct = open === 0 ? 0 : (change / open) * 100;
  const direction = change > 0 ? "positive" : change < 0 ? "negative" : "flat";
  return {
    range: Math.max(0, high - low),
    body: Math.abs(change),
    change,
    changePct,
    direction,
    signal: signalForBar(open, high, low, close),
  };
}

export function tickLatencyMs(tick: Tick | null | undefined, now = Date.now()) {
  if (!tick) return null;
  const anchor = tick.recv_ms || tick.ts_ms;
  return latencyFromTimestamp(anchor, now);
}

export function latencyFromTimestamp(value: number | null | undefined, now = Date.now()) {
  if (!value) return null;
  return Math.max(0, now - Number(value));
}

export function quoteAmountForBar(bar: Bar | null | undefined) {
  if (!bar) return null;
  const quoteVolume = Number(bar.quote_volume);
  if (Number.isFinite(quoteVolume) && quoteVolume > 0) return quoteVolume;
  const close = Number(bar.close_price);
  const volume = Number(bar.volume);
  if (!Number.isFinite(close) || !Number.isFinite(volume)) return null;
  return close * volume;
}

export function formatPrice(value: number | null | undefined) {
  if (value === null || value === undefined) return "-";
  const parsed = Number(value);
  if (!Number.isFinite(parsed)) return "-";
  return parsed.toLocaleString("en-US", {
    minimumFractionDigits: parsed >= 100 ? 2 : parsed >= 1 ? 4 : 6,
    maximumFractionDigits: parsed >= 100 ? 2 : parsed >= 1 ? 4 : 8,
  });
}

export function formatQty(value: number | null | undefined, digits = 4) {
  if (value === null || value === undefined) return "-";
  const parsed = Number(value);
  if (!Number.isFinite(parsed)) return "-";
  return parsed.toLocaleString("en-US", {
    maximumFractionDigits: digits,
  });
}

export function formatCompact(value: number | null | undefined) {
  if (value === null || value === undefined) return "-";
  const parsed = Number(value);
  if (!Number.isFinite(parsed)) return "-";
  return new Intl.NumberFormat("en-US", {
    notation: "compact",
    maximumFractionDigits: 2,
  }).format(parsed);
}

export function formatPct(value: number | null | undefined) {
  if (value === null || value === undefined || !Number.isFinite(Number(value))) return "-";
  const parsed = Number(value);
  return `${parsed >= 0 ? "+" : ""}${parsed.toFixed(2)}%`;
}

export function formatSignedPrice(value: number | null | undefined) {
  if (value === null || value === undefined || !Number.isFinite(Number(value))) return "-";
  const parsed = Number(value);
  return `${parsed >= 0 ? "+" : "-"}${formatPrice(Math.abs(parsed))}`;
}

export function formatLatency(value: number | null | undefined) {
  if (value === null || value === undefined || !Number.isFinite(Number(value))) return "-";
  const parsed = Number(value);
  if (parsed > 60_000) return "stale";
  return `${formatQty(parsed, 0)}ms`;
}

export function formatTime(ms: number | null | undefined) {
  if (!ms) return "-";
  return new Intl.DateTimeFormat("zh-CN", {
    hour: "2-digit",
    minute: "2-digit",
    second: "2-digit",
    hour12: false,
  }).format(new Date(ms));
}

export function formatTimeRange(bar: Bar | null | undefined) {
  if (!bar) return "-";
  return `${formatTime(bar.start_ms)} - ${formatTime(bar.end_ms)}`;
}

function signalForBar(open: number, high: number, low: number, close: number) {
  const range = Math.max(0, high - low);
  const body = Math.abs(close - open);
  if (range === 0) return "flat";
  const bodyRatio = body / range;
  if (bodyRatio < 0.18) return "doji";
  if (close > open && bodyRatio > 0.62) return "impulse up";
  if (close < open && bodyRatio > 0.62) return "impulse down";
  return close >= open ? "bid control" : "ask control";
}
