import { useEffect, useRef, useState } from "react";
import {
  CandlestickSeries,
  ColorType,
  HistogramSeries,
  LineSeries,
  createChart,
  type CandlestickData,
  type HistogramData,
  type IChartApi,
  type ISeriesApi,
  type LineData,
  type MouseEventParams,
  type Time,
} from "lightweight-charts";
import { formatCompact, formatPct, formatPrice, formatQty, formatTimeRange, metricsForBar, quoteAmountForBar } from "./marketMetrics";
import type { Bar } from "./types";

type Props = {
  bars: Bar[];
  resetKey: string;
  onSelectBar?: (bar: Bar | null) => void;
  showMovingAverages?: boolean;
  showVolume?: boolean;
};

type HoverTip = {
  bar: Bar;
  x: number;
  y: number;
};

export function ChartPanel({ bars, resetKey, onSelectBar, showMovingAverages = true, showVolume = true }: Props) {
  const rootRef = useRef<HTMLDivElement | null>(null);
  const chartRef = useRef<IChartApi | null>(null);
  const candleRef = useRef<ISeriesApi<"Candlestick"> | null>(null);
  const volumeRef = useRef<ISeriesApi<"Histogram"> | null>(null);
  const ema20Ref = useRef<ISeriesApi<"Line"> | null>(null);
  const ema50Ref = useRef<ISeriesApi<"Line"> | null>(null);
  const barsByTimeRef = useRef<Map<number, Bar>>(new Map());
  const onSelectBarRef = useRef<Props["onSelectBar"]>(onSelectBar);
  const lastFitKeyRef = useRef("");
  const [hoverTip, setHoverTip] = useState<HoverTip | null>(null);

  useEffect(() => {
    onSelectBarRef.current = onSelectBar;
  }, [onSelectBar]);

  useEffect(() => {
    if (!rootRef.current) return;
    const chart = createChart(rootRef.current, {
      layout: {
        background: { type: ColorType.Solid, color: "#0f1216" },
        textColor: "#8f98a7",
        fontFamily: '"IBM Plex Sans", Aptos, ui-sans-serif, system-ui',
      },
      grid: {
        vertLines: { color: "rgba(41,48,58,0.68)" },
        horzLines: { color: "rgba(41,48,58,0.72)" },
      },
      rightPriceScale: {
        borderColor: "#29303a",
        scaleMargins: { top: 0.08, bottom: 0.22 },
      },
      timeScale: {
        borderColor: "#29303a",
        timeVisible: true,
        secondsVisible: false,
        rightOffset: 8,
        barSpacing: 10,
      },
      crosshair: {
        mode: 1,
        vertLine: { color: "rgba(230,232,235,0.42)", style: 3 },
        horzLine: { color: "rgba(230,232,235,0.42)", style: 3 },
      },
    });

    const candle = chart.addSeries(CandlestickSeries, {
      upColor: "#1ecb82",
      downColor: "#ff5f6b",
      wickUpColor: "#1ecb82",
      wickDownColor: "#ff5f6b",
      borderVisible: false,
      priceLineColor: "#1ecb82",
    });
    const ema20 = chart.addSeries(LineSeries, {
      color: "#f3b64b",
      lineWidth: 2,
      priceLineVisible: false,
      lastValueVisible: false,
    });
    const ema50 = chart.addSeries(LineSeries, {
      color: "#4aa3ff",
      lineWidth: 2,
      priceLineVisible: false,
      lastValueVisible: false,
    });
    const volume = chart.addSeries(HistogramSeries, {
      priceScaleId: "",
      priceFormat: { type: "volume" },
      color: "rgba(30, 203, 130, 0.36)",
      lastValueVisible: false,
      priceLineVisible: false,
    });
    volume.priceScale().applyOptions({ scaleMargins: { top: 0.82, bottom: 0 } });

    const handleCrosshairMove = (param: MouseEventParams<Time>) => {
      const time = timeToEpochSeconds(param.time);
      const bar = time ? barsByTimeRef.current.get(time) ?? null : null;
      if (!bar || !param.point) {
        onSelectBarRef.current?.(null);
        setHoverTip(null);
        return;
      }
      onSelectBarRef.current?.(bar);
      setHoverTip(positionTooltip(bar, param.point, rootRef.current));
    };
    chart.subscribeCrosshairMove(handleCrosshairMove);

    const resize = () => chart.applyOptions({ width: rootRef.current?.clientWidth ?? 960, height: rootRef.current?.clientHeight ?? 560 });
    const observer = new ResizeObserver(resize);
    observer.observe(rootRef.current);
    resize();
    chartRef.current = chart;
    candleRef.current = candle;
    volumeRef.current = volume;
    ema20Ref.current = ema20;
    ema50Ref.current = ema50;
    return () => {
      observer.disconnect();
      chart.unsubscribeCrosshairMove(handleCrosshairMove);
      chart.remove();
      chartRef.current = null;
      candleRef.current = null;
      volumeRef.current = null;
      ema20Ref.current = null;
      ema50Ref.current = null;
    };
  }, []);

  useEffect(() => {
    barsByTimeRef.current = new Map(bars.map((bar) => [Math.floor(bar.start_ms / 1000), bar]));
    const candles: CandlestickData[] = bars.map((bar) => ({
      time: Math.floor(bar.start_ms / 1000) as CandlestickData["time"],
      open: Number(bar.open_price),
      high: Number(bar.high_price),
      low: Number(bar.low_price),
      close: Number(bar.close_price),
    }));
    const volumes: HistogramData[] = bars.map((bar) => ({
      time: Math.floor(bar.start_ms / 1000) as HistogramData["time"],
      value: Number(bar.volume || 0),
      color: Number(bar.close_price) >= Number(bar.open_price) ? "rgba(30,203,130,0.46)" : "rgba(255,95,107,0.46)",
    }));
    const closes = bars.map((bar) => Number(bar.close_price));

    candleRef.current?.setData(candles);
    volumeRef.current?.setData(showVolume ? volumes : []);
    ema20Ref.current?.setData(showMovingAverages ? movingAverage(closes, bars, 20) : []);
    ema50Ref.current?.setData(showMovingAverages ? movingAverage(closes, bars, 50) : []);
    if (candles.length && lastFitKeyRef.current !== resetKey) {
      chartRef.current?.timeScale().fitContent();
      lastFitKeyRef.current = resetKey;
    }
  }, [bars, resetKey, showMovingAverages, showVolume]);

  return (
    <div className="chart-shell">
      <div className="chart-panel" ref={rootRef} />
      {hoverTip ? <KlineTooltip tip={hoverTip} /> : null}
    </div>
  );
}

function KlineTooltip({ tip }: { tip: HoverTip }) {
  const metrics = metricsForBar(tip.bar);
  return (
    <div className="kline-tooltip" style={{ left: `${tip.x}px`, top: `${tip.y}px` }}>
      <header>
        <span>{formatTimeRange(tip.bar)}</span>
        <strong className={metrics?.direction ?? "flat"}>{metrics ? formatPct(metrics.changePct) : "-"}</strong>
      </header>
      <div className="tooltip-grid">
        <span>Open</span>
        <strong>{formatPrice(tip.bar.open_price)}</strong>
        <span>High</span>
        <strong>{formatPrice(tip.bar.high_price)}</strong>
        <span>Low</span>
        <strong>{formatPrice(tip.bar.low_price)}</strong>
        <span>Close</span>
        <strong>{formatPrice(tip.bar.close_price)}</strong>
        <span>Volume</span>
        <strong>{formatQty(tip.bar.volume, 6)}</strong>
        <span>Amount</span>
        <strong>{formatCompact(quoteAmountForBar(tip.bar))}</strong>
        <span>Trades</span>
        <strong>{formatCompact(tip.bar.trade_count)}</strong>
      </div>
    </div>
  );
}

function movingAverage(values: number[], bars: Bar[], windowSize: number): LineData[] {
  return values.map((_, index) => {
    const start = Math.max(0, index - windowSize + 1);
    const slice = values.slice(start, index + 1);
    const value = slice.reduce((sum, item) => sum + item, 0) / slice.length;
    return {
      time: Math.floor(bars[index].start_ms / 1000) as LineData["time"],
      value,
    };
  });
}

function timeToEpochSeconds(time: Time | undefined) {
  if (!time) return null;
  if (typeof time === "number") return Number(time);
  if (typeof time === "string") {
    const parsed = Date.parse(time);
    return Number.isFinite(parsed) ? Math.floor(parsed / 1000) : null;
  }
  return Date.UTC(time.year, time.month - 1, time.day) / 1000;
}

function positionTooltip(bar: Bar, point: { x: number; y: number }, root: HTMLDivElement | null): HoverTip {
  const width = 238;
  const height = 210;
  const padding = 12;
  const rootWidth = root?.clientWidth ?? 960;
  const rootHeight = root?.clientHeight ?? 560;
  const rightSideX = point.x + padding;
  const leftSideX = point.x - width - padding;
  const lowerY = point.y + padding;
  const upperY = point.y - height - padding;
  const x = rightSideX + width > rootWidth ? leftSideX : rightSideX;
  const y = lowerY + height > rootHeight ? upperY : lowerY;
  return {
    bar,
    x: Math.max(padding, Math.min(x, rootWidth - width - padding)),
    y: Math.max(padding, Math.min(y, rootHeight - height - padding)),
  };
}
