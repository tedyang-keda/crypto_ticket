import { useEffect, useRef } from "react";
import {
  CandlestickSeries,
  ColorType,
  HistogramSeries,
  createChart,
  type CandlestickData,
  type HistogramData,
  type IChartApi,
  type ISeriesApi,
} from "lightweight-charts";
import type { Bar } from "./types";

type Props = {
  bars: Bar[];
};

export function ChartPanel({ bars }: Props) {
  const rootRef = useRef<HTMLDivElement | null>(null);
  const chartRef = useRef<IChartApi | null>(null);
  const candleRef = useRef<ISeriesApi<"Candlestick"> | null>(null);
  const volumeRef = useRef<ISeriesApi<"Histogram"> | null>(null);

  useEffect(() => {
    if (!rootRef.current) return;
    const chart = createChart(rootRef.current, {
      layout: {
        background: { type: ColorType.Solid, color: "#0a0f14" },
        textColor: "#b9c5d1",
        fontFamily: "Aptos, ui-sans-serif, system-ui",
      },
      grid: {
        vertLines: { color: "rgba(255,255,255,0.045)" },
        horzLines: { color: "rgba(255,255,255,0.055)" },
      },
      rightPriceScale: { borderColor: "#25313d" },
      timeScale: { borderColor: "#25313d", timeVisible: true, secondsVisible: false },
      crosshair: { mode: 1 },
    });
    const candle = chart.addSeries(CandlestickSeries, {
      upColor: "#41d69a",
      downColor: "#ff5f6d",
      wickUpColor: "#41d69a",
      wickDownColor: "#ff5f6d",
      borderVisible: false,
    });
    const volume = chart.addSeries(HistogramSeries, {
      priceScaleId: "",
      priceFormat: { type: "volume" },
      color: "rgba(98, 151, 255, 0.45)",
    });
    volume.priceScale().applyOptions({ scaleMargins: { top: 0.82, bottom: 0 } });
    const resize = () => chart.applyOptions({ width: rootRef.current?.clientWidth ?? 800, height: rootRef.current?.clientHeight ?? 480 });
    const observer = new ResizeObserver(resize);
    observer.observe(rootRef.current);
    resize();
    chartRef.current = chart;
    candleRef.current = candle;
    volumeRef.current = volume;
    return () => {
      observer.disconnect();
      chart.remove();
      chartRef.current = null;
    };
  }, []);

  useEffect(() => {
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
      color: bar.close_price >= bar.open_price ? "rgba(65,214,154,0.42)" : "rgba(255,95,109,0.42)",
    }));
    candleRef.current?.setData(candles);
    volumeRef.current?.setData(volumes);
    chartRef.current?.timeScale().fitContent();
  }, [bars]);

  return <div className="chart-panel" ref={rootRef} />;
}
