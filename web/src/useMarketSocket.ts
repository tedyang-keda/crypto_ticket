import { useEffect } from "react";
import { useMarketStore } from "./store";
import type { RealtimeEvent } from "./types";

export function useMarketSocket() {
  const exchange = useMarketStore((state) => state.exchange);
  const symbol = useMarketStore((state) => state.symbol);
  const timeframe = useMarketStore((state) => state.timeframe);
  const priceMode = useMarketStore((state) => state.priceMode);
  const setLatestTick = useMarketStore((state) => state.setLatestTick);
  const updateBar = useMarketStore((state) => state.updateBar);
  const setConnection = useMarketStore((state) => state.setConnection);

  useEffect(() => {
    if (priceMode !== "raw") {
      setConnection("closed");
      return;
    }
    const protocol = window.location.protocol === "https:" ? "wss" : "ws";
    const ws = new WebSocket(`${protocol}://${window.location.host}/api/v1/ws`);
    let closed = false;
    setConnection("connecting");

    ws.addEventListener("open", () => {
      setConnection("open");
      ws.send(
        JSON.stringify({
          op: "subscribe",
          req_id: `${Date.now()}`,
          channels: [
            { type: "ticker", exchange, symbol },
            { type: "kline", exchange, symbol, timeframe },
          ],
        })
      );
    });

    ws.addEventListener("message", (message) => {
      const payload = JSON.parse(message.data) as RealtimeEvent | { op: string };
      if ("op" in payload) return;
      const clientRecvMS = Date.now();
      if (payload.type === "ticker" && payload.tick) setLatestTick({ ...payload.tick, client_recv_ms: clientRecvMS });
      if (payload.type === "kline" && payload.bar) updateBar({ ...payload.bar, client_recv_ms: clientRecvMS });
    });

    ws.addEventListener("close", () => {
      if (!closed) setConnection("closed");
    });
    ws.addEventListener("error", () => setConnection("closed"));

    return () => {
      closed = true;
      ws.close();
    };
  }, [exchange, symbol, timeframe, priceMode, setConnection, setLatestTick, updateBar]);
}
