const state = {
  meta: null,
  exchange: null,
  timeframe: "1m",
  symbol: null,
  symbols: [],
  filteredSymbols: [],
  timezone: "Asia/Shanghai",
  currentBars: [],
  barByTime: new Map(),
  chart: null,
  candleSeries: null,
  volumeSeries: null,
};

const el = (id) => document.getElementById(id);

const LOCAL_TIMEZONE = Intl.DateTimeFormat().resolvedOptions().timeZone || "local";
const TIMEZONE_OPTIONS = [
  ["Asia/Shanghai", "Shanghai UTC+8"],
  ["UTC", "UTC"],
  ["local", `Local ${LOCAL_TIMEZONE}`],
  ["America/New_York", "New York"],
  ["Europe/London", "London"],
  ["Asia/Tokyo", "Tokyo"],
];

const dateFormatterCache = new Map();

function effectiveTimezone() {
  return state.timezone === "local" ? LOCAL_TIMEZONE : state.timezone;
}

function getDateFormatter(options = {}) {
  const timezone = effectiveTimezone();
  const key = JSON.stringify([timezone, options]);
  if (!dateFormatterCache.has(key)) {
    dateFormatterCache.set(
      key,
      new Intl.DateTimeFormat("zh-CN", {
        timeZone: timezone === "local" ? undefined : timezone,
        year: "numeric",
        month: "2-digit",
        day: "2-digit",
        hour: "2-digit",
        minute: "2-digit",
        second: options.seconds === false ? undefined : "2-digit",
        hour12: false,
      })
    );
  }
  return dateFormatterCache.get(key);
}

const num = (value, digits = 2) => {
  const parsed = Number(value);
  if (!Number.isFinite(parsed)) return "-";
  return parsed.toLocaleString("en-US", {
    minimumFractionDigits: digits,
    maximumFractionDigits: digits,
  });
};

const priceNum = (value) => {
  const parsed = Math.abs(Number(value));
  if (!Number.isFinite(parsed)) return "-";
  if (parsed >= 100) return num(value, 2);
  if (parsed >= 1) return num(value, 4);
  return num(value, 8);
};

function setQueryParams() {
  const params = new URLSearchParams(window.location.search);
  if (state.exchange) params.set("exchange", state.exchange);
  else params.delete("exchange");
  if (state.symbol) params.set("symbol", state.symbol);
  else params.delete("symbol");
  if (state.timeframe) params.set("timeframe", state.timeframe);
  else params.delete("timeframe");
  if (state.timezone) params.set("tz", state.timezone);
  else params.delete("tz");
  const query = params.toString();
  history.replaceState(null, "", query ? `${window.location.pathname}?${query}` : window.location.pathname);
}

function loadStoredSelection() {
  const params = new URLSearchParams(window.location.search);
  return {
    exchange: params.get("exchange") || localStorage.getItem("dashboard_exchange"),
    symbol: params.get("symbol") || localStorage.getItem("dashboard_symbol"),
    timeframe: params.get("timeframe") || localStorage.getItem("dashboard_timeframe") || "1m",
    timezone: params.get("tz") || localStorage.getItem("dashboard_timezone") || "Asia/Shanghai",
  };
}

function persistSelection() {
  if (state.exchange) localStorage.setItem("dashboard_exchange", state.exchange);
  if (state.symbol) localStorage.setItem("dashboard_symbol", state.symbol);
  if (state.timeframe) localStorage.setItem("dashboard_timeframe", state.timeframe);
  if (state.timezone) localStorage.setItem("dashboard_timezone", state.timezone);
  setQueryParams();
}

function formatTs(ts) {
  if (!ts) return "-";
  if (typeof ts === "string" && !/^\d+(\.\d+)?$/.test(ts.trim())) {
    const parsedDate = new Date(ts.replace(" ", "T"));
    if (Number.isNaN(parsedDate.getTime())) return ts;
    return getDateFormatter().format(parsedDate);
  }
  const parsed = Number(ts);
  if (!Number.isFinite(parsed)) return String(ts);
  const ms = parsed > 1e12 ? parsed : parsed * 1000;
  return getDateFormatter().format(new Date(ms));
}

function formatChartTime(time, options = {}) {
  const value = typeof time === "object" ? Date.UTC(time.year, time.month - 1, time.day) : Number(time) * 1000;
  if (!Number.isFinite(value)) return "";
  return getDateFormatter({ seconds: options.seconds !== false }).format(new Date(value));
}

function formatTimeOfDay(ts) {
  const parsed = Number(ts);
  if (!Number.isFinite(parsed)) return "-";
  const ms = parsed > 1e12 ? parsed : parsed * 1000;
  return new Intl.DateTimeFormat("zh-CN", {
    timeZone: effectiveTimezone() === "local" ? undefined : effectiveTimezone(),
    hour: "2-digit",
    minute: "2-digit",
    second: "2-digit",
    hour12: false,
  }).format(new Date(ms));
}

function timezoneOffsetLabel() {
  try {
    const zoneName = new Intl.DateTimeFormat("en-US", {
      timeZone: effectiveTimezone(),
      timeZoneName: "shortOffset",
    })
      .formatToParts(new Date())
      .find((part) => part.type === "timeZoneName")?.value;
    return (zoneName || "").replace("GMT", "UTC") || effectiveTimezone();
  } catch {
    return effectiveTimezone();
  }
}

function renderTimezoneSelect() {
  const select = el("timezoneSelect");
  select.innerHTML = "";
  for (const [value, label] of TIMEZONE_OPTIONS) {
    const option = document.createElement("option");
    option.value = value;
    option.textContent = label;
    select.appendChild(option);
  }
  select.value = TIMEZONE_OPTIONS.some(([value]) => value === state.timezone) ? state.timezone : "Asia/Shanghai";
  state.timezone = select.value;
  updateTimezoneDisplay();
}

function updateTimezoneDisplay() {
  const label = state.timezone === "local" ? LOCAL_TIMEZONE : state.timezone;
  el("metaTimezone").textContent = `${label} ${timezoneOffsetLabel()}`;
  applyChartTimezone();
}

function applyChartTimezone() {
  if (!state.chart) return;
  state.chart.applyOptions({
    localization: {
      priceFormatter: (price) => num(price, 4),
      timeFormatter: (time) => formatChartTime(time, { seconds: true }),
    },
    timeScale: {
      tickMarkFormatter: (time) => formatChartTime(time, { seconds: false }),
    },
  });
}

function createChart() {
  const container = el("chart");
  const chart = LightweightCharts.createChart(container, {
    layout: {
      background: { type: "solid", color: "transparent" },
      textColor: "#dbe5ef",
      fontFamily: '"IBM Plex Sans", system-ui, sans-serif',
    },
    grid: {
      vertLines: { color: "rgba(255,255,255,0.04)" },
      horzLines: { color: "rgba(255,255,255,0.05)" },
    },
    rightPriceScale: {
      borderColor: "#243244",
    },
    timeScale: {
      borderColor: "#243244",
      timeVisible: true,
      secondsVisible: false,
    },
    crosshair: {
      mode: LightweightCharts.CrosshairMode.Normal,
    },
    localization: {
      priceFormatter: (price) => num(price, 4),
    },
  });

  const candleOptions = {
    upColor: "#57d69d",
    downColor: "#ff7b7b",
    borderVisible: false,
    wickUpColor: "#57d69d",
    wickDownColor: "#ff7b7b",
  };
  const candleSeries = chart.addCandlestickSeries
    ? chart.addCandlestickSeries(candleOptions)
    : chart.addSeries(LightweightCharts.CandlestickSeries, candleOptions);

  const volumeOptions = {
    color: "#76a7ff66",
    priceFormat: { type: "volume" },
    priceScaleId: "",
    scaleMargins: { top: 0.85, bottom: 0 },
  };
  const volumeSeries = chart.addHistogramSeries
    ? chart.addHistogramSeries(volumeOptions)
    : chart.addSeries(LightweightCharts.HistogramSeries, volumeOptions);

  const resize = () => chart.applyOptions({ width: container.clientWidth, height: container.clientHeight });
  new ResizeObserver(resize).observe(container);
  resize();
  chart.subscribeCrosshairMove((param) => {
    const rawTime = typeof param.time === "object" ? Date.UTC(param.time.year, param.time.month - 1, param.time.day) / 1000 : param.time;
    const bar = rawTime ? state.barByTime.get(Number(rawTime)) : null;
    renderHoverBar(bar || state.currentBars[state.currentBars.length - 1] || null);
  });

  state.chart = chart;
  state.candleSeries = candleSeries;
  state.volumeSeries = volumeSeries;
  applyChartTimezone();
}

function renderChips(container, items, activeValue, onClick) {
  container.innerHTML = "";
  for (const item of items) {
    const button = document.createElement("button");
    button.type = "button";
    button.className = `chip${item.value === activeValue ? " active" : ""}`;
    button.textContent = item.label;
    button.addEventListener("click", () => onClick(item.value));
    container.appendChild(button);
  }
}

function renderExchangeTabs() {
  const exchanges = (state.meta?.exchanges || []).map((item) => ({ label: item.toUpperCase(), value: item }));
  renderChips(el("exchangeTabs"), exchanges, state.exchange, async (exchange) => {
    state.exchange = exchange;
    state.symbol = null;
    persistSelection();
    await loadSymbols();
  });
  el("exchangeCount").textContent = `${exchanges.length} markets`;
  el("metaExchange").textContent = state.exchange ? state.exchange.toUpperCase() : "exchange";
}

function renderTimeframeTabs() {
  const timeframes = (state.meta?.timeframes || []).map((item) => ({ label: item, value: item }));
  renderChips(el("timeframeTabs"), timeframes, state.timeframe, async (timeframe) => {
    state.timeframe = timeframe;
    persistSelection();
    await loadSnapshotAndBars();
  });
  el("metaTimeframe").textContent = state.timeframe || "timeframe";
}

function renderSymbols() {
  const activeOnly = el("activeOnly").checked;
  const query = el("symbolSearch").value.trim().toLowerCase();
  state.filteredSymbols = state.symbols.filter((item) => {
    if (activeOnly && !item.is_active) return false;
    if (query && !String(item.symbol || "").toLowerCase().includes(query)) return false;
    return true;
  });

  const container = el("symbolList");
  container.innerHTML = "";

  for (const item of state.filteredSymbols) {
    const button = document.createElement("button");
    button.type = "button";
    button.className = `symbol-item${item.symbol === state.symbol ? " active" : ""}`;
    button.setAttribute("role", "option");
    button.setAttribute("aria-selected", String(item.symbol === state.symbol));

    const dot = document.createElement("span");
    dot.className = `symbol-dot${item.is_active ? " on" : ""}`;
    button.appendChild(dot);

    const main = document.createElement("div");
    main.className = "symbol-main";

    const code = document.createElement("div");
    code.className = "symbol-code";
    code.textContent = item.symbol;
    main.appendChild(code);

    const meta = document.createElement("div");
    meta.className = "symbol-meta";
    meta.innerHTML = `<span>${item.market_type}</span><span>${item.is_active ? "live" : "inactive"}</span>`;
    main.appendChild(meta);

    const badge = document.createElement("span");
    badge.className = `symbol-badge${item.is_active ? " live" : ""}`;
    badge.textContent = item.is_active ? "active" : item.last_status || "stale";

    button.appendChild(main);
    button.appendChild(badge);
    button.addEventListener("click", async () => {
      state.symbol = item.symbol;
      persistSelection();
      renderSymbols();
      await loadSnapshotAndBars();
    });
    container.appendChild(button);
  }

  el("symbolCount").textContent = `${state.filteredSymbols.length}/${state.symbols.length}`;
}

function renderDetails(target, data, keys) {
  const container = el(target);
  container.innerHTML = "";
  for (const [key, label, formatter] of keys) {
    const item = document.createElement("div");
    item.className = "kv-item";
    const value = data && Object.prototype.hasOwnProperty.call(data, key) ? data[key] : null;
    item.innerHTML = `
      <div class="kv-key">${label}</div>
      <div class="kv-val">${formatter ? formatter(value, data) : formatValue(value)}</div>
    `;
    container.appendChild(item);
  }
}

function formatValue(value) {
  if (value === null || value === undefined || value === "") return "-";
  if (typeof value === "boolean") return value ? "true" : "false";
  if (typeof value === "number") return num(value, Number.isInteger(value) ? 0 : 2);
  if (typeof value === "object") return JSON.stringify(value);
  return String(value);
}

function setReadoutValue(id, value) {
  el(id).textContent = value ?? "-";
}

function setReadoutDirection(bar) {
  const closeCell = el("hoverClose").parentElement;
  closeCell.classList.remove("positive", "negative");
  if (!bar) return;
  if (Number(bar.close_price) >= Number(bar.open_price)) closeCell.classList.add("positive");
  else closeCell.classList.add("negative");
}

function renderHoverBar(bar) {
  setReadoutDirection(bar);
  if (!bar) {
    for (const id of [
      "hoverTime",
      "hoverOpen",
      "hoverHigh",
      "hoverLow",
      "hoverClose",
      "hoverVolume",
      "hoverQuoteVolume",
      "hoverTrades",
      "hoverLastTick",
      "hoverState",
    ]) {
      setReadoutValue(id, "-");
    }
    return;
  }

  setReadoutValue("hoverTime", `${formatTs(bar.start_ms)} - ${formatTimeOfDay(bar.end_ms)}`);
  setReadoutValue("hoverOpen", priceNum(bar.open_price));
  setReadoutValue("hoverHigh", priceNum(bar.high_price));
  setReadoutValue("hoverLow", priceNum(bar.low_price));
  setReadoutValue("hoverClose", priceNum(bar.close_price));
  setReadoutValue("hoverVolume", num(bar.volume, 4));
  setReadoutValue("hoverQuoteVolume", num(bar.quote_volume, 2));
  setReadoutValue("hoverTrades", num(bar.trade_count, 0));
  setReadoutValue("hoverLastTick", formatTs(bar.last_tick_ms));
  setReadoutValue("hoverState", Number(bar.is_final) ? "final" : "live");
}

function updateHeadline(snapshot) {
  const title = `${state.exchange.toUpperCase()} / ${state.symbol} / ${state.timeframe}`;
  el("currentTitle").textContent = title;

  const symbolRow = snapshot?.symbol_row;
  const checkpoint = snapshot?.checkpoint;
  const registryBits = [];
  if (symbolRow) registryBits.push(`status ${symbolRow.last_status || "-"}`);
  if (checkpoint) registryBits.push(`checkpoint ${checkpoint.is_final ? "final" : "live"}`);
  if (snapshot?.bar_count !== undefined) registryBits.push(`bars ${snapshot.bar_count}`);
  el("currentSubtitle").textContent = registryBits.join(" · ") || "No data loaded yet";

  const pills = el("statusPills");
  pills.innerHTML = "";
  const items = [
    [state.exchange.toUpperCase(), "exchange"],
    [state.timeframe, "timeframe"],
    [String(snapshot?.bar_count ?? 0), "bars"],
    [timezoneOffsetLabel(), "tz"],
  ];
  for (const [text, label] of items) {
    const pill = document.createElement("div");
    pill.className = "pill";
    pill.textContent = `${label}: ${text}`;
    pills.appendChild(pill);
  }
}

async function fetchJson(url) {
  const response = await fetch(url, { headers: { Accept: "application/json" } });
  if (!response.ok) {
    const text = await response.text();
    throw new Error(`${response.status} ${response.statusText}: ${text}`);
  }
  return response.json();
}

async function loadMeta() {
  state.meta = await fetchJson("/api/meta");
  renderExchangeTabs();
  renderTimeframeTabs();
}

async function loadSymbols() {
  if (!state.exchange) return;
  const payload = await fetchJson(`/api/symbols?exchange=${encodeURIComponent(state.exchange)}`);
  state.symbols = payload.symbols || [];
  const selected = state.symbols.find((item) => item.symbol === state.symbol);
  if (!selected) {
    const preferred = state.symbols.find((item) => item.is_active) || state.symbols[0];
    state.symbol = preferred ? preferred.symbol : null;
  }
  persistSelection();
  renderExchangeTabs();
  renderSymbols();
  if (state.symbol) {
    await loadSnapshotAndBars();
  }
}

async function loadSnapshotAndBars() {
  if (!state.exchange || !state.symbol || !state.timeframe) return;
  const [snapshot, barsPayload] = await Promise.all([
    fetchJson(`/api/snapshot?exchange=${encodeURIComponent(state.exchange)}&symbol=${encodeURIComponent(state.symbol)}&timeframe=${encodeURIComponent(state.timeframe)}`),
    fetchJson(`/api/bars?exchange=${encodeURIComponent(state.exchange)}&symbol=${encodeURIComponent(state.symbol)}&timeframe=${encodeURIComponent(state.timeframe)}&limit=500`),
  ]);

  const rawBars = barsPayload.bars || [];
  state.currentBars = rawBars;
  state.barByTime = new Map(rawBars.map((bar) => [Math.floor(Number(bar.start_ms) / 1000), bar]));
  snapshot.bar_count = rawBars.length;
  snapshot.first_bar = rawBars[0] || null;
  snapshot.last_bar = rawBars[rawBars.length - 1] || null;
  updateHeadline(snapshot);
  renderDetails("registryDetails", snapshot?.symbol_row || {}, [
    ["exchange", "Exchange"],
    ["symbol", "Symbol"],
    ["market_type", "Market"],
    ["is_active", "Active", (value) => (value ? "true" : "false")],
    ["last_status", "Last Status"],
    ["first_seen_at_ms", "First Seen", (value) => formatTs(value)],
    ["last_seen_at_ms", "Last Seen", (value) => formatTs(value)],
    ["updated_at", "Updated At", (value) => formatTs(value)],
  ]);

  renderDetails("checkpointDetails", snapshot?.checkpoint || {}, [
    ["timeframe", "Timeframe"],
    ["start_ms", "Start", (value) => formatTs(value)],
    ["end_ms", "End", (value) => formatTs(value)],
    ["open_price", "Open"],
    ["high_price", "High"],
    ["low_price", "Low"],
    ["close_price", "Close"],
    ["volume", "Volume"],
    ["quote_volume", "Quote Vol"],
    ["trade_count", "Trades"],
    ["last_tick_ms", "Last Tick", (value) => formatTs(value)],
    ["is_final", "Final", (value) => (value ? "true" : "false")],
  ]);

  const bars = rawBars.map((bar) => ({
    time: Math.floor(Number(bar.start_ms) / 1000),
    open: Number(bar.open_price),
    high: Number(bar.high_price),
    low: Number(bar.low_price),
    close: Number(bar.close_price),
  }));
  const volumes = rawBars.map((bar) => ({
    time: Math.floor(Number(bar.start_ms) / 1000),
    value: Number(bar.volume || 0),
    color: Number(bar.close_price) >= Number(bar.open_price) ? "rgba(87, 214, 157, 0.55)" : "rgba(255, 123, 123, 0.55)",
  }));
  state.candleSeries.setData(bars);
  state.volumeSeries.setData(volumes);
  state.chart.timeScale().fitContent();
  renderHoverBar(rawBars[rawBars.length - 1] || null);
}

async function refreshAll() {
  await loadMeta();
  await loadSymbols();
}

function bindEvents() {
  el("symbolSearch").addEventListener("input", renderSymbols);
  el("activeOnly").addEventListener("change", renderSymbols);
  el("refreshAll").addEventListener("click", refreshAll);
  el("refreshSymbols").addEventListener("click", loadSymbols);
  el("timezoneSelect").addEventListener("change", async (event) => {
    state.timezone = event.target.value;
    persistSelection();
    updateTimezoneDisplay();
    renderHoverBar(state.currentBars[state.currentBars.length - 1] || null);
    if (state.symbol) await loadSnapshotAndBars();
  });
}

async function bootstrap() {
  createChart();
  bindEvents();
  const stored = loadStoredSelection();
  state.exchange = stored.exchange;
  state.symbol = stored.symbol;
  state.timeframe = stored.timeframe;
  state.timezone = TIMEZONE_OPTIONS.some(([value]) => value === stored.timezone) ? stored.timezone : "Asia/Shanghai";
  renderTimezoneSelect();
  await loadMeta();
  const exchanges = state.meta?.exchanges || [];
  const timeframes = state.meta?.timeframes || [];
  if (!state.exchange || !exchanges.includes(state.exchange)) {
    state.exchange = exchanges[0] || "binance";
  }
  if (!state.timeframe || !timeframes.includes(state.timeframe)) {
    state.timeframe = timeframes[0] || "1m";
  }
  await loadSymbols();
  renderTimeframeTabs();
}

bootstrap().catch((err) => {
  console.error(err);
  el("currentSubtitle").textContent = `Failed to load dashboard: ${err.message}`;
});
