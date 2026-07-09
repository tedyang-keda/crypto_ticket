package api

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"github.com/gorilla/websocket"

	"crypto-ticket/internal/app"
	"crypto-ticket/internal/market"
	"crypto-ticket/internal/realtime"
	"crypto-ticket/internal/timeframe"
)

type Server struct {
	market       *app.MarketService
	hub          *realtime.Hub
	dashboardDir string
	upgrader     websocket.Upgrader
}

func NewServer(marketService *app.MarketService, hub *realtime.Hub, dashboardDir string) *Server {
	return &Server{
		market:       marketService,
		hub:          hub,
		dashboardDir: dashboardDir,
		upgrader: websocket.Upgrader{
			CheckOrigin: func(r *http.Request) bool { return true },
		},
	}
}

func (s *Server) Handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /healthz", s.healthz)
	mux.HandleFunc("GET /api/v1/ticker/latest", s.latestTicker)
	mux.HandleFunc("GET /api/v1/klines", s.klines)
	mux.HandleFunc("GET /api/v1/symbols", s.symbols)
	mux.HandleFunc("GET /api/v1/ws", s.websocket)
	mux.HandleFunc("GET /favicon.ico", func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusNoContent)
	})
	mux.Handle("/", s.dashboard())
	return gzipResponses(cors(mux))
}

func (s *Server) healthz(w http.ResponseWriter, _ *http.Request) {
	writeJSON(w, http.StatusOK, map[string]any{"ok": true, "ts_ms": market.NowMS()})
}

func (s *Server) latestTicker(w http.ResponseWriter, r *http.Request) {
	exchange, symbol, err := requiredMarketParams(r)
	if err != nil {
		writeError(w, http.StatusBadRequest, err)
		return
	}
	priceMode, err := priceModeParam(r)
	if err != nil {
		writeError(w, http.StatusBadRequest, err)
		return
	}
	tick, err := s.market.LatestTick(r.Context(), exchange, symbol, priceMode)
	if err != nil {
		writeError(w, http.StatusInternalServerError, err)
		return
	}
	if tick == nil {
		writeError(w, http.StatusNotFound, errors.New("ticker not found"))
		return
	}
	writeJSON(w, http.StatusOK, tick)
}

func (s *Server) klines(w http.ResponseWriter, r *http.Request) {
	exchange, symbol, err := requiredMarketParams(r)
	if err != nil {
		writeError(w, http.StatusBadRequest, err)
		return
	}
	tf := strings.TrimSpace(r.URL.Query().Get("timeframe"))
	if _, err := timeframe.Normalize(tf); err != nil {
		writeError(w, http.StatusBadRequest, err)
		return
	}
	limit := 300
	if raw := r.URL.Query().Get("limit"); raw != "" {
		parsed, err := strconv.Atoi(raw)
		if err != nil {
			writeError(w, http.StatusBadRequest, errors.New("invalid limit"))
			return
		}
		limit = parsed
	}
	includeLive := true
	if raw := r.URL.Query().Get("include_live"); raw != "" {
		includeLive = raw != "false" && raw != "0"
	}
	priceMode, err := priceModeParam(r)
	if err != nil {
		writeError(w, http.StatusBadRequest, err)
		return
	}
	bars, err := s.market.Klines(r.Context(), market.KlineQuery{
		Exchange:    exchange,
		Symbol:      symbol,
		Timeframe:   tf,
		Limit:       limit,
		IncludeLive: includeLive,
		PriceMode:   priceMode,
	})
	if err != nil {
		writeError(w, http.StatusInternalServerError, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"exchange":   exchange,
		"symbol":     symbol,
		"timeframe":  tf,
		"price_mode": priceMode,
		"bars":       bars,
	})
}

func (s *Server) symbols(w http.ResponseWriter, r *http.Request) {
	exchange := strings.ToLower(strings.TrimSpace(r.URL.Query().Get("exchange")))
	if exchange == "" {
		writeError(w, http.StatusBadRequest, errors.New("missing exchange"))
		return
	}
	var activeOnly *bool
	if raw := r.URL.Query().Get("active"); raw != "" && raw != "all" {
		value := raw == "1" || raw == "true" || raw == "yes"
		activeOnly = &value
	}
	symbols, err := s.market.ListSymbols(r.Context(), exchange, activeOnly)
	if err != nil {
		writeError(w, http.StatusInternalServerError, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"exchange": exchange, "symbols": symbols})
}

func (s *Server) websocket(w http.ResponseWriter, r *http.Request) {
	conn, err := s.upgrader.Upgrade(w, r, nil)
	if err != nil {
		return
	}
	defer conn.Close()
	sub := s.hub.Subscribe()
	defer sub.Close()

	ctx, cancel := context.WithCancel(r.Context())
	defer cancel()

	go func() {
		defer cancel()
		for {
			var message wsMessage
			if err := conn.ReadJSON(&message); err != nil {
				return
			}
			if message.Op == "ping" {
				_ = conn.WriteJSON(map[string]any{"op": "pong", "ts_ms": market.NowMS()})
				continue
			}
			if message.Op != "subscribe" {
				continue
			}
			for _, channel := range message.Channels {
				exchange := strings.ToLower(channel.Exchange)
				symbol := strings.ToUpper(channel.Symbol)
				switch channel.Type {
				case "ticker":
					sub.Add(realtime.TickerChannel(exchange, symbol))
				case "kline":
					if _, err := timeframe.Normalize(channel.Timeframe); err == nil {
						sub.Add(realtime.KlineChannel(exchange, symbol, channel.Timeframe))
					}
				}
			}
			_ = conn.WriteJSON(map[string]any{"op": "subscribed", "req_id": message.ReqID, "channels": message.Channels})
		}
	}()

	ping := time.NewTicker(15 * time.Second)
	defer ping.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case event, ok := <-sub.Events():
			if !ok {
				return
			}
			if err := conn.WriteJSON(event); err != nil {
				return
			}
		case <-ping.C:
			if err := conn.WriteJSON(map[string]any{"op": "ping", "ts_ms": market.NowMS()}); err != nil {
				return
			}
		}
	}
}

func (s *Server) dashboard() http.Handler {
	fs := http.FileServer(http.Dir(s.dashboardDir))
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		path := filepath.Clean(r.URL.Path)
		if strings.HasPrefix(path, "/api/") {
			http.NotFound(w, r)
			return
		}
		fs.ServeHTTP(w, r)
	})
}

func requiredMarketParams(r *http.Request) (string, string, error) {
	exchange := strings.ToLower(strings.TrimSpace(r.URL.Query().Get("exchange")))
	symbol := strings.ToUpper(strings.TrimSpace(r.URL.Query().Get("symbol")))
	if exchange == "" {
		return "", "", errors.New("missing exchange")
	}
	if symbol == "" {
		return "", "", errors.New("missing symbol")
	}
	return exchange, symbol, nil
}

func priceModeParam(r *http.Request) (string, error) {
	mode, err := market.NormalizePriceMode(r.URL.Query().Get("price_mode"))
	if err != nil {
		return "", err
	}
	return mode, nil
}

func writeJSON(w http.ResponseWriter, status int, value any) {
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(value)
}

func writeError(w http.ResponseWriter, status int, err error) {
	writeJSON(w, status, map[string]any{"error": err.Error()})
}

func cors(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Access-Control-Allow-Origin", "*")
		w.Header().Set("Access-Control-Allow-Headers", "Content-Type")
		w.Header().Set("Access-Control-Allow-Methods", "GET,POST,OPTIONS")
		if r.Method == http.MethodOptions {
			w.WriteHeader(http.StatusNoContent)
			return
		}
		next.ServeHTTP(w, r)
	})
}

type wsMessage struct {
	Op       string      `json:"op"`
	ReqID    string      `json:"req_id"`
	Channels []wsChannel `json:"channels"`
}

type wsChannel struct {
	Type      string `json:"type"`
	Exchange  string `json:"exchange"`
	Symbol    string `json:"symbol"`
	Timeframe string `json:"timeframe,omitempty"`
}
