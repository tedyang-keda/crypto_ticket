package api

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/gorilla/websocket"

	"crypto-ticket/internal/app"
	"crypto-ticket/internal/realtime"
	"crypto-ticket/internal/storage"
)

func TestHTTPRejectsDisabledThreeDayForBothExchanges(t *testing.T) {
	service := app.NewMarketService(nil, realtime.NewHub(), []string{"1m", "3D"}, 300)
	handler := NewServer(service, realtime.NewHub(), "").Handler()
	for _, exchange := range []string{"binance", "okx"} {
		response := httptest.NewRecorder()
		handler.ServeHTTP(response, httptest.NewRequest(http.MethodGet, "/api/v1/klines?exchange="+exchange+"&symbol=TEST&timeframe=3D", nil))
		if response.Code != http.StatusBadRequest || !strings.Contains(response.Body.String(), "disabled timeframe") {
			t.Fatalf("%s 3D should return 400: status=%d body=%s", exchange, response.Code, response.Body.String())
		}
	}
}

func TestWebsocketRejectsThreeDayAndAcknowledgesOnlyAcceptedChannels(t *testing.T) {
	hub := realtime.NewHub()
	service := app.NewMarketService(storage.NewMemoryHistoricalStore(), hub, []string{"1m", "3D"}, 300)
	server := httptest.NewServer(NewServer(service, hub, "").Handler())
	defer server.Close()
	conn, _, err := websocket.DefaultDialer.Dial("ws"+strings.TrimPrefix(server.URL, "http")+"/api/v1/ws", nil)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	if err := conn.SetReadDeadline(time.Now().Add(3 * time.Second)); err != nil {
		t.Fatal(err)
	}
	channels := []wsChannel{{Type: "kline", Exchange: "binance", Symbol: "TEST", Timeframe: "3D"}, {Type: "kline", Exchange: "okx", Symbol: "TEST", Timeframe: "3D"}, {Type: "kline", Exchange: "okx", Symbol: "TEST", Timeframe: "1D"}}
	if err := conn.WriteJSON(wsMessage{Op: "subscribe", ReqID: "policy", Channels: channels}); err != nil {
		t.Fatal(err)
	}
	for index := 0; index < 2; index++ {
		var response struct {
			Op      string    `json:"op"`
			Error   string    `json:"error"`
			Channel wsChannel `json:"channel"`
		}
		if err := conn.ReadJSON(&response); err != nil {
			t.Fatal(err)
		}
		if response.Op != "error" || !strings.Contains(response.Error, "disabled timeframe") || response.Channel.Timeframe != "3D" {
			t.Fatalf("disabled subscription was not explicitly rejected: %+v", response)
		}
	}
	var ack wsMessage
	if err := conn.ReadJSON(&ack); err != nil {
		t.Fatal(err)
	}
	if ack.Op != "subscribed" || ack.ReqID != "policy" || len(ack.Channels) != 1 || ack.Channels[0].Timeframe != "1D" {
		t.Fatalf("acknowledgment includes rejected channels: %+v", ack)
	}
	for _, exchange := range []string{"binance", "okx"} {
		if hub.HasSubscribers(realtime.KlineChannel(exchange, "TEST", "3D")) {
			t.Fatalf("%s 3D actually subscribed", exchange)
		}
	}
	if !hub.HasSubscribers(realtime.KlineChannel("okx", "TEST", "1D")) {
		t.Fatal("unrelated accepted interval was not subscribed")
	}
}
