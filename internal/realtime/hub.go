package realtime

import (
	"encoding/json"
	"sync"
	"sync/atomic"

	"crypto-ticket/internal/market"
)

type Hub struct {
	mu          sync.RWMutex
	subscribers map[*Subscriber]struct{}
	seq         atomic.Int64
}

type Subscriber struct {
	hub      *Hub
	mu       sync.RWMutex
	channels map[string]struct{}
	events   chan market.Event
}

func NewHub() *Hub {
	return &Hub{subscribers: make(map[*Subscriber]struct{})}
}

func (h *Hub) Subscribe() *Subscriber {
	sub := &Subscriber{
		hub:      h,
		channels: make(map[string]struct{}),
		events:   make(chan market.Event, 256),
	}
	h.mu.Lock()
	h.subscribers[sub] = struct{}{}
	h.mu.Unlock()
	return sub
}

func (h *Hub) Publish(event market.Event) {
	event.Seq = h.seq.Add(1)
	channels := eventChannels(event)
	h.mu.RLock()
	defer h.mu.RUnlock()
	for sub := range h.subscribers {
		if !sub.matches(channels) {
			continue
		}
		select {
		case sub.events <- event:
		default:
		}
	}
}

func (s *Subscriber) Add(channel string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.channels[channel] = struct{}{}
}

func (s *Subscriber) Events() <-chan market.Event {
	return s.events
}

func (s *Subscriber) Close() {
	s.hub.mu.Lock()
	delete(s.hub.subscribers, s)
	s.hub.mu.Unlock()
	close(s.events)
}

func (s *Subscriber) matches(channels []string) bool {
	s.mu.RLock()
	defer s.mu.RUnlock()
	for _, channel := range channels {
		if _, ok := s.channels[channel]; ok {
			return true
		}
	}
	return false
}

func TickerChannel(exchange string, symbol string) string {
	return "ticker:" + exchange + ":" + symbol
}

func KlineChannel(exchange string, symbol string, timeframe string) string {
	return "kline:" + exchange + ":" + symbol + ":" + timeframe
}

func eventChannels(event market.Event) []string {
	switch event.Type {
	case "ticker":
		return []string{TickerChannel(event.Exchange, event.Symbol)}
	case "kline":
		return []string{KlineChannel(event.Exchange, event.Symbol, event.Timeframe)}
	default:
		return nil
	}
}

func MarshalEvent(event market.Event) ([]byte, error) {
	return json.Marshal(event)
}
