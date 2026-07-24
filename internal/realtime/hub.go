package realtime

import (
	"encoding/json"
	"sort"
	"strings"
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

type KlineSubscription struct {
	Exchange  string
	Symbol    string
	Timeframe string
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

func (h *Hub) HasSubscribers(channel string) bool {
	h.mu.RLock()
	defer h.mu.RUnlock()
	for sub := range h.subscribers {
		if sub.hasChannel(channel) {
			return true
		}
	}
	return false
}

func (h *Hub) KlineSubscriptions() []KlineSubscription {
	h.mu.RLock()
	defer h.mu.RUnlock()
	deduplicated := make(map[string]KlineSubscription)
	for sub := range h.subscribers {
		sub.mu.RLock()
		for channel := range sub.channels {
			parts := strings.SplitN(channel, ":", 4)
			if len(parts) != 4 || parts[0] != "kline" {
				continue
			}
			deduplicated[channel] = KlineSubscription{Exchange: parts[1], Symbol: parts[2], Timeframe: parts[3]}
		}
		sub.mu.RUnlock()
	}
	out := make([]KlineSubscription, 0, len(deduplicated))
	for _, subscription := range deduplicated {
		out = append(out, subscription)
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].Exchange != out[j].Exchange {
			return out[i].Exchange < out[j].Exchange
		}
		if out[i].Symbol != out[j].Symbol {
			return out[i].Symbol < out[j].Symbol
		}
		return out[i].Timeframe < out[j].Timeframe
	})
	return out
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

func (s *Subscriber) hasChannel(channel string) bool {
	s.mu.RLock()
	defer s.mu.RUnlock()
	_, ok := s.channels[channel]
	return ok
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
