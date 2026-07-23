// Package instrument provides a real-time monitor over an exchange's
// instrument metadata. It detects instrument state transitions — in particular
// corporate-action signals (rebase, split, pre-IPO share rebase, suspension,
// delisting) on equity-like perpetuals — and emits them as candidate events for
// the adjustment pipeline. This is layer A of the corporate-action handling:
// detection only. Factor derivation and bar re-adjustment build on the events
// surfaced here.
//
// WebSocket and REST-polling drivers feed the same snapshot-diff core. OKX uses
// its instruments channel; Binance USDⓈ-M uses !contractInfo with exchangeInfo
// bootstrap/reconciliation.
package instrument

import (
	"context"
	"encoding/json"
	"errors"
	"log"
	"net/http"
	"strings"
	"time"

	"github.com/gorilla/websocket"

	"crypto-ticket/internal/market"
)

// Source is the WebSocket side of the monitor: how to reach the instruments
// channel and how to interpret its frames. *exchange.OKXAdapter satisfies it.
type Source interface {
	Name() string
	MarketType() string
	WSURL() string
	BuildInstrumentsSubscribePayload() ([]byte, error)
	ParseInstrumentsMessage(payload []byte) ([]market.SymbolInfo, error)
}

type controlPingSource interface {
	UseWebSocketControlPing() bool
}

// SymbolFetcher is the REST-polling side of the monitor for venues without an
// instruments WebSocket channel. *exchange.BinanceFuturesAdapter satisfies it.
type SymbolFetcher interface {
	Name() string
	MarketType() string
	FetchSymbols(ctx context.Context, client *http.Client) ([]market.SymbolInfo, error)
}

// EventSink receives corporate-action candidate events. Implementations may
// log, persist, or forward them to the factor-derivation stage.
type EventSink interface {
	HandleInstrumentEvent(ctx context.Context, event market.InstrumentChangeEvent) error
}

// SymbolStore keeps instrument metadata fresh in real time as the channel
// pushes state changes, complementing the collector's periodic REST refresh.
type SymbolStore interface {
	UpsertSymbols(ctx context.Context, symbols []market.SymbolInfo) error
}

// AliasLinker records instrument renames (successor <- predecessor) so the
// serving layer can stitch histories. Optional.
type AliasLinker interface {
	Link(exchange string, sourceMarket string, successor string, predecessor string, boundaryMS int64)
}

// Config tunes reconnect backoff, keepalive, and polling. Zero values fall back
// to sensible defaults.
type Config struct {
	ReconnectBaseDelay time.Duration
	ReconnectMaxDelay  time.Duration
	PingInterval       time.Duration
	PollInterval       time.Duration
	// EmitInitialCorporateState recovers an event when the process starts while
	// an equity instrument is already halted/cancel-only/rebasing.
	EmitInitialCorporateState bool
}

func (c Config) withDefaults() Config {
	if c.ReconnectBaseDelay <= 0 {
		c.ReconnectBaseDelay = time.Second
	}
	if c.ReconnectMaxDelay <= 0 {
		c.ReconnectMaxDelay = 60 * time.Second
	}
	if c.PingInterval <= 0 {
		c.PingInterval = 20 * time.Second
	}
	if c.PollInterval <= 0 {
		c.PollInterval = 60 * time.Second
	}
	return c
}

// Monitor tracks one instrument feed and surfaces state changes. It is driven
// either by a WebSocket Source (Run) or a REST SymbolFetcher (RunPolling).
type Monitor struct {
	source     Source
	fetcher    SymbolFetcher
	client     *http.Client
	name       string
	marketType string
	sink       EventSink
	store      SymbolStore
	aliases    AliasLinker
	cfg        Config
	snapshot   map[string]market.SymbolInfo
	familyTop  map[string]string // instFamily -> current live symbol
}

// New builds a WebSocket-driven Monitor. sink is required; store may be nil to
// skip metadata persistence.
func New(source Source, sink EventSink, store SymbolStore, cfg Config) *Monitor {
	return &Monitor{
		source:     source,
		client:     &http.Client{Timeout: 20 * time.Second},
		name:       source.Name(),
		marketType: source.MarketType(),
		sink:       sink,
		store:      store,
		cfg:        cfg.withDefaults(),
		snapshot:   make(map[string]market.SymbolInfo),
		familyTop:  make(map[string]string),
	}
}

// NewPolling builds a REST-polling Monitor for venues without an instruments
// WebSocket channel (e.g. Binance USDⓈ-M futures). store may be nil to leave
// metadata persistence to the collector.
func NewPolling(fetcher SymbolFetcher, sink EventSink, store SymbolStore, cfg Config) *Monitor {
	return &Monitor{
		fetcher:    fetcher,
		client:     &http.Client{Timeout: 20 * time.Second},
		name:       fetcher.Name(),
		marketType: fetcher.MarketType(),
		sink:       sink,
		store:      store,
		cfg:        cfg.withDefaults(),
		snapshot:   make(map[string]market.SymbolInfo),
		familyTop:  make(map[string]string),
	}
}

// SetAliasLinker attaches the rename-alias recorder (layer D). Optional.
func (m *Monitor) SetAliasLinker(aliases AliasLinker) {
	m.aliases = aliases
}

// RunPolling periodically fetches instrument metadata over REST and feeds it
// through the same diff/emit core, until ctx is cancelled. The first fetch
// establishes the baseline; subsequent fetches surface state transitions.
func (m *Monitor) RunPolling(ctx context.Context) error {
	log.Printf("%s %s instrument monitor polling every %s", m.name, m.marketType, m.cfg.PollInterval)
	m.pollOnce(ctx)
	ticker := time.NewTicker(m.cfg.PollInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
			m.pollOnce(ctx)
		}
	}
}

func (m *Monitor) pollOnce(ctx context.Context) {
	symbols, err := m.fetcher.FetchSymbols(ctx, m.client)
	if err != nil {
		if ctx.Err() == nil {
			log.Printf("%s %s instrument poll failed: %v", m.name, m.marketType, err)
		}
		return
	}
	if len(symbols) == 0 {
		return
	}
	m.processBatch(ctx, symbols)
}

// Run connects, subscribes, and processes pushes until ctx is cancelled,
// reconnecting with exponential backoff on transient failures.
func (m *Monitor) Run(ctx context.Context) error {
	backoff := m.cfg.ReconnectBaseDelay
	for {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		err := m.connectOnce(ctx)
		if err == nil || errors.Is(err, context.Canceled) {
			if ctx.Err() != nil {
				return ctx.Err()
			}
			backoff = m.cfg.ReconnectBaseDelay
			continue
		}
		log.Printf("%s %s instrument monitor reconnect after error: %v",
			m.source.Name(), m.source.MarketType(), err)
		timer := time.NewTimer(backoff)
		select {
		case <-ctx.Done():
			timer.Stop()
			return ctx.Err()
		case <-timer.C:
		}
		backoff = minDuration(backoff*2, m.cfg.ReconnectMaxDelay)
	}
}

func (m *Monitor) connectOnce(ctx context.Context) error {
	if fetcher, ok := m.source.(SymbolFetcher); ok {
		symbols, err := fetcher.FetchSymbols(ctx, m.client)
		if err != nil {
			log.Printf("%s %s instrument bootstrap failed: %v", m.name, m.marketType, err)
		} else if len(symbols) > 0 {
			m.processBatch(ctx, symbols)
		}
	}
	conn, _, err := websocket.DefaultDialer.DialContext(ctx, m.source.WSURL(), nil)
	if err != nil {
		return err
	}
	defer conn.Close()

	payload, err := m.source.BuildInstrumentsSubscribePayload()
	if err != nil {
		return err
	}
	if len(payload) > 0 {
		if err := conn.WriteMessage(websocket.TextMessage, payload); err != nil {
			return err
		}
	}
	log.Printf("%s %s instrument monitor connected", m.source.Name(), m.source.MarketType())

	// Close the connection when the context ends so the blocking reader unwinds.
	readerDone := make(chan struct{})
	go func() {
		select {
		case <-ctx.Done():
			_ = conn.Close()
		case <-readerDone:
		}
	}()
	defer close(readerDone)

	// The instruments channel is idle between state changes; keep the socket
	// alive with periodic pings. OKX replies "pong", which parses to no symbols.
	go m.keepAlive(ctx, conn, readerDone)

	readTimeout := m.cfg.PingInterval * 2
	if source, ok := m.source.(controlPingSource); ok && source.UseWebSocketControlPing() {
		conn.SetPongHandler(func(string) error {
			return conn.SetReadDeadline(time.Now().Add(readTimeout))
		})
	}
	for {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		_ = conn.SetReadDeadline(time.Now().Add(readTimeout))
		_, message, err := conn.ReadMessage()
		if err != nil {
			if ctx.Err() != nil {
				return ctx.Err()
			}
			return err
		}
		symbols, err := m.source.ParseInstrumentsMessage(message)
		if err != nil {
			log.Printf("%s %s parse instruments failed: %v",
				m.source.Name(), m.source.MarketType(), err)
			continue
		}
		if len(symbols) == 0 {
			continue
		}
		m.processBatch(ctx, symbols)
	}
}

func (m *Monitor) keepAlive(ctx context.Context, conn *websocket.Conn, done <-chan struct{}) {
	ticker := time.NewTicker(m.cfg.PingInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-done:
			return
		case <-ticker.C:
			_ = conn.SetWriteDeadline(time.Now().Add(5 * time.Second))
			messageType := websocket.TextMessage
			payload := []byte("ping")
			if source, ok := m.source.(controlPingSource); ok && source.UseWebSocketControlPing() {
				messageType = websocket.PingMessage
				payload = nil
			}
			if err := conn.WriteMessage(messageType, payload); err != nil {
				_ = conn.Close()
				return
			}
		}
	}
}

// processBatch diffs an incoming push against the last-known snapshot, emits
// corporate-action candidate events, and refreshes stored metadata. It is pure
// with respect to I/O boundaries (sink/store) so it can be unit-tested by
// feeding batches directly.
func (m *Monitor) processBatch(ctx context.Context, symbols []market.SymbolInfo) {
	fresh := make([]market.SymbolInfo, 0, len(symbols))
	for _, current := range symbols {
		key := strings.ToUpper(strings.TrimSpace(current.Symbol))
		if key == "" {
			continue
		}
		previous, existed := m.snapshot[key]
		if existed {
			current.FirstSeenAtMS = previous.FirstSeenAtMS
			if market.InstrumentSignature(previous) != market.InstrumentSignature(current) {
				m.emitChange(ctx, previous, current)
			}
		} else if m.cfg.EmitInitialCorporateState {
			m.emitInitialCorporateState(ctx, current)
		}
		m.detectRename(ctx, current)
		m.snapshot[key] = current
		fresh = append(fresh, current)
	}
	if m.store != nil && len(fresh) > 0 {
		if err := m.store.UpsertSymbols(ctx, fresh); err != nil {
			log.Printf("%s %s instrument monitor upsert symbols failed: %v",
				m.name, m.marketType, err)
		}
	}
}

func (m *Monitor) emitInitialCorporateState(ctx context.Context, current market.SymbolInfo) {
	if !market.IsEquityLikeAssetClass(current.AssetClass) {
		return
	}
	previous := current
	previous.LifecyclePhase = market.PhaseContinuous
	if current.LifecyclePhase == market.PhaseCancelOnly {
		previous.LifecyclePhase = market.PhaseHalt
	}
	m.emitChange(ctx, previous, current)
}

// detectRename correlates instruments by instFamily: when the live instId for
// a family changes (e.g. SPACEX-USDT-SWAP -> SPCX-USDT-SWAP), it records the
// alias and emits a rename event so the successor's history can be stitched.
func (m *Monitor) detectRename(ctx context.Context, current market.SymbolInfo) {
	if !market.IsEquityLikeAssetClass(current.AssetClass) {
		return
	}
	if current.LifecyclePhase != market.PhaseContinuous && !current.IsActive {
		return // only the live leg defines a family's current instId
	}
	family := instFamilyOf(current.Raw)
	if family == "" {
		return
	}
	symbol := strings.ToUpper(strings.TrimSpace(current.Symbol))
	previous := m.familyTop[family]
	m.familyTop[family] = symbol
	if previous == "" || previous == symbol {
		return
	}

	nowMS := market.NowMS()
	if m.aliases != nil {
		m.aliases.Link(current.Exchange, current.SourceMarket, symbol, previous, nowMS)
	}
	log.Printf("%s %s instrument rename %s -> %s (family=%s)",
		current.Exchange, current.MarketType, previous, symbol, family)
	if m.sink == nil {
		return
	}
	event := market.InstrumentChangeEvent{
		Exchange:     current.Exchange,
		SourceMarket: current.SourceMarket,
		Symbol:       symbol,
		EventTSMS:    nowMS,
		EventType:    market.InstrumentEventRenamed,
		PreviousHash: previous,
		CurrentHash:  symbol,
		CurrentJSON:  current.Raw,
	}
	if err := m.sink.HandleInstrumentEvent(ctx, event); err != nil {
		log.Printf("%s %s rename sink failed symbol=%s: %v", current.Exchange, current.MarketType, symbol, err)
	}
}

func instFamilyOf(raw json.RawMessage) string {
	if len(raw) == 0 {
		return ""
	}
	var fields struct {
		InstFamily string `json:"instFamily"`
	}
	if err := json.Unmarshal(raw, &fields); err != nil {
		return ""
	}
	return strings.ToUpper(strings.TrimSpace(fields.InstFamily))
}

func (m *Monitor) emitChange(ctx context.Context, previous market.SymbolInfo, current market.SymbolInfo) {
	eventType, corporateAction := market.CorporateActionEventType(previous, current)
	if !corporateAction {
		// Non-corporate metadata drift: recorded via the snapshot/store update,
		// but not escalated as an adjustment-pipeline candidate.
		return
	}
	event := market.InstrumentChangeEvent{
		Exchange:     current.Exchange,
		SourceMarket: current.SourceMarket,
		Symbol:       current.Symbol,
		EventTSMS:    market.NowMS(),
		EventType:    eventType,
		PreviousHash: market.InstrumentSignature(previous),
		CurrentHash:  market.InstrumentSignature(current),
		PreviousJSON: previous.Raw,
		CurrentJSON:  current.Raw,
	}
	if m.sink == nil {
		logCorporateAction(previous, current, eventType)
		return
	}
	if err := m.sink.HandleInstrumentEvent(ctx, event); err != nil {
		log.Printf("%s %s instrument event sink failed symbol=%s: %v",
			current.Exchange, current.MarketType, current.Symbol, err)
	}
}

func logCorporateAction(previous market.SymbolInfo, current market.SymbolInfo, eventType string) {
	log.Printf("%s %s corporate-action candidate symbol=%s event=%s phase=%s->%s rule=%s->%s",
		current.Exchange, current.MarketType, current.Symbol, eventType,
		previous.LifecyclePhase, current.LifecyclePhase, previous.RuleType, current.RuleType)
}

func minDuration(a time.Duration, b time.Duration) time.Duration {
	if a < b {
		return a
	}
	return b
}
