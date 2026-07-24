package collector

import (
	"context"
	"testing"
	"time"

	"crypto-ticket/internal/market"
	"crypto-ticket/internal/realtime"
)

type fakeOfficialLatestSource struct {
	bars  []market.Bar
	calls int
}

func (f *fakeOfficialLatestSource) RecentKlines(_ context.Context, exchange string, symbol string, tf string) ([]market.Bar, error) {
	f.calls++
	bars := append([]market.Bar(nil), f.bars...)
	for i := range bars {
		bars[i].Exchange = exchange
		bars[i].Symbol = symbol
		bars[i].Timeframe = tf
	}
	return bars, nil
}

type recordingOfficialPublisher struct {
	bars []market.Bar
}

func (p *recordingOfficialPublisher) PublishOfficialKline(_ context.Context, bar market.Bar) error {
	p.bars = append(p.bars, bar)
	return nil
}

func TestOfficialLiveRunnerPublishesSubscribedHigherTimeframeOnce(t *testing.T) {
	hub := realtime.NewHub()
	sub := hub.Subscribe()
	defer sub.Close()
	sub.Add(realtime.KlineChannel("okx", "KORU-USDT-SWAP", "1W"))
	sub.Add(realtime.KlineChannel("okx", "KORU-USDT-SWAP", "1m"))
	source := &fakeOfficialLatestSource{bars: []market.Bar{{
		StartMS: 1_780_000_000_000, EndMS: 1_780_604_799_999,
		OpenPrice: 20, HighPrice: 21, LowPrice: 19, ClosePrice: 20.5,
		Volume: 10, QuoteVolume: 205, IsFinal: false,
	}}}
	publisher := &recordingOfficialPublisher{}
	runner := NewOfficialLiveRunner(hub, source, publisher, time.Second)

	if err := runner.poll(context.Background()); err != nil {
		t.Fatal(err)
	}
	if err := runner.poll(context.Background()); err != nil {
		t.Fatal(err)
	}
	if source.calls != 2 || len(publisher.bars) != 1 {
		t.Fatalf("unexpected relay calls=%d bars=%+v", source.calls, publisher.bars)
	}
	if publisher.bars[0].Timeframe != "1W" {
		t.Fatalf("unexpected relayed bar: %+v", publisher.bars[0])
	}
}

func TestOfficialLiveRunnerPublishesOfficialFinalAtPeriodTransition(t *testing.T) {
	hub := realtime.NewHub()
	sub := hub.Subscribe()
	defer sub.Close()
	sub.Add(realtime.KlineChannel("okx", "KORU-USDT-SWAP", "1W"))
	firstStart := int64(1_780_000_000_000)
	source := &fakeOfficialLatestSource{bars: []market.Bar{{
		StartMS: firstStart, EndMS: firstStart + 604_799_999,
		OpenPrice: 20, HighPrice: 21, LowPrice: 19, ClosePrice: 20.5, IsFinal: false,
	}}}
	publisher := &recordingOfficialPublisher{}
	runner := NewOfficialLiveRunner(hub, source, publisher, time.Second)
	if err := runner.poll(context.Background()); err != nil {
		t.Fatal(err)
	}

	source.bars = []market.Bar{
		{StartMS: firstStart, EndMS: firstStart + 604_799_999, OpenPrice: 20, HighPrice: 22, LowPrice: 19, ClosePrice: 21, IsFinal: true},
		{StartMS: firstStart + 604_800_000, EndMS: firstStart + 1_209_599_999, OpenPrice: 21, HighPrice: 21.5, LowPrice: 20.5, ClosePrice: 21.2, IsFinal: false},
	}
	if err := runner.poll(context.Background()); err != nil {
		t.Fatal(err)
	}
	if len(publisher.bars) != 3 || !publisher.bars[1].IsFinal || publisher.bars[2].IsFinal {
		t.Fatalf("expected live, official final, new live; got %+v", publisher.bars)
	}
}
