package collector

import (
	"context"
	"testing"
	"time"

	"crypto-ticket/internal/market"
	"crypto-ticket/internal/realtime"
)

type fakeOfficialLatestSource struct {
	bar   market.Bar
	calls int
}

func (f *fakeOfficialLatestSource) LatestKline(_ context.Context, exchange string, symbol string, tf string) (*market.Bar, error) {
	f.calls++
	bar := f.bar
	bar.Exchange = exchange
	bar.Symbol = symbol
	bar.Timeframe = tf
	return &bar, nil
}

type recordingOfficialPublisher struct {
	bars []market.Bar
}

func (p *recordingOfficialPublisher) PublishOfficialLiveKline(_ context.Context, bar market.Bar) error {
	p.bars = append(p.bars, bar)
	return nil
}

func TestOfficialLiveRunnerPublishesSubscribedHigherTimeframeOnce(t *testing.T) {
	hub := realtime.NewHub()
	sub := hub.Subscribe()
	defer sub.Close()
	sub.Add(realtime.KlineChannel("okx", "KORU-USDT-SWAP", "1W"))
	sub.Add(realtime.KlineChannel("okx", "KORU-USDT-SWAP", "1m"))
	source := &fakeOfficialLatestSource{bar: market.Bar{
		StartMS: 1_780_000_000_000, EndMS: 1_780_604_799_999,
		OpenPrice: 20, HighPrice: 21, LowPrice: 19, ClosePrice: 20.5,
		Volume: 10, QuoteVolume: 205, IsFinal: false,
	}}
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
