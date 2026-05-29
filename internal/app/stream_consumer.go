package app

import (
	"context"
	"errors"
	"log"
	"time"

	"crypto-ticket/internal/stream"
)

type StreamConsumerConfig struct {
	StreamName   string
	Group        string
	Consumer     string
	ReadCount    int64
	Block        time.Duration
	PublishTicks bool
}

type StreamConsumer struct {
	ticks   stream.TickStream
	service *MarketService
	config  StreamConsumerConfig
}

func NewStreamConsumer(ticks stream.TickStream, service *MarketService, config StreamConsumerConfig) *StreamConsumer {
	if config.ReadCount <= 0 {
		config.ReadCount = 200
	}
	if config.Block <= 0 {
		config.Block = time.Second
	}
	return &StreamConsumer{ticks: ticks, service: service, config: config}
}

func (c *StreamConsumer) Run(ctx context.Context) error {
	if err := c.ticks.EnsureGroup(ctx, c.config.StreamName, c.config.Group); err != nil {
		return err
	}
	if err := c.drainPending(ctx); err != nil {
		return err
	}
	for {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		if err := c.readAndProcess(ctx, ">"); err != nil && !errors.Is(err, context.Canceled) {
			return err
		}
	}
}

func (c *StreamConsumer) drainPending(ctx context.Context) error {
	for {
		messages, err := c.ticks.ReadGroup(ctx, c.config.StreamName, c.config.Group, c.config.Consumer, "0", c.config.ReadCount, 0)
		if err != nil {
			return err
		}
		if len(messages) == 0 {
			return nil
		}
		log.Printf("draining pending stream=%s messages=%d", c.config.StreamName, len(messages))
		if err := c.processMessages(ctx, messages); err != nil {
			return err
		}
	}
}

func (c *StreamConsumer) readAndProcess(ctx context.Context, startID string) error {
	messages, err := c.ticks.ReadGroup(ctx, c.config.StreamName, c.config.Group, c.config.Consumer, startID, c.config.ReadCount, c.config.Block)
	if err != nil {
		return err
	}
	return c.processMessages(ctx, messages)
}

func (c *StreamConsumer) processMessages(ctx context.Context, messages []stream.TickMessage) error {
	ackIDs := make(map[string][]string)
	for _, message := range messages {
		var err error
		if c.config.PublishTicks {
			err = c.service.IngestTick(ctx, message.Tick)
		} else {
			err = c.service.AggregateTick(ctx, message.Tick)
		}
		if err != nil {
			return err
		}
		ackIDs[message.Stream] = append(ackIDs[message.Stream], message.ID)
	}
	for streamName, ids := range ackIDs {
		if err := c.ticks.Ack(ctx, streamName, c.config.Group, ids...); err != nil {
			return err
		}
	}
	return nil
}
