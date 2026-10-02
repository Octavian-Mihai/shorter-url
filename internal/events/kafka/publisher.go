// Package kafka implements events.Publisher on Apache Kafka.
package kafka

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	kg "github.com/segmentio/kafka-go"

	"github.com/Octavian-Mihai/shorter-url/internal/events"
)

type Publisher struct{ w *kg.Writer }

var _ events.Publisher = (*Publisher)(nil)

// NewPublisher creates a producer. Messages are keyed by slug, so all clicks
// for one link land on the same partition (ordered per link) while different
// links spread across partitions.
func NewPublisher(brokers []string, topic string) *Publisher {
	return &Publisher{w: &kg.Writer{
		Addr:                   kg.TCP(brokers...),
		Topic:                  topic,
		Balancer:               &kg.Hash{},
		RequiredAcks:           kg.RequireOne, // leader ack: low latency; clicks are not financial data
		BatchTimeout:           5 * time.Millisecond,
		AllowAutoTopicCreation: false, // topics are created explicitly via EnsureTopic
		Async:                  false, // batching/async is handled by events.Async
	}}
}

func (p *Publisher) Publish(ctx context.Context, batch []events.Click) error {
	msgs := make([]kg.Message, 0, len(batch))
	for _, c := range batch {
		m, err := Encode(c)
		if err != nil {
			return err
		}
		msgs = append(msgs, m)
	}
	if err := p.w.WriteMessages(ctx, msgs...); err != nil {
		return fmt.Errorf("kafka publish: %w", err)
	}
	return nil
}

func (p *Publisher) Close() error { return p.w.Close() }

// Encode turns a click into a Kafka message (key = slug, value = JSON).
func Encode(c events.Click) (kg.Message, error) {
	v, err := json.Marshal(c)
	if err != nil {
		return kg.Message{}, fmt.Errorf("encode click: %w", err)
	}
	return kg.Message{Key: []byte(c.Slug), Value: v}, nil
}

// Decode is the inverse of Encode.
func Decode(m kg.Message) (events.Click, error) {
	var c events.Click
	if err := json.Unmarshal(m.Value, &c); err != nil {
		return c, fmt.Errorf("decode click: %w", err)
	}
	return c, nil
}
