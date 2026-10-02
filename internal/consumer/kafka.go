package consumer

import (
	"context"

	kg "github.com/segmentio/kafka-go"

	"github.com/Octavian-Mihai/shorter-url/internal/events/kafka"
)

// KafkaSource reads click events as a member of a consumer group, so running
// more consumer instances splits the topic's partitions between them.
type KafkaSource struct{ r *kg.Reader }

var _ Source = (*KafkaSource)(nil)

func NewKafkaSource(brokers []string, topic, group string) *KafkaSource {
	return &KafkaSource{r: kg.NewReader(kg.ReaderConfig{
		Brokers:     brokers,
		GroupID:     group,
		Topic:       topic,
		MinBytes:    1,
		MaxBytes:    10 << 20,
		MaxWait:     500e6,          // 500ms
		StartOffset: kg.FirstOffset, // a brand-new group must not skip existing clicks
		// CommitInterval is 0: offsets are committed manually, after the DB write.
	})}
}

func (s *KafkaSource) Fetch(ctx context.Context) (Delivery, error) {
	m, err := s.r.FetchMessage(ctx)
	if err != nil {
		return Delivery{}, err
	}
	click, derr := kafka.Decode(m)
	return Delivery{Click: click, Poison: derr != nil, Token: m}, nil
}

func (s *KafkaSource) Commit(ctx context.Context, batch []Delivery) error {
	msgs := make([]kg.Message, 0, len(batch))
	for _, d := range batch {
		msgs = append(msgs, d.Token.(kg.Message))
	}
	return s.r.CommitMessages(ctx, msgs...)
}

func (s *KafkaSource) Close() error { return s.r.Close() }
