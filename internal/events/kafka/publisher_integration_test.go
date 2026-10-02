//go:build integration

package kafka

import (
	"context"
	"os"
	"testing"
	"time"

	kg "github.com/segmentio/kafka-go"

	"github.com/Octavian-Mihai/shorter-url/internal/events"
)

// Run: TEST_KAFKA_BROKERS=localhost:9092 go test -tags integration ./internal/events/kafka
func TestPublishToRealKafka(t *testing.T) {
	broker := os.Getenv("TEST_KAFKA_BROKERS")
	if broker == "" {
		t.Skip("TEST_KAFKA_BROKERS not set")
	}
	topic := "it-clicks-" + time.Now().Format("150405")
	if err := EnsureTopic(context.Background(), []string{broker}, topic, 3, 1); err != nil {
		t.Fatal(err)
	}
	pub := NewPublisher([]string{broker}, topic)
	async := events.NewAsync(pub, events.AsyncOptions{})

	want := map[string]bool{}
	for i := 0; i < 25; i++ {
		c := events.NewClick("slug", time.Now())
		want[c.EventID] = true
		async.Emit(c)
	}
	if err := async.Close(); err != nil {
		t.Fatal(err)
	}
	if async.Failed() != 0 || async.Sent() != 25 {
		t.Fatalf("sent %d failed %d", async.Sent(), async.Failed())
	}

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	msgs := make(chan kg.Message, 64)
	for part := 0; part < 3; part++ {
		r := kg.NewReader(kg.ReaderConfig{Brokers: []string{broker}, Topic: topic, Partition: part, MinBytes: 1, MaxBytes: 1 << 20})
		defer r.Close()
		go func() {
			for {
				m, err := r.ReadMessage(ctx)
				if err != nil {
					return
				}
				msgs <- m
			}
		}()
	}
	for len(want) > 0 {
		var m kg.Message
		select {
		case m = <-msgs:
		case <-ctx.Done():
			t.Fatalf("timeout; missing %d", len(want))
		}
		c, err := Decode(m)
		if err != nil {
			t.Fatal(err)
		}
		delete(want, c.EventID)
	}
}
