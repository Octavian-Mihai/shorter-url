// Package events defines the click-event pipeline between the API (producer)
// and the consumer service. Backends (Kafka today, SQS later) implement
// Publisher; the redirect path only ever talks to Async.
package events

import (
	"context"
	"time"

	"github.com/google/uuid"
)

// Click is one redirect. EventID is generated at redirect time and is the
// idempotency key: the consumer inserts with ON CONFLICT DO NOTHING, so
// at-least-once delivery never double counts.
type Click struct {
	EventID   string    `json:"event_id"`
	Slug      string    `json:"slug"`
	At        time.Time `json:"at"`
	Referer   string    `json:"referer,omitempty"`
	UserAgent string    `json:"user_agent,omitempty"`
	IP        string    `json:"ip,omitempty"`
}

// NewClick stamps a fresh event ID.
func NewClick(slug string, at time.Time) Click {
	return Click{EventID: uuid.NewString(), Slug: slug, At: at.UTC()}
}

// Publisher ships a batch of clicks to the message bus. Batch-shaped because
// both Kafka (WriteMessages) and SQS (SendMessageBatch) amortize round trips.
// Implementations must be safe for concurrent use.
type Publisher interface {
	Publish(ctx context.Context, batch []Click) error
	Close() error
}
