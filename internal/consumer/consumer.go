// Package consumer moves click events from the message bus into Postgres.
//
// Delivery semantics: at-least-once. Offsets are committed only after the
// batch is durably written, so a crash re-delivers events. The sink is
// idempotent (keyed on event_id), so re-delivery never double counts.
package consumer

import (
	"context"
	"errors"
	"log/slog"
	"time"

	"github.com/google/uuid"

	"github.com/Octavian-Mihai/shorter-url/internal/events"
)

// Delivery is one message from the bus. Token is opaque to the consumer and
// handed back to Source.Commit (for Kafka, the original kafka.Message).
type Delivery struct {
	Click  events.Click
	Poison bool // payload could not be decoded
	Token  any
}

// Source is the consuming side of the bus (Kafka today, SQS later).
type Source interface {
	Fetch(ctx context.Context) (Delivery, error)
	Commit(ctx context.Context, batch []Delivery) error
	Close() error
}

// Sink persists clicks. It MUST be idempotent on Click.EventID.
type Sink interface {
	InsertClicks(ctx context.Context, clicks []events.Click) (inserted int64, err error)
}

// Observer receives pipeline events for metrics.
type Observer interface {
	BatchStored(events int, inserted int64, took time.Duration)
	InsertFailed()
	Skipped(n int)
}

type noopObserver struct{}

func (noopObserver) BatchStored(int, int64, time.Duration) {}
func (noopObserver) InsertFailed()                         {}
func (noopObserver) Skipped(int)                           {}

type Options struct {
	Observer   Observer
	BatchSize  int
	FlushEvery time.Duration
	Logger     *slog.Logger
	// MaxBackoff caps the retry delay when the sink fails.
	MaxBackoff time.Duration
	// ShutdownTimeout bounds the final flush after ctx is cancelled.
	ShutdownTimeout time.Duration
}

type Consumer struct {
	src  Source
	sink Sink
	opt  Options
}

func New(src Source, sink Sink, opt Options) *Consumer {
	if opt.BatchSize <= 0 {
		opt.BatchSize = 500
	}
	if opt.FlushEvery <= 0 {
		opt.FlushEvery = 2 * time.Second
	}
	if opt.MaxBackoff <= 0 {
		opt.MaxBackoff = 10 * time.Second
	}
	if opt.ShutdownTimeout <= 0 {
		opt.ShutdownTimeout = 10 * time.Second
	}
	if opt.Logger == nil {
		opt.Logger = slog.Default()
	}
	if opt.Observer == nil {
		opt.Observer = noopObserver{}
	}
	return &Consumer{src: src, sink: sink, opt: opt}
}

// Run consumes until ctx is cancelled, then flushes what it holds and returns.
func (c *Consumer) Run(ctx context.Context) error {
	in := make(chan Delivery)
	fetchDone := make(chan struct{})
	go c.fetchLoop(ctx, in, fetchDone)

	pending := make([]Delivery, 0, c.opt.BatchSize)
	ticker := time.NewTicker(c.opt.FlushEvery)
	defer ticker.Stop()

	flush := func(ctx context.Context) error {
		if len(pending) == 0 {
			return nil
		}
		err := c.flush(ctx, pending)
		if err == nil {
			pending = pending[:0]
		}
		return err
	}

	for {
		select {
		case d := <-in:
			if d.Poison || !valid(d.Click) {
				// Undecodable or invalid: retrying can never succeed and would
				// block the partition forever. Log and skip (a DLQ in prod).
				c.opt.Logger.Error("skipping invalid click event", "event_id", d.Click.EventID, "slug", d.Click.Slug)
			}
			pending = append(pending, d)
			if len(pending) >= c.opt.BatchSize {
				if err := flush(ctx); err != nil {
					return c.finish(ctx, pending, fetchDone, err)
				}
			}
		case <-ticker.C:
			if err := flush(ctx); err != nil {
				return c.finish(ctx, pending, fetchDone, err)
			}
		case <-ctx.Done():
			return c.finish(ctx, pending, fetchDone, nil)
		}
	}
}

// finish drains and flushes remaining deliveries with a fresh context so a
// graceful shutdown does not lose events that were already fetched.
func (c *Consumer) finish(ctx context.Context, pending []Delivery, fetchDone <-chan struct{}, cause error) error {
	if cause != nil && ctx.Err() == nil {
		// Unexpected failure: fetch loop keeps running until ctx is cancelled by
		// the caller; do not block here.
		return cause
	}
	<-fetchDone
	if len(pending) == 0 {
		return nil
	}
	shutdownCtx, cancel := context.WithTimeout(context.Background(), c.opt.ShutdownTimeout)
	defer cancel()
	if err := c.flush(shutdownCtx, pending); err != nil {
		c.opt.Logger.Error("final flush failed; events will be re-delivered", "err", err, "size", len(pending))
		return err
	}
	return nil
}

func (c *Consumer) fetchLoop(ctx context.Context, out chan<- Delivery, done chan<- struct{}) {
	defer close(done)
	backoff := 100 * time.Millisecond
	for {
		d, err := c.src.Fetch(ctx)
		if err != nil {
			if ctx.Err() != nil || errors.Is(err, context.Canceled) {
				return
			}
			c.opt.Logger.Error("fetch failed", "err", err)
			if !sleep(ctx, backoff) {
				return
			}
			backoff = min(backoff*2, c.opt.MaxBackoff)
			continue
		}
		backoff = 100 * time.Millisecond
		select {
		case out <- d:
		case <-ctx.Done():
			return // d is uncommitted; it will be re-delivered
		}
	}
}

// flush writes the valid clicks, then commits offsets for the whole batch
// (including skipped poison messages). Insert is retried with backoff until it
// succeeds or ctx ends; we never commit past data we failed to store.
func (c *Consumer) flush(ctx context.Context, batch []Delivery) error {
	clicks := make([]events.Click, 0, len(batch))
	for _, d := range batch {
		if !d.Poison && valid(d.Click) {
			clicks = append(clicks, d.Click)
		}
	}
	if skipped := len(batch) - len(clicks); skipped > 0 {
		c.opt.Observer.Skipped(skipped)
	}
	backoff := 200 * time.Millisecond
	for len(clicks) > 0 {
		started := time.Now()
		inserted, err := c.sink.InsertClicks(ctx, clicks)
		if err == nil {
			c.opt.Observer.BatchStored(len(clicks), inserted, time.Since(started))
			c.opt.Logger.Info("batch stored", "events", len(clicks), "new", inserted, "duplicates", int64(len(clicks))-inserted)
			break
		}
		c.opt.Observer.InsertFailed()
		c.opt.Logger.Error("insert failed; retrying", "err", err, "backoff", backoff)
		if !sleep(ctx, backoff) {
			return ctx.Err()
		}
		backoff = min(backoff*2, c.opt.MaxBackoff)
	}
	for {
		err := c.src.Commit(ctx, batch)
		if err == nil {
			return nil
		}
		// Failing to commit is safe (events re-deliver and dedupe) but retry briefly.
		c.opt.Logger.Error("commit failed; retrying", "err", err)
		if !sleep(ctx, backoff) {
			return ctx.Err()
		}
	}
}

func valid(c events.Click) bool {
	if c.Slug == "" || len(c.Slug) > 64 {
		return false
	}
	_, err := uuid.Parse(c.EventID)
	return err == nil
}

func sleep(ctx context.Context, d time.Duration) bool {
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-t.C:
		return true
	case <-ctx.Done():
		return false
	}
}
