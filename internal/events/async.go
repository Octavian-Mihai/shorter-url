package events

import (
	"context"
	"log/slog"
	"sync"
	"sync/atomic"
	"time"
)

type AsyncOptions struct {
	Buffer         int           // bounded queue size; full => drop
	Workers        int           // concurrent publishers
	MaxBatch       int           // max clicks per Publish call
	Linger         time.Duration // wait this long to fill a batch
	PublishTimeout time.Duration // per Publish call
	Logger         *slog.Logger
}

func (o *AsyncOptions) defaults() {
	if o.Buffer <= 0 {
		o.Buffer = 10000
	}
	if o.Workers <= 0 {
		o.Workers = 2
	}
	if o.MaxBatch <= 0 {
		o.MaxBatch = 100
	}
	if o.Linger <= 0 {
		o.Linger = 5 * time.Millisecond
	}
	if o.PublishTimeout <= 0 {
		o.PublishTimeout = 5 * time.Second
	}
	if o.Logger == nil {
		o.Logger = slog.Default()
	}
}

// Async decouples the redirect from the message bus. Emit never blocks and
// never fails: if the queue is full (bus slow or down) the click is dropped
// and counted. Trade-off: analytics are best-effort under overload, but the
// redirect latency and availability never depend on Kafka.
type Async struct {
	pub  Publisher
	opt  AsyncOptions
	ch   chan Click
	wg   sync.WaitGroup
	once sync.Once

	dropped atomic.Int64
	failed  atomic.Int64 // clicks lost because Publish returned an error
	sent    atomic.Int64
}

func NewAsync(pub Publisher, opt AsyncOptions) *Async {
	opt.defaults()
	a := &Async{pub: pub, opt: opt, ch: make(chan Click, opt.Buffer)}
	for i := 0; i < opt.Workers; i++ {
		a.wg.Add(1)
		go a.worker()
	}
	return a
}

// Emit enqueues c without blocking. Must not be called after Close.
func (a *Async) Emit(c Click) {
	select {
	case a.ch <- c:
	default:
		if n := a.dropped.Add(1); n == 1 || n%1000 == 0 {
			a.opt.Logger.Warn("click queue full; dropping events", "dropped_total", n)
		}
	}
}

func (a *Async) QueueLen() int  { return len(a.ch) }
func (a *Async) Dropped() int64 { return a.dropped.Load() }
func (a *Async) Failed() int64  { return a.failed.Load() }
func (a *Async) Sent() int64    { return a.sent.Load() }

func (a *Async) worker() {
	defer a.wg.Done()
	batch := make([]Click, 0, a.opt.MaxBatch)
	for first := range a.ch { // blocks until work; exits when ch is closed and drained
		batch = append(batch[:0], first)
		timer := time.NewTimer(a.opt.Linger)
	fill:
		for len(batch) < a.opt.MaxBatch {
			select {
			case c, ok := <-a.ch:
				if !ok {
					break fill
				}
				batch = append(batch, c)
			case <-timer.C:
				break fill
			}
		}
		timer.Stop()
		a.flush(batch)
	}
}

func (a *Async) flush(batch []Click) {
	ctx, cancel := context.WithTimeout(context.Background(), a.opt.PublishTimeout)
	defer cancel()
	if err := a.pub.Publish(ctx, batch); err != nil {
		a.failed.Add(int64(len(batch)))
		a.opt.Logger.Error("publish click batch failed", "err", err, "size", len(batch))
		return
	}
	a.sent.Add(int64(len(batch)))
}

// Close stops accepting events, flushes what is queued, and closes the
// underlying publisher. Call after the HTTP server has stopped.
func (a *Async) Close() error {
	var err error
	a.once.Do(func() {
		close(a.ch)
		a.wg.Wait()
		err = a.pub.Close()
	})
	return err
}
