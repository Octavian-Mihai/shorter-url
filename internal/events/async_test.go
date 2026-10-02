package events

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"
)

type fakePub struct {
	mu     sync.Mutex
	got    []Click
	calls  int
	block  chan struct{} // if non-nil, Publish waits on it
	err    error
	closed bool
}

func (f *fakePub) Publish(ctx context.Context, b []Click) error {
	if f.block != nil {
		select {
		case <-f.block:
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	f.calls++
	if f.err != nil {
		return f.err
	}
	f.got = append(f.got, b...)
	return nil
}

func (f *fakePub) Close() error { f.closed = true; return nil }

func (f *fakePub) count() int { f.mu.Lock(); defer f.mu.Unlock(); return len(f.got) }

func TestAsyncDeliversAndBatches(t *testing.T) {
	p := &fakePub{}
	a := NewAsync(p, AsyncOptions{Workers: 1, MaxBatch: 50, Linger: 20 * time.Millisecond})
	for i := 0; i < 200; i++ {
		a.Emit(NewClick("abc", time.Now()))
	}
	if err := a.Close(); err != nil {
		t.Fatal(err)
	}
	if p.count() != 200 || a.Sent() != 200 || a.Dropped() != 0 {
		t.Errorf("got %d sent %d dropped %d", p.count(), a.Sent(), a.Dropped())
	}
	if p.calls >= 200 {
		t.Errorf("expected batching, got %d publish calls", p.calls)
	}
	if !p.closed {
		t.Error("underlying publisher not closed")
	}
}

func TestAsyncEmitNeverBlocksWhenBackendStalls(t *testing.T) {
	p := &fakePub{block: make(chan struct{})}
	a := NewAsync(p, AsyncOptions{Buffer: 10, Workers: 1, MaxBatch: 1, PublishTimeout: time.Second})

	done := make(chan struct{})
	go func() {
		for i := 0; i < 1000; i++ {
			a.Emit(NewClick("abc", time.Now()))
		}
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("Emit blocked while backend was stalled")
	}
	if a.Dropped() < 900 {
		t.Errorf("dropped = %d, want most of 1000 dropped with buffer 10", a.Dropped())
	}
	close(p.block)
	_ = a.Close()
}

func TestAsyncPublishErrorIsCountedNotFatal(t *testing.T) {
	p := &fakePub{err: errors.New("broker down")}
	a := NewAsync(p, AsyncOptions{Workers: 1})
	for i := 0; i < 5; i++ {
		a.Emit(NewClick("abc", time.Now()))
	}
	_ = a.Close()
	if a.Failed() != 5 || a.Sent() != 0 {
		t.Errorf("failed %d sent %d", a.Failed(), a.Sent())
	}
}

func TestAsyncCloseIsIdempotent(t *testing.T) {
	a := NewAsync(&fakePub{}, AsyncOptions{})
	if err := a.Close(); err != nil {
		t.Fatal(err)
	}
	if err := a.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestNewClickUniqueIDsAndUTC(t *testing.T) {
	loc := time.FixedZone("x", 3600)
	a, b := NewClick("s", time.Now().In(loc)), NewClick("s", time.Now())
	if a.EventID == "" || a.EventID == b.EventID {
		t.Error("event ids must be unique")
	}
	if a.At.Location() != time.UTC {
		t.Error("timestamp must be UTC")
	}
}
