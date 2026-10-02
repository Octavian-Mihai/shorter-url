package consumer

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/Octavian-Mihai/shorter-url/internal/events"
)

// ---- fakes ----

type fakeSource struct {
	mu        sync.Mutex
	queue     []Delivery
	commits   [][]Delivery
	commitLog []string // "commit" / "insert" ordering shared with sink
	order     *orderLog
}

type orderLog struct {
	mu sync.Mutex
	ev []string
}

func (o *orderLog) add(s string) { o.mu.Lock(); o.ev = append(o.ev, s); o.mu.Unlock() }
func (o *orderLog) get() []string {
	o.mu.Lock()
	defer o.mu.Unlock()
	return append([]string(nil), o.ev...)
}

func (s *fakeSource) Fetch(ctx context.Context) (Delivery, error) {
	for {
		s.mu.Lock()
		if len(s.queue) > 0 {
			d := s.queue[0]
			s.queue = s.queue[1:]
			s.mu.Unlock()
			return d, nil
		}
		s.mu.Unlock()
		select {
		case <-ctx.Done():
			return Delivery{}, ctx.Err()
		case <-time.After(2 * time.Millisecond):
		}
	}
}

func (s *fakeSource) Commit(_ context.Context, b []Delivery) error {
	s.mu.Lock()
	s.commits = append(s.commits, append([]Delivery(nil), b...))
	s.mu.Unlock()
	s.order.add("commit")
	return nil
}
func (s *fakeSource) Close() error     { return nil }
func (s *fakeSource) commitCount() int { s.mu.Lock(); defer s.mu.Unlock(); return len(s.commits) }

// fakeSink mimics ON CONFLICT DO NOTHING keyed on EventID.
type fakeSink struct {
	mu       sync.Mutex
	rows     map[string]events.Click
	batches  []int
	failures int // fail this many calls first
	order    *orderLog
}

func newSink(o *orderLog) *fakeSink { return &fakeSink{rows: map[string]events.Click{}, order: o} }

func (k *fakeSink) InsertClicks(_ context.Context, cs []events.Click) (int64, error) {
	k.mu.Lock()
	defer k.mu.Unlock()
	if k.failures > 0 {
		k.failures--
		return 0, errors.New("pg unavailable")
	}
	var n int64
	for _, c := range cs {
		if _, dup := k.rows[c.EventID]; !dup {
			k.rows[c.EventID] = c
			n++
		}
	}
	k.batches = append(k.batches, len(cs))
	k.order.add("insert")
	return n, nil
}

func (k *fakeSink) count() int { k.mu.Lock(); defer k.mu.Unlock(); return len(k.rows) }

func click(slug string) events.Click {
	return events.Click{EventID: uuid.NewString(), Slug: slug, At: time.Now()}
}

func deliveries(n int) []Delivery {
	out := make([]Delivery, n)
	for i := range out {
		out[i] = Delivery{Click: click("abc")}
	}
	return out
}

func waitFor(t *testing.T, cond func() bool, what string) {
	t.Helper()
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatalf("timeout waiting for %s", what)
}

func run(t *testing.T, src Source, sink Sink, opt Options) (cancel func(), done <-chan error) {
	ctx, cancelFn := context.WithCancel(context.Background())
	ch := make(chan error, 1)
	go func() { ch <- New(src, sink, opt).Run(ctx) }()
	return cancelFn, ch
}

// ---- tests ----

func TestFlushesWhenBatchIsFull(t *testing.T) {
	o := &orderLog{}
	src, sink := &fakeSource{queue: deliveries(10), order: o}, newSink(o)
	cancel, done := run(t, src, sink, Options{BatchSize: 5, FlushEvery: time.Hour})
	waitFor(t, func() bool { return sink.count() == 10 }, "10 rows")
	cancel()
	<-done
	if len(sink.batches) != 2 || sink.batches[0] != 5 {
		t.Errorf("batches = %v, want two of 5", sink.batches)
	}
}

func TestFlushesOnTimerWhenBatchNotFull(t *testing.T) {
	o := &orderLog{}
	src, sink := &fakeSource{queue: deliveries(3), order: o}, newSink(o)
	cancel, done := run(t, src, sink, Options{BatchSize: 100, FlushEvery: 30 * time.Millisecond})
	waitFor(t, func() bool { return sink.count() == 3 }, "timer flush")
	cancel()
	<-done
}

func TestCommitsOnlyAfterSuccessfulInsert(t *testing.T) {
	o := &orderLog{}
	src, sink := &fakeSource{queue: deliveries(4), order: o}, newSink(o)
	sink.failures = 2
	cancel, done := run(t, src, sink, Options{BatchSize: 4, FlushEvery: time.Hour, MaxBackoff: 10 * time.Millisecond})
	waitFor(t, func() bool { return src.commitCount() == 1 }, "commit")
	cancel()
	<-done
	if ev := o.get(); len(ev) != 2 || ev[0] != "insert" || ev[1] != "commit" {
		t.Errorf("order = %v, want insert then commit (after 2 failed attempts)", ev)
	}
	if sink.count() != 4 {
		t.Errorf("rows = %d", sink.count())
	}
}

func TestNeverCommitsWhenSinkKeepsFailing(t *testing.T) {
	o := &orderLog{}
	src, sink := &fakeSource{queue: deliveries(3), order: o}, newSink(o)
	sink.failures = 1 << 30
	cancel, done := run(t, src, sink, Options{BatchSize: 3, FlushEvery: time.Hour, MaxBackoff: 5 * time.Millisecond, ShutdownTimeout: 100 * time.Millisecond})
	time.Sleep(100 * time.Millisecond)
	cancel()
	select {
	case <-done:
	case <-time.After(3 * time.Second):
		t.Fatal("Run did not return after cancel")
	}
	if src.commitCount() != 0 {
		t.Error("must not commit offsets for data that was never stored")
	}
}

func TestPoisonAndInvalidAreSkippedButCommitted(t *testing.T) {
	o := &orderLog{}
	q := deliveries(2)
	q = append(q, Delivery{Poison: true})
	q = append(q, Delivery{Click: events.Click{EventID: "not-a-uuid", Slug: "abc"}})
	q = append(q, Delivery{Click: events.Click{EventID: uuid.NewString(), Slug: ""}})
	src, sink := &fakeSource{queue: q, order: o}, newSink(o)
	cancel, done := run(t, src, sink, Options{BatchSize: 5, FlushEvery: time.Hour})
	waitFor(t, func() bool { return src.commitCount() == 1 }, "commit")
	cancel()
	<-done
	if sink.count() != 2 {
		t.Errorf("stored %d, want only the 2 valid clicks", sink.count())
	}
	if len(src.commits[0]) != 5 {
		t.Errorf("committed %d deliveries, want all 5 (poison must not block the partition)", len(src.commits[0]))
	}
}

func TestShutdownFlushesPartialBatch(t *testing.T) {
	o := &orderLog{}
	src, sink := &fakeSource{queue: deliveries(7), order: o}, newSink(o)
	cancel, done := run(t, src, sink, Options{BatchSize: 100, FlushEvery: time.Hour})
	time.Sleep(50 * time.Millisecond) // let them be fetched into the pending batch
	cancel()
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	if sink.count() != 7 || src.commitCount() != 1 {
		t.Errorf("rows %d commits %d, want 7 and 1", sink.count(), src.commitCount())
	}
}

// Simulates a crash after the DB write but before the offset commit: the same
// messages are delivered again to a fresh consumer. Idempotent sink => no dupes.
func TestRedeliveryAfterCrashDoesNotDoubleCount(t *testing.T) {
	o := &orderLog{}
	batch := deliveries(6)
	sink := newSink(o)

	for round := 0; round < 3; round++ {
		src := &fakeSource{queue: append([]Delivery(nil), batch...), order: o}
		cancel, done := run(t, src, sink, Options{BatchSize: 6, FlushEvery: time.Hour})
		waitFor(t, func() bool { return src.commitCount() == 1 }, "commit")
		cancel()
		<-done
	}
	if sink.count() != 6 {
		t.Errorf("rows = %d after 3 deliveries of the same 6 events, want 6", sink.count())
	}
}
