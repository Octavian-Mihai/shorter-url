package idgen

import (
	"context"
	"errors"
	"sync"
	"testing"
)

func TestEncodeDecodeRoundTrip(t *testing.T) {
	cases := []uint64{0, 1, 61, 62, 63, 3843, 3844, 1 << 40, 1<<64 - 1}
	for _, n := range cases {
		got, err := Decode(Encode(n))
		if err != nil || got != n {
			t.Errorf("round trip %d: got %d, err %v", n, got, err)
		}
	}
}

func TestEncodeKnownValues(t *testing.T) {
	cases := map[uint64]string{0: "0", 9: "9", 10: "A", 35: "Z", 36: "a", 61: "z", 62: "10", 3844: "100"}
	for n, want := range cases {
		if got := Encode(n); got != want {
			t.Errorf("Encode(%d) = %q, want %q", n, got, want)
		}
	}
}

func TestDecodeInvalid(t *testing.T) {
	for _, s := range []string{"", "ab-c", "a b", "é"} {
		if _, err := Decode(s); !errors.Is(err, ErrInvalidSlug) {
			t.Errorf("Decode(%q) err = %v, want ErrInvalidSlug", s, err)
		}
	}
}

func TestScrambleRoundTripAndBounds(t *testing.T) {
	s := NewScrambler(42)
	for _, id := range []uint64{0, 1, 2, 999, 123456789, MaxID - 1} {
		v, err := s.Scramble(id)
		if err != nil || v >= MaxID {
			t.Fatalf("Scramble(%d) = %d, %v", id, v, err)
		}
		back, err := s.Unscramble(v)
		if err != nil || back != id {
			t.Errorf("Unscramble(Scramble(%d)) = %d, %v", id, back, err)
		}
	}
	if _, err := s.Scramble(MaxID); err == nil {
		t.Error("expected error at MaxID")
	}
}

func TestScrambleNoCollisions(t *testing.T) {
	s := NewScrambler(7)
	seen := make(map[uint64]uint64, 200000)
	for id := uint64(0); id < 200000; id++ {
		v, _ := s.Scramble(id)
		if prev, ok := seen[v]; ok {
			t.Fatalf("collision: ids %d and %d -> %d", prev, id, v)
		}
		seen[v] = id
	}
}

func TestScrambleDependsOnSecret(t *testing.T) {
	a, _ := NewScrambler(1).Scramble(100)
	b, _ := NewScrambler(2).Scramble(100)
	if a == b {
		t.Error("different secrets produced identical output")
	}
}

func TestSlugLengthBounded(t *testing.T) {
	s := NewScrambler(9)
	for id := uint64(0); id < 10000; id++ {
		v, _ := s.Scramble(id)
		if l := len(Encode(v)); l > 7 {
			t.Fatalf("slug length %d > 7 for id %d", l, id)
		}
	}
}

// fakeSource hands out sequential blocks and counts calls.
type fakeSource struct {
	mu    sync.Mutex
	next  int64
	calls int
	err   error
}

func (f *fakeSource) ClaimBlock(_ context.Context, size int64) (int64, int64, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.err != nil {
		return 0, 0, f.err
	}
	f.calls++
	start := f.next
	f.next += size
	return start, f.next, nil
}

func TestAllocatorClaimsOneBlockPerSize(t *testing.T) {
	src := &fakeSource{}
	a, _ := NewAllocator(src, 10)
	for i := 0; i < 25; i++ {
		id, err := a.Next(context.Background())
		if err != nil || id != uint64(i) {
			t.Fatalf("Next #%d = %d, %v", i, id, err)
		}
	}
	if src.calls != 3 {
		t.Errorf("claim calls = %d, want 3", src.calls)
	}
}

func TestAllocatorConcurrentUnique(t *testing.T) {
	src := &fakeSource{}
	a, _ := NewAllocator(src, 7)
	const workers, per = 16, 500
	var wg sync.WaitGroup
	out := make(chan uint64, workers*per)
	for w := 0; w < workers; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < per; i++ {
				id, err := a.Next(context.Background())
				if err != nil {
					t.Error(err)
					return
				}
				out <- id
			}
		}()
	}
	wg.Wait()
	close(out)
	seen := map[uint64]bool{}
	for id := range out {
		if seen[id] {
			t.Fatalf("duplicate id %d", id)
		}
		seen[id] = true
	}
	if len(seen) != workers*per {
		t.Errorf("got %d ids, want %d", len(seen), workers*per)
	}
}

func TestAllocatorTwoInstancesDisjoint(t *testing.T) {
	src := &fakeSource{} // shared "database"
	a1, _ := NewAllocator(src, 5)
	a2, _ := NewAllocator(src, 5)
	seen := map[uint64]bool{}
	for i := 0; i < 20; i++ {
		for _, a := range []*Allocator{a1, a2} {
			id, _ := a.Next(context.Background())
			if seen[id] {
				t.Fatalf("duplicate %d across instances", id)
			}
			seen[id] = true
		}
	}
}

func TestAllocatorErrors(t *testing.T) {
	if _, err := NewAllocator(&fakeSource{}, 0); err == nil {
		t.Error("expected error for zero block size")
	}
	boom := errors.New("db down")
	a, _ := NewAllocator(&fakeSource{err: boom}, 5)
	if _, err := a.Next(context.Background()); !errors.Is(err, boom) {
		t.Errorf("err = %v, want wrapped boom", err)
	}
}

func TestGeneratorSlugsUniqueAndDecodable(t *testing.T) {
	a, _ := NewAllocator(&fakeSource{}, 100)
	scr := NewScrambler(99)
	g := NewGenerator(a, scr)
	seen := map[string]bool{}
	for i := 0; i < 5000; i++ {
		slug, err := g.NewSlug(context.Background())
		if err != nil {
			t.Fatal(err)
		}
		if seen[slug] || !ValidAlphabet(slug) {
			t.Fatalf("bad slug %q", slug)
		}
		seen[slug] = true
		v, _ := Decode(slug)
		id, _ := scr.Unscramble(v)
		if id != uint64(i) {
			t.Fatalf("slug %q decodes to id %d, want %d", slug, id, i)
		}
	}
}
