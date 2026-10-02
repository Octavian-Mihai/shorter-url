package link

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// ---- fakes ----

type fakeRepo struct {
	mu    sync.Mutex
	links map[string]*Link
	gets  atomic.Int32
	delay time.Duration
	err   error
}

func newFakeRepo() *fakeRepo { return &fakeRepo{links: map[string]*Link{}} }

func (r *fakeRepo) Create(_ context.Context, l *Link) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	if _, ok := r.links[l.Slug]; ok {
		return ErrSlugTaken
	}
	cp := *l
	r.links[l.Slug] = &cp
	return nil
}

func (r *fakeRepo) Get(_ context.Context, slug string) (*Link, error) {
	r.gets.Add(1)
	time.Sleep(r.delay)
	if r.err != nil {
		return nil, r.err
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	l, ok := r.links[slug]
	if !ok {
		return nil, ErrNotFound
	}
	cp := *l
	return &cp, nil
}

type setCall struct {
	e   CacheEntry
	ttl time.Duration
}

type fakeCache struct {
	mu     sync.Mutex
	m      map[string]CacheEntry
	sets   map[string]setCall
	getErr error
	setErr error
}

func newFakeCache() *fakeCache {
	return &fakeCache{m: map[string]CacheEntry{}, sets: map[string]setCall{}}
}

func (c *fakeCache) Get(_ context.Context, slug string) (*CacheEntry, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.getErr != nil {
		return nil, c.getErr
	}
	e, ok := c.m[slug]
	if !ok {
		return nil, nil
	}
	return &e, nil
}

func (c *fakeCache) Set(_ context.Context, slug string, e CacheEntry, ttl time.Duration) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.setErr != nil {
		return c.setErr
	}
	c.m[slug] = e
	c.sets[slug] = setCall{e, ttl}
	return nil
}

type seqGen struct {
	mu   sync.Mutex
	list []string
}

func (g *seqGen) NewSlug(context.Context) (string, error) {
	g.mu.Lock()
	defer g.mu.Unlock()
	s := g.list[0]
	if len(g.list) > 1 {
		g.list = g.list[1:]
	}
	return s, nil
}

var t0 = time.Date(2026, 1, 1, 12, 0, 0, 0, time.UTC)

func newSvc(repo *fakeRepo, c *fakeCache, slugs ...string) *Service {
	if len(slugs) == 0 {
		slugs = []string{"gen0001"}
	}
	return NewService(repo, c, &seqGen{list: slugs}, Options{
		CacheTTL: time.Hour, NegativeCacheTTL: 30 * time.Second,
		SelfHost: "short.test", Now: func() time.Time { return t0 },
	})
}

// ---- redirect path ----

func TestResolveCacheHitSkipsDB(t *testing.T) {
	repo, c := newFakeRepo(), newFakeCache()
	c.m["abc"] = CacheEntry{URL: "https://example.com"}
	url, err := newSvc(repo, c).Resolve(context.Background(), "abc")
	if err != nil || url != "https://example.com" {
		t.Fatalf("got %q, %v", url, err)
	}
	if repo.gets.Load() != 0 {
		t.Error("db must not be touched on cache hit")
	}
}

func TestResolveMissFillsCache(t *testing.T) {
	repo, c := newFakeRepo(), newFakeCache()
	repo.links["abc"] = &Link{Slug: "abc", URL: "https://example.com"}
	svc := newSvc(repo, c)
	for i := 0; i < 3; i++ {
		if url, err := svc.Resolve(context.Background(), "abc"); err != nil || url != "https://example.com" {
			t.Fatalf("got %q, %v", url, err)
		}
	}
	if repo.gets.Load() != 1 {
		t.Errorf("db gets = %d, want 1", repo.gets.Load())
	}
	if c.sets["abc"].ttl != time.Hour {
		t.Errorf("ttl = %v", c.sets["abc"].ttl)
	}
}

func TestResolveUnknownIsNegativelyCached(t *testing.T) {
	repo, c := newFakeRepo(), newFakeCache()
	svc := newSvc(repo, c)
	for i := 0; i < 3; i++ {
		if _, err := svc.Resolve(context.Background(), "ghost"); !errors.Is(err, ErrNotFound) {
			t.Fatalf("err = %v", err)
		}
	}
	if repo.gets.Load() != 1 {
		t.Errorf("db gets = %d, want 1", repo.gets.Load())
	}
	if got := c.sets["ghost"]; !got.e.Missing || got.ttl != 30*time.Second {
		t.Errorf("negative set = %+v", got)
	}
}

func TestResolveExpiredInDB(t *testing.T) {
	repo, c := newFakeRepo(), newFakeCache()
	past := t0.Add(-time.Minute)
	repo.links["old"] = &Link{Slug: "old", URL: "https://example.com", ExpiresAt: &past}
	svc := newSvc(repo, c)
	for i := 0; i < 2; i++ {
		if _, err := svc.Resolve(context.Background(), "old"); !errors.Is(err, ErrExpired) {
			t.Fatalf("err = %v", err)
		}
	}
	if repo.gets.Load() != 1 {
		t.Errorf("expired lookups should be cached; gets = %d", repo.gets.Load())
	}
}

func TestResolveCachedEntryExpiresByClock(t *testing.T) {
	repo, c := newFakeRepo(), newFakeCache()
	past := t0.Add(-time.Second)
	c.m["abc"] = CacheEntry{URL: "https://example.com", ExpiresAt: &past}
	if _, err := newSvc(repo, c).Resolve(context.Background(), "abc"); !errors.Is(err, ErrExpired) {
		t.Fatalf("err = %v", err)
	}
}

func TestResolveCacheTTLCappedByExpiry(t *testing.T) {
	repo, c := newFakeRepo(), newFakeCache()
	soon := t0.Add(90 * time.Second)
	repo.links["abc"] = &Link{Slug: "abc", URL: "https://example.com", ExpiresAt: &soon}
	if _, err := newSvc(repo, c).Resolve(context.Background(), "abc"); err != nil {
		t.Fatal(err)
	}
	if got := c.sets["abc"].ttl; got != 90*time.Second {
		t.Errorf("ttl = %v, want 90s", got)
	}
}

func TestResolveCacheFailureFallsBackToDB(t *testing.T) {
	repo, c := newFakeRepo(), newFakeCache()
	c.getErr, c.setErr = errors.New("redis down"), errors.New("redis down")
	repo.links["abc"] = &Link{Slug: "abc", URL: "https://example.com"}
	url, err := newSvc(repo, c).Resolve(context.Background(), "abc")
	if err != nil || url != "https://example.com" {
		t.Fatalf("got %q, %v", url, err)
	}
}

func TestResolveDBErrorNotCached(t *testing.T) {
	repo, c := newFakeRepo(), newFakeCache()
	repo.err = errors.New("pg down")
	if _, err := newSvc(repo, c).Resolve(context.Background(), "abc"); err == nil {
		t.Fatal("expected error")
	}
	if len(c.sets) != 0 {
		t.Error("errors must not poison the cache")
	}
}

func TestResolveCoalescesConcurrentMisses(t *testing.T) {
	repo, c := newFakeRepo(), newFakeCache()
	repo.delay = 50 * time.Millisecond
	repo.links["hot"] = &Link{Slug: "hot", URL: "https://example.com"}
	svc := newSvc(repo, c)
	var wg sync.WaitGroup
	for i := 0; i < 50; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if _, err := svc.Resolve(context.Background(), "hot"); err != nil {
				t.Error(err)
			}
		}()
	}
	wg.Wait()
	if n := repo.gets.Load(); n != 1 {
		t.Errorf("db gets = %d, want 1 (singleflight)", n)
	}
}

// ---- create ----

func TestCreateGenerated(t *testing.T) {
	repo := newFakeRepo()
	l, err := newSvc(repo, newFakeCache(), "aaa1111").Create(context.Background(),
		CreateInput{URL: "https://example.com/path?q=1", APIKeyID: 7})
	if err != nil || l.Slug != "aaa1111" || l.IsCustom || repo.links["aaa1111"].APIKeyID != 7 {
		t.Fatalf("got %+v, %v", l, err)
	}
}

func TestCreateRetriesOnSlugCollision(t *testing.T) {
	repo := newFakeRepo()
	repo.links["taken01"] = &Link{Slug: "taken01"}
	l, err := newSvc(repo, newFakeCache(), "taken01", "free002").Create(context.Background(),
		CreateInput{URL: "https://example.com"})
	if err != nil || l.Slug != "free002" {
		t.Fatalf("got %+v, %v", l, err)
	}
}

func TestCreateGivesUpAfterRepeatedCollisions(t *testing.T) {
	repo := newFakeRepo()
	repo.links["same"] = &Link{Slug: "same"}
	if _, err := newSvc(repo, newFakeCache(), "same").Create(context.Background(),
		CreateInput{URL: "https://example.com"}); err == nil {
		t.Fatal("expected error")
	}
}

func TestCreateCustomAlias(t *testing.T) {
	repo := newFakeRepo()
	svc := newSvc(repo, newFakeCache())
	l, err := svc.Create(context.Background(), CreateInput{URL: "https://example.com", Alias: "my-link_1"})
	if err != nil || l.Slug != "my-link_1" || !l.IsCustom {
		t.Fatalf("got %+v, %v", l, err)
	}
	if _, err := svc.Create(context.Background(), CreateInput{URL: "https://example.com", Alias: "my-link_1"}); !errors.Is(err, ErrSlugTaken) {
		t.Errorf("duplicate alias err = %v", err)
	}
}

func TestCreateValidation(t *testing.T) {
	svc := newSvc(newFakeRepo(), newFakeCache())
	past := t0.Add(-time.Hour)
	long := "https://example.com/" + string(make([]byte, 2100))
	cases := []struct {
		name string
		in   CreateInput
		want error
	}{
		{"empty url", CreateInput{}, ErrInvalidURL},
		{"no scheme", CreateInput{URL: "example.com"}, ErrInvalidURL},
		{"ftp", CreateInput{URL: "ftp://example.com"}, ErrInvalidURL},
		{"javascript", CreateInput{URL: "javascript:alert(1)"}, ErrInvalidURL},
		{"no host", CreateInput{URL: "https://"}, ErrInvalidURL},
		{"self host", CreateInput{URL: "https://short.test/abc"}, ErrInvalidURL},
		{"too long", CreateInput{URL: long}, ErrInvalidURL},
		{"short alias", CreateInput{URL: "https://a.io", Alias: "ab"}, ErrInvalidAlias},
		{"bad chars", CreateInput{URL: "https://a.io", Alias: "a/b!c"}, ErrInvalidAlias},
		{"reserved", CreateInput{URL: "https://a.io", Alias: "Docs"}, ErrInvalidAlias},
		{"past expiry", CreateInput{URL: "https://a.io", ExpiresAt: &past}, ErrInvalidExpiry},
	}
	for _, tc := range cases {
		if _, err := svc.Create(context.Background(), tc.in); !errors.Is(err, tc.want) {
			t.Errorf("%s: err = %v, want %v", tc.name, err, tc.want)
		}
	}
}
