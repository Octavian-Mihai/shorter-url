package idgen

import (
	"context"
	"errors"
	"fmt"
	"sync"
)

// BlockSource claims a contiguous, exclusive block of IDs [start, end).
// The Postgres implementation does this with a single atomic UPDATE...RETURNING,
// so concurrent API instances never receive overlapping blocks.
type BlockSource interface {
	ClaimBlock(ctx context.Context, size int64) (start, end int64, err error)
}

// Allocator hands out IDs from a locally held block and only talks to the
// BlockSource when the block is exhausted: one DB round trip per `size` links.
// IDs lost when an instance dies mid-block are simply skipped (gaps are fine).
type Allocator struct {
	src  BlockSource
	size int64

	mu   sync.Mutex
	next int64
	end  int64
}

func NewAllocator(src BlockSource, blockSize int64) (*Allocator, error) {
	if blockSize <= 0 {
		return nil, errors.New("idgen: block size must be positive")
	}
	return &Allocator{src: src, size: blockSize}, nil
}

// Next returns a unique ID. It is safe for concurrent use.
func (a *Allocator) Next(ctx context.Context) (uint64, error) {
	a.mu.Lock()
	defer a.mu.Unlock()
	if a.next >= a.end {
		start, end, err := a.src.ClaimBlock(ctx, a.size)
		if err != nil {
			return 0, fmt.Errorf("idgen: claim block: %w", err)
		}
		if end <= start || start < 0 {
			return 0, fmt.Errorf("idgen: source returned invalid block [%d,%d)", start, end)
		}
		a.next, a.end = start, end
	}
	id := a.next
	a.next++
	return uint64(id), nil
}
