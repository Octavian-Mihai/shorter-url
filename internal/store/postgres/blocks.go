package postgres

import (
	"context"
	"fmt"

	"github.com/jackc/pgx/v5/pgxpool"
)

// BlockSource implements idgen.BlockSource on top of the id_sequences table.
type BlockSource struct {
	pool *pgxpool.Pool
	name string
}

func NewBlockSource(pool *pgxpool.Pool) *BlockSource {
	return &BlockSource{pool: pool, name: "links"}
}

// ClaimBlock atomically reserves [start, end). A single UPDATE takes a row
// lock, so concurrent instances serialize briefly and get disjoint ranges.
// No explicit transaction or SELECT FOR UPDATE is needed.
func (b *BlockSource) ClaimBlock(ctx context.Context, size int64) (int64, int64, error) {
	var end int64
	err := b.pool.QueryRow(ctx,
		`UPDATE id_sequences SET next_id = next_id + $1 WHERE name = $2 RETURNING next_id`,
		size, b.name).Scan(&end)
	if err != nil {
		return 0, 0, fmt.Errorf("postgres: claim block: %w", err)
	}
	return end - size, end, nil
}
