package idgen

import "context"

// IDSource yields unique integers (implemented by *Allocator).
type IDSource interface {
	Next(ctx context.Context) (uint64, error)
}

// Generator produces short codes: unique ID -> scramble -> base62.
type Generator struct {
	ids IDSource
	scr *Scrambler
}

func NewGenerator(ids IDSource, scr *Scrambler) *Generator {
	return &Generator{ids: ids, scr: scr}
}

// NewSlug returns the next unique short code.
func (g *Generator) NewSlug(ctx context.Context) (string, error) {
	id, err := g.ids.Next(ctx)
	if err != nil {
		return "", err
	}
	v, err := g.scr.Scramble(id)
	if err != nil {
		return "", err
	}
	return Encode(v), nil
}
