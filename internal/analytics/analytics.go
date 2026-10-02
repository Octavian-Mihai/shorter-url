// Package analytics defines the per-link statistics read model.
package analytics

import "context"

type DayCount struct {
	Date   string `json:"date"` // YYYY-MM-DD, UTC
	Clicks int64  `json:"clicks"`
}

type RefererCount struct {
	Referer string `json:"referer"`
	Clicks  int64  `json:"clicks"`
}

type Stats struct {
	Slug        string         `json:"slug"`
	TotalClicks int64          `json:"total_clicks"`
	ByDay       []DayCount     `json:"clicks_by_day"`
	TopReferers []RefererCount `json:"top_referers"`
}

// Repository reads aggregated click data. Counts are eventually consistent:
// they trail redirects by the consumer's batching delay.
type Repository interface {
	Stats(ctx context.Context, slug string) (*Stats, error)
}
