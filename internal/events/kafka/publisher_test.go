package kafka

import (
	"testing"
	"time"

	"github.com/Octavian-Mihai/shorter-url/internal/events"
)

func TestEncodeDecodeRoundTrip(t *testing.T) {
	c := events.NewClick("abc123", time.Now())
	c.Referer, c.UserAgent, c.IP = "https://r.example", "curl/8", "203.0.113.9"
	m, err := Encode(c)
	if err != nil {
		t.Fatal(err)
	}
	if string(m.Key) != "abc123" {
		t.Errorf("key = %q, want slug (partition affinity)", m.Key)
	}
	got, err := Decode(m)
	if err != nil || got.EventID != c.EventID || got.Slug != c.Slug || !got.At.Equal(c.At) ||
		got.Referer != c.Referer || got.UserAgent != c.UserAgent || got.IP != c.IP {
		t.Fatalf("got %+v, %v", got, err)
	}
}

func TestDecodeGarbage(t *testing.T) {
	m, _ := Encode(events.Click{})
	m.Value = []byte("not json")
	if _, err := Decode(m); err == nil {
		t.Error("expected error")
	}
}
