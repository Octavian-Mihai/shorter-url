package config

import "testing"

func TestBackendValidation(t *testing.T) {
	t.Setenv("EVENT_BACKEND", "sqs")
	t.Setenv("SQS_QUEUE_URL", "")
	if _, err := Load(); err == nil {
		t.Error("sqs without queue url must be rejected")
	}
	t.Setenv("SQS_QUEUE_URL", "http://q")
	if c, err := Load(); err != nil || c.EventBackend != "sqs" {
		t.Errorf("valid sqs: %+v, %v", c, err)
	}
	t.Setenv("EVENT_BACKEND", "rabbit")
	if _, err := Load(); err == nil {
		t.Error("unknown backend must be rejected")
	}
	t.Setenv("EVENT_BACKEND", "")
	if c, err := Load(); err != nil || c.EventBackend != "kafka" {
		t.Errorf("default should be kafka: %+v, %v", c, err)
	}
}

func TestDefaults(t *testing.T) {
	c, err := Load()
	if err != nil {
		t.Fatal(err)
	}
	if c.BlockSize != 1000 || c.RateLimitBurst != 10 || c.KafkaPartitions != 3 || c.MetricsAddr != ":9100" {
		t.Errorf("unexpected defaults: %+v", c)
	}
}
