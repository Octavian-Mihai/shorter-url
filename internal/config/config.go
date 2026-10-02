// Package config loads service configuration from environment variables.
package config

import (
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"
)

type Config struct {
	HTTPAddr        string
	BaseURL         string // public origin used when building short URLs
	DatabaseURL     string
	RedisAddr       string
	KafkaBrokers    []string
	KafkaTopic      string
	KafkaGroup      string
	KafkaPartitions int
	TrustProxy      bool
	MetricsAddr     string
	EventBackend    string // "kafka" or "sqs"
	SQSQueueURL     string
	SQSEndpoint     string // optional: local emulator
	AWSRegion       string

	BlockSize      int64
	ScrambleSecret uint64

	CacheTTL           time.Duration
	NegativeCacheTTL   time.Duration
	RateLimitBurst     int
	RateLimitPerMinute int

	ConsumerBatchSize  int
	ConsumerFlushEvery time.Duration

	SeedAPIKey string // optional: inserted at startup for demos
}

// Load reads configuration, applying defaults suited to docker compose.
func Load() (*Config, error) {
	var err error
	c := &Config{
		HTTPAddr:     env("HTTP_ADDR", ":8080"),
		BaseURL:      env("BASE_URL", "http://localhost:8080"),
		DatabaseURL:  env("DATABASE_URL", "postgres://shortener:shortener@localhost:5432/shortener?sslmode=disable"),
		RedisAddr:    env("REDIS_ADDR", "localhost:6379"),
		KafkaBrokers: strings.Split(env("KAFKA_BROKERS", "localhost:9092"), ","),
		KafkaTopic:   env("KAFKA_TOPIC", "click-events"),
		KafkaGroup:   env("KAFKA_GROUP", "click-consumer"),
		SeedAPIKey:   os.Getenv("SEED_API_KEY"),
		MetricsAddr:  env("METRICS_ADDR", ":9100"),
		EventBackend: env("EVENT_BACKEND", "kafka"),
		SQSQueueURL:  os.Getenv("SQS_QUEUE_URL"),
		SQSEndpoint:  os.Getenv("SQS_ENDPOINT"),
		AWSRegion:    env("AWS_REGION", "us-east-1"),
	}
	if c.BlockSize, err = envInt64("BLOCK_SIZE", 1000); err != nil {
		return nil, err
	}
	secret, err := envInt64("SCRAMBLE_SECRET", 0x5eed)
	if err != nil {
		return nil, err
	}
	c.ScrambleSecret = uint64(secret)
	if c.CacheTTL, err = envDuration("CACHE_TTL", time.Hour); err != nil {
		return nil, err
	}
	if c.NegativeCacheTTL, err = envDuration("NEGATIVE_CACHE_TTL", 30*time.Second); err != nil {
		return nil, err
	}
	burst, err := envInt64("RATE_LIMIT_BURST", 10)
	if err != nil {
		return nil, err
	}
	perMin, err := envInt64("RATE_LIMIT_PER_MINUTE", 60)
	if err != nil {
		return nil, err
	}
	c.RateLimitBurst, c.RateLimitPerMinute = int(burst), int(perMin)
	batch, err := envInt64("CONSUMER_BATCH_SIZE", 500)
	if err != nil {
		return nil, err
	}
	c.ConsumerBatchSize = int(batch)
	parts, err := envInt64("KAFKA_PARTITIONS", 3)
	if err != nil {
		return nil, err
	}
	c.KafkaPartitions = int(parts)
	c.TrustProxy = os.Getenv("TRUST_PROXY") == "true"
	if c.ConsumerFlushEvery, err = envDuration("CONSUMER_FLUSH_EVERY", 2*time.Second); err != nil {
		return nil, err
	}
	switch c.EventBackend {
	case "kafka":
	case "sqs":
		if c.SQSQueueURL == "" {
			return nil, fmt.Errorf("config: SQS_QUEUE_URL is required when EVENT_BACKEND=sqs")
		}
	default:
		return nil, fmt.Errorf("config: EVENT_BACKEND must be \"kafka\" or \"sqs\", got %q", c.EventBackend)
	}
	return c, nil
}

func env(k, def string) string {
	if v := os.Getenv(k); v != "" {
		return v
	}
	return def
}

func envInt64(k string, def int64) (int64, error) {
	v := os.Getenv(k)
	if v == "" {
		return def, nil
	}
	n, err := strconv.ParseInt(v, 0, 64)
	if err != nil {
		return 0, fmt.Errorf("config: %s: %w", k, err)
	}
	return n, nil
}

func envDuration(k string, def time.Duration) (time.Duration, error) {
	v := os.Getenv(k)
	if v == "" {
		return def, nil
	}
	d, err := time.ParseDuration(v)
	if err != nil {
		return 0, fmt.Errorf("config: %s: %w", k, err)
	}
	return d, nil
}
