package kafka

import (
	"context"
	"fmt"
	"net"
	"strconv"
	"time"

	kg "github.com/segmentio/kafka-go"
)

// EnsureTopic creates the topic if it does not exist (idempotent) and waits
// until it has partition leaders. Services call this at startup instead of
// relying on broker auto-creation, which races with the first write and
// would give us a default partition count.
func EnsureTopic(ctx context.Context, brokers []string, topic string, partitions, replication int) error {
	var lastErr error
	for attempt := 0; attempt < 30; attempt++ {
		if lastErr = ensureOnce(brokers, topic, partitions, replication); lastErr == nil {
			return nil
		}
		select {
		case <-ctx.Done():
			return fmt.Errorf("ensure topic: %w (last: %v)", ctx.Err(), lastErr)
		case <-time.After(time.Second): // broker may still be starting
		}
	}
	return fmt.Errorf("ensure topic: %w", lastErr)
}

func ensureOnce(brokers []string, topic string, partitions, replication int) error {
	conn, err := kg.Dial("tcp", brokers[0])
	if err != nil {
		return err
	}
	defer conn.Close()
	ctrl, err := conn.Controller()
	if err != nil {
		return err
	}
	cc, err := kg.Dial("tcp", net.JoinHostPort(ctrl.Host, strconv.Itoa(ctrl.Port)))
	if err != nil {
		return err
	}
	defer cc.Close()
	// Creating an existing topic is a no-op success in Kafka.
	if err := cc.CreateTopics(kg.TopicConfig{Topic: topic, NumPartitions: partitions, ReplicationFactor: replication}); err != nil {
		return err
	}
	parts, err := cc.ReadPartitions(topic)
	if err != nil {
		return err
	}
	if len(parts) == 0 {
		return fmt.Errorf("topic %q has no partitions yet", topic)
	}
	for _, p := range parts {
		if p.Leader.ID < 0 || p.Leader.Host == "" {
			return fmt.Errorf("topic %q partition %d has no leader yet", topic, p.ID)
		}
	}
	return nil
}
