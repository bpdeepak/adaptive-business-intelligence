// Package kafka centralizes franz-go client construction so the producer and
// the server consumers share identical options (broker discovery, offsets, and
// idempotent topic bootstrap).
package kafka

import (
	"context"
	"fmt"
	"time"

	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kgo"
)

// Producer builds a producing client for the given seed brokers.
func Producer(seeds []string) (*kgo.Client, error) {
	return kgo.NewClient(
		kgo.SeedBrokers(seeds...),
		kgo.AllowAutoTopicCreation(),
		kgo.ProducerLinger(20*time.Millisecond),
		kgo.ProducerBatchMaxBytes(1_000_000),
		kgo.RecordPartitioner(kgo.StickyKeyPartitioner(nil)),
	)
}

// Consumer builds a consumer in `group` over `topics`, starting from the
// earliest offset when the group has no committed offsets yet (so a fresh
// bronze-writer / aggregator picks up everything a running producer has
// already emitted).
func Consumer(seeds []string, group string, topics []string) (*kgo.Client, error) {
	return kgo.NewClient(
		kgo.SeedBrokers(seeds...),
		kgo.ConsumerGroup(group),
		kgo.ConsumeTopics(topics...),
		kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()),
		kgo.SessionTimeout(15*time.Second),
		kgo.RetryTimeout(10*time.Second),
	)
}

// EnsureTopics creates any missing topics. Idempotent: existing topics are
// left untouched.
func EnsureTopics(ctx context.Context, seeds []string, topics []string, partitions int32) error {
	adminCl, err := kgo.NewClient(kgo.SeedBrokers(seeds...))
	if err != nil {
		return fmt.Errorf("kafka admin client: %w", err)
	}
	defer adminCl.Close()

	adm := kadm.NewClient(adminCl)
	listed, err := adm.ListTopics(ctx)
	if err != nil {
		return fmt.Errorf("list topics: %w", err)
	}

	var missing []string
	for _, t := range topics {
		_, ok := listed[t]
		if !ok {
			missing = append(missing, t)
		}
	}
	if len(missing) == 0 {
		return nil
	}

	for _, t := range missing {
		if _, err := adm.CreateTopic(ctx, partitions, 1, nil, t); err != nil {
			return fmt.Errorf("create topic %s: %w", t, err)
		}
	}
	return nil
}

// TrimTopics truncates every partition of the given topics to its newest
// offset, discarding all existing records while keeping the topics alive.
// Integration tests call this before replaying a synthetic dataset so a fresh
// consumer group sees exactly the records just produced — regardless of what
// previous test runs left on the broker.
func TrimTopics(ctx context.Context, seeds []string, topics []string, partitions int32) error {
	if err := EnsureTopics(ctx, seeds, topics, partitions); err != nil {
		return err
	}
	adminCl, err := kgo.NewClient(kgo.SeedBrokers(seeds...))
	if err != nil {
		return fmt.Errorf("kafka admin client: %w", err)
	}
	defer adminCl.Close()

	adm := kadm.NewClient(adminCl)
	end, err := adm.ListEndOffsets(ctx, topics...)
	if err != nil {
		return fmt.Errorf("list end offsets: %w", err)
	}
	offs := make(kadm.Offsets, len(end))
	for topic, parts := range end {
		offs[topic] = make(map[int32]kadm.Offset, len(parts))
		for p, lo := range parts {
			if lo.Err != nil {
				return fmt.Errorf("end offset %s/%d: %w", topic, p, lo.Err)
			}
			// Trimming at the newest offset removes every record < offset.
			offs[topic][p] = kadm.Offset{Topic: topic, Partition: p, At: lo.Offset}
		}
	}
	if _, err := adm.DeleteRecords(ctx, offs); err != nil {
		return fmt.Errorf("trim topics: %w", err)
	}
	return nil
}
