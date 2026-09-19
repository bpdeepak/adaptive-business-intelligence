// Package config loads runtime configuration from environment variables.
package config

import (
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"
)

// Config holds all runtime settings for both binaries (producer + server).
type Config struct {
	// HTTPAddr is the listen address of the REST/SSE/dashboard server.
	HTTPAddr string
	// GRPCAddr is the listen address of the gRPC live-metrics server.
	GRPCAddr string
	// DatabaseURL is the Postgres DSN for the gold/semantic layer.
	DatabaseURL string

	// KafkaSeedBrokers is the list of Redpanda/Kafka brokers.
	KafkaSeedBrokers []string
	// ConsumerGroupBronze is the consumer group of the bronze-writer.
	ConsumerGroupBronze string
	// ConsumerGroupRT is the consumer group of the realtime aggregator.
	ConsumerGroupRT string

	// SpeedMultiplier is simulated seconds per wall second (replay pacing).
	SpeedMultiplier float64
	// BotRatio is the fraction of synthetic bot sessions.
	BotRatio float64
	// SessionConversionRate is the fraction of sessions that convert.
	SessionConversionRate float64
	// RingCapacity bounds the producer's in-memory event buffer.
	RingCapacity int
	// RetentionKeep is how long bronze.stream_events rows survive.
	RetentionKeep string
	// RetentionBucketKeep is how long realtime_metrics buckets survive.
	RetentionBucketKeep string
}

// FromEnv builds a Config from environment variables with sane defaults.
func FromEnv() Config {
	return Config{
		HTTPAddr:              getenv("ABI_HTTP_ADDR", ":8080"),
		GRPCAddr:              getenv("ABI_GRPC_ADDR", ":8090"),
		DatabaseURL:           databaseURLFromEnv(),
		KafkaSeedBrokers:      listenv("ABI_KAFKA_SEED_BROKERS", "localhost:29092"),
		ConsumerGroupBronze:   getenv("ABI_CONSUMER_GROUP_BRONZE", "bronze-writer"),
		ConsumerGroupRT:       getenv("ABI_CONSUMER_GROUP_RT", "rt-aggregator"),
		SpeedMultiplier:       floatenv("ABI_SPEED_MULTIPLIER", 2880),
		BotRatio:              floatenv("ABI_BOT_RATIO", 0.02),
		SessionConversionRate: floatenv("ABI_SESSION_CONVERSION_RATE", 0.03),
		RingCapacity:          intenv("ABI_RING_CAPACITY", 4096),
		RetentionKeep:         getenv("ABI_RETENTION_STREAM_EVENTS", "12h"),
		RetentionBucketKeep:   getenv("ABI_RETENTION_METRICS_BUCKETS", "3h"),
	}
}

// SecondsPerDay is the number of simulated seconds in one replayed day.
const SecondsPerDay = 86400

// LoopDurationAtSpeed returns the wall-clock duration of one full replay loop
// at the given pacing, for a data span of `days`.
func LoopDurationAtSpeed(days float64, speed float64) time.Duration {
	simSeconds := days * SecondsPerDay
	return time.Duration(simSeconds / speed * float64(time.Second))
}

// databaseURLFromEnv prefers an explicit DATABASE_URL, otherwise composes one
// from the ABI_PG_* variables shared with docker-compose and dbt.
func databaseURLFromEnv() string {
	if v := os.Getenv("DATABASE_URL"); v != "" {
		return v
	}
	host := getenv("ABI_PG_HOST", "localhost")
	port := getenv("ABI_PG_PORT", "5432")
	user := getenv("ABI_PG_USER", "abi")
	pass := getenv("ABI_PG_PASSWORD", "abi")
	db := getenv("ABI_PG_DB", "abi")
	return fmt.Sprintf("postgres://%s:%s@%s:%s/%s?sslmode=disable", user, pass, host, port, db)
}

func getenv(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}

func listenv(key, fallback string) []string {
	v := strings.TrimSpace(os.Getenv(key))
	if v == "" {
		return []string{fallback}
	}
	parts := strings.Split(v, ",")
	out := make([]string, 0, len(parts))
	for _, p := range parts {
		if p = strings.TrimSpace(p); p != "" {
			out = append(out, p)
		}
	}
	if len(out) == 0 {
		return []string{fallback}
	}
	return out
}

func floatenv(key string, fallback float64) float64 {
	if v := os.Getenv(key); v != "" {
		if f, err := strconv.ParseFloat(v, 64); err == nil && f > 0 {
			return f
		}
	}
	return fallback
}

func intenv(key string, fallback int) int {
	if v := os.Getenv(key); v != "" {
		if n, err := strconv.Atoi(v); err == nil && n > 0 {
			return n
		}
	}
	return fallback
}
