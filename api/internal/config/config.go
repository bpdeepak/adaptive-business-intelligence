// Package config loads runtime configuration from environment variables.
package config

import (
	"fmt"
	"os"
)

// Config holds all runtime settings for the API server.
type Config struct {
	// HTTPAddr is the listen address, e.g. ":8080".
	HTTPAddr string
	// DatabaseURL is the Postgres DSN for the gold/semantic layer.
	DatabaseURL string
}

// FromEnv builds a Config from environment variables with sane defaults.
func FromEnv() Config {
	return Config{
		HTTPAddr:    getenv("ABI_HTTP_ADDR", ":8080"),
		DatabaseURL: databaseURLFromEnv(),
	}
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