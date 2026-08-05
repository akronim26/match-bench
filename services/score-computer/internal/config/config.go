// Package config implements config behavior.
//
// This file is part of the IICPC benchmarking platform and keeps its
// responsibilities local to the surrounding package. It should be read with
// the service-level design in design.md for broader operational context.
package config

import (
	"log/slog"
	"os"
	"strconv"
	"strings"
)

// Config groups the state and dependencies used by this package.
// Keep this type aligned with the runtime contract around it.
type Config struct {
	Port                 string
	MetadataDatabaseURL  string
	TimescaleDatabaseURL string
	RedisAddr            string
	KafkaBrokers         []string
	StatusGroup          string
	CorrectnessGroup     string
	Concurrency          int
	MetricsAddr          string
}

// Load performs the package-specific operation described by its name.
// It keeps validation, side effects, and returned values within this package's contract.
func Load(log *slog.Logger) Config {
	return Config{
		Port:                 envOr("PORT", "8080"),
		MetadataDatabaseURL:  mustEnv("DATABASE_URL", log),
		TimescaleDatabaseURL: mustEnv("TIMESCALE_URL", log),
		RedisAddr:            envOr("REDIS_ADDR", "redis.data.svc:6379"),
		KafkaBrokers:         parseBrokers(mustEnv("KAFKA_BROKERS", log)),
		StatusGroup:          envOr("KAFKA_STATUS_GROUP", "score-computer"),
		CorrectnessGroup:     envOr("KAFKA_CORRECTNESS_GROUP", "score-computer-correctness"),
		Concurrency:          envInt("SCORER_CONCURRENCY", 4),
		MetricsAddr:          envOr("METRICS_ADDR", "0.0.0.0:9090"),
	}
}

// envOr performs the package-specific operation described by its name.
// It keeps validation, side effects, and returned values within this package's contract.
func envOr(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}

// mustEnv performs the package-specific operation described by its name.
// It keeps validation, side effects, and returned values within this package's contract.
func mustEnv(key string, log *slog.Logger) string {
	v := os.Getenv(key)
	if v == "" {
		log.Error("required env var missing", "var", key)
		os.Exit(1)
	}
	return v
}

// envInt performs the package-specific operation described by its name.
// It keeps validation, side effects, and returned values within this package's contract.
func envInt(key string, def int) int {
	v := os.Getenv(key)
	if v == "" {
		return def
	}
	n, err := strconv.Atoi(v)
	if err != nil || n <= 0 {
		return def
	}
	return n
}

// parseBrokers performs the package-specific operation described by its name.
// It keeps validation, side effects, and returned values within this package's contract.
func parseBrokers(s string) []string {
	var out []string
	for _, b := range strings.Split(s, ",") {
		if b = strings.TrimSpace(b); b != "" {
			out = append(out, b)
		}
	}
	return out
}
