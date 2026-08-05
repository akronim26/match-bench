// Package source implements per-partition offset resolution for session reads.
//
// This is what remains of drain.go after the batch DrainSession path was deleted:
// StreamSession uses exactly the same [start, last) window arithmetic, so bounding a
// session's reads by its UUIDv7 timestamp rather than scanning the topic from the
// beginning survived the batch path it was written for.
//
// This file is part of the IICPC benchmarking platform and keeps its
// responsibilities local to the surrounding package. It should be read with
// the service-level design in design.md for broader operational context.
package source

import (
	"context"
	"encoding/hex"
	"fmt"
	"log/slog"
	"strings"
	"time"

	"github.com/iicpc/libs/metrics"
	"github.com/segmentio/kafka-go"
)

// startMargin backs the session-start offset lookup off by this much, so clock skew
// between the producer and the broker cannot push the window past the first record.
const startMargin = 60 * time.Second

// recordDecodeError performs the package-specific operation described by its name.
// It keeps validation, side effects, and returned values within this package's contract.
func recordDecodeError(topic string, m kafka.Message, err error) {
	metrics.Counter("validator_validation_errors_total", "Correctness-validator validation errors by stage.", metrics.Labels("stage", "drain_decode"), 1)
	slog.Warn("drain: msgpack decode failed; skipping message", "topic", topic, "partition", m.Partition, "offset", m.Offset, "error", err)
}

// partitionOffsets resolves the [start, last) offset window one partition should be read
// over for this session.
func partitionOffsets(ctx context.Context, broker, topic string, partition int, sessionID string) (int64, int64, error) {
	conn, err := kafka.DialLeader(ctx, "tcp", broker, topic, partition)
	if err != nil {
		return 0, 0, fmt.Errorf("dial leader %s/%d: %w", topic, partition, err)
	}
	defer conn.Close()

	start, err := startOffsetForSession(sessionID, conn.ReadOffset)
	if err != nil {
		return 0, 0, fmt.Errorf("start offset %s/%d: %w", topic, partition, err)
	}
	last, err := conn.ReadLastOffset()
	if err != nil {
		return 0, 0, fmt.Errorf("read last offset %s/%d: %w", topic, partition, err)
	}
	if start == kafka.FirstOffset {
		earliest, err := conn.ReadFirstOffset()
		if err != nil {
			return 0, 0, fmt.Errorf("read first offset %s/%d: %w", topic, partition, err)
		}
		return earliest, last, nil
	}
	return resolveStart(start, last), last, nil
}

// resolveStart performs the package-specific operation described by its name.
// It keeps validation, side effects, and returned values within this package's contract.
func resolveStart(seek, last int64) int64 {
	if seek < 0 {
		return last
	}
	return seek
}

// startOffsetForSession performs the package-specific operation described by its name.
// It keeps validation, side effects, and returned values within this package's contract.
func startOffsetForSession(sessionID string, lookup func(time.Time) (int64, error)) (int64, error) {
	start, ok := sessionStartFromID(sessionID)
	if !ok {
		return kafka.FirstOffset, nil
	}
	return lookup(start.Add(-startMargin))
}

// sessionStartFromID performs the package-specific operation described by its name.
// It keeps validation, side effects, and returned values within this package's contract.
func sessionStartFromID(sessionID string) (time.Time, bool) {
	hexDigits := strings.ReplaceAll(sessionID, "-", "")
	if len(hexDigits) != 32 {
		return time.Time{}, false
	}
	raw, err := hex.DecodeString(hexDigits)
	if err != nil {
		return time.Time{}, false
	}
	if raw[6]>>4 != 7 {
		return time.Time{}, false
	}
	var ms int64
	for _, b := range raw[:6] {
		ms = ms<<8 | int64(b)
	}
	if ms <= 0 {
		return time.Time{}, false
	}
	return time.UnixMilli(ms), true
}
