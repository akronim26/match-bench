// Package controller defines tests for producer test.
//
// This file is part of the IICPC benchmarking platform and keeps its
// responsibilities local to the surrounding package. It should be read with
// the service-level design in design.md for broader operational context.
package controller

import (
	"fmt"
	"log/slog"
	"testing"

	"github.com/iicpc/schemas/topics"
	kafka "github.com/segmentio/kafka-go"
)

// TestKeyedWritersUseKeySensitiveBalancer performs the package-specific operation described by its name.
// It keeps validation, side effects, and returned values within this package's contract.
func TestKeyedWritersUseKeySensitiveBalancer(t *testing.T) {
	p := NewProducer("localhost:9092", slog.Default())
	t.Cleanup(func() { _ = p.Close() })

	for name, w := range map[string]*kafka.Writer{
		"barrier": p.barrierWriter,
		"status":  p.statusWriter,
	} {
		if _, ok := w.Balancer.(*kafka.Hash); !ok {
			t.Errorf("%s writer balancer = %T, want *kafka.Hash (key-sensitive)", name, w.Balancer)
		}
	}
	if _, ok := p.workloadWriter.Balancer.(*leasedPartitionBalancer); !ok {
		t.Errorf("workload writer balancer = %T, want *leasedPartitionBalancer (explicit leased partition)", p.workloadWriter.Balancer)
	}
}

// TestLeasedPartitionBalancerRoutesToLeasedPartition performs the package-specific operation described by its name.
// It keeps validation, side effects, and returned values within this package's contract.
func TestLeasedPartitionBalancerRoutesToLeasedPartition(t *testing.T) {
	b := &leasedPartitionBalancer{}
	parts := make([]int, 24)
	for i := range parts {
		parts[i] = i
	}

	for i := 0; i < 24; i++ {
		msg := kafka.Message{
			Key:        []byte(fmt.Sprintf("sess-1:%d", i)),
			WriterData: i,
		}
		if got := b.Balance(msg, parts...); got != i {
			t.Errorf("Balance(leased=%d) = partition %d, want %d", i, got, i)
		}
	}

	noHint := kafka.Message{Key: []byte("sess-1:0")}
	first := b.Balance(noHint, parts...)
	if again := b.Balance(noHint, parts...); again != first {
		t.Errorf("fallback not deterministic: %d vs %d", first, again)
	}
	if first < 0 || first >= len(parts) {
		t.Errorf("fallback partition %d out of range [0,%d)", first, len(parts))
	}

	// A leased partition missing from the metadata view must be returned
	// as-is, NOT hash-rerouted: the lease is the routing authority, and a
	// genuinely nonexistent partition should fail the write loudly rather
	// than silently landing on another session's leased partition.
	staleView := kafka.Message{Key: []byte("sess-1:9"), WriterData: 99}
	if got := b.Balance(staleView, parts...); got != 99 {
		t.Errorf("stale-metadata Balance = %d, want the leased partition 99 (loud failure over silent reroute)", got)
	}
}

// TestBuildWorkloadMessagesCarriesLeasedPartition performs the package-specific operation described by its name.
// It keeps validation, side effects, and returned values within this package's contract.
func TestBuildWorkloadMessagesCarriesLeasedPartition(t *testing.T) {
	specs := []topics.WorkloadSpec{
		{SessionID: "sess-1", WorkerIndex: 0, WorkerCount: 3},
		{SessionID: "sess-1", WorkerIndex: 1, WorkerCount: 3},
		{SessionID: "sess-1", WorkerIndex: 2, WorkerCount: 3},
	}
	leases := []int{11, 12, 13}
	msgs, err := buildWorkloadMessages(specs, leases)
	if err != nil {
		t.Fatalf("buildWorkloadMessages: %v", err)
	}
	if len(msgs) != len(specs) {
		t.Fatalf("got %d messages, want %d", len(msgs), len(specs))
	}
	for i, msg := range msgs {
		wantKey := fmt.Sprintf("sess-1:%d", i)
		if string(msg.Key) != wantKey {
			t.Errorf("message %d key = %q, want %q", i, msg.Key, wantKey)
		}
		hint, ok := msg.WriterData.(int)
		if !ok {
			t.Fatalf("message %d WriterData = %T, want int leased partition", i, msg.WriterData)
		}
		if hint != leases[i] {
			t.Errorf("message %d WriterData = %d, want %d", i, hint, leases[i])
		}
	}
}

// TestBuildWorkloadMessagesRejectsLeaseMismatch performs the package-specific operation described by its name.
// It keeps validation, side effects, and returned values within this package's contract.
func TestBuildWorkloadMessagesRejectsLeaseMismatch(t *testing.T) {
	specs := []topics.WorkloadSpec{{SessionID: "sess-1", WorkerIndex: 0}}
	p := NewProducer("localhost:9092", slog.Default())
	t.Cleanup(func() { _ = p.Close() })
	if err := p.PublishWorkloadSpec(nil, specs, []int{1, 2}); err == nil { //nolint:staticcheck // noop producer, ctx unused
		t.Fatal("expected error when lease count does not match spec count")
	}
}

// TestHashBalancerRoutesSameKeyToSamePartition performs the package-specific operation described by its name.
// It keeps validation, side effects, and returned values within this package's contract.
func TestHashBalancerRoutesSameKeyToSamePartition(t *testing.T) {
	var b kafka.Hash
	parts := []int{0, 1, 2, 3, 4, 5}

	a := b.Balance(kafka.Message{Key: []byte("session-A")}, parts...)
	aAgain := b.Balance(kafka.Message{Key: []byte("session-A")}, parts...)
	if a != aAgain {
		t.Fatalf("same key routed to different partitions: %d vs %d", a, aAgain)
	}

	differs := false
	for _, k := range []string{"session-B", "session-C", "session-D", "session-E"} {
		if b.Balance(kafka.Message{Key: []byte(k)}, parts...) != a {
			differs = true
			break
		}
	}
	if !differs {
		t.Fatal("Hash balancer routed every key to the same partition — key not honored")
	}
}
