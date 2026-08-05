// Package controller defines tests for the pre-scale fleet-capacity gate.
//
// This file is part of the IICPC benchmarking platform and keeps its
// responsibilities local to the surrounding package. It should be read with
// the service-level design in design.md for broader operational context.
package controller

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"sync/atomic"
	"testing"
	"time"

	"github.com/segmentio/kafka-go/protocol/describegroups"
)

func quietLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(io.Discard, nil))
}

// TestCapacityFromProtocolCountsRdkafkaMembers is a regression test for the reason the
// probe reads the RAW protocol response instead of kafka-go's Client.DescribeGroups.
//
// The high-level helper eagerly decodes each member's subscription metadata and, on
// failure, sets group.Error AND clears group.Members. bot-fleet workers are rdkafka
// clients whose metadata carries trailing bytes its decoder rejects ("Got non-zero
// number of bytes remaining: 10"), so with the high-level call the member list is
// discarded and the gate saw 0 members while three healthy workers were joined —
// timing out every session. Counting from the raw response must be immune to whatever
// the metadata contains.
func TestCapacityFromProtocolCountsRdkafkaMembers(t *testing.T) {
	// Deliberately undecodable-by-kafka-go member metadata: a plausible version +
	// topic list followed by trailing bytes, which is the shape rdkafka sends.
	rdkafkaMetadata := []byte{
		0x00, 0x01, // version 1
		0x00, 0x00, 0x00, 0x01, // 1 topic
		0x00, 0x14, // topic name length 20
		'w', 'o', 'r', 'k', 'l', 'o', 'a', 'd', '.', 'a', 's', 's', 'i', 'g', 'n', 'm', 'e', 'n', 't', 's',
		0x00, 0x00, 0x00, 0x00, // empty user data
		0xDE, 0xAD, 0xBE, 0xEF, 0x01, 0x02, 0x03, 0x04, 0x05, 0x06, // 10 trailing bytes
	}
	resp := &describegroups.Response{
		Groups: []describegroups.ResponseGroup{{
			GroupID:    "bot-fleet",
			GroupState: "Stable",
			Members: []describegroups.ResponseGroupMember{
				{MemberID: "a", MemberMetadata: rdkafkaMetadata},
				{MemberID: "b", MemberMetadata: rdkafkaMetadata},
				{MemberID: "c", MemberMetadata: rdkafkaMetadata},
			},
		}},
	}
	got := capacityFromProtocol(resp, "bot-fleet")
	if got.Members != 3 {
		t.Errorf("Members = %d, want 3 — member count must not depend on decoding metadata", got.Members)
	}
	if !got.Stable {
		t.Errorf("Stable = false, want true")
	}
	if got.Err != nil {
		t.Errorf("Err = %v, want nil", got.Err)
	}
}

// TestCapacityFromProtocolEdgeCases covers the raw-response paths the gate depends on.
func TestCapacityFromProtocolEdgeCases(t *testing.T) {
	if got := capacityFromProtocol(nil, "bot-fleet"); got.Members != 0 || got.Stable {
		t.Errorf("nil response: got %+v, want zero capacity", got)
	}

	// Group absent (no worker has ever joined) is "keep waiting", not an error.
	absent := &describegroups.Response{Groups: []describegroups.ResponseGroup{{
		GroupID: "other-group", GroupState: "Stable",
	}}}
	if got := capacityFromProtocol(absent, "bot-fleet"); got.Members != 0 || got.Err != nil {
		t.Errorf("absent group: got %+v, want zero members and no error", got)
	}

	// Mid-rebalance with members present is NOT stable.
	rebal := &describegroups.Response{Groups: []describegroups.ResponseGroup{{
		GroupID: "bot-fleet", GroupState: "PreparingRebalance",
		Members: []describegroups.ResponseGroupMember{{MemberID: "a"}, {MemberID: "b"}},
	}}}
	if got := capacityFromProtocol(rebal, "bot-fleet"); got.Stable || got.Members != 2 {
		t.Errorf("rebalancing: got %+v, want 2 members and not stable", got)
	}

	// A broker-side error code must surface rather than read as zero capacity.
	errResp := &describegroups.Response{Groups: []describegroups.ResponseGroup{{
		GroupID: "bot-fleet", ErrorCode: 15, // COORDINATOR_NOT_AVAILABLE
	}}}
	if got := capacityFromProtocol(errResp, "bot-fleet"); got.Err == nil {
		t.Error("error code 15 must surface as Err")
	}
}

// TestAwaitCapacityPassesWhenAlreadySufficient: the common case must not add latency.
func TestAwaitCapacityPassesWhenAlreadySufficient(t *testing.T) {
	probe := func(context.Context) (GroupCapacity, error) {
		return GroupCapacity{Members: 3, Stable: true}, nil
	}
	start := time.Now()
	err := awaitCapacity(context.Background(), probe, 3, time.Second, 10*time.Millisecond, quietLogger())
	if err != nil {
		t.Fatalf("awaitCapacity: %v", err)
	}
	if elapsed := time.Since(start); elapsed > 200*time.Millisecond {
		t.Errorf("took %v; a satisfied gate must return immediately", elapsed)
	}
}

// TestAwaitCapacityBlocksThenProceeds is the scale-up case B5 exposed: the controller
// must wait for KEDA to add the pod, THEN publish. This is what closes the window in
// which an uncommitted spec can be re-delivered to a newly joined pod.
func TestAwaitCapacityBlocksThenProceeds(t *testing.T) {
	var calls int32
	probe := func(context.Context) (GroupCapacity, error) {
		n := atomic.AddInt32(&calls, 1)
		if n < 3 {
			return GroupCapacity{Members: 2, Stable: true}, nil
		}
		return GroupCapacity{Members: 3, Stable: true}, nil
	}
	if err := awaitCapacity(context.Background(), probe, 3, 2*time.Second, 5*time.Millisecond, quietLogger()); err != nil {
		t.Fatalf("awaitCapacity: %v", err)
	}
	if got := atomic.LoadInt32(&calls); got < 3 {
		t.Errorf("probe called %d times, expected to poll until capacity arrived", got)
	}
}

// TestAwaitCapacityWaitsForStable: enough members but mid-rebalance is NOT ready.
// Publishing then is exactly the hazard — the assignment is still moving.
func TestAwaitCapacityWaitsForStable(t *testing.T) {
	var calls int32
	probe := func(context.Context) (GroupCapacity, error) {
		n := atomic.AddInt32(&calls, 1)
		if n < 3 {
			return GroupCapacity{Members: 5, Stable: false, State: "CompletingRebalance"}, nil
		}
		return GroupCapacity{Members: 5, Stable: true, State: "Stable"}, nil
	}
	if err := awaitCapacity(context.Background(), probe, 3, 2*time.Second, 5*time.Millisecond, quietLogger()); err != nil {
		t.Fatalf("awaitCapacity: %v", err)
	}
	if got := atomic.LoadInt32(&calls); got < 3 {
		t.Errorf("probe called %d times; must keep polling while the group rebalances", got)
	}
}

// TestAwaitCapacityTimesOutWithDiagnosis: when the fleet never arrives the error must
// name the shortfall. Before this gate existed the same condition surfaced as a
// confusing "partial fan-in: received 1 of 3" much later in the run.
func TestAwaitCapacityTimesOutWithDiagnosis(t *testing.T) {
	probe := func(context.Context) (GroupCapacity, error) {
		return GroupCapacity{Members: 1, Stable: true}, nil
	}
	err := awaitCapacity(context.Background(), probe, 4, 60*time.Millisecond, 5*time.Millisecond, quietLogger())
	if err == nil {
		t.Fatal("expected a timeout error")
	}
	msg := err.Error()
	for _, want := range []string{"1", "4"} {
		if !contains(msg, want) {
			t.Errorf("error %q should mention %q so the shortfall is obvious", msg, want)
		}
	}
}

// TestAwaitCapacityProbeErrorsDoNotAbort: a transient metadata error must be retried,
// not fail the session. Broker blips are routine; the gate should ride them out and
// only give up at the deadline.
func TestAwaitCapacityProbeErrorsDoNotAbort(t *testing.T) {
	var calls int32
	probe := func(context.Context) (GroupCapacity, error) {
		if atomic.AddInt32(&calls, 1) < 3 {
			return GroupCapacity{}, errors.New("broker unreachable")
		}
		return GroupCapacity{Members: 2, Stable: true}, nil
	}
	if err := awaitCapacity(context.Background(), probe, 2, 2*time.Second, 5*time.Millisecond, quietLogger()); err != nil {
		t.Fatalf("transient probe errors must be retried, got: %v", err)
	}
}

// TestAwaitCapacityDisabled: want <= 1 needs no gate (a single shard is served by any
// one member), and a zero timeout disables the gate entirely so the previous
// publish-immediately behaviour remains available.
func TestAwaitCapacityDisabled(t *testing.T) {
	called := false
	probe := func(context.Context) (GroupCapacity, error) {
		called = true
		return GroupCapacity{}, errors.New("must not be called")
	}
	if err := awaitCapacity(context.Background(), probe, 3, 0, 5*time.Millisecond, quietLogger()); err != nil {
		t.Fatalf("zero timeout must disable the gate, got: %v", err)
	}
	if called {
		t.Error("probe called even though the gate is disabled")
	}
}

// TestAwaitCapacityHonoursContext: a cancelled run must abandon the wait promptly
// rather than holding its leases until the deadline.
func TestAwaitCapacityHonoursContext(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	probe := func(context.Context) (GroupCapacity, error) {
		return GroupCapacity{Members: 0, Stable: false}, nil
	}
	err := awaitCapacity(ctx, probe, 3, 5*time.Second, 5*time.Millisecond, quietLogger())
	if err == nil {
		t.Fatal("expected an error when the context is already cancelled")
	}
}

func contains(haystack, needle string) bool {
	return len(haystack) >= len(needle) && (func() bool {
		for i := 0; i+len(needle) <= len(haystack); i++ {
			if haystack[i:i+len(needle)] == needle {
				return true
			}
		}
		return false
	})()
}
