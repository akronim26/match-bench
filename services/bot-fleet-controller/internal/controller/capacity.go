// Package controller implements the pre-scale fleet-capacity gate.
//
// This file is part of the IICPC benchmarking platform and keeps its
// responsibilities local to the surrounding package. It should be read with
// the service-level design in design.md for broader operational context.
package controller

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	"github.com/iicpc/libs/metrics"
	kafka "github.com/segmentio/kafka-go"
	"github.com/segmentio/kafka-go/protocol/describegroups"
)

// GroupCapacity is what the bot-fleet consumer group can serve right now.
//
// Members — not Deployment replicas — is the meaningful number: a pod that exists but
// has not yet joined the group cannot receive a workload spec, so replica count would
// overstate capacity exactly when it matters. Reading the group also needs no
// Kubernetes RBAC, which a readyReplicas check would.
type GroupCapacity struct {
	// Members currently joined to the group.
	Members int
	// Stable is true only when the group is not mid-rebalance.
	Stable bool
	// State is the raw Kafka group state, for logging.
	State string
	// Err carries a per-group error from the DescribeGroups response.
	Err error
}

// CapacityProbe reports the group's current capacity. Injected so the gate's waiting
// logic is testable without a broker.
type CapacityProbe func(ctx context.Context) (GroupCapacity, error)

// capacityFromProtocol extracts capacity from the RAW DescribeGroups protocol
// response, counting members without decoding their metadata.
//
// This exists because kafka-go's high-level Client.DescribeGroups cannot be used
// against this fleet at all. It eagerly runs decodeMemberMetadata on every member and,
// on failure, sets group.Error AND executes `group.Members = nil` ("clear any
// previously decoded members"). The bot-fleet workers are rdkafka clients whose
// subscription metadata carries trailing bytes its decoder rejects with "Got non-zero
// number of bytes remaining: 10", so the member list is discarded before any caller can
// count it — the gate would time out with 0 members while three healthy workers were
// joined. Member COUNT and group STATE are all this gate needs; the per-member
// metadata is irrelevant to it.
func capacityFromProtocol(resp *describegroups.Response, groupID string) GroupCapacity {
	if resp == nil {
		return GroupCapacity{}
	}
	for i := range resp.Groups {
		g := &resp.Groups[i]
		if g.GroupID != groupID {
			continue
		}
		if g.ErrorCode != 0 {
			return GroupCapacity{
				State: g.GroupState,
				Err:   fmt.Errorf("describe group %s: kafka error code %d", groupID, g.ErrorCode),
			}
		}
		return GroupCapacity{
			Members: len(g.Members),
			Stable:  g.GroupState == "Stable" && len(g.Members) > 0,
			State:   g.GroupState,
		}
	}
	return GroupCapacity{}
}

// GroupCapacityProbe builds a CapacityProbe over the bot-fleet consumer group.
func (p *Producer) GroupCapacityProbe(groupID string) CapacityProbe {
	return func(ctx context.Context) (GroupCapacity, error) {
		if p.noop {
			return GroupCapacity{}, fmt.Errorf("producer is in noop mode (KAFKA_BROKERS not set)")
		}
		ctx, cancel := context.WithTimeout(ctx, writerTimeout)
		defer cancel()
		// Transport.RoundTrip speaks the raw protocol, so the response arrives before
		// kafka-go's member-metadata decoding can discard the member list.
		raw, err := kafka.DefaultTransport.RoundTrip(ctx, p.workloadWriter.Addr,
			&describegroups.Request{Groups: []string{groupID}})
		if err != nil {
			return GroupCapacity{}, err
		}
		resp, ok := raw.(*describegroups.Response)
		if !ok {
			return GroupCapacity{}, fmt.Errorf("unexpected DescribeGroups response type %T", raw)
		}
		c := capacityFromProtocol(resp, groupID)
		if c.Err != nil {
			return c, c.Err
		}
		return c, nil
	}
}

// awaitCapacity blocks until the consumer group has at least `want` members and has
// finished rebalancing, or `timeout` elapses.
//
// WHY THIS GATE EXISTS. The controller decides worker_count up front, then publishes
// that many workload specs. If the fleet is smaller than worker_count at publish time,
// two things go wrong, and a local 3-shard run reproduced both:
//
//  1. A pod that receives more specs than it can run concurrently leaves the surplus
//     UNCOMMITTED. When KEDA's new pod joins, the resulting rebalance hands those
//     uncommitted specs to it — so a shard runs TWICE, doubling that shard's load and
//     silently corrupting the measurement. Observed: worker_index 0 and 1 each prepared
//     twice, and the session delivered 142k orders against an expected 324k, because
//     the duplicate runs started after the barrier epoch had already passed.
//  2. Alternatively the surplus spec simply waits, ready fan-in never reaches
//     worker_count, and the session fails at READY_DEADLINE with a "partial fan-in"
//     error that says nothing about the real cause.
//
// Both windows are exactly "specs published -> all specs consumed and committed".
// Waiting for the fleet BEFORE publishing empties that window: nothing is in flight,
// nothing is uncommitted, so a rebalance is harmless.
//
// want <= 1 needs no gate (any single member serves a single shard), and timeout == 0
// disables it, preserving the previous publish-immediately behaviour.
func awaitCapacity(
	ctx context.Context,
	probe CapacityProbe,
	want int,
	timeout, pollInterval time.Duration,
	log *slog.Logger,
) error {
	if want <= 1 || timeout <= 0 {
		return nil
	}
	if pollInterval <= 0 {
		pollInterval = time.Second
	}

	deadline := time.Now().Add(timeout)
	var (
		last    GroupCapacity
		lastErr error
		waited  bool
	)
	for {
		cap, err := probe(ctx)
		if err != nil {
			// Transient metadata/coordinator errors are routine. Retry until the
			// deadline rather than failing the session on a broker blip.
			lastErr = err
		} else {
			lastErr = nil
			last = cap
			if cap.Members >= want && cap.Stable {
				if waited {
					log.Info("pre-scale gate satisfied",
						"members", cap.Members, "want", want, "group_state", cap.State)
					metrics.Counter("controller_prescale_wait_total",
						"Sessions that waited for bot-fleet capacity before publishing.",
						metrics.Labels("result", "satisfied"), 1)
				}
				return nil
			}
		}

		if !waited {
			waited = true
			log.Info("pre-scale gate: waiting for bot-fleet capacity",
				"want", want, "members", last.Members, "group_state", last.State,
				"timeout", timeout.String(),
				"note", "publishing before the fleet can serve every shard risks a duplicate shard on rebalance")
		}

		remaining := time.Until(deadline)
		if remaining <= 0 {
			metrics.Counter("controller_prescale_wait_total",
				"Sessions that waited for bot-fleet capacity before publishing.",
				metrics.Labels("result", "timeout"), 1)
			if lastErr != nil {
				return fmt.Errorf("bot-fleet capacity probe failed for %s (want %d members): %w",
					timeout, want, lastErr)
			}
			return fmt.Errorf(
				"bot-fleet has %d group members after %s, need %d (one per shard); group_state=%s",
				last.Members, timeout, want, last.State)
		}

		sleep := pollInterval
		if remaining < sleep {
			sleep = remaining
		}
		select {
		case <-time.After(sleep):
		case <-ctx.Done():
			return ctx.Err()
		}
	}
}
