// Package controller defines tests for the worker-demand signal.
//
// This file is part of the IICPC benchmarking platform and keeps its
// responsibilities local to the surrounding package. It should be read with
// the service-level design in design.md for broader operational context.
package controller

import (
	"testing"

	"github.com/iicpc/libs/metrics"
)

// TestDemandedWorkers sums WorkerCount across in-flight sessions. This is the
// autoscaling signal: it is a DECLARATION of how many shards must be servable,
// known at admission before any workload spec is published — unlike Kafka consumer
// lag, which only appears after publishing and so cannot prevent the under-provisioned
// publish in the first place.
func TestDemandedWorkers(t *testing.T) {
	m := NewSessionManager()
	if got := m.DemandedWorkers(); got != 0 {
		t.Errorf("no sessions: got %d, want 0", got)
	}

	a := newTestSession("sess-A")
	a.WorkerCount = 3
	m.Add(a)
	if got := m.DemandedWorkers(); got != 3 {
		t.Errorf("one session of 3 shards: got %d, want 3", got)
	}

	b := newTestSession("sess-B")
	b.WorkerCount = 2
	m.Add(b)
	if got := m.DemandedWorkers(); got != 5 {
		t.Errorf("two concurrent sessions (3+2): got %d, want 5", got)
	}

	// A duplicate Add must not double-count: benchmark.requested can be redelivered,
	// and inflating demand would scale the fleet on a phantom session.
	m.Add(b)
	if got := m.DemandedWorkers(); got != 5 {
		t.Errorf("duplicate Add double-counted: got %d, want 5", got)
	}

	m.Drop("sess-A")
	if got := m.DemandedWorkers(); got != 2 {
		t.Errorf("after dropping the 3-shard session: got %d, want 2", got)
	}

	m.Drop("sess-B")
	if got := m.DemandedWorkers(); got != 0 {
		t.Errorf("after dropping all sessions: got %d, want 0", got)
	}
}

// TestDemandedWorkersGaugeIsRegistered guards the failure mode that made the lease
// gauges silently useless: libs/go/metrics exports ONLY names present in its
// pre-declared catalog, and metric() returns nil while counting
// `unregistered_metric` for anything else. A gauge that is set but not declared is
// a no-op, and nothing in the emitting code fails — so the omission is invisible
// until someone scrapes /metrics and finds nothing. KEDA would then read a missing
// series and never scale.
func TestDemandedWorkersGaugeIsRegistered(t *testing.T) {
	before := metrics.RegistryErrorCount("unregistered_metric")

	m := NewSessionManager()
	s := newTestSession("sess-gauge")
	s.WorkerCount = 4
	m.Add(s)
	m.Drop("sess-gauge")

	if after := metrics.RegistryErrorCount("unregistered_metric"); after != before {
		t.Errorf("emitting the demand gauge raised unregistered_metric from %d to %d; "+
			"add controller_demanded_workers to the catalog in libs/go/metrics/metrics.go",
			before, after)
	}
}
