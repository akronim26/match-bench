// Package controller defines tests for runner shard test.
//
// This file is part of the IICPC benchmarking platform and keeps its
// responsibilities local to the surrounding package. It should be read with
// the service-level design in design.md for broader operational context.
package controller

import (
	"math"
	"testing"

	"github.com/iicpc/schemas/topics"
)

// TestComputeWorkerCount performs the package-specific operation described by its name.
// It keeps validation, side effects, and returned values within this package's contract.
func TestComputeWorkerCount(t *testing.T) {
	cases := []struct {
		totalTasks int
		want       uint32
	}{
		{0, 1},
		{1, 1},
		{999, 1},
		{1000, 1},
		{1001, 2},
		{2000, 2},
		{2001, 3},
		{5110, 6},
		{511, 1},
		{2555, 3},
	}
	for _, c := range cases {
		got := computeWorkerCount(c.totalTasks, DefaultMaxTasksPerWorker)
		if got != c.want {
			t.Errorf("computeWorkerCount(%d) = %d, want %d", c.totalTasks, got, c.want)
		}
	}

	if got := computeWorkerCount(2555, 100000); got != 1 {
		t.Errorf("pin-to-one-pod: computeWorkerCount(2555, 100000) = %d, want 1", got)
	}
	if got := computeWorkerCount(2555, 511); got != 5 {
		t.Errorf("fan-out: computeWorkerCount(2555, 511) = %d, want 5", got)
	}
	if got := computeWorkerCount(500, 0); got != 1 {
		t.Errorf("zero ceiling falls back to default: got %d, want 1", got)
	}
}

// TestTotalTargetRPS sums target_rps across a scenario's task specs. This is the
// input to the throughput ceiling in computeWorkerCount, and it is the quantity the
// task-count ceiling was standing in for (badly): 1000 retail bots at 5 rps and 1000
// HFT bots at 1000 rps have identical task counts and a 200x difference in rate.
func TestTotalTargetRPS(t *testing.T) {
	if got := totalTargetRPS(nil); got != 0 {
		t.Errorf("nil specs: got %d, want 0", got)
	}
	if got := totalTargetRPS([]topics.TaskSpec{}); got != 0 {
		t.Errorf("empty specs: got %d, want 0", got)
	}

	// The local `constant` scenario's real shape: 2 hft@1000 + 200 retail@5 + 2 inst@300.
	local := make([]topics.TaskSpec, 0, 204)
	for i := 0; i < 2; i++ {
		local = append(local, topics.TaskSpec{Profile: "hft", TargetRPS: 1000})
	}
	for i := 0; i < 200; i++ {
		local = append(local, topics.TaskSpec{Profile: "retail", TargetRPS: 5})
	}
	for i := 0; i < 2; i++ {
		local = append(local, topics.TaskSpec{Profile: "institutional", TargetRPS: 300})
	}
	if got, want := totalTargetRPS(local), uint64(3600); got != want {
		t.Errorf("local constant scenario: got %d, want %d", got, want)
	}

	// A task with target_rps=0 contributes nothing but must not break the sum.
	if got := totalTargetRPS([]topics.TaskSpec{{TargetRPS: 0}, {TargetRPS: 7}}); got != 7 {
		t.Errorf("zero-rate task: got %d, want 7", got)
	}

	// uint64 accumulator: 1000 tasks x max uint32 must not overflow the way a
	// uint32 sum would.
	big := make([]topics.TaskSpec, 1000)
	for i := range big {
		big[i] = topics.TaskSpec{TargetRPS: math.MaxUint32}
	}
	if got, want := totalTargetRPS(big), uint64(1000)*uint64(math.MaxUint32); got != want {
		t.Errorf("no overflow: got %d, want %d", got, want)
	}
}

// TestComputeWorkerCountRateAware covers the throughput ceiling. Both ceilings are
// independent; whichever demands more shards wins.
func TestComputeWorkerCountRateAware(t *testing.T) {
	cases := []struct {
		name       string
		totalTasks int
		totalRPS   uint64
		maxTasks   int
		rpsCap     uint64
		want       uint32
	}{
		// The regression that motivated this: 1000 HFT bots at 1000 rps is 1M/s and
		// used to shard to ONE worker because only the task count was consulted.
		{"rate ceiling binds", 1000, 1_000_000, 1000, 60_000, 17},
		// Same task count, 200x less load: the task ceiling is correct here.
		{"task ceiling binds", 1000, 5_000, 1000, 60_000, 1},
		// Both ceilings agree.
		{"ceilings agree", 2000, 120_000, 1000, 60_000, 2},
		// Task ceiling higher than rate ceiling: max() keeps the larger.
		{"task ceiling higher", 5000, 60_000, 1000, 60_000, 5},
		// Rate ceiling higher than task ceiling: max() keeps the larger.
		{"rate ceiling higher", 500, 240_000, 1000, 60_000, 4},
		// rpsCap == 0 disables the rate ceiling entirely, preserving the exact
		// pre-change behaviour so an unset env var is a no-op.
		{"rate ceiling disabled", 1000, 1_000_000, 1000, 0, 1},
		// A scenario with no measurable rate falls back to the task ceiling.
		{"zero rate", 1500, 0, 1000, 60_000, 2},
		// Exact multiple must not round up to an extra shard.
		{"exact multiple", 10, 120_000, 1000, 60_000, 2},
		// One order over the cap demands another shard.
		{"one over", 10, 120_001, 1000, 60_000, 3},
		// The Phase-5 test setup: lowering the cap makes a small scenario fan out,
		// exercising the multi-shard path without generating real load.
		{"lowered cap for local test", 204, 6_000, 1000, 2_000, 3},
		// Empty scenario still yields a floor of one worker.
		{"no tasks", 0, 0, 1000, 60_000, 1},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			got := computeWorkerCountForLoad(c.totalTasks, c.totalRPS, c.maxTasks, c.rpsCap)
			if got != c.want {
				t.Errorf("computeWorkerCountForLoad(tasks=%d, rps=%d, maxTasks=%d, cap=%d) = %d, want %d",
					c.totalTasks, c.totalRPS, c.maxTasks, c.rpsCap, got, c.want)
			}
		})
	}
}

// TestComputeWorkerCountRateAwarePreservesLegacy pins that the existing two-argument
// helper keeps behaving exactly as before, so every caller that has not been taught
// about rates is unaffected.
func TestComputeWorkerCountRateAwarePreservesLegacy(t *testing.T) {
	for _, tasks := range []int{0, 1, 999, 1000, 1001, 5110} {
		legacy := computeWorkerCount(tasks, DefaultMaxTasksPerWorker)
		rateDisabled := computeWorkerCountForLoad(tasks, 1_000_000, DefaultMaxTasksPerWorker, 0)
		if legacy != rateDisabled {
			t.Errorf("tasks=%d: legacy=%d but rate-disabled=%d; they must agree",
				tasks, legacy, rateDisabled)
		}
	}
}

// TestRoundRobinShard performs the package-specific operation described by its name.
// It keeps validation, side effects, and returned values within this package's contract.
func TestRoundRobinShard(t *testing.T) {
	tasks := make([]topics.TaskSpec, 12)
	for i := range tasks {
		tasks[i] = topics.TaskSpec{TaskID: uint32(i)}
	}
	const workerCount uint32 = 3

	tasksByWorker := make([][]topics.TaskSpec, workerCount)
	for i := range tasksByWorker {
		tasksByWorker[i] = make([]topics.TaskSpec, 0)
	}
	for i, ts := range tasks {
		shard := uint32(i) % workerCount
		tasksByWorker[shard] = append(tasksByWorker[shard], ts)
	}

	for w := uint32(0); w < workerCount; w++ {
		if got, want := len(tasksByWorker[w]), 4; got != want {
			t.Errorf("worker %d: %d tasks, want %d", w, got, want)
		}
		for i, ts := range tasksByWorker[w] {
			expected := w + uint32(i)*workerCount
			if ts.TaskID != expected {
				t.Errorf("worker %d task %d: id=%d, want %d", w, i, ts.TaskID, expected)
			}
		}
	}
}
