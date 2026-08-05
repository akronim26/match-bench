/**
 * This file defines tests for useLeaderboard.test.
 * It is part of the IICPC frontend and keeps UI, API, or test behavior
 * scoped to this module so callers can rely on stable boundaries.
 */
import { act, renderHook, waitFor } from "@testing-library/react";
import { QueryClient, QueryClientProvider } from "@tanstack/react-query";
import type { ReactNode } from "react";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import type {
  LeaderboardEntry,
  LeaderboardResponse,
  LeaderboardUpdateEvent,
  LiveMetricsEvent,
} from "@/types/leaderboard";
import { applyLeaderboardUpdate, useLeaderboard } from "./useLeaderboard";

vi.mock("@/api/leaderboard", () => ({
  getLeaderboard: vi.fn(() =>
    Promise.resolve({ source: "store", rows: [] } as LeaderboardResponse),
  ),
}));

class FakeEventSource {
  static instances: FakeEventSource[] = [];
  url: string;
  closed = false;
  onopen: (() => void) | null = null;
  onerror: (() => void) | null = null;
  private listeners = new Map<string, Array<(event: MessageEvent) => void>>();

  constructor(url: string) {
    this.url = url;
    FakeEventSource.instances.push(this);
  }

  addEventListener(type: string, listener: (event: MessageEvent) => void) {
    this.listeners.set(type, [...(this.listeners.get(type) ?? []), listener]);
  }

  emit(type: string, data: string) {
    for (const listener of this.listeners.get(type) ?? []) {
      listener(new MessageEvent(type, { data }));
    }
  }

  close() {
    this.closed = true;
  }
}

/**
 * entry performs the module-specific operation described by its name.
 * It keeps inputs, side effects, and returned values within this module's contract.
 */
function entry(overrides: Partial<LeaderboardEntry> = {}): LeaderboardEntry {
  return {
    rank: 1,
    run_group_id: "rg-1",
    submission_id: "sub-1",
    contestant_id: "c-1",
    team_name: "Ada",
    peak_sustained_tps: 9001,
    p99_ns_at_peak_tps: 2000,
    spike_recovery_ns: 3000,
    total_correctness: 0.99,
    disqualified: false,
    rank_delta: 0,
    computed_at_ns: 1000,
    ...overrides,
  };
}

/**
 * update performs the module-specific operation described by its name.
 * It keeps inputs, side effects, and returned values within this module's contract.
 */
function update(
  overrides: Partial<LeaderboardUpdateEvent> = {},
): LeaderboardUpdateEvent {
  return {
    run_group_id: "rg-1",
    submission_id: "sub-1",
    contestant_id: "c-1",
    team_name: "Ada",
    rank: 2,
    rank_delta: -1,
    peak_sustained_tps: 9500,
    p99_ns_at_peak_tps: 1900,
    spike_recovery_ns: 2800,
    total_correctness: 1,
    disqualified: false,
    updated_at_ns: 2000,
    ...overrides,
  };
}

const current: LeaderboardResponse = {
  source: "store",
  rows: [
    entry(),
    entry({
      rank: 2,
      run_group_id: "rg-2",
      contestant_id: "c-2",
      team_name: "Bob",
    }),
  ],
  next_cursor: "cursor-1",
};

describe("applyLeaderboardUpdate", () => {
  it("replaces the row matching run_group_id/contestant_id", () => {
    const next = applyLeaderboardUpdate(current, update());
    expect(next.rows).toHaveLength(2);
    const merged = next.rows.find((row) => row.run_group_id === "rg-1");
    expect(merged).toMatchObject({
      rank: 2,
      rank_delta: -1,
      peak_sustained_tps: 9500,
      p99_ns_at_peak_tps: 1900,
      spike_recovery_ns: 2800,
      total_correctness: 1,
      computed_at_ns: 2000,
    });
    expect(next.rows.find((row) => row.run_group_id === "rg-2")).toEqual(
      current.rows[1],
    );
  });

  it("appends a row when no run_group_id/contestant_id matches", () => {
    const next = applyLeaderboardUpdate(
      current,
      update({ run_group_id: "rg-3", contestant_id: "c-3", team_name: "Eve" }),
    );
    expect(next.rows).toHaveLength(3);
    expect(next.rows[2]).toMatchObject({
      run_group_id: "rg-3",
      contestant_id: "c-3",
      team_name: "Eve",
    });
  });

  it("does not merge across rows that share only one of the two keys", () => {
    const next = applyLeaderboardUpdate(
      current,
      update({ run_group_id: "rg-2", contestant_id: "c-1" }),
    );
    expect(next.rows).toHaveLength(3);
  });

  it("preserves source and next_cursor", () => {
    const next = applyLeaderboardUpdate(current, update());
    expect(next.source).toBe("store");
    expect(next.next_cursor).toBe("cursor-1");
  });

  it("preserves jitter_p99_us from the existing row (not present on update events)", () => {
    const withJitter: LeaderboardResponse = {
      ...current,
      rows: current.rows.map((row) =>
        row.run_group_id === "rg-1" ? { ...row, jitter_p99_us: 42.5 } : row,
      ),
    };
    const next = applyLeaderboardUpdate(withJitter, update());
    const merged = next.rows.find((row) => row.run_group_id === "rg-1");
    expect(merged?.jitter_p99_us).toBe(42.5);
    expect(merged?.rank).toBe(2);
  });

  it("carries the disqualification fields through", () => {
    const next = applyLeaderboardUpdate(
      current,
      update({ disqualified: true, disqualification_code: "DQ_SPOOF" }),
    );
    const merged = next.rows.find((row) => row.run_group_id === "rg-1");
    expect(merged?.disqualified).toBe(true);
    expect(merged?.disqualification_code).toBe("DQ_SPOOF");
  });
});

function liveMetricsEvent(
  overrides: Partial<LiveMetricsEvent> = {},
): LiveMetricsEvent {
  return {
    contestant_id: "c-1",
    session_id: "sess-1",
    wave_index: 3,
    p50_ns: 100_000,
    p99_ns: 900_000,
    p999_ns: 1_500_000,
    tps_1s: 12345.6,
    error_rate: 0.01,
    updated_at_ns: 9999,
    ...overrides,
  };
}

describe("useLeaderboard live_metrics", () => {
  beforeEach(() => {
    FakeEventSource.instances = [];
    vi.stubGlobal("EventSource", FakeEventSource);
  });

  afterEach(() => {
    vi.unstubAllGlobals();
  });

  function wrapper({ children }: { children: ReactNode }) {
    const queryClient = new QueryClient({
      defaultOptions: { queries: { retry: false } },
    });
    return (
      <QueryClientProvider client={queryClient}>
        {children}
      </QueryClientProvider>
    );
  }

  it("keys incoming live_metrics events by session_id:contestant_id", async () => {
    const { result } = renderHook(() => useLeaderboard(), { wrapper });

    await waitFor(() => expect(FakeEventSource.instances).toHaveLength(1));

    const source = FakeEventSource.instances[0];
    const event = liveMetricsEvent();
    act(() => source.emit("live_metrics", JSON.stringify(event)));

    await waitFor(() =>
      expect(result.current.liveMetrics["sess-1:c-1"]).toEqual(event),
    );
  });

  it("keeps separate entries per session/contestant and updates in place", async () => {
    const { result } = renderHook(() => useLeaderboard(), { wrapper });

    await waitFor(() => expect(FakeEventSource.instances).toHaveLength(1));
    const source = FakeEventSource.instances[0];

    act(() =>
      source.emit(
        "live_metrics",
        JSON.stringify(liveMetricsEvent({ session_id: "sess-2", tps_1s: 1 })),
      ),
    );
    await waitFor(() =>
      expect(result.current.liveMetrics["sess-2:c-1"]?.tps_1s).toBe(1),
    );

    act(() =>
      source.emit(
        "live_metrics",
        JSON.stringify(
          liveMetricsEvent({ session_id: "sess-2", tps_1s: 42 }),
        ),
      ),
    );
    await waitFor(() =>
      expect(result.current.liveMetrics["sess-2:c-1"]?.tps_1s).toBe(42),
    );
    expect(Object.keys(result.current.liveMetrics)).toEqual(["sess-2:c-1"]);
  });
});
