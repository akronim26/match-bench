/**
 * This file defines tests for useSSE.test.
 * It is part of the IICPC frontend and keeps UI, API, or test behavior
 * scoped to this module so callers can rely on stable boundaries.
 */
import { act, renderHook } from "@testing-library/react";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { useSSE } from "./useSSE";

/**
 * FakeEventSource describes structured data exchanged by this module.
 * Keep this shape aligned with API and component expectations.
 */

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

describe("useSSE", () => {
  beforeEach(() => {
    FakeEventSource.instances = [];
    vi.stubGlobal("EventSource", FakeEventSource);
  });

  afterEach(() => {
    vi.unstubAllGlobals();
  });

  it.each([
    {
      name: "snapshot carries the raw LeaderboardResponse",
      event: "snapshot" as const,
      payload: { source: "store", rows: [], next_cursor: "abc" },
    },
    {
      name: "update carries the flat LeaderboardUpdateEvent",
      event: "update" as const,
      payload: {
        run_group_id: "rg-1",
        submission_id: "sub-1",
        contestant_id: "c-1",
        team_name: "Ada",
        rank: 1,
        rank_delta: 2,
        peak_sustained_tps: 9001,
        p99_ns_at_peak_tps: 2000,
        spike_recovery_ns: 3000,
        total_correctness: 0.99,
        disqualified: false,
        updated_at_ns: 1717,
      },
    },
    {
      name: "live_metrics carries the LiveMetricsEvent payload",
      event: "live_metrics" as const,
      payload: {
        contestant_id: "c-1",
        session_id: "sess-1",
        wave_index: 3,
        p50_ns: 100_000,
        p99_ns: 900_000,
        p999_ns: 1_500_000,
        tps_1s: 12345.6,
        error_rate: 0.01,
        updated_at_ns: 9999,
      },
    },
  ])("dispatches named events: $name", ({ event, payload }) => {
    const onMessage = vi.fn();
    renderHook(() => useSSE("/api/leaderboard/v1/events", { onMessage }));
    const source = FakeEventSource.instances[0];
    act(() => source.emit(event, JSON.stringify(payload)));
    expect(onMessage).toHaveBeenCalledTimes(1);
    expect(onMessage).toHaveBeenCalledWith({ type: event, data: payload });
  });

  it("connects to the URL as-is, without a token query param", () => {
    renderHook(() =>
      useSSE("/api/leaderboard/v1/events", { onMessage: vi.fn() }),
    );
    expect(FakeEventSource.instances).toHaveLength(1);
    expect(FakeEventSource.instances[0].url).toBe("/api/leaderboard/v1/events");
  });

  it("does not connect while disabled", () => {
    renderHook(() =>
      useSSE("/api/leaderboard/v1/events", {
        enabled: false,
        onMessage: vi.fn(),
      }),
    );
    expect(FakeEventSource.instances).toHaveLength(0);
  });

  it("closes the stream when enabled flips to false", () => {
    const { rerender } = renderHook(
      ({ enabled }) =>
        useSSE("/api/leaderboard/v1/events", { enabled, onMessage: vi.fn() }),
      { initialProps: { enabled: true } },
    );
    expect(FakeEventSource.instances).toHaveLength(1);
    rerender({ enabled: false });
    expect(FakeEventSource.instances[0].closed).toBe(true);
    expect(FakeEventSource.instances).toHaveLength(1);
  });

  it("ignores malformed payloads and keeps the stream alive", () => {
    const onMessage = vi.fn();
    renderHook(() => useSSE("/api/leaderboard/v1/events", { onMessage }));
    const source = FakeEventSource.instances[0];
    act(() => source.emit("update", "{not json"));
    expect(onMessage).not.toHaveBeenCalled();
    expect(source.closed).toBe(false);
  });
});
