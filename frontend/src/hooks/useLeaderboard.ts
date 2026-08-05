/**
 * This file defines frontend behavior for useLeaderboard.
 * It is part of the IICPC frontend and keeps UI, API, or test behavior
 * scoped to this module so callers can rely on stable boundaries.
 */
"use client";

import { useCallback, useState } from "react";
import { useQuery, useQueryClient } from "@tanstack/react-query";
import { getLeaderboard } from "@/api/leaderboard";
import { ApiError } from "@/api/client";
import { platformConfig } from "@/config/platform";
import type {
  LeaderboardEntry,
  LeaderboardResponse,
  LeaderboardUpdateEvent,
  LiveMetricsEvent,
  SSEEvent,
} from "@/types/leaderboard";
import { useSSE, type SSEStatus } from "./useSSE";

/**
 * applyLeaderboardUpdate performs the module-specific operation described by its name.
 * It keeps inputs, side effects, and returned values within this module's contract.
 */
export function applyLeaderboardUpdate(
  current: LeaderboardResponse,
  update: LeaderboardUpdateEvent,
): LeaderboardResponse {
  const matches = (row: LeaderboardEntry) =>
    row.run_group_id === update.run_group_id &&
    row.contestant_id === update.contestant_id;
  const existing = current.rows.find(matches);
  // LeaderboardUpdateEvent (the SSE "update" payload) doesn't carry jitter_p99_us
  // — that only comes from the initial REST snapshot — so preserve it from the
  // matched row rather than dropping it on every live update.
  const entry: LeaderboardEntry = {
    ...existing,
    rank: update.rank,
    run_group_id: update.run_group_id,
    submission_id: update.submission_id,
    contestant_id: update.contestant_id,
    team_name: update.team_name,
    peak_sustained_tps: update.peak_sustained_tps,
    p99_ns_at_peak_tps: update.p99_ns_at_peak_tps,
    spike_recovery_ns: update.spike_recovery_ns,
    total_correctness: update.total_correctness,
    disqualified: update.disqualified,
    disqualification_code: update.disqualification_code,
    rank_delta: update.rank_delta,
    computed_at_ns: update.updated_at_ns,
  };
  const rows = existing
    ? current.rows.map((row) => (matches(row) ? entry : row))
    : [...current.rows, entry];
  return { ...current, rows };
}

/**
 * useLeaderboard performs the module-specific operation described by its name.
 * It keeps inputs, side effects, and returned values within this module's contract.
 */
export function useLeaderboard(scenario?: string) {
  const queryClient = useQueryClient();
  const [sseStatus, setSseStatus] = useState<SSEStatus>("closed");
  const [flashedRows, setFlashedRows] = useState<Set<string>>(new Set());
  const [liveMetrics, setLiveMetrics] = useState<
    Record<string, LiveMetricsEvent>
  >({});

  const query = useQuery<LeaderboardResponse, ApiError>({
    queryKey: ["leaderboard", scenario ?? "all"],
    queryFn: () =>
      getLeaderboard({ scenario, limit: platformConfig.leaderboardLimit }),
  });

  const flash = useCallback((contestantId: string) => {
    setFlashedRows((current) => new Set(current).add(contestantId));
    setTimeout(() => {
      setFlashedRows((current) => {
        const next = new Set(current);
        next.delete(contestantId);
        return next;
      });
    }, 400);
  }, []);

  const key = scenario ?? "all";
  const onMessage = useCallback(
    (event: SSEEvent) => {
      if (event.type === "snapshot") {
        queryClient.setQueryData(["leaderboard", key], event.data);
      }
      if (event.type === "update") {
        queryClient.setQueryData<LeaderboardResponse>(
          ["leaderboard", key],
          (current) => {
            if (!current) return current;
            return applyLeaderboardUpdate(current, event.data);
          },
        );
        flash(event.data.contestant_id);
      }
      if (event.type === "live_metrics") {
        const metricsKey = `${event.data.session_id}:${event.data.contestant_id}`;
        setLiveMetrics((current) => ({
          ...current,
          [metricsKey]: event.data,
        }));
      }
    },
    [flash, queryClient, key],
  );

  useSSE(platformConfig.endpoints.leaderboard.events, {
    enabled: Boolean(query.data && !query.error),
    onMessage,
    onStatusChange: setSseStatus,
  });

  return {
    data: query.data,
    isLoading: query.isLoading,
    error: query.error,
    sseStatus,
    flashedRows,
    liveMetrics,
  };
}
