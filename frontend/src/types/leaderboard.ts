/**
 * This file defines frontend behavior for leaderboard.
 * It is part of the IICPC frontend and keeps UI, API, or test behavior
 * scoped to this module so callers can rely on stable boundaries.
 */
/**
 * LeaderboardStatus describes structured data exchanged by this module.
 * Keep this shape aligned with API and component expectations.
 */
export type LeaderboardStatus =
  | "scored"
  | "running"
  | "failed"
  | "disqualified";

/**
 * LeaderboardEntry describes structured data exchanged by this module.
 * Keep this shape aligned with API and component expectations.
 */
export interface LeaderboardEntry {
  rank: number;
  run_group_id: string;
  submission_id: string;
  contestant_id: string;
  team_name: string;
  peak_sustained_tps: number;
  p99_ns_at_peak_tps: number;
  spike_recovery_ns: number;
  total_correctness: number;
  disqualified: boolean;
  disqualification_code?: string;
  rank_delta: number;
  computed_at_ns: number;
  jitter_p99_us?: number;
}

/**
 * LeaderboardResponse describes structured data exchanged by this module.
 * Keep this shape aligned with API and component expectations.
 */
export interface LeaderboardResponse {
  source: string;
  rows: LeaderboardEntry[];
  next_cursor?: string;
}

/**
 * LeaderboardUpdateEvent describes structured data exchanged by this module.
 * Keep this shape aligned with API and component expectations.
 */
export interface LeaderboardUpdateEvent {
  run_group_id: string;
  submission_id: string;
  contestant_id: string;
  team_name: string;
  rank: number;
  rank_delta: number;
  peak_sustained_tps: number;
  p99_ns_at_peak_tps: number;
  spike_recovery_ns: number;
  total_correctness: number;
  disqualified: boolean;
  disqualification_code?: string;
  updated_at_ns: number;
}

/**
 * SSEEvent describes structured data exchanged by this module.
 * Keep this shape aligned with API and component expectations.
 */
export interface LiveMetricsEvent {
  contestant_id: string;
  session_id: string;
  wave_index: number;
  p50_ns: number;
  p99_ns: number;
  p999_ns: number;
  tps_1s: number;
  error_rate: number;
  updated_at_ns: number;
}

export type SSEEvent =
  | { type: "snapshot"; data: LeaderboardResponse }
  | { type: "update"; data: LeaderboardUpdateEvent }
  | { type: "live_metrics"; data: LiveMetricsEvent };

/**
 * statusForEntry performs the module-specific operation described by its name.
 * It keeps inputs, side effects, and returned values within this module's contract.
 */
export function statusForEntry(entry: LeaderboardEntry): LeaderboardStatus {
  return entry.disqualified ? "disqualified" : "scored";
}
