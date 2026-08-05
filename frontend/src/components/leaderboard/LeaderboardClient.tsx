/**
 * This file defines frontend behavior for LeaderboardClient.
 * It is part of the IICPC frontend and keeps UI, API, or test behavior
 * scoped to this module so callers can rely on stable boundaries.
 */
"use client";

import { useMemo, useState } from "react";
import { ErrorBanner } from "@/components/common/ErrorBanner";
import { Tabs } from "@/components/common/Tabs";
import { defaultContestantId, platformConfig } from "@/config/platform";
import { useLeaderboard } from "@/hooks/useLeaderboard";
import type { LeaderboardEntry } from "@/types/leaderboard";
import { LeaderboardControls } from "./LeaderboardControls";
import { ScoringRules } from "./ScoringRules";
import { LeaderboardTable } from "./LeaderboardTable";
import styles from "./LeaderboardClient.module.css";

/**
 * SortBy is the set of numeric columns the board can be ordered by. Averaged
 * percentile columns are intentionally excluded (see leaderboard column set).
 */
export type SortBy = "rank" | "peak_sustained_tps" | "total_correctness";
/**
 * SortOrder describes structured data exchanged by this module.
 * Keep this shape aligned with API and component expectations.
 */
export type SortOrder = "asc" | "desc";

/** Scenario categories the board can be filtered to. */
const CATEGORIES = [
  { key: "constant", label: "Constant" },
  { key: "ramp", label: "Ramp" },
  { key: "stress", label: "Stress" },
];

/**
 * LeaderboardClient performs the module-specific operation described by its name.
 * It keeps inputs, side effects, and returned values within this module's contract.
 */
export function LeaderboardClient() {
  const [category, setCategory] = useState<string>("constant");
  const [sortBy, setSortBy] = useState<SortBy>("rank");
  const [sortOrder, setSortOrder] = useState<SortOrder>("asc");
  const [page, setPage] = useState(1);
  const { data, isLoading, error, sseStatus, flashedRows, liveMetrics } =
    useLeaderboard(category);
  const liveMetricsList = Object.values(liveMetrics);

  const rows = useMemo(() => {
    const sorted = [...(data?.rows ?? [])].sort((a, b) => {
      const left = a[sortBy] as number;
      const right = b[sortBy] as number;
      return sortOrder === "asc" ? left - right : right - left;
    });
    return sorted.slice(
      (page - 1) * platformConfig.pageSize,
      page * platformConfig.pageSize,
    );
  }, [data?.rows, page, sortBy, sortOrder]);

  const totalPages = Math.max(
    1,
    Math.ceil((data?.rows.length ?? rows.length) / platformConfig.pageSize),
  );

  return (
    <section className={styles.page}>
      <div className={styles.topRow}>
        <Tabs
          ariaLabel="Scenario category"
          tabs={CATEGORIES}
          active={category}
          onChange={(key) => {
            setCategory(key);
            setPage(1);
          }}
        />
        <LeaderboardControls
          sortBy={sortBy}
          sortOrder={sortOrder}
          live={sseStatus === "open"}
          onSortBy={setSortBy}
          onSortOrder={() =>
            setSortOrder((value) => (value === "asc" ? "desc" : "asc"))
          }
        />
      </div>
      {error && (
        <ErrorBanner message="Leaderboard API is unavailable. Start or connect the platform leaderboard service to load live standings." />
      )}
      {liveMetricsList.length > 0 && (
        <div className={styles.liveTiles}>
          {liveMetricsList.map((metric) => (
            <div
              key={`${metric.session_id}:${metric.contestant_id}`}
              className={styles.liveTile}
            >
              <span className={styles.liveTileLabel}>
                {metric.contestant_id} · {metric.session_id}
              </span>
              <span className={styles.liveTileStat}>
                {Math.round(metric.tps_1s).toLocaleString()} tps
              </span>
              <span className={styles.liveTileStat}>
                p99 {(metric.p99_ns / 1_000_000).toFixed(1)}ms
              </span>
            </div>
          ))}
        </div>
      )}
      <div className={styles.split}>
        <div className={styles.tableCol}>
          <LeaderboardTable
            rows={rows as LeaderboardEntry[]}
            loading={isLoading && !error}
            ownContestantId={defaultContestantId()}
            flashedRows={flashedRows}
            sortBy={sortBy}
            sortOrder={sortOrder}
          />
          <div className={styles.pagination}>
            <button
              type="button"
              disabled={page === 1}
              onClick={() => setPage((value) => Math.max(1, value - 1))}
            >
              Prev
            </button>
            <span>
              Page {page} of {totalPages}
            </span>
            <button
              type="button"
              disabled={page === totalPages}
              onClick={() => setPage((value) => Math.min(totalPages, value + 1))}
            >
              Next
            </button>
          </div>
        </div>
        <ScoringRules />
      </div>
    </section>
  );
}
