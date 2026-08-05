/**
 * This file defines frontend behavior for LeaderboardTable.
 * It is part of the IICPC frontend and keeps UI, API, or test behavior
 * scoped to this module so callers can rely on stable boundaries.
 */
import { SkeletonRows } from "@/components/common/Skeleton";
import type { LeaderboardEntry } from "@/types/leaderboard";
import type { SortBy, SortOrder } from "./LeaderboardClient";
import { LeaderboardRow } from "./LeaderboardRow";
import styles from "./LeaderboardTable.module.css";

/**
 * Props describes structured data exchanged by this module.
 * Keep this shape aligned with API and component expectations.
 */
interface Props {
  rows: LeaderboardEntry[];
  loading: boolean;
  ownContestantId?: string;
  flashedRows: Set<string>;
  sortBy: SortBy;
  sortOrder: SortOrder;
}

const headers = [
  ["rank", "#"],
  ["team_name", "TEAM"],
  ["peak_sustained_tps", "PEAK TPS"],
  ["total_correctness", "CORRECT"],
  ["jitter_p99_us", "JITTER P99"],
  ["rank_delta", "Δ"],
  ["status", "STATUS"],
] as const;

/**
 * LeaderboardTable performs the module-specific operation described by its name.
 * It keeps inputs, side effects, and returned values within this module's contract.
 */
export function LeaderboardTable({
  rows,
  loading,
  ownContestantId,
  flashedRows,
  sortBy,
  sortOrder,
}: Props) {
  return (
    <div className={styles.wrap}>
      <table className={styles.table} aria-label="Leaderboard">
        <colgroup>
          <col className={styles.rankCol} />
          <col />
          <col className={styles.tpsCol} />
          <col className={styles.correctCol} />
          <col className={styles.jitterCol} />
          <col className={styles.deltaCol} />
          <col className={styles.statusCol} />
        </colgroup>
        <thead>
          <tr>
            {headers.map(([key, label]) => (
              <th
                key={key}
                scope="col"
                aria-sort={
                  key === sortBy
                    ? sortOrder === "asc"
                      ? "ascending"
                      : "descending"
                    : undefined
                }
              >
                {label}
              </th>
            ))}
          </tr>
        </thead>
        <tbody>
          {loading ? (
            <SkeletonRows columns={7} />
          ) : rows.length === 0 ? (
            <tr>
              <td colSpan={7} className={styles.emptyCell}>
                No ranked results for this scenario yet.
              </td>
            </tr>
          ) : (
            rows.map((entry) => (
              <LeaderboardRow
                key={entry.contestant_id}
                entry={entry}
                isOwnRow={ownContestantId === entry.contestant_id}
                flash={flashedRows.has(entry.contestant_id)}
              />
            ))
          )}
        </tbody>
      </table>
    </div>
  );
}
