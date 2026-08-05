/**
 * This file defines frontend behavior for LeaderboardRow.
 * It is part of the IICPC frontend and keeps UI, API, or test behavior
 * scoped to this module so callers can rely on stable boundaries.
 */
"use client";

import { useRouter } from "next/navigation";
import { Badge } from "@/components/common/Badge";
import { statusForEntry, type LeaderboardEntry } from "@/types/leaderboard";
import { formatJitterUs, formatNumber, formatPct, shortId } from "@/utils/format";
import { rankTone } from "@/utils/score";
import styles from "./LeaderboardRow.module.css";

/**
 * Props describes structured data exchanged by this module.
 * Keep this shape aligned with API and component expectations.
 */
interface Props {
  entry: LeaderboardEntry;
  isOwnRow: boolean;
  flash: boolean;
}

/**
 * LeaderboardRow performs the module-specific operation described by its name.
 * It keeps inputs, side effects, and returned values within this module's contract.
 */
export function LeaderboardRow({ entry, isOwnRow, flash }: Props) {
  const router = useRouter();
  const correctness = Math.max(
    0,
    Math.min(
      100,
      entry.total_correctness <= 1
        ? entry.total_correctness * 100
        : entry.total_correctness,
    ),
  );
  const openRun = () => {
    if (entry.run_group_id)
      router.push(`/run?run_group_id=${encodeURIComponent(entry.run_group_id)}`);
  };
  return (
    <tr
      className={`${styles.row} ${styles.clickable} ${isOwnRow ? styles.own : ""} ${flash ? styles.flash : ""}`}
      onClick={openRun}
      role="link"
      tabIndex={0}
      onKeyDown={(e) => {
        if (e.key === "Enter") openRun();
      }}
    >
      <td className={`${styles.rank} ${styles[rankTone(entry.rank)]}`}>
        {entry.rank}
      </td>
      <td>
        <div className={styles.name}>{entry.team_name || "Untitled team"}</div>
        <div className={styles.id}>{shortId(entry.contestant_id)}</div>
      </td>
      <td
        className={`${styles.number} ${entry.rank <= 3 ? styles.scoreHot : ""}`}
      >
        {formatNumber(entry.peak_sustained_tps)}
      </td>
      <td aria-label={`Correctness: ${correctness.toFixed(1)}%`}>
        <div className={styles.track}>
          <div className={styles.fill} style={{ width: `${correctness}%` }} />
        </div>
        <div className={styles.correctLabel}>
          {formatPct(entry.total_correctness)}
        </div>
      </td>
      <td className={styles.number}>{formatJitterUs(entry.jitter_p99_us)}</td>
      <td className={`${styles.number} ${styles.delta}`}>
        {entry.rank_delta > 0
          ? `+${entry.rank_delta}`
          : entry.rank_delta}
      </td>
      <td className={styles.status}>
        <Badge variant={statusForEntry(entry)} />
      </td>
    </tr>
  );
}
