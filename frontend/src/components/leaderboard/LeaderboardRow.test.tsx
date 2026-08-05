/**
 * This file defines tests for LeaderboardRow.test.
 * It is part of the IICPC frontend and keeps UI, API, or test behavior
 * scoped to this module so callers can rely on stable boundaries.
 */
import { render, screen } from "@testing-library/react";
import { describe, expect, it, vi } from "vitest";
import type { LeaderboardEntry } from "@/types/leaderboard";
import { LeaderboardRow } from "./LeaderboardRow";

vi.mock('next/navigation', () => ({ useRouter: () => ({ push: vi.fn() }) }));

const entry: LeaderboardEntry = {
  rank: 1,
  contestant_id: "contestant_123456789",
  team_name: "Ada",
  submission_id: "sub_1",
  run_group_id: "run_1",
  peak_sustained_tps: 9001,
  p99_ns_at_peak_tps: 2000,
  spike_recovery_ns: 3000,
  total_correctness: 0.99,
  disqualified: false,
  rank_delta: 2,
  computed_at_ns: Date.now() * 1_000_000,
};

describe("LeaderboardRow", () => {
  it("renders rank, score, and own row state", () => {
    const { container } = render(
      <table>
        <tbody>
          <LeaderboardRow entry={entry} isOwnRow flash={false} />
        </tbody>
      </table>,
    );
    expect(screen.getByText("1")).toBeInTheDocument();
    expect(screen.getByText("9,001")).toBeInTheDocument();
    expect(container.querySelector("tr")?.className).toContain("own");
  });

  it("renders an em-dash when jitter_p99_us is absent", () => {
    render(
      <table>
        <tbody>
          <LeaderboardRow entry={entry} isOwnRow={false} flash={false} />
        </tbody>
      </table>,
    );
    expect(screen.getByText("—")).toBeInTheDocument();
  });

  it("renders the formatted jitter value when present", () => {
    render(
      <table>
        <tbody>
          <LeaderboardRow
            entry={{ ...entry, jitter_p99_us: 88.25 }}
            isOwnRow={false}
            flash={false}
          />
        </tbody>
      </table>,
    );
    expect(screen.getByText("88.3 us")).toBeInTheDocument();
  });
});
