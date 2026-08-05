/**
 * This file defines tests for format.test.
 * It is part of the IICPC frontend and keeps UI, API, or test behavior
 * scoped to this module so callers can rely on stable boundaries.
 */
import { describe, expect, it } from "vitest";
import { formatJitterUs, formatLatencyNs } from "./format";

describe("formatLatencyNs", () => {
  it("formats edge values", () => {
    expect(formatLatencyNs(0)).toBe("0 us");
    expect(formatLatencyNs(1_500_000_000)).toBe("1.50 s");
    expect(formatLatencyNs(Number.NaN)).toBe("-");
  });
});

describe("formatJitterUs", () => {
  it("renders an em-dash for null, undefined, and zero (no recorded inversions)", () => {
    expect(formatJitterUs(null)).toBe("—");
    expect(formatJitterUs(undefined)).toBe("—");
    expect(formatJitterUs(0)).toBe("—");
  });

  it("formats a positive value to one decimal place", () => {
    expect(formatJitterUs(42.53)).toBe("42.5 us");
    expect(formatJitterUs(100)).toBe("100.0 us");
  });
});
