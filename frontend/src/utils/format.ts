/**
 * This file defines frontend behavior for format.
 * It is part of the IICPC frontend and keeps UI, API, or test behavior
 * scoped to this module so callers can rely on stable boundaries.
 */
/**
 * formatLatencyNs performs the module-specific operation described by its name.
 * It keeps inputs, side effects, and returned values within this module's contract.
 */
export function formatLatencyNs(value: number): string {
  if (!Number.isFinite(value)) return "-";
  if (value === 0) return "0 us";
  return formatLatencyUs(value / 1000);
}

/**
 * formatLatencyUs performs the module-specific operation described by its name.
 * It keeps inputs, side effects, and returned values within this module's contract.
 */
export function formatLatencyUs(value: number): string {
  if (!Number.isFinite(value)) return "-";
  if (value === 0) return "0 us";
  const microseconds = value;
  if (microseconds < 1000) return `${microseconds.toFixed(1)} us`;
  const milliseconds = microseconds / 1000;
  if (milliseconds < 1000) return `${milliseconds.toFixed(2)} ms`;
  return `${(milliseconds / 1000).toFixed(2)} s`;
}

/**
 * formatJitterUs formats a P-G jitter percentile (microseconds) for the
 * leaderboard: one decimal place, em-dash for "no data" (null/undefined) or
 * the zero-inversions convention shared with CorrectnessScoreEvent.
 */
export function formatJitterUs(value: number | null | undefined): string {
  if (value === null || value === undefined || !Number.isFinite(value) || value === 0) {
    return "—";
  }
  return `${value.toFixed(1)} us`;
}

/**
 * formatNumber performs the module-specific operation described by its name.
 * It keeps inputs, side effects, and returned values within this module's contract.
 */
export function formatNumber(value: number, digits = 0): string {
  if (!Number.isFinite(value)) return "-";
  return value.toLocaleString("en-US", {
    maximumFractionDigits: digits,
    minimumFractionDigits: digits,
  });
}

/**
 * formatPct performs the module-specific operation described by its name.
 * It keeps inputs, side effects, and returned values within this module's contract.
 */
export function formatPct(value: number): string {
  if (!Number.isFinite(value)) return "-";
  const pct = value <= 1 ? value * 100 : value;
  return `${pct.toFixed(1)}%`;
}

/**
 * shortId performs the module-specific operation described by its name.
 * It keeps inputs, side effects, and returned values within this module's contract.
 */
export function shortId(value: string, length = 12): string {
  return value.length > length ? value.slice(0, length) : value;
}

/**
 * formatClock performs the module-specific operation described by its name.
 * It keeps inputs, side effects, and returned values within this module's contract.
 */
export function formatClock(value: string): string {
  const date = new Date(value);
  if (Number.isNaN(date.getTime())) return "-";
  return date.toLocaleTimeString("en-US", { hour12: false });
}
