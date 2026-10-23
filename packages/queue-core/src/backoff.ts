import { BackoffStrategy } from "./types.js";

export interface BackoffOptions {
  strategy?: BackoffStrategy;
  baseDelayMs?: number;
  maxCapMs?: number;
  randomFn?: () => number; // Deterministic seed injection for unit testing
}

export const DEFAULT_BASE_DELAY_MS = 1000;
export const DEFAULT_MAX_CAP_MS = 30000;

/**
 * Calculates retry backoff delay with support for Full Jitter Exponential Backoff.
 * Mathematical formula: delay = random(0, min(cap, base * 2^attempt))
 */
export function calculateBackoff(
  attempt: number,
  options: BackoffOptions = {}
): number {
  const {
    strategy = "EXPONENTIAL_JITTER",
    baseDelayMs = DEFAULT_BASE_DELAY_MS,
    maxCapMs = DEFAULT_MAX_CAP_MS,
    randomFn = Math.random,
  } = options;

  if (attempt <= 0) {
    return 0;
  }

  if (strategy === "FIXED") {
    return Math.min(baseDelayMs, maxCapMs);
  }

  // Calculate exponential ceiling: min(cap, base * 2^attempt)
  // Ensure we avoid overflow if attempt is unusually large
  const safeAttempt = Math.min(attempt, 30);
  const exponentialCeiling = Math.min(
    maxCapMs,
    baseDelayMs * Math.pow(2, safeAttempt)
  );

  // Apply Full Jitter: uniform random value between 0 and ceiling
  const jitteredDelay = Math.floor(randomFn() * (exponentialCeiling + 1));
  return Math.max(0, jitteredDelay);
}
