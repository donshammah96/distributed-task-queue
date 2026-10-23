import { describe, it, expect } from "vitest";
import { calculateBackoff } from "../src/backoff.js";

describe("Backoff Math Engine", () => {
  it("should return 0 delay for attempt 0", () => {
    const delay = calculateBackoff(0);
    expect(delay).toBe(0);
  });

  it("should calculate fixed backoff accurately", () => {
    const delay = calculateBackoff(3, {
      strategy: "FIXED",
      baseDelayMs: 2500,
      maxCapMs: 10000,
    });
    expect(delay).toBe(2500);
  });

  it("should enforce maxCapMs in fixed strategy", () => {
    const delay = calculateBackoff(3, {
      strategy: "FIXED",
      baseDelayMs: 25000,
      maxCapMs: 10000,
    });
    expect(delay).toBe(10000);
  });

  it("should strictly respect mathematical bounds of Full Jitter Exponential Backoff", () => {
    const base = 1000;
    const cap = 30000;

    for (let attempt = 1; attempt <= 10; attempt++) {
      const ceiling = Math.min(cap, base * Math.pow(2, attempt));

      // Test with min jitter (randomFn returning 0)
      const minDelay = calculateBackoff(attempt, {
        strategy: "EXPONENTIAL_JITTER",
        baseDelayMs: base,
        maxCapMs: cap,
        randomFn: () => 0,
      });
      expect(minDelay).toBe(0);

      // Test with max jitter (randomFn returning 1.0)
      const maxDelay = calculateBackoff(attempt, {
        strategy: "EXPONENTIAL_JITTER",
        baseDelayMs: base,
        maxCapMs: cap,
        randomFn: () => 1.0,
      });
      expect(maxDelay).toBeLessThanOrEqual(ceiling + 1);
      expect(maxDelay).toBeGreaterThanOrEqual(ceiling);

      // Test standard statistical random values stay within [0, ceiling]
      for (let i = 0; i < 20; i++) {
        const sample = calculateBackoff(attempt, {
          strategy: "EXPONENTIAL_JITTER",
          baseDelayMs: base,
          maxCapMs: cap,
        });
        expect(sample).toBeGreaterThanOrEqual(0);
        expect(sample).toBeLessThanOrEqual(ceiling);
      }
    }
  });

  it("should never overflow even with very large attempt numbers", () => {
    const delay = calculateBackoff(1000, {
      strategy: "EXPONENTIAL_JITTER",
      baseDelayMs: 1000,
      maxCapMs: 60000,
    });
    expect(delay).toBeGreaterThanOrEqual(0);
    expect(delay).toBeLessThanOrEqual(60000);
  });
});
