import { describe, it, expect, vi } from "vitest";
import { IdempotencyStatus, IdempotencyRecord } from "@dtq/database";
import {
  createIdempotentConsumer,
  JobLockedError,
} from "../src/idempotency.js";
import { JobEnvelope } from "../src/types.js";

// In-Memory Idempotency Repository implementation for deterministic isolated unit testing
class MockIdempotencyRepository {
  private records = new Map<string, IdempotencyRecord>();

  async acquireLock(params: {
    idempotencyKey: string;
    jobId: string;
    lockDurationMs: number;
  }) {
    const existing = this.records.get(params.idempotencyKey);
    const now = new Date();
    const lockedUntil = new Date(now.getTime() + params.lockDurationMs);

    if (!existing) {
      const record: IdempotencyRecord = {
        idempotencyKey: params.idempotencyKey,
        jobId: params.jobId,
        status: IdempotencyStatus.IN_PROGRESS,
        lockedUntil,
        resultHash: null,
        createdAt: now,
        updatedAt: now,
      };
      this.records.set(params.idempotencyKey, record);
      return { acquired: true, status: IdempotencyStatus.IN_PROGRESS, record, reason: "lock_acquired" as const };
    }

    if (existing.status === IdempotencyStatus.COMPLETED) {
      return { acquired: false, status: IdempotencyStatus.COMPLETED, record: existing, reason: "already_completed" as const };
    }

    if (existing.lockedUntil > now) {
      return { acquired: false, status: existing.status, record: existing, reason: "currently_locked" as const };
    }

    // Expired lock takeover
    existing.status = IdempotencyStatus.IN_PROGRESS;
    existing.jobId = params.jobId;
    existing.lockedUntil = lockedUntil;
    existing.updatedAt = now;
    return { acquired: true, status: IdempotencyStatus.IN_PROGRESS, record: existing, reason: "lock_acquired" as const };
  }

  async extendLock(idempotencyKey: string, additionalMs: number) {
    const record = this.records.get(idempotencyKey);
    if (record && record.status === IdempotencyStatus.IN_PROGRESS) {
      record.lockedUntil = new Date(Date.now() + additionalMs);
      return true;
    }
    return false;
  }

  async markCompleted(idempotencyKey: string, resultHash?: string) {
    const record = this.records.get(idempotencyKey);
    if (record) {
      record.status = IdempotencyStatus.COMPLETED;
      record.resultHash = resultHash ?? null;
      record.updatedAt = new Date();
    }
  }

  async markFailed(idempotencyKey: string) {
    const record = this.records.get(idempotencyKey);
    if (record) {
      record.status = IdempotencyStatus.FAILED;
      record.updatedAt = new Date();
    }
  }
}

describe("Idempotent Consumer Middleware & At-Least-Once Chaos", () => {
  it("should execute job and transition status to COMPLETED", async () => {
    const repo = new MockIdempotencyRepository();
    const handler = vi.fn().mockResolvedValue({ success: true, processedCount: 42 });

    const consumer = createIdempotentConsumer(handler, {
      repository: repo as any,
    });

    const job: JobEnvelope = {
      id: "job-1",
      idempotencyKey: "key-1",
      name: "test.job",
      payload: { value: 123 },
      priority: 0,
      maxAttempts: 3,
      backoffStrategy: "EXPONENTIAL_JITTER",
      baseDelayMs: 1000,
      attempt: 0,
    };

    const result = await consumer(job);
    expect(result).toEqual({ success: true, processedCount: 42 });
    expect(handler).toHaveBeenCalledTimes(1);

    // Second execution with same key should be skipped gracefully
    const duplicateResult = await consumer(job);
    expect(duplicateResult).toMatchObject({ skipped: true, reason: "already_completed" });
    expect(handler).toHaveBeenCalledTimes(1); // handler NOT called again
  });

  it("should fail with JobLockedError when lock is active on another worker", async () => {
    const repo = new MockIdempotencyRepository();
    // Simulate lock already taken and active
    await repo.acquireLock({
      idempotencyKey: "locked-key",
      jobId: "other-worker-job",
      lockDurationMs: 60000,
    });

    const handler = vi.fn();
    const consumer = createIdempotentConsumer(handler, {
      repository: repo as any,
    });

    const job: JobEnvelope = {
      id: "job-2",
      idempotencyKey: "locked-key",
      name: "test.job",
      payload: {},
      priority: 0,
      maxAttempts: 3,
      backoffStrategy: "EXPONENTIAL_JITTER",
      baseDelayMs: 1000,
      attempt: 0,
    };

    await expect(consumer(job)).rejects.toThrow(JobLockedError);
    expect(handler).not.toHaveBeenCalled();
  });

  it("At-Least-Once Chaos Test: 10 concurrent duplicate submissions execute side-effect exactly once", async () => {
    const repo = new MockIdempotencyRepository();
    let sideEffectExecutionCount = 0;

    const sideEffectHandler = async (_job: JobEnvelope) => {
      // Simulate asynchronous work
      await new Promise((resolve) => setTimeout(resolve, 50));
      sideEffectExecutionCount++;
      return { executionId: sideEffectExecutionCount };
    };

    const consumer = createIdempotentConsumer(sideEffectHandler, {
      repository: repo as any,
      lockDurationMs: 30000,
    });

    const identicalKey = "chaos-key-duplicate-999";
    const concurrentJobs: JobEnvelope[] = Array.from({ length: 10 }, (_, i) => ({
      id: `job-chaos-${i}`,
      idempotencyKey: identicalKey,
      name: "chaos.financial.transfer",
      payload: { amount: 1000, destinationAccount: "NL91BUNQ1234" },
      priority: 0,
      maxAttempts: 3,
      backoffStrategy: "EXPONENTIAL_JITTER",
      baseDelayMs: 1000,
      attempt: 0,
    }));

    // Trigger all 10 submissions concurrently
    const settled = await Promise.allSettled(
      concurrentJobs.map((job) => consumer(job))
    );

    // Assert that the side-effect handler was executed EXACTLY ONCE
    expect(sideEffectExecutionCount).toBe(1);

    // Other requests either were skipped or intercepted as locked
    const successfulExecutions = settled.filter(
      (s) => s.status === "fulfilled" && !(s.value as any)?.skipped
    );
    expect(successfulExecutions.length).toBe(1);
  });
});
