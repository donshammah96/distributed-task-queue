import { createHash } from "crypto";
import { IdempotencyRepository } from "@dtq/database";
import { logger } from "@dtq/telemetry";
import { JobEnvelope, JobProcessor } from "./types.js";

export interface IdempotentConsumerOptions {
  repository?: IdempotencyRepository;
  lockDurationMs?: number;
  heartbeatIntervalMs?: number;
}

export class DuplicateJobError extends Error {
  constructor(public readonly idempotencyKey: string) {
    super(`Job with idempotency key '${idempotencyKey}' has already been processed.`);
    this.name = "DuplicateJobError";
  }
}

export class JobLockedError extends Error {
  constructor(public readonly idempotencyKey: string) {
    super(`Job with idempotency key '${idempotencyKey}' is currently locked by another worker.`);
    this.name = "JobLockedError";
  }
}

/**
 * Wraps a JobProcessor with atomic idempotency locks, active heartbeat renewal,
 * and deduplication guarantees to safeguard against at-least-once broker redeliveries.
 */
export function createIdempotentConsumer<T>(
  handler: JobProcessor<T>,
  options: IdempotentConsumerOptions = {}
): JobProcessor<T> {
  const repository = options.repository ?? new IdempotencyRepository();
  const lockDurationMs = options.lockDurationMs ?? 30000;
  const heartbeatIntervalMs = options.heartbeatIntervalMs ?? 5000;

  return async (job: JobEnvelope<T>): Promise<unknown> => {
    const { idempotencyKey, id: jobId } = job;

    // 1. Attempt atomic lock acquisition
    const lockResult = await repository.acquireLock({
      idempotencyKey,
      jobId,
      lockDurationMs,
    });

    if (!lockResult.acquired) {
      if (lockResult.reason === "already_completed") {
        logger.info(
          { idempotencyKey, jobId },
          "Duplicate execution skipped: job already completed"
        );
        return {
          skipped: true,
          reason: "already_completed",
          idempotencyKey,
        };
      }

      logger.warn(
        { idempotencyKey, jobId },
        "Lock contention: job currently being processed by another worker"
      );
      throw new JobLockedError(idempotencyKey);
    }

    // 2. Start heartbeat timer to prevent lock expiry for long-running jobs
    let heartbeatTimer: NodeJS.Timeout | undefined;
    heartbeatTimer = setInterval(async () => {
      try {
        await repository.extendLock(idempotencyKey, lockDurationMs);
      } catch (err) {
        logger.warn(
          { err, idempotencyKey },
          "Failed to extend idempotency lock heartbeat"
        );
      }
    }, heartbeatIntervalMs);

    try {
      // 3. Execute the user job processor
      const result = await handler(job);

      // 4. Calculate result hash for auditability
      const resultHash = result !== undefined
        ? createHash("sha256").update(JSON.stringify(result)).digest("hex")
        : undefined;

      // 5. Mark idempotency record as COMPLETED
      await repository.markCompleted(idempotencyKey, resultHash);

      return result;
    } catch (error) {
      // Mark as FAILED to allow retry according to backoff policy
      try {
        await repository.markFailed(idempotencyKey);
      } catch (err) {
        logger.error({ err, idempotencyKey }, "Failed to mark idempotency status as FAILED");
      }
      throw error;
    } finally {
      if (heartbeatTimer) {
        clearInterval(heartbeatTimer);
      }
    }
  };
}
