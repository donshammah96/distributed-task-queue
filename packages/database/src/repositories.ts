import {
  IdempotencyRecord,
  IdempotencyStatus,
  OutboxEvent,
  OutboxStatus,
  DeadLetterJob,
  JobExecutionLog,
} from "@prisma/client";
import { prisma } from "./client.js";

export interface AcquireLockParams {
  idempotencyKey: string;
  jobId: string;
  lockDurationMs: number;
}

export interface AcquireLockResult {
  acquired: boolean;
  status: IdempotencyStatus;
  record?: IdempotencyRecord;
  reason?: "already_completed" | "currently_locked" | "lock_acquired";
}

export class IdempotencyRepository {
  constructor(private readonly db = prisma) {}

  /**
   * Atomically acquires an idempotency lock if record does not exist,
   * or if lock has expired and status is not COMPLETED.
   */
  async acquireLock(params: AcquireLockParams): Promise<AcquireLockResult> {
    const now = new Date();
    const lockedUntil = new Date(now.getTime() + params.lockDurationMs);

    // Try finding existing record
    const existing = await this.db.idempotencyRecord.findUnique({
      where: { idempotencyKey: params.idempotencyKey },
    });

    if (!existing) {
      // Not seen yet, create new IN_PROGRESS record
      try {
        const created = await this.db.idempotencyRecord.create({
          data: {
            idempotencyKey: params.idempotencyKey,
            jobId: params.jobId,
            status: IdempotencyStatus.IN_PROGRESS,
            lockedUntil,
          },
        });
        return {
          acquired: true,
          status: IdempotencyStatus.IN_PROGRESS,
          record: created,
          reason: "lock_acquired",
        };
      } catch (err) {
        // Race condition: another worker inserted concurrently
        return this.acquireLock(params);
      }
    }

    if (existing.status === IdempotencyStatus.COMPLETED) {
      return {
        acquired: false,
        status: IdempotencyStatus.COMPLETED,
        record: existing,
        reason: "already_completed",
      };
    }

    // Check if previous lock is still active
    if (existing.lockedUntil > now) {
      return {
        acquired: false,
        status: existing.status,
        record: existing,
        reason: "currently_locked",
      };
    }

    // Lock has expired or previous attempt failed, acquire ownership with optimistic update
    const updated = await this.db.idempotencyRecord.updateMany({
      where: {
        idempotencyKey: params.idempotencyKey,
        status: { not: IdempotencyStatus.COMPLETED },
        lockedUntil: { lte: now },
      },
      data: {
        jobId: params.jobId,
        status: IdempotencyStatus.IN_PROGRESS,
        lockedUntil,
        updatedAt: now,
      },
    });

    if (updated.count > 0) {
      const fresh = await this.db.idempotencyRecord.findUnique({
        where: { idempotencyKey: params.idempotencyKey },
      });
      return {
        acquired: true,
        status: IdempotencyStatus.IN_PROGRESS,
        record: fresh ?? undefined,
        reason: "lock_acquired",
      };
    }

    return {
      acquired: false,
      status: existing.status,
      record: existing,
      reason: "currently_locked",
    };
  }

  async extendLock(
    idempotencyKey: string,
    additionalMs: number
  ): Promise<boolean> {
    const lockedUntil = new Date(Date.now() + additionalMs);
    const res = await this.db.idempotencyRecord.updateMany({
      where: {
        idempotencyKey,
        status: IdempotencyStatus.IN_PROGRESS,
      },
      data: {
        lockedUntil,
        updatedAt: new Date(),
      },
    });
    return res.count > 0;
  }

  async markCompleted(
    idempotencyKey: string,
    resultHash?: string
  ): Promise<void> {
    await this.db.idempotencyRecord.updateMany({
      where: { idempotencyKey },
      data: {
        status: IdempotencyStatus.COMPLETED,
        resultHash,
        updatedAt: new Date(),
      },
    });
  }

  async markFailed(idempotencyKey: string): Promise<void> {
    await this.db.idempotencyRecord.updateMany({
      where: { idempotencyKey },
      data: {
        status: IdempotencyStatus.FAILED,
        updatedAt: new Date(),
      },
    });
  }
}

export class OutboxRepository {
  constructor(private readonly db = prisma) {}

  /**
   * Polls PENDING rows using SELECT ... FOR UPDATE SKIP LOCKED
   */
  async fetchPendingBatch(batchSize: number = 100): Promise<OutboxEvent[]> {
    try {
      // Execute raw query with SKIP LOCKED for high-throughput concurrency
      const rawEvents = await this.db.$queryRaw<OutboxEvent[]>`
        SELECT * FROM "outbox_events"
        WHERE "status" = 'PENDING'
        ORDER BY "created_at" ASC
        LIMIT ${batchSize}
        FOR UPDATE SKIP LOCKED
      `;
      return rawEvents;
    } catch {
      // Fallback for database engines without FOR UPDATE SKIP LOCKED support (e.g. SQLite / mock testing)
      return this.db.outboxEvent.findMany({
        where: { status: OutboxStatus.PENDING },
        orderBy: { createdAt: "asc" },
        take: batchSize,
      });
    }
  }

  async markPublished(id: string): Promise<void> {
    await this.db.outboxEvent.update({
      where: { id },
      data: {
        status: OutboxStatus.PUBLISHED,
        processedAt: new Date(),
      },
    });
  }

  async incrementRetry(id: string, maxRetries: number = 5): Promise<void> {
    const event = await this.db.outboxEvent.findUnique({ where: { id } });
    if (!event) return;

    const newRetryCount = event.retryCount + 1;
    await this.db.outboxEvent.update({
      where: { id },
      data: {
        retryCount: newRetryCount,
        status:
          newRetryCount >= maxRetries
            ? OutboxStatus.FAILED
            : OutboxStatus.PENDING,
        processedAt: newRetryCount >= maxRetries ? new Date() : null,
      },
    });
  }
}

export class DeadLetterRepository {
  constructor(private readonly db = prisma) {}

  async storeDeadLetter(params: {
    originalJobId: string;
    queueName: string;
    payload: unknown;
    failureReason: string;
  }): Promise<DeadLetterJob> {
    return this.db.deadLetterJob.create({
      data: {
        originalJobId: params.originalJobId,
        queueName: params.queueName,
        payload: params.payload as any,
        failureReason: params.failureReason,
        replayed: false,
      },
    });
  }

  async listDeadLetters(params?: {
    queueName?: string;
    limit?: number;
  }): Promise<DeadLetterJob[]> {
    return this.db.deadLetterJob.findMany({
      where: {
        ...(params?.queueName ? { queueName: params.queueName } : {}),
      },
      orderBy: { failedAt: "desc" },
      take: params?.limit ?? 50,
    });
  }

  async getById(id: string): Promise<DeadLetterJob | null> {
    return this.db.deadLetterJob.findUnique({ where: { id } });
  }

  async markReplayed(id: string): Promise<void> {
    await this.db.deadLetterJob.update({
      where: { id },
      data: {
        replayed: true,
        replayedAt: new Date(),
      },
    });
  }

  async getDeadLetterCount(queueName?: string): Promise<number> {
    return this.db.deadLetterJob.count({
      where: {
        replayed: false,
        ...(queueName ? { queueName } : {}),
      },
    });
  }
}

export class JobExecutionLogRepository {
  constructor(private readonly db = prisma) {}

  async logExecution(params: {
    jobId: string;
    queueName: string;
    adapterType: string;
    attempt: number;
    status: string;
    durationMs: number;
    errorStack?: string;
    traceId?: string;
  }): Promise<JobExecutionLog> {
    return this.db.jobExecutionLog.create({
      data: {
        jobId: params.jobId,
        queueName: params.queueName,
        adapterType: params.adapterType,
        attempt: params.attempt,
        status: params.status,
        durationMs: params.durationMs,
        errorStack: params.errorStack,
        traceId: params.traceId,
      },
    });
  }

  async getLogsForJob(jobId: string): Promise<JobExecutionLog[]> {
    return this.db.jobExecutionLog.findMany({
      where: { jobId },
      orderBy: { createdAt: "desc" },
    });
  }
}
