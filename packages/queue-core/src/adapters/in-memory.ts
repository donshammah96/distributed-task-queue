import { CronExpressionParser } from "cron-parser";
import { randomUUID } from "crypto";
import { DeadLetterRepository } from "@dtq/database";
import { logger } from "@dtq/telemetry";
import { calculateBackoff } from "../backoff.js";
import {
  IBrokerAdapter,
  JobEnvelope,
  JobProcessor,
  QueueTelemetry,
  WorkerPoolOptions,
} from "../types.js";

interface InternalJobState<T = unknown> {
  job: JobEnvelope<T>;
  enqueuedAt: number;
  availableAt: number;
}

interface QueueState {
  waiting: InternalJobState[];
  active: Map<string, InternalJobState>;
  completed: InternalJobState[];
  failed: InternalJobState[];
  delayed: InternalJobState[];
  dlq: InternalJobState[];
  isPaused: boolean;
  worker?: {
    handler: JobProcessor<any>;
    options: WorkerPoolOptions;
  };
  cronJobs: Map<string, { interval: NodeJS.Timeout; cron: string }>;
}

export class InMemoryAdapter implements IBrokerAdapter {
  readonly name = "memory" as const;
  private isConnected = false;
  private readonly queues = new Map<string, QueueState>();
  private readonly dlqRepository: DeadLetterRepository;
  private processInterval: NodeJS.Timeout | null = null;

  constructor(dlqRepository?: DeadLetterRepository) {
    this.dlqRepository = dlqRepository ?? new DeadLetterRepository();
  }

  async connect(): Promise<void> {
    this.isConnected = true;
    if (!this.processInterval) {
      this.processInterval = setInterval(() => this.tick(), 25);
    }
    logger.debug("InMemoryAdapter connected");
  }

  async disconnect(): Promise<void> {
    this.isConnected = false;
    if (this.processInterval) {
      clearInterval(this.processInterval);
      this.processInterval = null;
    }
    for (const q of this.queues.values()) {
      for (const cron of q.cronJobs.values()) {
        clearInterval(cron.interval);
      }
    }
    logger.debug("InMemoryAdapter disconnected");
  }

  private getOrCreateQueue(queue: string): QueueState {
    let state = this.queues.get(queue);
    if (!state) {
      state = {
        waiting: [],
        active: new Map(),
        completed: [],
        failed: [],
        delayed: [],
        dlq: [],
        isPaused: false,
        cronJobs: new Map(),
      };
      this.queues.set(queue, state);
    }
    return state;
  }

  async enqueue<T>(queue: string, job: JobEnvelope<T>): Promise<string> {
    const q = this.getOrCreateQueue(queue);
    const item: InternalJobState<T> = {
      job: { ...job, id: job.id || randomUUID() },
      enqueuedAt: Date.now(),
      availableAt: Date.now(),
    };

    q.waiting.push(item as InternalJobState);
    // Sort by priority descending
    q.waiting.sort((a, b) => b.job.priority - a.job.priority);
    return item.job.id;
  }

  async enqueueBatch<T>(
    queue: string,
    jobs: JobEnvelope<T>[]
  ): Promise<string[]> {
    const ids: string[] = [];
    for (const job of jobs) {
      const id = await this.enqueue(queue, job);
      ids.push(id);
    }
    return ids;
  }

  async scheduleCron<T>(
    queue: string,
    cronExpression: string,
    jobTemplate: Omit<JobEnvelope<T>, "id">
  ): Promise<void> {
    const q = this.getOrCreateQueue(queue);

    // Cancel existing cron if any
    const existing = q.cronJobs.get(cronExpression);
    if (existing) {
      clearInterval(existing.interval);
    }

    const cron = CronExpressionParser.parse(cronExpression);
    let nextDate = cron.next().toDate();

    const checkCron = async () => {
      if (Date.now() >= nextDate.getTime()) {
        const id = randomUUID();
        await this.enqueue(queue, {
          ...(jobTemplate as JobEnvelope<T>),
          id,
          cronExpression,
        });
        nextDate = cron.next().toDate();
      }
    };

    const interval = setInterval(checkCron, 1000);
    q.cronJobs.set(cronExpression, { interval, cron: cronExpression });
  }

  async registerWorker<T>(
    queue: string,
    handler: JobProcessor<T>,
    options: WorkerPoolOptions
  ): Promise<void> {
    const q = this.getOrCreateQueue(queue);
    q.worker = { handler, options };
  }

  async getQueueMetrics(queue: string): Promise<QueueTelemetry> {
    const q = this.getOrCreateQueue(queue);
    return {
      queue,
      waiting: q.waiting.length,
      active: q.active.size,
      completed: q.completed.length,
      failed: q.failed.length,
      delayed: q.delayed.length,
      dlq: q.dlq.length,
      paused: q.isPaused,
      throughputJobsPerSec: 0,
    };
  }

  async pauseQueue(queue: string): Promise<void> {
    const q = this.getOrCreateQueue(queue);
    q.isPaused = true;
  }

  async resumeQueue(queue: string): Promise<void> {
    const q = this.getOrCreateQueue(queue);
    q.isPaused = false;
  }

  private async tick(): Promise<void> {
    const now = Date.now();

    for (const [queueName, q] of this.queues.entries()) {
      if (q.isPaused || !q.worker) continue;

      // 1. Move ready delayed jobs back to waiting
      const stillDelayed: InternalJobState[] = [];
      for (const item of q.delayed) {
        if (item.availableAt <= now) {
          q.waiting.push(item);
        } else {
          stillDelayed.push(item);
        }
      }
      q.delayed = stillDelayed;

      // 2. Sort waiting queue by priority descending
      q.waiting.sort((a, b) => b.job.priority - a.job.priority);

      // 3. Dispatch to worker pool up to max concurrency
      const maxConcurrency = q.worker.options.concurrency || 1;
      while (q.active.size < maxConcurrency && q.waiting.length > 0) {
        const item = q.waiting.shift();
        if (!item) break;

        q.active.set(item.job.id, item);

        // Process asynchronously without blocking loop
        void this.processItem(queueName, q, item);
      }
    }
  }

  private async processItem(
    queueName: string,
    q: QueueState,
    item: InternalJobState
  ): Promise<void> {
    if (!q.worker) return;

    try {
      await q.worker.handler(item.job);
      q.active.delete(item.job.id);
      q.completed.push(item);
    } catch (error) {
      q.active.delete(item.job.id);
      item.job.attempt = (item.job.attempt || 0) + 1;

      if (item.job.attempt < item.job.maxAttempts) {
        // Retry with backoff
        const delay = calculateBackoff(item.job.attempt, {
          strategy: item.job.backoffStrategy,
          baseDelayMs: item.job.baseDelayMs,
        });
        item.availableAt = Date.now() + delay;
        q.delayed.push(item);
      } else {
        // Exceeded maxAttempts -> Dead Letter Queue
        q.failed.push(item);
        q.dlq.push(item);
        try {
          await this.dlqRepository.storeDeadLetter({
            originalJobId: item.job.id,
            queueName,
            payload: item.job.payload,
            failureReason: error instanceof Error ? error.message : String(error),
          });
        } catch {
          // Ignore DLQ persistence error in pure in-memory test mode
        }
      }
    }
  }
}
