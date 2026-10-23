import { z } from "zod";
import { W3CTraceContext } from "@dtq/telemetry";

export const BackoffStrategySchema = z.enum(["EXPONENTIAL_JITTER", "FIXED"]);
export type BackoffStrategy = z.infer<typeof BackoffStrategySchema>;

export const W3CTraceContextSchema = z.object({
  traceparent: z.string().optional(),
  tracestate: z.string().optional(),
});

export const JobEnvelopeSchema = z.object({
  id: z.string().uuid(),
  idempotencyKey: z.string().min(1),
  name: z.string().min(1),
  payload: z.unknown(),
  priority: z.number().int().default(0),
  maxAttempts: z.number().int().min(1).default(3),
  backoffStrategy: BackoffStrategySchema.default("EXPONENTIAL_JITTER"),
  baseDelayMs: z.number().int().min(100).default(1000),
  cronExpression: z.string().optional(),
  parentJobId: z.string().uuid().optional(),
  traceContext: W3CTraceContextSchema.optional(),
  attempt: z.number().int().min(0).default(0),
});

export type JobEnvelope<TPayload = unknown> = Omit<
  z.infer<typeof JobEnvelopeSchema>,
  "payload"
> & {
  payload: TPayload;
};

export interface QueueTelemetry {
  queue: string;
  waiting: number;
  active: number;
  completed: number;
  failed: number;
  delayed: number;
  dlq: number;
  paused: boolean;
  throughputJobsPerSec?: number;
}

export interface WorkerPoolOptions {
  concurrency: number;
  useWorkerThreads?: boolean;
  lockDurationMs?: number;
  heartbeatIntervalMs?: number;
}

export type JobProcessor<T = unknown> = (job: JobEnvelope<T>) => Promise<unknown>;

export interface IBrokerAdapter {
  readonly name: "bullmq" | "nats" | "memory";
  connect(): Promise<void>;
  disconnect(): Promise<void>;
  enqueue<T>(queue: string, job: JobEnvelope<T>): Promise<string>;
  enqueueBatch<T>(queue: string, jobs: JobEnvelope<T>[]): Promise<string[]>;
  scheduleCron<T>(
    queue: string,
    cron: string,
    jobTemplate: Omit<JobEnvelope<T>, "id">
  ): Promise<void>;
  registerWorker<T>(
    queue: string,
    handler: JobProcessor<T>,
    options: WorkerPoolOptions
  ): Promise<void>;
  getQueueMetrics(queue: string): Promise<QueueTelemetry>;
  pauseQueue(queue: string): Promise<void>;
  resumeQueue(queue: string): Promise<void>;
}
