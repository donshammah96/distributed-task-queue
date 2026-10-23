import {
  Queue,
  Worker,
  QueueEvents,
  FlowProducer,
  Job as BullJob,
  JobsOptions,
  ConnectionOptions,
} from "bullmq";
import { Redis } from "ioredis";
import { DeadLetterRepository, JobExecutionLogRepository } from "@dtq/database";
import {
  IBrokerAdapter,
  JobEnvelope,
  JobProcessor,
  QueueTelemetry,
  WorkerPoolOptions,
  calculateBackoff,
} from "@dtq/queue-core";
import {
  logger,
  injectTraceContext,
  extractTraceContext,
  withTraceSpan,
  jobsProcessedCounter,
  jobDurationHistogram,
  activeWorkersGauge,
  dlqDepthGauge,
} from "@dtq/telemetry";

export interface BullMQAdapterOptions {
  connection?: ConnectionOptions;
  dlqRepository?: DeadLetterRepository;
  logRepository?: JobExecutionLogRepository;
}

export class BullMQAdapter implements IBrokerAdapter {
  readonly name = "bullmq" as const;
  private readonly connectionOptions: ConnectionOptions;
  private readonly queues = new Map<string, Queue>();
  private readonly dlqQueues = new Map<string, Queue>();
  private readonly workers = new Map<string, Worker>();
  private readonly queueEvents = new Map<string, QueueEvents>();
  private readonly dlqRepository: DeadLetterRepository;
  private readonly logRepository: JobExecutionLogRepository;
  private flowProducer: FlowProducer | null = null;
  private sharedRedis: Redis | null = null;

  constructor(options: BullMQAdapterOptions = {}) {
    this.connectionOptions = options.connection ?? {
      host: process.env["REDIS_HOST"] || "localhost",
      port: Number(process.env["REDIS_PORT"] || 6379),
      password: process.env["REDIS_PASSWORD"] || undefined,
      maxRetriesPerRequest: null,
    };
    this.dlqRepository = options.dlqRepository ?? new DeadLetterRepository();
    this.logRepository = options.logRepository ?? new JobExecutionLogRepository();
  }

  async connect(): Promise<void> {
    this.sharedRedis = new Redis(this.connectionOptions as any);
    this.flowProducer = new FlowProducer({ connection: this.connectionOptions });
    logger.info("BullMQAdapter connected to Redis");
  }

  async disconnect(): Promise<void> {
    for (const worker of this.workers.values()) {
      await worker.close();
    }
    for (const q of this.queues.values()) {
      await q.close();
    }
    for (const dlq of this.dlqQueues.values()) {
      await dlq.close();
    }
    for (const qe of this.queueEvents.values()) {
      await qe.close();
    }
    if (this.flowProducer) {
      await this.flowProducer.close();
      this.flowProducer = null;
    }
    if (this.sharedRedis) {
      this.sharedRedis.disconnect();
      this.sharedRedis = null;
    }
    logger.info("BullMQAdapter disconnected");
  }

  getFlowProducer(): FlowProducer {
    if (!this.flowProducer) {
      this.flowProducer = new FlowProducer({ connection: this.connectionOptions });
    }
    return this.flowProducer;
  }

  private getQueue(name: string): Queue {
    let q = this.queues.get(name);
    if (!q) {
      q = new Queue(name, {
        connection: this.connectionOptions,
        defaultJobOptions: {
          removeOnComplete: 1000,
          removeOnFail: 1000,
        },
      });
      this.queues.set(name, q);
    }
    return q;
  }

  private getDLQ(name: string): Queue {
    const dlqName = `${name}-dlq`;
    let dlq = this.dlqQueues.get(dlqName);
    if (!dlq) {
      dlq = new Queue(dlqName, { connection: this.connectionOptions });
      this.dlqQueues.set(dlqName, dlq);
    }
    return dlq;
  }

  async enqueue<T>(queueName: string, job: JobEnvelope<T>): Promise<string> {
    const q = this.getQueue(queueName);

    // Inject active OpenTelemetry trace context if not already present
    const traceContext = job.traceContext ?? injectTraceContext();
    const enrichedJob: JobEnvelope<T> = {
      ...job,
      traceContext,
    };

    const jobOptions: JobsOptions = {
      jobId: job.id,
      priority: job.priority,
      attempts: job.maxAttempts,
      backoff: {
        type: "custom",
        delay: job.baseDelayMs,
      },
    };

    const bullJob = await q.add(job.name, enrichedJob, jobOptions);
    return bullJob.id ?? job.id;
  }

  async enqueueBatch<T>(
    queueName: string,
    jobs: JobEnvelope<T>[]
  ): Promise<string[]> {
    const q = this.getQueue(queueName);
    const bulkData = jobs.map((job) => {
      const traceContext = job.traceContext ?? injectTraceContext();
      return {
        name: job.name,
        data: { ...job, traceContext },
        opts: {
          jobId: job.id,
          priority: job.priority,
          attempts: job.maxAttempts,
          backoff: {
            type: "custom",
            delay: job.baseDelayMs,
          },
        } as JobsOptions,
      };
    });

    const added = await q.addBulk(bulkData);
    return added.map((j) => j.id ?? "");
  }

  async scheduleCron<T>(
    queueName: string,
    cron: string,
    jobTemplate: Omit<JobEnvelope<T>, "id">
  ): Promise<void> {
    const q = this.getQueue(queueName);
    await q.add(
      jobTemplate.name,
      {
        ...jobTemplate,
        cronExpression: cron,
        traceContext: injectTraceContext(),
      },
      {
        repeat: {
          pattern: cron,
        },
      }
    );
  }

  async registerWorker<T>(
    queueName: string,
    handler: JobProcessor<T>,
    options: WorkerPoolOptions
  ): Promise<void> {
    const worker = new Worker(
      queueName,
      async (bullJob: BullJob) => {
        const envelope = bullJob.data as JobEnvelope<T>;
        const parentContext = extractTraceContext(envelope.traceContext);
        const startTime = Date.now();

        activeWorkersGauge.inc({ queue: queueName, adapter: this.name });

        return withTraceSpan(
          `worker.${queueName}.process`,
          async (span) => {
            span.setAttribute("job.id", envelope.id);
            span.setAttribute("job.name", envelope.name);
            span.setAttribute("job.idempotencyKey", envelope.idempotencyKey);
            span.setAttribute("job.attempt", bullJob.attemptsMade + 1);

            try {
              const result = await handler(envelope);
              const durationSec = (Date.now() - startTime) / 1000;

              jobsProcessedCounter.inc({
                queue: queueName,
                adapter: this.name,
                status: "success",
              });
              jobDurationHistogram.observe(
                { queue: queueName, adapter: this.name },
                durationSec
              );

              await this.logRepository.logExecution({
                jobId: envelope.id,
                queueName,
                adapterType: this.name,
                attempt: bullJob.attemptsMade + 1,
                status: "SUCCESS",
                durationMs: Math.round(durationSec * 1000),
                traceId: span.spanContext().traceId,
              });

              return result;
            } catch (error) {
              const durationSec = (Date.now() - startTime) / 1000;
              jobsProcessedCounter.inc({
                queue: queueName,
                adapter: this.name,
                status: "failed",
              });

              await this.logRepository.logExecution({
                jobId: envelope.id,
                queueName,
                adapterType: this.name,
                attempt: bullJob.attemptsMade + 1,
                status: "FAILED",
                durationMs: Math.round(durationSec * 1000),
                errorStack: error instanceof Error ? error.stack : String(error),
                traceId: span.spanContext().traceId,
              });

              throw error;
            } finally {
              activeWorkersGauge.dec({ queue: queueName, adapter: this.name });
            }
          },
          parentContext
        );
      },
      {
        connection: this.connectionOptions,
        concurrency: options.concurrency,
        lockDuration: options.lockDurationMs ?? 30000,
        settings: {
          backoffStrategy: (
            attemptsMade: number,
            _type?: string,
            _err?: Error,
            job?: any
          ) => {
            const env = job?.data as JobEnvelope<any> | undefined;
            return calculateBackoff(attemptsMade, {
              strategy: env?.backoffStrategy ?? "EXPONENTIAL_JITTER",
              baseDelayMs: env?.baseDelayMs ?? 1000,
            });
          },
        },
      }
    );

    // Terminal Failure / Dead-Letter Queue hook
    worker.on("failed", async (job, err) => {
      if (!job) return;
      const attemptsMade = job.attemptsMade;
      const maxAttempts = job.opts.attempts ?? 3;

      if (attemptsMade >= maxAttempts) {
        logger.warn(
          { jobId: job.id, queue: queueName, attemptsMade, err: err.message },
          "Job exceeded max attempts, routing to Dead-Letter Queue"
        );

        const dlq = this.getDLQ(queueName);
        await dlq.add(job.name, {
          ...job.data,
          terminalError: err.message,
          failedAt: new Date().toISOString(),
        });

        dlqDepthGauge.inc({ queue: queueName });

        await this.dlqRepository.storeDeadLetter({
          originalJobId: job.id || "",
          queueName,
          payload: job.data,
          failureReason: err.message || "Max attempts exceeded",
        });
      }
    });

    this.workers.set(queueName, worker);
    logger.info(
      { queue: queueName, concurrency: options.concurrency },
      "BullMQ worker registered"
    );
  }

  async getQueueMetrics(queueName: string): Promise<QueueTelemetry> {
    const q = this.getQueue(queueName);
    const dlq = this.getDLQ(queueName);

    const [waiting, active, completed, failed, delayed, dlqCount, isPaused] =
      await Promise.all([
        q.getWaitingCount(),
        q.getActiveCount(),
        q.getCompletedCount(),
        q.getFailedCount(),
        q.getDelayedCount(),
        dlq.getWaitingCount(),
        q.isPaused(),
      ]);

    dlqDepthGauge.set({ queue: queueName }, dlqCount);

    return {
      queue: queueName,
      waiting,
      active,
      completed,
      failed,
      delayed,
      dlq: dlqCount,
      paused: isPaused,
    };
  }

  async pauseQueue(queueName: string): Promise<void> {
    const q = this.getQueue(queueName);
    await q.pause();
  }

  async resumeQueue(queueName: string): Promise<void> {
    const q = this.getQueue(queueName);
    await q.resume();
  }
}
