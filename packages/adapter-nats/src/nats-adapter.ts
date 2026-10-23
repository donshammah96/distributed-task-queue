import {
  connect,
  NatsConnection,
  JetStreamClient,
  JetStreamManager,
  JSONCodec,
  RetentionPolicy,
  AckPolicy,
  StorageType,
  ConsumerConfig,
} from "nats";
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

export interface NatsAdapterOptions {
  servers?: string;
  user?: string;
  password?: string;
  dlqRepository?: DeadLetterRepository;
  logRepository?: JobExecutionLogRepository;
}

export class NatsJetStreamAdapter implements IBrokerAdapter {
  readonly name = "nats" as const;
  private readonly servers: string;
  private readonly user?: string;
  private readonly password?: string;
  private readonly dlqRepository: DeadLetterRepository;
  private readonly logRepository: JobExecutionLogRepository;
  private nc: NatsConnection | null = null;
  private js: JetStreamClient | null = null;
  private jsm: JetStreamManager | null = null;
  private readonly codec = JSONCodec();
  private readonly streamProvisioned = new Set<string>();
  private readonly pausedQueues = new Set<string>();
  private readonly activeWorkers = new Map<string, { abort: () => void }>();

  constructor(options: NatsAdapterOptions = {}) {
    this.servers = options.servers || process.env["NATS_SERVERS"] || "nats://localhost:4222";
    this.user = options.user || process.env["NATS_USER"];
    this.password = options.password || process.env["NATS_PASSWORD"];
    this.dlqRepository = options.dlqRepository ?? new DeadLetterRepository();
    this.logRepository = options.logRepository ?? new JobExecutionLogRepository();
  }

  async connect(): Promise<void> {
    this.nc = await connect({
      servers: this.servers,
      user: this.user,
      pass: this.password,
    });
    this.js = this.nc.jetstream();
    this.jsm = await this.nc.jetstreamManager();
    logger.info({ servers: this.servers }, "NatsJetStreamAdapter connected");
  }

  async disconnect(): Promise<void> {
    for (const worker of this.activeWorkers.values()) {
      worker.abort();
    }
    this.activeWorkers.clear();
    if (this.nc) {
      await this.nc.drain();
      this.nc = null;
      this.js = null;
      this.jsm = null;
    }
    logger.info("NatsJetStreamAdapter disconnected");
  }

  private streamNameForQueue(queue: string): string {
    const sanitized = queue.toUpperCase().replace(/[^A-Z0-9_]/g, "_");
    return `DTQ_STREAM_${sanitized}`;
  }

  private dlqStreamNameForQueue(queue: string): string {
    const sanitized = queue.toUpperCase().replace(/[^A-Z0-9_]/g, "_");
    return `DTQ_DLQ_${sanitized}`;
  }

  private async ensureStream(queue: string): Promise<void> {
    if (!this.jsm) {
      throw new Error("NATS JetStream manager not connected");
    }

    if (this.streamProvisioned.has(queue)) {
      return;
    }

    const stream = this.streamNameForQueue(queue);
    const subject = `${queue}.*`;

    try {
      await this.jsm.streams.info(stream);
    } catch {
      try {
        await this.jsm.streams.add({
          name: stream,
          subjects: [subject],
          retention: RetentionPolicy.Workqueue,
          storage: StorageType.File,
          max_age: 7 * 24 * 60 * 60 * 1e9, // 7 days in nanoseconds
        });
        logger.info({ stream, subject }, "Provisioned JetStream Workqueue stream");
      } catch (err) {
        logger.warn({ err, stream }, "Stream already exists or provisioning race");
      }
    }

    // Ensure DLQ stream as well
    const dlqStream = this.dlqStreamNameForQueue(queue);
    const dlqSubject = `${queue}.dlq`;
    try {
      await this.jsm.streams.info(dlqStream);
    } catch {
      try {
        await this.jsm.streams.add({
          name: dlqStream,
          subjects: [dlqSubject],
          retention: RetentionPolicy.Limits,
          storage: StorageType.File,
        });
        logger.info({ dlqStream, dlqSubject }, "Provisioned JetStream DLQ stream");
      } catch (err) {
        logger.warn({ err, dlqStream }, "DLQ Stream already exists or race");
      }
    }

    this.streamProvisioned.add(queue);
  }

  async enqueue<T>(queue: string, job: JobEnvelope<T>): Promise<string> {
    if (!this.js) {
      throw new Error("JetStream client not connected");
    }

    await this.ensureStream(queue);

    const traceContext = job.traceContext ?? injectTraceContext();
    const enrichedJob: JobEnvelope<T> = {
      ...job,
      traceContext,
    };

    const subject = `${queue}.${job.name}`;
    const data = this.codec.encode(enrichedJob);

    await this.js.publish(subject, data, {
      msgID: job.id,
    });

    return job.id;
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
    cron: string,
    jobTemplate: Omit<JobEnvelope<T>, "id">
  ): Promise<void> {
    // JetStream Cron schedules by publishing with metadata
    await this.enqueue(queue, {
      ...(jobTemplate as JobEnvelope<T>),
      id: crypto.randomUUID(),
      cronExpression: cron,
    });
  }

  async registerWorker<T>(
    queue: string,
    handler: JobProcessor<T>,
    options: WorkerPoolOptions
  ): Promise<void> {
    if (!this.nc || !this.js) {
      throw new Error("NATS not connected");
    }

    await this.ensureStream(queue);

    const stream = this.streamNameForQueue(queue);
    const consumerName = `worker-${queue}`;
    const consumerConfig: Partial<ConsumerConfig> = {
      durable_name: consumerName,
      ack_policy: AckPolicy.Explicit,
      ack_wait: (options.lockDurationMs ?? 30000) * 1e6, // nanos
      max_deliver: 5,
    };

    let consumer;
    try {
      consumer = await this.js.consumers.get(stream, consumerName);
    } catch {
      await this.jsm!.consumers.add(stream, consumerConfig);
      consumer = await this.js.consumers.get(stream, consumerName);
    }

    let isAborted = false;
    this.activeWorkers.set(queue, {
      abort: () => {
        isAborted = true;
      },
    });

    const concurrency = options.concurrency || 1;

    // Run parallel consumer pull loops up to concurrency
    for (let i = 0; i < concurrency; i++) {
      void (async () => {
        while (!isAborted && this.nc) {
          if (this.pausedQueues.has(queue)) {
            await new Promise((res) => setTimeout(res, 200));
            continue;
          }

          try {
            const batch = await consumer.fetch({ max_messages: 1, expires: 2000 });
            for await (const msg of batch) {
              if (isAborted) break;

              const envelope = this.codec.decode(msg.data) as JobEnvelope<T>;
              const parentContext = extractTraceContext(envelope.traceContext);
              const startTime = Date.now();
              const deliveryCount = msg.info.deliveryCount;

              activeWorkersGauge.inc({ queue, adapter: this.name });

              await withTraceSpan(
                `nats.worker.${queue}.process`,
                async (span) => {
                  span.setAttribute("job.id", envelope.id);
                  span.setAttribute("job.name", envelope.name);
                  span.setAttribute("job.deliveryCount", deliveryCount);

                  try {
                    const result = await handler(envelope);
                    const durationSec = (Date.now() - startTime) / 1000;

                    msg.ack();

                    jobsProcessedCounter.inc({
                      queue,
                      adapter: this.name,
                      status: "success",
                    });
                    jobDurationHistogram.observe(
                      { queue, adapter: this.name },
                      durationSec
                    );

                    await this.logRepository.logExecution({
                      jobId: envelope.id,
                      queueName: queue,
                      adapterType: this.name,
                      attempt: deliveryCount,
                      status: "SUCCESS",
                      durationMs: Math.round(durationSec * 1000),
                      traceId: span.spanContext().traceId,
                    });

                    return result;
                  } catch (error) {
                    const durationSec = (Date.now() - startTime) / 1000;
                    jobsProcessedCounter.inc({
                      queue,
                      adapter: this.name,
                      status: "failed",
                    });

                    await this.logRepository.logExecution({
                      jobId: envelope.id,
                      queueName: queue,
                      adapterType: this.name,
                      attempt: deliveryCount,
                      status: "FAILED",
                      durationMs: Math.round(durationSec * 1000),
                      errorStack: error instanceof Error ? error.stack : String(error),
                      traceId: span.spanContext().traceId,
                    });

                    const maxAttempts = envelope.maxAttempts || 3;

                    if (deliveryCount >= maxAttempts) {
                      logger.warn(
                        { jobId: envelope.id, queue, deliveryCount },
                        "NATS job exceeded max deliveries, routing to DLQ stream"
                      );

                      // Terminal failure: publish to DLQ and persist in Postgres
                      const dlqData = this.codec.encode({
                        ...envelope,
                        terminalError: error instanceof Error ? error.message : String(error),
                        failedAt: new Date().toISOString(),
                      });
                      await this.js!.publish(`${queue}.dlq`, dlqData);

                      dlqDepthGauge.inc({ queue });

                      await this.dlqRepository.storeDeadLetter({
                        originalJobId: envelope.id,
                        queueName: queue,
                        payload: envelope.payload,
                        failureReason: error instanceof Error ? error.message : String(error),
                      });

                      // Terminate message so workqueue drops it
                      msg.term();
                    } else {
                      // Apply Exponential Full Jitter backoff
                      const delayMs = calculateBackoff(deliveryCount, {
                        strategy: envelope.backoffStrategy,
                        baseDelayMs: envelope.baseDelayMs,
                      });
                      msg.nak(delayMs);
                    }
                  } finally {
                    activeWorkersGauge.dec({ queue, adapter: this.name });
                  }
                },
                parentContext
              );
            }
          } catch (err: any) {
            // Fetch timeout is normal when queue is empty
            if (err?.code !== "TIMEOUT" && !isAborted) {
              logger.debug({ err }, "NATS consumer fetch idle");
            }
          }
        }
      })();
    }

    logger.info({ queue, concurrency }, "NATS JetStream worker registered");
  }

  async getQueueMetrics(queue: string): Promise<QueueTelemetry> {
    let waiting = 0;
    let dlqCount = 0;

    if (this.jsm) {
      try {
        const stream = this.streamNameForQueue(queue);
        const info = await this.jsm.streams.info(stream);
        waiting = info.state.messages;
      } catch {
        waiting = 0;
      }

      try {
        const dlqStream = this.dlqStreamNameForQueue(queue);
        const dlqInfo = await this.jsm.streams.info(dlqStream);
        dlqCount = dlqInfo.state.messages;
      } catch {
        dlqCount = 0;
      }
    }

    dlqDepthGauge.set({ queue }, dlqCount);

    return {
      queue,
      waiting,
      active: 0,
      completed: 0,
      failed: 0,
      delayed: 0,
      dlq: dlqCount,
      paused: this.pausedQueues.has(queue),
    };
  }

  async pauseQueue(queue: string): Promise<void> {
    this.pausedQueues.add(queue);
  }

  async resumeQueue(queue: string): Promise<void> {
    this.pausedQueues.delete(queue);
  }
}
