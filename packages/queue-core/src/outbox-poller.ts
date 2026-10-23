import { OutboxRepository, OutboxEvent } from "@dtq/database";
import { logger } from "@dtq/telemetry";
import { IBrokerAdapter, JobEnvelope, JobEnvelopeSchema } from "./types.js";

export interface OutboxRelayPollerOptions {
  broker: IBrokerAdapter;
  repository?: OutboxRepository;
  pollIntervalMs?: number;
  batchSize?: number;
  maxRetries?: number;
}

export class OutboxRelayPoller {
  private readonly broker: IBrokerAdapter;
  private readonly repository: OutboxRepository;
  private readonly pollIntervalMs: number;
  private readonly batchSize: number;
  private readonly maxRetries: number;
  private isRunning = false;
  private timer: NodeJS.Timeout | null = null;
  private isPolling = false;

  constructor(options: OutboxRelayPollerOptions) {
    this.broker = options.broker;
    this.repository = options.repository ?? new OutboxRepository();
    this.pollIntervalMs = options.pollIntervalMs ?? 1000;
    this.batchSize = options.batchSize ?? 100;
    this.maxRetries = options.maxRetries ?? 5;
  }

  start(): void {
    if (this.isRunning) return;
    this.isRunning = true;
    logger.info(
      { intervalMs: this.pollIntervalMs, batchSize: this.batchSize },
      "OutboxRelayPoller started"
    );
    this.scheduleNextPoll(0);
  }

  stop(): void {
    this.isRunning = false;
    if (this.timer) {
      clearTimeout(this.timer);
      this.timer = null;
    }
    logger.info("OutboxRelayPoller stopped");
  }

  private scheduleNextPoll(delayMs: number): void {
    if (!this.isRunning) return;
    this.timer = setTimeout(async () => {
      try {
        await this.pollAndDispatch();
      } catch (err) {
        logger.error({ err }, "Error during OutboxRelayPoller cycle");
      } finally {
        if (this.isRunning) {
          this.scheduleNextPoll(this.pollIntervalMs);
        }
      }
    }, delayMs);
  }

  async pollAndDispatch(): Promise<number> {
    if (this.isPolling) return 0;
    this.isPolling = true;

    try {
      // 1. Fetch pending events with transactional row-level lock (SKIP LOCKED)
      const events: OutboxEvent[] = await this.repository.fetchPendingBatch(
        this.batchSize
      );

      if (events.length === 0) {
        return 0;
      }

      logger.debug(
        { count: events.length },
        "OutboxRelayPoller fetched pending events"
      );

      let dispatchedCount = 0;

      for (const event of events) {
        try {
          // Parse or construct the JobEnvelope
          let job: JobEnvelope;
          const rawPayload = event.payload as any;

          if (rawPayload && typeof rawPayload === "object" && rawPayload.idempotencyKey) {
            const parsed = JobEnvelopeSchema.safeParse(rawPayload);
            if (parsed.success) {
              job = parsed.data as JobEnvelope;
            } else {
              job = {
                id: event.aggregateId || event.id,
                idempotencyKey: `${event.topic}:${event.aggregateId}:${event.id}`,
                name: event.aggregateType,
                payload: rawPayload,
                priority: 0,
                maxAttempts: 3,
                backoffStrategy: "EXPONENTIAL_JITTER",
                baseDelayMs: 1000,
                attempt: 0,
              };
            }
          } else {
            job = {
              id: event.aggregateId || event.id,
              idempotencyKey: `${event.topic}:${event.aggregateId}:${event.id}`,
              name: event.aggregateType,
              payload: rawPayload,
              priority: 0,
              maxAttempts: 3,
              backoffStrategy: "EXPONENTIAL_JITTER",
              baseDelayMs: 1000,
              attempt: 0,
            };
          }

          // 2. Dispatch to broker
          await this.broker.enqueue(event.topic, job);

          // 3. Mark as published
          await this.repository.markPublished(event.id);
          dispatchedCount++;
        } catch (error) {
          logger.error(
            { error, eventId: event.id, topic: event.topic },
            "Failed to dispatch outbox event to broker"
          );
          await this.repository.incrementRetry(event.id, this.maxRetries);
        }
      }

      return dispatchedCount;
    } finally {
      this.isPolling = false;
    }
  }
}
