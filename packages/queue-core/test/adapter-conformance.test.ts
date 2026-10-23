import { describe, it, expect, beforeEach, afterEach } from "vitest";
import { InMemoryAdapter } from "../src/adapters/in-memory.js";
import { IBrokerAdapter, JobEnvelope } from "../src/types.js";

/**
 * Reusable conformance test suite ensuring Liskov Substitution Principle
 * across any IBrokerAdapter implementation (InMemoryAdapter, BullMQAdapter, NatsJetStreamAdapter).
 */
export function runAdapterConformanceSuite(
  name: string,
  createAdapter: () => IBrokerAdapter
) {
  describe(`IBrokerAdapter Conformance Suite: ${name}`, () => {
    let adapter: IBrokerAdapter;

    beforeEach(async () => {
      adapter = createAdapter();
      await adapter.connect();
    });

    afterEach(async () => {
      await adapter.disconnect();
    });

    it("should enqueue a job and return a valid string ID", async () => {
      const job: JobEnvelope = {
        id: "job-conformance-1",
        idempotencyKey: "key-c1",
        name: "test.conformance",
        payload: { greeting: "hello world" },
        priority: 1,
        maxAttempts: 3,
        backoffStrategy: "EXPONENTIAL_JITTER",
        baseDelayMs: 1000,
        attempt: 0,
      };

      const jobId = await adapter.enqueue("conformance-queue", job);
      expect(jobId).toBe("job-conformance-1");

      const metrics = await adapter.getQueueMetrics("conformance-queue");
      expect(metrics.waiting).toBeGreaterThanOrEqual(1);
    });

    it("should enqueue a batch of jobs atomically", async () => {
      const jobs: JobEnvelope[] = [
        {
          id: "job-batch-1",
          idempotencyKey: "k-batch-1",
          name: "batch.task",
          payload: {},
          priority: 0,
          maxAttempts: 3,
          backoffStrategy: "FIXED",
          baseDelayMs: 1000,
          attempt: 0,
        },
        {
          id: "job-batch-2",
          idempotencyKey: "k-batch-2",
          name: "batch.task",
          payload: {},
          priority: 0,
          maxAttempts: 3,
          backoffStrategy: "FIXED",
          baseDelayMs: 1000,
          attempt: 0,
        },
      ];

      const ids = await adapter.enqueueBatch("batch-queue", jobs);
      expect(ids).toEqual(["job-batch-1", "job-batch-2"]);
    });

    it("should register a worker and process enqueued jobs with priority ordering", async () => {
      const processed: string[] = [];

      await adapter.registerWorker(
        "priority-queue",
        async (job) => {
          processed.push(job.id);
        },
        { concurrency: 1 }
      );

      // Enqueue lower priority first, then higher priority
      await adapter.enqueue("priority-queue", {
        id: "low-priority",
        idempotencyKey: "k-low",
        name: "low",
        payload: {},
        priority: 1,
        maxAttempts: 3,
        backoffStrategy: "FIXED",
        baseDelayMs: 1000,
        attempt: 0,
      });

      await adapter.enqueue("priority-queue", {
        id: "high-priority",
        idempotencyKey: "k-high",
        name: "high",
        payload: {},
        priority: 10,
        maxAttempts: 3,
        backoffStrategy: "FIXED",
        baseDelayMs: 1000,
        attempt: 0,
      });

      // Allow worker ticks to process
      await new Promise((resolve) => setTimeout(resolve, 200));

      expect(processed.length).toBe(2);
      expect(processed[0]).toBe("high-priority");
      expect(processed[1]).toBe("low-priority");
    });

    it("should pause and resume queue processing", async () => {
      const processed: string[] = [];

      await adapter.registerWorker(
        "pause-queue",
        async (job) => {
          processed.push(job.id);
        },
        { concurrency: 2 }
      );

      await adapter.pauseQueue("pause-queue");
      const metricsPaused = await adapter.getQueueMetrics("pause-queue");
      expect(metricsPaused.paused).toBe(true);

      await adapter.enqueue("pause-queue", {
        id: "job-while-paused",
        idempotencyKey: "k-paused",
        name: "paused.task",
        payload: {},
        priority: 0,
        maxAttempts: 3,
        backoffStrategy: "FIXED",
        baseDelayMs: 1000,
        attempt: 0,
      });

      await new Promise((resolve) => setTimeout(resolve, 100));
      expect(processed.length).toBe(0);

      await adapter.resumeQueue("pause-queue");
      const metricsResumed = await adapter.getQueueMetrics("pause-queue");
      expect(metricsResumed.paused).toBe(false);

      await new Promise((resolve) => setTimeout(resolve, 150));
      expect(processed).toContain("job-while-paused");
    });
  });
}

// Conformance execution against InMemoryAdapter
runAdapterConformanceSuite("InMemoryAdapter", () => new InMemoryAdapter());
