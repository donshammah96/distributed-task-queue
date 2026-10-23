import { describe, it, expect } from "vitest";
import { NatsJetStreamAdapter } from "../src/nats-adapter.js";

describe("NatsJetStreamAdapter Unit Test", () => {
  it("should instantiate with default configuration", () => {
    const adapter = new NatsJetStreamAdapter();
    expect(adapter.name).toBe("nats");
  });

  it("should support pause and resume state", async () => {
    const adapter = new NatsJetStreamAdapter();
    await adapter.pauseQueue("test-queue");
    const metrics = await adapter.getQueueMetrics("test-queue");
    expect(metrics.paused).toBe(true);

    await adapter.resumeQueue("test-queue");
    const metricsAfter = await adapter.getQueueMetrics("test-queue");
    expect(metricsAfter.paused).toBe(false);
  });
});
