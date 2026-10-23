import { describe, it, expect } from "vitest";
import { BullMQAdapter } from "../src/bullmq-adapter.js";

describe("BullMQAdapter Unit Test", () => {
  it("should instantiate with default configuration", () => {
    const adapter = new BullMQAdapter();
    expect(adapter.name).toBe("bullmq");
  });

  it("should expose FlowProducer getter", () => {
    const adapter = new BullMQAdapter();
    const flowProducer = adapter.getFlowProducer();
    expect(flowProducer).toBeDefined();
  });
});
