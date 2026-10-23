import { BullMQAdapter } from "@dtq/adapter-bullmq";
import { NatsJetStreamAdapter } from "@dtq/adapter-nats";
import { IBrokerAdapter, InMemoryAdapter } from "@dtq/queue-core";
import { logger } from "@dtq/telemetry";

export function createBrokerAdapter(): IBrokerAdapter {
  const adapterType = (process.env["BROKER_ADAPTER"] || "memory").toLowerCase();

  switch (adapterType) {
    case "bullmq":
      logger.info("Initializing BullMQ / Redis broker adapter");
      return new BullMQAdapter();
    case "nats":
      logger.info("Initializing NATS JetStream broker adapter");
      return new NatsJetStreamAdapter();
    case "memory":
    default:
      logger.info("Initializing InMemory broker adapter");
      return new InMemoryAdapter();
  }
}
