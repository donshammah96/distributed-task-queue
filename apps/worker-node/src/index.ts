import fastify from "fastify";
import { BullMQAdapter } from "@dtq/adapter-bullmq";
import { NatsJetStreamAdapter } from "@dtq/adapter-nats";
import {
  IBrokerAdapter,
  InMemoryAdapter,
  createIdempotentConsumer,
} from "@dtq/queue-core";
import {
  logger,
  getMetrics,
  getContentType,
} from "@dtq/telemetry";
import { webhookDispatcherProcessor } from "./processors/webhook.js";
import { reportPdfGeneratorProcessor } from "./processors/report-pdf.js";
import { mediaDagPipelineProcessor } from "./processors/media-pipeline.js";

function createBroker(): IBrokerAdapter {
  const adapterType = (process.env["BROKER_ADAPTER"] || "memory").toLowerCase();
  switch (adapterType) {
    case "bullmq":
      logger.info("Worker node using BullMQ broker");
      return new BullMQAdapter();
    case "nats":
      logger.info("Worker node using NATS JetStream broker");
      return new NatsJetStreamAdapter();
    case "memory":
    default:
      logger.info("Worker node using InMemory broker");
      return new InMemoryAdapter();
  }
}

async function main() {
  const broker = createBroker();
  await broker.connect();

  const concurrency = Number(process.env["WORKER_CONCURRENCY"] || 10);
  const useWorkerThreads = process.env["WORKER_USE_THREADS"] === "true";
  const lockDurationMs = Number(process.env["WORKER_LOCK_DURATION_MS"] || 30000);
  const heartbeatIntervalMs = Number(process.env["WORKER_HEARTBEAT_INTERVAL_MS"] || 5000);

  const workerOptions = {
    concurrency,
    useWorkerThreads,
    lockDurationMs,
    heartbeatIntervalMs,
  };

  // Register worker processors with Idempotent Consumer Middleware
  await broker.registerWorker(
    "webhook.dispatcher",
    createIdempotentConsumer(webhookDispatcherProcessor, {
      lockDurationMs,
      heartbeatIntervalMs,
    }),
    workerOptions
  );

  await broker.registerWorker(
    "report.pdf.generator",
    createIdempotentConsumer(reportPdfGeneratorProcessor, {
      lockDurationMs,
      heartbeatIntervalMs,
    }),
    workerOptions
  );

  await broker.registerWorker(
    "media.dag.pipeline",
    createIdempotentConsumer(mediaDagPipelineProcessor, {
      lockDurationMs,
      heartbeatIntervalMs,
    }),
    workerOptions
  );

  logger.info(
    { concurrency, useWorkerThreads },
    "All worker pool processors registered and ready"
  );

  // Setup small HTTP server for Prometheus metrics and health check
  const metricsApp = fastify({ logger: false });

  metricsApp.get("/health", async () => ({
    status: "healthy",
    role: "worker-node",
    broker: broker.name,
    concurrency,
    uptimeSeconds: Math.floor(process.uptime()),
  }));

  metricsApp.get("/metrics", async (_req, reply) => {
    reply.header("Content-Type", getContentType());
    return reply.send(await getMetrics());
  });

  const metricsPort = Number(process.env["WORKER_METRICS_PORT"] || 4001);
  await metricsApp.listen({ port: metricsPort, host: "0.0.0.0" });
  logger.info(`Worker metrics endpoint running on port ${metricsPort}`);

  // Graceful shutdown handling
  let isShuttingDown = false;
  const shutdown = async (signal: string) => {
    if (isShuttingDown) return;
    isShuttingDown = true;

    logger.info({ signal }, "Graceful shutdown initiated. Draining worker pool (up to 30s timeout)...");

    const drainTimeout = setTimeout(() => {
      logger.error("Drain timeout reached, forcing exit.");
      process.exit(1);
    }, 30000);

    try {
      await metricsApp.close();
      await broker.disconnect();
      clearTimeout(drainTimeout);
      logger.info("Worker pool drained and connections closed cleanly.");
      process.exit(0);
    } catch (err) {
      logger.error({ err }, "Error during worker shutdown");
      process.exit(1);
    }
  };

  process.on("SIGTERM", () => shutdown("SIGTERM"));
  process.on("SIGINT", () => shutdown("SIGINT"));
}

main().catch((err) => {
  logger.fatal({ err }, "Worker node fatal error");
  process.exit(1);
});
