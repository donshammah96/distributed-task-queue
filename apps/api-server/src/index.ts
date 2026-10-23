import { createBrokerAdapter } from "./broker.js";
import { buildServer } from "./server.js";
import { logger } from "@dtq/telemetry";

async function main() {
  const port = Number(process.env["API_PORT"] || 4000);
  const host = process.env["API_HOST"] || "0.0.0.0";

  const broker = createBrokerAdapter();
  await broker.connect();

  const { app, outboxPoller } = await buildServer({
    broker,
    startOutboxRelay: true,
  });

  const shutdown = async (signal: string) => {
    logger.info({ signal }, "Received shutdown signal, terminating API server...");
    outboxPoller.stop();
    await app.close();
    await broker.disconnect();
    logger.info("API server exited cleanly");
    process.exit(0);
  };

  process.on("SIGTERM", () => shutdown("SIGTERM"));
  process.on("SIGINT", () => shutdown("SIGINT"));

  try {
    const address = await app.listen({ port, host });
    logger.info({ address }, `API Control Plane Server listening on ${address}`);
    logger.info(`Swagger documentation available at http://localhost:${port}/docs`);
  } catch (err) {
    logger.fatal({ err }, "Failed to start API server");
    process.exit(1);
  }
}

main().catch((err) => {
  console.error("Unhandled error in api-server:", err);
  process.exit(1);
});
