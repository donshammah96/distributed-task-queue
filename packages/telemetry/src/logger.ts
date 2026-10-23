import { pino } from "pino";

export const logger = pino({
  level: process.env["LOG_LEVEL"] || "info",
  transport:
    process.env["NODE_ENV"] !== "production"
      ? {
          target: "pino-pretty",
          options: {
            colorize: true,
            translateTime: "HH:MM:ss Z",
            ignore: "pid,hostname",
          },
        }
      : undefined,
  base: {
    service: process.env["OTEL_SERVICE_NAME"] || "distributed-task-queue",
  },
});

export type Logger = typeof logger;
