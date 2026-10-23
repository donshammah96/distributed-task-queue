import { JobEnvelope, JobProcessor } from "@dtq/queue-core";
import { logger } from "@dtq/telemetry";

export interface WebhookJobPayload {
  url?: string;
  method?: string;
  headers?: Record<string, string>;
  data?: unknown;
  chaosFailureRate?: number; // 0.0 - 1.0 (e.g. 0.3 = 30% synthetic failure)
}

export const webhookDispatcherProcessor: JobProcessor<WebhookJobPayload> = async (
  job: JobEnvelope<WebhookJobPayload>
) => {
  const { payload, id, attempt } = job;
  const failureRate = payload?.chaosFailureRate ?? 0;

  logger.info(
    { jobId: id, attempt, url: payload?.url || "https://api.webhook.internal/events" },
    "Processing webhook dispatch"
  );

  // Simulate I/O network latency (100ms - 300ms)
  const networkDelay = 100 + Math.random() * 200;
  await new Promise((resolve) => setTimeout(resolve, networkDelay));

  // Chaos injection check
  if (failureRate > 0 && Math.random() < failureRate) {
    logger.warn(
      { jobId: id, attempt, failureRate },
      "Chaos injection triggered: simulated 503 Service Unavailable downstream"
    );
    throw new Error(
      `Webhook dispatch failed (Downstream HTTP 503 Service Unavailable, Chaos Rate: ${Math.round(failureRate * 100)}%)`
    );
  }

  return {
    delivered: true,
    statusCode: 200,
    jobId: id,
    latencyMs: Math.round(networkDelay),
    timestamp: new Date().toISOString(),
  };
};
