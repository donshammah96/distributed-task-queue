import client from "prom-client";

// Global Prometheus Registry
export const register = new client.Registry();

// Add default system metrics (cpu, event loop, memory, etc.)
client.collectDefaultMetrics({ register });

// 1. Total jobs processed counter
export const jobsProcessedCounter = new client.Counter({
  name: "queue_jobs_processed_total",
  help: "Total number of jobs processed across queues",
  labelNames: ["queue", "adapter", "status"] as const,
  registers: [register],
});

// 2. Job execution duration histogram
export const jobDurationHistogram = new client.Histogram({
  name: "queue_job_duration_seconds",
  help: "Duration of job executions in seconds",
  labelNames: ["queue", "adapter"] as const,
  buckets: [0.01, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10, 30],
  registers: [register],
});

// 3. Active workers gauge
export const activeWorkersGauge = new client.Gauge({
  name: "queue_active_workers",
  help: "Number of active workers processing jobs concurrently",
  labelNames: ["queue", "adapter"] as const,
  registers: [register],
});

// 4. Dead-letter queue depth gauge
export const dlqDepthGauge = new client.Gauge({
  name: "queue_dlq_depth",
  help: "Depth of dead letter queue for a given queue",
  labelNames: ["queue"] as const,
  registers: [register],
});

/**
 * Returns raw Prometheus metric scrape output.
 */
export async function getMetrics(): Promise<string> {
  return register.metrics();
}

/**
 * Returns content type for metrics header.
 */
export function getContentType(): string {
  return register.contentType;
}
