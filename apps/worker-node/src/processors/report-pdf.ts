import { createHash } from "crypto";
import { JobEnvelope, JobProcessor } from "@dtq/queue-core";
import { logger } from "@dtq/telemetry";

export interface ReportJobPayload {
  reportId?: string;
  tenantId?: string;
  rowCount?: number;
  format?: "pdf" | "csv" | "xlsx";
}

export const reportPdfGeneratorProcessor: JobProcessor<ReportJobPayload> = async (
  job: JobEnvelope<ReportJobPayload>
) => {
  const { payload, id } = job;
  const rowCount = payload?.rowCount || 5000;
  const format = payload?.format || "pdf";

  logger.info(
    { jobId: id, reportId: payload?.reportId, rowCount, format },
    "Starting CPU-bound report PDF generation"
  );

  const startTime = Date.now();

  // Synthetic CPU-intensive calculation (cryptographic hash rounds to simulate PDF compression)
  let state = "init-report-matrix";
  for (let i = 0; i < Math.min(rowCount, 20000); i++) {
    state = createHash("sha256").update(state + i).digest("hex");
  }

  // Brief yield for event loop
  await new Promise((resolve) => setTimeout(resolve, 150));

  const durationMs = Date.now() - startTime;

  return {
    reportId: payload?.reportId || id,
    format,
    pages: Math.ceil(rowCount / 50),
    fileSizeBytes: rowCount * 128,
    computedChecksum: state.slice(0, 16),
    generationDurationMs: durationMs,
    s3Url: `https://storage.internal.s3/reports/${id}.${format}`,
  };
};
