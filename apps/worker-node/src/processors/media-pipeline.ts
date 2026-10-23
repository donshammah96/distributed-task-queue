import { JobEnvelope, JobProcessor } from "@dtq/queue-core";
import { logger } from "@dtq/telemetry";

export interface MediaJobPayload {
  mediaId?: string;
  sourceUrl?: string;
  workflowId?: string;
  stepData?: any;
  dependencies?: string[];
}

export const mediaDagPipelineProcessor: JobProcessor<MediaJobPayload> = async (
  job: JobEnvelope<MediaJobPayload>
) => {
  const { payload, id, name } = job;

  logger.info(
    { jobId: id, stepName: name, workflowId: payload?.workflowId },
    "Executing DAG media pipeline stage"
  );

  // Simulate video transcoding / thumbnail slicing
  await new Promise((resolve) => setTimeout(resolve, 200));

  return {
    stage: name,
    mediaId: payload?.mediaId || id,
    workflowId: payload?.workflowId,
    status: "COMPLETED",
    outputArtifacts: [
      `artifact-${name}-1080p.mp4`,
      `artifact-${name}-thumb.webp`,
    ],
    timestamp: new Date().toISOString(),
  };
};
