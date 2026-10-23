import fastify, { FastifyInstance } from "fastify";
import cors from "@fastify/cors";
import swagger from "@fastify/swagger";
import swaggerUi from "@fastify/swagger-ui";
import {
  serializerCompiler,
  validatorCompiler,
  ZodTypeProvider,
} from "fastify-type-provider-zod";
import { z } from "zod";
import { v4 as uuidv4 } from "uuid";
import {
  prisma,
  withOutboxTransaction,
  DeadLetterRepository,
} from "@dtq/database";
import {
  IBrokerAdapter,
  JobEnvelope,
  OutboxRelayPoller,
} from "@dtq/queue-core";
import {
  logger,
  getMetrics,
  getContentType,
  injectTraceContext,
} from "@dtq/telemetry";

export interface BuildServerOptions {
  broker: IBrokerAdapter;
  startOutboxRelay?: boolean;
}

export async function buildServer(options: BuildServerOptions): Promise<{
  app: FastifyInstance;
  outboxPoller: OutboxRelayPoller;
}> {
  const { broker, startOutboxRelay = true } = options;
  const dlqRepo = new DeadLetterRepository(prisma);

  const app = fastify({
    logger: false,
  }).withTypeProvider<ZodTypeProvider>();

  app.setValidatorCompiler(validatorCompiler);
  app.setSerializerCompiler(serializerCompiler);

  await app.register(cors, {
    origin: "*",
    methods: ["GET", "POST", "PUT", "DELETE", "PATCH", "OPTIONS"],
  });

  await app.register(swagger, {
    openapi: {
      info: {
        title: "Distributed Task Queue & Scheduler API",
        description: "Control plane for job ingestion, DAG workflows, DLQ replay, and real-time telemetry",
        version: "1.0.0",
      },
      servers: [{ url: `http://localhost:${process.env["API_PORT"] || 4000}` }],
    },
  });

  await app.register(swaggerUi, {
    routePrefix: "/docs",
  });

  // Outbox Relay Poller
  const outboxPoller = new OutboxRelayPoller({
    broker,
    pollIntervalMs: Number(process.env["OUTBOX_POLL_INTERVAL_MS"] || 1000),
    batchSize: Number(process.env["OUTBOX_BATCH_SIZE"] || 100),
  });

  if (startOutboxRelay) {
    outboxPoller.start();
  }

  // Track known queues
  const knownQueues = new Set<string>([
    "webhook.dispatcher",
    "report.pdf.generator",
    "media.dag.pipeline",
    "default",
  ]);

  // --- Health Check ---
  app.get(
    "/health",
    {
      schema: {
        summary: "System Health Check",
        response: {
          200: z.object({
            status: z.string(),
            broker: z.string(),
            uptimeSeconds: z.number(),
            timestamp: z.string(),
          }),
        },
      },
    },
    async () => {
      return {
        status: "healthy",
        broker: broker.name,
        uptimeSeconds: Math.floor(process.uptime()),
        timestamp: new Date().toISOString(),
      };
    }
  );

  // --- Prometheus Metrics ---
  app.get("/metrics", async (_req, reply) => {
    reply.header("Content-Type", getContentType());
    return reply.send(await getMetrics());
  });

  // --- POST /api/v1/jobs ---
  const CreateJobSchema = z.object({
    queue: z.string().min(1).default("default"),
    name: z.string().min(1),
    payload: z.unknown().default({}),
    idempotencyKey: z.string().optional(),
    priority: z.number().int().default(0),
    maxAttempts: z.number().int().min(1).default(3),
    backoffStrategy: z.enum(["EXPONENTIAL_JITTER", "FIXED"]).default("EXPONENTIAL_JITTER"),
    baseDelayMs: z.number().int().min(100).default(1000),
    useOutbox: z.boolean().default(true),
  });

  app.post(
    "/api/v1/jobs",
    {
      schema: {
        summary: "Ingest a new job with Outbox transactional guarantee",
        body: CreateJobSchema,
      },
    },
    async (req, reply) => {
      const data = req.body;
      const jobId = uuidv4();
      const idempotencyKey =
        data.idempotencyKey || `${data.queue}:${data.name}:${jobId}`;

      knownQueues.add(data.queue);

      const jobEnvelope: JobEnvelope = {
        id: jobId,
        idempotencyKey,
        name: data.name,
        payload: data.payload,
        priority: data.priority,
        maxAttempts: data.maxAttempts,
        backoffStrategy: data.backoffStrategy,
        baseDelayMs: data.baseDelayMs,
        traceContext: injectTraceContext(),
        attempt: 0,
      };

      if (data.useOutbox) {
        try {
          // Atomically write to PostgreSQL Transactional Outbox
          await withOutboxTransaction(async (_tx, emitOutbox) => {
            emitOutbox({
              aggregateType: data.name,
              aggregateId: jobId,
              topic: data.queue,
              payload: jobEnvelope,
            });
          });
        } catch (dbErr) {
          logger.warn(
            { dbErr },
            "Outbox transaction failed or DB offline, fallback direct enqueue"
          );
          await broker.enqueue(data.queue, jobEnvelope);
        }
      } else {
        // Direct enqueue without Outbox
        await broker.enqueue(data.queue, jobEnvelope);
      }

      return reply.status(202).send({
        jobId,
        idempotencyKey,
        status: "ACCEPTED",
        queue: data.queue,
        broker: broker.name,
      });
    }
  );

  // --- POST /api/v1/workflows ---
  const CreateWorkflowSchema = z.object({
    name: z.string().min(1),
    steps: z.array(
      z.object({
        queue: z.string().min(1),
        name: z.string().min(1),
        payload: z.unknown(),
        dependsOn: z.array(z.string()).optional(),
      })
    ),
  });

  app.post(
    "/api/v1/workflows",
    {
      schema: {
        summary: "Enqueue a multi-step DAG job tree",
        body: CreateWorkflowSchema,
      },
    },
    async (req, reply) => {
      const { name, steps } = req.body;
      const workflowId = uuidv4();
      const enqueuedJobIds: string[] = [];

      for (const step of steps) {
        knownQueues.add(step.queue);
        const jobId = uuidv4();
        const envelope: JobEnvelope = {
          id: jobId,
          idempotencyKey: `wf:${workflowId}:${step.name}:${jobId}`,
          name: step.name,
          payload: {
            workflowId,
            workflowName: name,
            stepData: step.payload,
            dependencies: step.dependsOn || [],
          },
          priority: 5,
          maxAttempts: 3,
          backoffStrategy: "EXPONENTIAL_JITTER",
          baseDelayMs: 1000,
          traceContext: injectTraceContext(),
          attempt: 0,
        };

        await broker.enqueue(step.queue, envelope);
        enqueuedJobIds.push(jobId);
      }

      return reply.status(202).send({
        workflowId,
        workflowName: name,
        stepCount: steps.length,
        jobIds: enqueuedJobIds,
        status: "ACCEPTED",
      });
    }
  );

  // --- POST /api/v1/schedules ---
  const CreateScheduleSchema = z.object({
    queue: z.string().min(1),
    name: z.string().min(1),
    cron: z.string().min(1),
    payload: z.unknown().default({}),
  });

  app.post(
    "/api/v1/schedules",
    {
      schema: {
        summary: "Register a repeatable cron schedule",
        body: CreateScheduleSchema,
      },
    },
    async (req, reply) => {
      const { queue, name, cron, payload } = req.body;
      knownQueues.add(queue);

      const jobTemplate: Omit<JobEnvelope, "id"> = {
        name,
        idempotencyKey: `cron:${queue}:${name}:${cron}`,
        payload,
        priority: 0,
        maxAttempts: 3,
        backoffStrategy: "EXPONENTIAL_JITTER",
        baseDelayMs: 1000,
        attempt: 0,
      };

      await broker.scheduleCron(queue, cron, jobTemplate);

      return reply.status(201).send({
        scheduled: true,
        queue,
        name,
        cron,
      });
    }
  );

  // --- GET /api/v1/queues ---
  app.get(
    "/api/v1/queues",
    {
      schema: {
        summary: "List all known queues and their live telemetry",
      },
    },
    async () => {
      const results = [];
      for (const q of knownQueues) {
        try {
          const metrics = await broker.getQueueMetrics(q);
          results.push(metrics);
        } catch {
          results.push({
            queue: q,
            waiting: 0,
            active: 0,
            completed: 0,
            failed: 0,
            delayed: 0,
            dlq: 0,
            paused: false,
          });
        }
      }
      return results;
    }
  );

  // --- GET /api/v1/queues/:name/metrics ---
  app.get(
    "/api/v1/queues/:name/metrics",
    {
      schema: {
        summary: "Get metrics for a specific queue",
        params: z.object({ name: z.string() }),
      },
    },
    async (req, reply) => {
      const { name } = req.params;
      knownQueues.add(name);
      try {
        const metrics = await broker.getQueueMetrics(name);
        return metrics;
      } catch (err: any) {
        return reply.status(500).send({ error: err.message });
      }
    }
  );

  // --- GET /api/v1/dlq ---
  app.get(
    "/api/v1/dlq",
    {
      schema: {
        summary: "List Dead-Letter Queue records",
        querystring: z.object({
          queue: z.string().optional(),
          limit: z.coerce.number().int().default(50),
        }),
      },
    },
    async (req) => {
      try {
        const deadLetters = await dlqRepo.listDeadLetters({
          queueName: req.query.queue,
          limit: req.query.limit,
        });
        return deadLetters;
      } catch {
        return [];
      }
    }
  );

  // --- POST /api/v1/dlq/:id/replay ---
  app.post(
    "/api/v1/dlq/:id/replay",
    {
      schema: {
        summary: "Replay a Dead-Letter Queue job with fresh attempt counter",
        params: z.object({ id: z.string() }),
      },
    },
    async (req, reply) => {
      const { id } = req.params;
      let record;
      try {
        record = await dlqRepo.getById(id);
      } catch {
        record = null;
      }

      if (!record) {
        return reply.status(404).send({ error: "Dead letter job not found" });
      }

      const newJobId = uuidv4();
      const replayedEnvelope: JobEnvelope = {
        id: newJobId,
        idempotencyKey: `replay:${record.originalJobId}:${newJobId}`,
        name: `${record.queueName}.replayed`,
        payload: record.payload,
        priority: 10,
        maxAttempts: 3,
        backoffStrategy: "EXPONENTIAL_JITTER",
        baseDelayMs: 1000,
        traceContext: injectTraceContext(),
        attempt: 0,
      };

      await broker.enqueue(record.queueName, replayedEnvelope);
      try {
        await dlqRepo.markReplayed(id);
      } catch {
        // Continue
      }

      return reply.status(200).send({
        replayed: true,
        oldJobId: record.originalJobId,
        newJobId,
        queue: record.queueName,
      });
    }
  );

  // --- GET /api/v1/stream/telemetry (Server-Sent Events) ---
  app.get("/api/v1/stream/telemetry", async (req, reply) => {
    reply.raw.setHeader("Content-Type", "text/event-stream");
    reply.raw.setHeader("Cache-Control", "no-cache");
    reply.raw.setHeader("Connection", "keep-alive");
    reply.raw.setHeader("Access-Control-Allow-Origin", "*");
    reply.raw.flushHeaders?.();

    const pushTelemetry = async () => {
      try {
        const queueMetrics = [];
        for (const q of knownQueues) {
          try {
            const m = await broker.getQueueMetrics(q);
            queueMetrics.push(m);
          } catch {
            queueMetrics.push({
              queue: q,
              waiting: 0,
              active: 0,
              completed: 0,
              failed: 0,
              delayed: 0,
              dlq: 0,
              paused: false,
            });
          }
        }

        let pendingOutbox = 0;
        let dlqTotal = 0;
        try {
          pendingOutbox = await prisma.outboxEvent.count({
            where: { status: "PENDING" },
          });
          dlqTotal = await dlqRepo.getDeadLetterCount();
        } catch {
          // In case DB is not running
        }

        const data = {
          timestamp: new Date().toISOString(),
          broker: broker.name,
          queues: queueMetrics,
          outboxLag: pendingOutbox,
          dlqTotal,
        };

        reply.raw.write(`data: ${JSON.stringify(data)}\n\n`);
      } catch (err) {
        logger.error({ err }, "Error pushing SSE telemetry");
      }
    };

    // Push initial payload immediately
    await pushTelemetry();

    // Push every 1000ms
    const interval = setInterval(pushTelemetry, 1000);

    req.raw.on("close", () => {
      clearInterval(interval);
    });
  });

  return { app, outboxPoller };
}
