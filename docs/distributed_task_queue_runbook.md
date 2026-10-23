# AGENT EXECUTION RUNBOOK: Distributed Fault-Tolerant Task Queue & Job Scheduler

**Target Repository:** `https://github.com/donshammah96/distributed-task-queue`  
**Objective:** Architect, implement, test, and document an enterprise-grade, fault-tolerant distributed task queue and cron scheduler featuring at-least-once delivery, dynamic worker pools, transactional outbox guarantees, and a pluggable backend broker interface (BullMQ/Redis + NATS JetStream), complete with a real-time observability dashboard.

---

## 1. Architectural Standards & Tech Stack

You must strictly adhere to the following stack and engineering constraints:

- **Monorepo Management:** `pnpm` workspaces + `Turborepo` (`turbo.json` with `build`, `lint`, `test`, `dev` pipelines).
- **Language & Runtime:** TypeScript 5.x (`strict: true`, `noUncheckedIndexedAccess: true`), Node.js >= 20.
- **Database & Persistence:** PostgreSQL 16 + Prisma ORM (used for the Transactional Outbox, Idempotency Locks, Job Audit Logs, and Dead-Letter Queue archive).
- **Pluggable Broker Adapters:**
  1. `BullMQAdapter` (Redis 7+ / DragonflyDB) — primary worker pool engine with Sandboxed Processors.
  2. `NatsJetStreamAdapter` (NATS 2.10+ with JetStream) — high-throughput alternative broker demonstrating interface polymorphism.
  3. `InMemoryAdapter` — deterministic adapter for fast unit testing.
- **API & Control Plane:** Fastify (with `@fastify/swagger` and Zod type provider) for high-throughput job ingestion, health checks, and metrics.
- **Observability:** OpenTelemetry SDK (`@opentelemetry/sdk-node`), Prometheus metrics (`prom-client`), and structured JSON logging via `pino`.
- **Frontend Dashboard (`apps/dashboard`):** Next.js 15 (App Router), React 19, Tailwind CSS, Shadcn UI, Server-Sent Events (SSE) for live queue telemetry, DLQ replay controls, and DAG workflow visualization.
- **Testing:** Vitest for unit/integration tests, Testcontainers (or Docker Compose harness) for Redis/Postgres/NATS integration tests, Playwright for E2E dashboard verification.

---

## 2. Target Monorepo Directory Structure

Initialize and scaffold the exact structure below:

```text
distributed-task-queue/
├── .github/workflows/ci.yml
├── apps/
│   ├── api-server/          # Fastify ingestion API, Outbox relay, Prometheus exporter
│   ├── worker-node/         # Horizontally scalable BullMQ/NATS worker pool & sandboxed processors
│   └── dashboard/           # Next.js 15 real-time control plane & DLQ management UI
├── packages/
│   ├── queue-core/          # Broker interfaces, Outbox pattern, Idempotency engine, Backoff math
│   ├── adapter-bullmq/      # BullMQ implementation + Sandboxed worker pool manager
│   ├── adapter-nats/        # NATS JetStream implementation
│   ├── database/            # Prisma schema, migrations, Outbox & Idempotency repositories
│   ├── telemetry/           # OpenTelemetry tracing & Pino structured logger
│   └── config/              # Shared TypeScript, ESLint, and Prettier configs
├── docker/
│   └── docker-compose.yml   # Postgres 16, Redis 7, NATS JetStream, Prometheus, Jaeger
├── package.json
├── pnpm-workspace.yaml
├── turbo.json
└── README.md
```

---

## 3. Step-by-Step Implementation Phases

### Phase 1: Workspace Initialization & Infrastructure (`docker/`)

1. Initialize Git, `pnpm-workspace.yaml`, and `turbo.json`.
2. Create `docker/docker-compose.yml` provisioning:
   - `postgres:16-alpine` (Port `5432`)
   - `redis:7-alpine` with `appendonly yes` and `maxmemory-policy noeviction` (Port `6379`)
   - `nats:2.10-alpine` with `-js` (JetStream enabled, Ports `4222`, `8222`)
   - `jaegertracing/all-in-one:latest` (OTLP collector on `4317`, UI on `16686`)
3. Create root `.env.example` with all connection strings and tunable worker pool concurrency variables.

### Phase 2: Database Layer & Transactional Outbox (`packages/database`)

1. Initialize Prisma with the following core models in `schema.prisma`:
   - `OutboxEvent`: `id (UUID)`, `aggregateType`, `aggregateId`, `topic`, `payload (Json)`, `status (PENDING | PUBLISHED | FAILED)`, `retryCount`, `createdAt`, `processedAt`.
   - `IdempotencyRecord`: `idempotencyKey (String @id)`, `jobId`, `status (IN_PROGRESS | COMPLETED | FAILED)`, `lockedUntil (DateTime)`, `resultHash (String?)`, `createdAt`, `updatedAt`.
   - `JobExecutionLog`: `id`, `jobId`, `queueName`, `adapterType`, `attempt`, `status`, `durationMs`, `errorStack`, `traceId`, `createdAt`.
   - `DeadLetterJob`: `id`, `originalJobId`, `queueName`, `payload (Json)`, `failureReason`, `failedAt`, `replayed (Boolean)`, `replayedAt`.
2. Export a singleton `prisma` client and a helper `withOutboxTransaction(fn)` that executes domain mutations and inserts an `OutboxEvent` atomically inside `prisma.$transaction`.

### Phase 3: Pluggable Queue Core & Guarantees (`packages/queue-core`)

1. Define strict Zod schemas for `JobEnvelope<TPayload>`:
   - Fields: `id`, `idempotencyKey`, `name`, `payload`, `priority`, `maxAttempts`, `backoffStrategy ('EXPONENTIAL_JITTER' | 'FIXED')`, `baseDelayMs`, `cronExpression?`, `parentJobId?`, `traceContext`.
2. Define the core `IBrokerAdapter` interface:
   ```typescript
   export interface IBrokerAdapter {
     readonly name: "bullmq" | "nats" | "memory";
     connect(): Promise<void>;
     disconnect(): Promise<void>;
     enqueue<T>(queue: string, job: JobEnvelope<T>): Promise<string>;
     enqueueBatch<T>(queue: string, jobs: JobEnvelope<T>[]): Promise<string[]>;
     scheduleCron<T>(
       queue: string,
       cron: string,
       jobTemplate: Omit<JobEnvelope<T>, "id">,
     ): Promise<void>;
     registerWorker<T>(
       queue: string,
       handler: JobProcessor<T>,
       options: WorkerPoolOptions,
     ): Promise<void>;
     getQueueMetrics(queue: string): Promise<QueueTelemetry>;
     pauseQueue(queue: string): Promise<void>;
     resumeQueue(queue: string): Promise<void>;
   }
   ```
3. Implement **At-Least-Once Delivery + Idempotent Consumer Guard**:
   - Create `IdempotentConsumerMiddleware` that wraps every `JobProcessor<T>`.
   - Before running the handler, acquire an atomic upsert lock on `IdempotencyRecord` where `status != COMPLETED` or `lockedUntil < now()`.
   - Extend heartbeats during long-running jobs. On completion, mark `COMPLETED`. If a duplicate message arrives via at-least-once redelivery, short-circuit gracefully.
4. Implement **Full Jitter Exponential Backoff** calculation:
   $$\text{delay} = \text{random}\left(0, \min\left(\text{cap}, \text{base} \times 2^{\text{attempt}}\right)\right)$$
5. Implement `OutboxRelayPoller`: Uses `SELECT ... FOR UPDATE SKIP LOCKED` in raw SQL via Prisma to poll `PENDING` rows from `OutboxEvent` in batches of 100 and dispatch them to the active `IBrokerAdapter`.

### Phase 4: Broker Adapter Implementations (`packages/adapter-bullmq` & `packages/adapter-nats`)

1. **`packages/adapter-bullmq`:**
   - Implement `BullMQAdapter` using `Queue`, `Worker`, `QueueEvents`, and `FlowProducer` (for parent-child DAG dependencies).
   - Implement **Dynamic Worker Pools & Sandboxed Processors**: Support both async in-process handlers (`concurrency: N`) and isolated Node.js child process/worker_threads sandboxes (`useWorkerThreads: true`) so CPU-intensive jobs cannot block the Redis heartbeat and cause stalled jobs.
   - Configure automatic Dead-Letter Queue (`<queue>-dlq`) routing when `attemptsMade >= opts.attempts`, persisting the terminal failure into PostgreSQL `DeadLetterJob`.
2. **`packages/adapter-nats`:**
   - Implement `NatsJetStreamAdapter` using the official `nats` package.
   - Auto-provision a JetStream Stream with `RetentionPolicy.Workqueue` and explicit ACK policy (`AckPolicy.Explicit`), `ack_wait`, and `max_deliver`.
   - Map `nak(delayMs)` to the exponential jitter calculator and route messages exceeding `max_deliver` to the DLQ stream and Postgres `DeadLetterJob` table.

### Phase 5: OpenTelemetry & Prometheus Observability (`packages/telemetry`)

1. Configure W3C TraceContext injection into `JobEnvelope.traceContext` on `enqueue()`, and context extraction inside the worker before executing the job processor (enabling unbroken distributed traces across API -> Outbox -> Redis/NATS -> Worker).
2. Expose Prometheus metrics:
   - `queue_jobs_processed_total{queue, adapter, status}`
   - `queue_job_duration_seconds{queue, adapter}` (Histogram)
   - `queue_active_workers{queue, adapter}` (Gauge)
   - `queue_dlq_depth{queue}` (Gauge)

### Phase 6: API Control Plane & Worker Node (`apps/api-server` & `apps/worker-node`)

1. **`apps/api-server` (Fastify):**
   - `POST /api/v1/jobs`: Validates job payload, writes to Postgres Outbox, returns `202 Accepted` with `jobId` and `idempotencyKey`.
   - `POST /api/v1/workflows`: Enqueues a multi-step DAG job tree (e.g., `VideoTranscode -> GenerateThumbnails -> NotifyWebhook`).
   - `POST /api/v1/schedules`: Registers repeatable cron jobs.
   - `GET /api/v1/queues/:name/metrics`: Returns live queue counts (waiting, active, delayed, failed, DLQ) and throughput rates.
   - `POST /api/v1/dlq/:id/replay`: Re-enqueues a dead-lettered job with a fresh attempt counter.
   - `GET /api/v1/stream/telemetry`: Server-Sent Events (SSE) endpoint pushing real-time queue metrics every 1000ms.
   - `GET /metrics`: Prometheus scrape endpoint.
2. **`apps/worker-node`:**
   - Bootstraps the selected broker adapter via `BROKER_ADAPTER=bullmq|nats` environment variable.
   - Includes 3 realistic portfolio job processors: `webhook.dispatcher` (network I/O with simulated chaos failure rates), `report.pdf.generator` (CPU-bound sandboxed worker), and `media.dag.pipeline`.
   - Implements graceful `SIGTERM` / `SIGINT` shutdown: stops pulling new jobs, drains active in-flight executions up to a 30s timeout, closes broker connections, and exits cleanly.

### Phase 7: Next.js 15 Real-Time Operations Dashboard (`apps/dashboard`)

1. Build a dark-mode systems engineering dashboard featuring:
   - **Broker Switcher & Status Bar:** Live indicator showing active adapter (`BullMQ / Redis` vs `NATS JetStream`), worker pool saturation, and Outbox relay lag.
   - **Throughput & Latency Charts:** Real-time SSE-driven charts showing jobs/sec, P95 execution latency, and retry rates.
   - **Chaos & Load Generator Panel:** Interactive UI buttons to trigger burst loads (e.g., "Enqueue 1,000 Jobs", "Inject 30% Chaos Failure Rate", "Simulate Duplicate Delivery") so portfolio reviewers can immediately see the system self-heal, backoff, and deduplicate.
   - **DLQ Inspector & 1-Click Replay:** Table inspecting failed jobs, stack traces, OpenTelemetry trace links, and batch replay triggers.

### Phase 8: Verification, Testing & Portfolio Documentation

1. **Automated Tests:**
   - Write unit tests in `packages/queue-core` verifying exponential backoff bounds and idempotency lock state transitions.
   - Write integration tests verifying that switching `IBrokerAdapter` from `BullMQAdapter` to `NatsJetStreamAdapter` passes the exact same test suite (Liskov Substitution Principle).
   - Write an at-least-once chaos test that enqueues the same `idempotencyKey` 10 times concurrently and asserts the side-effect handler executes **exactly once**.
2. **CI/CD & Documentation:**
   - Create `.github/workflows/ci.yml` running lint, typecheck, and tests against Redis, NATS, and Postgres service containers.
   - Author a comprehensive `README.md` with Mermaid architecture diagrams (Transactional Outbox flow, Worker Pool Sandbox architecture, Idempotency State Machine), benchmark results, and a 1-command quickstart (`pnpm docker:up && pnpm db:migrate && pnpm dev`).

---

## 4. Final Agent Acceptance Criteria

Before completing your run, verify and ensure:

- [ ] `pnpm build` and `pnpm typecheck` succeed across all packages with 0 errors.
- [ ] `pnpm test` passes all unit and adapter conformance tests.
- [ ] All code is committed with clean, conventional commits (`feat:`, `arch:`, `test:`, `docs:`) and pushed to `github.com/donshammah96/distributed-task-queue`.
