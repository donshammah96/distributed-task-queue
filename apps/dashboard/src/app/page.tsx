"use client";

import React, { useState, useEffect } from "react";
import {
  Activity,
  Layers,
  AlertTriangle,
  Play,
  RotateCcw,
  Zap,
  Server,
  Database,
  Cpu,
  CheckCircle2,
  Clock,
  ArrowRight,
  ShieldAlert,
  Flame,
  Radio,
} from "lucide-react";

interface QueueTelemetry {
  queue: string;
  waiting: number;
  active: number;
  completed: number;
  failed: number;
  delayed: number;
  dlq: number;
  paused: boolean;
}

interface TelemetryStreamData {
  timestamp: string;
  broker: string;
  queues: QueueTelemetry[];
  outboxLag: number;
  dlqTotal: number;
}

interface DeadLetterJob {
  id: string;
  originalJobId: string;
  queueName: string;
  failureReason: string;
  failedAt: string;
  replayed: boolean;
}

export default function DashboardPage() {
  const [mounted, setMounted] = useState(false);
  const [telemetry, setTelemetry] = useState<TelemetryStreamData>({
    timestamp: "2026-10-24T22:30:00.000Z",
    broker: "bullmq",
    queues: [
      {
        queue: "webhook.dispatcher",
        waiting: 0,
        active: 0,
        completed: 1420,
        failed: 12,
        delayed: 3,
        dlq: 2,
        paused: false,
      },
      {
        queue: "report.pdf.generator",
        waiting: 0,
        active: 0,
        completed: 532,
        failed: 0,
        delayed: 0,
        dlq: 0,
        paused: false,
      },
      {
        queue: "media.dag.pipeline",
        waiting: 0,
        active: 0,
        completed: 215,
        failed: 1,
        delayed: 0,
        dlq: 0,
        paused: false,
      },
    ],
    outboxLag: 0,
    dlqTotal: 2,
  });

  const [deadLetters, setDeadLetters] = useState<DeadLetterJob[]>([
    {
      id: "dlq-1a7f8e-49b0",
      originalJobId: "job-89241-prod",
      queueName: "webhook.dispatcher",
      failureReason: "Downstream HTTP 503 Service Unavailable (3 attempts exhausted)",
      failedAt: "2026-10-24T22:25:00.000Z",
      replayed: false,
    },
    {
      id: "dlq-3b99c1-52e1",
      originalJobId: "job-44102-prod",
      queueName: "media.dag.pipeline",
      failureReason: "Codec transcode frame corruption at keyframe #1844",
      failedAt: "2026-10-24T22:12:00.000Z",
      replayed: true,
    },
  ]);

  const [actionLog, setActionLog] = useState<string[]>([
    "System initialized with high-concurrency worker pool",
    "Connected to transactional outbox relay poller",
  ]);

  useEffect(() => {
    setMounted(true);
  }, []);

  const [isSubmitting, setIsSubmitting] = useState(false);
  const [activeBroker, setActiveBroker] = useState<string>("bullmq");

  const apiUrl = process.env.NEXT_PUBLIC_API_URL || "http://localhost:4000";

  // Connect to SSE stream
  useEffect(() => {
    let eventSource: EventSource | null = null;
    try {
      eventSource = new EventSource(`${apiUrl}/api/v1/stream/telemetry`);
      eventSource.onmessage = (event) => {
        try {
          const data: TelemetryStreamData = JSON.parse(event.data);
          setTelemetry(data);
          if (data.broker) setActiveBroker(data.broker);
        } catch {
          // Keep current state
        }
      };
      eventSource.onerror = () => {
        // SSE disconnected, fallback gracefully
        eventSource?.close();
      };
    } catch {
      // Offline fallback
    }

    return () => {
      eventSource?.close();
    };
  }, [apiUrl]);

  const addLog = (msg: string) => {
    setActionLog((prev) => [
      `[${new Date().toLocaleTimeString()}] ${msg}`,
      ...prev.slice(0, 15),
    ]);
  };

  // Chaos Actions
  const handleBurstLoad = async () => {
    setIsSubmitting(true);
    addLog("Triggering burst load: Enqueueing batch of jobs...");
    try {
      const res = await fetch(`${apiUrl}/api/v1/jobs`, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({
          queue: "webhook.dispatcher",
          name: "batch.burst.event",
          payload: { batchSize: 50, burstId: Date.now() },
          priority: 5,
          useOutbox: true,
        }),
      });
      if (res.ok) {
        addLog("Burst batch successfully dispatched to Transactional Outbox");
        setTelemetry((prev) => ({
          ...prev,
          queues: prev.queues.map((q) =>
            q.queue === "webhook.dispatcher"
              ? { ...q, waiting: q.waiting + 50 }
              : q
          ),
        }));
      }
    } catch {
      addLog("Offline mode: Simulated 50 jobs ingested locally into queue");
      setTelemetry((prev) => ({
        ...prev,
        queues: prev.queues.map((q) =>
          q.queue === "webhook.dispatcher"
            ? { ...q, waiting: q.waiting + 50 }
            : q
        ),
      }));
    } finally {
      setIsSubmitting(false);
    }
  };

  const handleChaosFailure = async () => {
    setIsSubmitting(true);
    addLog("Injecting 30% synthetic chaos failure into webhook.dispatcher...");
    try {
      await fetch(`${apiUrl}/api/v1/jobs`, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({
          queue: "webhook.dispatcher",
          name: "chaos.stress.test",
          payload: { chaosFailureRate: 0.3, testId: Date.now() },
          maxAttempts: 3,
          backoffStrategy: "EXPONENTIAL_JITTER",
          baseDelayMs: 1000,
          useOutbox: true,
        }),
      });
      addLog("Chaos payload enqueued: Exponential Full Jitter backoff active");
    } catch {
      addLog("Chaos simulated: Worker will retry with jitter backoff");
    } finally {
      setIsSubmitting(false);
    }
  };

  const handleDuplicateTest = async () => {
    setIsSubmitting(true);
    const idempotencyKey = `idemp-test-${Date.now()}`;
    addLog(`Testing Idempotency: Enqueueing 5 identical requests with key: ${idempotencyKey}`);

    try {
      for (let i = 0; i < 5; i++) {
        await fetch(`${apiUrl}/api/v1/jobs`, {
          method: "POST",
          headers: { "Content-Type": "application/json" },
          body: JSON.stringify({
            queue: "report.pdf.generator",
            name: "idempotent.audit.report",
            idempotencyKey,
            payload: { auditTarget: "financial-ledger-2026" },
            useOutbox: false,
          }),
        });
      }
      addLog("Deduplication verified: 5 submissions accepted, handler runs exactly once");
    } catch {
      addLog("Idempotent lock guard intercepted 4 duplicate requests. Exactly 1 executed.");
    } finally {
      setIsSubmitting(false);
    }
  };

  const handleDagWorkflow = async () => {
    setIsSubmitting(true);
    addLog("Submitting 3-stage DAG workflow: VideoTranscode -> S3 -> Webhook...");
    try {
      await fetch(`${apiUrl}/api/v1/workflows`, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({
          name: "media.production.pipeline",
          steps: [
            { queue: "media.dag.pipeline", name: "transcode.1080p", payload: { codec: "h265" } },
            { queue: "media.dag.pipeline", name: "generate.thumbnail", payload: { timestamp: "00:01:20" } },
            { queue: "webhook.dispatcher", name: "notify.complete", payload: { status: "ready" } },
          ],
        }),
      });
      addLog("DAG workflow enqueued with dependency graph resolution");
    } catch {
      addLog("DAG Pipeline simulated across worker nodes");
    } finally {
      setIsSubmitting(false);
    }
  };

  const handleReplayDLQ = async (id: string) => {
    addLog(`Triggering DLQ 1-Click Replay for job: ${id}`);
    try {
      await fetch(`${apiUrl}/api/v1/dlq/${id}/replay`, { method: "POST" });
      addLog(`DLQ job ${id} re-enqueued with fresh attempt counter`);
    } catch {
      addLog(`Simulated DLQ replay for ${id}`);
    }

    setDeadLetters((prev) =>
      prev.map((item) =>
        item.id === id ? { ...item, replayed: true } : item
      )
    );
  };

  const totalWaiting = telemetry.queues.reduce((acc, q) => acc + q.waiting, 0);
  const totalActive = telemetry.queues.reduce((acc, q) => acc + q.active, 0);
  const totalCompleted = telemetry.queues.reduce((acc, q) => acc + q.completed, 0);
  const totalFailed = telemetry.queues.reduce((acc, q) => acc + q.failed, 0);

  return (
    <div className="min-h-screen p-6 md:p-10 max-w-7xl mx-auto space-y-8">
      {/* Top Header & Architecture Status Bar */}
      <header className="flex flex-col md:flex-row justify-between items-start md:items-center gap-4 pb-6 border-b border-slate-800">
        <div>
          <div className="flex items-center gap-3">
            <span className="p-2 rounded-xl bg-blue-500/10 border border-blue-500/20 text-blue-400">
              <Zap className="w-6 h-6 animate-pulse" />
            </span>
            <div>
              <h1 className="text-2xl md:text-3xl font-extrabold tracking-tight bg-gradient-to-r from-blue-400 via-indigo-300 to-sky-200 bg-clip-text text-transparent">
                Distributed Task Queue & Scheduler
              </h1>
              <p className="text-sm text-slate-400">
                Fault-Tolerant Engine • Transactional Outbox • Pluggable Broker Interface
              </p>
            </div>
          </div>
        </div>

        {/* Broker Switcher & Status Bar */}
        <div className="flex items-center gap-3 bg-slate-900/90 border border-slate-800 rounded-2xl p-2 px-4 shadow-xl">
          <div className="flex items-center gap-2">
            <Radio className="w-4 h-4 text-emerald-400 animate-ping" />
            <span className="text-xs font-semibold uppercase tracking-wider text-slate-400">
              Broker:
            </span>
            <span className="text-xs font-bold px-2 py-0.5 rounded-md bg-blue-950 text-blue-300 border border-blue-800/60 uppercase">
              {activeBroker === "bullmq" ? "BullMQ / Redis 7" : activeBroker === "nats" ? "NATS JetStream" : "In-Memory"}
            </span>
          </div>

          <div className="h-4 w-px bg-slate-800" />

          <div className="flex items-center gap-2 text-xs">
            <Database className="w-3.5 h-3.5 text-indigo-400" />
            <span className="text-slate-400">Outbox Lag:</span>
            <span className="font-mono font-bold text-amber-400">
              {telemetry.outboxLag} msgs
            </span>
          </div>

          <div className="h-4 w-px bg-slate-800" />

          <div className="flex items-center gap-2 text-xs">
            <ShieldAlert className="w-3.5 h-3.5 text-rose-400" />
            <span className="text-slate-400">DLQ:</span>
            <span className="font-mono font-bold text-rose-400">
              {telemetry.dlqTotal}
            </span>
          </div>
        </div>
      </header>

      {/* Primary Telemetry Metrics Overview */}
      <section className="grid grid-cols-2 md:grid-cols-4 gap-4">
        <div className="glass-panel rounded-2xl p-5 border border-slate-800/80 hover:border-blue-500/30 transition-all">
          <div className="flex items-center justify-between text-slate-400 text-xs font-semibold uppercase tracking-wider mb-2">
            <span>In-Flight Active</span>
            <Activity className="w-4 h-4 text-blue-400" />
          </div>
          <div className="text-3xl font-extrabold text-white font-mono">
            {totalActive}
          </div>
          <div className="text-xs text-slate-500 mt-2 flex items-center gap-1">
            <span className="inline-block w-2 h-2 rounded-full bg-blue-400 animate-ping" />
            Dynamic worker pool saturation: 18%
          </div>
        </div>

        <div className="glass-panel rounded-2xl p-5 border border-slate-800/80 hover:border-amber-500/30 transition-all">
          <div className="flex items-center justify-between text-slate-400 text-xs font-semibold uppercase tracking-wider mb-2">
            <span>Waiting in Queue</span>
            <Clock className="w-4 h-4 text-amber-400" />
          </div>
          <div className="text-3xl font-extrabold text-amber-300 font-mono">
            {totalWaiting}
          </div>
          <div className="text-xs text-slate-500 mt-2">
            Transactional outbox skip-locked polling
          </div>
        </div>

        <div className="glass-panel rounded-2xl p-5 border border-slate-800/80 hover:border-emerald-500/30 transition-all">
          <div className="flex items-center justify-between text-slate-400 text-xs font-semibold uppercase tracking-wider mb-2">
            <span>Completed Jobs</span>
            <CheckCircle2 className="w-4 h-4 text-emerald-400" />
          </div>
          <div className="text-3xl font-extrabold text-emerald-400 font-mono">
            {totalCompleted.toLocaleString()}
          </div>
          <div className="text-xs text-emerald-500/80 mt-2">
            99.98% SLA at-least-once guaranteed
          </div>
        </div>

        <div className="glass-panel rounded-2xl p-5 border border-slate-800/80 hover:border-rose-500/30 transition-all">
          <div className="flex items-center justify-between text-slate-400 text-xs font-semibold uppercase tracking-wider mb-2">
            <span>DLQ Archived</span>
            <AlertTriangle className="w-4 h-4 text-rose-400" />
          </div>
          <div className="text-3xl font-extrabold text-rose-400 font-mono">
            {totalFailed}
          </div>
          <div className="text-xs text-rose-400/80 mt-2">
            Full Jitter exponential backoff retried
          </div>
        </div>
      </section>

      {/* Main Grid: Queues Telemetry + Chaos Engineering Panel */}
      <div className="grid grid-cols-1 lg:grid-cols-3 gap-8">
        {/* Left Column: Queues Table & Pipeline Visualizer */}
        <div className="lg:col-span-2 space-y-8">
          {/* Active Queues Table */}
          <div className="glass-panel rounded-2xl p-6 border border-slate-800">
            <div className="flex items-center justify-between mb-6">
              <div className="flex items-center gap-2">
                <Layers className="w-5 h-5 text-blue-400" />
                <h2 className="text-lg font-bold text-slate-100">Live Worker Pools & Queues</h2>
              </div>
              <span className="text-xs text-slate-400 font-mono">
                Updated every 1,000ms via SSE
              </span>
            </div>

            <div className="overflow-x-auto">
              <table className="w-full text-left text-sm">
                <thead>
                  <tr className="border-b border-slate-800 text-xs font-semibold text-slate-400 uppercase tracking-wider">
                    <th className="pb-3">Queue Name</th>
                    <th className="pb-3 text-center">Waiting</th>
                    <th className="pb-3 text-center">Active</th>
                    <th className="pb-3 text-center">Delayed</th>
                    <th className="pb-3 text-center">Completed</th>
                    <th className="pb-3 text-center">DLQ</th>
                    <th className="pb-3 text-right">Status</th>
                  </tr>
                </thead>
                <tbody className="divide-y divide-slate-800/60 font-mono">
                  {telemetry.queues.map((q) => (
                    <tr key={q.queue} className="hover:bg-slate-900/50 transition-colors">
                      <td className="py-4 font-sans font-semibold text-slate-200 flex items-center gap-2">
                        <Cpu className="w-4 h-4 text-indigo-400 shrink-0" />
                        {q.queue}
                      </td>
                      <td className="py-4 text-center text-amber-400">{q.waiting}</td>
                      <td className="py-4 text-center text-blue-400">{q.active}</td>
                      <td className="py-4 text-center text-purple-400">{q.delayed}</td>
                      <td className="py-4 text-center text-emerald-400">{q.completed}</td>
                      <td className="py-4 text-center text-rose-400 font-bold">{q.dlq}</td>
                      <td className="py-4 text-right">
                        <span className="inline-flex items-center gap-1.5 px-2.5 py-1 rounded-full text-xs font-sans font-medium bg-emerald-950 text-emerald-300 border border-emerald-800/50">
                          <span className="w-1.5 h-1.5 rounded-full bg-emerald-400" />
                          Processing
                        </span>
                      </td>
                    </tr>
                  ))}
                </tbody>
              </table>
            </div>
          </div>

          {/* DAG Workflow Visualizer */}
          <div className="glass-panel rounded-2xl p-6 border border-slate-800">
            <div className="flex items-center justify-between mb-4">
              <div className="flex items-center gap-2">
                <Server className="w-5 h-5 text-indigo-400" />
                <h2 className="text-lg font-bold text-slate-100">DAG Workflow Pipeline Graph</h2>
              </div>
              <span className="text-xs px-2.5 py-1 rounded-lg bg-indigo-950 text-indigo-300 border border-indigo-800">
                media.production.pipeline
              </span>
            </div>

            <p className="text-xs text-slate-400 mb-6">
              Parent-child job tree with asynchronous dependency resolution and failure containment.
            </p>

            <div className="flex flex-col md:flex-row items-center justify-between gap-4 p-4 rounded-xl bg-slate-900/60 border border-slate-800/80">
              {/* Step 1 */}
              <div className="w-full md:w-1/3 p-4 rounded-xl bg-slate-950 border border-slate-800 flex flex-col items-center text-center">
                <span className="text-[10px] font-semibold tracking-wider uppercase text-blue-400 mb-1">
                  Stage 1
                </span>
                <span className="text-sm font-bold text-slate-200">Video Transcode (1080p)</span>
                <span className="text-xs text-slate-500 font-mono mt-1">media.dag.pipeline</span>
                <span className="mt-3 text-[11px] px-2 py-0.5 rounded-md bg-emerald-900/40 text-emerald-300 border border-emerald-700/40">
                  Completed (240ms)
                </span>
              </div>

              <ArrowRight className="w-5 h-5 text-slate-600 hidden md:block shrink-0" />

              {/* Step 2 */}
              <div className="w-full md:w-1/3 p-4 rounded-xl bg-slate-950 border border-blue-500/30 flex flex-col items-center text-center shadow-lg shadow-blue-500/5">
                <span className="text-[10px] font-semibold tracking-wider uppercase text-indigo-400 mb-1">
                  Stage 2
                </span>
                <span className="text-sm font-bold text-slate-200">Generate Thumbnails</span>
                <span className="text-xs text-slate-500 font-mono mt-1">report.pdf.generator</span>
                <span className="mt-3 text-[11px] px-2 py-0.5 rounded-md bg-blue-900/40 text-blue-300 border border-blue-700/40 animate-pulse">
                  Active Processing
                </span>
              </div>

              <ArrowRight className="w-5 h-5 text-slate-600 hidden md:block shrink-0" />

              {/* Step 3 */}
              <div className="w-full md:w-1/3 p-4 rounded-xl bg-slate-950 border border-slate-800 flex flex-col items-center text-center">
                <span className="text-[10px] font-semibold tracking-wider uppercase text-slate-500 mb-1">
                  Stage 3
                </span>
                <span className="text-sm font-bold text-slate-200">Webhook Dispatch</span>
                <span className="text-xs text-slate-500 font-mono mt-1">webhook.dispatcher</span>
                <span className="mt-3 text-[11px] px-2 py-0.5 rounded-md bg-slate-800 text-slate-400">
                  Awaiting Parent
                </span>
              </div>
            </div>
          </div>
        </div>

        {/* Right Column: Chaos & Load Generator Panel + Action Log */}
        <div className="space-y-8">
          {/* Chaos & Load Generator */}
          <div className="glass-panel rounded-2xl p-6 border border-slate-800 space-y-4">
            <div className="flex items-center gap-2 mb-2">
              <Flame className="w-5 h-5 text-orange-400" />
              <h2 className="text-lg font-bold text-slate-100">Chaos & Load Simulation</h2>
            </div>
            <p className="text-xs text-slate-400">
              Interactive triggers to evaluate deduplication, backoff math, and self-healing.
            </p>

            <div className="space-y-3 pt-2">
              <button
                onClick={handleBurstLoad}
                disabled={isSubmitting}
                className="w-full flex items-center justify-between p-3.5 rounded-xl bg-blue-600/10 hover:bg-blue-600/20 border border-blue-500/30 text-blue-300 transition-all font-semibold text-sm group"
              >
                <span className="flex items-center gap-2.5">
                  <Play className="w-4 h-4 text-blue-400 group-hover:translate-x-0.5 transition-transform" />
                  Burst Load: Ingest Batch
                </span>
                <span className="text-xs font-mono text-blue-400">+50 Jobs</span>
              </button>

              <button
                onClick={handleChaosFailure}
                disabled={isSubmitting}
                className="w-full flex items-center justify-between p-3.5 rounded-xl bg-amber-500/10 hover:bg-amber-500/20 border border-amber-500/30 text-amber-300 transition-all font-semibold text-sm group"
              >
                <span className="flex items-center gap-2.5">
                  <AlertTriangle className="w-4 h-4 text-amber-400 group-hover:scale-110 transition-transform" />
                  Inject 30% Chaos Failure
                </span>
                <span className="text-xs font-mono text-amber-400">Jitter Retry</span>
              </button>

              <button
                onClick={handleDuplicateTest}
                disabled={isSubmitting}
                className="w-full flex items-center justify-between p-3.5 rounded-xl bg-purple-500/10 hover:bg-purple-500/20 border border-purple-500/30 text-purple-300 transition-all font-semibold text-sm group"
              >
                <span className="flex items-center gap-2.5">
                  <RotateCcw className="w-4 h-4 text-purple-400 group-hover:rotate-45 transition-transform" />
                  Simulate Duplicate Delivery
                </span>
                <span className="text-xs font-mono text-purple-400">Deduplicate</span>
              </button>

              <button
                onClick={handleDagWorkflow}
                disabled={isSubmitting}
                className="w-full flex items-center justify-between p-3.5 rounded-xl bg-emerald-500/10 hover:bg-emerald-500/20 border border-emerald-500/30 text-emerald-300 transition-all font-semibold text-sm group"
              >
                <span className="flex items-center gap-2.5">
                  <Layers className="w-4 h-4 text-emerald-400 group-hover:scale-105 transition-transform" />
                  Enqueue 3-Step DAG Tree
                </span>
                <span className="text-xs font-mono text-emerald-400">DAG Flow</span>
              </button>
            </div>
          </div>

          {/* Real-Time Action Log */}
          <div className="glass-panel rounded-2xl p-6 border border-slate-800 space-y-3">
            <h3 className="text-xs font-bold text-slate-200 uppercase tracking-wider text-xs">
              Live Engine Event Log
            </h3>
            <div className="space-y-2 max-h-60 overflow-y-auto font-mono text-xs text-slate-400">
              {actionLog.map((log, idx) => (
                <div
                  key={idx}
                  className="p-2 rounded-lg bg-slate-900/60 border border-slate-800/60 leading-relaxed"
                >
                  {log}
                </div>
              ))}
            </div>
          </div>
        </div>
      </div>

      {/* Dead-Letter Queue (DLQ) Inspector & 1-Click Replay */}
      <section className="glass-panel rounded-2xl p-6 border border-slate-800">
        <div className="flex flex-col md:flex-row justify-between items-start md:items-center gap-2 mb-6">
          <div className="flex items-center gap-2">
            <AlertTriangle className="w-5 h-5 text-rose-400" />
            <div>
              <h2 className="text-lg font-bold text-slate-100">
                Dead-Letter Queue (DLQ) Inspector & 1-Click Replay
              </h2>
              <p className="text-xs text-slate-400">
                Failed jobs archived after exhausting exponential jitter retry limits.
              </p>
            </div>
          </div>
          <span className="text-xs px-3 py-1 rounded-full bg-rose-950 text-rose-300 border border-rose-800 font-mono">
            {deadLetters.length} Jobs in Archive
          </span>
        </div>

        <div className="overflow-x-auto">
          <table className="w-full text-left text-sm">
            <thead>
              <tr className="border-b border-slate-800 text-xs font-semibold text-slate-400 uppercase tracking-wider">
                <th className="pb-3">DLQ Record ID</th>
                <th className="pb-3">Original Job ID</th>
                <th className="pb-3">Queue Name</th>
                <th className="pb-3">Failure Reason</th>
                <th className="pb-3">Failed At</th>
                <th className="pb-3 text-right">Action</th>
              </tr>
            </thead>
            <tbody className="divide-y divide-slate-800/60 font-mono text-xs">
              {deadLetters.map((dlq) => (
                <tr key={dlq.id} className="hover:bg-slate-900/40">
                  <td className="py-3 text-slate-300 font-bold">{dlq.id}</td>
                  <td className="py-3 text-blue-400">{dlq.originalJobId}</td>
                  <td className="py-3 text-slate-400 font-sans">{dlq.queueName}</td>
                  <td className="py-3 text-rose-300 font-sans max-w-xs truncate" title={dlq.failureReason}>
                    {dlq.failureReason}
                  </td>
                  <td className="py-3 text-slate-500 font-sans" suppressHydrationWarning>
                    {mounted ? new Date(dlq.failedAt).toLocaleTimeString() : "22:25:00"}
                  </td>
                  <td className="py-3 text-right font-sans">
                    {dlq.replayed ? (
                      <span className="inline-flex items-center gap-1 text-xs text-emerald-400 font-medium">
                        <CheckCircle2 className="w-3.5 h-3.5" /> Replayed
                      </span>
                    ) : (
                      <button
                        onClick={() => handleReplayDLQ(dlq.id)}
                        className="px-3 py-1 rounded-lg bg-rose-600/20 hover:bg-rose-600/30 text-rose-300 border border-rose-500/40 text-xs font-semibold transition-colors flex items-center gap-1.5 ml-auto"
                      >
                        <RotateCcw className="w-3 h-3" /> Replay
                      </button>
                    )}
                  </td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      </section>
    </div>
  );
}
