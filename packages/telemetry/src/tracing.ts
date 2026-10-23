import {
  trace,
  context,
  propagation,
  Span,
  Tracer,
  SpanStatusCode,
  Context,
} from "@opentelemetry/api";

export const TRACER_NAME = "dtq-tracer";

export function getTracer(): Tracer {
  return trace.getTracer(TRACER_NAME);
}

export interface W3CTraceContext {
  traceparent?: string;
  tracestate?: string;
}

/**
 * Injects current active span context into a W3C trace context carrier object.
 */
export function injectTraceContext(
  ctx: Context = context.active()
): W3CTraceContext {
  const carrier: Record<string, string> = {};
  propagation.inject(ctx, carrier);
  return {
    traceparent: carrier["traceparent"],
    tracestate: carrier["tracestate"],
  };
}

/**
 * Extracts W3C trace context from carrier into an OpenTelemetry Context.
 */
export function extractTraceContext(
  traceContext?: W3CTraceContext
): Context {
  if (!traceContext || !traceContext.traceparent) {
    return context.active();
  }

  const carrier: Record<string, string> = {};
  if (traceContext.traceparent) {
    carrier["traceparent"] = traceContext.traceparent;
  }
  if (traceContext.tracestate) {
    carrier["tracestate"] = traceContext.tracestate;
  }

  return propagation.extract(context.active(), carrier);
}

/**
 * Executes a function within a named OpenTelemetry trace span.
 */
export async function withTraceSpan<T>(
  spanName: string,
  fn: (span: Span) => Promise<T>,
  parentContext?: Context
): Promise<T> {
  const tracer = getTracer();
  const activeCtx = parentContext || context.active();

  return context.with(activeCtx, async () => {
    const span = tracer.startSpan(spanName);
    return context.with(trace.setSpan(activeCtx, span), async () => {
      try {
        const result = await fn(span);
        span.setStatus({ code: SpanStatusCode.OK });
        return result;
      } catch (error) {
        span.setStatus({
          code: SpanStatusCode.ERROR,
          message: error instanceof Error ? error.message : String(error),
        });
        span.recordException(error as Error);
        throw error;
      } finally {
        span.end();
      }
    });
  });
}
