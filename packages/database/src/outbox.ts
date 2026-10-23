import { PrismaClient, OutboxEvent, OutboxStatus } from "@prisma/client";
import { prisma, PrismaTransactionClient } from "./client.js";

export interface OutboxEventInput {
  aggregateType: string;
  aggregateId: string;
  topic: string;
  payload: unknown;
}

export interface OutboxTransactionResult<T> {
  result: T;
  outboxEvents: OutboxEvent[];
}

/**
 * Executes domain mutations and inserts OutboxEvent records atomically inside a single database transaction.
 */
export async function withOutboxTransaction<T>(
  fn: (
    tx: PrismaTransactionClient,
    emitOutbox: (event: OutboxEventInput) => void
  ) => Promise<T>,
  client: PrismaClient = prisma
): Promise<OutboxTransactionResult<T>> {
  const pendingEvents: OutboxEventInput[] = [];

  const emitOutbox = (event: OutboxEventInput): void => {
    pendingEvents.push(event);
  };

  return await client.$transaction(async (tx) => {
    // 1. Execute domain operation
    const result = await fn(tx as PrismaTransactionClient, emitOutbox);

    // 2. Persist outbox events within the same transaction
    const createdEvents: OutboxEvent[] = [];
    for (const event of pendingEvents) {
      const created = await tx.outboxEvent.create({
        data: {
          aggregateType: event.aggregateType,
          aggregateId: event.aggregateId,
          topic: event.topic,
          payload: event.payload as any,
          status: OutboxStatus.PENDING,
          retryCount: 0,
        },
      });
      createdEvents.push(created);
    }

    return {
      result,
      outboxEvents: createdEvents,
    };
  });
}
