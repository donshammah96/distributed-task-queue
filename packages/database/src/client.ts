import { PrismaClient } from "@prisma/client";

declare global {
  // eslint-disable-next-line no-var
  var __dtq_prisma: PrismaClient | undefined;
}

export const prisma =
  global.__dtq_prisma ||
  new PrismaClient({
    log:
      process.env["NODE_ENV"] === "development"
        ? ["query", "error", "warn"]
        : ["error"],
  });

if (process.env["NODE_ENV"] !== "production") {
  global.__dtq_prisma = prisma;
}

export type PrismaTransactionClient = Omit<
  PrismaClient,
  "$connect" | "$disconnect" | "$on" | "$transaction" | "$use" | "$extends"
>;
