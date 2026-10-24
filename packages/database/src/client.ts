import { existsSync } from "fs";
import { resolve } from "path";
import { PrismaClient } from "@prisma/client";

// Ensure environment variables are loaded from root or current directory
if (!process.env["DATABASE_URL"]) {
  const candidates = [
    resolve(process.cwd(), ".env"),
    resolve(process.cwd(), ".env.local"),
    resolve(process.cwd(), "../../.env"),
    resolve(process.cwd(), "../../.env.local"),
    resolve(__dirname, "../../../.env"),
    resolve(__dirname, "../../../.env.local"),
  ];
  for (const file of candidates) {
    if (existsSync(file)) {
      try {
        process.loadEnvFile?.(file);
        if (process.env["DATABASE_URL"]) break;
      } catch {
        // ignore
      }
    }
  }
}

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
