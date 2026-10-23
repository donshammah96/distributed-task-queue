import type { Metadata } from "next";
import "./globals.css";

export const metadata: Metadata = {
  title: "Distributed Task Queue & Job Scheduler Control Plane",
  description:
    "Enterprise-grade distributed task queue and scheduler operations dashboard featuring real-time SSE telemetry, chaos engineering controls, and DLQ management.",
};

export default function RootLayout({
  children,
}: Readonly<{
  children: React.ReactNode;
}>) {
  return (
    <html lang="en" className="dark">
      <body className="antialiased min-h-screen bg-slate-950 text-slate-100 selection:bg-blue-600 selection:text-white">
        {children}
      </body>
    </html>
  );
}
