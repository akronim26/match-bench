/**
 * This file defines frontend behavior for useSSE.
 * It is part of the IICPC frontend and keeps UI, API, or test behavior
 * scoped to this module so callers can rely on stable boundaries.
 */
"use client";

import { useEffect, useRef } from "react";
import type { SSEEvent } from "@/types/leaderboard";

/**
 * SSEStatus describes structured data exchanged by this module.
 * Keep this shape aligned with API and component expectations.
 */
export type SSEStatus = "connecting" | "open" | "closed" | "error";

/**
 * SSEOptions describes structured data exchanged by this module.
 * Keep this shape aligned with API and component expectations.
 */

interface SSEOptions {
  enabled?: boolean;
  onMessage: (event: SSEEvent) => void;
  onStatusChange?: (status: SSEStatus) => void;
}

/**
 * useSSE performs the module-specific operation described by its name.
 * It keeps inputs, side effects, and returned values within this module's contract.
 */
export function useSSE(
  url: string,
  { enabled = true, onMessage, onStatusChange }: SSEOptions,
) {
  const messageRef = useRef(onMessage);
  const statusRef = useRef(onStatusChange);

  useEffect(() => {
    messageRef.current = onMessage;
    statusRef.current = onStatusChange;
  }, [onMessage, onStatusChange]);

  useEffect(() => {
    if (!enabled) {
      statusRef.current?.("closed");
      return;
    }

    let source: EventSource | null = null;
    let reconnect: ReturnType<typeof setTimeout> | null = null;
    let closed = false;
    let attempt = 0;

    const dispatch = (type: SSEEvent["type"], raw: string) => {
      try {
        messageRef.current({ type, data: JSON.parse(raw) } as SSEEvent);
      } catch {}
    };

    const connect = () => {
      statusRef.current?.("connecting");
      source = new EventSource(url);
      source.onopen = () => {
        attempt = 0;
        statusRef.current?.("open");
      };
      source.addEventListener("snapshot", (event) =>
        dispatch("snapshot", (event as MessageEvent).data),
      );
      source.addEventListener("update", (event) =>
        dispatch("update", (event as MessageEvent).data),
      );
      source.addEventListener("live_metrics", (event) =>
        dispatch("live_metrics", (event as MessageEvent).data),
      );
      source.onerror = () => {
        statusRef.current?.("error");
        source?.close();
        source = null;
        if (!closed) {
          const delay = Math.min(1000 * 2 ** attempt, 30_000);
          attempt += 1;
          reconnect = setTimeout(connect, delay);
        }
      };
    };

    connect();

    return () => {
      closed = true;
      statusRef.current?.("closed");
      source?.close();
      if (reconnect) clearTimeout(reconnect);
    };
  }, [url, enabled]);
}
