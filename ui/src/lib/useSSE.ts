import { useEffect } from "react";
import { sseUrl } from "./api";

export type SSEEvent = { type: "write" | "delete"; path: string; actor?: string };

export function useSSE(onEvent: (e: SSEEvent) => void) {
  useEffect(() => {
    const es = new EventSource(sseUrl());
    es.onmessage = (msg) => {
      try {
        const data = JSON.parse(msg.data) as SSEEvent;
        onEvent(data);
      } catch { /* ignore malformed events */ }
    };
    return () => es.close();
  }, [onEvent]);
}
