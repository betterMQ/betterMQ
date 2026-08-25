import { reactive } from "vue";

export const outbound = reactive({
  method: "POST",
  sign: false,
  headers: "",
});

export function parseBody(raw: string): unknown {
  try {
    return JSON.parse(raw);
  } catch {
    return raw;
  }
}

export function mergeOutbound(payload: Record<string, unknown>): string | null {
  payload.method = outbound.method;
  if (outbound.sign) payload.sign = true;
  const raw = outbound.headers.trim();
  if (raw) {
    try {
      payload.headers = JSON.parse(raw);
    } catch {
      return "Headers must be valid JSON";
    }
  }
  return null;
}

export type RetryState = {
  maxRetries: number;
  kind: string;
  initialMs: number;
  maxMs: number;
  multiplier: number;
};

export function defaultRetry(): RetryState {
  return { maxRetries: 0, kind: "exponential", initialMs: 500, maxMs: 30000, multiplier: 2 };
}

export function mergeRetry(payload: Record<string, unknown>, retry: RetryState): Record<string, unknown> {
  const max = Math.max(0, Number(retry.maxRetries) || 0);
  if (max <= 0) return payload;
  payload.max_retries = max;
  const kind = (retry.kind || "exponential").toLowerCase();
  const backoff: Record<string, unknown> = { kind, initialMs: Number(retry.initialMs) || 500 };
  if (kind === "exponential") {
    backoff.maxMs = Number(retry.maxMs) || 30000;
    const mult = Number(retry.multiplier);
    if (mult > 0) backoff.multiplier = mult;
  }
  payload.retry_backoff = backoff;
  return payload;
}

export function errText(e: unknown): string {
  if (e && typeof e === "object" && "message" in e) return String((e as { message: unknown }).message);
  return String(e || "request failed");
}

export async function copyText(text: string): Promise<void> {
  await navigator.clipboard.writeText(text);
}
