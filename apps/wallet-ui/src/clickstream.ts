import type { ClickstreamEvent } from "./types";
import { DEMO_USER_ID } from "./api";

const EVENT_ENDPOINT = "/api/clickstream/events";
const SESSION_ID_KEY = "microdp.session_id";
const SESSION_STARTED_AT_KEY = "microdp.session_started_at";
const SESSION_LAST_SEEN_AT_KEY = "microdp.session_last_seen_at";
const SESSION_IDLE_TTL_MS = 30 * 60 * 1000;
const SESSION_LAST_SEEN_WRITE_INTERVAL_MS = 5000;
const RECENT_EVENTS_LIMIT = 8;

type UserIdProvider = () => string;

export type ClickstreamSnapshot = {
  session_id: string;
  session_started_at: string | null;
  current_page: string;
  page_dwell_ms: number;
  queue_size: number;
  last_events: ClickstreamEvent[];
};

function storedNumber(key: string): number | null {
  const value = window.localStorage.getItem(key);
  if (!value) return null;
  const parsed = Number(value);
  return Number.isFinite(parsed) ? parsed : null;
}

function createSession(now: number): string {
  const created = crypto.randomUUID();
  window.localStorage.setItem(SESSION_ID_KEY, created);
  window.localStorage.setItem(SESSION_STARTED_AT_KEY, String(now));
  window.localStorage.setItem(SESSION_LAST_SEEN_AT_KEY, String(now));
  return created;
}

let lastSeenWriteAt = 0;

function sessionId(now = Date.now()): string {
  const existing = window.localStorage.getItem(SESSION_ID_KEY);
  const lastSeenAt = storedNumber(SESSION_LAST_SEEN_AT_KEY);
  if (existing && lastSeenAt && now - lastSeenAt <= SESSION_IDLE_TTL_MS) {
    if (now - lastSeenWriteAt >= SESSION_LAST_SEEN_WRITE_INTERVAL_MS) {
      window.localStorage.setItem(SESSION_LAST_SEEN_AT_KEY, String(now));
      lastSeenWriteAt = now;
    }
    return existing;
  }
  lastSeenWriteAt = now;
  return createSession(now);
}

function sessionStartedAt(): string | null {
  const startedAt = storedNumber(SESSION_STARTED_AT_KEY);
  return startedAt ? new Date(startedAt).toISOString() : null;
}

export class ClickstreamTracker {
  private readonly userIdProvider: UserIdProvider;
  private currentSession = sessionId();
  private queue: ClickstreamEvent[] = [];
  private lastEvents: ClickstreamEvent[] = [];
  private lastMoveAt = 0;
  private pageEnteredAt = Date.now();
  private currentPage = "";
  private hasCurrentPage = false;
  private timer: number | undefined;
  private flushing = false;

  constructor(userIdProvider: UserIdProvider = () => DEMO_USER_ID) {
    this.userIdProvider = userIdProvider;
  }

  start(): void {
    this.timer = window.setInterval(() => void this.flush(), 3000);
    window.addEventListener("beforeunload", this.beforeUnload);
  }

  stop(): void {
    if (this.timer) window.clearInterval(this.timer);
    window.removeEventListener("beforeunload", this.beforeUnload);
    void this.flush();
  }

  getSnapshot(): ClickstreamSnapshot {
    return {
      session_id: this.currentSession,
      session_started_at: sessionStartedAt(),
      current_page: this.currentPage,
      page_dwell_ms: this.hasCurrentPage ? Math.max(0, Date.now() - this.pageEnteredAt) : 0,
      queue_size: this.queue.length,
      last_events: [...this.lastEvents]
    };
  }

  enterPage(page: string): void {
    const now = Date.now();
    if (this.hasCurrentPage) {
      this.enqueue("page_leave", this.currentPage, {
        dwell_ms: Math.max(0, now - this.pageEnteredAt)
      });
    }
    this.currentPage = page;
    this.hasCurrentPage = true;
    this.pageEnteredAt = now;
    this.enqueue("route_change", page);
    this.enqueue("page_enter", page);
  }

  click(page: string, elementId: string | null, x: number, y: number): void {
    this.enqueue("click", page, { element_id: elementId, x, y });
  }

  mouseMove(page: string, x: number, y: number): void {
    const now = Date.now();
    if (now - this.lastMoveAt < 1500) return;
    this.lastMoveAt = now;
    this.enqueue("mouse_move", page, { x, y });
  }

  private ensureSession(): string {
    this.currentSession = sessionId();
    return this.currentSession;
  }

  private remember(event: ClickstreamEvent): void {
    this.lastEvents = [event, ...this.lastEvents].slice(0, RECENT_EVENTS_LIMIT);
  }

  private enqueue(
    eventType: ClickstreamEvent["event_type"],
    page: string,
    extra: Partial<ClickstreamEvent> = {}
  ): void {
    const event: ClickstreamEvent = {
      event_id: crypto.randomUUID(),
      session_id: this.ensureSession(),
      user_id: this.userIdProvider(),
      event_type: eventType,
      page,
      ts: new Date().toISOString(),
      element_id: extra.element_id ?? null,
      x: extra.x ?? null,
      y: extra.y ?? null,
      dwell_ms: extra.dwell_ms ?? 0,
      payload: extra.payload ?? {}
    };

    this.queue.push(event);
    this.remember(event);

    if (this.queue.length >= 25) {
      void this.flush();
    }
  }

  async flush(): Promise<void> {
    if (this.queue.length === 0) return;
    // In-flight lock: setInterval(3s) + порог queue.length>=25 могут запустить
    // второй flush, пока первый ещё ждёт сеть. Без лока оба сделают splice и
    // при двух последовательных failures порядок событий разъедется.
    if (this.flushing) return;
    this.flushing = true;
    const events = this.queue.splice(0, this.queue.length);

    const controller = new AbortController();
    const timeoutId = window.setTimeout(() => controller.abort(), 5000);
    try {
      const response = await fetch(EVENT_ENDPOINT, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ events }),
        signal: controller.signal
      });
      if (!response.ok) {
        throw new Error(`clickstream publish failed: HTTP ${response.status}`);
      }
    } catch (error) {
      const dropped = Math.max(0, events.length - 50);
      if (dropped > 0) {
        console.warn(
          `clickstream: flush failed, dropping ${dropped} events (re-queue cap=50)`,
          error
        );
      }
      this.queue.unshift(...events.slice(0, 50));
    } finally {
      window.clearTimeout(timeoutId);
      this.flushing = false;
    }
  }

  private beforeUnload = (): void => {
    if (!this.hasCurrentPage) return;
    const now = Date.now();
    this.enqueue("page_leave", this.currentPage, {
      dwell_ms: Math.max(0, now - this.pageEnteredAt)
    });
    const events = this.queue.splice(0, this.queue.length);
    if (events.length > 0 && navigator.sendBeacon) {
      const blob = new Blob([JSON.stringify({ events })], {
        type: "application/json"
      });
      navigator.sendBeacon(EVENT_ENDPOINT, blob);
    }
  };
}
