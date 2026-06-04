// A tiny rune-based polling primitive. Wraps an async loader on an interval,
// exposing reactive { data, error, loading, lastFetched } and start/stop/refresh.
//
// Usage (inside a component's <script>):
//   const p = createPoll(() => api.stats(), 2000);
//   $effect(() => { p.start(); return () => p.stop(); });
//   ...read p.data, p.error, p.loading

import { ApiError } from './api';

export interface Poll<T> {
  readonly data: T | undefined;
  readonly error: string | undefined;
  readonly loading: boolean;
  readonly lastFetched: number | undefined;
  readonly stale: boolean;
  start(): void;
  stop(): void;
  refresh(): Promise<void>;
}

export function createPoll<T>(loader: () => Promise<T>, intervalMs = 2000): Poll<T> {
  let data = $state<T | undefined>(undefined);
  let error = $state<string | undefined>(undefined);
  let loading = $state(false);
  let lastFetched = $state<number | undefined>(undefined);
  let consecutiveErrors = $state(0);

  let timer: ReturnType<typeof setInterval> | undefined;
  let inflight = false;
  let generation = 0;

  async function tick() {
    if (inflight) return;
    inflight = true;
    const gen = generation;
    loading = data === undefined; // only show the big spinner on first load
    try {
      const result = await loader();
      if (gen !== generation) return; // a stop()/restart happened mid-flight
      data = result;
      error = undefined;
      consecutiveErrors = 0;
      lastFetched = Date.now();
    } catch (e) {
      if (gen !== generation) return;
      consecutiveErrors += 1;
      if (e instanceof ApiError) {
        error = `${e.status} ${e.body || e.message}`.trim();
      } else if (e instanceof Error) {
        error = e.message;
      } else {
        error = String(e);
      }
    } finally {
      if (gen === generation) loading = false;
      inflight = false;
    }
  }

  return {
    get data() {
      return data;
    },
    get error() {
      return error;
    },
    get loading() {
      return loading;
    },
    get lastFetched() {
      return lastFetched;
    },
    // "stale" once two intervals have elapsed without a fresh success.
    get stale() {
      if (lastFetched === undefined) return false;
      return Date.now() - lastFetched > intervalMs * 2.5 || consecutiveErrors > 0;
    },
    start() {
      if (timer) return;
      generation += 1;
      void tick();
      timer = setInterval(() => void tick(), intervalMs);
    },
    stop() {
      generation += 1;
      if (timer) {
        clearInterval(timer);
        timer = undefined;
      }
    },
    async refresh() {
      await tick();
    }
  };
}

// A reactive "now" clock for countdown displays. Ticks every `ms`.
export function createClock(ms = 1000): { now: number; start(): void; stop(): void } {
  let now = $state(Date.now());
  let timer: ReturnType<typeof setInterval> | undefined;
  return {
    get now() {
      return now;
    },
    start() {
      if (timer) return;
      now = Date.now();
      timer = setInterval(() => (now = Date.now()), ms);
    },
    stop() {
      if (timer) {
        clearInterval(timer);
        timer = undefined;
      }
    }
  };
}
