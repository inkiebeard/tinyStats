import type {
  CollectorOptions,
  FlushAdapter,
  FlushAttemptContext,
  FlushExecutor,
  FlushRetryOptions,
} from './types';

function floorToHour(date: Date): Date {
  return new Date(Math.floor(date.getTime() / 3_600_000) * 3_600_000);
}

export class StatsCollector {
  private active = new Map<string, number>();
  private readonly adapter: FlushAdapter;
  private readonly onFlushError: (err: unknown) => void;
  private readonly flushExecutor: FlushExecutor;
  private readonly flushRetry: Required<Omit<FlushRetryOptions, 'shouldRetry'>> & {
    shouldRetry: (ctx: FlushAttemptContext) => boolean;
  };
  private readonly retryableCodes: ReadonlySet<string>;
  private readonly nonRetryableCodes: ReadonlySet<string>;
  private readonly timer: ReturnType<typeof setInterval>;
  private flushing = false;
  private destroyed = false;

  constructor(opts: CollectorOptions) {
    this.adapter = opts.adapter;
    this.flushExecutor = opts.flushExecutor ?? defaultFlushExecutor;
    const retryOpts = opts.flushRetry ?? {};
    this.flushRetry = {
      maxAttempts: Math.max(1, retryOpts.maxAttempts ?? 3),
      baseDelayMs: Math.max(0, retryOpts.baseDelayMs ?? 25),
      maxDelayMs: Math.max(0, retryOpts.maxDelayMs ?? 1_000),
      jitterRatio: clamp(retryOpts.jitterRatio ?? 0.25, 0, 1),
      retryableCodes: retryOpts.retryableCodes ?? [],
      nonRetryableCodes: retryOpts.nonRetryableCodes ?? [],
      shouldRetry: retryOpts.shouldRetry ?? isLikelyTransientFlushError,
    };
    this.retryableCodes = new Set(this.flushRetry.retryableCodes.map(normalizeCode));
    this.nonRetryableCodes = new Set(this.flushRetry.nonRetryableCodes.map(normalizeCode));
    this.onFlushError =
      opts.onFlushError ?? ((e) => console.error('[stats:collector] flush error', e));

    this.timer = setInterval(
      () => { void this.flush(); },
      opts.flushIntervalMs ?? 5_000
    );

    // Don't prevent the process from exiting naturally
    this.timer.unref?.();
  }

  /**
   * Hot path — single Map write, synchronous, zero I/O.
   * Node.js single-threaded event loop makes this safe without locks.
   */
  increment(key: string, delta = 1): void {
    if (this.destroyed) return;
    const cur = this.active.get(key);
    this.active.set(key, (cur ?? 0) + delta);
  }

  /**
   * Batch increment — useful when processing multiple events at once.
   */
  incrementMany(entries: Iterable<[key: string, delta: number]>): void {
    if (this.destroyed) return;
    for (const [key, delta] of entries) {
      const cur = this.active.get(key);
      this.active.set(key, (cur ?? 0) + delta);
    }
  }

  /** Manually trigger a flush — useful for graceful shutdown. */
  async flush(): Promise<void> {
    if (this.active.size === 0) return;
    if (this.flushing) return; // don't stack concurrent flushes

    // ── Double-buffer swap ─────────────────────────────────
    // Capture the current buffer and immediately give the hot path
    // a fresh empty map. Increments arriving during the async flush
    // go straight into the new map — zero loss under normal operation.
    const toFlush = this.active;
    this.active = new Map();
    const bucket = floorToHour(new Date());
    // ──────────────────────────────────────────────────────

    this.flushing = true;
    try {
      await this.flushWithRetry(toFlush, bucket);
    } catch (err) {
      this.onFlushError(err);
      // Re-merge failed deltas back so they're retried next cycle.
      // Means a transient adapter failure results in slightly over-counting
      // on recovery rather than dropping counts.
      for (const [key, count] of toFlush) {
        const cur = this.active.get(key);
        this.active.set(key, (cur ?? 0) + count);
      }
    } finally {
      this.flushing = false;
    }
  }

  private async flushWithRetry(deltas: ReadonlyMap<string, number>, bucket: Date): Promise<void> {
    const { maxAttempts, baseDelayMs, maxDelayMs, jitterRatio, shouldRetry } = this.flushRetry;

    for (let attempt = 1; attempt <= maxAttempts; attempt++) {
      try {
        await this.flushExecutor({ deltas, bucket, adapter: this.adapter, attempt });
        return;
      } catch (error) {
        const isLastAttempt = attempt >= maxAttempts;
        if (isLastAttempt || !this.shouldRetryFlushError({ attempt, maxAttempts, error, deltas, bucket }, shouldRetry)) {
          throw error;
        }

        const expDelay = Math.min(maxDelayMs, baseDelayMs * 2 ** (attempt - 1));
        const jitter = expDelay * jitterRatio;
        const delayMs = Math.max(0, expDelay - jitter + Math.random() * (jitter * 2));
        await sleep(delayMs);
      }
    }
  }

  private shouldRetryFlushError(
    ctx: FlushAttemptContext,
    fallbackShouldRetry: (ctx: FlushAttemptContext) => boolean
  ): boolean {
    const code = getErrorCode(ctx.error);
    if (code && this.nonRetryableCodes.has(code)) return false;
    if (code && this.retryableCodes.has(code)) return true;
    return fallbackShouldRetry(ctx);
  }

  /** Stop the timer, flush remaining counts, close the adapter. */
  async destroy(): Promise<void> {
    this.destroyed = true;
    clearInterval(this.timer);
    await this.flush();
    await this.adapter.close?.();
  }

  /** Current unflushed buffer size — useful for monitoring. */
  get pendingCount(): number {
    return this.active.size;
  }
}

const defaultFlushExecutor: FlushExecutor = async ({ deltas, bucket, adapter }) => {
  await adapter.flush(deltas, bucket);
};

function clamp(value: number, min: number, max: number): number {
  return Math.min(max, Math.max(min, value));
}

function sleep(ms: number): Promise<void> {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

function isLikelyTransientFlushError(ctx: FlushAttemptContext): boolean {
  const err = ctx.error;
  if (!err || typeof err !== 'object') return false;

  const code = 'code' in err && typeof err.code === 'string'
    ? err.code.toUpperCase()
    : '';

  if (code === '40P01' || code === '40001' || code === '55P03') return true; // Postgres
  if (code === 'ER_LOCK_DEADLOCK' || code === 'ER_LOCK_WAIT_TIMEOUT') return true; // MySQL
  if (code === 'SQLITE_BUSY' || code === 'SQLITE_LOCKED') return true; // SQLite

  const message = 'message' in err && typeof err.message === 'string'
    ? err.message.toLowerCase()
    : '';

  return (
    message.includes('deadlock') ||
    message.includes('lock wait timeout') ||
    message.includes('serialization failure') ||
    message.includes('could not serialize')
  );
}

function getErrorCode(err: unknown): string {
  if (!err || typeof err !== 'object') return '';
  const raw = 'code' in err ? err.code : undefined;
  if (typeof raw !== 'string') return '';
  return normalizeCode(raw);
}

function normalizeCode(code: string): string {
  return code.trim().toUpperCase();
}
