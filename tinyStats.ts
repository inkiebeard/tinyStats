import type { CollectorOptions, FlushAdapter } from './types';

function floorToHour(date: Date): Date {
  const d = new Date(date);
  d.setUTCMinutes(0, 0, 0);
  return d;
}

export class StatsCollector {
  private active = new Map<string, number>();
  private readonly adapter: FlushAdapter;
  private readonly onFlushError: (err: unknown) => void;
  private readonly timer: ReturnType<typeof setInterval>;
  private flushing = false;
  private destroyed = false;

  constructor(opts: CollectorOptions) {
    this.adapter = opts.adapter;
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
      await this.adapter.flush(toFlush, bucket);
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
