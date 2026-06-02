/**
 * The single extension point for storage backends.
 * Implement this to support any storage target.
 */
export interface FlushAdapter {
  /**
   * Called on every flush cycle with accumulated deltas since last flush.
   * @param deltas  key → count increments (never zero values)
   * @param bucket  hour-truncated UTC timestamp for this flush window
   */
  flush(deltas: ReadonlyMap<string, number>, bucket: Date): Promise<void>;

  /** Optional cleanup — called on collector.destroy() */
  close?(): Promise<void>;
}

export interface CollectorOptions {
  adapter: FlushAdapter;
  /** How often to flush in-memory buffer. Default: 5000ms */
  flushIntervalMs?: number;
  /**
   * Retry behavior for a failed flush attempt.
   * Defaults are tuned for transient lock/deadlock contention.
   */
  flushRetry?: FlushRetryOptions;
  /**
   * Optional hook to override the flushing mechanism.
   * Default behavior calls adapter.flush(deltas, bucket).
   */
  flushExecutor?: FlushExecutor;
  /** Called on flush errors. Default: console.error. Must never throw. */
  onFlushError?: (err: unknown) => void;
}

export interface FlushAttemptContext {
  attempt: number;
  maxAttempts: number;
  error: unknown;
  deltas: ReadonlyMap<string, number>;
  bucket: Date;
}

export interface FlushRetryOptions {
  /** Total attempts including the first one. Default: 3 */
  maxAttempts?: number;
  /** Base backoff delay in ms. Default: 25 */
  baseDelayMs?: number;
  /** Maximum backoff delay in ms. Default: 1000 */
  maxDelayMs?: number;
  /** Randomization ratio applied around backoff delay. Default: 0.25 */
  jitterRatio?: number;
  /** Explicitly retry when error.code matches one of these values. */
  retryableCodes?: string[];
  /** Never retry when error.code matches one of these values. */
  nonRetryableCodes?: string[];
  /**
   * Return true to retry. Default retries known transient lock/contention errors.
   */
  shouldRetry?: (ctx: FlushAttemptContext) => boolean;
}

export type FlushExecutor = (ctx: {
  deltas: ReadonlyMap<string, number>;
  bucket: Date;
  adapter: FlushAdapter;
  attempt: number;
}) => Promise<void>;

export interface RollupOptions {
  /** How many days to retain hourly rows before rolling to daily. Default: 3 */
  hourlyRetentionDays?: number;
  /** How many days to retain daily rows before rolling to monthly. Default: 30 */
  dailyRetentionDays?: number;
  /** Advisory lock IDs for coordinating rollup jobs across instances. */
  advisoryLocks?: {
    hourlyToDaily?: bigint;
    dailyToMonthly?: bigint;
  };
  /** Called after each successful rollup. */
  onRollup?: (result: RollupResult) => void;
  /** Called on rollup errors. Must never throw. */
  onRollupError?: (err: unknown) => void;
}

export interface RollupResult {
  job: 'hourly-to-daily' | 'daily-to-monthly';
  rowsRolled: number;
  rowsDeleted: number;
  durationMs: number;
  /** True if another instance held the advisory lock — job was skipped safely */
  skipped: boolean;
}

// ── Structural client interfaces ──────────────────────────
//
// These are structural (duck-typed) interfaces, not stubs to replace.
// Pass your existing client instances directly — TypeScript checks
// shape, not identity.
//
// Verified compatible with:
//   pg (node-postgres) Pool      → satisfies PgPool / PgClient
//   ioredis Redis                → satisfies RedisClient
//
// node-redis v4 uses .multi() not .pipeline() — use the ioredis
// adapter as a reference to write a thin wrapper if needed.

export interface PgQueryResult {
  rowCount: number | null;
  rows: Record<string, unknown>[];
}

export interface PgClient {
  query(sql: string, values?: unknown[]): Promise<PgQueryResult>;
  release(): void;
}

export interface PgPool {
  query(sql: string, values?: unknown[]): Promise<PgQueryResult>;
  connect(): Promise<PgClient>;
}

export interface RedisPipeline {
  // Return types are unknown — we never use the chained return value,
  // only call .exec() at the end. Using `this` here would reject
  // ioredis's ChainableCommander despite being runtime-compatible.
  hincrby(key: string, field: string, increment: number): unknown;
  expire(key: string, seconds: number): unknown;
  exec(): Promise<unknown>;
}

export interface RedisClient {
  pipeline(): RedisPipeline;
}

// ── Query types ───────────────────────────────────────────

export interface StatRow {
  bucket: Date;
  count: number;
}

/** A bucketed row with its storage tier — returned by queryRange */
export interface StatRangeRow extends StatRow {
  tier: Granularity;
}

export type Granularity = 'hourly' | 'daily' | 'monthly';

/**
 * Optional extension point for storage backends that support reads.
 * Implement this alongside FlushAdapter to enable query capabilities.
 *
 * Overloaded signatures:
 *   query(key, from, to)               → total count as number
 *   query(key, from, to, granularity)  → bucketed rows as StatRow[]
 *
 * queryRange(key, from, to) automatically selects the best tier per
 * sub-range (hourly for recent, daily for medium, monthly for old)
 * and returns StatRangeRow[] with a tier tag on each bucket.
 */
export interface QueryAdapter {
  query(key: string, from: Date, to: Date): Promise<number>;
  query(key: string, from: Date, to: Date, granularity: Granularity): Promise<StatRow[]>;
  queryRange(key: string, from: Date, to: Date): Promise<StatRangeRow[]>;
}
