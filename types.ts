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
  /** Called on flush errors. Default: console.error. Must never throw. */
  onFlushError?: (err: unknown) => void;
}

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
