/**
 * The single extension point for storage backends.
 * Implement this to support any storage target.
 */
interface FlushAdapter {
    /**
     * Called on every flush cycle with accumulated deltas since last flush.
     * @param deltas  key → count increments (never zero values)
     * @param bucket  hour-truncated UTC timestamp for this flush window
     */
    flush(deltas: ReadonlyMap<string, number>, bucket: Date): Promise<void>;
    /** Optional cleanup — called on collector.destroy() */
    close?(): Promise<void>;
}
interface CollectorOptions {
    adapter: FlushAdapter;
    /** How often to flush in-memory buffer. Default: 5000ms */
    flushIntervalMs?: number;
    /** Called on flush errors. Default: console.error. Must never throw. */
    onFlushError?: (err: unknown) => void;
}
interface RollupOptions {
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
interface RollupResult {
    job: 'hourly-to-daily' | 'daily-to-monthly';
    rowsRolled: number;
    rowsDeleted: number;
    durationMs: number;
    /** True if another instance held the advisory lock — job was skipped safely */
    skipped: boolean;
}
interface PgQueryResult {
    rowCount: number | null;
    rows: Record<string, unknown>[];
}
interface PgClient {
    query(sql: string, values?: unknown[]): Promise<PgQueryResult>;
    release(): void;
}
interface PgPool {
    query(sql: string, values?: unknown[]): Promise<PgQueryResult>;
    connect(): Promise<PgClient>;
}
interface RedisPipeline {
    hincrby(key: string, field: string, increment: number): unknown;
    expire(key: string, seconds: number): unknown;
    exec(): Promise<unknown>;
}
interface RedisClient {
    pipeline(): RedisPipeline;
}

declare class StatsCollector {
    private active;
    private readonly adapter;
    private readonly onFlushError;
    private readonly timer;
    private flushing;
    private destroyed;
    constructor(opts: CollectorOptions);
    /**
     * Hot path — single Map write, synchronous, zero I/O.
     * Node.js single-threaded event loop makes this safe without locks.
     */
    increment(key: string, delta?: number): void;
    /**
     * Batch increment — useful when processing multiple events at once.
     */
    incrementMany(entries: Iterable<[key: string, delta: number]>): void;
    /** Manually trigger a flush — useful for graceful shutdown. */
    flush(): Promise<void>;
    /** Stop the timer, flush remaining counts, close the adapter. */
    destroy(): Promise<void>;
    /** Current unflushed buffer size — useful for monitoring. */
    get pendingCount(): number;
}

declare class RollupJob {
    private readonly pool;
    private readonly hourlyRetentionDays;
    private readonly dailyRetentionDays;
    private readonly locks;
    private readonly onRollup;
    private readonly onRollupError;
    private readonly timers;
    constructor(pool: PgPool, opts?: RollupOptions);
    /** Start scheduled rollup jobs. Call once at application startup. */
    start(): void;
    /** Stop scheduled jobs. Does not flush in-flight work. */
    stop(): void;
    /** Manually run a specific job — useful for backfills or testing. */
    run(job: RollupResult['job']): Promise<RollupResult>;
    runHourlyToDaily(): Promise<RollupResult>;
    runDailyToMonthly(): Promise<RollupResult>;
    /**
     * Acquires a transaction-level advisory lock, runs the callback,
     * and commits. If the lock is not available (another instance is
     * running the same job), returns a skipped result immediately.
     *
     * pg_try_advisory_xact_lock:
     *   - Non-blocking (returns false rather than waiting)
     *   - Transaction-scoped (auto-released on COMMIT/ROLLBACK)
     *   - Safe across connection pools
     */
    private withLock;
    private safeRun;
}

interface BucketEntry {
    bucket: Date;
    count: number;
}
/**
 * Stores all stats in-memory. No persistence, no network.
 * Useful for tests, local dev, or as a read-back layer in unit tests.
 *
 * Internally keeps hourly buckets; exposes helpers for
 * daily/monthly aggregation matching the tiered schema.
 */
declare class LocalAdapter implements FlushAdapter {
    private readonly store;
    flush(deltas: ReadonlyMap<string, number>, bucket: Date): Promise<void>;
    /** Total count for a key across an arbitrary date range */
    query(key: string, from: Date, to: Date): number;
    /** All known keys */
    keys(): string[];
    /** Raw hourly buckets for a key — useful for assertions in tests */
    rawHourly(key: string): BucketEntry[];
    /** Wipe all data — useful between test cases */
    clear(): void;
}

interface RedisAdapterOptions {
    /** Key prefix. Default: 'stats' */
    namespace?: string;
    /**
     * TTL for hourly hash keys in seconds.
     * Should be longer than your hourlyRetentionDays to give the
     * rollup job time to process before Redis expires the key.
     * Default: 4 days (slightly longer than the 3-day hot window)
     */
    ttlSeconds?: number;
}
declare class RedisAdapter implements FlushAdapter {
    private readonly client;
    private readonly namespace;
    private readonly ttlSeconds;
    constructor(client: RedisClient, opts?: RedisAdapterOptions);
    flush(deltas: ReadonlyMap<string, number>, bucket: Date): Promise<void>;
}
/** All hourly hash keys between two dates (inclusive) */
declare function hourlyKeysInRange(namespace: string, from: Date, to: Date): string[];

declare class PostgresAdapter implements FlushAdapter {
    private readonly pool;
    private readonly knownPartitions;
    constructor(pool: PgPool);
    flush(deltas: ReadonlyMap<string, number>, bucket: Date): Promise<void>;
    /**
     * Ensures the monthly partition for this bucket exists.
     * Creates both the current and next month's partition to avoid
     * a miss at midnight on the first day of a new month.
     */
    private ensureHourlyPartition;
}
/** Hourly counts for a key in a time range (from stats_hourly) */
declare function queryHourly(pool: PgPool, key: string, from: Date, to: Date): Promise<Array<{
    bucket: Date;
    count: number;
}>>;
/** Daily counts for a key in a date range (from stats_daily) */
declare function queryDaily(pool: PgPool, key: string, from: Date, to: Date): Promise<Array<{
    bucket: Date;
    count: number;
}>>;
/** Monthly counts for a key (from stats_monthly) */
declare function queryMonthly(pool: PgPool, key: string, from: Date, to: Date): Promise<Array<{
    bucket: Date;
    count: number;
}>>;
/**
 * Unified query across all three tiers for a key.
 * Automatically selects the appropriate tier based on the date range.
 * Returns rows in ascending time order.
 */
declare function queryStats(pool: PgPool, key: string, from: Date, to: Date): Promise<Array<{
    bucket: Date;
    count: number;
    tier: 'hourly' | 'daily' | 'monthly';
}>>;

declare class CompositeAdapter implements FlushAdapter {
    private readonly adapters;
    private readonly onPartialFailure?;
    constructor(adapters: FlushAdapter[], onPartialFailure?: ((err: unknown, index: number) => void) | undefined);
    flush(deltas: ReadonlyMap<string, number>, bucket: Date): Promise<void>;
    close(): Promise<void>;
}

export { type CollectorOptions, CompositeAdapter, type FlushAdapter, LocalAdapter, type PgClient, type PgPool, PostgresAdapter, RedisAdapter, type RedisClient, RollupJob, type RollupOptions, type RollupResult, StatsCollector, hourlyKeysInRange, queryDaily, queryHourly, queryMonthly, queryStats };
