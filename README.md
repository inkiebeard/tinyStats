# tinyStats

Zero-latency request counter with tiered time-series storage.

Hot path cost is a single `Map.set()`. Counts are flushed asynchronously on a configurable interval. Storage is pluggable via adapters.

## How it works

```
increment(key)          → in-memory Map (nanoseconds, no I/O)
flush every 5s          → adapter writes deltas to storage
rollup every hour       → hourly rows → daily aggregates (after 3 days)
rollup every day        → daily rows  → monthly aggregates (after 30 days)
```

## Install

```bash
npm install tinyStats
```

Peer dependencies: `pg` and/or `ioredis` depending on which adapters you use.

## Development

```bash
npm install          # Install dependencies
npm run build        # Build to dist/ (ESM + CJS + types)
npm test             # Run unit and scale tests
npm run test:watch   # Run tests in watch mode
```

**Tests include:**
- Unit tests for collector behavior (double-buffer, error handling, flush)
- Scale tests validating memory footprint (10K, 100K keys)
- Performance benchmarks (increment latency, throughput, non-blocking flush)

The build outputs:
- `dist/index.js` — ESM bundle
- `dist/index.cjs` — CommonJS bundle
- `dist/index.d.ts` — TypeScript declarations
- Source maps for both formats

## Quick start

```typescript
import { StatsCollector, PostgresAdapter, RollupJob } from 'tinyStats';
import { Pool } from 'pg';

const pool  = new Pool({ connectionString: process.env.DATABASE_URL });
const stats = new StatsCollector({ adapter: new PostgresAdapter(pool) });
const rollup = new RollupJob(pool);

rollup.start();

// Hot path — this is the entire overhead
stats.increment('product:abc123:views');

// Graceful shutdown
process.on('SIGTERM', async () => {
  rollup.stop();
  await stats.destroy();
});
```

Run the schema once before first use:

```bash
psql $DATABASE_URL -f node_modules/tinyStats/schema.sql
```

## Performance footprint

**Memory (per active key in buffer):**
- ~50-100 bytes per key-value pair in the active Map
- 10K active keys ≈ 0.5-1 MB
- 100K active keys ≈ 5-10 MB
- Double-buffer swap temporarily holds 2× during flush

**CPU per increment:**
- Single `Map.set()` operation ≈ 10-50 nanoseconds
- 1M increments/sec ≈ 5-10% of one core (amortized hash table cost)
- Zero I/O blocking, zero syscalls

**Flush operation:**
- Cost depends entirely on adapter (network + storage write)
- PostgresAdapter: single `INSERT ... ON CONFLICT` per flush batch
- RedisAdapter: single pipeline with `HINCRBY` commands
- Active buffer swaps immediately — hot path never waits

At 10K active keys with 5s flush interval, steady-state memory is **~1 MB** with negligible CPU overhead.

## Adapters

| Adapter | Use case |
|---|---|
| `LocalAdapter` | Dev, testing, no persistence |
| `PostgresAdapter` | Primary storage with full three-tier rollup |
| `RedisAdapter` | Hot-tier cache, hourly hashes with TTL |
| `CompositeAdapter` | Fan-out to multiple adapters simultaneously |

Pass your existing client instances directly — adapters accept structural interfaces, not library-specific types:

```typescript
// Existing clients work as-is
const redis = new Redis(existingConfig);
const pool  = new Pool(existingConfig);

new RedisAdapter(redis);
new PostgresAdapter(pool);
new RollupJob(pool);        // same pool instance, no extra connection
```

**node-redis v4** is not compatible out of the box — it uses `.multi()` instead of `.pipeline()`. A thin wrapper implementing `RedisClient` is required.

## Tiered storage

| Tier | Resolution | Retention | Table |
|---|---|---|---|
| Hot | Hourly | 3 days | `stats_hourly` (partitioned) |
| Warm | Daily | 30 days | `stats_daily` |
| Cold | Monthly | 18 months | `stats_monthly` |

Retention and flush interval are configurable:

```typescript
new StatsCollector({ adapter, flushIntervalMs: 10_000 });

new RollupJob(pool, {
  hourlyRetentionDays: 7,
  dailyRetentionDays:  60,
  onRollup: (r) => logger.info(r),
});

new StatsCollector({
  adapter,
  flushRetry: {
    maxAttempts: 5,
    baseDelayMs: 30,
    maxDelayMs: 750,
    jitterRatio: 0.35,
    retryableCodes: ['ER_LOCK_DEADLOCK'],
    nonRetryableCodes: ['23505'],
  },
});
```

## Querying

```typescript
import { queryStats } from 'tinyStats';

// Automatically selects the right tier based on date range
const rows = await queryStats(pool, 'product:abc123:views', from, to);
// [{ bucket: Date, count: number, tier: 'hourly' | 'daily' | 'monthly' }]
```

## Multiple instances

Rollup jobs are safe to run across multiple instances. Each job acquires a `pg_try_advisory_xact_lock` before executing — only one instance runs per cycle, the rest skip silently. Lock is transaction-scoped and released automatically on completion or crash.

**Custom lock IDs:** If multiple apps share the same database, specify unique advisory lock IDs to prevent conflicts:

```typescript
new RollupJob(pool, {
  advisoryLocks: {
    hourlyToDaily:   0x12_34_56_01n,
    dailyToMonthly:  0x12_34_56_02n,
  }
});
```

Default locks are `0x574101n` and `0x574102n`.

## Storage sizing

At the default retention settings (3d hourly / 30d daily / 18mo monthly), each tracked entity uses a maximum of **120 rows** across all three tiers.

| Active entities | Approx. storage |
|---|---|
| 100K | ~1.5 GB |
| 1M | ~15 GB |
| 10M | ~150 GB |

Rows are only written for windows where activity occurred — sparse entities use proportionally less.

## Flush error behaviour

On adapter failure, unwritten deltas are re-merged into the active buffer and retried on the next flush cycle. A persistent adapter failure causes slight over-counting on recovery rather than data loss.

By default, each flush attempt also retries transient lock/contention failures (for example deadlocks) with exponential backoff + jitter before invoking `onFlushError` and re-merging deltas.

You can pass retryable and non-retryable error codes in the constructor via `flushRetry`. Code matching is case-insensitive, and `nonRetryableCodes` always wins if a code appears in both lists.

You can fully override the flush execution mechanism:

```typescript
new StatsCollector({
  adapter,
  flushExecutor: async ({ deltas, bucket, adapter, attempt }) => {
    // Custom behavior (circuit breaker, tracing, custom lock handling, etc.)
    await adapter.flush(deltas, bucket);
  },
});
```

## API

```typescript
class StatsCollector {
  increment(key: string, delta?: number): void
  incrementMany(entries: Iterable<[string, number]>): void
  flush(): Promise<void>
  destroy(): Promise<void>
  readonly pendingCount: number
}

class RollupJob {
  start(): void
  stop(): void
  run(job: 'hourly-to-daily' | 'daily-to-monthly'): Promise<RollupResult>
}
```
