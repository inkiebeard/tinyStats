// ─────────────────────────────────────────────────────────
// examples.ts
// ─────────────────────────────────────────────────────────

import { Pool } from 'pg';
import Redis from 'ioredis';
import {
  StatsCollector,
  RollupJob,
  LocalAdapter,
  RedisAdapter,
  PostgresAdapter,
  CompositeAdapter,
  queryStats,
} from './src';

// ── 1. Local only (testing / dev) ────────────────────────

const local = new LocalAdapter();
const devStats = new StatsCollector({ adapter: local });

devStats.increment('game:match:abc123');
devStats.increment('game:match:abc123', 3);

// After flush:
const total = local.query(
  'game:match:abc123',
  new Date('2025-01-01'),
  new Date()
);


// ── 2. Postgres (full three-tier + rollup) ────────────────

const pool = new Pool({ connectionString: process.env.DATABASE_URL });

const stats = new StatsCollector({
  adapter: new PostgresAdapter(pool),
  flushIntervalMs: 5_000,
  onFlushError: (err) => console.error('flush failed', err),
});

const rollup = new RollupJob(pool, {
  hourlyRetentionDays: 3,
  dailyRetentionDays: 30,
  onRollup: (r) => console.info(`[rollup] ${r.job}: ${r.rowsRolled} rolled, ${r.rowsDeleted} deleted (${r.durationMs}ms)${r.skipped ? ' [skipped]' : ''}`),
  onRollupError: (err) => console.error('[rollup] error', err),
});

rollup.start(); // begins hourly + daily scheduled jobs


// ── 3. Redis hot tier + Postgres cold tier ────────────────

const redis = new Redis(process.env.REDIS_URL!);

const dualStats = new StatsCollector({
  adapter: new CompositeAdapter(
    [
      new RedisAdapter(redis, { namespace: 'stats', ttlSeconds: 60 * 60 * 24 * 4 }),
      new PostgresAdapter(pool),
    ],
    (err, adapterIndex) =>
      console.error(`[stats] adapter[${adapterIndex}] partial failure`, err)
  ),
  flushIntervalMs: 5_000,
});


// ── 4. Hot path usage (e.g. Express, Fastify, game loop) ──

// Game server — action attribution
function onPlayerAction(playerId: string, action: string) {
  stats.increment(`player:${playerId}:${action}`);  // ← entire overhead
}

// Ecommerce — product view counts
function onProductView(productId: string) {
  stats.increment(`product:${productId}:views`);
}

// Social — post engagement
function onPostEngagement(postId: string, type: 'like' | 'share' | 'comment') {
  stats.increment(`post:${postId}:${type}`);
}


// ── 5. Querying across tiers ──────────────────────────────

async function getProductStats(productId: string) {
  const thirtyDaysAgo = new Date(Date.now() - 30 * 24 * 60 * 60 * 1000);
  const now = new Date();

  // Automatically selects hourly / daily / monthly tier based on date range
  const rows = await queryStats(pool, `product:${productId}:views`, thirtyDaysAgo, now);

  return rows;
  // [
  //   { bucket: Date, count: 142, tier: 'hourly' },
  //   { bucket: Date, count: 891, tier: 'daily' },
  //   ...
  // ]
}


// ── 6. Graceful shutdown ──────────────────────────────────

process.on('SIGTERM', async () => {
  rollup.stop();
  await stats.destroy();     // flush remaining buffer, close adapter
  await pool.end();
  await redis.quit();
  process.exit(0);
});


// ── 7. Manual rollup for backfill ────────────────────────

async function backfill() {
  const result = await rollup.run('hourly-to-daily');
  console.log(result);
  // { job: 'hourly-to-daily', rowsRolled: 14823, rowsDeleted: 72000, durationMs: 341, skipped: false }
}
