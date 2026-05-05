// ─────────────────────────────────────────────────────────
// adapters/postgres.ts — full three-tier Postgres adapter
// ─────────────────────────────────────────────────────────
//
// Writes to stats_hourly only. The RollupJob handles promotion
// to stats_daily and stats_monthly on schedule.
//
// Partitions stats_hourly by calendar month. Partitions are
// created on-demand at flush time via CREATE TABLE IF NOT EXISTS.
// A small in-process cache prevents redundant partition checks.
// ─────────────────────────────────────────────────────────

import type { FlushAdapter, PgPool } from '../types';

export class PostgresAdapter implements FlushAdapter {
  // Tracks which month partitions we've already confirmed exist
  private readonly knownPartitions = new Set<string>();

  constructor(private readonly pool: PgPool) {}

  async flush(deltas: ReadonlyMap<string, number>, bucket: Date): Promise<void> {
    if (deltas.size === 0) return;

    await this.ensureHourlyPartition(bucket);

    const keys: string[] = [];
    const counts: number[] = [];
    for (const [key, count] of deltas) {
      keys.push(key);
      counts.push(count);
    }

    // Single round-trip regardless of delta size via unnest batch
    await this.pool.query(
      `INSERT INTO stats_hourly (key, bucket, count)
       SELECT
         unnest($1::text[]),
         $2::timestamptz,
         unnest($3::bigint[])
       ON CONFLICT (key, bucket)
       DO UPDATE SET count = stats_hourly.count + excluded.count`,
      [keys, bucket, counts]
    );
  }

  /**
   * Ensures the monthly partition for this bucket exists.
   * Creates both the current and next month's partition to avoid
   * a miss at midnight on the first day of a new month.
   */
  private async ensureHourlyPartition(date: Date): Promise<void> {
    const targets = [monthOf(date), nextMonthOf(date)];

    for (const { year, month } of targets) {
      const partitionName = `stats_hourly_${year}_${pad(month)}`;
      if (this.knownPartitions.has(partitionName)) continue;

      const start = `${year}-${pad(month)}-01`;
      const { year: ny, month: nm } = nextMonthOf(new Date(`${start}T00:00:00Z`));
      const end = `${ny}-${pad(nm)}-01`;

      await this.pool.query(`
        CREATE TABLE IF NOT EXISTS ${partitionName}
          PARTITION OF stats_hourly
          FOR VALUES FROM ('${start}') TO ('${end}')
      `);

      this.knownPartitions.add(partitionName);
    }
  }
}

// ─────────────────────────────────────────────────────────
// Query helpers — composable, not coupled to the adapter
// ─────────────────────────────────────────────────────────

/** Hourly counts for a key in a time range (from stats_hourly) */
export async function queryHourly(
  pool: PgPool,
  key: string,
  from: Date,
  to: Date
): Promise<Array<{ bucket: Date; count: number }>> {
  const { rows } = await pool.query(
    `SELECT bucket, count FROM stats_hourly
     WHERE key = $1 AND bucket >= $2 AND bucket <= $3
     ORDER BY bucket ASC`,
    [key, from, to]
  );
  return rows.map((r) => ({ bucket: new Date(r['bucket'] as string), count: Number(r['count']) }));
}

/** Daily counts for a key in a date range (from stats_daily) */
export async function queryDaily(
  pool: PgPool,
  key: string,
  from: Date,
  to: Date
): Promise<Array<{ bucket: Date; count: number }>> {
  const { rows } = await pool.query(
    `SELECT bucket, count FROM stats_daily
     WHERE key = $1 AND bucket >= $2 AND bucket <= $3
     ORDER BY bucket ASC`,
    [key, from, to]
  );
  return rows.map((r) => ({ bucket: new Date(r['bucket'] as string), count: Number(r['count']) }));
}

/** Monthly counts for a key (from stats_monthly) */
export async function queryMonthly(
  pool: PgPool,
  key: string,
  from: Date,
  to: Date
): Promise<Array<{ bucket: Date; count: number }>> {
  const { rows } = await pool.query(
    `SELECT bucket, count FROM stats_monthly
     WHERE key = $1 AND bucket >= $2 AND bucket <= $3
     ORDER BY bucket ASC`,
    [key, from, to]
  );
  return rows.map((r) => ({ bucket: new Date(r['bucket'] as string), count: Number(r['count']) }));
}

/**
 * Unified query across all three tiers for a key.
 * Automatically selects the appropriate tier based on the date range.
 * Returns rows in ascending time order.
 */
export async function queryStats(
  pool: PgPool,
  key: string,
  from: Date,
  to: Date
): Promise<Array<{ bucket: Date; count: number; tier: 'hourly' | 'daily' | 'monthly' }>> {
  const now = new Date();
  const hourlyFrom = new Date(now.getTime() - 3 * 24 * 60 * 60 * 1000);
  const dailyFrom = new Date(now.getTime() - 30 * 24 * 60 * 60 * 1000);

  const results: Array<{ bucket: Date; count: number; tier: 'hourly' | 'daily' | 'monthly' }> = [];

  if (to >= hourlyFrom) {
    const rows = await queryHourly(pool, key, from > hourlyFrom ? from : hourlyFrom, to);
    results.push(...rows.map((r) => ({ ...r, tier: 'hourly' as const })));
  }

  if (from < hourlyFrom && to >= dailyFrom) {
    const rows = await queryDaily(
      pool, key,
      from > dailyFrom ? from : dailyFrom,
      to < hourlyFrom ? to : hourlyFrom
    );
    results.push(...rows.map((r) => ({ ...r, tier: 'daily' as const })));
  }

  if (from < dailyFrom) {
    const rows = await queryMonthly(pool, key, from, to < dailyFrom ? to : dailyFrom);
    results.push(...rows.map((r) => ({ ...r, tier: 'monthly' as const })));
  }

  return results.sort((a, b) => a.bucket.getTime() - b.bucket.getTime());
}

// ── Date helpers ──────────────────────────────────────────

function monthOf(date: Date) {
  return { year: date.getUTCFullYear(), month: date.getUTCMonth() + 1 };
}

function nextMonthOf(date: Date) {
  const m = date.getUTCMonth() + 1; // 1-12
  return m === 12
    ? { year: date.getUTCFullYear() + 1, month: 1 }
    : { year: date.getUTCFullYear(), month: m + 1 };
}

function pad(n: number): string {
  return String(n).padStart(2, '0');
}
