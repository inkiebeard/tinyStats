// Writes to stats_hourly only. The RollupJob handles promotion
// to stats_daily and stats_monthly on schedule.
//
// Partitions stats_hourly by calendar month. Partitions are
// created on-demand at flush time via CREATE TABLE IF NOT EXISTS.
// A small in-process cache prevents redundant partition checks.
// ─────────────────────────────────────────────────────────

import type { FlushAdapter, QueryAdapter, StatRow, StatRangeRow, Granularity, PgPool } from '../types';

export class PostgresAdapter implements FlushAdapter, QueryAdapter {
  // Tracks which month partitions we've already confirmed exist
  private readonly knownPartitions = new Set<string>();

  constructor(private readonly pool: PgPool) {}

  query(key: string, from: Date, to: Date): Promise<number>;
  query(key: string, from: Date, to: Date, granularity: Granularity): Promise<StatRow[]>;
  async query(
    key: string,
    from: Date,
    to: Date,
    granularity?: Granularity,
  ): Promise<number | StatRow[]> {
    if (!granularity) {
      // Single aggregating query across all tiers — one round-trip, no row transfer
      return queryTotal(this.pool, key, from, to);
    }
    switch (granularity) {
      case 'hourly':  return queryHourly(this.pool, key, from, to);
      case 'daily':   return queryDaily(this.pool, key, from, to);
      case 'monthly': return queryMonthly(this.pool, key, from, to);
    }
  }

  /** Auto-selects tiers based on the date range. Recent data is hourly,
   *  medium-range is daily, older data is monthly. */
  queryRange(key: string, from: Date, to: Date): Promise<StatRangeRow[]> {
    return queryStats(this.pool, key, from, to);
  }

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

/**
 * Total count for a key across all tiers in a single aggregating query.
 * Uses UNION ALL across all three tables so Postgres does the SUM
 * server-side — one round-trip, minimal data transfer.
 */
async function queryTotal(pool: PgPool, key: string, from: Date, to: Date): Promise<number> {
  const { rows } = await pool.query(
    `SELECT COALESCE(SUM(count), 0)::bigint AS total
     FROM (
       SELECT count FROM stats_hourly  WHERE key = $1 AND bucket >= $2 AND bucket <= $3
       UNION ALL
       SELECT count FROM stats_daily   WHERE key = $1 AND bucket >= $2 AND bucket <= $3
       UNION ALL
       SELECT count FROM stats_monthly WHERE key = $1 AND bucket >= $2 AND bucket <= $3
     ) t`,
    [key, from, to]
  );
  return Number(rows[0]?.['total'] ?? 0);
}

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
): Promise<StatRangeRow[]> {
  const nowMs = Date.now();
  const hourlyFrom = new Date(nowMs - 3 * 24 * 60 * 60 * 1_000);
  const dailyFrom  = new Date(nowMs - 30 * 24 * 60 * 60 * 1_000);

  // Build all needed queries upfront and run them in parallel.
  // Ordered oldest→newest so concatenation is already chronological.
  const monthly = from < dailyFrom
    ? queryMonthly(pool, key, from, to < dailyFrom ? to : dailyFrom)
        .then(rows => rows.map(r => ({ ...r, tier: 'monthly' as const })))
    : Promise.resolve([] as StatRangeRow[]);

  const daily = from < hourlyFrom && to >= dailyFrom
    ? queryDaily(pool, key, from > dailyFrom ? from : dailyFrom, to < hourlyFrom ? to : hourlyFrom)
        .then(rows => rows.map(r => ({ ...r, tier: 'daily' as const })))
    : Promise.resolve([] as StatRangeRow[]);

  const hourly = to >= hourlyFrom
    ? queryHourly(pool, key, from > hourlyFrom ? from : hourlyFrom, to)
        .then(rows => rows.map(r => ({ ...r, tier: 'hourly' as const })))
    : Promise.resolve([] as StatRangeRow[]);

  // Each segment sorted ASC by SQL ORDER BY; segments are in chronological
  // order, so concatenation produces a globally sorted result — no sort needed.
  const [m, d, h] = await Promise.all([monthly, daily, hourly]);
  return [...m, ...d, ...h];
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
