// Runs two jobs on schedule:
//   • Hourly → Daily  (every hour, after hourlyRetentionDays)
//   • Daily  → Monthly (every day,  after dailyRetentionDays)
//
// Distributed safety:
//   Uses pg_try_advisory_xact_lock — a transaction-level advisory
//   lock that is automatically released at COMMIT/ROLLBACK.
//   On a pool of N instances, only one will acquire the lock;
//   the rest see `locked = false` and skip gracefully.
//   No deadlocks, no orphaned locks on crash.
//
// Idempotency:
//   ON CONFLICT DO UPDATE ensures re-running a rollup for the same
//   window only adds the delta once. Safe to run multiple times.
// ─────────────────────────────────────────────────────────

import type { PgPool, PgClient, RollupOptions, RollupResult } from './types';

// Default advisory lock keys — can be overridden via RollupOptions
const DEFAULT_LOCKS = {
  hourlyToDaily:   0x57_41_01n,  // bigint literals for pg_try_advisory_xact_lock
  dailyToMonthly:  0x57_41_02n,
} as const;

export class RollupJob {
  private readonly hourlyRetentionDays: number;
  private readonly dailyRetentionDays: number;
  private readonly locks: { hourlyToDaily: bigint; dailyToMonthly: bigint };
  private readonly onRollup: (result: RollupResult) => void;
  private readonly onRollupError: (err: unknown) => void;
  private readonly timers: ReturnType<typeof setInterval>[] = [];

  constructor(
    private readonly pool: PgPool,
    opts: RollupOptions = {}
  ) {
    this.hourlyRetentionDays = opts.hourlyRetentionDays ?? 3;
    this.dailyRetentionDays  = opts.dailyRetentionDays  ?? 30;
    this.locks = {
      hourlyToDaily:   opts.advisoryLocks?.hourlyToDaily   ?? DEFAULT_LOCKS.hourlyToDaily,
      dailyToMonthly:  opts.advisoryLocks?.dailyToMonthly  ?? DEFAULT_LOCKS.dailyToMonthly,
    };
    this.onRollup      = opts.onRollup      ?? ((r) => console.info('[stats:rollup]', r));
    this.onRollupError = opts.onRollupError ?? ((e) => console.error('[stats:rollup] error', e));
  }

  // ── Public API ───────────────────────────────────────────

  /** Start scheduled rollup jobs. Call once at application startup. */
  start(): void {
    // Hourly → Daily: run every hour
    const t1 = setInterval(() => void this.safeRun('hourly-to-daily'), 60 * 60 * 1_000);
    t1.unref?.();

    // Daily → Monthly: run every 24 hours
    const t2 = setInterval(() => void this.safeRun('daily-to-monthly'), 24 * 60 * 60 * 1_000);
    t2.unref?.();

    this.timers.push(t1, t2);
  }

  /** Stop scheduled jobs. Does not flush in-flight work. */
  stop(): void {
    for (const t of this.timers) clearInterval(t);
    this.timers.length = 0;
  }

  /** Manually run a specific job — useful for backfills or testing. */
  async run(job: RollupResult['job']): Promise<RollupResult> {
    return job === 'hourly-to-daily'
      ? this.runHourlyToDaily()
      : this.runDailyToMonthly();
  }

  // ── Job implementations ──────────────────────────────────

  async runHourlyToDaily(): Promise<RollupResult> {
    return this.withLock(this.locks.hourlyToDaily, 'hourly-to-daily', async (client) => {
      const { rowCount: rowsRolled } = await client.query(
        `INSERT INTO stats_daily (key, bucket, count)
         SELECT
           key,
           date_trunc('day', bucket)::date,
           SUM(count)
         FROM stats_hourly
         WHERE bucket < now() - make_interval(days => $1)
         GROUP BY key, date_trunc('day', bucket)::date
         ON CONFLICT (key, bucket)
         DO UPDATE SET count = stats_daily.count + excluded.count`,
        [this.hourlyRetentionDays]
      );

      const { rowCount: rowsDeleted } = await client.query(
        `DELETE FROM stats_hourly
         WHERE bucket < now() - make_interval(days => $1)`,
        [this.hourlyRetentionDays]
      );

      return { rowsRolled: rowsRolled ?? 0, rowsDeleted: rowsDeleted ?? 0 };
    });
  }

  async runDailyToMonthly(): Promise<RollupResult> {
    return this.withLock(this.locks.dailyToMonthly, 'daily-to-monthly', async (client) => {
      const { rowCount: rowsRolled } = await client.query(
        `INSERT INTO stats_monthly (key, bucket, count)
         SELECT
           key,
           date_trunc('month', bucket)::date,
           SUM(count)
         FROM stats_daily
         WHERE bucket < now() - make_interval(days => $1)
         GROUP BY key, date_trunc('month', bucket)::date
         ON CONFLICT (key, bucket)
         DO UPDATE SET count = stats_monthly.count + excluded.count`,
        [this.dailyRetentionDays]
      );

      const { rowCount: rowsDeleted } = await client.query(
        `DELETE FROM stats_daily
         WHERE bucket < now() - make_interval(days => $1)`,
        [this.dailyRetentionDays]
      );

      return { rowsRolled: rowsRolled ?? 0, rowsDeleted: rowsDeleted ?? 0 };
    });
  }

  // ── Infrastructure ───────────────────────────────────────

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
  private async withLock(
    lockKey: bigint,
    job: RollupResult['job'],
    fn: (client: PgClient) => Promise<{ rowsRolled: number; rowsDeleted: number }>
  ): Promise<RollupResult> {
    const start = Date.now();
    const client = await this.pool.connect();

    try {
      await client.query('BEGIN');

      const { rows } = await client.query(
        'SELECT pg_try_advisory_xact_lock($1) AS locked',
        [lockKey]
      );

      if (!rows[0]?.['locked']) {
        await client.query('ROLLBACK');
        return { job, rowsRolled: 0, rowsDeleted: 0, durationMs: Date.now() - start, skipped: true };
      }

      const { rowsRolled, rowsDeleted } = await fn(client);

      await client.query('COMMIT'); // lock released here automatically

      return { job, rowsRolled, rowsDeleted, durationMs: Date.now() - start, skipped: false };
    } catch (err) {
      await client.query('ROLLBACK').catch(() => {}); // lock released here automatically
      throw err;
    } finally {
      client.release();
    }
  }

  private async safeRun(job: RollupResult['job']): Promise<void> {
    try {
      const result = await this.run(job);
      this.onRollup(result);
    } catch (err) {
      this.onRollupError(err);
    }
  }
}
