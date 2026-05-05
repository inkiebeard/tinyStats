// tinyStats.ts
function floorToHour(date) {
  const d = new Date(date);
  d.setUTCMinutes(0, 0, 0);
  return d;
}
var StatsCollector = class {
  active = /* @__PURE__ */ new Map();
  adapter;
  onFlushError;
  timer;
  flushing = false;
  destroyed = false;
  constructor(opts) {
    this.adapter = opts.adapter;
    this.onFlushError = opts.onFlushError ?? ((e) => console.error("[stats:collector] flush error", e));
    this.timer = setInterval(
      () => {
        void this.flush();
      },
      opts.flushIntervalMs ?? 5e3
    );
    this.timer.unref?.();
  }
  /**
   * Hot path — single Map write, synchronous, zero I/O.
   * Node.js single-threaded event loop makes this safe without locks.
   */
  increment(key, delta = 1) {
    if (this.destroyed) return;
    const cur = this.active.get(key);
    this.active.set(key, (cur ?? 0) + delta);
  }
  /**
   * Batch increment — useful when processing multiple events at once.
   */
  incrementMany(entries) {
    if (this.destroyed) return;
    for (const [key, delta] of entries) {
      const cur = this.active.get(key);
      this.active.set(key, (cur ?? 0) + delta);
    }
  }
  /** Manually trigger a flush — useful for graceful shutdown. */
  async flush() {
    if (this.active.size === 0) return;
    if (this.flushing) return;
    const toFlush = this.active;
    this.active = /* @__PURE__ */ new Map();
    const bucket = floorToHour(/* @__PURE__ */ new Date());
    this.flushing = true;
    try {
      await this.adapter.flush(toFlush, bucket);
    } catch (err) {
      this.onFlushError(err);
      for (const [key, count] of toFlush) {
        const cur = this.active.get(key);
        this.active.set(key, (cur ?? 0) + count);
      }
    } finally {
      this.flushing = false;
    }
  }
  /** Stop the timer, flush remaining counts, close the adapter. */
  async destroy() {
    this.destroyed = true;
    clearInterval(this.timer);
    await this.flush();
    await this.adapter.close?.();
  }
  /** Current unflushed buffer size — useful for monitoring. */
  get pendingCount() {
    return this.active.size;
  }
};

// rollup.ts
var DEFAULT_LOCKS = {
  hourlyToDaily: 0x574101n,
  // bigint literals for pg_try_advisory_xact_lock
  dailyToMonthly: 0x574102n
};
var RollupJob = class {
  constructor(pool, opts = {}) {
    this.pool = pool;
    this.hourlyRetentionDays = opts.hourlyRetentionDays ?? 3;
    this.dailyRetentionDays = opts.dailyRetentionDays ?? 30;
    this.locks = {
      hourlyToDaily: opts.advisoryLocks?.hourlyToDaily ?? DEFAULT_LOCKS.hourlyToDaily,
      dailyToMonthly: opts.advisoryLocks?.dailyToMonthly ?? DEFAULT_LOCKS.dailyToMonthly
    };
    this.onRollup = opts.onRollup ?? ((r) => console.info("[stats:rollup]", r));
    this.onRollupError = opts.onRollupError ?? ((e) => console.error("[stats:rollup] error", e));
  }
  pool;
  hourlyRetentionDays;
  dailyRetentionDays;
  locks;
  onRollup;
  onRollupError;
  timers = [];
  // ── Public API ───────────────────────────────────────────
  /** Start scheduled rollup jobs. Call once at application startup. */
  start() {
    const t1 = setInterval(() => void this.safeRun("hourly-to-daily"), 60 * 60 * 1e3);
    t1.unref?.();
    const t2 = setInterval(() => void this.safeRun("daily-to-monthly"), 24 * 60 * 60 * 1e3);
    t2.unref?.();
    this.timers.push(t1, t2);
  }
  /** Stop scheduled jobs. Does not flush in-flight work. */
  stop() {
    for (const t of this.timers) clearInterval(t);
    this.timers.length = 0;
  }
  /** Manually run a specific job — useful for backfills or testing. */
  async run(job) {
    return job === "hourly-to-daily" ? this.runHourlyToDaily() : this.runDailyToMonthly();
  }
  // ── Job implementations ──────────────────────────────────
  async runHourlyToDaily() {
    return this.withLock(this.locks.hourlyToDaily, "hourly-to-daily", async (client) => {
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
  async runDailyToMonthly() {
    return this.withLock(this.locks.dailyToMonthly, "daily-to-monthly", async (client) => {
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
  async withLock(lockKey, job, fn) {
    const start = Date.now();
    const client = await this.pool.connect();
    try {
      await client.query("BEGIN");
      const { rows } = await client.query(
        "SELECT pg_try_advisory_xact_lock($1) AS locked",
        [lockKey]
      );
      if (!rows[0]?.["locked"]) {
        await client.query("ROLLBACK");
        return { job, rowsRolled: 0, rowsDeleted: 0, durationMs: Date.now() - start, skipped: true };
      }
      const { rowsRolled, rowsDeleted } = await fn(client);
      await client.query("COMMIT");
      return { job, rowsRolled, rowsDeleted, durationMs: Date.now() - start, skipped: false };
    } catch (err) {
      await client.query("ROLLBACK").catch(() => {
      });
      throw err;
    } finally {
      client.release();
    }
  }
  async safeRun(job) {
    try {
      const result = await this.run(job);
      this.onRollup(result);
    } catch (err) {
      this.onRollupError(err);
    }
  }
};

// adapters/local.ts
var LocalAdapter = class {
  // key → sorted list of hourly buckets
  store = /* @__PURE__ */ new Map();
  async flush(deltas, bucket) {
    for (const [key, count] of deltas) {
      let entries = this.store.get(key);
      if (!entries) {
        entries = [];
        this.store.set(key, entries);
      }
      const existing = entries.find(
        (e) => e.bucket.getTime() === bucket.getTime()
      );
      if (existing) {
        existing.count += count;
      } else {
        entries.push({ bucket: new Date(bucket), count });
      }
    }
  }
  /** Total count for a key across an arbitrary date range */
  query(key, from, to) {
    const entries = this.store.get(key) ?? [];
    return entries.filter((e) => e.bucket >= from && e.bucket <= to).reduce((sum, e) => sum + e.count, 0);
  }
  /** All known keys */
  keys() {
    return Array.from(this.store.keys());
  }
  /** Raw hourly buckets for a key — useful for assertions in tests */
  rawHourly(key) {
    return [...this.store.get(key) ?? []];
  }
  /** Wipe all data — useful between test cases */
  clear() {
    this.store.clear();
  }
};

// adapters/redis.ts
var RedisAdapter = class {
  constructor(client, opts = {}) {
    this.client = client;
    this.namespace = opts.namespace ?? "stats";
    this.ttlSeconds = opts.ttlSeconds ?? 60 * 60 * 24 * 4;
  }
  client;
  namespace;
  ttlSeconds;
  async flush(deltas, bucket) {
    if (deltas.size === 0) return;
    const hashKey = `${this.namespace}:h:${formatHour(bucket)}`;
    const pipeline = this.client.pipeline();
    for (const [key, count] of deltas) {
      pipeline.hincrby(hashKey, key, count);
    }
    pipeline.expire(hashKey, this.ttlSeconds);
    await pipeline.exec();
  }
};
function formatHour(date) {
  return date.toISOString().slice(0, 13);
}
function hourlyKeysInRange(namespace, from, to) {
  const keys = [];
  const cur = floorHour(from);
  while (cur <= to) {
    keys.push(`${namespace}:h:${formatHour(cur)}`);
    cur.setUTCHours(cur.getUTCHours() + 1);
  }
  return keys;
}
function floorHour(date) {
  const d = new Date(date);
  d.setUTCMinutes(0, 0, 0);
  return d;
}

// adapters/postgres.ts
var PostgresAdapter = class {
  constructor(pool) {
    this.pool = pool;
  }
  pool;
  // Tracks which month partitions we've already confirmed exist
  knownPartitions = /* @__PURE__ */ new Set();
  async flush(deltas, bucket) {
    if (deltas.size === 0) return;
    await this.ensureHourlyPartition(bucket);
    const keys = [];
    const counts = [];
    for (const [key, count] of deltas) {
      keys.push(key);
      counts.push(count);
    }
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
  async ensureHourlyPartition(date) {
    const targets = [monthOf(date), nextMonthOf(date)];
    for (const { year, month } of targets) {
      const partitionName = `stats_hourly_${year}_${pad(month)}`;
      if (this.knownPartitions.has(partitionName)) continue;
      const start = `${year}-${pad(month)}-01`;
      const { year: ny, month: nm } = nextMonthOf(/* @__PURE__ */ new Date(`${start}T00:00:00Z`));
      const end = `${ny}-${pad(nm)}-01`;
      await this.pool.query(`
        CREATE TABLE IF NOT EXISTS ${partitionName}
          PARTITION OF stats_hourly
          FOR VALUES FROM ('${start}') TO ('${end}')
      `);
      this.knownPartitions.add(partitionName);
    }
  }
};
async function queryHourly(pool, key, from, to) {
  const { rows } = await pool.query(
    `SELECT bucket, count FROM stats_hourly
     WHERE key = $1 AND bucket >= $2 AND bucket <= $3
     ORDER BY bucket ASC`,
    [key, from, to]
  );
  return rows.map((r) => ({ bucket: new Date(r["bucket"]), count: Number(r["count"]) }));
}
async function queryDaily(pool, key, from, to) {
  const { rows } = await pool.query(
    `SELECT bucket, count FROM stats_daily
     WHERE key = $1 AND bucket >= $2 AND bucket <= $3
     ORDER BY bucket ASC`,
    [key, from, to]
  );
  return rows.map((r) => ({ bucket: new Date(r["bucket"]), count: Number(r["count"]) }));
}
async function queryMonthly(pool, key, from, to) {
  const { rows } = await pool.query(
    `SELECT bucket, count FROM stats_monthly
     WHERE key = $1 AND bucket >= $2 AND bucket <= $3
     ORDER BY bucket ASC`,
    [key, from, to]
  );
  return rows.map((r) => ({ bucket: new Date(r["bucket"]), count: Number(r["count"]) }));
}
async function queryStats(pool, key, from, to) {
  const now = /* @__PURE__ */ new Date();
  const hourlyFrom = new Date(now.getTime() - 3 * 24 * 60 * 60 * 1e3);
  const dailyFrom = new Date(now.getTime() - 30 * 24 * 60 * 60 * 1e3);
  const results = [];
  if (to >= hourlyFrom) {
    const rows = await queryHourly(pool, key, from > hourlyFrom ? from : hourlyFrom, to);
    results.push(...rows.map((r) => ({ ...r, tier: "hourly" })));
  }
  if (from < hourlyFrom && to >= dailyFrom) {
    const rows = await queryDaily(
      pool,
      key,
      from > dailyFrom ? from : dailyFrom,
      to < hourlyFrom ? to : hourlyFrom
    );
    results.push(...rows.map((r) => ({ ...r, tier: "daily" })));
  }
  if (from < dailyFrom) {
    const rows = await queryMonthly(pool, key, from, to < dailyFrom ? to : dailyFrom);
    results.push(...rows.map((r) => ({ ...r, tier: "monthly" })));
  }
  return results.sort((a, b) => a.bucket.getTime() - b.bucket.getTime());
}
function monthOf(date) {
  return { year: date.getUTCFullYear(), month: date.getUTCMonth() + 1 };
}
function nextMonthOf(date) {
  const m = date.getUTCMonth() + 1;
  return m === 12 ? { year: date.getUTCFullYear() + 1, month: 1 } : { year: date.getUTCFullYear(), month: m + 1 };
}
function pad(n) {
  return String(n).padStart(2, "0");
}

// adapters/composite.ts
var CompositeAdapter = class {
  constructor(adapters, onPartialFailure) {
    this.adapters = adapters;
    this.onPartialFailure = onPartialFailure;
  }
  adapters;
  onPartialFailure;
  async flush(deltas, bucket) {
    const results = await Promise.allSettled(
      this.adapters.map((a) => a.flush(deltas, bucket))
    );
    const failures = results.flatMap(
      (r, i) => r.status === "rejected" ? [{ err: r.reason, index: i }] : []
    );
    for (const { err, index } of failures) {
      this.onPartialFailure?.(err, index);
    }
    if (failures.length === this.adapters.length) {
      throw new AggregateError(
        failures.map((f) => f.err),
        "All adapters failed during flush"
      );
    }
  }
  async close() {
    await Promise.allSettled(this.adapters.map((a) => a.close?.()));
  }
};
export {
  CompositeAdapter,
  LocalAdapter,
  PostgresAdapter,
  RedisAdapter,
  RollupJob,
  StatsCollector,
  hourlyKeysInRange,
  queryDaily,
  queryHourly,
  queryMonthly,
  queryStats
};
//# sourceMappingURL=index.js.map