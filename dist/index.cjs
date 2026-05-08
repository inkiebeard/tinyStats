"use strict";
var __defProp = Object.defineProperty;
var __getOwnPropDesc = Object.getOwnPropertyDescriptor;
var __getOwnPropNames = Object.getOwnPropertyNames;
var __hasOwnProp = Object.prototype.hasOwnProperty;
var __export = (target, all) => {
  for (var name in all)
    __defProp(target, name, { get: all[name], enumerable: true });
};
var __copyProps = (to, from, except, desc) => {
  if (from && typeof from === "object" || typeof from === "function") {
    for (let key of __getOwnPropNames(from))
      if (!__hasOwnProp.call(to, key) && key !== except)
        __defProp(to, key, { get: () => from[key], enumerable: !(desc = __getOwnPropDesc(from, key)) || desc.enumerable });
  }
  return to;
};
var __toCommonJS = (mod) => __copyProps(__defProp({}, "__esModule", { value: true }), mod);

// index.ts
var index_exports = {};
__export(index_exports, {
  CompositeAdapter: () => CompositeAdapter,
  LocalAdapter: () => LocalAdapter,
  PostgresAdapter: () => PostgresAdapter,
  RedisAdapter: () => RedisAdapter,
  RollupJob: () => RollupJob,
  StatsCollector: () => StatsCollector,
  hourlyKeysInRange: () => hourlyKeysInRange,
  queryDaily: () => queryDaily,
  queryHourly: () => queryHourly,
  queryMonthly: () => queryMonthly,
  queryStats: () => queryStats
});
module.exports = __toCommonJS(index_exports);

// tinyStats.ts
function floorToHour(date) {
  return new Date(Math.floor(date.getTime() / 36e5) * 36e5);
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
  // key → (bucket_ms → count): O(1) lookup and update on every flush
  store = /* @__PURE__ */ new Map();
  async flush(deltas, bucket) {
    const bucketMs = bucket.getTime();
    for (const [key, count] of deltas) {
      let buckets = this.store.get(key);
      if (!buckets) this.store.set(key, buckets = /* @__PURE__ */ new Map());
      buckets.set(bucketMs, (buckets.get(bucketMs) ?? 0) + count);
    }
  }
  async query(key, from, to, granularity) {
    const src = this.store.get(key);
    const fromMs = from.getTime();
    const toMs = to.getTime();
    if (!granularity) {
      if (!src) return 0;
      let total = 0;
      for (const [t, count] of src) {
        if (t >= fromMs && t <= toMs) total += count;
      }
      return total;
    }
    if (!src) return [];
    const agg = /* @__PURE__ */ new Map();
    for (const [t, count] of src) {
      if (t < fromMs || t > toMs) continue;
      const k = floorBucketMs(t, granularity);
      agg.set(k, (agg.get(k) ?? 0) + count);
    }
    const result = [];
    for (const [ts, count] of agg) {
      result.push({ bucket: new Date(ts), count });
    }
    return result.sort((a, b) => a.bucket.getTime() - b.bucket.getTime());
  }
  /**
   * Auto-selects granularity per sub-range matching the tiered schema:
   * ≤3 days ago → hourly, 3–30 days ago → daily, >30 days ago → monthly.
   */
  async queryRange(key, from, to) {
    const src = this.store.get(key);
    if (!src) return [];
    const now = Date.now();
    const hourlyFromMs = now - 3 * 24 * 60 * 60 * 1e3;
    const dailyFromMs = now - 30 * 24 * 60 * 60 * 1e3;
    const fromMs = from.getTime();
    const toMs = to.getTime();
    const buckets = /* @__PURE__ */ new Map();
    for (const [t, count] of src) {
      if (t < fromMs || t > toMs) continue;
      let tier;
      let k;
      if (t >= hourlyFromMs) {
        tier = "hourly";
        k = floorBucketMs(t, "hourly");
      } else if (t >= dailyFromMs) {
        tier = "daily";
        k = floorBucketMs(t, "daily");
      } else {
        tier = "monthly";
        k = floorBucketMs(t, "monthly");
      }
      const existing = buckets.get(k);
      if (existing) {
        existing.count += count;
      } else {
        buckets.set(k, { count, tier });
      }
    }
    const result = [];
    for (const [ts, { count, tier }] of buckets) {
      result.push({ bucket: new Date(ts), count, tier });
    }
    return result.sort((a, b) => a.bucket.getTime() - b.bucket.getTime());
  }
  /** All known keys */
  keys() {
    return [...this.store.keys()];
  }
  /** Raw hourly buckets for a key — useful for assertions in tests */
  rawHourly(key) {
    const buckets = this.store.get(key);
    if (!buckets) return [];
    return [...buckets.entries()].map(([ts, count]) => ({ bucket: new Date(ts), count })).sort((a, b) => a.bucket.getTime() - b.bucket.getTime());
  }
  /** Wipe all data — useful between test cases */
  clear() {
    this.store.clear();
  }
};
function floorBucketMs(ms, granularity) {
  if (granularity === "hourly") return Math.floor(ms / 36e5) * 36e5;
  if (granularity === "daily") return Math.floor(ms / 864e5) * 864e5;
  const d = new Date(ms);
  d.setUTCDate(1);
  d.setUTCHours(0, 0, 0, 0);
  return d.getTime();
}

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
  return new Date(Math.floor(date.getTime() / 36e5) * 36e5);
}

// adapters/postgres.ts
var PostgresAdapter = class {
  constructor(pool) {
    this.pool = pool;
  }
  pool;
  // Tracks which month partitions we've already confirmed exist
  knownPartitions = /* @__PURE__ */ new Set();
  async query(key, from, to, granularity) {
    if (!granularity) {
      return queryTotal(this.pool, key, from, to);
    }
    switch (granularity) {
      case "hourly":
        return queryHourly(this.pool, key, from, to);
      case "daily":
        return queryDaily(this.pool, key, from, to);
      case "monthly":
        return queryMonthly(this.pool, key, from, to);
    }
  }
  /** Auto-selects tiers based on the date range. Recent data is hourly,
   *  medium-range is daily, older data is monthly. */
  queryRange(key, from, to) {
    return queryStats(this.pool, key, from, to);
  }
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
async function queryTotal(pool, key, from, to) {
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
  return Number(rows[0]?.["total"] ?? 0);
}
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
  const nowMs = Date.now();
  const hourlyFrom = new Date(nowMs - 3 * 24 * 60 * 60 * 1e3);
  const dailyFrom = new Date(nowMs - 30 * 24 * 60 * 60 * 1e3);
  const monthly = from < dailyFrom ? queryMonthly(pool, key, from, to < dailyFrom ? to : dailyFrom).then((rows) => rows.map((r) => ({ ...r, tier: "monthly" }))) : Promise.resolve([]);
  const daily = from < hourlyFrom && to >= dailyFrom ? queryDaily(pool, key, from > dailyFrom ? from : dailyFrom, to < hourlyFrom ? to : hourlyFrom).then((rows) => rows.map((r) => ({ ...r, tier: "daily" }))) : Promise.resolve([]);
  const hourly = to >= hourlyFrom ? queryHourly(pool, key, from > hourlyFrom ? from : hourlyFrom, to).then((rows) => rows.map((r) => ({ ...r, tier: "hourly" }))) : Promise.resolve([]);
  const [m, d, h] = await Promise.all([monthly, daily, hourly]);
  return [...m, ...d, ...h];
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
    const errors = [];
    for (let i = 0; i < results.length; i++) {
      const r = results[i];
      if (r.status === "rejected") {
        errors.push(r.reason);
        this.onPartialFailure?.(r.reason, i);
      }
    }
    if (errors.length === this.adapters.length) {
      throw new AggregateError(errors, "All adapters failed during flush");
    }
  }
  async close() {
    await Promise.allSettled(this.adapters.map((a) => a.close?.()));
  }
};
// Annotate the CommonJS export names for ESM import in node:
0 && (module.exports = {
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
});
//# sourceMappingURL=index.cjs.map