-- ─────────────────────────────────────────────────────────
-- schema.sql — three-tier stats schema
-- Run once at application bootstrap or as a migration.
-- ─────────────────────────────────────────────────────────

-- ── Hot tier: hourly buckets (last 3 days) ────────────────
--
-- Partitioned by calendar month so expiry is a fast partition
-- DROP rather than expensive row-level DELETE with table bloat.
-- The PostgresAdapter creates monthly child partitions on demand.

CREATE TABLE IF NOT EXISTS stats_hourly (
  key     TEXT        NOT NULL,
  bucket  TIMESTAMPTZ NOT NULL,  -- always truncated to the hour (UTC)
  count   BIGINT      NOT NULL DEFAULT 0,
  PRIMARY KEY (key, bucket)
) PARTITION BY RANGE (bucket);

-- Seed the first two partitions so the table is immediately usable.
-- The adapter will create subsequent months at flush time.
DO $$
DECLARE
  y   INT  := EXTRACT(YEAR  FROM now())::INT;
  m   INT  := EXTRACT(MONTH FROM now())::INT;
  nm  INT  := CASE WHEN m = 12 THEN 1  ELSE m + 1 END;
  ny  INT  := CASE WHEN m = 12 THEN y + 1 ELSE y END;
  nnm INT  := CASE WHEN nm = 12 THEN 1  ELSE nm + 1 END;
  nny INT  := CASE WHEN nm = 12 THEN ny + 1 ELSE ny END;
  cur_name  TEXT;
  next_name TEXT;
BEGIN
  cur_name  := format('stats_hourly_%s_%s', y,   lpad(m::TEXT,  2, '0'));
  next_name := format('stats_hourly_%s_%s', ny,  lpad(nm::TEXT, 2, '0'));

  EXECUTE format(
    'CREATE TABLE IF NOT EXISTS %I PARTITION OF stats_hourly
     FOR VALUES FROM (%L) TO (%L)',
    cur_name,
    format('%s-%s-01', y,  lpad(m::TEXT,  2, '0')),
    format('%s-%s-01', ny, lpad(nm::TEXT, 2, '0'))
  );

  EXECUTE format(
    'CREATE TABLE IF NOT EXISTS %I PARTITION OF stats_hourly
     FOR VALUES FROM (%L) TO (%L)',
    next_name,
    format('%s-%s-01', ny,  lpad(nm::TEXT,  2, '0')),
    format('%s-%s-01', nny, lpad(nnm::TEXT, 2, '0'))
  );
END;
$$;

-- ── Warm tier: daily buckets (last 30 days) ───────────────
--
-- Not partitioned — at max 30 * N_records rows it's a manageable
-- table size. Index on (key, bucket) covers all access patterns.

CREATE TABLE IF NOT EXISTS stats_daily (
  key     TEXT NOT NULL,
  bucket  DATE NOT NULL,  -- calendar day (UTC)
  count   BIGINT NOT NULL DEFAULT 0,
  PRIMARY KEY (key, bucket)
);

CREATE INDEX IF NOT EXISTS stats_daily_bucket_idx ON stats_daily (bucket);

-- ── Cold tier: monthly rollups (up to 18 months) ─────────
--
-- Smallest tier. 18 * N_records rows maximum.

CREATE TABLE IF NOT EXISTS stats_monthly (
  key     TEXT NOT NULL,
  bucket  DATE NOT NULL,  -- first day of the month (UTC)
  count   BIGINT NOT NULL DEFAULT 0,
  PRIMARY KEY (key, bucket)
);

CREATE INDEX IF NOT EXISTS stats_monthly_bucket_idx ON stats_monthly (bucket);

-- ── Convenience view: unified stats across all tiers ──────
--
-- Useful for ad-hoc queries. Do not use in hot read paths —
-- build tier-aware queries with explicit date range filtering.

CREATE OR REPLACE VIEW stats_unified AS
  SELECT key, bucket::timestamptz, count, 'hourly'  AS tier FROM stats_hourly
  UNION ALL
  SELECT key, bucket::timestamptz, count, 'daily'   AS tier FROM stats_daily
  UNION ALL
  SELECT key, bucket::timestamptz, count, 'monthly' AS tier FROM stats_monthly;

-- ── Partition expiry helper ───────────────────────────────
--
-- Call this in a weekly maintenance job to drop old hourly partitions.
-- Dropping a partition is near-instant (metadata only) vs DELETE.
--
-- Usage: SELECT drop_expired_hourly_partitions(3);

CREATE OR REPLACE FUNCTION drop_expired_hourly_partitions(retain_days INT DEFAULT 3)
RETURNS TABLE(dropped_partition TEXT) LANGUAGE plpgsql AS $$
DECLARE
  rec RECORD;
  cutoff DATE := (now() - make_interval(days => retain_days))::date;
  part_date DATE;
BEGIN
  FOR rec IN
    SELECT inhrelid::regclass::text AS partition_name
    FROM pg_inherits
    JOIN pg_class ON inhrelid = pg_class.oid
    JOIN pg_namespace ON relnamespace = pg_namespace.oid
    WHERE inhparent = 'stats_hourly'::regclass
  LOOP
    -- Extract YYYY_MM from partition name: stats_hourly_2025_03
    BEGIN
      part_date := to_date(
        substring(rec.partition_name FROM 'stats_hourly_(\d{4}_\d{2})'),
        'YYYY_MM'
      );
    EXCEPTION WHEN OTHERS THEN
      CONTINUE;
    END;

    -- Drop if partition month is entirely before the retention cutoff
    IF (part_date + interval '1 month')::date <= cutoff THEN
      EXECUTE format('DROP TABLE IF EXISTS %s', rec.partition_name);
      dropped_partition := rec.partition_name;
      RETURN NEXT;
    END IF;
  END LOOP;
END;
$$;
