import type { FlushAdapter, QueryAdapter, StatRow, StatRangeRow, Granularity } from '../types';

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
export class LocalAdapter implements FlushAdapter, QueryAdapter {
  // key → (bucket_ms → count): O(1) lookup and update on every flush
  private readonly store = new Map<string, Map<number, number>>();

  async flush(deltas: ReadonlyMap<string, number>, bucket: Date): Promise<void> {
    const bucketMs = bucket.getTime();
    for (const [key, count] of deltas) {
      let buckets = this.store.get(key);
      if (!buckets) this.store.set(key, buckets = new Map());
      buckets.set(bucketMs, (buckets.get(bucketMs) ?? 0) + count);
    }
  }

  /** Total count for a key across an arbitrary date range */
  query(key: string, from: Date, to: Date): Promise<number>;
  /** Bucketed counts aggregated to the requested granularity */
  query(key: string, from: Date, to: Date, granularity: Granularity): Promise<StatRow[]>;
  async query(
    key: string,
    from: Date,
    to: Date,
    granularity?: Granularity,
  ): Promise<number | StatRow[]> {
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
    const agg = new Map<number, number>();
    for (const [t, count] of src) {
      if (t < fromMs || t > toMs) continue;
      const k = floorBucketMs(t, granularity);
      agg.set(k, (agg.get(k) ?? 0) + count);
    }
    const result: StatRow[] = [];
    for (const [ts, count] of agg) {
      result.push({ bucket: new Date(ts), count });
    }
    return result.sort((a, b) => a.bucket.getTime() - b.bucket.getTime());
  }

  /**
   * Auto-selects granularity per sub-range matching the tiered schema:
   * ≤3 days ago → hourly, 3–30 days ago → daily, >30 days ago → monthly.
   */
  async queryRange(key: string, from: Date, to: Date): Promise<StatRangeRow[]> {
    const src = this.store.get(key);
    if (!src) return [];
    const now = Date.now();
    const hourlyFromMs = now - 3 * 24 * 60 * 60 * 1_000;
    const dailyFromMs  = now - 30 * 24 * 60 * 60 * 1_000;
    const fromMs = from.getTime();
    const toMs   = to.getTime();

    const buckets = new Map<number, { count: number; tier: 'hourly' | 'daily' | 'monthly' }>();

    for (const [t, count] of src) {
      if (t < fromMs || t > toMs) continue;

      let tier: 'hourly' | 'daily' | 'monthly';
      let k: number;
      if (t >= hourlyFromMs) {
        tier = 'hourly';
        k = floorBucketMs(t, 'hourly');
      } else if (t >= dailyFromMs) {
        tier = 'daily';
        k = floorBucketMs(t, 'daily');
      } else {
        tier = 'monthly';
        k = floorBucketMs(t, 'monthly');
      }

      const existing = buckets.get(k);
      if (existing) {
        existing.count += count;
      } else {
        buckets.set(k, { count, tier });
      }
    }

    const result: StatRangeRow[] = [];
    for (const [ts, { count, tier }] of buckets) {
      result.push({ bucket: new Date(ts), count, tier });
    }
    return result.sort((a, b) => a.bucket.getTime() - b.bucket.getTime());
  }

  /** All known keys */
  keys(): string[] {
    return [...this.store.keys()];
  }

  /** Raw hourly buckets for a key — useful for assertions in tests */
  rawHourly(key: string): BucketEntry[] {
    const buckets = this.store.get(key);
    if (!buckets) return [];
    return [...buckets.entries()]
      .map(([ts, count]) => ({ bucket: new Date(ts), count }))
      .sort((a, b) => a.bucket.getTime() - b.bucket.getTime());
  }

  /** Wipe all data — useful between test cases */
  clear(): void {
    this.store.clear();
  }
}

function floorBucketMs(ms: number, granularity: Granularity): number {
  if (granularity === 'hourly') return Math.floor(ms / 3_600_000) * 3_600_000;
  if (granularity === 'daily')  return Math.floor(ms / 86_400_000) * 86_400_000;
  // monthly — requires calendar math
  const d = new Date(ms);
  d.setUTCDate(1);
  d.setUTCHours(0, 0, 0, 0);
  return d.getTime();
}
