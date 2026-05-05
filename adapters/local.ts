import type { FlushAdapter } from '../types';

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
export class LocalAdapter implements FlushAdapter {
  // key → sorted list of hourly buckets
  private readonly store = new Map<string, BucketEntry[]>();

  async flush(deltas: ReadonlyMap<string, number>, bucket: Date): Promise<void> {
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
  query(key: string, from: Date, to: Date): number {
    const entries = this.store.get(key) ?? [];
    return entries
      .filter((e) => e.bucket >= from && e.bucket <= to)
      .reduce((sum, e) => sum + e.count, 0);
  }

  /** All known keys */
  keys(): string[] {
    return Array.from(this.store.keys());
  }

  /** Raw hourly buckets for a key — useful for assertions in tests */
  rawHourly(key: string): BucketEntry[] {
    return [...(this.store.get(key) ?? [])];
  }

  /** Wipe all data — useful between test cases */
  clear(): void {
    this.store.clear();
  }
}
