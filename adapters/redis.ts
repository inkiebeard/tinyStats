// Key structure:
//   {namespace}:h:2025-05-05T14   → HASH { [recordKey]: count }
//
// One Redis hash per hour. Each hash has a TTL slightly longer
// than hourlyRetentionDays so the Postgres rollup job has time
// to pick up the data before expiry.
//
// Compatible with: ioredis, node-redis (both expose .pipeline())
// ─────────────────────────────────────────────────────────

import type { FlushAdapter, RedisClient } from '../types';

export interface RedisAdapterOptions {
  /** Key prefix. Default: 'stats' */
  namespace?: string;
  /**
   * TTL for hourly hash keys in seconds.
   * Should be longer than your hourlyRetentionDays to give the
   * rollup job time to process before Redis expires the key.
   * Default: 4 days (slightly longer than the 3-day hot window)
   */
  ttlSeconds?: number;
}

export class RedisAdapter implements FlushAdapter {
  private readonly namespace: string;
  private readonly ttlSeconds: number;

  constructor(
    private readonly client: RedisClient,
    opts: RedisAdapterOptions = {}
  ) {
    this.namespace = opts.namespace ?? 'stats';
    this.ttlSeconds = opts.ttlSeconds ?? 60 * 60 * 24 * 4; // 4 days
  }

  async flush(deltas: ReadonlyMap<string, number>, bucket: Date): Promise<void> {
    if (deltas.size === 0) return;

    const hashKey = `${this.namespace}:h:${formatHour(bucket)}`;
    const pipeline = this.client.pipeline();

    for (const [key, count] of deltas) {
      pipeline.hincrby(hashKey, key, count);
    }

    // EXPIRE is idempotent — resets TTL on each write which is fine.
    // The hash lives until the last write to it + ttlSeconds.
    pipeline.expire(hashKey, this.ttlSeconds);

    await pipeline.exec();
  }
}

// ─────────────────────────────────────────────────────────
// Key format helpers — exported so query code can reconstruct keys
// ─────────────────────────────────────────────────────────

/** '2025-05-05T14' — matches hash key format */
export function formatHour(date: Date): string {
  return date.toISOString().slice(0, 13);
}

/** All hourly hash keys between two dates (inclusive) */
export function hourlyKeysInRange(
  namespace: string,
  from: Date,
  to: Date
): string[] {
  const keys: string[] = [];
  const cur = floorHour(from);
  while (cur <= to) {
    keys.push(`${namespace}:h:${formatHour(cur)}`);
    cur.setUTCHours(cur.getUTCHours() + 1);
  }
  return keys;
}

function floorHour(date: Date): Date {
  return new Date(Math.floor(date.getTime() / 3_600_000) * 3_600_000);
}
