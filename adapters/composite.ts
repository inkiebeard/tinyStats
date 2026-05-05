// ─────────────────────────────────────────────────────────
// adapters/composite.ts — fan-out across multiple adapters
// ─────────────────────────────────────────────────────────
//
// Useful when you want e.g. Redis (hot reads) + Postgres (durability)
// simultaneously. Uses Promise.allSettled so one failing adapter
// doesn't block others.
// ─────────────────────────────────────────────────────────

import type { FlushAdapter } from '../types';

export class CompositeAdapter implements FlushAdapter {
  constructor(
    private readonly adapters: FlushAdapter[],
    private readonly onPartialFailure?: (err: unknown, index: number) => void
  ) {}

  async flush(deltas: ReadonlyMap<string, number>, bucket: Date): Promise<void> {
    const results = await Promise.allSettled(
      this.adapters.map((a) => a.flush(deltas, bucket))
    );

    const failures = results.flatMap((r, i) =>
      r.status === 'rejected' ? [{ err: r.reason, index: i }] : []
    );

    for (const { err, index } of failures) {
      this.onPartialFailure?.(err, index);
    }

    // Re-throw only if ALL adapters failed — partial failures are surfaced
    // via onPartialFailure and the collector's re-merge behaviour handles retry.
    if (failures.length === this.adapters.length) {
      throw new AggregateError(
        failures.map((f) => f.err),
        'All adapters failed during flush'
      );
    }
  }

  async close(): Promise<void> {
    await Promise.allSettled(this.adapters.map((a) => a.close?.()));
  }
}
