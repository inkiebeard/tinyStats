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

    // Single pass: invoke callback and collect errors simultaneously.
    const errors: unknown[] = [];
    for (let i = 0; i < results.length; i++) {
      const r = results[i]!;
      if (r.status === 'rejected') {
        errors.push(r.reason);
        this.onPartialFailure?.(r.reason, i);
      }
    }

    // Re-throw only if ALL adapters failed — partial failures are surfaced
    // via onPartialFailure and the collector's re-merge behaviour handles retry.
    if (errors.length === this.adapters.length) {
      throw new AggregateError(errors, 'All adapters failed during flush');
    }
  }

  async close(): Promise<void> {
    await Promise.allSettled(this.adapters.map((a) => a.close?.()));
  }
}
