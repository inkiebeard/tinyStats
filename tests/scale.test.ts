import { describe, it, expect } from 'vitest';
import { StatsCollector } from '../tinyStats';
import { LocalAdapter } from '../adapters/local';

// Helper to measure memory before/after
function getMemoryUsageMB(): number {
  if (global.gc) global.gc(); // Force GC if --expose-gc is set
  return process.memoryUsage().heapUsed / 1024 / 1024;
}

describe('Scale & Performance', () => {
  it('handles 10K active keys with ~1MB memory footprint', async () => {
    const adapter = new LocalAdapter();
    const stats = new StatsCollector({ adapter, flushIntervalMs: 60_000 });

    const memBefore = getMemoryUsageMB();

    // Simulate 10K unique keys
    for (let i = 0; i < 10_000; i++) {
      stats.increment(`key:${i}`, Math.floor(Math.random() * 100));
    }

    const memAfter = getMemoryUsageMB();
    const memDelta = memAfter - memBefore;

    expect(stats.pendingCount).toBe(10_000);

    // Per README: "10K active keys ≈ 0.5-1 MB"
    // Allow up to 2MB to account for test overhead
    expect(memDelta).toBeLessThan(2);

    await stats.destroy();
  });

  it('handles 100K active keys with ~5-10MB memory', async () => {
    const adapter = new LocalAdapter();
    const stats = new StatsCollector({ adapter, flushIntervalMs: 60_000 });

    const memBefore = getMemoryUsageMB();

    // Simulate 100K unique keys
    for (let i = 0; i < 100_000; i++) {
      stats.increment(`entity:${i}:views`, 1);
    }

    const memAfter = getMemoryUsageMB();
    const memDelta = memAfter - memBefore;

    expect(stats.pendingCount).toBe(100_000);

    // Per README: "100K active keys ≈ 5-10 MB"
    // Allow up to 15MB to account for test overhead and GC timing
    expect(memDelta).toBeLessThan(15);

    await stats.destroy();
  });

  it('increment operation is sub-microsecond (hot path)', async () => {
    const adapter = new LocalAdapter();
    const stats = new StatsCollector({ adapter, flushIntervalMs: 60_000 });

    const iterations = 100_000;
    const key = 'perf:test';

    const start = process.hrtime.bigint();

    for (let i = 0; i < iterations; i++) {
      stats.increment(key);
    }

    const end = process.hrtime.bigint();
    const totalNs = Number(end - start);
    const nsPerIncrement = totalNs / iterations;

    // Per README: "Single Map.set() operation ≈ 10-50 nanoseconds"
    // In practice with loop overhead, expect < 500ns per increment
    expect(nsPerIncrement).toBeLessThan(500);

    await stats.destroy();
  });

  it('handles 1M increments/sec throughput', async () => {
    const adapter = new LocalAdapter();
    const stats = new StatsCollector({ adapter, flushIntervalMs: 60_000 });

    const targetOps = 1_000_000;
    const keys = Array.from({ length: 1000 }, (_, i) => `key:${i}`);

    const start = Date.now();

    for (let i = 0; i < targetOps; i++) {
      stats.increment(keys[i % keys.length]);
    }

    const elapsed = Date.now() - start;
    const opsPerSec = (targetOps / elapsed) * 1000;

    // Should handle 1M ops/sec easily (per README claims)
    expect(opsPerSec).toBeGreaterThan(1_000_000);

    await stats.destroy();
  });

  it('flush does not block hot path', async () => {
    let flushStarted = false;
    let flushCompleted = false;

    const slowAdapter = {
      async flush() {
        flushStarted = true;
        await new Promise(resolve => setTimeout(resolve, 100)); // 100ms flush
        flushCompleted = true;
      },
    };

    const stats = new StatsCollector({ adapter: slowAdapter, flushIntervalMs: 1000 });

    // Add some data to flush
    stats.increment('initial-data', 1);

    // Start a slow flush
    const flushPromise = stats.flush();

    // Give the flush a moment to start
    await new Promise(resolve => setTimeout(resolve, 10));
    expect(flushStarted).toBe(true);
    expect(flushCompleted).toBe(false);

    // Increments during flush should NOT block
    const start = Date.now();
    for (let i = 0; i < 10_000; i++) {
      stats.increment('hot-path', 1);
    }
    const elapsed = Date.now() - start;

    // Should complete in microseconds, not wait for 100ms flush
    expect(elapsed).toBeLessThan(50);
    expect(flushCompleted).toBe(false); // Flush still running

    await flushPromise;
    expect(flushCompleted).toBe(true);

    await stats.destroy();
  });

  it('incrementMany is efficient for batch operations', async () => {
    const adapter = new LocalAdapter();
    const stats = new StatsCollector({ adapter, flushIntervalMs: 60_000 });

    const batch = Array.from({ length: 10_000 }, (_, i) =>
      [`batch:${i}`, i] as [string, number]
    );

    const start = process.hrtime.bigint();
    stats.incrementMany(batch);
    const end = process.hrtime.bigint();

    const totalNs = Number(end - start);
    const nsPerOp = totalNs / batch.length;

    // Should be similar to individual increments
    expect(nsPerOp).toBeLessThan(1000);
    expect(stats.pendingCount).toBe(10_000);

    await stats.destroy();
  });

  it('accumulates counts correctly over multiple increments', async () => {
    const adapter = new LocalAdapter();
    const stats = new StatsCollector({ adapter, flushIntervalMs: 1000 });

    const key = 'accumulate:test';
    const increments = 1000;
    const deltaPerIncrement = 7;

    for (let i = 0; i < increments; i++) {
      stats.increment(key, deltaPerIncrement);
    }

    await stats.flush();

    const from = new Date(0);
    const to = new Date('2099-12-31');
    expect(adapter.query(key, from, to)).toBe(increments * deltaPerIncrement);

    await stats.destroy();
  });
});
