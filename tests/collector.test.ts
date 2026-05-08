import { describe, it, expect, beforeEach, vi } from 'vitest';
import { StatsCollector } from '../tinyStats';
import { LocalAdapter } from '../adapters/local';
import type { FlushAdapter } from '../types';

describe('StatsCollector', () => {
  it('increments a single key', async () => {
    const adapter = new LocalAdapter();
    const stats = new StatsCollector({ adapter, flushIntervalMs: 1000 });

    stats.increment('test:key');
    stats.increment('test:key');
    stats.increment('test:key', 3);

    expect(stats.pendingCount).toBe(1);
    await stats.flush();
    expect(stats.pendingCount).toBe(0);

    const from = new Date(0);
    const to = new Date('2099-12-31');
    expect(adapter.keys()).toContain('test:key');
    expect(await adapter.query('test:key', from, to)).toBe(5);

    await stats.destroy();
  });

  it('increments multiple keys', async () => {
    const adapter = new LocalAdapter();
    const stats = new StatsCollector({ adapter, flushIntervalMs: 1000 });

    stats.increment('key:a', 10);
    stats.increment('key:b', 20);
    stats.increment('key:c', 30);

    expect(stats.pendingCount).toBe(3);
    await stats.flush();

    const from = new Date(0);
    const to = new Date('2099-12-31');
    expect(await adapter.query('key:a', from, to)).toBe(10);
    expect(await adapter.query('key:b', from, to)).toBe(20);
    expect(await adapter.query('key:c', from, to)).toBe(30);

    await stats.destroy();
  });

  it('uses incrementMany for batch operations', async () => {
    const adapter = new LocalAdapter();
    const stats = new StatsCollector({ adapter, flushIntervalMs: 1000 });

    stats.incrementMany([
      ['batch:1', 100],
      ['batch:2', 200],
      ['batch:3', 300],
    ]);

    await stats.flush();
    const from = new Date(0);
    const to = new Date('2099-12-31');

    expect(await adapter.query('batch:1', from, to)).toBe(100);
    expect(await adapter.query('batch:2', from, to)).toBe(200);
    expect(await adapter.query('batch:3', from, to)).toBe(300);

    await stats.destroy();
  });

  it('double-buffer swap allows increments during flush', async () => {
    let flushDelayMs = 50;
    const flushSpy = vi.fn();

    const slowAdapter: FlushAdapter = {
      async flush(deltas) {
        flushSpy(Array.from(deltas.entries()));
        await new Promise(resolve => setTimeout(resolve, flushDelayMs));
      },
    };

    const stats = new StatsCollector({ adapter: slowAdapter, flushIntervalMs: 1000 });

    stats.increment('before-flush', 1);
    const flushPromise = stats.flush();

    // These increments happen DURING the flush — should go into new buffer
    stats.increment('during-flush-1', 10);
    stats.increment('during-flush-2', 20);

    await flushPromise;
    expect(flushSpy).toHaveBeenCalledTimes(1);
    expect(flushSpy).toHaveBeenCalledWith([['before-flush', 1]]);

    // New buffer should have the during-flush increments
    expect(stats.pendingCount).toBe(2);

    await stats.flush();
    expect(flushSpy).toHaveBeenCalledTimes(2);
    expect(flushSpy).toHaveBeenCalledWith([
      ['during-flush-1', 10],
      ['during-flush-2', 20],
    ]);

    await stats.destroy();
  });

  it('handles flush errors by re-merging deltas', async () => {
    const errorAdapter: FlushAdapter = {
      flush: vi.fn().mockRejectedValue(new Error('flush failed')),
    };

    const onFlushError = vi.fn();
    const stats = new StatsCollector({
      adapter: errorAdapter,
      flushIntervalMs: 1000,
      onFlushError,
    });

    stats.increment('key:fail', 100);
    await stats.flush();

    expect(onFlushError).toHaveBeenCalledWith(expect.any(Error));
    // Deltas should be re-merged back into active buffer
    expect(stats.pendingCount).toBe(1);

    await stats.destroy();
  });

  it('ignores increments after destroy', async () => {
    const adapter = new LocalAdapter();
    const stats = new StatsCollector({ adapter, flushIntervalMs: 1000 });

    stats.increment('before', 1);
    await stats.destroy();

    stats.increment('after', 999);
    expect(stats.pendingCount).toBe(0);

    const from = new Date(0);
    const to = new Date('2099-12-31');
    expect(await adapter.query('before', from, to)).toBe(1);
    expect(adapter.keys()).not.toContain('after');
  });

  it('flushes remaining counts on destroy', async () => {
    const adapter = new LocalAdapter();
    const stats = new StatsCollector({ adapter, flushIntervalMs: 10_000 });

    stats.increment('pending', 42);
    expect(stats.pendingCount).toBe(1);

    await stats.destroy();

    const from = new Date(0);
    const to = new Date('2099-12-31');
    expect(await adapter.query('pending', from, to)).toBe(42);
  });
});
