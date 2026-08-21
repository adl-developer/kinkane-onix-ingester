import { describe, it, expect, vi, afterEach } from 'vitest';
import { runCronTick } from '../cron/run-tick';

/** The logger writes JSON lines to stdout (info) / stderr (warn+error). */
function captureLogs() {
  const lines: Record<string, unknown>[] = [];
  const collect = (chunk: unknown): boolean => {
    lines.push(JSON.parse(String(chunk)));
    return true;
  };
  vi.spyOn(process.stdout, 'write').mockImplementation(collect as never);
  vi.spyOn(process.stderr, 'write').mockImplementation(collect as never);
  return lines;
}

afterEach(() => {
  vi.restoreAllMocks();
});

describe('runCronTick', () => {
  it('logs a start and a completion line for a successful tick', async () => {
    const lines = captureLogs();

    await runCronTick('demo', async () => undefined);

    expect(lines.map((l) => l.message)).toEqual(['Cron tick started', 'Cron tick complete']);
    expect(lines[1].job).toBe('demo');
    expect(typeof lines[1].durationMs).toBe('number');
  });

  it('spreads a returned object onto the completion line', async () => {
    const lines = captureLogs();

    await runCronTick('demo', async () => ({ filesEnqueued: 3 }));

    expect(lines[1].filesEnqueued).toBe(3);
  });

  it('ignores a non-object return value', async () => {
    const lines = captureLogs();

    await runCronTick('demo', async () => 42);

    expect(lines[1].message).toBe('Cron tick complete');
    expect(lines[1]).not.toHaveProperty('0');
  });

  it('swallows a throwing tick and logs it as failed', async () => {
    const lines = captureLogs();

    await expect(runCronTick('demo', async () => {
      throw new Error('boom');
    })).resolves.toBeUndefined();

    expect(lines[1].message).toBe('Cron tick failed');
    expect(lines[1].level).toBe('error');
    expect(lines[1].error).toBe('boom');
  });
});
