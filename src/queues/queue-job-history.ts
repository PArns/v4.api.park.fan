/** The Redis commands the history reads use: a queue's client, or a spec's stand-in. */
export interface JobHistoryClient {
  zrange(
    key: string,
    start: number,
    stop: number,
    withScores: "WITHSCORES",
  ): Promise<string[]>;
  zrangebyscore(
    key: string,
    min: string,
    max: string,
    withScores: "WITHSCORES",
  ): Promise<string[]>;
  hmget(key: string, ...fields: string[]): Promise<Array<string | null>>;
}

/** Where one queue keeps its sets and job hashes: `<prefix>:<queue>:<type or id>`. */
export type QueueKey = (typeOrId: string) => string;

/** A failed run as Bull's `failed` set holds it. */
export interface RecordedFailure {
  id: string;
  name: string;
  /** When it failed, ms; the set's score. */
  failedAt: number;
}

/**
 * The newest `limit` failures of a queue, oldest first, read off Bull's
 * `failed` ZSET (scored by the time each run failed) and the jobs' names.
 */
export async function recentFailures(
  client: JobHistoryClient,
  key: QueueKey,
  limit: number,
): Promise<RecordedFailure[]> {
  const scored: string[] = await client.zrange(
    key("failed"),
    -limit,
    -1,
    "WITHSCORES",
  );
  const failures: RecordedFailure[] = [];
  for (let i = 0; i < scored.length; i += 2) {
    const [name] = await client.hmget(key(scored[i]), "name");
    failures.push({
      id: scored[i],
      name: name ?? "unknown",
      failedAt: Number(scored[i + 1]),
    });
  }
  return failures;
}

/**
 * When each job name of a queue last completed after `after` (ms), read off
 * Bull's `completed` ZSET (scored by the finish time). The set keeps the last
 * 100 runs per queue, so on a busy queue an old success may be gone; that
 * errs towards a failure counting as open, never towards hiding one.
 */
export async function newestSuccessByName(
  client: JobHistoryClient,
  key: QueueKey,
  after: number,
): Promise<Map<string, number>> {
  const completed: string[] = await client.zrangebyscore(
    key("completed"),
    `(${after}`,
    "+inf",
    "WITHSCORES",
  );
  const newest = new Map<string, number>();
  for (let i = 0; i < completed.length; i += 2) {
    const doneAt = Number(completed[i + 1]);
    const [name] = await client.hmget(key(completed[i]), "name");
    const jobName = name ?? "unknown";
    newest.set(jobName, Math.max(newest.get(jobName) ?? 0, doneAt));
  }
  return newest;
}

/**
 * When a failure was settled: a later run of the same job name succeeded.
 * Null while the newest run of that name is still a failure.
 */
export function settledAt(
  successes: ReadonlyMap<string, number>,
  name: string,
  failedAt: number,
): number | null {
  const success = successes.get(name) ?? 0;
  return success > failedAt ? success : null;
}
