/**
 * A Redis good enough for the admin session tests.
 *
 * Only the commands the session store and the login limiter actually use, and
 * only the semantics they depend on — string get/set with EX, integer INCR,
 * TTL, EXPIRE, and set membership. Enough to prove that a session expires, that
 * an index is pruned, and that a fixed window does not slide; not enough to
 * pass for Redis, which is the point: a fake that grows features nobody tests
 * becomes a second implementation to keep correct.
 *
 * Expiry is checked lazily on read against an injectable clock, so a test can
 * jump forward a day without waiting one.
 */
export class RedisMock {
  private strings = new Map<
    string,
    { value: string; expiresAt: number | null }
  >();
  private sets = new Map<
    string,
    { members: Set<string>; expiresAt: number | null }
  >();

  /** Overridable clock — tests advance this instead of sleeping. */
  now: () => number = () => Date.now();

  private alive<T extends { expiresAt: number | null }>(
    entry: T | undefined,
  ): T | undefined {
    if (!entry) return undefined;
    if (entry.expiresAt !== null && entry.expiresAt <= this.now())
      return undefined;
    return entry;
  }

  async get(key: string): Promise<string | null> {
    const entry = this.alive(this.strings.get(key));
    if (!entry) {
      this.strings.delete(key);
      return null;
    }
    return entry.value;
  }

  async set(
    key: string,
    value: string,
    mode?: string,
    ttlSeconds?: number,
  ): Promise<"OK"> {
    const expiresAt =
      mode?.toUpperCase() === "EX" && typeof ttlSeconds === "number"
        ? this.now() + ttlSeconds * 1000
        : null;
    this.strings.set(key, { value, expiresAt });
    return "OK";
  }

  async del(...keys: string[]): Promise<number> {
    let removed = 0;
    for (const key of keys) {
      if (this.strings.delete(key)) removed++;
      if (this.sets.delete(key)) removed++;
    }
    return removed;
  }

  async ttl(key: string): Promise<number> {
    const entry =
      this.alive(this.strings.get(key)) ?? this.alive(this.sets.get(key));
    if (!entry) return -2;
    if (entry.expiresAt === null) return -1;
    return Math.ceil((entry.expiresAt - this.now()) / 1000);
  }

  async expire(key: string, ttlSeconds: number): Promise<number> {
    const stringEntry = this.alive(this.strings.get(key));
    if (stringEntry) {
      stringEntry.expiresAt = this.now() + ttlSeconds * 1000;
      return 1;
    }
    const setEntry = this.alive(this.sets.get(key));
    if (setEntry) {
      setEntry.expiresAt = this.now() + ttlSeconds * 1000;
      return 1;
    }
    return 0;
  }

  async incr(key: string): Promise<number> {
    const entry = this.alive(this.strings.get(key));
    const next = entry ? Number.parseInt(entry.value, 10) + 1 : 1;
    this.strings.set(key, {
      value: String(next),
      expiresAt: entry?.expiresAt ?? null,
    });
    return next;
  }

  async sadd(key: string, ...members: string[]): Promise<number> {
    const entry = this.alive(this.sets.get(key)) ?? {
      members: new Set<string>(),
      expiresAt: null,
    };
    let added = 0;
    for (const member of members) {
      if (!entry.members.has(member)) {
        entry.members.add(member);
        added++;
      }
    }
    this.sets.set(key, entry);
    return added;
  }

  async srem(key: string, ...members: string[]): Promise<number> {
    const entry = this.alive(this.sets.get(key));
    if (!entry) return 0;
    let removed = 0;
    for (const member of members) {
      if (entry.members.delete(member)) removed++;
    }
    return removed;
  }

  async smembers(key: string): Promise<string[]> {
    const entry = this.alive(this.sets.get(key));
    if (!entry) {
      this.sets.delete(key);
      return [];
    }
    return [...entry.members];
  }

  /** Test helper: how many string keys are live right now. */
  size(): number {
    return [...this.strings.keys()].filter((key) =>
      this.alive(this.strings.get(key)),
    ).length;
  }
}
