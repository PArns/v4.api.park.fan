/**
 * A Redis good enough for the site statistic's unit tests: counters, hashes,
 * sets (a HyperLogLog is a set here, exact rather than estimated), RENAME, and
 * the TTL each key was given. Pipelines run their commands in order and
 * answer like ioredis, `[error, result]` per command. Expiry is not
 * simulated; a test ends a visit with `expireKeys(prefix)`.
 */
export class SiteAnalyticsRedisMock {
  counters = new Map<string, number>();
  strings = new Map<string, string>();
  hashes = new Map<string, Map<string, string>>();
  sets = new Map<string, Set<string>>();
  ttls = new Map<string, number>();
  /** Every command a pipeline ran, as `[name, key, ...args]`. */
  commands: Array<[string, ...unknown[]]> = [];
  /** Throw on the next `exec` (one per entry). */
  failNext: string[] = [];

  private has(key: string): boolean {
    return (
      this.counters.has(key) ||
      this.strings.has(key) ||
      this.hashes.has(key) ||
      this.sets.has(key)
    );
  }

  private run(name: string, key: string, args: unknown[]): unknown {
    this.commands.push([name, key, ...args]);
    switch (name) {
      case "incrby":
      case "incr": {
        const by = name === "incr" ? 1 : Number(args[0]);
        this.counters.set(key, (this.counters.get(key) ?? 0) + by);
        return this.counters.get(key);
      }
      case "expire": {
        const [ttl, mode] = args as [number, string | undefined];
        if (!this.has(key) || (mode === "NX" && this.ttls.has(key))) return 0;
        this.ttls.set(key, ttl);
        return 1;
      }
      case "hget":
        return this.hashes.get(key)?.get(String(args[0])) ?? null;
      case "hset": {
        const hash = this.hashes.get(key) ?? new Map<string, string>();
        this.hashes.set(key, hash);
        const added = hash.has(String(args[0])) ? 0 : 1;
        hash.set(String(args[0]), String(args[1]));
        return added;
      }
      case "hincrby": {
        const hash = this.hashes.get(key) ?? new Map<string, string>();
        this.hashes.set(key, hash);
        const next = Number(hash.get(String(args[0])) ?? 0) + Number(args[1]);
        hash.set(String(args[0]), String(next));
        return next;
      }
      case "sadd":
      case "pfadd": {
        const set = this.sets.get(key) ?? new Set<string>();
        this.sets.set(key, set);
        let added = 0;
        for (const member of args as string[]) {
          if (!set.has(member)) added++;
          set.add(member);
        }
        return added > 0 ? 1 : 0;
      }
      case "scard":
      case "pfcount":
        return this.sets.get(key)?.size ?? 0;
      case "srem": {
        const removed = this.sets.get(key)?.delete(String(args[0])) ? 1 : 0;
        return removed;
      }
      case "exists":
        return this.has(key) ? 1 : 0;
      default:
        throw new Error(`Unmocked command ${name}`);
    }
  }

  pipeline() {
    const queued: Array<[string, string, unknown[]]> = [];
    const chain: Record<string, (...args: unknown[]) => unknown> = {};
    for (const name of [
      "incr",
      "incrby",
      "expire",
      "hget",
      "hset",
      "hincrby",
      "sadd",
      "scard",
      "srem",
      "pfadd",
      "pfcount",
      "exists",
    ]) {
      chain[name] = (key: unknown, ...args: unknown[]) => {
        queued.push([name, String(key), args]);
        return chain;
      };
    }
    chain.exec = async () => {
      const failure = this.failNext.shift();
      if (failure) throw new Error(failure);
      return queued.map(([name, key, args]) => [
        null,
        this.run(name, key, args),
      ]);
    };
    return chain as unknown as {
      exec(): Promise<Array<[Error | null, unknown]>>;
    } & Record<string, (...args: unknown[]) => unknown>;
  }

  async set(
    key: string,
    value: string,
    ..._options: unknown[]
  ): Promise<"OK" | null> {
    if (this.strings.has(key)) return null;
    this.strings.set(key, value);
    return "OK";
  }

  async get(key: string): Promise<string | null> {
    return this.strings.get(key) ?? null;
  }

  async del(...keys: string[]): Promise<number> {
    let removed = 0;
    for (const key of keys) {
      for (const map of [this.counters, this.strings, this.hashes, this.sets]) {
        if (map.delete(key)) removed++;
      }
    }
    return removed;
  }

  async exists(key: string): Promise<number> {
    return this.has(key) ? 1 : 0;
  }

  async rename(from: string, to: string): Promise<"OK"> {
    const hash = this.hashes.get(from);
    if (!hash) throw new Error("ERR no such key");
    this.hashes.delete(from);
    this.hashes.set(to, hash);
    return "OK";
  }

  async hgetall(key: string): Promise<Record<string, string>> {
    return Object.fromEntries(this.hashes.get(key) ?? []);
  }

  async smembers(key: string): Promise<string[]> {
    return [...(this.sets.get(key) ?? [])];
  }

  async srem(key: string, member: string): Promise<number> {
    return this.sets.get(key)?.delete(member) ? 1 : 0;
  }

  async pfcount(...keys: string[]): Promise<number> {
    const union = new Set<string>();
    for (const key of keys)
      for (const m of this.sets.get(key) ?? []) union.add(m);
    return union.size;
  }

  async mget(...keys: string[]): Promise<Array<string | null>> {
    return keys.map((key) =>
      this.counters.has(key) ? String(this.counters.get(key)) : null,
    );
  }

  /** Ends every key that starts with `prefix`, as its TTL would. */
  expireKeys(prefix: string): void {
    for (const map of [this.counters, this.strings, this.hashes, this.sets]) {
      for (const key of [...map.keys()])
        if (key.startsWith(prefix)) map.delete(key);
    }
  }
}
