/**
 * A Redis good enough for the site statistic's unit tests: counters, hashes,
 * sets (a HyperLogLog is a set here, exact rather than estimated), sorted
 * sets, RENAME, HSCAN, the guard and close-visit scripts
 * (`siteAnalyticsAdmit` and `siteAnalyticsCloseVisit`, the same rules as
 * their Lua in `site-analytics-scripts.ts`) and the TTL each key was given. Pipelines and
 * `MULTI` run their commands in order and answer like ioredis, `[error,
 * result]` per command. Expiry is not simulated; a test ends a visit with
 * `expireKeys(prefix)`.
 */
export class SiteAnalyticsRedisMock {
  counters = new Map<string, number>();
  strings = new Map<string, string>();
  hashes = new Map<string, Map<string, string>>();
  sets = new Map<string, Set<string>>();
  zsets = new Map<string, Map<string, number>>();
  ttls = new Map<string, number>();
  /** Every command a pipeline ran, as `[name, key, ...args]`. */
  commands: Array<[string, ...unknown[]]> = [];
  /** Throw on the next `exec` (one per entry). */
  failNext: string[] = [];
  /** Commands that answer an error inside a `MULTI`'s `exec` rather than run. */
  failCommands = new Set<string>();
  /** Called after each top-level command, to interleave a test's own writes. */
  onCommand: ((name: string, key: string) => void) | null = null;

  private static readonly COMMANDS = [
    "incr",
    "incrby",
    "expire",
    "get",
    "del",
    "hget",
    "hset",
    "hincrby",
    "sadd",
    "scard",
    "srem",
    "sismember",
    "pfadd",
    "pfcount",
    "exists",
    "hmget",
    "hgetall",
    "mget",
    "zadd",
    "zrem",
    "zremrangebyscore",
    "siteAnalyticsAdmit",
    "siteAnalyticsCloseVisit",
  ];

  private has(key: string): boolean {
    return (
      this.counters.has(key) ||
      this.strings.has(key) ||
      this.hashes.has(key) ||
      this.sets.has(key) ||
      this.zsets.has(key)
    );
  }

  private increment(key: string, field: string, by: number): number {
    const hash = this.hashes.get(key) ?? new Map<string, string>();
    this.hashes.set(key, hash);
    const next = Number(hash.get(field) ?? 0) + by;
    hash.set(field, String(next));
    return next;
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
      case "get":
        return this.counters.has(key)
          ? String(this.counters.get(key))
          : (this.strings.get(key) ?? null);
      case "del": {
        let removed = 0;
        for (const map of [
          this.counters,
          this.strings,
          this.hashes,
          this.sets,
          this.zsets,
        ]) {
          if (map.delete(key)) removed++;
        }
        this.ttls.delete(key);
        return removed;
      }
      case "hget":
        return this.hashes.get(key)?.get(String(args[0])) ?? null;
      case "hset": {
        const hash = this.hashes.get(key) ?? new Map<string, string>();
        this.hashes.set(key, hash);
        let added = 0;
        for (let i = 0; i + 1 < args.length; i += 2) {
          if (!hash.has(String(args[i]))) added++;
          hash.set(String(args[i]), String(args[i + 1]));
        }
        return added;
      }
      case "hmget":
        return args.map(
          (field) => this.hashes.get(key)?.get(String(field)) ?? null,
        );
      case "hgetall":
        return Object.fromEntries(this.hashes.get(key) ?? []);
      case "mget":
        return [key, ...(args as string[])].map((name) =>
          this.counters.has(name) ? String(this.counters.get(name)) : null,
        );
      case "zadd": {
        const zset = this.zsets.get(key) ?? new Map<string, number>();
        this.zsets.set(key, zset);
        const added = zset.has(String(args[1])) ? 0 : 1;
        zset.set(String(args[1]), Number(args[0]));
        return added;
      }
      case "zrem": {
        const zset = this.zsets.get(key);
        const removed = zset?.delete(String(args[0])) ? 1 : 0;
        if (zset && zset.size === 0) this.zsets.delete(key);
        return removed;
      }
      case "zremrangebyscore": {
        const zset = this.zsets.get(key);
        if (!zset) return 0;
        const [min, max] = args.map((a) =>
          a === "-inf" ? -Infinity : a === "+inf" ? Infinity : Number(a),
        );
        let removed = 0;
        for (const [member, score] of [...zset]) {
          if (score >= min && score <= max) {
            zset.delete(member);
            removed++;
          }
        }
        if (zset.size === 0) this.zsets.delete(key);
        return removed;
      }
      case "hsetnx": {
        const hash = this.hashes.get(key) ?? new Map<string, string>();
        this.hashes.set(key, hash);
        if (hash.has(String(args[0]))) return 0;
        hash.set(String(args[0]), String(args[1]));
        return 1;
      }
      case "hdel": {
        const hash = this.hashes.get(key);
        const removed = hash?.delete(String(args[0])) ? 1 : 0;
        if (hash && hash.size === 0) this.hashes.delete(key);
        return removed;
      }
      case "hincrby":
        return this.increment(key, String(args[0]), Number(args[1]));
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
      case "sismember":
        return this.sets.get(key)?.has(String(args[0])) ? 1 : 0;
      case "scard":
        return this.sets.get(key)?.size ?? 0;
      case "pfcount": {
        const union = new Set<string>();
        for (const name of [key, ...(args as string[])])
          for (const m of this.sets.get(name) ?? []) union.add(m);
        return union.size;
      }
      case "srem": {
        const set = this.sets.get(key);
        const removed = set?.delete(String(args[0])) ? 1 : 0;
        if (set && set.size === 0) this.sets.delete(key);
        return removed;
      }
      case "exists":
        return this.has(key) ? 1 : 0;
      case "siteAnalyticsAdmit": {
        const [yesterday, counter, value, limit, hard, budget, setTtl, ttl] =
          args as [
            string,
            string,
            string,
            number,
            number,
            number,
            number,
            number,
          ];
        const today = this.sets.get(key) ?? new Set<string>();
        if (today.has(value)) return 1;
        if (this.sets.get(yesterday)?.has(value)) {
          if (today.size >= hard) return 0;
        } else {
          if (today.size >= limit) return 0;
          const used = this.counters.get(counter) ?? 0;
          if (used >= budget) return 0;
          this.counters.set(counter, used + 1);
          if (!this.ttls.has(counter)) this.ttls.set(counter, ttl);
        }
        today.add(value);
        this.sets.set(key, today);
        if (!this.ttls.has(key)) this.ttls.set(key, setTtl);
        return 1;
      }
      case "siteAnalyticsCloseVisit": {
        const [open, buffer, windows, last, member, window, exit, span] =
          args as string[];
        if (this.hashes.get(key)?.get("l") !== String(last)) return 0;
        this.increment(buffer, `x${exit}`, 1);
        if (Number(span) > 0) this.increment(buffer, `y${exit}`, Number(span));
        const set = this.sets.get(windows) ?? new Set<string>();
        set.add(window);
        this.sets.set(windows, set);
        this.hashes.delete(key);
        this.ttls.delete(key);
        this.zsets.get(open)?.delete(member);
        return 1;
      }
      default:
        throw new Error(`Unmocked command ${name}`);
    }
  }

  private chain(transaction: boolean) {
    const queued: Array<[string, string, unknown[]]> = [];
    const chain: Record<string, (...args: unknown[]) => unknown> = {};
    for (const name of SiteAnalyticsRedisMock.COMMANDS) {
      chain[name] = (key: unknown, ...args: unknown[]) => {
        queued.push([name, String(key), args]);
        return chain;
      };
    }
    chain.exec = async () => {
      const failure = this.failNext.shift();
      if (failure) throw new Error(failure);
      return queued.map(([name, key, args]) =>
        transaction && this.failCommands.has(name)
          ? [new Error(`${name} refused`), null]
          : [null, this.run(name, key, args)],
      );
    };
    return chain as unknown as {
      exec(): Promise<Array<[Error | null, unknown]>>;
    } & Record<string, (...args: unknown[]) => unknown>;
  }

  pipeline() {
    return this.chain(false);
  }

  multi() {
    return this.chain(true);
  }

  /** The guard script is answered in JavaScript; nothing to register. */
  defineCommand(): void {}

  private top(name: string, key: string, args: unknown[]): unknown {
    const result = this.run(name, key, args);
    this.onCommand?.(name, key);
    return result;
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
    return this.top("get", key, []) as string | null;
  }

  async del(...keys: string[]): Promise<number> {
    let removed = 0;
    for (const key of keys) removed += this.top("del", key, []) as number;
    return removed;
  }

  async exists(key: string): Promise<number> {
    return this.top("exists", key, []) as number;
  }

  async expire(key: string, ttl: number, mode?: string): Promise<number> {
    return this.top("expire", key, [ttl, mode]) as number;
  }

  async rename(from: string, to: string): Promise<"OK"> {
    const hash = this.hashes.get(from);
    if (!hash) throw new Error("ERR no such key");
    this.hashes.delete(from);
    this.hashes.set(to, hash);
    const ttl = this.ttls.get(from);
    this.ttls.delete(from);
    if (ttl !== undefined) this.ttls.set(to, ttl);
    this.onCommand?.("rename", from);
    return "OK";
  }

  async hgetall(key: string): Promise<Record<string, string>> {
    return Object.fromEntries(this.hashes.get(key) ?? []);
  }

  async hincrby(key: string, field: string, by: number): Promise<number> {
    return this.top("hincrby", key, [field, by]) as number;
  }

  async hsetnx(key: string, field: string, value: string): Promise<number> {
    return this.top("hsetnx", key, [field, value]) as number;
  }

  async hdel(key: string, field: string): Promise<number> {
    return this.top("hdel", key, [field]) as number;
  }

  /** Two fields per step, so a test sees more than one round. */
  async hscan(
    key: string,
    cursor: string,
    ..._options: unknown[]
  ): Promise<[string, string[]]> {
    const entries = [...(this.hashes.get(key) ?? [])];
    const at = Number(cursor);
    const step = entries.slice(at, at + 2).flat();
    const next = at + 2 >= entries.length ? "0" : String(at + 2);
    this.onCommand?.("hscan", key);
    return [next, step];
  }

  async smembers(key: string): Promise<string[]> {
    return [...(this.sets.get(key) ?? [])];
  }

  async sadd(key: string, member: string): Promise<number> {
    return this.top("sadd", key, [member]) as number;
  }

  async zcard(key: string): Promise<number> {
    return this.zsets.get(key)?.size ?? 0;
  }

  /** `min max LIMIT offset count`, members by ascending score. */
  async zrangebyscore(
    key: string,
    min: string | number,
    max: string | number,
    ...options: unknown[]
  ): Promise<string[]> {
    const bound = (a: string | number) =>
      a === "-inf" ? -Infinity : a === "+inf" ? Infinity : Number(a);
    const sorted = [...(this.zsets.get(key) ?? [])]
      .filter(([, score]) => score >= bound(min) && score <= bound(max))
      .sort(([a, x], [b, y]) => x - y || (a < b ? -1 : 1))
      .map(([member]) => member);
    const at = options.indexOf("LIMIT");
    return at >= 0
      ? sorted.slice(
          Number(options[at + 1]),
          Number(options[at + 1]) + Number(options[at + 2]),
        )
      : sorted;
  }

  async scard(key: string): Promise<number> {
    return this.top("scard", key, []) as number;
  }

  async srem(key: string, member: string): Promise<number> {
    return this.top("srem", key, [member]) as number;
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
    for (const map of [
      this.counters,
      this.strings,
      this.hashes,
      this.sets,
      this.zsets,
    ]) {
      for (const key of [...map.keys()])
        if (key.startsWith(prefix)) map.delete(key);
    }
  }
}
