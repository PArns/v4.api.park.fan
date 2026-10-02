import { readdirSync, readFileSync, statSync } from "fs";
import { join } from "path";
import { PARK_CACHE_FLUSH_PATTERNS } from "./flush-cache-patterns";
import {
  BULL_QUEUE_REGISTRATIONS,
  bullPrefix,
} from "../queues/queue-registrations";
import {
  attractionTimezoneCacheKey,
  latestQueueCacheKey,
} from "../queue-data/queue-data-cache-keys";
import { QueueType } from "../external-apis/themeparks/themeparks.types";

/** Redis KEYS glob → RegExp. The list only uses `*`; `?` and `[` are refused below. */
function globToRegExp(glob: string): RegExp {
  const escaped = glob.replace(/[.+^${}()|\\]/g, "\\$&").replace(/\*/g, ".*");
  return new RegExp(`^${escaped}$`);
}

/** Every queue name the source mentions, registered here or not. */
function queueNamesInSource(): Set<string> {
  const names = new Set<string>();
  const re =
    /(?:@Processor|@InjectQueue)\(\s*"([^"]+)"|registerQueue\(\s*\{\s*name:\s*"([^"]+)"/g;
  const walk = (dir: string): void => {
    for (const entry of readdirSync(dir)) {
      const path = join(dir, entry);
      if (statSync(path).isDirectory()) walk(path);
      else if (path.endsWith(".ts") && !path.endsWith(".spec.ts")) {
        for (const m of readFileSync(path, "utf8").matchAll(re)) {
          names.add(m[1] ?? m[2]);
        }
      }
    }
  };
  walk(join(__dirname, ".."));
  return names;
}

describe("PARK_CACHE_FLUSH_PATTERNS", () => {
  const registered = BULL_QUEUE_REGISTRATIONS.map((q) => q.name as string);
  const queues = [...new Set([...registered, ...queueNamesInSource()])];
  const prefixes = [...new Set(["parkfan", bullPrefix()])];

  // Bull's own key shapes per queue: job hashes, lists, sets, repeat entries.
  const bullSuffixes = [
    "1",
    "id",
    "wait",
    "active",
    "delayed",
    "failed",
    "completed",
    "paused",
    "priority",
    "stalled-check",
    "repeat",
    "repeat:3f9a1c:1759392000000",
    "events",
    "meta",
  ];

  it("finds the registered queues", () => {
    expect(registered.length).toBeGreaterThan(20);
    expect(registered).toContain("analytics");
  });

  it("uses only a trailing * so a literal prefix decides what matches", () => {
    for (const pattern of PARK_CACHE_FLUSH_PATTERNS) {
      expect(pattern).toMatch(/^[^*?[\]]+\*$/);
    }
  });

  it("matches no Bull key of any queue", () => {
    const hits: string[] = [];
    for (const prefix of prefixes) {
      for (const queue of queues) {
        const queueRoot = `${prefix}:${queue}:`;
        for (const pattern of PARK_CACHE_FLUSH_PATTERNS) {
          // With a single trailing `*`, the pattern can match some
          // `<prefix>:<queue>:…` key exactly when one literal prefix starts
          // with the other.
          const literal = pattern.slice(0, -1);
          if (queueRoot.startsWith(literal) || literal.startsWith(queueRoot)) {
            hits.push(`${pattern} ⊇ ${queueRoot}*`);
          }
          for (const suffix of bullSuffixes) {
            if (globToRegExp(pattern).test(queueRoot + suffix)) {
              hits.push(`${pattern} matches ${queueRoot}${suffix}`);
            }
          }
        }
      }
    }
    expect(hits).toEqual([]);
  });

  it("would have caught the old parkfan:* entry", () => {
    expect(globToRegExp("parkfan:*").test("parkfan:analytics:repeat")).toBe(
      true,
    );
  });

  it("still matches the caches QueueDataService writes under parkfan:", () => {
    const keys = [
      latestQueueCacheKey("a1b2", QueueType.STANDBY),
      attractionTimezoneCacheKey("a1b2"),
    ];
    for (const key of keys) {
      expect(
        PARK_CACHE_FLUSH_PATTERNS.some((p) => globToRegExp(p).test(key)),
      ).toBe(true);
    }
  });
});
