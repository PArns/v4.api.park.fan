import type { Redis, RedisKey, Result } from "ioredis";

/**
 * The statistic's two Redis scripts, each one atomic step: a guard's decision
 * on a value, and the end of an idle visit.
 */

/**
 * One guard's decision on one value, atomically in Redis, so two requests
 * racing at a limit cannot both pass it and nothing has to be taken back:
 *
 * 1. a value already in today's set passes;
 * 2. a value in yesterday's set joins up to the hard limit, without using the
 *    address's budget, so the site's real values come back each day however
 *    full someone made the set after midnight;
 * 3. any other value joins while the set is under its limit and the address
 *    has budget left, and uses one unit of that budget.
 *
 * KEYS: today's set, yesterday's set, the address's counter of new values.
 * ARGV: value, limit, hard limit, address budget, set TTL, counter TTL.
 * Answers 1 when the value passes, 0 when it does not.
 */
export const ADMIT_SCRIPT = `
if redis.call('SISMEMBER', KEYS[1], ARGV[1]) == 1 then return 1 end
local size = redis.call('SCARD', KEYS[1])
if redis.call('SISMEMBER', KEYS[2], ARGV[1]) == 1 then
  if size >= tonumber(ARGV[3]) then return 0 end
else
  if size >= tonumber(ARGV[2]) then return 0 end
  local used = tonumber(redis.call('GET', KEYS[3]) or '0')
  if used >= tonumber(ARGV[4]) then return 0 end
  redis.call('INCR', KEYS[3])
  redis.call('EXPIRE', KEYS[3], ARGV[6], 'NX')
end
redis.call('SADD', KEYS[1], ARGV[1])
redis.call('EXPIRE', KEYS[1], ARGV[5], 'NX')
return 1
`;

/** The command {@link ADMIT_SCRIPT} is registered as on the client. */
export const ADMIT_COMMAND = "siteAnalyticsAdmit";

/**
 * Ends one idle visit, if it is still the visit the flusher read: adds its
 * exit and its span to the buffer of the window of its last hit, puts that
 * window back among those to write, and deletes the visit and its place among
 * the open ones. A hit that arrived after the read changed `l`, and the visit
 * stays open.
 *
 * KEYS: the visit, the open visits, the window's buffer, the windows to write.
 * ARGV: `l` as read, the visit's member, the window, the exit row's
 * dimensions (JSON), the span in seconds. Answers 1 when it closed the visit.
 */
export const CLOSE_VISIT_SCRIPT = `
if redis.call('HGET', KEYS[1], 'l') ~= ARGV[1] then return 0 end
redis.call('HINCRBY', KEYS[3], 'x' .. ARGV[4], 1)
if tonumber(ARGV[5]) > 0 then
  redis.call('HINCRBY', KEYS[3], 'y' .. ARGV[4], ARGV[5])
end
redis.call('SADD', KEYS[4], ARGV[3])
redis.call('DEL', KEYS[1])
redis.call('ZREM', KEYS[2], ARGV[2])
return 1
`;

/** The command {@link CLOSE_VISIT_SCRIPT} is registered as on the client. */
export const CLOSE_VISIT_COMMAND = "siteAnalyticsCloseVisit";

declare module "ioredis" {
  interface RedisCommander<Context> {
    siteAnalyticsCloseVisit(
      visit: RedisKey,
      openVisits: RedisKey,
      buffer: RedisKey,
      windows: RedisKey,
      lastSeen: string,
      member: string,
      window: string,
      exitDims: string,
      span: number,
    ): Result<number, Context>;
    siteAnalyticsAdmit(
      today: RedisKey,
      yesterday: RedisKey,
      counter: RedisKey,
      value: string,
      limit: number,
      hardLimit: number,
      budget: number,
      setTtl: number,
      counterTtl: number,
    ): Result<number, Context>;
  }
}

/**
 * Registers the command on a client; ioredis sends it as `EVALSHA` and loads
 * the script once where Redis does not know it yet. Registering twice is
 * harmless.
 */
export function defineAdmitCommand(redis: Redis): void {
  redis.defineCommand(ADMIT_COMMAND, { numberOfKeys: 3, lua: ADMIT_SCRIPT });
}

/** Registers {@link CLOSE_VISIT_SCRIPT}, as {@link defineAdmitCommand} does. */
export function defineCloseVisitCommand(redis: Redis): void {
  redis.defineCommand(CLOSE_VISIT_COMMAND, {
    numberOfKeys: 4,
    lua: CLOSE_VISIT_SCRIPT,
  });
}
