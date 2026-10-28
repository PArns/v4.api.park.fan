import type { Redis, RedisKey, Result } from "ioredis";

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

declare module "ioredis" {
  interface RedisCommander<Context> {
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
