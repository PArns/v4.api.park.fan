import { readdirSync, readFileSync, statSync } from "fs";
import { join, relative } from "path";

/**
 * The check behind docs/rules/no-address-in-logs.md: no logger call, console
 * call or `logToFile` in `src/` is handed a caller's address. It reads the
 * source, so a new line that interpolates `clientIp(request)` or `request.ip`
 * fails here rather than in a privacy review. What may carry a network is
 * `addressForLog` (a /24 or /48 on an admin request line), not a raw value.
 */

/** Names an address goes by in this code base. */
const ADDRESS_CODE =
  /\b(?:ip|ipAddress|clientIp|getClientIp|getRateLimitIp|remoteAddress|actorIp|lastLoginIp)\b|\.ip\b|x-forwarded-for|cf-connecting-ip/i;

const LOG_CALL =
  /(?:\b(?:this\.)?logger|\bconsole|\bLogger)\.(?:log|warn|error|debug|verbose|fatal)\(|\blogToFile\(/g;

/** The text of a call from its opening parenthesis to the matching one. */
function callText(source: string, open: number): string {
  let depth = 0;
  let quote: string | null = null;
  for (let i = open; i < source.length; i++) {
    const char = source[i];
    if (quote) {
      if (char === "\\") i++;
      else if (char === quote) quote = null;
      continue;
    }
    if (char === '"' || char === "'" || char === "`") quote = char;
    else if (char === "(") depth++;
    else if (char === ")" && --depth === 0) return source.slice(open, i + 1);
  }
  return source.slice(open);
}

/** The call with the text of its string literals taken out; `${…}` code stays. */
function codeOf(call: string): string {
  let out = "";
  let quote: string | null = null;
  for (let i = 0; i < call.length; i++) {
    const char = call[i];
    if (quote === "`" && char === "$" && call[i + 1] === "{") {
      let depth = 0;
      let j = i + 1;
      for (; j < call.length; j++) {
        if (call[j] === "{") depth++;
        else if (call[j] === "}" && --depth === 0) break;
      }
      out += ` ${call.slice(i + 2, j)} `;
      i = j;
      continue;
    }
    if (quote) {
      if (char === "\\") i++;
      else if (char === quote) quote = null;
      continue;
    }
    if (char === '"' || char === "'" || char === "`") quote = char;
    else out += char;
  }
  return out;
}

/** The log calls in `source` that are handed something that names an address. */
export function addressLogCalls(source: string): string[] {
  const found: string[] = [];
  for (const match of source.matchAll(LOG_CALL)) {
    const open = (match.index ?? 0) + match[0].length - 1;
    const call = callText(source, open);
    if (ADDRESS_CODE.test(codeOf(call))) found.push(call.replace(/\s+/g, " "));
  }
  return found;
}

function sourceFiles(dir: string): string[] {
  return readdirSync(dir).flatMap((name) => {
    const path = join(dir, name);
    if (statSync(path).isDirectory()) return sourceFiles(path);
    return /\.ts$/.test(name) && !/\.(spec|e2e-spec)\.ts$/.test(name)
      ? [path]
      : [];
  });
}

describe("no log line carries an address", () => {
  it("finds the line it exists for", () => {
    expect(
      addressLogCalls(
        "this.logger.warn(`Turnstile refused a login from ${clientIp(request) ?? 'x'}`);",
      ),
    ).toHaveLength(1);
    expect(
      addressLogCalls("logger.log(`${method} ${url} - ${ip}`)"),
    ).toHaveLength(1);
    expect(
      addressLogCalls(
        "logToFile('x', { remote: request.socket.remoteAddress })",
      ),
    ).toHaveLength(1);
  });

  it("does not mind the word in a message", () => {
    expect(
      addressLogCalls(
        "this.logger.warn('Could not resolve location from IP, x-forwarded-for missing')",
      ),
    ).toEqual([]);
  });

  it("holds for every logger call in src/", () => {
    const root = join(__dirname, "../..");
    const offenders = sourceFiles(root).flatMap((file) =>
      addressLogCalls(readFileSync(file, "utf8")).map(
        (call) => `${relative(root, file)}: ${call.slice(0, 140)}`,
      ),
    );
    expect(offenders).toEqual([]);
  });
});
