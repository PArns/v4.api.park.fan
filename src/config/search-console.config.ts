import {
  DEFAULT_IMPORT_DAYS,
  MAX_IMPORT_DAYS,
} from "../search-console/search-console.constants";

/**
 * The Google Search Console import's settings, read from the environment.
 *
 * Functions, like every config file here, so a test can set the variables
 * after import. Without credentials the whole module is off and says why in
 * its status; it never throws at boot over a missing or malformed value.
 */

/** The parts of a Google service account key the import uses. */
export interface ServiceAccount {
  clientEmail: string;
  privateKey: string;
}

export type SearchConsoleSetup =
  | {
      configured: true;
      siteUrl: string;
      account: ServiceAccount;
      /** The hosts whose pages count: `sc-domain:` takes the bare and the `www` host. */
      hosts: string[];
    }
  | {
      configured: false;
      /** Why, for the status; null when nothing was set at all. */
      reason: string | null;
      siteUrl: string | null;
    };

/**
 * The key file's JSON, raw or base64 (a multi-line private key does not travel
 * well in an env file), reduced to what is used. Throws a message that names
 * the problem and never the content.
 */
export function parseServiceAccount(raw: string): ServiceAccount {
  const text = raw.trim();
  let json: string;
  if (text.startsWith("{")) {
    json = text;
  } else {
    json = Buffer.from(text, "base64").toString("utf8").trim();
    if (!json.startsWith("{")) {
      throw new Error(
        "GSC_SERVICE_ACCOUNT_JSON is neither JSON nor base64 of JSON",
      );
    }
  }
  let parsed: unknown;
  try {
    parsed = JSON.parse(json);
  } catch {
    throw new Error("GSC_SERVICE_ACCOUNT_JSON is not valid JSON");
  }
  const key = parsed as { client_email?: unknown; private_key?: unknown };
  if (
    typeof key.client_email !== "string" ||
    !key.client_email.includes("@") ||
    typeof key.private_key !== "string" ||
    !key.private_key.includes("PRIVATE KEY")
  ) {
    throw new Error(
      "GSC_SERVICE_ACCOUNT_JSON has no client_email and private_key: use the key file of a service account",
    );
  }
  return {
    clientEmail: key.client_email,
    // A key that went through a shell or a compose file arrives with literal `\n`.
    privateKey: key.private_key.replace(/\\n/g, "\n"),
  };
}

/** The hosts a property's pages may have, or null for a value that is no property. */
export function siteHosts(siteUrl: string): string[] | null {
  if (siteUrl.startsWith("sc-domain:")) {
    const domain = siteUrl.slice("sc-domain:".length).trim().toLowerCase();
    return /^[a-z0-9.-]+\.[a-z]{2,}$/.test(domain)
      ? [domain, `www.${domain}`]
      : null;
  }
  try {
    const url = new URL(siteUrl);
    return url.protocol === "https:" || url.protocol === "http:"
      ? [url.host.toLowerCase()]
      : null;
  } catch {
    return null;
  }
}

export function getSearchConsoleSetup(
  env: NodeJS.ProcessEnv = process.env,
): SearchConsoleSetup {
  const raw = env.GSC_SERVICE_ACCOUNT_JSON?.trim();
  const siteUrl = env.GSC_SITE_URL?.trim() || null;
  if (!raw) return { configured: false, reason: null, siteUrl };
  if (!siteUrl) {
    return {
      configured: false,
      reason: "GSC_SITE_URL is not set (e.g. sc-domain:park.fan)",
      siteUrl,
    };
  }
  const hosts = siteHosts(siteUrl);
  if (!hosts) {
    return {
      configured: false,
      reason: `GSC_SITE_URL "${siteUrl}" is neither sc-domain:<domain> nor an http(s) URL`,
      siteUrl,
    };
  }
  try {
    return {
      configured: true,
      siteUrl,
      account: parseServiceAccount(raw),
      hosts,
    };
  } catch (error) {
    return {
      configured: false,
      reason: (error as Error).message,
      siteUrl,
    };
  }
}

/** Days every run re-imports: `GSC_IMPORT_DAYS`, 1–30, default 3. */
export function getSearchConsoleImportDays(
  env: NodeJS.ProcessEnv = process.env,
): number {
  const configured = Number.parseInt(env.GSC_IMPORT_DAYS ?? "", 10);
  return Number.isInteger(configured) &&
    configured >= 1 &&
    configured <= MAX_IMPORT_DAYS
    ? configured
    : DEFAULT_IMPORT_DAYS;
}
