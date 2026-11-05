/**
 * Clients that render a page without a person reading it: crawlers (Googlebot
 * runs the page's JavaScript and would fire the view), Google's testing tools,
 * AI agents fetching for a user, link previews, uptime and speed checks,
 * headless browsers and plain HTTP libraries.
 *
 * `bot` only counts as a word ending (`Googlebot/`, `AdsBot-Google`,
 * `Applebot;`), and not after `cu` or `ro`: CUBOT phones and devices named
 * "Robot" are people.
 */
const BOT_USER_AGENT =
  /(?<!cu|ro)bot(?:[/\-_;)]|$)|crawl|spider|slurp|archiver|facebookexternalhit|embedly|preview|headless|lighthouse|pagespeed|gtmetrix|pingdom|uptime|ptst\/|cypress|google-inspectiontool|googleother|mediapartners-google|google-read-aloud|chatgpt-user|claude-user|perplexity-user|curl|wget|python|java\/|go-http|okhttp|axios|node-fetch|undici|libwww|httpclient|scrapy|phantomjs|puppeteer|playwright|selenium/i;

/**
 * True when a view from this User-Agent must not be counted. A missing one
 * counts as a bot: every browser sends one.
 */
export function isBotUserAgent(userAgent: string | null | undefined): boolean {
  return !userAgent || BOT_USER_AGENT.test(userAgent);
}
