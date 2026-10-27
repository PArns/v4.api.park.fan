/**
 * Clients that render a page without a person reading it: crawlers (Googlebot
 * runs the page's JavaScript and would fire the view), link previews, uptime
 * and speed checks, headless browsers and plain HTTP libraries.
 */
const BOT_USER_AGENT =
  /bot|crawl|spider|slurp|archiver|facebookexternalhit|embedly|preview|headless|lighthouse|pagespeed|gtmetrix|pingdom|uptime|curl|wget|python|java\/|go-http|okhttp|axios|node-fetch|undici|libwww|httpclient|scrapy|phantomjs|puppeteer|playwright|selenium/i;

/**
 * True when a view from this User-Agent must not be counted. A missing one
 * counts as a bot: every browser sends one.
 */
export function isBotUserAgent(userAgent: string | null | undefined): boolean {
  return !userAgent || BOT_USER_AGENT.test(userAgent);
}
