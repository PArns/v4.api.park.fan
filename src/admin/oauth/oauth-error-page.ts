import type { Response } from "express";

/**
 * The page `/v1/oauth/authorize` shows when it cannot trust the redirect URI:
 * an unknown client, or a redirect URI the client never registered. Sending
 * the error back to that URI would turn the endpoint into an open redirect,
 * so the browser stays here.
 *
 * Nothing from the request is echoed: the messages are fixed strings chosen
 * by the caller of this function, so there is nothing to escape.
 */
const CONTENT_SECURITY_POLICY = [
  "default-src 'none'",
  "style-src 'unsafe-inline'",
  "base-uri 'none'",
  "form-action 'none'",
  "frame-ancestors 'none'",
].join("; ");

export type AuthorizeErrorMessage =
  "unknown-client" | "invalid-redirect" | "invalid-request" | "rate-limited";

const MESSAGES: Record<AuthorizeErrorMessage, string> = {
  "unknown-client":
    "This application is not registered with park.fan. Start the connection again from the application.",
  "invalid-redirect":
    "This application asked to be sent somewhere it never registered. Nothing was authorized.",
  "invalid-request":
    "The authorization request is incomplete or malformed. Start the connection again from the application.",
  "rate-limited":
    "Too many authorization requests from this network. Wait a few minutes and start the connection again.",
};

export function sendAuthorizeErrorPage(
  response: Response,
  message: AuthorizeErrorMessage,
): void {
  response
    .status(message === "rate-limited" ? 429 : 400)
    .setHeader("Content-Type", "text/html; charset=utf-8")
    .setHeader("Content-Security-Policy", CONTENT_SECURITY_POLICY)
    .setHeader("Cache-Control", "no-store")
    .setHeader("Referrer-Policy", "no-referrer")
    .send(
      `<!doctype html><html lang="en"><head><meta charset="utf-8">` +
        `<meta name="viewport" content="width=device-width, initial-scale=1">` +
        `<meta name="robots" content="noindex">` +
        `<title>Authorization failed · park.fan</title>` +
        `<style>body{font:16px/1.5 system-ui,sans-serif;max-width:32rem;margin:15vh auto;padding:0 1rem;color:#1f2937}` +
        `h1{font-size:1.25rem}@media(prefers-color-scheme:dark){body{background:#111827;color:#e5e7eb}}</style>` +
        `</head><body><h1>Authorization failed</h1><p>${MESSAGES[message]}</p></body></html>`,
    );
}
