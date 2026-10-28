import type { NextFunction, Request, Response } from "express";

/** A collect body of 20 hits is under 8 KiB; this is twice that. */
export const COLLECT_BODY_LIMIT_BYTES = 16 * 1024;

/** Set on a request whose text body was larger than the limit. */
export const TEXT_BODY_TOO_LARGE = Symbol("textBodyTooLarge");

/**
 * Reads a `text/plain` body into `req.body` as a string, on the route it is
 * applied to only. `navigator.sendBeacon` sends a string as `text/plain`,
 * which is what spares the browser a CORS preflight; Nest's own parsers read
 * JSON and forms, and every other route keeps refusing text.
 *
 * Hand-written because the body parser Nest ships is not a dependency of this
 * package, and reading a stream into a capped string is all this needs. A
 * declared length over the limit is not read at all; an undeclared one is
 * read until it passes the limit and the connection is then dropped.
 */
export function textBodyMiddleware(limit = COLLECT_BODY_LIMIT_BYTES) {
  return (req: Request, _res: Response, next: NextFunction): void => {
    const type = (req.headers["content-type"] ?? "").toLowerCase();
    if (!type.startsWith("text/plain") || req.readableEnded) return next();

    const declared = Number(req.headers["content-length"] ?? NaN);
    if (declared > limit) {
      (req as Request & { [TEXT_BODY_TOO_LARGE]?: true })[TEXT_BODY_TOO_LARGE] =
        true;
      return next();
    }

    const chunks: Buffer[] = [];
    let size = 0;
    let done = false;
    req.on("data", (chunk: Buffer) => {
      size += chunk.length;
      if (size > limit) {
        done = true;
        req.destroy();
        return;
      }
      chunks.push(chunk);
    });
    req.on("end", () => {
      if (done) return;
      done = true;
      req.body = Buffer.concat(chunks).toString("utf8");
      next();
    });
    req.on("error", (error) => {
      if (done) return;
      done = true;
      next(error);
    });
  };
}
