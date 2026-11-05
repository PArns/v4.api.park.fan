import type { NextFunction, Request, Response } from "express";

/** A collect body of 20 hits is under 8 KiB; this is twice that. */
export const COLLECT_BODY_LIMIT_BYTES = 16 * 1024;

/** Set on a request whose body was larger than the limit; the route answers 413. */
export const BODY_TOO_LARGE = Symbol("collectBodyTooLarge");

const flagTooLarge = (req: Request) => {
  (req as Request & { [BODY_TOO_LARGE]?: true })[BODY_TOO_LARGE] = true;
};

/**
 * The collect route's body, held to {@link COLLECT_BODY_LIMIT_BYTES} whatever
 * its type, on that route only.
 *
 * - `text/plain` (what `navigator.sendBeacon` sends for a string, which spares
 *   the browser a CORS preflight) is read here into `req.body` as a string;
 *   Nest's own parsers read JSON and forms, and every other route keeps
 *   refusing text. A declared length over the limit is not read at all; an
 *   undeclared one is read until it passes the limit and the connection is
 *   then dropped. Hand-written because the body parser Nest ships is not a
 *   dependency of this package.
 * - `application/json` was parsed before any route middleware, under the
 *   app's own limit; a declared length over this one, or a parsed body that
 *   serialises to more, is flagged all the same.
 */
export function collectBodyMiddleware(limit = COLLECT_BODY_LIMIT_BYTES) {
  return (req: Request, _res: Response, next: NextFunction): void => {
    const type = (req.headers["content-type"] ?? "").toLowerCase();
    const declared = Number(req.headers["content-length"] ?? NaN);

    if (!type.startsWith("text/plain")) {
      if (
        declared > limit ||
        (req.body !== undefined &&
          typeof req.body === "object" &&
          Buffer.byteLength(JSON.stringify(req.body)) > limit)
      ) {
        flagTooLarge(req);
      }
      return next();
    }
    if (req.readableEnded) return next();
    if (declared > limit) {
      flagTooLarge(req);
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
