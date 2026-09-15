import { HttpStatus } from "@nestjs/common";
import type { ApiResponseOptions } from "@nestjs/swagger";

/**
 * The 429 every hand-rolled limiter in this API answers with.
 *
 * One object rather than seven copies of the same paragraph, and it declares
 * `Retry-After` as a **header** instead of only mentioning it in prose: the
 * generated OpenAPI document is what an agent or a code generator reads, and a
 * header named in a description sentence is not part of the contract they see.
 *
 * `HttpExceptionFilter` sets the header and carries the body field; both come
 * from the same figure, so they cannot disagree. The body field is the exact
 * one the limiter counted, the header is that value rounded up to whole
 * seconds because `Retry-After` has no sub-second form.
 */
export const RATE_LIMITED_RESPONSE: ApiResponseOptions = {
  status: HttpStatus.TOO_MANY_REQUESTS,
  description:
    "Too many writes from this address. The body carries `retryAfterSeconds` " +
    "(the exact figure) and the response a `Retry-After` header with the same " +
    "wait rounded up to whole seconds.",
  headers: {
    "Retry-After": {
      description:
        "How long to wait before retrying, in whole seconds (RFC 9110 " +
        "§10.2.3). Not readable from a browser on a cross-origin call — " +
        "`Retry-After` is not CORS-safelisted — so read `retryAfterSeconds` " +
        "from the body there.",
      schema: { type: "string" },
    },
  },
};
