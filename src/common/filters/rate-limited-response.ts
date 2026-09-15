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
  // The body gets a schema for the same reason the header did: a field named
  // only in a description sentence is not something a generated client knows
  // about, and `retryAfterSeconds` is the copy a browser has to read.
  schema: {
    type: "object",
    properties: {
      statusCode: { type: "integer", example: 429 },
      message: { type: "string" },
      retryAfterSeconds: {
        type: "number",
        description:
          "The exact wait the limiter counted, in seconds. May be fractional; " +
          "the `Retry-After` header carries the same wait rounded up.",
      },
    },
  },
};

/**
 * The same 429, for a route whose bucket is not the one its neighbours use.
 *
 * `POST /v1/trips` is counted in the create bucket, which is about thirty times
 * tighter than the update bucket a `PUT` or `DELETE` spends
 * (`TripWriteRateLimitService`). That difference was the whole content of this
 * route's old description and is not visible anywhere else in the spec, so it
 * is appended rather than dropped into the shared text — the other six really
 * do say the same thing.
 *
 * Spread rather than mutated: `ApiResponse` writes to the object it is handed.
 */
export const rateLimitedResponseWith = (
  extraSentence: string,
): ApiResponseOptions => ({
  ...RATE_LIMITED_RESPONSE,
  description: `${RATE_LIMITED_RESPONSE.description} ${extraSentence}`,
});
