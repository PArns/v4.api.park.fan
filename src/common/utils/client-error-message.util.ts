import { HttpException } from "@nestjs/common";

/**
 * The message a 4xx carries for its client, with the `problems` by path a
 * ticket product's refusal lists; null for a 5xx or anything that is no
 * `HttpException`, whose message may hold SQL or a hostname.
 */
export function clientErrorMessage(error: unknown): string | null {
  if (!(error instanceof HttpException) || error.getStatus() >= 500) {
    return null;
  }
  const body = error.getResponse();
  if (typeof body === "string") return body;
  const { message, problems } = body as {
    message?: unknown;
    problems?: Array<{ path?: unknown; message?: unknown }>;
  };
  const head = Array.isArray(message)
    ? message.join("; ")
    : typeof message === "string"
      ? message
      : error.message;
  if (!Array.isArray(problems) || problems.length === 0) return head;
  return `${head}: ${problems
    .map((problem) => `${String(problem.path)}: ${String(problem.message)}`)
    .join("; ")}`;
}
