/**
 * Base URLs of the Python ML services. Single source of truth for the
 * env-var fallbacks, which were previously repeated in 7 files.
 *
 * Functions (not constants) so tests can override the env vars after import.
 */
export function getMlServiceUrl(): string {
  return process.env.ML_SERVICE_URL || "http://ml-service:8000";
}

export function getNfServiceUrl(): string {
  return process.env.NF_SERVICE_URL || "http://nf-service:8000";
}

export function getPcnServiceUrl(): string {
  return process.env.PCN_SERVICE_URL || "http://pcn-service:8000";
}
