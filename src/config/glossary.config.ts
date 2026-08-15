/**
 * Where the canonical glossary term ids come from.
 *
 * The frontend owns the glossary; this API only stores its term ids on curated
 * ride profiles. Until the ride-profile seed was removed, the id list was
 * mirrored into `glossary-term-ids.ts` and a spec failed CI on an unknown id.
 * With the seed gone there is no checked-in list to check against, so the
 * frontend publishes it instead and the audit fetches it from there.
 *
 * A function rather than a constant so tests can override the env var after
 * import, matching `revalidation.config.ts`.
 */
export function getGlossaryTermIdsUrl(): string {
  return (
    process.env.GLOSSARY_TERM_IDS_URL ||
    "https://park.fan/api/glossary/term-ids"
  );
}
