/**
 * Small in-memory negative cache: remembers keys that resolved to "not found"
 * for a TTL, so hot 404 paths (e.g. crawlers probing geographic URLs) skip
 * the DB query. Shared by ParksService and AttractionsService.
 */
export class NegativeCache {
  private readonly entries = new Map<string, number>();

  constructor(private readonly ttlMs: number = 60 * 60 * 1000) {}

  /** True if the key is a known miss whose TTL has not expired. */
  has(key: string): boolean {
    const expiry = this.entries.get(key);
    if (expiry === undefined) return false;
    if (Date.now() < expiry) return true;
    this.entries.delete(key);
    return false;
  }

  /** Record a miss for the key. */
  add(key: string): void {
    this.entries.set(key, Date.now() + this.ttlMs);
  }
}
