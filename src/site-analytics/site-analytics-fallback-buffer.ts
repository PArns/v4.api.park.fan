/**
 * The counts one instance holds while Redis cannot take them, per flush
 * window, up to a fixed number of fields. The collector writes them to
 * Postgres once a minute; what does not fit is refused, and the caller counts
 * it as dropped. So a Redis outage costs Postgres one small transaction per
 * window and minute, whatever the traffic, instead of one per request.
 */
export class FallbackBuffer {
  private readonly windows = new Map<string, Map<string, number>>();
  private fields = 0;

  constructor(private readonly maxFields: number) {}

  /** The fields held, over all windows. */
  get size(): number {
    return this.fields;
  }

  /**
   * Adds one request's counts to its window, all or nothing: false, and
   * nothing added, when they would not fit.
   */
  add(window: string, increments: ReadonlyMap<string, number>): boolean {
    const held = this.windows.get(window) ?? new Map<string, number>();
    let fresh = 0;
    for (const field of increments.keys()) if (!held.has(field)) fresh++;
    if (this.fields + fresh > this.maxFields) return false;
    for (const [field, count] of increments) {
      held.set(field, (held.get(field) ?? 0) + count);
    }
    this.windows.set(window, held);
    this.fields += fresh;
    return true;
  }

  /** Every window held, emptied out of the buffer. */
  take(): Array<[string, Map<string, number>]> {
    const taken = [...this.windows.entries()];
    this.windows.clear();
    this.fields = 0;
    return taken;
  }
}
