import {
  MAX_PROFILE_AGE_DAYS,
  toDowntimeBlock,
} from "./downtime-reliability.dto";
import type { AttractionDowntimeProfile } from "../../analytics/entities/attraction-downtime-profile.entity";

const NOW = new Date("2026-09-07T20:00:00Z");

const publishable = (generatedAt: Date): AttractionDowntimeProfile =>
  ({
    attractionId: "a1",
    parkId: "p1",
    publishable: true,
    withheldReason: null,
    windowDays: 90,
    outages: 34,
    observedDays: 61,
    medianMinutes: 25,
    usableDurations: 28,
    longestMinutes: 200,
    downShare: "0.0200",
    windowFrom: "2026-06-09",
    windowTo: "2026-09-07",
    generatedAt,
  }) as AttractionDowntimeProfile;

describe("toDowntimeBlock", () => {
  it("publishes a fresh profile", () => {
    const block = toDowntimeBlock(
      publishable(new Date("2026-09-07T05:00:00Z")),
      NOW,
    );
    expect(block.kind).toBe("figures");
  });

  it("withholds a profile older than the job's own cadence", () => {
    // The job runs nightly, so three days is three missed runs. Without this the
    // numbers keep being served as "the last 90 days" from a window that has
    // moved on — a dead job publishing its last good night forever, which is
    // the recovery-curve failure one layer out and facing a reader.
    const old = new Date(
      NOW.getTime() - (MAX_PROFILE_AGE_DAYS + 1) * 86_400_000,
    );
    const block = toDowntimeBlock(publishable(old), NOW);
    expect(block.kind).toBe("withheld");
    // NOT thin_events: that reason keeps the count and renders „34 Störungen
    // gemeldet … für eine belastbare Zahl zu wenige", refuted by its own
    // number. Since the event floor is 24, every stale publishable ride hit it.
    expect(block).toMatchObject({ reason: "stale_data", outages: 34 });
  });

  it("withholds when the profile carries no generation time at all", () => {
    const block = toDowntimeBlock(
      {
        ...publishable(NOW),
        generatedAt: null,
      } as unknown as AttractionDowntimeProfile,
      NOW,
    );
    expect(block.kind).toBe("withheld");
  });

  it("still says not_down_capable when there is no profile row", () => {
    expect(toDowntimeBlock(null, NOW)).toMatchObject({
      kind: "withheld",
      reason: "not_down_capable",
    });
  });

  it("a permanent reason outranks staleness", () => {
    // A blind park whose nightly job stalls would otherwise read „diese Zahlen
    // sind nicht aktuell" — promising a resolution that cannot arrive, because
    // its source has no DOWN status and never will. Blind, artefact and
    // no-schedule parks outnumber near-miss publishable rides by far, so the
    // wrong branch was also the common one.
    const old = new Date(
      NOW.getTime() - (MAX_PROFILE_AGE_DAYS + 3) * 86_400_000,
    );
    const blind = {
      ...publishable(old),
      publishable: false,
      withheldReason: "park_never_reports",
    } as AttractionDowntimeProfile;
    expect(toDowntimeBlock(blind, NOW)).toMatchObject({
      reason: "park_never_reports",
    });
  });

  it("still reports staleness for a ride that was genuinely publishable", () => {
    const old = new Date(
      NOW.getTime() - (MAX_PROFILE_AGE_DAYS + 1) * 86_400_000,
    );
    expect(toDowntimeBlock(publishable(old), NOW)).toMatchObject({
      reason: "stale_data",
    });
  });
});
