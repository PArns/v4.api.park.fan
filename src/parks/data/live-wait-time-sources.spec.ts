import {
  PARKS_WITHOUT_LIVE_WAIT_TIMES,
  getNoLiveWaitTimesReason,
} from "./live-wait-time-sources";
import { buildLiveWaitTimes } from "../dto/live-wait-times.dto";

describe("live wait time sources", () => {
  describe("getNoLiveWaitTimesReason", () => {
    it("finds a curated park by city + park slug", () => {
      expect(getNoLiveWaitTimesReason("sierksdorf", "hansa-park")).toBe(
        "in_park_app_only",
      );
    });

    it("returns null for a park with a readable source", () => {
      expect(getNoLiveWaitTimesReason("rust", "europa-park")).toBeNull();
    });

    it("does not match the park slug in the wrong city", () => {
      // The keying exists because park slugs are not globally unique — a
      // second "hansa-park" elsewhere would be a different park.
      expect(getNoLiveWaitTimesReason("orlando", "hansa-park")).toBeNull();
    });

    it("ignores slug casing", () => {
      expect(getNoLiveWaitTimesReason("Sierksdorf", "Hansa-Park")).toBe(
        "in_park_app_only",
      );
    });

    it("reads a park with no city slug as readable", () => {
      // Geocoding may never have resolved a city. Guessing "unreadable" here
      // would tell visitors of a healthy park that we have no data for it.
      expect(getNoLiveWaitTimesReason(null, "hansa-park")).toBeNull();
      expect(getNoLiveWaitTimesReason(undefined, undefined)).toBeNull();
    });
  });

  describe("buildLiveWaitTimes", () => {
    it("carries the reason when there is one", () => {
      expect(buildLiveWaitTimes("in_park_app_only")).toEqual({
        available: false,
        reason: "in_park_app_only",
      });
    });

    it("reports available with a null reason otherwise", () => {
      expect(buildLiveWaitTimes(null)).toEqual({
        available: true,
        reason: null,
      });
    });
  });

  describe("the list itself", () => {
    it("holds no duplicate city+park keys", () => {
      const keys = PARKS_WITHOUT_LIVE_WAIT_TIMES.map(
        (e) => `${e.citySlug}/${e.parkSlug}`,
      );
      expect(new Set(keys).size).toBe(keys.length);
    });

    it("explains every entry", () => {
      // The note is what lets a future reader re-check an entry instead of
      // trusting it. An entry without one is an unfalsifiable claim.
      for (const entry of PARKS_WITHOUT_LIVE_WAIT_TIMES) {
        expect(entry.note.length).toBeGreaterThan(20);
      }
    });
  });
});
