import {
  attractionLastSeenKey,
  lastObserved,
  readAttractionLastSeen,
} from "./attraction-last-seen.util";

const at = (iso: string) => new Date(iso);

describe("lastObserved", () => {
  const row = {
    lastUpdated: at("2026-11-25T13:10:00Z"),
    timestamp: at("2026-11-25T13:10:05Z"),
  };

  it("is the last read when the value did not change since", () => {
    const seen = at("2026-11-25T18:14:00Z").getTime();
    expect(lastObserved(row, seen)).toBe("2026-11-25T18:14:00.000Z");
  });

  it("is not refreshed for a source that stopped answering (old last-seen)", () => {
    const seen = at("2026-11-25T15:00:00Z").getTime();
    expect(lastObserved(row, seen)).toBe("2026-11-25T15:00:00.000Z");
  });

  it("falls back to lastUpdated, never earlier, without a last-seen", () => {
    expect(lastObserved(row, undefined)).toBe("2026-11-25T13:10:00.000Z");
    expect(lastObserved(row, at("2026-11-25T12:00:00Z").getTime())).toBe(
      "2026-11-25T13:10:00.000Z",
    );
  });

  it("does not take a heartbeat's lastUpdated for a reading", () => {
    const hb = {
      lastUpdated: at("2026-11-25T18:00:00Z"),
      timestamp: at("2026-11-25T18:00:00Z"),
      isHeartbeat: true,
    };
    expect(lastObserved(hb, undefined)).toBeNull();
    expect(lastObserved(hb, at("2026-11-25T16:00:00Z").getTime())).toBe(
      "2026-11-25T16:00:00.000Z",
    );
  });
});

describe("readAttractionLastSeen", () => {
  it("maps ids to epoch ms with one MGET and skips missing values", async () => {
    const mget = jest.fn().mockResolvedValue(["1700000000000", null]);
    const m = await readAttractionLastSeen({ mget } as any, ["a", "b"]);
    expect(mget).toHaveBeenCalledWith(
      attractionLastSeenKey("a"),
      attractionLastSeenKey("b"),
    );
    expect([...m]).toEqual([["a", 1700000000000]]);
  });

  it("is empty when Redis fails", async () => {
    const mget = jest.fn().mockRejectedValue(new Error("down"));
    expect((await readAttractionLastSeen({ mget } as any, ["a"])).size).toBe(0);
  });
});
