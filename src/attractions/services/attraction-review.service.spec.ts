import { AttractionReviewService } from "./attraction-review.service";

/**
 * Both detectors are behavioural: they describe what the feed is doing, not
 * what is true. So a candidate returns tomorrow however carefully it was
 * investigated. A research round on 2026-08-16 established that 57 of 73
 * silenced attractions are alive and that 5 duplicate pairs are genuinely
 * different rides — none of which the detectors can remember.
 */
describe("AttractionReviewService", () => {
  const build = () => {
    const query = jest.fn().mockResolvedValue([{ id: "m1" }]);
    return {
      service: new AttractionReviewService({} as never, { query } as never),
      query,
    };
  };

  describe("record", () => {
    it("stores a pair in a canonical order, whichever way it is given", async () => {
      // "A is not a duplicate of B" is the same statement as its mirror.
      // Storing both would let the pair surface twice — the bug that made the
      // detector count 63 rows for 53 real pairs.
      const { service, query } = build();

      await service.record([
        {
          kind: "not_a_duplicate",
          attractionId: "bbb",
          otherAttractionId: "aaa",
          reason: "different rides",
        },
      ]);

      const insert = query.mock.calls.find(([sql]) =>
        /INSERT/.test(sql as string),
      );
      expect(insert?.[1]).toEqual(
        expect.arrayContaining(["not_a_duplicate", "aaa", "bbb"]),
      );
    });

    it("is idempotent without relying on ON CONFLICT", async () => {
      // A single-attraction mark leaves other_attraction_id NULL, and Postgres
      // treats NULLs in a unique index as distinct — so a conflict target would
      // never fire for exactly the marks that need it.
      const { service, query } = build();

      await service.record([
        { kind: "not_retired", attractionId: "a1", reason: "still operating" },
      ]);

      const [deleteSql] = query.mock.calls[0] as [string];
      expect(deleteSql).toMatch(/DELETE FROM attraction_review_marks/);
      expect(deleteSql).toMatch(/IS NOT DISTINCT FROM/);
    });

    it("keeps a null second id for a single-attraction mark", async () => {
      const { service, query } = build();

      await service.record([
        { kind: "not_retired", attractionId: "a1", reason: "still operating" },
      ]);

      const insert = query.mock.calls.find(([sql]) =>
        /INSERT/.test(sql as string),
      );
      expect((insert?.[1] as unknown[])[2]).toBeNull();
    });
  });

  describe("findRetirementCandidates", () => {
    it("hides candidates a human has cleared, until their recheck date", async () => {
      const query = jest.fn().mockResolvedValue([]);
      const service = new AttractionReviewService(
        {} as never,
        { query } as never,
      );

      await service.findRetirementCandidates();
      const [sql] = query.mock.calls[0] as [string];

      expect(sql).toMatch(/kind = 'not_retired'/);
      // A verdict may expire: Shock Wave has stood unused since March 2026 with
      // no announcement either way, and a permanent mark would hide its
      // eventual retirement forever.
      expect(sql).toMatch(
        /recheck_after IS NULL OR m\.recheck_after > CURRENT_DATE/,
      );
      expect(sql).toMatch(/m\.id IS NULL/);
    });

    it("still excludes whole-park blocks that fell silent on one day", async () => {
      const query = jest.fn().mockResolvedValue([]);
      const service = new AttractionReviewService(
        {} as never,
        { query } as never,
      );

      await service.findRetirementCandidates();
      const [sql] = query.mock.calls[0] as [string];

      // Wet'n'Wild's 13+9 on 2026-06-29 is the Southern-Hemisphere winter, not
      // 22 retirements.
      expect(sql).toMatch(/s\.n <= 2/);
      expect(sql).toMatch(/retired_at IS NULL/);
    });
  });
});
