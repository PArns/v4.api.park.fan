import { BadRequestException } from "@nestjs/common";
import { AttractionReviewService } from "./attraction-review.service";

const RIDE_A = "00000000-0000-4000-8000-00000000000a";
const RIDE_B = "00000000-0000-4000-8000-00000000000b";

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
          attractionId: RIDE_B,
          otherAttractionId: RIDE_A,
          reason: "different rides",
        },
      ]);

      const insert = query.mock.calls.find(([sql]) =>
        /INSERT/.test(sql as string),
      );
      expect(insert?.[1]).toEqual(
        expect.arrayContaining(["not_a_duplicate", RIDE_A, RIDE_B]),
      );
    });

    it("is idempotent without relying on ON CONFLICT", async () => {
      // A single-attraction mark leaves other_attraction_id NULL, and Postgres
      // treats NULLs in a unique index as distinct — so a conflict target would
      // never fire for exactly the marks that need it.
      const { service, query } = build();

      await service.record([
        {
          kind: "not_retired",
          attractionId: RIDE_A,
          reason: "still operating",
        },
      ]);

      const [deleteSql] = query.mock.calls[0] as [string];
      expect(deleteSql).toMatch(/DELETE FROM attraction_review_marks/);
      expect(deleteSql).toMatch(/IS NOT DISTINCT FROM/);
    });

    it("keeps a null second id for a single-attraction mark", async () => {
      const { service, query } = build();

      await service.record([
        {
          kind: "not_retired",
          attractionId: RIDE_A,
          reason: "still operating",
        },
      ]);

      const insert = query.mock.calls.find(([sql]) =>
        /INSERT/.test(sql as string),
      );
      expect((insert?.[1] as unknown[])[2]).toBeNull();
    });

    it("stores no_kind_fits as a mark on one ride", async () => {
      const { service, query } = build();

      await expect(
        service.record([
          {
            kind: "no_kind_fits",
            attractionId: RIDE_A,
            reason: "A wave pool; none of the kinds fits",
          },
        ]),
      ).resolves.toBe(1);

      const insert = query.mock.calls.find(([sql]) =>
        /INSERT/.test(sql as string),
      );
      expect(insert?.[1]).toEqual([
        "no_kind_fits",
        RIDE_A,
        null,
        "A wave pool; none of the kinds fits",
        null,
      ]);
    });

    it.each([
      [
        "an unknown kind",
        { kind: "no_kind", attractionId: RIDE_A, reason: "a pool" },
        /kind "no_kind" is not one of .*no_kind_fits/,
      ],
      [
        "an id that is not a uuid",
        { kind: "no_kind_fits", attractionId: "a1", reason: "a pool" },
        /attractionId must be a lowercase uuid/,
      ],
      [
        "a second ride on a single-ride mark",
        {
          kind: "no_kind_fits",
          attractionId: RIDE_A,
          otherAttractionId: RIDE_B,
          reason: "a pool",
        },
        /otherAttractionId belongs to not_a_duplicate only/,
      ],
      [
        "a pair mark without its second ride",
        { kind: "not_a_duplicate", attractionId: RIDE_A, reason: "two rides" },
        /not_a_duplicate needs otherAttractionId/,
      ],
      [
        "a pair mark naming one ride twice",
        {
          kind: "not_a_duplicate",
          attractionId: RIDE_A,
          otherAttractionId: RIDE_A,
          reason: "two rides",
        },
        /not_a_duplicate needs otherAttractionId/,
      ],
      [
        "a blank reason",
        { kind: "no_kind_fits", attractionId: RIDE_A, reason: "  " },
        /reason is required/,
      ],
      [
        "a recheck date that is not a day",
        {
          kind: "not_retired",
          attractionId: RIDE_A,
          reason: "still runs",
          recheckAfter: "2027-02-31",
        },
        /recheckAfter must be a date/,
      ],
    ])(
      "refuses %s with a 400 and writes no mark of the list",
      async (_case, bad, message) => {
        const { service, query } = build();

        const recorded = service.record([
          { kind: "not_retired", attractionId: RIDE_B, reason: "still runs" },
          bad as never,
        ]);

        await expect(recorded).rejects.toBeInstanceOf(BadRequestException);
        await expect(recorded).rejects.toThrow(message);
        await expect(recorded).rejects.toThrow(/^marks\[1\]: /);
        expect(query).not.toHaveBeenCalled();
      },
    );

    it("refuses a body whose marks are not a list", async () => {
      const { service, query } = build();

      await expect(
        service.record({ kind: "no_kind_fits" } as never),
      ).rejects.toThrow("marks must be a list");
      expect(query).not.toHaveBeenCalled();
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
