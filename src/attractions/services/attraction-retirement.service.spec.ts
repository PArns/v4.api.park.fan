import {
  AttractionRetirementService,
  RECLASSIFIED_UPSTREAM_REASON,
  retiredKindOf,
} from "./attraction-retirement.service";

/**
 * A demolished ride is neither "closed today" nor "unknown" — both describe a
 * state it could come back from. Disney's Dino-Sue was torn down with DinoLand
 * in February 2026 and sat in Animal Kingdom's attraction list regardless,
 * because the API had no way to say the third thing.
 */
describe("AttractionRetirementService", () => {
  const build = () => {
    const findOne = jest.fn();
    const update = jest.fn().mockResolvedValue({});
    const find = jest.fn().mockResolvedValue([]);
    const redis = { del: jest.fn(), keys: jest.fn().mockResolvedValue([]) };
    const revalidateTags = jest.fn().mockResolvedValue(undefined);

    const service = new AttractionRetirementService(
      { findOne, update, find } as never,
      redis as never,
      { revalidateTags } as never,
    );
    return { service, findOne, update, revalidateTags };
  };

  it("records the date and the evidence together", async () => {
    const { service, findOne, update } = build();
    findOne.mockResolvedValue({ id: "a1", name: "Dino-Sue", parkId: "p1" });

    const [result] = await service.retire([
      {
        attractionId: "a1",
        retiredAt: "2026-02-15",
        reason:
          "Removed with DinoLand — blogmickey.com/2026/02/dino-sue-removed",
      },
    ]);

    expect(update).toHaveBeenCalledWith("a1", {
      retiredAt: new Date("2026-02-15"),
      // A retirement is a claim about the world, so it travels with its source.
      retiredReason: expect.stringContaining("blogmickey.com"),
    });
    expect(result.name).toBe("Dino-Sue");
  });

  it("revalidates the frontend, because the sitemap keeps advertising the slug", async () => {
    const { service, findOne, revalidateTags } = build();
    findOne.mockResolvedValue({ id: "a1", name: "Dino-Sue", parkId: "p1" });

    await service.retire([
      { attractionId: "a1", retiredAt: "2026-02-15", reason: "gone" },
    ]);

    // The park page deduplicates at read time; the sitemap does not.
    expect(revalidateTags).toHaveBeenCalledWith([
      "geo",
      "parks",
      "attractions",
    ]);
  });

  it("does not revalidate when nothing was retired", async () => {
    const { service, findOne, revalidateTags } = build();
    findOne.mockResolvedValue(null);

    await service.retire([
      { attractionId: "missing", retiredAt: "2026-02-15", reason: "gone" },
    ]);

    expect(revalidateTags).not.toHaveBeenCalled();
  });

  it("can be undone, because a claim about the world can be wrong", async () => {
    const { service, findOne, update } = build();
    findOne.mockResolvedValue({
      id: "a1",
      name: "Dino-Sue",
      parkId: "p1",
      retiredAt: new Date("2026-02-15"),
    });

    expect(await service.unretire("a1")).toBe(true);
    expect(update).toHaveBeenCalledWith("a1", {
      retiredAt: null,
      retiredReason: null,
      // A ride that comes back must not stay hidden by a leftover flag.
      retiredHidden: false,
    });
  });

  it("hides a closed ride on its park page and tells the frontend", async () => {
    const { service, findOne, update, revalidateTags } = build();
    findOne.mockResolvedValue({
      id: "a1",
      name: "X2",
      parkId: "p1",
      retiredAt: new Date("2026-07-13T00:00:00Z"),
    });

    expect(await service.setRetiredHidden("a1", true)).toBe(true);
    expect(update).toHaveBeenCalledWith("a1", { retiredHidden: true });
    expect(revalidateTags).toHaveBeenCalledWith(["parks"]);
  });

  it("refuses to hide a ride that is not retired", async () => {
    const { service, findOne, update } = build();
    findOne.mockResolvedValue({ id: "a1", name: "Taron", retiredAt: null });

    expect(await service.setRetiredHidden("a1", true)).toBe(false);
    expect(update).not.toHaveBeenCalled();
  });

  it("reports nothing to undo for an attraction that is not retired", async () => {
    const { service, findOne, update } = build();
    findOne.mockResolvedValue({ id: "a1", name: "Taron", retiredAt: null });

    expect(await service.unretire("a1")).toBe(false);
    expect(update).not.toHaveBeenCalled();
  });
});

/**
 * The frontend keeps a `closed` ride's page and sitemap entry and prints
 * "closed permanently" on it. A row the children sync retired is a show or a
 * restaurant now, and printing that on it would be a claim nobody made.
 */
describe("retiredKindOf", () => {
  it("is null while the row is not retired", () => {
    expect(retiredKindOf({ retiredAt: null, retiredReason: null })).toBeNull();
  });

  it("is closed for a retirement a human entered", () => {
    expect(
      retiredKindOf({
        retiredAt: new Date("2026-07-13T00:00:00Z"),
        retiredReason:
          "https://park.fan/news/x2-six-flags-magic-mountain-closed",
      }),
    ).toBe("closed");
  });

  it("is closed when a human gave no reason", () => {
    expect(
      retiredKindOf({
        retiredAt: new Date("2026-02-01T00:00:00Z"),
        retiredReason: null,
      }),
    ).toBe("closed");
  });

  it("is reclassified for the children sync's own retirement", () => {
    expect(
      retiredKindOf({
        retiredAt: new Date("2026-09-01T00:00:00Z"),
        retiredReason: RECLASSIFIED_UPSTREAM_REASON,
      }),
    ).toBe("reclassified");
  });
});
