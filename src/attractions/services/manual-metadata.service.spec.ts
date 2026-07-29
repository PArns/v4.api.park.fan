import { ManualMetadataService } from "./manual-metadata.service";

/**
 * Applying the curated seed used to be the last step of the attraction detail
 * sweep, which first pulls ~7000 rate-limited wiki documents. So a one-line
 * seed correction stayed invisible for hours, and the longer the job ran the
 * likelier a deploy killed it mid-flight — which is exactly what happened
 * three times in a row, each time losing the seed application with it.
 *
 * This is pure database work: no upstream calls, seconds not hours.
 */
describe("ManualMetadataService", () => {
  const qb = {
    innerJoin: jest.fn().mockReturnThis(),
    where: jest.fn().mockReturnThis(),
    andWhere: jest.fn().mockReturnThis(),
    select: jest.fn().mockReturnThis(),
    getOne: jest.fn(),
  };
  const repo = { createQueryBuilder: jest.fn(() => qb), update: jest.fn() };
  const attractionsService = { getRepository: () => repo };

  const service = (seed: unknown[]) =>
    new ManualMetadataService(attractionsService as never, seed as never);

  beforeEach(() => {
    jest.clearAllMocks();
    qb.getOne.mockResolvedValue(null);
  });

  it("corrects a wrong wet flag", async () => {
    qb.getOne.mockResolvedValue({
      id: "a1",
      mayGetWet: true,
      minimumHeight: 120,
      rcdbId: null,
    });

    const result = await service([
      {
        citySlug: "genting-highlands",
        parkSlug: "genting-skyworlds-theme-park",
        attractionSlug: "terraform-tower-challenge",
        mayGetWet: false,
      },
    ]).apply();

    expect(repo.update).toHaveBeenCalledWith("a1", { mayGetWet: false });
    expect(result.wet).toBe(1);
  });

  it("fills a missing height and records the published unit", async () => {
    qb.getOne.mockResolvedValue({
      id: "a1",
      mayGetWet: null,
      minimumHeight: null,
      rcdbId: null,
    });

    await service([
      {
        citySlug: "sandusky",
        parkSlug: "cedar-point",
        attractionSlug: "maverick",
        minimumHeightCm: 132,
        minimumHeightUnit: "in",
      },
    ]).apply();

    expect(repo.update).toHaveBeenCalledWith("a1", {
      minimumHeight: 132,
      minimumHeightUnit: "in",
    });
  });

  it("never overwrites a height we already hold", async () => {
    // Curated heights are a fallback; upstream wins when it has one.
    qb.getOne.mockResolvedValue({
      id: "a1",
      mayGetWet: null,
      minimumHeight: 140,
      rcdbId: null,
    });

    await service([
      {
        citySlug: "sandusky",
        parkSlug: "cedar-point",
        attractionSlug: "maverick",
        minimumHeightCm: 132,
      },
    ]).apply();

    expect(repo.update).not.toHaveBeenCalled();
  });

  it("skips an attraction whose slug has drifted", async () => {
    qb.getOne.mockResolvedValue(null);

    const result = await service([
      {
        citySlug: "sandusky",
        parkSlug: "cedar-point",
        attractionSlug: "renamed-since",
        mayGetWet: true,
      },
    ]).apply();

    expect(repo.update).not.toHaveBeenCalled();
    expect(result.wet).toBe(0);
  });

  it("writes nothing when the row already matches the seed", async () => {
    qb.getOne.mockResolvedValue({
      id: "a1",
      mayGetWet: false,
      minimumHeight: null,
      rcdbId: null,
    });

    await service([
      {
        citySlug: "x",
        parkSlug: "y",
        attractionSlug: "z",
        mayGetWet: false,
      },
    ]).apply();

    expect(repo.update).not.toHaveBeenCalled();
  });

  it("names the parks it wrote into, and only those", async () => {
    // The caller evicts `park:integrated:{parkId}` from this list. A run that
    // changed nothing must report nothing — otherwise every no-op seed run
    // throws away warm park responses for the whole catalogue.
    qb.getOne
      .mockResolvedValueOnce({
        id: "a1",
        parkId: "p1",
        mayGetWet: true,
        minimumHeight: null,
        rcdbId: null,
      })
      .mockResolvedValueOnce({
        id: "a2",
        parkId: "p2",
        mayGetWet: false,
        minimumHeight: null,
        rcdbId: null,
      });

    const result = await service([
      { citySlug: "c", parkSlug: "p", attractionSlug: "a1", mayGetWet: false },
      { citySlug: "c", parkSlug: "q", attractionSlug: "a2", mayGetWet: false },
    ]).apply();

    expect(result.touchedParks).toEqual([
      { parkId: "p1", attractionIds: ["a1"] },
    ]);
  });
});
