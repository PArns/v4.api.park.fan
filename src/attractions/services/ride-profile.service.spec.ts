import {
  RideProfileService,
  parseTermAttractionSort,
} from "./ride-profile.service";

/**
 * The reverse lookup shipped ordered by park name, which means a term with 151
 * rides opens on whatever park sorts first alphabetically. Ranking has to
 * happen in SQL rather than after the fetch: `LIMIT` is applied before the
 * ordered result is handed back, so sorting the returned page would rank an
 * arbitrary slice and call it the top 3.
 */
describe("parseTermAttractionSort", () => {
  it("defaults to park ordering", () => {
    expect(parseTermAttractionSort(undefined)).toBe("park");
  });

  it("accepts the popularity mode", () => {
    expect(parseTermAttractionSort("popularity")).toBe("popularity");
  });

  it("falls back to park ordering instead of throwing on junk", () => {
    // This is a public endpoint. A typo in a query string should return a
    // sensible list, not a 400.
    expect(parseTermAttractionSort("best")).toBe("park");
  });
});

describe("RideProfileService.findAttractionsByTerm ordering", () => {
  const qb = {
    innerJoin: jest.fn().mockReturnThis(),
    leftJoin: jest.fn().mockReturnThis(),
    select: jest.fn().mockReturnThis(),
    where: jest.fn().mockReturnThis(),
    orderBy: jest.fn().mockReturnThis(),
    addOrderBy: jest.fn().mockReturnThis(),
    limit: jest.fn().mockReturnThis(),
    getRawMany: jest.fn(),
  };
  const repo = { createQueryBuilder: jest.fn(() => qb) };
  const service = new RideProfileService(repo as never, [] as never);

  beforeEach(() => {
    jest.clearAllMocks();
    qb.getRawMany.mockResolvedValue([]);
  });

  it("keeps park ordering as the default", async () => {
    await service.findAttractionsByTerm("launch");

    expect(qb.orderBy).toHaveBeenCalledWith("park.name", "ASC");
    expect(qb.addOrderBy).toHaveBeenCalledWith("attraction.name", "ASC");
  });

  it("ranks by confidence bucket before the P90 value", async () => {
    await service.findAttractionsByTerm("launch", 200, "popularity");

    // The bucket has to be the FIRST ordering term. Ordering by P90 first
    // would let a ride with four samples and a freak 180-minute reading lead
    // a list of well-measured headliners.
    const [firstOrdering] = qb.orderBy.mock.calls[0] as [string];
    expect(firstOrdering).toContain("confidence");
    expect(qb.addOrderBy).toHaveBeenCalledWith(
      'baseline."p90Baseline"',
      "DESC",
      "NULLS LAST",
    );
  });

  it("still breaks ties deterministically when ranking", async () => {
    await service.findAttractionsByTerm("launch", 200, "popularity");

    expect(qb.addOrderBy).toHaveBeenCalledWith("park.name", "ASC");
    expect(qb.addOrderBy).toHaveBeenCalledWith("attraction.name", "ASC");
  });

  it("does not order by baseline columns in park mode", async () => {
    await service.findAttractionsByTerm("launch", 200, "park");

    const ordered = [
      ...qb.orderBy.mock.calls,
      ...qb.addOrderBy.mock.calls,
    ].map(([clause]) => clause as string);
    expect(ordered.some((clause) => clause.includes("baseline"))).toBe(false);
  });

  it("joins the baseline table on the outside", async () => {
    await service.findAttractionsByTerm("launch");

    // An inner join here would drop every ride that has no baseline yet, so
    // the list would disagree with the count endpoint rendered beside it.
    expect(qb.leftJoin).toHaveBeenCalledWith(
      "attraction_p90_baselines",
      "baseline",
      'baseline."attractionId" = profile."attractionId"',
    );
    expect(qb.innerJoin).not.toHaveBeenCalledWith(
      "attraction_p90_baselines",
      expect.anything(),
      expect.anything(),
    );
  });
});
