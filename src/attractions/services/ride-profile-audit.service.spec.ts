import axios from "axios";
import { RideProfileAuditService } from "./ride-profile-audit.service";

jest.mock("axios");
const mockedGet = axios.get as jest.Mock;

/**
 * The check that replaced the seed's CI validation.
 *
 * Its whole value is naming ids that resolve to nothing, because at runtime
 * they are invisible: the ride page drops an unknown term rather than
 * rendering a dead link, so a renamed glossary term silently shortens a
 * layout. These tests pin the two things that make the report trustworthy —
 * it finds the broken ids, and it refuses to guess when the glossary side is
 * unavailable.
 */
describe("RideProfileAuditService", () => {
  const rideProfiles = {
    findDistinctTermIds: jest.fn(),
    findRidesUsingTermIds: jest.fn(),
  };
  const service = () => new RideProfileAuditService(rideProfiles as never);

  beforeEach(() => {
    jest.clearAllMocks();
    rideProfiles.findRidesUsingTermIds.mockResolvedValue([]);
  });

  it("reports nothing when every stored id resolves", async () => {
    mockedGet.mockResolvedValue({
      data: { count: 3, ids: ["cobra-roll", "lifthill", "airtime"] },
    });
    rideProfiles.findDistinctTermIds.mockResolvedValue([
      "cobra-roll",
      "lifthill",
    ]);

    const result = await service().audit();

    expect(result.broken).toEqual([]);
    expect(result.storedTermIds).toBe(2);
    expect(result.glossaryTermIds).toBe(3);
    // "airtime" is a concept no layout names — unused is normal, not a fault.
    expect(result.unusedGlossaryTermIds).toBe(1);
  });

  it("names the broken id and the rides it shortens", async () => {
    mockedGet.mockResolvedValue({ data: { count: 1, ids: ["lifthill"] } });
    rideProfiles.findDistinctTermIds.mockResolvedValue([
      "lifthill",
      "jojo-rol",
    ]);
    rideProfiles.findRidesUsingTermIds.mockResolvedValue([
      { termId: "jojo-rol", parkSlug: "dorney-park", attractionSlug: "hydra" },
    ]);

    const result = await service().audit();

    expect(rideProfiles.findRidesUsingTermIds).toHaveBeenCalledWith([
      "jojo-rol",
    ]);
    expect(result.broken).toEqual([
      { termId: "jojo-rol", usedBy: ["dorney-park/hydra"] },
    ]);
  });

  it("throws instead of reporting the whole curation broken", async () => {
    // A frontend blip that answers with an empty list would otherwise mark
    // every stored id as dead — a false alarm loud enough to bury a real one.
    mockedGet.mockResolvedValue({ data: { count: 0, ids: [] } });
    rideProfiles.findDistinctTermIds.mockResolvedValue(["lifthill"]);

    await expect(service().audit()).rejects.toThrow(/audit aborted/);
    expect(rideProfiles.findRidesUsingTermIds).not.toHaveBeenCalled();
  });

  it("throws when the response has no id list at all", async () => {
    mockedGet.mockResolvedValue({ data: { unexpected: true } });
    rideProfiles.findDistinctTermIds.mockResolvedValue(["lifthill"]);

    await expect(service().audit()).rejects.toThrow(/audit aborted/);
  });
});
