import { NotFoundException } from "@nestjs/common";
import { ParksController } from "./parks.controller";

/**
 * An attraction slug that stopped answering — a merge removed the loser's
 * `-2` slug, a slug was corrected by hand — used to be a permanent 404
 * (PAR-687). It now redirects when `attraction_slug_aliases` knows where the
 * ride went, and stays a 404 when nothing is recorded.
 */
describe("ParksController — attraction slug aliases", () => {
  const attractionsService = {
    findByGeographicPath: jest.fn(),
    resolveSlugAlias: jest.fn(),
  };
  const attractionIntegrationService = { buildIntegratedResponse: jest.fn() };
  const res = { redirect: jest.fn() };

  const controller = Object.assign(Object.create(ParksController.prototype), {
    attractionsService,
    attractionIntegrationService,
  }) as ParksController;

  const call = (slug: string, days?: number) =>
    controller.getAttractionByGeographicPath(
      "europe",
      "germany",
      "bottrop",
      "movie-park-germany",
      slug,
      res as any,
      days,
    );

  beforeEach(() => {
    jest.clearAllMocks();
    attractionsService.findByGeographicPath.mockResolvedValue(null);
  });

  it("redirects a recorded old slug to the current one, permanently", async () => {
    attractionsService.resolveSlugAlias.mockResolvedValue({
      continentSlug: "europe",
      countrySlug: "germany",
      citySlug: "bottrop",
      parkSlug: "movie-park-germany",
      attractionSlug: "jason-universe",
    });

    const result = await call("jason-universum");

    expect(result).toBeUndefined();
    expect(res.redirect).toHaveBeenCalledWith(
      301,
      "/v1/parks/europe/germany/bottrop/movie-park-germany/attractions/jason-universe",
    );
  });

  it("keeps the requested history window across the redirect", async () => {
    attractionsService.resolveSlugAlias.mockResolvedValue({
      continentSlug: "europe",
      countrySlug: "germany",
      citySlug: "bottrop",
      parkSlug: "movie-park-germany",
      attractionSlug: "jason-universe",
    });

    await call("jason-universum", 90);

    expect(res.redirect).toHaveBeenCalledWith(
      301,
      "/v1/parks/europe/germany/bottrop/movie-park-germany/attractions/jason-universe?days=90",
    );
  });

  it("stays a 404 when no alias is recorded", async () => {
    attractionsService.resolveSlugAlias.mockResolvedValue(null);

    await expect(call("never-existed")).rejects.toBeInstanceOf(
      NotFoundException,
    );
    expect(res.redirect).not.toHaveBeenCalled();
  });

  it("does not look for an alias when the slug answers", async () => {
    attractionsService.findByGeographicPath.mockResolvedValue({ id: "a" });

    await call("jason-universe");

    expect(attractionsService.resolveSlugAlias).not.toHaveBeenCalled();
    expect(
      attractionIntegrationService.buildIntegratedResponse,
    ).toHaveBeenCalledWith({ id: "a" }, 30);
  });
});
