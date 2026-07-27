import { MANUAL_ATTRACTION_METADATA } from "./manual-attraction-metadata";

/**
 * `mayGetWet` comes straight from ThemeParks.wiki and is populated for only
 * 11 of ~7000 attractions, so a wrong entry stands out badly on a ride page.
 *
 * All 11 were checked against the parks' own attraction pages. Ten are right —
 * including Efteling's Symbolica, which looks absurd for a trackless dark ride
 * until you read Efteling's own page, where "You may get wet" is listed under
 * Sensory stimuli for the Botanicum whale scene.
 *
 * The one that is wrong is Genting SkyWorlds' Terraform Tower Challenge: an
 * S&S shot tower in the space-themed Andromeda Base, whose official page lists
 * "Thrill, Big Drops, Scary, Outdoor" and no water at all.
 */
describe("MANUAL_ATTRACTION_METADATA", () => {
  it("corrects the Terraform Tower Challenge wet flag", () => {
    const entry = MANUAL_ATTRACTION_METADATA.find(
      (e) => e.attractionSlug === "terraform-tower-challenge",
    );

    expect(entry?.mayGetWet).toBe(false);
  });

  it("does not contradict a park that warns about water itself", () => {
    // Symbolica and Archipel are correctly flagged upstream — a curated
    // override for either would be wrong, not a cleanup.
    for (const slug of ["symbolica", "archipel"]) {
      const entry = MANUAL_ATTRACTION_METADATA.find(
        (e) => e.attractionSlug === slug,
      );
      expect(entry?.mayGetWet).toBeUndefined();
    }
  });

  it("keys every entry by city, park and attraction", () => {
    for (const entry of MANUAL_ATTRACTION_METADATA) {
      expect(entry.citySlug).toBeTruthy();
      expect(entry.parkSlug).toBeTruthy();
      expect(entry.attractionSlug).toBeTruthy();
    }
  });
});
