import { ParkWithAttractionsDto } from "./park-with-attractions.dto";
import { Park } from "../entities/park.entity";

/**
 * `scheduleCoverage` on the park payload — the window inside which a future `status` is a
 * statement about the park rather than about our sync.
 *
 * `fromEntity` runs on paths that never reach `ParkIntegrationService`, so its default is not
 * cosmetic: it is what a caller reads when nothing filled the field in. `{ from: null, to: null }`
 * is the same answer the field carries for a park that genuinely has no OPERATING rows, so an
 * unfilled payload degrades to "no published schedule" — which is safe, because a consumer that
 * finds no window stops rather than publishing a year of inferred closures.
 *
 * The dangerous default would be a window: today→+12 months, or the range of whatever days
 * happened to be in hand. That reads as a promise the data does not make, and it would be right
 * in development and wrong in production, which is the failure mode this field was added to end.
 */
describe("ParkWithAttractionsDto.fromEntity › scheduleCoverage", () => {
  const park = {
    id: "park-1",
    name: "Phantasialand",
    slug: "phantasialand",
    timezone: "Europe/Berlin",
    attractions: [],
    shows: [],
    restaurants: [],
  } as unknown as Park;

  it("defaults to a null window rather than inventing one", () => {
    const dto = ParkWithAttractionsDto.fromEntity(park);
    expect(dto.scheduleCoverage).toEqual({ from: null, to: null });
  });

  it("defaults alongside hasOperatingSchedule: false, so the two never disagree", () => {
    // Both describe the same park — one as a boolean, one as a window. A payload claiming no
    // official hours while advertising a schedule window would be incoherent.
    const dto = ParkWithAttractionsDto.fromEntity(park);
    expect(dto.hasOperatingSchedule).toBe(false);
    expect(dto.scheduleCoverage.to).toBeNull();
  });
});
