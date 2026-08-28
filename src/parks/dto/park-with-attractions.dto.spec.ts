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

/**
 * The fast-pass product on the park's attraction list.
 *
 * The name lives on the park row and the flag on the ride, so this mapper is
 * one of the two places that has to bring the halves together — and the only
 * one where the park is the object being mapped rather than a relation hanging
 * off the attraction. It got its own test because that difference is exactly
 * the kind of thing a copy-paste of the attraction mapper gets wrong.
 */
describe("ParkWithAttractionsDto.fromEntity › fastPass", () => {
  const parkWith = (attractions: unknown[], overrides = {}) =>
    ({
      id: "park-1",
      name: "Phantasialand",
      slug: "phantasialand",
      timezone: "Europe/Berlin",
      curatedFastPassName: "QuickPass",
      curatedCurrency: "EUR",
      curatedFastPassTermId: "quick-pass",
      attractions,
      shows: [],
      restaurants: [],
      ...overrides,
    }) as unknown as Park;

  const ride = (overrides = {}) => ({
    id: "ride-1",
    name: "Taron",
    slug: "taron",
    ...overrides,
  });

  it("gives the ride the park's brand and its own price", () => {
    const dto = ParkWithAttractionsDto.fromEntity(
      parkWith([ride({ hasFastPass: true, fastPassPrice: 12 })]),
    );
    expect(dto.attractions[0]!.fastPass).toEqual({
      name: "QuickPass",
      price: 12,
      priceFrom: null,
      currency: "EUR",
      termId: "quick-pass",
    });
  });

  it("says nothing about a ride nobody has checked", () => {
    const dto = ParkWithAttractionsDto.fromEntity(parkWith([ride()]));
    expect(dto.attractions[0]!.fastPass).toBeNull();
  });

  it("says nothing about a ride checked and found to have none either", () => {
    // Two different facts to an editor, one absence to a visitor.
    const dto = ParkWithAttractionsDto.fromEntity(
      parkWith([ride({ hasFastPass: false })]),
    );
    expect(dto.attractions[0]!.fastPass).toBeNull();
  });

  it("withholds the price when the park never got a currency", () => {
    const dto = ParkWithAttractionsDto.fromEntity(
      parkWith([ride({ hasFastPass: true, fastPassPrice: 12 })], {
        curatedCurrency: null,
      }),
    );
    expect(dto.attractions[0]!.fastPass).toEqual({
      name: "QuickPass",
      price: null,
      priceFrom: null,
      currency: null,
      termId: "quick-pass",
    });
  });

  it("carries the park's product name into its info block", () => {
    const dto = ParkWithAttractionsDto.fromEntity(parkWith([]));
    expect(dto.info?.fastPassName).toBe("QuickPass");
    expect(dto.info?.currency).toBe("EUR");
    expect(dto.info?.fastPassTermId).toBe("quick-pass");
  });
});
