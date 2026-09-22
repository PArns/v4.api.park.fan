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

/**
 * The curated works period on the park's attraction list.
 *
 * The park payload is the surface that draws the ride cards, so a rebuild that
 * only reaches the ride's own page would be invisible where most people meet
 * the ride. This mapper is the second of the two that has to carry the block,
 * and it is a hand-written copy of the first — which is why it is asserted here
 * rather than trusted to stay in step.
 */
describe("ParkWithAttractionsDto.fromEntity › worksPeriod", () => {
  const parkWith = (attractions: unknown[]) =>
    ({
      id: "park-1",
      name: "Phantasialand",
      slug: "phantasialand",
      timezone: "Europe/Berlin",
      attractions,
      shows: [],
      restaurants: [],
    }) as unknown as Park;

  const ride = (overrides = {}) => ({
    id: "ride-1",
    name: "Chiapas",
    slug: "chiapas",
    ...overrides,
  });

  it("carries the window and the estimate flag onto the card", () => {
    const dto = ParkWithAttractionsDto.fromEntity(
      parkWith([
        ride({
          curatedOutOfServiceFrom: "2026-01-16",
          curatedOutOfServiceTo: "2026-03-03",
          curatedOutOfServiceToUncertain: true,
        }),
      ]),
    );

    expect(dto.attractions[0].worksPeriod).toEqual({
      from: "2026-01-16",
      to: "2026-03-03",
      toUncertain: true,
    });
  });

  it("is null for a ride nobody has curated", () => {
    const dto = ParkWithAttractionsDto.fromEntity(parkWith([ride()]));
    expect(dto.attractions[0].worksPeriod).toBeNull();
  });

  it("carries a window that has no end yet", () => {
    // The usual shape while work is running. `outage` is deliberately NOT
    // asserted beside it: this mapper sets it for no ride at all, so an
    // assertion that it is absent would be green however the two were folded
    // together — ParkIntegrationService attaches it later, and that is where a
    // test of the pair belongs.
    const dto = ParkWithAttractionsDto.fromEntity(
      parkWith([ride({ curatedOutOfServiceFrom: "2026-01-16" })]),
    );

    expect(dto.attractions[0].worksPeriod).toEqual({
      from: "2026-01-16",
      to: null,
      toUncertain: false,
    });
  });
});

/**
 * The hand-decided kind on the park's attraction list.
 *
 * The frontend reads the badge off THIS payload — a park page renders its
 * rides from the park, never from ~40 attraction calls — so a field that
 * reaches only the attraction endpoint reaches nobody. `hasSingleRider` had
 * to be added to both mappers for the same reason.
 */
describe("ParkWithAttractionsDto.fromEntity › attractionKind", () => {
  const parkWith = (attractions: unknown[]) =>
    ({
      id: "park-1",
      name: "Efteling",
      slug: "efteling",
      timezone: "Europe/Amsterdam",
      attractions,
      shows: [],
      restaurants: [],
    }) as unknown as Park;

  const ride = (overrides = {}) => ({
    id: "ride-1",
    name: "Stoomtrein - Oost",
    slug: "stoomtrein-oost",
    ...overrides,
  });

  it("carries the decided kind through to the list", () => {
    const dto = ParkWithAttractionsDto.fromEntity(
      parkWith([ride({ attractionKind: "TRANSPORT" })]),
    );
    expect(dto.attractions[0]!.attractionKind).toBe("TRANSPORT");
  });

  it("reads null for a ride nobody has judged, not a default kind", () => {
    // The whole catalogue is in this state. A mapper defaulting to "RIDE"
    // would publish our own silence as a statement about the park.
    const dto = ParkWithAttractionsDto.fromEntity(parkWith([ride()]));
    expect(dto.attractions[0]!.attractionKind).toBeNull();
  });
});
