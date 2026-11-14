import {
  TEST_JAPAN,
  TEST_PARIS,
  TEST_TOKYO,
} from "./__fixtures__/test-regions";
import { hotelKinds, TRIP_REGIONS, type RegionConfig } from "./regions";
import { seedTickets } from "./tickets";
import { TRAVEL_MATRICES } from "./travel-matrix.generated";

/**
 * What every region's config must hold together, so a new region (or an edit
 * of one) fails here rather than in a plan: its vocabularies name each other,
 * its weather and travel are complete for their source, its seed tickets sell
 * in its markets, and its bases name its own hotel areas and parks.
 */

const MD = /^(0[1-9]|1[0-2])-(0[1-9]|[12]\d|3[01])$/;

function problemsOf(region: RegionConfig): string[] {
  const out: string[] = [];
  const parks = new Set(region.parks.map((p) => p.slug));
  const groups = new Set(region.groups.map((g) => g.key));
  const areas = new Set(region.hotelAreas.map((a) => a.key));
  const hotels = hotelKinds(region);
  const say = (message: string) => out.push(`${region.slug}: ${message}`);

  if (!hotels.includes("offsite")) say("hotel kinds lack offsite");
  if ((region.perks.offsite ?? []).length > 0) say("offsite carries perks");
  for (const kind of hotels) {
    if (!region.hotelNames[kind]) say(`hotel ${kind} has no name`);
    for (const rule of region.perks[kind]) {
      if (!region.perkNames[rule.perk]) say(`perk ${rule.perk} has no name`);
      if (!(rule.share > 0 && rule.share < 1)) {
        say(`perk ${rule.perk} share ${rule.share} is not in (0, 1)`);
      }
      for (const p of rule.parks) {
        if (!parks.has(p)) say(`perk ${rule.perk} names ${p}`);
      }
    }
  }
  for (const area of region.hotelAreas) {
    if (!hotels.includes(area.hotel)) {
      say(`area ${area.key}'s hotel ${area.hotel} is no hotel kind`);
    }
  }
  for (const park of region.parks) {
    if (!groups.has(park.group)) say(`park ${park.slug}'s group is unknown`);
    if (region.travel.source === "osrm" && !park.carPark) {
      say(`park ${park.slug} has no car park for OSRM`);
    }
  }
  if (!region.parks.some((p) => p.headliner)) say("no headliner park");
  if (!parks.has(region.weather.climatePark)) {
    say(`climate park ${region.weather.climatePark} is no region park`);
  }
  const storm = region.weather.storm;
  if (storm) {
    for (const [m, a, b] of storm.weights) {
      if (m < 1 || m > 12 || a < 0 || a > 1 || b < 0 || b > 1) {
        say(`storm weight [${m}, ${a}, ${b}] out of range`);
      }
    }
    if (!MD.test(storm.peak.from) || !MD.test(storm.peak.to)) {
      say("storm peak is not MM-DD");
    }
  }
  for (const note of region.weather.notes ?? []) {
    if (!MD.test(note.from) || !MD.test(note.to) || note.from > note.to) {
      say(`season note ${note.kind} is not MM-DD … MM-DD`);
    }
  }
  const points = new Set([
    ...parks,
    ...[...areas].map((a) => `hotel:${a}`),
    ...region.venues.map((v) => `venue:${v.key}`),
  ]);
  for (const leg of region.legs) {
    for (const end of [leg.from, leg.to]) {
      if (!points.has(end)) say(`leg names ${end}`);
    }
    if (region.travel.source === "curated" && !leg.primary) {
      say(`curated leg ${leg.from}–${leg.to} has no primary`);
    }
  }
  if (region.travel.source === "osrm" && !TRAVEL_MATRICES[region.slug]) {
    say("no OSRM travel matrix (build-travel-matrix.mjs)");
  }
  for (const set of region.hopperSets) {
    for (const p of set) if (!parks.has(p)) say(`hopper set names ${p}`);
  }
  const markets = new Set(region.ticketMarkets.codes);
  if (!markets.has(region.ticketMarkets.home)) say("home market not a code");
  for (const product of seedTickets(region)) {
    if (!groups.has(product.resort)) say(`ticket ${product.id}'s resort`);
    for (const m of product.markets) {
      if (!markets.has(m)) say(`ticket ${product.id} market ${m}`);
    }
    for (const p of product.parks) {
      if (!parks.has(p)) say(`ticket ${product.id} names ${p}`);
    }
  }
  const bases = new Set((region.bases ?? []).map((b) => b.key));
  const areaBase = new Map<string, string>();
  for (const base of region.bases ?? []) {
    for (const area of base.hotelAreas) {
      if (!areas.has(area)) say(`base ${base.key} names area ${area}`);
      if (areaBase.has(area)) say(`area ${area} is in two bases`);
      areaBase.set(area, base.key);
    }
    for (const reach of base.parks) {
      if (!parks.has(reach.slug)) say(`base ${base.key} names ${reach.slug}`);
      if (!(reach.minutes > 0)) say(`base ${base.key} minutes`);
    }
  }
  for (const leg of region.baseLegs ?? []) {
    if (!bases.has(leg.from) || !bases.has(leg.to)) {
      say(`base leg ${leg.from}–${leg.to} names an unknown base`);
    }
    if (!(leg.minutes > 0)) say(`base leg ${leg.from}–${leg.to} minutes`);
  }
  if (region.bases) {
    const ordered = region.bases.filter((b) => b.order !== undefined).length;
    if (ordered !== 0 && ordered !== region.bases.length) {
      say("either every base has an order on the route line or none");
    }
    for (const area of areas) {
      if (!areaBase.has(area)) say(`area ${area} is in no base`);
    }
    for (const base of region.bases) {
      if (base.defaultArea && !base.hotelAreas.includes(base.defaultArea)) {
        say(`base ${base.key}'s default area is not its own`);
      }
      for (const g of base.gateways ?? []) {
        if (!g.code || !g.name || !["airport", "station"].includes(g.kind)) {
          say(`base ${base.key} gateway ${g.code}`);
        }
      }
    }
    for (const park of region.parks) {
      if (
        !region.bases.some((b) => b.parks.some((p) => p.slug === park.slug))
      ) {
        say(`no base reaches ${park.slug}`);
      }
    }
  }
  const codes = region.parks.map((p) => p.code).filter((c) => c !== undefined);
  if (new Set(codes).size !== codes.length) say("park codes repeat");
  return out;
}

describe("the region configs", () => {
  const regions = [
    ...Object.values(TRIP_REGIONS),
    TEST_TOKYO,
    TEST_PARIS,
    TEST_JAPAN,
  ];

  it.each(regions.map((r) => [r.slug, r] as const))(
    "%s holds together",
    (_slug, region) => {
      expect(problemsOf(region)).toEqual([]);
    },
  );

  it("lists each region under its own slug", () => {
    for (const [slug, region] of Object.entries(TRIP_REGIONS)) {
      expect(region.slug).toBe(slug);
    }
  });
});
