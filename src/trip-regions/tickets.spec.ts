import { ORLANDO } from "./regions";
import {
  allowanceOf,
  hopSetsOf,
  onSaleAt,
  ORLANDO_TICKETS,
  parksOf,
  publicTicketCatalog,
  sellsDays,
  windowOf,
  type TicketProduct,
} from "./tickets";

const product = (id: string): TicketProduct => {
  const found = ORLANDO_TICKETS.find((p) => p.id === id);
  if (!found) throw new Error(`no product ${id}`);
  return found;
};

const windows = (id: string): Array<number | undefined> => {
  const p = product(id);
  if (p.days === "unlimited") return [windowOf(p, "unlimited")];
  const out: Array<number | undefined> = [];
  for (let d = p.days.min; d <= p.days.max; d++) out.push(windowOf(p, d));
  return out;
};

describe("the Orlando ticket catalog", () => {
  it("holds each product's researched window table", () => {
    expect(windows("wdw-date-based")).toEqual([
      1, 4, 5, 7, 8, 9, 10, 12, 13, 14,
    ]);
    // The Plus option keeps the base window: the extra day is disputed.
    expect(windows("wdw-date-based-plus")).toEqual([
      4, 5, 7, 8, 9, 10, 12, 13, 14,
    ]);
    expect(windows("wdw-4-park-magic")).toEqual([6]);
    expect(windows("wdw-uk-14-day-magic")).toEqual([18]);
    expect(windows("wdw-uk-7-day-magic")).toEqual([15]);
    expect(windows("wdw-annual-pass")).toEqual([Infinity]);
    expect(windows("uor-date-based")).toEqual([1, 4, 5, 6, 7]);
    expect(windows("uor-date-based-volcano-bay")).toEqual([5, 6, 7]);
    expect(windows("uor-1-day-plus-epic")).toEqual([4]);
    expect(windows("uor-2-day-p2p-plus-epic")).toEqual([7]);
    expect(windows("uor-epic-1-day")).toEqual([1]);
    expect(windows("uor-uk-all-parks")).toEqual([14]);
    expect(windows("uor-uk-three-park-adventure")).toEqual([14]);
    expect(windows("uor-annual-pass")).toEqual([Infinity]);
    expect(windows("seaworld-1-day")).toEqual([1]);
    expect(windows("seaworld-two-park")).toEqual([14]);
    expect(windows("seaworld-three-park-3-day")).toEqual([14]);
    expect(windows("seaworld-four-parks-14-day")).toEqual([14]);
    expect(windows("seaworld-uk-3-visit")).toEqual([14]);
    expect(windows("discovery-cove")).toEqual([1]);
    expect(windows("discovery-cove-plus-3")).toEqual([14]);
    expect(windows("legoland-1-day")).toEqual([1]);
    // "Within 3 days of the first scan", counted as a 4-day span.
    expect(windows("legoland-2-day")).toEqual([4]);
    expect(windows("legoland-merlin-7-attractions")).toEqual([14]);
  });

  it("gives every length on sale a window, and every reference a real park or group", () => {
    const parks = new Set(ORLANDO.parks.map((p) => p.slug));
    const groups = new Set(ORLANDO.groups.map((g) => g.key));
    const ids = new Set<string>();
    for (const p of ORLANDO_TICKETS) {
      expect(ids.has(p.id)).toBe(false);
      ids.add(p.id);
      expect(p.id).toMatch(/^[a-z0-9-]+$/);
      expect(groups.has(p.resort)).toBe(true);
      expect(p.markets.length).toBeGreaterThan(0);
      expect(p.shopUrl).toMatch(/^https:\/\//);
      for (const slug of [
        ...p.parks,
        ...(p.allowance?.parks ?? []),
        ...(p.reservedDayParks ?? []),
      ]) {
        expect(parks.has(slug)).toBe(true);
      }
      for (const set of p.hopSets ?? []) {
        for (const slug of set.parks) expect(p.parks).toContain(slug);
      }
      for (const cap of p.caps ?? []) {
        for (const slug of cap.parks) expect(p.parks).toContain(slug);
      }
      for (const slug of p.allowance?.parks ?? []) {
        expect(p.parks).not.toContain(slug);
      }
      if (p.hopping === "none") expect(p.hopSets).toBeUndefined();
      else expect(p.hopSets?.length).toBeGreaterThan(0);
      if (p.days === "unlimited") {
        expect(p.windowTable).toBeUndefined();
      } else {
        expect(p.windowDays).toBeUndefined();
        for (let d = p.days.min; d <= p.days.max; d++) {
          const w = windowOf(p, d);
          expect(w).toBeGreaterThanOrEqual(d);
        }
      }
      if (p.pass) expect(p.days).toBe("unlimited");
    }
  });

  it("covers Epic Universe on a Universal date-based ticket from three days on", () => {
    const p = product("uor-date-based");
    expect(parksOf(p, 2)).not.toContain("universal-epic-universe");
    expect(parksOf(p, 3)).toContain("universal-epic-universe");
    expect(hopSetsOf(p, 2, true)).toEqual([
      ["universal-studios-florida", "universal-islands-of-adventure"],
    ]);
    expect(hopSetsOf(p, 3, true)).toEqual([
      [
        "universal-studios-florida",
        "universal-islands-of-adventure",
        "universal-epic-universe",
      ],
    ]);
    expect(hopSetsOf(p, 3, false)).toEqual([]);
    expect(hopSetsOf(product("wdw-uk-14-day-magic"), 14, false)).toHaveLength(
      1,
    );
  });

  it("reads lengths, allowances and sale dates", () => {
    expect(sellsDays(product("wdw-date-based"), 10)).toBe(true);
    expect(sellsDays(product("wdw-date-based"), 11)).toBe(false);
    expect(sellsDays(product("wdw-annual-pass"), 1)).toBe(false);
    expect(allowanceOf(product("wdw-date-based-plus"), 5)?.visits).toBe(5);
    expect(allowanceOf(product("wdw-uk-14-day-magic"), 14)?.visits).toBe(14);
    expect(allowanceOf(product("wdw-date-based"), 5)).toBeNull();
    const magic = product("wdw-4-park-magic");
    expect(onSaleAt(magic, "2027-01-02")).toBe(false);
    expect(onSaleAt(magic, "2027-01-03")).toBe(true);
    expect(onSaleAt(magic, "2027-05-23")).toBe(false);
    const allParks = product("uor-uk-all-parks");
    expect(onSaleAt(allParks, "2026-12-01")).toBe(false);
    expect(onSaleAt(allParks, "2027-04-01")).toBe(true);
    expect(onSaleAt(product("wdw-date-based"), "2027-08-01")).toBe(true);
  });

  it("serves the catalog without the suggestion order", () => {
    const served = publicTicketCatalog(ORLANDO);
    expect(served).toHaveLength(ORLANDO_TICKETS.length);
    expect(served.some((p) => "suggest" in p)).toBe(false);
  });
});
