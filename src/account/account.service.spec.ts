import { uniqueVisits } from "./account.service";

describe("uniqueVisits", () => {
  it("keeps each region's park once, with the later date", () => {
    expect(
      uniqueVisits([
        { region: "orlando", park: "epcot", date: "2026-11-18" },
        { region: "orlando", park: "magic-kingdom-park", date: "2026-11-19" },
        { region: "orlando", park: "epcot", date: "2026-11-20" },
        { region: "japan", park: "epcot", date: "2026-04-02" },
      ]),
    ).toEqual([
      { region: "orlando", park: "epcot", date: "2026-11-20" },
      { region: "orlando", park: "magic-kingdom-park", date: "2026-11-19" },
      { region: "japan", park: "epcot", date: "2026-04-02" },
    ]);
  });

  it("drops anything a visit carries beyond its three fields", () => {
    const visit = { region: "orlando", park: "epcot", date: "2026-11-20" };
    expect(uniqueVisits([{ ...visit, extra: true } as typeof visit])).toEqual([
      visit,
    ]);
  });
});
