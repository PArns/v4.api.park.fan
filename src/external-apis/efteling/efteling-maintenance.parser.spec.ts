import { readFileSync } from "node:fs";
import { join } from "node:path";
import {
  parseEftelingMaintenance,
  parseWindowText,
} from "./efteling-maintenance.parser";

const FIXTURE = readFileSync(
  join(__dirname, "__fixtures__", "in-maintenance-2026-11-06.html"),
  "utf8",
);

describe("parseEftelingMaintenance", () => {
  it("reads all twelve windows of the 2026-11-06 page", () => {
    const { rows, unparsed } = parseEftelingMaintenance(FIXTURE);
    expect(unparsed).toEqual([]);
    expect(rows).toEqual([
      { name: "Danse Macabre", from: null, to: "2026-11-06" },
      { name: "Droomvlucht", from: "2026-11-09", to: "2026-12-04" },
      { name: "Halve Maen", from: "2026-11-09", to: "2026-11-11" },
      { name: "De Oude Tufferbaan", from: "2026-11-16", to: "2026-11-27" },
      { name: "Joris en de Draak", from: "2026-11-30", to: "2026-12-18" },
      { name: "Baron 1898", from: "2026-12-08", to: "2026-12-08" },
      { name: "Pagode", from: "2026-12-15", to: "2026-12-17" },
      { name: "De Vliegende Hollander", from: "2027-01-04", to: "2027-03-25" },
      { name: "Droomvlucht", from: "2027-01-04", to: "2027-03-25" },
      { name: "Piraña", from: "2027-01-04", to: "2027-03-25" },
      { name: "De Oude Tufferbaan", from: "2027-01-04", to: "2027-02-19" },
      { name: "Baron 1898", from: "2027-03-31", to: "2027-03-31" },
    ]);
  });

  it("fails on a page without the Closed list", () => {
    expect(() =>
      parseEftelingMaintenance(FIXTURE.replace(/>\s*Closed\s*</g, "><")),
    ).toThrow(/no "Closed" list/);
  });

  it("fails on a list that parses to zero windows", () => {
    const empty = FIXTURE.replace(
      /(<div class="attractions">)[\s\S]*?(<div class="container)/,
      "$1</div></div></div></div>$2",
    );
    expect(() => parseEftelingMaintenance(empty)).toThrow(/zero windows/);
  });
});

describe("parseWindowText", () => {
  it.each([
    ["until November 6, 2026 inclusive", { from: null, to: "2026-11-06" }],
    ["December 8, 2026", { from: "2026-12-08", to: "2026-12-08" }],
    [
      "November 9 until December 4, 2026 inclusive",
      { from: "2026-11-09", to: "2026-12-04" },
    ],
    // A start month after the end month is the year before.
    [
      "December 20 until January 5, 2027 inclusive",
      { from: "2026-12-20", to: "2027-01-05" },
    ],
    [
      "December 20, 2026 until January 5, 2027",
      { from: "2026-12-20", to: "2027-01-05" },
    ],
  ])("%s", (text, expected) => {
    expect(parseWindowText(text)).toEqual(expected);
  });

  it.each([
    "until further notice",
    "November 9",
    "February 30, 2027",
    "November 20 until November 9, 2026",
  ])("rejects %s", (text) => {
    expect(parseWindowText(text)).toBeNull();
  });
});
