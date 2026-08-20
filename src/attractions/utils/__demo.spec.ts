import { resolveCuratedFacts } from "./curated-attraction-facts.util";
import { isFreeFlowOpen } from "../../common/utils/free-flow-status.util";

it("free-flow row: is_seasonal=false, season_months hand-written", () => {
  const row = {
    name: "Wasserspielplatz",
    isSeasonal: false, // default column value; detector never sets it for free-flow
    seasonMonths: [5, 6, 7, 8, 9], // hand-written summer season
  };
  const resolved = resolveCuratedFacts(row);
  console.log("RESOLVED seasonMonths =", JSON.stringify(resolved.seasonMonths));

  const january = new Date("2026-01-15T12:00:00Z");
  jest.useFakeTimers().setSystemTime(january);
  console.log(
    "OLD (raw months)   ->",
    isFreeFlowOpen({
      openWithPark: true,
      parkStatus: "OPERATING",
      seasonMonths: row.seasonMonths,
      parkTimezone: "Europe/Berlin",
    }),
  );
  console.log(
    "NEW (resolved)     ->",
    isFreeFlowOpen({
      openWithPark: true,
      parkStatus: "OPERATING",
      seasonMonths: resolved.seasonMonths,
      parkTimezone: "Europe/Berlin",
    }),
  );
  jest.useRealTimers();
});

it("land empty string", () => {
  console.log(
    "landName '' ->",
    JSON.stringify(resolveCuratedFacts({ name: "x", landName: "" }).landName),
  );
});
