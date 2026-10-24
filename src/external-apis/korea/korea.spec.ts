import { readFileSync } from "fs";
import { join } from "path";
import {
  NeisScheduleRow,
  isVacationRow,
  parseNeisResponse,
  sampleEvenly,
  vacationDaysByMajority,
} from "./neis.client";
import { parseKasiResponse } from "./kasi.client";
import { krImportTargets, neisHolidayRows } from "./korea-holidays";
import { krRegionKey, resolveKrRegion } from "./kr-regions";

/**
 * Fixtures: the `neis-*` files are real NEIS responses recorded on
 * 2026-10-24 — without a key NEIS returns at most five rows per call, so the
 * three summer calendars were recorded one day per call and joined into one
 * response envelope. `kasi-getRestDeInfo-guide-example.json` is the response
 * example of KASI's own API guide (v1.4) in its JSON form; `kasi-no-key` is a
 * real keyless response. No test touches the network.
 */
const fixture = (name: string): unknown =>
  JSON.parse(readFileSync(join(__dirname, "__fixtures__", name), "utf8"));

const schedule = (name: string): NeisScheduleRow[] =>
  parseNeisResponse<NeisScheduleRow>(fixture(name), "SchoolSchedule").rows;

describe("NEIS SchoolSchedule", () => {
  it("parses a recorded calendar", () => {
    const { rows, total } = parseNeisResponse<NeisScheduleRow>(
      fixture("neis-schedule-kyungbok-2026-summer.json"),
      "SchoolSchedule",
    );
    expect(rows.length).toBe(total);
    expect(rows).toContainEqual(
      expect.objectContaining({
        AA_YMD: "20260717",
        EVENT_NM: "여름방학",
        SBTR_DD_SC_NM: "휴업일",
      }),
    );
  });

  it("reads 'no data' as no rows and any other result code as a failure", () => {
    expect(
      parseNeisResponse(fixture("neis-no-data.json"), "SchoolSchedule"),
    ).toEqual({ rows: [], total: 0 });
    expect(() =>
      parseNeisResponse(
        fixture("neis-missing-parameter.json"),
        "SchoolSchedule",
      ),
    ).toThrow(/ERROR-300/);
  });

  it("counts vacation days, not the closing ceremony, Saturdays or public holidays", () => {
    const rows = schedule("neis-schedule-kyungbok-2026-summer.json");
    const days = rows.filter(isVacationRow).map((r) => r.AA_YMD);
    expect(days[0]).toBe("20260717"); // 07-16 is 여름방학식, a school day
    expect(days[days.length - 1]).toBe("20260819"); // 08-20 is 개학식
    expect(
      rows.filter((r) => !isVacationRow(r)).map((r) => r.EVENT_NM),
    ).toEqual(
      expect.arrayContaining(["여름방학식", "토요휴업일", "광복절", "개학식"]),
    );
  });

  it("keeps the days most sampled schools are off, from three recorded Seoul calendars", () => {
    // 경복 07-17..08-19, 경기 07-16..08-10, 가락 07-24..08-25 (recorded).
    const days = vacationDaysByMajority([
      schedule("neis-schedule-kyungbok-2026-summer.json"),
      schedule("neis-schedule-kyonggi-2026-summer.json"),
      schedule("neis-schedule-garak-2026-summer.json"),
      [], // a school that returned no calendar does not vote
    ]);
    const list = [...days.keys()];
    expect(list[0]).toBe("2026-07-17");
    expect(list[list.length - 1]).toBe("2026-08-19");
    expect(list).toHaveLength(34);
    expect(days.get("2026-08-01")).toBe("여름방학");
  });

  it("bridges the holiday and weekend a school files instead of 방학 inside its break", () => {
    // 경기초 recorded 07-16 여름방학, 07-17 제헌절, 07-18 토요휴업일, 07-19 nothing,
    // 07-20 여름방학 — one event per day.
    const days = vacationDaysByMajority([
      schedule("neis-schedule-kyonggi-2026-summer.json"),
    ]);
    for (const d of ["2026-07-16", "2026-07-17", "2026-07-18", "2026-07-19"]) {
      expect(days.has(d)).toBe(true);
    }
    expect(days.has("2026-07-15")).toBe(false); // 여름방학식 — a school day
    expect(days.has("2026-08-11")).toBe(false); // 개학식
  });

  it("samples schools evenly and stably", () => {
    expect(sampleEvenly(["a", "b", "c", "d"], 2)).toEqual(["a", "c"]);
    expect(sampleEvenly(["a"], 15)).toEqual(["a"]);
  });

  it("parses the school list and drops a school without a code yet", () => {
    const { rows } = parseNeisResponse<{ SD_SCHUL_CODE: string }>(
      fixture("neis-schoolinfo-seoul.json"),
      "schoolInfo",
    );
    const codes = rows.map((r) => r.SD_SCHUL_CODE.trim()).filter(Boolean);
    expect(rows).toHaveLength(5);
    expect(codes).toEqual(["7031110", "7134077", "7021079", "7091368"]);
  });
});

describe("KASI getRestDeInfo", () => {
  it("parses the guide's single-item response", () => {
    expect(
      parseKasiResponse(fixture("kasi-getRestDeInfo-guide-example.json")),
    ).toEqual([{ date: "2019-03-01", name: "삼일절" }]);
  });

  it("parses a list, skips non-holidays and reads an empty year as none", () => {
    const ok = (items: unknown) => ({
      response: { header: { resultCode: "00" }, body: { items } },
    });
    expect(
      parseKasiResponse(
        ok({
          item: [
            { locdate: 20260924, dateName: "추석", isHoliday: "Y" },
            { locdate: 20260717, dateName: "제헌절", isHoliday: "N" },
          ],
        }),
      ),
    ).toEqual([{ date: "2026-09-24", name: "추석" }]);
    expect(parseKasiResponse(ok(""))).toEqual([]);
  });

  it("throws on a refused call instead of reading it as a year without holidays", () => {
    expect(() => parseKasiResponse(fixture("kasi-no-key.json"))).toThrow(
      /SERVICE_KEY_IS_NULL/,
    );
  });
});

describe("Korean regions", () => {
  it.each([
    ["11", "B10"],
    ["KR-41", "J10"],
    ["Seoul", "B10"],
    ["Gyeonggi-do", "J10"],
    ["Seoul Special City", "B10"],
    ["Gyeongsangbuk-do", "R10"],
    ["Jeju-do", "T10"],
  ])("resolves %s to office %s", (code, office) => {
    expect(resolveKrRegion(code)?.office).toBe(office);
  });

  it("names nothing it cannot place", () => {
    expect(resolveKrRegion("FL")).toBeNull();
    expect(resolveKrRegion("do")).toBeNull(); // "Gyeonggi-do" after a split on "-"
    expect(krRegionKey("Gangwon State")).toBe("gangwon");
  });

  it("writes a park's own region code and the ISO code, once each", () => {
    const { targets, unknown } = krImportTargets([
      "11",
      "Gyeonggi",
      "41",
      "XX",
    ]);
    expect(unknown).toEqual(["XX"]);
    const byOffice = Object.fromEntries(
      [...targets].map(([r, codes]) => [r.office, [...codes].sort()]),
    );
    expect(byOffice).toEqual({
      B10: ["KR-11"],
      J10: ["KR-41", "KR-Gyeonggi"],
    });
    const rows = neisHolidayRows(
      byOffice.J10,
      new Map([["2026-07-21", "여름방학"]]),
    );
    expect(rows.map((r) => [r.externalId, r.region, r.name])).toEqual([
      ["neis:KR:KR-41:2026-07-21", "KR-41", "Summer vacation"],
      ["neis:KR:KR-Gyeonggi:2026-07-21", "KR-Gyeonggi", "Summer vacation"],
    ]);
  });
});
