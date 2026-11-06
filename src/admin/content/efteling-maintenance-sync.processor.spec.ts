import { readFileSync } from "node:fs";
import { join } from "node:path";
import axios from "axios";
import {
  diffRideWindows,
  EFTELING_MAINTENANCE_SOURCE as SOURCE,
  EFTELING_MAINTENANCE_SYNC,
  EftelingMaintenanceSyncProcessor,
  matchRides,
} from "./efteling-maintenance-sync.processor";
import { EFTELING_MAINTENANCE_URL } from "../../external-apis/efteling/efteling-maintenance.parser";

jest.mock("axios", () => ({ __esModule: true, default: { get: jest.fn() } }));
jest.mock("../../common/utils/date.util", () => ({
  getCurrentDateInTimezone: () => "2026-11-06",
}));
jest.mock("../../common/utils/file-logger.util", () => ({
  logExternalApiError: jest.fn(),
  logJobFailure: jest.fn(),
}));
import {
  logExternalApiError,
  logJobFailure,
} from "../../common/utils/file-logger.util";

const TODAY = "2026-11-06";
const own = (from: string | null, to: string | null) => ({
  from,
  to,
  toUncertain: false,
  source: SOURCE,
});
const hand = (from: string | null, to: string | null) => ({
  from,
  to,
  toUncertain: false,
});

describe("matchRides", () => {
  const rides = [
    { id: "1", name: "Piraña", curatedName: null },
    { id: "2", name: "Python", curatedName: "Python (Efteling)" },
    { id: "3", name: "Twin", curatedName: null },
    { id: "4", name: "twin", curatedName: null },
  ];

  it("matches on name ignoring case and diacritics, then on curated name", () => {
    const { matched, unmatched } = matchRides(
      ["PIRANA", "python (efteling)", "Nobody"],
      rides,
    );
    expect(matched.get("PIRANA")?.id).toBe("1");
    expect(matched.get("python (efteling)")?.id).toBe("2");
    expect(unmatched).toEqual(["Nobody"]);
  });

  it("never guesses between two rides of one name", () => {
    expect(matchRides(["Twin"], rides).unmatched).toEqual(["Twin"]);
  });
});

describe("diffRideWindows", () => {
  it("adds what the ride lacks, a missing start becoming today", () => {
    const diff = diffRideWindows(
      [],
      [
        { from: null, to: "2026-11-20" },
        { from: "2027-01-04", to: "2027-03-25" },
      ],
      TODAY,
    );
    expect(diff.added).toEqual([
      own(TODAY, "2026-11-20"),
      own("2027-01-04", "2027-03-25"),
    ]);
    expect(diff.next).toEqual(diff.added);
  });

  it("keeps the first-seen start of a window the page lists as 'until'", () => {
    const diff = diffRideWindows(
      [own("2026-10-30", "2026-11-20")],
      [{ from: null, to: "2026-11-20" }],
      TODAY,
    );
    expect(diff).toMatchObject({ added: [], updated: [], removed: [] });
    expect(diff.next).toEqual([own("2026-10-30", "2026-11-20")]);
  });

  it("updates its own window whose dates moved, matched by overlap", () => {
    const diff = diffRideWindows(
      [own("2026-11-09", "2026-12-04")],
      [{ from: "2026-11-09", to: "2026-12-11" }],
      TODAY,
    );
    expect(diff.updated).toEqual([
      {
        before: own("2026-11-09", "2026-12-04"),
        after: own("2026-11-09", "2026-12-11"),
      },
    ]);
    expect(diff.next).toEqual([own("2026-11-09", "2026-12-11")]);
  });

  it("removes its own unlisted windows only while they have not started", () => {
    const diff = diffRideWindows(
      [own("2026-10-01", "2026-10-20"), own("2026-12-01", "2026-12-05")],
      [],
      TODAY,
    );
    expect(diff.removed).toEqual([own("2026-12-01", "2026-12-05")]);
    expect(diff.next).toEqual([own("2026-10-01", "2026-10-20")]);
  });

  it("never touches a hand-written window and skips a row that would overlap it", () => {
    const diff = diffRideWindows(
      [hand("2026-11-01", "2026-11-30"), hand("2027-06-01", null)],
      [
        { from: "2026-11-09", to: "2026-12-04" },
        { from: "2027-01-04", to: "2027-03-25" },
      ],
      TODAY,
    );
    expect(diff.skipped).toEqual([
      {
        window: own("2026-11-09", "2026-12-04"),
        reason: "overlaps a hand-written window (2026-11-01 – 2026-11-30)",
      },
    ]);
    expect(diff.added).toEqual([own("2027-01-04", "2027-03-25")]);
    expect(diff.next).toEqual([
      hand("2026-11-01", "2026-11-30"),
      own("2027-01-04", "2027-03-25"),
      hand("2027-06-01", null),
    ]);
  });

  it("skips an 'until' row whose day is already past", () => {
    const diff = diffRideWindows([], [{ from: null, to: "2026-11-05" }], TODAY);
    expect(diff.added).toEqual([]);
    expect(diff.skipped[0].reason).toBe("already over");
  });
});

describe("EftelingMaintenanceSyncProcessor", () => {
  const html = readFileSync(
    join(
      __dirname,
      "../../external-apis/efteling/__fixtures__/in-maintenance-2026-11-06.html",
    ),
    "utf8",
  );
  const NAMES = [
    "Baron 1898",
    "Danse Macabre",
    "De Oude Tufferbaan",
    "De Vliegende Hollander",
    "Droomvlucht",
    "Halve Maen",
    "Joris en de Draak",
    "Pagode",
    "Piraña",
  ];

  function build() {
    (axios.get as jest.Mock).mockResolvedValue({ data: html });
    const rides = [...NAMES, "Python"].map((name, i) => ({
      id: `ride-${i}`,
      name,
      curatedName: null,
      curatedOutOfServicePeriods: null,
    }));
    const curation = {
      curateAttractionsBulk: jest.fn(
        async (_park: string, patches: { id: string }[], _actor: unknown) => ({
          changed: patches.map((p) => ({
            id: p.id,
            changed: [],
            auditId: `a-${p.id}`,
          })),
        }),
      ),
    };
    const processor = new EftelingMaintenanceSyncProcessor(
      {
        find: jest.fn(async () => [
          { id: "park-1", slug: "efteling", timezone: "Europe/Amsterdam" },
        ]),
      } as never,
      { find: jest.fn(async () => rides) } as never,
      curation as never,
    );
    return { processor, curation };
  }

  it("adds all twelve windows to the nine rides on a first run, through the curation path", async () => {
    const { processor, curation } = build();
    const result = await processor.sync(false);

    expect(result.totals).toEqual({
      added: 12,
      updated: 0,
      removed: 0,
      skipped: 0,
    });
    expect(result.unmatched).toEqual([]);
    expect(result.rides.map((r) => r.name).sort()).toEqual(NAMES);
    const [parkId, patches, actor] =
      curation.curateAttractionsBulk.mock.calls[0];
    expect(parkId).toBe("park-1");
    expect(actor).toBe(EFTELING_MAINTENANCE_SYNC);
    expect(patches).toHaveLength(9);
    const droomvlucht = (patches as Array<Record<string, any>>).find(
      (p) => p.id === "ride-4",
    )!;
    expect(droomvlucht.sourceUrl).toBe(EFTELING_MAINTENANCE_URL);
    expect(droomvlucht.fields.curatedOutOfServicePeriods).toEqual([
      own("2026-11-09", "2026-12-04"),
      own("2027-01-04", "2027-03-25"),
    ]);
    const danse = (patches as Array<Record<string, any>>).find(
      (p) => p.id === "ride-1",
    )!;
    expect(danse.fields.curatedOutOfServicePeriods).toEqual([
      own(TODAY, TODAY),
    ]);
  });

  it("writes nothing on a dry run but returns the diff", async () => {
    const { processor, curation } = build();
    const result = await processor.sync(true);
    expect(result.totals.added).toBe(12);
    expect(curation.curateAttractionsBulk).not.toHaveBeenCalled();
  });

  it("fails and writes nothing when the page layout changed", async () => {
    const { processor, curation } = build();
    (axios.get as jest.Mock).mockResolvedValue({ data: "<html></html>" });
    await expect(processor.sync(false)).rejects.toThrow(/layout changed/);
    expect(curation.curateAttractionsBulk).not.toHaveBeenCalled();
  });

  describe("error logging", () => {
    beforeEach(() => jest.clearAllMocks());
    const job = (data = {}) => ({ data }) as never;

    it("logs a failed run to the job-failure log and still fails the job", async () => {
      const { processor } = build();
      (axios.get as jest.Mock).mockResolvedValue({ data: "<html></html>" });
      await expect(processor.handle(job())).rejects.toThrow(/layout changed/);
      expect(logJobFailure).toHaveBeenCalledWith(
        "sync-efteling-maintenance",
        "manual-metadata",
        expect.any(Error),
        expect.objectContaining({ url: EFTELING_MAINTENANCE_URL }),
      );
    });

    it("logs a failed fetch as an external API error as well", async () => {
      const { processor } = build();
      (axios.get as jest.Mock).mockRejectedValue(new Error("HTTP 503"));
      await expect(processor.handle(job())).rejects.toThrow("HTTP 503");
      expect(logExternalApiError).toHaveBeenCalledWith(
        "efteling",
        "maintenance-calendar",
        expect.any(Error),
        expect.anything(),
      );
      expect(logJobFailure).toHaveBeenCalled();
    });

    it("logs names it could not match, without failing the run", async () => {
      const { processor } = build();
      (axios.get as jest.Mock).mockResolvedValue({
        data: html.replace(/Pagode/g, "Pagoda Renamed"),
      });
      const result = await processor.handle(job({ dryRun: true }));
      expect(result.unmatched).toEqual(["Pagoda Renamed"]);
      expect(logExternalApiError).toHaveBeenCalledWith(
        "efteling",
        "maintenance-calendar",
        expect.objectContaining({
          message: expect.stringContaining("Pagoda Renamed"),
        }),
        { unmatched: ["Pagoda Renamed"] },
      );
      expect(logJobFailure).not.toHaveBeenCalled();
    });
  });
});
