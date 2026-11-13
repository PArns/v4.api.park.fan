import {
  buildParkDay,
  readDaySlots,
  seasonCovers,
  seasonEvening,
  toLastYearEntry,
  toPublicParkDay,
  type CalendarDayLike,
  type ParkDayInput,
  type ScheduleRowLike,
  type SeasonLike,
} from "./trip-calendar.util";

const TZ = "America/New_York";

/** A schedule row with park-local times, as the feed sends them. */
function row(
  type: string,
  date: string,
  open: string,
  close: string,
  description: string | null = null,
): ScheduleRowLike {
  const [closeDate, closeTime] = close.includes("T")
    ? close.split("T")
    : [date, close];
  return {
    date,
    scheduleType: type,
    openingTime: new Date(`${date}T${open}:00-05:00`),
    closingTime: new Date(`${closeDate}T${closeTime}:00-05:00`),
    description,
  };
}

function season(over: Partial<SeasonLike>): SeasonLike {
  return {
    id: "s1",
    kind: "christmas",
    startDate: "2026-11-07",
    endDate: "2026-12-19",
    dates: null,
    status: "confirmed",
    separateTicket: true,
    opensAt: "19:00",
    closesAt: "24:00",
    ...over,
  };
}

function day(over: Partial<CalendarDayLike> = {}): CalendarDayLike {
  return {
    date: "2026-12-01",
    status: "OPERATING",
    crowdLevel: "moderate",
    hours: {
      openingTime: "2026-12-01T14:00:00.000Z",
      closingTime: "2026-12-01T23:00:00.000Z",
      isInferred: false,
    },
    headlinerForecast: { avgWait: 47 },
    ...over,
  };
}

function input(over: Partial<ParkDayInput> = {}): ParkDayInput {
  return {
    date: "2026-12-01",
    today: "2026-11-12",
    timezone: TZ,
    day: day(),
    rows: [row("OPERATING", "2026-12-01", "09:00", "18:00")],
    seasons: [],
    publishedHoursUntil: "2027-01-21",
    earlyKind: "early_entry",
    longRangeAfterDays: 60,
    ...over,
  };
}

describe("readDaySlots", () => {
  it("reads Early Entry before opening and a party night after the day guests' close", () => {
    const slots = readDaySlots(
      [
        row("TICKETED_EVENT", "2026-12-01", "08:30", "09:00", "Early Entry"),
        row("OPERATING", "2026-12-01", "09:00", "18:00"),
        row("TICKETED_EVENT", "2026-12-01", "19:00", "2026-12-02T00:00"),
      ],
      TZ,
      "early_entry",
    );
    expect(slots.early).toEqual({
      kind: "early_entry",
      opensAt: "2026-12-01T08:30:00-05:00",
      closesAt: "2026-12-01T09:00:00-05:00",
    });
    expect(slots.evening).toEqual({
      kind: "ticketed_event",
      opensAt: "2026-12-01T19:00:00-05:00",
      closesAt: "2026-12-02T00:00:00-05:00",
    });
  });

  it.each([
    ["EPCOT, Mondays", "09:00", "21:00", "21:00", "23:00"],
    ["Animal Kingdom, Wednesdays", "08:00", "18:00", "18:00", "20:00"],
    ["Magic Kingdom, 6 January", "09:00", "21:00", "21:00", "23:00"],
  ])(
    "reads a two-hour evening as Extended Evening Hours (%s)",
    (_park, open, close, from, to) => {
      const slots = readDaySlots(
        [
          row("OPERATING", "2027-01-06", open, close),
          row("TICKETED_EVENT", "2027-01-06", from, to),
        ],
        TZ,
        "early_entry",
      );
      expect(slots.evening?.kind).toBe("extended_evening");
    },
  );

  it("reads an After Hours night as a ticketed event that leaves the day as it is", () => {
    const slots = readDaySlots(
      [
        row("OPERATING", "2027-01-18", "09:00", "21:00"),
        row("TICKETED_EVENT", "2027-01-18", "21:30", "2027-01-19T00:30"),
      ],
      TZ,
      "early_entry",
    );
    expect(slots.evening?.kind).toBe("ticketed_event");
    expect(slots.close?.toISOString()).toBe("2027-01-19T02:00:00.000Z");
  });

  it("names Universal's early hour by its own name, from an EXTRA_HOURS row", () => {
    const slots = readDaySlots(
      [
        row("EXTRA_HOURS", "2026-12-01", "08:00", "09:00"),
        row("OPERATING", "2026-12-01", "09:00", "21:00"),
      ],
      TZ,
      "early_park_admission",
    );
    expect(slots.early?.kind).toBe("early_park_admission");
    expect(slots.evening).toBeUndefined();
  });

  it("places nothing without an OPERATING row, and nothing that overlaps the day", () => {
    expect(
      readDaySlots(
        [row("TICKETED_EVENT", "2026-12-01", "19:00", "23:00")],
        TZ,
        "early_entry",
      ),
    ).toEqual({});
    const overlap = readDaySlots(
      [
        row("OPERATING", "2026-12-01", "09:00", "18:00"),
        row("TICKETED_EVENT", "2026-12-01", "17:00", "20:00"),
      ],
      TZ,
      "early_entry",
    );
    expect(overlap.evening).toBeUndefined();
  });
});

describe("seasons", () => {
  it("covers its listed dates, or its range when it lists none", () => {
    expect(seasonCovers(season({}), "2026-12-01")).toBe(true);
    expect(seasonCovers(season({}), "2026-12-20")).toBe(false);
    const listed = season({ dates: ["2026-11-24", "2026-11-25"] });
    expect(seasonCovers(listed, "2026-11-25")).toBe(true);
    expect(seasonCovers(listed, "2026-11-26")).toBe(false);
  });

  it("supplies the party evening the feed lost, from a stated, separately ticketed season", () => {
    const close = new Date("2026-11-24T23:00:00Z"); // 18:00 in Orlando
    expect(seasonEvening([season({})], "2026-11-24", close, TZ)).toEqual({
      kind: "ticketed_event",
      opensAt: "2026-11-24T19:00:00-05:00",
      closesAt: "2026-11-25T00:00:00-05:00",
      source: "season",
    });
    for (const off of [
      season({ status: "expected" }),
      season({ status: "cancelled" }),
      season({ separateTicket: false }),
      season({ opensAt: null }),
      // Starts before the day guests leave: not an evening.
      season({ opensAt: "17:00" }),
    ]) {
      expect(seasonEvening([off], "2026-11-24", close, TZ)).toBeUndefined();
    }
  });
});

describe("buildParkDay", () => {
  it("serves a forecast day with its hours, slots, figure and basis", () => {
    const out = buildParkDay(
      input({
        rows: [
          row("TICKETED_EVENT", "2026-12-01", "08:30", "09:00"),
          row("OPERATING", "2026-12-01", "09:00", "18:00"),
          row("TICKETED_EVENT", "2026-12-01", "19:00", "2026-12-02T00:00"),
        ],
      }),
    );
    expect(toPublicParkDay(out)).toEqual({
      status: "OPERATING",
      hoursPublished: true,
      opensAt: "2026-12-01T09:00:00-05:00",
      closesAt: "2026-12-01T18:00:00-05:00",
      early: {
        kind: "early_entry",
        opensAt: "2026-12-01T08:30:00-05:00",
        closesAt: "2026-12-01T09:00:00-05:00",
      },
      evening: {
        kind: "ticketed_event",
        opensAt: "2026-12-01T19:00:00-05:00",
        closesAt: "2026-12-02T00:00:00-05:00",
      },
      crowdLevel: "moderate",
      headlinerWait: 45,
      basis: "forecast",
    });
    // The hours factor reads the OPERATING close, not the party's.
    expect(out.dayHours).toBe(9);
  });

  it("names no early slot where the feed has none, and claims none", () => {
    const out = buildParkDay(
      input({
        rows: [
          row("OPERATING", "2026-12-01", "09:00", "18:00"),
          row("TICKETED_EVENT", "2026-12-01", "19:00", "2026-12-02T00:00"),
        ],
      }),
    );
    expect(out.early).toBeUndefined();
    expect(out.evening?.kind).toBe("ticketed_event");
  });

  it("takes the evening from the season where the feed has no evening row", () => {
    const out = buildParkDay(
      input({
        date: "2026-11-24",
        day: day({ date: "2026-11-24" }),
        rows: [
          row("TICKETED_EVENT", "2026-11-24", "08:30", "09:00"),
          row("OPERATING", "2026-11-24", "09:00", "18:00"),
        ],
        seasons: [season({ id: "mvmcp" })],
      }),
    );
    expect(out.early?.kind).toBe("early_entry");
    expect(out.evening).toMatchObject({
      kind: "ticketed_event",
      source: "season",
    });
    expect(out.seasons).toEqual(["mvmcp"]);
  });

  it("closes a day a stated closure season covers, but never over an OPERATING row", () => {
    const refurb = season({
      id: "tl-refurb",
      kind: "closure",
      startDate: "2026-09-09",
      endDate: "2027-01-26",
      separateTicket: false,
      opensAt: null,
      closesAt: null,
    });
    const shut = buildParkDay(
      input({
        day: day({ status: "UNKNOWN", headlinerForecast: undefined }),
        rows: [],
        seasons: [refurb],
        publishedHoursUntil: null,
      }),
    );
    expect(shut).toMatchObject({
      status: "CLOSED",
      crowdLevel: "closed",
      closedBy: "tl-refurb",
      hoursPublished: false,
      basis: "none",
    });
    const open = buildParkDay(input({ seasons: [refurb] }));
    expect(open.status).toBe("OPERATING");
    expect(open.closedBy).toBeUndefined();
    const expected = buildParkDay(
      input({
        day: day({ status: "UNKNOWN" }),
        rows: [],
        seasons: [{ ...refurb, status: "expected" }],
      }),
    );
    expect(expected.status).toBe("UNKNOWN");
  });

  it("reads an inferred off-season CLOSED past the published hours as UNKNOWN", () => {
    const inferred = buildParkDay(
      input({
        date: "2027-02-10",
        day: day({
          status: "CLOSED",
          crowdLevel: "closed",
          headlinerForecast: undefined,
        }),
        rows: [],
        publishedHoursUntil: "2027-01-21",
      }),
    );
    expect(inferred).toMatchObject({
      status: "UNKNOWN",
      hoursPublished: false,
    });

    const stated = buildParkDay(
      input({
        date: "2027-02-10",
        day: day({ status: "CLOSED", crowdLevel: "closed" }),
        rows: [
          {
            ...row("CLOSED", "2027-02-10", "00:00", "00:01"),
            description: null,
          },
        ],
        publishedHoursUntil: "2027-01-21",
      }),
    );
    expect(stated).toMatchObject({ status: "CLOSED", hoursPublished: true });
  });

  it("labels a forecast past the TFT horizon long_range", () => {
    const out = buildParkDay(
      input({ date: "2027-02-20", day: day({ date: "2027-02-20" }) }),
    );
    expect(out.basis).toBe("long_range");
  });

  it("falls back to the same weekday a year back past the forecast, as an estimate", () => {
    const out = buildParkDay(
      input({
        date: "2027-08-10",
        day: day({
          date: "2027-08-10",
          status: "UNKNOWN",
          crowdLevel: "unknown",
          hours: undefined,
          headlinerForecast: undefined,
        }),
        rows: [],
        lastYear: { c: "high", w: 55 },
      }),
    );
    expect(out).toMatchObject({
      status: "UNKNOWN",
      hoursPublished: false,
      crowdLevel: "high",
      headlinerWait: 55,
      basis: "last_year",
    });
    const nothing = buildParkDay(
      input({
        date: "2027-08-10",
        day: day({
          status: "UNKNOWN",
          crowdLevel: "unknown",
          hours: undefined,
          headlinerForecast: undefined,
        }),
        rows: [],
        lastYear: { c: "closed" },
      }),
    );
    expect(nothing).toMatchObject({ crowdLevel: "unknown", basis: "none" });
    expect(nothing.headlinerWait).toBeUndefined();
  });
});

describe("toLastYearEntry", () => {
  it("keeps the measured level and the headliners' recorded day peak", () => {
    expect(
      toLastYearEntry(
        day({
          crowdLevel: "high",
          headlinerForecast: { avgWait: 52, actual: true },
        }),
      ),
    ).toEqual({ c: "high", w: 50 });
    // A prediction cached before the day passed is no measurement.
    expect(
      toLastYearEntry(day({ headlinerForecast: { avgWait: 52 } })),
    ).toEqual({
      c: "moderate",
    });
    expect(
      toLastYearEntry(day({ status: "CLOSED", crowdLevel: "closed" })),
    ).toEqual({
      c: "closed",
    });
  });
});
