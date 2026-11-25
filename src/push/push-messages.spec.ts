import {
  TITLE_MAX_CHARS,
  fit,
  writeMorningPlanMessage,
  writeMessage,
  writePlanChangeMessage,
  writeRideAlertMessage,
  writeRainSoonMessage,
  writeRideReopenMessage,
} from "./push-messages";
import {
  DEVICE_PUSH_KINDS,
  PUSH_PICTOGRAMS,
  PUSH_PICTOGRAM_CHARS,
  PUSH_PICTOGRAM_LOCAL,
  pushKindOf,
  withPictogram,
  type DevicePushKind,
} from "./push-config";
import {
  POST_FEEDS,
  writeNewsMessage,
} from "../queues/services/news-push.service";

describe("the pictograms", () => {
  const all = [...Object.values(PUSH_PICTOGRAMS), PUSH_PICTOGRAM_LOCAL];

  it("has one per kind and one for the phone's own reminder, no two alike", () => {
    expect(Object.keys(PUSH_PICTOGRAMS).sort()).toEqual(
      [...DEVICE_PUSH_KINDS].sort(),
    );
    expect(new Set(all).size).toBe(all.length);
  });

  it("is one code point each that is an emoji by itself: no ZWJ, no selector, no skin tone", () => {
    for (const pictogram of all) {
      expect([...pictogram]).toHaveLength(1);
      expect(pictogram).toMatch(/^\p{Emoji_Presentation}$/u);
    }
  });

  it("leaves a message of no known kind as it is", () => {
    const test = { title: "Test", tag: "test:1" };
    expect(withPictogram(test)).toBe(test);
  });
});

const LOCALES = ["de", "en", "nl", "fr", "es", "it"];

describe("fit", () => {
  it("leaves a title that fits alone", () => {
    expect(fit("Taron", ": 15 Min.")).toBe("Taron: 15 Min.");
  });

  it("cuts the name, never the number behind it", () => {
    const title = fit("Harry Potter and the Forbidden Journey", ": 15 Min.");
    expect(title).toBe("Harry Potter and th…: 15 Min.");
    expect([...title].length).toBeLessThanOrEqual(
      TITLE_MAX_CHARS - PUSH_PICTOGRAM_CHARS,
    );
  });

  it("never cuts a name of sixteen characters or fewer", () => {
    expect(fit("Phantasialand", " öffnet um 09:00 Uhr")).toBe(
      "Phantasialand öffnet um 09:00 Uhr",
    );
    expect(fit("Black Mamba Extended Cut", " ".repeat(40))).toMatch(
      /^Black Mamba Ext…/,
    );
  });
});

describe("writeMessage", () => {
  const notification = {
    dedupeKey: "next-up:phantasialand:2026-10-17:taron-1:600",
    parkName: "Phantasialand",
    what: "Taron",
    inMinutes: 15,
    atTime: "10:00",
    url: "/",
  };

  it("leads with the ride and the minutes, the park on its own line", () => {
    expect(writeMessage(notification, "de")).toEqual({
      title: "Taron in 15 Min.",
      subtitle: "Phantasialand",
      body: "Geplant für 10:00 Uhr.",
      url: "/",
      tag: notification.dedupeKey,
    });
  });

  it("matches the base language for a regional tag", () => {
    // de-AT and de-CH are German, not a fourth language of their own.
    expect(writeMessage(notification, "de-AT").title).toBe("Taron in 15 Min.");
  });

  it("falls back to English for a locale this table does not carry", () => {
    expect(writeMessage(notification, "pt").title).toBe("Taron in 15 min");
    expect(writeMessage(notification, "xx").body).toBe("Planned for 10:00.");
  });

  it.each([
    ["nl", "Taron over 15 min.", "Gepland om 10:00 uur."],
    ["fr", "Taron dans 15 min", "Prévu à 10:00."],
    ["es", "Taron en 15 min", "Previsto a las 10:00."],
    ["it", "Taron tra 15 min", "Previsto alle 10:00."],
  ])("writes %s", (locale, title, body) => {
    expect(writeMessage(notification, locale)).toMatchObject({ title, body });
  });

  it("hands the device fields through and leaves absent ones out", () => {
    const message = writeMessage(
      {
        ...notification,
        photo: "europe/germany/bruehl/phantasialand/taron",
        expiresAtMs: 5,
        threadId: "phantasialand:2026-10-17",
        data: { entry: "taron-1" },
      },
      "en",
    );
    expect(message).toMatchObject({
      photo: "europe/germany/bruehl/phantasialand/taron",
      expiresAtMs: 5,
      threadId: "phantasialand:2026-10-17",
      data: { entry: "taron-1" },
    });
    expect(writeMessage(notification, "en")).not.toHaveProperty("photo");
  });

  it("says a show starts, where a plan block is planned", () => {
    const show = {
      dedupeKey: "show-start:show-1:2026-10-17T20:00:00.000Z",
      parkName: "Europa-Park",
      what: "Feuerwerk",
      inMinutes: 30,
      atTime: "20:00",
      url: "/parks/europe/germany/rust/europa-park#shows",
    };
    expect(writeMessage(show, "de")).toMatchObject({
      title: "Feuerwerk in 30 Min.",
      subtitle: "Europa-Park",
      body: "Beginn 20:00 Uhr.",
    });
    expect(writeMessage(show, "en").body).toBe("Starts at 20:00.");
  });
});

describe("writeRideAlertMessage", () => {
  const notification = {
    dedupeKey: "ride-alert:alert-1",
    attractionName: "Taron",
    parkName: "Phantasialand",
    waitTime: 15,
    thresholdMinutes: 20,
    url: "/parks/europe/germany/bruehl/phantasialand/taron",
  };

  it("names the ride and the wait first, then the limit it fell under", () => {
    expect(writeRideAlertMessage(notification, "de")).toMatchObject({
      title: "Taron: 15 Min.",
      subtitle: "Phantasialand",
      body: "Unter deiner Grenze von 20 Min.",
    });
  });

  it.each([
    ["en", "Taron: 15 min", "Below your 20 min limit."],
    ["nl", "Taron: 15 min.", "Onder je grens van 20 min."],
    ["fr", "Taron : 15 min", "Sous ta limite de 20 min."],
    ["es", "Taron: 15 min", "Por debajo de tu límite de 20 min."],
    ["it", "Taron: 15 min", "Sotto il tuo limite di 20 min."],
    ["xx", "Taron: 15 min", "Below your 20 min limit."],
  ])("writes %s", (locale, title, body) => {
    expect(writeRideAlertMessage(notification, locale)).toMatchObject({
      title,
      body,
    });
  });

  it("keeps the wait on screen under a long ride name", () => {
    const title = writeRideAlertMessage(
      {
        ...notification,
        attractionName: "Harry Potter and the Forbidden Journey",
      },
      "de",
    ).title;
    expect(title.endsWith(": 15 Min.")).toBe(true);
    expect([...title].length).toBeLessThanOrEqual(
      TITLE_MAX_CHARS - PUSH_PICTOGRAM_CHARS,
    );
  });

  it("carries the url and dedupe key through", () => {
    const message = writeRideAlertMessage(notification, "en");
    expect(message.url).toBe(notification.url);
    expect(message.tag).toBe(notification.dedupeKey);
  });
});

describe("writeRideReopenMessage", () => {
  const notification = {
    dedupeKey: "ride-alert:alert-1",
    attractionName: "Taron",
    parkName: "Phantasialand",
    url: "/parks/europe/germany/bruehl/phantasialand/taron",
  };

  it.each([
    ["de", "Taron ist wieder offen"],
    ["en", "Taron is open again"],
    ["nl", "Taron is weer open"],
    ["fr", "Taron a rouvert"],
    ["es", "Taron vuelve a estar abierta"],
    ["it", "Taron è di nuovo aperta"],
    ["xx", "Taron is open again"],
  ])("writes %s", (locale, title) => {
    const message = writeRideReopenMessage(notification, locale);
    expect(message.title).toBe(title);
    expect(message.subtitle).toBe("Phantasialand");
    expect(message.body).toBe("");
    expect(message.tag).toBe(notification.dedupeKey);
  });
});

describe("writeRainSoonMessage", () => {
  const base = {
    dedupeKey: "rain-soon:park-1:2026-10-17",
    parkName: "Phantasialand",
    atTime: "14:20",
    url: "/parks/europe/germany/bruehl/phantasialand",
  };
  const rides = [
    { name: "Taron", waitTime: 15 },
    { name: "Maus au Chocolat", waitTime: 20 },
  ];

  it("names the time, the park and the covered rides with their wait", () => {
    const message = writeRainSoonMessage({ ...base, rides }, "de");
    expect(message.title).toBe("Regen ab etwa 14:20 Uhr");
    expect(message.subtitle).toBe("Phantasialand");
    expect(message.body).toBe(
      "Überdacht und offen: Taron (15 Min.), Maus au Chocolat (20 Min.)",
    );
    expect(message.tag).toBe(base.dedupeKey);
    expect(message.url).toBe(base.url);
  });

  it("is only the warning when no ride is named", () => {
    const message = writeRainSoonMessage({ ...base, rides: [] }, "en");
    expect(message.title).toBe("Rain from about 14:20");
    expect(message.subtitle).toBe("Phantasialand");
    expect(message.body).toBe("");
  });

  it("writes a ride without a posted wait as its name alone", () => {
    const message = writeRainSoonMessage(
      { ...base, rides: [{ name: "Taron", waitTime: null }] },
      "en",
    );
    expect(message.body).toBe("Covered and open: Taron");
  });

  it("sets the French colon with its space", () => {
    expect(writeRainSoonMessage({ ...base, rides }, "fr").body).toBe(
      "Couvert et ouvert : Taron (15 min), Maus au Chocolat (20 min)",
    );
  });

  it("is written in all six languages and falls back to English", () => {
    const titles = LOCALES.map(
      (l) => writeRainSoonMessage({ ...base, rides }, l).title,
    );
    expect(new Set(titles).size).toBe(6);
    for (const title of titles) expect(title).toContain("14:20");
    expect(writeRainSoonMessage({ ...base, rides }, "pt").title).toBe(
      "Rain from about 14:20",
    );
    expect(writeRainSoonMessage({ ...base, rides }, "nl-BE").title).toBe(
      "Regen vanaf ongeveer 14:20 uur",
    );
  });
});

describe("writeMorningPlanMessage", () => {
  const copy = {
    dedupeKey: "morning-plan:phantasialand:2026-10-17",
    parkName: "Phantasialand",
    opensAt: "09:00",
    entries: [
      { name: "Taron", atTime: "09:30" },
      { name: "Black Mamba", atTime: "10:00" },
      { name: "Chiapas", atTime: "10:40" },
    ],
    crowdLevel: "low",
    tempMax: 14.4,
    rainChance: 20,
    url: "/",
  };
  // Intl writes the space before a unit as a (narrow) no-break space.
  const plain = (text: string) => text.replace(/[  ]/g, " ");

  it.each([
    [
      "de",
      "Phantasialand öffnet um 09:00 Uhr",
      "Guten Morgen",
      "Andrang niedrig, bis 14 °C, 20 % Regenrisiko. Zuerst Taron um 09:30 Uhr, dann Black Mamba.",
    ],
    [
      "en",
      "Phantasialand opens at 09:00",
      "Good morning",
      "Crowds low, up to 14°C, 20% chance of rain. First Taron at 09:30, then Black Mamba.",
    ],
    [
      "nl",
      "Phantasialand opent om 09:00 uur",
      "Goedemorgen",
      "Drukte laag, tot 14°C, 20% kans op regen. Eerst Taron om 09:30 uur, daarna Black Mamba.",
    ],
    [
      "fr",
      "Phantasialand ouvre à 09:00",
      "Bonjour",
      "Affluence faible, jusqu’à 14 °C, 20 % de risque de pluie. D’abord Taron à 09:30, puis Black Mamba.",
    ],
    [
      "es",
      "Phantasialand abre a las 09:00",
      "Buenos días",
      "Afluencia baja, hasta 14 °C, 20 % de probabilidad de lluvia. Primero Taron a las 09:30, luego Black Mamba.",
    ],
    [
      "it",
      "Phantasialand apre alle 09:00",
      "Buongiorno",
      "Affluenza bassa, fino a 14 °C, 20% di probabilità di pioggia. Prima Taron alle 09:30, poi Black Mamba.",
    ],
  ])("writes %s", (locale, title, subtitle, body) => {
    const message = writeMorningPlanMessage(copy, locale);
    expect(message.title).toBe(title);
    expect(message.subtitle).toBe(subtitle);
    expect(plain(message.body)).toBe(body);
    expect(message.tag).toBe(copy.dedupeKey);
  });

  it("says only what is known: no level for `unknown`, no weather without one", () => {
    const bare = {
      ...copy,
      crowdLevel: "unknown",
      tempMax: null,
      rainChance: undefined,
    };
    expect(writeMorningPlanMessage(bare, "de-AT").body).toBe(
      "Zuerst Taron um 09:30 Uhr, dann Black Mamba.",
    );
    expect(
      plain(
        writeMorningPlanMessage(
          { ...bare, tempMax: 21, entries: [copy.entries[0]] },
          "pt",
        ).body,
      ),
    ).toBe("Up to 21°C. First Taron at 09:30.");
  });
});

describe("every kind, in every language", () => {
  const plan = {
    dedupeKey: "plan-change:phantasialand:2026-10-17",
    parkName: "Phantasialand",
    date: "2026-10-17",
    isToday: true,
    url: "/trip-planner",
    changes: [
      { type: "ride-down" as const, name: "Taron" },
      { type: "wait" as const, name: "Chiapas", at: "14:00", from: 30, to: 50 },
    ],
  };
  const ride = {
    dedupeKey: "ride-alert:a",
    attractionName: "Taron",
    parkName: "Phantasialand",
    waitTime: 15,
    thresholdMinutes: 20,
    url: "/",
  };
  const start = {
    dedupeKey: "next-up:p:2026-10-17:e:600",
    parkName: "Phantasialand",
    what: "Taron",
    inMinutes: 10,
    atTime: "10:00",
    url: "/",
  };

  it.each(LOCALES)(
    "%s: exactly the kind's pictogram, once, leading the title; no exclamation mark, no em dash",
    (locale) => {
      const messages = [
        writeMessage(start, locale),
        writeMessage({ ...start, dedupeKey: "show-start:s:t" }, locale),
        { ...writeRideAlertMessage(ride, locale), kind: "ride-wait" as const },
        {
          ...writeRideReopenMessage(ride, locale),
          kind: "ride-reopen" as const,
        },
        ...POST_FEEDS.map((feed) =>
          writeNewsMessage(
            { slug: "s", title: "Titel", teaser: "Text.", publishedAt: "" },
            feed,
            locale,
          ),
        ),
        writeRainSoonMessage(
          {
            ...start,
            dedupeKey: "rain-soon:p:2026-10-17",
            atTime: "14:20",
            rides: [{ name: "Fly", waitTime: 5 }],
          },
          locale,
        ),
        writeMorningPlanMessage(
          {
            ...start,
            dedupeKey: "morning-plan:p:2026-10-17",
            opensAt: "09:00",
            entries: [{ name: "Taron", atTime: "09:30" }],
            crowdLevel: "extreme",
            tempMax: 30,
            rainChance: 0,
          },
          locale,
        ),
        writePlanChangeMessage(plan, locale),
      ];
      const kinds = new Set<string>();
      for (const written of messages) {
        const message = withPictogram(written);
        const kind = pushKindOf(written) as DevicePushKind;
        kinds.add(kind);
        const text = `${message.title}|${message.subtitle}|${message.body}`;
        expect(message.title.startsWith(`${PUSH_PICTOGRAMS[kind]} `)).toBe(
          true,
        );
        expect(text.match(/\p{Extended_Pictographic}/gu)).toEqual([
          PUSH_PICTOGRAMS[kind],
        ]);
        expect(message.title.length).toBeGreaterThan(0);
        expect(message.subtitle).toBeTruthy();
        expect(text).not.toMatch(/undefined|null|NaN|[!—]/u);
        // A title is a headline: no full stop but an abbreviation's.
        expect(message.title).not.toMatch(/[a-zà-ü]{4,}\.$/);
      }
      expect([...kinds].sort()).toEqual([...DEVICE_PUSH_KINDS].sort());
    },
  );
});
