import {
  writeMorningPlanMessage,
  writeMessage,
  writeRideAlertMessage,
  writeRainSoonMessage,
  writeRideReopenMessage,
} from "./push-messages";

describe("writeMessage", () => {
  const notification = {
    dedupeKey: "next-up:phantasialand:2026-10-17:taron-1:600",
    parkName: "Phantasialand",
    what: "Taron",
    inMinutes: 15,
    atTime: "10:00",
    url: "/",
  };

  it("writes German for a German locale", () => {
    expect(writeMessage(notification, "de").title).toBe("In 15 Min.: Taron");
  });

  it("matches the base language for a regional tag", () => {
    // de-AT and de-CH are German, not a fourth language of their own.
    expect(writeMessage(notification, "de-AT").title).toBe("In 15 Min.: Taron");
  });

  it("falls back to English for a locale this table does not carry", () => {
    expect(writeMessage(notification, "pt").title).toBe("In 15 min: Taron");
    expect(writeMessage(notification, "xx").title).toBe("In 15 min: Taron");
  });

  it("carries the dedupe key through as the notification tag", () => {
    expect(writeMessage(notification, "en").tag).toBe(notification.dedupeKey);
  });

  it("accepts a show-follow notification — same shape, no `topic` field", () => {
    // ScheduledStartCopy has no `topic`; dueShowNotifications produces exactly
    // this shape, so this pins that writeMessage stays usable for it.
    const showNotification = {
      dedupeKey: "show-start:show-1:2026-10-17:2026-10-17T20:00:00.000Z",
      parkName: "Europa-Park",
      what: "Feuerwerk",
      inMinutes: 30,
      atTime: "20:00",
      url: "/parks/europe/germany/rust/europa-park#shows",
    };
    expect(writeMessage(showNotification, "de")).toMatchObject({
      title: "In 30 Min.: Feuerwerk",
      body: "20:00 Uhr, Europa-Park",
    });
  });
});

describe("writeRideAlertMessage", () => {
  const notification = {
    dedupeKey: "ride-alert:alert-1",
    attractionName: "Taron",
    parkName: "Phantasialand",
    waitTime: 15,
    url: "/parks/europe/germany/bruehl/phantasialand/taron",
  };

  it("names the ride and the current wait, in German by default", () => {
    expect(writeRideAlertMessage(notification, "de")).toMatchObject({
      title: "Taron: nur noch 15 Min.",
      body: "Phantasialand",
    });
  });

  it("falls back to English for an unknown locale", () => {
    expect(writeRideAlertMessage(notification, "xx").title).toBe(
      "Taron: only 15 min now",
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
    expect(message.body).toBe("Phantasialand");
    expect(message.tag).toBe(notification.dedupeKey);
  });
});

describe("writeRainSoonMessage", () => {
  const base = {
    dedupeKey: "rain-soon:park-1:2026-10-17:1",
    parkName: "Phantasialand",
    atTime: "14:20",
    url: "/parks/europe/germany/bruehl/phantasialand",
  };
  const rides = [
    { name: "Taron", waitTime: 15 },
    { name: "Maus au Chocolat", waitTime: 20 },
  ];

  it("names the park, the time and the covered rides with their wait", () => {
    const message = writeRainSoonMessage({ ...base, rides }, "de");
    expect(message.title).toBe("Regen ab etwa 14:20 Uhr");
    expect(message.body).toBe(
      "Phantasialand. Überdacht und offen: Taron (15 Min.), Maus au Chocolat (20 Min.)",
    );
    expect(message.tag).toBe(base.dedupeKey);
    expect(message.url).toBe(base.url);
  });

  it("is only the warning when no ride is named", () => {
    const message = writeRainSoonMessage({ ...base, rides: [] }, "en");
    expect(message.title).toBe("Rain from about 14:20");
    expect(message.body).toBe("Phantasialand");
  });

  it("writes a ride without a posted wait as its name alone", () => {
    const message = writeRainSoonMessage(
      { ...base, rides: [{ name: "Taron", waitTime: null }] },
      "en",
    );
    expect(message.body).toBe("Phantasialand. Covered and open: Taron");
  });

  it("is written in all six languages and falls back to English", () => {
    const titles = ["de", "en", "nl", "fr", "es", "it"].map(
      (l) => writeRainSoonMessage({ ...base, rides }, l).title,
    );
    expect(new Set(titles).size).toBe(6);
    for (const title of titles) expect(title).toContain("14:20");
    expect(writeRainSoonMessage({ ...base, rides }, "pt").title).toBe(
      "Rain from about 14:20",
    );
    expect(writeRainSoonMessage({ ...base, rides }, "nl-BE").title).toBe(
      "Regen vanaf ongeveer 14:20",
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
    ],
    url: "/",
  };

  it.each([
    ["de", "Phantasialand öffnet um 09:00 Uhr"],
    ["en", "Phantasialand opens at 09:00"],
    ["nl", "Phantasialand opent om 09:00"],
    ["fr", "Phantasialand ouvre à 09:00"],
    ["es", "Phantasialand abre a las 09:00"],
    ["it", "Phantasialand apre alle 09:00"],
    ["de-AT", "Phantasialand öffnet um 09:00 Uhr"],
    ["pt", "Phantasialand opens at 09:00"],
  ])("writes %s", (locale, title) => {
    const message = writeMorningPlanMessage(copy, locale);
    expect(message.title).toBe(title);
    expect(message.body).toBe("09:30 Taron\n10:00 Black Mamba");
    expect(message.tag).toBe(copy.dedupeKey);
  });
});
