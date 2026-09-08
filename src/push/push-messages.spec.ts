import { writeMessage, writeRideAlertMessage } from "./push-messages";

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
