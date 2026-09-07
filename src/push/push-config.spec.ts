import { isAllowedPushEndpoint, isPushTopic, PUSH_TOPICS } from "./push-config";

describe("isAllowedPushEndpoint", () => {
  const allowed = (url: string) => isAllowedPushEndpoint(new URL(url));

  it("accepts what the four push services actually issue", () => {
    expect(allowed("https://fcm.googleapis.com/fcm/send/eX9-abc")).toBe(true);
    expect(
      allowed("https://updates.push.services.mozilla.com/wpush/v2/gAAA"),
    ).toBe(true);
    expect(allowed("https://wns2-par02p.notify.windows.com/w/?token=Ab")).toBe(
      true,
    );
    expect(allowed("https://web.push.apple.com/QF1x2")).toBe(true);
  });

  it("refuses a host that only ends in the right string", () => {
    // The reason the match is on a dot boundary and not `endsWith`. Each of
    // these would be a POST from inside this network to somebody else's server.
    expect(allowed("https://web.push.apple.com.evil.test/x")).toBe(false);
    expect(allowed("https://fcm.googleapis.com.attacker.test/x")).toBe(false);
    expect(allowed("https://notfcm.googleapis.com.co/x")).toBe(false);
  });

  it("refuses the internal network and the obvious SSRF targets", () => {
    expect(allowed("https://ml-service:8000/predict")).toBe(false);
    expect(allowed("https://127.0.0.1/")).toBe(false);
    expect(allowed("https://169.254.169.254/latest/meta-data/")).toBe(false);
    expect(allowed("https://evil.test/collect?u=fcm.googleapis.com")).toBe(
      false,
    );
  });

  it("takes an extra service from the environment without losing the defaults", () => {
    const before = process.env.PUSH_ENDPOINT_HOSTS;
    process.env.PUSH_ENDPOINT_HOSTS = "push.example-browser.test";
    try {
      expect(allowed("https://a.push.example-browser.test/x")).toBe(true);
      // Read per call, not at import time — the same reason `getVapidConfig`
      // is lazy: ConfigModule assigns .env after this module is evaluated.
      expect(allowed("https://fcm.googleapis.com/fcm/send/x")).toBe(true);
    } finally {
      if (before === undefined) delete process.env.PUSH_ENDPOINT_HOSTS;
      else process.env.PUSH_ENDPOINT_HOSTS = before;
    }
  });
});

describe("isPushTopic", () => {
  it("accepts only what something sends", () => {
    for (const topic of PUSH_TOPICS) expect(isPushTopic(topic)).toBe(true);
    // Listed in the DTO example once, and nothing produces it.
    expect(isPushTopic("ride-down")).toBe(false);
    expect(isPushTopic("")).toBe(false);
    expect(isPushTopic(undefined)).toBe(false);
  });
});
