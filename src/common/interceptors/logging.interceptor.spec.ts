import { isAddressFreePath } from "./logging.interceptor";

/** The reader counters promise readers that no log line keeps their address. */
describe("isAddressFreePath", () => {
  it("covers the page-view counter and the site statistic's collect endpoint", () => {
    expect(isAddressFreePath("/v1/page-views")).toBe(true);
    expect(isAddressFreePath("/v1/page-views/top?kind=news")).toBe(true);
    expect(isAddressFreePath("/v1/analytics/collect")).toBe(true);
  });

  it("leaves every other route's address in its line", () => {
    expect(isAddressFreePath("/v1/analytics/realtime")).toBe(false);
    expect(isAddressFreePath("/v1/admin/analytics/overview")).toBe(false);
  });
});
