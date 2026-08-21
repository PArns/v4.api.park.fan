import { statusWithoutLiveData } from "./no-live-data-status.util";

describe("statusWithoutLiveData", () => {
  it("keeps the optimism for a ride with no season on file", () => {
    expect(statusWithoutLiveData("OPERATING", null)).toBe("OPERATING");
    expect(statusWithoutLiveData("OPERATING", undefined)).toBe("OPERATING");
  });

  it("closes a ride the season says is out", () => {
    expect(statusWithoutLiveData("OPERATING", false)).toBe("CLOSED");
  });

  it("keeps a seasonal ride open inside its season", () => {
    expect(statusWithoutLiveData("OPERATING", true)).toBe("OPERATING");
  });

  it("closes everything below a closed park, season or no season", () => {
    expect(statusWithoutLiveData("CLOSED", true)).toBe("CLOSED");
    expect(statusWithoutLiveData("CLOSED", null)).toBe("CLOSED");
    expect(statusWithoutLiveData(null, null)).toBe("CLOSED");
  });

  it("does not treat UNKNOWN as an open park", () => {
    // The park-level UNKNOWN means we cannot read the park at all. Nothing
    // below it may claim to be running — the same direction the Hansa-Park
    // branch takes one level up.
    expect(statusWithoutLiveData("UNKNOWN", null)).toBe("CLOSED");
  });
});
