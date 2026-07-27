import { SIX_FLAGS_PARK_SLUGS, sixFlagsSlugFor } from "./six-flags.parks";

describe("SIX_FLAGS_PARK_SLUGS", () => {
  it("maps the former Cedar Fair parks, which now live on sixflags.com", () => {
    expect(sixFlagsSlugFor("cedar-point")).toBe("cedarpoint");
    expect(sixFlagsSlugFor("kings-island")).toBe("kingsisland");
  });

  it("leaves out parks that only look like Six Flags properties", () => {
    // Hersheypark is Hershey Entertainment, Dollywood is Herschend. Both were
    // in the candidate list until the landing pages came back 404.
    expect(sixFlagsSlugFor("hersheypark")).toBeNull();
    expect(sixFlagsSlugFor("dollywood")).toBeNull();
  });

  it("treats an unknown park as not-a-Six-Flags-park", () => {
    expect(sixFlagsSlugFor("phantasialand")).toBeNull();
  });

  it("uses bare site slugs, since they go straight into a URL", () => {
    for (const slug of Object.values(SIX_FLAGS_PARK_SLUGS)) {
      expect(slug).toMatch(/^[a-z0-9]+$/);
    }
  });
});
