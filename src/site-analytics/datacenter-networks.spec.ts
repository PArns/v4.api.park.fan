import {
  DATACENTER_ASNS,
  NEVER_DATACENTER,
  networkOf,
} from "./datacenter-networks";
import {
  automatedVisitorScope,
  datacenterVisitorScope,
  isAutomatedStart,
  markedNetwork,
  markedVisitorScopes,
  visitorScopeFor,
  visitorScopesOf,
} from "./site-analytics.constants";

/**
 * What is pinned: the list never takes a network whose readers are people
 * (Private Relay, WARP, VPN providers), a reader's network is unknown only
 * without the database, and the visitor scopes stay disjoint and three
 * characters wide, the width of the scope columns.
 */
describe("datacenter networks", () => {
  it("never lists a network whose readers are mostly people", () => {
    for (const asn of NEVER_DATACENTER.keys()) {
      expect(DATACENTER_ASNS.has(asn)).toBe(false);
    }
  });

  it("names an operator for every network", () => {
    for (const [asn, operator] of DATACENTER_ASNS) {
      expect(Number.isInteger(asn) && asn > 0).toBe(true);
      expect(operator.trim()).not.toBe("");
    }
  });

  it("reads a network from the ASN, and unknown only without the database", () => {
    expect(networkOf(true, 16509)).toBe("datacenter");
    expect(networkOf(true, 3320)).toBe("other");
    expect(networkOf(true, null)).toBe("other");
    expect(networkOf(false, 16509)).toBe("unknown");
  });

  it("counts a data centre's visitors under scopes of their own", () => {
    expect(datacenterVisitorScope("all")).toBe("dca");
    expect(datacenterVisitorScope("de")).toBe("dde");
    expect(visitorScopeFor("de", "datacenter")).toBe("dde");
    expect(visitorScopeFor("de", "other")).toBe("de");
    expect(visitorScopeFor("all", "unknown")).toBe("all");
    const choice = (datacenter: "exclude" | "include" | "only") => ({
      datacenter,
      automated: "exclude" as const,
    });
    expect(visitorScopesOf("all", choice("exclude"))).toEqual(["all"]);
    expect(visitorScopesOf("all", choice("only"))).toEqual(["dca"]);
    expect(visitorScopesOf("nl", choice("include"))).toEqual(["nl", "dnl"]);
    for (const scope of ["all", "en", "de", "nl", "fr", "es", "it"]) {
      expect(datacenterVisitorScope(scope)).toHaveLength(3);
      expect(datacenterVisitorScope(scope)).not.toBe(scope);
    }
  });

  it("marks a visit automated only when it opens without a referrer on a page in no language the browser names", () => {
    const english = new Set(["en"]);
    expect(isAutomatedStart({ locale: "es", referrer: null }, english)).toBe(
      true,
    );
    // A request that names no language says nothing either way.
    expect(isAutomatedStart({ locale: "es", referrer: null }, new Set())).toBe(
      false,
    );
    expect(
      isAutomatedStart({ locale: "es", referrer: "google" }, english),
    ).toBe(false);
    expect(isAutomatedStart({ locale: "en", referrer: null }, new Set())).toBe(
      false,
    );
    expect(
      isAutomatedStart({ locale: "es", referrer: null }, new Set(["en", "es"])),
    ).toBe(false);
  });

  it("lets a data centre stay one, and marks any other network automated", () => {
    expect(markedNetwork("datacenter", true)).toBe("datacenter");
    expect(markedNetwork("other", true)).toBe("automated");
    expect(markedNetwork("unknown", true)).toBe("automated");
    expect(markedNetwork("other", false)).toBe("other");
  });

  it("counts an automated visit's readers under scopes of their own, apart from the data centres'", () => {
    expect(automatedVisitorScope("all")).toBe("aca");
    expect(automatedVisitorScope("de")).toBe("ade");
    expect(visitorScopeFor("de", "automated")).toBe("ade");
    expect(markedVisitorScopes("nl")).toEqual(["nl", "dnl", "anl"]);
    const of = (
      datacenter: "exclude" | "include" | "only",
      automated: "exclude" | "include" | "only",
    ) => visitorScopesOf("all", { datacenter, automated });
    expect(of("exclude", "exclude")).toEqual(["all"]);
    expect(of("exclude", "include")).toEqual(["all", "aca"]);
    expect(of("include", "include")).toEqual(["all", "dca", "aca"]);
    expect(of("exclude", "only")).toEqual(["aca"]);
    expect(of("include", "only")).toEqual(["aca"]);
    expect(of("only", "only")).toEqual(["dca", "aca"]);
    const marked = ["all", "en", "de", "nl", "fr", "es", "it"].flatMap(
      markedVisitorScopes,
    );
    expect(new Set(marked).size).toBe(marked.length);
    for (const scope of marked) expect(scope.length).toBeLessThanOrEqual(3);
  });
});
