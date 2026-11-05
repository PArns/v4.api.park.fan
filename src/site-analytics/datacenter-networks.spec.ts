import {
  DATACENTER_ASNS,
  NEVER_DATACENTER,
  networkOf,
} from "./datacenter-networks";
import {
  datacenterVisitorScope,
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
    expect(visitorScopesOf("all", "exclude")).toEqual(["all"]);
    expect(visitorScopesOf("all", "only")).toEqual(["dca"]);
    expect(visitorScopesOf("nl", "include")).toEqual(["nl", "dnl"]);
    for (const scope of ["all", "en", "de", "nl", "fr", "es", "it"]) {
      expect(datacenterVisitorScope(scope)).toHaveLength(3);
      expect(datacenterVisitorScope(scope)).not.toBe(scope);
    }
  });
});
