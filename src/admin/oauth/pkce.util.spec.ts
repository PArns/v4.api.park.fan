import {
  isValidCodeChallenge,
  isValidCodeVerifier,
  s256Challenge,
  verifyPkceS256,
} from "./pkce.util";

// RFC 7636 Appendix B.
const VERIFIER = "dBjftJeZ4CVP-mB92K27uhbUJU1p1r_wW1gFWFOEjXk";
const CHALLENGE = "E9Melhoa2OwvFrEMTJguCHaoeK1t8URWbuGJSstw-cM";

describe("PKCE S256", () => {
  it("derives the RFC 7636 test vector", () => {
    expect(s256Challenge(VERIFIER)).toBe(CHALLENGE);
  });

  it("accepts the verifier a challenge was made from", () => {
    expect(verifyPkceS256(VERIFIER, CHALLENGE)).toBe(true);
  });

  it("refuses any other verifier", () => {
    expect(verifyPkceS256(`${VERIFIER.slice(0, -1)}A`, CHALLENGE)).toBe(false);
  });

  it("refuses the plain method: the challenge itself is not a verifier", () => {
    expect(verifyPkceS256(CHALLENGE, CHALLENGE)).toBe(false);
  });

  it("refuses a missing or malformed verifier", () => {
    expect(verifyPkceS256(undefined, CHALLENGE)).toBe(false);
    expect(verifyPkceS256("short", CHALLENGE)).toBe(false);
    expect(verifyPkceS256(`${"a".repeat(42)} `, CHALLENGE)).toBe(false);
    expect(isValidCodeVerifier("a".repeat(129))).toBe(false);
    expect(isValidCodeVerifier("a".repeat(43))).toBe(true);
  });

  it("knows an S256 challenge by its shape", () => {
    expect(isValidCodeChallenge(CHALLENGE)).toBe(true);
    expect(isValidCodeChallenge(CHALLENGE.slice(1))).toBe(false);
    expect(isValidCodeChallenge(`${CHALLENGE.slice(1)}=`)).toBe(false);
    expect(isValidCodeChallenge(undefined)).toBe(false);
  });
});
