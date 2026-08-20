import {
  base32Decode,
  base32Encode,
  generateTotpSecret,
  totpCodeForStep,
  totpStep,
  totpUri,
  verifyTotp,
} from "./totp.util";

describe("totp.util", () => {
  describe("base32", () => {
    it("round-trips arbitrary bytes", () => {
      const input = Buffer.from([0x00, 0xff, 0x10, 0x7a, 0x5c, 0x01, 0x99]);
      expect(base32Decode(base32Encode(input))).toEqual(input);
    });

    it("decodes the RFC 4648 vectors", () => {
      expect(base32Encode(Buffer.from("f"))).toBe("MY");
      expect(base32Encode(Buffer.from("fo"))).toBe("MZXQ");
      expect(base32Encode(Buffer.from("foobar"))).toBe("MZXW6YTBOI");
    });

    it("rejects characters outside the alphabet", () => {
      expect(() => base32Decode("MZXW6YTBOI!")).toThrow();
    });
  });

  describe("verification", () => {
    // RFC 6238 test vector: the ASCII secret "12345678901234567890" at
    // T = 59 s (step 1) produces 287082 for SHA-1/6 digits.
    const RFC_SECRET = base32Encode(Buffer.from("12345678901234567890"));

    it("matches the RFC 6238 vector", () => {
      expect(totpCodeForStep(RFC_SECRET, 1)).toBe("287082");
    });

    it("accepts the current code", () => {
      const at = 1_700_000_000_000;
      const code = totpCodeForStep(RFC_SECRET, totpStep(at));
      expect(verifyTotp(RFC_SECRET, code, null, at)).toEqual({
        valid: true,
        step: totpStep(at),
      });
    });

    it("forgives one step of clock drift in either direction", () => {
      const at = 1_700_000_000_000;
      const previous = totpCodeForStep(RFC_SECRET, totpStep(at) - 1);
      const next = totpCodeForStep(RFC_SECRET, totpStep(at) + 1);
      expect(verifyTotp(RFC_SECRET, previous, null, at).valid).toBe(true);
      expect(verifyTotp(RFC_SECRET, next, null, at).valid).toBe(true);
    });

    it("refuses two steps out", () => {
      const at = 1_700_000_000_000;
      const stale = totpCodeForStep(RFC_SECRET, totpStep(at) - 2);
      expect(verifyTotp(RFC_SECRET, stale, null, at).valid).toBe(false);
    });

    it("refuses a code whose step was already spent", () => {
      // The replay guard: without it a code observed once stays usable for the
      // rest of its window.
      const at = 1_700_000_000_000;
      const step = totpStep(at);
      const code = totpCodeForStep(RFC_SECRET, step);
      expect(verifyTotp(RFC_SECRET, code, null, at).valid).toBe(true);
      expect(verifyTotp(RFC_SECRET, code, step, at).valid).toBe(false);
    });

    it("refuses anything that is not six digits", () => {
      for (const bad of ["", "12345", "1234567", "abcdef", "12 34 56"]) {
        expect(verifyTotp(RFC_SECRET, bad, null).valid).toBe(false);
      }
    });

    it("returns false rather than throwing on a corrupt secret", () => {
      expect(verifyTotp("not!base32", "123456", null).valid).toBe(false);
    });
  });

  it("generates secrets an authenticator can read back", () => {
    const secret = generateTotpSecret();
    expect(secret).toMatch(/^[A-Z2-7]+$/);
    expect(base32Decode(secret)).toHaveLength(20);
  });

  it("builds an otpauth URI with the parameters the code was computed with", () => {
    const uri = totpUri("JBSWY3DPEHPK3PXP", "you@park.fan");
    expect(uri).toContain("otpauth://totp/park.fan%3Ayou%40park.fan");
    expect(uri).toContain("secret=JBSWY3DPEHPK3PXP");
    expect(uri).toContain("digits=6");
    expect(uri).toContain("period=30");
    expect(uri).toContain("algorithm=SHA1");
  });
});
