import {
  hashPassword,
  needsRehash,
  validatePasswordStrength,
  verifyPassword,
} from "./password.util";

describe("password.util", () => {
  // scrypt at production parameters costs ~100 ms per call by design, and
  // these tests make a dozen of them.
  jest.setTimeout(30_000);

  it("round-trips a password", async () => {
    const hash = await hashPassword("correct horse battery staple");
    expect(await verifyPassword("correct horse battery staple", hash)).toBe(
      true,
    );
    expect(await verifyPassword("Correct horse battery staple", hash)).toBe(
      false,
    );
  });

  it("salts each hash, so two identical passwords do not share a digest", async () => {
    const a = await hashPassword("the same password");
    const b = await hashPassword("the same password");
    expect(a).not.toEqual(b);
    expect(await verifyPassword("the same password", a)).toBe(true);
    expect(await verifyPassword("the same password", b)).toBe(true);
  });

  it("stores its parameters, so the cost can be raised without invalidating old hashes", async () => {
    const hash = await hashPassword("whatever");
    const [scheme, n, r, p] = hash.split("$");
    expect(scheme).toBe("scrypt");
    expect(Number(n)).toBeGreaterThanOrEqual(32768);
    expect(Number(r)).toBe(8);
    expect(Number(p)).toBe(1);
  });

  it("normalises unicode, so a password typed on two keyboards still matches", async () => {
    // "é" as one codepoint vs. "e" + combining acute — the same password to a
    // person, two different byte strings to scrypt.
    const composed = "café-passphrase";
    const decomposed = "café-passphrase";
    // Guards the test itself: if an editor ever normalises this file, the
    // two literals collapse into one and the assertion below proves nothing.
    expect(composed).not.toBe(decomposed);
    const hash = await hashPassword(composed);
    expect(await verifyPassword(decomposed, hash)).toBe(true);
  });

  it.each([
    ["not a hash at all", "garbage"],
    ["a hash with too few fields", "scrypt$32768$8$salt"],
    ["an unknown scheme", "bcrypt$32768$8$1$c2FsdA==$aGFzaA=="],
    ["a non-numeric cost", "scrypt$abc$8$1$c2FsdA==$aGFzaA=="],
    ["an empty string", ""],
  ])("returns false rather than throwing for %s", async (_label, stored) => {
    await expect(verifyPassword("anything", stored)).resolves.toBe(false);
  });

  it("flags a hash written at weaker parameters for rehashing", () => {
    expect(needsRehash("scrypt$16384$8$1$c2FsdA==$aGFzaA==")).toBe(true);
    expect(needsRehash("scrypt$32768$8$1$c2FsdA==$aGFzaA==")).toBe(false);
    expect(needsRehash("garbage")).toBe(true);
  });

  describe("strength policy", () => {
    it("asks for length and nothing else", () => {
      expect(validatePasswordStrength("correct horse battery")).toBeNull();
      // No composition rules: this is 12 lowercase letters and it passes,
      // deliberately — "Password1!" is shorter and worse.
      expect(validatePasswordStrength("abcdefghijkl")).toBeNull();
    });

    it("rejects short, empty and absurd inputs", () => {
      expect(validatePasswordStrength("short")).toMatch(/12 characters/);
      expect(validatePasswordStrength("            ")).toMatch(/whitespace/);
      expect(validatePasswordStrength("x".repeat(300))).toMatch(/at most/);
    });
  });
});
