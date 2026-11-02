import { junkReason, repeatsLastQuestion } from "./assistant-junk";

describe("junkReason", () => {
  it("lets every kind of real question through", () => {
    for (const question of [
      "Magic kingdom",
      "Wie lang ist die Wartezeit bei Taron?",
      "TTD",
      "BTMRR TTD",
      "rhythm",
      "1",
      "ja",
      "Ok!",
      "Efteling 🎢",
      "東京ディズニーランド",
      "Ist Phantasialand am 24.12. offen?",
    ]) {
      expect(junkReason(question)).toBeNull();
    }
  });

  it("refuses what has nothing to read", () => {
    expect(junkReason("???")).toBe("no-words");
    expect(junkReason("🙂🙂🙂")).toBe("no-words");
    expect(junkReason("   ...  ")).toBe("no-words");
  });

  it("refuses one key held down", () => {
    expect(junkReason("aaaaaaaa")).toBe("one-key");
    expect(junkReason("k")).toBe("one-key");
    expect(junkReason("xx XX")).toBe("one-key");
  });

  it("refuses letters typed without a vowel", () => {
    expect(junkReason("sdfghjk")).toBe("mash");
    expect(junkReason("hjkl jkhg")).toBe("mash");
    expect(junkReason("dfg")).toBeNull();
  });
});

describe("repeatsLastQuestion", () => {
  const now = new Date("2026-11-02T12:00:00Z");
  const at = (seconds: number) => new Date(now.getTime() - seconds * 1000);

  it("is a repeat when the same question was just answered", () => {
    const earlier = [
      { role: "user", text: "Wartezeit Taron", createdAt: at(30) },
      { role: "assistant", text: "15 Minuten.", createdAt: at(25) },
    ];
    expect(repeatsLastQuestion(" wartezeit  taron ", earlier, now)).toBe(true);
  });

  it("is no repeat after a failed answer, minutes later, or for another question", () => {
    const failed = [
      { role: "user", text: "Wartezeit Taron", createdAt: at(30) },
      { role: "assistant", text: "", createdAt: at(25) },
    ];
    expect(repeatsLastQuestion("Wartezeit Taron", failed, now)).toBe(false);
    const later = [
      { role: "user", text: "Wartezeit Taron", createdAt: at(300) },
      { role: "assistant", text: "15 Minuten.", createdAt: at(295) },
    ];
    expect(repeatsLastQuestion("Wartezeit Taron", later, now)).toBe(false);
    expect(repeatsLastQuestion("Und Black Mamba?", failed, now)).toBe(false);
  });
});
