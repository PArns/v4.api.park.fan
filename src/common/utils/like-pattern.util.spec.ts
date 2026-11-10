import { escapeLike, likeContains, likeStartsWith } from "./like-pattern.util";

describe("like patterns", () => {
  it("escapes the wildcards and the escape character, nothing else", () => {
    expect(escapeLike("50%_off\\x")).toBe("50\\%\\_off\\\\x");
    expect(escapeLike("Taron")).toBe("Taron");
  });

  it("wraps for a substring or a prefix match", () => {
    expect(likeContains("__")).toBe("%\\_\\_%");
    expect(likeStartsWith("park.")).toBe("park.%");
  });
});
