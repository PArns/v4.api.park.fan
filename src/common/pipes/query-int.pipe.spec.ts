import { BadRequestException } from "@nestjs/common";
import { QueryIntPipe } from "./query-int.pipe";

describe("QueryIntPipe", () => {
  const pipe = new QueryIntPipe({
    name: "limit",
    fallback: 10,
    min: 0,
    max: 100,
  });

  it("returns the fallback when the parameter is omitted", () => {
    expect(pipe.transform(undefined)).toBe(10);
    expect(pipe.transform(null)).toBe(10);
  });

  it.each(["", " ", "  \t"])(
    "returns the fallback for the empty value %j",
    (value) => {
      expect(pipe.transform(value)).toBe(10);
    },
  );

  it("returns undefined for an empty value when there is no fallback", () => {
    expect(new QueryIntPipe({ name: "days" }).transform("")).toBeUndefined();
  });

  it("keeps 0 as a real value", () => {
    expect(pipe.transform("0")).toBe(0);
  });

  it("parses integers and clamps them into range", () => {
    expect(pipe.transform("42")).toBe(42);
    expect(pipe.transform("500")).toBe(100);
    expect(new QueryIntPipe({ name: "page", min: 1 }).transform("0")).toBe(1);
  });

  it.each(["abc", "1.5", "Infinity"])("rejects %j with 400", (value) => {
    expect(() => pipe.transform(value)).toThrow(BadRequestException);
  });
});
