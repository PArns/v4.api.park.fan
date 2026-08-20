import { CallHandler, ExecutionContext } from "@nestjs/common";
import { of, firstValueFrom } from "rxjs";
import { ExcludeNullInterceptor } from "./exclude-null.interceptor";

function ctxWithQuery(
  query: Record<string, unknown> = {},
  url = "/v1/parks/europe/germany/bruehl/phantasialand",
): ExecutionContext {
  return {
    switchToHttp: () => ({ getRequest: () => ({ query, url }) }),
  } as unknown as ExecutionContext;
}

const handlerOf = (body: unknown): CallHandler => ({ handle: () => of(body) });

const run = (
  interceptor: ExcludeNullInterceptor,
  body: unknown,
  query = {},
  url?: string,
) =>
  firstValueFrom(
    interceptor.intercept(ctxWithQuery(query, url), handlerOf(body)),
  );

describe("ExcludeNullInterceptor", () => {
  const interceptor = new ExcludeNullInterceptor();

  it("drops null-valued object keys but keeps everything else", async () => {
    const out = (await run(interceptor, {
      a: 1,
      b: null,
      c: "x",
      d: 0,
      e: false,
    })) as Record<string, unknown>;

    expect(out).toEqual({ a: 1, c: "x", d: 0, e: false });
    expect("b" in out).toBe(false);
  });

  it("recurses into nested objects", async () => {
    const out = await run(interceptor, {
      park: { id: "p1", name: null, nested: { keep: 1, drop: null } },
    });

    expect(out).toEqual({ park: { id: "p1", nested: { keep: 1 } } });
  });

  it("strips null keys from objects inside arrays", async () => {
    const out = await run(interceptor, {
      attractions: [
        { id: "a1", url: null },
        { id: "a2", url: "u" },
      ],
    });

    expect(out).toEqual({
      attractions: [{ id: "a1" }, { id: "a2", url: "u" }],
    });
  });

  it("serializes identically to the legacy behaviour (null array slots → null)", async () => {
    const out = await run(interceptor, { list: [1, null, 2], drop: null });
    // The wire contract is the JSON, not the in-memory null/undefined.
    expect(JSON.stringify(out)).toBe('{"list":[1,null,2]}');
  });

  it("leaves Date values untouched", async () => {
    const d = new Date("2026-01-01T00:00:00.000Z");
    const out = (await run(interceptor, { when: d, gone: null })) as {
      when: Date;
    };
    expect(out.when).toBe(d);
    expect(out).toEqual({ when: d });
  });

  it("passes primitives through unchanged", async () => {
    expect(await run(interceptor, 42)).toBe(42);
    expect(await run(interceptor, "hi")).toBe("hi");
    expect(await run(interceptor, false)).toBe(false);
  });

  it("returns undefined for a top-level null", async () => {
    expect(await run(interceptor, null)).toBeUndefined();
  });

  it("skips null removal entirely when ?debug=true", async () => {
    const body = { a: 1, b: null };
    const out = await run(interceptor, body, { debug: "true" });
    expect(out).toBe(body); // untouched, nulls preserved
    expect((out as Record<string, unknown>).b).toBeNull();
  });

  describe("the admin surface is exempt", () => {
    // On /v1/admin/* a null is data, not clutter. The curated-field editor
    // reads syncedValue / curatedValue / resolvedValue, each of which is null
    // exactly when there is nothing there — which is the common case and the
    // most informative one. Stripped, the editor cannot tell "no correction on
    // this field" from "this field does not exist".
    const curatedField = {
      key: "curatedMinimumHeight",
      syncedValue: 100,
      curatedValue: null,
      resolvedValue: 100,
      overridden: false,
    };

    it("keeps nulls under /v1/admin", async () => {
      const out = await run(
        interceptor,
        { fields: [curatedField] },
        {},
        "/v1/admin/content/attractions/abc",
      );
      expect(out).toEqual({ fields: [curatedField] });
    });

    it("keeps a season's null `dates`, which means 'every day'", async () => {
      // Null here is not an absent field: it is the difference between a
      // season that runs daily and one that runs on listed days only.
      const out = (await run(
        interceptor,
        { startDate: "2026-10-03", endDate: "2026-11-01", dates: null },
        {},
        "/v1/admin/content/seasons/s1",
      )) as Record<string, unknown>;
      expect("dates" in out).toBe(true);
      expect(out.dates).toBeNull();
    });

    it("still strips on a public path", async () => {
      const out = (await run(
        interceptor,
        { name: "Taron", curatedName: null },
        {},
        "/v1/parks/europe/germany/bruehl/phantasialand",
      )) as Record<string, unknown>;
      expect("curatedName" in out).toBe(false);
    });

    it("does not mistake a slug containing 'admin' for the admin surface", async () => {
      const out = (await run(
        interceptor,
        { name: "Badminton Park", note: null },
        {},
        "/v1/parks/europe/uk/london/badminton-park",
      )) as Record<string, unknown>;
      expect("note" in out).toBe(false);
    });

    it("matches the admin path even with a query string attached", async () => {
      const out = (await run(
        interceptor,
        { curatedValue: null },
        {},
        "/v1/admin/content/parks?q=phantasialand",
      )) as Record<string, unknown>;
      expect("curatedValue" in out).toBe(true);
    });
  });
});
