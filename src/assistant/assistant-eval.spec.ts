import { ASSISTANT_TOOL_PARAMS } from "./tools/assistant-tools";
import {
  alertsSnapshotSchema,
  interviewSnapshotSchema,
  tripsSnapshotSchema,
  plannerSnapshotSchema,
} from "./assistant.schemas";
import { judge } from "./assistant-eval";
import { EVAL_CASES, type EvalCase } from "./assistant-eval.fixtures";

const base: EvalCase = {
  id: "t",
  locale: "en",
  pagePath: "/en",
  message: "hi",
};

describe("assistant eval", () => {
  it("passes the wanted calls in order, after the allowed lookups, inputs as a subset", () => {
    const c: EvalCase = {
      ...base,
      allowBefore: ["find_rides"],
      want: [
        { tool: "search_posts", input: { kind: "article" } },
        { tool: ["get_post", "get_ride"], input: { question: "*" } },
      ],
    };
    const post = { name: "get_post", input: { slug: "a", question: "q" } };
    expect(
      judge(c, [
        { name: "find_rides", input: {} },
        { name: "search_posts", input: { kind: "article", query: "x" } },
        post,
      ]),
    ).toBeNull();
    expect(
      judge(c, [{ name: "search_posts", input: { kind: "news" } }]),
    ).toMatch(/search_posts with/);
    expect(
      judge(c, [{ name: "search_posts", input: { kind: "article" } }]),
    ).toBe("no get_post/get_ride call");
    expect(
      judge(c, [
        { name: "search_posts", input: { kind: "article" } },
        { name: "get_post", input: { slug: "a" } },
      ]),
    ).toMatch(/get_post with/);
  });

  it("checks the answer where a case asks: a rule applies only when its trigger matches", () => {
    const c: EvalCase = {
      ...base,
      answer: [{ when: "magic kingdom", must: "18[:.]00" }],
    };
    expect(judge(c, [], "Animal Kingdom is your best pick.")).toBeNull();
    expect(
      judge(
        c,
        [],
        "Magic Kingdom, but day guests leave at 18:00 for the party.",
      ),
    ).toBeNull();
    expect(judge(c, [], "Go to Magic Kingdom, it is the quietest.")).toMatch(
      /answer without/,
    );
  });

  it("fails a forbidden call: listed, any action, or any tool at all", () => {
    expect(judge({ ...base, forbid: "any" }, [])).toBeNull();
    expect(
      judge({ ...base, forbid: "any" }, [{ name: "get_ride", input: {} }]),
    ).toBe("called get_ride");
    expect(
      judge({ ...base, forbid: "actions" }, [{ name: "get_ride", input: {} }]),
    ).toBeNull();
    expect(
      judge({ ...base, forbid: "actions" }, [
        { name: "plan_remove", input: {} },
      ]),
    ).toBe("called plan_remove");
    expect(
      judge({ ...base, forbid: ["end_chat"] }, [
        { name: "end_chat", input: {} },
      ]),
    ).toBe("called end_chat");
  });

  it("names only real tools and holds only snapshots the chat would accept", () => {
    const names = new Set(ASSISTANT_TOOL_PARAMS.map((t) => t.name));
    for (const c of EVAL_CASES) {
      const used = [
        ...(c.want ?? []).flatMap((w) => [w.tool].flat()),
        ...(c.allowBefore ?? []),
        ...(Array.isArray(c.forbid) ? c.forbid : []),
        ...Object.keys(c.results ?? {}),
      ];
      for (const name of used)
        expect([c.id, names.has(name)]).toEqual([c.id, true]);
      if (c.planner)
        expect(plannerSnapshotSchema.parse(c.planner)).toEqual({
          more: [],
          ...c.planner,
        });
      if (c.alerts)
        expect(alertsSnapshotSchema.parse(c.alerts)).toEqual(c.alerts);
      if (c.trips)
        expect(tripsSnapshotSchema.parse(c.trips)).toHaveLength(c.trips.length);
      if (c.interview)
        expect(interviewSnapshotSchema.parse(c.interview)).toEqual(c.interview);
    }
    expect(new Set(EVAL_CASES.map((c) => c.id)).size).toBe(EVAL_CASES.length);
  });
});
