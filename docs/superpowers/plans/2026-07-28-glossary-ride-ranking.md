# Glossary Ride Ranking Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Let `GET /v1/glossary/terms/:termId/attractions` rank rides by their typical peak wait, so a glossary term page can lead with the rides that matter instead of starting alphabetically at "Adventureland Resort".

**Architecture:** `RideProfileService.findAttractionsByTerm` gains a `LEFT JOIN` onto `attraction_p90_baselines` — the 548-day P90 wait per attraction, computed nightly — and a `sort` mode that orders by confidence bucket, then P90 descending. The join surfaces two new DTO fields (`typicalPeakWait`, `isHeadliner`) so the frontend can label a ride "typisch bis 75 min". Ordering happens in SQL, not TypeScript, because `LIMIT` is applied before ordering — sorting after the fetch would return the top N of an arbitrary slice.

**Tech Stack:** NestJS, TypeORM query builder, PostgreSQL, Jest.

## Global Constraints

- **Default stays `park`.** An absent or unrecognised `sort` must produce today's byte-identical response. This endpoint is public and already consumed.
- **`LEFT JOIN`, never `INNER JOIN`.** Rides without a baseline sink to the bottom but stay in the list, so `total` is identical across sort modes. An inner join would report "151 Bahnen" on the overview and return 96.
- **Confidence bucket outranks the P90 value.** A ride with five samples must not lead the list on a fluke-high P90.
- **Every ordering ends with park name, then ride name.** Ties must be deterministic, or paging over the same term reshuffles between requests.
- **Column naming:** this repo has no global TypeORM naming strategy. `attraction_p90_baselines` declares no explicit column names, so its columns are camelCase and MUST be double-quoted in raw SQL fragments: `"attractionId"`, `"p90Baseline"`, `"isHeadliner"`, `confidence`.
- **Decimals arrive as strings.** `p90Baseline` is `decimal(10,2)`; the pg driver returns `"75.00"`. Convert with `Number(...)` and round before it reaches the DTO.
- **Comment the why, not the what.** This codebase explains non-obvious decisions in prose above the code. Match it.

---

### Task 1: Join the P90 baseline into the reverse lookup

Surfaces the ranking data. No ordering change yet — this task only makes the numbers available and proves they land in the DTO.

**Files:**
- Modify: `src/attractions/services/ride-profile.service.ts` (interface `AttractionWithTerm` ~line 27, `findAttractionsByTerm` ~line 207)
- Modify: `src/attractions/dto/ride-profile.dto.ts` (`TermAttractionDto`, `mapTermAttraction`)
- Test: `src/attractions/dto/ride-profile.dto.spec.ts` (exists — add cases)

**Interfaces:**
- Consumes: nothing from earlier tasks.
- Produces: `AttractionWithTerm` gains `typicalPeakWait: number | null` and `isHeadliner: boolean`. `TermAttractionDto` gains the same two fields. Task 2 orders by the joined columns; Task 3 exposes the mode.

- [ ] **Step 1: Write the failing test**

Append to `src/attractions/dto/ride-profile.dto.spec.ts`, inside the existing `describe("mapTermAttraction", …)` block, after the last `it(...)`:

```ts
  it("carries the typical peak wait through to the DTO", () => {
    const dto = mapTermAttraction({ ...row, typicalPeakWait: 75, isHeadliner: true });

    expect(dto.typicalPeakWait).toBe(75);
    expect(dto.isHeadliner).toBe(true);
  });

  it("reports a missing baseline as null rather than zero", () => {
    // A ride with no baseline has not been measured. Zero would read as "no
    // queue ever", which is a claim the absence of data does not support —
    // and it would sort as the calmest ride on the list.
    const dto = mapTermAttraction({ ...row, typicalPeakWait: null, isHeadliner: false });

    expect(dto.typicalPeakWait).toBeNull();
    expect(dto.isHeadliner).toBe(false);
  });
```

Also extend the shared `row` fixture at the top of the file so it satisfies the widened interface — add these two lines after `openedYear: 1993,`:

```ts
    typicalPeakWait: null,
    isHeadliner: false,
```

- [ ] **Step 2: Run test to verify it fails**

Run: `pnpm test -- src/attractions/dto/ride-profile.dto.spec.ts`
Expected: FAIL — TypeScript rejects `typicalPeakWait` as an unknown property on `AttractionWithTerm`, and `dto.typicalPeakWait` does not exist on `TermAttractionDto`.

- [ ] **Step 3: Widen the service interface**

In `src/attractions/services/ride-profile.service.ts`, add to the `AttractionWithTerm` interface, after `openedYear: number | null;`:

```ts
  /**
   * Typical peak wait in whole minutes — the P90 over 548 days from
   * `attraction_p90_baselines`, not a live reading. Null when the ride has no
   * baseline yet, which is normal for recently added or rarely open rides.
   */
  typicalPeakWait: number | null;
  /** Whether the baseline job classified this ride as one of its park's headliners. */
  isHeadliner: boolean;
```

- [ ] **Step 4: Join the baseline table**

In `findAttractionsByTerm`, add the join immediately after the existing `.innerJoin("parks", "park", …)` call:

```ts
      // LEFT, not INNER: a ride with no baseline must still appear in the
      // list. The count endpoint and this list are read side by side on the
      // glossary overview, and an inner join would make them disagree.
      .leftJoin(
        "attraction_p90_baselines",
        "baseline",
        'baseline."attractionId" = profile."attractionId"',
      )
```

Add to the `.select([...])` array, after `"profile.types AS types",`:

```ts
        'baseline."p90Baseline" AS p90baseline',
        'baseline."isHeadliner" AS isheadliner',
        "baseline.confidence AS confidence",
```

Add to the `getRawMany<{...}>()` type literal, after `types: string[] | null;`:

```ts
        p90baseline: string | null;
        isheadliner: boolean | null;
        confidence: "high" | "medium" | "low" | null;
```

- [ ] **Step 5: Map the joined columns**

In the same method's `rows.map(...)` return object, add after `openedYear: row.openedyear,`:

```ts
      // `decimal` comes back from pg as a string ("75.00"), so this needs an
      // explicit conversion — `row.p90baseline > 60` would compare strings.
      typicalPeakWait:
        row.p90baseline === null ? null : Math.round(Number(row.p90baseline)),
      isHeadliner: row.isheadliner ?? false,
```

- [ ] **Step 6: Extend the DTO**

In `src/attractions/dto/ride-profile.dto.ts`, add to `TermAttractionDto` after the `openedYear` property:

```ts
  @ApiProperty({
    example: 75,
    nullable: true,
    description:
      "Typical peak wait in minutes (P90 over 548 days). Null when the ride has no baseline yet. This is a long-run average, not a live wait time.",
  })
  typicalPeakWait: number | null;

  @ApiProperty({
    example: true,
    description: "Whether this ride is one of its park's headliners.",
  })
  isHeadliner: boolean;
```

And in `mapTermAttraction`, add to the returned object after `openedYear: row.openedYear,`:

```ts
    typicalPeakWait: row.typicalPeakWait,
    isHeadliner: row.isHeadliner,
```

- [ ] **Step 7: Run test to verify it passes**

Run: `pnpm test -- src/attractions/dto/ride-profile.dto.spec.ts`
Expected: PASS, 4 tests.

- [ ] **Step 8: Run the full suite for regressions**

Run: `pnpm test`
Expected: PASS — baseline is 750 passed, 11 skipped. Any other spec constructing an `AttractionWithTerm` must be updated with the two new fields.

- [ ] **Step 9: Commit**

```bash
git add src/attractions/services/ride-profile.service.ts src/attractions/dto/ride-profile.dto.ts src/attractions/dto/ride-profile.dto.spec.ts
git commit -m "feat(glossary): surface each ride's typical peak wait in the reverse lookup"
```

---

### Task 2: Sort modes

**Files:**
- Modify: `src/attractions/services/ride-profile.service.ts`
- Test: `src/attractions/services/ride-profile.service.spec.ts` (create)

**Interfaces:**
- Consumes: the `baseline` join alias and `AttractionWithTerm` fields from Task 1.
- Produces: exported type `TermAttractionSort = "park" | "popularity"`, exported `parseTermAttractionSort(raw: string | undefined): TermAttractionSort`, and `findAttractionsByTerm(termId: string, limit?: number, sort?: TermAttractionSort)`. Task 3 calls both.

- [ ] **Step 1: Write the failing test**

Create `src/attractions/services/ride-profile.service.spec.ts`:

```ts
import {
  RideProfileService,
  parseTermAttractionSort,
} from "./ride-profile.service";

/**
 * The reverse lookup shipped ordered by park name, which means a term with 151
 * rides opens on whatever park sorts first alphabetically. Ranking has to
 * happen in SQL rather than after the fetch: `LIMIT` is applied before
 * `ORDER BY` results are handed back, so sorting the returned page would rank
 * an arbitrary slice and call it the top 3.
 */
describe("parseTermAttractionSort", () => {
  it("defaults to park ordering", () => {
    expect(parseTermAttractionSort(undefined)).toBe("park");
  });

  it("accepts the popularity mode", () => {
    expect(parseTermAttractionSort("popularity")).toBe("popularity");
  });

  it("falls back to park ordering instead of throwing on junk", () => {
    // This is a public endpoint. A typo in a query string should return a
    // sensible list, not a 400.
    expect(parseTermAttractionSort("best")).toBe("park");
  });
});

describe("RideProfileService.findAttractionsByTerm ordering", () => {
  const qb = {
    innerJoin: jest.fn().mockReturnThis(),
    leftJoin: jest.fn().mockReturnThis(),
    select: jest.fn().mockReturnThis(),
    where: jest.fn().mockReturnThis(),
    orderBy: jest.fn().mockReturnThis(),
    addOrderBy: jest.fn().mockReturnThis(),
    limit: jest.fn().mockReturnThis(),
    getRawMany: jest.fn(),
  };
  const repo = { createQueryBuilder: jest.fn(() => qb) };
  const service = new RideProfileService(repo as never, [] as never);

  beforeEach(() => {
    jest.clearAllMocks();
    qb.getRawMany.mockResolvedValue([]);
  });

  it("keeps park ordering as the default", async () => {
    await service.findAttractionsByTerm("launch");

    expect(qb.orderBy).toHaveBeenCalledWith("park.name", "ASC");
    expect(qb.addOrderBy).toHaveBeenCalledWith("attraction.name", "ASC");
  });

  it("ranks by confidence bucket before the P90 value", async () => {
    await service.findAttractionsByTerm("launch", 200, "popularity");

    // The bucket has to be the FIRST ordering term. Ordering by P90 first
    // would let a ride with four samples and a freak 180-minute reading lead
    // a list of well-measured headliners.
    const [firstOrdering] = qb.orderBy.mock.calls[0] as [string];
    expect(firstOrdering).toContain("confidence");
    expect(qb.addOrderBy).toHaveBeenCalledWith(
      'baseline."p90Baseline"',
      "DESC",
      "NULLS LAST",
    );
  });

  it("still breaks ties deterministically when ranking", async () => {
    await service.findAttractionsByTerm("launch", 200, "popularity");

    expect(qb.addOrderBy).toHaveBeenCalledWith("park.name", "ASC");
    expect(qb.addOrderBy).toHaveBeenCalledWith("attraction.name", "ASC");
  });

  it("does not order by baseline columns in park mode", async () => {
    await service.findAttractionsByTerm("launch", 200, "park");

    const ordered = [
      ...qb.orderBy.mock.calls,
      ...qb.addOrderBy.mock.calls,
    ].map(([clause]) => clause as string);
    expect(ordered.some((clause) => clause.includes("baseline"))).toBe(false);
  });

  it("joins the baseline table on the outside", async () => {
    await service.findAttractionsByTerm("launch");

    // An inner join here would drop every ride that has no baseline yet, so
    // the list would disagree with the count endpoint rendered beside it.
    expect(qb.leftJoin).toHaveBeenCalledWith(
      "attraction_p90_baselines",
      "baseline",
      'baseline."attractionId" = profile."attractionId"',
    );
    expect(qb.innerJoin).not.toHaveBeenCalledWith(
      "attraction_p90_baselines",
      expect.anything(),
      expect.anything(),
    );
  });
});
```

- [ ] **Step 2: Run test to verify it fails**

Run: `pnpm test -- src/attractions/services/ride-profile.service.spec.ts`
Expected: FAIL — `parseTermAttractionSort` is not exported.

- [ ] **Step 3: Add the sort type and parser**

In `src/attractions/services/ride-profile.service.ts`, add above the `RIDE_PROFILE_SEED_TOKEN` declaration:

```ts
/** How the reverse lookup orders its rides. */
export type TermAttractionSort = "park" | "popularity";

/**
 * Reads the `sort` query parameter.
 *
 * Unknown values fall back to `park` rather than raising: this endpoint is
 * public and already in use, and a mistyped query string should still answer
 * with a usable list.
 */
export function parseTermAttractionSort(
  raw: string | undefined,
): TermAttractionSort {
  return raw === "popularity" ? "popularity" : "park";
}
```

- [ ] **Step 4: Apply the ordering**

Change the signature of `findAttractionsByTerm`:

```ts
  async findAttractionsByTerm(
    termId: string,
    limit = 200,
    sort: TermAttractionSort = "park",
  ): Promise<AttractionWithTerm[]> {
```

The ordering can no longer be a fixed link in one long chain, so split the chain in three. Change `const rows = await this.repo.createQueryBuilder("profile")` to `const query = this.repo.createQueryBuilder("profile")`, and end that chain at the `.where(...)` call — delete the `.orderBy("park.name", "ASC")`, `.addOrderBy("attraction.name", "ASC")`, `.limit(limit)` and `.getRawMany<{...}>()` links from it, keeping the `getRawMany` type literal for reuse in a moment. The statement now reads:

```ts
    const query = this.repo
      .createQueryBuilder("profile")
      .innerJoin(/* … unchanged … */)
      .innerJoin(/* … unchanged … */)
      .leftJoin(/* … from Task 1 … */)
      .select([/* … unchanged … */])
      .where(
        `(profile.elements @> :containment::jsonb
          OR profile.types @> :containment::jsonb
          OR profile.manufacturerTermId = :termId)`,
        { containment, termId },
      );
```

Then apply the ordering:

```ts
    if (sort === "popularity") {
      // Confidence first: `low` means a handful of samples, and those readings
      // swing wildly. Sorting purely by P90 would put them on top.
      query
        .orderBy(
          `CASE baseline.confidence
             WHEN 'high' THEN 0
             WHEN 'medium' THEN 1
             ELSE 2
           END`,
          "ASC",
        )
        .addOrderBy('baseline."p90Baseline"', "DESC", "NULLS LAST");
    } else {
      query.orderBy("park.name", "ASC");
    }

    // Always last, in both modes: without a total order, two rides with the
    // same baseline can swap places between identical requests.
    query.addOrderBy("park.name", "ASC").addOrderBy("attraction.name", "ASC");
```

Note that in `park` mode `park.name` is set by `orderBy` and then repeated by `addOrderBy`; that is harmless in Postgres (the second mention of an already-ordered column is a no-op) and keeps the tiebreak rule in exactly one place.

Finally, re-attach the fetch that was detached from the chain, immediately after the ordering block. Keep the `getRawMany` type literal exactly as it was after Task 1 widened it:

```ts
    const rows = await query.limit(limit).getRawMany<{
      attractionid: string;
      attractionname: string;
      attractionslug: string;
      parkid: string;
      parkname: string;
      parkslug: string;
      cityslug: string;
      countryslug: string;
      continentslug: string;
      openedyear: number | null;
      elements: string[] | null;
      types: string[] | null;
      p90baseline: string | null;
      isheadliner: boolean | null;
      confidence: "high" | "medium" | "low" | null;
    }>();
```

The `rows.map(...)` return block below it is unchanged.

- [ ] **Step 5: Run test to verify it passes**

Run: `pnpm test -- src/attractions/services/ride-profile.service.spec.ts`
Expected: PASS, 8 tests.

- [ ] **Step 6: Commit**

```bash
git add src/attractions/services/ride-profile.service.ts src/attractions/services/ride-profile.service.spec.ts
git commit -m "feat(glossary): rank term rides by typical peak wait behind a sort mode"
```

---

### Task 3: Expose the sort mode on the endpoint

**Files:**
- Modify: `src/attractions/glossary-rides.controller.ts`

**Interfaces:**
- Consumes: `parseTermAttractionSort` and the three-argument `findAttractionsByTerm` from Task 2.
- Produces: `GET /v1/glossary/terms/:termId/attractions?sort=popularity`.

- [ ] **Step 1: Extend the import**

In `src/attractions/glossary-rides.controller.ts`, replace the `RideProfileService` import line with:

```ts
import {
  RideProfileService,
  parseTermAttractionSort,
} from "./services/ride-profile.service";
```

- [ ] **Step 2: Document the parameter**

Add after the existing `@ApiQuery({ name: "limit", … })` block:

```ts
  @ApiQuery({
    name: "sort",
    required: false,
    enum: ["park", "popularity"],
    description:
      "`park` (default) orders alphabetically by park, then ride. `popularity` ranks by typical peak wait — the P90 over 548 days — with well-measured rides first. Unknown values fall back to `park`.",
    example: "popularity",
  })
```

Update the `@ApiOperation` description on the same handler to match the new behaviour — replace `"matches the term. Ordered by park name, then ride name."` with:

```ts
      "matches the term. Ordered by park name, then ride name; pass " +
      "`sort=popularity` to rank by typical peak wait instead.",
```

- [ ] **Step 3: Thread the parameter through**

Change the handler signature to accept it:

```ts
  async attractionsForTerm(
    @Param("termId") termId: string,
    @Query("limit") limit?: string,
    @Query("sort") sort?: string,
  ): Promise<{ termId: string; total: number; data: TermAttractionDto[] }> {
```

and the service call:

```ts
    const rows = await this.rideProfileService.findAttractionsByTerm(
      termId,
      capped,
      parseTermAttractionSort(sort),
    );
```

- [ ] **Step 4: Verify the whole suite**

Run: `pnpm test`
Expected: PASS, 758 passed / 11 skipped (750 baseline + 8 new).

- [ ] **Step 5: Verify types and lint**

Run: `pnpm exec tsc --noEmit && pnpm lint`
Expected: both exit 0.

- [ ] **Step 6: Commit**

```bash
git add src/attractions/glossary-rides.controller.ts
git commit -m "feat(glossary): accept sort=popularity on the term rides endpoint"
```

---

### Task 4: Confirm the baseline coverage is real

The spec flags this as the plan's one open risk: if few curated rides have a P90 baseline, the ranking is decoration. This task answers it with data before the frontend is built on top.

**Files:** none — this is a verification task producing a written finding.

**Interfaces:**
- Consumes: the deployed endpoint from Task 3, once running locally against the real database.

- [ ] **Step 1: Start the API against the real database**

Run: `pnpm start:dev`
Wait for the Nest bootstrap log to report the HTTP listener.

- [ ] **Step 2: Measure coverage on a broad term and a narrow one**

```bash
for term in launch zero-g-roll dive-coaster celestial-spin; do
  echo "── $term"
  curl -s "http://localhost:3000/v1/glossary/terms/$term/attractions?sort=popularity&limit=500" \
    | python3 -c "import json,sys; d=json.load(sys.stdin); rows=d['data']; withb=[r for r in rows if r['typicalPeakWait'] is not None]; print(f'  total={d[\"total\"]} mit Baseline={len(withb)}'); [print(f'  {i+1}. {r[\"name\"]} ({r[\"parkName\"]}) {r[\"typicalPeakWait\"]}min') for i,r in enumerate(rows[:3])]"
done
```

- [ ] **Step 3: Judge the result against the spec's fallback rule**

Expected: the first three rows of each term carry a non-null `typicalPeakWait`, and coverage is high enough that the top 3 are recognisable headliners rather than obscure rides.

If the top 3 of a term come back with `typicalPeakWait: null`, coverage is too thin to claim a ranking. Report that to the user and stop — per the spec the fallback is `openedYear DESC` under an honest "Neueste" heading, and switching to it is the user's call, not a silent substitution.

- [ ] **Step 4: Record the finding**

Append a short "Baseline coverage" section to `docs/superpowers/plans/2026-07-28-glossary-ride-ranking.md` with the measured numbers, so the frontend plan can cite them rather than re-measuring.

- [ ] **Step 5: Commit**

```bash
git add docs/superpowers/plans/2026-07-28-glossary-ride-ranking.md
git commit -m "docs(glossary): record P90 baseline coverage for the ride ranking"
```

---

## Out of Scope

- Frontend consumption of `sort`, `typicalPeakWait` and `isHeadliner` — that is the second plan, written after this one lands.
- Any change to `GET /v1/glossary/terms/counts`. It already returns correct data; the frontend simply never called it with the right prefix.
- New indexes. `attraction_p90_baselines` is keyed on `attractionId`, which is exactly the join column, and the result set is capped at 500 rows.
