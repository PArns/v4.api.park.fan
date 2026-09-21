import type { Park } from "../../parks/entities/park.entity";
import type { ParksService } from "../../parks/parks.service";
import type { ThemeParksClient } from "../../external-apis/themeparks/themeparks.client";
import type { EntityResponse } from "../../external-apis/themeparks/themeparks.types";
import { ParkSyncState, ThemeParksEntitySync } from "./theme-parks-entity-sync";

/**
 * Cover for the skeleton `syncAttractions`, `syncShows` and `syncRestaurants`
 * share. The three services' own specs prove their hooks; this one proves the
 * walk around them, including the one thing that is new rather than moved:
 * a park without an external ID is skipped instead of being asked about.
 */

interface TestRow {
  name: string;
  slug: string;
  externalId: string;
}

interface TestState extends ParkSyncState {
  byExternalId: Map<string, { id: string }>;
}

interface TestUpdate {
  id: string;
  name?: string;
}

class TestSync extends ThemeParksEntitySync<
  TestRow,
  EntityResponse,
  TestState,
  TestUpdate
> {
  /** Every park the template handed to `loadParkState`, in order. */
  readonly visitedParks: string[] = [];
  readonly inserted: Partial<TestRow>[] = [];
  readonly updated: TestUpdate[] = [];

  /** Parks `claimPark` takes, mapped to the count it reports. */
  claims = new Map<string, number>();
  existing = new Map<string, { id: string }>();
  existingSlugs: string[] = [];
  /** External IDs `mapChild` refuses to map. */
  unmappable = new Set<string>();

  constructor(parksService: ParksService, client: ThemeParksClient) {
    super(parksService, client);
  }

  sync(): Promise<number> {
    return this.syncFromThemeParksWiki();
  }

  protected override claimPark(park: Park): Promise<number | null> {
    const claimed = this.claims.get(park.id);
    return Promise.resolve(claimed === undefined ? null : claimed);
  }

  protected filterChildren(children: EntityResponse[]): EntityResponse[] {
    return children.filter((child) => child.entityType === "ATTRACTION");
  }

  protected loadParkState(park: Park): Promise<TestState> {
    this.visitedParks.push(park.id);
    return Promise.resolve({
      byExternalId: new Map(this.existing),
      usedSlugs: new Set(this.existingSlugs),
    });
  }

  protected mapChild(
    child: EntityResponse,
    parkId: string,
  ): Partial<TestRow> | null {
    if (this.unmappable.has(child.id)) {
      return null;
    }
    return { name: child.name, externalId: `${parkId}:${child.id}` };
  }

  protected reconcile(
    mapped: Partial<TestRow>,
    state: TestState,
  ): TestUpdate | null {
    const existing = state.byExternalId.get(mapped.externalId!);
    return existing ? { id: existing.id, name: mapped.name } : null;
  }

  protected persist(
    toInsert: Partial<TestRow>[],
    toUpdate: TestUpdate[],
  ): Promise<void> {
    this.inserted.push(...toInsert);
    this.updated.push(...toUpdate);
    return Promise.resolve();
  }
}

describe("ThemeParksEntitySync", () => {
  const ensureParksLoaded = jest.fn();
  const getEntityChildren = jest.fn();

  const parksService = { ensureParksLoaded } as unknown as ParksService;
  const client = { getEntityChildren } as unknown as ThemeParksClient;

  const park = (id: string, externalId: unknown): Park =>
    ({ id, externalId }) as Park;

  const child = (id: string, name: string): EntityResponse =>
    ({ id, name, entityType: "ATTRACTION" }) as EntityResponse;

  let sync: TestSync;

  beforeEach(() => {
    jest.clearAllMocks();
    getEntityChildren.mockResolvedValue({ children: [] });
    sync = new TestSync(parksService, client);
  });

  it("asks the wiki only about parks the wiki knows", async () => {
    ensureParksLoaded.mockResolvedValue([
      park("wiki", "tp_1"),
      park("queue-times", "qt-2"),
      park("wartezeiten", "wz-3"),
      // No external ID at all: the wiki has nothing to answer about it, and
      // asking used to send `undefined` down the client.
      park("unsourced", null),
      park("blank", ""),
    ]);

    await sync.sync();

    expect(getEntityChildren).toHaveBeenCalledTimes(1);
    expect(getEntityChildren).toHaveBeenCalledWith("tp_1");
    expect(sync.visitedParks).toEqual(["wiki"]);
  });

  it("lets a claimed park skip the wiki path and still count", async () => {
    ensureParksLoaded.mockResolvedValue([
      park("claimed", "tp_1"),
      park("wiki", "tp_2"),
    ]);
    sync.claims.set("claimed", 7);
    getEntityChildren.mockResolvedValue({
      children: [child("a", "Ride A")],
    });

    const synced = await sync.sync();

    // The claimed park is never fetched, and its own count is carried through.
    expect(getEntityChildren).toHaveBeenCalledTimes(1);
    expect(getEntityChildren).toHaveBeenCalledWith("tp_2");
    expect(synced).toBe(8);
  });

  it("counts a claimed park that reports nothing without falling through", async () => {
    ensureParksLoaded.mockResolvedValue([park("claimed", "tp_1")]);
    sync.claims.set("claimed", 0);

    const synced = await sync.sync();

    expect(getEntityChildren).not.toHaveBeenCalled();
    expect(synced).toBe(0);
  });

  it("mints a slug no row in the park holds, including within one response", async () => {
    ensureParksLoaded.mockResolvedValue([park("wiki", "tp_1")]);
    sync.existingSlugs = ["haunted-house"];
    getEntityChildren.mockResolvedValue({
      children: [
        child("a", "Haunted House"),
        child("b", "Haunted House"),
        { id: "c", name: "A Show", entityType: "SHOW" } as EntityResponse,
      ],
    });

    const synced = await sync.sync();

    expect(sync.inserted.map((row) => row.slug)).toEqual([
      "haunted-house-2",
      "haunted-house-3",
    ]);
    // The show never reached the mapper, so it never reached the count.
    expect(synced).toBe(2);
  });

  it("keeps the slug of a row it already has", async () => {
    ensureParksLoaded.mockResolvedValue([park("wiki", "tp_1")]);
    sync.existing.set("wiki:a", { id: "row-a" });
    getEntityChildren.mockResolvedValue({
      children: [child("a", "Renamed Ride")],
    });

    await sync.sync();

    expect(sync.inserted).toEqual([]);
    expect(sync.updated).toEqual([{ id: "row-a", name: "Renamed Ride" }]);
  });

  it("drops a child the mapper refuses without counting it", async () => {
    ensureParksLoaded.mockResolvedValue([park("wiki", "tp_1")]);
    sync.unmappable.add("b");
    getEntityChildren.mockResolvedValue({
      children: [
        child("a", "Ride A"),
        child("b", "No ID"),
        child("c", "Ride C"),
      ],
    });

    const synced = await sync.sync();

    expect(synced).toBe(2);
    expect(sync.inserted.map((row) => row.name)).toEqual(["Ride A", "Ride C"]);
  });

  it("reads a park's rows only when the response holds something for it", async () => {
    ensureParksLoaded.mockResolvedValue([park("wiki", "tp_1")]);
    getEntityChildren.mockResolvedValue({
      children: [{ id: "c", name: "A Show", entityType: "SHOW" }],
    });

    // Default: the park state is loaded even for an empty result.
    await sync.sync();
    expect(sync.visitedParks).toEqual(["wiki"]);

    const skipping = new (class extends TestSync {
      protected override readonly skipParksWithoutChildren = true;
    })(parksService, client);

    await skipping.sync();
    expect(skipping.visitedParks).toEqual([]);
  });
});
