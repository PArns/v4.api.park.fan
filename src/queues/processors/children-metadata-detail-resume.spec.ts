import {
  ChildrenMetadataProcessor,
  detailSyncResumePoint,
} from "./children-metadata.processor";

/**
 * The weekly `sync-attraction-details` walks ~7k rides at one request every
 * 2 s, ~4 h. On 2026-10-25 two deploys landed in it: each restart began again
 * at the first ride, and the second stall failed the job (PAR-1183). It now
 * stores the last ride it finished as the job's progress, which Bull keeps
 * when a stalled job goes back to the queue, and a run picks up after it.
 */
describe("ChildrenMetadataProcessor — sync-attraction-details resumes", () => {
  const wikiId = (n: number) => `0000000${n}-0000-4000-8000-000000000000`;
  /** Three rides in an order the database may return them in. */
  const rides = [
    { id: "c", externalId: wikiId(3), minimumHeight: null, park: null },
    { id: "a", externalId: wikiId(1), minimumHeight: null, park: null },
    { id: "qt", externalId: "qt-ride-8", minimumHeight: null, park: null },
    { id: "b", externalId: wikiId(2), minimumHeight: null, park: null },
  ];

  function build() {
    const repo = {
      find: jest.fn().mockResolvedValue(rides),
      update: jest.fn(),
    };
    const getEntity = jest.fn(async (_id: string) => ({ minimumHeight: 120 }));
    const processor = new ChildrenMetadataProcessor(
      { getRepository: () => repo } as any,
      {} as any,
      {} as any,
      {} as any,
      {} as any,
      { getEntity } as any,
      {} as any,
      {} as any,
      {} as any,
      {} as any,
      {} as any,
      { revalidateTags: jest.fn() } as any,
    );
    jest.spyOn(processor as any, "sleep").mockResolvedValue(undefined);
    return { processor, getEntity, repo };
  }

  /** A Bull job whose progress lives in memory, as the job hash keeps it. */
  function job(progress: unknown = 0) {
    let stored = progress;
    const stores: unknown[] = [];
    return {
      progress: jest.fn((value?: unknown) => {
        if (value === undefined) return stored;
        stored = value;
        stores.push(value);
        return Promise.resolve();
      }),
      stores,
    };
  }

  it("walks the wiki rides in id order and stores each one it finished", async () => {
    const { processor, getEntity } = build();
    const fresh = job();
    await processor.handleSyncAttractionDetails(fresh as any);

    expect(getEntity.mock.calls.map(([id]) => id)).toEqual([
      wikiId(1),
      wikiId(2),
      wikiId(3),
    ]);
    expect(fresh.stores).toEqual([
      { after: "a" },
      { after: "b" },
      { after: "c" },
    ]);
  });

  it("picks up after the last ride the stopped run finished", async () => {
    const { processor, getEntity, repo } = build();
    await processor.handleSyncAttractionDetails(job({ after: "a" }) as any);

    expect(getEntity.mock.calls.map(([id]) => id)).toEqual([
      wikiId(2),
      wikiId(3),
    ]);
    // Every ride it did reach is still written.
    expect(repo.update).toHaveBeenCalledTimes(2);
  });

  it("goes on when the progress cannot be stored", async () => {
    const { processor, getEntity } = build();
    const broken = {
      progress: jest.fn((value?: unknown) =>
        value === undefined ? 0 : Promise.reject(new Error("Redis away")),
      ),
    };
    await processor.handleSyncAttractionDetails(broken as any);
    expect(getEntity).toHaveBeenCalledTimes(3);
  });

  it("reads a fresh job, a number or anything else as no resume point", () => {
    const progressOf = (value: unknown) =>
      detailSyncResumePoint({ progress: () => value } as any);
    expect(progressOf(0)).toBeNull();
    expect(progressOf(42)).toBeNull();
    expect(progressOf(null)).toBeNull();
    expect(progressOf({ after: 7 })).toBeNull();
    expect(progressOf({ after: "b" })).toBe("b");
  });
});
