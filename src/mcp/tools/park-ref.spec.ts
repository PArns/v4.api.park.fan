import { DataSource } from "typeorm";
import { ParksService } from "../../parks/parks.service";
import { SearchService } from "../../search/search.service";
import type { McpToolContext } from "../mcp-tool";
import { resolvePark } from "./park-ref";

describe("resolvePark by name", () => {
  const park = (id: string, name: string) => ({
    id,
    name,
    type: "park",
    city: "Rust",
    country: "Germany",
  });
  const context = (hits: unknown[]) => {
    const providers = new Map<unknown, unknown>([
      [
        DataSource,
        { getRepository: () => ({ find: jest.fn(async () => []) }) },
      ],
      [
        ParksService,
        {
          findById: jest.fn(async (id: string) => ({ id, name: `park ${id}` })),
        },
      ],
      [SearchService, { search: jest.fn(async () => ({ results: hits })) }],
    ]);
    return {
      get: <T>(token: unknown) => providers.get(token) as T,
    } as unknown as McpToolContext;
  };

  it("takes the only park the search finds for a name", async () => {
    await expect(
      resolvePark(context([park("a", "Europa-Park")]), "Europa Park"),
    ).resolves.toMatchObject({ id: "a" });
  });

  it("takes the park whose name is the reference among several matches", async () => {
    const hits = [park("a", "Rulantica"), park("b", "Europa-Park")];
    await expect(
      resolvePark(context(hits), "europa park"),
    ).resolves.toMatchObject({ id: "b" });
  });

  it("knows a park by its name and city", async () => {
    const hits = [
      { ...park("a", "Disneyland Park"), city: "Anaheim" },
      { ...park("b", "Disneyland Park"), city: "Paris" },
      { ...park("c", "Walt Disney Studios Park"), city: "Paris" },
    ];
    await expect(
      resolvePark(context(hits), "Disneyland Paris"),
    ).resolves.toMatchObject({ id: "b" });
  });

  it("hands several equal candidates back to pick from", async () => {
    const hits = [park("a", "Legoland"), park("b", "Legoland")];
    await expect(resolvePark(context(hits), "Legoland")).resolves.toMatchObject(
      { ambiguous: true, parks: [{ id: "a" }, { id: "b" }] },
    );
  });

  it("still says no park when the search finds none", async () => {
    await expect(resolvePark(context([]), "Kinderstaad")).rejects.toThrow(
      /No park/,
    );
  });
});
