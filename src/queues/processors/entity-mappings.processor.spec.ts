import { In } from "typeorm";
import { EntityMappingsProcessor } from "./entity-mappings.processor";
import {
  EntityMetadata,
  EntityType,
} from "../../external-apis/data-sources/interfaces/data-source.interface";

/** PAR-924: the curated name of a wiki row travels to the matcher as a second name. */
describe("EntityMappingsProcessor.withCuratedNames", () => {
  const entity = (
    externalId: string,
    name: string,
    entityType = EntityType.ATTRACTION,
  ): EntityMetadata => ({
    externalId,
    source: "themeparks-wiki",
    entityType,
    name,
  });

  const run = async (
    rows: Array<{ externalId: string; curatedName: string | null }>,
    entities: EntityMetadata[],
  ) => {
    const find = jest.fn().mockResolvedValue(rows);
    const processor = new EntityMappingsProcessor(
      {} as never,
      { getRepository: () => ({ find }) } as never,
      {} as never,
      {} as never,
      {} as never,
      {} as never,
      {} as never,
    );
    const result: EntityMetadata[] = await (processor as any).withCuratedNames(
      "park-1",
      entities,
    );
    return { result, find };
  };

  it("offers the curated name next to the feed name and leaves the input untouched", async () => {
    const input = [entity("w1", "Helhuis"), entity("w2", "Taron")];
    const { result, find } = await run(
      [
        { externalId: "w1", curatedName: "Hell House" },
        { externalId: "w2", curatedName: null },
      ],
      input,
    );
    expect(result[0].altNames).toEqual(["Hell House"]);
    expect(result[1].altNames).toBeUndefined();
    expect(input[0].altNames).toBeUndefined();
    expect(find).toHaveBeenCalledWith({
      where: { parkId: "park-1", externalId: In(["w1", "w2"]) },
      select: { externalId: true, curatedName: true },
    });
  });

  it("adds nothing when the curated name equals the feed name or the row is missing", async () => {
    const { result } = await run(
      [{ externalId: "w1", curatedName: "Taron" }],
      [entity("w1", "Taron"), entity("w3", "Unknown")],
    );
    expect(result.every((e) => e.altNames === undefined)).toBe(true);
  });

  it("does not query when the park lists no attraction", async () => {
    const { result, find } = await run(
      [],
      [entity("s1", "Show", EntityType.SHOW)],
    );
    expect(result).toHaveLength(1);
    expect(find).not.toHaveBeenCalled();
  });
});
