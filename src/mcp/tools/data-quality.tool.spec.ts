import { DataQualityMonitorService } from "../../monitoring/data-quality-monitor.service";
import { dataQualityTool } from "./data-quality.tool";
import { adminContext } from "./__fixtures__/admin-context";

describe("data_quality", () => {
  it("lists the rides without a kind by id, name and slug, parks cut to the limit", async () => {
    const parks = ["Magic Kingdom Park", "Epcot", "Phantasialand"].map(
      (parkName, index) => ({
        parkId: `p${index}`,
        parkName,
        attractions: 40,
        withoutKind: ["Astro Orbiter"],
        withoutKindRefs: [
          { id: `a${index}`, name: "Astro Orbiter", slug: "astro-orbiter" },
        ],
      }),
    );
    const { ctx } = adminContext("viewer", [
      [
        DataQualityMonitorService,
        {
          report: async () => ({
            windowDays: 14,
            silencedClusters: [],
            failingJobs: [],
            absenceRetiredUnreviewed: [],
            reissueCandidates: [],
            attractionsWithoutKind: parks,
            displayConsistency: null,
          }),
        },
      ],
    ]);

    const answer = (await dataQualityTool.handler(
      { windowDays: 14, limit: 2 },
      ctx,
    )) as Record<string, any>;

    expect(answer.attractionsWithoutKind).toEqual({
      total: 3,
      items: [
        {
          parkId: "p0",
          parkName: "Magic Kingdom Park",
          attractions: 40,
          withoutKindRefs: [
            { id: "a0", name: "Astro Orbiter", slug: "astro-orbiter" },
          ],
        },
        {
          parkId: "p1",
          parkName: "Epcot",
          attractions: 40,
          withoutKindRefs: [
            { id: "a1", name: "Astro Orbiter", slug: "astro-orbiter" },
          ],
        },
      ],
    });
  });
});
