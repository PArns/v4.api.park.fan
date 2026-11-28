import { DataQualityMonitorService } from "../../monitoring/data-quality-monitor.service";
import { dataQualityTool } from "./data-quality.tool";
import { adminContext } from "./__fixtures__/admin-context";

describe("data_quality", () => {
  it("lists the rides without a kind by id, name and slug, parks and rides cut to the limit", async () => {
    const parks = ["Magic Kingdom Park", "Epcot", "Phantasialand"].map(
      (parkName, index) => ({
        parkId: `p${index}`,
        parkName,
        attractions: 40,
        withoutKind: ["Astro Orbiter", "Barnstormer", "Carousel"],
        withoutKindRefs: ["Astro Orbiter", "Barnstormer", "Carousel"].map(
          (name, ride) => ({
            id: `a${index}-${ride}`,
            name,
            slug: name.toLowerCase(),
          }),
        ),
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
          withoutKindRefs: {
            total: 3,
            items: [
              { id: "a0-0", name: "Astro Orbiter", slug: "astro orbiter" },
              { id: "a0-1", name: "Barnstormer", slug: "barnstormer" },
            ],
          },
        },
        {
          parkId: "p1",
          parkName: "Epcot",
          attractions: 40,
          withoutKindRefs: {
            total: 3,
            items: [
              { id: "a1-0", name: "Astro Orbiter", slug: "astro orbiter" },
              { id: "a1-1", name: "Barnstormer", slug: "barnstormer" },
            ],
          },
        },
      ],
    });
  });
});
