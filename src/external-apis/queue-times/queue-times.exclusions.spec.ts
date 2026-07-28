import { QueueTimesDataSource } from "./queue-times-data-source";
import {
  QUEUE_TIMES_EXCLUSIONS,
  isQueueTimesExcluded,
} from "./queue-times.exclusions";

/**
 * Energylandia publishes its turnstile counters through the same feed as its
 * rides — "Licznik" rows that carry a wait time and were therefore ingested as
 * attractions. Fifteen of them, every one counted as a ride in the park's
 * totals and in every crowd aggregate built over "all rides in this park".
 *
 * The filter has to sit on every path that turns a feed ride into an entity,
 * not just the live one: the metadata path recreates the row on the next sync.
 */
describe("Queue-Times exclusions", () => {
  const ride = (id: number, name: string) => ({
    id,
    name,
    is_open: true,
    wait_time: 10,
    last_updated: "2026-07-28T10:00:00Z",
  });

  const client = {
    getParkQueueTimes: jest.fn(),
    getParks: jest.fn(),
  };
  const source = new QueueTimesDataSource(client as never);

  beforeEach(() => {
    client.getParkQueueTimes.mockReset();
    client.getParks.mockReset();
  });

  it("knows a counter from a ride", () => {
    expect(isQueueTimesExcluded("qt-ride-14585")).toBe(true); // Fast Pass Anaconda Kol Licznik
    expect(isQueueTimesExcluded("qt-ride-99999")).toBe(false);
  });

  it("keeps every excluded id namespaced, so it cannot collide with the wiki's", () => {
    for (const id of QUEUE_TIMES_EXCLUSIONS) {
      expect(id).toMatch(/^qt-ride-\d+$/);
    }
  });

  it("drops counters from live data, in lands and outside them", async () => {
    client.getParkQueueTimes.mockResolvedValue({
      lands: [
        {
          id: 1,
          name: "Strefa Familijna",
          rides: [
            ride(14585, "Fast Pass Anaconda Kol Licznik"),
            ride(1, "Anaconda"),
          ],
        },
      ],
      rides: [
        ride(16259, "Fastpass Tsunami Drop Licz"),
        ride(2, "Tsunami Drop"),
      ],
    });

    const live = await source.fetchParkLiveData("qt-park-1");

    expect(live.entities.map((e) => e.name)).toEqual([
      "Anaconda",
      "Tsunami Drop",
    ]);
    expect(live.lands?.[0].attractions).toEqual(["qt-ride-1"]);
  });

  it("drops them from the metadata path too, or the next sync recreates them", async () => {
    client.getParkQueueTimes.mockResolvedValue({
      lands: [
        {
          id: 1,
          name: "Bajkolandia",
          rides: [
            ride(11250, "17 Teatr Coloseo Zew Licznik"),
            ride(3, "Energuś"),
          ],
        },
      ],
      rides: [ride(11447, "Fast Pass Main Train Licznik")],
    });

    const entities = await source.fetchParkEntities("qt-park-1");

    expect(entities.map((e) => e.name)).toEqual(["Energuś"]);
  });
});
