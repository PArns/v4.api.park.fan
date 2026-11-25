import {
  buildBoundaryQuery,
  buildFacilitiesQuery,
  parseFacilities,
  pickBoundary,
  type OverpassElement,
} from "./overpass.parser";

const meta = { version: 3, timestamp: "2024-05-26T13:06:10Z" };
const node = (
  id: number,
  tags: Record<string, string>,
  lat = 50.8,
  lon = 6.88,
): OverpassElement => ({ type: "node", id, lat, lon, tags, ...meta });

describe("parseFacilities", () => {
  it("maps the tags we show and keeps the OSM element's identity", () => {
    const [toilet] = parseFacilities([
      node(1, {
        amenity: "toilets",
        name: " Berlin ",
        "toilets:wheelchair": "designated",
        changing_table: "yes",
        fee: "no",
      }),
    ]);
    expect(toilet).toEqual({
      type: "toilets",
      latitude: 50.8,
      longitude: 6.88,
      name: "Berlin",
      wheelchair: "yes",
      changingTable: true,
      fee: false,
      osmType: "node",
      osmId: "1",
      osmVersion: 3,
      osmTimestamp: new Date("2024-05-26T13:06:10Z"),
    });
  });

  it("leaves a tag OSM does not carry as null, never as a no", () => {
    const [toilet] = parseFacilities([node(1, { amenity: "toilets" })]);
    expect(toilet).toMatchObject({
      name: null,
      wheelchair: null,
      changingTable: null,
      fee: null,
    });
  });

  it("places a way at its centre", () => {
    const [toilet] = parseFacilities([
      {
        type: "way",
        id: 9,
        center: { lat: 51.65, lon: 5.05 },
        tags: { amenity: "toilets", building: "yes" },
        ...meta,
      },
    ]);
    expect(toilet).toMatchObject({
      osmType: "way",
      latitude: 51.65,
      longitude: 5.05,
    });
  });

  it.each([
    ["amenity", "drinking_water", "drinking_water"],
    ["amenity", "nursing_room", "baby_care"],
    ["amenity", "luggage_locker", "lockers"],
    ["amenity", "locker", "lockers"],
    ["amenity", "atm", "atm"],
    ["amenity", "first_aid", "first_aid"],
    ["emergency", "first_aid", "first_aid"],
    ["emergency", "defibrillator", "defibrillator"],
  ])("%s=%s is %s", (key, value, type) => {
    expect(parseFacilities([node(1, { [key]: value })])[0].type).toBe(type);
  });

  it("drops what is not one of our types", () => {
    expect(
      parseFacilities([
        node(1, { amenity: "restaurant", toilets: "yes" }),
        node(2, { amenity: "water_point" }),
        node(3, {}),
      ]),
    ).toEqual([]);
  });

  it.each(["private", "no", "staff", "employees", "delivery"])(
    "drops access=%s",
    (access) => {
      expect(
        parseFacilities([node(1, { amenity: "toilets", access })]),
      ).toEqual([]);
    },
  );

  it.each(["customers", "yes", "permissive"])("keeps access=%s", (access) => {
    expect(
      parseFacilities([node(1, { amenity: "toilets", access })]),
    ).toHaveLength(1);
  });

  it("drops drinking water that is not for drinking", () => {
    expect(
      parseFacilities([
        node(1, { amenity: "drinking_water", drinking_water: "no" }),
        node(2, { amenity: "drinking_water", "drinking_water:legal": "no" }),
      ]),
    ).toEqual([]);
  });

  it("drops an element without position, version or timestamp", () => {
    expect(
      parseFacilities([
        { type: "way", id: 1, tags: { amenity: "toilets" }, ...meta },
        { type: "node", id: 2, lat: 1, lon: 1, tags: { amenity: "toilets" } },
        { ...node(3, { amenity: "toilets" }), timestamp: "soon" },
      ]),
    ).toEqual([]);
  });

  describe("one facility mapped twice", () => {
    // ~7 m apart: a toilet building and the node inside it.
    const building: OverpassElement = {
      type: "way",
      id: 50,
      center: { lat: 50.80006, lon: 6.88 },
      tags: { amenity: "toilets", building: "toilets" },
      ...meta,
    };

    it("keeps the one that says more", () => {
      const result = parseFacilities([
        building,
        node(51, { amenity: "toilets", wheelchair: "yes" }),
      ]);
      expect(result.map((f) => f.osmId)).toEqual(["51"]);
    });

    it("keeps the node when both say the same, whatever the order", () => {
      const twin = node(51, { amenity: "toilets" });
      expect(parseFacilities([building, twin])[0].osmType).toBe("node");
      expect(parseFacilities([twin, building])[0].osmType).toBe("node");
    });

    it("keeps two types at one spot and two toilets 40 m apart", () => {
      expect(
        parseFacilities([
          node(1, { amenity: "toilets" }),
          node(2, { amenity: "drinking_water" }),
          node(3, { amenity: "toilets" }, 50.80036),
        ]),
      ).toHaveLength(3);
    });
  });
});

describe("pickBoundary", () => {
  const outline = (
    id: number,
    name: string,
    size: number,
    extra: Record<string, string> = {},
  ): OverpassElement => ({
    type: "way",
    id,
    tags: { tourism: "theme_park", name, ...extra },
    bounds: { minlat: 0, minlon: 0, maxlat: size, maxlon: size },
  });

  it("takes the outline that carries the park's name", () => {
    const picked = pickBoundary(
      [outline(1, "Rulantica", 1), outline(2, "Europa-Park", 2)],
      ["Europa-Park"],
    );
    expect(picked?.id).toBe(2);
  });

  it("matches across accents, case and a longer official name", () => {
    expect(
      pickBoundary([outline(1, "Parc Astérix", 1)], ["parc asterix"])?.id,
    ).toBe(1);
    expect(
      pickBoundary([outline(1, "Efteling", 1)], ["Efteling Theme Park"])?.id,
    ).toBe(1);
    expect(
      pickBoundary(
        [outline(1, "東京ディズニーシー", 1, { "name:en": "Tokyo DisneySea" })],
        ["Tokyo DisneySea"],
      )?.id,
    ).toBe(1);
  });

  it("takes the smallest of several, the park inside its resort", () => {
    const picked = pickBoundary(
      [outline(1, "Walibi Holland Resort", 5), outline(2, "Walibi Holland", 1)],
      ["Walibi Holland"],
    );
    expect(picked?.id).toBe(2);
  });

  it("takes the equal name before a smaller one that only contains it", () => {
    const picked = pickBoundary(
      [
        outline(1, "PortAventura Caribe Aquatic Park", 1),
        outline(2, "PortAventura Park", 3),
        outline(3, "PortAventura World Parks & Resort", 9),
      ],
      ["PortAventura Park"],
    );
    expect(picked?.id).toBe(2);
  });

  it("refuses an outline chosen by position alone", () => {
    expect(
      pickBoundary([outline(1, "Rulantica", 1)], ["Europa-Park"]),
    ).toBeNull();
    expect(pickBoundary([], ["Europa-Park"])).toBeNull();
  });

  it("ignores nodes and a missing name of ours", () => {
    const point: OverpassElement = {
      type: "node",
      id: 1,
      lat: 0,
      lon: 0,
      tags: { tourism: "theme_park", name: "Toverland" },
    };
    expect(pickBoundary([point], ["Toverland"])).toBeNull();
    expect(pickBoundary([outline(1, "Toverland", 1)], [null, ""])).toBeNull();
  });
});

describe("the queries", () => {
  it("looks for an outline at and around the coordinate", () => {
    const ql = buildBoundaryQuery(50.7998, 6.8793);
    expect(ql).toContain("is_in(50.7998,6.8793)");
    expect(ql).toContain("(around:2000,50.7998,6.8793)");
    expect(ql).toContain("out tags bb;");
  });

  it("asks for everything inside one outline, with its metadata", () => {
    const ql = buildFacilitiesQuery({ type: "way", id: 31052062 });
    expect(ql).toContain("way(31052062);map_to_area->.a;");
    expect(ql).toContain("(area.a);");
    expect(ql).toContain("out meta center;");
    expect(buildFacilitiesQuery({ type: "relation", id: "7" })).toContain(
      "rel(7);map_to_area",
    );
  });
});
