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

  it("reads drinking water", () => {
    expect(
      parseFacilities([node(1, { amenity: "drinking_water" })])[0].type,
    ).toBe("drinking_water");
  });

  // The public route serves toilets and drinking water; nothing else is read.
  it.each([
    ["amenity", "nursing_room"],
    ["amenity", "luggage_locker"],
    ["amenity", "atm"],
    ["amenity", "first_aid"],
    ["emergency", "defibrillator"],
    ["amenity", "water_point"],
  ])("leaves %s=%s out", (key, value) => {
    expect(parseFacilities([node(1, { [key]: value })])).toEqual([]);
  });

  describe("sights", () => {
    // Efteling's figure bins and statues, tagged as OpenStreetMap has them.
    it("reads a named figure bin and a named artwork as sights", () => {
      const result = parseFacilities([
        node(7170488297, {
          amenity: "waste_basket",
          name: "Opa Gijs",
          "subject:wikidata": "Q2650179",
        }),
        node(10166286245, {
          tourism: "artwork",
          artwork_type: "statue",
          name: "Sindbad de Zeeman",
        }),
      ]);
      expect(result.map((f) => [f.type, f.name])).toEqual([
        ["sight", "Opa Gijs"],
        ["sight", "Sindbad de Zeeman"],
      ]);
      expect(result[0]).toMatchObject({
        wheelchair: null,
        changingTable: null,
        fee: null,
      });
    });

    it("leaves an unnamed bin or artwork out", () => {
      expect(
        parseFacilities([
          node(1, { amenity: "waste_basket", material: "metal" }),
          node(2, { amenity: "waste_basket", name: "  " }),
          node(3, { tourism: "artwork", artwork_type: "statue" }),
        ]),
      ).toEqual([]);
    });

    it("keeps a drinking fountain shaped as a statue as drinking water", () => {
      const [drink] = parseFacilities([
        node(6804096856, {
          amenity: "drinking_water",
          tourism: "artwork",
          artwork_type: "statue",
          name: "Drinkebroer",
        }),
      ]);
      expect(drink).toMatchObject({
        type: "drinking_water",
        name: "Drinkebroer",
      });
    });

    it("keeps two different statues side by side", () => {
      expect(
        parseFacilities([
          node(7056788150, { tourism: "artwork", name: "Reiziger" }),
          node(
            7056788149,
            { tourism: "artwork", name: "Reizigster" },
            50.80003,
          ),
        ]),
      ).toHaveLength(2);
    });

    it("keeps one of a sight mapped twice under the same name", () => {
      const result = parseFacilities([
        {
          type: "way",
          id: 60,
          center: { lat: 50.80004, lon: 6.88 },
          tags: { tourism: "artwork", name: "Pardoesbeeld" },
          ...meta,
        },
        node(61, { tourism: "artwork", name: "Pardoesbeeld" }),
      ]);
      expect(result.map((f) => f.osmId)).toEqual(["61"]);
    });

    it.each(["private", "no", "staff"])(
      "drops a sight with access=%s",
      (access) => {
        expect(
          parseFacilities([
            node(1, { tourism: "artwork", name: "Grimm", access }),
          ]),
        ).toEqual([]);
      },
    );
  });

  it("drops what is not one of our types", () => {
    expect(
      parseFacilities([
        node(1, { amenity: "restaurant", toilets: "yes" }),
        node(2, { amenity: "water_point" }),
        node(3, {}),
        // A fairy tale or a ride tagged as an attraction is not a sight (yet).
        node(4, { tourism: "attraction", name: "Roodkapje" }),
        node(5, { amenity: "fountain", name: "Wensbron" }),
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
    expect(ql).toContain("way(31052062)->.b;");
    expect(ql).toContain(".b out geom;");
    expect(ql).toContain(
      'nwr["amenity"~"^(toilets|drinking_water)$"](area.a);',
    );
    expect(ql).toContain('nwr["amenity"="waste_basket"]["name"](area.a);');
    expect(ql).toContain('nwr["tourism"="artwork"]["name"](area.a);');
    expect(ql).toContain("out meta center;");
    expect(buildFacilitiesQuery({ type: "relation", id: "7" })).toContain(
      "rel(7)->.b;",
    );
  });
});
