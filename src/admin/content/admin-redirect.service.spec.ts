import {
  BadRequestException,
  ConflictException,
  NotFoundException,
} from "@nestjs/common";
import {
  AdminRedirectService,
  parseRedirectPath,
} from "./admin-redirect.service";
import type { AdminPrincipal } from "../auth/admin-principal";

const PARK_ID = "11111111-1111-4111-8111-111111111111";
const OTHER_PARK_ID = "22222222-2222-4222-8222-222222222222";
const RIDE_ID = "33333333-3333-4333-8333-333333333333";
const OTHER_RIDE_ID = "44444444-4444-4444-8444-444444444444";
const GEO = {
  continentSlug: "europe",
  countrySlug: "germany",
  citySlug: "bruehl",
};
const PARK = {
  id: PARK_ID,
  name: "Phantasialand",
  slug: "phantasialand",
  ...GEO,
};
const RIDE = {
  id: RIDE_ID,
  name: "Taron",
  slug: "taron",
  parkId: PARK_ID,
  park: PARK,
};
const ADMIN = { email: "editor@example.com" } as AdminPrincipal;
const BASE = "/parks/europe/germany/bruehl/phantasialand";

function build(
  options: {
    park?: object | null;
    ride?: object | null;
    livePark?: boolean;
    liveRide?: boolean;
    parkAlias?: object | null;
    rideAliases?: object[];
  } = {},
) {
  const parks = {
    findOne: jest.fn(async ({ where }: { where: { id?: string } }) =>
      where.id ? (options.park === undefined ? PARK : options.park) : PARK,
    ),
    exists: jest.fn(async () => options.livePark ?? false),
  };
  const attractions = {
    findOne: jest.fn(async () =>
      options.ride === undefined ? RIDE : options.ride,
    ),
    exists: jest.fn(async () => options.liveRide ?? false),
  };
  const create = (row: object) => row;
  const save = jest.fn(async (row: object) => ({
    id: "alias-1",
    createdAt: new Date(0),
    ...row,
  }));
  const parkAliases = {
    findOne: jest.fn(async () => options.parkAlias ?? null),
    create,
    save,
    delete: jest.fn(async () => ({})),
  };
  const attractionAliases = {
    find: jest.fn(async () => options.rideAliases ?? []),
    create,
    save,
    delete: jest.fn(async () => ({})),
  };
  const curation = { publish: jest.fn(async () => undefined) };
  const audit = { record: jest.fn(async () => null) };
  const service = new AdminRedirectService(
    parks as never,
    attractions as never,
    parkAliases as never,
    attractionAliases as never,
    curation as never,
    audit as never,
  );
  return {
    service,
    parks,
    save,
    parkAliases,
    attractionAliases,
    curation,
    audit,
  };
}

describe("parseRedirectPath", () => {
  it("reads a park and a ride path, with or without locale", () => {
    expect(parseRedirectPath(`/de${BASE}`)).toEqual({
      ...GEO,
      slug: "phantasialand",
      rideSlug: null,
    });
    expect(parseRedirectPath(`${BASE}/taron/`)).toMatchObject({
      rideSlug: "taron",
    });
  });

  it("refuses any other shape", () => {
    for (const path of [
      "parks/europe/germany/bruehl/phantasialand",
      "/de/blog/some-post",
      "/de/parks/europe/germany/bruehl",
      `${BASE}/taron/extra`,
      `${BASE}?x=1`,
      "/de/parks/europe/germany/bruehl/Phantasia_Land",
    ]) {
      expect(parseRedirectPath(path)).toBeNull();
    }
  });
});

describe("AdminRedirectService.create", () => {
  it("rejects a non-canonical id and a bad path before reading anything", async () => {
    const { service, parks } = build();
    await expect(
      service.create(`/de${BASE}-old`, { kind: "park", id: "nope" }, ADMIN),
    ).rejects.toBeInstanceOf(BadRequestException);
    await expect(
      service.create("/de/blog/x", { kind: "park", id: PARK_ID }, ADMIN),
    ).rejects.toBeInstanceOf(BadRequestException);
    expect(parks.findOne).not.toHaveBeenCalled();
  });

  it("writes a park alias, audits it and publishes outside-in", async () => {
    const { service, save, audit, curation } = build();
    const result = await service.create(
      "/de/parks/europe/germany/bruehl/phantasialand-old",
      { kind: "park", id: PARK_ID },
      ADMIN,
    );
    expect(save).toHaveBeenCalledWith(
      expect.objectContaining({
        parkId: PARK_ID,
        slug: "phantasialand-old",
        createdBy: ADMIN.email,
      }),
    );
    expect(result).toMatchObject({
      created: true,
      redirect: {
        kind: "park",
        fromPath: "/parks/europe/germany/bruehl/phantasialand-old",
        toPath: BASE,
        targetId: PARK_ID,
      },
    });
    expect(audit.record).toHaveBeenCalledWith(
      expect.objectContaining({ action: "redirect.create", entityId: PARK_ID }),
    );
    expect(curation.publish).toHaveBeenCalledWith(PARK_ID, []);
  });

  it("refuses an unknown or retired target", async () => {
    await expect(
      build({ park: null }).service.create(
        `${BASE}-old`,
        { kind: "park", id: PARK_ID },
        ADMIN,
      ),
    ).rejects.toBeInstanceOf(NotFoundException);
    await expect(
      build({ ride: { ...RIDE, retiredAt: new Date() } }).service.create(
        `${BASE}/old`,
        { kind: "attraction", id: RIDE_ID },
        ADMIN,
      ),
    ).rejects.toBeInstanceOf(BadRequestException);
  });

  it("409s on a live park page and on an alias of another park", async () => {
    await expect(
      build({ livePark: true }).service.create(
        `${BASE}-other`,
        { kind: "park", id: PARK_ID },
        ADMIN,
      ),
    ).rejects.toBeInstanceOf(ConflictException);
    await expect(
      build({ parkAlias: { parkId: OTHER_PARK_ID } }).service.create(
        `${BASE}-old`,
        { kind: "park", id: PARK_ID },
        ADMIN,
      ),
    ).rejects.toBeInstanceOf(ConflictException);
  });

  it("returns an identical existing alias without writing", async () => {
    const existing = {
      id: "alias-0",
      parkId: PARK_ID,
      ...GEO,
      slug: "phantasialand-old",
      createdBy: null,
      createdAt: new Date(0),
    };
    const { service, save, curation } = build({ parkAlias: existing });
    const result = await service.create(
      `${BASE}-old`,
      { kind: "park", id: PARK_ID },
      ADMIN,
    );
    expect(result.created).toBe(false);
    expect(result.redirect.id).toBe("alias-0");
    expect(save).not.toHaveBeenCalled();
    expect(curation.publish).not.toHaveBeenCalled();
  });

  it("writes a ride alias only inside the ride's current park", async () => {
    const { service, save, curation } = build();
    await expect(
      service.create(
        "/parks/europe/germany/bruehl/other-park/old",
        { kind: "attraction", id: RIDE_ID },
        ADMIN,
      ),
    ).rejects.toBeInstanceOf(BadRequestException);
    await expect(
      service.create(BASE, { kind: "attraction", id: RIDE_ID }, ADMIN),
    ).rejects.toBeInstanceOf(BadRequestException);

    const result = await service.create(
      `/en${BASE}/taron-old`,
      { kind: "attraction", id: RIDE_ID },
      ADMIN,
    );
    expect(save).toHaveBeenCalledWith(
      expect.objectContaining({
        attractionId: RIDE_ID,
        slug: "taron-old",
        createdBy: ADMIN.email,
      }),
    );
    expect(result.redirect).toMatchObject({
      fromPath: `${BASE}/taron-old`,
      toPath: `${BASE}/taron`,
    });
    expect(curation.publish).toHaveBeenCalledWith(PARK_ID, [RIDE_ID]);
  });

  it("409s on a live ride, a park sub-page and another ride's alias", async () => {
    for (const [options, path] of [
      [{ liveRide: true }, `${BASE}/klugheim`],
      [{}, `${BASE}/with-kids`],
      [
        {
          rideAliases: [
            {
              attractionId: OTHER_RIDE_ID,
              attraction: { parkId: PARK_ID },
            },
          ],
        },
        `${BASE}/old`,
      ],
    ] as Array<[Parameters<typeof build>[0], string]>) {
      await expect(
        build(options).service.create(
          path,
          { kind: "attraction", id: RIDE_ID },
          ADMIN,
        ),
      ).rejects.toBeInstanceOf(ConflictException);
    }
  });

  it("turns a lost race on the unique index into a 409", async () => {
    const { service, save } = build();
    save.mockRejectedValueOnce(
      Object.assign(new Error("dup"), { code: "23505" }),
    );
    await expect(
      service.create(`${BASE}-old`, { kind: "park", id: PARK_ID }, ADMIN),
    ).rejects.toBeInstanceOf(ConflictException);
  });
});

describe("AdminRedirectService.remove", () => {
  it("deletes a hand-made park alias, audits and publishes", async () => {
    const alias = {
      id: "alias-1",
      parkId: PARK_ID,
      park: PARK,
      ...GEO,
      slug: "phantasialand-old",
      createdBy: ADMIN.email,
    };
    const { service, parkAliases, audit, curation } = build({
      parkAlias: alias,
    });
    await service.remove(`/de${BASE}-old`, ADMIN);
    expect(parkAliases.delete).toHaveBeenCalledWith({ id: "alias-1" });
    expect(audit.record).toHaveBeenCalledWith(
      expect.objectContaining({ action: "redirect.delete" }),
    );
    expect(curation.publish).toHaveBeenCalledWith(PARK_ID, []);
  });

  it("refuses an alias a rename, merge or the sync recorded", async () => {
    const { service, parkAliases } = build({
      parkAlias: { id: "a", parkId: PARK_ID, createdBy: null },
    });
    await expect(service.remove(`${BASE}-old`, ADMIN)).rejects.toBeInstanceOf(
      ConflictException,
    );
    expect(parkAliases.delete).not.toHaveBeenCalled();

    await expect(
      build({
        rideAliases: [
          { id: "b", attractionId: RIDE_ID, createdBy: null, attraction: RIDE },
        ],
      }).service.remove(`${BASE}/old`, ADMIN),
    ).rejects.toBeInstanceOf(ConflictException);
  });

  it("deletes a hand-made ride alias and 404s on nothing", async () => {
    const { service, attractionAliases, curation } = build({
      rideAliases: [
        {
          id: "c",
          attractionId: RIDE_ID,
          slug: "old",
          createdBy: ADMIN.email,
          attraction: RIDE,
        },
      ],
    });
    await service.remove(`${BASE}/old`, ADMIN);
    expect(attractionAliases.delete).toHaveBeenCalledWith({ id: "c" });
    expect(curation.publish).toHaveBeenCalledWith(PARK_ID, [RIDE_ID]);

    await expect(
      build().service.remove(`${BASE}/old`, ADMIN),
    ).rejects.toBeInstanceOf(NotFoundException);
    await expect(
      build().service.remove(`${BASE}-old`, ADMIN),
    ).rejects.toBeInstanceOf(NotFoundException);
  });
});
