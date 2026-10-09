import { ConfigService } from "@nestjs/config";
import * as fs from "fs/promises";
import maxmind from "maxmind";
import axios from "axios";
import * as tar from "tar";
import { GeoipService } from "./geoip.service";

jest.mock("fs/promises");
jest.mock("maxmind", () => ({
  __esModule: true,
  default: { open: jest.fn(), validate: jest.fn() },
}));
jest.mock("axios");
jest.mock("tar", () => ({ x: jest.fn() }));

const mockedFs = fs as jest.Mocked<typeof fs>;
const mockedMaxmind = maxmind as unknown as {
  open: jest.Mock;
  validate: jest.Mock;
};
const mockedAxios = axios as jest.Mocked<typeof axios>;
const mockedTar = tar as unknown as { x: jest.Mock };

function makeService(config: Record<string, string | undefined> = {}) {
  const configService = {
    get: jest.fn((key: string) => config[key]),
  } as unknown as ConfigService;
  return new GeoipService(configService);
}

/** A reader stub whose `get` answers from a map of ip -> city record. */
function readerFor(records: Record<string, unknown>) {
  return { get: jest.fn((ip: string) => records[ip] ?? null) };
}

describe("GeoipService", () => {
  beforeEach(() => {
    jest.resetAllMocks();
    mockedFs.mkdir.mockResolvedValue(undefined);
    mockedFs.rm.mockResolvedValue(undefined);
    mockedMaxmind.validate.mockImplementation((ip: string) =>
      /^[0-9a-f:.]+$/i.test(ip),
    );
  });

  describe("lookupCoordinates", () => {
    const records = {
      "8.8.8.8": { location: { latitude: 37.751, longitude: -97.822 } },
      "2001:4860:4860::8888": {
        location: { latitude: 48.1, longitude: 11.6 },
      },
      "10.0.0.1": null,
      "1.2.3.4": { country: { iso_code: "XX" } },
      "5.6.7.8": { location: { latitude: Number.NaN, longitude: 1 } },
      "9.9.9.9": { location: { latitude: 52.5 } },
    };

    async function loadedService() {
      mockedFs.access.mockResolvedValue(undefined);
      mockedMaxmind.open.mockResolvedValue(readerFor(records));
      const service = makeService({ GEOIP_DATABASE_PATH: "/data/geo.mmdb" });
      await service.onModuleInit();
      return service;
    }

    it("returns null before the database is loaded", () => {
      const service = makeService();
      expect(service.isAvailable()).toBe(false);
      expect(service.lookupCoordinates("8.8.8.8")).toBeNull();
    });

    it("resolves an IPv4 address", async () => {
      const service = await loadedService();
      expect(service.isAvailable()).toBe(true);
      expect(service.lookupCoordinates("8.8.8.8")).toEqual({
        latitude: 37.751,
        longitude: -97.822,
      });
    });

    it("resolves an IPv6 address", async () => {
      const service = await loadedService();
      expect(service.lookupCoordinates("2001:4860:4860::8888")).toEqual({
        latitude: 48.1,
        longitude: 11.6,
      });
    });

    it("returns null for a private address the database does not know", async () => {
      const service = await loadedService();
      expect(service.lookupCoordinates("10.0.0.1")).toBeNull();
    });

    it("returns null for an unknown address", async () => {
      const service = await loadedService();
      expect(service.lookupCoordinates("203.0.113.9")).toBeNull();
    });

    it("returns null for a string that is not an IP", async () => {
      const service = await loadedService();
      expect(service.lookupCoordinates("not an ip")).toBeNull();
    });

    it("returns null when the record has no location", async () => {
      const service = await loadedService();
      expect(service.lookupCoordinates("1.2.3.4")).toBeNull();
    });

    it("returns null for NaN or missing coordinates", async () => {
      const service = await loadedService();
      expect(service.lookupCoordinates("5.6.7.8")).toBeNull();
      expect(service.lookupCoordinates("9.9.9.9")).toBeNull();
    });
  });

  describe("onModuleInit", () => {
    it("opens the database from GEOIP_DATABASE_PATH with update watching", async () => {
      mockedFs.access.mockResolvedValue(undefined);
      mockedMaxmind.open.mockResolvedValue(readerFor({}));
      const service = makeService({ GEOIP_DATABASE_PATH: "/data/geo.mmdb" });

      await service.onModuleInit();

      expect(mockedMaxmind.open).toHaveBeenCalledWith("/data/geo.mmdb", {
        watchForUpdates: true,
      });
      expect(service.isAvailable()).toBe(true);
    });

    it("stays unavailable when the database file is missing and no credentials are set", async () => {
      mockedFs.access.mockRejectedValue(new Error("ENOENT"));
      const service = makeService({ GEOIP_DATABASE_PATH: "/data/geo.mmdb" });

      await service.onModuleInit();

      expect(mockedMaxmind.open).not.toHaveBeenCalled();
      expect(mockedAxios.get).not.toHaveBeenCalled();
      expect(service.isAvailable()).toBe(false);
      expect(service.lookupCoordinates("8.8.8.8")).toBeNull();
    });

    it("stays unavailable when the file exists but cannot be opened", async () => {
      mockedFs.access.mockResolvedValue(undefined);
      mockedMaxmind.open.mockRejectedValue(new Error("corrupt"));
      const service = makeService({ GEOIP_DATABASE_PATH: "/data/geo.mmdb" });

      await service.onModuleInit();

      expect(service.isAvailable()).toBe(false);
    });

    it("does not download when credentials are set but the directory is not writable", async () => {
      mockedFs.access.mockRejectedValue(new Error("ENOENT"));
      mockedFs.writeFile.mockRejectedValue(new Error("EACCES"));
      const service = makeService({
        GEOIP_DATABASE_PATH: "/data/geo.mmdb",
        GEOIP_MAXMIND_ACCOUNT_ID: "id",
        GEOIP_MAXMIND_LICENSE_KEY: "key",
      });

      await service.onModuleInit();

      expect(mockedAxios.get).not.toHaveBeenCalled();
      expect(service.isAvailable()).toBe(false);
    });

    it("survives a failing background download without throwing", async () => {
      mockedFs.access.mockRejectedValue(new Error("ENOENT"));
      mockedFs.writeFile.mockResolvedValue(undefined);
      mockedFs.unlink.mockResolvedValue(undefined);
      mockedAxios.get.mockRejectedValue({
        message: "Unauthorized",
        response: { status: 401 },
      });
      const service = makeService({
        GEOIP_DATABASE_PATH: "/data/geo.mmdb",
        GEOIP_MAXMIND_ACCOUNT_ID: "id",
        GEOIP_MAXMIND_LICENSE_KEY: "key",
      });

      await expect(service.onModuleInit()).resolves.toBeUndefined();
      await new Promise((resolve) => setImmediate(resolve));

      expect(mockedAxios.get).toHaveBeenCalledTimes(1);
      expect(service.isAvailable()).toBe(false);
    });
  });

  describe("downloadAndReplace", () => {
    it("skips the download without credentials", async () => {
      const service = makeService({ GEOIP_DATABASE_PATH: "/data/geo.mmdb" });

      await service.downloadAndReplace();

      expect(mockedAxios.get).not.toHaveBeenCalled();
    });

    it("reports an HTTP status when the download fails", async () => {
      mockedAxios.get.mockRejectedValue({
        message: "Unauthorized",
        response: { status: 401 },
      });
      const service = makeService({
        GEOIP_MAXMIND_ACCOUNT_ID: "id",
        GEOIP_MAXMIND_LICENSE_KEY: "key",
      });

      await expect(service.downloadAndReplace()).rejects.toThrow(
        "MaxMind download failed: HTTP 401 - Unauthorized",
      );
      expect(mockedFs.rm).toHaveBeenCalledWith(expect.any(String), {
        recursive: true,
        force: true,
      });
    });

    it("reports the message when the download fails without a response", async () => {
      mockedAxios.get.mockRejectedValue(new Error("timeout"));
      const service = makeService({
        GEOIP_MAXMIND_ACCOUNT_ID: "id",
        GEOIP_MAXMIND_LICENSE_KEY: "key",
      });

      await expect(service.downloadAndReplace()).rejects.toThrow(
        "MaxMind download failed: timeout",
      );
    });

    it("fails when the archive holds no GeoLite2-City.mmdb", async () => {
      mockedAxios.get.mockResolvedValue({ data: Buffer.from("x") });
      mockedFs.writeFile.mockResolvedValue(undefined);
      mockedTar.x.mockResolvedValue(undefined);
      mockedFs.readdir.mockResolvedValue([
        { name: "README.txt", isDirectory: () => false },
      ] as never);
      const service = makeService({
        GEOIP_MAXMIND_ACCOUNT_ID: "id",
        GEOIP_MAXMIND_LICENSE_KEY: "key",
      });

      await expect(service.downloadAndReplace()).rejects.toThrow(
        "GeoLite2-City.mmdb not found in archive",
      );
    });

    it("finds the file in a nested directory and swaps it in", async () => {
      mockedAxios.get.mockResolvedValue({ data: Buffer.from("x") });
      mockedFs.writeFile.mockResolvedValue(undefined);
      mockedFs.copyFile.mockResolvedValue(undefined);
      mockedFs.rename.mockResolvedValue(undefined);
      mockedFs.access.mockRejectedValue(new Error("ENOENT"));
      mockedFs.unlink.mockResolvedValue(undefined);
      mockedTar.x.mockResolvedValue(undefined);
      mockedFs.readdir.mockImplementation((async (dir: string) =>
        dir.endsWith("extract")
          ? [{ name: "GeoLite2-City_20261009", isDirectory: () => true }]
          : [
              { name: "GeoLite2-City.mmdb", isDirectory: () => false },
            ]) as never);
      mockedMaxmind.open.mockResolvedValue(readerFor({}));
      // No credentials, so onModuleInit only sets dbPath and does not download.
      const service = makeService({ GEOIP_DATABASE_PATH: "/data/geo.mmdb" });
      await service.onModuleInit();
      const credentialed = service as unknown as {
        configService: { get: jest.Mock };
      };
      credentialed.configService.get.mockImplementation((key: string) =>
        key.startsWith("GEOIP_MAXMIND") ? "secret" : undefined,
      );

      mockedFs.access.mockResolvedValue(undefined);

      await service.downloadAndReplace();

      expect(mockedFs.rename).toHaveBeenCalledWith(
        expect.stringMatching(/^\/data\/GeoLite2-City\.mmdb\.\d+\.tmp$/),
        "/data/GeoLite2-City.mmdb",
      );
      expect(mockedMaxmind.open).toHaveBeenCalledWith("/data/geo.mmdb", {
        watchForUpdates: true,
      });
      expect(service.isAvailable()).toBe(true);
    });
  });
});
