import { Injectable, Logger, OnModuleInit } from "@nestjs/common";
import { ConfigService } from "@nestjs/config";
import * as fs from "fs/promises";
import * as path from "path";
import * as os from "os";
import * as tar from "tar";
import axios from "axios";
import maxmind, { CityResponse } from "maxmind";

const GEOIP_DOWNLOAD_URL =
  "https://download.maxmind.com/geoip/databases/GeoLite2-City/download?suffix=tar.gz";
// MaxMind redirects to R2 presigned URLs (mm-prod-geoip-databases.*.r2.cloudflarestorage.com). Client must follow redirects.

export interface GeoIpCoordinates {
  latitude: number;
  longitude: number;
}

/**
 * GeoIP Service using MaxMind GeoLite2-City.
 * Downloads/updates the DB every 48h (via Bull job) and resolves IP to city coordinates.
 */
@Injectable()
export class GeoipService implements OnModuleInit {
  private readonly logger = new Logger(GeoipService.name);
  private lookup: Awaited<
    ReturnType<typeof maxmind.open<CityResponse>>
  > | null = null;
  private dbPath: string = "";
  private lastOpenPath: string = "";

  constructor(private readonly configService: ConfigService) {}

  async onModuleInit(): Promise<void> {
    this.dbPath =
      this.configService.get<string>("GEOIP_DATABASE_PATH") ||
      path.join(process.cwd(), "data", "GeoLite2-City.mmdb");

    const dir = path.dirname(this.dbPath);
    await fs.mkdir(dir, { recursive: true }).catch(() => {});

    const hasCredentials =
      !!this.configService.get<string>("GEOIP_MAXMIND_ACCOUNT_ID") &&
      !!this.configService.get<string>("GEOIP_MAXMIND_LICENSE_KEY");
    this.logger.log(
      `GeoIP: path=${this.dbPath}, credentials=${hasCredentials ? "set" : "missing"}`,
    );

    const exists = await this.openDatabaseIfExists();
    if (!exists) {
      if (hasCredentials) {
        const canWrite = await this.checkDirWritable(dir);
        if (!canWrite) {
          this.logger.warn(
            `GeoIP: directory not writable: ${dir}. Fix volume permissions (e.g. chown) so GeoLite2-City can be downloaded.`,
          );
        } else {
          this.logger.log(
            "GeoLite2-City database not found. Downloading in background (app start not blocked).",
          );
          this.downloadAndReplace()
            .then(() => this.openDatabaseIfExists())
            .then((loaded) => {
              if (loaded) {
                this.logger.log("GeoLite2-City loaded after background download.");
              }
            })
            .catch((err) => {
              const msg =
                err?.response?.status != null
                  ? `HTTP ${err.response.status}`
                  : err?.message ?? String(err);
              this.logger.warn(
                `GeoLite2-City download on start failed: ${msg}. Nearby without lat/lng will fail until DB is present or next 48h update.`,
              );
            });
        }
      } else {
        this.logger.warn(
          "GeoLite2-City database not available. Set GEOIP_MAXMIND_ACCOUNT_ID and GEOIP_MAXMIND_LICENSE_KEY to download on start, or run the geoip-update job. Nearby without lat/lng will fail until DB is present.",
        );
      }
    }
  }

  /** Check if the GeoIP target directory is writable (e.g. volume permissions). */
  private async checkDirWritable(dir: string): Promise<boolean> {
    try {
      const probe = path.join(dir, `.write-probe-${Date.now()}`);
      await fs.writeFile(probe, "");
      await fs.unlink(probe);
      return true;
    } catch {
      return false;
    }
  }

  /**
   * Opens the MMDB file if it exists. Uses watchForUpdates so replacement by the update job is picked up.
   */
  private async openDatabaseIfExists(): Promise<boolean> {
    try {
      await fs.access(this.dbPath);
    } catch {
      return false;
    }
    try {
      this.lookup = await maxmind.open<CityResponse>(this.dbPath, {
        watchForUpdates: true,
      });
      this.lastOpenPath = this.dbPath;
      this.logger.log(`GeoLite2-City loaded: ${this.dbPath}`);
      return true;
    } catch (err) {
      this.logger.warn(
        `Failed to open GeoLite2-City at ${this.dbPath}: ${err}`,
      );
      return false;
    }
  }

  /**
   * Resolve IP to city coordinates (latitude, longitude). Returns null if not found or DB not loaded.
   */
  lookupCoordinates(ip: string): GeoIpCoordinates | null {
    if (!this.lookup) return null;
    if (!maxmind.validate(ip)) return null;
    const city = this.lookup.get(ip);
    if (!city?.location) return null;
    const { latitude, longitude } = city.location;
    if (
      typeof latitude !== "number" ||
      typeof longitude !== "number" ||
      Number.isNaN(latitude) ||
      Number.isNaN(longitude)
    ) {
      return null;
    }
    return { latitude, longitude };
  }

  /**
   * Whether the GeoIP database is available for lookups.
   */
  isAvailable(): boolean {
    return this.lookup !== null;
  }

  /**
   * Download GeoLite2-City from MaxMind (Basic Auth), extract, and replace the current DB file.
   * Called by the geoip-update Bull job. Requires GEOIP_MAXMIND_ACCOUNT_ID and GEOIP_MAXMIND_LICENSE_KEY.
   */
  async downloadAndReplace(): Promise<void> {
    const accountId = this.configService.get<string>(
      "GEOIP_MAXMIND_ACCOUNT_ID",
    );
    const licenseKey = this.configService.get<string>(
      "GEOIP_MAXMIND_LICENSE_KEY",
    );

    if (!accountId || !licenseKey) {
      this.logger.warn(
        "GEOIP_MAXMIND_ACCOUNT_ID and GEOIP_MAXMIND_LICENSE_KEY must be set to download GeoLite2-City. Skipping update.",
      );
      return;
    }

    const tmpDir = path.join(os.tmpdir(), `geoip-${Date.now()}`);
    const archivePath = path.join(tmpDir, "GeoLite2-City.tar.gz");

    try {
      await fs.mkdir(tmpDir, { recursive: true });

      this.logger.log("Downloading GeoLite2-City from MaxMind...");
      let response;
      try {
        response = await axios.get(GEOIP_DOWNLOAD_URL, {
          responseType: "arraybuffer",
          auth: {
            username: accountId,
            password: licenseKey,
          },
          timeout: 120_000,
          maxContentLength: 100 * 1024 * 1024, // 100 MB
          maxRedirects: 5, // MaxMind redirects to R2 presigned URL; must follow redirects
        });
      } catch (err: unknown) {
        const status = (err as { response?: { status?: number } })?.response?.status;
        const msg = (err as { message?: string })?.message ?? String(err);
        throw new Error(
          status != null ? `MaxMind download failed: HTTP ${status} - ${msg}` : `MaxMind download failed: ${msg}`,
        );
      }

      await fs.writeFile(archivePath, response.data);

      const extractDir = path.join(tmpDir, "extract");
      await fs.mkdir(extractDir, { recursive: true });
      await tar.x({ file: archivePath, cwd: extractDir });

      const mmdbPath = await this.findMmdbInDir(extractDir);
      if (!mmdbPath) {
        throw new Error("GeoLite2-City.mmdb not found in archive");
      }

      const targetDir = path.dirname(this.dbPath);
      await fs.mkdir(targetDir, { recursive: true });
      const targetPath = path.join(targetDir, "GeoLite2-City.mmdb");
      const targetPathTmp = `${targetPath}.${Date.now()}.tmp`;

      await fs.copyFile(mmdbPath, targetPathTmp);
      await fs.rename(targetPathTmp, targetPath);

      this.logger.log(`GeoLite2-City updated at ${targetPath}`);

      if (this.lastOpenPath === this.dbPath && this.lookup) {
        this.lookup = await maxmind.open<CityResponse>(this.dbPath, {
          watchForUpdates: true,
        });
        this.logger.log("GeoLite2-City reader reloaded.");
      } else if (!this.lookup) {
        await this.openDatabaseIfExists();
      }
    } finally {
      await fs.rm(tmpDir, { recursive: true, force: true }).catch(() => {});
    }
  }

  private async findMmdbInDir(dir: string): Promise<string | null> {
    const entries = await fs.readdir(dir, { withFileTypes: true });
    for (const e of entries) {
      const full = path.join(dir, e.name);
      if (e.isDirectory()) {
        const found = await this.findMmdbInDir(full);
        if (found) return found;
      } else if (e.name === "GeoLite2-City.mmdb") {
        return full;
      }
    }
    return null;
  }
}
