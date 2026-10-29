import { Injectable, Logger, OnModuleInit } from "@nestjs/common";
import { ConfigService } from "@nestjs/config";
import * as fs from "fs/promises";
import * as path from "path";
import * as os from "os";
import * as tar from "tar";
import axios from "axios";
import maxmind, { AsnResponse, CityResponse } from "maxmind";

/** The MaxMind databases this service keeps: the city for coordinates and country, the network. */
type Edition = "GeoLite2-City" | "GeoLite2-ASN";

// MaxMind redirects to R2 presigned URLs (mm-prod-geoip-databases.*.r2.cloudflarestorage.com). Client must follow redirects.
const downloadUrlOf = (edition: Edition) =>
  `https://download.maxmind.com/geoip/databases/${edition}/download?suffix=tar.gz`;

export interface GeoIpCoordinates {
  latitude: number;
  longitude: number;
}

/**
 * GeoIP Service using MaxMind GeoLite2-City, and GeoLite2-ASN for the network
 * an address belongs to. Downloads/updates both every 48h (via Bull job) and
 * resolves an IP to city coordinates, its country and its network's number.
 * The ASN database is optional: without it the site statistic reads every
 * network as unknown, and nothing else depends on it.
 */
@Injectable()
export class GeoipService implements OnModuleInit {
  private readonly logger = new Logger(GeoipService.name);
  private lookup: Awaited<
    ReturnType<typeof maxmind.open<CityResponse>>
  > | null = null;
  private dbPath: string = "";
  private lastOpenPath: string = "";
  private asnLookup: Awaited<
    ReturnType<typeof maxmind.open<AsnResponse>>
  > | null = null;
  private asnPath: string = "";

  constructor(private readonly configService: ConfigService) {}

  async onModuleInit(): Promise<void> {
    this.dbPath =
      this.configService.get<string>("GEOIP_DATABASE_PATH") ||
      path.join(process.cwd(), "data", "GeoLite2-City.mmdb");

    const dir = path.dirname(this.dbPath);
    await fs.mkdir(dir, { recursive: true }).catch(() => {});
    this.asnPath =
      this.configService.get<string>("GEOIP_ASN_DATABASE_PATH") ||
      path.join(dir, "GeoLite2-ASN.mmdb");

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
                this.logger.log(
                  "GeoLite2-City loaded after background download.",
                );
              }
            })
            .catch((err) => {
              this.logger.warn(
                `GeoLite2-City download on start failed: ${describeError(err)}. Nearby without lat/lng will fail until DB is present or next 48h update.`,
              );
            });
        }
      } else {
        this.logger.warn(
          "GeoLite2-City database not available. Set GEOIP_MAXMIND_ACCOUNT_ID and GEOIP_MAXMIND_LICENSE_KEY to download on start, or run the geoip-update job. Nearby without lat/lng will fail until DB is present.",
        );
      }
    }

    // Without the city database the download above fetches both.
    if (!(await this.openAsnIfExists()) && hasCredentials && exists) {
      this.downloadAsn().catch((err) =>
        this.logger.warn(
          `GeoLite2-ASN download on start failed: ${describeError(err)}. The site statistic reads every network as unknown until the next update.`,
        ),
      );
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
   * ISO 3166-1 alpha-2 country of an address, lowercase (`de`), or null when
   * the database is not loaded or does not place it. The site statistic keeps
   * this and nothing else of the address.
   */
  lookupCountry(ip: string): string | null {
    if (!this.lookup) return null;
    if (!maxmind.validate(ip)) return null;
    const code = this.lookup.get(ip)?.country?.iso_code;
    return typeof code === "string" && /^[A-Za-z]{2}$/.test(code)
      ? code.toLowerCase()
      : null;
  }

  /**
   * Whether the GeoIP database is available for lookups.
   */
  isAvailable(): boolean {
    return this.lookup !== null;
  }

  /**
   * The number of the autonomous system (the network) an address belongs to,
   * or null when the ASN database is not loaded, the address is not valid or
   * the database does not place it. Read in memory and not kept.
   */
  lookupAsn(ip: string): number | null {
    if (!this.asnLookup) return null;
    if (!maxmind.validate(ip)) return null;
    const asn = this.asnLookup.get(ip)?.autonomous_system_number;
    return typeof asn === "number" && Number.isInteger(asn) && asn > 0
      ? asn
      : null;
  }

  /** Whether the ASN database is loaded, so that a null ASN means "not placed" rather than "not known". */
  isAsnAvailable(): boolean {
    return this.asnLookup !== null;
  }

  private async openAsnIfExists(): Promise<boolean> {
    try {
      await fs.access(this.asnPath);
    } catch {
      return false;
    }
    try {
      this.asnLookup = await maxmind.open<AsnResponse>(this.asnPath, {
        watchForUpdates: true,
      });
      this.logger.log(`GeoLite2-ASN loaded: ${this.asnPath}`);
      return true;
    } catch (err) {
      this.logger.warn(
        `Failed to open GeoLite2-ASN at ${this.asnPath}: ${err}`,
      );
      return false;
    }
  }

  /** Downloads GeoLite2-ASN to its path and opens it. */
  private async downloadAsn(): Promise<void> {
    if (!(await this.downloadEdition("GeoLite2-ASN", this.asnPath))) return;
    this.logger.log(`GeoLite2-ASN updated at ${this.asnPath}`);
    if (this.asnLookup) {
      this.asnLookup = await maxmind.open<AsnResponse>(this.asnPath, {
        watchForUpdates: true,
      });
    } else {
      await this.openAsnIfExists();
    }
  }

  /**
   * Download GeoLite2-City and GeoLite2-ASN from MaxMind (Basic Auth), extract, and replace the
   * current DB files. Called by the geoip-update Bull job. Requires GEOIP_MAXMIND_ACCOUNT_ID and
   * GEOIP_MAXMIND_LICENSE_KEY. A failed city download throws; a failed ASN download only warns,
   * so the nearby endpoint never waits on the statistic's database.
   */
  async downloadAndReplace(): Promise<void> {
    const targetPath = path.join(
      path.dirname(this.dbPath),
      "GeoLite2-City.mmdb",
    );
    if (!(await this.downloadEdition("GeoLite2-City", targetPath))) return;
    this.logger.log(`GeoLite2-City updated at ${targetPath}`);

    if (this.lastOpenPath === this.dbPath && this.lookup) {
      this.lookup = await maxmind.open<CityResponse>(this.dbPath, {
        watchForUpdates: true,
      });
      this.logger.log("GeoLite2-City reader reloaded.");
    } else if (!this.lookup) {
      await this.openDatabaseIfExists();
    }

    if (this.asnPath) {
      await this.downloadAsn().catch((err) =>
        this.logger.warn(`GeoLite2-ASN update failed: ${describeError(err)}`),
      );
    }
  }

  /**
   * Downloads one edition's archive, extracts it and swaps its `.mmdb` in at `targetPath`. False
   * without credentials, which it warns about; throws when the download or the archive fails.
   */
  private async downloadEdition(
    edition: Edition,
    targetPath: string,
  ): Promise<boolean> {
    const accountId = this.configService.get<string>(
      "GEOIP_MAXMIND_ACCOUNT_ID",
    );
    const licenseKey = this.configService.get<string>(
      "GEOIP_MAXMIND_LICENSE_KEY",
    );

    if (!accountId || !licenseKey) {
      this.logger.warn(
        `GEOIP_MAXMIND_ACCOUNT_ID and GEOIP_MAXMIND_LICENSE_KEY must be set to download ${edition}. Skipping update.`,
      );
      return false;
    }

    const tmpDir = path.join(os.tmpdir(), `geoip-${edition}-${Date.now()}`);
    const archivePath = path.join(tmpDir, `${edition}.tar.gz`);

    try {
      await fs.mkdir(tmpDir, { recursive: true });

      this.logger.log(`Downloading ${edition} from MaxMind...`);
      let response;
      try {
        response = await axios.get(downloadUrlOf(edition), {
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
        const status = (err as { response?: { status?: number } })?.response
          ?.status;
        const msg = (err as { message?: string })?.message ?? String(err);
        throw new Error(
          status != null
            ? `MaxMind download failed: HTTP ${status} - ${msg}`
            : `MaxMind download failed: ${msg}`,
        );
      }

      await fs.writeFile(archivePath, response.data);

      const extractDir = path.join(tmpDir, "extract");
      await fs.mkdir(extractDir, { recursive: true });
      await tar.x({ file: archivePath, cwd: extractDir });

      const mmdbPath = await this.findMmdbInDir(extractDir, `${edition}.mmdb`);
      if (!mmdbPath) {
        throw new Error(`${edition}.mmdb not found in archive`);
      }

      await fs.mkdir(path.dirname(targetPath), { recursive: true });
      const targetPathTmp = `${targetPath}.${Date.now()}.tmp`;

      await fs.copyFile(mmdbPath, targetPathTmp);
      await fs.rename(targetPathTmp, targetPath);
      return true;
    } finally {
      await fs.rm(tmpDir, { recursive: true, force: true }).catch(() => {});
    }
  }

  private async findMmdbInDir(
    dir: string,
    fileName: string,
  ): Promise<string | null> {
    const entries = await fs.readdir(dir, { withFileTypes: true });
    for (const e of entries) {
      const full = path.join(dir, e.name);
      if (e.isDirectory()) {
        const found = await this.findMmdbInDir(full, fileName);
        if (found) return found;
      } else if (e.name === fileName) {
        return full;
      }
    }
    return null;
  }
}

/** An axios error's HTTP status, or the message. */
function describeError(err: unknown): string {
  const status = (err as { response?: { status?: number } })?.response?.status;
  return status != null
    ? `HTTP ${status}`
    : ((err as { message?: string })?.message ?? String(err));
}
