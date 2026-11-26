import { HttpException, HttpStatus, Injectable, Logger } from "@nestjs/common";
import { InjectRepository } from "@nestjs/typeorm";
import { Repository } from "typeorm";
import { z } from "zod";
import { AppConfig, type PlatformConfig } from "./app-config.entity";
import { VERSION_PATTERN, isBelowMinimum } from "./app-version.util";

const LANGUAGES = ["de", "en", "nl", "fr", "es", "it"] as const;

/** A third platform is one more entry here. */
export const PLATFORMS = ["ios", "android"] as const;
export type Platform = (typeof PLATFORMS)[number];

/** What `GET /v1/app/config` answers: every platform, always. */
export type AppPublicConfig = Record<Platform, PlatformConfig>;

export interface AppSample {
  version?: string;
  build?: number;
}

/** The admin's view: the values, who set them and what a sample app would meet. */
export interface AppConfigAdminView {
  platforms: AppPublicConfig;
  updatedBy: string | null;
  updatedAt: Date | null;
  samples: Record<
    Platform,
    { version: string; build: number | null; blocked: boolean; text: string }
  >;
}

const NO_BLOCK: PlatformConfig = {
  minVersion: null,
  minBuild: null,
  updateUrl: null,
  message: null,
};

const text = z.string().trim();
export const platformConfigSchema = z.object({
  minVersion: z
    .string()
    .trim()
    .regex(VERSION_PATTERN, "minVersion must look like 1, 1.0 or 1.0.1")
    .nullable(),
  minBuild: z.number().int().positive().nullable(),
  updateUrl: z
    .string()
    .trim()
    .max(500)
    .refine((u) => {
      try {
        return new URL(u).protocol === "https:";
      } catch {
        return false;
      }
    }, "updateUrl must be an https URL")
    .nullable(),
  message: z
    .strictObject(
      Object.fromEntries(
        LANGUAGES.map((l) => [l, text.max(300).nullish()]),
      ) as Record<
        (typeof LANGUAGES)[number],
        z.ZodOptional<z.ZodNullable<z.ZodString>>
      >,
    )
    .nullable(),
});

/** Where an app may start from: the minimum version per platform the owner sets in the admin. */
@Injectable()
export class AppConfigService {
  private readonly logger = new Logger(AppConfigService.name);

  constructor(
    @InjectRepository(AppConfig)
    private readonly repository: Repository<AppConfig>,
  ) {}

  /** Never throws: a database that cannot answer must not stop an app from starting. */
  async publicConfig(): Promise<AppPublicConfig> {
    try {
      return all(await this.repository.findOneBy({ id: 1 }));
    } catch (error) {
      this.logger.warn(`App config unreadable: ${(error as Error).message}`);
      return all(null);
    }
  }

  async view(sample?: AppSample): Promise<AppConfigAdminView> {
    return this.present(await this.repository.findOneBy({ id: 1 }), sample);
  }

  /** Replaces one platform's values; `body` is validated here, a refusal says which field. */
  async update(
    platform: Platform,
    body: unknown,
    updatedBy: string,
    sample?: AppSample,
  ): Promise<AppConfigAdminView> {
    const parsed = platformConfigSchema.safeParse(body);
    if (!parsed.success) {
      throw new HttpException(
        z.prettifyError(parsed.error),
        HttpStatus.BAD_REQUEST,
      );
    }
    const v = parsed.data;
    if (v.minBuild !== null && v.minVersion === null) {
      throw new HttpException(
        "minBuild needs a minVersion",
        HttpStatus.BAD_REQUEST,
      );
    }
    const message = Object.fromEntries(
      Object.entries(v.message ?? {}).filter(([, t]) => t),
    ) as PlatformConfig["message"];
    const row =
      (await this.repository.findOneBy({ id: 1 })) ??
      this.repository.create({ id: 1 });
    row.platforms = {
      ...row.platforms,
      [platform]: {
        ...v,
        message: message && Object.keys(message).length ? message : null,
      },
    };
    row.updatedBy = updatedBy;
    return this.present(await this.repository.save(row), sample);
  }

  private present(
    row: AppConfig | null,
    sample?: AppSample,
  ): AppConfigAdminView {
    const platforms = all(row);
    const version = sample?.version ?? "1.0.0";
    const build = sample?.build ?? null;
    const label = build === null ? version : `${version} (${build})`;
    const samples = Object.fromEntries(
      PLATFORMS.map((p) => {
        const blocked = isBelowMinimum({ version, build }, platforms[p]);
        return [
          p,
          {
            version,
            build,
            blocked,
            text: `${label} → ${blocked ? "blocked" : "allowed"}`,
          },
        ];
      }),
    ) as AppConfigAdminView["samples"];
    return {
      platforms,
      updatedBy: row?.updatedBy ?? null,
      updatedAt: row?.updatedAt ?? null,
      samples,
    };
  }
}

function all(row: AppConfig | null): AppPublicConfig {
  return Object.fromEntries(
    PLATFORMS.map((p) => [p, { ...NO_BLOCK, ...row?.platforms?.[p] }]),
  ) as AppPublicConfig;
}
