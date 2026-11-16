import {
  BadRequestException,
  Injectable,
  NotFoundException,
  PayloadTooLargeException,
  type OnModuleInit,
} from "@nestjs/common";
import { InjectRepository } from "@nestjs/typeorm";
import { Repository } from "typeorm";
import { AdminAuthService } from "../admin/auth/admin-auth.service";
import {
  ACCOUNT_STORAGE_KEY,
  MAX_ACCOUNT_STORAGE_BYTES,
  MAX_ACCOUNT_STORAGE_KEYS,
  type UpdateAccountDataDto,
} from "./dto/account.dto";
import { AccountStorageEntry } from "./entities/account-storage.entity";
import {
  AccountData,
  type AccountFavorites,
} from "./entities/account-data.entity";

/** What an account carries before its browser wrote anything. */
const EMPTY_FAVORITES: AccountFavorites = {
  parks: [],
  attractions: [],
  shows: [],
  restaurants: [],
};

/** An account's synced state as the API answers it. */
export interface AccountDataView {
  favorites: AccountFavorites;
  riddenRides: string[];
  temperatureUnit: "C" | "F" | null;
  updatedAt: string | null;
}

/** Each id once, in the order it first came. */
function unique(ids: readonly string[]): string[] {
  return [...new Set(ids)];
}

/**
 * A visitor's synced state: read, written field by field, and erased with the
 * account (registered with `AdminAuthService.onAccountDeletion`).
 */
@Injectable()
export class AccountService implements OnModuleInit {
  constructor(
    @InjectRepository(AccountData)
    private readonly data: Repository<AccountData>,
    @InjectRepository(AccountStorageEntry)
    private readonly storage: Repository<AccountStorageEntry>,
    private readonly accounts: AdminAuthService,
  ) {}

  onModuleInit(): void {
    this.accounts.onAccountDeletion(async (userId) => {
      const [data, storage] = await Promise.all([
        this.data.delete({ userId }),
        this.storage.delete({ userId }),
      ]);
      return {
        accountData: data.affected ?? 0,
        accountStorage: storage.affected ?? 0,
      };
    });
  }

  /** The account's state, empty until its browser wrote some. */
  async read(userId: string): Promise<AccountDataView> {
    const row = await this.data.findOne({ where: { userId } });
    return {
      favorites: row?.favorites ?? EMPTY_FAVORITES,
      riddenRides: row?.riddenRides ?? [],
      temperatureUnit: row?.temperatureUnit ?? null,
      updatedAt: row?.updatedAt?.toISOString() ?? null,
    };
  }

  /** Replaces the fields the body names; the others stay. */
  async write(
    userId: string,
    patch: UpdateAccountDataDto,
  ): Promise<AccountDataView> {
    const current = await this.read(userId);
    await this.data.save(
      this.data.create({
        userId,
        favorites: patch.favorites
          ? {
              parks: unique(patch.favorites.parks),
              attractions: unique(patch.favorites.attractions),
              shows: unique(patch.favorites.shows),
              restaurants: unique(patch.favorites.restaurants),
            }
          : current.favorites,
        riddenRides: patch.riddenRides
          ? unique(patch.riddenRides)
          : current.riddenRides,
        temperatureUnit: patch.temperatureUnit ?? current.temperatureUnit,
      }),
    );
    return this.read(userId);
  }

  /** The account's storage keys, newest first, with their size; not the values. */
  async listStorage(
    userId: string,
  ): Promise<{ key: string; bytes: number; updatedAt: string }[]> {
    const rows = await this.storage
      .createQueryBuilder("s")
      .select("s.key", "key")
      .addSelect("octet_length(s.value::text)", "bytes")
      .addSelect("s.updated_at", "updatedAt")
      .where("s.user_id = :userId", { userId })
      .orderBy("s.updated_at", "DESC")
      .getRawMany<{ key: string; bytes: string; updatedAt: Date }>();
    return rows.map((row) => ({
      key: row.key,
      bytes: Number(row.bytes),
      updatedAt: new Date(row.updatedAt).toISOString(),
    }));
  }

  /** One stored value; 404 when the key holds nothing. */
  async readStorage(
    userId: string,
    key: string,
  ): Promise<{ key: string; value: unknown; updatedAt: string }> {
    const row = await this.storage.findOne({
      where: { userId, key: storageKey(key) },
    });
    if (!row) throw new NotFoundException("Nothing stored under this key");
    return {
      key: row.key,
      value: row.value,
      updatedAt: row.updatedAt.toISOString(),
    };
  }

  /**
   * Stores a value under a key, replacing what was there. Refused past the size of one value or
   * the number of keys an account keeps, so the storage stays a feature's notebook rather than a
   * free disk.
   */
  async writeStorage(
    userId: string,
    key: string,
    value: unknown,
  ): Promise<{ key: string; updatedAt: string }> {
    const name = storageKey(key);
    if (
      Buffer.byteLength(JSON.stringify(value), "utf8") >
      MAX_ACCOUNT_STORAGE_BYTES
    ) {
      throw new PayloadTooLargeException({
        error: "value-too-large",
        message: `A stored value may be at most ${MAX_ACCOUNT_STORAGE_BYTES} bytes as JSON`,
      });
    }
    const exists = await this.storage.exists({ where: { userId, key: name } });
    if (!exists) {
      const count = await this.storage.count({ where: { userId } });
      if (count >= MAX_ACCOUNT_STORAGE_KEYS) {
        throw new BadRequestException({
          error: "too-many-keys",
          message: `An account keeps at most ${MAX_ACCOUNT_STORAGE_KEYS} keys`,
        });
      }
    }
    // Raw upsert: TypeORM's typed one cannot take an opaque JSON value.
    await this.storage.query(
      `INSERT INTO account_storage (user_id, key, value, updated_at)
       VALUES ($1::uuid, $2, $3::jsonb, now())
       ON CONFLICT (user_id, key) DO UPDATE SET value = EXCLUDED.value, updated_at = now()`,
      [userId, name, JSON.stringify(value)],
    );
    return this.readStorage(userId, name).then(({ key: k, updatedAt }) => ({
      key: k,
      updatedAt,
    }));
  }

  /** Removes a key; true when there was something under it. */
  async deleteStorage(userId: string, key: string): Promise<boolean> {
    const result = await this.storage.delete({ userId, key: storageKey(key) });
    return (result.affected ?? 0) > 0;
  }
}

/** A key as the storage takes it, or a 400. */
function storageKey(key: string): string {
  if (!ACCOUNT_STORAGE_KEY.test(key)) {
    throw new BadRequestException({
      error: "invalid-key",
      message:
        "A key is 1 to 64 lowercase letters, digits, dots, underscores or hyphens",
    });
  }
  return key;
}
