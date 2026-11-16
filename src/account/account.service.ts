import { Injectable, type OnModuleInit } from "@nestjs/common";
import { InjectRepository } from "@nestjs/typeorm";
import { Repository } from "typeorm";
import { AdminAuthService } from "../admin/auth/admin-auth.service";
import type { UpdateAccountDataDto } from "./dto/account.dto";
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
    private readonly accounts: AdminAuthService,
  ) {}

  onModuleInit(): void {
    this.accounts.onAccountDeletion(async (userId) => {
      const result = await this.data.delete({ userId });
      return { accountData: result.affected ?? 0 };
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
}
