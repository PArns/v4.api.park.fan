import { Module } from "@nestjs/common";
import { TypeOrmModule } from "@nestjs/typeorm";
import { AccountController } from "./account.controller";
import { AccountService } from "./account.service";
import { AccountData } from "./entities/account-data.entity";
import { AccountStorageEntry } from "./entities/account-storage.entity";

/** Visitor accounts: registration, synced state, a member's own storage, self-deletion. */
@Module({
  imports: [TypeOrmModule.forFeature([AccountData, AccountStorageEntry])],
  controllers: [AccountController],
  providers: [AccountService],
})
export class AccountModule {}
