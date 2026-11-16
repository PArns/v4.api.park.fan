import { Module } from "@nestjs/common";
import { TypeOrmModule } from "@nestjs/typeorm";
import { AccountController } from "./account.controller";
import { AccountService } from "./account.service";
import { AccountData } from "./entities/account-data.entity";

/** Visitor accounts: registration, synced state, self-deletion. */
@Module({
  imports: [TypeOrmModule.forFeature([AccountData])],
  controllers: [AccountController],
  providers: [AccountService],
})
export class AccountModule {}
