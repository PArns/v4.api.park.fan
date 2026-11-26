import { Module } from "@nestjs/common";
import { TypeOrmModule } from "@nestjs/typeorm";
import { AppConfig } from "./app-config.entity";
import {
  AdminAppConfigController,
  AppConfigController,
} from "./app-config.controller";
import { AppConfigService } from "./app-config.service";

@Module({
  imports: [TypeOrmModule.forFeature([AppConfig])],
  controllers: [AppConfigController, AdminAppConfigController],
  providers: [AppConfigService],
})
export class AppConfigModule {}
