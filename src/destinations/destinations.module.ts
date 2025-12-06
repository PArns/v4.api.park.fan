import { Module } from "@nestjs/common";
import { TypeOrmModule } from "@nestjs/typeorm";
import { Destination } from "./entities/destination.entity";
import { DestinationsService } from "./destinations.service";
import { DestinationsController } from "./destinations.controller";
import { ThemeParksModule } from "../external-apis/themeparks/themeparks.module";

@Module({
  imports: [TypeOrmModule.forFeature([Destination]), ThemeParksModule],
  controllers: [DestinationsController],
  providers: [DestinationsService],
  exports: [DestinationsService],
})
export class DestinationsModule {}
