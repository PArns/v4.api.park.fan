import { Module } from "@nestjs/common";
import { TypeOrmModule } from "@nestjs/typeorm";
import { AdminAuthModule } from "../admin/auth/admin-auth.module";
import { Park } from "../parks/entities/park.entity";
import { AdminFacilitiesController } from "./admin-facilities.controller";
import { ParkFacilityImport } from "./entities/park-facility-import.entity";
import { ParkFacilityOsm } from "./entities/park-facility-osm.entity";
import { FacilitiesController } from "./facilities.controller";
import { FacilitiesService } from "./facilities.service";
import { OverpassClient } from "./overpass.client";

/** Park facilities from OpenStreetMap: docs/frontend/facilities.md. */
@Module({
  imports: [
    TypeOrmModule.forFeature([Park, ParkFacilityOsm, ParkFacilityImport]),
    AdminAuthModule,
  ],
  controllers: [FacilitiesController, AdminFacilitiesController],
  providers: [FacilitiesService, OverpassClient],
})
export class FacilitiesModule {}
