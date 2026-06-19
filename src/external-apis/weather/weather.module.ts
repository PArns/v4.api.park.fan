import { Module } from "@nestjs/common";
import { OpenMeteoClient } from "./open-meteo.client";
import { MeteoGateWarningsClient } from "./meteogate-warnings.client";
import { BrightSkyWarningsClient } from "./brightsky-warnings.client";

@Module({
  providers: [
    OpenMeteoClient,
    MeteoGateWarningsClient,
    BrightSkyWarningsClient,
  ],
  exports: [OpenMeteoClient, MeteoGateWarningsClient, BrightSkyWarningsClient],
})
export class WeatherModule {}
