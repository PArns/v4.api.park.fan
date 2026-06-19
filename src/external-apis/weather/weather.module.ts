import { Module } from "@nestjs/common";
import { OpenMeteoClient } from "./open-meteo.client";
import { MeteoGateWarningsClient } from "./meteogate-warnings.client";

@Module({
  providers: [OpenMeteoClient, MeteoGateWarningsClient],
  exports: [OpenMeteoClient, MeteoGateWarningsClient],
})
export class WeatherModule {}
