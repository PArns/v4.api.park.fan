import { Module } from "@nestjs/common";
import { OpenMeteoClient } from "./open-meteo.client";

@Module({
  providers: [OpenMeteoClient],
  exports: [OpenMeteoClient],
})
export class WeatherModule {}
