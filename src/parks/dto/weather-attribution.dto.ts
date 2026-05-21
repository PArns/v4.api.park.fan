import { ApiProperty } from "@nestjs/swagger";

/**
 * Attribution metadata for weather data.
 *
 * Required by Open-Meteo's CC BY 4.0 licence: any place where Open-Meteo
 * data is displayed must include a credit + link.
 * See https://open-meteo.com/en/licence
 *
 * Kept minimal on purpose — three non-localisable fields (link, licence
 * identifier, attribution text). Display labels are the client's job.
 */
export class WeatherAttributionDto {
  @ApiProperty({
    description: "URL of the data provider (link target for attribution)",
    example: "https://open-meteo.com/",
  })
  url: string;

  @ApiProperty({
    description: "SPDX-style identifier of the data licence",
    example: "CC-BY-4.0",
  })
  license: string;

  @ApiProperty({
    description:
      "Canonical attribution string clients should render next to the data",
    example: "Weather data by Open-Meteo.com",
  })
  attribution: string;
}

/**
 * Canonical attribution payload for Open-Meteo-sourced weather data.
 * Use this constant whenever a weather DTO is built from Open-Meteo.
 */
export const OPEN_METEO_ATTRIBUTION: WeatherAttributionDto = {
  url: "https://open-meteo.com/",
  license: "CC-BY-4.0",
  attribution: "Weather data by Open-Meteo.com",
};
