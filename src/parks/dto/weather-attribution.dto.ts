import { ApiProperty } from "@nestjs/swagger";

/**
 * Attribution metadata for weather data.
 *
 * Required by Open-Meteo's CC BY 4.0 licence: any place where Open-Meteo
 * data is displayed must include a credit + link.
 * See https://open-meteo.com/en/licence
 *
 * Surfacing this in API responses lets clients render the attribution
 * inline, and makes it obvious in the data itself which provider a given
 * weather payload came from — useful if we ever swap or add providers.
 */
export class WeatherAttributionDto {
  @ApiProperty({
    description: "Name of the data provider",
    example: "Open-Meteo.com",
  })
  provider: string;

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
    description: "URL of the licence text",
    example: "https://creativecommons.org/licenses/by/4.0/",
  })
  licenseUrl: string;

  @ApiProperty({
    description:
      "Human-readable attribution string clients can render verbatim",
    example: "Weather data by Open-Meteo.com",
  })
  attribution: string;
}

/**
 * Canonical attribution payload for Open-Meteo-sourced weather data.
 * Use this constant whenever a weather DTO is built from Open-Meteo.
 */
export const OPEN_METEO_ATTRIBUTION: WeatherAttributionDto = {
  provider: "Open-Meteo.com",
  url: "https://open-meteo.com/",
  license: "CC-BY-4.0",
  licenseUrl: "https://creativecommons.org/licenses/by/4.0/",
  attribution: "Weather data by Open-Meteo.com",
};
