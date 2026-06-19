import { ApiProperty } from "@nestjs/swagger";
import { WeatherWarning } from "../entities/weather-warning.entity";

/**
 * A severe-weather warning on the weather response. German and English variants
 * are both included; the frontend picks per locale. Source: MeteoGate
 * (EUMETNET → DWD/MeteoAlarm).
 */
export class WeatherWarningDto {
  @ApiProperty({ description: "Stable warning id (CAP alert id)." })
  alertId: string;

  @ApiProperty({ description: "Event type (German), e.g. 'GEWITTER'." })
  event: string;

  @ApiProperty({ required: false, description: "Event type (English)." })
  eventEn?: string | null;

  @ApiProperty({
    required: false,
    description: "Severity: Minor | Moderate | Severe | Extreme.",
  })
  severity?: string | null;

  @ApiProperty({ required: false, description: "CAP urgency." })
  urgency?: string | null;

  @ApiProperty({
    required: false,
    description: "CAP category / awareness type.",
  })
  category?: string | null;

  @ApiProperty({ required: false, description: "Valid from (ISO 8601)." })
  onset?: string | null;

  @ApiProperty({ required: false, description: "Valid until (ISO 8601)." })
  expires?: string | null;

  @ApiProperty({ required: false, description: "Headline (German)." })
  headline?: string | null;

  @ApiProperty({ required: false, description: "Headline (English)." })
  headlineEn?: string | null;

  @ApiProperty({ required: false, description: "Description (German)." })
  description?: string | null;

  @ApiProperty({ required: false, description: "Description (English)." })
  descriptionEn?: string | null;

  @ApiProperty({ required: false, description: "Safety instruction (German)." })
  instruction?: string | null;

  @ApiProperty({
    required: false,
    description: "Safety instruction (English).",
  })
  instructionEn?: string | null;

  @ApiProperty({
    required: false,
    description: "Affected area name, e.g. 'Kreis Freyung-Grafenau'.",
  })
  area?: string | null;

  @ApiProperty({ description: "Source identifier, e.g. 'meteogate'." })
  source: string;

  /** Map a stored warning entity to the response shape. */
  static fromEntity(w: WeatherWarning): WeatherWarningDto {
    return {
      alertId: w.alertId,
      event: w.event,
      eventEn: w.eventEn,
      severity: w.severity,
      urgency: w.urgency,
      category: w.category,
      onset: w.onset ? w.onset.toISOString() : null,
      expires: w.expires ? w.expires.toISOString() : null,
      headline: w.headline,
      headlineEn: w.headlineEn,
      description: w.description,
      descriptionEn: w.descriptionEn,
      instruction: w.instruction,
      instructionEn: w.instructionEn,
      area: w.area,
      source: w.source,
    };
  }
}
