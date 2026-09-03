import { ApiProperty } from "@nestjs/swagger";

export class TripWriteDto {
  @ApiProperty({
    description:
      "The plan as the browser holds it. Stored verbatim and handed back " +
      "unchanged — this API does not own the planner's shape. It does insist " +
      "the payload IS a plan (a version, a map of parks, each with a slug and " +
      "a map of dated days) and that it is under 256 KB, because an " +
      "unauthenticated write endpoint that accepts any JSON is a file host.",
    type: "object",
    additionalProperties: true,
  })
  payload: Record<string, unknown>;
}

export class TripResponseDto {
  @ApiProperty({
    example: "n7Qk2Fd3Xb9pLmZa",
    description:
      "The trip's id, and its ONLY credential. There is no account system for " +
      "visitors: whoever has this string can read and overwrite the trip, so a " +
      "UI that shows it has to say so. 96 bits of randomness, never derived " +
      "from the plan's contents.",
  })
  id: string;

  @ApiProperty({ type: "object", additionalProperties: true })
  payload: Record<string, unknown>;

  @ApiProperty({
    example: "2027-10-07T09:12:44.000Z",
    description:
      "When an untouched trip may be swept. Pushed forward on every write, so " +
      "a plan somebody keeps editing never expires.",
  })
  expiresAt: string;

  @ApiProperty({ example: "2026-09-03T09:12:44.000Z" })
  updatedAt: string;
}
