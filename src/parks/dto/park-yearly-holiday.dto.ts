import { ApiProperty } from "@nestjs/swagger";

/**
 * One day of `/predictions/yearly`'s `holidays`: a public holiday, a school
 * break or a bridge day in the park's own region, by the calendar's rule.
 */
export class ParkYearlyHolidayDto {
  @ApiProperty({
    description: "Park-local day (YYYY-MM-DD)",
    example: "2026-12-25",
  })
  date: string;

  @ApiProperty({
    description: "A public holiday at the park's own location.",
  })
  isHoliday: boolean;

  @ApiProperty({
    description:
      "School break in the park's own region. The weekends and public " +
      "holidays a break absorbs count too, as in the calendar.",
  })
  isSchoolVacation: boolean;

  @ApiProperty({
    description: "A working day between a public holiday and the weekend.",
  })
  isBridgeDay: boolean;

  @ApiProperty({
    description:
      "English name of the public holiday, as in the calendar's " +
      "`events[].name`. Only on an `isHoliday` day.",
    required: false,
    example: "Christmas Day",
  })
  holidayName?: string;

  @ApiProperty({
    description:
      "English name of the school break. Only on a day a break row covers: " +
      "the weekends it absorbs read `isSchoolVacation` without a name.",
    required: false,
    example: "Christmas Holidays",
  })
  schoolVacationName?: string;
}
