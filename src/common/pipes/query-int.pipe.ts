import { BadRequestException, PipeTransform } from "@nestjs/common";

interface QueryIntOptions {
  /** Query parameter name, used in the 400 message. */
  name: string;
  /** Returned when the parameter is absent. */
  fallback?: number;
  /** Smaller values are clamped up to this. */
  min?: number;
  /** Larger values are clamped down to this. */
  max?: number;
}

/**
 * Parses an integer query parameter. Anything that is not an integer is a
 * caller's typo and answers 400; without this the global `ValidationPipe`
 * (`enableImplicitConversion`) turns `?page=abc` into `NaN`, which reached
 * TypeORM's `skip()` and came back as HTTP 500 (PAR-415).
 *
 * Out-of-range integers are clamped, not rejected, so a caller that already
 * sends `limit=500` keeps getting a page.
 */
export class QueryIntPipe implements PipeTransform<
  unknown,
  number | undefined
> {
  constructor(private readonly options: QueryIntOptions) {}

  transform(value: unknown): number | undefined {
    const { name, fallback, min, max } = this.options;
    if (value === undefined || value === null || value === "") {
      return fallback;
    }
    if (typeof value !== "number" && typeof value !== "string") {
      throw new BadRequestException(`${name} must be an integer`);
    }
    const parsed = typeof value === "number" ? value : Number(value);
    if (!Number.isInteger(parsed)) {
      throw new BadRequestException(`${name} must be an integer`);
    }
    let result = parsed;
    if (min !== undefined) result = Math.max(min, result);
    if (max !== undefined) result = Math.min(max, result);
    return result;
  }
}
