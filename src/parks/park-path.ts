import { NotFoundException, applyDecorators } from "@nestjs/common";
import { ApiParam } from "@nestjs/swagger";

/**
 * Swagger for the four path segments every park route starts with,
 * `:continent/:country/:city/:parkSlug`.
 */
export function ApiParkPath() {
  return applyDecorators(
    ApiParam({
      name: "continent",
      description: "Continent slug (e.g., 'europe', 'north-america')",
      example: "europe",
    }),
    ApiParam({
      name: "country",
      description: "Country slug (e.g., 'germany', 'united-states')",
      example: "germany",
    }),
    ApiParam({
      name: "city",
      description: "City slug (e.g., 'bruehl', 'orlando')",
      example: "bruehl",
    }),
    ApiParam({
      name: "parkSlug",
      description: "Park slug (e.g., 'phantasialand', 'magic-kingdom')",
      example: "phantasialand",
    }),
  );
}

/** The 404 for a park path that names no park. */
export function parkNotFound(
  continent: string,
  country: string,
  city: string,
  parkSlug: string,
): NotFoundException {
  return new NotFoundException(
    `Park with slug "${parkSlug}" not found in ${city}, ${country}, ${continent}`,
  );
}
