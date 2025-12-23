export interface WartezeitenParkOverride {
  /** Force a specific timezone */
  timezone?: string;
  /** Force specific latitude */
  latitude?: number;
  /** Force specific longitude */
  longitude?: number;
  /** Force a specific name (renaming) */
  overrideName?: string;
}

/**
 * Whitelist of parks that are allowed to be created from Wartezeiten.app ONLY.
 *
 * Usually, we only create parks if they exist in ThemeParks.wiki or Queue-Times.
 * However, some parks (like Nigloland) are only in Wartezeiten.app and are high quality enough to include.
 *
 * Key: The "Cleaned" Name of the park (e.g. without "(FR)" suffix)
 * Value: Configuration overrides (Timezone, Geo) since Wartezeiten lacks this data.
 */
export const WARTEZEITEN_CREATION_WHITELIST: Record<
  string,
  WartezeitenParkOverride
> = {
  Nigloland: {
    timezone: "Europe/Paris",
    latitude: 48.262162,
    longitude: 4.611446,
  },
};
