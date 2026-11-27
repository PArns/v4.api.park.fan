import type { KnowledgeEntry } from "./knowledge-entry";

/**
 * The seed of the topic on where park.fan's numbers come from and when a field stays empty.
 * The facts follow the "How park.fan works" page (chapters 07 and 08) and
 * docs/frontend/effective-status.md; once seeded, the stored topic is edited in the admin.
 */
export const DATA_SOURCES: KnowledgeEntry = {
  id: "data_sources",
  readWhen:
    "where park.fan's numbers come from (wait-time sources, holidays, weather, opening hours) and " +
    "why some stay empty. Read it when the user asks where the data comes from, why a number " +
    "differs from the park's app or sign, or why a park, ride or rating shows nothing.",
  text: `Where the numbers come from:
- Wait times: every five minutes for every park on park.fan, from up to three public sources. ThemeParks.wiki is the base; Queue-Times.com and Wartezeiten.app add rides it lacks and correct a "closed" that is out of date. For a few parks one of those two is the only source. Every wait ever measured is kept, the quiet Tuesday mornings too.
- The wait is the one the park posts. park.fan fetches it every five minutes, so a sign that just changed can be a few minutes ahead.
- Holidays and school breaks: region by region, in Europe from Nager.Date and OpenHolidays, elsewhere from the calendars of authorities and school districts, the neighbouring regions' included, since day guests cross borders.
- Weather: Open-Meteo for the forecast and a short-term rain forecast in quarter hours; severe weather warnings from DWD and MeteoAlarm.
- Opening hours: from the park calendars. Where a park publishes none, today's and the past days' hours are worked out from ride activity and marked as estimated.
- Forecasts: three models, one for today, one for the coming weeks, one for the rest of the year, each checked against what really happened (topic forecasts).

Where park.fan would have to guess, it leaves the field empty:
- A park with no readable source: some parks show their waits only in their own app (Hansa-Park, only on the park's Wi-Fi). Its rides have no times and read unknown while the park is open, never closed.
- A ride outside its season: an ice rink in August reads out of season and does not count among the open rides.
- No basis for a rating: under about 30 operating days a park has no reference, so it reads "No forecast". A ride with no measured day in the past year gets no forecast and no best times.
Unknown means nobody reports anything, never that a ride is closed. Say what is missing and why in a few words; "not known" is never "there is none".`,
};
