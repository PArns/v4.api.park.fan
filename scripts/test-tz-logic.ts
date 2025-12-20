import { formatInTimeZone } from "date-fns-tz";

const date = new Date("2025-12-26T00:00:00Z");
const timezone = "Europe/Berlin";

console.log("UTC ISO:", date.toISOString());
console.log("UTC Date string:", date.toISOString().split("T")[0]);
console.log(
  "Park Timezone (Europe/Berlin):",
  formatInTimeZone(date, timezone, "yyyy-MM-dd"),
);

const date2 = new Date("2025-12-25T23:30:00Z"); // Still Dec 25th in UTC, but Dec 26th in Berlin
console.log("\nTesting 23:30 UTC (Dec 25) vs Berlin (Dec 26):");
console.log("UTC ISO:", date2.toISOString());
console.log("UTC Date string:", date2.toISOString().split("T")[0]);
console.log(
  "Park Timezone (Europe/Berlin):",
  formatInTimeZone(date2, timezone, "yyyy-MM-dd"),
);
