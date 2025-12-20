import { Client } from "pg";

const config = {
  user: "parkfan",
  password: process.env.DB_PASSWORD,
  host: "192.168.100.5",
  port: 5433,
  database: "parkfan",
  ssl: false,
};

async function run() {
  const client = new Client(config);
  try {
    await client.connect();
    console.log("✅ Connected to DB");

    // 1. Check holidays for Germany (DE) around Christmas 2025
    console.log("\n--- Holidays in Germany (DE) Dec 2025 ---");
    const holidayRes = await client.query(`
            SELECT date, name, "localName", "isNationwide", region
            FROM holidays 
            WHERE country = 'DE' AND date >= '2025-12-20' AND date <= '2025-12-31'
            ORDER BY date ASC;
        `);
    console.table(holidayRes.rows);

    // 2. Find Phantasialand
    const parkRes = await client.query(`
            SELECT id, name, slug, "countryCode", "regionCode"
            FROM parks 
            WHERE slug = 'phantasialand';
        `);
    const park = parkRes.rows[0];

    if (!park) {
      console.log("❌ Phantasialand NOT FOUND");
      return;
    }

    console.log("\n--- Phantasialand Entity ---");
    console.table(parkRes.rows);

    // 3. Check schedule entries for Phantasialand
    console.log("\n--- Schedule for Phantasialand Dec 2025 ---");
    const scheduleRes = await client.query(
      `
            SELECT date, "scheduleType", "openingTime", "closingTime", "isHoliday", "holidayName", "isBridgeDay"
            FROM schedule_entries 
            WHERE "parkId" = $1 AND date >= '2025-12-20' AND date <= '2025-12-31'
            ORDER BY date ASC;
        `,
      [park.id],
    );
    console.table(scheduleRes.rows);
  } catch (err) {
    console.error("❌ Error:", err);
  } finally {
    await client.end();
  }
}

run();
