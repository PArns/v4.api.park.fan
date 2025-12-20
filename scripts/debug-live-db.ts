
import { Client } from 'pg';

const config = {
    user: 'parkfan',
    password: 'XYU9upd0pqj6uyd!rxh',
    host: '192.168.100.5',
    port: 5433,
    database: 'parkfan',
    ssl: false
};

async function run() {
    const client = new Client(config);
    try {
        await client.connect();
        console.log('✅ Connected to Live DB');

        // 1. Find all matching parks
        console.log('\n--- Finding Phantasialand Parks ---');
        const parkRes = await client.query(`
            SELECT id, name, slug, timezone, "geocodingAttemptedAt", "externalId", "queue_times_entity_id" as "qtId", "wiki_entity_id" as "wikiId"
            FROM parks 
            WHERE slug LIKE '%phantasia%' OR name LIKE '%Phantasia%';
        `);
        console.table(parkRes.rows);

        if (parkRes.rows.length === 0) {
            console.log('❌ Epic Universe NOT FOUND in DB');
            return;
        }

        // 2. Iterate and check data for each
        for (const park of parkRes.rows) {
            console.log(`\n\n=== Checking Park: ${park.name} (${park.slug}) ===`);
            console.log(`ID: ${park.id}`);

            // Check Schedule
            console.log('--- Schedule (schedule_entries) ---');
            const scheduleRes = await client.query(`
                SELECT date, "scheduleType", "openingTime", "closingTime" 
                FROM schedule_entries 
                WHERE "parkId" = $1 AND date >= CURRENT_DATE
                ORDER BY date ASC 
                LIMIT 5;
            `, [park.id]);

            if (scheduleRes.rows.length === 0) {
                console.log('❌ No schedule found.');
            } else {
                console.table(scheduleRes.rows);
            }

            // Check Attractions & Lands
            console.log('--- Attractions (Sample) ---');
            const attrRes = await client.query(`
                SELECT id, name, "land_name", "land_external_id"
                FROM attractions 
                WHERE "parkId" = $1 
                ORDER BY name ASC
                LIMIT 5;
            `, [park.id]);

            if (attrRes.rows.length === 0) {
                console.log('❌ No attractions found.');
            } else {
                console.table(attrRes.rows);
            }
        }

    } catch (err) {
        console.error('❌ DB Error:', err);
    } finally {
        await client.end();
    }
}

run();
