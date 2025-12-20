
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
        console.log('✅ Connected to DB');

        // 1. Find Phantasialand
        const parkRes = await client.query(`SELECT id FROM parks WHERE slug = 'phantasialand'`);
        const phlId = parkRes.rows[0]?.id;

        if (!phlId) {
            console.log('❌ Phantasialand not found');
            return;
        }

        console.log(`Triggering gap filling for Phl (${phlId})...`);

        // Unfortunately, we can't easily trigger the NestJS service from here without a complex setup.
        // However, we can simulate the "fill-gaps" process or check if the live system's worker
        // will now pick it up correctly. 

        // For verification, I will manually insert a gap entry if it's missing, 
        // OR I can use a script that uses the actual backend code if available.

        // Since I can run ts-node, I'll try to trigger the processor or a script that uses the service.
        // But for now, let's just check if the new code works by running a small test case 
        // against the database to see if we CAN find the holiday now with the new logic.

    } catch (err) {
        console.error('❌ Error:', err);
    } finally {
        await client.end();
    }
}

run();
