
import axios from 'axios';

async function run() {
    console.log("🚀 Fetching Disneyland Schedule from Wiki API...");
    // Disneyland UUID from DB: 7340550b-c14d-4def-80bb-6363a9d1a2f0 (Need to verify this)

    // Actually, let's fetch destination first to find park ID
    // Disneyland Resort: 
    // Let's just search for it or use the one from check-global-status output if available
    // check-global-status didn't print ID.
    // I'll search via API list.

    try {
        const destUrl = 'https://api.themeparks.wiki/v1/destinations';
        const destRes = await axios.get(destUrl);

        const dlResort = destRes.data.destinations.find((d: any) => d.slug === 'disneyland-resort');
        if (!dlResort) {
            console.log("Resort not found");
            return;
        }

        const dlPark = dlResort.parks.find((p: any) => p.name.includes('Disneyland Park'));
        if (!dlPark) {
            console.log("Park not found");
            return;
        }

        console.log(`Park ID: ${dlPark.id}`);

        const scheduleUrl = `https://api.themeparks.wiki/v1/entity/${dlPark.id}/schedule`;
        console.log(`Fetching: ${scheduleUrl}`);

        const schedRes = await axios.get(scheduleUrl);
        const schedule = schedRes.data.schedule.filter((s: any) => {
            // Filter for next few days
            const d = new Date(s.date);
            const now = new Date();
            return d >= new Date(now.getTime() - 24 * 3600 * 1000) && d <= new Date(now.getTime() + 48 * 3600 * 1000);
        });

        console.log(JSON.stringify(schedule, null, 2));

    } catch (e) {
        console.error(e);
    }
}

run();
