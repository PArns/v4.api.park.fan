
import { DataSource } from "typeorm";
import * as dotenv from "dotenv";
import * as path from "path";
import axios from "axios";

// Entities - Minimal definition for check
// We import from src but need to make sure ts-node can handle it or just use raw query
// Using raw query is safer for a script without full nest context

dotenv.config({ path: path.resolve(__dirname, "../.env") });

async function check() {
    console.log("🔍 Verifying ML System Status...");

    const POSTGRES_USER = process.env.POSTGRES_USER || "parkfan";
    const POSTGRES_PASSWORD = process.env.POSTGRES_PASSWORD || "parkfan_dev_password";
    const POSTGRES_DB = process.env.POSTGRES_DB || "parkfan";
    const DB_HOST = process.env.DB_HOST || "localhost";
    const DB_PORT = parseInt(process.env.DB_PORT || "5432");

    const dataSource = new DataSource({
        type: "postgres",
        host: DB_HOST,
        port: DB_PORT,
        username: POSTGRES_USER,
        password: POSTGRES_PASSWORD,
        database: POSTGRES_DB,
    });

    try {
        await dataSource.initialize();
        console.log("✅ Database connected.");

        // Check Parks
        const parks = await dataSource.query(`SELECT count(*) as count FROM parks`);
        console.log(`🎡 Parks count: ${parks[0].count}`);

        // Check Attractions
        const attractions = await dataSource.query(`SELECT count(*) as count FROM attractions`);
        console.log(`🎢 Attractions count: ${attractions[0].count}`);

        // Check Wait Times (recent)
        const recentWaitTimes = await dataSource.query(`SELECT count(*) as count FROM queue_data WHERE timestamp > NOW() - INTERVAL '1 hour'`);
        console.log(`⏱️  Wait Times (last 1h): ${recentWaitTimes[0].count}`);

        // Check Total Wait Times
        const totalWaitTimes = await dataSource.query(`SELECT count(*) as count FROM queue_data`);
        console.log(`📚 Total Wait Times: ${totalWaitTimes[0].count}`);

        // Check ML Models
        const models = await dataSource.query(`SELECT * FROM ml_models ORDER BY "createdAt" DESC LIMIT 5`);
        console.log(`🤖 ML Models in DB: ${models.length}`);
        models.forEach((m: any) => {
            console.log(`   - ${m.version} (Active: ${m.isActive}) - MAE: ${m.mae}`);
        });

        // Check ML Service API
        try {
            const health = await axios.get("http://localhost:8000/health");
            console.log("✅ ML Service Health: ", health.data);
        } catch (e: any) {
            console.log("❌ ML Service Health Check Failed:", e.message);
        }

        try {
            const info = await axios.get("http://localhost:8000/model/info");
            console.log("ℹ️  ML Model Info: ", info.data);
        } catch (e: any) {
            console.log("⚠️  ML Model Info Check Failed (likely no model loaded):", e.message);
        }

    } catch (error) {
        console.error("❌ Error during verification:", error);
    } finally {
        if (dataSource.isInitialized) {
            await dataSource.destroy();
        }
    }
}

check();
