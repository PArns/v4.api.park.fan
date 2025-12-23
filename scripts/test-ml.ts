import axios from "axios";

async function testMLPredictions() {
    console.log("═══════════════════════════════════════════════════════");
    console.log("ML SERVICE PREDICTION TEST");
    console.log("═══════════════════════════════════════════════════════\n");

    // 1. Check service health
    console.log("1️⃣  Checking ML Service Health...");
    try {
        const health = await axios.get("http://localhost:8000/");
        console.log(`   Status: ${health.data.status}`);
        console.log(`   Model Loaded: ${health.data.model_loaded}`);
        console.log(`   Model Version: ${health.data.model_version || "none"}`);

        if (!health.data.model_loaded) {
            console.log("\n⚠️  No model loaded yet. Training may still be in progress.");
            console.log("   Check: docker logs parkfan_ml_service\n");
            return;
        }
    } catch (error: any) {
        console.log(`   ❌ Service not reachable: ${error.message}\n`);
        return;
    }

    // 2. Get model info
    console.log("\n2️⃣  Model Information...");
    try {
        const info = await axios.get("http://localhost:8000/model/info");
        console.log(`   Version: ${info.data.version}`);
        console.log(`   Trained At: ${info.data.trainedAt}`);
        if (info.data.metrics) {
            console.log(`   Metrics:`);
            console.log(`     MAE: ${info.data.metrics.mae?.toFixed(2)} min`);
            console.log(`     RMSE: ${info.data.metrics.rmse?.toFixed(2)} min`);
            console.log(`     MAPE: ${info.data.metrics.mape?.toFixed(2)}%`);
            console.log(`     R²: ${info.data.metrics.r2?.toFixed(4)}`);
        }
    } catch (error: any) {
        console.log(`   ⚠️  Could not fetch model info: ${error.message}`);
    }

    // 3. Test prediction for Epic Universe
    console.log("\n3️⃣  Testing Predictions (Epic Universe)...");
    try {
        const parkResponse = await axios.get("http://localhost:3000/v1/parks/universals-epic-universe");
        const park = parkResponse.data;

        if (park.attractions && park.attractions.length > 0) {
            const testAttraction = park.attractions[0];

            console.log(`   Test Attraction: ${testAttraction.name}`);
            console.log(`   Current Wait: ${testAttraction.queues[0]?.waitTime || 0} min`);

            const predictionRequest = {
                attractionIds: [testAttraction.id],
                parkIds: [park.id],
                predictionType: "hourly"
            };

            const predResponse = await axios.post("http://localhost:8000/predict", predictionRequest);

            console.log(`\n   Predictions (next ${predResponse.data.count} hours):`);
            predResponse.data.predictions.slice(0, 5).forEach((p: any) => {
                const time = new Date(p.predictedTime).toLocaleTimeString();
                console.log(`     ${time}: ${p.predictedWaitTime} min (${p.crowdLevel}, confidence: ${p.confidence}%)`);
            });

            console.log(`\n   ✅ Predictions working!`);
        }
    } catch (error: any) {
        console.log(`   ❌ Prediction failed: ${error.response?.data?.detail || error.message}`);
    }

    // 4. Test NestJS ML integration
    console.log("\n4️⃣  Testing NestJS Integration...");
    try {
        const response = await axios.get("http://localhost:3000/v1/parks/universals-epic-universe");
        const park = response.data;

        if (park.predictions) {
            console.log(`   Hourly Predictions: ${park.predictions.hourly?.length || 0}`);
            console.log(`   Daily Predictions: ${park.predictions.daily?.length || 0}`);

            if (park.predictions.hourly && park.predictions.hourly.length > 0) {
                console.log(`   ✅ NestJS successfully calling ML service!`);
            } else {
                console.log(`   ⚠️  No predictions returned (ML service may have just started)`);
            }
        } else {
            console.log(`   ⚠️  No predictions field in response`);
        }
    } catch (error: any) {
        console.log(`   ❌ Integration test failed: ${error.message}`);
    }

    console.log("\n═══════════════════════════════════════════════════════");
    console.log("TEST COMPLETE");
    console.log("═══════════════════════════════════════════════════════");
}

testMLPredictions().catch(console.error);
