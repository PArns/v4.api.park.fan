#!/bin/bash
# ML Training Docker Test Script

set -e

echo "🐳 ML Training System - Docker Test"
echo "====================================="
echo ""

# Colors
GREEN='\033[0;32m'
RED='\033[0;31m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

# Check if docker-compose is running
echo "📋 Step 1: Checking Docker Compose services..."
if ! docker-compose -f docker-compose.yml ps | grep -q "ml-service"; then
    echo -e "${YELLOW}⚠️  ML service not running. Starting services...${NC}"
    docker-compose -f docker-compose.yml up -d ml-service postgres redis
    echo "Waiting 30s for services to be ready..."
    sleep 30
fi

# Get ML service container name
ML_CONTAINER=$(docker-compose -f docker-compose.yml ps -q ml-service)
if [ -z "$ML_CONTAINER" ]; then
    echo -e "${RED}❌ ML service container not found${NC}"
    exit 1
fi

echo -e "${GREEN}✅ ML service container: $ML_CONTAINER${NC}"
echo ""

# Test 1: Health check
echo "📋 Step 2: Testing ML service health..."
HEALTH_RESPONSE=$(docker exec $ML_CONTAINER curl -s http://localhost:8000/health)
echo "$HEALTH_RESPONSE" | jq .
if echo "$HEALTH_RESPONSE" | jq -e '.status == "healthy"' > /dev/null; then
    echo -e "${GREEN}✅ Health check passed${NC}"
else
    echo -e "${RED}❌ Health check failed${NC}"
    exit 1
fi
echo ""

# Test 2: Check training endpoint exists
echo "📋 Step 3: Testing /train endpoint availability..."
TRAIN_RESPONSE=$(docker exec $ML_CONTAINER curl -s -X POST http://localhost:8000/train \
    -H "Content-Type: application/json" \
    -d '{"version":"v_test_local"}')
echo "$TRAIN_RESPONSE" | jq .

if echo "$TRAIN_RESPONSE" | jq -e '.status == "training_started"' > /dev/null; then
    echo -e "${GREEN}✅ Training endpoint working${NC}"
    echo -e "${YELLOW}⏳ Training started in background...${NC}"
else
    echo -e "${YELLOW}⚠️  Training response: $TRAIN_RESPONSE${NC}"
fi
echo ""

# Test 3: Monitor training status
echo "📋 Step 4: Monitoring training status..."
for i in {1..10}; do
    echo "Check $i/10..."
    STATUS_RESPONSE=$(docker exec $ML_CONTAINER curl -s http://localhost:8000/train/status)
    echo "$STATUS_RESPONSE" | jq .
    
    STATUS=$(echo "$STATUS_RESPONSE" | jq -r '.status')
    
    if [ "$STATUS" == "completed" ]; then
        echo -e "${GREEN}✅ Training completed successfully!${NC}"
        break
    elif [ "$STATUS" == "failed" ]; then
        echo -e "${RED}❌ Training failed!${NC}"
        ERROR=$(echo "$STATUS_RESPONSE" | jq -r '.error')
        echo "Error: $ERROR"
        exit 1
    fi
    
    if [ $i -lt 10 ]; then
        echo "Waiting 30s before next check..."
        sleep 30
    fi
done
echo ""

# Test 4: Check model info
echo "📋 Step 5: Checking model information..."
MODEL_RESPONSE=$(docker exec $ML_CONTAINER curl -s http://localhost:8000/model/info)
if [ $? -eq 0 ]; then
    echo "$MODEL_RESPONSE" | jq .
    echo -e "${GREEN}✅ Model info retrieved${NC}"
else
    echo -e "${YELLOW}⚠️  No model loaded yet (expected if training just started)${NC}"
fi
echo ""

# Test 5: Check logs
echo "📋 Step 6: Recent ML service logs..."
echo -e "${YELLOW}Last 20 lines:${NC}"
docker logs --tail 20 $ML_CONTAINER
echo ""

echo "======================================="
echo -e "${GREEN}🎉 Docker test completed!${NC}"
echo "======================================="
