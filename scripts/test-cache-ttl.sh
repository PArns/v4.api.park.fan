#!/bin/bash

# Test Script for Dynamic Cache TTL Solution
# Tests the fix for park status caching issue

set -e

API_URL="${API_URL:-http://localhost:3000}"
PARK_SLUG="${PARK_SLUG:-phantasialand}"

echo "🧪 Testing Dynamic Cache TTL Solution"
echo "======================================"
echo ""

# Colors
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

# Test 1: Request park endpoint
echo -e "${YELLOW}Test 1: Request park endpoint${NC}"
echo "GET $API_URL/v1/parks/$PARK_SLUG"
echo ""

RESPONSE=$(curl -s "$API_URL/v1/parks/$PARK_SLUG")

# Parse response (using jq if available, otherwise show raw)
if command -v jq >/dev/null 2>&1; then
  STATUS=$(echo "$RESPONSE" | jq -r '.status')
  CACHED_AT=$(echo "$RESPONSE" | jq -r '.meta.cachedAt')
  CACHE_TTL=$(echo "$RESPONSE" | jq -r '.meta.cacheTTL')
  NEXT_OPENING=$(echo "$RESPONSE" | jq -r '.meta.nextOpeningTime')
  
  echo "Status: $STATUS"
  echo "Cached At: $CACHED_AT"
  echo "Cache TTL: $CACHE_TTL seconds ($(($CACHE_TTL / 60)) minutes)"
  
  if [ "$STATUS" == "CLOSED" ]; then
    echo "Next Opening: $NEXT_OPENING"
    
    if [ "$NEXT_OPENING" != "null" ]; then
      # Calculate time until opening
      if command -v date >/dev/null 2>&1; then
        NOW_TIMESTAMP=$(date +%s)
        OPENING_TIMESTAMP=$(date -j -f "%Y-%m-%dT%H:%M:%S" "${NEXT_OPENING:0:19}" +%s 2>/dev/null || echo "0")
        
        if [ "$OPENING_TIMESTAMP" != "0" ]; then
          SECONDS_UNTIL_OPENING=$(($OPENING_TIMESTAMP - $NOW_TIMESTAMP))
          MINUTES_UNTIL_OPENING=$(($SECONDS_UNTIL_OPENING / 60))
          
          echo ""
          echo -e "${GREEN}✅ Park opens in: $MINUTES_UNTIL_OPENING minutes${NC}"
          echo -e "${GREEN}✅ Cache expires in: $(($CACHE_TTL / 60)) minutes${NC}"
          
          # Verify TTL is less than time until opening
          if [ $CACHE_TTL -lt $SECONDS_UNTIL_OPENING ]; then
            echo -e "${GREEN}✅ Cache will expire before park opens (good!)${NC}"
          else
            echo -e "${RED}❌ WARNING: Cache will NOT expire before park opens${NC}"
          fi
        fi
      fi
    else
      echo -e "${YELLOW}⚠️  No next opening time found (off-season?)${NC}"
      echo -e "${YELLOW}   Using fallback TTL: $CACHE_TTL seconds${NC}"
    fi
  else
    echo -e "${GREEN}✅ Park is OPERATING${NC}"
    echo -e "${GREEN}✅ Using short TTL for live data: $CACHE_TTL seconds${NC}"
  fi
else
  echo "⚠️  jq not installed, showing raw response:"
  echo "$RESPONSE" | python3 -m json.tool || echo "$RESPONSE"
fi

echo ""
echo "======================================"
echo ""

# Test 2: Check Redis TTL (if redis-cli is available)
if command -v docker >/dev/null 2>&1; then
  echo -e "${YELLOW}Test 2: Check Redis Cache TTL${NC}"
  
  # Try to get park ID from response
  if command -v jq >/dev/null 2>&1; then
    PARK_ID=$(echo "$RESPONSE" | jq -r '.id')
    
    if [ "$PARK_ID" != "null" ]; then
      echo "Checking Redis key: park:integrated:$PARK_ID"
      
      REDIS_TTL=$(docker exec redis redis-cli TTL "park:integrated:$PARK_ID" 2>/dev/null || echo "-1")
      
      if [ "$REDIS_TTL" != "-1" ] && [ "$REDIS_TTL" != "-2" ]; then
        echo -e "${GREEN}✅ Redis TTL: $REDIS_TTL seconds ($(($REDIS_TTL / 60)) minutes)${NC}"
        
        # Compare with reported TTL
        TTL_DIFF=$(($CACHE_TTL - $REDIS_TTL))
        if [ $TTL_DIFF -lt 10 ] && [ $TTL_DIFF -gt -10 ]; then
          echo -e "${GREEN}✅ Redis TTL matches response metadata${NC}"
        else
          echo -e "${YELLOW}⚠️  TTL mismatch: Response=$CACHE_TTL, Redis=$REDIS_TTL${NC}"
        fi
      else
        echo -e "${YELLOW}⚠️  Key not found in Redis (cache miss or expired)${NC}"
      fi
    fi
  fi
else
  echo "⚠️  Docker not available, skipping Redis check"
fi

echo ""
echo "======================================"
echo -e "${GREEN}✅ Test complete${NC}"
echo ""
echo "Manual verification steps:"
echo "1. Check if park is currently closed"
echo "2. Verify cacheTTL expires before next opening time"
echo "3. Wait until cache expires and request again"
echo "4. Verify fresh data is fetched"
