#!/bin/bash
#
# Test script for geo-discovery endpoint
#

BASE_URL="http://localhost:3000/v1"

echo "🧪 Testing Geo-Discovery Endpoint"
echo "=================================="
echo ""

# Test 1: Full structure
echo "1️⃣  Testing GET /discovery/geo (full structure)"
RESPONSE=$(curl -s -w "\n%{http_code}" "$BASE_URL/discovery/geo")
HTTP_CODE=$(echo "$RESPONSE" | tail -1)
BODY=$(echo "$RESPONSE" | sed '$d')

if [ "$HTTP_CODE" = "200" ]; then
    echo "   ✅ HTTP 200 OK"
    echo "   📊 Summary:"
    echo "$BODY" | jq '{continentCount, countryCount, cityCount, parkCount}'
    echo ""
    echo "   🌍 Sample Continent:"
    echo "$BODY" | jq '.continents[0] | {name, slug, countryCount, parkCount, sampleCountry: .countries[0].name}'
else
    echo "   ❌ HTTP $HTTP_CODE FAILED"
    echo "$BODY" | jq .
fi

echo ""

# Test 2: Cache Headers
echo "2️⃣  Testing Cache Headers"
HEADERS=$(curl -I -s "$BASE_URL/discovery/geo" | grep -i cache-control)
if [ -n "$HEADERS" ]; then
    echo "   ✅ $HEADERS"
else
    echo "   ❌ No Cache-Control header found"
fi

echo ""

# Test 3: Continents only
echo "3️⃣  Testing GET /discovery/continents"
RESPONSE=$(curl -s -w "\n%{http_code}" "$BASE_URL/discovery/continents")
HTTP_CODE=$(echo "$RESPONSE" | tail -1)
BODY=$(echo "$RESPONSE" | sed '$d')

if [ "$HTTP_CODE" = "200" ]; then
    echo "   ✅ HTTP 200 OK"
    echo "   🌍 Continents:"
    echo "$BODY" | jq '[.[] | {name, slug, countryCount}]'
else
    echo "   ❌ HTTP $HTTP_CODE FAILED"
fi

echo ""

# Test 4: Countries in Europe
echo "4️⃣  Testing GET /discovery/continents/europe"
RESPONSE=$(curl -s -w "\n%{http_code}" "$BASE_URL/discovery/continents/europe")
HTTP_CODE=$(echo "$RESPONSE" | tail -1)
BODY=$(echo "$RESPONSE" | sed '$d')

if [ "$HTTP_CODE" = "200" ]; then
    echo "   ✅ HTTP 200 OK"
    echo "   🇪🇺 European Countries (first 3):"
    echo "$BODY" | jq '.[0:3] | [.[] | {name, code, cityCount, parkCount}]'
else
    echo "   ❌ HTTP $HTTP_CODE FAILED"
fi

echo ""

# Test 5: Cities in Germany
echo "5️⃣  Testing GET /discovery/continents/europe/germany"
RESPONSE=$(curl -s -w "\n%{http_code}" "$BASE_URL/discovery/continents/europe/germany")
HTTP_CODE=$(echo "$RESPONSE" | tail -1)
BODY=$(echo "$RESPONSE" | sed '$d')

if [ "$HTTP_CODE" = "200" ]; then
    echo "   ✅ HTTP 200 OK"
    echo "   🇩🇪 German Cities (first 3):"
    echo "$BODY" | jq '.[0:3] | [.[] | {name, slug, parkCount, samplePark: .parks[0].name}]'
else
    echo "   ❌ HTTP $HTTP_CODE FAILED"
fi

echo ""
echo "✨ Testing complete!"
