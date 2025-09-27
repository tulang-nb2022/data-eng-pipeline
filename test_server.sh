#!/bin/bash
# Test script for Simple Secure S3 OData Server

# Colors for output
GREEN='\033[0;32m'
RED='\033[0;31m'
YELLOW='\033[1;33m'
NC='\033[0m'

print_status() {
    echo -e "${GREEN}[TEST]${NC} $1"
}

print_error() {
    echo -e "${RED}[ERROR]${NC} $1"
}

print_warning() {
    echo -e "${YELLOW}[WARNING]${NC} $1"
}

# Configuration
SERVER_URL=${1:-"http://localhost:8000"}
USERNAME=${2:-"test_user"}
PASSWORD=${3:-"test_password123"}

echo "🧪 Testing S3 OData Server"
echo "=========================="
echo "Server URL: $SERVER_URL"
echo "Username: $USERNAME"
echo "Password: $PASSWORD"
echo ""

# Test 1: Health Check
print_status "Testing health endpoint..."
HEALTH_RESPONSE=$(curl -s -w "%{http_code}" -o /tmp/health_response.json "$SERVER_URL/health")
HEALTH_CODE=$(echo $HEALTH_RESPONSE | tail -c 4)

if [ "$HEALTH_CODE" = "200" ]; then
    print_status "✅ Health check passed"
    cat /tmp/health_response.json | jq '.' 2>/dev/null || cat /tmp/health_response.json
else
    print_error "❌ Health check failed (HTTP $HEALTH_CODE)"
fi
echo ""

# Test 2: Authentication
print_status "Testing authentication..."
AUTH_RESPONSE=$(curl -s -w "%{http_code}" -u "$USERNAME:$PASSWORD" -o /tmp/auth_response.json "$SERVER_URL/files")
AUTH_CODE=$(echo $AUTH_RESPONSE | tail -c 4)

if [ "$AUTH_CODE" = "200" ]; then
    print_status "✅ Authentication passed"
    echo "Available files:"
    cat /tmp/auth_response.json | jq '.files[] | {name: .name, size: .size, is_partitioned: .is_partitioned}' 2>/dev/null || cat /tmp/auth_response.json
else
    print_error "❌ Authentication failed (HTTP $AUTH_CODE)"
    cat /tmp/auth_response.json
fi
echo ""

# Test 3: Invalid Authentication
print_status "Testing invalid authentication..."
INVALID_AUTH_RESPONSE=$(curl -s -w "%{http_code}" -u "wrong:password" -o /tmp/invalid_auth_response.json "$SERVER_URL/files")
INVALID_AUTH_CODE=$(echo $INVALID_AUTH_RESPONSE | tail -c 4)

if [ "$INVALID_AUTH_CODE" = "401" ]; then
    print_status "✅ Invalid authentication properly rejected"
else
    print_warning "⚠️ Invalid authentication test unexpected (HTTP $INVALID_AUTH_CODE)"
fi
echo ""

# Test 4: Invalid File Name
print_status "Testing invalid file name..."
INVALID_FILE_RESPONSE=$(curl -s -w "%{http_code}" -u "$USERNAME:$PASSWORD" -o /tmp/invalid_file_response.json "$SERVER_URL/data/../../../etc/passwd")
INVALID_FILE_CODE=$(echo $INVALID_FILE_RESPONSE | tail -c 4)

if [ "$INVALID_FILE_CODE" = "400" ]; then
    print_status "✅ Invalid file name properly rejected"
else
    print_warning "⚠️ Invalid file name test unexpected (HTTP $INVALID_FILE_CODE)"
fi
echo ""

# Test 5: Non-existent File
print_status "Testing non-existent file..."
NOT_FOUND_RESPONSE=$(curl -s -w "%{http_code}" -u "$USERNAME:$PASSWORD" -o /tmp/not_found_response.json "$SERVER_URL/data/nonexistent_file.csv")
NOT_FOUND_CODE=$(echo $NOT_FOUND_RESPONSE | tail -c 4)

if [ "$NOT_FOUND_CODE" = "404" ]; then
    print_status "✅ Non-existent file properly handled"
else
    print_warning "⚠️ Non-existent file test unexpected (HTTP $NOT_FOUND_CODE)"
fi
echo ""

# Test 6: Data Access (if files exist)
print_status "Testing data access..."
if [ -f /tmp/auth_response.json ]; then
    FIRST_FILE=$(cat /tmp/auth_response.json | jq -r '.files[0].name' 2>/dev/null)
    if [ "$FIRST_FILE" != "null" ] && [ -n "$FIRST_FILE" ]; then
        DATA_RESPONSE=$(curl -s -w "%{http_code}" -u "$USERNAME:$PASSWORD" -o /tmp/data_response.json "$SERVER_URL/data/$FIRST_FILE")
        DATA_CODE=$(echo $DATA_RESPONSE | tail -c 4)
        
        if [ "$DATA_CODE" = "200" ]; then
            print_status "✅ Data access successful for file: $FIRST_FILE"
            echo "Sample data (first 3 rows):"
            cat /tmp/data_response.json | jq '.value[0:3]' 2>/dev/null || echo "Raw response:"
            cat /tmp/data_response.json | head -20
        else
            print_error "❌ Data access failed (HTTP $DATA_CODE)"
            cat /tmp/data_response.json
        fi
    else
        print_warning "⚠️ No files available for data testing"
    fi
fi
echo ""

# Test 7: Partition Information (if partitioned data exists)
print_status "Testing partition information..."
if [ -f /tmp/auth_response.json ]; then
    PARTITIONED_FILE=$(cat /tmp/auth_response.json | jq -r '.files[] | select(.is_partitioned == true) | .name' 2>/dev/null | head -1)
    if [ -n "$PARTITIONED_FILE" ]; then
        # Extract dataset name from partitioned file name
        DATASET_NAME=$(echo $PARTITIONED_FILE | sed 's/_partitioned$//')
        PARTITION_RESPONSE=$(curl -s -w "%{http_code}" -u "$USERNAME:$PASSWORD" -o /tmp/partition_response.json "$SERVER_URL/partitions/$DATASET_NAME")
        PARTITION_CODE=$(echo $PARTITION_RESPONSE | tail -c 4)
        
        if [ "$PARTITION_CODE" = "200" ]; then
            print_status "✅ Partition information retrieved for dataset: $DATASET_NAME"
            cat /tmp/partition_response.json | jq '.' 2>/dev/null || cat /tmp/partition_response.json
        else
            print_error "❌ Partition information failed (HTTP $PARTITION_CODE)"
            cat /tmp/partition_response.json
        fi
    else
        print_warning "⚠️ No partitioned data available for testing"
    fi
fi
echo ""

# Test 8: Rate Limiting (IP Lockout)
print_status "Testing rate limiting (IP lockout)..."
echo "Attempting 6 failed logins to trigger IP lockout..."

for i in {1..6}; do
    RATE_LIMIT_RESPONSE=$(curl -s -w "%{http_code}" -u "wrong:password" -o /dev/null "$SERVER_URL/files")
    RATE_LIMIT_CODE=$(echo $RATE_LIMIT_RESPONSE | tail -c 4)
    echo "Attempt $i: HTTP $RATE_LIMIT_CODE"
done

# Test if IP is locked
LOCKED_RESPONSE=$(curl -s -w "%{http_code}" -u "$USERNAME:$PASSWORD" -o /tmp/locked_response.json "$SERVER_URL/files")
LOCKED_CODE=$(echo $LOCKED_RESPONSE | tail -c 4)

if [ "$LOCKED_CODE" = "423" ]; then
    print_status "✅ IP lockout working correctly"
else
    print_warning "⚠️ IP lockout test unexpected (HTTP $LOCKED_CODE)"
fi
echo ""

# Cleanup
rm -f /tmp/*_response.json

echo "🏁 Testing complete!"
echo ""
echo "📋 Test Summary:"
echo "- Health check: $([ "$HEALTH_CODE" = "200" ] && echo "✅ PASS" || echo "❌ FAIL")"
echo "- Authentication: $([ "$AUTH_CODE" = "200" ] && echo "✅ PASS" || echo "❌ FAIL")"
echo "- Invalid auth: $([ "$INVALID_AUTH_CODE" = "401" ] && echo "✅ PASS" || echo "❌ FAIL")"
echo "- Invalid file: $([ "$INVALID_FILE_CODE" = "400" ] && echo "✅ PASS" || echo "❌ FAIL")"
echo "- Not found: $([ "$NOT_FOUND_CODE" = "404" ] && echo "✅ PASS" || echo "❌ FAIL")"
echo "- IP lockout: $([ "$LOCKED_CODE" = "423" ] && echo "✅ PASS" || echo "❌ FAIL")"
