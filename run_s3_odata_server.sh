#!/bin/bash

# S3 OData Server Startup Script
# This script provides an easy way to start the S3 OData server with common configurations

set -e

# Default values from environment variables
DEFAULT_BUCKET="${S3_BUCKET:-}"
DEFAULT_PREFIX="${S3_PREFIX:-}"
DEFAULT_USERNAME="${ODATA_USERNAME:-}"
DEFAULT_AUTH_PASS="${ODATA_PASSWORD:-}"
DEFAULT_HOST="${ODATA_HOST:-localhost}"
DEFAULT_PORT="${ODATA_PORT:-8000}"

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

# Function to print colored output
print_info() {
    echo -e "${GREEN}[INFO]${NC} $1"
}

print_warning() {
    echo -e "${YELLOW}[WARNING]${NC} $1"
}

print_error() {
    echo -e "${RED}[ERROR]${NC} $1"
}

# Function to show usage
show_usage() {
    echo "Usage: $0 [OPTIONS]"
    echo ""
    echo "Options:"
    echo "  -b, --bucket BUCKET     S3 bucket name (required or set S3_BUCKET env var)"
    echo "  -p, --prefix PREFIX     S3 prefix/folder path (optional or set S3_PREFIX env var)"
    echo "  -u, --username USER    Username for authentication (required or set ODATA_USERNAME env var)"
    echo "  -w, --password PASS    Password for authentication (required or set ODATA_PASSWORD env var)"
    echo "  -h, --host HOST        Host to bind to (default: localhost or set ODATA_HOST env var)"
    echo "  -P, --port PORT        Port to bind to (default: 8000 or set ODATA_PORT env var)"
    echo "  --help                 Show this help message"
    echo ""
    echo "Environment Variables:"
    echo "  S3_BUCKET              S3 bucket name"
    echo "  S3_PREFIX              S3 prefix/folder path"
    echo "  ODATA_USERNAME         Username for authentication"
    echo "  ODATA_PASSWORD         Password for authentication"
    echo "  ODATA_HOST             Host to bind to (default: localhost)"
    echo "  ODATA_PORT             Port to bind to (default: 8000)"
    echo ""
    echo "Examples:"
    echo "  # Using command line arguments:"
    echo "  $0 --bucket my-data-bucket --username tableau --password secure123"
    echo "  $0 --bucket my-bucket --prefix data/processed --username tableau --password secure123"
    echo ""
    echo "  # Using environment variables:"
    echo "  export S3_BUCKET=my-data-bucket"
    echo "  export ODATA_USERNAME=tableau"
    echo "  export ODATA_PASSWORD=secure123"
    echo "  $0"
}

# Parse command line arguments
BUCKET=""
PREFIX=""
USERNAME="$DEFAULT_USERNAME"
AUTH_PASS="$DEFAULT_AUTH_PASS"
HOST="$DEFAULT_HOST"
PORT="$DEFAULT_PORT"

while [[ $# -gt 0 ]]; do
    case $1 in
        -b|--bucket)
            BUCKET="$2"
            shift 2
            ;;
        -p|--prefix)
            PREFIX="$2"
            shift 2
            ;;
        -u|--username)
            USERNAME="$2"
            shift 2
            ;;
        -w|--password)
            AUTH_PASS="$2"
            shift 2
            ;;
        -h|--host)
            HOST="$2"
            shift 2
            ;;
        -P|--port)
            PORT="$2"
            shift 2
            ;;
        --help)
            show_usage
            exit 0
            ;;
        *)
            print_error "Unknown option: $1"
            show_usage
            exit 1
            ;;
    esac
done

# Validate required parameters
if [[ -z "$BUCKET" ]]; then
    print_error "S3 bucket name is required!"
    print_info "Set --bucket argument or S3_BUCKET environment variable"
    show_usage
    exit 1
fi

if [[ -z "$USERNAME" ]]; then
    print_error "Username is required!"
    print_info "Set --username argument or ODATA_USERNAME environment variable"
    show_usage
    exit 1
fi

if [[ -z "$AUTH_PASS" ]]; then
    print_error "Password is required!"
    print_info "Set --password argument or ODATA_PASSWORD environment variable"
    show_usage
    exit 1
fi

# Check if Python script exists
if [[ ! -f "s3_odata_server.py" ]]; then
    print_error "s3_odata_server.py not found in current directory!"
    exit 1
fi

# Check if required Python packages are installed
print_info "Checking dependencies..."
python3 -c "import fastapi, uvicorn, boto3, pandas" 2>/dev/null || {
    print_error "Required Python packages not found!"
    print_info "Please install dependencies: pip install -r requirements.txt"
    exit 1
}

# Check AWS credentials
print_info "Checking AWS credentials..."
if ! aws sts get-caller-identity &>/dev/null; then
    print_warning "AWS credentials not configured or invalid!"
    print_info "Please configure AWS credentials using:"
    print_info "  aws configure"
    print_info "  or set AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY environment variables"
    echo ""
    read -p "Continue anyway? (y/N): " -n 1 -r
    echo
    if [[ ! $REPLY =~ ^[Yy]$ ]]; then
        exit 1
    fi
fi

# Test S3 bucket access
print_info "Testing S3 bucket access..."
if ! aws s3 ls "s3://$BUCKET" &>/dev/null; then
    print_error "Cannot access S3 bucket: $BUCKET"
    print_info "Please check:"
    print_info "  1. Bucket name is correct"
    print_info "  2. AWS credentials have access to the bucket"
    print_info "  3. Bucket exists and is accessible"
    exit 1
fi

print_info "S3 bucket access confirmed!"

# Build command
CMD="python3 s3_odata_server.py"
CMD="$CMD --s3-bucket $BUCKET"
if [[ -n "$PREFIX" ]]; then
    CMD="$CMD --s3-prefix $PREFIX"
fi
CMD="$CMD --username $USERNAME"
CMD="$CMD --password $AUTH_PASS"
CMD="$CMD --host $HOST"
CMD="$CMD --port $PORT"

# Display configuration
echo ""
print_info "Starting S3 OData Server with configuration:"
echo "  S3 Bucket: $BUCKET"
if [[ -n "$PREFIX" ]]; then
    echo "  S3 Prefix: $PREFIX"
fi
echo "  Username: $USERNAME"
echo "  Password: $AUTH_PASS"
echo "  Host: $HOST"
echo "  Port: $PORT"
echo ""

# Start server
print_info "Starting server..."
print_info "Server will be available at: http://$HOST:$PORT"
print_info "Press Ctrl+C to stop the server"
echo ""

exec $CMD
