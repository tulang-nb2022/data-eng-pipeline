#!/bin/bash

# dbt Gold Layer Runner
# Runs dbt models for the gold layer (business metrics and aggregations)

echo "=========================================="
echo "dbt Gold Layer - Business Metrics"
echo "=========================================="

# Check if dbt is installed
if ! command -v dbt &> /dev/null; then
    echo "❌ dbt is not installed. Please install dbt first:"
    echo "   pip install dbt-spark"
    exit 1
fi

# Check if profiles.yml exists
if [ ! -f "profiles.yml" ]; then
    echo "❌ profiles.yml not found. Please configure your dbt profile first."
    exit 1
fi

# Set default values
DBT_PROFILE=${1:-"data_engineering_project"}
DBT_TARGET=${2:-"dev"}
SILVER_S3_PATH=${3:-"s3://your-data-bucket/silver/weather"}
GOLD_S3_PATH=${4:-"s3://your-data-bucket/gold/weather"}

echo "dbt Profile: $DBT_PROFILE"
echo "dbt Target: $DBT_TARGET"
echo "Silver S3 Path: $SILVER_S3_PATH"
echo "Gold S3 Path: $GOLD_S3_PATH"
echo "=========================================="

# Set environment variables for dbt
export DBT_PROFILES_DIR=$(pwd)
export SILVER_S3_PATH="$SILVER_S3_PATH"
export GOLD_S3_PATH="$GOLD_S3_PATH"

# Run dbt models
echo "🔍 Running dbt debug..."
dbt debug --profile "$DBT_PROFILE" --target "$DBT_TARGET"

if [ $? -ne 0 ]; then
    echo "❌ dbt debug failed. Please check your configuration."
    exit 1
fi

echo ""
echo "📊 Running dbt models for gold layer..."
dbt run --models gold --profile "$DBT_PROFILE" --target "$DBT_TARGET"

if [ $? -eq 0 ]; then
    echo ""
    echo "✅ dbt gold layer completed successfully!"
    echo "📈 Business metrics generated and saved to: $GOLD_S3_PATH"
    
    # Run tests
    echo ""
    echo "🧪 Running dbt tests..."
    dbt test --models gold --profile "$DBT_PROFILE" --target "$DBT_TARGET"
    
    if [ $? -eq 0 ]; then
        echo "✅ All dbt tests passed!"
    else
        echo "⚠️  Some dbt tests failed. Check the output above."
    fi
    
    # Generate documentation
    echo ""
    echo "📚 Generating documentation..."
    dbt docs generate --profile "$DBT_PROFILE" --target "$DBT_TARGET"
    
    echo ""
    echo "🎉 Gold layer processing completed!"
    echo "📊 View documentation: dbt docs serve"
    
else
    echo "❌ dbt run failed. Check the output above."
    exit 1
fi
