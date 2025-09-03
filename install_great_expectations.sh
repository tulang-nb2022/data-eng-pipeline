#!/bin/bash

echo "🚀 Installing Great Expectations for Data Validation"
echo "=================================================="

# Check if Python is installed
if ! command -v python3 &> /dev/null; then
    echo "❌ Python 3 is not installed. Please install Python 3.8+ first."
    exit 1
fi

# Check Python version
python_version=$(python3 -c 'import sys; print(".".join(map(str, sys.version_info[:2])))')
echo "✅ Python version: $python_version"

# Upgrade pip
echo "📦 Upgrading pip..."
python3 -m pip install --upgrade pip setuptools>=65.0.0

# Install Great Expectations and dependencies
echo "📦 Installing Great Expectations 0.15.2 and dependencies..."
python3 -m pip install -r requirements.txt

# Verify installation
echo "🔍 Verifying Great Expectations installation..."
python3 -c "import great_expectations as ge; print('✅ Great Expectations installed successfully')"

# Initialize Great Expectations project
echo "🚀 Initializing Great Expectations project..."
python3 great_expectations/weather_data_suite.py

echo ""
echo "✅ Great Expectations setup completed!"
echo ""
echo "📋 Next steps:"
echo "1. Configure your AWS credentials for S3/Athena access"
echo "2. Update your S3 bucket names in the validation scripts"
echo "3. Test validation with: python great_expectations/weather_data_suite.py"
echo "4. For S3 validation: python great_expectations/weather_data_suite.py s3 s3://your-bucket/path/"
echo "5. For Athena validation: python great_expectations/weather_data_suite.py athena database table"
echo ""
echo "📚 Documentation: https://docs.greatexpectations.io/" 