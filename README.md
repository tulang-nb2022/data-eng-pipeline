# Cursor Data Engineering Project

A comprehensive data engineering pipeline for weather data processing, validation, and analytics using modern cloud technologies.

## Project Overview

This project implements a complete data engineering pipeline that:
- Extracts weather data from NOAA APIs
- Processes and validates data using Great Expectations
- Stores data in S3 with partitioning
- Provides analytics through dbt transformations
- Serves data via OData API for Tableau Public

## Architecture

### Data Flow
```
NOAA API → Data Processing → S3 (Raw) → Validation → S3 (Silver) → dbt (Gold) → OData API → Tableau Public
```

### Key Components
- **Data Extraction**: NOAA weather data crawler
- **Data Validation**: Great Expectations suite
- **Data Storage**: S3 with date partitioning
- **Data Transformation**: dbt models (Silver → Gold)
- **Data Serving**: FastAPI OData server
- **Analytics**: Tableau Public dashboards

## Quick Start

### Prerequisites
- Python 3.8+
- AWS CLI configured
- dbt installed
- Tableau Public

### 1. Install Dependencies
```bash
pip install -r requirements.txt
```

### 2. Configure Environment
```bash
cp env.example .env
# Edit .env with your AWS credentials and configuration
```

### 3. Run Data Pipeline
```bash
# Extract weather data
python src/crawler/noaa_crawler.py

# Validate data
python great_expectations/weather_data_suite.py

# Transform data with dbt
dbt run

# Serve data via OData API
python simple_secure_s3_odata_server.py
```

## Data Pipeline Components

### 1. Data Extraction (`src/crawler/`)
- NOAA weather data crawler
- Handles API rate limiting
- Stores raw data in S3

### 2. Data Validation (`great_expectations/`)
- Great Expectations validation suite
- Pre and post-processing validation
- S3 and Athena integration

### 3. Data Storage (`data/`)
- S3 bucket structure with partitioning
- Raw, Silver, and Gold layers
- Parquet format for efficiency

### 4. Data Transformation (`models/`)
- dbt models for Silver and Gold layers
- Data quality transformations
- Business logic implementation

### 5. Data Serving (`simple_secure_s3_odata_server.py`)
- FastAPI OData server
- Tableau Public connectivity
- Secure authentication

## S3 OData Server for Tableau Public

A minimal FastAPI server that serves S3 data via OData endpoints for Tableau Public connectivity.

### Quick Start

#### 1. Set Environment Variables
```bash
export S3_BUCKET=your-bucket-name
export ODATA_USERNAME=your-username
export ODATA_PASSWORD=your-password
export ODATA_HOST=localhost
export ODATA_PORT=8000
```

#### 2. Install Dependencies
```bash
pip install fastapi uvicorn boto3 pandas python-dotenv passlib[bcrypt]
```

#### 3. Run Server
```bash
python3 simple_secure_s3_odata_server.py
```

#### 4. Set Up Cloudflare Tunnel
```bash
# Install cloudflared
curl -L --output cloudflared.deb https://github.com/cloudflare/cloudflared/releases/latest/download/cloudflared-linux-amd64.deb
sudo dpkg -i cloudflared.deb

# Create tunnel
cloudflared tunnel --url http://localhost:8000
```

#### 5. Connect from Tableau Public
1. Open Tableau Public
2. Connect to Data → More Servers → OData
3. URL: Use the Cloudflare tunnel URL (e.g., `https://abc123.trycloudflare.com`)
4. Username/Password: Your configured credentials

### OData Endpoints

- `/` - OData Service Document
- `/$metadata` - OData Metadata Document  
- `/weather_data` - Weather data entity set
- `/health` - Health check

### Data Structure

The server automatically reads from `gold/weather/processed/` in your S3 bucket and combines all parquet files into a single dataset with partition columns (`year`, `month`, `day`).

### Security Features

- HTTP Basic Authentication
- IP-based lockout after 5 failed attempts
- Input validation and sanitization
- CORS restricted to Tableau Public only
- Row limits (10,000 per partition file)

### Troubleshooting

#### Service Document Empty
```bash
# Check S3 access
aws s3 ls s3://your-bucket-name/gold/weather/processed/ --recursive | head -5

# Check server logs
python3 simple_secure_s3_odata_server.py
```

#### Cloudflare Tunnel Issues
```bash
# Test local server
curl http://localhost:8000/health

# Test tunnel
curl https://your-tunnel-url.trycloudflare.com/health
```

#### Tableau Connection Issues
- Ensure HTTPS URL (Cloudflare tunnel provides this)
- Check credentials are correct
- Verify server is running and accessible

## Project Structure

```
├── src/                    # Source code
│   ├── crawler/           # Data extraction
│   ├── main/              # Main processing logic
│   └── utils.py           # Utility functions
├── great_expectations/    # Data validation
├── models/                # dbt models
│   ├── silver/           # Silver layer
│   └── gold/             # Gold layer
├── data/                 # Data storage
│   └── raw/             # Raw data
├── scripts/             # Automation scripts
├── tests/               # Test files
└── simple_secure_s3_odata_server.py  # OData API server
```

## Configuration

### Environment Variables
- `S3_BUCKET`: S3 bucket name
- `ODATA_USERNAME`: OData server username
- `ODATA_PASSWORD`: OData server password
- `AWS_ACCESS_KEY_ID`: AWS access key
- `AWS_SECRET_ACCESS_KEY`: AWS secret key
- `AWS_DEFAULT_REGION`: AWS region

### dbt Configuration
- `profiles.yml`: dbt connection profiles
- `dbt_project.yml`: dbt project configuration

## Monitoring and Maintenance

### Data Quality
- Great Expectations validation reports
- dbt test results
- S3 data freshness checks

### Performance
- S3 storage optimization
- Query performance monitoring
- API response times

### Security
- AWS IAM role management
- API authentication logs
- Data access auditing

## Contributing

1. Fork the repository
2. Create a feature branch
3. Make your changes
4. Add tests if applicable
5. Submit a pull request

## License

This project is licensed under the MIT License.