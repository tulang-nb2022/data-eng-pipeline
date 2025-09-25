# S3 OData Server for Tableau Public

A FastAPI-based server that hosts data from S3 buckets with OData endpoints compatible with Tableau Public's OData connector. The server provides authentication-protected access to your S3 data.

## Features

- **S3 Data Access**: Read data from S3 buckets (CSV, JSON, Parquet formats)
- **OData Protocol**: Full OData 1.0/2.0 support for Tableau Public compatibility
- **Authentication**: Username/password protection using HTTP Basic Auth
- **Query Support**: Filtering, sorting, pagination, and column selection
- **Multiple Formats**: Support for CSV, JSON, and Parquet files
- **Command Line Interface**: Easy configuration via command line arguments

## Installation

1. Install the required dependencies:
```bash
pip install -r requirements.txt
```

2. Configure environment variables:
```bash
# Copy the example configuration file
cp env.example .env

# Edit .env with your actual values
nano .env
```

3. Ensure your AWS credentials are configured (via AWS CLI, environment variables, or IAM roles)

## Usage

### Using Environment Variables (Recommended)

1. Set your configuration in `.env` file or environment variables:
```bash
export S3_BUCKET=your-bucket-name
export ODATA_USERNAME=your-username
export ODATA_PASSWORD=your-password
export ODATA_HOST=localhost
export ODATA_PORT=8000
```

2. Run the server:
```bash
python s3_odata_server.py
```

### Using Command Line Arguments

```bash
python s3_odata_server.py --s3-bucket your-bucket-name --username your-username --password your-password
```

### Advanced Usage

```bash
python s3_odata_server.py \
    --s3-bucket your-data-bucket \
    --s3-prefix data/processed/ \
    --username tableau_user \
    --password secure_password123 \
    --host localhost \
    --port 8080
```

### Command Line Arguments

- `--s3-bucket`: S3 bucket name containing your data (required or set `S3_BUCKET` env var)
- `--s3-prefix`: S3 prefix/folder path to filter files (optional or set `S3_PREFIX` env var)
- `--username`: Username for authentication (required or set `ODATA_USERNAME` env var)
- `--password`: Password for authentication (required or set `ODATA_PASSWORD` env var)
- `--host`: Host to bind the server to (default: `localhost` or set `ODATA_HOST` env var)
- `--port`: Port to bind the server to (default: `8000` or set `ODATA_PORT` env var)

### Environment Variables

| Variable | Description | Required | Default |
|----------|-------------|----------|---------|
| `S3_BUCKET` | S3 bucket name | Yes | - |
| `S3_PREFIX` | S3 prefix/folder path | No | "" |
| `ODATA_USERNAME` | Username for authentication | Yes | - |
| `ODATA_PASSWORD` | Password for authentication | Yes | - |
| `ODATA_HOST` | Host to bind to | No | `localhost` |
| `ODATA_PORT` | Port to bind to | No | `8000` |

## API Endpoints

### 1. Root Endpoint
```
GET /
```
Returns service information and available endpoints.

### 2. File Listing
```
GET /files
```
Lists all available files in the S3 bucket (requires authentication).

### 3. OData Metadata
```
GET /$metadata
```
Returns OData metadata for Tableau Public connector (requires authentication).

### 4. Data Access
```
GET /data/{file_name}
```
Access data from specific files with OData query support (requires authentication).

#### Query Parameters:
- `$top`: Limit number of records
- `$skip`: Skip number of records (pagination)
- `$filter`: Filter records (basic equality filters)
- `$select`: Select specific columns
- `$orderby`: Sort records

#### Example Queries:
```
GET /data/sales_data.csv?$top=100&$skip=0
GET /data/customers.json?$filter=status eq 'active'
GET /data/products.parquet?$select=name,price&$orderby=price desc
```

## Tableau Public Integration

### Step 1: Start the Server
```bash
# Using environment variables (recommended)
export S3_BUCKET=your-bucket
export ODATA_USERNAME=tableau
export ODATA_PASSWORD=yourpassword
python s3_odata_server.py

# Or using command line arguments
python s3_odata_server.py --s3-bucket your-bucket --username tableau --password yourpassword
```

### Step 2: Connect from Tableau Public
1. Open Tableau Public
2. Choose "Connect to Data" → "More Servers" → "OData"
3. Enter your server URL: `http://your-server:8000`
4. Enter your username and password when prompted
5. Select the data source you want to connect to

### Step 3: Configure Data Source
- The server will automatically discover available files
- Select the file you want to analyze
- Apply any filters or transformations as needed

## Example Data Structure

Your S3 bucket should contain files in supported formats:

```
your-bucket/
├── data/
│   ├── sales_data.csv
│   ├── customer_info.json
│   └── product_catalog.parquet
└── reports/
    └── monthly_summary.csv
```

## Security Considerations

1. **Environment Variables**: Store sensitive data in environment variables or `.env` files (never commit `.env` to version control)
2. **Authentication**: Always use strong passwords
3. **Network Security**: Consider running behind a reverse proxy (nginx) with SSL
4. **AWS Permissions**: Use IAM roles with minimal required permissions
5. **Access Control**: Limit S3 bucket access to necessary files only
6. **Default Host**: Uses `localhost` as the default host for better security

## Troubleshooting

### Common Issues

1. **AWS Credentials**: Ensure AWS credentials are properly configured
2. **S3 Permissions**: Verify the server has read access to the S3 bucket
3. **File Formats**: Only CSV, JSON, and Parquet files are supported
4. **Network Access**: Ensure the server is accessible from Tableau Public

### Logs
The server logs important information including:
- S3 access attempts
- Authentication events
- Query processing
- Error messages

### Testing the Server

Test the server endpoints:

```bash
# Test root endpoint
curl http://localhost:8000/

# Test authentication (replace with your actual credentials)
curl -u $ODATA_USERNAME:$ODATA_PASSWORD http://localhost:8000/files

# Test OData metadata
curl -u $ODATA_USERNAME:$ODATA_PASSWORD http://localhost:8000/\$metadata

# Test data access
curl -u $ODATA_USERNAME:$ODATA_PASSWORD "http://localhost:8000/data/sample.csv?$top=10"
```

## Performance Considerations

1. **Large Files**: For very large files, consider using pagination (`$top`, `$skip`)
2. **Caching**: The server reads files fresh each time - consider caching for frequently accessed data
3. **Memory Usage**: Large datasets are loaded into memory - monitor server resources
4. **Concurrent Access**: FastAPI handles concurrent requests efficiently

## Development

### Adding New File Formats
To add support for new file formats, modify the `_read_s3_file` method in `s3_odata_server.py`.

### Extending OData Support
The current implementation supports basic OData features. To add more complex filtering or querying, extend the `_apply_filter` and `_apply_orderby` methods.

### Custom Authentication
To implement custom authentication (e.g., JWT tokens), modify the `_verify_credentials` method.

## License

This project is part of the cursor-data-eng repository and follows the same licensing terms.
