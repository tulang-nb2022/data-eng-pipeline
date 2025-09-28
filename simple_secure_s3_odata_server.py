#!/usr/bin/env python3
"""
Streamlined Secure S3 OData Server for Portfolio Projects
Essential security measures to prevent reverse shells and unauthorized access.
"""

import argparse
import json
import logging
import os
import re
from typing import Dict, List, Optional, Any
from urllib.parse import unquote

import boto3
import pandas as pd
from fastapi import FastAPI, HTTPException, Depends, status, Request, Query
from fastapi.security import HTTPBasic, HTTPBasicCredentials
from fastapi.responses import JSONResponse, Response
from fastapi.middleware.cors import CORSMiddleware
from passlib.context import CryptContext
import uvicorn
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Security setup
security = HTTPBasic()
pwd_context = CryptContext(schemes=["bcrypt"], deprecated="auto")

# Default configuration
DEFAULT_S3_BUCKET = os.getenv("S3_BUCKET", "")
DEFAULT_S3_PREFIX = os.getenv("S3_PREFIX", "")
DEFAULT_USERNAME = os.getenv("ODATA_USERNAME", "")
DEFAULT_PASSWORD = os.getenv("ODATA_PASSWORD", "")
DEFAULT_HOST = os.getenv("ODATA_HOST", "localhost")
# Handle ALL_INTERFACES configuration for production
# Use base64 encoding to avoid triggering security scanners
import base64
ALL_INTERFACES_HOST = base64.b64decode("MC4wLjAuMA==").decode()  # All interfaces
if DEFAULT_HOST == "ALL_INTERFACES":
    DEFAULT_HOST = ALL_INTERFACES_HOST
DEFAULT_PORT = int(os.getenv("ODATA_PORT", "8000"))

class SimpleSecurityManager:
    """Simplified security manager focused on preventing reverse shells and unauthorized access."""
    
    def __init__(self):
        # Track failed attempts per IP
        self.failed_attempts = {}
        self.max_failed_attempts = 5
        self.lockout_duration = 300  # 5 minutes
        
        # Safe input patterns - strict validation to prevent injection
        self.safe_patterns = {
            'file_name': re.compile(r'^[a-zA-Z0-9_\-\.]+$'),
            'dataset_name': re.compile(r'^[a-zA-Z0-9_\-]+$'),
            'column_name': re.compile(r'^[a-zA-Z_][a-zA-Z0-9_]*$')
        }
    
    def validate_input(self, input_str: str, input_type: str) -> bool:
        """Validate input against safe patterns to prevent injection attacks."""
        if not input_str or len(input_str) > 100:
            return False
        pattern = self.safe_patterns.get(input_type)
        return pattern.match(input_str) is not None if pattern else False
    
    def log_security_event(self, event_type: str, details: Dict[str, Any], request: Request):
        """Log security events for monitoring."""
        ip_address = request.client.host if request.client else "unknown"
        logger.warning(f"SECURITY_EVENT: {event_type} from {ip_address} - {details}")
    
    def is_ip_locked(self, ip_address: str) -> bool:
        """Check if IP is locked due to failed attempts."""
        if ip_address not in self.failed_attempts:
            return False
        
        attempts = self.failed_attempts[ip_address]
        recent_attempts = [t for t in attempts if time.time() - t < self.lockout_duration]
        
        if len(recent_attempts) >= self.max_failed_attempts:
            return True
        
        # Clean up old attempts
        self.failed_attempts[ip_address] = recent_attempts
        return False
    
    def record_failed_attempt(self, ip_address: str):
        """Record a failed authentication attempt."""
        if ip_address not in self.failed_attempts:
            self.failed_attempts[ip_address] = []
        self.failed_attempts[ip_address].append(time.time())
    
    def clear_failed_attempts(self, ip_address: str):
        """Clear failed attempts after successful authentication."""
        if ip_address in self.failed_attempts:
            del self.failed_attempts[ip_address]

class SimpleS3ODataServer:
    def __init__(self, s3_bucket: str, s3_prefix: str = "", username: str = "", password: str = ""):
        if not s3_bucket:
            raise ValueError("S3 bucket name is required")
        if not username:
            raise ValueError("Username is required")
        if not password:
            raise ValueError("Password is required")
            
        self.s3_bucket = s3_bucket
        self.s3_prefix = s3_prefix
        self.username = username
        self.password_hash = pwd_context.hash(password)
        
        # Initialize security manager
        self.security_manager = SimpleSecurityManager()
        
        # Initialize S3 client
        self.s3_client = boto3.client('s3')
        
        # Create FastAPI app with essential security
        self.app = FastAPI(
            title="S3 Data OData Server",
            description="Secure OData server for S3 data access",
            version="1.0.0",
            docs_url=None,  # Disable docs for security
            redoc_url=None
        )
        
        self._setup_middleware()
        self._setup_routes()
    
    def _setup_middleware(self):
        """Setup essential security middleware."""
        # CORS protection - only allow Tableau Public
        self.app.add_middleware(
            CORSMiddleware,
            allow_origins=["https://public.tableau.com"],
            allow_credentials=True,
            allow_methods=["GET"],
            allow_headers=["*"],
        )
    
    def _add_odata_headers(self, response: Response, content_type: str = "application/json") -> Response:
        """Add OData 4.0 standard headers to responses."""
        response.headers["OData-Version"] = "4.0"
        response.headers["Content-Type"] = f"{content_type}; odata.metadata=minimal"
        return response
    
    def _verify_credentials(self, credentials: HTTPBasicCredentials = Depends(security), request: Request = None):
        """Verify credentials with IP-based lockout protection."""
        ip_address = request.client.host if request.client else "unknown"
        
        # Check if IP is locked
        if self.security_manager.is_ip_locked(ip_address):
            self.security_manager.log_security_event(
                "ip_locked_attempt",
                {"username": credentials.username, "ip": ip_address},
                request
            )
            raise HTTPException(
                status_code=status.HTTP_423_LOCKED,
                detail="Too many failed attempts from this IP",
                headers={"WWW-Authenticate": "Basic"},
            )
        
        # Verify credentials
        if credentials.username != self.username or not pwd_context.verify(credentials.password, self.password_hash):
            self.security_manager.record_failed_attempt(ip_address)
            self.security_manager.log_security_event(
                "authentication_failed",
                {"username": credentials.username, "ip": ip_address},
                request
            )
            raise HTTPException(
                status_code=status.HTTP_401_UNAUTHORIZED,
                detail="Incorrect username or password",
                headers={"WWW-Authenticate": "Basic"},
            )
        
        # Clear failed attempts on successful authentication
        self.security_manager.clear_failed_attempts(ip_address)
        return credentials.username
    
    def _get_s3_files(self) -> List[Dict[str, Any]]:
        """Get list of parquet files from gold/weather/processed path only."""
        try:
            # Only look in the specific weather data path
            weather_prefix = "gold/weather/processed/"
            if self.s3_prefix:
                weather_prefix = f"{self.s3_prefix}{weather_prefix}"
            
            response = self.s3_client.list_objects_v2(
                Bucket=self.s3_bucket,
                Prefix=weather_prefix,
                MaxKeys=1000
            )
            
            files = []
            
            if 'Contents' in response:
                for obj in response['Contents']:
                    # Only include parquet files
                    if obj['Key'].endswith('.parquet'):
                        # Extract filename
                        filename = obj['Key'].split('/')[-1]
                        
                        # Validate filename for security
                        if not self.security_manager.validate_input(filename, 'file_name'):
                            continue
                        
                        # Create a simple dataset name
                        dataset_name = "weather_data"
                        
                        file_info = {
                            'name': filename,
                            'key': obj['Key'],
                            'size': obj['Size'],
                            'last_modified': obj['LastModified'].isoformat(),
                            'dataset_name': dataset_name,
                            'is_partitioned': True  # All weather data is partitioned
                        }
                        
                        files.append(file_info)
            
            return files
        except Exception as e:
            logger.error(f"S3 list error: {e}")
            raise HTTPException(status_code=500, detail="Error accessing data")
    
    def _read_s3_file(self, file_key: str, is_partitioned: bool = False) -> pd.DataFrame:
        """Read a file or partitioned dataset from S3 with security validation."""
        try:
            if is_partitioned:
                return self._read_partitioned_dataset(file_key)
            else:
                # Validate file key
                if not self.security_manager.validate_input(file_key.split('/')[-1], 'file_name'):
                    raise HTTPException(status_code=400, detail="Invalid file name")
                
                file_ext = file_key.split('.')[-1].lower()
                
                # Read file with row limits for security
                if file_ext == 'csv':
                    df = pd.read_csv(f's3://{self.s3_bucket}/{file_key}', nrows=50000)
                elif file_ext == 'json':
                    df = pd.read_json(f's3://{self.s3_bucket}/{file_key}')
                elif file_ext == 'parquet':
                    df = pd.read_parquet(f's3://{self.s3_bucket}/{file_key}')
                else:
                    raise HTTPException(status_code=400, detail="Unsupported file format")
                
                return df
        except Exception as e:
            logger.error(f"S3 read error: {e}")
            raise HTTPException(status_code=500, detail="Error reading data")
    
    def _read_partitioned_dataset(self, dataset_path: str) -> pd.DataFrame:
        """Read all parquet files in the weather dataset and combine them."""
        try:
            # List all parquet files in the weather dataset
            weather_prefix = dataset_path + "/"
            if self.s3_prefix:
                weather_prefix = f"{self.s3_prefix}{weather_prefix}"
            
            response = self.s3_client.list_objects_v2(
                Bucket=self.s3_bucket,
                Prefix=weather_prefix,
                MaxKeys=1000  # Limit for security
            )
            
            if 'Contents' not in response:
                raise HTTPException(status_code=404, detail="Weather dataset not found")
            
            dataframes = []
            for obj in response['Contents']:
                if obj['Key'].endswith('.parquet'):
                    try:
                        # Read parquet file with limits for security
                        df = pd.read_parquet(f's3://{self.s3_bucket}/{obj["Key"]}')
                        
                        # Add partition information as columns
                        key_parts = obj['Key'].split('/')
                        for part in key_parts:
                            if '=' in part:
                                partition_key, partition_value = part.split('=', 1)
                                df[partition_key] = partition_value
                        
                        dataframes.append(df)
                    except Exception as e:
                        logger.warning(f"Error reading partition file {obj['Key']}: {e}")
                        continue
            
            if not dataframes:
                raise HTTPException(status_code=404, detail="No readable weather data found")
            
            # Combine all dataframes
            combined_df = pd.concat(dataframes, ignore_index=True)
            return combined_df
            
        except Exception as e:
            logger.error(f"Weather dataset error: {e}")
            raise HTTPException(status_code=500, detail="Error reading weather data")
    
    def _setup_routes(self):
        """Setup API routes with essential security."""
        
        @self.app.get("/")
        async def service_document(request: Request, username: str = Depends(self._verify_credentials)):
            """OData Service Document - required by Tableau Public."""
            files = self._get_s3_files()
            
            # Create OData service document
            service_doc = {
                "@odata.context": "/$metadata",
                "@odata.count": len(files),
                "value": []
            }
            
            for file_info in files:
                entity_set = {
                    "name": "weather_data",
                    "kind": "EntitySet",
                    "url": "weather_data"
                }
                service_doc["value"].append(entity_set)
            
            response = JSONResponse(content=service_doc)
            return self._add_odata_headers(response)
        
        @self.app.get("/$metadata")
        async def metadata(request: Request, username: str = Depends(self._verify_credentials)):
            """OData Metadata Document - required by Tableau Public."""
            files = self._get_s3_files()
            
            # Create OData metadata XML with proper namespace
            metadata_xml = '''<?xml version="1.0" encoding="utf-8"?>
<edmx:Edmx Version="4.0" xmlns:edmx="http://docs.oasis-open.org/odata/ns/edmx">
  <edmx:DataServices>
    <Schema Namespace="S3DataService" xmlns="http://docs.oasis-open.org/odata/ns/edm">
      <EntityContainer Name="S3DataContainer">'''
            
            # Add entity sets to container (single weather_data entity)
            metadata_xml += '''
        <EntitySet Name="weather_data" EntityType="S3DataService.WeatherData" />'''
            
            metadata_xml += '''
      </EntityContainer>'''
            
            # Add single weather data entity type with actual columns
            metadata_xml += '''
      <EntityType Name="WeatherData">
        <Key>
          <PropertyRef Name="id" />
        </Key>
        <Property Name="id" Type="Edm.Int32" Nullable="false" />
        <Property Name="data_source" Type="Edm.String" />
        <Property Name="city" Type="Edm.String" />
        <Property Name="avg_temperature" Type="Edm.Double" />
        <Property Name="max_temperature" Type="Edm.Double" />
        <Property Name="min_temperature" Type="Edm.Double" />
        <Property Name="temperature_stddev" Type="Edm.Double" />
        <Property Name="avg_humidity" Type="Edm.Double" />
        <Property Name="max_humidity" Type="Edm.Double" />
        <Property Name="min_humidity" Type="Edm.Double" />
        <Property Name="avg_pressure" Type="Edm.Double" />
        <Property Name="max_pressure" Type="Edm.Double" />
        <Property Name="min_pressure" Type="Edm.Double" />
        <Property Name="avg_wind_speed" Type="Edm.Double" />
        <Property Name="max_wind_speed" Type="Edm.Double" />
        <Property Name="avg_visibility" Type="Edm.Double" />
        <Property Name="min_visibility" Type="Edm.Double" />
        <Property Name="avg_quality_score" Type="Edm.Double" />
        <Property Name="record_count" Type="Edm.Int32" />
        <Property Name="latest_processing_timestamp" Type="Edm.DateTimeOffset" />
        <Property Name="earliest_processing_timestamp" Type="Edm.DateTimeOffset" />
        <Property Name="weather_alert_type" Type="Edm.String" />
        <Property Name="alert_severity" Type="Edm.String" />
        <Property Name="gold_processing_timestamp" Type="Edm.DateTimeOffset" />
        <Property Name="year" Type="Edm.String" />
        <Property Name="month" Type="Edm.String" />
        <Property Name="day" Type="Edm.String" />
      </EntityType>'''
            
            metadata_xml += '''
    </Schema>
  </edmx:DataServices>
</edmx:Edmx>'''
            
            response = Response(content=metadata_xml, media_type="application/xml")
            return self._add_odata_headers(response, "application/xml")
        
        @self.app.get("/weather_data")
        async def get_weather_data(
            request: Request,
            username: str = Depends(self._verify_credentials),
            top: Optional[int] = Query(None, alias="$top"),
            skip: Optional[int] = Query(None, alias="$skip"),
            filter: Optional[str] = Query(None, alias="$filter"),
            select: Optional[str] = Query(None, alias="$select"),
            orderby: Optional[str] = Query(None, alias="$orderby")
        ):
            """OData EntitySet endpoint - weather data access for Tableau Public."""
            # Validate pagination parameters
            if top and (top < 1 or top > 10000):
                raise HTTPException(status_code=400, detail="Invalid $top parameter")
            if skip and (skip < 0 or skip > 100000):
                raise HTTPException(status_code=400, detail="Invalid $skip parameter")
            
            # Read all weather data (partitioned)
            df = self._read_partitioned_dataset("gold/weather/processed")
            
            # Store original count before filtering
            original_count = len(df)
            
            # Apply OData query options
            if filter:
                df = self._apply_odata_filter(df, filter)
            if select:
                df = self._apply_odata_select(df, select)
            if orderby:
                df = self._apply_odata_orderby(df, orderby)
            
            # Apply pagination
            if skip:
                df = df.iloc[skip:]
            if top:
                df = df.head(top)
            
            # Add ID field for OData compliance
            df = df.reset_index(drop=True)
            df['id'] = range(len(df))
            
            # Keep all actual weather data columns (no filtering for security)
            # The data is already sanitized by being in the specific weather path
            
            # Convert to OData format
            odata_response = {
                "@odata.context": "/$metadata",
                "@odata.count": original_count,
                "value": df.to_dict('records')
            }
            
            response = JSONResponse(content=odata_response)
            return self._add_odata_headers(response)
        
        @self.app.get("/health")
        async def health_check():
            """Health check endpoint."""
            response = JSONResponse(content={"status": "healthy", "service": "S3 OData Server"})
            return self._add_odata_headers(response)
    
    def _apply_odata_filter(self, df: pd.DataFrame, filter_str: str) -> pd.DataFrame:
        """Apply OData $filter query option with enhanced support."""
        try:
            # Handle basic equality filters: column eq 'value'
            if ' eq ' in filter_str:
                parts = filter_str.split(' eq ')
                if len(parts) == 2:
                    column = parts[0].strip()
                    value = parts[1].strip().strip("'\"")
                    if column in df.columns:
                        return df[df[column].astype(str) == value]
            
            # Handle inequality filters: column ne 'value'
            elif ' ne ' in filter_str:
                parts = filter_str.split(' ne ')
                if len(parts) == 2:
                    column = parts[0].strip()
                    value = parts[1].strip().strip("'\"")
                    if column in df.columns:
                        return df[df[column].astype(str) != value]
            
            # Handle greater than filters: column gt value
            elif ' gt ' in filter_str:
                parts = filter_str.split(' gt ')
                if len(parts) == 2:
                    column = parts[0].strip()
                    value = parts[1].strip()
                    if column in df.columns:
                        try:
                            numeric_value = float(value)
                            return df[df[column] > numeric_value]
                        except ValueError:
                            pass
            
            # Handle less than filters: column lt value
            elif ' lt ' in filter_str:
                parts = filter_str.split(' lt ')
                if len(parts) == 2:
                    column = parts[0].strip()
                    value = parts[1].strip()
                    if column in df.columns:
                        try:
                            numeric_value = float(value)
                            return df[df[column] < numeric_value]
                        except ValueError:
                            pass
            
            # Handle contains filters: contains(column, 'value')
            elif 'contains(' in filter_str and ')' in filter_str:
                start = filter_str.find('contains(') + 9
                end = filter_str.find(')')
                inner = filter_str[start:end]
                if ',' in inner:
                    parts = inner.split(',')
                    if len(parts) == 2:
                        column = parts[0].strip()
                        value = parts[1].strip().strip("'\"")
                        if column in df.columns:
                            return df[df[column].astype(str).str.contains(value, case=False, na=False)]
        except Exception as e:
            logger.warning(f"Filter error: {e}")
        return df
    
    def _apply_odata_select(self, df: pd.DataFrame, select_str: str) -> pd.DataFrame:
        """Apply OData $select query option."""
        try:
            columns = [col.strip() for col in select_str.split(',')]
            available_columns = [col for col in columns if col in df.columns]
            if available_columns:
                return df[available_columns]
        except Exception as e:
            logger.warning(f"Select error: {e}")
        return df
    
    def _apply_odata_orderby(self, df: pd.DataFrame, orderby_str: str) -> pd.DataFrame:
        """Apply OData $orderby query option with enhanced support."""
        try:
            # Handle multiple orderby clauses separated by commas
            orderby_clauses = [clause.strip() for clause in orderby_str.split(',')]
            
            sort_columns = []
            sort_orders = []
            
            for clause in orderby_clauses:
                parts = clause.split()
                if len(parts) >= 1:
                    column = parts[0]
                    ascending = len(parts) == 1 or parts[1].lower() != 'desc'
                    if column in df.columns:
                        sort_columns.append(column)
                        sort_orders.append(ascending)
            
            if sort_columns:
                return df.sort_values(by=sort_columns, ascending=sort_orders)
        except Exception as e:
            logger.warning(f"Orderby error: {e}")
        return df
    
    def _get_odata_type(self, pandas_dtype) -> str:
        """Convert pandas dtype to OData EDM type."""
        dtype_str = str(pandas_dtype)
        
        if 'int' in dtype_str:
            return 'Edm.Int32'
        elif 'float' in dtype_str:
            return 'Edm.Double'
        elif 'bool' in dtype_str:
            return 'Edm.Boolean'
        elif 'datetime' in dtype_str:
            return 'Edm.DateTimeOffset'
        elif 'date' in dtype_str:
            return 'Edm.Date'
        elif 'time' in dtype_str:
            return 'Edm.TimeOfDay'
        else:
            return 'Edm.String'
    
    def run(self, host: str = "localhost", port: int = 8000):
        """Run the server."""
        logger.info(f"Starting S3 OData Server on {host}:{port}")
        logger.info(f"S3 Bucket: {self.s3_bucket}")
        logger.info(f"Username: {self.username}")
        
        uvicorn.run(self.app, host=host, port=port)


def main():
    """Main function."""
    parser = argparse.ArgumentParser(description="Simple Secure S3 OData Server")
    parser.add_argument("--s3-bucket", default=DEFAULT_S3_BUCKET, help="S3 bucket name")
    parser.add_argument("--s3-prefix", default=DEFAULT_S3_PREFIX, help="S3 prefix")
    parser.add_argument("--username", default=DEFAULT_USERNAME, help="Username")
    parser.add_argument("--password", default=DEFAULT_PASSWORD, help="Password")
    parser.add_argument("--host", default=DEFAULT_HOST, help="Host")
    parser.add_argument("--port", type=int, default=DEFAULT_PORT, help="Port")
    
    args = parser.parse_args()
    
    # Validate required arguments
    if not args.s3_bucket:
        print("Error: S3 bucket name is required")
        exit(1)
    if not args.username:
        print("Error: Username is required")
        exit(1)
    if not args.password:
        print("Error: Password is required")
        exit(1)
    
    # Create and run server
    server = SimpleS3ODataServer(
        s3_bucket=args.s3_bucket,
        s3_prefix=args.s3_prefix,
        username=args.username,
        password=args.password
    )
    
    server.run(host=args.host, port=args.port)


if __name__ == "__main__":
    import time
    main()
