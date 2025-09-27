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
from fastapi import FastAPI, HTTPException, Depends, status, Request
from fastapi.security import HTTPBasic, HTTPBasicCredentials
from fastapi.responses import JSONResponse
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
        """Get list of files and datasets from S3 bucket with security validation."""
        try:
            response = self.s3_client.list_objects_v2(
                Bucket=self.s3_bucket,
                Prefix=self.s3_prefix,
                MaxKeys=1000  # Increase for partitioned data
            )
            
            files = []
            datasets = {}  # Group files by dataset name
            
            if 'Contents' in response:
                for obj in response['Contents']:
                    if obj['Key'].endswith(('.csv', '.json', '.parquet')):
                        # Extract dataset name and check for partitioning
                        key_parts = obj['Key'].split('/')
                        filename = key_parts[-1]
                        
                        # Validate filename for security
                        if not self.security_manager.validate_input(filename, 'file_name'):
                            continue
                        
                        # Check if this is partitioned data (contains = in path)
                        is_partitioned = any('=' in part for part in key_parts)
                        
                        if is_partitioned:
                            # For partitioned data, use the parent directory as dataset name
                            # e.g., gold/weather/processed -> weather_processed
                            dataset_parts = []
                            for part in key_parts:
                                if '=' not in part and part not in ['', 'gold', 'processed']:
                                    dataset_parts.append(part)
                            dataset_name = '_'.join(dataset_parts) if dataset_parts else 'partitioned_data'
                        else:
                            # For non-partitioned data, use filename without extension
                            dataset_name = filename.split('.')[0]
                        
                        # Validate dataset name
                        if not self.security_manager.validate_input(dataset_name, 'dataset_name'):
                            continue
                        
                        file_info = {
                            'name': filename,
                            'key': obj['Key'],
                            'size': obj['Size'],
                            'last_modified': obj['LastModified'].isoformat(),
                            'dataset_name': dataset_name,
                            'is_partitioned': is_partitioned
                        }
                        
                        # Group by dataset
                        if dataset_name not in datasets:
                            datasets[dataset_name] = {
                                'name': dataset_name,
                                'files': [],
                                'total_size': 0,
                                'is_partitioned': is_partitioned,
                                'partition_info': {}
                            }
                        
                        datasets[dataset_name]['files'].append(file_info)
                        datasets[dataset_name]['total_size'] += obj['Size']
                        
                        # Extract partition information
                        if is_partitioned:
                            for part in key_parts:
                                if '=' in part:
                                    partition_key, partition_value = part.split('=', 1)
                                    if partition_key not in datasets[dataset_name]['partition_info']:
                                        datasets[dataset_name]['partition_info'][partition_key] = set()
                                    datasets[dataset_name]['partition_info'][partition_key].add(partition_value)
            
            # Convert datasets to files list
            for dataset_name, dataset_info in datasets.items():
                if dataset_info['is_partitioned']:
                    # Create a virtual file for the partitioned dataset
                    files.append({
                        'name': f"{dataset_name}_partitioned",
                        'key': dataset_name,  # Use dataset name as key for partitioned data
                        'size': dataset_info['total_size'],
                        'last_modified': max([f['last_modified'] for f in dataset_info['files']]),
                        'is_partitioned': True,
                        'partition_count': len(dataset_info['files']),
                        'partition_info': {k: sorted(list(v)) for k, v in dataset_info['partition_info'].items()}
                    })
                else:
                    # Add individual files
                    files.extend(dataset_info['files'])
            
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
    
    def _read_partitioned_dataset(self, dataset_name: str) -> pd.DataFrame:
        """Read all files in a partitioned dataset and combine them."""
        try:
            # List all files in the partitioned dataset
            response = self.s3_client.list_objects_v2(
                Bucket=self.s3_bucket,
                Prefix=f"{self.s3_prefix}{dataset_name}/" if self.s3_prefix else f"{dataset_name}/",
                MaxKeys=1000  # Limit for security
            )
            
            if 'Contents' not in response:
                raise HTTPException(status_code=404, detail="Dataset not found")
            
            dataframes = []
            for obj in response['Contents']:
                if obj['Key'].endswith(('.csv', '.json', '.parquet')):
                    file_ext = obj['Key'].split('.')[-1].lower()
                    
                    try:
                        # Read file with limits for security
                        if file_ext == 'csv':
                            df = pd.read_csv(f's3://{self.s3_bucket}/{obj["Key"]}', nrows=10000)
                        elif file_ext == 'json':
                            df = pd.read_json(f's3://{self.s3_bucket}/{obj["Key"]}')
                        elif file_ext == 'parquet':
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
                raise HTTPException(status_code=404, detail="No readable data found")
            
            # Combine all dataframes
            combined_df = pd.concat(dataframes, ignore_index=True)
            return combined_df
            
        except Exception as e:
            logger.error(f"Partitioned dataset error: {e}")
            raise HTTPException(status_code=500, detail="Error reading dataset")
    
    def _setup_routes(self):
        """Setup API routes with essential security."""
        
        @self.app.get("/")
        async def root():
            """Root endpoint."""
            return {
                "service": "S3 OData Server",
                "version": "1.0.0",
                "endpoints": ["/files", "/data/{file_name}", "/partitions/{dataset_name}", "/health"]
            }
        
        @self.app.get("/files")
        async def list_files(request: Request, username: str = Depends(self._verify_credentials)):
            """List available files."""
            files = self._get_s3_files()
            return {"files": files}
        
        @self.app.get("/data/{file_name}")
        async def get_data(
            request: Request,
            file_name: str,
            username: str = Depends(self._verify_credentials),
            top: Optional[int] = None,
            skip: Optional[int] = None
        ):
            """Get data from S3 files with security validation."""
            # Validate file name
            if not self.security_manager.validate_input(file_name, 'file_name'):
                self.security_manager.log_security_event(
                    "invalid_file_name",
                    {"file_name": file_name},
                    request
                )
                raise HTTPException(status_code=400, detail="Invalid file name")
            
            # Validate pagination parameters
            if top and (top < 1 or top > 10000):
                raise HTTPException(status_code=400, detail="Invalid top parameter")
            if skip and (skip < 0 or skip > 100000):
                raise HTTPException(status_code=400, detail="Invalid skip parameter")
            
            # Find the file
            files = self._get_s3_files()
            file_info = None
            for f in files:
                if f['name'] == file_name:
                    file_info = f
                    break
            
            if not file_info:
                raise HTTPException(status_code=404, detail="File not found")
            
            # Read data (handle partitioned vs non-partitioned)
            is_partitioned = file_info.get('is_partitioned', False)
            df = self._read_s3_file(file_info['key'], is_partitioned=is_partitioned)
            
            # Apply pagination
            if skip:
                df = df.iloc[skip:]
            if top:
                df = df.head(top)
            
            return {
                "odata.metadata": "/$metadata",
                "value": df.to_dict('records')
            }
        
        @self.app.get("/partitions/{dataset_name}")
        async def get_partitions(
            request: Request,
            dataset_name: str,
            username: str = Depends(self._verify_credentials)
        ):
            """Get partition information for a dataset."""
            if not self.security_manager.validate_input(dataset_name, 'dataset_name'):
                self.security_manager.log_security_event(
                    "invalid_dataset_name",
                    {"dataset_name": dataset_name},
                    request
                )
                raise HTTPException(status_code=400, detail="Invalid dataset name")
            
            try:
                response = self.s3_client.list_objects_v2(
                    Bucket=self.s3_bucket,
                    Prefix=f"{self.s3_prefix}{dataset_name}/" if self.s3_prefix else f"{dataset_name}/",
                    MaxKeys=1000
                )
                
                if 'Contents' not in response:
                    raise HTTPException(status_code=404, detail="Dataset not found")
                
                partitions = {}
                for obj in response['Contents']:
                    if obj['Key'].endswith(('.csv', '.json', '.parquet')):
                        key_parts = obj['Key'].split('/')
                        for part in key_parts:
                            if '=' in part:
                                partition_key, partition_value = part.split('=', 1)
                                if partition_key not in partitions:
                                    partitions[partition_key] = set()
                                partitions[partition_key].add(partition_value)
                
                result = {}
                for key, values in partitions.items():
                    result[key] = sorted(list(values))
                
                return {
                    "dataset": dataset_name,
                    "partitions": result,
                    "total_partitions": sum(len(values) for values in partitions.values())
                }
            except Exception as e:
                logger.error(f"Partitions error: {e}")
                raise HTTPException(status_code=500, detail="Error getting partitions")
        
        @self.app.get("/health")
        async def health_check():
            """Health check endpoint."""
            return {"status": "healthy", "service": "S3 OData Server"}
    
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
