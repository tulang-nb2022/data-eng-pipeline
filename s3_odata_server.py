#!/usr/bin/env python3
"""
FastAPI server to host S3 data with OData endpoints for Tableau Public.
Provides authentication-protected access to data stored in S3 buckets.
"""

import argparse
import json
import logging
import os
from typing import Dict, List, Optional, Any
from urllib.parse import unquote

import boto3
import pandas as pd
from fastapi import FastAPI, HTTPException, Depends, status
from fastapi.security import HTTPBasic, HTTPBasicCredentials
from fastapi.responses import JSONResponse
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

# Default configuration from environment variables
DEFAULT_S3_BUCKET = os.getenv("S3_BUCKET", "")
DEFAULT_S3_PREFIX = os.getenv("S3_PREFIX", "")
DEFAULT_USERNAME = os.getenv("ODATA_USERNAME", "")
DEFAULT_PASSWORD = os.getenv("ODATA_PASSWORD", "")
DEFAULT_HOST = os.getenv("ODATA_HOST", "localhost")
DEFAULT_PORT = int(os.getenv("ODATA_PORT", "8000"))

class S3ODataServer:
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
        
        # Initialize S3 client
        self.s3_client = boto3.client('s3')
        
        # Create FastAPI app
        self.app = FastAPI(
            title="S3 Data OData Server",
            description="OData server for accessing S3 data with Tableau Public compatibility",
            version="1.0.0"
        )
        
        self._setup_routes()
    
    def _verify_credentials(self, credentials: HTTPBasicCredentials = Depends(security)):
        """Verify username and password credentials."""
        if credentials.username != self.username or not pwd_context.verify(credentials.password, self.password_hash):
            raise HTTPException(
                status_code=status.HTTP_401_UNAUTHORIZED,
                detail="Incorrect username or password",
                headers={"WWW-Authenticate": "Basic"},
            )
        return credentials.username
    
    def _get_s3_files(self) -> List[Dict[str, Any]]:
        """Get list of files and datasets from S3 bucket with prefix."""
        try:
            response = self.s3_client.list_objects_v2(
                Bucket=self.s3_bucket,
                Prefix=self.s3_prefix
            )
            
            files = []
            datasets = {}  # Group files by dataset name
            
            if 'Contents' in response:
                for obj in response['Contents']:
                    if obj['Key'].endswith(('.csv', '.json', '.parquet')):
                        # Extract dataset name (remove file extension and partition info)
                        key_parts = obj['Key'].split('/')
                        filename = key_parts[-1]
                        
                        # For partitioned data, try to extract dataset name
                        # Assumes format like: dataset_name/partition_key=value/file.parquet
                        if '=' in obj['Key']:
                            # This looks like partitioned data
                            dataset_name = key_parts[-3] if len(key_parts) >= 3 else filename.split('.')[0]
                        else:
                            dataset_name = filename.split('.')[0]
                        
                        file_info = {
                            'name': filename,
                            'key': obj['Key'],
                            'size': obj['Size'],
                            'last_modified': obj['LastModified'].isoformat(),
                            'dataset_name': dataset_name
                        }
                        
                        # Group by dataset
                        if dataset_name not in datasets:
                            datasets[dataset_name] = {
                                'name': dataset_name,
                                'files': [],
                                'total_size': 0,
                                'is_partitioned': '=' in obj['Key']
                            }
                        
                        datasets[dataset_name]['files'].append(file_info)
                        datasets[dataset_name]['total_size'] += obj['Size']
            
            # Convert datasets to files list
            for dataset_name, dataset_info in datasets.items():
                if dataset_info['is_partitioned']:
                    # For partitioned data, create a virtual "file" representing the dataset
                    files.append({
                        'name': f"{dataset_name}_partitioned",
                        'key': dataset_name,  # Use dataset name as key
                        'size': dataset_info['total_size'],
                        'last_modified': max([f['last_modified'] for f in dataset_info['files']]),
                        'is_partitioned': True,
                        'partition_count': len(dataset_info['files'])
                    })
                else:
                    # For non-partitioned data, add individual files
                    files.extend(dataset_info['files'])
            
            return files
        except Exception as e:
            logger.error(f"Error listing S3 files: {e}")
            raise HTTPException(status_code=500, detail=f"Error accessing S3: {str(e)}")
    
    def _read_s3_file(self, file_key: str, is_partitioned: bool = False) -> pd.DataFrame:
        """Read a file or partitioned dataset from S3 and return as DataFrame."""
        try:
            if is_partitioned:
                # For partitioned data, read all files in the dataset
                return self._read_partitioned_dataset(file_key)
            else:
                # For single files, read normally
                file_ext = file_key.split('.')[-1].lower()
                
                # Read file based on extension
                if file_ext == 'csv':
                    df = pd.read_csv(f's3://{self.s3_bucket}/{file_key}')
                elif file_ext == 'json':
                    df = pd.read_json(f's3://{self.s3_bucket}/{file_key}')
                elif file_ext == 'parquet':
                    df = pd.read_parquet(f's3://{self.s3_bucket}/{file_key}')
                else:
                    raise HTTPException(status_code=400, detail=f"Unsupported file format: {file_ext}")
                
                return df
        except Exception as e:
            logger.error(f"Error reading S3 file {file_key}: {e}")
            raise HTTPException(status_code=500, detail=f"Error reading file: {str(e)}")
    
    def _read_partitioned_dataset(self, dataset_name: str) -> pd.DataFrame:
        """Read all files in a partitioned dataset and combine them."""
        try:
            # List all files for this dataset
            response = self.s3_client.list_objects_v2(
                Bucket=self.s3_bucket,
                Prefix=f"{self.s3_prefix}{dataset_name}/" if self.s3_prefix else f"{dataset_name}/"
            )
            
            if 'Contents' not in response:
                raise HTTPException(status_code=404, detail=f"No files found for dataset {dataset_name}")
            
            # Collect all data files
            dataframes = []
            for obj in response['Contents']:
                if obj['Key'].endswith(('.csv', '.json', '.parquet')):
                    file_ext = obj['Key'].split('.')[-1].lower()
                    
                    try:
                        if file_ext == 'csv':
                            df = pd.read_csv(f's3://{self.s3_bucket}/{obj["Key"]}')
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
                raise HTTPException(status_code=404, detail=f"No readable files found for dataset {dataset_name}")
            
            # Combine all dataframes
            combined_df = pd.concat(dataframes, ignore_index=True)
            return combined_df
            
        except Exception as e:
            logger.error(f"Error reading partitioned dataset {dataset_name}: {e}")
            raise HTTPException(status_code=500, detail=f"Error reading partitioned dataset: {str(e)}")
    
    def _setup_routes(self):
        """Setup FastAPI routes."""
        
        @self.app.get("/")
        async def root():
            """Root endpoint with service information."""
            return {
                "service": "S3 OData Server",
                "version": "1.0.0",
                "description": "OData server for S3 data access",
                "endpoints": {
                    "metadata": "/$metadata",
                    "files": "/files",
                    "data": "/data/{file_name}",
                    "partitions": "/partitions/{dataset_name}"
                }
            }
        
        @self.app.get("/files")
        async def list_files(username: str = Depends(self._verify_credentials)):
            """List available files in S3 bucket."""
            files = self._get_s3_files()
            return {"files": files}
        
        @self.app.get("/$metadata")
        async def get_metadata(username: str = Depends(self._verify_credentials)):
            """OData metadata endpoint for Tableau Public."""
            files = self._get_s3_files()
            
            # Generate OData metadata
            metadata = {
                "edmx:Edmx": {
                    "@xmlns:edmx": "http://schemas.microsoft.com/ado/2007/06/edmx",
                    "@Version": "1.0",
                    "edmx:DataServices": {
                        "@xmlns:m": "http://schemas.microsoft.com/ado/2007/08/dataservices/metadata",
                        "@m:DataServiceVersion": "1.0",
                        "@m:MaxDataServiceVersion": "3.0",
                        "Schema": {
                            "@xmlns": "http://schemas.microsoft.com/ado/2008/09/edm",
                            "@Namespace": "Default",
                            "EntityContainer": {
                                "@Name": "Container",
                                "EntitySet": []
                            }
                        }
                    }
                }
            }
            
            # Add entity sets for each file
            for file_info in files:
                entity_set = {
                    "@Name": file_info['name'].replace('.', '_').replace('-', '_'),
                    "@EntityType": f"Default.{file_info['name'].replace('.', '_').replace('-', '_')}"
                }
                metadata["edmx:Edmx"]["edmx:DataServices"]["Schema"]["EntityContainer"]["EntitySet"].append(entity_set)
            
            return JSONResponse(content=metadata, media_type="application/xml")
        
        @self.app.get("/data/{file_name}")
        async def get_data(
            file_name: str,
            username: str = Depends(self._verify_credentials),
            top: Optional[int] = None,
            skip: Optional[int] = None,
            filter: Optional[str] = None,
            select: Optional[str] = None,
            orderby: Optional[str] = None
        ):
            """OData endpoint to get data from S3 files."""
            # Find the file key
            files = self._get_s3_files()
            file_info = None
            for f in files:
                if f['name'] == file_name:
                    file_info = f
                    break
            
            if not file_info:
                raise HTTPException(status_code=404, detail=f"File {file_name} not found")
            
            # Read data from S3
            is_partitioned = file_info.get('is_partitioned', False)
            df = self._read_s3_file(file_info['key'], is_partitioned=is_partitioned)
            
            # Apply OData query parameters
            if filter:
                # Basic filtering - you can extend this for more complex filters
                df = self._apply_filter(df, filter)
            
            if select:
                # Select specific columns
                columns = [col.strip() for col in select.split(',')]
                available_columns = [col for col in columns if col in df.columns]
                if available_columns:
                    df = df[available_columns]
            
            if orderby:
                # Apply ordering
                df = self._apply_orderby(df, orderby)
            
            # Apply pagination
            if skip:
                df = df.iloc[skip:]
            if top:
                df = df.head(top)
            
            # Convert to OData format
            result = {
                "odata.metadata": f"/$metadata",
                "value": df.to_dict('records')
            }
            
            return result
        
        @self.app.get("/partitions/{dataset_name}")
        async def get_partitions(
            dataset_name: str,
            username: str = Depends(self._verify_credentials)
        ):
            """Get partition information for a dataset."""
            try:
                # List all files for this dataset
                response = self.s3_client.list_objects_v2(
                    Bucket=self.s3_bucket,
                    Prefix=f"{self.s3_prefix}{dataset_name}/" if self.s3_prefix else f"{dataset_name}/"
                )
                
                if 'Contents' not in response:
                    raise HTTPException(status_code=404, detail=f"Dataset {dataset_name} not found")
                
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
                
                # Convert sets to sorted lists
                result = {}
                for key, values in partitions.items():
                    result[key] = sorted(list(values))
                
                return {
                    "dataset": dataset_name,
                    "partitions": result,
                    "total_partitions": sum(len(values) for values in partitions.values())
                }
            except Exception as e:
                logger.error(f"Error getting partitions for {dataset_name}: {e}")
                raise HTTPException(status_code=500, detail=f"Error getting partitions: {str(e)}")
        
        @self.app.get("/health")
        async def health_check():
            """Health check endpoint."""
            return {"status": "healthy", "service": "S3 OData Server"}
    
    def _apply_filter(self, df: pd.DataFrame, filter_str: str) -> pd.DataFrame:
        """Apply basic OData filter to DataFrame."""
        # This is a simplified filter implementation
        # For production, you'd want a more robust OData filter parser
        try:
            # Basic equality filter: column eq 'value'
            if ' eq ' in filter_str:
                parts = filter_str.split(' eq ')
                if len(parts) == 2:
                    column = parts[0].strip()
                    value = parts[1].strip().strip("'\"")
                    if column in df.columns:
                        return df[df[column].astype(str) == value]
        except Exception as e:
            logger.warning(f"Error applying filter {filter_str}: {e}")
        
        return df
    
    def _apply_orderby(self, df: pd.DataFrame, orderby_str: str) -> pd.DataFrame:
        """Apply OData orderby to DataFrame."""
        try:
            # Basic ordering: column asc/desc
            parts = orderby_str.split()
            if len(parts) >= 1:
                column = parts[0]
                ascending = len(parts) == 1 or parts[1].lower() != 'desc'
                if column in df.columns:
                    return df.sort_values(by=column, ascending=ascending)
        except Exception as e:
            logger.warning(f"Error applying orderby {orderby_str}: {e}")
        
        return df
    
    def run(self, host: str = "localhost", port: int = 8000):
        """Run the FastAPI server."""
        logger.info(f"Starting S3 OData Server on {host}:{port}")
        logger.info(f"S3 Bucket: {self.s3_bucket}")
        logger.info(f"S3 Prefix: {self.s3_prefix}")
        logger.info(f"Username: {self.username}")
        
        uvicorn.run(self.app, host=host, port=port)


def main():
    """Main function to run the server."""
    parser = argparse.ArgumentParser(description="S3 OData Server for Tableau Public")
    parser.add_argument("--s3-bucket", default=DEFAULT_S3_BUCKET, help="S3 bucket name (or set S3_BUCKET env var)")
    parser.add_argument("--s3-prefix", default=DEFAULT_S3_PREFIX, help="S3 prefix/folder path (or set S3_PREFIX env var)")
    parser.add_argument("--username", default=DEFAULT_USERNAME, help="Username for authentication (or set ODATA_USERNAME env var)")
    parser.add_argument("--password", default=DEFAULT_PASSWORD, help="Password for authentication (or set ODATA_PASSWORD env var)")
    parser.add_argument("--host", default=DEFAULT_HOST, help="Host to bind to (or set ODATA_HOST env var)")
    parser.add_argument("--port", type=int, default=DEFAULT_PORT, help="Port to bind to (or set ODATA_PORT env var)")
    
    args = parser.parse_args()
    
    # Validate required arguments
    if not args.s3_bucket:
        print("Error: S3 bucket name is required. Set --s3-bucket argument or S3_BUCKET environment variable.")
        exit(1)
    if not args.username:
        print("Error: Username is required. Set --username argument or ODATA_USERNAME environment variable.")
        exit(1)
    if not args.password:
        print("Error: Password is required. Set --password argument or ODATA_PASSWORD environment variable.")
        exit(1)
    
    # Create and run server
    server = S3ODataServer(
        s3_bucket=args.s3_bucket,
        s3_prefix=args.s3_prefix,
        username=args.username,
        password=args.password
    )
    
    server.run(host=args.host, port=args.port)


if __name__ == "__main__":
    main()
