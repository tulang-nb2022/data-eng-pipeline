#!/usr/bin/env python3
"""
Example script demonstrating how to use the S3 OData Server.
This script shows how to start the server and test its endpoints.
"""

import os
import subprocess
import time
import requests
import json
from typing import Dict, Any
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

def test_server_endpoints(base_url: str, username: str, password: str) -> Dict[str, Any]:
    """Test all server endpoints and return results."""
    results = {}
    
    # Test credentials
    auth = (username, password)
    
    try:
        # Test root endpoint
        print("Testing root endpoint...")
        response = requests.get(f"{base_url}/")
        results['root'] = {
            'status_code': response.status_code,
            'data': response.json() if response.status_code == 200 else None
        }
        print(f"✓ Root endpoint: {response.status_code}")
        
        # Test files endpoint
        print("Testing files endpoint...")
        response = requests.get(f"{base_url}/files", auth=auth)
        results['files'] = {
            'status_code': response.status_code,
            'data': response.json() if response.status_code == 200 else None
        }
        print(f"✓ Files endpoint: {response.status_code}")
        
        # Test metadata endpoint
        print("Testing metadata endpoint...")
        response = requests.get(f"{base_url}/$metadata", auth=auth)
        results['metadata'] = {
            'status_code': response.status_code,
            'content_type': response.headers.get('content-type', 'unknown')
        }
        print(f"✓ Metadata endpoint: {response.status_code}")
        
        # Test health endpoint
        print("Testing health endpoint...")
        response = requests.get(f"{base_url}/health")
        results['health'] = {
            'status_code': response.status_code,
            'data': response.json() if response.status_code == 200 else None
        }
        print(f"✓ Health endpoint: {response.status_code}")
        
    except requests.exceptions.ConnectionError:
        print("✗ Could not connect to server. Make sure it's running.")
        results['error'] = 'Connection failed'
    except Exception as e:
        print(f"✗ Error testing endpoints: {e}")
        results['error'] = str(e)
    
    return results

def create_sample_data():
    """Create sample data files for testing."""
    import pandas as pd
    import os
    
    # Create sample directory
    os.makedirs('sample_data', exist_ok=True)
    
    # Create sample CSV data
    sample_data = {
        'id': [1, 2, 3, 4, 5],
        'name': ['Alice', 'Bob', 'Charlie', 'Diana', 'Eve'],
        'age': [25, 30, 35, 28, 32],
        'department': ['Sales', 'Marketing', 'Engineering', 'Sales', 'Marketing'],
        'salary': [50000, 60000, 80000, 55000, 65000]
    }
    
    df = pd.DataFrame(sample_data)
    df.to_csv('sample_data/employees.csv', index=False)
    
    # Create sample JSON data
    json_data = [
        {'product_id': 1, 'name': 'Laptop', 'price': 999.99, 'category': 'Electronics'},
        {'product_id': 2, 'name': 'Mouse', 'price': 29.99, 'category': 'Electronics'},
        {'product_id': 3, 'name': 'Desk', 'price': 299.99, 'category': 'Furniture'},
        {'product_id': 4, 'name': 'Chair', 'price': 199.99, 'category': 'Furniture'},
        {'product_id': 5, 'name': 'Monitor', 'price': 399.99, 'category': 'Electronics'}
    ]
    
    with open('sample_data/products.json', 'w') as f:
        json.dump(json_data, f, indent=2)
    
    print("✓ Created sample data files:")
    print("  - sample_data/employees.csv")
    print("  - sample_data/products.json")

def main():
    """Main function to demonstrate server usage."""
    print("S3 OData Server Example")
    print("=" * 50)
    
    # Configuration from environment variables
    s3_bucket = os.getenv("S3_BUCKET", "your-test-bucket")
    username = os.getenv("ODATA_USERNAME", "testuser")
    password = os.getenv("ODATA_PASSWORD", "testpass123")
    host = os.getenv("ODATA_HOST", "localhost")
    port = int(os.getenv("ODATA_PORT", "8000"))
    base_url = f"http://{host}:{port}"
    
    print(f"Configuration:")
    print(f"  S3 Bucket: {s3_bucket}")
    print(f"  Username: {username}")
    print(f"  Password: {'*' * len(password)}")  # Hide password in output
    print(f"  Server URL: {base_url}")
    print()
    
    # Check if required environment variables are set
    if s3_bucket == "your-test-bucket":
        print("⚠️  Warning: Using default S3 bucket name. Set S3_BUCKET environment variable.")
    if username == "testuser":
        print("⚠️  Warning: Using default username. Set ODATA_USERNAME environment variable.")
    if password == "testpass123":
        print("⚠️  Warning: Using default password. Set ODATA_PASSWORD environment variable.")
    print()
    
    # Create sample data
    print("Creating sample data...")
    create_sample_data()
    print()
    
    # Note: In a real scenario, you would upload these files to S3
    print("Note: Upload sample_data/ files to your S3 bucket before running the server.")
    print("You can use AWS CLI: aws s3 cp sample_data/ s3://your-bucket/data/ --recursive")
    print()
    
    # Server command
    server_command = [
        "python", "s3_odata_server.py",
        "--s3-bucket", s3_bucket,
        "--s3-prefix", "data/",
        "--username", username,
        "--password", password,
        "--host", host,
        "--port", str(port)
    ]
    
    print("To start the server, run:")
    print(" ".join(server_command))
    print()
    
    print("Or set environment variables and run:")
    print("export S3_BUCKET=your-bucket-name")
    print("export ODATA_USERNAME=your-username")
    print("export ODATA_PASSWORD=your-password")
    print("python s3_odata_server.py")
    print()
    
    # Test endpoints (if server is running)
    print("Testing server endpoints...")
    results = test_server_endpoints(base_url, username, password)
    
    if 'error' not in results:
        print("\n✓ All endpoints tested successfully!")
        print("\nServer is ready for Tableau Public connection:")
        print(f"  OData URL: {base_url}")
        print(f"  Username: {username}")
        print(f"  Password: {password}")
    else:
        print(f"\n✗ Server test failed: {results['error']}")
        print("Make sure the server is running before testing endpoints.")

if __name__ == "__main__":
    main()
