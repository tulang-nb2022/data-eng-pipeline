#!/usr/bin/env python3
"""
Test script to verify Great Expectations migration from Pandera
"""

import sys
import os

def test_great_expectations_import():
    """Test that Great Expectations can be imported"""
    try:
        import great_expectations as ge
        print("✅ Great Expectations imported successfully")
        print(f"   Version: {ge.__version__}")
        return True
    except ImportError as e:
        print(f"❌ Failed to import Great Expectations: {e}")
        return False

def test_pandera_removed():
    """Test that Pandera is no longer available"""
    try:
        import pandera
        print("❌ Pandera is still installed - migration incomplete")
        return False
    except ImportError:
        print("✅ Pandera successfully removed")
        return True

def test_validation_module():
    """Test that the validation module can be imported"""
    try:
        sys.path.append('great_expectations')
        from weather_data_suite import validate_weather_data_great_expectations
        print("✅ Validation module imported successfully")
        return True
    except ImportError as e:
        print(f"❌ Failed to import validation module: {e}")
        return False

def test_s3fs_import():
    """Test S3 filesystem support"""
    try:
        import s3fs
        print("✅ S3FS imported successfully")
        return True
    except ImportError as e:
        print(f"❌ Failed to import S3FS: {e}")
        return False

def test_boto3_import():
    """Test AWS SDK import"""
    try:
        import boto3
        print("✅ Boto3 imported successfully")
        return True
    except ImportError as e:
        print(f"❌ Failed to import Boto3: {e}")
        return False

def main():
    """Run all tests"""
    print("🧪 Testing Great Expectations Migration")
    print("=" * 50)
    
    tests = [
        ("Great Expectations Import", test_great_expectations_import),
        ("Pandera Removal", test_pandera_removed),
        ("Validation Module", test_validation_module),
        ("S3FS Support", test_s3fs_import),
        ("Boto3 Support", test_boto3_import),
    ]
    
    passed = 0
    total = len(tests)
    
    for test_name, test_func in tests:
        print(f"\n🔍 Testing: {test_name}")
        if test_func():
            passed += 1
        else:
            print(f"   ❌ {test_name} failed")
    
    print("\n" + "=" * 50)
    print(f"📊 Test Results: {passed}/{total} tests passed")
    
    if passed == total:
        print("🎉 All tests passed! Migration successful!")
        print("\n📋 Next steps:")
        print("1. Run: python great_expectations/weather_data_suite.py")
        print("2. Test S3 validation: python great_expectations/weather_data_suite.py s3 s3://your-bucket/path/")
        print("3. Test Athena validation: python great_expectations/weather_data_suite.py athena database table")
    else:
        print("⚠️  Some tests failed. Please check the installation.")
        sys.exit(1)

if __name__ == "__main__":
    main() 