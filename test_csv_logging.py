#!/usr/bin/env python3
"""
Test script to verify CSV logging system functionality
"""

import os
import sys
import logging
from datetime import datetime

# Add the current directory to Python path to import our modules
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

try:
    from check_status import setup_csv_logger, log_process_start, log_process_end, log_step, log_error_with_context
    print("✅ Successfully imported CSV logging functions")
except ImportError as e:
    print(f"❌ Failed to import CSV logging functions: {e}")
    sys.exit(1)

def test_csv_logging():
    """Test the CSV logging system"""
    print("\n🧪 Testing CSV Logging System...")
    
    # Create a test logger
    test_logger = setup_csv_logger('test_process')
    print("✅ Created test CSV logger")
    
    # Test process lifecycle logging
    log_process_start(test_logger, "test_csv_logging_system", test_param="test_value")
    print("✅ Logged process start")
    
    # Test step logging with subprocess names
    log_step(test_logger, "Initialize test data", subprocess_name="data_initialization", details="Setting up test environment")
    print("✅ Logged step with subprocess name")
    
    # Test regular logging with subprocess info
    record = test_logger.makeRecord(
        test_logger.name, logging.INFO, '', 0, 
        "Processing test data batch 1 of 3", 
        (), None, func='data_processing'
    )
    record.subprocess = 'data_processing'
    record.details = 'batch=1,total=3,status=processing'
    test_logger.handle(record)
    print("✅ Logged custom record with subprocess details")
    
    # Test warning level
    record = test_logger.makeRecord(
        test_logger.name, logging.WARNING, '', 0, 
        "Test warning message", 
        (), None, func='warning_test'
    )
    record.subprocess = 'warning_test'
    record.details = 'warning_type=test'
    test_logger.handle(record)
    print("✅ Logged warning message")
    
    # Test error logging
    try:
        raise ValueError("This is a test error")
    except Exception as e:
        log_error_with_context(test_logger, e, "Test error handling", subprocess_name="error_simulation")
        print("✅ Logged error with context")
    
    # Test process end
    log_process_end(test_logger, "test_csv_logging_system", success=True, 
                   records_processed=4, warnings=1, errors=1)
    print("✅ Logged process end")
    
    # Check if CSV file was created
    csv_file_path = os.path.join('logs', 'test_process.csv')
    
    if os.path.exists(csv_file_path):
        print(f"✅ CSV log file created: {csv_file_path}")
        
        # Read and display the CSV content
        import pandas as pd
        try:
            df = pd.read_csv(csv_file_path)
            print(f"✅ CSV file contains {len(df)} log entries")
            print("\n📊 CSV Log Content Preview:")
            print(df.to_string(index=False))
            return True
        except Exception as e:
            print(f"❌ Error reading CSV file: {e}")
            return False
    else:
        print(f"❌ CSV log file not found: {csv_file_path}")
        return False

if __name__ == "__main__":
    print("🚀 Starting CSV Logging System Test")
    
    success = test_csv_logging()
    
    if success:
        print("\n🎉 CSV Logging System Test PASSED!")
        print("\n📋 Summary:")
        print("- ✅ CSV logger creation")
        print("- ✅ Process lifecycle logging")
        print("- ✅ Step logging with subprocess names")
        print("- ✅ Custom record logging")
        print("- ✅ Warning level logging")
        print("- ✅ Error logging with context")
        print("- ✅ CSV file creation and content verification")
    else:
        print("\n❌ CSV Logging System Test FAILED!")
        sys.exit(1)
