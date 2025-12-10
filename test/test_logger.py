import os
import sys
import time
from datetime import datetime

# Add the parent directory to sys.path to import from src
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from src.logger import get_daily_logger

def test_logger():
    print("Starting logger test...")
    
    # Get the logger
    logger = get_daily_logger()
    
    # Log messages at different levels
    logger.info("This is an INFO test message")
    logger.warning("This is a WARNING test message")
    logger.error("This is an ERROR test message")
    
    # Expected log file path
    log_filename = os.path.join('logs', datetime.now().strftime('log_%Y_%m_%d.log'))
    
    # Check if log file was created
    if os.path.exists(log_filename):
        print(f"✅ Log file created successfully: {log_filename}")
    else:
        print(f"❌ Log file not found: {log_filename}")
        return
    
    # Read and display log content
    try:
        with open(log_filename, 'r', encoding='utf-8') as f:
            log_content = f.read()
            print("\nLog file content:")
            print("-" * 50)
            print(log_content)
            print("-" * 50)
        
        # Verify log messages
        expected_messages = [
            "INFO",
            "WARNING",
            "ERROR",
            "This is an INFO test message",
            "This is a WARNING test message",
            "This is an ERROR test message"
        ]
        
        all_found = True
        for msg in expected_messages:
            if msg in log_content:
                print(f"✅ Found expected message: '{msg}'")
            else:
                print(f"❌ Missing expected message: '{msg}'")
                all_found = False
        
        if all_found:
            print("\n✅ All expected messages found in log file")
        else:
            print("\n❌ Some expected messages are missing from log file")
            
    except Exception as e:
        print(f"❌ Error reading log file: {e}")

if __name__ == "__main__":
    test_logger()
