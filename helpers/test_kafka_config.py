#!/usr/bin/env python3
import os
import sys
import socket
from dotenv import load_dotenv

# Add the parent directory to the path so we can import the module
sys.path.insert(0, os.path.abspath(os.path.dirname(__file__)))

# Import the logger module first to avoid import errors
sys.path.insert(0, os.path.join(os.path.abspath(os.path.dirname(__file__)), 'src'))
import logger

from src.sql_to_kafka_pipeline import setup_kafka_producer, load_env

def is_port_open(host, port):
    """Check if a port is open on a host"""
    try:
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.settimeout(1)
        result = sock.connect_ex((host, port))
        sock.close()
        return result == 0
    except:
        return False

def test_kafka_config():
    """Test the Kafka producer configuration"""
    print("Loading environment variables...")
    config = load_env()
    
    # Parse the bootstrap servers to check if Kafka is available
    bootstrap_servers = config['kafka_bootstrap_servers']
    servers = bootstrap_servers.split(',')
    
    all_servers_available = True
    for server in servers:
        host, port_str = server.split(':')
        port = int(port_str)
        
        if not is_port_open(host, port):
            print(f"WARNING: Kafka server {host}:{port} is not available.")
            all_servers_available = False
    
    if not all_servers_available:
        print("\nKafka servers are not available. Please ensure that:")
        print("1. Kafka is running (e.g., via Docker Compose)")
        print("2. The bootstrap servers configuration is correct")
        print("3. There are no network issues preventing connection")
        print("\nTo start Kafka using Docker Compose, run:")
        print("docker-compose -f docker-compose-complete.yml up -d zookeeper kafka")
        return False
    
    print("Setting up Kafka producer...")
    producer = setup_kafka_producer(config)
    
    if producer:
        print("SUCCESS: Kafka producer configuration is valid!")
        return True
    else:
        print("FAILED: Kafka producer configuration is invalid.")
        return False

if __name__ == "__main__":
    test_kafka_config()
