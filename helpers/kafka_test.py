#!/usr/bin/env python3
import os
import socket
from confluent_kafka import Producer
from dotenv import load_dotenv

def is_kafka_available(bootstrap_servers):
    """Check if Kafka is available"""
    print(f"Testing connection to Kafka at {bootstrap_servers}")
    
    servers = bootstrap_servers.split(',')
    for server in servers:
        host, port_str = server.split(':')
        port = int(port_str)
        
        try:
            sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            sock.settimeout(1)
            result = sock.connect_ex((host, port))
            sock.close()
            if result == 0:
                print(f"✅ Successfully connected to {host}:{port}")
                return True
            else:
                print(f"❌ Failed to connect to {host}:{port} (Error code: {result})")
        except Exception as e:
            print(f"❌ Error connecting to {host}:{port}: {str(e)}")
    
    return False

def test_kafka_producer(bootstrap_servers):
    """Test creating a Kafka producer"""
    print(f"\nTesting Kafka producer with {bootstrap_servers}")
    try:
        kafka_config = {
            'bootstrap.servers': bootstrap_servers,
            'client.id': 'test-client',
            'socket.timeout.ms': 5000,  # 5 second timeout
        }
        
        producer = Producer(kafka_config)
        producer.flush(timeout=5)
        print("✅ Successfully created Kafka producer")
        return True
    except Exception as e:
        print(f"❌ Failed to create Kafka producer: {str(e)}")
        return False

if __name__ == "__main__":
    # Load environment variables from .env file
    load_dotenv()
    
    # Get Kafka bootstrap servers from environment variable
    # Prioritize the environment variable over the .env file
    bootstrap_servers = os.environ.get('KAFKA_BOOTSTRAP_SERVERS', 'kafka:29092')
    
    print(f"Using Kafka bootstrap servers: {bootstrap_servers}")
    
    # Test Kafka availability
    kafka_available = is_kafka_available(bootstrap_servers)
    
    if kafka_available:
        # Test Kafka producer
        producer_works = test_kafka_producer(bootstrap_servers)
        
        if producer_works:
            print("\n✅ SUCCESS: Kafka is available and producer works!")
            print("\nTo fix your connection issue permanently, update your .env file:")
            print(f"KAFKA_BOOTSTRAP_SERVERS={bootstrap_servers}")
        else:
            print("\n❌ ERROR: Kafka is available but producer creation failed.")
    else:
        print("\n❌ ERROR: Kafka is not available at the specified bootstrap servers.")
        print("Please check your Kafka configuration and ensure the servers are running.")
