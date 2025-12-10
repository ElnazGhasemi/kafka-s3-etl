#!/usr/bin/env python3
import os
import csv
import json
import sqlite3
import pandas as pd
import logging
import io
from datetime import datetime, timedelta
from dotenv import load_dotenv
from confluent_kafka import Producer
from src.logger import get_daily_logger
from contextlib import redirect_stdout

# Set up logger
logger = get_daily_logger()

# Create a string IO handler to capture logs for Airflow
class LogCapture:
    def __init__(self):
        self.log_capture_string = io.StringIO()
        self.log_handler = logging.StreamHandler(self.log_capture_string)
        self.log_handler.setFormatter(logging.Formatter('%(asctime)s %(levelname)s [%(filename)s:%(funcName)s:%(lineno)d]: %(message)s'))
        self.log_handler.setLevel(logging.INFO)
        
    def __enter__(self):
        logger.addHandler(self.log_handler)
        return self
        
    def __exit__(self, exc_type, exc_val, exc_tb):
        logger.removeHandler(self.log_handler)
        
    def get_logs(self):
        return self.log_capture_string.getvalue()

def load_env():
    """Load environment variables from .env file"""
    with LogCapture() as log_capture:
        load_dotenv()
        logger.info("Environment variables loaded")
        config = {
            'db_type': os.getenv('DB_TYPE', 'sqlite'),
            'db_memory': os.getenv('DB_MEMORY', 'true').lower() == 'true',
            'csv_file_path': os.getenv('CSV_FILE_PATH', 'data/products.csv'),
            'kafka_bootstrap_servers': os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'localhost:9092'),
            'kafka_topic': os.getenv('KAFKA_TOPIC', 'product-updates'),
            'kafka_acks': os.getenv('KAFKA_ACKS', 'all'),
            'kafka_retries': int(os.getenv('KAFKA_RETRIES', '3')),
            'kafka_batch_size': int(os.getenv('KAFKA_BATCH_SIZE', '16384')),
            'kafka_linger_ms': int(os.getenv('KAFKA_LINGER_MS', '1')),
            'kafka_buffer_memory': int(os.getenv('KAFKA_BUFFER_MEMORY', '33554432')),
            'default_days_back': int(os.getenv('DEFAULT_DAYS_BACK', '30')),
            'article_status': os.getenv('ARTICLE_STATUS', 'created')
        }
        return config, log_capture.get_logs()

def setup_database(config):
    """Set up SQLite database connection"""
    with LogCapture() as log_capture:
        if config['db_memory']:
            conn = sqlite3.connect(':memory:')
            logger.info("Connected to in-memory SQLite database")
        else:
            conn = sqlite3.connect('products.db')
            logger.info("Connected to SQLite database file: products.db")
        
        return conn, log_capture.get_logs()

def create_products_table(conn):
    """Create products table if it doesn't exist"""
    with LogCapture() as log_capture:
        cursor = conn.cursor()
        cursor.execute('''
        CREATE TABLE IF NOT EXISTS products (
            ean TEXT PRIMARY KEY,
            styleNumber TEXT,
            styleOption TEXT,
            size TEXT,
            color TEXT,
            brandName TEXT,
            brandcode TEXT,
            subbrandName TEXT,
            productCategory TEXT,
            productSubcategory TEXT,
            gender TEXT,
            ediSeason TEXT,
            ediStyleName TEXT,
            countryOfOrigin TEXT,
            price_eur REAL,
            price_usd REAL,
            price_gbp REAL,
            grossPrice_eur REAL,
            b2bReadinessDate TEXT,
            articleStatus TEXT,
            enrichmentStatus TEXT,
            createdOn TEXT,
            lastUpdated TEXT,
            fabricComposition TEXT,
            washingInstructions TEXT,
            ediDescription TEXT
        )
        ''')
        conn.commit()
        logger.info("Products table created or already exists")
        return log_capture.get_logs()

def load_csv_to_db(conn, csv_path):
    """Load data from CSV file into database"""
    with LogCapture() as log_capture:
        try:
            # Check if the file exists
            if not os.path.isfile(csv_path):
                # If the path is relative to the current directory, try to find it there
                local_path = os.path.join(os.getcwd(), csv_path.lstrip('/'))
                if os.path.isfile(local_path):
                    csv_path = local_path
                else:
                    # If the file is in the data directory
                    data_path = os.path.join(os.getcwd(), 'data', os.path.basename(csv_path))
                    if os.path.isfile(data_path):
                        csv_path = data_path
                    else:
                        logger.error(f"CSV file not found at {csv_path}")
                        return False, log_capture.get_logs()

            # Read CSV file using pandas
            df = pd.read_csv(csv_path)
            
            # Insert data into the database
            df.to_sql('products', conn, if_exists='replace', index=False)
            
            logger.info(f"Loaded {len(df)} products from {csv_path} into database")
            return True, log_capture.get_logs()
        except Exception as e:
            logger.error(f"Error loading CSV to database: {str(e)}")
            return False, log_capture.get_logs()

def query_recent_created_products(conn, days_back, article_status):
    """Query products where articleStatus='created' AND b2bReadinessDate is within the last X days"""
    with LogCapture() as log_capture:
        try:
            cursor = conn.cursor()
            
            # Calculate the date X days ago
            date_threshold = (datetime.now() - timedelta(days=days_back)).strftime('%Y-%m-%d')
            
            query = '''
            SELECT * FROM products 
            WHERE articleStatus = ? 
            AND b2bReadinessDate >= ?
            '''
            logger.info(f"Executing query to find products with articleStatus='{article_status}' and b2bReadinessDate >= '{date_threshold}'")
            cursor.execute(query, (article_status, date_threshold))
            products = cursor.fetchall()
            
            # Get column names
            column_names = [description[0] for description in cursor.description]
            
            # Convert to list of dictionaries
            products_list = []
            for product in products:
                product_dict = dict(zip(column_names, product))
                products_list.append(product_dict)
            
            logger.info(f"Found {len(products_list)} products with articleStatus='{article_status}' and b2bReadinessDate within the last {days_back} days")
            return products_list, log_capture.get_logs()
        except Exception as e:
            logger.error(f"Error querying products: {str(e)}")
            return [], log_capture.get_logs()

def setup_kafka_producer(config):
    """Set up Kafka producer"""
    with LogCapture() as log_capture:
        try:
            kafka_config = {
                'bootstrap.servers': config['kafka_bootstrap_servers'],
                'acks': config['kafka_acks'],
                'retries': config['kafka_retries'],
                'batch.size': config['kafka_batch_size'],
                'linger.ms': config['kafka_linger_ms'],
                'queue.buffering.max.kbytes': config['kafka_buffer_memory']
            }
            
            producer = Producer(kafka_config)
            logger.info(f"Kafka producer configured with bootstrap servers: {config['kafka_bootstrap_servers']}")
            return producer, log_capture.get_logs()
        except Exception as e:
            logger.error(f"Error setting up Kafka producer: {str(e)}")
            return None, log_capture.get_logs()

def delivery_report(err, msg):
    """Callback function for Kafka producer to report success/failure"""
    if err is not None:
        logger.error(f"Message delivery failed: {err}")
    else:
        logger.info(f"Message delivered to {msg.topic()} [{msg.partition()}] at offset {msg.offset()}")

def publish_products_to_kafka(producer, topic, products):
    """Publish products as JSON messages to Kafka topic"""
    with LogCapture() as log_capture:
        if not producer:
            logger.error("Kafka producer not available")
            return False, log_capture.get_logs()
        
        try:
            success_count = 0
            for product in products:
                # Convert product to JSON
                product_json = json.dumps(product).encode('utf-8')
                
                # Publish to Kafka
                producer.produce(topic, value=product_json, callback=delivery_report)
                
                # Serve delivery callbacks from previous produce calls
                producer.poll(0)
                
                success_count += 1
            
            # Wait for any outstanding messages to be delivered
            producer.flush()
            
            logger.info(f"Published {success_count} products to Kafka topic '{topic}'")
            return True, log_capture.get_logs()
        except Exception as e:
            logger.error(f"Error publishing products to Kafka: {str(e)}")
            return False, log_capture.get_logs()

def is_kafka_available(bootstrap_servers):
    """Check if Kafka is available"""
    with LogCapture() as log_capture:
        import socket
        
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
                    logger.info(f"Kafka is available at {host}:{port}")
                    return True, log_capture.get_logs()
            except Exception as e:
                logger.warning(f"Error checking Kafka availability at {host}:{port}: {str(e)}")
        
        logger.warning(f"Kafka is not available at any of the provided servers: {bootstrap_servers}")
        return False, log_capture.get_logs()

def main():
    """Main function to execute the SQL to Kafka pipeline"""
    with LogCapture() as log_capture:
        logger.info("Starting SQL to Kafka pipeline")
        
        # Load environment variables
        config, env_logs = load_env()
        
        # Set up database connection
        conn, db_logs = setup_database(config)
        
        # Create products table
        table_logs = create_products_table(conn)
        
        # Load CSV data into database
        csv_loaded, csv_logs = load_csv_to_db(conn, config['csv_file_path'])
        
        if not csv_loaded:
            logger.error("Failed to load CSV data into database. Exiting.")
            return
        
        # Query recent products
        products, query_logs = query_recent_created_products(
            conn, 
            config['default_days_back'], 
            config['article_status']
        )
        
        if not products:
            logger.info("No products found matching the criteria. Exiting.")
            conn.close()
            return
        
        # Check if Kafka is available
        kafka_available, kafka_check_logs = is_kafka_available(config['kafka_bootstrap_servers'])
        
        if not kafka_available:
            logger.warning(f"Kafka is not available at {config['kafka_bootstrap_servers']}. "
                          "The pipeline will process data but won't be able to publish to Kafka.")
        else:
            # Set up Kafka producer
            producer, producer_logs = setup_kafka_producer(config)
            
            if producer:
                # Publish products to Kafka
                publish_success, publish_logs = publish_products_to_kafka(producer, config['kafka_topic'], products)
                if not publish_success:
                    logger.error("Failed to publish products to Kafka.")
            else:
                logger.error("Failed to set up Kafka producer. Cannot publish to Kafka.")
        
        # Close database connection
        conn.close()
        logger.info("SQL to Kafka pipeline completed")
        
        # Combine all logs
        all_logs = env_logs + db_logs + table_logs + csv_logs + query_logs
        if kafka_available:
            all_logs += kafka_check_logs + producer_logs
            if producer:
                all_logs += publish_logs
        
        return all_logs

if __name__ == "__main__":
    main()
