#!/usr/bin/env python3
import os
import sys
import json
import unittest
import sqlite3
import pandas as pd
from unittest.mock import patch, MagicMock, mock_open, call
from datetime import datetime, timedelta
from pathlib import Path
from io import StringIO

# Add the src directory to the Python path so we can import the module
sys.path.append(str(Path(__file__).parent.parent))
from src.sql_to_kafka import (
    get_kafka_producer,
    load_csv_to_sqlite,
    query_recent_products,
    publish_to_kafka,
    process_sql_to_kafka
)


class TestSqlToKafka(unittest.TestCase):
    """Test SQL to Kafka functionality."""

    def setUp(self):
        """Set up test environment before each test."""
        # Create test directories
        os.makedirs('data', exist_ok=True)
        os.makedirs('logs', exist_ok=True)
        
        # Sample test data
        self.test_csv_data = """id,productId,articleStatus,b2bReadinessDate,name,description,price
1,P001,created,2025-11-01,Test Product 1,Description 1,19.99
2,P002,created,2025-11-05,Test Product 2,Description 2,29.99
3,P003,draft,2025-11-10,Test Product 3,Description 3,39.99
4,P004,created,2025-10-01,Test Product 4,Description 4,49.99
"""
        # Create a test CSV file
        self.test_csv_path = 'data/test_products.csv'
        with open(self.test_csv_path, 'w') as f:
            f.write(self.test_csv_data)
        
        # Sample products for testing
        self.test_products = [
            {
                'id': 1,
                'productId': 'P001',
                'articleStatus': 'created',
                'b2bReadinessDate': '2025-11-01',
                'name': 'Test Product 1',
                'description': 'Description 1',
                'price': 19.99
            },
            {
                'id': 2,
                'productId': 'P002',
                'articleStatus': 'created',
                'b2bReadinessDate': '2025-11-05',
                'name': 'Test Product 2',
                'description': 'Description 2',
                'price': 29.99
            }
        ]

    def tearDown(self):
        """Clean up after each test."""
        # Remove test files
        if os.path.exists(self.test_csv_path):
            os.remove(self.test_csv_path)

    @patch("src.sql_to_kafka.KafkaProducer")
    @patch("src.sql_to_kafka.logger")
    def test_get_kafka_producer(self, mock_logger, mock_kafka_producer):
        """Test creating a Kafka producer."""
        # Arrange
        mock_producer = MagicMock()
        mock_kafka_producer.return_value = mock_producer
        
        # Act
        result = get_kafka_producer()
        
        # Assert
        mock_kafka_producer.assert_called_once()
        mock_logger.info.assert_called_once_with("Kafka producer created successfully")
        self.assertEqual(result, mock_producer)

    @patch("src.sql_to_kafka.KafkaProducer")
    @patch("src.sql_to_kafka.logger")
    def test_get_kafka_producer_exception(self, mock_logger, mock_kafka_producer):
        """Test exception handling when creating a Kafka producer."""
        # Arrange
        mock_kafka_producer.side_effect = Exception("Connection error")
        
        # Act & Assert
        with self.assertRaises(Exception):
            get_kafka_producer()
        
        mock_logger.error.assert_called_once()

    @patch("src.sql_to_kafka.pd.read_csv")
    @patch("src.sql_to_kafka.sqlite3.connect")
    @patch("src.sql_to_kafka.logger")
    def test_load_csv_to_sqlite(self, mock_logger, mock_connect, mock_read_csv):
        """Test loading a CSV file into SQLite."""
        # Arrange
        # Create a real DataFrame but mock its to_sql method
        df = pd.DataFrame(self.test_products)
        mock_read_csv.return_value = df
        
        # Mock SQLite connection and cursor
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_conn.cursor.return_value = mock_cursor
        mock_connect.return_value = mock_conn
        
        # Mock cursor.fetchone() to return row count
        mock_cursor.fetchone.return_value = (2,)
        
        # Act
        with patch.object(pd.DataFrame, 'to_sql') as mock_to_sql:
            result = load_csv_to_sqlite(self.test_csv_path)
        
        # Assert
        mock_connect.assert_called_once_with(':memory:')
        mock_read_csv.assert_called_once_with(self.test_csv_path)
        mock_to_sql.assert_called_once()
        mock_cursor.execute.assert_called_once()
        self.assertEqual(result, mock_conn)
        self.assertEqual(mock_logger.info.call_count, 3)

    @patch("src.sql_to_kafka.pd.read_csv")
    @patch("src.sql_to_kafka.sqlite3.connect")
    @patch("src.sql_to_kafka.logger")
    def test_load_csv_to_sqlite_exception(self, mock_logger, mock_connect, mock_read_csv):
        """Test exception handling when loading a CSV file into SQLite."""
        # Arrange
        mock_read_csv.side_effect = Exception("File not found")
        
        # Act & Assert
        with self.assertRaises(Exception):
            load_csv_to_sqlite(self.test_csv_path)
        
        mock_logger.error.assert_called_once()

    @patch("src.sql_to_kafka.datetime")
    @patch("src.sql_to_kafka.logger")
    def test_query_recent_products(self, mock_logger, mock_datetime):
        """Test querying recent products from SQLite."""
        # Arrange
        # Set up a fixed date for testing
        fixed_date = datetime(2025, 12, 1)
        mock_datetime.now.return_value = fixed_date
        mock_datetime.strftime = datetime.strftime
        
        # Create a real in-memory SQLite database for this test
        conn = sqlite3.connect(':memory:')
        cursor = conn.cursor()
        
        # Create a test table
        cursor.execute('''
        CREATE TABLE products (
            id INTEGER,
            productId TEXT,
            articleStatus TEXT,
            b2bReadinessDate TEXT,
            name TEXT,
            description TEXT,
            price REAL
        )
        ''')
        
        # Insert test data
        cursor.executemany(
            'INSERT INTO products VALUES (?, ?, ?, ?, ?, ?, ?)',
            [
                (1, 'P001', 'created', '2025-11-01', 'Test Product 1', 'Description 1', 19.99),
                (2, 'P002', 'created', '2025-11-05', 'Test Product 2', 'Description 2', 29.99),
                (3, 'P003', 'draft', '2025-11-10', 'Test Product 3', 'Description 3', 39.99),
                (4, 'P004', 'created', '2025-10-01', 'Test Product 4', 'Description 4', 49.99)
            ]
        )
        conn.commit()
        
        # Act
        result = query_recent_products(conn, days_back=30)
        
        # Assert
        self.assertEqual(len(result), 2)  # Should return 2 'created' products within the last 30 days
        self.assertEqual(result[0]['productId'], 'P002')  # Most recent first
        self.assertEqual(result[1]['productId'], 'P001')
        
        # Clean up
        conn.close()

    @patch("src.sql_to_kafka.get_kafka_producer")
    @patch("src.sql_to_kafka.logger")
    def test_publish_to_kafka(self, mock_logger, mock_get_producer):
        """Test publishing products to Kafka."""
        # Arrange
        # Mock Kafka producer
        mock_producer = MagicMock()
        mock_get_producer.return_value = mock_producer
        
        # Mock future and record metadata
        mock_future = MagicMock()
        mock_metadata = MagicMock()
        mock_metadata.partition = 0
        mock_metadata.offset = 123
        mock_future.get.return_value = mock_metadata
        mock_producer.send.return_value = mock_future
        
        # Act
        result = publish_to_kafka(self.test_products)
        
        # Assert
        self.assertEqual(result, 2)  # 2 products published
        self.assertEqual(mock_producer.send.call_count, 2)
        mock_producer.flush.assert_called_once()
        mock_producer.close.assert_called_once()
        self.assertEqual(mock_logger.debug.call_count, 2)
        mock_logger.info.assert_called_with("Publishing complete: 2 successful, 0 failed")

    @patch("src.sql_to_kafka.get_kafka_producer")
    @patch("src.sql_to_kafka.logger")
    def test_publish_to_kafka_empty(self, mock_logger, mock_get_producer):
        """Test publishing empty product list to Kafka."""
        # Arrange
        
        # Act
        result = publish_to_kafka([])
        
        # Assert
        self.assertEqual(result, 0)
        mock_get_producer.assert_not_called()
        mock_logger.info.assert_called_with("No products to publish to Kafka")

    @patch("src.sql_to_kafka.get_kafka_producer")
    @patch("src.sql_to_kafka.logger")
    def test_publish_to_kafka_error(self, mock_logger, mock_get_producer):
        """Test error handling when publishing to Kafka."""
        # Arrange
        # Mock Kafka producer with error
        mock_producer = MagicMock()
        mock_get_producer.return_value = mock_producer
        
        # Mock future with error
        mock_future = MagicMock()
        mock_future.get.side_effect = Exception("Kafka error")
        mock_producer.send.return_value = mock_future
        
        # Act
        result = publish_to_kafka(self.test_products)
        
        # Assert
        self.assertEqual(result, 0)  # 0 products published successfully
        self.assertEqual(mock_producer.send.call_count, 2)
        mock_producer.flush.assert_called_once()
        mock_producer.close.assert_called_once()
        self.assertEqual(mock_logger.error.call_count, 2)
        mock_logger.info.assert_called_with("Publishing complete: 0 successful, 2 failed")

    @patch("src.sql_to_kafka.publish_to_kafka")
    @patch("src.sql_to_kafka.query_recent_products")
    @patch("src.sql_to_kafka.load_csv_to_sqlite")
    @patch("src.sql_to_kafka.logger")
    def test_process_sql_to_kafka(self, mock_logger, mock_load_csv, mock_query, mock_publish):
        """Test the end-to-end process from SQL to Kafka."""
        # Arrange
        # Mock database connection
        mock_conn = MagicMock()
        mock_load_csv.return_value = mock_conn
        
        # Mock query results
        mock_query.return_value = self.test_products
        
        # Mock publish results
        mock_publish.return_value = 2
        
        # Act
        result = process_sql_to_kafka(self.test_csv_path)
        
        # Assert
        mock_load_csv.assert_called_once_with(self.test_csv_path)
        mock_query.assert_called_once_with(mock_conn, 30)
        mock_conn.close.assert_called_once()
        mock_publish.assert_called_once()
        
        self.assertEqual(result['status'], 'success')
        self.assertEqual(result['products_processed'], 2)
        self.assertEqual(mock_logger.info.call_count, 2)

    @patch("src.sql_to_kafka.load_csv_to_sqlite")
    @patch("src.sql_to_kafka.logger")
    def test_process_sql_to_kafka_error(self, mock_logger, mock_load_csv):
        """Test error handling in the end-to-end process."""
        # Arrange
        # Mock load_csv_to_sqlite to raise an exception
        mock_load_csv.side_effect = Exception("CSV loading error")
        
        # Act
        result = process_sql_to_kafka(self.test_csv_path)
        
        # Assert
        self.assertEqual(result['status'], 'error')
        self.assertIn('error', result)
        mock_logger.error.assert_called_once()


if __name__ == '__main__':
    unittest.main()
