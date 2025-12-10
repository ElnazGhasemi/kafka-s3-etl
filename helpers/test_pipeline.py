#!/usr/bin/env python3
import os
import sys
import unittest
import sqlite3
from unittest.mock import MagicMock, patch
from datetime import datetime, timedelta

# Add the parent directory to the path so we can import the module
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from src.sql_to_kafka_pipeline import (
    load_env, setup_database, create_products_table, 
    load_csv_to_db, query_recent_created_products
)

class TestPipeline(unittest.TestCase):
    """Test cases for the SQL to Kafka pipeline"""
    
    def setUp(self):
        """Set up test environment"""
        # Create an in-memory database for testing
        self.conn = sqlite3.connect(':memory:')
        create_products_table(self.conn)
        
        # Sample test data
        self.cursor = self.conn.cursor()
        
        # Insert test data - some within the last 30 days, some older
        today = datetime.now()
        recent_date = (today - timedelta(days=15)).strftime('%Y-%m-%d')
        old_date = (today - timedelta(days=45)).strftime('%Y-%m-%d')
        
        self.cursor.execute('''
        INSERT INTO products (
            ean, styleNumber, articleStatus, b2bReadinessDate
        ) VALUES 
        ('123456789', 'STYLE1', 'created', ?),
        ('123456790', 'STYLE2', 'created', ?),
        ('123456791', 'STYLE3', 'in_progress', ?),
        ('123456792', 'STYLE4', 'created', ?)
        ''', (recent_date, recent_date, recent_date, old_date))
        
        self.conn.commit()
    
    def tearDown(self):
        """Clean up after tests"""
        self.conn.close()
    
    def test_query_recent_created_products(self):
        """Test querying products with articleStatus='created' and recent b2bReadinessDate"""
        products = query_recent_created_products(self.conn, 30, 'created')
        
        # Should find only the products with articleStatus='created' and b2bReadinessDate within last 30 days
        self.assertEqual(len(products), 2)
        
        # Check that all returned products have the correct articleStatus
        for product in products:
            self.assertEqual(product['articleStatus'], 'created')
    
    @patch('src.sql_to_kafka_pipeline.pd.read_csv')
    def test_load_csv_to_db(self, mock_read_csv):
        """Test loading CSV data into the database"""
        # Mock the pandas read_csv function
        mock_df = MagicMock()
        mock_df.to_sql = MagicMock()
        mock_read_csv.return_value = mock_df
        
        # Call the function with a test path
        result = load_csv_to_db(self.conn, 'test_path.csv')
        
        # Check that the function returned True (success)
        self.assertTrue(result)
        
        # Check that pandas read_csv was called with the correct path
        mock_read_csv.assert_called_once_with('test_path.csv')
        
        # Check that to_sql was called with the correct parameters
        mock_df.to_sql.assert_called_once_with('products', self.conn, if_exists='replace', index=False)

if __name__ == '__main__':
    unittest.main()
