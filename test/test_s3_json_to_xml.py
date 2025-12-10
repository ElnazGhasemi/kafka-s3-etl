#!/usr/bin/env python3
import os
import sys
import json
import unittest
from unittest.mock import patch, MagicMock, mock_open
from pathlib import Path
from botocore.exceptions import ClientError

# Add the src directory to the Python path so we can import the module
sys.path.append(str(Path(__file__).parent.parent))
from src.s3_json_to_xml import (
    get_s3_client,
    download_json_from_s3,
    read_json_file,
    json_to_xml,
    _to_xml_product,
    upload_file_to_s3,
    upload_xml_string_to_s3
)


class TestS3JsonToXml(unittest.TestCase):
    """Test S3 JSON to XML conversion functionality."""

    def setUp(self):
        """Set up test environment before each test."""
        # Create test directories
        os.makedirs('data', exist_ok=True)
        os.makedirs('logs', exist_ok=True)
        
        # Test file paths
        self.test_json_path = 'data/test_data.json'
        self.output_xml_path = 'data/output_test.xml'
        
        # Sample test data
        self.test_data = [
            {
                "ean": "5713447649590",
                "styleNumber": "16050217",
                "styleOption": "16050217_Black",
                "size": "M",
                "countryOfOrigin": [{"language": "English", "value": "BD"}],
                "itemSellingPrices": [
                    {"priceType": "EDI Retail Price", "priceCurrency": "Euro", "price": 39.99, "validInRegion": "Austria"}
                ],
                "color": "Black",
                "brandName": "Test Brand"
            }
        ]
        
        # Create a test JSON file
        with open(self.test_json_path, 'w') as f:
            json.dump(self.test_data, f)

    def tearDown(self):
        """Clean up after each test."""
        # Remove test files
        for file_path in [self.test_json_path, self.output_xml_path]:
            if os.path.exists(file_path):
                os.remove(file_path)

    @patch("src.s3_json_to_xml.os.getenv")
    def test_get_s3_client_development(self, mock_getenv):
        """Test getting S3 client for development environment."""
        # Arrange
        mock_getenv.side_effect = lambda key, default=None: {
            'ENV': 'development',
            'MINIO_ENDPOINT': 'http://localhost:9000',
            'MINIO_ACCESS_KEY': 'minioadmin',
            'MINIO_SECRET_KEY': 'minioadmin',
            'MINIO_REGION': 'us-east-1'
        }.get(key, default)
        
        # Act
        with patch("src.s3_json_to_xml.boto3.client") as mock_boto3_client:
            get_s3_client()
            
            # Assert
            mock_boto3_client.assert_called_once_with(
                's3',
                endpoint_url='http://localhost:9000',
                aws_access_key_id='minioadmin',
                aws_secret_access_key='minioadmin',
                region_name='us-east-1'
            )

    @patch("src.s3_json_to_xml.ENV", "production")
    def test_get_s3_client_production(self):
        """Test getting S3 client for production environment."""
        # Act
        with patch("src.s3_json_to_xml.boto3.client") as mock_boto3_client:
            get_s3_client()
            
            # Assert
            mock_boto3_client.assert_called_once_with('s3')

    @patch("src.s3_json_to_xml.get_s3_client")
    @patch("src.s3_json_to_xml.get_daily_logger")
    def test_download_json_from_s3(self, mock_get_logger, mock_get_s3_client):
        """Test downloading a JSON file from S3."""
        # Arrange
        bucket_name = "test-bucket"
        s3_key = "folder/test_file.json"
        local_filename = "test_file.json"
        
        # Configure the mocks
        mock_s3 = MagicMock()
        mock_get_s3_client.return_value = mock_s3
        mock_logger = MagicMock()
        mock_get_logger.return_value = mock_logger
        
        # Act
        result = download_json_from_s3(bucket_name, s3_key, local_filename)
        
        # Assert
        mock_get_s3_client.assert_called_once()
        mock_s3.download_file.assert_called_once_with(
            bucket_name,
            s3_key,
            f"/tmp/{local_filename}"
        )
        self.assertEqual(result, f"/tmp/{local_filename}")
        mock_logger.info.assert_called()

    @patch("src.s3_json_to_xml.get_s3_client")
    @patch("src.s3_json_to_xml.get_daily_logger")
    def test_download_json_from_s3_client_error(self, mock_get_logger, mock_get_s3_client):
        """Test downloading a JSON file from S3 with ClientError."""
        # Arrange
        bucket_name = "test-bucket"
        s3_key = "folder/test_file.json"
        local_filename = "test_file.json"
        
        # Configure the mocks
        mock_s3 = MagicMock()
        mock_s3.download_file.side_effect = ClientError(
            {'Error': {'Code': 'NoSuchBucket', 'Message': 'The specified bucket does not exist'}},
            'GetObject'
        )
        mock_get_s3_client.return_value = mock_s3
        mock_logger = MagicMock()
        mock_get_logger.return_value = mock_logger
        
        # Act & Assert
        with self.assertRaises(ClientError):
            download_json_from_s3(bucket_name, s3_key, local_filename)
        
        mock_get_s3_client.assert_called_once()
        mock_s3.download_file.assert_called_once()
        mock_logger.error.assert_called_once()

    @patch("builtins.open", new_callable=mock_open, read_data='{"key": "value"}')
    @patch("src.s3_json_to_xml.get_daily_logger")
    def test_read_json_file(self, mock_get_logger, mock_file):
        """Test reading a JSON file."""
        # Arrange
        filepath = "data/test.json"
        expected_data = {"key": "value"}
        
        # Configure mocks
        mock_file.return_value.__enter__.return_value.read.return_value = json.dumps(expected_data)
        mock_logger = MagicMock()
        mock_get_logger.return_value = mock_logger
        
        # Act
        result = read_json_file(filepath)
        
        # Assert
        mock_file.assert_called_once_with(filepath, 'r', encoding='utf-8')
        self.assertEqual(result, expected_data)
        mock_logger.info.assert_called_once()

    @patch("builtins.open")
    @patch("src.s3_json_to_xml.get_daily_logger")
    def test_read_json_file_not_found(self, mock_get_logger, mock_file):
        """Test reading a non-existent JSON file."""
        # Arrange
        filepath = "data/nonexistent.json"
        
        # Configure mocks
        mock_file.side_effect = FileNotFoundError("File not found")
        mock_logger = MagicMock()
        mock_get_logger.return_value = mock_logger
        
        # Act & Assert
        with self.assertRaises(FileNotFoundError):
            read_json_file(filepath)
        
        mock_logger.error.assert_called_once()

    @patch("src.s3_json_to_xml.os.makedirs")
    @patch("src.s3_json_to_xml.get_daily_logger")
    def test_json_to_xml_conversion(self, mock_get_logger, mock_makedirs):
        """Test converting JSON to XML."""
        # Arrange
        mock_logger = MagicMock()
        mock_get_logger.return_value = mock_logger
        
        # Act
        xml_output = json_to_xml(self.test_data)
        
        # Assert
        # makedirs is called multiple times (once in json_to_xml and once in _to_xml_product)
        self.assertTrue(mock_makedirs.call_count >= 1)
        mock_logger.info.assert_called_with("Successfully converted JSON to XML")
        
        # Check for expected XML elements
        expected_elements = [
            '<?xml version="1.0" encoding="UTF-8"?>',
            '<products>',
            '<product>',
            '<ean>5713447649590</ean>',
            '<styleNumber>16050217</styleNumber>',
            '<styleOption>16050217_Black</styleOption>',
            '<size>M</size>',
            '<countryOfOrigin>',
            '<entry>',
            '<language>English</language>',
            '<value>BD</value>',
            '</entry>',
            '</countryOfOrigin>',
            '<itemSellingPrices>',
            '<price>',
            '<priceType>EDI Retail Price</priceType>',
            '<priceCurrency>Euro</priceCurrency>',
            '<price>39.99</price>',
            '<validInRegion>Austria</validInRegion>',
            '</price>',
            '</itemSellingPrices>',
            '<color>Black</color>',
            '<brandName>Test Brand</brandName>',
            '</product>',
            '</products>'
        ]
        
        for element in expected_elements:
            self.assertIn(element, xml_output)

    @patch("src.s3_json_to_xml.os.makedirs")
    @patch("src.s3_json_to_xml.get_daily_logger")
    def test_json_to_xml_conversion_error(self, mock_get_logger, mock_makedirs):
        """Test converting invalid JSON to XML."""
        # Arrange
        invalid_data = "not a list"  # This will cause a TypeError
        mock_logger = MagicMock()
        mock_get_logger.return_value = mock_logger
        
        # Act & Assert
        with self.assertRaises(TypeError):
            json_to_xml(invalid_data)
        
        mock_makedirs.assert_called_once_with('logs', exist_ok=True)
        mock_logger.error.assert_called_once()

    @patch("src.s3_json_to_xml.os.makedirs")
    @patch("src.s3_json_to_xml.get_daily_logger")
    def test_to_xml_product(self, mock_get_logger, mock_makedirs):
        """Test the _to_xml_product helper function."""
        # Arrange
        product = self.test_data[0]
        padding = "  "
        mock_logger = MagicMock()
        mock_get_logger.return_value = mock_logger
        
        # Act
        xml_output = _to_xml_product(product, padding)
        
        # Assert
        mock_makedirs.assert_called_once_with('logs', exist_ok=True)
        
        expected_elements = [
            '  <ean>5713447649590</ean>',
            '  <styleNumber>16050217</styleNumber>',
            '  <countryOfOrigin>',
            '    <entry>',
            '      <language>English</language>',
            '      <value>BD</value>',
            '    </entry>',
            '  </countryOfOrigin>',
            '  <itemSellingPrices>',
            '    <price>',
            '      <priceType>EDI Retail Price</priceType>',
            '      <priceCurrency>Euro</priceCurrency>',
            '      <price>39.99</price>',
            '      <validInRegion>Austria</validInRegion>',
            '    </price>',
            '  </itemSellingPrices>'
        ]
        
        for element in expected_elements:
            self.assertIn(element, xml_output)

    @patch("src.s3_json_to_xml.os.makedirs")
    @patch("src.s3_json_to_xml.get_daily_logger")
    def test_to_xml_product_error(self, mock_get_logger, mock_makedirs):
        """Test the _to_xml_product helper function with invalid data."""
        # Arrange
        invalid_product = "not a dict"  # This will cause a TypeError
        padding = "  "
        mock_logger = MagicMock()
        mock_get_logger.return_value = mock_logger
        
        # Act & Assert
        with self.assertRaises(TypeError):
            _to_xml_product(invalid_product, padding)
        
        mock_makedirs.assert_called_once_with('logs', exist_ok=True)
        mock_logger.error.assert_called_once()

    @patch("src.s3_json_to_xml.get_s3_client")
    @patch("src.s3_json_to_xml.os.makedirs")
    @patch("src.s3_json_to_xml.get_daily_logger")
    def test_upload_file_to_s3_success(self, mock_get_logger, mock_makedirs, mock_get_s3_client):
        """Test successful file upload to S3."""
        # Arrange
        local_filepath = "data/test.xml"
        bucket_name = "test-bucket"
        s3_key = "output/test.xml"
        
        # Configure mocks
        mock_s3 = MagicMock()
        mock_get_s3_client.return_value = mock_s3
        mock_logger = MagicMock()
        mock_get_logger.return_value = mock_logger
        
        # Act
        result = upload_file_to_s3(local_filepath, bucket_name, s3_key)
        
        # Assert
        mock_makedirs.assert_called_once_with("logs", exist_ok=True)
        mock_get_s3_client.assert_called_once()
        mock_s3.upload_file.assert_called_once_with(local_filepath, bucket_name, s3_key)
        self.assertTrue(result)
        # Verify logging calls - there are 4 info log messages in the success case
        self.assertEqual(mock_logger.info.call_count, 4)
        mock_logger.error.assert_not_called()

    @patch("src.s3_json_to_xml.get_s3_client")
    @patch("src.s3_json_to_xml.os.makedirs")
    @patch("src.s3_json_to_xml.get_daily_logger")
    def test_upload_file_to_s3_failure(self, mock_get_logger, mock_makedirs, mock_get_s3_client):
        """Test file upload to S3 with failure."""
        # Arrange
        local_filepath = "data/test.xml"
        bucket_name = "test-bucket"
        s3_key = "output/test.xml"
        
        # Configure mocks
        mock_s3 = MagicMock()
        mock_s3.upload_file.side_effect = Exception("Upload failed")
        mock_get_s3_client.return_value = mock_s3
        mock_logger = MagicMock()
        mock_get_logger.return_value = mock_logger
        
        # Act
        result = upload_file_to_s3(local_filepath, bucket_name, s3_key)
        
        # Assert
        mock_makedirs.assert_called_once_with("logs", exist_ok=True)
        mock_get_s3_client.assert_called_once()
        mock_s3.upload_file.assert_called_once_with(local_filepath, bucket_name, s3_key)
        self.assertFalse(result)
        # Verify logging calls - there are 3 info log messages in the failure case
        self.assertEqual(mock_logger.info.call_count, 3)
        mock_logger.error.assert_called_once()

    @patch("src.s3_json_to_xml.get_s3_client")
    @patch("src.s3_json_to_xml.os.makedirs")
    @patch("src.s3_json_to_xml.get_daily_logger")
    def test_upload_xml_string_to_s3_success(self, mock_get_logger, mock_makedirs, mock_get_s3_client):
        """Test successful XML string upload to S3."""
        # Arrange
        xml_content = "<xml>test</xml>"
        bucket_name = "test-bucket"
        s3_key = "output/test.xml"
        
        # Configure mocks
        mock_s3 = MagicMock()
        mock_get_s3_client.return_value = mock_s3
        mock_logger = MagicMock()
        mock_get_logger.return_value = mock_logger
        
        # Act
        result = upload_xml_string_to_s3(xml_content, bucket_name, s3_key)
        
        # Assert
        mock_makedirs.assert_called_once_with("logs", exist_ok=True)
        mock_get_s3_client.assert_called_once()
        mock_s3.put_object.assert_called_once_with(
            Bucket=bucket_name,
            Key=s3_key,
            Body=xml_content.encode('utf-8'),
            ContentType='application/xml'
        )
        self.assertTrue(result)
        # Verify logging calls - there are 4 info log messages in the success case
        self.assertEqual(mock_logger.info.call_count, 4)
        mock_logger.error.assert_not_called()

    @patch("src.s3_json_to_xml.get_s3_client")
    @patch("src.s3_json_to_xml.os.makedirs")
    @patch("src.s3_json_to_xml.get_daily_logger")
    def test_upload_xml_string_to_s3_failure(self, mock_get_logger, mock_makedirs, mock_get_s3_client):
        """Test XML string upload to S3 with failure."""
        # Arrange
        xml_content = "<xml>test</xml>"
        bucket_name = "test-bucket"
        s3_key = "output/test.xml"
        
        # Configure mocks
        mock_s3 = MagicMock()
        mock_s3.put_object.side_effect = Exception("Upload failed")
        mock_get_s3_client.return_value = mock_s3
        mock_logger = MagicMock()
        mock_get_logger.return_value = mock_logger
        
        # Act
        result = upload_xml_string_to_s3(xml_content, bucket_name, s3_key)
        
        # Assert
        mock_makedirs.assert_called_once_with("logs", exist_ok=True)
        mock_get_s3_client.assert_called_once()
        mock_s3.put_object.assert_called_once()
        self.assertFalse(result)
        # Verify logging calls - there are 3 info log messages in the failure case
        self.assertEqual(mock_logger.info.call_count, 3)
        mock_logger.error.assert_called_once()

    @patch("src.s3_json_to_xml.upload_xml_string_to_s3")
    @patch("src.s3_json_to_xml.os.makedirs")
    @patch("src.s3_json_to_xml.get_daily_logger")
    def test_end_to_end_json_to_xml_process(self, mock_get_logger, mock_makedirs, mock_upload_xml_string_to_s3):
        """Test the end-to-end process from JSON to XML with direct upload."""
        # Arrange
        bucket_name = "test-bucket"
        s3_key = "output/test.xml"
        mock_logger = MagicMock()
        mock_get_logger.return_value = mock_logger
        mock_upload_xml_string_to_s3.return_value = True
        
        # Act - Read the test JSON file
        json_data = read_json_file(self.test_json_path)
        
        # Convert JSON to XML
        xml_output = json_to_xml(json_data)
        
        # Upload XML to S3
        upload_xml_string_to_s3(xml_output, bucket_name, s3_key)
        
        # Assert
        mock_upload_xml_string_to_s3.assert_called_once_with(xml_output, bucket_name, s3_key)


if __name__ == '__main__':
    unittest.main()
