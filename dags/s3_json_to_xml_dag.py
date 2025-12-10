from datetime import datetime, timedelta
import os
import json
import logging
from airflow import DAG
from airflow.operators.python import PythonOperator
import boto3
from botocore.exceptions import ClientError
# Import functions from sql_to_kafka.py
import sys
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from src.s3_json_to_xml import (
    download_json_from_s3, read_json_file, json_to_xml, upload_xml_string_to_s3 )


# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Define default arguments for the DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
}

# Define the DAG
dag = DAG(
    's3_json_to_xml_processing',
    default_args=default_args,
    description='A DAG to download JSON from S3, convert to XML, and upload back to S3',
    schedule='0 0 * * *',  # Run daily at midnight
    start_date=datetime(2025, 12, 6),  # Yesterday's date
    tags=['s3', 'json', 'xml'],
    catchup=False,
)

# Task 1: Download JSON from S3
def task_download_json_from_s3(**kwargs):
    """Download JSON file from S3 storage"""
    bucket_name = kwargs.get('bucket_name', 'input-bucket')
    s3_key = kwargs.get('s3_key', 'sample_products.json')
    local_filename = kwargs.get('local_filename', 'sample_products.json')
    
    local_path, download_logs = download_json_from_s3(bucket_name, s3_key, local_filename)
    logger.info(download_logs)
    
    # Push the local file path to XCom for next task
    kwargs['task_instance'].xcom_push(key='local_json_path', value=local_path)
    
    # Return both the summary and the detailed logs
    return f"Downloaded JSON file from s3://{bucket_name}/{s3_key} to {local_path}\n\nDetailed logs:\n{download_logs}"

download_task = PythonOperator(
    task_id='download_json_from_s3',
    python_callable=task_download_json_from_s3,
    op_kwargs={
        'bucket_name': 'input-bucket',
        's3_key': 'sample_products.json',
        'local_filename': 'sample_products.json',
    },
    dag=dag,
)

# Task 2: Convert JSON to XML
def task_json_to_xml(**kwargs):
    """Convert downloaded JSON file to XML format"""
    ti = kwargs['task_instance']
    local_json_path = ti.xcom_pull(task_ids='download_json_from_s3', key='local_json_path')
    
    if not local_json_path:
        raise ValueError("No JSON file path found from previous task")
    
    # Read the JSON file
    data, read_logs = read_json_file(local_json_path)
    logger.info(read_logs)
    
    # Convert JSON to XML
    xml_output, convert_logs = json_to_xml(data)
    logger.info(convert_logs)
    
    # Push XML content to XCom for next task
    ti.xcom_push(key='xml_content', value=xml_output)
    
    # Return both the summary and the detailed logs
    return f"Converted JSON from {local_json_path} to XML format\n\nDetailed logs:\n{read_logs}\n{convert_logs}"

convert_task = PythonOperator(
    task_id='json_to_xml_conversion',
    python_callable=task_json_to_xml,
    dag=dag,
)

# Task 3: Upload XML to S3
def task_upload_xml_to_s3(**kwargs):
    """Upload converted XML content directly to S3 storage"""
    ti = kwargs['task_instance']
    xml_content = ti.xcom_pull(task_ids='json_to_xml_conversion', key='xml_content')
    
    if not xml_content:
        raise ValueError("No XML content found from previous task")
    
    bucket_name = kwargs.get('bucket_name', 'output-bucket')
    s3_key = kwargs.get('s3_key', 'output.xml')
    
    # Upload XML content directly to S3
    result, upload_logs = upload_xml_string_to_s3(xml_content, bucket_name, s3_key)
    logger.info(upload_logs)
    
    # Clean up local files
    try:
        local_json_path = ti.xcom_pull(task_ids='download_json_from_s3', key='local_json_path')
        if local_json_path and os.path.exists(local_json_path):
            os.remove(local_json_path)
        logger.info("Cleaned up local temporary files")
    except Exception as e:
        logger.warning(f"Failed to clean up local files: {str(e)}")
    
    # Return both the summary and the detailed logs
    return f"{'Successfully' if result else 'Failed to'} uploaded XML content to s3://{bucket_name}/{s3_key}\n\nDetailed logs:\n{upload_logs}"

upload_task = PythonOperator(
    task_id='upload_xml_to_s3',
    python_callable=task_upload_xml_to_s3,
    op_kwargs={
        'bucket_name': 'output-bucket',
        's3_key': 'output.xml',
    },
    dag=dag,
)

# Set task dependencies
download_task >> convert_task >> upload_task
