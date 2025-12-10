from datetime import datetime, timedelta
import os
import logging
from airflow import DAG
from airflow.operators.python import PythonOperator
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

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
    'sql_to_kafka_processing',
    default_args=default_args,
    description='A DAG to process SQL data and publish to Kafka',
    schedule='0 1 * * *',  # Run daily at 1 AM
    start_date=datetime(2023, 1, 1),
    tags=['sql', 'kafka', 'sqlite', 'products'],
    catchup=False,
)

# Import the functions from sql_to_kafka.py
# We need to ensure the src directory is in the Python path
def import_sql_to_kafka_module():
    import sys
    import os
    # Add the parent directory to sys.path
    sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
    
    # Now we can import from src
    from src.sql_to_kafka import (
        load_env, setup_database, create_products_table, 
        load_csv_to_db, query_recent_created_products, 
        setup_kafka_producer, publish_products_to_kafka,
        is_kafka_available
    )
    
    return {
        'load_env': load_env,
        'setup_database': setup_database,
        'create_products_table': create_products_table,
        'load_csv_to_db': load_csv_to_db,
        'query_recent_created_products': query_recent_created_products,
        'setup_kafka_producer': setup_kafka_producer,
        'publish_products_to_kafka': publish_products_to_kafka,
        'is_kafka_available': is_kafka_available
    }

# Task 1: Load CSV data into SQLite
def task_load_csv_to_sqlite(**kwargs):
    """Load products.csv into SQLite database"""
    # Import functions
    funcs = import_sql_to_kafka_module()
    
    # Load environment variables
    config, env_logs = funcs['load_env']()
    logger.info(env_logs)
    
    # Set up database connection
    conn, db_logs = funcs['setup_database'](config)
    logger.info(db_logs)
    
    # Create products table
    table_logs = funcs['create_products_table'](conn)
    logger.info(table_logs)
    
    # Load CSV data into database
    csv_loaded, csv_logs = funcs['load_csv_to_db'](conn, config['csv_file_path'])
    logger.info(csv_logs)
    
    if not csv_loaded:
        raise Exception("Failed to load CSV data into SQLite database")
    
    # Store connection and config in XCom for next task
    # Note: We can't store the connection object directly, so we'll recreate it in the next task
    kwargs['ti'].xcom_push(key='csv_file_path', value=config['csv_file_path'])
    kwargs['ti'].xcom_push(key='db_memory', value=config['db_memory'])
    kwargs['ti'].xcom_push(key='article_status', value=config['article_status'])
    kwargs['ti'].xcom_push(key='default_days_back', value=config['default_days_back'])
    
    # Close connection
    conn.close()
    
    # Return both the summary and the detailed logs
    return f"Loaded CSV data from {config['csv_file_path']} into SQLite database\n\nDetailed logs:\n{env_logs}\n{db_logs}\n{table_logs}\n{csv_logs}"

# Task 2: Query recent products
def task_query_recent_products(**kwargs):
    """Query products with recent b2bReadinessDate and specified articleStatus"""
    # Import functions
    funcs = import_sql_to_kafka_module()
    
    # Get values from XCom
    ti = kwargs['ti']
    csv_file_path = ti.xcom_pull(task_ids='load_csv_to_sqlite', key='csv_file_path')
    db_memory = ti.xcom_pull(task_ids='load_csv_to_sqlite', key='db_memory')
    article_status = ti.xcom_pull(task_ids='load_csv_to_sqlite', key='article_status')
    days_back = ti.xcom_pull(task_ids='load_csv_to_sqlite', key='default_days_back')
    
    # Create config dictionary
    config = {
        'csv_file_path': csv_file_path,
        'db_memory': db_memory,
        'article_status': article_status,
        'default_days_back': days_back
    }
    
    # Set up database connection
    conn, db_logs = funcs['setup_database'](config)
    logger.info(db_logs)
    
    # Create products table
    table_logs = funcs['create_products_table'](conn)
    logger.info(table_logs)
    
    # Load CSV data into database (needed again since we're recreating the connection)
    csv_loaded, csv_logs = funcs['load_csv_to_db'](conn, config['csv_file_path'])
    logger.info(csv_logs)
    
    if not csv_loaded:
        raise Exception("Failed to load CSV data into SQLite database")
    
    # Query recent products
    products_result, query_logs = funcs['query_recent_created_products'](conn, days_back, article_status)
    logger.info(query_logs)
    
    # Close connection
    conn.close()
    
    # Store products in XCom for next task
    # To avoid XCom size limitations, store products in chunks
    max_products_per_chunk = 100
    for i in range(0, len(products_result), max_products_per_chunk):
        chunk = products_result[i:i + max_products_per_chunk]
        ti.xcom_push(key=f'products_chunk_{i//max_products_per_chunk}', value=chunk)
    
    ti.xcom_push(key='product_count', value=len(products_result))
    ti.xcom_push(key='chunk_count', value=(len(products_result) + max_products_per_chunk - 1) // max_products_per_chunk)
    
    # Return both the summary and the detailed logs
    return f"Found {len(products_result)} products matching criteria\n\nDetailed logs:\n{db_logs}\n{table_logs}\n{csv_logs}\n{query_logs}"

# Task 3: Publish to Kafka
def task_publish_to_kafka(**kwargs):
    """Publish queried products to Kafka topic"""
    # Import functions
    funcs = import_sql_to_kafka_module()
    
    # Load environment variables
    config, env_logs = funcs['load_env']()
    logger.info(env_logs)
    
    # Get values from XCom
    ti = kwargs['ti']
    product_count = ti.xcom_pull(task_ids='query_recent_products', key='product_count')
    chunk_count = ti.xcom_pull(task_ids='query_recent_products', key='chunk_count')
    
    if not product_count:
        logger.info("No products to publish")
        return "No products found to publish"
    
    # Check if Kafka is available
    kafka_available, kafka_check_logs = funcs['is_kafka_available'](config['kafka_bootstrap_servers'])
    logger.info(kafka_check_logs)
    
    if not kafka_available:
        logger.warning(f"Kafka is not available at {config['kafka_bootstrap_servers']}. "
                      "The task will process data but won't be able to publish to Kafka.")
        return f"Kafka not available at {config['kafka_bootstrap_servers']}. {product_count} products would have been published.\n\nDetailed logs:\n{env_logs}\n{kafka_check_logs}"
    
    # Set up Kafka producer
    producer, producer_logs = funcs['setup_kafka_producer'](config)
    logger.info(producer_logs)
    
    if not producer:
        raise Exception("Failed to set up Kafka producer")
    
    # Publish products in chunks to Kafka
    total_published = 0
    all_publish_logs = ""
    for i in range(chunk_count):
        products_chunk = ti.xcom_pull(task_ids='query_recent_products', key=f'products_chunk_{i}')
        if products_chunk:
            published, publish_logs = funcs['publish_products_to_kafka'](producer, config['kafka_topic'], products_chunk)
            logger.info(publish_logs)
            all_publish_logs += publish_logs
            if published:
                total_published += len(products_chunk)
    
    # Return both the summary and the detailed logs
    return f"Successfully published {total_published} products to Kafka topic '{config['kafka_topic']}'\n\nDetailed logs:\n{env_logs}\n{kafka_check_logs}\n{producer_logs}\n{all_publish_logs}"

# Task 4: Summary and monitoring
def task_pipeline_summary(**kwargs):
    """Generate pipeline execution summary"""
    # Import functions
    funcs = import_sql_to_kafka_module()
    
    # Load environment variables
    config, env_logs = funcs['load_env']()
    logger.info(env_logs)
    
    # Get values from XCom
    ti = kwargs['ti']
    product_count = ti.xcom_pull(task_ids='query_recent_products', key='product_count') or 0
    
    # Create summary
    summary = {
        'pipeline': 'sql_to_kafka_processing',
        'execution_time': datetime.now().isoformat(),
        'products_processed': product_count,
        'kafka_topic': config['kafka_topic'],
        'status': 'completed',
        'query_criteria': {
            'articleStatus': config['article_status'],
            'days_back': config['default_days_back']
        }
    }
    
    logger.info(f"Pipeline Summary: {summary}")
    
    # Get task logs from XCom
    load_task_logs = ti.xcom_pull(task_ids='load_csv_to_sqlite')
    query_task_logs = ti.xcom_pull(task_ids='query_recent_products')
    kafka_task_logs = ti.xcom_pull(task_ids='publish_to_kafka')
    
    # Combine all logs for a comprehensive pipeline summary
    all_logs = f"""
=== SQL to Kafka Pipeline Summary ===
Pipeline: sql_to_kafka_processing
Execution Time: {datetime.now().isoformat()}
Products Processed: {product_count}
Kafka Topic: {config['kafka_topic']}
Status: completed
Query Criteria:
  - Article Status: {config['article_status']}
  - Days Back: {config['default_days_back']}

=== Detailed Task Logs ===

--- Load CSV to SQLite Task ---
{load_task_logs}

--- Query Recent Products Task ---
{query_task_logs}

--- Publish to Kafka Task ---
{kafka_task_logs}
"""
    
    logger.info("Pipeline execution completed with full logs captured")
    return all_logs

# Create task instances
load_data_task = PythonOperator(
    task_id='load_csv_to_sqlite',
    python_callable=task_load_csv_to_sqlite,
    dag=dag,
)

query_task = PythonOperator(
    task_id='query_recent_products',
    python_callable=task_query_recent_products,
    dag=dag,
)

kafka_task = PythonOperator(
    task_id='publish_to_kafka',
    python_callable=task_publish_to_kafka,
    dag=dag,
)

summary_task = PythonOperator(
    task_id='pipeline_summary',
    python_callable=task_pipeline_summary,
    dag=dag,
)

# Set task dependencies
load_data_task >> query_task >> kafka_task >> summary_task
