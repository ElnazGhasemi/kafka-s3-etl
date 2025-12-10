# Data Engineering Pipeline Project

This project implements data processing pipelines for product data, including SQL to Kafka streaming and S3 JSON to XML conversion.

## Prerequisites

- Docker and Docker Compose
- Python 3.8+
- AWS CLI (for production deployment)
- Terraform (for infrastructure provisioning)

## Setup Instructions

1. **Clone the repository**
   ```
   git clone git@github.com:elnaz-deriv/Elnaz_DataEngineer_Assessment.git
   cd Elnaz_DataEngineer_Assessment
   ```

2. **Set up environment variables**
   ```
   cp .env.sample .env
   # Edit .env file with your configuration
   ```

3. **Build and start the services**
   ```
   docker compose -f docker-compose-complete.yml up -d
   ```

4. **Install Python dependencies (for local development)**
   ```
   pip install -r requirements.txt
   ```

## How to Run Each Pipeline

### SQL to Kafka Pipeline

This pipeline extracts product data from a SQLite database and publishes it to a Kafka topic.

**Manual execution:**
```
python -m src.sql_to_kafka
```

**Configuration options:**
- Set `DB_TYPE` in .env to specify database type (default: sqlite)
- Set `CSV_FILE_PATH` to specify the input CSV file
- Set `KAFKA_TOPIC` to specify the target Kafka topic

### S3 JSON to XML Pipeline

This pipeline downloads JSON data from S3, converts it to XML format, and uploads it back to S3.

**Manual execution:**
```
python -m src.s3_json_to_xml
```

**Configuration options:**
- Set `S3_BUCKET` to specify the source S3 bucket
- Set `S3_JSON_KEY` to specify the JSON file key
- Set `S3_XML_KEY` to specify the output XML file key

## How to Trigger the Airflow DAGs

1. **Access the Airflow UI**
   - Open http://localhost:8085 in your browser
   - Login with username: admin As I used standalone airflow for getting username and password go to the airflow docker container with this command
   ```
   Copy code
   docker ps

   docker exec -it <docker_id> bash
   cat /opt/airflow/simple_auth_manager_passwords.json.generated
   ```

2. **Trigger the SQL to Kafka DAG**
   - Find the DAG named `sql_to_kafka_processing`
   - Click the "Trigger" button to trigger it manually

3. **Trigger the S3 JSON to XML DAG**
   - Find the DAG named `s3_json_to_xml_processing`
   - Click the "Trigger" button to trigger it manually

## How to Run Tests

Run all tests:
```
pytest
```

Run specific test files:
```
pytest test/test_sql_to_kafka.py
pytest test/test_s3_json_to_xml.py
pytest test/test_logger.py
```

## Technology Choices and Justifications

### Core Technologies

- **Apache Airflow**: Chosen for workflow orchestration due to its flexibility, scalability, and extensive operator library.
- **Kafka**: Used for real-time data streaming, providing high throughput, fault tolerance, and decoupling of data producers and consumers.
- **SQLite**: Lightweight database for development and testing, easily replaceable with production databases.
- **AWS S3**: Scalable object storage for input/output data files.
- **Docker**: Ensures consistent development and deployment environments.

### Data Loading

- **Bulk Insert/Copy**: For loading data from CSV to database, bulk insert or copy commands are a better and faster approach than row-by-row insertion.
  - **SQLite Example**: Data can be easily loaded using the SQLite copy command:
    ```
    sqlite3 products.db ".mode csv" ".import --skip 1 data/products.csv products"
    ```
  - This approach significantly improves performance by reducing I/O operations and transaction overhead.
  - For large datasets, bulk loading can be orders of magnitude faster than individual inserts.

### Data Processing

- **Pandas**: Efficient data manipulation and transformation.
- **PyArrow**: High-performance columnar storage format support, ideal for XML processing with structured data.
- **Confluent Kafka**: Robust Kafka client with better performance than kafka-python.

### Infrastructure as Code

- **Terraform**: Manages AWS infrastructure, ensuring reproducible deployments.

## Known Limitations and Assumptions

- **Development Environment**: The project assumes MinIO as a local S3 alternative for development.
- **Data Volume**: Current implementation is optimized for moderate data volumes. For very large datasets, consider implementing batch processing or Spark integration.
- **Authentication**: Basic authentication is implemented for development. Production deployment should use proper IAM roles and secrets management.
- **Error Handling**: Basic error handling is implemented. Production systems may need more robust error recovery mechanisms.

## Amazon S3 with Multi-Part & Parallel Transfers

Use S3 with multi-part upload for large files and parallel uploads/downloads for many files. This improves throughput, allows retrying only failed parts, and scales well for large data volumes.

Benefits of this approach:
- **Improved Performance**: Parallel processing of file parts increases overall throughput
- **Resilience**: Failed parts can be retried independently without restarting the entire transfer
- **Bandwidth Utilization**: Better utilization of available network bandwidth
- **Scalability**: Handles files from megabytes to terabytes efficiently

## Checksums for Data Integrity

Generate a checksum (e.g., SHA-256) per file and store it with the object (S3 metadata/tags or DynamoDB). Verify it on upload and again on download to ensure end‑to‑end data integrity and detect corruption.

Implementation approach:
- Calculate checksums before uploading files to S3
- Store checksums in S3 object metadata or dedicated tracking system
- Verify checksums after downloads to ensure data hasn't been corrupted
- Implement automatic retry mechanisms for failed integrity checks

## DynamoDB for Pipeline State & Resumability

Use DynamoDB to track each file's status (PENDING, IN_PROGRESS, COMPLETED, FAILED) and checksum. Workers pick only PENDING/FAILED files and update to COMPLETED when done, making the pipeline fault-tolerant, resumable, and easy to scale.

Key advantages:
- **Fault Tolerance**: Pipeline can resume from failures without data loss
- **Scalability**: Multiple workers can process files in parallel without conflicts
- **Visibility**: Real-time status tracking of all files in the pipeline
- **Efficiency**: Prevents redundant processing by maintaining clear state

## Distributed Processing with Spark

For processing large files, Apache Spark provides:

1. **Distributed Computing**: Parallel processing across multiple nodes
2. **In-memory Processing**: Faster data operations compared to disk-based processing
3. **Fault Tolerance**: Automatic recovery from node failures
4. **Scalability**: Easy scaling by adding more worker nodes

## XML Processing with Columnar Storage

For XML-related tasks, columnar storage formats like Apache Arrow or Parquet offer significant benefits:

```xml
<products>
  <product>
    <ean>5713447649590</ean>
    <styleNumber>16050217</styleNumber>
    <!-- More fields -->
  </product>
  <!-- More products -->
</products>
```

Columnar storage organizes data by columns rather than rows, providing:
- Better compression ratios
- Faster query performance when accessing specific fields
- Efficient storage for sparse data

## Benefits of .env File

Using a `.env` file provides several advantages:

1. **Security**: Sensitive information like API keys and passwords are kept outside version control.
2. **Environment Separation**: Easy switching between development, testing, and production configurations.
3. **Configuration Management**: Centralized location for all environment variables.
4. **Developer Experience**: Simplifies onboarding as developers can quickly set up their environment.

## Secrets Management in Production

For production environments, using AWS Secrets Manager or AWS Systems Manager Parameter Store is recommended:

- **AWS Parameter Store**: More cost-effective with 10,000 free standard parameters (4KB limit), strong encryption, IAM-based access control, and versioning
- **AWS Secrets Manager**: Offers automatic secret rotation, direct integration with RDS/Redshift/DocumentDB, cross-account sharing, but at higher cost

Parameter Store is ideal for most applications where automatic rotation isn't required, while Secrets Manager provides advanced features for critical credentials.

This approach eliminates the need for .env files in production, enhancing security and simplifying deployment.

## Serverless Processing with AWS Lambda

Instead of introducing Airflow, use AWS Lambda to handle individual steps as small, stateless functions triggered by events. This keeps the system simpler, reduces operational overhead, and still supports scalable, event-driven processing without a separate orchestration platform.

Key benefits:
- **Simplified Architecture**: No need to maintain a separate orchestration platform
- **Cost Efficiency**: Pay only for the compute time you consume
- **Auto-scaling**: Automatic scaling based on the number of incoming events
- **Event-driven**: Functions can be triggered by various AWS services (S3, DynamoDB, SQS, etc.)
- **Reduced Operational Overhead**: AWS manages the infrastructure, patching, and scaling

Lambda functions can be chained together using AWS Step Functions for more complex workflows while maintaining the serverless paradigm.
