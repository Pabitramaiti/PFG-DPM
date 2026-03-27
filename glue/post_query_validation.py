import sys
import boto3
import json
import re
import os
from sqlalchemy import create_engine, text
import splunk

def get_run_id():
    """Get a unique run identifier based on AWS account ID"""
    account_id = boto3.client("sts").get_caller_identity()["Account"]
    return "arn:dpm:glue:br_icsdev_postquery_validation:" + account_id

def log_message(status, message):
    """Log messages to Splunk with status and message"""
    splunk.log_message({'FileName':inputFileName, 'Status': status, 'Message': message}, get_run_id())

def get_secret_values(secret_name):
    client = boto3.client("secretsmanager")
    try:
        response = client.get_secret_value(SecretId=secret_name)
        return json.loads(response['SecretString'])
    except Exception as e:
        raise Exception(f"Error retrieving secret values: {e}")

def load_db_engine(database_name, database_ssm_key, aws_region):
    log_message('info', f"Loading database engine for database: {database_name}")
    
    try:
        # Create an SSM client
        ssm_client = boto3.client('ssm', region_name=aws_region)
        # Retrieve a parameter from the Parameter Store
        response = ssm_client.get_parameter(Name=database_ssm_key, WithDecryption=True)
        # Extract the parameter value (secret name)
        secret_name = response['Parameter']['Value']
        # Get database credentials from Secrets Manager
        host = get_secret_values(secret_name)
        # Create SQLAlchemy engine with connection timeout settings
        DATABASE_URL = f"postgresql://{host['username']}:{host['password']}@{host['host']}:{host['port']}/{database_name}"
        
        # Add connection timeout and pool settings
        engine = create_engine(
            DATABASE_URL, 
            future=True, 
            echo=False,
            pool_pre_ping=True,
            pool_recycle=300,
            connect_args={
                "connect_timeout": 30,
                "application_name": "validation_script"
            }
        )
        
        # Test the connection
        with engine.connect() as test_conn:
            result = test_conn.execute(text("SELECT 1"))
            if result.fetchone()[0] == 1:
                log_message('success', "Database connection established successfully")
        
        return engine
        
    except Exception as e:
        log_message('failed', f"Database connection failed: {str(e)}")
        raise Exception(f"Failed to create database connection: {e}")

def load_config_from_s3(bucket_name, config_key):
    """Load validation configuration from S3"""
    try:
        log_message('info', f"Loading validation config from s3://{bucket_name}/{config_key}")
        s3 = boto3.client('s3')
        config_obj = s3.get_object(Bucket=bucket_name, Key=config_key)
        log_message('success', "Validation configuration loaded successfully from S3")
        return json.loads(config_obj['Body'].read().decode())
    except Exception as e:
        log_message('failed', f"Failed to load config from S3: {str(e)}")
        raise

def run_validations(engine, validation_config, database_schema):
    """Execute all validation rules"""
    validation_failures = []  # Track all validation failures
    log_message('info', "Starting validation execution")
    
    with engine.connect() as connection:
        # Execute merge key validations
        if 'merge_key_validations' in validation_config:
            log_message('info', f"Running {len(validation_config['merge_key_validations'])} merge key validations")
            total_merge_validations = len(validation_config['merge_key_validations'])
            for i, rule in enumerate(validation_config['merge_key_validations'], 1):
                description = rule['description']
                query = rule['query']
                failure_message = rule['failure_message']
                log_message('info', f"Executing merge key validation [{i}/{total_merge_validations}]: {description}")
                
                try:
                    result = connection.execute(text(query))
                    
                    # Handle different query types
                    if "SELECT COUNT(*)" in query:
                        count = result.fetchone()[0]
                        if count > 0:
                            log_message('failed', f"Merge key validation failed: {description} - Found {count} issues")
                            validation_failures.append({
                                'type': 'Merge Key Validation',
                                'description': description,
                                'issue_count': count,
                                'details': f"Found {count} issues",
                                'failure_message': failure_message
                            })
                        else:
                            log_message('success', f"Merge key validation passed: {description}")
                    else:
                        rows = result.fetchall()
                        if rows:
                            log_message('failed', f"Merge key validation failed: {description} - Found {len(rows)} issues")
                            problematic_values = []
                            for idx, row in enumerate(rows, 1):
                                problematic_values.append(str(row[0]))
                            
                            validation_failures.append({
                                'type': 'Merge Key Validation',
                                'description': description,
                                'issue_count': len(rows),
                                'details': f"Problematic values: {', '.join(problematic_values[:10])}{'...' if len(problematic_values) > 10 else ''}",
                                'failure_message': failure_message
                            })
                        else:
                            log_message('success', f"Merge key validation passed: {description}")
                            
                except Exception as e:
                    log_message('failed', f"Merge key validation error: {description} - {str(e)}")
                    validation_failures.append({
                        'type': 'Merge Key Validation',
                        'description': description,
                        'issue_count': 'ERROR',
                        'details': f"Query execution error: {str(e)}",
                        'failure_message': failure_message
                    })
        else:
            log_message('info', "No merge key validations found in config")

        # Execute duplicate rundate validation
        if 'duplicate_rundate_validation' in validation_config:
            dup_rule = validation_config['duplicate_rundate_validation']
            query = dup_rule['query']
            description = dup_rule['description']
            failure_message = dup_rule['failure_message']
            
            try:
                result = connection.execute(text(query))
                count = result.fetchone()[0]
                
                if count > 0:
                    log_message('failed', f"Duplicate rundate validation failed: {description} - Found {count} duplicate run dates")
                    validation_failures.append({
                        'type': 'Duplicate Rundate Validation',
                        'description': description,
                        'issue_count': count,
                        'details': f"Found {count} duplicate run dates in last 60 days",
                        'failure_message': failure_message
                    })
                else:
                    log_message('success', f"Duplicate rundate validation passed: {description}")
                    
            except Exception as e:
                log_message('failed', f"Duplicate rundate validation error: {description} - {str(e)}")
                validation_failures.append({
                    'type': 'Duplicate Rundate Validation',
                    'description': description,
                    'issue_count': 'ERROR',
                    'details': f"Query execution error: {str(e)}",
                    'failure_message': failure_message
                })
        else:
            log_message('info', "No duplicate rundate validation found in config")

        # Execute SPRO validations
        if 'spro_validations' in validation_config:
            log_message('info', f"Running {len(validation_config['spro_validations'])} SPRO validations")
            for i, rule in enumerate(validation_config['spro_validations'], 1):
                description = rule['description']
                query = rule['query']
                failure_message = rule['failure_message']
                
                log_message('info', f"Executing SPRO validation [{i}/{len(validation_config['spro_validations'])}]: {description}")
                
                try:
                    result = connection.execute(text(query))
                    rows = result.fetchall()
                    if rows:
                        log_message('failed', f"SPRO validation failed: {description} - Found {len(rows)} issues")
                        problematic_values = []
                        for idx, row in enumerate(rows, 1):
                            problematic_values.append(str(row[0]))
                        
                        validation_failures.append({
                            'type': 'SPRO Validation',
                            'description': description,
                            'issue_count': len(rows),
                            'details': f"Problematic values: {', '.join(problematic_values[:10])}{'...' if len(problematic_values) > 10 else ''}",
                            'failure_message': failure_message
                        })
                    else:
                        log_message('success', f"SPRO validation passed: {description}")
                        
                except Exception as e:
                    log_message('failed', f"SPRO validation error: {description} - {str(e)}")
                    validation_failures.append({
                        'type': 'SPRO Validation',
                        'description': description,
                        'issue_count': 'ERROR',
                        'details': f"Query execution error: {str(e)}",
                        'failure_message': failure_message
                    })
        else:
            log_message('info', "No SPRO validations found in config")

        # Execute record count validations
        if 'record_count_validations' in validation_config:
            log_message('info', f"Running {len(validation_config['record_count_validations'])} record count validations")
            for i, rule in enumerate(validation_config['record_count_validations'], 1):
                description = rule['description']
                query = rule['query']
                failure_message = rule['failure_message']
                log_message('info', f"Executing record count validation [{i}/{len(validation_config['record_count_validations'])}]: {description}")
                
                try:
                    result = connection.execute(text(query))
                    rows = result.fetchall()
                    
                    if rows:
                        log_message('failed', f"Record count validation failed: {description} - Found {len(rows)} mismatches")
                        mismatched_files = []
                        for idx, row in enumerate(rows, 1):
                            mismatched_files.append(str(row[0]))
                        
                        validation_failures.append({
                            'type': 'Record Count Validation',
                            'description': description,
                            'issue_count': len(rows),
                            'details': f"Mismatched files: {', '.join(mismatched_files[:5])}{'...' if len(mismatched_files) > 5 else ''}",
                            'failure_message': failure_message
                        })
                    else:
                        log_message('success', f"Record count validation passed: {description}")
                        
                except Exception as e:
                    log_message('failed', f"Record count validation error: {description} - {str(e)}")
                    validation_failures.append({
                        'type': 'Record Count Validation',
                        'description': description,
                        'issue_count': 'ERROR',
                        'details': f"Query execution error: {str(e)}",
                        'failure_message': failure_message
                    })
        else:
            log_message('info', "No record count validations found in config")

    log_message('info', f"Validation execution completed. Total failures: {len(validation_failures)}")
    return validation_failures

def generate_failure_report(validation_failures, bucket_name, report_key):
    """Generate comprehensive failure report and upload to S3"""
    if not validation_failures:
        return None
    if not bucket_name or not report_key:
        log_message('warning', "Skipping failure report upload because bucket or report key is unset")
        return None
    log_message('info', f"Generating failure report for {len(validation_failures)} failures")
    # Create a lightweight report summary first
    report_summary = f"VALIDATION FAILURES: {len(validation_failures)} total"
    for i, failure in enumerate(validation_failures, 1):
        report_summary += f"\n{i}. {failure['type']}: {failure['description']} - {failure['issue_count']} issues"
    
    # Log the summary to Splunk (fast)
    log_message('failed', f"Validation failure summary: {report_summary}")
    
    # Upload detailed report to S3 instead of local file
    try:
        # Use a more efficient approach - build report content in memory
        report_lines = [
            "VALIDATION FAILURE REPORT",
            f"Total Failed Validations: {len(validation_failures)}"
        ]
        
        for i, failure in enumerate(validation_failures, 1):
            report_lines.extend([
                f"\n{i}. {failure['type']}",
                f"   Description: {failure['description']}",
                f"   Issue Count: {failure['issue_count']}",
                f"   Details: {failure['details']}",
                f"   Message: {failure['failure_message']}"
            ])
        
        # Upload to S3 in single operation
        report_content = '\n'.join(report_lines)
        s3_client = boto3.client('s3')
        s3_client.put_object(
            Bucket=bucket_name, 
            Key=report_key, 
            Body=report_content,
            ContentType='text/plain'
        )
        
        log_message('success', f"Detailed failure report uploaded to s3://{bucket_name}/{report_key}")
        
    except Exception as e:
        log_message('warning', f"Could not upload detailed failure report to S3: {str(e)}")
        raise Exception(f"Failed to upload detailed failure report to S3: {str(e)}")

# Cleanup Logic
def cleanup_tables(connection, schema, tables, exclude_patterns=None):
    exclude_patterns = exclude_patterns or []
    tables_to_drop = [t for t in tables if not any(pat in t for pat in exclude_patterns)]

    with connection.begin():  # <-- ensures commit
        for table in tables_to_drop:
            sql = f'DROP TABLE IF EXISTS "{schema}"."{table}" CASCADE;'
            try:
                print(f"Executing cleanup: {sql}")
                connection.execute(text(sql))
            except Exception as e:
                print(f"Failed to drop table {schema}.{table}: {str(e)}")

def set_job_params_as_env_vars():
    for i in range(1, len(sys.argv), 2):
        if sys.argv[i].startswith('--'):
            key = sys.argv[i][2:]
            value = sys.argv[i + 1]
            os.environ[key] = value

def main():
    set_job_params_as_env_vars()
    """Main function with configurable parameters"""
    global inputFileName
    inputFileName = os.getenv("inputFileName")
    database_name = os.getenv("database_name")
    database_ssm_key = os.getenv("database_ssm_key")
    aws_region = os.getenv("aws_region", "us-east-1")
    database_schema = os.getenv("database_schema")
    bucket_name = os.getenv("bucket_name")
    report_key = os.getenv("report_key")
    config_key = os.getenv("config_key")
    
    log_message('info', "Post validation script started")
    log_message('info', f"Parameters - Database: {database_name}, Schema: {database_schema}")

    try:
        # Load configuration from S3
        config = load_config_from_s3(bucket_name, config_key)
        validation_config = config['validation_config']
        cleanup_config = config.get("cleanup_config", {})
        # Connect to PostgreSQL using SQLAlchemy
        engine = load_db_engine(database_name, database_ssm_key, aws_region)
        # Run all validations and collect failures
        validation_failures = run_validations(engine, validation_config, database_schema)

        # Generate failure report if there are any failures
        if validation_failures:
            generate_failure_report(validation_failures, bucket_name, report_key)
            if cleanup_config.get("enabled") and cleanup_config.get("cleanup_on_any_failure"):
                cleanup_schema = cleanup_config.get("schema", "public")
                cleanup_tables_list = cleanup_config.get("tables", [])
                exclude_patterns = cleanup_config.get("exclude_patterns", [])

                if cleanup_tables_list:
                    print("Validation failures detected. Cleaning up tables...")
                    # Use SQLAlchemy connection, not Spark
                    with engine.connect() as connection:
                        cleanup_tables(connection, cleanup_schema, cleanup_tables_list, exclude_patterns)
                    print("Cleanup completed.")
            raise ValueError(f"Validation failed with {len(validation_failures)} issues. Check S3 report: s3://{bucket_name}/{report_key}")
        else:
            log_message('success', "All validations passed. No failures detected.")
    except ValueError:
        # Re-raise validation failures
        raise
    except Exception as e:
        log_message('failed', f"Critical error in validation script: {str(e)}")
        raise Exception(f"Critical error during validation: {str(e)}")
        
inputFileName=" "
if __name__ == "__main__":
    main()