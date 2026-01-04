-- Create Airflow database for workflow orchestration
-- This script runs after the main schema.sql

-- Create airflow database if it doesn't exist
SELECT 'CREATE DATABASE airflow_db'
WHERE NOT EXISTS (SELECT FROM pg_database WHERE datname = 'airflow_db')\gexec
