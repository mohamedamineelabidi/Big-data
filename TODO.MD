
# 📋 Project Roadmap: Simplified Procurement Data Pipeline

## Phase 1: Infrastructure & Environment Setup

**Goal:** Establish the distributed Big Data environment using Docker.

* [x] **1.1. Docker Orchestration**: Create `docker-compose.yml` to launch:
* 
**PostgreSQL**: For the Master Data (OLTP) layer.


* 
**HDFS**: For the Centralized Data Lake (Namenode/Datanode).


* 
**Presto/Trino**: For distributed SQL queries across HDFS and Postgres.




* [x] **1.2. PostgreSQL Schema**: Write `sql/schema.sql` to create tables for:
* 
`products`: ID, name, pack size.


* 
`suppliers`: ID, name, lead times.


* 
`replenishment_rules`: MOQ (Minimum Order Quantity), and safety stock levels per SKU.




* [x] 1.3. Test Data Engine: Create `scripts/data_gen.py` using the `Faker` library:


* Generate **Daily Orders** (JSON/CSV) for multiple Points of Sale (POS).


* Generate **Warehouse Stock Snapshots** (CSV) with available, reserved, and safety stock.

* [x] 1.4. Infrastructure Verification:
    * [x] Run `docker-compose up -d` and verify service health.
    * [x] Create `sql/init_master_data.sql` and populate PostgreSQL.
    * [x] Write `scripts/test_connection.py` to verify connectivity.





---

## Phase 2: Ingestion & Data Lake Management

**Goal:** Implement batch ingestion into HDFS with a clear organizational hierarchy.

* [x] 2.1. HDFS Directory Hierarchy: Set up folders via script:


* 
`/raw/orders/YYYY-MM-DD/` ✅


* 
`/raw/stock/YYYY-MM-DD/` ✅




* [x] **2.2. Batch Ingestion Script**: Develop `scripts/upload_to_hdfs.ps1` to:
* Move generated files to their respective HDFS date folders. ✅


* Successfully uploaded 5 POS orders + 2 stock files ✅




* [x] 2.3. Data Quality Checks: Implement basic file validation (check structure and mandatory fields). ✅
    * Validated 7 files (5 orders + 2 stock)
    * Checked 134 orders and 100 stock records
    * All validations passed!



---

## Phase 3: Distributed Analytical Processing

**Goal:** Use Trino to aggregate data and calculate replenishment needs.

* [x] 3.1. Trino Connector Config: Connect Trino to HDFS (Hive catalog) and PostgreSQL (Postgres catalog). ✅
    * PostgreSQL catalog configured and tested ✅
    * Hive catalog configured for local file access ✅
    * Successfully querying master data through Trino ✅


* [x] 3.2. Demand Aggregation: Write Trino SQL to aggregate daily customer demand per SKU across all stores. ✅
    * Built Python analyzer to load orders from 15 POS locations ✅
    * Aggregated 15,150 order items across 50 unique SKUs ✅
    * Total demand calculated: 121,840 units ✅


* [x] 3.3. Net Demand Computation: Implement the required formula: ✅
    * **Formula Implemented:** `Net Demand = Total Demand - Available Stock + Safety Stock` ✅
    * Loaded stock from 5 warehouses (250 records) ✅
    * Fetched master data from PostgreSQL via Trino (49 products) ✅
    * Applied business rules: MOQ and Case Size rounding ✅
    * **Result:** 24 SKUs require replenishment, 32,748 units to order ✅


* [ ] 3.4. Staging Transformation: Optionally convert raw files to **Parquet** in HDFS for better query performance.




* [ ] 3.4. Staging Transformation: Optionally convert raw files to **Parquet** in HDFS for better query performance.



---

## Phase 4: Procurement Rules & Output Generation

**Goal:** Apply business constraints and generate final supplier orders.

* [x] **4.1. Apply Business Logic**: Refine the demand by: ✅
    * Rounding up to the nearest **Case/Pack Size** ✅
    * Ensuring **Minimum Order Quantities (MOQ)** are met ✅
    * Successfully grouped 24 SKUs into 5 supplier orders ✅


* [x] **4.2. Supplier Order Export**: Create `scripts/export_orders.py` to: ✅
    * Group orders by supplier ✅
    * Save results as **JSON** files in `/output/supplier_orders/` ✅
    * Generated 5 supplier order files (JSON format) ✅
    * Order priorities assigned: 4 HIGH, 1 MEDIUM ✅


* [x] **4.3. Exception Reporting**: `scripts/generate_exceptions.py` ✅
    * Detects HIGH_DEMAND (>2000 units threshold) ✅
    * Detects LOW_STOCK (stock < 30% of demand) ✅
    * Detects MISSING_SUPPLIER mappings ✅
    * Detects HIGH_VALUE_ORDER and DEMAND_STOCK_GAP ✅
    * **Result:** 30 exceptions (28 HIGH, 2 MEDIUM, 0 CRITICAL) ✅
    * Outputs: JSON report + human-readable text summary ✅


* [x] **4.4. Phase 4 Integration**: `scripts/run_phase4.py` ✅
    * Orchestrates demand → export → exceptions ✅
    * Supports --skip-demand and --date arguments ✅
    * Complete execution in 0.04 seconds ✅



---

## Phase 5: Orchestration & Final Delivery

**Goal:** Automate the pipeline and ensure auditability.

* [x] **5.1. Master Orchestrator**: `scripts/run_pipeline.py` ✅
    * Complete end-to-end pipeline execution ✅
    * Infrastructure validation before execution ✅
    * 4-stage execution: validate → demand → export → exceptions ✅
    * Detailed logging and error handling ✅
    * Execution summary saved to `data/output/pipeline_run_{date}.txt` ✅
    * **Execution time:** 1.01 seconds for full pipeline ✅


* [x] **5.2. Time-Window Simulation**: Airflow DAG configured for 22:00 batch window ✅
    * Airflow container added to docker-compose.yml ✅
    * DAG: `airflow/dags/procurement_dag.py` ✅
    * Schedule: `0 22 * * *` (22:00 daily) ✅
    * Airflow Web UI: `http://localhost:8081` ✅
    * Task dependencies with parallel execution ✅


* [x] **5.3. Reproducibility Check**: Historical replay capability ✅
    * CLI argument `--replay N` for last N days ✅
    * CLI argument `--date YYYY-MM-DD` for specific date ✅
    * Tested with 7 days of historical data ✅


* [ ] **5.4. Final Presentation**: Summarize architecture, data modeling, and team critique for the deliverable.



---

### AI Context for VS Code (GitHub Copilot)

> 
> **Instructions**: Follow the batch-only constraint. Use Python for orchestration and SQL for Presto/Postgres transformations. Ensure all HDFS paths follow the structure defined in the `README.md`.
> 
> 
