
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


* [ ] 3.2. Demand Aggregation: Write Trino SQL to aggregate daily customer demand per SKU across all stores.


* [ ] 3.3. Net Demand Computation: Implement the required formula:




* [ ] 3.4. Staging Transformation: Optionally convert raw files to **Parquet** in HDFS for better query performance.



---

## Phase 4: Procurement Rules & Output Generation

**Goal:** Apply business constraints and generate final supplier orders.

* [ ] **4.1. Apply Business Logic**: Refine the demand by:
* Rounding up to the nearest **Case/Pack Size**.


* Ensuring **Minimum Order Quantities (MOQ)** are met.




* [ ] **4.2. Supplier Order Export**: Create `scripts/export_orders.py` to:
* Group orders by supplier.


* Save results as **JSON** files in `/output/supplier_orders/`.




* [ ] 4.3. Exception Reporting: Generate a report for anomalies like missing supplier mappings or demand spikes.



---

## Phase 5: Orchestration & Final Delivery

**Goal:** Automate the pipeline and ensure auditability.

* [ ] 5.1. Master Orchestrator: Build a `main.py` (Python) or Bash script to run all stages in sequence.


* [ ] 5.2. Time-Window Simulation: Ensure the process runs within the 22:00–00:00 batch window.


* [ ] 5.3. Reproducibility Check: Verify the pipeline can re-process any past date by replaying historical raw data.


* [ ] 5.4. Final Presentation: Summarize architecture, data modeling, and team critique for the deliverable.



---

### AI Context for VS Code (GitHub Copilot)

> 
> **Instructions**: Follow the batch-only constraint. Use Python for orchestration and SQL for Presto/Postgres transformations. Ensure all HDFS paths follow the structure defined in the `README.md`.
> 
> 
