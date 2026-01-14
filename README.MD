# 🏭 Procurement Data Pipeline

**Module:** Fondements Big Data (ENSA Al Hoceima)  
**Academic Year:** 2025-2026  
**Branch:** `data_ing-phase3`

---

## 📌 Project Overview

This project implements a **batch-oriented data pipeline** for a retail procurement system. The system collects daily customer orders from 15 Points of Sale (POS) and warehouse stock levels from 5 warehouses, then calculates net demand and automatically generates supplier replenishment orders.

### 🎯 Business Objective

Transform raw sales and inventory data into actionable procurement decisions:

```
┌─────────────────┐     ┌─────────────────┐     ┌─────────────────┐
│   15 Retail     │     │   Data Pipeline │     │   5 Supplier    │
│    Stores       │ ──▶ │   Processing    │ ──▶ │    Orders       │
│  (JSON Orders)  │     │   (Batch ETL)   │     │   (JSON Files)  │
└─────────────────┘     └─────────────────┘     └─────────────────┘
```

### ⚠️ Key Constraints

| Constraint | Description |
|------------|-------------|
| **Batch Only** | No streaming technologies (Kafka/Flink/Spark Streaming) allowed |
| **Distributed** | Uses HDFS for storage and Trino for distributed SQL queries |
| **Separation** | Clearly separated OLTP (PostgreSQL) and Analytical (HDFS) layers |
| **Time Window** | Pipeline runs in 22:00 - 00:00 batch window |

---

## 🏗️ System Architecture

```
┌─────────────────────────────────────────────────────────────────────────────────┐
│                         PROCUREMENT PIPELINE ARCHITECTURE                        │
├─────────────────────────────────────────────────────────────────────────────────┤
│                                                                                  │
│   DATA SOURCES                                                                   │
│   ════════════                                                                   │
│   ┌──────────────┐    ┌──────────────┐    ┌──────────────┐                      │
│   │   15 POS     │    │  5 Warehouses │    │  PostgreSQL  │                      │
│   │   Stores     │    │    Stock      │    │  Master Data │                      │
│   │  (JSON)      │    │   (CSV)       │    │  (Products,  │                      │
│   │              │    │              │    │  Suppliers)  │                      │
│   └──────┬───────┘    └──────┬───────┘    └──────┬───────┘                      │
│          │                   │                   │                               │
│          ▼                   ▼                   │                               │
│   ┌─────────────────────────────────────┐       │                               │
│   │         HDFS DATA LAKE              │       │                               │
│   │  ┌─────────────┐ ┌─────────────┐   │       │                               │
│   │  │ /raw/orders │ │ /raw/stock  │   │       │                               │
│   │  │  105 files  │ │  35 files   │   │       │                               │
│   │  └─────────────┘ └─────────────┘   │       │                               │
│   └──────────────────┬──────────────────┘       │                               │
│                      │                           │                               │
│                      ▼                           ▼                               │
│   ┌─────────────────────────────────────────────────────────────────┐           │
│   │                      TRINO QUERY ENGINE                          │           │
│   │         (Federated SQL across HDFS + PostgreSQL)                │           │
│   │                                                                  │           │
│   │  ┌─────────────────┐              ┌─────────────────┐          │           │
│   │  │  Hive Catalog   │              │ PostgreSQL      │          │           │
│   │  │  (HDFS Data)    │              │ Catalog         │          │           │
│   │  └─────────────────┘              └─────────────────┘          │           │
│   └──────────────────────────┬──────────────────────────────────────┘           │
│                              │                                                   │
│                              ▼                                                   │
│   ┌─────────────────────────────────────────────────────────────────┐           │
│   │                    PYTHON PROCESSING                             │           │
│   │                                                                  │           │
│   │  ┌────────────┐ ┌────────────┐ ┌────────────┐ ┌────────────┐   │           │
│   │  │ Aggregate  │ │ Calculate  │ │ Apply      │ │ Generate   │   │           │
│   │  │ Demand     │ │ Net Demand │ │ Business   │ │ Reports    │   │           │
│   │  │            │ │            │ │ Rules      │ │            │   │           │
│   │  └────────────┘ └────────────┘ └────────────┘ └────────────┘   │           │
│   └──────────────────────────┬──────────────────────────────────────┘           │
│                              │                                                   │
│                              ▼                                                   │
│   ┌─────────────────────────────────────────────────────────────────┐           │
│   │                      OUTPUT LAYER                                │           │
│   │                                                                  │           │
│   │  ┌─────────────┐  ┌─────────────┐  ┌─────────────┐              │           │
│   │  │ Supplier    │  │ Exception   │  │ Pipeline    │              │           │
│   │  │ Orders      │  │ Reports     │  │ Summary     │              │           │
│   │  │ (5 JSON)    │  │ (JSON+TXT)  │  │ (TXT)       │              │           │
│   │  └─────────────┘  └─────────────┘  └─────────────┘              │           │
│   └─────────────────────────────────────────────────────────────────┘           │
│                                                                                  │
│   ORCHESTRATION                                                                  │
│   ═════════════                                                                  │
│   ┌─────────────────────────────────────────────────────────────────┐           │
│   │                    APACHE AIRFLOW                                │           │
│   │         Schedule: Daily at 22:00 | DAG: procurement_daily       │           │
│   └─────────────────────────────────────────────────────────────────┘           │
│                                                                                  │
└─────────────────────────────────────────────────────────────────────────────────┘
```

---

## 🔄 Airflow DAG Workflow

The pipeline orchestration follows this execution flow:

```
                            start
                            │
                            ▼
                    get_processing_date
                            │
                            ▼
                  validate_data_sources
                            │
                            ▼
                    data_quality_check
                            │
                            ▼
                     compute_demand
                            │
                 ┌──────────┴──────────┐
                 ▼                     ▼
           export_orders      generate_exceptions
                 │                     │
                 └──────────┬──────────┘
                            ▼
                    generate_summary
                            │
                            ▼
                           end
```

### Task Breakdown

| Task # | Task Name | Duration | Description |
|--------|-----------|----------|-------------|
| 1 | start_pipeline | <1s | Initialize pipeline execution |
| 2 | get_processing_date | <1s | Determine date to process (execution_date) |
| 3 | validate_data_sources | ~2s | Verify order & stock files exist |
| 4 | data_quality_check | ~5s | Validate JSON/CSV structure & data types |
| 5 | compute_demand | ~15s | Run Trino queries & calculate net demand |
| 6 | export_orders | ~8s | Generate supplier order JSON files |
| 7 | generate_exceptions | ~8s | Detect anomalies & create alerts |
| 8 | generate_summary | ~2s | Compile execution report |
| 9 | end_pipeline | <1s | Mark completion |

**Total Execution Time:** ~40-60 seconds  
**Schedule:** `0 22 * * *` (Daily at 22:00)  
**Retries:** 2 attempts with 5-minute delay  
**Timeout:** 2 hours max

---

## 💻 Technical Stack

| Component | Technology | Version | Purpose |
|-----------|------------|---------|---------|
| **Container Platform** | Docker Compose | 3.x | Infrastructure orchestration |
| **Data Lake** | Apache HDFS | 3.2.1 | Distributed file storage |
| **OLTP Database** | PostgreSQL | 13 | Master data storage |
| **Query Engine** | Trino | 435 | Distributed SQL processing |
| **Orchestration** | Apache Airflow | 2.7.3 | Workflow automation |
| **Programming** | Python | 3.10+ | Data processing scripts |
| **Web UI** | pgAdmin | Latest | Database management |

### Python Dependencies

```
pandas>=2.0.0          # Data manipulation
trino>=0.327.0         # Trino Python client
psycopg2-binary>=2.9.0 # PostgreSQL client
faker>=18.0.0          # Test data generation
```

---

## 📊 Business Logic

### 1. Net Demand Formula

The core replenishment calculation:

```
┌─────────────────────────────────────────────────────────────────┐
│                                                                  │
│   Net Demand = Total Orders - Available Stock + Safety Stock    │
│                                                                  │
└─────────────────────────────────────────────────────────────────┘
```

### Real-World Example - Step by Step:

**Product: Whole Milk 1L (SKU-0002)**

```
═══════════════════════════════════════════════════════════════

📅 Step 1: Aggregate Orders (Last 7 Days)
   Day 1 (Dec 28): 65 units
   Day 2 (Dec 29): 72 units
   Day 3 (Dec 30): 68 units
   Day 4 (Dec 31): 80 units  ← Holiday spike
   Day 5 (Jan 01): 85 units  ← Holiday spike
   Day 6 (Jan 02): 70 units
   Day 7 (Jan 03): 80 units
   ────────────────────────
   TOTAL DEMAND: 520 units

📦 Step 2: Check Current Stock
   Warehouse 1: 50 units
   Warehouse 2: 60 units
   Warehouse 3: 40 units
   ────────────────────────
   TOTAL STOCK: 150 units

📋 Step 3: Load Business Rules (PostgreSQL)
   • Supplier: DairyFresh LLC
   • Lead Time: 3 days
   • Safety Stock: 100 units (always keep as buffer)
   • MOQ (Minimum Order): 50 units
   • Case Size: 24 units/case

🧮 Step 4: Calculate Net Demand
   Basic Need = 520 - 150 = 370 units
   Add Safety Buffer = 370 + 100 = 470 units
   
📦 Step 5: Round to Case Size
   Cases Needed = ⌈470 ÷ 24⌉ = 20 cases
   Final Order = 20 × 24 = 480 units ✓

✅ RESULT: Order 480 units (20 cases) from DairyFresh LLC
   Priority: HIGH (stock < 30% of demand)
   Expected Delivery: Jan 06 (3 days)
   Cost: 480 × $4.50 = $2,160.00
```

**Example Calculation:**

```
Product: Cola 2L (SKU-0012)
├── Total Orders from 15 stores:  2,644 units
├── Available Stock in warehouses:  774 units
├── Safety Stock required:         100 units
└── Net Demand = 2,644 - 774 + 100 = 1,970 units
```

### 2. Procurement Rules

| Rule | Description | Example |
|------|-------------|---------|
| **MOQ** | Minimum Order Quantity per supplier | Min 100 units |
| **Case Size** | Orders rounded UP to nearest case | 6 units/case |
| **Priority** | Based on order volume | >5000 = HIGH |

**Case Size Rounding Example:**

```
Net Demand: 1,970 units
Case Size: 6 units
────────────────────────────
Cases Needed: ⌈1970 ÷ 6⌉ = 329 cases
Order Quantity: 329 × 6 = 1,974 units ✓
```

### 3. Exception Detection

The system automatically detects anomalies:

| Exception Type | Threshold | Severity |
|----------------|-----------|----------|
| HIGH_DEMAND | > 2,000 units | 🔴 HIGH |
| LOW_STOCK | Stock < 30% of demand | 🔴 HIGH |
| CRITICAL_STOCK | Stock < 10% of demand | 🔴 CRITICAL |
| MISSING_SUPPLIER | No supplier assigned | 🔴 HIGH |
| DEMAND_STOCK_GAP | Net demand > 3x stock | 🟡 MEDIUM |

---

## 📂 Project Structure

```
Big-data/
├── README.MD                          # This file
├── TODO.MD                            # Project roadmap & task tracking
│
└── procurement-pipeline/
    │
    ├── docker-compose.yml             # Infrastructure definition
    │
    ├── airflow/
    │   ├── dags/
    │   │   └── procurement_dag.py     # Airflow DAG (22:00 schedule)
    │   └── logs/                      # Airflow execution logs
    │
    ├── config/
    │   ├── trino/
    │   │   └── postgresql.properties  # Trino PostgreSQL catalog
    │   └── trino-config/
    │       └── config.properties      # Trino server config
    │
    ├── data/
    │   ├── raw/
    │   │   ├── orders/                # Daily POS order files (JSON)
    │   │   │   ├── pos_1_2025-12-28.json
    │   │   │   ├── pos_1_2025-12-29.json
    │   │   │   └── ... (105 files: 15 POS × 7 days)
    │   │   │
    │   │   └── stock/                 # Daily warehouse snapshots (CSV)
    │   │       ├── warehouse_1_2025-12-28.csv
    │   │       └── ... (35 files: 5 warehouses × 7 days)
    │   │
    │   └── output/
    │       ├── replenishment_2026-01-03.csv
    │       ├── pipeline_run_2026-01-03.txt
    │       │
    │       ├── supplier_orders/       # JSON orders per supplier
    │       │   ├── BevCo_Distributors_2026-01-03.json
    │       │   ├── DairyFresh_LLC_2026-01-03.json
    │       │   ├── ElectroSupply_Co_2026-01-03.json
    │       │   ├── FreshMart_Wholesale_2026-01-03.json
    │       │   └── TechGear_Plus_2026-01-03.json
    │       │
    │       └── exceptions/            # Quality control reports
    │           ├── exception_report_2026-01-03.json
    │           └── exception_summary_2026-01-03.txt
    │
    ├── scripts/
    │   ├── data_gen.py                # Faker-based test data generator
    │   ├── generate_orders.py         # Order generation script
    │   ├── ingest_hdfs.py             # HDFS data ingestion
    │   ├── upload_to_hdfs.ps1         # PowerShell upload script
    │   ├── validate_data_quality.py   # Data quality validation
    │   ├── compute_demand.py          # Main demand calculation
    │   ├── export_orders.py           # Supplier JSON generation
    │   ├── generate_exceptions.py     # Anomaly detection & reporting
    │   ├── run_phase4.py              # Phase 4 orchestration
    │   ├── run_pipeline.py            # Master pipeline orchestrator
    │   ├── test_connection.py         # Infrastructure connectivity test
    │   ├── test_trino_catalog.py      # Trino catalog validation
    │   └── test_system.py             # Full system integration test
    │
    └── sql/
        └── postgres/
            ├── schema.sql             # Database schema
            └── init_master_data.sql   # Seed data (products, suppliers)
```

---

## 🚀 Quick Start Guide

### Prerequisites

- Docker Desktop installed and running
- Python 3.10+ with pip
- Git

### 1. Clone Repository

```bash
git clone https://github.com/mohamedamineelabidi/Big-data.git
cd Big-data/procurement-pipeline
```

### 2. Start Infrastructure

```bash
docker-compose up -d
```

### 3. Install Python Dependencies

```bash
pip install pandas trino psycopg2-binary faker
```

### 4. Verify Services

```bash
python scripts/test_connection.py
```

### 5. Run Full Pipeline

```bash
# Run for specific date
python scripts/run_pipeline.py --date 2026-01-03

# Or run system test
python scripts/test_system.py
```

### 6. Access Web Interfaces

| Service | URL | Credentials |
|---------|-----|-------------|
| **Airflow UI** | http://localhost:8081 | admin / admin |
| **Trino UI** | http://localhost:8080 | admin (no password) |
| **pgAdmin** | http://localhost:5050 | admin@admin.com / admin |
| **HDFS UI** | http://localhost:9870 | No auth required |

---

## 📜 Scripts Documentation

### Core Pipeline Scripts

| Script | Purpose | Usage |
|--------|---------|-------|
| `run_pipeline.py` | Master orchestrator | `python scripts/run_pipeline.py --date 2026-01-03` |
| `compute_demand.py` | Demand calculation | `python scripts/compute_demand.py` |
| `export_orders.py` | Supplier JSON export | `python scripts/export_orders.py --date 2026-01-03` |
| `generate_exceptions.py` | Exception reporting | `python scripts/generate_exceptions.py --date 2026-01-03` |

### Utility Scripts

| Script | Purpose | Usage |
|--------|---------|-------|
| `test_system.py` | Integration test (37 tests) | `python scripts/test_system.py` |
| `test_connection.py` | Service connectivity | `python scripts/test_connection.py` |
| `data_gen.py` | Generate test data | `python scripts/data_gen.py --days 7` |

### CLI Options

```bash
# Master Pipeline
python scripts/run_pipeline.py --date 2026-01-03     # Specific date
python scripts/run_pipeline.py --replay 7            # Last 7 days
python scripts/run_pipeline.py --validate-only       # Validation only
python scripts/run_pipeline.py --skip-validation     # Skip validation

# Export Orders
python scripts/export_orders.py --date 2026-01-03 --output data
```

---

## 📦 Data Models

### Input: Order JSON Schema

```json
{
  "pos_id": "POS-001",
  "date": "2026-01-03",
  "items": [
    {
      "sku": "SKU-0012",
      "quantity": 24,
      "unit_price": 2.99
    }
  ]
}
```

### Input: Stock CSV Schema

| Column | Type | Description |
|--------|------|-------------|
| warehouse_id | string | Warehouse identifier |
| sku | string | Product SKU |
| available_stock | integer | Units available |
| reserved_stock | integer | Units reserved |
| date | date | Snapshot date |

### Output: Supplier Order JSON

```json
{
  "order_id": "ORD-20260103-006-001",
  "supplier_id": "SUP-006",
  "supplier_name": "BevCo Distributors",
  "order_date": "2026-01-03",
  "requested_delivery_date": "2026-01-05",
  "status": "PENDING",
  "priority": "HIGH",
  "items": [
    {
      "line_number": 1,
      "sku": "SKU-0001",
      "product_name": "Organic Apple Juice",
      "category": "Beverages",
      "quantity_ordered": 1344,
      "cases": 56,
      "case_size": 24,
      "net_demand": 1333.0
    }
  ],
  "summary": {
    "total_line_items": 7,
    "total_units": 10494,
    "total_cases": 874
  },
  "metadata": {
    "generated_by": "procurement_pipeline",
    "generation_timestamp": "2026-01-04T03:52:28",
    "pipeline_version": "1.0"
  }
}
```

### Output: Exception Report

```json
{
  "report_date": "2026-01-03",
  "generated_at": "2026-01-04T02:58:07",
  "summary": {
    "total_skus_analyzed": 24,
    "total_exceptions": 30,
    "by_severity": {
      "CRITICAL": 0,
      "HIGH": 28,
      "MEDIUM": 2
    }
  },
  "exceptions": [
    {
      "type": "HIGH_DEMAND",
      "severity": "HIGH",
      "sku": "SKU-0022",
      "product_name": "Bagels 6pk",
      "metric_value": 2852,
      "threshold": 2000,
      "recommendation": "Consider expedited supplier contact"
    }
  ]
}
```

---

## 📈 Pipeline Results

### Latest Execution (2026-01-03)

```
╔════════════════════════════════════════════════════════════════════╗
║                    PIPELINE EXECUTION SUMMARY                       ║
╠════════════════════════════════════════════════════════════════════╣
║  📅 Processing Date:     2026-01-03                                 ║
║  ⏱️  Execution Time:      1.06 seconds                              ║
║  📊 Status:              ✅ SUCCESS                                 ║
╠════════════════════════════════════════════════════════════════════╣
║  📦 INPUT DATA                                                      ║
║     • Order Files:        105 JSON files                            ║
║     • Stock Files:        35 CSV files                              ║
║     • Order Items:        15,150 items                              ║
║     • Stock Records:      250 records                               ║
║     • POS Locations:      15 stores                                 ║
║     • Warehouses:         5 locations                               ║
╠════════════════════════════════════════════════════════════════════╣
║  📊 PROCESSING                                                      ║
║     • Total Demand:       121,840 units                             ║
║     • Total Stock:        58,401 units                              ║
║     • Unique SKUs:        50 products                               ║
║     • Products (Master):  49 in database                            ║
╠════════════════════════════════════════════════════════════════════╣
║  📤 OUTPUT DATA                                                     ║
║     • SKUs to Reorder:    24 products                               ║
║     • Total Units:        32,748 units                              ║
║     • Suppliers:          5 orders generated                        ║
║     • Exceptions:         30 (0 critical)                           ║
╚════════════════════════════════════════════════════════════════════╝
```

### Supplier Orders Generated

| Supplier | SKUs | Units | Cases | Priority |
|----------|------|-------|-------|----------|
| BevCo Distributors | 7 | 10,494 | 874 | 🔴 HIGH |
| FreshMart Wholesale | 5 | 6,744 | 562 | 🔴 HIGH |
| DairyFresh LLC | 5 | 6,594 | 549 | 🔴 HIGH |
| ElectroSupply Co | 4 | 5,461 | 455 | 🔴 HIGH |
| TechGear Plus | 3 | 3,455 | 288 | 🟡 MEDIUM |
| **TOTAL** | **24** | **32,748** | **2,728** | |

### Top 10 Items to Reorder

| Rank | SKU | Product | Category | Order Qty | Supplier |
|------|-----|---------|----------|-----------|----------|
| 1 | SKU-0031 | USB Cable 2m | Electronics | 2,200 | ElectroSupply Co |
| 2 | SKU-0021 | Croissant Pack | Bakery | 2,040 | FreshMart Wholesale |
| 3 | SKU-0012 | Cola 2L | Beverages | 1,974 | BevCo Distributors |
| 4 | SKU-0005 | Almond Milk | Beverages | 1,728 | BevCo Distributors |
| 5 | SKU-0045 | Cream Cheese | Dairy | 1,692 | DairyFresh LLC |
| 6 | SKU-0024 | Baguette | Bakery | 1,540 | FreshMart Wholesale |
| 7 | SKU-0043 | Cheddar Cheese | Dairy | 1,510 | DairyFresh LLC |
| 8 | SKU-0003 | Wireless Mouse | Electronics | 1,440 | ElectroSupply Co |
| 9 | SKU-0022 | Bagels 6pk | Bakery | 1,424 | FreshMart Wholesale |
| 10 | SKU-0011 | Orange Juice 1L | Beverages | 1,416 | BevCo Distributors |

---

## ✅ Project Phases Completed

### Phase 1: Infrastructure Setup ✅

- [x] Docker Compose with PostgreSQL, HDFS, Trino, pgAdmin, Airflow
- [x] PostgreSQL schema with products, suppliers, replenishment_rules
- [x] Master data: 49 products, 10 suppliers, 49 rules
- [x] All services verified and running

### Phase 2: Data Ingestion ✅

- [x] HDFS directory hierarchy (`/raw/orders/`, `/raw/stock/`)
- [x] 7 days of test data generated (Dec 28 - Jan 3)
- [x] 105 order files (15 POS × 7 days)
- [x] 35 stock files (5 warehouses × 7 days)
- [x] Data quality validation implemented

### Phase 3: Analytical Processing ✅

- [x] Trino connectors: PostgreSQL catalog, Hive catalog
- [x] Demand aggregation: 121,840 units across 50 SKUs
- [x] Net demand computation with safety stock
- [x] Business rules: MOQ and case size rounding
- [x] Result: 24 SKUs require 32,748 units

### Phase 4: Output Generation ✅

- [x] Supplier order export (5 JSON files)
- [x] Exception reporting (30 exceptions detected)
- [x] Order priorities assigned (4 HIGH, 1 MEDIUM)
- [x] Human-readable reports generated

### Phase 5: Orchestration ✅

- [x] Master orchestrator (`run_pipeline.py`)
- [x] Airflow DAG scheduled at 22:00 daily
- [x] Historical replay capability (`--replay N`)
- [x] System integration tests (37 tests, 100% pass)

---

## 🧪 System Tests

Run the comprehensive test suite:

```bash
python scripts/test_system.py
```

### Test Categories

| Category | Tests | Description |
|----------|-------|-------------|
| Docker Services | 5 | Container health checks |
| PostgreSQL | 4 | Connection & data validation |
| Trino | 2 | Query engine verification |
| Data Files | 3 | File existence & counts |
| Demand Module | 4 | Data loading & processing |
| Export Module | 4 | JSON generation |
| Exception Module | 4 | Report generation |
| Orchestrator | 3 | Infrastructure validation |
| Airflow DAG | 4 | DAG syntax & configuration |
| End-to-End | 4 | Output file verification |
| **TOTAL** | **37** | **All passing ✅** |

---

## 🐳 Docker Services

```yaml
services:
  postgres:        # Master data storage
  namenode:        # HDFS name node
  datanode:        # HDFS data node
  trino:           # Query engine
  pgadmin:         # Database UI
  airflow:         # Workflow orchestration
```

### Service Ports

| Service | Internal Port | External Port |
|---------|---------------|---------------|
| PostgreSQL | 5432 | 5432 |
| HDFS Namenode | 9000, 9870 | 9000, 9870 |
| Trino | 8080 | 8080 |
| Airflow | 8080 | 8081 |
| pgAdmin | 80 | 5050 |

---

## 🔧 Troubleshooting

### Common Issues

**1. Airflow not starting**
```bash
# Create airflow database
docker exec procurement_postgres psql -U admin -d procurement_db -c "CREATE DATABASE airflow_db;"
docker restart procurement_airflow
```

**2. Trino connection refused**
```bash
# Check Trino logs
docker logs procurement_trino
# Verify config
cat config/trino-config/config.properties
```

**3. HDFS not accessible**
```bash
# Check HDFS health
docker logs procurement_namenode
# Verify safe mode
docker exec procurement_namenode hdfs dfsadmin -safemode get
```

**4. PostgreSQL connection issues**
```bash
# Test connection
docker exec procurement_postgres psql -U admin -d procurement_db -c "SELECT 1"
```

---

## 🤖 AI Context (GitHub Copilot)

When working with this project, Copilot should:

- **Variable Naming:** Use clear business terms (`sku`, `moq`, `safety_stock`, `net_demand`)
- **Context:** This is a Data Engineering / ETL project, not a web application
- **Constraint:** No streaming - batch processing only
- **Query Engine:** Use `trino` Python client, not `prestodb`
- **Data Format:** JSON for orders, CSV for stock, PostgreSQL for master data

---

## 👥 Team

**Project:** Big Data Fundamentals (Fondements Big Data)  
**Institution:** ENSA Al Hoceima  
**Academic Year:** 2025-2026

---

## 📄 License

Academic project - ENSA Al Hoceima © 2025-2026
