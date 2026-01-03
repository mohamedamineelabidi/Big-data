This `README.md` is designed to serve as the "Source of Truth" for your project. By keeping this file in your root directory, **GitHub Copilot** will be able to index it and provide much more accurate code suggestions because it will understand your specific business logic (like the net demand formula) and your technical constraints (like the no-streaming rule).

---

# Simplified Data Pipeline for a Procurement System

**Module:** Fondements Big Data (ENSA Al Hoceima) **Academic Year:** 2025-2026 

## 📌 Project Overview

The objective of this project is to implement a simplified, batch-oriented data pipeline for a retail procurement system. The system collects daily customer orders and warehouse stock levels to calculate net demand and automatically generate supplier replenishment orders.

### Key Constraints:

* 
**Batch Processing Only:** No streaming technologies (Kafka/Flink/Spark Streaming) are allowed.


* 
**Distributed Architecture:** Uses HDFS for storage and Presto/Trino for distributed SQL queries.


* 
**Separation of Concerns:** Clearly separated OLTP (PostgreSQL) and Analytical (HDFS) layers.



---

## 🏗️ Architecture

The system follows a modular distributed architecture as shown in the diagram:

1. 
**Landing Zone (HDFS):** Ingests raw JSON/CSV files from Points of Sale (POS) and Warehouses.


2. 
**Master Data (PostgreSQL):** Stores product catalogs, supplier info, and replenishment rules.


3. 
**Compute Layer (Presto):** Joins HDFS files with SQL tables to perform heavy aggregations.


4. 
**Output Layer:** Generates standardized supplier order files (JSON).



---

## 💻 Technical Stack

* 
**Storage:** HDFS (Distributed File System).


* 
**Database (OLTP):** PostgreSQL.


* 
**Query Engine:** Presto / Trino.


* 
**Language:** Python (for data generation, orchestration, and scripts).


* 
**Orchestration:** Bash/Python scripts (simulating a 22:00 - 00:00 batch window).



---

## 📊 Business Logic & Data Modeling

### 1. Net Demand Formula

The core of the replenishment logic uses the following formula:


### 2. Procurement Rules

* 
**MOQ:** Minimum Order Quantity must be respected for every supplier order.


* 
**Rounding:** Order quantities must be rounded to the nearest **Case Size** or **Pack Size**.



---

## 📂 Project Structure

```text
.
├── config/              # Configuration files (DB connections, HDFS paths)
├── data/
│   ├── raw/             # Local simulation of POS/Warehouse files
│   └── master/          # SQL scripts for PostgreSQL initialization
├── scripts/
[cite_start]│   ├── data_gen.py      # Faker script to generate mock data [cite: 203]
[cite_start]│   ├── ingest_hdfs.py   # Moves data from local to HDFS [cite: 96]
[cite_start]│   ├── compute_demand.py# Triggers Presto queries [cite: 88]
[cite_start]│   └── generate_orders.py# Exports final JSON for suppliers [cite: 55]
├── sql/
[cite_start]│   ├── schema.sql       # PostgreSQL table definitions [cite: 112]
[cite_start]│   └── analysis.sql     # Presto SQL for demand calculation [cite: 88]
[cite_start]├── logs/                # Exception reports and daily logs [cite: 60, 110]
├── docker-compose.yml   # Infrastructure setup (Postgres, HDFS, Presto)
└── README.md            # You are here

```

---

## 🚀 To-Do List (Roadmap)

1. [x] **Phase 1:** Setup Docker environment and generate test data with `Faker`.


2. [ ] **Phase 2:** Implement HDFS ingestion with the `/raw/YYYY-MM-DD/` hierarchy.


3. [ ] **Phase 3:** Create PostgreSQL master tables and link them to Presto.


4. [ ] **Phase 4:** Build the SQL logic for net demand and supplier rules.


5. [ ] **Phase 5:** Automate the end-to-end pipeline and generate Exception Reports.



---

## 🤖 Context for AI (GitHub Copilot)

* **Variable Naming:** Use clear business terms (e.g., `sku_id`, `moq`, `safety_stock`).
* **Context:** This is a **Data Engineering** project. When suggesting code, prioritize efficient data handling and distributed computing principles.
* **Constraint:** Avoid any library that implies real-time streaming. Stick to `os`, `psycopg2`, `presto-python-client`, and `pandas` for local processing if needed.

---

### Next Step for You

Copy the content above into a file named `README.md` in your VS Code project folder. Once you do that, **would you like me to generate the `data_gen.py` script using the Faker library so you can start creating your test data?**