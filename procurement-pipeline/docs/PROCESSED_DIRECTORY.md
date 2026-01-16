# HDFS /processed Directory - Complete Guide

## What is `/processed`?

The `/processed` directory in HDFS stores **raw input files that have been successfully processed** by the pipeline. This creates a clear separation between:
- **New/unprocessed data** → `/raw/`
- **Successfully processed data** → `/processed/`

## Why Use `/processed`?

### 1. **Prevent Duplicate Processing**
- Raw files moved to `/processed` won't be reprocessed on next pipeline run
- Ensures data is processed exactly once

### 2. **Audit Trail & Compliance**
- Maintains historical record of all processed data
- Required for regulatory compliance (SOX, GDPR, etc.)
- Answers: "Which orders were processed on 2026-01-14?"

### 3. **Data Lineage Tracking**
```
Input Data → Processing → Output → Archive
  /raw/    →  Pipeline  → /output/ → /processed/
```

### 4. **Rollback & Reprocessing**
- If pipeline output is incorrect, you can:
  1. Delete incorrect outputs
  2. Move data from `/processed/` back to `/raw/`
  3. Reprocess with corrected logic

### 5. **Storage Management**
- Clean separation makes it easy to:
  - Delete old `/processed/` data after retention period
  - Identify unprocessed files in `/raw/`
  - Monitor data flow

## Directory Structure

```
/processed/
├── orders/
│   ├── 2026-01-03/
│   │   ├── pos_001_2026-01-03.json  (from POS terminal 1)
│   │   ├── pos_002_2026-01-03.json  (from POS terminal 2)
│   │   ├── pos_003_2026-01-03.json  (from POS terminal 3)
│   │   ├── ... (15 files total - one per store)
│   │   └── pos_015_2026-01-03.json
│   │
│   ├── 2026-01-14/
│   │   ├── pos_001_2026-01-14.json
│   │   ├── ... (15 files)
│   │   └── pos_015_2026-01-14.json
│   │
│   └── 2026-01-15/
│       └── (today's processed files)
│
└── stock/
    ├── 2026-01-03/
    │   ├── wh_1_2026-01-03.csv  (Warehouse 1 inventory)
    │   ├── wh_2_2026-01-03.csv  (Warehouse 2 inventory)
    │   ├── wh_3_2026-01-03.csv  (Warehouse 3 inventory)
    │   ├── wh_4_2026-01-03.csv  (Warehouse 4 inventory)
    │   └── wh_5_2026-01-03.csv  (Warehouse 5 inventory)
    │
    ├── 2026-01-14/
    │   ├── wh_1_2026-01-14.csv
    │   ├── ... (5 files)
    │   └── wh_5_2026-01-14.csv
    │
    └── 2026-01-15/
        └── (today's processed stock files)
```

## What Files Go Into `/processed`?

### Order Files (JSON)
**Source:** Point-of-Sale (POS) terminals at 15 retail stores

**Format:**
```json
{
  "pos_id": "POS-001",
  "date": "2026-01-14",
  "items": [
    {"sku": "SKU-0001", "quantity": 12, "unit_price": 2.99},
    {"sku": "SKU-0014", "quantity": 45, "unit_price": 1.49}
  ]
}
```

**Volume:** 15 files/day (one per store)  
**Destination:** `/processed/orders/YYYY-MM-DD/`

### Stock Files (CSV)
**Source:** Warehouse management systems at 5 warehouses

**Format:**
```csv
warehouse_id,sku,quantity,stock_date
WH-001,SKU-0001,1250,2026-01-14
WH-001,SKU-0002,890,2026-01-14
```

**Volume:** 5 files/day (one per warehouse)  
**Destination:** `/processed/stock/YYYY-MM-DD/`

## How to Archive Files

### Method 1: Automated Script

```powershell
# After successful pipeline execution
.\scripts\archive_processed_files.ps1 -Date "2026-01-14"
```

**What it does:**
1. Creates `/processed/orders/2026-01-14/` and `/processed/stock/2026-01-14/`
2. Moves all files from `/raw/orders/2026-01-14/` → `/processed/orders/2026-01-14/`
3. Moves all files from `/raw/stock/2026-01-14/` → `/processed/stock/2026-01-14/`
4. Verifies move was successful
5. Reports count of archived files

### Method 2: Manual HDFS Commands

```bash
# Create archive directories
docker exec procurement_namenode hdfs dfs -mkdir -p /processed/orders/2026-01-14
docker exec procurement_namenode hdfs dfs -mkdir -p /processed/stock/2026-01-14

# Move order files
docker exec procurement_namenode hdfs dfs -mv /raw/orders/2026-01-14/* /processed/orders/2026-01-14/

# Move stock files
docker exec procurement_namenode hdfs dfs -mv /raw/stock/2026-01-14/* /processed/stock/2026-01-14/

# Verify
docker exec procurement_namenode hdfs dfs -ls /processed/orders/2026-01-14
docker exec procurement_namenode hdfs dfs -ls /processed/stock/2026-01-14
```

## Complete Daily Workflow

```powershell
# ========================================
# DAILY PROCUREMENT PIPELINE WORKFLOW
# ========================================

# Step 1: Upload raw data to HDFS /raw
# (Usually done by data ingestion jobs)
python scripts/upload_raw_data.py --date 2026-01-14

# Step 2: Run pipeline processing
python scripts/compute_demand.py --date 2026-01-14
python scripts/export_orders.py --date 2026-01-14
python scripts/generate_exceptions.py --date 2026-01-14

# Step 3: Upload pipeline outputs to HDFS
.\scripts\upload_to_hdfs.ps1 -Date "2026-01-14"

# Step 4: Archive processed raw files
.\scripts\archive_processed_files.ps1 -Date "2026-01-14"

# ========================================
# VERIFICATION
# ========================================

# Check /raw is empty (data moved to /processed)
docker exec procurement_namenode hdfs dfs -count /raw/orders/2026-01-14
# Should show: 1  0  0 (directory exists but empty)

# Check /processed has files
docker exec procurement_namenode hdfs dfs -count /processed/orders/2026-01-14
# Should show: 1  15  <total_bytes> (15 order files)

docker exec procurement_namenode hdfs dfs -count /processed/stock/2026-01-14
# Should show: 1  5  <total_bytes> (5 stock files)

# Check /output has results
docker exec procurement_namenode hdfs dfs -count /output/replenishment/2026-01-14
# Should show: 1  1  <bytes> (1 replenishment CSV)

docker exec procurement_namenode hdfs dfs -count /output/orders/2026-01-14
# Should show: 1  7  <bytes> (7 supplier order JSONs)
```

## Data Retention Policy

### Recommended Retention

| Directory | Retention Period | Reason |
|-----------|------------------|--------|
| `/raw/` | 1 day | Only holds unprocessed files; should be empty after daily run |
| `/processed/` | 90 days | Regulatory compliance, reprocessing capability |
| `/output/` | 365 days | Business analytics, historical reporting |
| `/logs/` | 180 days | Troubleshooting, audit trail |

### Cleanup Script Example

```powershell
# Delete processed files older than 90 days
$retentionDate = (Get-Date).AddDays(-90).ToString("yyyy-MM-dd")

# List old dates in /processed
docker exec procurement_namenode hdfs dfs -ls /processed/orders | Where-Object { $_ -match '\d{4}-\d{2}-\d{2}' -and $Matches[0] -lt $retentionDate }

# Delete (add -rm -r to actually delete)
# docker exec procurement_namenode hdfs dfs -rm -r /processed/orders/$oldDate
```

## Monitoring & Alerting

### Daily Checks

**1. Verify /raw is processed**
```bash
# Alert if /raw still has files after 23:00
RAW_COUNT=$(docker exec procurement_namenode hdfs dfs -count -q /raw/orders/$(date +%Y-%m-%d) | awk '{print $3}')
if [ $RAW_COUNT -gt 0 ]; then
    echo "ALERT: Unprocessed files in /raw"
fi
```

**2. Verify /processed archive**
```bash
# Check expected file counts
EXPECTED_ORDERS=15
EXPECTED_STOCK=5

ACTUAL_ORDERS=$(docker exec procurement_namenode hdfs dfs -count /processed/orders/$(date +%Y-%m-%d) | awk '{print $2}')
ACTUAL_STOCK=$(docker exec procurement_namenode hdfs dfs -count /processed/stock/$(date +%Y-%m-%d) | awk '{print $2}')

if [ $ACTUAL_ORDERS -ne $EXPECTED_ORDERS ]; then
    echo "ALERT: Expected $EXPECTED_ORDERS orders, found $ACTUAL_ORDERS"
fi
```

## Reprocessing from `/processed`

If you need to reprocess data:

```powershell
# 1. Copy archived data back to /raw
docker exec procurement_namenode hdfs dfs -cp /processed/orders/2026-01-14/* /raw/orders/2026-01-14/
docker exec procurement_namenode hdfs dfs -cp /processed/stock/2026-01-14/* /raw/stock/2026-01-14/

# 2. Delete old outputs
docker exec procurement_namenode hdfs dfs -rm -r /output/replenishment/2026-01-14
docker exec procurement_namenode hdfs dfs -rm -r /output/orders/2026-01-14

# 3. Rerun pipeline
python scripts/compute_demand.py --date 2026-01-14
python scripts/export_orders.py --date 2026-01-14
python scripts/generate_exceptions.py --date 2026-01-14

# 4. Upload new outputs
.\scripts\upload_to_hdfs.ps1 -Date "2026-01-14"

# 5. Re-archive (move /raw back to /processed)
docker exec procurement_namenode hdfs dfs -rm -r /processed/orders/2026-01-14/*
docker exec procurement_namenode hdfs dfs -rm -r /processed/stock/2026-01-14/*
.\scripts\archive_processed_files.ps1 -Date "2026-01-14"
```

## View Processed Files in HDFS UI

1. Open http://localhost:9870
2. Click **Utilities** → **Browse the file system**
3. Navigate to `/processed/orders/2026-01-14/`
4. Click any file to:
   - View file info (size, replication, blocks)
   - Download file
   - View block locations on datanodes

## Summary

**The `/processed` directory is essential for:**
- ✅ Data governance & compliance
- ✅ Preventing duplicate processing
- ✅ Audit trails & data lineage
- ✅ Rollback & reprocessing capability
- ✅ Clean data lifecycle management

**Typical daily volumes:**
- **Input:** 20 files/day (15 orders + 5 stock)
- **Archived:** 20 files moved to `/processed/`
- **Output:** 10 files generated (1 replenishment + 7 supplier orders + 2 exception reports)

**Storage estimate:**
- 20 files/day × 30 days × 3 replicas = 1,800 file copies in HDFS
- At ~50KB/file average = ~90MB/month for processed archives
