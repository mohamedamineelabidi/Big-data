# HDFS Ingestion Guide

## Overview
This guide explains how to upload pipeline outputs (logs, replenishment files, supplier orders) to HDFS for long-term storage and analytics.

## HDFS Directory Structure

```
/raw/                           # Source data (before processing)
  ├── orders/
  │   └── YYYY-MM-DD/
  │       ├── pos_001_YYYY-MM-DD.json
  │       ├── pos_002_YYYY-MM-DD.json
  │       └── ... (15 POS files)
  └── stock/
      └── YYYY-MM-DD/
          ├── wh_1_YYYY-MM-DD.csv
          ├── wh_2_YYYY-MM-DD.csv
          └── ... (5 warehouse files)

/processed/                     # Archived source data (after successful processing)
  ├── orders/
  │   └── YYYY-MM-DD/
  │       └── (moved from /raw/orders/)
  └── stock/
      └── YYYY-MM-DD/
          └── (moved from /raw/stock/)

/logs/                          # Pipeline execution logs
  └── exceptions/
      └── YYYY-MM-DD/
          ├── exception_report_YYYY-MM-DD.json
          └── exception_summary_YYYY-MM-DD.txt

/output/                        # Pipeline outputs (analytics results)
  ├── replenishment/
  │   └── YYYY-MM-DD/
  │       └── replenishment_YYYY-MM-DD.csv
  └── orders/
      └── YYYY-MM-DD/
          ├── BevCo_Distributors_YYYY-MM-DD.json
          ├── DairyFresh_LLC_YYYY-MM-DD.json
          ├── ElectroSupply_Co_YYYY-MM-DD.json
          ├── FreshMart_Wholesale_YYYY-MM-DD.json
          ├── HomeGoods_Supply_YYYY-MM-DD.json
          ├── SnackWorld_Inc_YYYY-MM-DD.json
          └── TechGear_Plus_YYYY-MM-DD.json
```

## Method 1: Automated Upload Script (PowerShell)

### Usage

```powershell
# Upload files for specific date
.\scripts\upload_to_hdfs.ps1 -Date "2026-01-14"

# Upload all available files
.\scripts\upload_to_hdfs.ps1 -All
```

### What It Does
1. Creates HDFS directories organized by date
2. Copies local files to namenode container  
3. Uploads files to HDFS using `hdfs dfs -put`
4. Verifies uploads and shows directory listing

## Method 2: Manual Upload Commands

### Step 1: Create HDFS Directories
```bash
docker exec procurement_namenode hdfs dfs -mkdir -p /logs/exceptions/2026-01-14
docker exec procurement_namenode hdfs dfs -mkdir -p /output/replenishment/2026-01-14
docker exec procurement_namenode hdfs dfs -mkdir -p /output/orders/2026-01-14
```

### Step 2: Upload Exception Logs
```powershell
# Upload JSON report
docker cp logs\exception_report_2026-01-14.json procurement_namenode:/tmp/
docker exec procurement_namenode hdfs dfs -put -f /tmp/exception_report_2026-01-14.json /logs/exceptions/2026-01-14/

# Upload text summary
docker cp logs\exception_summary_2026-01-14.txt procurement_namenode:/tmp/
docker exec procurement_namenode hdfs dfs -put -f /tmp/exception_summary_2026-01-14.txt /logs/exceptions/2026-01-14/
```

### Step 3: Upload Replenishment File
```powershell
docker cp output\replenishment_2026-01-14.csv procurement_namenode:/tmp/
docker exec procurement_namenode hdfs dfs -put -f /tmp/replenishment_2026-01-14.csv /output/replenishment/2026-01-14/
```

### Step 4: Upload Supplier Orders
```powershell
# Upload all supplier order JSONs for the date
Get-ChildItem output\*_2026-01-14.json | ForEach-Object {
    docker cp $_.FullName procurement_namenode:/tmp/
    docker exec procurement_namenode hdfs dfs -put -f /tmp/$($_.Name) /output/orders/2026-01-14/
}
```

## Method 3: Python Script (Network Issues - NOT RECOMMENDED)

⚠️ **Note**: The Python `ingest_hdfs.py` script has DNS resolution issues when running outside Docker because it can't resolve datanode hostnames (datanode1, datanode2, datanode3).

**Workaround**: Use PowerShell/Docker commands instead (Methods 1 or 2).

## Verification Commands

### List all uploaded files for a date
```bash
docker exec procurement_namenode hdfs dfs -ls -R /logs/exceptions/2026-01-14
docker exec procurement_namenode hdfs dfs -ls -R /output/replenishment/2026-01-14
docker exec procurement_namenode hdfs dfs -ls -R /output/orders/2026-01-14
```

### Check file replication
```bash
docker exec procurement_namenode hdfs dfs -ls /output/replenishment/2026-01-14/replenishment_2026-01-14.csv
```
Look for the number after `-rw-r--r--` (should be 3 for 3x replication).

### View file contents from HDFS
```bash
# View exception report
docker exec procurement_namenode hdfs dfs -cat /logs/exceptions/2026-01-14/exception_report_2026-01-14.json

# View replenishment CSV
docker exec procurement_namenode hdfs dfs -cat /output/replenishment/2026-01-14/replenishment_2026-01-14.csv | head -20
```

### Calculate storage usage
```bash
docker exec procurement_namenode hdfs dfs -du -s -h /logs
docker exec procurement_namenode hdfs dfs -du -s -h /output
```

## HDFS Web UI

Access the HDFS NameNode UI at: **http://localhost:9870**

### Navigate to Files
1. Open http://localhost:9870
2. Click "Utilities" → "Browse the file system"
3. Navigate to `/logs/exceptions/` or `/output/` directories
4. Click on any file to view details or download

## Integration with Airflow DAG

To automate HDFS upload after pipeline execution, add this task to `procurement_dag.py`:

```python
upload_to_hdfs = BashOperator(
    task_id='upload_to_hdfs',
    bash_command='powershell -ExecutionPolicy Bypass -File /opt/airflow/scripts/upload_to_hdfs.ps1 -Date {{ ds }}',
    dag=dag
)

# Set dependency
generate_exceptions >> upload_to_hdfs >> summary_task
```

## Troubleshooting

### Problem: "Failed to resolve datanode hostname"
**Solution**: Use Docker exec commands instead of Python WebHDFS client.

### Problem: "No such file or directory"
**Solution**: Ensure local files exist before uploading:
```powershell
Test-Path output\replenishment_2026-01-14.csv
Test-Path logs\exception_report_2026-01-14.json
```

### Problem: "Permission denied"
**Solution**: HDFS directories are created with full permissions (drwxrwxrwx), but verify:
```bash
docker exec procurement_namenode hdfs dfs -chmod -R 777 /logs
docker exec procurement_namenode hdfs dfs -chmod -R 777 /output
```

### Problem: "Out of space"
**Solution**: Check HDFS capacity:
```bash
docker exec procurement_namenode hdfs dfsadmin -report
```

## Best Practices

1. **Organize by Date**: Always partition data by execution date for easy querying
2. **Verify Replication**: Ensure replication factor is 3 for fault tolerance
3. **Clean Up**: Archive old files after 90 days to save space
4. **Use Compression**: For large CSV files, consider compressing before upload
5. **Automate**: Integrate HDFS upload into Airflow DAG for automatic execution

## Example: Complete Workflow for Date 2026-01-14

```powershell
# 1. Run pipeline
python scripts/compute_demand.py --date 2026-01-14
python scripts/export_orders.py --date 2026-01-14
python scripts/generate_exceptions.py --date 2026-01-14

# 2. Upload pipeline outputs to HDFS
.\scripts\upload_to_hdfs.ps1 -Date "2026-01-14"

# 3. Archive processed raw data (move /raw → /processed)
.\scripts\archive_processed_files.ps1 -Date "2026-01-14"

# 4. Verify
docker exec procurement_namenode hdfs dfs -ls -R /logs/exceptions/2026-01-14
docker exec procurement_namenode hdfs dfs -ls -R /output/replenishment/2026-01-14
docker exec procurement_namenode hdfs dfs -ls -R /output/orders/2026-01-14
docker exec procurement_namenode hdfs dfs -ls -R /processed/orders/2026-01-14
docker exec procurement_namenode hdfs dfs -ls -R /processed/stock/2026-01-14

# 5. View in Web UI
# Open http://localhost:9870
```

## Data Lifecycle Management

### Purpose of `/processed` Directory

The `/processed` directory serves as an **archive for successfully processed raw data**:

**Benefits:**
1. ✅ **Prevents Duplicate Processing** - Raw files moved to /processed won't be reprocessed
2. ✅ **Audit Trail** - Maintains record of what data was processed and when
3. ✅ **Data Lineage** - Clear path: raw → processing → output → archive
4. ✅ **Rollback Capability** - Can reprocess from /processed if needed
5. ✅ **Clean /raw Directory** - Only unprocessed data remains in /raw

### Workflow

```
┌─────────────┐
│  New Data   │  Daily files arrive
│   /raw/     │  (15 orders + 5 stock = 20 files)
└──────┬──────┘
       │
       ▼
┌─────────────┐
│  Pipeline   │  compute_demand.py
│ Processing  │  export_orders.py
│             │  generate_exceptions.py
└──────┬──────┘
       │
       ▼
┌─────────────┐
│  Outputs    │  replenishment.csv (1 file)
│  /output/   │  supplier_orders.json (7 files)
│  /logs/     │  exceptions.json/txt (2 files)
└──────┬──────┘
       │
       ▼
┌─────────────┐
│  Archive    │  Move /raw → /processed
│ /processed/ │  (20 files archived)
└─────────────┘
```

### Archive Command

```powershell
.\scripts\archive_processed_files.ps1 -Date "2026-01-14"
```

This moves:
- `/raw/orders/2026-01-14/*.json` → `/processed/orders/2026-01-14/`
- `/raw/stock/2026-01-14/*.csv` → `/processed/stock/2026-01-14/`

## Summary

✅ **Uploaded for 2026-01-14**:
- 2 exception files → `/logs/exceptions/2026-01-14/`
- 1 replenishment file → `/output/replenishment/2026-01-14/`
- 7 supplier order files → `/output/orders/2026-01-14/`

Total: **10 files** stored in HDFS with 3x replication
