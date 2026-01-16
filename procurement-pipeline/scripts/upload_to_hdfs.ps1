# Upload Pipeline Outputs to HDFS - Simple Version
param([string]$Date = "2026-01-14")

Write-Host "Uploading files for date: $Date" -ForegroundColor Cyan

# Create directories in HDFS
docker exec procurement_namenode hdfs dfs -mkdir -p /logs/exceptions/$Date
docker exec procurement_namenode hdfs dfs -mkdir -p /output/replenishment/$Date
docker exec procurement_namenode hdfs dfs -mkdir -p /output/orders/$Date

$count = 0

# Upload exception report JSON
if (Test-Path "logs\exception_report_$Date.json") {
    docker cp "logs\exception_report_$Date.json" procurement_namenode:/tmp/
    docker exec procurement_namenode hdfs dfs -put -f /tmp/exception_report_$Date.json /logs/exceptions/$Date/
    Write-Host "✓ Uploaded exception_report_$Date.json" -ForegroundColor Green
    $count++
}

# Upload exception summary TXT
if (Test-Path "logs\exception_summary_$Date.txt") {
    docker cp "logs\exception_summary_$Date.txt" procurement_namenode:/tmp/
    docker exec procurement_namenode hdfs dfs -put -f /tmp/exception_summary_$Date.txt /logs/exceptions/$Date/
    Write-Host "✓ Uploaded exception_summary_$Date.txt" -ForegroundColor Green
    $count++
}

# Upload replenishment CSV
if (Test-Path "output\replenishment_$Date.csv") {
    docker cp "output\replenishment_$Date.csv" procurement_namenode:/tmp/
    docker exec procurement_namenode hdfs dfs -put -f /tmp/replenishment_$Date.csv /output/replenishment/$Date/
    Write-Host "✓ Uploaded replenishment_$Date.csv" -ForegroundColor Green
    $count++
}

# Upload supplier order JSONs
$orders = Get-ChildItem "output\*_$Date.json" -ErrorAction SilentlyContinue
foreach ($file in $orders) {
    docker cp $file.FullName procurement_namenode:/tmp/
    docker exec procurement_namenode hdfs dfs -put -f /tmp/$($file.Name) /output/orders/$Date/
    Write-Host "✓ Uploaded $($file.Name)" -ForegroundColor Green
    $count++
}

Write-Host "`nTotal uploaded: $count files" -ForegroundColor Yellow
Write-Host "View in HDFS: http://localhost:9870" -ForegroundColor Cyan

# Verify
Write-Host "`n=== HDFS Contents ===" -ForegroundColor Yellow
docker exec procurement_namenode hdfs dfs -ls /logs/exceptions/$Date
docker exec procurement_namenode hdfs dfs -ls /output/replenishment/$Date
docker exec procurement_namenode hdfs dfs -ls /output/orders/$Date
