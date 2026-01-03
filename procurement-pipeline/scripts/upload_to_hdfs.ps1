# Batch Upload Script for HDFS Ingestion

Write-Host "Starting HDFS Batch Upload..." -ForegroundColor Green

# Create directories
Write-Host "Creating HDFS directories..."
docker exec procurement_namenode hdfs dfs -mkdir -p /raw/orders/2026-01-03
docker exec procurement_namenode hdfs dfs -mkdir -p /raw/stock/2026-01-03

# Upload Orders
Write-Host "Uploading POS orders..." -ForegroundColor Yellow
Get-ChildItem "data/raw/orders/*.json" | ForEach-Object {
    $filename = $_.Name
    Write-Host "  Uploading $filename"
    docker cp $_.FullName procurement_namenode:/tmp/$filename
    docker exec procurement_namenode hdfs dfs -put -f /tmp/$filename /raw/orders/2026-01-03/
}

# Upload Stock
Write-Host "Uploading warehouse stock..." -ForegroundColor Yellow
Get-ChildItem "data/raw/stock/*.csv" | ForEach-Object {
    $filename = $_.Name
    Write-Host "  Uploading $filename"
    docker cp $_.FullName procurement_namenode:/tmp/$filename
    docker exec procurement_namenode hdfs dfs -put -f /tmp/$filename /raw/stock/2026-01-03/
}

# Verify
Write-Host "`nVerifying uploads..." -ForegroundColor Green
Write-Host "`nOrders:"
docker exec procurement_namenode hdfs dfs -ls /raw/orders/2026-01-03/
Write-Host "`nStock:"
docker exec procurement_namenode hdfs dfs -ls /raw/stock/2026-01-03/

Write-Host "`nHDFS Upload Complete!" -ForegroundColor Green
