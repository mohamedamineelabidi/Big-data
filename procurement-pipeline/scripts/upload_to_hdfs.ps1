how i do file system in hh# Batch Upload Script for HDFS Ingestion

Write-Host "Starting HDFS Batch Upload..." -ForegroundColor Green

# Get all unique dates from generated files
$dates = @()
Get-ChildItem "data/raw/orders/*.json" | ForEach-Object {
    if ($_.Name -match '\d{4}-\d{2}-\d{2}') {
        $date = $Matches[0]
        if ($dates -notcontains $date) {
            $dates += $date
        }
    }
}

Write-Host "Found $($dates.Count) dates to process" -ForegroundColor Cyan

foreach ($date in $dates) {
    Write-Host "`nProcessing date: $date" -ForegroundColor Yellow
    
    # Create directories
    docker exec procurement_namenode hdfs dfs -mkdir -p /raw/orders/$date 2>$null
    docker exec procurement_namenode hdfs dfs -mkdir -p /raw/stock/$date 2>$null
    
    # Upload Orders for this date
    $orderFiles = Get-ChildItem "data/raw/orders/*$date.json"
    Write-Host "  Uploading $($orderFiles.Count) order files..."
    foreach ($file in $orderFiles) {
        docker cp $file.FullName procurement_namenode:/tmp/$($file.Name) 2>$null
        docker exec procurement_namenode hdfs dfs -put -f /tmp/$($file.Name) /raw/orders/$date/ 2>$null
    }
    
    # Upload Stock for this date
    $stockFiles = Get-ChildItem "data/raw/stock/*$date.csv"
    Write-Host "  Uploading $($stockFiles.Count) stock files..."
    foreach ($file in $stockFiles) {
        docker cp $file.FullName procurement_namenode:/tmp/$($file.Name) 2>$null
        docker exec procurement_namenode hdfs dfs -put -f /tmp/$($file.Name) /raw/stock/$date/ 2>$null
    }
}

# Verify
Write-Host "`nVerifying uploads..." -ForegroundColor Green
Write-Host "`nHDFS Directory Structure:"
docker exec procurement_namenode hdfs dfs -ls -R /raw | Select-String -Pattern "drw|\.json|\.csv"

Write-Host "`nCalculating total size..."
$report = docker exec procurement_namenode hdfs dfs -du -s -h /raw
Write-Host $report -ForegroundColor Cyan

Write-Host "`nHDFS Upload Complete!" -ForegroundColor Green
