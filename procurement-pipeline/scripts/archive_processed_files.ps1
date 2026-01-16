# Archive Processed Raw Data to HDFS /processed
# After successful pipeline execution, move raw files to /processed for archival

param(
    [Parameter(Mandatory=$true)]
    [string]$Date
)

Write-Host "========================================" -ForegroundColor Cyan
Write-Host " Archive Processed Files to HDFS" -ForegroundColor Cyan
Write-Host " Date: $Date" -ForegroundColor Cyan
Write-Host "========================================" -ForegroundColor Cyan

$archivedCount = 0

# Create processed directories
Write-Host "`nCreating /processed directories..." -ForegroundColor Yellow
docker exec procurement_namenode hdfs dfs -mkdir -p /processed/orders/$Date 2>$null
docker exec procurement_namenode hdfs dfs -mkdir -p /processed/stock/$Date 2>$null

# Archive order files from /raw to /processed
Write-Host "`n=== Archiving Order Files ===" -ForegroundColor Yellow
$orderFiles = docker exec procurement_namenode hdfs dfs -ls /raw/orders/$Date 2>$null | Select-String "\.json"
if ($orderFiles) {
    foreach ($line in $orderFiles) {
        if ($line -match '(/raw/orders/.+\.json)') {
            $sourcePath = $Matches[1]
            $filename = Split-Path -Leaf $sourcePath
            $destPath = "/processed/orders/$Date/$filename"
            
            Write-Host "Moving $filename..." -NoNewline
            docker exec procurement_namenode hdfs dfs -mv $sourcePath $destPath 2>$null
            if ($LASTEXITCODE -eq 0) {
                Write-Host " ✓" -ForegroundColor Green
                $archivedCount++
            } else {
                Write-Host " ✗" -ForegroundColor Red
            }
        }
    }
} else {
    Write-Host "No order files found in /raw/orders/$Date" -ForegroundColor Yellow
}

# Archive stock files from /raw to /processed
Write-Host "`n=== Archiving Stock Files ===" -ForegroundColor Yellow
$stockFiles = docker exec procurement_namenode hdfs dfs -ls /raw/stock/$Date 2>$null | Select-String "\.csv"
if ($stockFiles) {
    foreach ($line in $stockFiles) {
        if ($line -match '(/raw/stock/.+\.csv)') {
            $sourcePath = $Matches[1]
            $filename = Split-Path -Leaf $sourcePath
            $destPath = "/processed/stock/$Date/$filename"
            
            Write-Host "Moving $filename..." -NoNewline
            docker exec procurement_namenode hdfs dfs -mv $sourcePath $destPath 2>$null
            if ($LASTEXITCODE -eq 0) {
                Write-Host " ✓" -ForegroundColor Green
                $archivedCount++
            } else {
                Write-Host " ✗" -ForegroundColor Red
            }
        }
    }
} else {
    Write-Host "No stock files found in /raw/stock/$Date" -ForegroundColor Yellow
}

# Summary
Write-Host "`n========================================" -ForegroundColor Cyan
Write-Host " Archive Complete!" -ForegroundColor Green
Write-Host "========================================" -ForegroundColor Cyan
Write-Host " Files archived: $archivedCount" -ForegroundColor White

# Verify
Write-Host "`n=== Processed Directory Contents ===" -ForegroundColor Yellow
docker exec procurement_namenode hdfs dfs -count /processed/orders/$Date
docker exec procurement_namenode hdfs dfs -count /processed/stock/$Date

Write-Host "`n=== Raw Directory Status (should be empty) ===" -ForegroundColor Yellow
docker exec procurement_namenode hdfs dfs -count /raw/orders/$Date 2>$null
docker exec procurement_namenode hdfs dfs -count /raw/stock/$Date 2>$null
