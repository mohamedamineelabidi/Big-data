# Big Data Procurement Pipeline Setup
Write-Host "🚀 Setting up Big Data Procurement Pipeline..." -ForegroundColor Green

# Create required directories
Write-Host "📁 Creating directory structure..." -ForegroundColor Yellow
$directories = @(
    "data\raw\orders",
    "data\raw\stock",
    "data\output\supplier_orders",
    "data\output\exceptions",
    "logs",
    "airflow\logs"
)

foreach ($dir in $directories) {
    if (!(Test-Path $dir)) {
        New-Item -ItemType Directory -Path $dir -Force | Out-Null
        Write-Host "  ✓ Created $dir" -ForegroundColor Gray
    }
}

# Create .gitkeep files
$gitkeepFiles = @(
    "data\raw\orders\.gitkeep",
    "data\raw\stock\.gitkeep",
    "data\output\.gitkeep",
    "logs\.gitkeep"
)

foreach ($file in $gitkeepFiles) {
    if (!(Test-Path $file)) {
        New-Item -ItemType File -Path $file -Force | Out-Null
    }
}

# Install Python dependencies
Write-Host "📦 Installing Python dependencies..." -ForegroundColor Yellow
if (Test-Path "requirements.txt") {
    pip install -r requirements.txt
} else {
    Write-Host "  ⚠️  requirements.txt not found" -ForegroundColor Red
}

# Check Docker
Write-Host "🐳 Checking Docker..." -ForegroundColor Yellow
try {
    docker info | Out-Null
    Write-Host "  ✅ Docker is ready" -ForegroundColor Green
} catch {
    Write-Host "  ❌ Docker is not running. Please start Docker." -ForegroundColor Red
    exit 1
}

# Check for placeholder Fernet key
Write-Host "🔐 Checking Airflow configuration..." -ForegroundColor Yellow
$composeContent = Get-Content "docker-compose.yml" -Raw
if ($composeContent -match "your-fernet-key-here") {
    Write-Host "  ⚠️  Found placeholder Fernet key. Generate a secure key with:" -ForegroundColor Yellow
    Write-Host '   python -c "from cryptography.fernet import Fernet; print(Fernet.generate_key().decode())"' -ForegroundColor Cyan
}

Write-Host ""
Write-Host "✅ Setup complete!" -ForegroundColor Green
Write-Host ""
Write-Host "Next steps:" -ForegroundColor Cyan
Write-Host "  1. Start services: docker-compose up -d"
Write-Host "  2. Check status: docker ps"
Write-Host "  3. Generate data: docker exec procurement_airflow python /opt/airflow/scripts/data_gen.py"
Write-Host "  4. Access Airflow UI: http://localhost:8081 (user: admin, password: admin)"
Write-Host "  5. Access pgAdmin: http://localhost:5050 (email: admin@admin.com, password: admin)"
Write-Host "  6. Access Trino: docker exec -it procurement_trino trino"
