#!/bin/bash

echo "🚀 Setting up Big Data Procurement Pipeline..."

# Create required directories
echo "📁 Creating directory structure..."
mkdir -p data/raw/orders
mkdir -p data/raw/stock
mkdir -p data/output/supplier_orders
mkdir -p data/output/exceptions
mkdir -p logs
mkdir -p airflow/logs

# Create .gitkeep files to preserve directory structure
touch data/raw/orders/.gitkeep
touch data/raw/stock/.gitkeep
touch data/output/.gitkeep
touch logs/.gitkeep

# Install Python dependencies
echo "📦 Installing Python dependencies..."
if [ -f "requirements.txt" ]; then
    pip install -r requirements.txt
else
    echo "⚠️  requirements.txt not found"
fi

# Check Docker
echo "🐳 Checking Docker..."
if ! command -v docker &> /dev/null; then
    echo "❌ Docker is not installed. Please install Docker first."
    exit 1
fi

if ! docker info &> /dev/null; then
    echo "❌ Docker daemon is not running. Please start Docker."
    exit 1
fi

echo "✅ Docker is ready"

# Generate Airflow Fernet key if needed
echo "🔐 Checking Airflow configuration..."
if grep -q "your-fernet-key-here" docker-compose.yml 2>/dev/null; then
    echo "⚠️  Found placeholder Fernet key. Generate a secure key with:"
    echo "   python -c \"from cryptography.fernet import Fernet; print(Fernet.generate_key().decode())\""
fi

echo ""
echo "✅ Setup complete!"
echo ""
echo "Next steps:"
echo "  1. Start services: docker-compose up -d"
echo "  2. Check status: docker ps"
echo "  3. Generate data: docker exec procurement_airflow python /opt/airflow/scripts/data_gen.py"
echo "  4. Access Airflow UI: http://localhost:8081 (user: admin, password: admin)"
echo "  5. Access pgAdmin: http://localhost:5050 (email: admin@admin.com, password: admin)"
echo "  6. Access Trino: docker exec -it procurement_trino trino"
