# Trino Configuration Guide

## 📁 Directory Structure

```
trino-config/
├── config.properties      # Main Trino coordinator configuration
├── jvm.config            # JVM memory and performance settings
├── node.properties       # Node identification and data directory
├── catalog/              # Data source connectors
│   └── postgresql.properties  # PostgreSQL catalog configuration
└── README.md            # This file
```

## 🔧 Configuration Files

### 1. config.properties
**Purpose**: Main Trino server configuration

**Key Settings**:
- `coordinator=true` - This node acts as coordinator
- `http-server.http.port=8080` - Trino Web UI port
- `query.max-memory=1GB` - Maximum memory per query
- `spill-enabled=true` - Enable disk spill for large queries

**Performance Tuning**:
- Adjusted for 2GB heap size
- Query memory limited to prevent OOM errors
- Spill to disk enabled for large aggregations

### 2. jvm.config
**Purpose**: Java Virtual Machine configuration

**Key Settings**:
- `-Xmx2G -Xms2G` - Fixed heap size (2GB)
- `UseG1GC` - G1 garbage collector for better performance
- `HeapDumpOnOutOfMemoryError` - Debug memory issues
- `ReservedCodeCacheSize=512M` - Optimized for query compilation

**Memory Allocation**:
```
Total JVM: 2GB
├── Query Execution: 1GB
├── Metadata & Cache: 512MB
└── JVM Overhead: 512MB
```

### 3. node.properties
**Purpose**: Node identification

**Key Settings**:
- `node.environment=production` - Environment identifier
- `node.id=trino-coordinator` - Unique node identifier
- `node.data-dir=/data/trino` - Local data storage path

### 4. catalog/postgresql.properties
**Purpose**: PostgreSQL data source connector

**Key Settings**:
- `connection-url` - PostgreSQL JDBC connection string
- `connection-pool.max-size=20` - Maximum connections
- `jdbc.pushdown-enabled=true` - Push queries to PostgreSQL for better performance

**Connected Database**: `procurement_db`
**Tables Available**:
- `products` (49 rows) - Product master data
- `suppliers` (10 rows) - Supplier information
- `replenishment_rules` (49 rows) - Business rules for procurement

## 🚀 Usage

### Access Trino CLI
```bash
# From host machine
docker exec -it procurement_trino trino

# Run query directly
docker exec procurement_trino trino --execute "SHOW TABLES FROM postgresql.public;"
```

### Query Examples
```sql
-- List all catalogs
SHOW CATALOGS;

-- List schemas in PostgreSQL
SHOW SCHEMAS FROM postgresql;

-- Count products
SELECT COUNT(*) FROM postgresql.public.products;

-- Get products with suppliers
SELECT p.product_name, s.supplier_name 
FROM postgresql.public.products p
JOIN postgresql.public.replenishment_rules r ON p.product_id = r.product_id
JOIN postgresql.public.suppliers s ON r.supplier_id = s.supplier_id
LIMIT 10;
```

### Web UI
Access Trino Web UI at: **http://localhost:8080**
- Username: `admin`
- No password required

## 📊 Performance Monitoring

### Query Performance
```sql
-- View running queries
SELECT * FROM system.runtime.queries WHERE state = 'RUNNING';

-- View query statistics
SELECT 
  query_id,
  state,
  elapsed_time,
  total_memory,
  peak_memory
FROM system.runtime.queries
ORDER BY created DESC
LIMIT 10;
```

### Memory Usage
```sql
-- Check memory pools
SELECT * FROM system.runtime.memory_pool_usage;

-- Node memory usage
SELECT * FROM system.runtime.nodes;
```

## 🔍 Troubleshooting

### Issue: Connection Refused
**Solution**: Ensure PostgreSQL container is running
```bash
docker ps --filter "name=postgres"
docker logs procurement_postgres
```

### Issue: Out of Memory
**Solution**: Adjust JVM heap or query memory limits
- Edit `jvm.config`: Increase `-Xmx` value
- Edit `config.properties`: Adjust `query.max-memory`
- Restart Trino: `docker-compose restart trino`

### Issue: Slow Queries
**Solution**: Enable query pushdown and check statistics
```sql
-- Check if pushdown is working
EXPLAIN SELECT COUNT(*) FROM postgresql.public.products;
```

### Issue: Cannot Connect to PostgreSQL
**Solution**: Verify connection settings
```bash
# Test PostgreSQL connection
docker exec procurement_postgres psql -U admin -d procurement_db -c "SELECT 1"

# Check network connectivity
docker exec procurement_trino ping postgres
```

## 📝 Configuration Changes

### To Apply Changes:
1. Edit configuration files in `config/trino-config/`
2. Restart Trino container:
   ```bash
   docker-compose restart trino
   ```
3. Verify changes:
   ```bash
   docker logs procurement_trino
   ```

### Best Practices:
- **Memory**: Set `-Xms` = `-Xmx` for consistent performance
- **Connection Pool**: Adjust based on concurrent query load
- **Query Limits**: Prevent runaway queries with memory/timeout limits
- **Monitoring**: Regularly check Web UI for slow queries

## 🔗 Integration with Pipeline

The Airflow pipeline uses Trino to:
1. **Load Master Data**: Read products, suppliers, and rules from PostgreSQL
2. **Join Data**: Combine transactional data (HDFS) with master data (PostgreSQL)
3. **Calculate Demand**: Apply business rules using federated queries

**Python Connection Example** (from `compute_demand.py`):
```python
from trino.dbapi import connect

conn = connect(
    host='localhost',
    port=8080,
    user='admin',
    catalog='postgresql',
    schema='public',
    http_scheme='http'
)

cursor = conn.cursor()
cursor.execute("SELECT * FROM products")
products = cursor.fetchall()
```

## 📚 Additional Resources

- [Trino Documentation](https://trino.io/docs/current/)
- [PostgreSQL Connector](https://trino.io/docs/current/connector/postgresql.html)
- [Query Tuning Guide](https://trino.io/docs/current/admin/tuning.html)
- [Memory Management](https://trino.io/docs/current/admin/properties-memory-management.html)

---
**Last Updated**: January 2026
**Trino Version**: 479
**Environment**: Big Data Procurement Pipeline - ENSA Al Hoceima
