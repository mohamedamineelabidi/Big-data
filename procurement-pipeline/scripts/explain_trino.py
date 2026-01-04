"""
TRINO DEMONSTRATION & EXPLANATION
What is Trino and Why We Use It in Our Procurement Pipeline
"""

from trino.dbapi import connect
import json

def explain_trino():
    """Explain what Trino is and demonstrate its capabilities"""
    
    print("=" * 70)
    print("🔍 WHAT IS TRINO?")
    print("=" * 70)
    print("""
Trino (formerly Presto) is a DISTRIBUTED SQL QUERY ENGINE designed for 
BIG DATA analytics. Think of it as a "SQL translator" that can:

1. Query MULTIPLE data sources using ONE SQL language
2. Process data WHERE IT LIVES (no data movement)
3. Join data from DIFFERENT systems in a single query
4. Scale to PETABYTES of data

""")
    
    print("=" * 70)
    print("🏗️ HOW TRINO WORKS IN OUR PROJECT")
    print("=" * 70)
    print("""
Our Architecture:
┌─────────────────────────────────────────────────┐
│                    TRINO                        │
│          (Distributed Query Engine)             │
└──────────────┬──────────────┬───────────────────┘
               │              │
               │              │
      ┌────────▼──────┐  ┌───▼──────────┐
      │  PostgreSQL   │  │     HDFS     │
      │  (Master Data)│  │ (Orders/Stock)│
      └───────────────┘  └──────────────┘

Trino connects to BOTH:
- PostgreSQL: Products, Suppliers, Replenishment Rules
- HDFS: Daily Orders from 15 POS + Warehouse Stock
""")
    
    print("\n" + "=" * 70)
    print("✨ TRINO'S SUPERPOWERS FOR OUR PIPELINE")
    print("=" * 70)
    print("""
1. FEDERATED QUERIES (Join Different Databases)
   - Combine orders from HDFS with product details from PostgreSQL
   - No need to copy data between systems

2. BATCH ANALYTICS AT SCALE
   - Process millions of orders across 15 stores in seconds
   - Aggregate daily demand per SKU across all locations

3. SQL INTERFACE (Easy to Use)
   - Use standard SQL instead of complex MapReduce/Spark code
   - Data analysts can write queries without learning new tools

4. SEPARATION OF STORAGE & COMPUTE
   - Data stays in HDFS (cheap storage)
   - Trino only processes when needed (elastic compute)
""")

    print("\n" + "=" * 70)
    print("🧪 LIVE DEMONSTRATION")
    print("=" * 70)
    
    try:
        conn = connect(
            host='localhost',
            port=8080,
            user='admin',
            catalog='postgresql',
            schema='public',
            http_scheme='http'
        )
        cur = conn.cursor()
        
        # Demo 1: Show catalogs
        print("\n1️⃣  AVAILABLE DATA SOURCES (Catalogs):")
        cur.execute("SHOW CATALOGS")
        catalogs = cur.fetchall()
        for cat in catalogs:
            print(f"   📂 {cat[0]}")
        
        # Demo 2: Query PostgreSQL
        print("\n2️⃣  QUERYING POSTGRESQL (Master Data):")
        cur.execute("""
            SELECT 
                COUNT(*) as total_products,
                COUNT(DISTINCT category) as categories
            FROM products
        """)
        result = cur.fetchone()
        print(f"   📦 Total Products: {result[0]}")
        print(f"   🏷️  Categories: {result[1]}")
        
        # Demo 3: Complex analytics query
        print("\n3️⃣  ADVANCED ANALYTICS QUERY:")
        cur.execute("""
            SELECT 
                p.category,
                COUNT(*) as product_count,
                AVG(p.case_size) as avg_case_size,
                COUNT(DISTINCT s.supplier_id) as suppliers
            FROM products p
            LEFT JOIN replenishment_rules r ON p.product_id = r.product_id
            LEFT JOIN suppliers s ON r.supplier_id = s.supplier_id
            GROUP BY p.category
            ORDER BY product_count DESC
            LIMIT 5
        """)
        print("   📊 Top Categories by Product Count:")
        results = cur.fetchall()
        for row in results:
            print(f"      • {row[0]}: {row[1]} products, {row[3]} suppliers")
        
        # Demo 4: Show query execution
        print("\n4️⃣  TRINO QUERY EXECUTION:")
        cur.execute("""
            SELECT 
                p.product_id,
                p.product_name,
                r.safety_stock_level,
                s.supplier_name
            FROM products p
            JOIN replenishment_rules r ON p.product_id = r.product_id
            JOIN suppliers s ON r.supplier_id = s.supplier_id
            WHERE p.category = 'Beverages'
            LIMIT 3
        """)
        print("   🔗 JOIN across 3 tables (Products + Rules + Suppliers):")
        results = cur.fetchall()
        for row in results:
            print(f"      • {row[1]} → Supplier: {row[3]}, Safety Stock: {row[2]}")
        
        print("\n" + "=" * 70)
        print("💡 WHY THIS MATTERS FOR PROCUREMENT")
        print("=" * 70)
        print("""
Tomorrow, Trino will help us:

1. Read orders from HDFS (JSON files from 15 stores)
2. Join with product data from PostgreSQL
3. Calculate: Total Demand - Stock + Safety = Net Replenishment
4. Group by supplier and apply MOQ/case size rules
5. Generate purchase orders

ALL IN ONE SQL QUERY - No data movement, no ETL delays!
""")
        
        print("✅ Trino is ready for Phase 3 analytics!")
        
    except Exception as e:
        print(f"❌ Error: {e}")

if __name__ == "__main__":
    explain_trino()
