"""
Phase 3: Distributed Analytical Processing
Trino Query Script for Demand Calculation
"""

from trino.dbapi import connect
import pandas as pd
from datetime import datetime

def get_trino_connection():
    """Get Trino connection"""
    return connect(
        host='localhost',
        port=8080,
        user='admin',
        catalog='postgresql',
        schema='public',
        http_scheme='http'
    )

def test_postgresql_catalog():
    """Test querying PostgreSQL through Trino"""
    print("=" * 60)
    print("Testing Trino → PostgreSQL Connection")
    print("=" * 60)
    
    try:
        conn = get_trino_connection()
        cur = conn.cursor()
        
        # Query 1: List tables
        print("\n1. Available Tables:")
        cur.execute("SHOW TABLES")
        tables = cur.fetchall()
        for table in tables:
            print(f"   - {table[0]}")
        
        # Query 2: Get product count
        print("\n2. Products Count:")
        cur.execute("SELECT COUNT(*) as count FROM products")
        count = cur.fetchone()[0]
        print(f"   Total Products: {count}")
        
        # Query 3: Sample products
        print("\n3. Sample Products:")
        cur.execute("""
            SELECT product_id, product_name, case_size 
            FROM products 
            LIMIT 5
        """)
        products = cur.fetchall()
        for p in products:
            print(f"   Product ID: {p[0]}, Name: {p[1]}, Case Size: {p[2]}")
        
        # Query 4: Suppliers
        print("\n4. Suppliers Count:")
        cur.execute("SELECT COUNT(*) FROM suppliers")
        suppliers_count = cur.fetchone()[0]
        print(f"   Total Suppliers: {suppliers_count}")
        
        print("\n✅ PostgreSQL catalog working perfectly!")
        return True
        
    except Exception as e:
        print(f"\n❌ Error: {e}")
        return False

if __name__ == "__main__":
    test_postgresql_catalog()
