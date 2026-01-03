import psycopg2
from hdfs import InsecureClient
import sys

def test_postgres():
    print("Testing PostgreSQL connection...")
    try:
        conn = psycopg2.connect(
            host="localhost",
            database="procurement_db",
            user="admin",
            password="password"
        )
        cur = conn.cursor()
        cur.execute("SELECT version();")
        version = cur.fetchone()
        print(f"✅ PostgreSQL Connected: {version[0]}")
        
        # Verify Data
        cur.execute("SELECT count(*) FROM products;")
        count = cur.fetchone()[0]
        print(f"   Products Table Count: {count}")
        
        conn.close()
    except Exception as e:
        print(f"❌ PostgreSQL Connection Failed: {e}")

def test_hdfs():
    print("\nTesting HDFS connection...")
    try:
        client = InsecureClient('http://localhost:9870', user='root')
        status = client.status('/')
        print(f"✅ HDFS Connected. Root owner: {status['owner']}")
    except Exception as e:
        print(f"❌ HDFS Connection Failed: {e}")

def test_trino():
    print("\nTesting Trino connection...")
    try:
        from trino.dbapi import connect
        conn = connect(
            host='localhost',
            port=8080,
            user='admin',
            catalog='system',
            schema='runtime',
            http_scheme='http'
        )
        cur = conn.cursor()
        cur.execute("SELECT 1")
        rows = cur.fetchall()
        print(f"✅ Trino Connected. Query result: {rows[0][0]}")
    except Exception as e:
        print(f"❌ Trino Connection Failed: {e}")

if __name__ == "__main__":
    print("Starting Infrastructure Health Check...\n")
    test_postgres()
    test_hdfs()
    test_trino()
