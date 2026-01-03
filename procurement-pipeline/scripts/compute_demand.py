import math
from prestodb import dbapi

# Configuration
PRESTO_HOST = 'localhost'
PRESTO_PORT = 8080
PRESTO_USER = 'admin'
PRESTO_CATALOG = 'hive'
PRESTO_SCHEMA = 'raw'

def get_presto_connection():
    """Connect to Presto."""
    return dbapi.connect(
        host=PRESTO_HOST,
        port=PRESTO_PORT,
        user=PRESTO_USER,
        catalog=PRESTO_CATALOG,
        schema=PRESTO_SCHEMA
    )

def calculate_procurement(date_str):
    """
    Fetch raw net demand from Presto and apply business rules:
    1. MOQ (Minimum Order Quantity)
    2. Rounding to Case Size
    """
    conn = get_presto_connection()
    cur = conn.cursor()
    
    # Query the view we created in sql/analysis.sql
    query = f"""
        SELECT 
            sku, product_name, supplier_id, 
            case_size, moq, raw_net_demand 
        FROM hive.raw.net_demand_calculation 
        WHERE date = '{date_str}' AND raw_net_demand > 0
    """
    
    print(f"Executing Presto query for {date_str}...")
    cur.execute(query)
    rows = cur.fetchall()
    
    orders = []
    
    for row in rows:
        sku, product_name, supplier_id, case_size, moq, raw_net_demand = row
        
        # Business Logic
        quantity_needed = raw_net_demand
        
        # 1. Apply MOQ
        if quantity_needed < moq:
            quantity_needed = moq
            
        # 2. Round up to nearest Case Size
        # If case_size is 10 and needed is 12 -> 20
        if case_size > 0:
            cases = math.ceil(quantity_needed / case_size)
            final_quantity = cases * case_size
        else:
            final_quantity = quantity_needed
            
        orders.append({
            "sku": sku,
            "product_name": product_name,
            "supplier_id": supplier_id,
            "original_demand": raw_net_demand,
            "final_quantity": int(final_quantity),
            "cases": int(final_quantity / case_size) if case_size > 0 else 0
        })
        
    return orders

if __name__ == "__main__":
    # Test run
    import datetime
    today = datetime.datetime.now().strftime("%Y-%m-%d")
    orders = calculate_procurement(today)
    print(f"Calculated {len(orders)} replenishment orders.")
    for o in orders[:5]:
        print(o)
