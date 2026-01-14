import json
import csv
import os
import random
from datetime import datetime, timedelta
from faker import Faker

# Initialize Faker
fake = Faker()

# Configuration
NUM_POS = 15  # Increased from 5 to 15 stores
NUM_WAREHOUSES = 5  # Increased from 2 to 5 warehouses
DAYS_TO_GENERATE = 7  # Generate 7 days of data
OUTPUT_DIR_ORDERS = "data/raw/orders"
OUTPUT_DIR_STOCK = "data/raw/stock"

# Ensure output directories exist
os.makedirs(OUTPUT_DIR_ORDERS, exist_ok=True)
os.makedirs(OUTPUT_DIR_STOCK, exist_ok=True)

# Product IDs (SKUs) - ALIGNED with PostgreSQL master data (init_master_data.sql)
# These are the exact SKUs that exist in the products table with supplier mappings
product_ids = [
    # Beverages (7 items)
    'SKU-0001', 'SKU-0005', 'SKU-0011', 'SKU-0012', 'SKU-0013', 'SKU-0014', 'SKU-0015',
    # Bakery (5 items)
    'SKU-0002', 'SKU-0021', 'SKU-0022', 'SKU-0023', 'SKU-0024',
    # Electronics (7 items)
    'SKU-0003', 'SKU-0004', 'SKU-0031', 'SKU-0032', 'SKU-0033', 'SKU-0034', 'SKU-0035',
    # Dairy (5 items)
    'SKU-0041', 'SKU-0042', 'SKU-0043', 'SKU-0044', 'SKU-0045',
    # Snacks (5 items)
    'SKU-0051', 'SKU-0052', 'SKU-0053', 'SKU-0054', 'SKU-0055',
    # Fresh Produce (5 items)
    'SKU-0061', 'SKU-0062', 'SKU-0063', 'SKU-0064', 'SKU-0065',
    # Frozen Foods (5 items)
    'SKU-0071', 'SKU-0072', 'SKU-0073', 'SKU-0074', 'SKU-0075',
    # Household (5 items)
    'SKU-0081', 'SKU-0082', 'SKU-0083', 'SKU-0084', 'SKU-0085',
    # Personal Care (5 items)
    'SKU-0091', 'SKU-0092', 'SKU-0093', 'SKU-0094', 'SKU-0095',
]
# Total: 49 products matching the database

def generate_pos_orders(date_str):
    """Generate JSON files for POS orders."""
    for pos_id in range(1, NUM_POS + 1):
        orders = []
        num_orders = random.randint(150, 300)  # Much more orders per store
        
        for _ in range(num_orders):
            order = {
                "order_id": fake.uuid4(),
                "pos_id": f"POS-{pos_id:03d}",
                "timestamp": f"{date_str}T{fake.time()}",
                "items": []
            }
            
            num_items = random.randint(1, 8)  # More items per order
            for _ in range(num_items):
                item = {
                    "sku": random.choice(product_ids),
                    "quantity": random.randint(1, 15),  # Higher quantities
                    "price": round(random.uniform(1.0, 150.0), 2)
                }
                order["items"].append(item)
            orders.append(order)
            
        # Save to JSON
        filename = f"{OUTPUT_DIR_ORDERS}/pos_{pos_id}_{date_str}.json"
        with open(filename, 'w') as f:
            json.dump(orders, f, indent=2)
        print(f"Generated {filename}")

def generate_warehouse_stock(date_str):
    """Generate CSV files for Warehouse stock snapshots."""
    for wh_id in range(1, NUM_WAREHOUSES + 1):
        filename = f"{OUTPUT_DIR_STOCK}/wh_{wh_id}_{date_str}.csv"
        
        with open(filename, 'w', newline='') as f:
            writer = csv.writer(f)
            writer.writerow(["warehouse_id", "date", "sku", "quantity_on_hand"])
            
            for sku in product_ids:
                # Random stock level
                qty = random.randint(0, 500)
                writer.writerow([f"WH-{wh_id:03d}", date_str, sku, qty])
        print(f"Generated {filename}")

if __name__ == "__main__":
    from datetime import timedelta
    
    # Generate data for the last DAYS_TO_GENERATE days
    base_date = datetime.now()
    
    for day_offset in range(DAYS_TO_GENERATE):
        current_date = base_date - timedelta(days=DAYS_TO_GENERATE - day_offset - 1)
        date_str = current_date.strftime("%Y-%m-%d")
        print(f"\nGenerating data for {date_str}...")
        
        generate_pos_orders(date_str)
        generate_warehouse_stock(date_str)
    
    print(f"\nData generation complete! Generated {DAYS_TO_GENERATE} days of data.")
