import json
import csv
import os
import random
from datetime import datetime, timedelta
from faker import Faker

# Initialize Faker
fake = Faker()

# Configuration
NUM_PRODUCTS = 50
NUM_POS = 5
NUM_WAREHOUSES = 2
DAYS_TO_GENERATE = 1
OUTPUT_DIR_ORDERS = "data/raw/orders"
OUTPUT_DIR_STOCK = "data/raw/stock"

# Ensure output directories exist
os.makedirs(OUTPUT_DIR_ORDERS, exist_ok=True)
os.makedirs(OUTPUT_DIR_STOCK, exist_ok=True)

# Generate Product IDs (SKUs)
product_ids = [f"SKU-{i:04d}" for i in range(1, NUM_PRODUCTS + 1)]

def generate_pos_orders(date_str):
    """Generate JSON files for POS orders."""
    for pos_id in range(1, NUM_POS + 1):
        orders = []
        num_orders = random.randint(10, 50)
        
        for _ in range(num_orders):
            order = {
                "order_id": fake.uuid4(),
                "pos_id": f"POS-{pos_id:03d}",
                "timestamp": f"{date_str}T{fake.time()}",
                "items": []
            }
            
            num_items = random.randint(1, 5)
            for _ in range(num_items):
                item = {
                    "sku": random.choice(product_ids),
                    "quantity": random.randint(1, 10),
                    "price": round(random.uniform(10.0, 100.0), 2)
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
    today = datetime.now().strftime("%Y-%m-%d")
    print(f"Generating data for {today}...")
    
    generate_pos_orders(today)
    generate_warehouse_stock(today)
    
    print("Data generation complete.")
