import json
import os
from datetime import datetime
from compute_demand import calculate_procurement

# Configuration
OUTPUT_DIR = 'data/output/supplier_orders'

def generate_supplier_orders(date_str):
    """
    Generate JSON order files grouped by supplier.
    """
    # 1. Get calculated orders
    all_orders = calculate_procurement(date_str)
    
    if not all_orders:
        print(f"No orders to generate for {date_str}.")
        return

    # 2. Group by Supplier
    orders_by_supplier = {}
    for order in all_orders:
        supplier_id = order.get('supplier_id')
        if not supplier_id:
            supplier_id = "UNKNOWN_SUPPLIER"
            
        if supplier_id not in orders_by_supplier:
            orders_by_supplier[supplier_id] = []
            
        orders_by_supplier[supplier_id].append(order)
        
    # 3. Write to JSON files
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    
    for supplier_id, items in orders_by_supplier.items():
        filename = f"{OUTPUT_DIR}/order_{supplier_id}_{date_str}.json"
        
        order_document = {
            "order_date": date_str,
            "supplier_id": supplier_id,
            "total_items": len(items),
            "items": items
        }
        
        with open(filename, 'w') as f:
            json.dump(order_document, f, indent=2)
            
        print(f"Generated order file: {filename}")

if __name__ == "__main__":
    today = datetime.now().strftime("%Y-%m-%d")
    print(f"Generating supplier orders for {today}...")
    generate_supplier_orders(today)
    print("Order generation complete.")
