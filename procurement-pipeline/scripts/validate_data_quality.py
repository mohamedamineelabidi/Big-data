import os
import json
import csv
from datetime import datetime

# Configuration
ORDERS_DIR = "data/raw/orders"
STOCK_DIR = "data/raw/stock"

class DataQualityValidator:
    def __init__(self):
        self.errors = []
        self.warnings = []
        self.stats = {
            'files_checked': 0,
            'orders_validated': 0,
            'stock_records_validated': 0
        }
    
    def validate_orders(self):
        """Validate JSON order files."""
        print("\n=== Validating POS Orders ===")
        
        if not os.path.exists(ORDERS_DIR):
            self.errors.append(f"Orders directory not found: {ORDERS_DIR}")
            return
        
        files = [f for f in os.listdir(ORDERS_DIR) if f.endswith('.json')]
        if not files:
            self.errors.append("No order files found")
            return
        
        for filename in files:
            filepath = os.path.join(ORDERS_DIR, filename)
            self.stats['files_checked'] += 1
            
            # Check file size
            file_size = os.path.getsize(filepath)
            if file_size == 0:
                self.errors.append(f"{filename}: File is empty")
                continue
            
            # Validate JSON structure
            try:
                with open(filepath, 'r') as f:
                    orders = json.load(f)
                
                if not isinstance(orders, list):
                    self.errors.append(f"{filename}: Root element must be an array")
                    continue
                
                # Validate each order
                for idx, order in enumerate(orders):
                    self.validate_order_structure(filename, idx, order)
                    self.stats['orders_validated'] += 1
                
                print(f"✅ {filename}: {len(orders)} orders validated")
                
            except json.JSONDecodeError as e:
                self.errors.append(f"{filename}: Invalid JSON - {str(e)}")
            except Exception as e:
                self.errors.append(f"{filename}: Validation error - {str(e)}")
    
    def validate_order_structure(self, filename, idx, order):
        """Validate individual order structure."""
        required_fields = ['order_id', 'pos_id', 'timestamp', 'items']
        
        for field in required_fields:
            if field not in order:
                self.errors.append(f"{filename} order {idx}: Missing field '{field}'")
        
        # Validate items array
        if 'items' in order:
            if not isinstance(order['items'], list):
                self.errors.append(f"{filename} order {idx}: 'items' must be an array")
            elif len(order['items']) == 0:
                self.warnings.append(f"{filename} order {idx}: Empty items array")
            else:
                for item_idx, item in enumerate(order['items']):
                    self.validate_order_item(filename, idx, item_idx, item)
    
    def validate_order_item(self, filename, order_idx, item_idx, item):
        """Validate order item."""
        required_fields = ['sku', 'quantity', 'price']
        
        for field in required_fields:
            if field not in item:
                self.errors.append(f"{filename} order {order_idx} item {item_idx}: Missing '{field}'")
        
        # Validate data types and ranges
        if 'quantity' in item:
            if not isinstance(item['quantity'], int) or item['quantity'] <= 0:
                self.errors.append(f"{filename} order {order_idx} item {item_idx}: Invalid quantity")
        
        if 'price' in item:
            if not isinstance(item['price'], (int, float)) or item['price'] < 0:
                self.errors.append(f"{filename} order {order_idx} item {item_idx}: Invalid price")
    
    def validate_stock(self):
        """Validate CSV stock files."""
        print("\n=== Validating Warehouse Stock ===")
        
        if not os.path.exists(STOCK_DIR):
            self.errors.append(f"Stock directory not found: {STOCK_DIR}")
            return
        
        files = [f for f in os.listdir(STOCK_DIR) if f.endswith('.csv')]
        if not files:
            self.errors.append("No stock files found")
            return
        
        required_columns = ['warehouse_id', 'date', 'sku', 'quantity_on_hand']
        
        for filename in files:
            filepath = os.path.join(STOCK_DIR, filename)
            self.stats['files_checked'] += 1
            
            # Check file size
            file_size = os.path.getsize(filepath)
            if file_size == 0:
                self.errors.append(f"{filename}: File is empty")
                continue
            
            try:
                with open(filepath, 'r') as f:
                    reader = csv.DictReader(f)
                    
                    # Check headers
                    headers = reader.fieldnames
                    missing_cols = [col for col in required_columns if col not in headers]
                    if missing_cols:
                        self.errors.append(f"{filename}: Missing columns {missing_cols}")
                        continue
                    
                    # Validate rows
                    row_count = 0
                    for row_idx, row in enumerate(reader, start=1):
                        row_count += 1
                        self.stats['stock_records_validated'] += 1
                        
                        # Check for empty required fields
                        for col in required_columns:
                            if not row.get(col, '').strip():
                                self.errors.append(f"{filename} row {row_idx}: Empty '{col}'")
                        
                        # Validate quantity
                        try:
                            qty = int(row.get('quantity_on_hand', 0))
                            if qty < 0:
                                self.warnings.append(f"{filename} row {row_idx}: Negative quantity")
                        except ValueError:
                            self.errors.append(f"{filename} row {row_idx}: Invalid quantity format")
                    
                    print(f"✅ {filename}: {row_count} records validated")
                    
            except Exception as e:
                self.errors.append(f"{filename}: Validation error - {str(e)}")
    
    def generate_report(self):
        """Generate validation report."""
        print("\n" + "="*60)
        print("DATA QUALITY VALIDATION REPORT")
        print("="*60)
        
        print(f"\n📊 Statistics:")
        print(f"   Files Checked: {self.stats['files_checked']}")
        print(f"   Orders Validated: {self.stats['orders_validated']}")
        print(f"   Stock Records Validated: {self.stats['stock_records_validated']}")
        
        if self.errors:
            print(f"\n❌ Errors ({len(self.errors)}):")
            for error in self.errors[:10]:  # Show first 10
                print(f"   - {error}")
            if len(self.errors) > 10:
                print(f"   ... and {len(self.errors) - 10} more errors")
        else:
            print("\n✅ No Errors Found!")
        
        if self.warnings:
            print(f"\n⚠️  Warnings ({len(self.warnings)}):")
            for warning in self.warnings[:10]:  # Show first 10
                print(f"   - {warning}")
            if len(self.warnings) > 10:
                print(f"   ... and {len(self.warnings) - 10} more warnings")
        
        # Write detailed log
        log_dir = "logs"
        os.makedirs(log_dir, exist_ok=True)
        log_file = os.path.join(log_dir, f"data_quality_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log")
        
        with open(log_file, 'w') as f:
            f.write("DATA QUALITY VALIDATION LOG\n")
            f.write(f"Timestamp: {datetime.now()}\n")
            f.write("="*60 + "\n\n")
            
            f.write(f"Statistics:\n")
            for key, value in self.stats.items():
                f.write(f"  {key}: {value}\n")
            
            if self.errors:
                f.write(f"\nErrors ({len(self.errors)}):\n")
                for error in self.errors:
                    f.write(f"  - {error}\n")
            
            if self.warnings:
                f.write(f"\nWarnings ({len(self.warnings)}):\n")
                for warning in self.warnings:
                    f.write(f"  - {warning}\n")
        
        print(f"\n📄 Detailed log saved to: {log_file}")
        
        return len(self.errors) == 0

if __name__ == "__main__":
    print("Starting Data Quality Validation...")
    
    validator = DataQualityValidator()
    validator.validate_orders()
    validator.validate_stock()
    success = validator.generate_report()
    
    if success:
        print("\n✅ All validations passed!")
        exit(0)
    else:
        print("\n❌ Validation failed with errors")
        exit(1)
