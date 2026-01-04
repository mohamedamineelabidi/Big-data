"""
Phase 4.2: Supplier Order Export
Transforms replenishment analysis into JSON orders per supplier

Author: Data Engineering Team
Date: January 2026
"""

import json
import pandas as pd
from pathlib import Path
from datetime import datetime, timedelta
import argparse
import sys


class SupplierOrderExporter:
    """Exports replenishment data as JSON orders grouped by supplier"""
    
    def __init__(self, base_path="data"):
        self.base_path = Path(base_path)
        self.output_path = self.base_path / "output" / "supplier_orders"
        self.output_path.mkdir(parents=True, exist_ok=True)
        
    def load_replenishment_data(self, date_str):
        """Load the replenishment CSV for a given date"""
        input_file = self.base_path / "output" / f"replenishment_{date_str}.csv"
        
        if not input_file.exists():
            raise FileNotFoundError(f"Replenishment file not found: {input_file}")
        
        df = pd.read_csv(input_file)
        print(f"   📄 Loaded {len(df)} replenishment records")
        return df
    
    def generate_order_id(self, supplier_id, date_str, sequence=1):
        """Generate unique order ID"""
        date_compact = date_str.replace("-", "")
        return f"ORD-{date_compact}-{supplier_id[-3:]}-{sequence:03d}"
    
    def calculate_delivery_date(self, order_date_str, lead_days=2):
        """Calculate expected delivery date"""
        order_date = datetime.strptime(order_date_str, "%Y-%m-%d")
        delivery_date = order_date + timedelta(days=lead_days)
        return delivery_date.strftime("%Y-%m-%d")
    
    def create_supplier_order(self, supplier_name, supplier_id, items_df, date_str):
        """Create order JSON structure for a single supplier"""
        
        order = {
            "order_id": self.generate_order_id(supplier_id or "UNK", date_str),
            "supplier_id": supplier_id,
            "supplier_name": supplier_name,
            "order_date": date_str,
            "requested_delivery_date": self.calculate_delivery_date(date_str),
            "status": "PENDING",
            "priority": "NORMAL",
            "items": [],
            "summary": {
                "total_line_items": 0,
                "total_units": 0,
                "total_cases": 0
            },
            "metadata": {
                "generated_by": "procurement_pipeline",
                "generation_timestamp": datetime.now().isoformat(),
                "pipeline_version": "1.0",
                "source_file": f"replenishment_{date_str}.csv"
            }
        }
        
        # Sort items by SKU for consistency
        items_df = items_df.sort_values('sku')
        
        line_number = 1
        total_units = 0
        total_cases = 0
        
        for _, row in items_df.iterrows():
            item = {
                "line_number": line_number,
                "sku": row['sku'],
                "product_name": row.get('product_name', 'Unknown'),
                "category": row.get('category', 'Unknown'),
                "quantity_ordered": int(row['order_quantity']),
                "cases": int(row['cases_needed']),
                "case_size": int(row.get('case_size', 1)),
                "net_demand": float(row.get('net_demand', 0)),
                "available_stock": float(row.get('available_stock', 0)),
                "total_demand": float(row.get('total_demand', 0))
            }
            
            order["items"].append(item)
            total_units += item["quantity_ordered"]
            total_cases += item["cases"]
            line_number += 1
        
        order["summary"]["total_line_items"] = len(order["items"])
        order["summary"]["total_units"] = total_units
        order["summary"]["total_cases"] = total_cases
        
        # Set priority based on volume
        if total_units > 5000:
            order["priority"] = "HIGH"
        elif total_units > 2000:
            order["priority"] = "MEDIUM"
        
        return order
    
    def save_order_json(self, order, supplier_name, date_str):
        """Save order to JSON file"""
        # Create safe filename
        safe_name = supplier_name.replace(" ", "_").replace("/", "-")
        filename = f"{safe_name}_{date_str}.json"
        filepath = self.output_path / filename
        
        with open(filepath, 'w', encoding='utf-8') as f:
            json.dump(order, f, indent=2, ensure_ascii=False)
        
        return filepath
    
    def export_all_suppliers(self, date_str):
        """Export orders for all suppliers"""
        print("\n" + "="*70)
        print("📦 SUPPLIER ORDER EXPORT")
        print("="*70)
        print(f"   Date: {date_str}")
        
        # Load data
        df = self.load_replenishment_data(date_str)
        
        # Filter out rows with missing supplier
        valid_df = df[df['supplier_name'].notna()]
        invalid_count = len(df) - len(valid_df)
        
        if invalid_count > 0:
            print(f"   ⚠️  Skipping {invalid_count} items with missing supplier")
        
        # Group by supplier
        suppliers = valid_df.groupby(['supplier_name', 'supplier_id'])
        
        results = []
        print(f"\n   📋 Generating {len(suppliers)} supplier orders...")
        
        for (supplier_name, supplier_id), group in suppliers:
            # Create order
            order = self.create_supplier_order(
                supplier_name=supplier_name,
                supplier_id=supplier_id,
                items_df=group,
                date_str=date_str
            )
            
            # Save to file
            filepath = self.save_order_json(order, supplier_name, date_str)
            
            results.append({
                'supplier': supplier_name,
                'items': order['summary']['total_line_items'],
                'units': order['summary']['total_units'],
                'priority': order['priority'],
                'file': str(filepath)
            })
            
            print(f"   ✅ {supplier_name}: {order['summary']['total_line_items']} items, "
                  f"{order['summary']['total_units']} units [{order['priority']}]")
        
        # Summary
        print("\n" + "-"*70)
        print("📊 EXPORT SUMMARY")
        print("-"*70)
        total_items = sum(r['items'] for r in results)
        total_units = sum(r['units'] for r in results)
        print(f"   • Suppliers: {len(results)}")
        print(f"   • Total SKUs: {total_items}")
        print(f"   • Total Units: {total_units:,}")
        print(f"   • Output Directory: {self.output_path}")
        
        return {
            'suppliers': len(results),
            'total_items': total_items,
            'total_units': total_units,
            'files': [r['file'] for r in results],
            'details': results
        }


def main():
    """Main entry point with CLI support"""
    parser = argparse.ArgumentParser(description='Export supplier orders from replenishment data')
    parser.add_argument('--date', default='2026-01-03', help='Date to process (YYYY-MM-DD)')
    parser.add_argument('--output', default='data', help='Base output directory')
    
    args = parser.parse_args()
    
    exporter = SupplierOrderExporter(base_path=args.output)
    
    try:
        result = exporter.export_all_suppliers(date_str=args.date)
        print(f"\n✅ Export Complete! Generated {result['suppliers']} supplier orders.")
        return 0
    except FileNotFoundError as e:
        print(f"\n❌ Error: {e}")
        print("   Run compute_demand.py first to generate replenishment data.")
        return 1
    except Exception as e:
        print(f"\n❌ Unexpected error: {e}")
        import traceback
        traceback.print_exc()
        return 1


if __name__ == "__main__":
    sys.exit(main())
