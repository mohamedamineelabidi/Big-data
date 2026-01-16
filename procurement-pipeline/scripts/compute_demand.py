"""
Phase 3: Demand Aggregation and Net Demand Calculation
This script analyzes orders and stock to compute replenishment needs
"""

import sys
import os

# Fix Windows console encoding for emojis
if sys.platform == 'win32':
    if sys.stdout.encoding != 'utf-8':
        sys.stdout.reconfigure(encoding='utf-8')
    if sys.stderr.encoding != 'utf-8':
        sys.stderr.reconfigure(encoding='utf-8')

import json
import pandas as pd
from pathlib import Path
from trino.dbapi import connect

class DemandAnalyzer:
    def __init__(self, base_path="data/raw"):
        self.base_path = Path(base_path)
        self.trino_conn = None
        
    def connect_trino(self):
        """Connect to Trino"""
        self.trino_conn = connect(
            host='localhost',
            port=8080,
            user='admin',
            catalog='postgresql',
            schema='public',
            http_scheme='http'
        )
        return self.trino_conn
    
    def load_orders_from_json(self, date_str="2026-01-03"):
        """Load all orders for a specific date from JSON files"""
        print(f"\n📦 Loading Orders for {date_str}...")
        
        orders_path = self.base_path / "orders"
        all_items = []
        
        # Find all JSON files for the date
        order_files = list(orders_path.glob(f"*_{date_str}.json"))
        print(f"   Found {len(order_files)} POS files")
        
        for file in order_files:
            with open(file, 'r') as f:
                orders = json.load(f)
                
            # Flatten nested structure
            for order in orders:
                pos_id = order['pos_id']
                for item in order['items']:
                    all_items.append({
                        'pos_id': pos_id,
                        'sku': item['sku'],
                        'quantity': item['quantity'],
                        'date': date_str
                    })
        
        print(f"   Total order items: {len(all_items)}")
        return pd.DataFrame(all_items)
    
    def load_stock_from_csv(self, date_str="2026-01-03"):
        """Load all stock data for a specific date from CSV files"""
        print(f"\n📊 Loading Stock for {date_str}...")
        
        stock_path = self.base_path / "stock"
        all_stock = []
        
        # Find all CSV files for the date
        stock_files = list(stock_path.glob(f"*_{date_str}.csv"))
        print(f"   Found {len(stock_files)} warehouse files")
        
        for file in stock_files:
            df = pd.read_csv(file)
            all_stock.append(df)
        
        stock_df = pd.concat(all_stock, ignore_index=True)
        print(f"   Total stock records: {len(stock_df)}")
        return stock_df
    
    def aggregate_demand(self, orders_df):
        """Aggregate total demand per SKU"""
        print("\n🔄 Aggregating Demand by SKU...")
        
        demand = orders_df.groupby('sku')['quantity'].sum().reset_index()
        demand.columns = ['sku', 'total_demand']
        demand = demand.sort_values('total_demand', ascending=False)
        
        print(f"   Unique SKUs with demand: {len(demand)}")
        print(f"   Total units ordered: {demand['total_demand'].sum()}")
        
        return demand
    
    def aggregate_stock(self, stock_df):
        """Aggregate total stock per SKU across all warehouses"""
        print("\n📦 Aggregating Stock by SKU...")
        
        stock = stock_df.groupby('sku')['quantity_on_hand'].sum().reset_index()
        stock.columns = ['sku', 'available_stock']
        
        print(f"   Unique SKUs in stock: {len(stock)}")
        print(f"   Total units in stock: {stock['available_stock'].sum()}")
        
        return stock
    
    def get_master_data(self):
        """Get products and replenishment rules from PostgreSQL via Trino"""
        print("\n🔗 Fetching Master Data from PostgreSQL...")
        
        cur = self.trino_conn.cursor()
        
        # Get products with replenishment rules
        query = """
            SELECT 
                p.product_id as sku,
                p.product_name,
                p.category,
                p.case_size,
                r.moq as minimum_order_qty,
                r.safety_stock_level,
                r.supplier_id,
                s.supplier_name
            FROM products p
            LEFT JOIN replenishment_rules r ON p.product_id = r.product_id
            LEFT JOIN suppliers s ON r.supplier_id = s.supplier_id
        """
        
        cur.execute(query)
        columns = [desc[0] for desc in cur.description]
        data = cur.fetchall()
        
        master_df = pd.DataFrame(data, columns=columns)
        print(f"   Loaded {len(master_df)} products with rules")
        
        return master_df
    
    def calculate_net_demand(self, demand_df, stock_df, master_df):
        """
        Calculate Net Replenishment Need:
        Net Demand = Total Demand - Available Stock + Safety Stock
        """
        print("\n💡 Calculating Net Replenishment Needs...")
        
        # Merge demand with stock
        result = demand_df.merge(stock_df, on='sku', how='left')
        result['available_stock'] = result['available_stock'].fillna(0)
        
        # Merge with master data
        result = result.merge(master_df, on='sku', how='left')
        
        # Calculate net demand
        result['net_demand'] = (
            result['total_demand'] 
            - result['available_stock'] 
            + result['safety_stock_level'].fillna(0)
        )
        
        # Only keep positive net demand (items we need to order)
        result = result[result['net_demand'] > 0].copy()
        
        # Drop rows with missing case_size
        result = result.dropna(subset=['case_size'])
        
        # Round up to case size
        result['cases_needed'] = (result['net_demand'] / result['case_size']).apply(lambda x: int(x) + (1 if x % 1 > 0 else 0))
        result['order_quantity'] = result['cases_needed'] * result['case_size']
        
        # Apply minimum order quantity
        result['order_quantity'] = result.apply(
            lambda row: max(row['order_quantity'], row['minimum_order_qty'] or 0),
            axis=1
        )
        
        print(f"   SKUs requiring replenishment: {len(result)}")
        print(f"   Total units to order: {result['order_quantity'].sum()}")
        
        return result
    
    def generate_report(self, replenishment_df, date_str="2026-01-03"):
        """Generate replenishment report"""
        print("\n" + "="*70)
        print(f"📋 REPLENISHMENT REPORT - {date_str}")
        print("="*70)
        
        if len(replenishment_df) == 0:
            print("✅ No replenishment needed!")
            return
        
        # Group by supplier
        print("\n📦 Orders by Supplier:")
        by_supplier = replenishment_df.groupby('supplier_name').agg({
            'sku': 'count',
            'order_quantity': 'sum'
        }).reset_index()
        by_supplier.columns = ['Supplier', 'SKU Count', 'Total Units']
        
        for _, row in by_supplier.iterrows():
            print(f"   • {row['Supplier']}: {row['SKU Count']} SKUs, {row['Total Units']} units")
        
        # Top 10 items to reorder
        print("\n🔝 Top 10 Items to Reorder:")
        top_items = replenishment_df.nlargest(10, 'order_quantity')[
            ['sku', 'product_name', 'category', 'net_demand', 'order_quantity', 'supplier_name']
        ]
        
        for _, row in top_items.iterrows():
            print(f"   • {row['sku']}: {row['product_name']}")
            print(f"     Category: {row['category']}, Net Need: {row['net_demand']:.0f}, Order: {row['order_quantity']:.0f} units")
            print(f"     Supplier: {row['supplier_name']}")
        
        return by_supplier, top_items
    
    def run_analysis(self, date_str="2026-01-03"):
        """Run complete demand analysis"""
        print("="*70)
        print("🚀 STARTING DEMAND ANALYSIS PIPELINE")
        print("="*70)
        
        try:
            # Connect to Trino
            self.connect_trino()
            
            # Step 1: Load raw data
            orders_df = self.load_orders_from_json(date_str)
            stock_df = self.load_stock_from_csv(date_str)
            
            # Step 2: Aggregate
            demand_df = self.aggregate_demand(orders_df)
            stock_agg_df = self.aggregate_stock(stock_df)
            
            # Step 3: Get master data
            master_df = self.get_master_data()
            
            # Step 4: Calculate net demand
            replenishment_df = self.calculate_net_demand(demand_df, stock_agg_df, master_df)
            
            # Step 5: Generate report
            self.generate_report(replenishment_df, date_str)
            
            # Save results
            output_path = Path("output")
            output_path.mkdir(exist_ok=True)
            
            output_file = output_path / f"replenishment_{date_str}.csv"
            replenishment_df.to_csv(output_file, index=False)
            print(f"\n💾 Results saved to: {output_file}")
            
            print("\n✅ Analysis Complete!")
            return replenishment_df
            
        except Exception as e:
            print(f"\n❌ Error during analysis: {e}")
            import traceback
            traceback.print_exc()
            return None

if __name__ == "__main__":
    import argparse
    
    parser = argparse.ArgumentParser(description='Compute procurement demand')
    parser.add_argument('--date', default='2026-01-03', help='Date to process (YYYY-MM-DD)')
    args = parser.parse_args()
    
    analyzer = DemandAnalyzer()
    result = analyzer.run_analysis(date_str=args.date)
