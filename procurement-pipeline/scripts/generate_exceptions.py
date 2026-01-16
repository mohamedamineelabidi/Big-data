"""
Phase 4.3: Exception Reporting
Generates reports for anomalies: demand spikes, low stock, missing suppliers

Author: Data Engineering Team
Date: January 2026
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
from datetime import datetime
import argparse
import sys


class ExceptionReporter:
    """Detects and reports anomalies in the procurement data"""
    
    # Thresholds for exception detection
    HIGH_DEMAND_THRESHOLD = 2000  # Units that trigger high demand alert
    LOW_STOCK_RATIO = 0.3  # Alert if stock < 30% of demand
    CRITICAL_STOCK_RATIO = 0.1  # Critical if stock < 10% of demand
    HIGH_VALUE_UNITS = 5000  # Units that trigger high-value order alert
    
    SEVERITY_LEVELS = {
        'CRITICAL': 1,
        'HIGH': 2,
        'MEDIUM': 3,
        'LOW': 4
    }
    
    def __init__(self, base_path="."):
        self.base_path = Path(base_path)
        self.output_path = Path("logs")
        self.output_path.mkdir(parents=True, exist_ok=True)
        
    def load_replenishment_data(self, date_str):
        """Load the replenishment CSV for a given date"""
        input_file = Path("output") / f"replenishment_{date_str}.csv"
        
        if not input_file.exists():
            raise FileNotFoundError(f"Replenishment file not found: {input_file}")
        
        df = pd.read_csv(input_file)
        print(f"   📄 Loaded {len(df)} records for analysis")
        return df
    
    def detect_high_demand(self, df):
        """Detect SKUs with unusually high demand"""
        exceptions = []
        high_demand = df[df['total_demand'] > self.HIGH_DEMAND_THRESHOLD]
        
        for _, row in high_demand.iterrows():
            severity = 'CRITICAL' if row['total_demand'] > self.HIGH_DEMAND_THRESHOLD * 1.5 else 'HIGH'
            exceptions.append({
                'type': 'HIGH_DEMAND',
                'severity': severity,
                'sku': row['sku'],
                'product_name': row.get('product_name', 'Unknown'),
                'category': row.get('category', 'Unknown'),
                'metric_value': float(row['total_demand']),
                'threshold': self.HIGH_DEMAND_THRESHOLD,
                'description': f"Demand of {int(row['total_demand'])} units exceeds threshold of {self.HIGH_DEMAND_THRESHOLD}",
                'recommendation': 'Consider expedited supplier contact or alternative sourcing',
                'supplier': row.get('supplier_name', 'Unknown')
            })
        
        return exceptions
    
    def detect_low_stock(self, df):
        """Detect SKUs with critically low stock relative to demand"""
        exceptions = []
        
        for _, row in df.iterrows():
            if row['total_demand'] > 0:
                stock_ratio = row['available_stock'] / row['total_demand']
                
                if stock_ratio < self.CRITICAL_STOCK_RATIO:
                    severity = 'CRITICAL'
                    reason = f"Stock at {stock_ratio:.1%} of demand (critical < {self.CRITICAL_STOCK_RATIO:.0%})"
                elif stock_ratio < self.LOW_STOCK_RATIO:
                    severity = 'HIGH'
                    reason = f"Stock at {stock_ratio:.1%} of demand (low < {self.LOW_STOCK_RATIO:.0%})"
                else:
                    continue
                
                exceptions.append({
                    'type': 'LOW_STOCK',
                    'severity': severity,
                    'sku': row['sku'],
                    'product_name': row.get('product_name', 'Unknown'),
                    'category': row.get('category', 'Unknown'),
                    'metric_value': float(stock_ratio),
                    'threshold': self.LOW_STOCK_RATIO if severity == 'HIGH' else self.CRITICAL_STOCK_RATIO,
                    'description': reason,
                    'available_stock': int(row['available_stock']),
                    'total_demand': int(row['total_demand']),
                    'recommendation': 'Prioritize replenishment; consider safety stock review',
                    'supplier': row.get('supplier_name', 'Unknown')
                })
        
        return exceptions
    
    def detect_missing_supplier(self, df):
        """Detect items with missing or null supplier information"""
        exceptions = []
        missing = df[df['supplier_name'].isna() | (df['supplier_name'] == '')]
        
        for _, row in missing.iterrows():
            exceptions.append({
                'type': 'MISSING_SUPPLIER',
                'severity': 'HIGH',
                'sku': row['sku'],
                'product_name': row.get('product_name', 'Unknown'),
                'category': row.get('category', 'Unknown'),
                'metric_value': None,
                'threshold': None,
                'description': 'No supplier assigned to this SKU',
                'recommendation': 'Update master data with valid supplier assignment',
                'order_quantity_at_risk': int(row.get('order_quantity', 0))
            })
        
        return exceptions
    
    def detect_high_value_orders(self, df):
        """Detect individual SKUs with very high order quantities"""
        exceptions = []
        high_orders = df[df['order_quantity'] > self.HIGH_VALUE_UNITS]
        
        for _, row in high_orders.iterrows():
            exceptions.append({
                'type': 'HIGH_VALUE_ORDER',
                'severity': 'MEDIUM',
                'sku': row['sku'],
                'product_name': row.get('product_name', 'Unknown'),
                'category': row.get('category', 'Unknown'),
                'metric_value': int(row['order_quantity']),
                'threshold': self.HIGH_VALUE_UNITS,
                'description': f"Order of {int(row['order_quantity'])} units is high-value",
                'recommendation': 'Verify capacity with supplier; consider split delivery',
                'supplier': row.get('supplier_name', 'Unknown'),
                'cases_needed': int(row.get('cases_needed', 0))
            })
        
        return exceptions
    
    def detect_large_net_demand_gap(self, df):
        """Detect SKUs where net demand is significantly higher than available stock"""
        exceptions = []
        
        for _, row in df.iterrows():
            if row['available_stock'] > 0:
                gap_ratio = row['net_demand'] / row['available_stock']
                
                if gap_ratio > 3:  # Net demand is more than 3x available stock
                    exceptions.append({
                        'type': 'DEMAND_STOCK_GAP',
                        'severity': 'MEDIUM',
                        'sku': row['sku'],
                        'product_name': row.get('product_name', 'Unknown'),
                        'category': row.get('category', 'Unknown'),
                        'metric_value': float(gap_ratio),
                        'threshold': 3.0,
                        'description': f"Net demand is {gap_ratio:.1f}x available stock",
                        'net_demand': int(row['net_demand']),
                        'available_stock': int(row['available_stock']),
                        'recommendation': 'Review demand forecast accuracy and stock replenishment frequency',
                        'supplier': row.get('supplier_name', 'Unknown')
                    })
        
        return exceptions
    
    def generate_summary_stats(self, df, exceptions):
        """Generate summary statistics"""
        return {
            'total_skus_analyzed': len(df),
            'total_exceptions': len(exceptions),
            'by_severity': {
                'CRITICAL': len([e for e in exceptions if e['severity'] == 'CRITICAL']),
                'HIGH': len([e for e in exceptions if e['severity'] == 'HIGH']),
                'MEDIUM': len([e for e in exceptions if e['severity'] == 'MEDIUM']),
                'LOW': len([e for e in exceptions if e['severity'] == 'LOW'])
            },
            'by_type': {
                'HIGH_DEMAND': len([e for e in exceptions if e['type'] == 'HIGH_DEMAND']),
                'LOW_STOCK': len([e for e in exceptions if e['type'] == 'LOW_STOCK']),
                'MISSING_SUPPLIER': len([e for e in exceptions if e['type'] == 'MISSING_SUPPLIER']),
                'HIGH_VALUE_ORDER': len([e for e in exceptions if e['type'] == 'HIGH_VALUE_ORDER']),
                'DEMAND_STOCK_GAP': len([e for e in exceptions if e['type'] == 'DEMAND_STOCK_GAP'])
            },
            'total_demand': int(df['total_demand'].sum()),
            'total_order_quantity': int(df['order_quantity'].sum()),
            'unique_suppliers': df['supplier_name'].nunique()
        }
    
    def create_exception_report(self, date_str):
        """Create complete exception report"""
        print("\n" + "="*70)
        print("⚠️  EXCEPTION REPORT GENERATOR")
        print("="*70)
        print(f"   Date: {date_str}")
        
        # Load data
        df = self.load_replenishment_data(date_str)
        
        # Collect all exceptions
        all_exceptions = []
        
        print("\n   🔍 Analyzing exceptions...")
        
        # Run detections
        high_demand = self.detect_high_demand(df)
        print(f"   • High Demand: {len(high_demand)} alerts")
        all_exceptions.extend(high_demand)
        
        low_stock = self.detect_low_stock(df)
        print(f"   • Low Stock: {len(low_stock)} alerts")
        all_exceptions.extend(low_stock)
        
        missing_supplier = self.detect_missing_supplier(df)
        print(f"   • Missing Supplier: {len(missing_supplier)} alerts")
        all_exceptions.extend(missing_supplier)
        
        high_value = self.detect_high_value_orders(df)
        print(f"   • High Value Orders: {len(high_value)} alerts")
        all_exceptions.extend(high_value)
        
        demand_gap = self.detect_large_net_demand_gap(df)
        print(f"   • Demand-Stock Gap: {len(demand_gap)} alerts")
        all_exceptions.extend(demand_gap)
        
        # Sort by severity
        all_exceptions.sort(key=lambda x: self.SEVERITY_LEVELS.get(x['severity'], 99))
        
        # Generate summary
        summary = self.generate_summary_stats(df, all_exceptions)
        
        # Build report
        report = {
            'report_date': date_str,
            'generated_at': datetime.now().isoformat(),
            'pipeline_version': '1.0',
            'summary': summary,
            'exceptions': all_exceptions
        }
        
        return report
    
    def save_json_report(self, report, date_str):
        """Save report as JSON"""
        filepath = self.output_path / f"exception_report_{date_str}.json"
        with open(filepath, 'w', encoding='utf-8') as f:
            json.dump(report, f, indent=2, ensure_ascii=False)
        return filepath
    
    def save_text_summary(self, report, date_str):
        """Save human-readable summary"""
        filepath = self.output_path / f"exception_summary_{date_str}.txt"
        
        lines = []
        lines.append("=" * 70)
        lines.append("PROCUREMENT EXCEPTION REPORT")
        lines.append("=" * 70)
        lines.append(f"Report Date: {date_str}")
        lines.append(f"Generated: {report['generated_at']}")
        lines.append("")
        
        # Summary section
        lines.append("-" * 70)
        lines.append("SUMMARY")
        lines.append("-" * 70)
        summary = report['summary']
        lines.append(f"SKUs Analyzed: {summary['total_skus_analyzed']}")
        lines.append(f"Total Exceptions: {summary['total_exceptions']}")
        lines.append(f"Total Demand: {summary['total_demand']:,} units")
        lines.append(f"Total Order Quantity: {summary['total_order_quantity']:,} units")
        lines.append("")
        
        # Severity breakdown
        lines.append("By Severity:")
        for severity, count in summary['by_severity'].items():
            if count > 0:
                lines.append(f"  • {severity}: {count}")
        lines.append("")
        
        # Type breakdown
        lines.append("By Type:")
        for type_name, count in summary['by_type'].items():
            if count > 0:
                lines.append(f"  • {type_name}: {count}")
        lines.append("")
        
        # Critical and High exceptions detail
        critical_high = [e for e in report['exceptions'] 
                        if e['severity'] in ('CRITICAL', 'HIGH')]
        
        if critical_high:
            lines.append("-" * 70)
            lines.append("CRITICAL & HIGH PRIORITY EXCEPTIONS")
            lines.append("-" * 70)
            for exc in critical_high:
                lines.append("")
                lines.append(f"[{exc['severity']}] {exc['type']}")
                lines.append(f"  SKU: {exc['sku']} - {exc.get('product_name', 'Unknown')}")
                lines.append(f"  Description: {exc['description']}")
                lines.append(f"  Recommendation: {exc['recommendation']}")
                if exc.get('supplier'):
                    lines.append(f"  Supplier: {exc['supplier']}")
        
        lines.append("")
        lines.append("=" * 70)
        lines.append("END OF REPORT")
        lines.append("=" * 70)
        
        with open(filepath, 'w', encoding='utf-8') as f:
            f.write('\n'.join(lines))
        
        return filepath
    
    def print_console_summary(self, report):
        """Print summary to console"""
        summary = report['summary']
        
        print("\n" + "-"*70)
        print("📊 EXCEPTION SUMMARY")
        print("-"*70)
        print(f"   • Total Exceptions: {summary['total_exceptions']}")
        
        if summary['by_severity']['CRITICAL'] > 0:
            print(f"   • 🔴 CRITICAL: {summary['by_severity']['CRITICAL']}")
        if summary['by_severity']['HIGH'] > 0:
            print(f"   • 🟠 HIGH: {summary['by_severity']['HIGH']}")
        if summary['by_severity']['MEDIUM'] > 0:
            print(f"   • 🟡 MEDIUM: {summary['by_severity']['MEDIUM']}")
        if summary['by_severity']['LOW'] > 0:
            print(f"   • 🟢 LOW: {summary['by_severity']['LOW']}")
    
    def run(self, date_str):
        """Run complete exception reporting"""
        # Generate report
        report = self.create_exception_report(date_str)
        
        # Save outputs
        json_path = self.save_json_report(report, date_str)
        text_path = self.save_text_summary(report, date_str)
        
        # Print console summary
        self.print_console_summary(report)
        
        print("\n   📁 Output Files:")
        print(f"      • JSON: {json_path}")
        print(f"      • Text: {text_path}")
        
        return report


def main():
    """Main entry point"""
    parser = argparse.ArgumentParser(description='Generate procurement exception report')
    parser.add_argument('--date', default='2026-01-03', help='Date to analyze (YYYY-MM-DD)')
    parser.add_argument('--output', default='data', help='Base output directory')
    
    args = parser.parse_args()
    
    reporter = ExceptionReporter(base_path=args.output)
    
    try:
        report = reporter.run(date_str=args.date)
        total = report['summary']['total_exceptions']
        critical = report['summary']['by_severity']['CRITICAL']
        
        print(f"\n✅ Exception Report Complete! Found {total} exceptions ({critical} critical).")
        return 0 if critical == 0 else 1  # Non-zero exit if critical issues
        
    except FileNotFoundError as e:
        print(f"\n❌ Error: {e}")
        return 1
    except Exception as e:
        print(f"\n❌ Unexpected error: {e}")
        import traceback
        traceback.print_exc()
        return 1


if __name__ == "__main__":
    sys.exit(main())
