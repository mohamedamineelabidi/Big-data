"""
Phase 4 Runner: Complete Procurement Output Generation
Orchestrates demand computation, supplier export, and exception reporting

Author: Data Engineering Team
Date: January 2026
"""

import sys
import argparse
from datetime import datetime
from pathlib import Path

# Import phase 4 modules
from compute_demand import DemandAnalyzer
from export_orders import SupplierOrderExporter
from generate_exceptions import ExceptionReporter


class Phase4Runner:
    """Orchestrates all Phase 4 processing"""
    
    def __init__(self, base_path="data"):
        self.base_path = Path(base_path)
        self.start_time = None
        self.end_time = None
        
    def print_header(self, date_str):
        """Print execution header"""
        print("\n" + "="*70)
        print("🚀 PHASE 4: PROCUREMENT OUTPUT GENERATION")
        print("="*70)
        print(f"   Execution Date: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        print(f"   Processing Date: {date_str}")
        print(f"   Base Path: {self.base_path}")
        print("="*70)
        
    def print_footer(self, results):
        """Print execution summary"""
        duration = (self.end_time - self.start_time).total_seconds()
        
        print("\n" + "="*70)
        print("📊 PHASE 4 EXECUTION SUMMARY")
        print("="*70)
        
        if results.get('demand'):
            print(f"\n   💡 Demand Analysis:")
            print(f"      • SKUs requiring replenishment: {results['demand'].get('skus', 'N/A')}")
            print(f"      • Total units to order: {results['demand'].get('units', 'N/A'):,}")
        
        if results.get('export'):
            print(f"\n   📦 Supplier Export:")
            print(f"      • Suppliers: {results['export']['suppliers']}")
            print(f"      • Order files generated: {len(results['export']['files'])}")
        
        if results.get('exceptions'):
            summary = results['exceptions']['summary']
            print(f"\n   ⚠️  Exception Report:")
            print(f"      • Total exceptions: {summary['total_exceptions']}")
            print(f"      • Critical: {summary['by_severity']['CRITICAL']}")
            print(f"      • High: {summary['by_severity']['HIGH']}")
        
        print(f"\n   ⏱️  Total Duration: {duration:.2f} seconds")
        print("="*70)
        
        # Final status
        critical_count = results.get('exceptions', {}).get('summary', {}).get('by_severity', {}).get('CRITICAL', 0)
        if critical_count > 0:
            print("⚠️  COMPLETED WITH CRITICAL ALERTS - Review required!")
        else:
            print("✅ PHASE 4 COMPLETED SUCCESSFULLY")
        print("="*70)
    
    def run(self, date_str, skip_demand=False):
        """Run complete Phase 4 pipeline"""
        self.start_time = datetime.now()
        results = {}
        
        self.print_header(date_str)
        
        try:
            # Step 1: Demand Analysis (optional if already run)
            if not skip_demand:
                print("\n📋 STEP 1/3: Running Demand Analysis...")
                analyzer = DemandAnalyzer(base_path=str(self.base_path))
                demand_result = analyzer.run(date_str=date_str)
                results['demand'] = demand_result
                print("   ✅ Demand analysis complete")
            else:
                print("\n📋 STEP 1/3: Skipping Demand Analysis (using existing data)")
                # Just count existing data
                replenishment_file = self.base_path / "output" / f"replenishment_{date_str}.csv"
                if replenishment_file.exists():
                    import pandas as pd
                    df = pd.read_csv(replenishment_file)
                    results['demand'] = {
                        'skus': len(df),
                        'units': int(df['order_quantity'].sum())
                    }
            
            # Step 2: Export Supplier Orders
            print("\n📋 STEP 2/3: Exporting Supplier Orders...")
            exporter = SupplierOrderExporter(base_path=str(self.base_path))
            export_result = exporter.export_all_suppliers(date_str=date_str)
            results['export'] = export_result
            print("   ✅ Supplier export complete")
            
            # Step 3: Generate Exception Report
            print("\n📋 STEP 3/3: Generating Exception Report...")
            reporter = ExceptionReporter(base_path=str(self.base_path))
            exception_result = reporter.run(date_str=date_str)
            results['exceptions'] = exception_result
            print("   ✅ Exception report complete")
            
            self.end_time = datetime.now()
            self.print_footer(results)
            
            return results
            
        except Exception as e:
            self.end_time = datetime.now()
            print(f"\n❌ PHASE 4 FAILED: {e}")
            import traceback
            traceback.print_exc()
            raise


def main():
    """Main entry point"""
    parser = argparse.ArgumentParser(
        description='Run Phase 4 Procurement Output Generation',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python run_phase4.py                    # Run all steps for today
  python run_phase4.py --date 2026-01-03  # Run for specific date
  python run_phase4.py --skip-demand      # Skip demand computation (use existing data)
        """
    )
    parser.add_argument('--date', default='2026-01-03', help='Date to process (YYYY-MM-DD)')
    parser.add_argument('--skip-demand', action='store_true', 
                       help='Skip demand computation (use existing replenishment data)')
    parser.add_argument('--output', default='data', help='Base output directory')
    
    args = parser.parse_args()
    
    runner = Phase4Runner(base_path=args.output)
    
    try:
        runner.run(date_str=args.date, skip_demand=args.skip_demand)
        return 0
    except Exception as e:
        return 1


if __name__ == "__main__":
    sys.exit(main())
