"""
Master Orchestrator: Full Pipeline Execution
=============================================
Standalone script to run the complete procurement pipeline
Can be used with or without Airflow

Usage:
    python run_pipeline.py                      # Run for today
    python run_pipeline.py --date 2026-01-03    # Run for specific date
    python run_pipeline.py --replay 7           # Replay last 7 days

Author: Data Engineering Team
Date: January 2026
"""

import sys
import os
import argparse
import logging
from datetime import datetime, timedelta
from pathlib import Path

# Add scripts directory to path
SCRIPT_DIR = Path(__file__).parent
sys.path.insert(0, str(SCRIPT_DIR))

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s | %(levelname)-8s | %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
logger = logging.getLogger(__name__)


class ProcurementPipeline:
    """
    Master orchestrator for the procurement pipeline.
    Runs all phases in sequence with proper error handling.
    """
    
    def __init__(self, base_path="data"):
        self.base_path = Path(base_path)
        self.output_path = self.base_path / "output"
        self.output_path.mkdir(parents=True, exist_ok=True)
        
        # Pipeline statistics
        self.stats = {
            'start_time': None,
            'end_time': None,
            'status': 'PENDING',
            'stages': {}
        }
    
    def log_stage(self, stage_name, status, details=None):
        """Log stage execution"""
        self.stats['stages'][stage_name] = {
            'status': status,
            'timestamp': datetime.now().isoformat(),
            'details': details or {}
        }
        
        icon = '✅' if status == 'SUCCESS' else '❌' if status == 'FAILED' else '🔄'
        logger.info(f"{icon} Stage '{stage_name}': {status}")
    
    def validate_infrastructure(self):
        """Check that all required services are available"""
        logger.info("Validating infrastructure...")
        
        issues = []
        
        # Check data directories
        orders_path = self.base_path / "raw" / "orders"
        stock_path = self.base_path / "raw" / "stock"
        
        if not orders_path.exists():
            issues.append(f"Orders directory not found: {orders_path}")
        if not stock_path.exists():
            issues.append(f"Stock directory not found: {stock_path}")
        
        # Check for data files
        if orders_path.exists():
            order_files = list(orders_path.glob("*.json"))
            if len(order_files) == 0:
                issues.append("No order JSON files found")
            else:
                logger.info(f"  Found {len(order_files)} order files")
        
        if stock_path.exists():
            stock_files = list(stock_path.glob("*.csv"))
            if len(stock_files) == 0:
                issues.append("No stock CSV files found")
            else:
                logger.info(f"  Found {len(stock_files)} stock files")
        
        # Try Trino connection
        try:
            from trino.dbapi import connect
            conn = connect(
                host='localhost',
                port=8080,
                user='admin',
                catalog='postgresql',
                schema='public'
            )
            cursor = conn.cursor()
            cursor.execute("SELECT 1")
            cursor.fetchone()
            cursor.close()
            conn.close()
            logger.info("  Trino connection: OK")
        except Exception as e:
            issues.append(f"Trino connection failed: {e}")
        
        if issues:
            for issue in issues:
                logger.warning(f"  ⚠️ {issue}")
            return False, issues
        
        return True, []
    
    def run_stage_validation(self, date_str):
        """Stage 1: Data Validation"""
        logger.info(f"Stage 1: Validating data for {date_str}")
        
        try:
            from validate_data_quality import DataValidator
            validator = DataValidator(base_path=str(self.base_path))
            results = validator.validate_all()
            
            self.log_stage('validation', 'SUCCESS', {
                'files_checked': results.get('total_files', 0),
                'passed': results.get('all_passed', False)
            })
            return True, results
        except ImportError:
            # Validation script not available, skip
            logger.warning("  Validation script not found, skipping...")
            self.log_stage('validation', 'SKIPPED')
            return True, {}
        except Exception as e:
            self.log_stage('validation', 'FAILED', {'error': str(e)})
            return False, {'error': str(e)}
    
    def run_stage_demand(self, date_str):
        """Stage 2: Compute Demand"""
        logger.info(f"Stage 2: Computing demand for {date_str}")
        
        try:
            from compute_demand import DemandAnalyzer
            analyzer = DemandAnalyzer(base_path=str(self.base_path / "raw"))
            result_df = analyzer.run_analysis(date_str=date_str)
            
            if result_df is None or result_df.empty:
                raise ValueError("No replenishment data generated")
            
            result = {
                'skus': len(result_df),
                'units': int(result_df['order_quantity'].sum()) if 'order_quantity' in result_df.columns else 0
            }
            
            self.log_stage('demand_computation', 'SUCCESS', result)
            return True, result
        except Exception as e:
            self.log_stage('demand_computation', 'FAILED', {'error': str(e)})
            logger.exception("Demand computation failed")
            return False, {'error': str(e)}
    
    def run_stage_export(self, date_str):
        """Stage 3: Export Supplier Orders"""
        logger.info(f"Stage 3: Exporting supplier orders for {date_str}")
        
        try:
            from export_orders import SupplierOrderExporter
            exporter = SupplierOrderExporter(base_path=str(self.base_path))
            result = exporter.export_all_suppliers(date_str=date_str)
            
            self.log_stage('supplier_export', 'SUCCESS', {
                'suppliers': result.get('suppliers', 0),
                'total_units': result.get('total_units', 0),
                'files': len(result.get('files', []))
            })
            return True, result
        except Exception as e:
            self.log_stage('supplier_export', 'FAILED', {'error': str(e)})
            logger.exception("Supplier export failed")
            return False, {'error': str(e)}
    
    def run_stage_exceptions(self, date_str):
        """Stage 4: Generate Exception Report"""
        logger.info(f"Stage 4: Generating exception report for {date_str}")
        
        try:
            from generate_exceptions import ExceptionReporter
            reporter = ExceptionReporter(base_path=str(self.base_path))
            result = reporter.run(date_str=date_str)
            
            summary = result.get('summary', {})
            self.log_stage('exception_report', 'SUCCESS', {
                'total_exceptions': summary.get('total_exceptions', 0),
                'critical': summary.get('by_severity', {}).get('CRITICAL', 0),
                'high': summary.get('by_severity', {}).get('HIGH', 0)
            })
            return True, result
        except Exception as e:
            self.log_stage('exception_report', 'FAILED', {'error': str(e)})
            logger.exception("Exception report failed")
            return False, {'error': str(e)}
    
    def generate_summary(self, date_str, results):
        """Generate pipeline execution summary"""
        duration = (self.stats['end_time'] - self.stats['start_time']).total_seconds()
        
        summary_lines = [
            "",
            "═" * 70,
            "              PROCUREMENT PIPELINE EXECUTION SUMMARY",
            "═" * 70,
            f"  Processing Date:    {date_str}",
            f"  Execution Start:    {self.stats['start_time'].strftime('%Y-%m-%d %H:%M:%S')}",
            f"  Execution End:      {self.stats['end_time'].strftime('%Y-%m-%d %H:%M:%S')}",
            f"  Duration:           {duration:.2f} seconds",
            f"  Status:             {self.stats['status']}",
            "─" * 70,
            "  STAGE RESULTS:",
        ]
        
        for stage_name, stage_info in self.stats['stages'].items():
            icon = '✅' if stage_info['status'] == 'SUCCESS' else '❌' if stage_info['status'] == 'FAILED' else '⏭️'
            summary_lines.append(f"    {icon} {stage_name}: {stage_info['status']}")
            
            # Add key metrics
            details = stage_info.get('details', {})
            for key, value in details.items():
                if key != 'error':
                    summary_lines.append(f"       • {key}: {value}")
        
        summary_lines.extend([
            "─" * 70,
            "  OUTPUT FILES:",
            f"    📄 Replenishment: data/output/replenishment_{date_str}.csv",
            f"    📁 Supplier Orders: data/output/supplier_orders/",
            f"    📁 Exceptions: data/output/exceptions/",
            "═" * 70,
        ])
        
        summary = "\n".join(summary_lines)
        
        # Save summary
        summary_file = self.output_path / f"pipeline_run_{date_str}.txt"
        with open(summary_file, 'w', encoding='utf-8') as f:
            f.write(summary)
        
        print(summary)
        logger.info(f"Summary saved to: {summary_file}")
        
        return summary
    
    def run(self, date_str, skip_validation=False):
        """Execute the complete pipeline"""
        self.stats['start_time'] = datetime.now()
        
        print("\n" + "═" * 70)
        print("        🚀 PROCUREMENT PIPELINE - MASTER ORCHESTRATOR")
        print("═" * 70)
        print(f"  Date: {date_str}")
        print(f"  Started: {self.stats['start_time'].strftime('%Y-%m-%d %H:%M:%S')}")
        print("═" * 70 + "\n")
        
        results = {}
        
        try:
            # Pre-flight check
            valid, issues = self.validate_infrastructure()
            if not valid:
                logger.error("Infrastructure validation failed!")
                self.stats['status'] = 'FAILED'
                self.stats['end_time'] = datetime.now()
                return False, results
            
            # Stage 1: Validation
            if not skip_validation:
                success, result = self.run_stage_validation(date_str)
                results['validation'] = result
            
            # Stage 2: Demand Computation
            success, result = self.run_stage_demand(date_str)
            results['demand'] = result
            if not success:
                raise RuntimeError("Demand computation failed")
            
            # Stage 3: Supplier Export
            success, result = self.run_stage_export(date_str)
            results['export'] = result
            if not success:
                raise RuntimeError("Supplier export failed")
            
            # Stage 4: Exception Report
            success, result = self.run_stage_exceptions(date_str)
            results['exceptions'] = result
            if not success:
                raise RuntimeError("Exception report failed")
            
            self.stats['status'] = 'SUCCESS'
            
        except Exception as e:
            self.stats['status'] = 'FAILED'
            logger.exception(f"Pipeline failed: {e}")
        
        finally:
            self.stats['end_time'] = datetime.now()
            self.generate_summary(date_str, results)
        
        return self.stats['status'] == 'SUCCESS', results
    
    def replay_dates(self, days_back):
        """Replay pipeline for multiple historical dates"""
        today = datetime.now()
        results = {}
        
        print(f"\n📅 Replaying pipeline for last {days_back} days...\n")
        
        for i in range(days_back, 0, -1):
            target_date = today - timedelta(days=i)
            date_str = target_date.strftime('%Y-%m-%d')
            
            print(f"\n{'─' * 70}")
            print(f"  Processing: {date_str} ({i} days ago)")
            print(f"{'─' * 70}")
            
            success, result = self.run(date_str, skip_validation=True)
            results[date_str] = {
                'success': success,
                'result': result
            }
        
        # Summary of replay
        successful = sum(1 for r in results.values() if r['success'])
        print(f"\n📊 Replay Summary: {successful}/{days_back} successful")
        
        return results


def main():
    """Main entry point with CLI support"""
    parser = argparse.ArgumentParser(
        description='Procurement Pipeline Master Orchestrator',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python run_pipeline.py                       # Run for default date (2026-01-03)
  python run_pipeline.py --date 2026-01-03     # Run for specific date
  python run_pipeline.py --replay 7            # Replay last 7 days
  python run_pipeline.py --validate-only       # Only validate infrastructure
        """
    )
    
    parser.add_argument(
        '--date', 
        default='2026-01-03',
        help='Date to process (YYYY-MM-DD format)'
    )
    parser.add_argument(
        '--replay', 
        type=int, 
        default=0,
        help='Replay pipeline for last N days'
    )
    parser.add_argument(
        '--output', 
        default='data',
        help='Base output directory'
    )
    parser.add_argument(
        '--validate-only',
        action='store_true',
        help='Only validate infrastructure, do not run pipeline'
    )
    parser.add_argument(
        '--skip-validation',
        action='store_true',
        help='Skip data validation stage'
    )
    
    args = parser.parse_args()
    
    pipeline = ProcurementPipeline(base_path=args.output)
    
    # Validation only mode
    if args.validate_only:
        print("\n🔍 Running infrastructure validation only...\n")
        valid, issues = pipeline.validate_infrastructure()
        if valid:
            print("\n✅ All infrastructure checks passed!")
            return 0
        else:
            print(f"\n❌ Validation failed with {len(issues)} issues")
            return 1
    
    # Replay mode
    if args.replay > 0:
        results = pipeline.replay_dates(args.replay)
        successful = sum(1 for r in results.values() if r['success'])
        return 0 if successful == len(results) else 1
    
    # Normal execution
    success, _ = pipeline.run(date_str=args.date, skip_validation=args.skip_validation)
    return 0 if success else 1


if __name__ == "__main__":
    sys.exit(main())
