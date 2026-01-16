"""
Complete Pipeline Execution with Automatic HDFS Ingestion
Runs the procurement pipeline and uploads all outputs to HDFS

Author: Data Engineering Team
Date: January 2026
"""

import sys
import argparse
import subprocess
from datetime import datetime
from pathlib import Path


def run_command(cmd, description):
    """Execute a command and handle errors"""
    print(f"\n{'='*70}")
    print(f"🔹 {description}")
    print(f"{'='*70}")
    
    try:
        result = subprocess.run(
            cmd, 
            shell=True, 
            check=True, 
            capture_output=True, 
            text=True
        )
        print(result.stdout)
        if result.stderr:
            print("STDERR:", result.stderr)
        return True
    except subprocess.CalledProcessError as e:
        print(f"❌ Error: {e}")
        print(f"STDOUT: {e.stdout}")
        print(f"STDERR: {e.stderr}")
        return False


def main():
    """Main pipeline orchestration with HDFS upload"""
    parser = argparse.ArgumentParser(
        description='Run procurement pipeline with automatic HDFS ingestion'
    )
    parser.add_argument(
        '--date', 
        default=datetime.now().strftime('%Y-%m-%d'),
        help='Date to process (YYYY-MM-DD)'
    )
    parser.add_argument(
        '--skip-pipeline', 
        action='store_true',
        help='Skip pipeline execution, only upload to HDFS'
    )
    parser.add_argument(
        '--skip-hdfs', 
        action='store_true',
        help='Skip HDFS upload, only run pipeline'
    )
    
    args = parser.parse_args()
    date_str = args.date
    
    print(f"""
╔══════════════════════════════════════════════════════════════════════╗
║           PROCUREMENT PIPELINE WITH HDFS INGESTION                   ║
║           Date: {date_str}                                      ║
╚══════════════════════════════════════════════════════════════════════╝
""")
    
    success = True
    
    # =====================================================================
    # PHASE 1: RUN PROCUREMENT PIPELINE
    # =====================================================================
    if not args.skip_pipeline:
        print("\n📊 PHASE 1: EXECUTING PROCUREMENT PIPELINE")
        print("="*70)
        
        # Step 1: Compute Demand
        if not run_command(
            f"python scripts/compute_demand.py --date {date_str}",
            "Step 1: Computing Net Demand"
        ):
            success = False
        
        # Step 2: Export Orders
        if success and not run_command(
            f"python scripts/export_orders.py --date {date_str}",
            "Step 2: Exporting Supplier Orders"
        ):
            success = False
        
        # Step 3: Generate Exceptions
        if success and not run_command(
            f"python scripts/generate_exceptions.py --date {date_str}",
            "Step 3: Generating Exception Reports"
        ):
            success = False
        
        if not success:
            print("\n❌ Pipeline execution failed. Skipping HDFS ingestion.")
            return 1
        
        print("\n✅ Pipeline execution completed successfully!")
    
    # =====================================================================
    # PHASE 2: UPLOAD TO HDFS
    # =====================================================================
    if not args.skip_hdfs:
        print("\n\n📤 PHASE 2: UPLOADING TO HDFS")
        print("="*70)
        
        # Upload logs (exceptions)
        if not run_command(
            "python scripts/ingest_hdfs.py --type logs",
            "Uploading Exception Logs to HDFS /logs"
        ):
            print("⚠️ Warning: Failed to upload logs")
        
        # Upload outputs (replenishment + orders)
        if not run_command(
            "python scripts/ingest_hdfs.py --type output",
            "Uploading Outputs to HDFS /output"
        ):
            print("⚠️ Warning: Failed to upload outputs")
        
        print("\n✅ HDFS ingestion completed!")
    
    # =====================================================================
    # SUMMARY
    # =====================================================================
    print("\n" + "="*70)
    print("📋 EXECUTION SUMMARY")
    print("="*70)
    print(f"   Date Processed: {date_str}")
    
    if not args.skip_pipeline:
        print("\n   Pipeline Outputs Generated:")
        output_path = Path("output")
        logs_path = Path("logs")
        
        replenishment = output_path / f"replenishment_{date_str}.csv"
        if replenishment.exists():
            print(f"      ✓ {replenishment}")
        
        # Count supplier orders
        order_files = list(output_path.glob(f"order_supplier_*_{date_str}.json"))
        if order_files:
            print(f"      ✓ {len(order_files)} supplier order files")
        
        # Check exception files
        exception_csv = logs_path / f"exceptions_{date_str}.csv"
        exception_txt = logs_path / f"exceptions_{date_str}.txt"
        if exception_csv.exists():
            print(f"      ✓ {exception_csv}")
        if exception_txt.exists():
            print(f"      ✓ {exception_txt}")
    
    if not args.skip_hdfs:
        print("\n   HDFS Directories Updated:")
        print("      ✓ /logs/exceptions/")
        print("      ✓ /output/replenishment/")
        print("      ✓ /output/orders/")
        
        print("\n   Access HDFS UI:")
        print("      🌐 http://localhost:9870")
    
    print("\n" + "="*70)
    print("✅ ALL OPERATIONS COMPLETED SUCCESSFULLY!")
    print("="*70 + "\n")
    
    return 0


if __name__ == "__main__":
    sys.exit(main())
