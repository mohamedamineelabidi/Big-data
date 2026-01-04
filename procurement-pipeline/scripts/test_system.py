"""
System Integration Test
========================
Comprehensive test of all pipeline components

Author: Data Engineering Team
Date: January 2026
"""

import sys
import os
from pathlib import Path
from datetime import datetime

# Test results tracking
results = {
    'passed': 0,
    'failed': 0,
    'tests': []
}

def test_result(name, passed, message=""):
    """Record test result"""
    status = "✅ PASS" if passed else "❌ FAIL"
    results['passed' if passed else 'failed'] += 1
    results['tests'].append({
        'name': name,
        'passed': passed,
        'message': message
    })
    print(f"   {status}: {name}")
    if message and not passed:
        print(f"          {message}")


def print_header(title):
    """Print section header"""
    print(f"\n{'─'*60}")
    print(f"  {title}")
    print(f"{'─'*60}")


def test_docker_services():
    """Test 1: Docker Services"""
    print_header("TEST 1: Docker Services")
    
    import subprocess
    
    services = ['procurement_postgres', 'procurement_namenode', 'procurement_datanode', 
                'procurement_trino', 'procurement_airflow']
    
    for service in services:
        try:
            result = subprocess.run(
                ['docker', 'inspect', '--format', '{{.State.Running}}', service],
                capture_output=True, text=True, timeout=10
            )
            running = result.stdout.strip() == 'true'
            test_result(f"Container {service}", running, 
                       f"Not running" if not running else "")
        except Exception as e:
            test_result(f"Container {service}", False, str(e))


def test_postgres_connection():
    """Test 2: PostgreSQL Connection"""
    print_header("TEST 2: PostgreSQL Connection")
    
    try:
        import psycopg2
        conn = psycopg2.connect(
            host='localhost',
            port=5432,
            user='admin',
            password='password',
            database='procurement_db'
        )
        test_result("PostgreSQL connection", True)
        
        cursor = conn.cursor()
        
        # Test tables exist
        cursor.execute("SELECT COUNT(*) FROM products")
        count = cursor.fetchone()[0]
        test_result(f"Products table ({count} rows)", count > 0)
        
        cursor.execute("SELECT COUNT(*) FROM suppliers")
        count = cursor.fetchone()[0]
        test_result(f"Suppliers table ({count} rows)", count > 0)
        
        cursor.execute("SELECT COUNT(*) FROM replenishment_rules")
        count = cursor.fetchone()[0]
        test_result(f"Replenishment rules table ({count} rows)", count > 0)
        
        cursor.close()
        conn.close()
        
    except ImportError:
        test_result("PostgreSQL connection", False, "psycopg2 not installed")
    except Exception as e:
        test_result("PostgreSQL connection", False, str(e))


def test_trino_connection():
    """Test 3: Trino Connection"""
    print_header("TEST 3: Trino Connection")
    
    try:
        from trino.dbapi import connect
        
        conn = connect(
            host='localhost',
            port=8080,
            user='admin',
            catalog='postgresql',
            schema='public'
        )
        test_result("Trino connection", True)
        
        cursor = conn.cursor()
        
        # Test query through Trino
        cursor.execute("SELECT COUNT(*) FROM products")
        count = cursor.fetchone()[0]
        test_result(f"Trino query to PostgreSQL ({count} products)", count > 0)
        
        cursor.close()
        conn.close()
        
    except ImportError:
        test_result("Trino connection", False, "trino package not installed")
    except Exception as e:
        test_result("Trino connection", False, str(e))


def test_data_files():
    """Test 4: Data Files"""
    print_header("TEST 4: Data Files")
    
    base_path = Path("data")
    
    # Check orders
    orders_path = base_path / "raw" / "orders"
    order_files = list(orders_path.glob("*.json")) if orders_path.exists() else []
    test_result(f"Order files ({len(order_files)} files)", len(order_files) > 0)
    
    # Check stock
    stock_path = base_path / "raw" / "stock"
    stock_files = list(stock_path.glob("*.csv")) if stock_path.exists() else []
    test_result(f"Stock files ({len(stock_files)} files)", len(stock_files) > 0)
    
    # Check output directory
    output_path = base_path / "output"
    test_result("Output directory exists", output_path.exists())


def test_compute_demand():
    """Test 5: Demand Computation"""
    print_header("TEST 5: Demand Computation Module")
    
    try:
        sys.path.insert(0, str(Path(__file__).parent))
        from compute_demand import DemandAnalyzer
        
        test_result("Import DemandAnalyzer", True)
        
        analyzer = DemandAnalyzer(base_path="data/raw")
        test_result("Create DemandAnalyzer instance", True)
        
        # Test loading orders
        orders_df = analyzer.load_orders_from_json("2026-01-03")
        test_result(f"Load orders ({len(orders_df)} items)", len(orders_df) > 0)
        
        # Test loading stock
        stock_df = analyzer.load_stock_from_csv("2026-01-03")
        test_result(f"Load stock ({len(stock_df)} records)", len(stock_df) > 0)
        
    except Exception as e:
        test_result("Demand computation module", False, str(e))


def test_export_orders():
    """Test 6: Export Orders Module"""
    print_header("TEST 6: Export Orders Module")
    
    try:
        from export_orders import SupplierOrderExporter
        test_result("Import SupplierOrderExporter", True)
        
        exporter = SupplierOrderExporter(base_path="data")
        test_result("Create exporter instance", True)
        
        # Check if replenishment file exists
        replenishment_file = Path("data/output/replenishment_2026-01-03.csv")
        test_result("Replenishment CSV exists", replenishment_file.exists())
        
        # Check supplier order files
        supplier_orders_path = Path("data/output/supplier_orders")
        if supplier_orders_path.exists():
            order_files = list(supplier_orders_path.glob("*.json"))
            test_result(f"Supplier order files ({len(order_files)} files)", len(order_files) > 0)
        else:
            test_result("Supplier orders directory exists", False)
            
    except Exception as e:
        test_result("Export orders module", False, str(e))


def test_exception_reporting():
    """Test 7: Exception Reporting Module"""
    print_header("TEST 7: Exception Reporting Module")
    
    try:
        from generate_exceptions import ExceptionReporter
        test_result("Import ExceptionReporter", True)
        
        reporter = ExceptionReporter(base_path="data")
        test_result("Create reporter instance", True)
        
        # Check exception files
        exceptions_path = Path("data/output/exceptions")
        if exceptions_path.exists():
            json_files = list(exceptions_path.glob("*.json"))
            txt_files = list(exceptions_path.glob("*.txt"))
            test_result(f"Exception JSON files ({len(json_files)})", len(json_files) > 0)
            test_result(f"Exception TXT files ({len(txt_files)})", len(txt_files) > 0)
        else:
            test_result("Exceptions directory exists", False)
            
    except Exception as e:
        test_result("Exception reporting module", False, str(e))


def test_pipeline_orchestrator():
    """Test 8: Pipeline Orchestrator"""
    print_header("TEST 8: Pipeline Orchestrator")
    
    try:
        from run_pipeline import ProcurementPipeline
        test_result("Import ProcurementPipeline", True)
        
        pipeline = ProcurementPipeline(base_path="data")
        test_result("Create pipeline instance", True)
        
        # Test infrastructure validation
        valid, issues = pipeline.validate_infrastructure()
        test_result("Infrastructure validation", valid, 
                   f"{len(issues)} issues" if not valid else "")
        
    except Exception as e:
        test_result("Pipeline orchestrator", False, str(e))


def test_airflow_dag():
    """Test 9: Airflow DAG"""
    print_header("TEST 9: Airflow DAG")
    
    dag_file = Path("airflow/dags/procurement_dag.py")
    test_result("DAG file exists", dag_file.exists())
    
    if dag_file.exists():
        # Check syntax
        try:
            with open(dag_file, 'r', encoding='utf-8') as f:
                code = f.read()
            compile(code, dag_file, 'exec')
            test_result("DAG syntax valid", True)
        except SyntaxError as e:
            test_result("DAG syntax valid", False, str(e))
        
        # Check for required elements
        test_result("DAG has schedule", "schedule_interval" in code or "schedule" in code)
        test_result("DAG has tasks", "PythonOperator" in code)


def test_end_to_end():
    """Test 10: End-to-End Pipeline Run"""
    print_header("TEST 10: End-to-End Pipeline (Quick)")
    
    try:
        # Test with existing data, skip demand computation
        from run_phase4 import Phase4Runner
        
        runner = Phase4Runner(base_path="data")
        test_result("Create Phase4Runner", True)
        
        # Just verify files were generated previously
        replenishment = Path("data/output/replenishment_2026-01-03.csv")
        supplier_orders = list(Path("data/output/supplier_orders").glob("*.json"))
        exceptions = list(Path("data/output/exceptions").glob("*.json"))
        
        test_result("Replenishment output exists", replenishment.exists())
        test_result(f"Supplier orders generated ({len(supplier_orders)})", len(supplier_orders) == 5)
        test_result(f"Exception report generated ({len(exceptions)})", len(exceptions) > 0)
        
    except Exception as e:
        test_result("End-to-end test", False, str(e))


def print_summary():
    """Print final test summary"""
    print("\n" + "="*60)
    print("  📊 TEST SUMMARY")
    print("="*60)
    
    total = results['passed'] + results['failed']
    pass_rate = (results['passed'] / total * 100) if total > 0 else 0
    
    print(f"\n   Total Tests:  {total}")
    print(f"   ✅ Passed:    {results['passed']}")
    print(f"   ❌ Failed:    {results['failed']}")
    print(f"   Pass Rate:   {pass_rate:.1f}%")
    
    if results['failed'] > 0:
        print("\n   Failed Tests:")
        for test in results['tests']:
            if not test['passed']:
                print(f"   • {test['name']}: {test['message']}")
    
    print("\n" + "="*60)
    
    if results['failed'] == 0:
        print("   🎉 ALL TESTS PASSED! System is working correctly.")
    else:
        print(f"   ⚠️  {results['failed']} test(s) failed. Review issues above.")
    
    print("="*60 + "\n")
    
    return results['failed'] == 0


def main():
    """Run all tests"""
    print("\n" + "="*60)
    print("  🧪 PROCUREMENT PIPELINE - SYSTEM INTEGRATION TEST")
    print("="*60)
    print(f"  Started: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    
    # Run all tests
    test_docker_services()
    test_postgres_connection()
    test_trino_connection()
    test_data_files()
    test_compute_demand()
    test_export_orders()
    test_exception_reporting()
    test_pipeline_orchestrator()
    test_airflow_dag()
    test_end_to_end()
    
    # Print summary
    success = print_summary()
    
    return 0 if success else 1


if __name__ == "__main__":
    sys.exit(main())
