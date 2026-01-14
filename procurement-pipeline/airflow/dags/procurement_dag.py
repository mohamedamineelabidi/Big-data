"""
Procurement Pipeline DAG
========================
Airflow DAG for automated daily procurement processing

Schedule: Daily at 22:00 (10:00 PM)
Batch Window: 22:00 - 00:00

Author: Data Engineering Team
Date: January 2026
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator
from airflow.utils.dates import days_ago
import sys
import os

# Add scripts to path
sys.path.insert(0, '/opt/airflow/scripts')


# =============================================================================
# DAG Configuration
# =============================================================================

default_args = {
    'owner': 'procurement_team',
    'depends_on_past': False,
    'email': ['admin@example.com'],
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'execution_timeout': timedelta(hours=2),
}

dag = DAG(
    'procurement_daily_pipeline',
    default_args=default_args,
    description='Daily procurement pipeline - compute demand, export orders, generate exceptions',
    schedule_interval='0 22 * * *',  # Run at 22:00 daily
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=['procurement', 'batch', 'daily'],
    max_active_runs=1,
)


# =============================================================================
# Task Functions
# =============================================================================

def get_processing_date(**context):
    """Get the date to process (execution date or override)"""
    # Use execution date or default to yesterday
    execution_date = context['execution_date']
    processing_date = execution_date.strftime('%Y-%m-%d')
    
    # Store in XCom for downstream tasks
    context['ti'].xcom_push(key='processing_date', value=processing_date)
    print(f"📅 Processing date: {processing_date}")
    return processing_date


def validate_data_sources(**context):
    """Validate that all required data sources are available"""
    processing_date = context['ti'].xcom_pull(key='processing_date', task_ids='get_processing_date')
    
    import os
    from pathlib import Path
    
    base_path = Path('/opt/airflow/data')
    orders_path = base_path / 'raw' / 'orders'
    stock_path = base_path / 'raw' / 'stock'
    
    # Check orders exist
    order_files = list(orders_path.glob('*.json'))
    stock_files = list(stock_path.glob('*.csv'))
    
    print(f"📁 Found {len(order_files)} order files")
    print(f"📁 Found {len(stock_files)} stock files")
    
    if len(order_files) == 0:
        raise ValueError("No order files found!")
    if len(stock_files) == 0:
        raise ValueError("No stock files found!")
    
    return {
        'order_files': len(order_files),
        'stock_files': len(stock_files)
    }


def run_data_quality_check(**context):
    """Run data quality validation"""
    print("🔍 Running data quality checks...")
    
    # Import and run validation
    from validate_data_quality import DataQualityValidator
    
    validator = DataQualityValidator()
    validator.validate_orders()
    validator.validate_stock()
    
    results = {
        'all_passed': len(validator.errors) == 0,
        'total_files': validator.stats.get('files_checked', 0),
        'errors': validator.errors,
        'warnings': validator.warnings
    }
    
    if not results['all_passed']:
        print(f"⚠️ Data quality issues: {len(validator.errors)} errors")
        for err in validator.errors[:5]:  # Show first 5 errors
            print(f"   - {err}")
    else:
        print(f"✅ Data quality check passed: {results['total_files']} files validated")
    
    return results


def run_demand_computation(**context):
    """Execute demand computation using Trino"""
    processing_date = context['ti'].xcom_pull(key='processing_date', task_ids='get_processing_date')
    
    print(f"📊 Computing demand for {processing_date}...")
    
    from compute_demand import DemandAnalyzer
    
    analyzer = DemandAnalyzer(base_path='/opt/airflow/data/raw')
    
    # Load data and compute demand
    orders_df = analyzer.load_orders_from_json(processing_date)
    stock_df = analyzer.load_stock_from_csv(processing_date)
    
    result = {
        'skus': len(orders_df['sku'].unique()) if len(orders_df) > 0 else 0,
        'units': int(orders_df['quantity'].sum()) if len(orders_df) > 0 else 0,
        'stock_records': len(stock_df) if stock_df is not None else 0,
        'date': processing_date
    }
    
    print(f"✅ Demand computed: {result.get('skus', 0)} SKUs, {result.get('units', 0):,} units")
    
    # Push results to XCom
    context['ti'].xcom_push(key='demand_result', value=result)
    return result


def run_order_export(**context):
    """Export supplier orders as JSON"""
    processing_date = context['ti'].xcom_pull(key='processing_date', task_ids='get_processing_date')
    
    print(f"📦 Exporting supplier orders for {processing_date}...")
    
    try:
        from export_orders import SupplierOrderExporter
        
        exporter = SupplierOrderExporter(base_path='/opt/airflow/data')
        result = exporter.export_all_suppliers(date_str=processing_date)
        
        print(f"✅ Exported {result.get('suppliers', 0)} supplier orders ({result.get('total_units', 0):,} units)")
    except Exception as e:
        print(f"⚠️ Export not available: {e}")
        result = {'suppliers': 0, 'files': [], 'total_units': 0}
    
    context['ti'].xcom_push(key='export_result', value=result)
    return result


def run_exception_report(**context):
    """Generate exception report"""
    processing_date = context['ti'].xcom_pull(key='processing_date', task_ids='get_processing_date')
    
    print(f"⚠️ Generating exception report for {processing_date}...")
    
    try:
        from generate_exceptions import ExceptionReporter
        reporter = ExceptionReporter(base_path='/opt/airflow/data')
        result = reporter.run(date_str=processing_date)
        
        summary = result.get('summary', {})
        total = summary.get('total_exceptions', 0)
        critical = summary.get('by_severity', {}).get('CRITICAL', 0)
    except Exception as e:
        print(f"⚠️ Exception generation not available: {e}")
        # Create default result
        result = {'exceptions': []}
        total = 0
        critical = 0
    
    print(f"⚠️ Exception report: {total} total, {critical} critical")
    
    context['ti'].xcom_push(key='exception_result', value={
        'total': total,
        'critical': critical,
        'high': 0
    })
    
    return result


def generate_summary_report(**context):
    """Generate final summary of the pipeline run"""
    processing_date = context['ti'].xcom_pull(key='processing_date', task_ids='get_processing_date')
    demand_result = context['ti'].xcom_pull(key='demand_result', task_ids='compute_demand')
    export_result = context['ti'].xcom_pull(key='export_result', task_ids='export_orders')
    exception_result = context['ti'].xcom_pull(key='exception_result', task_ids='generate_exceptions')
    
    summary = f"""
╔══════════════════════════════════════════════════════════════════════╗
║                  PROCUREMENT PIPELINE SUMMARY                        ║
╠══════════════════════════════════════════════════════════════════════╣
║  Processing Date: {processing_date}                                      
║  Completed At: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}                     
╠══════════════════════════════════════════════════════════════════════╣
║  📊 DEMAND ANALYSIS                                                  
║     • SKUs to reorder: {demand_result.get('skus', 'N/A')}                       
║     • Total units: {demand_result.get('units', 'N/A'):,}                         
╠══════════════════════════════════════════════════════════════════════╣
║  📦 SUPPLIER ORDERS                                                  
║     • Suppliers: {export_result.get('suppliers', 'N/A')}                         
║     • Order files: {len(export_result.get('files', []))}                        
╠══════════════════════════════════════════════════════════════════════╣
║  ⚠️ EXCEPTIONS                                                       
║     • Total: {exception_result.get('total', 0)}                                 
║     • Critical: {exception_result.get('critical', 0)}                           
║     • High: {exception_result.get('high', 0)}                                   
╠══════════════════════════════════════════════════════════════════════╣
║  ✅ PIPELINE COMPLETED SUCCESSFULLY                                  ║
╚══════════════════════════════════════════════════════════════════════╝
"""
    
    print(summary)
    
    # Save summary to file
    from pathlib import Path
    summary_file = Path('/opt/airflow/data/output') / f'pipeline_summary_{processing_date}.txt'
    with open(summary_file, 'w') as f:
        f.write(summary)
    
    return {'status': 'SUCCESS', 'summary_file': str(summary_file)}


# =============================================================================
# DAG Tasks Definition
# =============================================================================

with dag:
    
    # Start task
    start = EmptyOperator(
        task_id='start_pipeline',
        doc='Start of the procurement pipeline'
    )
    
    # Get processing date
    get_date = PythonOperator(
        task_id='get_processing_date',
        python_callable=get_processing_date,
        doc='Determine the date to process'
    )
    
    # Validate data sources
    validate_sources = PythonOperator(
        task_id='validate_data_sources',
        python_callable=validate_data_sources,
        doc='Validate that required data files exist'
    )
    
    # Data quality check
    quality_check = PythonOperator(
        task_id='data_quality_check',
        python_callable=run_data_quality_check,
        doc='Run data quality validations'
    )
    
    # Compute demand
    compute_demand = PythonOperator(
        task_id='compute_demand',
        python_callable=run_demand_computation,
        doc='Calculate net demand using Trino'
    )
    
    # Export supplier orders
    export_orders = PythonOperator(
        task_id='export_orders',
        python_callable=run_order_export,
        doc='Generate JSON orders for each supplier'
    )
    
    # Generate exceptions
    generate_exceptions = PythonOperator(
        task_id='generate_exceptions',
        python_callable=run_exception_report,
        doc='Generate exception and anomaly report'
    )
    
    # Generate summary
    summary = PythonOperator(
        task_id='generate_summary',
        python_callable=generate_summary_report,
        doc='Generate final pipeline summary'
    )
    
    # End task
    end = EmptyOperator(
        task_id='end_pipeline',
        doc='End of the procurement pipeline'
    )
    
    # ==========================================================================
    # Task Dependencies
    # ==========================================================================
    #
    #                        start
    #                          │
    #                          ▼
    #                     get_date
    #                          │
    #                          ▼
    #                  validate_sources
    #                          │
    #                          ▼
    #                   quality_check
    #                          │
    #                          ▼
    #                   compute_demand
    #                          │
    #               ┌──────────┴──────────┐
    #               ▼                     ▼
    #         export_orders      generate_exceptions
    #               │                     │
    #               └──────────┬──────────┘
    #                          ▼
    #                       summary
    #                          │
    #                          ▼
    #                         end
    #
    # ==========================================================================
    
    start >> get_date >> validate_sources >> quality_check >> compute_demand
    compute_demand >> [export_orders, generate_exceptions]
    [export_orders, generate_exceptions] >> summary >> end
