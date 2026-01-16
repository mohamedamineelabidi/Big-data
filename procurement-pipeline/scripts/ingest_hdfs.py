import os
import sys
import argparse
from hdfs import InsecureClient
from datetime import datetime
import glob

# Configuration
HDFS_URL = 'http://localhost:9870'
HDFS_USER = 'root'
LOCAL_DATA_DIR = 'data/raw'
HDFS_BASE_DIR = '/raw'

def get_hdfs_client():
    """Connect to HDFS via WebHDFS."""
    try:
        # Note: You may need to install the hdfs library: pip install hdfs
        client = InsecureClient(HDFS_URL, user=HDFS_USER)
        return client
    except Exception as e:
        print(f"Error connecting to HDFS: {e}")
        sys.exit(1)

def ingest_files(client, subfolder):
    """Ingest files from a local subfolder to HDFS."""
    local_path = os.path.join(LOCAL_DATA_DIR, subfolder)
    
    if not os.path.exists(local_path):
        print(f"Local directory {local_path} does not exist. Skipping.")
        return

    print(f"Scanning {local_path}...")
    
    for filename in os.listdir(local_path):
        file_path = os.path.join(local_path, filename)
        
        if not os.path.isfile(file_path):
            continue

        # Extract date from filename (assuming format: name_id_YYYY-MM-DD.ext)
        # Example: pos_001_2023-10-27.json -> 2023-10-27
        try:
            date_str = filename.split('_')[-1].split('.')[0]
            # Validate date format
            datetime.strptime(date_str, '%Y-%m-%d')
        except (IndexError, ValueError):
            print(f"Skipping {filename}: Could not extract valid date.")
            continue

        # Construct HDFS path
        hdfs_dir = f"{HDFS_BASE_DIR}/{subfolder}/{date_str}"
        hdfs_path = f"{hdfs_dir}/{filename}"

        # Create directory if not exists
        try:
            if not client.status(hdfs_dir, strict=False):
                client.makedirs(hdfs_dir)
        except Exception as e:
             print(f"Error creating directory {hdfs_dir}: {e}")

        # Upload file
        try:
            print(f"Uploading {filename} to {hdfs_path}...")
            client.upload(hdfs_path, file_path, overwrite=True)
            print("Success.")
        except Exception as e:
            print(f"Failed to upload {filename}: {e}")


def ingest_logs(client):
    """Ingest exception logs and data quality logs to HDFS /logs."""
    print("\n=== Ingesting Logs ===")
    
    uploaded_count = 0
    
    # Exception JSON reports
    exception_json = glob.glob('logs/exception_report_*.json')
    for file_path in exception_json:
        filename = os.path.basename(file_path)
        try:
            # Extract date from filename: exception_report_2026-01-14.json
            date_str = filename.replace('exception_report_', '').replace('.json', '')
            datetime.strptime(date_str, '%Y-%m-%d')
            
            hdfs_dir = f"/logs/exceptions/{date_str}"
            hdfs_path = f"{hdfs_dir}/{filename}"
            
            # Create directory
            if not client.status(hdfs_dir, strict=False):
                client.makedirs(hdfs_dir)
            
            print(f"Uploading {filename} to {hdfs_path}...")
            client.upload(hdfs_path, file_path, overwrite=True)
            print("✓ Success.")
            uploaded_count += 1
        except Exception as e:
            print(f"✗ Failed to upload {filename}: {e}")
    
    # Exception text summaries
    exception_txt = glob.glob('logs/exception_summary_*.txt')
    for file_path in exception_txt:
        filename = os.path.basename(file_path)
        try:
            # Extract date from filename: exception_summary_2026-01-14.txt
            date_str = filename.replace('exception_summary_', '').replace('.txt', '')
            datetime.strptime(date_str, '%Y-%m-%d')
            
            hdfs_dir = f"/logs/exceptions/{date_str}"
            hdfs_path = f"{hdfs_dir}/{filename}"
            
            # Create directory
            if not client.status(hdfs_dir, strict=False):
                client.makedirs(hdfs_dir)
            
            print(f"Uploading {filename} to {hdfs_path}...")
            client.upload(hdfs_path, file_path, overwrite=True)
            print("✓ Success.")
            uploaded_count += 1
        except Exception as e:
            print(f"✗ Failed to upload {filename}: {e}")
    
    # Data quality logs
    quality_logs = glob.glob('logs/data_quality_*.log')
    for file_path in quality_logs:
        filename = os.path.basename(file_path)
        try:
            # Extract timestamp: data_quality_20260103_051920.log
            timestamp = filename.replace('data_quality_', '').replace('.log', '')
            date_str = timestamp[:8]  # Get YYYYMMDD
            date_obj = datetime.strptime(date_str, '%Y%m%d')
            date_formatted = date_obj.strftime('%Y-%m-%d')
            
            hdfs_dir = f"/logs/data_quality/{date_formatted}"
            hdfs_path = f"{hdfs_dir}/{filename}"
            
            # Create directory
            if not client.status(hdfs_dir, strict=False):
                client.makedirs(hdfs_dir)
            
            print(f"Uploading {filename} to {hdfs_path}...")
            client.upload(hdfs_path, file_path, overwrite=True)
            print("✓ Success.")
            uploaded_count += 1
        except Exception as e:
            print(f"✗ Failed to upload {filename}: {e}")
    
    print(f"\n✓ Logs: {uploaded_count} files uploaded")
    return uploaded_count



def ingest_outputs(client):
    """Ingest replenishment outputs and supplier orders to HDFS /output."""
    print("\n=== Ingesting Outputs ===")
    
    uploaded_count = 0
    
    # Replenishment files
    replenishment_files = glob.glob('output/replenishment_*.csv')
    for file_path in replenishment_files:
        filename = os.path.basename(file_path)
        try:
            # Extract date: replenishment_2026-01-14.csv
            date_str = filename.replace('replenishment_', '').replace('.csv', '')
            datetime.strptime(date_str, '%Y-%m-%d')
            
            hdfs_dir = f"/output/replenishment/{date_str}"
            hdfs_path = f"{hdfs_dir}/{filename}"
            
            # Create directory
            if not client.status(hdfs_dir, strict=False):
                client.makedirs(hdfs_dir)
            
            print(f"Uploading {filename} to {hdfs_path}...")
            client.upload(hdfs_path, file_path, overwrite=True)
            print("✓ Success.")
            uploaded_count += 1
        except Exception as e:
            print(f"✗ Failed to upload {filename}: {e}")
    
    # Supplier order files (new naming pattern: SupplierName_2026-01-14.json)
    order_files = glob.glob('output/*_202*.json')
    for file_path in order_files:
        filename = os.path.basename(file_path)
        try:
            # Extract date from filename: BevCo_Distributors_2026-01-14.json
            # Date is at the end before .json
            date_str = filename.split('_')[-1].replace('.json', '')
            if len(date_str) == 10 and '-' in date_str:  # Validate format YYYY-MM-DD
                datetime.strptime(date_str, '%Y-%m-%d')
                
                hdfs_dir = f"/output/orders/{date_str}"
                hdfs_path = f"{hdfs_dir}/{filename}"
                
                # Create directory
                if not client.status(hdfs_dir, strict=False):
                    client.makedirs(hdfs_dir)
                
                print(f"Uploading {filename} to {hdfs_path}...")
                client.upload(hdfs_path, file_path, overwrite=True)
                print("✓ Success.")
                uploaded_count += 1
        except Exception as e:
            print(f"✗ Failed to upload {filename}: {e}")
    
    print(f"\n✓ Outputs: {uploaded_count} files uploaded")
    return uploaded_count


def ingest_processed(client):
    """Ingest processed raw data to HDFS /processed."""
    print("\n=== Ingesting Processed Files ===")
    
    # After pipeline execution, mark original files as processed
    # This function moves/copies processed orders and stock to HDFS /processed
    
    # Processed orders
    processed_orders = glob.glob('processed/pos_*_*.json')
    for file_path in processed_orders:
        filename = os.path.basename(file_path)
        try:
            # Extract date from filename
            date_str = filename.split('_')[-1].replace('.json', '')
            datetime.strptime(date_str, '%Y-%m-%d')
            
            hdfs_dir = f"/processed/orders/{date_str}"
            hdfs_path = f"{hdfs_dir}/{filename}"
            
            # Create directory
            if not client.status(hdfs_dir, strict=False):
                client.makedirs(hdfs_dir)
            
            print(f"Uploading {filename} to {hdfs_path}...")
            client.upload(hdfs_path, file_path, overwrite=True)
            print("Success.")
        except Exception as e:
            print(f"Failed to upload {filename}: {e}")
    
    # Processed stock
    processed_stock = glob.glob('processed/wh_*_*.csv')
    for file_path in processed_stock:
        filename = os.path.basename(file_path)
        try:
            # Extract date from filename
            date_str = filename.split('_')[-1].replace('.csv', '')
            datetime.strptime(date_str, '%Y-%m-%d')
            
            hdfs_dir = f"/processed/stock/{date_str}"
            hdfs_path = f"{hdfs_dir}/{filename}"
            
            # Create directory
            if not client.status(hdfs_dir, strict=False):
                client.makedirs(hdfs_dir)
            
            print(f"Uploading {filename} to {hdfs_path}...")
            client.upload(hdfs_path, file_path, overwrite=True)
            print("Success.")
        except Exception as e:
            print(f"Failed to upload {filename}: {e}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Ingest files to HDFS')
    parser.add_argument('--type', choices=['raw', 'logs', 'output', 'processed', 'all'], 
                        default='all', help='Type of files to ingest')
    args = parser.parse_args()
    
    print("Starting HDFS Ingestion...")
    client = get_hdfs_client()
    
    if args.type in ['raw', 'all']:
        # Ingest Raw Orders
        ingest_files(client, 'orders')
        
        # Ingest Raw Stock
        ingest_files(client, 'stock')
    
    if args.type in ['logs', 'all']:
        ingest_logs(client)
    
    if args.type in ['output', 'all']:
        ingest_outputs(client)
    
    if args.type in ['processed', 'all']:
        ingest_processed(client)
    
    print("\n✓ Ingestion complete.")
