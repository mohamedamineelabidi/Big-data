import os
import sys
from hdfs import InsecureClient
from datetime import datetime

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

if __name__ == "__main__":
    print("Starting HDFS Ingestion...")
    client = get_hdfs_client()
    
    # Ingest Orders
    ingest_files(client, 'orders')
    
    # Ingest Stock
    ingest_files(client, 'stock')
    
    print("Ingestion complete.")
