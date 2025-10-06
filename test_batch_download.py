#!/usr/bin/env python

# Test batch download logic: download only the first record of each batch
# Usage: python test_batch_download.py

import pandas as pd
from sodapy import Socrata
import os
from datetime import datetime
import time

dataset_id = "jbjy-vk9h"
#"jbjy-vk9h"
chunk_size = 50000
client = Socrata("www.datos.gov.co", None,  timeout=60)  # Added timeout parameter

today = datetime.now().strftime("%m%d%Y")
test_filename = f"test_{dataset_id}_{today}.json"

def safe_get_count(client, dataset_id, retries=5, delay=10):
    for attempt in range(retries):
        try:
            count_result = client.get(dataset_id, select="count(*)")
            return int(count_result[0]['count'])
        except Exception as e:
            print(f"Attempt {attempt+1}/{retries} to get count failed: {e}")
            time.sleep(delay)
    raise Exception("Failed to get count after retries.")

def safe_get_record(client, dataset_id, limit, offset, retries=5, delay=10):
    for attempt in range(retries):
        try:
            return client.get(dataset_id, limit=limit, offset=offset)
        except Exception as e:
            print(f"Attempt {attempt+1}/{retries} to get record failed: {e}")
            time.sleep(delay)
    print(f"Failed to get record for offset {offset} after retries.")
    return []


# Get total rows and batches with retry
total_rows = safe_get_count(client, dataset_id)
total_batches = (total_rows + chunk_size - 1) // chunk_size

# Print total number of records in the dataset
print(f"\nTotal records in dataset '{dataset_id}': {total_rows}")

# --- Data Summary Section ---
print("\n--- DATASET SUMMARY ---")
sample_records = safe_get_record(client, dataset_id, limit=100, offset=0)

excel_filename = f"summary_{dataset_id}_{today}.xlsx"
if sample_records:
    df_sample = pd.DataFrame(sample_records)
    print(f"Number of sample rows: {df_sample.shape[0]}")
    print(f"Number of columns: {df_sample.shape[1]}")
    print("Column names:")
    print(list(df_sample.columns))
    print("\nData types:")
    print(df_sample.dtypes)
    # Save summary to Excel
    df_sample.to_excel(excel_filename, index=False)
    print(f"Summary saved to {excel_filename}")
else:
    print("Could not retrieve sample records for summary.")
print("--- END SUMMARY ---\n")

with open(test_filename, "w", encoding="utf-8") as f:
    for batch_num in range(total_batches):
        offset = batch_num * chunk_size
        record = safe_get_record(client, dataset_id, limit=1, offset=offset)
        if record:
            f.write(f"{pd.Series(record[0]).to_json(force_ascii=False)}\n")
            print(f"Batch {batch_num+1}/{total_batches}: Success, offset {offset}")
        else:
            print(f"Batch {batch_num+1}/{total_batches}: Error, offset {offset}, no record")

print(f"Test records saved to {test_filename}")
