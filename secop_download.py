#!/usr/bin/env python

# make sure to install these packages before running:
# pip install pandas
# pip install sodapy

import pandas as pd
from sodapy import Socrata
import os
from datetime import datetime
import time
import openpyxl

# Unauthenticated client only works with public data sets. Note 'None'
# in place of application token, and no username or password:
start_time = time.perf_counter()
client = Socrata("www.datos.gov.co", None)

# Example authenticated client (needed for non-public datasets):
# client = Socrata(www.datos.gov.co,
#                  MyAppToken,
#                  username="user@example.com",
#                  password="AFakePassword")

# First 2000 results, returned as JSON from API / converted to Python list of
# dictionaries by sodapy.
#results = client.get("jbjy-vk9h", limit=5)#, where="nombre_entidad='MUNICIPIO DE PITALITO'")
#results = client.get("jbjy-vk9h", limit=1000000)

## Get total number of rows
#available_datasets = ("jbjy-vk9h", "hska-shas")
#total_rows = int(count_result[0]['count'])

# Parameters
dataset_id = "jbjy-vk9h"
chunk_size = 50000
today = datetime.now().strftime("%m%d%Y")
json_filename = f"{dataset_id}_{today}.json"
aux_filename = f"{dataset_id}_progress_{today}.xlsx"

# Helper: Initialize or load progress Excel
def load_or_create_progress_excel(aux_filename, total_batches):
    if os.path.exists(aux_filename):
        wb = openpyxl.load_workbook(aux_filename)
        ws = wb.active
    else:
        wb = openpyxl.Workbook()
        ws = wb.active
        ws.append(["Batch", "Status", "Length", "Error"])
        for batch in range(total_batches):
            ws.append([batch, "Pending", 0, ""])
        wb.save(aux_filename)
    return wb, ws

# Helper: Append batch to JSON file
def append_to_json_file(filename, batch_data):
    with open(filename, "a", encoding="utf-8") as f:
        for record in batch_data:
            f.write(f"{pd.Series(record).to_json(force_ascii=False)}\n")

# Get total rows and batches
count_result = client.get(dataset_id, select="count(*)")
total_rows = int(count_result[0]['count'])
print(f"Total rows available: {total_rows}")
total_batches = (total_rows + chunk_size - 1) // chunk_size

# Load or create progress Excel
wb, ws = load_or_create_progress_excel(aux_filename, total_batches)

# Resume logic: find first incomplete batch
for row in ws.iter_rows(min_row=2, values_only=False):
    batch_num = row[0].value
    status = row[1].value
    if status != "Completed":
        offset = batch_num * chunk_size
        print(f"Downloading batch {batch_num+1}/{total_batches} (offset {offset})...")
        try:
            batch_data = client.get(dataset_id, limit=chunk_size, offset=offset)
            append_to_json_file(json_filename, batch_data)
            row[1].value = "Completed"
            row[2].value = len(batch_data)
            row[3].value = ""
            print(f"Batch {batch_num} completed, {len(batch_data)} records.")
        except Exception as e:
            row[1].value = "Error"
            row[2].value = 0
            row[3].value = str(e)
            print(f"Error in batch {batch_num}: {e}")
        wb.save(aux_filename)

print(f"All batches processed. Data saved to {json_filename}")
end_time = time.perf_counter()
print(f"Time taken to create JSON: {end_time - start_time} seconds")
