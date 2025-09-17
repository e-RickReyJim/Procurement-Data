#!/usr/bin/env python

# make sure to install these packages before running:
# pip install pandas
# pip install sodapy

import pandas as pd
from sodapy import Socrata
import os
from datetime import datetime
import time

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
results = client.get("jbjy-vk9h", limit=10)#, where="nombre_entidad='MUNICIPIO DE PITALITO'")
#results = client.get("jbjy-vk9h", limit=1000000)

## Get total number of rows
#count_result = client.get("jbjy-vk9h", select="count(*)")
#total_rows = int(count_result[0]['count'])
#print(f"Total rows available: {total_rows}")
#
## Download data in chunks
#chunk_size = 50000  # Adjust as needed
#all_results = []
#for offset in range(0, total_rows, chunk_size):
#    chunk = client.get("jbjy-vk9h", limit=chunk_size, offset=offset)
#    all_results.extend(chunk)
#    print(f"Downloaded {len(all_results)} rows so far...")
#
## Convert to pandas DataFrame
results_df = pd.DataFrame.from_records(results)
#results_df = pd.DataFrame.from_records(all_results)

#
# Generate output filename with script name and current date
script_name = os.path.splitext(os.path.basename(__file__))[0]
today = datetime.now().strftime("%m_%d_%Y")
json_filename = f"{script_name}_{today}.json"
#
results_df.to_json(json_filename, orient="records", lines=True, force_ascii=False)
#
print(f"Saved JSON to {json_filename}")
end_time = time.perf_counter()
print(f"Time taken to create JSON: {end_time - start_time} seconds") 
print(results_df.head())
print(results_df.shape)
