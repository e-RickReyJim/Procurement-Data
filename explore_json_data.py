import pandas as pd
import polars as pl
import numpy as np
import matplotlib.pyplot as plt

# Load the JSON Lines file
#df = pd.read_json('secop_download_09_11_2025.json', lines=True)
df = pl.read_ndjson('secop_download_09_11_2025.json')

# 1. Show the first few rows
#print('First 5 rows:')
#print(df.head())

# 2. DataFrame info
description = df.describe()
print(description)

# 3. Null values
##null_counts = df.null_count()
#print('\nNull values per column:')
#print(null_counts)

print(df.glimpse(max_items_per_column=1))
#rint("00")
#f.cast({"fecha_de_fin_del_contrato": pl.Date}).dtypes
#f.cast({"fecha_de_fin_del_contrato": pl.Date})
#fin = df.select(pl.col("fecha_de_fin_del_contrato"))
#print(fin)

#f."fecha_de_fin_del_contrato"] = df["fecha_de_fin_del_contrato"].str.slice(0, 10)

#riginal_date_time = "2020-08-15T00:00:00.000"
#ate_only = original_date_time.split('T')[0]
#rint(date_only)


#rint(df.glimpse(max_items_per_column=1))
## 4. Show column names and count unique values per column
#print('\nColumn names and unique value counts:')
#for col in df.columns:
#    try:
#        unique_count = df[col].unique()
#        print(f"{col}: {unique_count} unique values")
#    except TypeError:
#        # For unhashable types (e.g., dict), count unique string representations
#        unique_count = df[col].apply(lambda x: str(x)).unique()
#        #print(f"{col}: {unique_count} unique values (by string representation, unhashable type)")
##
## 5. Check for missing values
#print('\nMissing values per column:')
#print(df.isnull().sum())
#
## 6. (Optional) Prepare for plotting
## Example: Plot the distribution of contract values if present
#if 'valor_del_contrato' in df.columns:
#    # Convert to numeric, errors='coerce' will turn non-numeric to NaN
#    df['valor_del_contrato'] = pd.to_numeric(df['valor_del_contrato'], errors='coerce')
#    plt.figure(figsize=(10,6))
#    df['valor_del_contrato'].plot.hist(bins=30)
#    plt.title('Distribution of Contract Values')
#    plt.xlabel('Contract Value')
#    plt.ylabel('Frequency')
#    plt.show()
#else:
#    print("Column 'valor_del_contrato' not found for plotting.")
