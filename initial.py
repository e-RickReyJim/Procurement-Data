import pandas as pd
import time

def reading_json_with_retries(file_path, retries=3, delay=5):
    start_time = time.perf_counter()
    df = pd.read_json(file_path)
    end_time = time.perf_counter()
    print(f"Time taken to read JSON: {end_time - start_time} seconds") 
    return df     

df = reading_json_with_retries("2022.jsonl")

