#!/usr/bin/env python

"""
Convert a large newline-delimited JSON (NDJSON) file into partitioned Parquet using DuckDB.
- Tries DuckDB fast path first.
- Falls back to chunked pandas -> parquet if DuckDB isn't available or fails.

Usage:
    python ndjson_to_parquet.py --input jbjy-vk9h_10062025.json --out-dir parquet_out --partition-key fecha_de_firma

"""
import argparse
import logging
import os
from pathlib import Path
import sys


def try_duckdb_convert(input_path, out_dir, partition_key):
    try:
        import duckdb
    except Exception as e:
        logging.warning("DuckDB not available: %s", e)
        return False

    conn = duckdb.connect(database=':memory:')
    logging.info("Using DuckDB to convert %s -> %s (partition by %s)", input_path, out_dir, partition_key)

    # Build SQL to read NDJSON and extract year/month
    sql = f"""
    CREATE TABLE tmp AS
    SELECT *,
           TRY_CAST({partition_key} AS DATE) AS __fecha_dt,
           EXTRACT(year FROM TRY_CAST({partition_key} AS DATE)) AS __year,
           EXTRACT(month FROM TRY_CAST({partition_key} AS DATE)) AS __month
    FROM read_json_auto('{input_path}');
    """

    try:
        conn.execute(sql)
        # Write partitioned parquet
        out_dir_str = str(out_dir).replace('\\', '/')
        copy_sql = f"COPY (SELECT * FROM tmp) TO '{out_dir_str}' (FORMAT PARQUET, PARTITION_BY(__year, __month));"
        logging.info("Running DuckDB COPY to write partitioned Parquet (this may take a while)...")
        conn.execute(copy_sql)
        logging.info("DuckDB conversion finished successfully.")
        return True
    except Exception as e:
        logging.exception("DuckDB conversion failed: %s", e)
        return False
    finally:
        try:
            conn.close()
        except Exception:
            pass


def pandas_fallback(input_path, out_dir, partition_key, chunksize=50000):
    import pandas as pd
    import pyarrow as pa
    import pyarrow.parquet as pq
    from datetime import datetime

    logging.info("Falling back to chunked pandas -> parquet conversion")
    out_dir = Path(out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)

    reader = pd.read_json(input_path, lines=True, chunksize=chunksize)
    total = 0
    for i, chunk in enumerate(reader):
        logging.info("Processing chunk %d, rows=%d", i, len(chunk))
        # parse partition key to datetime
        if partition_key in chunk.columns:
            chunk['__fecha_dt'] = pd.to_datetime(chunk[partition_key], errors='coerce')
            chunk['__year'] = chunk['__fecha_dt'].dt.year
            chunk['__month'] = chunk['__fecha_dt'].dt.month
        else:
            chunk['__year'] = None
            chunk['__month'] = None

        # write to parquet using partitioning
        table = pa.Table.from_pandas(chunk)
        # Use filename with chunk index to avoid concurrent writes
        chunk_path = out_dir / f"chunk_{i}.parquet"
        pq.write_table(table, chunk_path, compression='snappy')
        total += len(chunk)
    logging.info("Pandas fallback finished, total rows processed: %d", total)
    return True


def main():
    parser = argparse.ArgumentParser(description='Convert NDJSON to partitioned Parquet')
    parser.add_argument('--input', required=True, help='Input NDJSON file')
    parser.add_argument('--out-dir', default='parquet_out', help='Output directory')
    parser.add_argument('--partition-key', default='fecha_de_firma', help='Date field to partition by (will extract year/month)')
    parser.add_argument('--chunksize', type=int, default=50000, help='Chunksize for pandas fallback')
    args = parser.parse_args()

    logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)s %(message)s')

    input_path = Path(args.input)
    out_dir = Path(args.out_dir)
    if not input_path.exists():
        logging.error('Input file not found: %s', input_path)
        sys.exit(1)

    out_dir.mkdir(parents=True, exist_ok=True)

    ok = try_duckdb_convert(str(input_path).replace('\\', '/'), out_dir, args.partition_key)
    if not ok:
        logging.info('DuckDB path failed or not available; trying pandas fallback')
        pandas_fallback(str(input_path), out_dir, args.partition_key, chunksize=args.chunksize)


if __name__ == '__main__':
    main()
