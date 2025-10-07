#!/usr/bin/env python
"""
Safe sampling and schema inference for large NDJSON files.
Creates separate outputs for each sample size (Excel sample, schema JSON, dtypes CSV).

Usage examples:
  python sample_schema_stream.py --input jbjy-vk9h_10062025.json
  python sample_schema_stream.py --input jbjy-vk9h_10062025.json --nrows-list 1000,3000,10000 --use-duckdb

The script will try DuckDB when --use-duckdb is passed and duckdb is installed; otherwise it will stream the file line-by-line.
"""
import argparse
import json
import logging
from pathlib import Path
from typing import List

import pandas as pd
#import duckdb


def infer_schema(df: pd.DataFrame):
    schema = {}
    for col in df.columns:
        series = df[col]
        schema[col] = {
            'dtype': str(series.dtype),
            'n_null': int(series.isna().sum()),
            'n_unique': int(series.nunique(dropna=True)),
            'sample_values': series.dropna().astype(str).head(5).tolist()
        }
    return schema


def stream_sample(path: Path, nrows: int):
    records = []
    with path.open('r', encoding='utf-8') as fh:
        for i, line in enumerate(fh):
            if i >= nrows:
                break
            line = line.strip()
            if not line:
                continue
            try:
                records.append(json.loads(line))
            except Exception as e:
                logging.debug('Skipping bad JSON line %d: %s', i, e)
    df = pd.DataFrame.from_records(records)
    return df


def duckdb_sample(path: Path, nrows: int):
    import duckdb
    # duckdb.connect returns a DuckDBPyConnection
    con = duckdb.connect(database=':memory:')
    # duckdb accepts file path; ensure windows backslashes handled by duckdb
    q = f"SELECT * FROM read_json_auto('{str(path).replace('\\\\','/')}' ) LIMIT {nrows}"
    logging.info('Running DuckDB query for %d rows (this is efficient)', nrows)
    try:
        df = con.execute(q).df()
    finally:
        try:
            con.close()
        except Exception:
            pass
    return df


def run_samples(input_file: Path, out_dir: Path, nrows_list: List[int], use_duckdb: bool):
    out_dir.mkdir(parents=True, exist_ok=True)
    for n in nrows_list:
        logging.info('Sampling %d rows', n)
        sample_out = out_dir / f'sample_{n}'
        sample_out.mkdir(parents=True, exist_ok=True)
        try:
            if use_duckdb:
                try:
                    df = duckdb_sample(input_file, n)
                except Exception as e:
                    logging.warning('DuckDB sample failed: %s; falling back to streaming', e)
                    df = stream_sample(input_file, n)
            else:
                df = stream_sample(input_file, n)

            logging.info('Sample %d rows loaded: %d columns', len(df), df.shape[1] if df.shape[0] > 0 else 0)

            # Save sample to Excel (first 1000 rows maximum to avoid huge files)
            sample_xlsx = sample_out / f'summary_sample_{input_file.stem}_{n}.xlsx'
            df.head(min(1000, len(df))).to_excel(sample_xlsx, index=False)
            logging.info('Saved sample Excel to %s', sample_xlsx)

            # Infer schema and save to JSON
            schema = infer_schema(df)
            schema_json = sample_out / f'schema_{input_file.stem}_{n}.json'
            with open(schema_json, 'w', encoding='utf-8') as f:
                json.dump(schema, f, ensure_ascii=False, indent=2)
            logging.info('Saved schema to %s', schema_json)

            # Save dtypes to CSV
            dtypes_csv = sample_out / f'dtypes_{input_file.stem}_{n}.csv'
            pd.DataFrame({'column': list(df.columns), 'dtype': [str(df[c].dtype) for c in df.columns]}).to_csv(dtypes_csv, index=False)
            logging.info('Saved dtypes CSV to %s', dtypes_csv)

        except Exception as e:
            logging.exception('Failed to process sample %d: %s', n, e)


def parse_nrows_list(s: str) -> List[int]:
    parts = [p.strip() for p in s.split(',') if p.strip()]
    return [int(p) for p in parts]


def main():
    parser = argparse.ArgumentParser(description='Safe sampling and schema inference for NDJSON')
    parser.add_argument('--input', required=True, help='Input NDJSON file')
    parser.add_argument('--nrows-list', default='1000,3000,10000', help='Comma-separated list of row counts to sample')
    parser.add_argument('--out-dir', default='sample_output', help='Directory to write samples')
    parser.add_argument('--use-duckdb', action='store_true', help='Use DuckDB for faster sampling (if installed)')
    args = parser.parse_args()

    logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)s %(message)s')

    input_file = Path(args.input)
    if not input_file.exists():
        logging.error('Input file does not exist: %s', input_file)
        return

    nrows_list = parse_nrows_list(args.nrows_list)
    out_dir = Path(args.out_dir)

    run_samples(input_file, out_dir, nrows_list, args.use_duckdb)


if __name__ == '__main__':
    main()
