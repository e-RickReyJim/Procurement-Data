#!/usr/bin/env python

"""
Sample first N rows of NDJSON, infer schema, and save summary (Excel) and schema (JSON).

Usage:
    python sample_schema.py --input jbjy-vk9h_10062025.json --nrows 100000
"""
import argparse
import json
import logging
from pathlib import Path

import pandas as pd


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


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--input', required=True)
    parser.add_argument('--nrows', type=int, default=100000)
    parser.add_argument('--out-dir', default='.')
    args = parser.parse_args()

    logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)s %(message)s')

    input_path = Path(args.input)
    out_dir = Path(args.out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)

    logging.info('Reading first %d rows from %s', args.nrows, input_path)
    df = pd.read_json(input_path, lines=True, nrows=args.nrows)

    logging.info('Sample rows loaded: %d', len(df))

    # Save sample to Excel
    sample_xlsx = out_dir / f'summary_sample_{input_path.stem}.xlsx'
    df.head(1000).to_excel(sample_xlsx, index=False)
    logging.info('Saved sample to %s', sample_xlsx)

    # Infer schema and save to JSON
    schema = infer_schema(df)
    schema_json = out_dir / f'schema_{input_path.stem}.json'
    with open(schema_json, 'w', encoding='utf-8') as f:
        json.dump(schema, f, ensure_ascii=False, indent=2)
    logging.info('Saved schema to %s', schema_json)

    # Save dtypes to a CSV for quick view
    dtypes_csv = out_dir / f'dtypes_{input_path.stem}.csv'
    pd.DataFrame({'column': list(df.columns), 'dtype': [str(df[c].dtype) for c in df.columns]}).to_csv(dtypes_csv, index=False)
    logging.info('Saved dtypes to %s', dtypes_csv)


if __name__ == '__main__':
    main()
