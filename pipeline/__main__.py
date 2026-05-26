"""CLI entry point: python -m pipeline [csv_path]"""
import sys

from pipeline import run_ingest

DEFAULT_CSV = "data/raw/data_jobs.csv"

if __name__ == "__main__":
    csv_path = sys.argv[1] if len(sys.argv) > 1 else DEFAULT_CSV
    run_ingest(csv_path)
