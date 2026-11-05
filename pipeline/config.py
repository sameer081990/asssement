from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
RAW_DIR = ROOT / "data" / "raw"
DB_PATH = ROOT / "data" / "podcast.duckdb"
DBT_DIR = ROOT / "dbt_project"
