# Airflow image with the extra packages the pipeline needs (dbt + DuckDB).
FROM apache/airflow:2.10.5-python3.11
RUN pip install --no-cache-dir duckdb "dbt-duckdb>=1.8"
