#!/bin/bash
# Run at 8:30am EST daily
python src/extract.py  # Get new data
dbt run --models marts.reporting.stock_daily_summary  # Update reporting views
dbt test  # Verify data quality 