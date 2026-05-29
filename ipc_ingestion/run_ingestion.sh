#!/bin/bash
# IPC Ingestion — runs every 2 hours via macOS launchd
# Logs to ~/Downloads/ipc_analytics/ingestion.log

cd /Users/sapaleague/Downloads/ipc_analytics
source venv312/bin/activate

echo ""
echo "========================================="
echo "  IPC Ingestion — $(date '+%Y-%m-%d %H:%M:%S')"
echo "========================================="

# Load env vars
export $(grep -v '^#' .env | xargs)

# Ingestion (sequential — DAASH and GoSource are the big ones)
echo ">>> DAASH ingestion..."
python ipc_ingestion/dash_incremental_load.py 2>&1 && echo "  ✅ DAASH done" || echo "  ❌ DAASH failed"

echo ">>> GoSource ingestion..."
python ipc_ingestion/gosource_incremental_load.py 2>&1 && echo "  ✅ GoSource done" || echo "  ❌ GoSource failed"

echo ">>> Lenco ingestion..."
python ipc_ingestion/lenco_incremental_load.py 2>&1 && echo "  ✅ Lenco done" || echo "  ❌ Lenco failed"

echo ">>> Paystack ingestion..."
python ipc_ingestion/paystack_incremental_load.py 2>&1 && echo "  ✅ Paystack done" || echo "  ❌ Paystack failed"

echo ">>> 9japay ingestion..."
python ipc_ingestion/9japay_incremental_load.py 2>&1 && echo "  ✅ 9japay done" || echo "  ❌ 9japay failed"

# dbt transformations
echo ">>> Running dbt..."
cd ipc_transform
PG_HOST=$(grep PG_HOST ../.env | head -1 | cut -d'=' -f2 | tr -d '\r"'"'"' ') \
PG_PORT=$(grep PG_PORT ../.env | head -1 | cut -d'=' -f2 | tr -d '\r"'"'"' ') \
PG_USER=$(grep PG_USER ../.env | head -1 | cut -d'=' -f2 | tr -d '\r"'"'"' ') \
PG_PASSWORD=$(grep PG_PASSWORD ../.env | head -1 | cut -d'=' -f2 | tr -d '\r"'"'"' ') \
python -m dbt run --profiles-dir . 2>&1 && echo "  ✅ dbt done" || echo "  ❌ dbt failed"

echo "========================================="
echo "  Finished — $(date '+%Y-%m-%d %H:%M:%S')"
echo "========================================="

# macOS notification so you know it's done
osascript -e 'display notification "Ingestion + dbt complete. Dashboards updated." with title "IPC Analytics" sound name "Glass"'
