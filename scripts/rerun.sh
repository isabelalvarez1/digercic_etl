#!/bin/bash
# Re-ejecuta la captación completa con el fix de batch fijo
# Usar en 95.24: bash scripts/rerun.sh
set -e
cd /home/python_etl/digercic_etl
echo "[$(date '+%Y-%m-%d %H:%M:%S')] Haciendo git pull..."
git pull origin main
echo "[$(date '+%Y-%m-%d %H:%M:%S')] Verificando conexiones..."
.venv/bin/python scripts/check_connections.py --config config/pipeline.yaml
echo "[$(date '+%Y-%m-%d %H:%M:%S')] Lanzando ETL..."
.venv/bin/python run.py --config config/pipeline.yaml
echo "[$(date '+%Y-%m-%d %H:%M:%S')] Finalizado. Revisa logs/ y SELECT COUNT(*) en Postgres."
