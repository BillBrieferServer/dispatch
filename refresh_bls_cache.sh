#!/bin/bash
# Monthly BLS cache refresh for Idaho Bill Briefer
# Run on 1st of each month at 6 AM Mountain Time

cd /opt/billbriefer-sand
source .env
docker exec -e BLS_API_KEY=$BLS_API_KEY billbriefer-sand-app-1 python /app/app/bls_data_fetch.py >> /var/log/bls_refresh.log 2>&1
echo "[$(date -Iseconds)] BLS cache refresh completed" >> /var/log/bls_refresh.log
