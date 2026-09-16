#!/usr/bin/env bash
# Create datasets and load data: 01_create.sqlpp, then 02_load.sqlpp with
# __NC__ / __DATA_DIR__ substituted from config.sh. Run once per cluster.
set -euo pipefail
cd "$(dirname "$0")"
source ../config.sh

echo "== creating datasets =="
./post_sqlpp.sh ../sql/01_create.sqlpp

echo "== loading data from $DATA_DIR on $NC (this is the slow step) =="
TMP=$(mktemp)
sed -e "s|__NC__|$NC|g" -e "s|__DATA_DIR__|$DATA_DIR|g" ../sql/02_load.sqlpp > "$TMP"
./post_sqlpp.sh "$TMP"
rm -f "$TMP"

echo "== done; now run verify.sh =="
