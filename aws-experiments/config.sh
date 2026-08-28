# Shared config for the NLJ static-memory experiments. Source'd by every script in bin/.

# AsterixDB HTTP API endpoint (CC node)
HOST=localhost
PORT=19002
API="http://$HOST:$PORT/query/service"

# NC name as registered in cc.conf, and the directory ON THE NC MACHINE
# holding customer.json and store_returns.json
NC=asterix_nc1
DATA_DIR=/home/ec2-user/tpcds_data

# NC log file to pull NLJ-STATIC lines from after each run.
# Sample cluster default shown; adjust to the actual logs dir.
NC_LOG="$HOME/asterix-cluster/opt/local/logs/nc-${NC}.log"

# Where run_query.sh drops the raw response JSONs and the summary ledger
RESULTS_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/results"
