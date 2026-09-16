#!/bin/bash
# Per-second system + NC monitor, CSV output. Based on Shiva's asterixdb_fullStat.sh,
# with the NC-process detection restored (jps) and the printf/substitution bugs fixed.
#   Usage: ./fullstat.sh <disk> [out.csv]      e.g. ./fullstat.sh nvme0n1 run42_stat.csv
# Find the disk name with: lsblk -d -o NAME,SIZE
# Note: iostat without an interval reports since-boot averages each tick; the MB_read /
# MB_write TOTALS are the reliable columns (diff them across the run for per-run I/O).

if [ $# -eq 0 ]; then
    echo "usage: $0 <diskname> [outfile]"
    exit 1
fi
DISK=$1
OUT=${2:-/dev/stdout}

printf "Time,tps,MB_read/s,MB_write/s,MB_read,MB_write,total_memory,memory_used,memory_free,memory_shared,buff_cache,memory_available,NC_virt,NC_res,NC_cpu\n" > "$OUT"

while true; do
    diskuse=$(iostat "$DISK" -m | awk -v d="$DISK" '$1==d { print $2","$3","$4","$5","$6; exit }')
    memoryuse=$(free -m | awk '/^Mem:/ { print $2","$3","$4","$5","$6","$7 }')

    # The NC is the java process jps names NCDriver (skip CCDriver/NCService, as in the original)
    pid=$(jps 2>/dev/null | awk '$2=="NCDriver" {print $1; exit}')
    if [ -n "$pid" ]; then
        nc=$(top -bn1 -p "$pid" | awk -v p="$pid" '$1==p {print $5","$6","$9; exit}')
    fi
    [ -n "$nc" ] || nc=",,"

    dt=$(date +'%Y-%m-%d_%T')
    printf "%s,%s,%s,%s\n" "$dt" "$diskuse" "$memoryuse" "$nc" >> "$OUT"
    nc=""
    sleep 1
done
