#!/usr/bin/env bash
# Saves the logs of every pod of an ixbench Job and prints the report:
#   collect.sh <job-name> [report options, e.g. --verify --per-endpoint --timeseries t.csv]
# --verify needs ES_URL (and credentials) in the environment.
set -euo pipefail

job=${1:?usage: collect.sh <job-name> [report options]}
shift
dir=$(cd "$(dirname "$0")" && pwd)
out="$job.log"

kubectl -n ixcache logs -l "job-name=$job" -c ixbench --prefix --tail=-1 --max-log-requests=200 > "$out"
echo "logs saved to $out ($(grep -c 'IXBENCH ' "$out") records)" >&2

py=$(command -v python3 || command -v python)
"$py" "$dir/../ixbench.py" report "$out" "$@"
