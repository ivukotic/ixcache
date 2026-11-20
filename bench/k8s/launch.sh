#!/usr/bin/env bash
# Starts one ixbench Job: launch.sh coldhot|warm|stress [start-delay-seconds]
#
# Fills in the run label (also the Job name suffix), a fresh random seed (used
# by cold selections, so each run reads new files) and, for stress, a common
# start time far enough ahead for every pod to be scheduled and pull the image.
set -euo pipefail

kind=${1:?usage: launch.sh coldhot|warm|stress [start-delay-seconds]}
delay=${2:-180}
dir=$(cd "$(dirname "$0")" && pwd)
tmpl="$dir/$kind.yaml"
[ -f "$tmpl" ] || { echo "no $tmpl" >&2; exit 1; }
if grep -q 'CHANGE-ME' "$tmpl"; then
  echo "set ENDPOINTS in $tmpl first" >&2
  exit 1
fi

run=$(date -u +%Y%m%d-%H%M%S)
start=$(( $(date +%s) + delay ))
seed=$(( RANDOM * 32768 + RANDOM ))

sed -e "s/__RUN__/$run/g" -e "s/__START_AT__/$start/g" -e "s/__SEED__/$seed/g" "$tmpl" \
  | kubectl create -f -

job="ixbench-$kind-$run"
echo
echo "job:      $job"
[ "$kind" = stress ] && echo "starts:   $(date -d "@$start" 2>/dev/null || echo "$start (epoch)")"
echo "follow:   kubectl -n ixcache logs -f -l job-name=$job --prefix --max-log-requests=100"
echo "results:  $dir/collect.sh $job"
