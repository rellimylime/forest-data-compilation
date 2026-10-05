#!/usr/bin/env bash
set -u

repo_root=$(cd "$(dirname "$0")/../../.." && pwd)
cd "$repo_root" || exit 1

output_dir=${1:-/home/tippingPoint/ermiller/bien_clipping_supplement}
limit=${BIEN_CLIPPING_ATTEMPT_LIMIT:-4h}
max_attempts=${BIEN_CLIPPING_MAX_ATTEMPTS:-2}
success_marker="$output_dir/RUN_SUCCESS"
runner_log="$output_dir/logs/watchdog_runner.log"
mkdir -p "$output_dir/logs"

attempt=1
while [ "$attempt" -le "$max_attempts" ]; do
  printf "%s | clipping watchdog attempt %s started (%s limit)\n" \
    "$(date -u +%Y-%m-%dT%H:%M:%SZ)" "$attempt" "$limit" | tee -a "$runner_log"
  timeout --signal=TERM --kill-after=2m "$limit" \
    Rscript 06_species_niches/qa/scripts/10_bien_clipping_supplement.R \
    --output-dir="$output_dir" --reuse-policy >> "$runner_log" 2>&1 &
  timeout_pid=$!

  while kill -0 "$timeout_pid" 2>/dev/null; do
    if [ -f "$success_marker" ] && grep -q "^status=success$" "$success_marker"; then
      child_pids=$(pgrep -P "$timeout_pid" 2>/dev/null || true)
      if [ -n "$child_pids" ]; then kill -TERM $child_pids 2>/dev/null || true; fi
      kill -TERM "$timeout_pid" 2>/dev/null || true
      wait "$timeout_pid" 2>/dev/null || true
      printf "%s | clipping completed successfully on attempt %s\n" \
        "$(date -u +%Y-%m-%dT%H:%M:%SZ)" "$attempt" | tee -a "$runner_log"
      exit 0
    fi
    sleep 5
  done
  wait "$timeout_pid"
  run_status=$?

  if [ -f "$success_marker" ] && grep -q "^status=success$" "$success_marker"; then
    printf "%s | clipping completed successfully on attempt %s\n" \
      "$(date -u +%Y-%m-%dT%H:%M:%SZ)" "$attempt" | tee -a "$runner_log"
    exit 0
  fi

  if [ "$run_status" -ne 124 ]; then
    printf "%s | clipping failed with status %s; non-timeout errors are not retried\n" \
      "$(date -u +%Y-%m-%dT%H:%M:%SZ)" "$run_status" | tee -a "$runner_log"
    exit "$run_status"
  fi

  printf "%s | attempt %s timed out; restarting from completed checkpoints\n" \
    "$(date -u +%Y-%m-%dT%H:%M:%SZ)" "$attempt" | tee -a "$runner_log"
  attempt=$((attempt + 1))
done

printf "%s | clipping exhausted %s attempts without success\n" \
  "$(date -u +%Y-%m-%dT%H:%M:%SZ)" "$max_attempts" | tee -a "$runner_log"
exit 124
