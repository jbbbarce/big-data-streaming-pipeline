#!/bin/bash
# Starts all 10 sensor producers in the background (logs in producers/logs/). Ctrl-C stops them all.
set -e
cd "$(dirname "$0")"

if ! ls virtual_producer_sensor_*.py >/dev/null 2>&1; then
  bash prep_producers.sh
fi

mkdir -p logs
trap 'kill $(jobs -p) 2>/dev/null' INT TERM EXIT

for f in virtual_producer_sensor_*.py; do
  python3 "$f" > "logs/${f%.py}.log" 2>&1 &
  echo "Started $f (pid $!)"
done

wait
