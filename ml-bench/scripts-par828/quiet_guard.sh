#!/bin/bash
# Stop every PAR-828 bench container before the 01:00 UTC quiet window.
while true; do
  hm=$(date -u +%H%M)
  if [ "$hm" -ge 0055 ] && [ "$hm" -lt 0930 ]; then
    for c in $(docker ps --format '{{.Names}}' | grep '^mlbench-par828-'); do
      echo "$(date -u +%FT%TZ) quiet window: stopping $c"; docker stop -t 30 $c; done
  fi
  sleep 60
done
