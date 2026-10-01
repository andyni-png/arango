#!/usr/bin/env bash
# Local stand-in for Antithesis fault injection, run from the host while the
# local stack is up. Each round picks one agent and one fault: restart,
# stop-then-start (long outage -> snapshot catch-up), pause (node hang), or
# network disconnect (partition). Usage: local-chaos.sh [seconds]
set -euo pipefail
DURATION="${1:-200}"
NET="arangodb-agency_default"
end=$(( $(date +%s) + DURATION ))
agents=(agency-agent1 agency-agent2 agency-agent3)
while [ "$(date +%s)" -lt "$end" ]; do
  a=${agents[$((RANDOM % 3))]}
  case $((RANDOM % 4)) in
    0) echo "chaos: restart $a"; docker restart -t 1 "$a" >/dev/null ;;
    1) d=$((5 + RANDOM % 15)); echo "chaos: stop $a for ${d}s"; docker stop -t 1 "$a" >/dev/null; sleep "$d"; docker start "$a" >/dev/null ;;
    2) d=$((2 + RANDOM % 8)); echo "chaos: pause $a for ${d}s"; docker pause "$a" >/dev/null; sleep "$d"; docker unpause "$a" >/dev/null ;;
    3) d=$((3 + RANDOM % 12)); echo "chaos: partition $a for ${d}s"; docker network disconnect "$NET" "$a"; sleep "$d"; docker network connect --alias "$a" "$NET" "$a" ;;
  esac
  sleep $((5 + RANDOM % 10))
done
echo "chaos: done"
