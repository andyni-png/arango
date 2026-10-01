#!/bin/sh
# Start one Agency member. The agent's own address is its hostname, which
# equals its compose container_name (agency-agent1..3).
set -eu
PORT=8531
SELF="$(hostname)"
exec arangod \
  --server.endpoint "tcp://0.0.0.0:${PORT}" \
  --server.jwt-secret-keyfile /etc/arangodb-agency/jwt.secret \
  --agency.activate true \
  --agency.size 3 \
  --agency.my-address "tcp://${SELF}:${PORT}" \
  --agency.endpoint "tcp://agency-agent1:${PORT}" \
  --agency.endpoint "tcp://agency-agent2:${PORT}" \
  --agency.endpoint "tcp://agency-agent3:${PORT}" \
  --agency.compaction-step-size "${AGENCY_COMPACTION_STEP_SIZE:-100}" \
  --agency.compaction-keep-size "${AGENCY_COMPACTION_KEEP_SIZE:-50}" \
  --agency.election-timeout-min "${AGENCY_ELECTION_TIMEOUT_MIN:-0.5}" \
  --agency.election-timeout-max "${AGENCY_ELECTION_TIMEOUT_MAX:-2.5}" \
  --log.use-json-format false \
  --log.color false \
  --database.directory /var/lib/arangodb-agency \
  "$@"
