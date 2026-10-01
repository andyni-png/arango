"""Workload container entrypoint.

Waits until the 3-agent Agency has elected a leader that every agent agrees
on, does one write and read back through the leader, fires the bootstrap
assertion, signals setup_complete, then runs the self-driving workload for
the replicated write path (workload.py) forever, or for WORKLOAD_DURATION
seconds when that is set (local runs).
"""

import os
import sys
import time

from antithesis.assertions import reachable
from antithesis.lifecycle import setup_complete

from agency_client import AGENTS, AgencyClient, Ambiguous
from workload import Workload, draw_config


def wait_for_agreed_leader(client):
    while True:
        leaders = {}
        for agent in AGENTS:
            try:
                leaders[agent] = client.config(agent).get("leaderId", "")
            except Exception as exc:  # agent not up yet
                leaders[agent] = f"<error: {exc}>"
        ids = set(leaders.values())
        if len(ids) == 1 and next(iter(ids)) and not next(iter(ids)).startswith("<"):
            print(f"leader agreed by all agents: {leaders}", flush=True)
            return next(iter(ids))
        print(f"waiting for leader agreement: {leaders}", flush=True)
        time.sleep(1)


def bootstrap_probe(client):
    """Write a fresh stamp and read it back through the leader. Retries until
    it works: the container can be restarted mid-run (node stop fault), so this
    may run while faults are active and a timeout here is not a failure."""
    while True:
        leader = wait_for_agreed_leader(client)
        stamp = str(time.time_ns())
        try:
            result = client.write([[{"/bootstrap/probe": stamp}]])
            print(f"bootstrap write result: {result}", flush=True)
            read = client.read([["/bootstrap/probe"]])
            print(f"bootstrap read result: {read}", flush=True)
        except Ambiguous as exc:
            print(f"bootstrap attempt was ambiguous, retrying: {exc}", flush=True)
            time.sleep(1)
            continue
        if read[0].get("bootstrap", {}).get("probe") != stamp:
            print("bootstrap read did not return the written value", flush=True)
            sys.exit(1)
        return leader, result


def main():
    client = AgencyClient()
    leader, result = bootstrap_probe(client)

    reachable(
        "agency cluster elected a leader and served a write",
        {"leader": leader, "write": result},
    )
    setup_complete({"leader": leader})
    print("setup_complete emitted; starting workload", flush=True)

    duration = os.environ.get("WORKLOAD_DURATION")
    Workload(draw_config()).run(float(duration) if duration else None)
    if duration:
        print("workload finished; idling", flush=True)
    while True:
        time.sleep(3600)


if __name__ == "__main__":
    main()
