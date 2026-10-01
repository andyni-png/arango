---
sut_path: /home/exedev/arangodb
commit: 0ced0d52ca82969240e115707899223c06ba2030
updated: 2026-09-29
external_references:
  - path: docker.io/arangodb/enterprise:3.12.12
    why: The binary under test. The source checkout is devel and is used for reading only.
  - path: /home/exedev/arangodb/k-scope.md
    why: Customer scope and demo setup from K's email.
---

# Deployment topology

## Summary

There are four containers. Three agents, one per container, so Antithesis can partition,
pause or kill each agent on its own. The fourth is a workload/client container. K's demo
ran all three agents in one container, and that doesn't work under fault injection.

| Service (container = hostname) | Image | Role |
|---|---|---|
| `agency-agent1` | `arangodb-agency-agent:latest` (thin, `FROM arangodb/enterprise:3.12.12`) | agent, `tcp://agency-agent1:8531` |
| `agency-agent2` | same | agent, `tcp://agency-agent2:8531` |
| `agency-agent3` | same | agent, `tcp://agency-agent3:8531` |
| `agency-workload` | `arangodb-agency-workload:latest` (debian:stable-slim + Python + Antithesis SDK) | client. Waits for the cluster, fires the bootstrap assertion, emits `setup_complete`, and later runs test templates from `/opt/antithesis/test/v1/` |

## Agent command line

These are K's flags, except that `[::1]` is replaced by hostnames and every agent uses
port 8531:

```
arangod
  --server.endpoint tcp://0.0.0.0:8531
  --server.jwt-secret-keyfile /etc/arangodb-agency/jwt.secret     # "hello world", as in K's demo
  --agency.activate true --agency.size 3
  --agency.my-address tcp://<self>:8531
  --agency.endpoint tcp://agency-agent1:8531 (x3, one per agent)
  --agency.compaction-step-size 100
  --agency.compaction-keep-size 50
  --database.directory /var/lib/arangodb-agency      # named volume per agent
```

**Compaction values.** The defaults are step 1000 and keep 50000, from
`arangod --dump-options` in the image. With those, a follower would have to fall 50000
entries behind before the leader sent a snapshot instead of entries, which a short run
never reaches. With step 100 and keep 50, a compaction runs about every 100 committed
entries and keeps only 50 behind the compaction point. So a follower that is partitioned
or down for a few hundred writes has to catch up by snapshot (the path at
`Agent.cpp:604-706`). The code doesn't enforce a minimum. `Agent.cpp:1866` only forbids
keep-size 0.

**Left at their defaults on purpose:**
- `--agency.wait-for-sync true`: faking durability would create false P2 failures.
- `--agency.supervision false`: out of scope.
- The election timeouts: 0.5–2.5 s (set in `images/agent/entrypoint.sh`, arangod default is 1–5 s), so the short partitions Antithesis injects can trigger elections.
- `--agency.max-append-size 250`.

## Volumes

Each agent has its own named volume, `agentN-data`, for its database directory. The volume
survives a container kill and restart, which the durability property (P2) needs.

## Auth

The JWT secret is baked into the agent image. The workload uses a static HS256 token signed
with the same secret: `iss=arangodb, server_id=foo`, the same token as in K's email.

## Instrumentation inventory

| Service | Language | Decision | Reason |
|---|---|---|---|
| agents | C++ | **uninstrumented** | Prebuilt vendor enterprise binary. Coverage instrumentation would mean rebuilding ArangoDB from source with `-fsanitize-coverage=trace-pc-guard`, which is out of scope for now. The follow-up option is an instrumented `devel` build, if K wants it. |
| workload | Python | **SDK + cataloged** | The `antithesis` SDK is installed. `/opt/antithesis/catalog/` symlinks to the workload source. Python has cataloging only, no coverage. |

The bootstrap property is in the workload entrypoint:
`reachable("agency cluster elected a leader and served a write")`.

## Health and ordering

- Agent healthcheck: an authenticated `GET /_api/version` using the image's BusyBox
  `wget`. The image has no `curl` and no Python.
- The workload `depends_on` all three agents with `service_healthy`. It then also waits
  until every agent reports the same leader through `/_api/agency/config`, because a
  healthy HTTP port doesn't mean a leader has been elected.

## Assumptions

- The fault surface is Antithesis's default network and process faults. No custom fault
  config.

## Open Questions

- The test templates in `/opt/antithesis/test/v1/` are to be defined by
  `antithesis-feature-workload`.
