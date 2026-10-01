# Scope: Antithesis test of the ArangoDB Agency

## Original email from K (verbatim)

```text
Hi gentlemen,

My straightforward answer would be any of the guarantees that RAFT makes. And really, the only relevant is a logical error either in appendEntries, i.e. the replicated log itself or the application to the state machine.

This starts a container with the latest image and exposes the ports 4001 to 4003 to the outside:
> docker run -d --rm --name agency -p 4001-4003:4001-4003 -e ARANGO_NO_AUTH=true arangodb/enterprise sleep 1000000

Now, create a shared secret, and start a 3-node agency serving the exposed ports.

> docker exec -it agency sh

# echo "hello world" > /jwt.secret
# arangod --server.jwt-secret-keyfile /jwt.secret --agency.size 3 --agency.activate true --agency.endpoint tcp://[::1]:4001 --agency.endpoint tcp://[::1]:4002 --agency.endpoint tcp://[::1]:4003 --agency.my-address tcp://[::1]:4001 --server.endpoint tcp://[::]:4001 --database.directory agent-1&
# arangod --server.jwt-secret-keyfile /jwt.secret --agency.size 3 --agency.activate true --agency.endpoint tcp://[::1]:4001 --agency.endpoint tcp://[::1]:4002 --agency.endpoint tcp://[::1]:4003 --agency.my-address tcp://[::1]:4002 --server.endpoint tcp://[::]:4002 --database.directory agent-2&
# arangod --server.jwt-secret-keyfile /jwt.secret --agency.size 3 --agency.activate true --agency.endpoint tcp://[::1]:4001 --agency.endpoint tcp://[::1]:4002 --agency.endpoint tcp://[::1]:4003 --agency.my-address tcp://[::1]:4003 --server.endpoint tcp://[::]:4003 --database.directory agent-3&
# exit

> curl -kH"Authorization: bearer eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJhcmFuZ29kYiIsInNlcnZlcl9pZCI6ImZvbyJ9.Q7ieHXQQk0A2BTFTeXDYEujLYxaxC7oYK5BD3Xg4jFg" localhost:4001/_api/version | jq 

This should now give you an output like this:

{
  "server": "arango",
  "license": "enterprise",
  "version": "3.12.12",
  "apiVersions": [
    "v0"
  ],
  "deprecatedApiVersions": [],
  "requestedApiVersion": "v0"
}

One writes something to the replicated log like so:
POST _api/agency/write -d’[[{“key1”:”value1"},{“key2”:”value2"}], [{“key3”:”value3"}]]’

write value1 to key1, if key2 equals value2 each of the inner arrays is a transaction.

POST _api/agency/write -d’[[{“key1”:”value1”, “key4”:”value4"},{“key2”:”value2"}], [{“key3”:”value3"}]]’

write key1 and key4 provided key2 is “value2"

This is only so you see, that I have not forgottem you. We don’t have a documentation of the agency, as it is purely meant to be used inside arangodb clusters.

POST _api/agency/read -d’[[“key1"]]’ 

read key1.

GET _api/agency/state | jq .agency returns the state machine

GET _api/agency/state | jq .log    returns the replicated log

Maybe we meet once to go over the funcitonality and then you guys have your way with the agency.

With my highest regards,
- K
```

## Practical details from the email

- **Auth:** agents run with `--server.jwt-secret-keyfile`. K's secret is `hello world`, and
  requests carry `Authorization: bearer <JWT>` signed with it. The token in his email
  (`iss: arangodb`, `server_id: foo`) works for that secret. The harness should use the
  same scheme.
- **Agent flags:** `--agency.size 3 --agency.activate true`, one `--agency.endpoint` per
  agent, `--agency.my-address`, `--server.endpoint`, `--database.directory`.
- **Write format:** the body is an array of transactions. Each transaction is
  `[operations, preconditions]`, and the preconditions are optional. Example:
  `[[{"key1":"value1","key4":"value4"},{"key2":"value2"}], [{"key3":"value3"}]]` means
  "write key1 and key4 if key2 equals value2", followed by an unconditional write of
  key3.
- **Read format:** `POST /_api/agency/read` with `[["key1"]]`.
- **Observation:** `GET /_api/agency/state` returns `.agency` (the state machine) and
  `.log` (the replicated log). This matches what's in the code (`Agent::readDB`).
- **No official Agency docs.** K offered a walkthrough call, which is worth taking before
  or right after the first run.

## What counts as a bug

K's words: "any of the guarantees that RAFT makes … the only relevant is a logical error
either in appendEntries, i.e. the replicated log itself or the application to the state
machine." That means **logical errors** in the Agency's Raft implementation:

1. **The replicated log (appendEntries).** Agents disagree on log contents, a committed
   entry is lost or rewritten, or a follower ends up with a wrong log after catching up,
   including catching up by snapshot.
2. **Applying the log to the state machine.** An agent's key-value store (`readDB`)
   doesn't equal the result of applying its committed log, agents that applied the same
   prefix end up with different stores, or a conditional transaction is applied partially
   or when its precondition didn't hold.

**Out of scope** (don't build properties for these): performance, availability and
liveness under faults, the Supervision logic, the coordinator and DB-server layers,
cluster-level behavior above the Agency, and the HTTP API's ergonomics.

## Open question for K

- **Release or devel?** Probably settled: K's own demo uses "the latest image",
  `arangodb/enterprise` 3.12.12. Pin `arangodb/enterprise:3.12.12` unless he says devel.
  Our sparse checkout is `devel`, so confirm the Agency code hasn't diverged much from
  3.12 before relying on line references.

## System under test

- The Agency is compiled into the `arangod` binary, so no separate build is needed. Use
  the `arangodb/enterprise` image.
- Source for reading only: `arangod/Agency` (sparse checkout of `devel` in this repo).
  Key files are `Agent.cpp`, `State.cpp`, `Constituent.cpp`, `Store.cpp`,
  `RestAgencyHandler.cpp`, and `AgentCallback.cpp`.

## Topology

- Three agents, **one container each**, all from the same image. Each is addressed by
  hostname (`tcp://agent1:8531`, etc.), not `[::1]`.
- Each agent's data directory is on a persistent volume so it survives a kill or restart.
- Set `--agency.compaction-keep-size` and `--agency.compaction-step-size` low so
  compaction, and therefore snapshots sent over appendEntries, actually happens during
  a run.
