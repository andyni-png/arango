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

# SUT analysis: ArangoDB Agency

Hand-written from `k-scope.md`, `properties.md` and a reading of `arangod/Agency`. This
replaces a full `antithesis-research` pass, which we skipped on purpose (see Assumptions).

## Summary

The Agency is ArangoDB's internal Raft-replicated key-value store. It's compiled into
`arangod` and switched on with `--agency.activate true`. In a real cluster it holds the
cluster plan and state for coordinators and DB servers. Here it runs **on its own**: three
agents and nothing else.

The test targets **logical errors in the replicated log (appendEntries) and in applying
that log to the state machine**. Those are K's words (see `k-scope.md`).

## Components (source: `arangod/Agency`, devel)

| File | Role |
|---|---|
| `Agent.cpp` | Raft agent: leader write path, appendEntries send and receive, commit index, snapshot shipping to lagging followers (`Agent.cpp:604-706`), `readDB` (`Agent.cpp:1909`), compaction trigger (`Agent.cpp:1866-1885`) |
| `State.cpp` | The replicated log: persistence into the `log` collection, compaction into the `compact` collection (`State::compact`), `toVelocyPack` (only the in-memory log) |
| `Constituent.cpp` | Leader election, terms, votes |
| `Store.cpp` | The state machine: applies transactions and preconditions to the key-value tree |
| `AgentCallback.cpp` | The leader's handling of appendEntries responses (match index, commit advance) |
| `RestAgencyHandler.cpp` | The HTTP surface |

## HTTP surface (every call needs `Authorization: bearer <JWT>`)

- `POST /_api/agency/write`: an array of `[ops, preconditions?]` transactions. Returns
  307 when the agent isn't the leader.
- `POST /_api/agency/transact`: reads and writes mixed.
- `POST /_api/agency/read`: `[["key", ...]]`.
- `GET /_api/agency/config`: leader id, term, configuration.
- `GET /_api/agency/state`: that agent's commit index, term, `agency` (readDB) and `log`
  (only the in-memory entries).
- `GET /_api/agency/stores`: `read_db`, `spearhead` (uncommitted), `transient`.

## Failure-prone areas (why the test is worth running)

1. **A snapshot sent inside appendEntries.** A follower that falls behind the compaction
   point gets a snapshot plus the entry at the snapshot index, not ordinary entries. The
   code comments call this a special case. Low compaction settings make it happen often.
2. **Compaction happening at the same time as replication.** `State::compact` removes
   in-memory entries while the leader may still be sending them.
3. **Apply path with preconditions.** A precondition is evaluated at apply time, so an
   atomicity bug is an apply bug.
4. **Restart and recovery.** An agent reloads `compact` plus the `log` collection from
   disk and has to rebuild the same readDB.

## Language / build

- C++, built by ArangoDB's CMake. We don't build it. We use the prebuilt
  `arangodb/enterprise:3.12.12` image, which is Alpine-based with a glibc `arangod`.
- There is no Antithesis SDK in the SUT, and no coverage instrumentation (see
  `deployment-topology.md`).

## Findings from setup (verified on the local 3.12.12 cluster, 2026-09-29)

- **Followers don't keep a state machine.** `/state` and `/stores` on a follower return
  an empty `agency`/`read_db`. `_readDB.applyLogEntries` runs only in the leader's commit
  path (`Agent.cpp:900`). A new leader rebuilds `_readDB` from the last compaction
  snapshot plus the log (`Agent::rebuildDBs`, `Agent.cpp:1825`, called from
  `prepareLead`). Consequences:
  - P4 as written (compare every agent's readDB at the same commit index) **can't be
    observed**. Rewrite it as continuity across leader changes: each new leader's store
    must contain every acknowledged write and equal the store the client model predicts.
  - The apply path K cares about runs in two places: incremental apply on the leader, and
    **rebuild from snapshot plus log at every leader change**. The rebuild is the riskier
    one. Add a sometimes assertion: "a leader change happened after at least one
    compaction."
- **Log entries in `/state` carry the index as `_key`** (a zero-padded string), not as
  `index`. The commit index is the top-level `index`.
- **The compaction settings work.** With step 100 and keep 50, after about 400 writes
  every agent's in-memory log started at index 350 or later. Agent3 was stopped during all
  400 writes. When it restarted, its log began at 353, not 350, which means it caught up
  from a snapshot and not from entries. **The S4 path is reachable** with these settings.
- **No log line at INFO level marks the install of a snapshot received through
  appendEntries** (`State.cpp:398-441`). The same goes for sending one. To detect S4 from
  outside, watch the follower's first log index jump past the index it had before.
- A leader that restarts hands off cleanly. The term goes up, a new leader is elected, and
  the data is intact.

## Assumptions

- The `arangod/Agency` code in `devel` is close enough to 3.12.12 for reading. Line
  references may be off.
- `antithesis-research` was skipped on purpose. The scope and properties came from K and
  from reading the code by hand. The catalog is in `property-catalog.md`.
- Supervision is off (the default, `--agency.supervision false`) and out of scope.

## Open Questions

- How can the compaction snapshot be read from outside an agent? `/state` leaves it out,
  and P3 (store = apply(log)) needs it as the starting point.
- How can we observe that a snapshot was sent inside appendEntries (S4)? Candidates are a
  log line on the leader or follower, or a jump in the follower's first log index.
- Does the `devel` Agency code differ materially from 3.12.12?
- Should we build an instrumented `devel` binary for coverage-guided exploration? That's a
  question for K.
