# Feature workload: Agency replicated write path

**Feature.** Writes and conditional transactions replicated through appendEntries
(including snapshot catch-up after compaction) and applied to the leader's key-value
store, both incrementally and by the rebuild on each leader change.

**Status.** Feature is done (shipped in `arangodb/enterprise:3.12.12`). All properties
target built code, so there are no stubs.

**Scope.** This follows `k-scope.md`: logical errors in the replicated log or in applying
it. The property catalog is `antithesis/scratchbook/property-catalog.md`, mirrored from
`properties.md`. For orientation see `antithesis/scratchbook/sut-analysis.md`.

## How the feature works (code paths, devel checkout; 3.12 may differ slightly)

| Step | Where |
|---|---|
| The client POSTs `/_api/agency/write`: `[[ops, preconditions, clientId], ...]`. A non-leader answers with 307. Before touching the log, the leader returns 503 "No leader". | `RestAgencyHandler::handleWrite` (343) |
| The leader applies to `_spearhead` (preconditions evaluated here), appends to the log, and chunks the input by `max-append-size` (250). Precondition failure means index 0, nothing logged, HTTP 412. | `Agent::write` (1326), `State::logLeaderMulti` |
| The handler waits for the commit of the highest index. On a timeout it returns 408, and if the commit is unknown it returns 503. **Either way the write may still commit.** | `Agent::waitFor` |
| appendEntries to followers. If the follower's next index is below the leader's first in-memory index, the leader sends the **snapshot plus the entry at the snapshot index**. | `Agent::sendAppendEntriesRPC` (604-706) |
| The follower installs the snapshot: it erases its log and compaction data and stores the snapshot. | `State::storeLogFromSnapshot`, `State.cpp:398-441` |
| The commit index advances once a quorum confirms. **Only the leader** applies committed entries to `_readDB`. | `Agent.cpp:880-900` |
| Compaction runs every `compaction-step-size` commits and keeps `compaction-keep-size` entries. | `Agent.cpp:1866-1885`, `State::compact` |
| A new leader runs `prepareLead` then `rebuildDBs`: it clears the stores, loads the last compaction snapshot, and applies the log up to the commit index. | `Agent.cpp:1676`, `1825` |
| Reads are served only by the leader, from `_readDB`, after `challengeLeadership()` (a time-based lease: recent acks from a majority). | `Agent::read` (1426) |
| `/_api/agency/inquire [clientIds]` returns the index of each clientId in the leader's log, or 0. It waits while a transaction is still in progress. | `Agent::inquire` (1282), `State::inquire` (1654) |
| A demoted leader keeps its old `_readDB`. `resign` doesn't clear it, while its commit index keeps advancing as a follower. | `Agent::resign`, `Constituent::followNoLock` |

Concurrency: many HTTP threads call `Agent::write` at once. They're serialized by
`_ioLock` for the log append and `_outputLock` for readDB. Replication and commit run on
the agent's own thread. Compaction runs under `_logLock` while appendEntries may be
building payloads. That interleaving is the risky one.

## Discovered properties and how the workload checks them

Assertion names are the exact strings in the code.

| # | Property | Assertion(s) | Type, why |
|---|---|---|---|
| P1 | Log matching | "Log entries with the same index and term are identical on every agent"; "If two agents share a log entry, they agree on every earlier entry they both hold" | Always. `(index, term)` pins down one entry and its prefix, so comparing observations taken at different times is valid. |
| P2a | Acknowledged writes are durable | "After quiescing, every acknowledged write is still in the store" / "...push is still in the list" / "...the counter includes every acknowledged increment" | Always, checked at quiesced checkpoints with writes and faults stopped. Checks lower bounds, not exact values. |
| P2a' | Linearizable reads (a Raft read guarantee) | "A read reflects every write acknowledged before the read began" / list / counter variants | Always, on every live read. **If only these fail and the quiesced ones pass, suspect a stale read from a deposed leader, not data loss.** |
| P2b | Committed entries never change | "A committed log entry never changes on any agent"; "The log index returned to the client holds that client's transaction"; "Each client transaction is committed at most once" | Always |
| P3 | Leader store = newest committed log entry per key | "The leader's store holds the value of the newest committed log entry for each key" | Always. Guarded: the agent must report itself leader in the same term before and after the `/state` read, because demoted leaders keep a stale store. |
| P4 | Store survives the rebuild at a leader change | Covered by all the store-model checks run against whichever leader answers. Reach claim "The client model was checked against a leader that rebuilt its store from a snapshot" proves it ran against a rebuilt leader. | Always plus Reachable |
| P5 | Snapshot catch-up is correct | P1 and P2b on the follower's log after catch-up. P3 and P4 once it leads. | Covered by other asserts |
| P6 | Conditional and multi-key atomicity | "A conditional write was only applied when its precondition held" (walks the committed ledger to the previous write of the key); "A write rejected by its precondition has no effect on the store"; "A transaction rejected by its precondition never appears in the committed log" (Unreachable); "A create-once key is never overwritten when oldEmpty is required" (Unreachable); "A multi-key transaction is never seen partially applied" | Always / Unreachable |
| - | Apply integrity | "The store only holds values the workload wrote to that key"; "The list only holds values the workload pushed"; "A pushed value appears in the list at most once"; "The counter never exceeds the number of increments attempted"; "A committed log entry carries exactly the operations the client sent" | Always. A double apply during a rebuild shows up as a duplicate push or an over-count. |
| P7 | At most one leader per term | "At most one agent claims leadership in any term" | Always. Only counts an agent claiming leadership for itself, so a follower's stale view can't trigger it. |

**Reach claims** (precondition, never the violation):
- "Compaction removed old entries from an agent's in-memory log" (S3)
- "A follower fell behind the start of the leader's log, so only a snapshot can catch it
  up" and "A lagging follower caught up from a snapshot sent in appendEntries" (S4)
- "A new leader took over with a compacted log and rebuilt its store from a snapshot" (S7)
- "The client model was checked against a leader that rebuilt its store from a snapshot"
  (S8)
- "A write was committed after a leader change" (S1)
- "agent answered again after being unreachable" (S2)
- "A conditional write was rejected because its precondition failed" (S5)
- The unknown-outcome claims, all S6:
  - "A write ended with an unknown outcome (408, 503 or timeout)"
  - "...was later found committed in the log"
  - "...was resolved as committed by inquire"
  - "...was not in the leader's log"
- "A quiesced durability check ran"
- Bootstrap: "agency cluster elected a leader and served a write"

## Workload design (`antithesis/workload/`)

- `entrypoint.py`: bootstrap, then `setup_complete`, then `Workload(draw_config()).run()`.
  It runs forever, or for `WORKLOAD_DURATION` seconds when that's set (local runs).
- `workload.py`:
  - writer threads send batches of transactions to random agents
  - a reader checks every `/wl` read against the model
  - checkpoints pause the writers, call `ANTITHESIS_STOP_FAULTS 45` when it's set, wait
    for all agents to agree on the leader and commit index, resolve maybe-writes with
    `inquire`, then run the quiesced durability check
- `model.py`: each transaction has a unique clientId and writes unique values, and is
  tracked as ACKED(index), REJECTED or MAYBE. Checks are bounds: a floor from writes
  acknowledged before the read started, a ceiling from writes attempted before the read
  ended.
- `log_checker.py`: polls `/config`, `/state` and `/config` again on every agent.
  - It keeps a committed ledger (index → term, request, clientId) and an `(index, term)`
    ledger.
  - Committed entries whose clientId belongs to a MAYBE transaction resolve it.
- **Key layout:**
  - registers `/wl/r/r<i>`
  - groups `/wl/g/g<j>/{a,b,c}` (only ever written together)
  - counter `/wl/c`
  - list `/wl/l`
  - create-once keys `/wl/f/<id>`
- **Swarm:** drawn once per run through `antithesis.random`:
  - writer count
  - per-kind op weights, with random omission
  - batch-size probabilities
  - number of registers and groups
  - how often CAS uses a stale expected value
  - op form (`{"op":"set"}` or a plain value)
  - value padding
  - pacing
  - client timeout
  - read, poll and checkpoint intervals
- **Value menu** (from configured limits): batch sizes
  `{1,2,3, keep±1, step±1, max-append-size(250)±1}` = `{49,50,51,99,100,101,249,250,251}`.
  These hit the compaction boundaries and the write-chunking and appendEntries batch
  boundary inside one request. Value padding is `{0, 64, 4096}` bytes, which grows
  snapshots. Stored values themselves are opaque unique strings, so there's no menu for
  them.
- **Faults** (`references/faults.md`):
  - Partitions and bad nodes cause leader changes and lagging followers (S4, S7).
  - Node hang behaves like a partition.
  - Node termination tests restarts and the durability of each agent's own log. It's
    **disabled by default. Confirm it's enabled for the tenant.**
  - Congestion and throttling produce 408/503 and maybe-writes.
  - Clock jitter matters: the read lease (`challengeLeadership`) and election timeouts
    are time-based, so it's relevant to P2a' and P7. Confirm it's enabled.
  - Thread pausing needs instrumentation, which `arangod` doesn't have. CPU modulation is
    the substitute.

## Local validation (2026-09-29)

**Local tooling:**
- `docker-compose.local.yaml` sets `ANTITHESIS_SDK_LOCAL_OUTPUT` and `WORKLOAD_DURATION`.
- `local-chaos.sh` runs on the host and injects restart, stop/start, pause and network
  disconnect.
- `python3 /opt/agency-workload/local_report.py` lists every cataloged assertion as pass,
  FAIL or never fired.

| Run | Config highlights | Result |
|---|---|---|
| 1, no faults | 8 writers, 0.5 s timeout | No violations. 97% of writes MAYBE (timeouts). **Harness bug found:** the old S4 detector ("first index jumped past the previous last") fired on ordinary compaction under fast writes. It was replaced by "follower.last < leader.first, with the leader polled first". The 0.5 s timeout is now drawn less often. |
| 2, chaos 200 s | 1 writer, 1 register, 3 groups, CAS-heavy | **No violations.** 10,490 acked, 7,424 rejected, 230 maybe, 15 quiesced checkpoints, 932 guarded leader-store checks. 36/37 assertions passed or fired, including snapshot catch-up, rebuild from snapshot, the model checked on a rebuilt leader, and a write after a leader change. |
| 3, oracle check | Wrote a foreign value to `/wl/f/...` | "The store only holds values the workload wrote to that key" **fired**, so the path from assertion to report works end to end. |
| Unit checks | Synthetic stores and logs | Each model check (stale, lost, rejected, unknown, torn, duplicate, missing, counter over/under) and each log check (entry rewrite, same index and term with different content, store ≠ log, CAS with the wrong old value, prefix divergence) caught its injected violation. |
| 4, chaos 260 s | 1 writer, increments and pushes heavy, 5 s timeout | **"A committed log entry never changes on any agent" FAILED.** See the candidate finding below. Everything else passed. 9,039 acked, 2,129 maybe, 671 reads checked, 10 checkpoints. |

**Known blind spot:** the store-read checks only see corruption still visible when the
read happens. A foreign or corrupt value written to a hot register and overwritten within
the read interval goes unnoticed. Two things cover this. The log checks (P1, P2b, "carries
exactly the operations the client sent") look at every committed entry, not just the ones
a read happens to see. The create-once keys never change, so any corruption to them stays
visible.

## Candidate finding F1: a follower commit index can cover stale, never-committed entries

**Status: PARKED.** It was found locally, not by Antithesis, so don't use it with K for now. The full write-up is in `antithesis/findings/F1-follower-commit-index-covers-stale-entries.md`. Mechanism confirmed in the code, seen once locally, not yet shown reaching a client-visible store.

**What was observed (run 4).**
- The committed ledger records index 101 as term 2: the new leader's `.agency` config
  entry.
- Later, agent2's own `/state` showed index 101 as term 1, holding
  `{"/wl/l": {"op":"push","new":"v0.99..."}}` from client `w0-99`, while agent2's commit
  index (`index`) was at least 101.
- `/state` reads `_commitIndex` and then that same agent's log in one request
  (`Agent::readDB`, `Agent.cpp:1909`). A log entry can only move from stale to correct.
  So at one moment agent2 both held the stale entry and counted it as committed.

**Mechanism** (devel, `Agent::recvAppendEntriesRPC`). Both follower commit updates use the
follower's own last log index:
- empty appendEntries: `min(leaderCommitIndex, _state.lastIndex())` (`Agent.cpp:~527`)
- non-empty appendEntries: `lastIndex = _state.logFollower(payload)`, which returns
  `_log.back().index` (`State.cpp`, end of `logFollower`), then
  `min(leaderCommitIndex, lastIndex)` (`Agent.cpp:~557`)

`removeConflicts` only truncates where the incoming batch overlaps. Stale entries past the
batch survive, and the commit index then covers them. Raft (Figure 2) specifies
`min(leaderCommit, index of last new entry)` precisely to prevent this.

**Why it can matter to K (apply correctness).**
- Followers do run compaction. `Agent::compact` passes `_commitIndex`, and `State::compact`
  applies this agent's own log up to that index and persists the result as the compaction
  snapshot.
- An uncommitted entry can therefore be baked into a persisted snapshot.
- That snapshot is what `rebuildDBs` loads when the agent becomes leader. It's also what a
  leader ships to lagging followers inside appendEntries.
- Result: a store could expose a write that was never committed.

**Not yet shown.** No store read has contained such a value yet. The follow-up check "A
transaction whose log slot was committed to a different entry never takes effect" now
catches it if it happens. Its reach claim is "A client's log entry was superseded by a
different committed entry at the same index".

**Next steps.**
1. Write a deterministic reproduction with 3 agents:
   - partition the leader right after it appends
   - let the others elect a new leader and commit a different entry at that index
   - heal the partition, then check the old leader's `/state` commit index against its log
2. Confirm it on 3.12.12, where it was observed. Line references are from devel.
3. Show K before calling it a bug. It could be known, or mitigated elsewhere.

## Open questions

- **Stale reads.** Is the read lease meant to give linearizable reads under clock jitter?
  If P2a' fires without P2a, ask K whether that's a known tradeoff.
- `inquire` can report 0 for a transaction that a later leader still commits, because an
  uncommitted entry on a minority can survive. So "not in the leader's log" is only a reach
  claim, never a safety check.
- The log-entry comparison ignores `timestamp` and `epoch_millis`, since it's unconfirmed
  that they're replicated byte for byte. Only term, request and clientId are compared.
- **Possible SUT-side assertions,** if we build `devel` with the SDK: in
  `storeLogFromSnapshot` (snapshot installed) and in `rebuildDBs` (rebuild from snapshot
  index X to commit Y). They'd replace the external heuristics for S4 and S7 with exact
  markers.

## Iteration log

- 2026-09-29: first version, runs 1–3 above.
- 2026-09-29: run 4 found F1. Added the dead-transaction tracking:
  - `Model.mark_dead`
  - the store check "A transaction whose log slot was committed to a different entry never
    takes effect"
  - the reach claim for an entry superseded at the same index

  Unit-checked. The workload now has 39 assertions, all names unique.
  `snouty validate` passes.
