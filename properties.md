# Property catalog: ArangoDB Agency

Scope is set by `k-scope.md`: logical errors in the replicated log (appendEntries) and in
applying it to the state machine. Everything here is about **committed** state only.

Revised 2026-09-29 after the harness came up locally. The details are under "Findings from
setup" in `antithesis/scratchbook/sut-analysis.md`.

## How the Agency actually behaves (read this before designing checks)

- **Only the leader keeps a state machine.** Followers store the replicated log but never
  apply it: `_readDB.applyLogEntries` runs only in the leader's commit path
  (`Agent.cpp:900`). On a follower, `/state` and `/stores` return an empty store.
- **Every new leader rebuilds the store** from its last compaction snapshot plus its log
  (`Agent::rebuildDBs`, `Agent.cpp:1825`, called from `prepareLead`). So the state machine
  is built in two ways:
  1. incremental apply on the leader as entries commit, and
  2. a full rebuild from snapshot plus log at every leader change.

  Bugs in either one are in scope.
- **Compaction is active in the harness** (step 100, keep 50). A follower that falls about
  100 or more entries behind catches up from a snapshot sent inside appendEntries
  (`Agent.cpp:604-706`). The follower installs it at `State.cpp:398-441`. Neither sending
  nor installing logs a line at INFO level.

## Observation points

Agents: `agency-agent1..3`, port 8531. Every request needs `Authorization: bearer <JWT>`.

| Endpoint (per agent) | Returns | Notes |
|---|---|---|
| `GET /_api/agency/state` | `index` (commit index), `term`, `agency` (store), `log` (entries still in memory) | Store is `{}` on followers. Each log entry's index is **`_key`** (zero-padded string). Entries also have `term`, `request`, `clientId`, `timestamp`, `epoch_millis`, and a local `_rev` (not replicated, ignore it). |
| `GET /_api/agency/stores` | `read_db`, `spearhead` (uncommitted), `transient` | Leader only in practice. **Never assert on `spearhead`.** |
| `GET /_api/agency/config` | `leaderId`, `term`, `commitIndex`, configuration and pool | Use it to find the leader and which agent is which. |
| `POST /_api/agency/read` | values for the requested keys | Goes to the leader. Returns 307 on a follower. |

- The store contains an internal `.agency` subtree (pool, `timeoutMult`). Leave it out of
  comparisons unless a check is specifically about it.
- `/state` doesn't include the compaction snapshot. There's no known way to read the
  snapshot from outside an agent. The checks below are designed so they don't need it.

## Workload expectations

- JWT auth on every request (see `antithesis/workload/agency_client.py`).
- Writes go to `/_api/agency/write` or `/transact` as `[[operations, preconditions?], ...]`.
- **Follow 307 redirects by hand.** HTTP clients drop the `Authorization` header when a
  redirect changes host.
- **408, 503, a timeout, or a dropped connection all mean "maybe committed"**, not
  failure. The client model has to accept either outcome for those writes.
- Include conditional writes (preconditions `oldEmpty`, `old`, `isArray`) and multi-key
  transactions so the apply path is exercised.
- Use a small key space so writes land on the same keys and preconditions actually fail.
- Give every write a unique value (for example, writer id plus sequence number). That lets
  a value seen in the store or the log be traced back to exactly one client request.
- Write fast enough to cross the compaction step (100 entries) many times in a run.

## Safety properties (`Always`)

### Replicated log / appendEntries

- **P1. Log matching.** For any two agents, if both hold an entry at the same `_key` with
  the same `term`, the entries are identical (`term`, `request`, `clientId`, `timestamp`)
  and so is every earlier index both of them hold in memory. It's checked on each agent's
  in-memory window from `/state`.
- **P2. Committed entries are durable.**
  - (a) A write acknowledged with 200 is never lost. Every later read through the leader,
    including after leader changes, shows its effect, unless a later committed write
    overwrote it.
  - (b) For entries at or below any agent's commit index, `request` and `term` at a given
    index never change across observations on any agent. Compaction may remove entries
    from memory but must never rewrite them.
- **P5. Snapshot catch-up is correct.** After a follower catches up from a snapshot, its
  log from the snapshot index up satisfies P1 with the leader's. If that follower later
  becomes leader, its rebuilt store satisfies P3 and P4. Followers have no store, so the
  snapshot's contents only become visible once the agent leads.

### Applying to the state machine

- **P3. The leader's store matches the log.** For every key whose most recent write is
  inside the leader's in-memory log window, the key's value in the store equals the value
  that entry wrote. This applies to writes that set a value directly. For other operations
  (increment, push and so on), compute the expected value by applying the entries in the
  window. Checked on the **leader only**, with writes paused briefly so the snapshot is
  consistent.
- **P4. The store stays the same across leader changes (rebuild correctness).**
  - With writes paused, the store read from leader A at commit index `c`, minus `.agency`,
    must equal the store read from the next leader B once B's commit index reaches `c`,
    for every key written at or below `c`.
  - In steady state, the leader's store must match the client model: the acknowledged
    writes, plus any subset of the maybe-committed ones, applied in log order.
- **P6. Conditional transactions are atomic.**
  - A transaction whose precondition failed has no effect.
  - A transaction whose precondition held is applied in full.
  - Multi-key writes never show up partially applied in the store, on either kind of store
    (incremental or rebuilt).

### Secondary (a Raft guarantee K named, but not his focus)

- **P7. At most one leader per term.** Across all `/config` observations, no term maps to
  two different `leaderId`s.

## Sometimes assertions (these prove the test reached the interesting states)

- **S1.** A leader change happened, and a write committed under the new leader.
- **S2.** An agent was killed or restarted and rejoined with its log intact.
- **S3.** Compaction happened: some agent's first in-memory log index moved past 1.
- **S4.** **A follower caught up from a snapshot.** Its first in-memory log index jumped
  past the last index it held before. This is the key one.
- **S5.** A conditional write was rejected because its precondition failed.
- **S6.** A write returned 408 or 503 or timed out, and was later found to be committed.
  Separately, one that wasn't.
- **S7.** **A leader change happened after at least one compaction**, so the new leader
  rebuilt its store from a snapshot plus log. This exercises the rebuild path that P4
  checks.
- **S8.** P4's paused comparison ran across a leader change at least once. Without it, P4
  is never actually checked.

## Open items

- Is it intended that followers keep no store? It's very likely by design; confirm with K.
- P3 only covers keys whose latest write is inside the in-memory window. Keys last written
  before the compaction point rely on P4's model check instead.
- The devel line references may be slightly off for 3.12.12.
