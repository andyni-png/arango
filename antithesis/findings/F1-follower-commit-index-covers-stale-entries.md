# F1 (parked): a follower's commit index can cover stale, never-committed entries

**Status: PARKED (2026-09-29).** This was found in a local chaos run
(`local-chaos.sh`) while bringing the workload up, **not by Antithesis**. Don't use it
in the pitch to K. We want findings that come from Antithesis runs. Come back to it only
if needed, for example if an Antithesis run hits the same property.

- **SUT:** `arangodb/enterprise:3.12.12`, 3 agents, compaction step 100 and keep 50.
- **Source read:** `devel` @ `0ced0d52ca82`. Line numbers are approximate for 3.12.
- **Property that fired:** "A committed log entry never changes on any agent" (P2b), in
  `antithesis/workload/log_checker.py`.

## Evidence (workload log, run 4)

```
VIOLATION: A committed log entry never changes on any agent:
  {"agent": "agency-agent2", "index": 101,
   "was": [2, "{\".agency\":{\"new\":{...\"term\":2...},\"op\":\"set\"}}", ""],
   "now": [1, "{\"/wl/l\":{\"new\":\"v0.99xxxx...\",\"op\":\"push\"}}", "w0-99"]}
```

- The committed ledger held index 101 as term 2: the new leader's `.agency` config entry.
- Agent2's own `GET /_api/agency/state` later returned index 101 as a term-1 entry, a
  list push from client `w0-99`, with its commit index (`index`) at 101 or higher.
- `/state` reads `_commitIndex` and then the same agent's log within one request
  (`Agent::readDB`, `Agent.cpp:1909`). A log entry can only go from stale to correct. So
  at one instant agent2 both held the stale entry and counted it as committed.
- **Run configuration:**
  - 1 writer, weighted toward increments and pushes, 5 s timeout
  - chaos: stop, restart, pause, and a partition of agent2, over about 260 s
- The raw SDK output wasn't kept (it was on tmpfs). The line above is from the workload's
  stdout.

## Mechanism (code)

`Agent::recvAppendEntriesRPC` sets the follower's commit index from the follower's own
last log index in both branches:
- **Empty appendEntries** (`Agent.cpp:~527`):
  `max(ci, min(leaderCommitIndex, _state.lastIndex()))`
- **Non-empty appendEntries** (`Agent.cpp:~557`): `lastIndex = _state.logFollower(payload)`,
  which returns `_log.back().index`, then `min(leaderCommitIndex, lastIndex)`.

`State::removeConflicts` only truncates where the incoming batch overlaps. Stale entries
past the batch survive, and the commit index then covers them. Raft (Figure 2) specifies
`min(leaderCommit, index of last new entry)` to prevent exactly this.

## Possible impact (not demonstrated)

- Followers run compaction. `Agent::compact` uses `_commitIndex`, and `State::compact`
  applies this agent's own log up to that index and persists the result as the compaction
  snapshot.
- An uncommitted entry could therefore be baked into a persisted snapshot.
- That snapshot is what `rebuildDBs` loads when the agent becomes leader, and what a leader
  ships to lagging followers inside appendEntries.
- The end result would be a store exposing a write that was never committed.
- No client-visible read has shown this yet. The workload now has
  "A transaction whose log slot was committed to a different entry never takes effect"
  to catch it.

## If we pick it back up

1. **Deterministic repro:**
   - partition the leader right after it appends an entry
   - let the other two elect a new leader and commit a different entry at that index
   - heal the partition
   - compare the old leader's `/state` commit index with its log at that index
2. Try to push it through to client-visible impact: follower compaction, then leadership,
   then a read.
3. Confirm the line references against the 3.12 branch, then raise it with K.
