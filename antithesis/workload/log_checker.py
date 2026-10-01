"""Polls every agent's own view (/config and /state) and checks the replicated
log and the leader's state machine against each other and across agents.

Properties: P1 log matching, P2(b) committed entries never change, P3 leader
store = last write in its log window, P6 applied CAS saw its expected value,
P7 one leader per term. Reach claims: compaction, snapshot catch-up, leader
rebuilt from a snapshot.
"""

import json
import threading
import time

from antithesis.assertions import always, reachable, sometimes, unreachable

from agency_client import AGENTS, AgencyClient
from model import ACKED, MAYBE, PENDING, REJECTED, WL, lookup

LEDGER_LIMIT = 200_000


def canon(obj):
    return json.dumps(obj, sort_keys=True, separators=(",", ":"))


def entry_writes(request):
    """Map path -> ("value", v) | ("opaque", None) for one log entry's request."""
    out = {}
    if not isinstance(request, dict):
        return out
    for path, op in request.items():
        if isinstance(op, dict) and "op" in op:
            if op["op"] == "set" and "new" in op:
                out[path] = ("value", op["new"])
            elif op["op"] == "delete":
                out[path] = ("value", None)
            else:
                out[path] = ("opaque", None)
        else:
            out[path] = ("value", op)
    return out


def violation(name, details):
    print(f"VIOLATION: {name}: {json.dumps(details, default=str)[:2000]}", flush=True)


class LogChecker(threading.Thread):
    def __init__(self, model, stats, interval):
        super().__init__(daemon=True, name="log-checker")
        self.model = model
        self.stats = stats
        self.interval = interval
        self.client = AgencyClient(timeout=5.0)
        self.committed = {}      # index -> (term, canon(request), clientId)   [committed, per a leader]
        self.claims = {}         # index -> [(host, entry)] follower commit claims not yet in the ledger
        self.by_index_term = {}  # (index, term) -> canon(request)             [any entry seen]
        self.cid_index = {}      # clientId -> committed index
        self.leader_of_term = {}  # term -> agent id that claimed leadership
        self.last_view = {}      # agent host -> {"first", "last", "terms": {idx: term}}
        self.rebuilt_terms = set()  # terms whose leader started with a compacted log
        self.max_term_seen = 0
        self.lock = threading.Lock()
        self.agent_ids = {}      # host -> agent id
        self.unreachable = set()
        self.processed_cids = set()
        self.needs_snapshot = {}  # follower host -> leader first index it fell behind

    # ---- public helpers used by the workload ------------------------------

    def term_for_host(self, host):
        with self.lock:
            aid = self.agent_ids.get(host)
            terms = [t for t, a in self.leader_of_term.items() if a == aid]
            return max(terms) if terms else None

    def leader_rebuilt_from_snapshot(self, host):
        term = self.term_for_host(host)
        with self.lock:
            return term is not None and term in self.rebuilt_terms

    # ---- polling -----------------------------------------------------------

    def run(self):
        while True:
            for host in AGENTS:
                try:
                    self.poll(host)
                except Exception as exc:  # agent down, partitioned, restarting
                    if host not in self.unreachable:
                        print(f"log-checker: {host} unreachable: {exc!r}", flush=True)
                    self.unreachable.add(host)
            self.check_prefixes()
            time.sleep(self.interval)

    def poll(self, host):
        poll_start = time.monotonic()
        cfg_before = self.client.config(host)
        st = self.client.state(host)
        cfg_after = self.client.config(host)

        if host in self.unreachable:
            self.unreachable.discard(host)
            reachable("agent answered again after being unreachable", {"agent": host})

        self_id = cfg_before["configuration"]["id"]
        with self.lock:
            self.agent_ids[host] = self_id
        commit = st["index"]
        log = st.get("log") or []
        entries = {}
        for e in log:
            idx = int(e["_key"])
            entries[idx] = (e["term"], canon(e.get("request")), e.get("clientId", ""))

        is_leader_throughout = (
            cfg_before["leaderId"] == self_id and cfg_after["leaderId"] == self_id
            and cfg_before["term"] == cfg_after["term"] == st["term"])
        self.check_leadership(host, self_id, cfg_before, cfg_after, entries)
        self.check_entries(host, commit, entries, is_leader_throughout)
        self.track_reach(host, entries, poll_start, is_leader_throughout)
        if is_leader_throughout:
            self.check_leader_store(host, commit, st.get("agency") or {}, log)

    # ---- P7 ----------------------------------------------------------------

    def check_leadership(self, host, self_id, cfg_before, cfg_after, entries):
        for cfg in (cfg_before, cfg_after):
            if cfg["leaderId"] != self_id:
                continue
            term = cfg["term"]
            with self.lock:
                prior = self.leader_of_term.setdefault(term, self_id)
                new_leader = prior == self_id and term > self.max_term_seen
                self.max_term_seen = max(self.max_term_seen, term)
            ok = prior == self_id
            always(ok, "At most one agent claims leadership in any term",
                   {"term": term, "first_claimant": prior, "second_claimant": self_id})
            if not ok:
                violation("At most one agent claims leadership in any term",
                          {"term": term, "a": prior, "b": self_id})
            if new_leader and entries:
                first = min(entries)
                if first > 1:
                    with self.lock:
                        self.rebuilt_terms.add(term)
                    reachable("A new leader took over with a compacted log and rebuilt its store from a snapshot",
                              {"term": term, "leader": host, "first_log_index": first})

    # ---- P1, P2(b), P6, clientId bookkeeping --------------------------------

    def check_entries(self, host, commit, entries, is_leader):
        """Only a leader's view (leader for the whole poll) defines the committed
        ledger and resolves client transactions. A follower's commit index is a
        claim: it is checked against the ledger (now, or once a leader reports
        that index) but never trusted, since a follower can count stale entries
        as committed."""
        for idx, (term, req, cid) in entries.items():
            key = (idx, term)
            with self.lock:
                prior = self.by_index_term.setdefault(key, req)
            ok = prior == req
            always(ok, "Log entries with the same index and term are identical on every agent",
                   {"agent": host, "index": idx, "term": term, "seen_before": prior[:500], "now": req[:500]})
            if not ok:
                violation("Log entries with the same index and term are identical on every agent",
                          {"agent": host, "index": idx, "term": term})

            if cid and cid in self.model.by_cid:
                with self.lock:
                    winner = self.committed.get(idx)
                if winner is not None and winner[2] != cid and self.model.mark_dead(cid):
                    reachable("A client's log entry was superseded by a different committed entry at the same index",
                              {"agent": host, "index": idx, "term": term, "clientId": cid,
                               "committed_term": winner[0], "committed_clientId": winner[2]})
            if idx > commit:
                continue
            entry = (term, req, cid)
            with self.lock:
                if is_leader:
                    prior_c = self.committed.setdefault(idx, entry)
                    pending = self.claims.pop(idx, [])
                else:
                    prior_c = self.committed.get(idx)
                    pending = []
                    if prior_c is None:
                        self.claims.setdefault(idx, []).append((host, entry))
            if prior_c is not None:
                self.check_committed(host, idx, prior_c, entry)
            for claim_host, claim in pending:
                self.check_committed(claim_host, idx, prior_c, claim)
            if is_leader and cid and prior_c == entry:
                self.on_committed_cid(idx, req, cid)

        with self.lock:
            if len(self.committed) > LEDGER_LIMIT:
                for idx in sorted(self.committed)[: len(self.committed) - LEDGER_LIMIT]:
                    del self.committed[idx]
            if len(self.claims) > LEDGER_LIMIT:
                for idx in sorted(self.claims)[: len(self.claims) - LEDGER_LIMIT]:
                    del self.claims[idx]
            if len(self.by_index_term) > LEDGER_LIMIT:
                for k in sorted(self.by_index_term)[: len(self.by_index_term) - LEDGER_LIMIT]:
                    del self.by_index_term[k]

    def check_committed(self, host, idx, committed_as, now):
        """P2(b): an agent's committed entry at idx matches the ledger."""
        term, req, cid = now
        ok = committed_as == now
        always(ok, "A committed log entry never changes on any agent",
               {"agent": host, "index": idx, "committed_as": committed_as, "now": (term, req[:500], cid)})
        if not ok:
            violation("A committed log entry never changes on any agent",
                      {"agent": host, "index": idx, "was": committed_as, "now": now})

    def on_committed_cid(self, idx, req, cid):
        with self.lock:
            prior = self.cid_index.setdefault(cid, idx)
        ok = prior == idx
        always(ok, "Each client transaction is committed at most once",
               {"clientId": cid, "first_index": prior, "second_index": idx})
        if not ok:
            violation("Each client transaction is committed at most once",
                      {"clientId": cid, "first_index": prior, "second_index": idx})

        txn = self.model.by_cid.get(cid)
        if txn is None or cid in self.processed_cids or txn.status == PENDING:
            return  # bootstrap/other client, already checked, or response not in yet
        self.processed_cids.add(cid)
        if txn.status == REJECTED:
            unreachable("A transaction rejected by its precondition never appears in the committed log",
                        {"clientId": cid, "index": idx})
            violation("A transaction rejected by its precondition never appears in the committed log",
                      {"clientId": cid, "index": idx})
            return
        if txn.status == ACKED and txn.index:
            ok = txn.index == idx
            always(ok, "The log index returned to the client holds that client's transaction",
                   {"clientId": cid, "acked_index": txn.index, "found_at": idx})
            if not ok:
                violation("The log index returned to the client holds that client's transaction",
                          {"clientId": cid, "acked_index": txn.index, "found_at": idx})
        else:
            was_maybe = txn.status == MAYBE
            self.model.resolve_committed(cid, idx)
            if was_maybe:
                reachable("A write with an unknown outcome was later found committed in the log",
                          {"clientId": cid, "index": idx})
        # The committed entry carries exactly the operations the client sent.
        sent = {}
        for path, value in txn.writes.items():
            if txn.kind == "incr":
                sent[path] = {"op": "increment", "step": 1}
            elif txn.kind == "push":
                sent[path] = {"op": "push", "new": value}
            else:
                sent[path] = value
        got = {p: (v["new"] if isinstance(v, dict) and v.get("op") == "set" else v)
               for p, v in json.loads(req).items()}
        ok = got == sent
        always(ok, "A committed log entry carries exactly the operations the client sent",
               {"clientId": cid, "index": idx, "sent": sent, "logged": got})
        if not ok:
            violation("A committed log entry carries exactly the operations the client sent",
                      {"clientId": cid, "sent": sent, "logged": got})

        if txn.expected:
            self.check_cas(idx, txn)

    def check_cas(self, idx, txn):
        """P6: an applied CAS saw its expected old value. Walk back through the
        committed ledger to the previous write of the guarded key; give up on a gap."""
        for path, expected in txn.expected.items():
            j = idx - 1
            with self.lock:
                while j >= 1:
                    entry = self.committed.get(j)
                    if entry is None:
                        return  # gap: can't determine
                    writes = entry_writes(json.loads(entry[1]))
                    if path in writes:
                        kind, value = writes[path]
                        if kind != "value":
                            return
                        break
                    j -= 1
                else:
                    return
            ok = value == expected
            always(ok, "A conditional write was only applied when its precondition held",
                   {"clientId": txn.cid, "index": idx, "key": path, "expected": expected,
                    "previous_value": value, "previous_index": j})
            if not ok:
                violation("A conditional write was only applied when its precondition held",
                          {"clientId": txn.cid, "index": idx, "key": path,
                           "expected": expected, "previous": value, "previous_index": j})

    # ---- P1 prefix ------------------------------------------------------------

    def check_prefixes(self):
        with self.lock:
            views = list(self.last_view.items())
        for i in range(len(views)):
            for k in range(i + 1, len(views)):
                (ha, a), (hb, b) = views[i], views[k]
                common = sorted(set(a["terms"]) & set(b["terms"]))
                match = [x for x in common if a["terms"][x] == b["terms"][x]]
                if not match:
                    continue
                top = match[-1]
                bad = [x for x in common if x < top and a["terms"][x] != b["terms"][x]]
                ok = not bad
                always(ok, "If two agents share a log entry, they agree on every earlier entry they both hold",
                       {"agents": [ha, hb], "shared_index": top, "diverging_indices": bad[:20]})
                if not ok:
                    violation("If two agents share a log entry, they agree on every earlier entry they both hold",
                              {"agents": [ha, hb], "shared_index": top, "diverging": bad[:20]})

    # ---- S3, S4 ---------------------------------------------------------------

    def track_reach(self, host, entries, poll_start, is_leader):
        if not entries:
            return
        first, last = min(entries), max(entries)
        sometimes(first > 1, "Compaction removed old entries from an agent's in-memory log",
                  {"agent": host, "first_log_index": first})
        now = time.monotonic()
        with self.lock:
            self.last_view[host] = {"first": first, "last": last, "time": now, "leader": is_leader,
                                    "terms": {i: t for i, (t, _, _) in entries.items()}}
            # A leader view taken before this poll started: its first index only
            # grows, so follower.last < leader.first means that, at this poll, the
            # follower needs entries the leader no longer has.
            leader_views = [v for h, v in self.last_view.items()
                            if h != host and v["leader"] and v["time"] < poll_start]
            behind_of = max((v["first"] for v in leader_views), default=None)
            pending = self.needs_snapshot.get(host)
        if is_leader:
            return
        if behind_of is not None and last < behind_of and pending is None:
            with self.lock:
                self.needs_snapshot[host] = behind_of
            reachable("A follower fell behind the start of the leader's log, so only a snapshot can catch it up",
                      {"agent": host, "follower_last_index": last, "leader_first_index": behind_of})
        elif pending is not None and last >= pending:
            with self.lock:
                self.needs_snapshot.pop(host, None)
            reachable("A lagging follower caught up from a snapshot sent in appendEntries",
                      {"agent": host, "leader_first_index_when_behind": pending, "now_last_index": last})
            self.stats["snapshot_catchups"] += 1

    # ---- P3 -------------------------------------------------------------------

    def check_leader_store(self, host, commit, store, log):
        last = {}   # path -> ("value", v) | ("opaque", None) from the newest entry <= commit
        for e in sorted(log, key=lambda e: int(e["_key"])):
            if int(e["_key"]) > commit:
                break
            for path, w in entry_writes(e.get("request")).items():
                if path.startswith(WL + "/"):
                    last[path] = w
        mismatches = []
        for path, (kind, value) in last.items():
            if kind != "value":
                continue
            got = lookup(store, path)
            if got != value:
                mismatches.append({"key": path, "log_says": value, "store_has": got})
        ok = not mismatches
        always(ok, "The leader's store holds the value of the newest committed log entry for each key",
               {"agent": host, "commit_index": commit, "mismatches": mismatches[:20]})
        if not ok:
            violation("The leader's store holds the value of the newest committed log entry for each key",
                      {"agent": host, "commit": commit, "mismatches": mismatches[:20]})
        self.stats["leader_store_checks"] += 1
