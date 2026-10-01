"""Client-side model of every transaction the workload sent, and the checks a
store read must pass against it.

Every transaction carries a unique clientId and writes unique values, so any
value seen in the store or in a log entry maps back to exactly one transaction.
Outcomes are tracked as bounds: ACKED (index known), REJECTED (precondition
failed, definitely no effect), or MAYBE (unknown; may or may not be committed).
"""

import threading
import time

PENDING, ACKED, REJECTED, MAYBE = "pending", "acked", "rejected", "maybe"

# Each workload process gets its own key namespace and clientId prefix. The
# container can be stopped and restarted mid-run (node fault); a restarted
# process starts with an empty model, so it must not read back (or find in the
# log) the keys, values and clientIds of the process that ran before it.
INCARNATION = f"{time.time_ns():x}"
WL = f"/wl/{INCARNATION}"
COUNTER = f"{WL}/c"
LIST = f"{WL}/l"


def register_key(i):
    return f"{WL}/r/r{i}"


def group_keys(g):
    return [f"{WL}/g/g{g}/a", f"{WL}/g/g{g}/b", f"{WL}/g/g{g}/c"]


def frozen_key(name):
    return f"{WL}/f/{name}"


def lookup(tree, path):
    """Value at /a/b/c in a nested store dict, or None if absent."""
    node = tree
    for part in path.strip("/").split("/"):
        if not isinstance(node, dict) or part not in node:
            return None
        node = node[part]
    return node


class Txn:
    __slots__ = ("cid", "kind", "writes", "expected", "start", "end", "status", "index",
                 "ack_time", "target_created_before")

    def __init__(self, cid, kind, writes, expected=None):
        self.cid = cid
        self.kind = kind            # set | cas | group | group_cas | incr | push | create | recreate
        self.writes = writes        # {path: value}; value None for incr
        self.expected = expected    # {path: expected old value} for cas kinds
        self.start = time.monotonic()
        self.end = None
        self.status = PENDING
        self.index = 0
        self.ack_time = None        # when we learned it is committed
        self.target_created_before = False  # recreate: target's create was acked before we started


class Model:
    def __init__(self, n_registers, n_groups):
        self.lock = threading.Lock()
        self.n_registers = n_registers
        self.n_groups = n_groups
        self.by_cid = {}
        self.by_value = {}          # unique value -> Txn
        self.by_key = {}            # path -> [Txn]
        self.incrs = []
        self.pushes = []
        self.frozen_acked = {}      # path -> Txn (the create that was acked)
        self.believed = {}          # path -> last value we saw or wrote (for CAS)
        self.dead = set()           # cids whose log slot was committed to a different entry

    # ---- recording ------------------------------------------------------

    def begin(self, txn):
        with self.lock:
            self.by_cid[txn.cid] = txn
            for path, value in txn.writes.items():
                self.by_key.setdefault(path, []).append(txn)
                if value is not None:
                    self.by_value[value] = txn
            if txn.kind == "incr":
                self.incrs.append(txn)
            elif txn.kind == "push":
                self.pushes.append(txn)
            elif txn.kind == "recreate":
                path = next(iter(txn.writes))
                txn.target_created_before = path in self.frozen_acked

    def finish(self, txn, status, index=0):
        with self.lock:
            txn.end = time.monotonic()
            txn.status = status
            if status == ACKED:
                txn.index = index
                txn.ack_time = txn.end
                self._on_committed(txn)

    def resolve_committed(self, cid, index):
        """A MAYBE (or still pending) transaction was found committed at index."""
        with self.lock:
            txn = self.by_cid.get(cid)
            if txn is None or txn.status in (ACKED,):
                return txn
            prior = txn.status
            txn.index = index
            if prior == MAYBE:
                txn.status = ACKED
                txn.ack_time = time.monotonic()
                self._on_committed(txn)
            return txn

    def _on_committed(self, txn):
        for path, value in txn.writes.items():
            if value is not None:
                self.believed[path] = value
        if txn.kind == "create":
            self.frozen_acked[next(iter(txn.writes))] = txn

    def note_seen(self, tree):
        with self.lock:
            for i in range(self.n_registers):
                v = lookup(tree, register_key(i))
                if v is not None:
                    self.believed[register_key(i)] = v
            for g in range(self.n_groups):
                a = group_keys(g)[0]
                v = lookup(tree, a)
                if v is not None:
                    self.believed[a] = v

    def mark_dead(self, cid):
        """cid's log slot was committed to a different entry, so it can never commit."""
        with self.lock:
            txn = self.by_cid.get(cid)
            if txn is None or txn.status in (ACKED, REJECTED) or cid in self.dead:
                return False
            self.dead.add(cid)
            return True

    def maybe_cids(self, older_than):
        with self.lock:
            return [t.cid for t in self.by_cid.values()
                    if t.status == MAYBE and t.end is not None and t.end < older_than]

    def frozen_paths(self):
        with self.lock:
            return list(self.frozen_acked.keys())

    # ---- checks ---------------------------------------------------------

    def check_store(self, tree, read_start, read_end):
        """Check one read of the WL subtree. Returns a dict of
        check name -> list of violation details (empty list = check passed),
        plus counter bounds. `tree` is the object returned for [[WL]]."""
        out = {
            "unknown_value": [], "rejected_value": [], "stale_or_lost": [],
            "group_torn": [], "list_duplicates": [], "list_extra": [], "list_missing": [],
            "dead_value": [],
        }
        with self.lock:
            paths = [register_key(i) for i in range(self.n_registers)]
            for g in range(self.n_groups):
                paths += group_keys(g)
            frozen_tree = lookup(tree, f"{WL}/f") or {}
            frozen_seen = [frozen_key(k) for k in frozen_tree] if isinstance(frozen_tree, dict) else []
            paths += sorted(set(frozen_seen) | set(self.frozen_acked))

            for path in paths:
                v = lookup(tree, path)
                txn = self.by_value.get(v) if v is not None else None
                if v is not None and (txn is None or path not in txn.writes):
                    out["unknown_value"].append({"key": path, "value": str(v)[:80]})
                    continue
                if txn is not None and txn.status == REJECTED:
                    out["rejected_value"].append({"key": path, "value": v, "cid": txn.cid})
                if txn is not None and txn.cid in self.dead:
                    out["dead_value"].append({"key": path, "value": v, "cid": txn.cid})
                # Newest write to this key that was acknowledged before the read began.
                floor = None
                for w in self.by_key.get(path, ()):
                    if w.status == ACKED and w.ack_time is not None and w.ack_time < read_start:
                        if floor is None or w.index > floor.index:
                            floor = w
                if floor is None:
                    continue
                if v is None:
                    out["stale_or_lost"].append(
                        {"key": path, "observed": None, "must_be_at_least_index": floor.index,
                         "acked_cid": floor.cid})
                elif txn.status == ACKED and txn.index < floor.index:
                    out["stale_or_lost"].append(
                        {"key": path, "observed": v, "observed_index": txn.index,
                         "must_be_at_least_index": floor.index, "acked_cid": floor.cid})

            for g in range(self.n_groups):
                vals = [lookup(tree, k) for k in group_keys(g)]
                if len(set(map(str, vals))) != 1:
                    out["group_torn"].append({"group": g, "values": vals})

            counter = lookup(tree, COUNTER) or 0
            lower = sum(1 for t in self.incrs
                        if t.status == ACKED and t.ack_time is not None and t.ack_time < read_start)
            upper = sum(1 for t in self.incrs if t.start < read_end and t.status != REJECTED)

            lst = lookup(tree, LIST) or []
            if not isinstance(lst, list):
                out["list_extra"].append({"list_is_not_array": str(lst)[:80]})
                lst = []
            seen = set()
            for v in lst:
                if v in seen:
                    out["list_duplicates"].append(v)
                seen.add(v)
                t = self.by_value.get(v)
                if t is not None and t.cid in self.dead:
                    out["dead_value"].append({"key": LIST, "value": v, "cid": t.cid})
                if t is None or t.kind != "push" or t.start >= read_end or t.status == REJECTED:
                    out["list_extra"].append(v)
            for t in self.pushes:
                if t.status == ACKED and t.ack_time is not None and t.ack_time < read_start:
                    v = t.writes[LIST]
                    if v not in seen:
                        out["list_missing"].append({"value": v, "index": t.index})

        out["counter"] = {"observed": counter, "lower": lower, "upper": upper}
        return out
