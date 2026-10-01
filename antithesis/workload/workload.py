"""Self-driving workload for the Agency's replicated write path.

Writers send batches of transactions (plain sets, CAS, multi-key group writes,
increments, pushes, create-once keys and deliberate re-creates) to random
agents. A reader checks every read of /wl against the client model. A log
checker compares every agent's log and the leader's store. Periodic quiesced
checkpoints pause writers (and faults, in Antithesis) and check durability.

All randomness goes through antithesis.random (it falls back to the stdlib
outside Antithesis). The shape of the run is drawn once at start (swarm).
"""

import json
import os
import subprocess
import threading
import time

from antithesis.assertions import (always, always_greater_than_or_equal_to,
                                   always_less_than_or_equal_to, reachable, sometimes,
                                   unreachable)
from antithesis.random import get_random, random_choice

from agency_client import AGENTS, AgencyClient, Ambiguous
from log_checker import LogChecker, violation
from model import (ACKED, COUNTER, INCARNATION, LIST, MAYBE, REJECTED, WL, Model, Txn,
                   frozen_key, group_keys, register_key)

# Configured limits on the feature's code paths (see analysis.md, value menu):
COMPACTION_STEP = int(os.environ.get("AGENCY_COMPACTION_STEP_SIZE", "100"))
COMPACTION_KEEP = int(os.environ.get("AGENCY_COMPACTION_KEEP_SIZE", "50"))
MAX_APPEND_SIZE = 250  # --agency.max-append-size default: write chunking and appendEntries batch size
LIST_CAP = 3000
FROZEN_CAP = 3000


def chance(p):
    return get_random() < int(p * 2**64)


def rand_below(n):
    return get_random() % n


def log(msg):
    print(msg, flush=True)


def draw_config():
    kinds = ["set", "cas", "group", "group_cas", "incr", "push", "create", "recreate"]
    weights = {k: random_choice([0, 1, 5, 20]) for k in kinds}
    # Action omission: drop each class with small probability; re-roll if empty.
    for k in kinds:
        if chance(0.1):
            weights[k] = 0
    if sum(weights.values()) == 0:
        weights[random_choice(kinds)] = 10
    batch_menu = [1, 2, 3, COMPACTION_KEEP - 1, COMPACTION_KEEP, COMPACTION_KEEP + 1,
                  COMPACTION_STEP - 1, COMPACTION_STEP, COMPACTION_STEP + 1,
                  MAX_APPEND_SIZE - 1, MAX_APPEND_SIZE, MAX_APPEND_SIZE + 1]
    return {
        "writers": random_choice([1, 2, 4, 8]),
        "weights": weights,
        "p_big_batch": random_choice([0.0, 0.01, 0.1]),
        "batch_menu": batch_menu,
        "p_multi_trx": random_choice([0.0, 0.3, 0.9]),
        "registers": random_choice([1, 2, 8]),
        "groups": random_choice([1, 3]),
        "p_stale_cas": random_choice([0.02, 0.3, 0.9]),
        "p_op_form_set": random_choice([0.0, 0.5, 1.0]),
        "value_pad": random_choice([0, 0, 64, 4096]),
        "writer_sleep": random_choice([0.0, 0.001, 0.01, 0.1]),
        "timeout": random_choice([0.5, 5.0, 5.0, 30.0]),
        "read_interval": random_choice([0.05, 0.3, 1.0]),
        "log_poll_interval": random_choice([0.2, 1.0]),
        "checkpoint_interval": random_choice([10, 30, 90]),
    }


class Workload:
    def __init__(self, cfg):
        self.cfg = cfg
        self.model = Model(cfg["registers"], cfg["groups"])
        self.stats = {"acked": 0, "rejected": 0, "maybe": 0, "reads_checked": 0,
                      "checkpoints": 0, "snapshot_catchups": 0, "leader_store_checks": 0}
        self.paused = threading.Event()
        self.active = 0
        self.active_cv = threading.Condition()
        self.log_checker = LogChecker(self.model, self.stats, cfg["log_poll_interval"])
        kinds = [k for k, w in cfg["weights"].items() for _ in range(w)]
        self.kind_bag = kinds
        self.leader_changes_seen = 0

    # ---- transactions -----------------------------------------------------

    def value(self, wid, seq):
        return f"v{wid}.{seq}" + ("x" * self.cfg["value_pad"])

    def make_txn(self, wid, seq):
        kind = random_choice(self.kind_bag)
        cid = f"{INCARNATION}-w{wid}-{seq}"
        v = self.value(wid, seq)
        m = self.model
        if kind in ("cas", "group_cas"):
            path = (register_key(rand_below(m.n_registers)) if kind == "cas"
                    else group_keys(rand_below(m.n_groups))[0])
            with m.lock:
                believed = m.believed.get(path)
                history = [t for t in m.by_key.get(path, ()) if t.status == ACKED]
            if believed is None:
                kind = "set" if kind == "cas" else "group"
            else:
                expected = believed
                if chance(self.cfg["p_stale_cas"]) and len(history) > 1:
                    expected = history[rand_below(len(history))].writes[path]
                if kind == "cas":
                    return Txn(cid, "cas", {path: v}, {path: expected})
                g = path.split("/")[-2]
                paths = [f"{WL}/g/{g}/{x}" for x in "abc"]
                return Txn(cid, "group_cas", {p: v for p in paths}, {path: expected})
        if kind == "push" and len(m.pushes) >= LIST_CAP:
            kind = "set"
        if kind in ("create", "recreate") and len(m.frozen_acked) >= FROZEN_CAP:
            kind = "set"
        if kind == "recreate":
            frozen = m.frozen_paths()
            if frozen:
                return Txn(cid, "recreate", {frozen[rand_below(len(frozen))]: v})
            kind = "create"
        if kind == "set":
            return Txn(cid, "set", {register_key(rand_below(m.n_registers)): v})
        if kind == "group":
            return Txn(cid, "group", {p: v for p in group_keys(rand_below(m.n_groups))})
        if kind == "incr":
            return Txn(cid, "incr", {COUNTER: None})
        if kind == "push":
            return Txn(cid, "push", {LIST: v})
        return Txn(cid, "create", {frozen_key(f"{wid}-{seq}"): v})

    def encode(self, txn):
        ops = {}
        for path, value in txn.writes.items():
            if txn.kind == "incr":
                ops[path] = {"op": "increment", "step": 1}
            elif txn.kind == "push":
                ops[path] = {"op": "push", "new": value}
            elif txn.kind in ("set", "group", "cas", "group_cas") and chance(self.cfg["p_op_form_set"]):
                ops[path] = {"op": "set", "new": value}
            else:
                ops[path] = value
        pre = {}
        if txn.expected:
            pre = {p: {"old": e} for p, e in txn.expected.items()}
        if txn.kind in ("create", "recreate"):
            pre = {next(iter(txn.writes)): {"oldEmpty": True}}
        return [ops, pre, txn.cid]

    # ---- writers ----------------------------------------------------------

    def writer(self, wid):
        client = AgencyClient(timeout=self.cfg["timeout"])
        seq = 0
        while True:
            if self.paused.is_set():
                time.sleep(0.05)
                continue
            with self.active_cv:
                if self.paused.is_set():
                    continue
                self.active += 1
            try:
                if chance(self.cfg["p_big_batch"]):
                    n = random_choice(self.cfg["batch_menu"])
                elif chance(self.cfg["p_multi_trx"]):
                    n = random_choice([2, 3, 5])
                else:
                    n = 1
                txns = []
                for _ in range(n):
                    seq += 1
                    txns.append(self.make_txn(wid, seq))
                for t in txns:
                    self.model.begin(t)
                self.send(client, txns)
            finally:
                with self.active_cv:
                    self.active -= 1
                    self.active_cv.notify_all()
            if self.cfg["writer_sleep"]:
                time.sleep(self.cfg["writer_sleep"] * (get_random() % 1000) / 500.0)

    def send(self, client, txns):
        body = [self.encode(t) for t in txns]
        try:
            results, host = client.write_raw(body, agent=random_choice(AGENTS))
        except Ambiguous as exc:
            for t in txns:
                self.model.finish(t, MAYBE)
            self.stats["maybe"] += len(txns)
            err = str(exc)
            sometimes("408" in err or "503" in err or "Timeout" in err,
                      "A write ended with an unknown outcome (408, 503 or timeout)",
                      {"error": str(exc)[:200]})
            return
        for t, idx in zip(txns, results):
            if idx and idx > 0:
                self.model.finish(t, ACKED, idx)
                self.stats["acked"] += 1
                if t.kind == "recreate" and t.target_created_before:
                    unreachable("A create-once key is never overwritten when oldEmpty is required",
                                {"clientId": t.cid, "key": next(iter(t.writes)), "index": idx})
                    violation("A create-once key is never overwritten when oldEmpty is required",
                              {"clientId": t.cid, "index": idx})
            else:
                self.model.finish(t, REJECTED)
                self.stats["rejected"] += 1
                if t.kind in ("cas", "group_cas", "recreate"):
                    reachable("A conditional write was rejected because its precondition failed",
                              {"kind": t.kind, "clientId": t.cid})
        if self.log_checker.max_term_seen > 1 and any(i for i in results):
            reachable("A write was committed after a leader change",
                      {"term": self.log_checker.max_term_seen, "host": host})

    # ---- reader / live checks ---------------------------------------------

    def reader(self):
        client = AgencyClient(timeout=self.cfg["timeout"])
        while True:
            time.sleep(self.cfg["read_interval"])
            self.read_and_check(client, quiesced=False)

    def read_and_check(self, client, quiesced):
        start = time.monotonic()
        try:
            result, host = client.read_raw([[WL]], agent=random_choice(AGENTS))
        except Ambiguous:
            return False
        end = time.monotonic()
        tree = result[0] if result else {}
        res = self.model.check_store(tree, start, end)
        self.model.note_seen(tree)
        self.stats["reads_checked"] += 1
        self.assert_store_invariants(res, host)
        if quiesced:
            self.assert_quiesced_durability(res, host)
        else:
            self.assert_live_recency(res, host)
        if self.log_checker.leader_rebuilt_from_snapshot(host):
            reachable("The client model was checked against a leader that rebuilt its store from a snapshot",
                      {"host": host, "quiesced": quiesced})
        return True

    def assert_store_invariants(self, res, host):
        c = res["counter"]
        details = {"host": host}
        ok = not res["unknown_value"]
        always(ok, "The store only holds values the workload wrote to that key",
               {**details, "violations": res["unknown_value"][:10]})
        if not ok:
            violation("The store only holds values the workload wrote to that key", res["unknown_value"][:10])
        ok = not res["rejected_value"]
        always(ok, "A write rejected by its precondition has no effect on the store",
               {**details, "violations": res["rejected_value"][:10]})
        if not ok:
            violation("A write rejected by its precondition has no effect on the store", res["rejected_value"][:10])
        ok = not res["dead_value"]
        always(ok, "A transaction whose log slot was committed to a different entry never takes effect",
               {**details, "violations": res["dead_value"][:10]})
        if not ok:
            violation("A transaction whose log slot was committed to a different entry never takes effect",
                      res["dead_value"][:10])
        ok = not res["group_torn"]
        always(ok, "A multi-key transaction is never seen partially applied",
               {**details, "violations": res["group_torn"][:10]})
        if not ok:
            violation("A multi-key transaction is never seen partially applied", res["group_torn"][:10])
        ok = not res["list_duplicates"]
        always(ok, "A pushed value appears in the list at most once",
               {**details, "duplicates": res["list_duplicates"][:10]})
        if not ok:
            violation("A pushed value appears in the list at most once", res["list_duplicates"][:10])
        ok = not res["list_extra"]
        always(ok, "The list only holds values the workload pushed",
               {**details, "extra": res["list_extra"][:10]})
        if not ok:
            violation("The list only holds values the workload pushed", res["list_extra"][:10])
        always_less_than_or_equal_to(c["observed"], c["upper"],
                                     "The counter never exceeds the number of increments attempted", details)
        if c["observed"] > c["upper"]:
            violation("The counter never exceeds the number of increments attempted", c)

    def assert_live_recency(self, res, host):
        c = res["counter"]
        ok = not res["stale_or_lost"]
        always(ok, "A read reflects every write acknowledged before the read began",
               {"host": host, "violations": res["stale_or_lost"][:10]})
        if not ok:
            violation("A read reflects every write acknowledged before the read began", res["stale_or_lost"][:10])
        ok = not res["list_missing"]
        always(ok, "A read of the list includes every push acknowledged before the read began",
               {"host": host, "missing": res["list_missing"][:10]})
        if not ok:
            violation("A read of the list includes every push acknowledged before the read began",
                      res["list_missing"][:10])
        always_greater_than_or_equal_to(c["observed"], c["lower"],
                                        "A read of the counter includes every increment acknowledged before the read began",
                                        {"host": host})
        if c["observed"] < c["lower"]:
            violation("A read of the counter includes every increment acknowledged before the read began", c)

    def assert_quiesced_durability(self, res, host):
        c = res["counter"]
        ok = not res["stale_or_lost"]
        always(ok, "After quiescing, every acknowledged write is still in the store",
               {"host": host, "violations": res["stale_or_lost"][:10]})
        if not ok:
            violation("After quiescing, every acknowledged write is still in the store", res["stale_or_lost"][:10])
        ok = not res["list_missing"]
        always(ok, "After quiescing, every acknowledged push is still in the list",
               {"host": host, "missing": res["list_missing"][:10]})
        if not ok:
            violation("After quiescing, every acknowledged push is still in the list", res["list_missing"][:10])
        always_greater_than_or_equal_to(c["observed"], c["lower"],
                                        "After quiescing, the counter includes every acknowledged increment",
                                        {"host": host})
        if c["observed"] < c["lower"]:
            violation("After quiescing, the counter includes every acknowledged increment", c)

    # ---- checkpoints --------------------------------------------------------

    def stop_faults(self, seconds):
        tool = os.environ.get("ANTITHESIS_STOP_FAULTS")
        if tool:
            try:
                subprocess.run([tool, str(seconds)], timeout=10, check=False)
            except Exception as exc:
                log(f"stop-faults failed: {exc!r}")

    def wait_stable(self, client, deadline):
        """All agents answer, agree on the leader, and share the leader's commit index."""
        while time.monotonic() < deadline:
            try:
                cfgs = [client.config(a) for a in AGENTS]
                leaders = {c["leaderId"] for c in cfgs}
                commits = {c["commitIndex"] for c in cfgs}
                if len(leaders) == 1 and "" not in leaders and len(commits) == 1:
                    return True
            except Exception:
                pass
            time.sleep(0.5)
        return False

    def checkpoint(self):
        client = AgencyClient(timeout=10.0)
        self.paused.set()
        try:
            with self.active_cv:
                self.active_cv.wait_for(lambda: self.active == 0, timeout=60)
            self.stop_faults(45)
            if not self.wait_stable(client, time.monotonic() + 40):
                log("checkpoint: cluster did not settle; skipping durability check")
                return
            # Resolve ambiguous writes where the leader can tell us.
            cids = self.model.maybe_cids(time.monotonic())
            for i in range(0, len(cids), 50):
                chunk = cids[i:i + 50]
                try:
                    indices = client.inquire(chunk, agent=random_choice(AGENTS))
                except Ambiguous:
                    break
                for cid, idx in zip(chunk, indices):
                    if idx:
                        self.model.resolve_committed(cid, idx)
                        reachable("A write with an unknown outcome was resolved as committed by inquire",
                                  {"clientId": cid, "index": idx})
                    else:
                        reachable("A write with an unknown outcome was not in the leader's log",
                                  {"clientId": cid})
            if self.read_and_check(client, quiesced=True):
                self.stats["checkpoints"] += 1
                reachable("A quiesced durability check ran", {"stats": dict(self.stats)})
        finally:
            self.paused.clear()

    # ---- main ---------------------------------------------------------------

    def run(self, duration=None):
        log(f"workload config: {json.dumps(self.cfg)}")
        self.log_checker.start()
        for wid in range(self.cfg["writers"]):
            threading.Thread(target=self.writer, args=(wid,), daemon=True, name=f"writer-{wid}").start()
        threading.Thread(target=self.reader, daemon=True, name="reader").start()
        began = time.monotonic()
        next_status = began + 10
        next_checkpoint = began + self.cfg["checkpoint_interval"]
        while duration is None or time.monotonic() - began < duration:
            time.sleep(1)
            now = time.monotonic()
            if now >= next_checkpoint:
                self.checkpoint()
                next_checkpoint = time.monotonic() + self.cfg["checkpoint_interval"]
            if now >= next_status:
                log(f"stats: {json.dumps(self.stats)} max_term={self.log_checker.max_term_seen}")
                next_status = now + 10
        self.checkpoint()
        log(f"final stats: {json.dumps(self.stats)}")
