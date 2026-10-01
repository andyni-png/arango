#!/usr/bin/env python3
"""Deterministic repro for F1: a follower's commit index covers stale entries.

Runs against the real arangod image the harness uses (3.12.12). Starts a fresh
3-agent agency, pauses two agents, and acts as the leader towards the third
(the follower F) by sending it hand-built appendEntries RPCs:

  1. As a leader in term T+1, append two entries at L+1 and L+2 that never get
     committed (leaderCommit stays at F's commit index).
  2. As a different leader in term T+2, whose log matches F only up to L, send
       variant "heartbeat": the leader's periodic heartbeat, exactly as
                        Agent::sendEmptyAppendEntriesRPC builds it (empty,
                        prevLogIndex=0, prevLogTerm=0, leaderCommit=L+2), or
       variant "empty": an empty appendEntries with prevLogIndex=L and
                        leaderCommit=L+2 (a heartbeat), or
       variant "dup":   a retransmission of entry L with prevLogIndex=L-1 and
                        leaderCommit=L+2.
     Both are valid messages from that leader: its log matches F through L and
     it has committed its own (different) entries at L+1 and L+2.

Raft (Figure 2, AppendEntries rule 5) caps the follower's commit index at the
index of the last new entry, i.e. L. The bug: F sets it to
min(leaderCommit, F's own last index) = L+2, so the two stale term T+1 entries
count as committed.

Usage: F1-repro.py [heartbeat|empty|dup|all]   (default: all). Exit 0 = bug reproduced.
"""

import json
import subprocess
import sys
import time

import requests

HERE = __file__.rsplit("/", 1)[0]
COMPOSE = ["docker", "compose", "-p", "agency-f1-repro", "-f", f"{HERE}/../config/docker-compose.yaml"]
AGENTS = ["agency-agent1", "agency-agent2", "agency-agent3"]
PORT = 8531
TOKEN = ("eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9."
         "eyJpc3MiOiJhcmFuZ29kYiIsInNlcnZlcl9pZCI6ImZvbyJ9."
         "Q7ieHXQQk0A2BTFTeXDYEujLYxaxC7oYK5BD3Xg4jFg")
S = requests.Session()
S.headers["Authorization"] = f"bearer {TOKEN}"


def sh(*args, check=True):
    return subprocess.run(args, check=check, capture_output=True, text=True).stdout


def container(name):
    return sh(*COMPOSE, "ps", "-q", name).strip()


def ip_of(name):
    return sh("docker", "inspect", "-f",
              "{{range .NetworkSettings.Networks}}{{.IPAddress}}{{end}}", container(name)).strip()


def url(ips, agent, path):
    return f"http://{ips[agent]}:{PORT}{path}"


def get(ips, agent, path):
    r = S.get(url(ips, agent, path), timeout=5)
    r.raise_for_status()
    return r.json()


def up():
    sh(*COMPOSE, "down", "-v", "--remove-orphans", check=False)
    sh(*COMPOSE, "up", "-d", "--wait", *AGENTS)
    ips = {a: ip_of(a) for a in AGENTS}
    for _ in range(120):
        try:
            leaders = {get(ips, a, "/_api/agency/config")["leaderId"] for a in AGENTS}
            if len(leaders) == 1 and "" not in leaders:
                return ips
        except requests.RequestException:
            pass
        time.sleep(0.5)
    raise RuntimeError("no leader elected")


def down():
    sh(*COMPOSE, "down", "-v", "--remove-orphans", check=False)


def append_entries(ips, agent, term, leader_id, prev_index, prev_term, leader_commit, entries):
    r = S.post(url(ips, agent, "/_api/agency_priv/appendEntries"),
               params={"term": term, "leaderId": leader_id, "prevLogIndex": prev_index,
                       "prevLogTerm": prev_term, "leaderCommit": leader_commit,
                       "senderTimeStamp": 0},
               json=entries, timeout=5)
    r.raise_for_status()
    return r.json()


def entry(index, term, query, client_id):
    return {"index": index, "term": term, "query": query, "clientId": client_id,
            "timestamp": int(time.time() * 1000)}


def view(ips, agent):
    st = get(ips, agent, "/_api/agency/state")
    log = {int(e["_key"]): e for e in st["log"]}
    return st, log


def run(variant):
    print(f"\n=== variant: {variant} ===")
    ips = up()
    try:
        cfg = get(ips, AGENTS[0], "/_api/agency/config")
        leader_id = cfg["leaderId"]
        ids = {a: get(ips, a, "/_api/agency/config")["configuration"]["id"] for a in AGENTS}
        leader = next(a for a, i in ids.items() if i == leader_id)
        # Commit a few ordinary writes so F has a committed prefix.
        for i in range(5):
            r = S.post(url(ips, leader, "/_api/agency/write"),
                       json=[[{"/f1/committed": f"c{i}"}]], timeout=5)
            r.raise_for_status()
        time.sleep(1)

        f = next(a for a in AGENTS if a != leader)
        others = [a for a in AGENTS if a != f]
        fake_a, fake_b = ids[others[0]], ids[others[1]]
        for a in others:
            sh("docker", "pause", container(a))

        st, log = view(ips, f)
        L = max(log)
        tL = log[L]["term"]
        commit0 = st["index"]
        print(f"follower {f}: term={st['term']} last_index={L} (term {tL}) commit={commit0}")
        assert commit0 == L, "setup: follower should be fully caught up and committed"

        # Step 1: a term T+1 leader appends two entries that never commit.
        t1 = get(ips, f, "/_api/agency/config")["term"] + 1
        stale = [entry(L + 1, t1, {"/f1/x": "STALE-1"}, "stale-1"),
                 entry(L + 2, t1, {"/f1/x": "STALE-2"}, "stale-2")]
        res1 = append_entries(ips, f, t1, fake_a, L, tL, commit0, stale)
        # /state only returns the persisted log up to the commit index, so the
        # uncommitted entries are not visible here; they show up in step 2 if
        # the commit index is moved over them.
        st, log = view(ips, f)
        print(f"step 1 (term {t1}, 2 uncommitted entries at {L+1},{L+2}): "
              f"success={res1.get('success')} commit={st['index']}")
        assert res1.get("success") and st["index"] == commit0, \
            "setup: stale entries not accepted as expected"

        # Step 2: a term T+2 leader whose log matches F only through L.
        t2 = get(ips, f, "/_api/agency/config")["term"] + 1
        if variant == "heartbeat":
            # Exactly what Agent::sendEmptyAppendEntriesRPC sends on every
            # heartbeat: no entries, prevLogIndex=0, prevLogTerm=0.
            res2 = append_entries(ips, f, t2, fake_b, 0, 0, L + 2, [])
            desc = f"heartbeat (as sendEmptyAppendEntriesRPC) prevLogIndex=0 leaderCommit={L+2}"
        elif variant == "empty":
            res2 = append_entries(ips, f, t2, fake_b, L, tL, L + 2, [])
            desc = f"empty appendEntries prevLogIndex={L} leaderCommit={L+2}"
        else:
            e = log[L - 1]
            dup = [entry(L, tL, log[L]["request"], log[L].get("clientId", ""))]
            res2 = append_entries(ips, f, t2, fake_b, L - 1, e["term"], L + 2, dup)
            desc = f"re-send of entry {L} prevLogIndex={L-1} leaderCommit={L+2}"
        st, log = view(ips, f)
        commit = st["index"]
        print(f"step 2 (term {t2}, {desc}): success={res2.get('success')} commit={commit}")
        for i in range(L, max(log) + 1):
            e = log[i]
            print(f"   index {i}: term {e['term']} {json.dumps(e['request'])[:60]}"
                  f"{'   <- counted as committed' if i <= commit and e['term'] == t1 else ''}")

        bug = commit > L and any(log[i]["term"] == t1 for i in range(L + 1, commit + 1))
        if bug:
            print(f"BUG REPRODUCED: commit index {commit} covers stale term-{t1} entries; "
                  f"Raft requires commit <= {L}")
        else:
            print(f"not reproduced: commit index {commit} (Raft bound {L})")
        return bug
    finally:
        down()


def main():
    variants = sys.argv[1:] or ["all"]
    if variants == ["all"]:
        variants = ["heartbeat", "empty", "dup"]
    results = {v: run(v) for v in variants}
    print("\nsummary:", results)
    sys.exit(0 if all(results.values()) else 1)


if __name__ == "__main__":
    main()
