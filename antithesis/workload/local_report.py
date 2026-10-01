"""Summarize a local run: every cataloged assertion vs. what the SDK logged to
ANTITHESIS_SDK_LOCAL_OUTPUT. Mirrors the Antithesis property report.

usage: python3 local_report.py /tmp/sdk/assertions.jsonl
"""

import json
import sys

from antithesis.catalog import scan_tree


def main(path):
    catalog = {a.message: a.kind.value for a in scan_tree("/opt/agency-workload")}
    seen = {}
    with open(path) as f:
        for line in f:
            ev = json.loads(line).get("antithesis_assert")
            if not ev:
                continue
            s = seen.setdefault(ev["message"], {"type": ev["assert_type"], "must_hit": ev["must_hit"],
                                                "true": 0, "false": 0, "example": None})
            if ev["condition"]:
                s["true"] += 1
            else:
                s["false"] += 1
                s["example"] = s["example"] or ev.get("details")
    failed = 0
    for msg in sorted(catalog):
        kind = catalog[msg]
        s = seen.get(msg)
        if s is None:
            ok = kind.startswith("Always") or kind == "Unreachable"
            status = "not evaluated" if ok else "NEVER REACHED"
        elif s["type"] == "always":
            ok = s["false"] == 0
            status = f"{'pass' if ok else 'FAIL'} ({s['true']} ok / {s['false']} failed)"
        elif s["type"] == "sometimes":
            ok = s["true"] > 0
            status = f"{'fired' if ok else 'NEVER TRUE'} ({s['true']} true / {s['false']} false)"
        elif not s["must_hit"]:  # unreachable
            ok = False
            status = f"FAIL: reached {s['true'] + s['false']}x"
        else:
            ok = True
            status = f"reached ({s['true']}x)"
        failed += not ok
        print(f"{'  ' if ok else '!!'} [{kind:<28}] {msg}\n       {status}")
        if s and not ok and s["example"]:
            print(f"       example: {json.dumps(s['example'], default=str)[:600]}")
    print(f"\n{len(catalog)} cataloged assertions, {failed} failed or never fired")


if __name__ == "__main__":
    main(sys.argv[1] if len(sys.argv) > 1 else "/tmp/sdk/assertions.jsonl")
