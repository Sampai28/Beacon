#!/usr/bin/env python3
"""Compare two load runs from bench/results/.

    python3 bench/compare.py 20000 20000-chunked

Reads the JSON each run writes and prints the figures that decide whether a
change helped: connection success, connect latency, JOIN latency percentiles,
and which thresholds failed. Percentiles come from the run's own k6 summary
rather than being recomputed, so the printed numbers are the same ones in the
committed .txt report.
"""

from __future__ import annotations

import argparse
import json
import os
import sys

RESULTS = os.path.join(os.path.dirname(os.path.abspath(__file__)), "results")


def load(tag: str) -> dict:
    path = os.path.join(RESULTS, f"load-{tag}.json")
    if not os.path.exists(path):
        sys.exit(f"no such run: {path}")
    with open(path, encoding="utf-8") as fh:
        return json.load(fh)


def metric(run: dict, name: str) -> dict:
    return (run.get("metrics") or {}).get(name) or {}


# k6 nests the numbers one level down under "values"; trends keep percentiles
# there as "p(95)" and rates keep passes/fails.
def values(run: dict, name: str) -> dict:
    return metric(run, name).get("values") or {}


def val(run: dict, name: str, key: str):
    v = values(run, name).get(key)
    return v if isinstance(v, (int, float)) else None


def fmt(v, suffix="", digits=2):
    if v is None:
        return "--"
    return f"{v:,.{digits}f}{suffix}"


def pct(run: dict, name: str):
    v = values(run, name)
    if "rate" in v:
        return 100.0 * v["rate"]
    passes, fails = v.get("passes"), v.get("fails")
    if passes is None:
        return None
    total = passes + (fails or 0)
    return 100.0 * passes / total if total else None


def delta(before, after, lower_is_better=True):
    if before is None or after is None or before == 0:
        return "--"
    change = (after - before) / before * 100.0
    better = change < 0 if lower_is_better else change > 0
    mark = "better" if better else "worse"
    if abs(change) < 1.0:
        mark = "same"
    return f"{change:+.1f}%  {mark}"


ROWS = [
    ("connect success", "beacon_connect_success", None, False),
    ("connect p95", "beacon_connect_ms", "p(95)", True),
    ("connect p99", "beacon_connect_ms", "p(99)", True),
    ("JOIN p50", "beacon_join_ms", "med", True),
    ("JOIN p95", "beacon_join_ms", "p(95)", True),
    ("JOIN p99", "beacon_join_ms", "p(99)", True),
    ("JOIN max", "beacon_join_ms", "max", True),
    ("JOIN success", "beacon_join_success", None, False),
]


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("before")
    ap.add_argument("after")
    args = ap.parse_args()

    b, a = load(args.before), load(args.after)

    print(f"{'':<18}{args.before:>16}{args.after:>16}   change")
    print("-" * 70)
    print(f"{'target conns':<18}{b['target_connections']:>16,}{a['target_connections']:>16,}")
    print("-" * 70)

    for label, name, key, higher_better in ROWS:
        if key is None:
            bv, av = pct(b, name), pct(a, name)
            print(f"{label:<18}{fmt(bv, '%'):>16}{fmt(av, '%'):>16}   "
                  f"{delta(bv, av, lower_is_better=False)}")
        else:
            bv, av = val(b, name, key), val(a, name, key)
            print(f"{label:<18}{fmt(bv, ' ms', 0):>16}{fmt(av, ' ms', 0):>16}   "
                  f"{delta(bv, av, lower_is_better=higher_better)}")

    print("-" * 70)
    for tag, run in ((args.before, b), (args.after, a)):
        fails = run.get("threshold_failures") or []
        print(f"{tag:<18}thresholds: " + ("all passed" if not fails else "FAILED " + "; ".join(fails)))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
