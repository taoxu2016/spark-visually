"""Spark, Visually · Episode 5 · the cast as the episode draws it.

Rebuilds every tray's records from the numbers file's deal and sorts them into bins by Spark's own hash, checking each
step against e05_numbers.json. Shared by episode/episode05.py and preview_stills.py; no drawing here.
"""
from __future__ import annotations

import json
from collections import Counter  # noqa: F401
from pathlib import Path

from e05_numbers import TRAYS  # noqa: F401
from sv.cast import trays_from_counts

HERE = Path(__file__).resolve().parent
N = json.loads((HERE / "e05_numbers.json").read_text())
NAMES = ["Alder", "Birch", "Cedar", "Dune", "Elm"]                      # series order
CODE = N["code_on_screen"]
HASH = {k: {int(r): p for r, p in v.items() if r != "hash"} for k, v in N["hash"].items()}   # R -> reduce partition
HASH_VALUE = {k: v["hash"] for k, v in N["hash"].items()}
ALL, COUNT, LIST = N["shuffles"]["all_rows"], N["shuffles"]["count"], N["shuffles"]["collect_list"]
HOME = {r: r % 4 for r in range(5)}           # reducer r runs on bench r mod 4: the home shot's placement
PATIENTS = N["cast"]["patients"]


TRAY = trays_from_counts(N["trays"])


def bin_contents(t: int, R: int = 5) -> list[list[tuple[str, bool]]]:
    """Tray t's records sorted into R bins by Spark's hash."""
    out = [[] for _ in range(R)]
    for clinic, event in TRAY[t]:
        out[HASH[clinic][R]].append((clinic, event))
    if R == 5:
        for b in range(R):
            assert len(out[b]) == ALL["bins"].get(f"{t},{b}", 0), (t, b)
    return out


def partial_cards(t: int, R: int = 5) -> list[list[tuple[str, int]]]:
    """Tray t's partial counts, one card per clinic, in the bin Spark's hash sends it to."""
    out = [[] for _ in range(R)]
    counts = Counter(c for c, _ in TRAY[t])
    for clinic in NAMES:
        if counts[clinic]:
            out[HASH[clinic][R]].append((clinic, counts[clinic]))
    return out


def reducer_rows(R: int = 5) -> dict[int, list[tuple[str, bool]]]:
    """What each reducer holds after the fetch, grouped by clinic."""
    got = {r: [rec for t in range(TRAYS) for rec in bin_contents(t, R)[r]] for r in range(R)}
    if R == 5:
        assert [len(got[r]) for r in range(R)] == ALL["records_read"]
    if R == 3:
        assert {str(r): len(v) for r, v in got.items() if v} == N["r3_patients_per_reducer"]
    return {r: sorted(v, key=lambda x: NAMES.index(x[0])) for r, v in got.items()}


def crossing() -> tuple[list[tuple[int, int, int, bool]], int, int]:
    """Every non-empty bin as (tray, reducer, records, crosses the network on the home shot), with the totals."""
    out = [(t, r, len(bin_contents(t)[r]), HOME[r] != t) for t in range(TRAYS) for r in range(5) if bin_contents(t)[r]]
    bins_x = sum(1 for o in out if o[3])
    rows_x = sum(o[2] for o in out if o[3])
    assert bins_x == ALL["home_shot_blocks_crossing_network"] and rows_x == ALL["home_shot_records_crossing_network"]
    return out, bins_x, rows_x
