"""Spark, Visually: the cast every episode runs on.

Model in Motion's five clinics and 103 patients, numbered in clinic order, dealt by a seeded shuffle and written as
four Parquet files, so every episode reads the same four trays. Numbers files import this; nothing here draws, and
nothing here imports Manim, so a numbers file runs without the animation stack.
"""
from __future__ import annotations

import random
from collections import Counter

# clinic -> (lower-risk patients, of whom readmitted, higher-risk patients, of whom readmitted): Model in Motion's cast
CAST = {
    "Alder": (24, 8, 6, 5),
    "Birch": (6, 1, 24, 13),
    "Cedar": (10, 2, 10, 6),
    "Dune": (6, 2, 6, 5),
    "Elm": (11, 0, 0, 0),
}
NAMES = list(CAST)
SEED = 2026                    # Model in Motion's seed
TRAYS = 4                      # the cast is written as four files: one tray per executor bench
PATIENTS = sum(n0 + n1 for n0, _, n1, _ in CAST.values())
READMITTED = sum(e0 + e1 for _, e0, _, e1 in CAST.values())


def cast_rows() -> list[tuple[int, str, int, int]]:
    """(patient, clinic, x, y): numbered in clinic order, then dealt by a seeded shuffle."""
    rows, pid = [], 0
    for k in NAMES:
        n0, e0, n1, e1 = CAST[k]
        for x, n, e in ((0, n0, e0), (1, n1, e1)):
            for j in range(n):
                pid += 1
                rows.append((pid, k, x, 1 if j < e else 0))
    random.Random(SEED).shuffle(rows)
    return rows


def write_cast(spark, path: str) -> None:
    """The cast as four Parquet files under `path`: the rows go to Spark in four slices, one file each."""
    from pyspark.sql.types import IntegerType, StringType, StructField, StructType
    schema = StructType([StructField("patient", IntegerType()), StructField("clinic", StringType()),
                         StructField("x", IntegerType()), StructField("y", IntegerType())])
    spark.createDataFrame(spark.sparkContext.parallelize(cast_rows(), TRAYS), schema).write.mode("overwrite").parquet(path)


def write_slices(rows: list) -> list[list]:
    """How a list of rows handed to Spark in four slices (sc.parallelize(rows, 4), as write_cast does) is cut into four
    files: batches of len // 4, and the last slice takes the rest."""
    size = len(rows) // TRAYS
    batches = [rows[k:k + size] for k in range(0, len(rows), size)]
    cuts = [(k * len(batches)) // TRAYS for k in range(TRAYS + 1)]
    return [sum(batches[a:b], []) for a, b in zip(cuts[:-1], cuts[1:])]


def trays_from_counts(counts: dict, with_ids: bool = False) -> dict[int, list[tuple]]:
    """Each tray's records in order, as (clinic, readmitted). write_cast hands Spark the dealt cast in batches of
    103 // 4 = 25; Spark slices the five batches into four (the last slice takes two) and writes one file per slice.
    Slices are matched to trays by the per-tray clinic counts a numbers file read back from Spark (`counts`, keyed
    by tray as a string), so a mismatch fails loudly instead of drawing the wrong chips. With `with_ids`, each record
    is (patient, clinic, readmitted), so a later re-deal can move the exact chips Spark moved."""
    slices = write_slices(cast_rows())
    out = {}
    for t in range(TRAYS):
        want = {k: v for k, v in counts[str(t)].items() if v}
        match = [sl for sl in slices if dict(Counter(r[1] for r in sl)) == want]
        assert len(match) == 1, f"tray {t}: no unique slice"
        out[t] = [(r[0], r[1], bool(r[3])) if with_ids else (r[1], bool(r[3])) for r in match[0]]
    assert sum(len(v) for v in out.values()) == PATIENTS
    return out
