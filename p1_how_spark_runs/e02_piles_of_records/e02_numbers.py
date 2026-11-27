"""Spark, Visually · Part I · Episode 2: Piles of Records.

Every number the episode shows or says is computed here, checked, and written to e02_numbers.json, except the task
timelines: those need real start and end times from a machine with eight real cores, and come from
e02_timings_databricks.py (see below).

    python e02_numbers.py        # runs Spark, prints the ledger, writes e02_numbers.json

Rules kept here (blueprint, "Rules Spark adds"):
  * Code on screen runs: the code-panel lines in CODE_ON_SCREEN are executed verbatim.
  * One pinned engine: the Spark version, master and every setting used are written into the JSON.
  * Exact where exact: partitions, tasks, rows and shuffle records come from Spark (event log, plans).

What is measured here and what is not. This runs local[8] on whatever machine it is given; the sandbox it was written
in has one CPU, where eight "cores" share one processor. Counts are exact on any machine. Times are not: eight tasks
sharing one CPU each take eight times as long, so wall-clock timelines from such a machine would show no gain from
parallelism. Two things are kept from timing here, both safe on one CPU: how many tasks ever run at once (never more
than the eight slots), and the fixed cost of a task measured in CPU time, which sharing a processor does not inflate.
"""
from __future__ import annotations

import contextlib
import io
import json
import math
import os
import shutil
import sys
from pathlib import Path

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE.parents[1]))      # the series root, for the shared sv package
from sv.cast import NAMES, PATIENTS, SEED, TRAYS, cast_rows, write_cast  # noqa: E402
from sv.runlog import MASTER, SPARK_PIN, parse_events, session, stage_of  # noqa: E402

WORK = Path("/tmp/sv_e02")
EVENTS = WORK / "events"
SCALE = 10_000                 # the cast ten thousand times over: enough rows per task to price the work per row
BIG_ROWS = 8_000_000           # one big file: 8 million random doubles, about 64 MB
BIG_ROW_GROUP = 8 * 2 ** 20    # Parquet row groups of 8 MB, so the file can be cut at eight places
UNEVEN = (70, 11, 11, 11)      # S07: four files, one far bigger than the rest

CODE_ON_SCREEN = [
    'piles = df.withColumn("pile", F.spark_partition_id())',   # S03: which pile each row sits in
    'piles.groupBy("pile").count().show()',                   # S03: rows per pile (an action: it runs)
    'df.coalesce(2)',                                         # S06: merge piles where they stand
    'df.repartition(8)',                                      # S06: re-deal every row
]

CHECKS: list[tuple[str, bool, str]] = []


def check(name: str, ok: bool, detail: str = "") -> None:
    CHECKS.append((name, bool(ok), detail))


def max_overlap(tasks: list[dict]) -> int:
    """The most tasks holding a slot at the same moment: from launch until the executor is done with the task
    (deserializing, running, serializing the result). The driver's finish time comes later, once it has the result."""
    ends = [t["launch"] + t["deser_ms"] + t["run_ms"] + t["ser_ms"] for t in tasks]
    ev = sorted([(t["launch"], 1) for t in tasks] + [(e, -1) for e in ends], key=lambda e: (e[0], e[1]))
    run = best = 0
    for _, d in ev:
        run += d
        best = max(best, run)
    return best


def main() -> int:
    shutil.rmtree(WORK, ignore_errors=True)
    EVENTS.mkdir(parents=True)

    import pyspark
    from pyspark.sql import functions as F
    from pyspark.sql.types import IntegerType, StringType, StructField, StructType

    spark = session("spark-visually-e02", EVENTS)
    sc = spark.sparkContext
    check(f"pinned engine: Apache Spark {SPARK_PIN}", pyspark.__version__ == SPARK_PIN, pyspark.__version__)
    slots = sc.defaultParallelism
    check("eight task slots, as on the home shot", slots == 8, str(slots))
    schema = StructType([StructField("patient", IntegerType()), StructField("clinic", StringType()),
                         StructField("x", IntegerType()), StructField("y", IntegerType())])

    def tagged(tag: str, action):
        sc.setJobDescription(tag)
        try:
            return action()
        finally:
            sc.setJobDescription(None)

    def parts(df) -> int:
        """Partition count, read from the plan's RDD in the JVM (the series' on-screen code never uses df.rdd)."""
        return df._jdf.rdd().getNumPartitions()

    def pile_sizes(df) -> list[int]:
        got = {r["pile"]: r["count"] for r in df.withColumn("pile", F.spark_partition_id()).groupBy("pile").count().collect()}
        return [got.get(p, 0) for p in range(parts(df))]

    # ---- the cast as one file, and as four (the series' cast)
    one = WORK / "cast_one_file"
    tagged("setup.one", lambda: spark.createDataFrame(cast_rows(), schema).coalesce(1).write.parquet(str(one)))
    four = WORK / "cast"
    tagged("setup.four", lambda: write_cast(spark, str(four)))
    df1, df = spark.read.parquet(str(one)), spark.read.parquet(str(four))
    check("the cast as one file is one pile", parts(df1) == 1, str(parts(df1)))
    tagged("one.count", lambda: df1.count())

    # the code panel, verbatim
    ns = {"df": df, "F": F}
    exec(CODE_ON_SCREEN[0], ns)
    shown = io.StringIO()
    with contextlib.redirect_stdout(shown):
        tagged("four.piles", lambda: exec(CODE_ON_SCREEN[1], ns))
    four_sizes = pile_sizes(df)
    trays = {t: {k: 0 for k in NAMES} for t in range(TRAYS)}
    for r in df.withColumn("pile", F.spark_partition_id()).groupBy("pile", "clinic").count().collect():
        trays[r["pile"]][r["clinic"]] = r["count"]
    check("the cast as four files is four piles: 28, 25, 25, 25", four_sizes == [28, 25, 25, 25], str(four_sizes))
    check("the code panel's show() prints the four piles", all(str(n) in shown.getvalue() for n in four_sizes))

    # ---- eight and thirty-two piles (re-dealt, so every pile has rows)
    r8, r32 = df.repartition(8), df.repartition(32)
    sizes8, sizes32 = pile_sizes(r8), pile_sizes(r32)
    check("repartition(8) makes eight piles", len(sizes8) == 8 and sum(sizes8) == PATIENTS, str(sizes8))
    check("repartition(32) makes thirty-two small piles, about three rows each", len(sizes32) == 32
          and min(sizes32) >= 1 and max(sizes32) <= 5 and sum(sizes32) == PATIENTS, f"{min(sizes32)} to {max(sizes32)} rows")
    tagged("r8.count", lambda: r8.count())
    tagged("r32.count", lambda: r32.count())

    # The work per row and the fixed cost per task, in CPU time: the same 32-pile job on the cast and on the cast
    # ten thousand times over (each copy with its own id).
    big_cast = df.crossJoin(spark.range(SCALE)).select((F.col("patient") * SCALE + F.col("id")).cast("int").alias("patient"),
                                                      "clinic", "x", "y")
    scaled = WORK / "cast_scaled"
    tagged("setup.scaled", lambda: big_cast.write.parquet(str(scaled)))
    s32 = spark.read.parquet(str(scaled)).repartition(32)
    tagged("s32.count", lambda: s32.count())
    REPEATS = 5                                   # the CPU measure: repeat both jobs and take medians over every task
    for i in range(REPEATS):
        tagged(f"cpu.small.{i}", lambda: r32.count())
        tagged(f"cpu.big.{i}", lambda: s32.count())

    # ---- where piles come from
    big = WORK / "big_file"
    tagged("setup.big", lambda: spark.range(0, BIG_ROWS, 1, 1).select(F.rand(SEED).alias("v")).write
           .option("parquet.block.size", BIG_ROW_GROUP).parquet(str(big)))
    big_files = list(big.glob("part-*.parquet"))
    check("the big file is one file", len(big_files) == 1, str(len(big_files)))
    big_mb = big_files[0].stat().st_size / 2 ** 20
    dfb = spark.read.parquet(str(big))
    big_parts = parts(dfb)
    tagged("big.count", lambda: dfb.count())
    # The same rows written with Spark's default row groups: one row group, so Spark still makes the pieces, but only
    # the piece holding the row group's midpoint gets any rows (why the narration names the 8 MB row groups).
    big_default = WORK / "big_file_default_row_groups"
    tagged("setup.big_default", lambda: spark.range(0, BIG_ROWS, 1, 1).select(F.rand(SEED).alias("v")).write.parquet(str(big_default)))
    dfd = spark.read.parquet(str(big_default))
    got_d = {r["p"]: r["count"] for r in dfd.select(F.spark_partition_id().alias("p")).groupBy("p").count().collect()}
    big_default_rows = [got_d.get(i, 0) for i in range(parts(dfd))]
    check("with Spark's default row groups the same rows make the same pieces, but only one piece holds rows",
          len(big_default_rows) == big_parts and sum(1 for n in big_default_rows if n) == 1, str(big_default_rows))
    open_cost_mb = int(spark.conf.get("spark.sql.files.openCostInBytes").rstrip("b")) // 2 ** 20

    small = WORK / "cast_32_files"
    tagged("setup.small", lambda: spark.read.parquet(str(four)).repartition(32).write.parquet(str(small)))
    n_small = len(list(small.glob("part-*.parquet")))
    small_parts = parts(spark.read.parquet(str(small)))
    created_parts = parts(spark.range(1000))

    spark.conf.set("spark.sql.adaptive.coalescePartitions.enabled", "false")
    shuffled_off = df.groupBy("clinic").count()
    tagged("shuffle.off", lambda: shuffled_off.collect())
    spark.conf.set("spark.sql.adaptive.coalescePartitions.enabled", "true")
    tagged("shuffle.on", lambda: df.groupBy("clinic").count().collect())

    # ---- merge or re-deal: the code panel's last two lines
    co = tagged("line3.coalesce", lambda: eval(CODE_ON_SCREEN[2], ns))
    re8 = tagged("line4.repartition", lambda: eval(CODE_ON_SCREEN[3], ns))
    co_sizes, re8_sizes = pile_sizes(co), pile_sizes(re8)
    tagged("coalesce.count", lambda: co.count())

    def pile_of(d) -> dict[str, int]:
        return {str(r["patient"]): r["pile"] for r in d.withColumn("pile", F.spark_partition_id()).select("patient", "pile").collect()}

    assign = {"four": pile_of(df), "coalesce2": pile_of(co), "repartition8": pile_of(re8), "repartition32": pile_of(r32)}
    tagged("repartition.count", lambda: re8.count())

    # ---- uneven piles: the cast written as four files of 70, 11, 11 and 11 rows
    rows, cuts = cast_rows(), [0]
    for n in UNEVEN:
        cuts.append(cuts[-1] + n)
    keyed = [(k, r) for k in range(TRAYS) for r in rows[cuts[k]:cuts[k + 1]]]
    uneven_rdd = sc.parallelize(keyed, TRAYS).partitionBy(TRAYS, lambda k: k).map(lambda kv: kv[1])
    uneven = WORK / "cast_uneven"
    tagged("setup.uneven", lambda: spark.createDataFrame(uneven_rdd, schema).write.parquet(str(uneven)))
    dfu = spark.read.parquet(str(uneven))
    uneven_sizes = sorted(pile_sizes(dfu), reverse=True)
    tagged("uneven.count", lambda: dfu.count())

    uneven_pile_of = {str(r["patient"]): r["pile"] for r in dfu.withColumn("pile", F.spark_partition_id()).select("patient", "pile").collect()}
    work = F.sum(F.length(F.sha2(F.concat_ws("-", "clinic", "patient", "x", "y"), 256))).alias("w")
    uneven_big = (dfu.crossJoin(F.broadcast(spark.range(SCALE)))
                  .select((F.col("patient") * SCALE + F.col("id")).cast("int").alias("patient"), "clinic", "x", "y")).cache()
    tagged("setup.uneven_big", lambda: uneven_big.count())
    uneven_big_sizes = sorted(pile_sizes(uneven_big), reverse=True)
    tagged("uneven_big.work", lambda: uneven_big.agg(work).collect())

    settings = {k: spark.conf.get(k) for k in ("spark.sql.files.maxPartitionBytes", "spark.sql.files.openCostInBytes",
                                               "spark.sql.shuffle.partitions", "spark.sql.adaptive.enabled",
                                               "spark.sql.adaptive.coalescePartitions.enabled")}
    spark.stop()

    # ------------------------------------------------------------------------- the event log
    ev = parse_events(EVENTS)

    def stage_tasks(tag: str, kind: str) -> list[dict]:
        return stage_of(ev[tag], kind)["tasks"]

    def dealt(tag: str) -> list[dict]:
        """The tasks of the stage that works on re-dealt piles: of the stages reading shuffled rows, the widest."""
        reading = [s for s in ev[tag] if any(t["read_records"] or t["blocks"] for t in s["tasks"])]
        return max(reading, key=lambda s: len(s["tasks"]))["tasks"]

    one_tasks = stage_tasks("one.count", "map")
    check("one pile: the count runs one task", len(one_tasks) == 1, str(len(one_tasks)))
    t8, t32 = dealt("r8.count"), dealt("r32.count")
    check("eight piles: eight tasks", len(t8) == 8, str(len(t8)))
    check("thirty-two piles: thirty-two tasks", len(t32) == 32, str(len(t32)))
    overlap32 = max_overlap(t32)
    check("never more than eight tasks run at once", overlap32 <= slots, str(overlap32))
    rounds32 = math.ceil(len(t32) / slots)

    s32t = dealt("s32.count")
    check("the scaled cast runs the same thirty-two tasks", len(s32t) == 32, str(len(s32t)))
    med = lambda xs: sorted(xs)[len(xs) // 2]
    cpu_small = med([t["cpu_ns"] for i in range(1, REPEATS) for t in dealt(f"cpu.small.{i}")])   # the first repeat warms up
    cpu_big = med([t["cpu_ns"] for i in range(1, REPEATS) for t in dealt(f"cpu.big.{i}")])
    rows_small, rows_big = PATIENTS / 32, PATIENTS * SCALE / 32
    per_row_ns = (cpu_big - cpu_small) / (rows_big - rows_small)
    fixed_ns = cpu_small - rows_small * per_row_ns
    fixed_share = fixed_ns / cpu_small
    check("a task's fixed cost is positive and the work per row is positive", fixed_ns > 0 and per_row_ns > 0,
          f"fixed {fixed_ns / 1e6:.2f} ms CPU, {per_row_ns:.0f} ns a row")
    check("with about three rows a pile, nearly all of a task is fixed cost", fixed_share > 0.99, f"{fixed_share:.4f}")
    rows_to_match = fixed_ns / per_row_ns
    check("a pile needs at least tens of thousands of rows before its work matches the fixed cost (a floor, not an estimate)",
          rows_to_match >= 10_000, f"{rows_to_match:,.0f} rows")

    big_tasks = stage_tasks("big.count", "map")
    big_rows_per_task = [t["input_records"] for t in big_tasks]
    check("the big file is cut into one piece per core, each holding rows", big_parts == slots and len(big_tasks) == slots
          and all(n > 0 for n in big_rows_per_task) and sum(big_rows_per_task) == BIG_ROWS, str(big_rows_per_task))
    check("thirty-two small files are packed into eight piles", n_small == 32 and small_parts == 8, f"{n_small} files, {small_parts} piles")
    check("data created in code starts as one pile per core", created_parts == slots, str(created_parts))
    sh_off, sh_on = stage_tasks("shuffle.off", "reduce"), stage_tasks("shuffle.on", "reduce")
    check("after a shuffle, the setting decides: 200 piles", len(sh_off) == 200, str(len(sh_off)))
    check("adaptive execution merges the cast's 200 tiny piles", len(sh_on) < 200, str(len(sh_on)))

    co_stages, re_stages = ev["coalesce.count"], ev["repartition.count"]
    co_scan = [s for s in co_stages if s["kind"] in ("map", "scan")]
    co_reads = [t["input_records"] for s in co_scan for t in s["tasks"] if t["input_records"]]
    check("coalesce(2): two piles, each made of two whole piles, nothing re-dealt",
          sorted(co_sizes) == sorted([four_sizes[0] + four_sizes[1], four_sizes[2] + four_sizes[3]])
          or sorted(co_sizes) == sorted([sum(p) for p in ((28, 25), (25, 25))]), str(co_sizes))
    co_shuffled = sum(t["write_records"] for s in co_stages for t in s["tasks"] if s["kind"] == "map" and len(s["tasks"]) > 2)
    re_map = [s for s in re_stages if s["kind"] == "map" and len(s["tasks"]) == TRAYS]
    re_shuffled = sum(t["write_records"] for t in re_map[0]["tasks"]) if re_map else -1
    check("repartition(8): every row is re-dealt", re_shuffled == PATIENTS and len(re8_sizes) == 8, f"{re_shuffled} rows")
    check("coalesce(2): no row is re-dealt", co_shuffled == 0 and len(co_reads) == 2 and sum(co_reads) == PATIENTS,
          f"{co_shuffled} rows re-dealt; tasks read {co_reads}")

    check("the coalesce and repartition lines run no job by themselves", "line3.coalesce" not in ev and "line4.repartition" not in ev)
    ut = stage_tasks("uneven.count", "map")
    check("uneven files read as uneven piles", uneven_sizes == list(UNEVEN) and sorted([t["input_records"] for t in ut], reverse=True)
          == list(UNEVEN), str(uneven_sizes))

    def timeline(ts: list[dict], rows_of=None) -> list[dict]:
        """Each task's real launch and the moment its executor was done with it (ms from the stage's first launch),
        its CPU time, and its fixed-cost share: everything that is not per-row work. With the rows of each pile known
        (rows_of), that is 1 - rows x CPU per row / task CPU; otherwise the fixed cost over the task's CPU."""
        t0 = min(t["launch"] for t in ts)

        def cap(t):
            if rows_of is not None:
                return 1.0 - rows_of[t["index"]] * per_row_ns / max(t["cpu_ns"], 1)
            return min(1.0, fixed_ns / max(t["cpu_ns"], 1))
        return [{"pile": t["index"], "start_ms": t["launch"] - t0, "end_ms": t["launch"] + t["deser_ms"] + t["run_ms"] + t["ser_ms"] - t0,
                 "cpu_ms": round(t["cpu_ns"] / 1e6, 3), "cap": round(max(0.0, cap(t)), 4)}
                for t in sorted(ts, key=lambda t: t["launch"])]

    ub = max([s for s in ev["uneven_big.work"]], key=lambda s: (len(s["tasks"]), sum(t["run_ms"] for t in s["tasks"])))["tasks"]
    check("the uneven work runs four tasks, the biggest pile's the longest", len(ub) == TRAYS
          and max(ub, key=lambda t: t["cpu_ns"])["run_ms"] == max(t["run_ms"] for t in ub), str(len(ub)))
    timelines = {"tiny_32": timeline(dealt(f"cpu.small.{REPEATS - 1}"), rows_of=sizes32), "uneven": timeline(ub),
                 "source": f"this run: Spark {SPARK_PIN}, {MASTER}, {os.cpu_count()} CPU",
                 "note": "real task times; with fewer CPUs than slots, tasks running together share the processor"}

    result = {
        "series": "Spark, Visually", "part": "I · How Spark runs a job", "episode": 2, "title": "Piles of Records",
        "engine": {"spark": SPARK_PIN, "master": MASTER, "cpus": os.cpu_count(), "settings": settings,
                   "note": "local mode: exact piles, tasks and rows; times are not used here (see the docstring)"},
        "code_on_screen": CODE_ON_SCREEN,
        "cast": {"patients": PATIENTS, "seed": SEED},
        "slots": slots,
        "piles": {"one_file": 1, "four_files": four_sizes, "eight": sizes8, "thirty_two": sizes32,
                  "rows_per_pile_at_32": [min(sizes32), max(sizes32)], "mean_rows_at_32": round(PATIENTS / 32, 1)},
        "tasks": {"one": len(one_tasks), "four": TRAYS, "eight": len(t8), "thirty_two": len(t32)},
        "rounds_at_32": rounds32, "max_tasks_at_once": overlap32,
        "fixed_cost": {"cpu_ms_per_task": round(fixed_ns / 1e6, 3), "cpu_ns_per_row": round(per_row_ns, 1),
                       "share_at_32_piles_of_the_cast": round(fixed_share, 4),
                       "rows_whose_work_equals_the_fixed_cost": round(rows_to_match),
                       "measured_from": f"median executor CPU time (incl. deserialization) over every task of {REPEATS - 1} "
                                        f"warm runs of a 32-pile count, on the cast and on the cast x {SCALE}"},
        "from_files": {"big_file_mb": round(big_mb, 1), "big_file_piles": big_parts, "big_rows_per_pile": big_rows_per_task,
                       "big_row_group_mb": BIG_ROW_GROUP // 2 ** 20, "big_default_row_groups_rows_per_pile": big_default_rows,
                       "min_piece_mb": open_cost_mb, "small_files": n_small, "small_file_piles": small_parts,
                       "max_partition_mb": int(settings["spark.sql.files.maxPartitionBytes"].rstrip("b")) // 2 ** 20},
        "created_piles": created_parts,
        "after_shuffle": {"setting": len(sh_off), "adaptive": len(sh_on)},
        "coalesce": {"piles": co_sizes, "rows_re_dealt": co_shuffled},
        "lines_3_4_jobs": 0 if "line3.coalesce" not in ev and "line4.repartition" not in ev else None,
        "repartition": {"piles": re8_sizes, "rows_re_dealt": re_shuffled},
        "uneven": {"piles": uneven_sizes, "ratio_biggest_to_others": round(UNEVEN[0] / UNEVEN[1], 1),
                   "scaled_piles": uneven_big_sizes, "pile_of": uneven_pile_of},
        "trays": {str(t): trays[t] for t in range(TRAYS)},
        "pile_of": assign,
        "timelines": timelines,
        "checks": [{"name": n, "ok": ok, "detail": d} for n, ok, d in CHECKS],
    }
    (HERE / "e02_numbers.json").write_text(json.dumps(result, indent=1, ensure_ascii=False) + "\n")

    for k in ("piles", "tasks", "rounds_at_32", "max_tasks_at_once", "fixed_cost", "from_files", "created_piles",
              "after_shuffle", "coalesce", "repartition", "uneven"):
        print(f"{k}: {result[k]}")
    failed = [c for c in CHECKS if not c[1]]
    for n, ok, d in CHECKS:
        print(("PASS " if ok else "FAIL ") + n + (f"  [{d}]" if d else ""))
    print(f"{len(CHECKS) - len(failed)} of {len(CHECKS)} checks pass")
    return 1 if failed else 0


if __name__ == "__main__":
    sys.exit(main())
