"""Spark, Visually · Part II · Episode 7: Counting Walls.

Every number the episode shows or says is computed here, checked, and written to e07_numbers.json.

    python e07_numbers.py        # runs Spark, prints the ledger, writes e07_numbers.json

Rules kept here (blueprint, "Rules Spark adds"):
  * Code on screen runs: the code panel (CODE_ON_SCREEN) and every gallery card run verbatim.
  * Walls come from Spark's plans: a wall is a shuffle Exchange in the plan (a BroadcastExchange is not one).
  * Exact where exact: walls, jobs, stages, rows and bytes written to each shuffle. No durations: this sandbox has
    one CPU, so the race is told in walls, rows and bytes.
"""
from __future__ import annotations

import contextlib
import io
import json
import os
import re
import shutil
import sys
from pathlib import Path

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE.parents[1]))      # the series root, for the shared sv package
from sv.cast import NAMES, PATIENTS, SEED, TRAYS, write_cast  # noqa: E402
from sv.runlog import MASTER, SPARK_PIN, parse_events, parse_jobs, session  # noqa: E402

WORK = Path("/tmp/sv_e07")
EVENTS = WORK / "events"
SCALE = 10_000                    # the race, on the cast ten thousand times over

CODE_ON_SCREEN = [
    'slow = df.repartition(200).groupBy("clinic").count()',     # S01: three versions of one question
    'fast = df.groupBy("clinic").count()',
    'kept = df.repartition("clinic").groupBy("clinic").count()',
    'slow.explain()',                                            # S05: count the walls in the plan
    '[q.collect() for q in (slow, fast, kept)]',                 # S06: the race
]
NARROW = ['df.filter(F.col("x") == 1)', 'df.select("clinic", "y")', 'df.withColumn("z", F.col("y") + 1)',
          'df.union(df)', 'df.coalesce(2)']                                                    # S03's cards
WIDE = ['df.groupBy("clinic").count()', 'df.distinct()', 'df.join(clinics.hint("merge"), "clinic")',
        'df.repartition(4)', 'df.withColumn("n", F.count("*").over(Window.partitionBy("clinic")))',
        'df.orderBy("patient")']                                                               # S04's cards
SORT_WITHIN = 'df.sortWithinPartitions("patient")'                                             # S04: no wall
UNION_SQL = ['spark.sql("SELECT * FROM cast UNION ALL SELECT * FROM cast")',                   # S03's note: no wall
             'spark.sql("SELECT * FROM cast UNION SELECT * FROM cast")']                       # plain UNION: a wall
REUSE = ['d = df.repartition("clinic")',                                                       # S07: one deal by clinic,
         'd.groupBy("clinic").count()',                                                        # and two later steps
         'd.withColumn("n", F.count("*").over(Window.partitionBy("clinic")))']                 # keyed by it

CHECKS: list[tuple[str, bool, str]] = []


def check(name: str, ok: bool, detail: str = "") -> None:
    CHECKS.append((name, bool(ok), detail))


def walls(plan: str) -> list[str]:
    """The shuffle Exchanges in a plan, as Spark prints them (partitioning only). A BroadcastExchange is not a wall."""
    return [m.group(1) for m in re.finditer(r"(?<!Broadcast)Exchange (\w+\([^)]*\)|SinglePartition)", plan)]


def main() -> int:
    shutil.rmtree(WORK, ignore_errors=True)
    EVENTS.mkdir(parents=True)

    import pyspark
    from pyspark.sql import Window, functions as F

    spark = session("spark-visually-e07", EVENTS)
    sc = spark.sparkContext
    check(f"pinned engine: Apache Spark {SPARK_PIN}", pyspark.__version__ == SPARK_PIN, pyspark.__version__)

    def tagged(tag: str, action):
        sc.setJobDescription(tag)
        try:
            return action()
        finally:
            sc.setJobDescription(None)

    four = WORK / "cast"
    tagged("setup.four", lambda: write_cast(spark, str(four)))
    tagged("setup.clinics", lambda: spark.createDataFrame([(k, i + 1) for i, k in enumerate(NAMES)], "clinic string, card int")
           .coalesce(1).write.parquet(str(WORK / "clinics")))
    df = spark.read.parquet(str(four))
    df.createOrReplaceTempView("cast")
    clinics = spark.read.parquet(str(WORK / "clinics"))
    ns = {"spark": spark, "df": df, "F": F, "Window": Window, "clinics": clinics}
    plan_of = lambda q: q._jdf.queryExecution().executedPlan().toString()

    # ---- the galleries: each card's walls from its plan, and its jobs and stages when it runs
    def card(code, tag):
        q = eval(code, ns)
        plan = plan_of(q)
        tagged(tag, lambda: q.write.format("noop").mode("overwrite").save())
        return {"code": code, "walls": walls(plan), "broadcasts": plan.count("BroadcastExchange")}
    narrow = [card(c, f"narrow{k}") for k, c in enumerate(NARROW)]
    wide = [card(c, f"wide{k}") for k, c in enumerate(WIDE)]
    sortw = card(SORT_WITHIN, "sortwithin")
    unions = [card(c, f"union_sql{k}") for k, c in enumerate(UNION_SQL)]
    exec(REUSE[0], ns)
    reuse = [{"code": c, "walls": walls(plan_of(eval(c, ns)))} for c in REUSE[1:]]

    # ---- the code panel, on the cast
    for line in CODE_ON_SCREEN[:3]:
        exec(line, ns)
    printed = io.StringIO()
    with contextlib.redirect_stdout(printed):
        tagged("line4", lambda: eval(CODE_ON_SCREEN[3], ns))
    explain_slow = printed.getvalue().strip()
    three = {k: plan_of(ns[k]) for k in ("slow", "fast", "kept")}
    results = tagged("race.cast", lambda: eval(CODE_ON_SCREEN[4], ns))
    answers = [{r["clinic"]: r["count"] for r in rows} for rows in results]
    check("all three give the same answer", answers[0] == answers[1] == answers[2], str(answers[1]))

    # ---- the race, on the cast ten thousand times over (the same three lines, on the bigger table)
    big_path = WORK / "cast_scaled"
    tagged("setup.big", lambda: df.crossJoin(spark.range(SCALE)).select(
        (F.col("patient") * SCALE + F.col("id")).alias("patient"), "clinic", "x", "y").write.parquet(str(big_path)))
    bns = dict(ns, df=spark.read.parquet(str(big_path)))
    for line in CODE_ON_SCREEN[:3]:
        exec(line, bns)
    # Fresh DataFrames from the same lines: re-running the ones line 5 collected would reuse their shuffle files
    # (Episode 4's skipped stages) and write nothing to measure.
    cns = dict(ns)
    for line in CODE_ON_SCREEN[:3]:
        exec(line, cns)
    for k in ("slow", "fast", "kept"):
        tagged(f"race.big.{k}", lambda k=k: bns[k].collect())
        tagged(f"race.cast.{k}", lambda k=k: cns[k].collect())
    settings = {k: spark.conf.get(k) for k in ("spark.sql.adaptive.enabled", "spark.sql.shuffle.partitions",
                                               "spark.sql.autoBroadcastJoinThreshold")}
    spark.stop()

    # ------------------------------------------------------------------------- the event log
    ev, J = parse_events(EVENTS), parse_jobs(EVENTS)
    jobs_of = lambda tag: [j for j in J["jobs"].values() if j["desc"] == tag]

    def crossings(tag):
        """Rows and bytes written into each shuffle, in the order the walls were built."""
        out = []
        for st in sorted(ev[tag], key=lambda s: s["stage"]):
            w = sum(t["write_records"] for t in st["tasks"])
            if w:
                out.append({"rows": w, "bytes": sum(t["write_bytes"] for t in st["tasks"]), "tasks": len(st["tasks"])})
        return out

    for name, cards in (("narrow", narrow), ("wide", wide)):
        for k, c in enumerate(cards):
            js = jobs_of(f"{name}{k}")
            c["jobs"], c["stages_run"] = len(js), sum(len(j["ran"]) for j in js)
    sortw["jobs"] = len(jobs_of("sortwithin"))
    check("every narrow card has no wall, and runs as one stage",
          all(not c["walls"] and c["stages_run"] == 1 for c in narrow), str([(c["code"], c["walls"], c["stages_run"]) for c in narrow]))
    check("every wide card has at least one wall", all(c["walls"] for c in wide), str([(c["code"], c["walls"]) for c in wide]))
    join = next(c for c in wide if "join" in c["code"])
    check("the sort-merge join has a wall on each side", len(join["walls"]) == 2 and join["broadcasts"] == 0, str(join["walls"]))
    order = next(c for c in wide if "orderBy" in c["code"])
    check("orderBy is a range wall, and runs an extra job to sample the ranges",
          order["walls"] and order["walls"][0].startswith("rangepartitioning") and order["jobs"] > max(
              c["jobs"] for c in wide if c is not order and len(c["walls"]) == 1), f"{order['walls']}, {order['jobs']} jobs")
    check("sortWithinPartitions sorts each pile where it lies: no wall", not sortw["walls"], str(sortw))
    order_jobs = sorted(jobs_of(f"wide{WIDE.index(order['code'])}"), key=lambda j: j["job"])
    def job_reads(j):
        return sum(t["input_records"] for st in ev[j["desc"]] if st["stage"] in j["ran"] for t in st["tasks"])
    order["rows_read_per_job"] = [job_reads(j) for j in order_jobs]
    check("orderBy's extra job reads the whole table to sample it: the table is read twice",
          order["rows_read_per_job"].count(PATIENTS) == 2, str(order["rows_read_per_job"]))
    check("SQL's UNION ALL stacks piles (no wall); plain UNION removes duplicates, and that is a wall",
          not unions[0]["walls"] and len(unions[1]["walls"]) >= 1, str([u["walls"] for u in unions]))
    check("one deal by clinic serves two later steps keyed by clinic: no new wall for either",
          all(len(r["walls"]) == 1 for r in reuse), str([r["walls"] for r in reuse]))

    w3 = {k: walls(p) for k, p in three.items()}
    check("slow has two walls: a round-robin deal, then the hash deal the count needs",
          len(w3["slow"]) == 2 and w3["slow"][1].startswith("RoundRobinPartitioning(200)")
          and w3["slow"][0].startswith("hashpartitioning(clinic"), str(w3["slow"]))
    check("fast has one wall", len(w3["fast"]) == 1 and w3["fast"][0].startswith("hashpartitioning(clinic"), str(w3["fast"]))
    check("kept has one wall: the count finds its rows already dealt by clinic",
          len(w3["kept"]) == 1 and w3["kept"][0].startswith("hashpartitioning(clinic"), str(w3["kept"]))
    check("explain() shows slow's two Exchange lines", len(walls(explain_slow)) == 2)

    race = {size: {k: crossings(f"race.{size}.{k}") for k in ("slow", "fast", "kept")} for size in ("cast", "big")}
    for size, rows_total in (("cast", PATIENTS), ("big", PATIENTS * SCALE)):
        R = race[size]
        check(f"{size}: slow's first wall carries every row, and its second the partial counts",
              len(R["slow"]) == 2 and R["slow"][0]["rows"] == rows_total and R["slow"][1]["rows"] < rows_total, str(R["slow"]))
        check(f"{size}: fast's one wall carries only partial counts",
              len(R["fast"]) == 1 and R["fast"][0]["rows"] < rows_total, str(R["fast"]))
        check(f"{size}: kept's one wall carries every row",
              len(R["kept"]) == 1 and R["kept"][0]["rows"] == rows_total, str(R["kept"]))
    B = race["big"]
    total = lambda xs: sum(x["bytes"] for x in xs)
    check("at scale, fast moves the fewest bytes, then kept, then slow",
          total(B["fast"]) < total(B["kept"]) < total(B["slow"]), str({k: total(v) for k, v in B.items()}))

    result = {
        "series": "Spark, Visually", "part": "II · Why jobs are slow", "episode": 7, "title": "Counting Walls",
        "engine": {"spark": SPARK_PIN, "master": MASTER, "cpus": os.cpu_count(), "settings": settings,
                   "note": "local mode: exact plans, walls, jobs, stages, rows and bytes; no durations"},
        "code_on_screen": CODE_ON_SCREEN, "cast": {"patients": PATIENTS, "files": TRAYS, "seed": SEED},
        "answers": answers[1], "narrow": narrow, "wide": wide, "sort_within": sortw, "union_sql": unions,
        "reuse": {"lines": REUSE, "walls": [r["walls"] for r in reuse]},
        "walls": w3, "explain_slow": explain_slow, "race": race, "scale": SCALE,
        "checks": [{"name": n, "ok": ok, "detail": d} for n, ok, d in CHECKS],
    }
    (HERE / "e07_numbers.json").write_text(json.dumps(result, indent=1, ensure_ascii=False) + "\n")
    for k in ("answers", "walls", "race"):
        print(f"{k}: {result[k]}")
    print("orderBy:", order, "| sortWithin:", sortw)
    failed = [c for c in CHECKS if not c[1]]
    for n, ok, d in CHECKS:
        print(("PASS " if ok else "FAIL ") + n + (f"  [{d}]" if d else ""))
    print(f"{len(CHECKS) - len(failed)} of {len(CHECKS)} checks pass")
    return 1 if failed else 0


if __name__ == "__main__":
    sys.exit(main())
