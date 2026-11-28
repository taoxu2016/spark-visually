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
import subprocess
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
# S07: why deal up front? A step that follows another on the same key reuses its deal with nothing dealt up front;
# a deal pays when it is cached and reused (Episode 17's teaser). The second review's case, a finer key then a
# coarser one, is measured too: a deal up front saves its wall but sends every row instead of partial counts.
FOLLOW = 'df.withColumn("n", F.count("*").over(Window.partitionBy("clinic"))).groupBy("clinic").count()'
CACHED = ['d = df.repartition("clinic").cache()', 'd.count()', 'd.groupBy("clinic").count()']
FINER_THEN_COARSER = 'df.groupBy("clinic", "x").count().withColumn("m", F.sum("count").over(Window.partitionBy("clinic")))'

# S05: explain() before and after line 5 runs, over Spark Connect (classic Spark is measured in the main run)
CONNECT_RUN = r'''
import contextlib, io, json, re, sys
from pyspark.sql import SparkSession
spark = SparkSession.builder.remote("local[4]").getOrCreate()
ns = {"df": spark.read.parquet(sys.argv[1])}
lines = json.loads(sys.argv[2])
for line in lines[:3]:
    exec(line, ns)
def exchanges(q):
    buf = io.StringIO()
    with contextlib.redirect_stdout(buf):
        q.explain()
    return len(re.findall(r"(?<!Broadcast)Exchange (\w+\([^)]*\)|SinglePartition)", buf.getvalue()))
before = [exchanges(ns[k]) for k in ("slow", "fast", "kept")]
eval(lines[4], ns)
after = [exchanges(ns[k]) for k in ("slow", "fast", "kept")]
spark.stop()
print("CONNECT_RESULT " + json.dumps({"before": before, "after": after}))
'''

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

    # S03's on-screen arrows: union stacks the two tables' piles; coalesce merges neighbouring piles
    def piles(q):
        return [r["count"] for r in tagged("piles", lambda: q.groupBy(F.spark_partition_id().alias("p")).count()
                                            .orderBy("p").collect())]
    pile_sizes = {"cast": piles(df), "union": piles(eval(NARROW[3], ns)), "coalesce": piles(eval(NARROW[4], ns))}

    # S07: a step that follows another on the same key reuses its deal; dealing up front adds nothing here
    follow = {"code": FOLLOW, "walls": walls(plan_of(eval(FOLLOW, ns))),
              "walls_with_deal_up_front": walls(plan_of(eval(FOLLOW, dict(ns, df=df.repartition("clinic")))))}
    # S07: a cached deal serves a later count with no new wall (Episode 17's teaser)
    cached_ns = dict(ns)
    exec(CACHED[0], cached_ns)
    tagged("cached.deal", lambda: eval(CACHED[1], cached_ns))
    later = eval(CACHED[2], cached_ns)
    later_plan, later_explained = plan_of(later), io.StringIO()
    with contextlib.redirect_stdout(later_explained):
        later.explain()
    tagged("cached.count", lambda: later.write.format("noop").mode("overwrite").save())
    cached = {"lines": CACHED, "walls_above_the_cache": walls(later_plan.split("InMemoryRelation")[0]),
              # explain() also prints the plan that built the cache: its deal, already run, shows as Exchange lines
              "exchange_lines_in_explain": len(walls(later_explained.getvalue()))}
    cached_ns["d"].unpersist(blocking=True)

    # ---- the code panel, on the cast
    def explained(q) -> str:
        buf = io.StringIO()
        with contextlib.redirect_stdout(buf):
            q.explain()
        return buf.getvalue().strip()
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
    # S05's caption: asked after line 5 has run, classic Spark prints the final plan above the starting plan
    after_run = {k: explained(ns[k]) for k in ("slow", "fast", "kept")}

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
    # The second review's case for dealing up front, at scale: a finer key, then a coarser one
    noop = lambda q: q.write.format("noop").mode("overwrite").save()
    tagged("finer.without", lambda: noop(eval(FINER_THEN_COARSER, bns)))
    tagged("finer.with", lambda: noop(eval(FINER_THEN_COARSER, dict(bns, df=bns["df"].repartition("clinic")))))
    settings = {k: spark.conf.get(k) for k in ("spark.sql.adaptive.enabled", "spark.sql.shuffle.partitions",
                                               "spark.sql.autoBroadcastJoinThreshold",
                                               "spark.sql.optimizer.canChangeCachedPlanOutputPartitioning")}
    spark.stop()

    # ---- S05 over Spark Connect: the code panel's lines on the same cast, explain() before and after line 5
    r = subprocess.run([sys.executable, "-c", CONNECT_RUN, str(four), json.dumps(CODE_ON_SCREEN)],
                       capture_output=True, text=True, timeout=600)
    line = next((ln for ln in r.stdout.splitlines() if ln.startswith("CONNECT_RESULT ")), None)
    assert line, f"Spark Connect run failed:\n{r.stdout[-1500:]}\n{r.stderr[-1500:]}"
    connect = json.loads(line[len("CONNECT_RESULT "):])

    # ------------------------------------------------------------------------- the event log
    ev, J = parse_events(EVENTS), parse_jobs(EVENTS)
    jobs_of = lambda tag: [j for j in J["jobs"].values() if j["desc"] == tag]

    def crossings(tag):
        """Rows and bytes written into each shuffle, in the order the walls were built."""
        out = []
        for st in sorted(ev.get(tag, []), key=lambda s: s["stage"]):
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
    check("union stacks the two tables' piles: 4 and 4 make 8, each pile whole",
          pile_sizes["union"] == pile_sizes["cast"] * 2, str(pile_sizes))
    P = pile_sizes["cast"]
    check("coalesce(2) merges neighbouring piles where they stand: piles 1 and 2, then piles 3 and 4",
          len(P) == TRAYS == 4 and pile_sizes["coalesce"] == [P[0] + P[1], P[2] + P[3]], str(pile_sizes))
    check("a step that follows another on the same key reuses its deal: a window over each clinic, then a count "
          "per clinic, builds one wall, with or without a deal up front",
          len(follow["walls"]) == 1 and len(follow["walls_with_deal_up_front"]) == 1, str(follow))
    cached["walls_crossed_by_the_count"] = crossings("cached.count")
    check("a cached deal by clinic serves a later count with no new wall (Episode 17's teaser)",
          not cached["walls_above_the_cache"] and not cached["walls_crossed_by_the_count"]
          and crossings("cached.deal")[0]["rows"] == PATIENTS, str(cached))

    w3 = {k: walls(p) for k, p in three.items()}
    check("slow has two walls: a round-robin deal, then the hash deal the count needs",
          len(w3["slow"]) == 2 and w3["slow"][1].startswith("RoundRobinPartitioning(200)")
          and w3["slow"][0].startswith("hashpartitioning(clinic"), str(w3["slow"]))
    check("fast has one wall", len(w3["fast"]) == 1 and w3["fast"][0].startswith("hashpartitioning(clinic"), str(w3["fast"]))
    check("kept has one wall: the count finds its rows already dealt by clinic",
          len(w3["kept"]) == 1 and w3["kept"][0].startswith("hashpartitioning(clinic"), str(w3["kept"]))
    check("explain() shows slow's two Exchange lines", len(walls(explain_slow)) == 2)
    after = {k: len(walls(t)) for k, t in after_run.items()}
    check("classic Spark, after line 5 has run: explain() prints the final plan above the starting plan, so each "
          "wall's Exchange line appears twice (slow 4, fast 2, kept 2)",
          after == {"slow": 4, "fast": 2, "kept": 2}
          and all("== Final Plan ==" in t and "== Initial Plan ==" in t for t in after_run.values()), str(after))
    check("over Spark Connect, explain() shows the starting plan before and after line 5 runs (slow 2, fast 1, kept 1)",
          connect["before"] == connect["after"] == [2, 1, 1], str(connect))

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
    clinics_n = len(answers[1])
    check("at scale, partial counts are one per clinic per pile: fast's 8 piles x 5 clinics, slow's 200 x 5",
          B["fast"][0]["rows"] == B["fast"][0]["tasks"] * clinics_n == 40
          and B["slow"][1]["rows"] == B["slow"][1]["tasks"] * clinics_n == 1000, str((B["fast"], B["slow"][1])))
    finer = {"code": FINER_THEN_COARSER, "without": crossings("finer.without"), "with_deal_up_front": crossings("finer.with")}
    check("a deal up front can save a wall yet send every row: groupBy(clinic, x), then a window over clinic",
          len(finer["without"]) == 2 and len(finer["with_deal_up_front"]) == 1
          and finer["with_deal_up_front"][0]["rows"] == PATIENTS * SCALE
          and sum(x["rows"] for x in finer["without"]) * 1000 < PATIENTS * SCALE, str(finer))
    total = lambda xs: sum(x["bytes"] for x in xs)
    check("at scale, fast moves the fewest bytes, then kept, then slow",
          total(B["fast"]) < total(B["kept"]) < total(B["slow"]), str({k: total(v) for k, v in B.items()}))

    result = {
        "series": "Spark, Visually", "part": "II · Why jobs are slow", "episode": 7, "title": "Counting Walls",
        "engine": {"spark": SPARK_PIN, "master": MASTER, "cpus": os.cpu_count(), "settings": settings,
                   "note": "local mode: exact plans, walls, jobs, stages, rows and bytes; no durations"},
        "code_on_screen": CODE_ON_SCREEN, "cast": {"patients": PATIENTS, "files": TRAYS, "seed": SEED},
        "answers": answers[1], "narrow": narrow, "wide": wide, "sort_within": sortw, "union_sql": unions,
        "piles": pile_sizes, "follow": follow, "cached": cached, "finer_then_coarser": finer,
        "explain_after_run": {"classic": {"exchange_lines": after, "slow": after_run["slow"]}, "connect": connect},
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
