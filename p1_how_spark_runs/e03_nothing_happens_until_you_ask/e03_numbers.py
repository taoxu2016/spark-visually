"""Spark, Visually · Part I · Episode 3: Nothing Happens Until You Ask.

Every number the episode shows or says is computed here, checked, and written to e03_numbers.json.

    python e03_numbers.py        # runs Spark, prints the ledger, writes e03_numbers.json

Rules kept here (blueprint, "Rules Spark adds"):
  * Code on screen runs: the code-panel lines in CODE_ON_SCREEN are executed verbatim, on the cast and on the cast
    ten thousand times over (for the timing hook).
  * Plans come from Spark: the plan after each line is read from Spark's own query execution.
  * Exact where exact: jobs, stages, tasks and rows come from the event log. The hook is told in rows read per line,
    which are exact anywhere; its wall-clock times on this machine are kept in the JSON for reference, not said.
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
import time
from pathlib import Path

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE.parents[1]))      # the series root, for the shared sv package
from sv.cast import NAMES, PATIENTS, SEED, TRAYS, cast_rows, write_cast  # noqa: E402
from sv.runlog import MASTER, SPARK_PIN, parse_events, plan_text, session, stage_of  # noqa: E402

WORK = Path("/tmp/sv_e03")
EVENTS = WORK / "events"
SCALE = 10_000                 # the hook: the cast ten thousand times over, about a million rows

CODE_ON_SCREEN = [
    'hi = df.filter(F.col("x") == 1)',                       # S03: the plan grows; nothing runs
    'hi = hi.withColumn("readmitted", F.col("y") == 1)',     # S03
    'hi = hi.select("clinic", "readmitted")',                # S03
    'hi.count()',                                           # S04: one job's worth of work, one pass per pile
    'hi.show()',                                            # S05: the whole recipe runs again
]

CONNECT_CSV = r'''
import json, sys, urllib.request
from pyspark.sql import SparkSession
spark = SparkSession.builder.remote("local[4]").getOrCreate()
def jobs():
    for port in range(4040, 4046):
        try:
            apps = json.load(urllib.request.urlopen(f"http://localhost:{port}/api/v1/applications", timeout=3))
            return len(json.load(urllib.request.urlopen(f"http://localhost:{port}/api/v1/applications/{apps[0]['id']}/jobs", timeout=3)))
        except Exception:
            continue
    raise SystemExit("no Spark UI found for the Connect server")
before = jobs()
df = spark.read.option("header", True).option("inferSchema", True).csv(sys.argv[1])
after_read = jobs()
df.columns
after_columns = jobs()
spark.stop()
print("CONNECT_RESULT " + json.dumps({"before": before, "after_read": after_read, "after_columns": after_columns}))
'''

CHECKS: list[tuple[str, bool, str]] = []


def check(name: str, ok: bool, detail: str = "") -> None:
    CHECKS.append((name, bool(ok), detail))


def run_line(line: str, ns: dict):
    """A code-panel line: statements (with =) by exec, expressions by eval; returns the expression's value."""
    if re.match(r"^\w+\s*=", line):
        exec(line, ns)
        return ns[line.split("=")[0].strip()]
    return eval(line, ns)


def operators(tree: str) -> list[str]:
    """The operator names in a plan's tree string, top first."""
    out = []
    for ln in tree.strip().splitlines():
        m = re.match(r"^[\s:+\-|]*(\*\(\d+\)\s*)?([A-Za-z]+)", ln)
        if m:
            out.append(m.group(2))
    return out


def main() -> int:
    shutil.rmtree(WORK, ignore_errors=True)
    EVENTS.mkdir(parents=True)

    import pyspark
    from pyspark.sql import functions as F
    from pyspark.sql.types import IntegerType, StringType, StructField, StructType

    spark = session("spark-visually-e03", EVENTS)
    sc = spark.sparkContext
    check(f"pinned engine: Apache Spark {SPARK_PIN}", pyspark.__version__ == SPARK_PIN, pyspark.__version__)
    schema = StructType([StructField("patient", IntegerType()), StructField("clinic", StringType()),
                         StructField("x", IntegerType()), StructField("y", IntegerType())])

    def tagged(tag: str, action):
        sc.setJobDescription(tag)
        try:
            return action()
        finally:
            sc.setJobDescription(None)

    # ---- the cast, four files, as in every episode
    four = WORK / "cast"
    tagged("setup.four", lambda: write_cast(spark, str(four)))
    df = spark.read.parquet(str(four))
    high = sum(1 for _, _, x, _ in cast_rows() if x == 1)
    trays = {i: {k: 0 for k in NAMES} for i in range(TRAYS)}
    for r in df.withColumn("pile", F.spark_partition_id()).groupBy("pile", "clinic").count().collect():
        trays[r["pile"]][r["clinic"]] = r["count"]

    # ---- the code panel, line by line, with the plan after each transformation
    ns = {"df": df, "F": F}
    results, analyzed = [], []
    for k, line in enumerate(CODE_ON_SCREEN[:3]):
        out = tagged(f"line{k + 1}", lambda line=line: run_line(line, ns))
        results.append(type(out).__name__)
        analyzed.append(operators(out._jdf.queryExecution().analyzed().toString()))
    hi = ns["hi"]
    optimized = operators(hi._jdf.queryExecution().optimizedPlan().toString())
    physical = plan_text(hi, "simple")
    count_plan = plan_text(hi.groupBy().count(), "simple")        # what count() runs
    show_plan = plan_text(hi.limit(21), "simple")                 # what show() runs: the first 21 rows
    count = tagged("line4", lambda: run_line(CODE_ON_SCREEN[3], ns))
    results.append(type(count).__name__)
    shown = io.StringIO()
    with contextlib.redirect_stdout(shown):
        out5 = tagged("line5", lambda: run_line(CODE_ON_SCREEN[4], ns))
    results.append(type(out5).__name__)
    check("the count is the cast's higher-risk patients", count == high, f"{count} of {PATIENTS}")
    check("each transformation adds one step to the plan", [len(a) for a in analyzed] == [2, 3, 4], str(analyzed))

    # ---- the hook: the same four lines on the cast ten thousand times over, timed (on this machine)
    big = df.crossJoin(spark.range(SCALE)).select((F.col("patient") * SCALE + F.col("id")).cast("int").alias("patient"),
                                                 "clinic", "x", "y")
    scaled = WORK / "cast_scaled"
    tagged("setup.scaled", lambda: big.write.parquet(str(scaled)))
    times = []
    for rep in range(2):                                  # the first pass warms up; the second is kept
        nb = {"df": spark.read.parquet(str(scaled)), "F": F}
        row = []
        for k, line in enumerate(CODE_ON_SCREEN[:4]):
            t0 = time.perf_counter()
            tagged(f"hook{rep}.line{k + 1}", lambda line=line: run_line(line, nb))
            row.append(time.perf_counter() - t0)
        times = row
    big_rows = PATIENTS * SCALE

    # ---- why wait: the filter moves into the read (the cast written sorted by risk)
    srt = WORK / "cast_by_risk"
    rows_sorted = sorted(cast_rows(), key=lambda r: (r[2], r[0]))
    tagged("setup.sorted", lambda: spark.createDataFrame(sc.parallelize(rows_sorted, TRAYS), schema).write.parquet(str(srt)))
    dfs = spark.read.parquet(str(srt))
    q_push = dfs.filter(F.col("x") == 1)
    pushed = re.search(r"PushedFilters: \[([^\]]*)\]", plan_text(q_push, "simple"))
    spark.conf.set("spark.sql.parquet.filterPushdown", "false")
    tagged("push.off", lambda: dfs.filter(F.col("x") == 1).count())
    spark.conf.set("spark.sql.parquet.filterPushdown", "true")
    sorted_count = tagged("push.on", lambda: dfs.filter(F.col("x") == 1).count())
    per_file = {r["f"]: (r["rows"], r["n"]) for r in dfs.withColumn("f", F.spark_partition_id()).groupBy("f")
                .agg(F.count("*").alias("rows"), F.sum("x").alias("n")).collect()}
    sorted_files = [{"rows": per_file[f][0], "higher_risk": per_file[f][1]} for f in sorted(per_file)]
    file_highs = sorted(v[1] for v in per_file.values())

    # ---- the sorting card: what each kind of line hands back
    collected = df.limit(3).collect()
    written = df.write.mode("overwrite").parquet(str(WORK / "written"))
    kinds = {"transformation": results[:3], "count": results[3], "show": results[4],
             "collect": type(collected).__name__, "write": type(written).__name__}

    # ---- the exception: CSV with schema inference reads at read time
    csvp = WORK / "cast_csv"
    tagged("setup.csv", lambda: df.write.option("header", True).csv(str(csvp)))
    tagged("csv.infer", lambda: spark.read.option("header", True).option("inferSchema", True).csv(str(csvp)))
    tagged("csv.schema", lambda: spark.read.option("header", True).schema(schema).csv(str(csvp)))
    # Reads by path also look at the files: Parquet and JSON each run a job to learn the schema (a catalog table,
    # as in this series' code, runs none).
    jsonp = WORK / "cast_json"
    tagged("setup.json", lambda: df.write.json(str(jsonp)))
    tagged("read.parquet", lambda: spark.read.parquet(str(four)))
    tagged("read.json", lambda: spark.read.json(str(jsonp)))

    # ---- even cache waits for an action
    tagged("cache", lambda: hi.cache())
    tagged("cache.fill", lambda: hi.count())
    settings = {k: spark.conf.get(k) for k in ("spark.sql.adaptive.enabled", "spark.sql.parquet.filterPushdown",
                                               "spark.sql.codegen.wholeStage")}
    spark.stop()

    # ---- the same CSV read over Spark Connect: the inference jobs wait until the schema is first needed. A fresh
    # process starts a local Connect server; its own Spark UI counts the server's jobs.
    r = subprocess.run([sys.executable, "-c", CONNECT_CSV, str(csvp)], capture_output=True, text=True, timeout=240)
    line = next((ln for ln in r.stdout.splitlines() if ln.startswith("CONNECT_RESULT ")), None)
    assert line, f"Spark Connect run failed:\n{r.stdout[-1500:]}\n{r.stderr[-1500:]}"
    connect_csv = json.loads(line[len("CONNECT_RESULT "):])

    # ------------------------------------------------------------------------- the event log
    ev = parse_events(EVENTS)
    for k in (1, 2, 3):
        check(f"line {k} runs no job", f"line{k}" not in ev)
    l4 = ev["line4"]
    scan = stage_of(l4, "map")
    rows_read = [t["input_records"] for t in scan["tasks"]]
    check("line 4: four tasks, one per file, each reading its file once", len(scan["tasks"]) == TRAYS
          and sum(rows_read) == PATIENTS, str(rows_read))
    jobs4, stages4 = len({s["job"] for s in l4}), len(l4)
    check("line 4 runs as two jobs and two stages", jobs4 == 2 and stages4 == 2, f"{jobs4} jobs, {stages4} stages")
    fused = [m.group(1) for m in re.finditer(r"\*\((\d+)\) (Project|Filter)", physical)]
    check("filter, new column and select run as one fused pass", len(fused) >= 2 and len(set(fused)) == 1, str(fused))
    check("the two Projects collapse into one before it runs", optimized.count("Project") == 1 and analyzed[2].count("Project") == 2,
          f"{analyzed[2]} -> {optimized}")
    count_reads = re.search(r"ReadSchema: struct<([^>]*)>", count_plan)
    check("the count never computes the new column, and reads only the column it filters on",
          "readmitted" not in count_plan and count_reads is not None and count_reads.group(1) == "x:int",
          count_reads.group(1) if count_reads else "")
    show_fused = [m.group(1) for m in re.finditer(r"\*\((\d+)\) (Project|Filter)", show_plan)]
    check("show runs all three steps, as one fused pass", "readmitted" in show_plan and len(show_fused) >= 2 and len(set(show_fused)) == 1,
          str(show_fused))
    l5 = ev["line5"]
    show_read = sum(t["input_records"] for s in l5 for t in s["tasks"])
    check("line 5 runs its own job and reads the files again", len(l5) >= 1 and show_read > 0, f"{show_read} rows read")
    hook_jobs = {k: (f"hook1.line{k}" in ev) for k in (1, 2, 3, 4)}
    check("in the hook, only the fourth line runs a job", hook_jobs == {1: False, 2: False, 3: False, 4: True}, str(hook_jobs))
    hook_read = sum(t["input_records"] for s in ev["hook1.line4"] for t in s["tasks"])
    check("the hook's count reads every row", hook_read == big_rows, f"{hook_read:,}")
    check("the count takes longer than any of the transformations (times are kept, not said)", times[3] > max(times[:3]),
          f"{[round(t * 1000, 1) for t in times]} ms")
    hook_rows = [0 if not hook_jobs[k] else sum(t["input_records"] for s in ev[f"hook1.line{k}"] for t in s["tasks"]) for k in (1, 2, 3, 4)]

    off_read = sum(t["input_records"] for s in ev["push.off"] for t in s["tasks"])
    on_read = sum(t["input_records"] for s in ev["push.on"] for t in s["tasks"])
    check("the pushed filter is in the scan", pushed is not None and "EqualTo(x,1)" in pushed.group(1), pushed.group(1) if pushed else "")
    check("with the files sorted by risk, the pushed filter skips files", on_read < off_read == PATIENTS and sorted_count == high,
          f"{on_read} rows read instead of {off_read}")
    check("transformations hand back a DataFrame; count a number; show nothing (it prints)",
          kinds["transformation"] == ["DataFrame"] * 3 and kinds["count"] == "int" and kinds["show"] == "NoneType")
    csv_jobs = len({s["job"] for s in ev.get("csv.infer", [])})
    parquet_jobs = len({s["job"] for s in ev.get("read.parquet", [])})
    json_jobs = len({s["job"] for s in ev.get("read.json", [])})
    check("reading Parquet or JSON by path runs one job each, to learn the schema", parquet_jobs == 1 and json_jobs == 1,
          f"parquet {parquet_jobs}, json {json_jobs}")
    check("over Spark Connect the CSV read runs no job, and the inference jobs run when the schema is first needed",
          connect_csv["after_read"] == connect_csv["before"] and connect_csv["after_columns"] - connect_csv["after_read"] == csv_jobs,
          str(connect_csv))
    by_job = {}
    for st in l5:
        b = by_job.setdefault(st["job"], {"tasks": 0, "rows": 0})
        b["tasks"] += len(st["tasks"]); b["rows"] += sum(t["input_records"] for t in st["tasks"])
    show_jobs = [by_job[j] for j in sorted(by_job)]
    check("show() reads one file at a time, in growing batches: two jobs of one task", [b["tasks"] for b in show_jobs] == [1, 1],
          str(show_jobs))
    scan_on = max(ev["push.on"], key=lambda st: sum(t["input_records"] for t in st["tasks"]))
    push_rows = [t["input_records"] for t in sorted(scan_on["tasks"], key=lambda t: t["index"])]
    check("with pushdown all four tasks still run; the files with no higher-risk patient give them nothing to read",
          len(push_rows) == TRAYS and push_rows.count(0) == 2 and sum(push_rows) == on_read, str(push_rows))
    check("reading CSV with schema inference runs jobs at read time", csv_jobs >= 1, f"{csv_jobs} jobs")
    check("reading CSV with a schema runs no job", "csv.schema" not in ev)
    check("cache() runs no job by itself", "cache" not in ev)

    result = {
        "series": "Spark, Visually", "part": "I · How Spark runs a job", "episode": 3, "title": "Nothing Happens Until You Ask",
        "engine": {"spark": SPARK_PIN, "master": MASTER, "cpus": os.cpu_count(), "settings": settings,
                   "note": "local mode: exact plans, jobs, tasks and rows; the hook's times are wall-clock on this machine"},
        "code_on_screen": CODE_ON_SCREEN,
        "cast": {"patients": PATIENTS, "higher_risk": high, "seed": SEED},
        "plans": {"analyzed_after_each_line": analyzed, "optimized": optimized, "physical": physical, "count": count_plan,
                  "show": show_plan, "count_reads": count_reads.group(1) if count_reads else None},
        "count": {"result": count, "jobs": jobs4, "stages": stages4, "tasks": len(scan["tasks"]), "rows_read_per_task": rows_read},
        "show": {"jobs": len({s["job"] for s in l5}), "rows_read": show_read, "printed": shown.getvalue()},
        "hook": {"rows": big_rows, "scale": SCALE, "rows_read_per_line": hook_rows, "seconds_per_line": [round(t, 4) for t in times],
                 "ms_lines_1_3": [round(t * 1000, 1) for t in times[:3]], "seconds_line_4": round(times[3], 2)},
        "pushdown": {"pushed_filters": pushed.group(1) if pushed else None, "rows_read_without": off_read, "rows_read_with": on_read,
                     "higher_risk_per_file": file_highs, "sorted_files": sorted_files},
        "kinds": kinds,
        "trays": {str(i): trays[i] for i in range(TRAYS)},
        "csv": {"jobs_with_inference": csv_jobs, "jobs_with_schema": 0 if "csv.schema" not in ev else None,
                "parquet_by_path_jobs": parquet_jobs, "json_by_path_jobs": json_jobs,
                "connect": {"jobs_after_read": connect_csv["after_read"] - connect_csv["before"],
                            "jobs_when_schema_needed": connect_csv["after_columns"] - connect_csv["after_read"]}},
        "show_jobs": show_jobs, "pushdown_rows_per_task": push_rows,
        "cache_jobs": 0 if "cache" not in ev else None,
        "checks": [{"name": n, "ok": ok, "detail": d} for n, ok, d in CHECKS],
    }
    (HERE / "e03_numbers.json").write_text(json.dumps(result, indent=1, ensure_ascii=False) + "\n")
    for k in ("cast", "count", "hook", "pushdown", "kinds", "csv"):
        print(f"{k}: {result[k]}")
    print("plans:", analyzed, "->", optimized)
    failed = [c for c in CHECKS if not c[1]]
    for n, ok, d in CHECKS:
        print(("PASS " if ok else "FAIL ") + n + (f"  [{d}]" if d else ""))
    print(f"{len(CHECKS) - len(failed)} of {len(CHECKS)} checks pass")
    return 1 if failed else 0


if __name__ == "__main__":
    sys.exit(main())
