"""Spark, Visually · Part I · Episode 1: Who Does the Work.

Every number the episode shows or says is computed here, checked, and written to e01_numbers.json.
Scenes and narration read e01_numbers.json; nothing numeric is typed into scene code or the script.

    python e01_numbers.py        # runs Spark (classic, then Spark Connect), prints the ledger, writes e01_numbers.json

Rules kept here (blueprint, "Rules Spark adds"):
  * Code on screen runs: the code-panel lines in CODE_ON_SCREEN are executed verbatim (exec / eval), in classic
    Spark and again through Spark Connect.
  * One pinned engine: the Spark version, master and every setting used are written into the JSON.
  * Exact where exact: tasks, rows and bytes come from Spark's event log, never from estimates.

The home shot against this run. The home shot draws four executors with two cores each; local[8] gives the same
eight task slots in one process, so tasks, rows and bytes are exact, but nothing crosses a network.
"""
from __future__ import annotations

import gc
import json
import re
import shutil
import subprocess
import sys
import tracemalloc
from pathlib import Path

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE.parents[1]))      # the series root, for the shared sv package
from sv.cast import CAST, NAMES, PATIENTS, READMITTED, SEED, TRAYS, write_cast  # noqa: E402
from sv.runlog import MASTER, SPARK_PIN, parse_events, plan_text, session, stage_of  # noqa: E402

WORK = Path("/tmp/sv_e01")
EVENTS = WORK / "events"
CAST_DIR = WORK / "cast"
SCALE = 10_000                 # S07: the cast, ten thousand times over
CONNECT_MASTER = "local[4]"    # the Spark Connect run starts its own local server

# The code panel, in the order its lines glow, run verbatim: statements with exec, expressions with eval.
CODE_ON_SCREEN = [
    'df = spark.read.table("cast")',            # S05: nothing runs yet
    'df.count()',                               # S06: four tasks count, one final task adds them
    'rows = df.collect()',                      # S07: every row travels to the driver
    'flip = F.udf(lambda y: 1 - y, "int")',     # S08: a Python function
    'df.select(F.sum(flip("y"))).collect()',    # S08: rows go out to Python workers and back (a count would prune it)
]

CHECKS: list[tuple[str, bool, str]] = []


def check(name: str, ok: bool, detail: str = "") -> None:
    CHECKS.append((name, bool(ok), detail))


# The Spark Connect run: a fresh Python process, because a local Connect server cannot share a process with a
# classic session. It runs the same code panel and reports what came back.
CONNECT_RUN = r'''
import json, sys
from pyspark.sql import SparkSession, functions as F
spark = SparkSession.builder.remote(sys.argv[1]).getOrCreate()
spark.read.parquet(sys.argv[2]).createOrReplaceTempView("cast")
code = json.loads(sys.argv[3])
ns = {"spark": spark, "F": F}
exec(code[0], ns)
out = {"mode": type(spark).__module__, "count": eval(code[1], ns)}
exec(code[2], ns)
out["collect_rows"] = len(ns["rows"])
exec(code[3], ns)
out["udf_sum"] = eval(code[4], ns)[0][0]
try:
    ns["df"].rdd
    out["rdd"] = "available"
except Exception as e:
    out["rdd"] = f"{type(e).__name__}: {str(e).splitlines()[0]}"
try:
    spark.sparkContext
    out["sparkContext"] = "available"
except Exception as e:
    out["sparkContext"] = f"{type(e).__name__}: {str(e).splitlines()[0].split(' If ')[0]}"
spark.stop()
print("CONNECT_RESULT " + json.dumps(out))
'''


def python_bytes(action):
    """Run an action that returns rows into Python, and measure the Python memory those rows hold afterwards."""
    gc.collect()
    tracemalloc.start()
    before = tracemalloc.get_traced_memory()[0]
    rows = action()
    held = tracemalloc.get_traced_memory()[0] - before
    tracemalloc.stop()
    return rows, held


def run_connect() -> dict:
    r = subprocess.run([sys.executable, "-c", CONNECT_RUN, CONNECT_MASTER, str(CAST_DIR), json.dumps(CODE_ON_SCREEN)],
                       capture_output=True, text=True, timeout=240)
    line = next((ln for ln in r.stdout.splitlines() if ln.startswith("CONNECT_RESULT ")), None)
    assert line, f"Spark Connect run failed:\n{r.stdout[-2000:]}\n{r.stderr[-2000:]}"
    return json.loads(line[len("CONNECT_RESULT "):])


def main() -> int:
    shutil.rmtree(WORK, ignore_errors=True)
    EVENTS.mkdir(parents=True)

    import pyspark
    from pyspark.sql import functions as F

    spark = session("spark-visually-e01", EVENTS)
    sc = spark.sparkContext
    check(f"pinned engine: Apache Spark {SPARK_PIN}", pyspark.__version__ == SPARK_PIN, pyspark.__version__)
    slots = sc.defaultParallelism
    check("eight task slots, as on the home shot (four executors, two cores each)", slots == 8, str(slots))

    def tagged(tag: str, action):
        sc.setJobDescription(tag)
        try:
            return action()
        finally:
            sc.setJobDescription(None)

    # The cast, written as four files, registered as the table the code panel reads.
    tagged("setup.write", lambda: write_cast(spark, str(CAST_DIR)))
    tagged("setup.view", lambda: spark.read.parquet(str(CAST_DIR)).createOrReplaceTempView("cast"))
    n_files = len(list(CAST_DIR.glob("part-*.parquet")))
    check("the cast is written as four files", n_files == TRAYS, f"{n_files} files")

    # The code panel, line by line.
    ns = {"spark": spark, "F": F}
    tagged("line1.read", lambda: exec(CODE_ON_SCREEN[0], ns))
    df = ns["df"]
    tray_rows = tagged("setup.trays", lambda: df.withColumn("tray", F.spark_partition_id())
                       .groupBy("tray", "clinic").count().collect())
    trays = {t: {k: 0 for k in NAMES} for t in range(TRAYS)}
    for r in tray_rows:
        trays[r["tray"]][r["clinic"]] = r["count"]
    tray_sizes = [sum(trays[t].values()) for t in range(TRAYS)]
    check("the four files read as four trays holding all 103 patients", sum(tray_sizes) == PATIENTS and len(tray_sizes) == TRAYS,
          str(tray_sizes))

    total = tagged("line2.count", lambda: eval(CODE_ON_SCREEN[1], ns))
    check("df.count() is 103", total == PATIENTS, str(total))
    _, cast_py = python_bytes(lambda: tagged("line3.collect", lambda: exec(CODE_ON_SCREEN[2], ns)) or ns["rows"])
    check("collect() brings all 103 rows to the driver", len(ns["rows"]) == PATIENTS, str(len(ns["rows"])))
    tagged("line4.udf_def", lambda: exec(CODE_ON_SCREEN[3], ns))
    udf_sum = tagged("line5.udf", lambda: eval(CODE_ON_SCREEN[4], ns))[0][0]
    readmitted = READMITTED
    check("the UDF line adds 1 - y over every patient: the patients not readmitted", udf_sum == PATIENTS - readmitted,
          f"{udf_sum} = {PATIENTS} - {readmitted}")
    # Why line 5 sums: a UDF whose value nothing uses is pruned before the tasks are planned. A function that always
    # fails shows it: in a count it never runs; in the line on screen it does.
    boom = F.udf(lambda y: 1 // 0, "int")
    pruned = tagged("probe.count", lambda: df.select(boom("y")).count()) == PATIENTS
    try:
        tagged("probe.sum", lambda: df.select(F.sum(boom("y"))).collect())
        runs = False
    except Exception as e:                                    # PythonException: the UDF ran in a Python worker
        runs = "PythonException" in type(e).__name__ or "ZeroDivisionError" in str(e)
    check("a UDF whose value a count doesn't use never runs (so line 5 sums instead)", pruned)
    check("in the line on screen the UDF does run, in Python workers", runs)
    plans = {"count": plan_text(df.groupBy().count()), "udf": plan_text(df.select(F.sum(ns["flip"]("y")))),
             "collect": plan_text(df)}
    py_node = re.search(r"\b(\w*EvalPython)\b", plans["udf"])
    check("the UDF's plan sends rows out to Python (an EvalPython node)", py_node is not None,
          py_node.group(1) if py_node else "none")
    py_node = py_node.group(1) if py_node else None
    check("the plain count's plan has no Python in it", "Python" not in plans["count"])

    # Scale: the cast, ten thousand times over, each copy of a patient with its own id (so the rows are not
    # ten thousand identical copies, which would compress unrealistically well on their way to the driver).
    big = df.crossJoin(spark.range(SCALE)).select((F.col("patient") * SCALE + F.col("id")).cast("int").alias("patient"),
                                                 "clinic", "x", "y")
    big_count = tagged("scale.count", lambda: big.count())
    check("the scaled cast has 1,030,000 rows", big_count == PATIENTS * SCALE, str(big_count))
    big_rows, big_py = python_bytes(lambda: tagged("scale.collect", lambda: big.collect()))
    n_big_rows = len(big_rows)
    check("the scaled collect brings every row into the driver's Python", n_big_rows == PATIENTS * SCALE, str(n_big_rows))
    del big_rows
    gc.collect()
    settings = {k: spark.conf.get(k) for k in ("spark.sql.adaptive.enabled", "spark.sql.files.maxPartitionBytes",
                                               "spark.sql.files.openCostInBytes", "spark.sql.autoBroadcastJoinThreshold",
                                               "spark.sql.execution.pythonUDF.arrow.enabled")}
    spark.stop()

    # ------------------------------------------------------------------------- the event log
    ev = parse_events(EVENTS)
    check("reading the table runs no job", not ev.get("line1.read"), str([s["stage"] for s in ev.get("line1.read", [])]))
    check("defining the UDF runs no job", "line4.udf_def" not in ev)

    def to_driver(tag: str) -> int:
        """Every byte the tag's tasks sent back to the driver as task results (rows, numbers, status reports)."""
        return sum(t["result_bytes"] for s in ev[tag] for t in s["tasks"])

    cnt = ev["line2.count"]
    cmap, cfin = stage_of(cnt, "map"), stage_of(cnt, "reduce")
    rows_per_task = [t["input_records"] for t in cmap["tasks"]]
    check("count: four tasks, one per file", len(cmap["tasks"]) == TRAYS, str(len(cmap["tasks"])))
    check("count: each task reads its own tray from storage", rows_per_task == tray_sizes, f"{rows_per_task} vs {tray_sizes}")
    partials = [t["write_records"] for t in cmap["tasks"]]
    check("count: each task passes on one partial count", partials == [1] * TRAYS, str(partials))
    check("count: one final task reads the four partial counts", len(cfin["tasks"]) == 1 and cfin["tasks"][0]["read_records"] == TRAYS,
          f"{len(cfin['tasks'])} task(s), {cfin['tasks'][0]['read_records']} records")
    count_jobs = sorted({s["job"] for s in cnt})

    col = ev["line3.collect"]
    col_scan = stage_of(col, "scan")
    check("collect: four tasks read the same four trays", [t["input_records"] for t in col_scan["tasks"]] == tray_sizes)

    udf_stages = ev["line5.udf"]
    udf_map = stage_of(udf_stages, "map")
    check("the UDF count reads the same four trays", [t["input_records"] for t in udf_map["tasks"]] == tray_sizes)

    # Bytes on the wire are task results: every task reports back (status, metrics), and collected rows travel
    # compressed. Kept for reference; the episode speaks of rows, and of the memory the rows take in Python.
    scale = {"factor": SCALE, "rows": big_count, "collect_python_bytes": big_py,
             "count_bytes_to_driver": to_driver("scale.count"), "collect_bytes_to_driver": to_driver("scale.collect"),
             "count_tasks": sum(len(s["tasks"]) for s in ev["scale.count"]),
             "collect_tasks": sum(len(s["tasks"]) for s in ev["scale.collect"])}
    cast_bytes = {"collect_python_bytes": cast_py,
                  "count_bytes_to_driver": to_driver("line2.count"), "collect_bytes_to_driver": to_driver("line3.collect")}
    per_row = (cast_py / PATIENTS, big_py / big_count)
    check("collected rows cost about the same Python memory each, at both sizes", 0.5 < per_row[1] / per_row[0] < 2.0,
          f"{per_row[0]:.0f} and {per_row[1]:.0f} bytes per row")
    check("at scale, the collected rows take hundreds of megabytes in the driver's Python", big_py > 100 * 2 ** 20,
          f"{big_py / 2 ** 20:.0f} MiB")
    scale["collect_python_megabytes"] = round(big_py / 1e6)

    # ------------------------------------------------------------------------- the same code through Spark Connect
    connect = run_connect()
    check("Spark Connect: the same code panel gives the same count", connect["count"] == PATIENTS, str(connect["count"]))
    check("Spark Connect: collect brings the same 103 rows", connect["collect_rows"] == PATIENTS)
    check("Spark Connect: the UDF line runs too", connect["udf_sum"] == udf_sum, str(connect["udf_sum"]))
    check("Spark Connect: df.rdd is not available", connect["rdd"].startswith("PySparkNotImplementedError"), connect["rdd"])
    check("Spark Connect: the session is a client (no SparkContext)", connect["sparkContext"] != "available", connect["sparkContext"])

    result = {
        "series": "Spark, Visually", "part": "I · How Spark runs a job", "episode": 1, "title": "Who Does the Work",
        "engine": {"spark": SPARK_PIN, "master": MASTER, "connect_master": CONNECT_MASTER, "settings": settings,
                   "note": "local mode: exact tasks, rows and bytes; no network"},
        "code_on_screen": CODE_ON_SCREEN,
        "cast": {"patients": PATIENTS, "readmitted": READMITTED, "files": n_files, "seed": SEED},
        "trays": {str(t): trays[t] for t in range(TRAYS)}, "tray_sizes": tray_sizes,
        "slots": slots,
        "count": {"result": total, "jobs": len(count_jobs), "tasks": len(cmap["tasks"]), "rows_per_task": rows_per_task,
                  "partials_written": sum(partials), "final_tasks": len(cfin["tasks"]),
                  "final_task_reads": cfin["tasks"][0]["read_records"]},
        "collect": {"rows": len(ns["rows"]), "tasks": len(col_scan["tasks"])},
        "bytes_to_driver": {"cast": cast_bytes, "scaled": scale},
        "udf": {"result": udf_sum, "pruned_in_a_count": pruned, "runs_in_line5": runs, "plan_node": py_node, "tasks": len(udf_map["tasks"]),
                "arrow": settings["spark.sql.execution.pythonUDF.arrow.enabled"]},
        "connect": connect,
        "plans": plans,
        "checks": [{"name": n, "ok": ok, "detail": d} for n, ok, d in CHECKS],
    }
    (HERE / "e01_numbers.json").write_text(json.dumps(result, indent=1, ensure_ascii=False) + "\n")

    # ------------------------------------------------------------------------- ledger
    print("Trays:", tray_sizes, "| slots:", slots)
    print("count:", result["count"])
    print("bytes to the driver, cast:", cast_bytes, "| scaled x", SCALE, ":", scale)
    print("udf:", result["udf"], "| connect:", connect)
    failed = [c for c in CHECKS if not c[1]]
    for n, ok, d in CHECKS:
        print(("PASS " if ok else "FAIL ") + n + (f"  [{d}]" if d else ""))
    print(f"{len(CHECKS) - len(failed)} of {len(CHECKS)} checks pass")
    return 1 if failed else 0


if __name__ == "__main__":
    sys.exit(main())
