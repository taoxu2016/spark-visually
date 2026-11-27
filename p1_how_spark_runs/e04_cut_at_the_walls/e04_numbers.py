"""Spark, Visually · Part I · Episode 4: Cut at the Walls.

Every number the episode shows or says is computed here, checked, and written to e04_numbers.json.

    python e04_numbers.py        # runs Spark, prints the ledger, writes e04_numbers.json

Rules kept here (blueprint, "Rules Spark adds"):
  * Code on screen runs: the code-panel lines in CODE_ON_SCREEN are executed verbatim.
  * Plans come from Spark: explain() is captured as printed; stage boundaries are read from the plan's Exchanges.
  * Exact where exact: jobs, stages (run and skipped), tasks and their order in time come from the event log.
    Order in time (a stage starts after the last task before its wall) holds on any machine; durations are not used.
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
from sv.cast import CAST, NAMES, PATIENTS, SEED, TRAYS, cast_rows, write_cast  # noqa: E402
from sv.runlog import MASTER, SPARK_PIN, parse_events, parse_jobs, session  # noqa: E402

WORK = Path("/tmp/sv_e04")
EVENTS = WORK / "events"

CODE_ON_SCREEN = [
    'hi = df.filter(F.col("x") == 1)',              # S03: Episode 3's higher-risk patients
    'per_clinic = hi.groupBy("clinic").count()',    # S03: a count per clinic: the plan gains an exchange
    'per_clinic.collect()',                         # S05: the action: stage 1, a wall, stage 2
    'per_clinic.explain()',                         # S06: the plan as it ran: the Exchange is the wall
    'df.join(clinics.hint("merge"), "clinic").collect()',   # S08: one line, three stages (the hint skips the broadcast)
]

# S07's settings card: adaptive execution off, and the same count as a new DataFrame (a DataFrame that has run keeps
# its plan, so the setting would not reach per_clinic). Run verbatim, like the code panel.
CARD_AQE_OFF = [
    'spark.conf.set("spark.sql.adaptive.enabled", False)',
    'hi.groupBy("clinic").count().collect()',
]

# Spark Connect, which some Databricks compute uses: the same lines in a fresh process with a local Connect server.
CONNECT_RUN = r'''
import contextlib, io, json, sys
from pyspark.sql import SparkSession, functions as F
spark = SparkSession.builder.remote("local[4]").getOrCreate()
code = json.loads(sys.argv[2])
ns = {"df": spark.read.parquet(sys.argv[1]), "F": F}
exec(code[0], ns); exec(code[1], ns); eval(code[2], ns)
buf = io.StringIO()
with contextlib.redirect_stdout(buf):
    eval(code[3], ns)
mode = type(spark).__module__
spark.stop()
print("CONNECT_RESULT " + json.dumps({"mode": mode, "explain_after_run": buf.getvalue()}))
'''

CHECKS: list[tuple[str, bool, str]] = []


def check(name: str, ok: bool, detail: str = "") -> None:
    CHECKS.append((name, bool(ok), detail))


def main() -> int:
    shutil.rmtree(WORK, ignore_errors=True)
    EVENTS.mkdir(parents=True)

    import pyspark
    from pyspark.sql import functions as F

    spark = session("spark-visually-e04", EVENTS)
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
    df = spark.read.parquet(str(four))
    ns = {"spark": spark, "df": df, "F": F}

    # ---- the code panel
    tagged("line1", lambda: exec(CODE_ON_SCREEN[0], ns))
    tagged("line2", lambda: exec(CODE_ON_SCREEN[1], ns))
    per_clinic = ns["per_clinic"]
    printed = io.StringIO()
    with contextlib.redirect_stdout(printed):
        per_clinic.explain()                                       # not on screen: the starting plan, before the run
    explain_text = printed.getvalue().strip()
    rows = tagged("main", lambda: eval(CODE_ON_SCREEN[2], ns))
    trays = {i: {k: 0 for k in NAMES} for i in range(TRAYS)}
    for r in tagged("setup.trays", lambda: df.withColumn("pile", F.spark_partition_id()).groupBy("pile", "clinic").count().collect()):
        trays[r["pile"]][r["clinic"]] = r["count"]
    hi_pairs = ns["hi"].select("clinic", F.spark_partition_id().alias("pile")).distinct()
    piles_per_clinic = {r["clinic"]: r["count"] for r in tagged("setup.pairs", lambda: hi_pairs.groupBy("clinic").count().collect())}
    after = io.StringIO()
    with contextlib.redirect_stdout(after):
        tagged("line4", lambda: eval(CODE_ON_SCREEN[3], ns))      # on screen: the plan as it ran
    counts = {r["clinic"]: r["count"] for r in rows}
    expect = {k: CAST[k][2] for k in NAMES if CAST[k][2]}
    check("the per-clinic counts of higher-risk patients", counts == expect, str(counts))

    # ---- the same action again: its first stage's shuffle files are reused (a skipped stage)
    tagged("again", lambda: eval(CODE_ON_SCREEN[2], ns))

    # ---- S07's card: adaptive execution off, one job, and the setting's 200 piles after the wall
    exec(CARD_AQE_OFF[0], ns)
    off_rows = tagged("aqe_off", lambda: eval(CARD_AQE_OFF[1], ns))
    spark.conf.set("spark.sql.adaptive.enabled", True)                 # back to the default (not on screen)

    # ---- the join: by default Spark broadcasts the small clinic table; the hint asks for the shuffle join instead.
    # The clinic table is stored like the cast, so Spark knows its size (a table built from a Python list has none,
    # and then Spark broadcasts the other side). Written after the main run, so the main run's stage numbers stay put.
    tagged("setup.clinics", lambda: spark.createDataFrame([(k, i + 1) for i, k in enumerate(NAMES)], "clinic string, card int")
           .coalesce(1).write.parquet(str(WORK / "clinics")))
    clinics = ns["clinics"] = spark.read.parquet(str(WORK / "clinics"))
    default_plan = df.join(clinics, "clinic")._jdf.queryExecution().executedPlan().toString()
    under_broadcast = default_plan.split("BroadcastExchange", 1)[1] if "BroadcastExchange" in default_plan else ""
    broadcast_scan = next((ln for ln in under_broadcast.splitlines() if "FileScan" in ln), "")
    joined = tagged("join", lambda: eval(CODE_ON_SCREEN[4], ns))
    check("the join keeps every patient", len(joined) == PATIENTS, str(len(joined)))
    settings = {k: spark.conf.get(k) for k in ("spark.sql.adaptive.enabled", "spark.sql.shuffle.partitions",
                                               "spark.sql.adaptive.coalescePartitions.enabled",
                                               "spark.sql.autoBroadcastJoinThreshold")}
    spark.stop()

    # ---- the same lines through Spark Connect: explain() after the run
    r = subprocess.run([sys.executable, "-c", CONNECT_RUN, str(four), json.dumps(CODE_ON_SCREEN)],
                       capture_output=True, text=True, timeout=240)
    line = next((ln for ln in r.stdout.splitlines() if ln.startswith("CONNECT_RESULT ")), None)
    assert line, f"Spark Connect run failed:\n{r.stdout[-1500:]}\n{r.stderr[-1500:]}"
    connect = json.loads(line[len("CONNECT_RESULT "):])
    check("the settings card's count gives the same answer", {r["clinic"]: r["count"] for r in off_rows} == counts)
    check("by default Spark broadcasts the clinic table (the shortcut the hint turns down)",
          "BroadcastHashJoin" in default_plan and "clinic#" in broadcast_scan and "card#" in broadcast_scan
          and "patient#" not in broadcast_scan, broadcast_scan.strip()[:70])
    check("through Spark Connect, explain() after the run still prints the starting plan, without codegen markers",
          connect["mode"].startswith("pyspark.sql.connect") and "isFinalPlan=false" in connect["explain_after_run"]
          and "*(" not in connect["explain_after_run"], connect["mode"])

    # ------------------------------------------------------------------------- the event log
    ev, J = parse_events(EVENTS), parse_jobs(EVENTS)
    jobs, stages = J["jobs"], J["stages"]

    def jobs_of(tag):
        return [j for j in sorted(jobs.values(), key=lambda j: j["job"]) if j["desc"] == tag]

    check("lines 1 and 2 and explain() run no job", not any(jobs_of(t) for t in ("line1", "line2", "line4")))
    after_text = after.getvalue().strip()
    check("before the run, explain() shows the starting plan, with no codegen markers",
          "isFinalPlan=false" in explain_text and "*(" not in explain_text)
    after_codegen = sorted({int(m) for m in re.findall(r"\*\((\d+)\)", after_text)})
    shown = after_text.split("+- == Initial Plan ==")[0].rstrip()      # on screen: the plan as it ran, cropped
    query_stages = sorted(int(m) for m in re.findall(r"(?:Shuffle|Result)QueryStage (\d+)", shown))
    check("the printed plan as it ran has one Exchange line, and adaptive execution's own query stages 0 and 1",
          shown.count("Exchange ") == 1 and query_stages == [0, 1], str(query_stages))
    check("after the run, explain() shows the plan as it ran, with codegen markers *(1) and *(2)",
          "isFinalPlan=true" in after_text and after_codegen == [1, 2], str(after_codegen))
    exchanges = re.findall(r"Exchange (\w+)\(([^)]*)\)", after.getvalue().split("== Initial Plan ==")[0])
    main_sql = {j["sql"] for j in jobs.values() if j["desc"] == "main"}
    check("the action's jobs belong to one SQL execution", len(main_sql) == 1, str(main_sql))
    final_plan = J["final_plans"][main_sql.pop()]
    codegen = sorted({int(m) for m in re.findall(r"codegen id : (\d+)", final_plan)})
    operators = len(re.findall(r"^\(\d+\) ", final_plan, flags=re.M))
    check("the plan as it ran (the UI's SQL tab) is final and carries codegen ids 1 and 2", "isFinalPlan=true" in final_plan
          and codegen == [1, 2], f"{codegen}, {operators} numbered operators")
    lines = final_plan.splitlines()
    top = next(k for k, ln in enumerate(lines) if "== Final Plan ==" in ln)
    end = next(k for k, ln in enumerate(lines) if "== Initial Plan ==" in ln)
    final_tree = [lines[0]] + lines[top:end]
    check("explain() shows one Exchange, hash-partitioned by clinic into 200", len(exchanges) == 1
          and exchanges[0][0] == "hashpartitioning" and exchanges[0][1].startswith("clinic") and exchanges[0][1].endswith("200"),
          str(exchanges))

    main_jobs = jobs_of("main")
    main_ran = [s for j in main_jobs for s in j["ran"]]
    main_skipped = [s for j in main_jobs for s in j["skipped"]]
    tasks_main = [stages[s]["tasks"] for s in main_ran]
    check("with adaptive execution: two jobs, two stages that run, four tasks then one", len(main_jobs) == 2
          and len(main_ran) == 2 and tasks_main == [TRAYS, 1], f"{len(main_jobs)} jobs, stages {main_ran}, tasks {tasks_main}")
    check("the second job lists the first stage's work again, as a skipped stage (new number, same four tasks)",
          len(main_skipped) == 1 and stages[main_skipped[0]]["tasks"] == TRAYS, f"skipped {main_skipped}")
    s1, s2 = main_ran
    s1_tasks = [t for st in ev["main"] if st["stage"] == s1 for t in st["tasks"]]
    last_s1 = max(t["finish"] for t in s1_tasks)
    check("the wall: stage 2 starts only after stage 1's last task has finished", stages[s2]["submitted"] >= last_s1,
          f"stage 2 at {stages[s2]['submitted'] - last_s1} ms after the last stage-1 task")
    finishes = sorted(t["finish"] for t in s1_tasks)
    finish_order = [t["index"] for t in sorted(s1_tasks, key=lambda t: (t["finish"], t["index"]))]
    partial_rows = sum(t["write_records"] for t in s1_tasks)
    check("14 partial counts cross the wall: one per clinic present in each pile", partial_rows == 14
          and f"rowCount={partial_rows}" in final_plan and sum(piles_per_clinic.values()) == partial_rows,
          f"{partial_rows} records written; piles per clinic {piles_per_clinic}")
    check("stage 1's tasks finish at different moments, and wait", len(set(finishes)) >= 2, str([f - finishes[0] for f in finishes]))
    check("codegen ids are not stage numbers", codegen == [1, 2] and sorted(main_ran) != codegen,
          f"codegen {codegen}, stages {main_ran}")

    again_jobs = jobs_of("again")
    again_skipped = [s for j in again_jobs for s in j["skipped"]]
    again_ran = [s for j in again_jobs for s in j["ran"]]
    check("running the action again skips its first stage: the shuffle files are reused", len(again_skipped) >= 1
          and all(stages[s]["tasks"] != TRAYS for s in again_ran), f"ran {again_ran}, skipped {again_skipped}")

    off_jobs = jobs_of("aqe_off")
    off_ran = [s for j in off_jobs for s in j["ran"]]
    off_tasks = [stages[s]["tasks"] for s in off_ran]
    off_reduce = [t for st in ev["aqe_off"] if st["stage"] == off_ran[-1] for t in st["tasks"]]
    off_nonempty = sorted(t["index"] for t in off_reduce if t["read_records"] > 0)
    check("adaptive execution off: only the piles holding a clinic's counts have rows", len(off_nonempty) == len(counts),
          str(off_nonempty))
    check("without adaptive execution: one job, two stages, four tasks then 200", len(off_jobs) == 1 and off_tasks == [TRAYS, 200],
          f"{len(off_jobs)} job, tasks {off_tasks}")

    join_jobs = jobs_of("join")
    join_ran = sorted({s for j in join_jobs for s in j["ran"]})
    join_tasks = {s: stages[s]["tasks"] for s in join_ran}
    windows = {s: (stages[s]["submitted"], stages[s]["completed"]) for s in join_ran}
    by_start = sorted(join_ran, key=lambda s: stages[s]["submitted"])
    inputs = by_start[:-1]                                            # the join stage is submitted last
    overlap = (len(inputs) >= 2 and windows[inputs[0]][0] < windows[inputs[1]][1] and windows[inputs[1]][0] < windows[inputs[0]][1])
    check("the join line runs as three stages", len(join_ran) == 3, str(join_tasks))
    check("its two input stages run at the same time", overlap, str({s: (w[0] - min(v[0] for v in windows.values()),
                                                                          w[1] - min(v[0] for v in windows.values()))
                                                                      for s, w in windows.items()}))

    result = {
        "series": "Spark, Visually", "part": "I · How Spark runs a job", "episode": 4, "title": "Cut at the Walls",
        "engine": {"spark": SPARK_PIN, "master": MASTER, "cpus": os.cpu_count(), "settings": settings,
                   "note": "local mode: exact plans, jobs, stages and tasks; order in time, not durations"},
        "code_on_screen": CODE_ON_SCREEN,
        "cast": {"patients": PATIENTS, "files": TRAYS, "seed": SEED},
        "counts": counts,
        "explain_before": explain_text, "explain": after_text, "explain_shown": shown, "query_stage_ids": query_stages,
        "exchanges": [f"{a}({b})" for a, b in exchanges], "card_aqe_off": CARD_AQE_OFF,
        "counted_piles_per_clinic": piles_per_clinic, "trays": {str(i): trays[i] for i in range(TRAYS)},
        "connect": {"explain_after_run_is_final": "isFinalPlan=true" in connect["explain_after_run"],
                    "explain_after_run": connect["explain_after_run"].strip()},
        "final_plan": "\n".join(final_tree), "codegen_ids": codegen, "plan_operators": operators,
        "main": {"jobs": len(main_jobs), "job_ids": [j["job"] for j in main_jobs], "stages_run": len(main_ran),
                 "stages_per_job": [len(j["ran"]) for j in main_jobs], "skipped_per_job": [len(j["skipped"]) for j in main_jobs],
                 "stage_ids": main_ran, "skipped_ids": main_skipped, "stage1_finish_order": finish_order,
                 "partial_counts_crossing": partial_rows,
                 "tasks": tasks_main,
                 "skipped_in_second_job": len(main_skipped), "total_tasks": sum(tasks_main),
                 "stage1_finish_offsets_ms": [f - finishes[0] for f in finishes],
                 "stage2_after_last_stage1_ms": stages[s2]["submitted"] - last_s1},
        "again": {"stages_run": len(again_ran), "stages_skipped": len(again_skipped)},
        "aqe_off": {"jobs": len(off_jobs), "stages_run": len(off_ran), "tasks": off_tasks, "total_tasks": sum(off_tasks),
                    "nonempty_piles": off_nonempty},
        "join": {"stages_run": len(join_ran), "input_tasks": sorted((join_tasks[s] for s in inputs), reverse=True),
                 "join_tasks": join_tasks[by_start[-1]], "inputs_overlap": overlap,
                 "stages_in_order": [{"tasks": join_tasks[s], "start_ms": windows[s][0] - windows[by_start[0]][0],
                                      "end_ms": windows[s][1] - windows[by_start[0]][0]} for s in by_start],
                 "note_order": "the two input stages are submitted together; which gets the lower stage number varies",
                 "windows_ms": [[windows[s][0] - min(w[0] for w in windows.values()), windows[s][1] - min(w[0] for w in windows.values())]
                                for s in join_ran],
                 "default_strategy": "BroadcastHashJoin, broadcasting the clinic table",
                 "note": "the merge hint asks for a sort-merge join, so both sides are dealt by clinic"},
        "checks": [{"name": n, "ok": ok, "detail": d} for n, ok, d in CHECKS],
    }
    (HERE / "e04_numbers.json").write_text(json.dumps(result, indent=1, ensure_ascii=False) + "\n")
    for k in ("counts", "exchanges", "codegen_ids", "main", "again", "aqe_off", "join"):
        print(f"{k}: {result[k]}")
    print(explain_text)
    failed = [c for c in CHECKS if not c[1]]
    for n, ok, d in CHECKS:
        print(("PASS " if ok else "FAIL ") + n + (f"  [{d}]" if d else ""))
    print(f"{len(CHECKS) - len(failed)} of {len(CHECKS)} checks pass")
    return 1 if failed else 0


if __name__ == "__main__":
    sys.exit(main())
