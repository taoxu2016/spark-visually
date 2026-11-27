"""Spark, Visually: running Spark for a numbers file, and reading back what it did.

Numbers files run real queries on a pinned Apache Spark in local mode with the event log on, tag each action with
a job description, and read task, record and byte counts back from the event log, never from estimates.
Nothing here imports Manim.
"""
from __future__ import annotations

import contextlib
import io
import json
from collections import defaultdict
from pathlib import Path

SPARK_PIN = "4.3.0"
MASTER = "local[8]"            # eight task slots, as on the home shot (4 executors x 2 cores)


def session(app: str, events: Path, master: str = MASTER, **conf):
    """A local SparkSession that writes an uncompressed event log to `events`, with the UI off."""
    from pyspark.sql import SparkSession
    b = (SparkSession.builder.master(master).appName(app)
         .config("spark.eventLog.enabled", "true")
         .config("spark.eventLog.dir", events.as_uri())
         .config("spark.eventLog.compress", "false")
         .config("spark.ui.enabled", "false"))
    for k, v in conf.items():
        b = b.config(k.replace("__", "."), v)
    spark = b.getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    return spark


def plan_text(q, mode: str = "formatted") -> str:
    buf = io.StringIO()
    with contextlib.redirect_stdout(buf):
        q.explain(mode)
    return buf.getvalue().strip()


def parse_events(events: Path) -> dict[str, list[dict]]:
    """Job description -> the stages that actually ran for it, each with its tasks' metrics."""
    # Spark 4 rolls its event log: EVENTS/eventlog_v2_<app>/events_<n>_<app>, read in order of n.
    logs = sorted(events.glob("eventlog_v2_*/events_*"), key=lambda p: int(p.name.split("_")[1]))
    assert logs, f"no event log under {events}"
    jobs, tasks = {}, defaultdict(list)
    for line in (ln for p in logs for ln in p.read_text().splitlines()):
        ev = json.loads(line)
        kind = ev["Event"]
        if kind == "SparkListenerJobStart":
            desc = (ev.get("Properties") or {}).get("spark.job.description")
            jobs[ev["Job ID"]] = (desc, ev["Stage IDs"])
        elif kind == "SparkListenerTaskEnd":
            m = ev.get("Task Metrics") or {}
            sw, sr, inp = m.get("Shuffle Write Metrics") or {}, m.get("Shuffle Read Metrics") or {}, m.get("Input Metrics") or {}
            info = ev["Task Info"]
            tasks[ev["Stage ID"]].append({
                "index": info["Index"],
                "result_bytes": m.get("Result Size", 0),
                "input_records": inp.get("Records Read", 0),
                "input_bytes": inp.get("Bytes Read", 0),
                "write_records": sw.get("Shuffle Records Written", 0),
                "write_bytes": sw.get("Shuffle Bytes Written", 0),
                "read_records": sr.get("Total Records Read", 0),
                "read_bytes": sr.get("Local Bytes Read", 0) + sr.get("Remote Bytes Read", 0),
                "blocks": sr.get("Local Blocks Fetched", 0) + sr.get("Remote Blocks Fetched", 0),
                "ms": info["Finish Time"] - info["Launch Time"],
                "launch": info["Launch Time"], "finish": info["Finish Time"],
                "run_ms": m.get("Executor Run Time", 0),
                "deser_ms": m.get("Executor Deserialize Time", 0), "ser_ms": m.get("Result Serialization Time", 0),
                "cpu_ns": m.get("Executor CPU Time", 0) + m.get("Executor Deserialize CPU Time", 0),
            })
    by_tag: dict[str, list[dict]] = defaultdict(list)
    for jid in sorted(jobs):
        desc, sids = jobs[jid]
        for sid in sorted(sids):
            if sid in tasks and all(s["stage"] != sid for s in by_tag[desc]):
                ts = sorted(tasks[sid], key=lambda t: t["index"])
                if any(t["write_bytes"] or t["write_records"] for t in ts):
                    kind = "map"
                elif any(t["blocks"] or t["read_records"] for t in ts):
                    kind = "reduce"
                else:
                    kind = "scan"
                by_tag[desc].append({"stage": sid, "job": jid, "kind": kind, "tasks": ts})
    return by_tag


def parse_jobs(events: Path) -> dict:
    """What the Spark UI's Jobs, Stages and SQL pages show, from the event log: every job with its description, its SQL
    execution and the stages it lists (a listed stage that never ran is a skipped one); every stage's task count and its
    submit and completion times (ms); and, for each SQL execution under adaptive execution, the plan as it finally ran
    (the SQL tab's plan, which explain() does not show)."""
    logs = sorted(events.glob("eventlog_v2_*/events_*"), key=lambda p: int(p.name.split("_")[1]))
    jobs, stages, final_plans = {}, {}, {}
    for line in (ln for p in logs for ln in p.read_text().splitlines()):
        ev = json.loads(line)
        kind = ev["Event"]
        if kind == "SparkListenerJobStart":
            props = ev.get("Properties") or {}
            jobs[ev["Job ID"]] = {"job": ev["Job ID"], "desc": props.get("spark.job.description"),
                                  "sql": props.get("spark.sql.execution.id"), "stages": sorted(ev["Stage IDs"])}
            for info in ev.get("Stage Infos", []):            # a skipped stage is only ever described here
                stages.setdefault(info["Stage ID"], {"stage": info["Stage ID"], "tasks": info["Number of Tasks"]})
        elif kind.endswith("SparkListenerSQLAdaptiveExecutionUpdate"):
            final_plans[str(ev["executionId"])] = ev["physicalPlanDescription"]      # the last update is what ran
        elif kind in ("SparkListenerStageSubmitted", "SparkListenerStageCompleted"):
            info = ev["Stage Info"]
            st = stages.setdefault(info["Stage ID"], {"stage": info["Stage ID"], "tasks": info["Number of Tasks"]})
            if info.get("Submission Time") is not None:
                st["submitted"] = info["Submission Time"]
            if info.get("Completion Time") is not None:
                st["completed"] = info["Completion Time"]
    for j in jobs.values():
        j["ran"] = [s for s in j["stages"] if "submitted" in stages.get(s, {})]
        j["skipped"] = [s for s in j["stages"] if s not in j["ran"]]
    return {"jobs": jobs, "stages": stages, "final_plans": final_plans}


def stage_of(stages: list[dict], kind: str) -> dict:
    found = [s for s in stages if s["kind"] == kind]
    assert len(found) == 1, (kind, [(s["stage"], s["kind"]) for s in stages])
    return found[0]
