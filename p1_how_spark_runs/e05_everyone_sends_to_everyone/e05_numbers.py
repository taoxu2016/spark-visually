"""Spark, Visually · Part I · Episode 5: Everyone Sends to Everyone.

Every number the episode shows or says is computed here, checked, and written to e05_numbers.json.
Scenes and narration read e05_numbers.json; nothing numeric is typed into scene code or the script.

    python e05_numbers.py        # runs Spark, prints the ledger, runs every check, writes e05_numbers.json

Rules kept here (blueprint, "Rules Spark adds"):
  * Plans come from Spark: plans are captured from explain(); hash assignments from Spark's own hash().
  * Code on screen runs: the code-panel lines in CODE_ON_SCREEN are executed verbatim (eval / exec).
  * One pinned engine: the Spark version, master and every setting used are written into the JSON.
  * Exact where exact: task, record and byte counts come from Spark's event log, never from estimates.

The home shot against this run. The home shot draws four executors with two cores each. This run uses
local mode on one machine, so plans, partitions, records and bytes are exact, but nothing crosses a
network. Which blocks WOULD cross it is worked out for the home shot's placement (tray k on bench k,
reducer r on bench r mod 4) and every such number is labelled "home shot".
"""
from __future__ import annotations

import json
import shutil
import sys
from collections import defaultdict
from pathlib import Path

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE.parents[1]))      # the series root, for the shared sv package
from sv.cast import CAST, NAMES, SEED, TRAYS, cast_rows, write_cast  # noqa: E402,F401  (e05_cast imports TRAYS, cast_rows)
from sv.runlog import MASTER, SPARK_PIN, parse_events as _parse_events, plan_text, stage_of  # noqa: E402

WORK = Path("/tmp/sv_e05")
EVENTS = WORK / "events"
CAST_DIR = WORK / "cast"
BENCHES = 4
R_MAIN, R_COLLIDE, R_DEFAULT = 5, 3, 200
R_TABLE = (3, 4, 5, 8, 200)

# The code panel, in the order its lines glow, run verbatim: statements with exec, DataFrames with eval.
CODE_ON_SCREEN = [
    'spark.conf.set("spark.sql.shuffle.partitions", 5)',   # S04: five reducers
    'df.repartition("clinic").collect()',                  # S04-S06: every row crosses the wall
    'df.groupBy("clinic").count().collect()',              # S07: partial counts cross the wall
    'df.groupBy("clinic").agg(F.collect_list("y")).collect()',   # S07: every value crosses
    'spark.conf.set("spark.sql.shuffle.partitions", 3)',   # S08: three reducers
]

PA_SCALE = 10_000      # the cast ten thousand times over, for partial aggregation at scale

CHECKS: list[tuple[str, bool, str]] = []


def check(name: str, ok: bool, detail: str = "") -> None:
    CHECKS.append((name, bool(ok), detail))


# ----------------------------------------------------------------------------- event log
def parse_events() -> dict[str, list[dict]]:
    return _parse_events(EVENTS)


# ----------------------------------------------------------------------------- the run
def main() -> int:
    shutil.rmtree(WORK, ignore_errors=True)
    EVENTS.mkdir(parents=True)

    import pyspark
    from pyspark.sql import SparkSession, functions as F

    spark = (SparkSession.builder.master(MASTER).appName("spark-visually-e05")
             .config("spark.eventLog.enabled", "true")
             .config("spark.eventLog.dir", EVENTS.as_uri())
             .config("spark.eventLog.compress", "false")
             .config("spark.ui.enabled", "false")
             .config("spark.sql.adaptive.coalescePartitions.enabled", "false")   # blueprint M5: see Numbers
             .getOrCreate())
    sc = spark.sparkContext
    sc.setLogLevel("ERROR")
    check(f"pinned engine: Apache Spark {SPARK_PIN}", pyspark.__version__ == SPARK_PIN, pyspark.__version__)

    def tagged(tag: str, action):
        sc.setJobDescription(tag)
        try:
            return action()
        finally:
            sc.setJobDescription(None)

    # The cast, written as four files and read back: four trays.
    tagged("setup.write", lambda: write_cast(spark, str(CAST_DIR)))
    df = spark.read.parquet(str(CAST_DIR))
    ns = {"df": df, "F": F, "spark": spark}

    n_files = len(list(CAST_DIR.glob("part-*.parquet")))
    n_trays = df.rdd.getNumPartitions()
    check("the cast is four files and reads as four trays", n_files == TRAYS and n_trays == TRAYS,
          f"{n_files} files, {n_trays} partitions")

    tray_rows = tagged("setup.trays", lambda: df.withColumn("tray", F.spark_partition_id())
                       .groupBy("tray", "clinic").agg(F.count("*").alias("n"), F.sum("y").alias("readmitted"))
                       .collect())
    trays = {t: {k: 0 for k in NAMES} for t in range(TRAYS)}
    for r in tray_rows:
        trays[r["tray"]][r["clinic"]] = r["n"]
    tray_sizes = [sum(trays[t].values()) for t in range(TRAYS)]
    check("trays hold all 103 patients", sum(tray_sizes) == 103, str(tray_sizes))
    check("every clinic's rows are spread over the trays as the cast says",
          all(sum(trays[t][k] for t in range(TRAYS)) == CAST[k][0] + CAST[k][2] for k in NAMES))
    check("Alder's chips sit on all four benches (the hook)", all(trays[t]["Alder"] > 0 for t in range(TRAYS)),
          str([trays[t]["Alder"] for t in range(TRAYS)]))

    # Spark's own hash: destination = pmod(hash(clinic), R).
    keys = spark.createDataFrame([(k,) for k in NAMES], "clinic string")
    hrows = tagged("setup.hash", lambda: keys.select(
        "clinic", F.expr("hash(clinic)").alias("hash"),
        *[F.expr(f"pmod(hash(clinic), {R})").alias(f"r{R}") for R in R_TABLE]).collect())
    hashes = {r["clinic"]: {"hash": r["hash"], **{str(R): r[f"r{R}"] for R in R_TABLE}} for r in hrows}

    # Wrong method first: every row to the driver, against one number.
    tagged("collect_all", lambda: df.collect())
    total = tagged("count_all", lambda: df.count())
    check("count() is 103", total == 103, str(total))

    # R = 5: the code panel's first four lines.
    exec(CODE_ON_SCREEN[0], ns)
    check("line 1 sets five reducers", int(spark.conf.get("spark.sql.shuffle.partitions")) == R_MAIN)
    query = lambda line: eval(line.removesuffix(".collect()"), ns)     # the same line, as a DataFrame to measure
    q_all, q_count, q_list = query(CODE_ON_SCREEN[1]), query(CODE_ON_SCREEN[2]), query(CODE_ON_SCREEN[3])
    ran = [tagged(f"line{k + 1}", lambda k=k: eval(CODE_ON_SCREEN[k], ns)) for k in (1, 2, 3)]    # verbatim: actions
    check("lines 2 to 4 are actions, and run as typed", all(isinstance(r, list) for r in ran)
          and len(ran[0]) == 103 and len(ran[1]) == len(NAMES) == len(ran[2]))
    plans = {"count": plan_text(q_count), "collect_list": plan_text(q_list), "all_rows": plan_text(q_all)}
    check("the count plan has one Exchange, hash-partitioned by clinic into 5",
          plans["count"].count("Exchange") >= 1 and "hashpartitioning(clinic" in plans["count"]
          and f", {R_MAIN}" in plans["count"], "")
    check("the count plan aggregates twice: partial before the wall, final after it",
          "partial_count" in plans["count"], "")

    out_all = tagged("all_r5", lambda: q_all.withColumn("reducer", F.spark_partition_id()).collect())
    out_count = tagged("count_r5", lambda: q_count.withColumn("reducer", F.spark_partition_id()).collect())
    out_list = tagged("list_r5", lambda: q_list.withColumn("reducer", F.spark_partition_id()).collect())

    counts = {r["clinic"]: r["count"] for r in out_count}
    check("groupBy count matches the cast", all(counts[k] == CAST[k][0] + CAST[k][2] for k in NAMES), str(counts))
    reducer5 = {r["clinic"]: r["reducer"] for r in out_count}
    check("each clinic lands where pmod(hash, 5) says (count)", all(reducer5[k] == hashes[k]["5"] for k in NAMES))
    check("each clinic lands where pmod(hash, 5) says (every chip)",
          all(r["reducer"] == hashes[r["clinic"]]["5"] for r in out_all))
    check("each clinic lands where pmod(hash, 5) says (collect_list)",
          all(r["reducer"] == hashes[r["clinic"]]["5"] for r in out_list))
    lists = {r["clinic"]: len(r[1]) for r in out_list}
    check("collect_list carries every value", all(lists[k] == counts[k] for k in NAMES), str(lists))

    # Partial aggregation, measured fairly: the same count with and without the partial counts (Spark 4.3's
    # spark.sql.execution.bypassPartialAggregation), on the cast and on the cast ten thousand times over.
    big = (df.crossJoin(spark.range(PA_SCALE)).select((F.col("patient") * PA_SCALE + F.col("id")).alias("patient"),
                                                      "clinic", "x", "y").cache())
    tagged("setup.big", lambda: big.count())
    for name, d in (("cast", df), ("scaled", big)):
        for bypass, side in (("false", "with"), ("true", "without")):
            spark.conf.set("spark.sql.execution.bypassPartialAggregation", bypass)
            tagged(f"pa.{name}.{side}", lambda d=d: d.groupBy("clinic").count().collect())
    spark.conf.set("spark.sql.execution.bypassPartialAggregation", "false")
    big.unpersist()

    # R = 3: the code panel's last line, then the count again.
    exec(CODE_ON_SCREEN[4], ns)
    q3 = query(CODE_ON_SCREEN[2])
    out3 = tagged("count_r3", lambda: q3.withColumn("reducer", F.spark_partition_id()).collect())
    reducer3 = {r["clinic"]: r["reducer"] for r in out3}
    check("each clinic lands where pmod(hash, 3) says", all(reducer3[k] == hashes[k]["3"] for k in NAMES))

    # R = 200, the default: without and with adaptive coalescing.
    spark.conf.set("spark.sql.shuffle.partitions", R_DEFAULT)
    q200 = query(CODE_ON_SCREEN[2])
    out200 = tagged("count_r200", lambda: q200.withColumn("reducer", F.spark_partition_id()).collect())
    reducer200 = {r["clinic"]: r["reducer"] for r in out200}
    check("each clinic lands where pmod(hash, 200) says", all(reducer200[k] == hashes[k]["200"] for k in NAMES))
    spark.conf.set("spark.sql.adaptive.coalescePartitions.enabled", "true")
    q200c = query(CODE_ON_SCREEN[2])
    tagged("count_r200_coalesced", lambda: q200c.collect())

    settings = {
        "spark.sql.adaptive.enabled": spark.conf.get("spark.sql.adaptive.enabled"),
        "spark.sql.adaptive.coalescePartitions.enabled": "false (true only for count_r200_coalesced)",
        "spark.sql.shuffle.partitions": "5 then 3 (code panel), then 200 (the default, on a settings card)",
        "spark.sql.files.maxPartitionBytes": spark.conf.get("spark.sql.files.maxPartitionBytes"),
        "spark.sql.files.openCostInBytes": spark.conf.get("spark.sql.files.openCostInBytes"),
    }
    spark.stop()

    # ------------------------------------------------------------------------- metrics from the event log
    ev = parse_events()

    def shuffle_summary(tag: str, R: int, partial: bool) -> dict:
        stages = ev[tag]
        m, r = stage_of(stages, "map"), stage_of(stages, "reduce")
        reducer_of = {k: hashes[k][str(R)] for k in NAMES} if str(R) in hashes[NAMES[0]] else None
        # blocks from the data: (tray, reducer) -> records written into that bin
        blocks = defaultdict(int)
        for t in range(TRAYS):
            for k in NAMES:
                if trays[t][k]:
                    blocks[(t, reducer_of[k])] += 1 if partial else trays[t][k]
        nonempty = sorted(blocks)
        crossing = [(t, rr) for (t, rr) in nonempty if t % BENCHES != rr % BENCHES]
        summ = {
            "reduce_partitions": R,
            "map_tasks": len(m["tasks"]),
            "reduce_tasks": len(r["tasks"]),
            "blocks_possible": len(m["tasks"]) * R,
            "blocks_nonempty": len(nonempty),
            "blocks_fetched": sum(t["blocks"] for t in r["tasks"]),
            "records_written": [t["write_records"] for t in m["tasks"]],
            "bytes_written": [t["write_bytes"] for t in m["tasks"]],
            "records_read": [t["read_records"] for t in r["tasks"]],
            "bytes_read": [t["read_bytes"] for t in r["tasks"]],
            "bins": {f"{t},{rr}": blocks[(t, rr)] for (t, rr) in nonempty},
            "home_shot_blocks_crossing_network": len(crossing),
            "home_shot_records_crossing_network": sum(blocks[b] for b in crossing),
        }
        summ["total_bytes_written"] = sum(summ["bytes_written"])
        summ["total_records_written"] = sum(summ["records_written"])
        return summ

    sh = {
        "all_rows": shuffle_summary("all_r5", R_MAIN, partial=False),
        "count": shuffle_summary("count_r5", R_MAIN, partial=True),
        "collect_list": shuffle_summary("list_r5", R_MAIN, partial=True),
        "count_r3": shuffle_summary("count_r3", R_COLLIDE, partial=True),
        "count_r200": shuffle_summary("count_r200", R_DEFAULT, partial=True),
    }
    for name, s in sh.items():
        check(f"{name}: one map task per tray", s["map_tasks"] == TRAYS, str(s["map_tasks"]))
        check(f"{name}: one reduce task per reduce partition", s["reduce_tasks"] == s["reduce_partitions"],
              str(s["reduce_tasks"]))
        check(f"{name}: records written per bin agree with the event log",
              s["total_records_written"] == sum(s["bins"].values()) == sum(s["records_read"]),
              f"{s['total_records_written']} written, {sum(s['records_read'])} read")
        check(f"{name}: reducers fetch exactly the non-empty bins", s["blocks_fetched"] == s["blocks_nonempty"],
              f"{s['blocks_fetched']} fetched, {s['blocks_nonempty']} non-empty")
    check("every chip crosses the wall when nothing is combined", sh["all_rows"]["total_records_written"] == 103)
    def written(tag):
        st = [s for s in ev[tag] if any(t["write_records"] for t in s["tasks"])]
        return {"records": sum(t["write_records"] for s in st for t in s["tasks"]),
                "bytes": sum(t["write_bytes"] for s in st for t in s["tasks"])}
    pa = {name: {side: written(f"pa.{name}.{side}") for side in ("with", "without")} for name in ("cast", "scaled")}
    c, b = pa["cast"], pa["scaled"]
    check("partial counts cut the rows crossing the wall (the same count, with and without them)",
          c["with"]["records"] < c["without"]["records"] and c["with"]["records"] == sh["count"]["total_records_written"], str(c))
    check("on the cast, bytes barely fall: each bin's fixed overhead outweighs the rows saved",
          c["with"]["bytes"] < c["without"]["bytes"] < 1.25 * c["with"]["bytes"], str(c))
    check("at ten thousand times the cast, partial counts cut the bytes by over 99 percent",
          b["with"]["bytes"] < 0.01 * b["without"]["bytes"], str(b))
    check("collect_list sends as many records as count, but more bytes",
          sh["collect_list"]["total_records_written"] == sh["count"]["total_records_written"]
          and sh["collect_list"]["total_bytes_written"] > sh["count"]["total_bytes_written"])

    coalesced = ev["count_r200_coalesced"]
    red_c = stage_of(coalesced, "reduce")
    check("adaptive coalescing merges the 200 tiny reduce partitions", len(red_c["tasks"]) < R_DEFAULT,
          f"{len(red_c['tasks'])} reduce tasks")

    collect_stages, count_stages = ev["collect_all"], ev["count_all"]
    collect_bytes = sum(t["result_bytes"] for s in collect_stages for t in s["tasks"])
    count_final = stage_of(count_stages, "reduce")
    count_map = stage_of(count_stages, "map")
    check("count(): four partial counts meet in one final task, which returns one number",
          len(count_map["tasks"]) == TRAYS and len(count_final["tasks"]) == 1
          and sum(t["write_records"] for t in count_map["tasks"]) == TRAYS)
    count_bytes = sum(t["result_bytes"] for t in count_final["tasks"])

    occupancy = {}
    for R in R_TABLE:
        occ = defaultdict(list)
        for k in NAMES:
            occ[hashes[k][str(R)]].append(k)
        occupancy[str(R)] = {"used": len(occ), "empty": R - len(occ),
                             "shared": {str(p): ks for p, ks in sorted(occ.items()) if len(ks) > 1}}
    reduce_patients_r3 = defaultdict(int)
    for k in NAMES:
        reduce_patients_r3[hashes[k]["3"]] += counts[k]

    result = {
        "series": "Spark, Visually", "part": "I · How Spark runs a job", "episode": 5,
        "title": "Everyone Sends to Everyone",
        "engine": {"spark": SPARK_PIN, "master": MASTER, "settings": settings,
                   "note": "local mode: exact plans, partitions, records and bytes; no network"},
        "code_on_screen": CODE_ON_SCREEN,
        "partial_aggregation": {"scale": PA_SCALE, **pa},
        "cast": {"patients": 103, "readmitted": 42, "per_clinic": counts, "seed": SEED},
        "trays": {str(t): trays[t] for t in range(TRAYS)}, "tray_sizes": tray_sizes,
        "hash": hashes, "occupancy": occupancy,
        "r3_patients_per_reducer": {str(p): n for p, n in sorted(reduce_patients_r3.items())},
        "shuffles": sh,
        "count_r200_coalesced_reduce_tasks": len(red_c["tasks"]),
        "driver": {"collect_result_bytes": collect_bytes, "count_result_bytes": count_bytes,
                   "count_partials_to_final_task": sum(t["write_records"] for t in count_map["tasks"])},
        "plans": plans,
        "checks": [{"name": n, "ok": ok, "detail": d} for n, ok, d in CHECKS],
    }
    (HERE / "e05_numbers.json").write_text(json.dumps(result, indent=1, ensure_ascii=False) + "\n")

    # ------------------------------------------------------------------------- ledger
    print("Trays (rows per clinic):", {t: trays[t] for t in range(TRAYS)}, "sizes", tray_sizes)
    print("Hash:", {k: hashes[k] for k in NAMES})
    print("Occupancy:", occupancy)
    for name, s in sh.items():
        print(f"{name:13s} R={s['reduce_partitions']:<3d} map={s['map_tasks']} reduce={s['reduce_tasks']} "
              f"bins={s['blocks_nonempty']}/{s['blocks_possible']} fetched={s['blocks_fetched']} "
              f"records={s['total_records_written']} bytes={s['total_bytes_written']} "
              f"home-shot crossing={s['home_shot_blocks_crossing_network']} bins / "
              f"{s['home_shot_records_crossing_network']} records")
    print("count_r200 with coalescing: reduce tasks =", len(red_c["tasks"]))
    print("driver: collect result bytes =", collect_bytes, "; count result bytes =", count_bytes)
    failed = [c for c in CHECKS if not c[1]]
    for n, ok, d in CHECKS:
        print(("PASS " if ok else "FAIL ") + n + (f"  [{d}]" if d else ""))
    print(f"{len(CHECKS) - len(failed)} of {len(CHECKS)} checks pass")
    return 1 if failed else 0


if __name__ == "__main__":
    sys.exit(main())
