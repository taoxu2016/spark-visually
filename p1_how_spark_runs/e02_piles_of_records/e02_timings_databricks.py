# Databricks notebook source
# MAGIC %md
# MAGIC # Spark, Visually · Episode 2 · task timelines
# MAGIC
# MAGIC Measures the real start and end of every task for the episode's timelines: the same work dealt into 1, 4, 8
# MAGIC and 32 piles, the cast's 32 tiny piles (nearly all fixed cost), and four uneven piles. Times need eight real
# MAGIC cores, which the sandbox that writes the episode does not have.
# MAGIC
# MAGIC **Run on:** a single-node cluster with **8 cores**, dedicated (single user) access mode, so that
# MAGIC `sparkContext` and the Spark UI's REST API are available (not serverless, not shared access mode).
# MAGIC Run all cells; the last one prints one JSON blob and saves it to `/dbfs/tmp/e02_timings.json`.
# MAGIC Send the JSON back; it becomes `e02_timings.json` beside `e02_numbers.json`.

# COMMAND ----------

import json, os, random, time
import requests
from pyspark.sql import functions as F
from pyspark.sql.types import IntegerType, StringType, StructField, StructType

sc = spark.sparkContext
assert sc.defaultParallelism == 8, f"this needs 8 task slots on one machine, found {sc.defaultParallelism}"

# The cast: a copy of sv/cast.py (Model in Motion's five clinics and 103 patients, seed 2026). The check below
# compares its four piles with the series' numbers, so a drifted copy fails here instead of drawing the wrong data.
CAST = {"Alder": (24, 8, 6, 5), "Birch": (6, 1, 24, 13), "Cedar": (10, 2, 10, 6), "Dune": (6, 2, 6, 5), "Elm": (11, 0, 0, 0)}


def cast_rows():
    rows, pid = [], 0
    for k, (n0, e0, n1, e1) in CAST.items():
        for x, n, e in ((0, n0, e0), (1, n1, e1)):
            for j in range(n):
                pid += 1
                rows.append((pid, k, x, 1 if j < e else 0))
    random.Random(2026).shuffle(rows)
    return rows


schema = StructType([StructField("patient", IntegerType()), StructField("clinic", StringType()),
                     StructField("x", IntegerType()), StructField("y", IntegerType())])
SCALE = 20_000                      # about two million patients, the size the blueprint gives the crowd
BASE = "/tmp/sv_e02_timings"
dbutils.fs.rm(BASE, True)

# COMMAND ----------

def tagged(tag, action):
    sc.setJobDescription(tag)
    try:
        return action()
    finally:
        sc.setJobDescription(None)


cast = spark.createDataFrame(sc.parallelize(cast_rows(), 4), schema)
tagged("setup.cast", lambda: cast.write.parquet(f"{BASE}/cast"))
cast = spark.read.parquet(f"{BASE}/cast")
piles4 = sorted(r["count"] for r in cast.withColumn("p", F.spark_partition_id()).groupBy("p").count().collect())
assert piles4 == [25, 25, 25, 28], f"the cast copy drifted: {piles4}"

crowd = cast.crossJoin(spark.range(SCALE)).select((F.col("patient") * SCALE + F.col("id")).cast("int").alias("patient"),
                                                 "clinic", "x", "y")
tagged("setup.crowd", lambda: crowd.write.parquet(f"{BASE}/crowd"))
crowd = spark.read.parquet(f"{BASE}/crowd")

# Uneven: four files of 70, 11, 11 and 11 shares of the cast; each row then copied SCALE times where it lies.
rows, cuts = cast_rows(), [0, 70, 81, 92, 103]
keyed = [(k, r) for k in range(4) for r in rows[cuts[k]:cuts[k + 1]]]
uneven_cast = spark.createDataFrame(sc.parallelize(keyed, 4).partitionBy(4, lambda k: k).map(lambda kv: kv[1]), schema)
tagged("setup.uneven", lambda: uneven_cast.write.parquet(f"{BASE}/uneven_cast"))
uneven = (spark.read.parquet(f"{BASE}/uneven_cast").crossJoin(F.broadcast(spark.range(SCALE)))
          .select((F.col("patient") * SCALE + F.col("id")).cast("int").alias("patient"), "clinic", "x", "y"))

# The work each task does per row: a SHA-256 of the row, so that a pile's time grows with its rows.
work = F.sum(F.length(F.sha2(F.concat_ws("-", "clinic", "patient", "x", "y"), 256))).alias("w")

# COMMAND ----------

def piles(df, n=None):
    """Deal the rows into n piles (or keep the piles they have) and cache them, so the timed job reads only piles."""
    d = (df.repartition(n) if n else df).cache()
    tagged("setup.cache", lambda: d.count())
    return d


DATA = {"piles_1": piles(crowd, 1), "piles_4": piles(crowd, 4), "piles_8": piles(crowd, 8), "piles_32": piles(crowd, 32),
        "tiny_32": piles(cast, 32), "uneven_4": piles(uneven)}
SIZES = {name: [r["count"] for r in sorted(d.withColumn("p", F.spark_partition_id()).groupBy("p").count().collect())]
         for name, d in DATA.items()}
for rep in (1, 2):                                # the first pass warms the JVM; the second is kept
    for name, d in DATA.items():
        tagged(f"{name}.{rep}", lambda d=d: d.agg(work).collect())
time.sleep(3)                                     # let the UI store catch up with the last task

# COMMAND ----------

ui, app = sc.uiWebUrl, sc.applicationId
api = f"{ui}/api/v1/applications/{app}"
jobs = requests.get(f"{api}/jobs", timeout=30).json()


def ms(stamp):
    return time.mktime(time.strptime(stamp[:19], "%Y-%m-%dT%H:%M:%S")) * 1000 + int(stamp[20:23])


def stage_tasks(tag):
    """The stage of the tagged job that works on the piles: the widest, and of equally wide ones the busiest."""
    stage_ids = sorted({s for j in jobs if j.get("description") == tag for s in j["stageIds"]})
    cands = []
    for sid in stage_ids:
        for att in requests.get(f"{api}/stages/{sid}", timeout=30).json():
            if att.get("status") != "SKIPPED":
                cands.append(requests.get(f"{api}/stages/{sid}/{att['attemptId']}/taskList?length=10000", timeout=30).json())
    best = max(cands, key=lambda tl: (len(tl), sum(t.get("taskMetrics", {}).get("executorRunTime", 0) for t in tl)))
    out = []
    for t in best:
        m = t.get("taskMetrics", {})
        start = ms(t["launchTime"])
        out.append({"index": t["index"], "start_ms": start, "duration_ms": t.get("duration", 0),
                    "deserialize_ms": m.get("executorDeserializeTime", 0), "run_ms": m.get("executorRunTime", 0),
                    "result_ms": m.get("resultSerializationTime", 0), "scheduler_delay_ms": t.get("schedulerDelay", 0),
                    "cpu_ms": round(m.get("executorCpuTime", 0) / 1e6, 2),
                    "rows": (m.get("inputMetrics", {}).get("recordsRead", 0) or m.get("shuffleReadMetrics", {}).get("recordsRead", 0))})
    t0 = min(t["start_ms"] for t in out)
    for t in out:
        t["start_ms"] -= t0
    return sorted(out, key=lambda t: t["index"])


result = {
    "cluster": {"spark": spark.version, "default_parallelism": sc.defaultParallelism, "cpus": os.cpu_count(),
                "node_type": spark.conf.get("spark.databricks.clusterUsageTags.clusterNodeType", "unknown"),
                "runtime": spark.conf.get("spark.databricks.clusterUsageTags.sparkVersion", "unknown"),
                "photon": spark.conf.get("spark.databricks.photon.enabled", "unknown"),
                "aqe": spark.conf.get("spark.sql.adaptive.enabled")},
    "scale": SCALE, "rows": crowd.count(),
    "pile_sizes": SIZES,
    "experiments": {name: stage_tasks(f"{name}.2") for name in DATA},
}
for name, ts in result["experiments"].items():
    end = max(t["start_ms"] + t["duration_ms"] for t in ts)
    assert len(ts) == len(SIZES[name]), (name, len(ts), len(SIZES[name]))
    print(f"{name:9s} tasks={len(ts):3d}  span={end:7.0f} ms  pile sizes={sorted(set(SIZES[name]))[:4]}")
blob = json.dumps(result)
os.makedirs("/dbfs/tmp", exist_ok=True)
with open("/dbfs/tmp/e02_timings.json", "w") as f:
    f.write(blob)
print(blob)
