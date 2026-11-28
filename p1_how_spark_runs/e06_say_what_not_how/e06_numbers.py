"""Spark, Visually · Part I · Episode 6: Say What, Not How.

Every number the episode shows or says is computed here, checked, and written to e06_numbers.json.

    python e06_numbers.py        # runs Spark, prints the ledger, writes e06_numbers.json

Rules kept here (blueprint, "Rules Spark adds"):
  * Code on screen runs: the code panel's statements (CODE_ON_SCREEN, grouped by STATEMENTS) run verbatim, and so
    does the settings card (CARD_PUSHDOWN_OFF).
  * Plans come from Spark: every phase is Spark's own plan string, captured from the query's execution.
  * Exact where exact: rows and bytes read, row groups, jobs. Durations are not used.
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
from sv.cast import NAMES, PATIENTS, SEED, TRAYS, cast_rows, write_cast  # noqa: E402
from sv.runlog import MASTER, SPARK_PIN, parse_events, parse_jobs, session  # noqa: E402

WORK = Path("/tmp/sv_e06")
EVENTS = WORK / "events"
SCALE = 10_000                    # the cast ten thousand times over, for what a read skips
ROW_GROUP = 512 * 1024            # Parquet row-group size, in bytes, for the scaled file

CODE_ON_SCREEN = [
    'q1 = df.filter(F.col("x") == 1).groupBy("clinic").count()',     # S01: the question, as DataFrame code
    'q2 = spark.sql("""SELECT clinic, count(*) AS count',             # S01: the same question, as SQL
    '                 FROM cast WHERE x = 1 GROUP BY clinic""")',
    'q1.explain("formatted"); q2.explain("formatted")',              # S01 and S05: the physical plans, node by node
    'q1.explain(True)',                                               # S03 and S04: parsed, analysed, optimised, physical
    'df.filter(F.col("xx") == 1)',                                    # S03: a misspelt column
    'hi = F.udf(lambda x: x == 1, "boolean")',                        # S07: the same filter, as a Python UDF
    'df.filter(hi("x")).groupBy("clinic").count().collect()',
]
STATEMENTS = [(0, 1), (1, 3), (3, 4), (4, 5), (5, 6), (6, 7), (7, 8)]  # panel lines that make one statement
CARD_PUSHDOWN_OFF = 'spark.conf.set("spark.sql.parquet.filterPushdown", False)'   # S06's settings card
CARD_REORDER = 'df.groupBy("clinic").count().filter(F.col("clinic") == "Birch")'  # S04's card: written last, run first

CONNECT_RUN = r'''
import json, sys
from pyspark.sql import SparkSession, functions as F
spark = SparkSession.builder.remote("local[4]").getOrCreate()
df = spark.read.parquet(sys.argv[1])
df.createOrReplaceTempView("cast")
out = {}
def attempt(key, f):
    try:
        f(); out[key] = None
    except Exception as e:
        out[key] = type(e).__name__ + (": UNRESOLVED_COLUMN" if "UNRESOLVED_COLUMN" in str(e) else "")
bad = {}
attempt("dataframe_at_its_line", lambda: bad.setdefault("df", df.filter(F.col("xx") == 1)))
attempt("dataframe_when_columns_are_asked", lambda: bad["df"].columns)
attempt("dataframe_at_count", lambda: bad["df"].count())
attempt("sql_at_its_call", lambda: spark.sql("SELECT clinic FROM cast WHERE xx = 1"))
spark.stop()
print("CONNECT_RESULT " + json.dumps(out))
'''

CHECKS: list[tuple[str, bool, str]] = []


def check(name: str, ok: bool, detail: str = "") -> None:
    CHECKS.append((name, bool(ok), detail))


def norm(plan: str) -> str:
    """A plan string without the ids that differ between two queries: expression ids and plan ids."""
    return re.sub(r"plan_id=\d+", "plan_id=", re.sub(r"#\d+L?", "#", plan))


def main() -> int:
    shutil.rmtree(WORK, ignore_errors=True)
    EVENTS.mkdir(parents=True)

    import pyarrow.parquet as pq
    import pyspark
    from pyspark.sql import functions as F

    spark = session("spark-visually-e06", EVENTS)
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
    df.createOrReplaceTempView("cast")
    columns = len(df.columns)
    ns = {"spark": spark, "df": df, "F": F}

    # ---- the code panel, statement by statement
    misspelt = None
    for k, (a, b) in enumerate(STATEMENTS):
        code = "\n".join(CODE_ON_SCREEN[a:b])
        tag = f"line{a + 1}"
        if "explain" in code:
            buf = io.StringIO()
            with contextlib.redirect_stdout(buf):
                tagged(tag, lambda: exec(code, ns))
            ns["printed_" + ("extended" if "True" in code else "formatted")] = buf.getvalue()
        elif '"xx"' in code:
            try:
                tagged(tag, lambda: exec(code, ns))
            except Exception as e:                                       # AnalysisException: resolved at the line
                misspelt = {"error": type(e).__name__, "unresolved_column": "UNRESOLVED_COLUMN" in str(e)}
        elif code.endswith(".collect()"):
            ns["udf_rows"] = tagged(tag, lambda: eval(code, ns))
        else:
            tagged(tag, lambda: exec(code, ns))
    both = ns["printed_formatted"].split("== Physical Plan ==")
    formatted, formatted_q2 = ("== Physical Plan ==" + both[1]).strip(), ("== Physical Plan ==" + both[2]).strip()
    extended = ns["printed_extended"]
    check("the panel prints both formatted plans, and they match with ids set aside", norm(formatted) == norm(formatted_q2))
    check("explain(True) prints all four phases", all(h in extended for h in (
          "== Parsed Logical Plan ==", "== Analyzed Logical Plan ==", "== Optimized Logical Plan ==", "== Physical Plan ==")))
    q1, q2 = ns["q1"], ns["q2"]
    counts = {r["clinic"]: r["count"] for r in tagged("q1.collect", lambda: q1.collect())}
    counts2 = {r["clinic"]: r["count"] for r in tagged("q2.collect", lambda: q2.collect())}
    check("the DataFrame and SQL forms give the same answer", counts == counts2, str(counts))

    def phases(q):
        qe = q._jdf.queryExecution()
        return {"parsed": qe.logical().toString(), "analyzed": qe.analyzed().toString(),
                "optimized": qe.optimizedPlan().toString(), "physical": qe.executedPlan().toString()}
    p1, p2 = phases(q1), phases(q2)
    check("SQL and DataFrame code reach the same optimised plan", norm(p1["optimized"]) == norm(p2["optimized"]))
    check("and the same physical plan", norm(p1["physical"]) == norm(p2["physical"]))
    check("the SQL's parsed plan names its table unresolved; analysis finds it",
          "UnresolvedRelation" in p2["parsed"] and "UnresolvedRelation" not in p2["analyzed"])
    check("DataFrame code is analysed as each line is written: its plan already carries types and ids (x#n)",
          re.search(r"\bx#\d+", p1["parsed"]) is not None and "'x" not in p1["parsed"])
    check("a misspelt column fails at analysis, on its own line, before any job",
          misspelt is not None and misspelt["unresolved_column"], str(misspelt))
    check("the optimiser adds a step nobody wrote: isnotnull(x)",
          "isnotnull(x" in p1["optimized"] and "isnotnull(x" not in p1["analyzed"])
    check("the optimised plan still has the Filter over the whole relation: pushdown and the two-column read are "
          "decided in physical planning", "PushedFilters" not in p1["optimized"] and "Relation [patient" in p1["optimized"]
          and "PushedFilters" in p1["physical"])
    check("the optimiser keeps only what the query needs: a Project of clinic over the filter",
          re.search(r"Project \[clinic#\d+\]", p1["optimized"]) is not None)
    reorder = eval(CARD_REORDER, ns)
    ro = reorder._jdf.queryExecution()
    ra, rop = ro.analyzed().toString(), ro.optimizedPlan().toString()
    first = lambda plan, *ops: min((plan.find(o), o) for o in ops if o in plan)[1]
    reorder_rows = {r["clinic"]: r["count"] for r in tagged("card.reorder", lambda: reorder.collect())}
    check("written after the count, the filter runs before it: the optimiser moves it below the aggregate",
          first(ra, "Filter", "Aggregate") == "Filter" and first(rop, "Filter", "Aggregate") == "Aggregate",
          f"{reorder_rows}")

    read_schema = re.search(r"ReadSchema: (struct<[^>]*>)", formatted)
    pushed = re.search(r"PushedFilters: \[([^\]]*)\]", formatted)
    read_cols = read_schema.group(1).count(":") if read_schema else None
    check("the scan reads 2 of the table's 4 columns (pruning)", read_cols == 2 and columns == 4,
          read_schema.group(1) if read_schema else "")
    check("the filter is pushed into the scan", pushed is not None and "EqualTo(x,1)" in pushed.group(1),
          pushed.group(1) if pushed else "")
    nodes = re.findall(r"^\((\d+)\) (\w[\w ]*?)\s*(?:\[codegen id : \d+\])?$", formatted, flags=re.M)
    names = [n for _, n in nodes]
    check("the physical plan splits the count around an Exchange: partial, then final",
          "partial_count" in formatted and "Exchange" in names and names.count("HashAggregate") == 2, str(names))
    check("explain() before any action shows the starting plan", "isFinalPlan=false" in formatted)

    udf_counts = {r["clinic"]: r["count"] for r in ns["udf_rows"]}
    check("the UDF version gives the same answer", udf_counts == counts, str(udf_counts))
    udf_plan = ns["df"].filter(ns["hi"]("x")).groupBy("clinic").count()._jdf.queryExecution().executedPlan().toString()
    udf_pushed = re.search(r"PushedFilters: \[([^\]]*)\]", udf_plan)
    udf_node = (re.search(r"\b(\w*EvalPython)\b", udf_plan) or [None, None])[1]
    check("with the UDF, a Python node evaluates the filter and nothing about x is pushed down",
          udf_node is not None and udf_pushed is not None and "x" not in udf_pushed.group(1),
          f"{udf_node}; PushedFilters: [{udf_pushed.group(1) if udf_pushed else ''}]")

    # ---- down to storage: the cast ten thousand times over, sorted by risk, in small row groups
    big_path = WORK / "cast_scaled_by_risk"
    big = (df.crossJoin(spark.range(SCALE)).select((F.col("patient") * SCALE + F.col("id")).alias("patient"),
                                                    "clinic", "x", "y").orderBy("x", "patient"))
    spark.sparkContext._jsc.hadoopConfiguration().setInt("parquet.block.size", ROW_GROUP)
    tagged("setup.big", lambda: big.coalesce(1).write.parquet(str(big_path)))
    spark.sparkContext._jsc.hadoopConfiguration().unset("parquet.block.size")
    meta = pq.ParquetFile(next(big_path.glob("part-*.parquet"))).metadata
    xcol = [meta.schema.column(i).name for i in range(meta.num_columns)].index("x")
    names_in_file = [meta.schema.column(i).name for i in range(meta.num_columns)]
    groups = [{"rows": meta.row_group(g).num_rows,
               "x_min": meta.row_group(g).column(xcol).statistics.min,
               "x_max": meta.row_group(g).column(xcol).statistics.max,
               "chunk_bytes": {names_in_file[i]: meta.row_group(g).column(i).total_compressed_size
                               for i in range(meta.num_columns)}} for g in range(meta.num_row_groups)]
    may_hold = [g for g in groups if g["x_min"] <= 1 <= g["x_max"]]
    bdf = spark.read.parquet(str(big_path))
    bns = {"spark": spark, "df": bdf, "F": F, "hi": ns["hi"]}
    tagged("store.query", lambda: bdf.filter(F.col("x") == 1).groupBy("clinic").count().collect())
    exec(CARD_PUSHDOWN_OFF, bns)
    tagged("store.no_pushdown", lambda: bdf.filter(F.col("x") == 1).groupBy("clinic").count().collect())
    spark.conf.set("spark.sql.parquet.filterPushdown", True)          # back to the default (not on screen)
    tagged("store.all_columns", lambda: bdf.filter(F.col("x") == 1).write.format("noop").mode("overwrite").save())
    tagged("store.udf", lambda: bdf.filter(ns["hi"]("x")).groupBy("clinic").count().collect())
    settings = {k: spark.conf.get(k) for k in ("spark.sql.adaptive.enabled", "spark.sql.parquet.filterPushdown",
                                               "spark.sql.shuffle.partitions")}
    spark.stop()

    # ---- the same misspelt column over Spark Connect: nothing is checked until an action
    r = subprocess.run([sys.executable, "-c", CONNECT_RUN, str(four)], capture_output=True, text=True, timeout=240)
    line = next((ln for ln in r.stdout.splitlines() if ln.startswith("CONNECT_RESULT ")), None)
    assert line, f"Spark Connect run failed:\n{r.stdout[-1500:]}\n{r.stderr[-1500:]}"
    connect = json.loads(line[len("CONNECT_RESULT "):])
    check("over Spark Connect, DataFrame code waits until its plan is first needed; a SQL string still fails at once",
          connect["dataframe_at_its_line"] is None
          and all((connect[k] or "").endswith("UNRESOLVED_COLUMN") for k in
                  ("dataframe_when_columns_are_asked", "dataframe_at_count", "sql_at_its_call")), str(connect))

    # ------------------------------------------------------------------------- the event log
    ev, J = parse_events(EVENTS), parse_jobs(EVENTS)
    jobs_of = lambda tag: [j for j in J["jobs"].values() if j["desc"] == tag]
    check("the panel's first five statements run no job: building, comparing and explaining plans moves no rows",
          not any(jobs_of(f"line{a + 1}") for a, _ in STATEMENTS[:5]))

    def read(tag):
        tasks = [t for st in ev[tag] for t in st["tasks"]]
        return {"rows": sum(t["input_records"] for t in tasks), "bytes": sum(t["input_bytes"] for t in tasks)}
    udf_read = read(f"line{STATEMENTS[-1][0] + 1}")                    # the UDF statement
    check("the UDF line reads every patient and sends each one to Python", udf_read["rows"] == PATIENTS, str(udf_read))
    st = {k: read(f"store.{k}") for k in ("query", "no_pushdown", "all_columns", "udf")}
    total = PATIENTS * SCALE
    check("with pushdown, only the row groups whose statistics allow x = 1 are read",
          st["query"]["rows"] == sum(g["rows"] for g in may_hold) < total, f"{st['query']['rows']:,} of {total:,}")
    check("without pushdown every row group is read", st["no_pushdown"]["rows"] == total)
    # Column data the query needs, from the file's own metadata: the column chunks of the columns in the scan's
    # ReadSchema, in the row groups it reads. That is not what the reader fetches: it reads through a 64 KiB buffer
    # (spark.buffer.size), so on chunks this tiny each surviving row group costs about one buffer. The event log's
    # bytes read (reads[...]["bytes"]) record what it fetched; they are kept, and not said.
    wanted = re.findall(r"(\w+):", read_schema.group(1))
    needed = lambda gs, cols: sum(g["chunk_bytes"][c] for g in gs for c in cols)
    chunk = {"query": needed(may_hold, wanted), "no_pushdown": needed(groups, wanted),
             "all_columns": needed(may_hold, names_in_file), "everything": needed(groups, names_in_file)}
    check("the column data the query needs is a small part of the file's",
          chunk["query"] < chunk["no_pushdown"] < chunk["everything"] and chunk["query"] < chunk["all_columns"], str(chunk))
    check("the UDF version reads every row: its filter can't be pushed down", st["udf"]["rows"] == total)

    result = {
        "series": "Spark, Visually", "part": "I · How Spark runs a job", "episode": 6, "title": "Say What, Not How",
        "engine": {"spark": SPARK_PIN, "master": MASTER, "cpus": os.cpu_count(), "settings": settings,
                   "note": "local mode: exact plans, rows, bytes and row groups; no durations"},
        "code_on_screen": CODE_ON_SCREEN, "statements": STATEMENTS, "card_pushdown_off": CARD_PUSHDOWN_OFF,
        "card_reorder": CARD_REORDER,
        "cast": {"patients": PATIENTS, "files": TRAYS, "seed": SEED, "columns": columns},
        "counts": counts,
        "plans": {"q1": p1, "q2_parsed": p2["parsed"], "formatted": formatted, "udf_physical": udf_plan},
        "same_plan": True, "read_schema": read_schema.group(1), "pushed_filters": pushed.group(1),
        "formatted_nodes": names, "misspelt": misspelt, "connect": connect,
        "udf": {"node": udf_node, "pushed_filters": udf_pushed.group(1), "rows_to_python": udf_read["rows"]},
        "storage": {"scale": SCALE, "rows": total, "row_group_bytes": ROW_GROUP, "row_groups": len(groups),
                    "row_groups_that_may_hold_x1": len(may_hold), "groups": groups, "reads": st,
                    "columns_read": wanted, "column_bytes_needed": chunk,
                    "note": "column_bytes_needed come from the file's metadata; reads[...].bytes are what the reader "
                            "fetched (64 KiB buffered reads), kept and not said"},
        "formatted_q2": formatted_q2, "extended": extended,
        "reorder": {"card": CARD_REORDER, "analyzed": ra, "optimized": rop, "rows": reorder_rows},
        "checks": [{"name": n, "ok": ok, "detail": d} for n, ok, d in CHECKS],
    }
    (HERE / "e06_numbers.json").write_text(json.dumps(result, indent=1, ensure_ascii=False) + "\n")
    for k in ("counts", "read_schema", "pushed_filters", "formatted_nodes", "misspelt", "connect", "udf"):
        print(f"{k}: {result[k]}")
    print("storage:", {k: v for k, v in result["storage"].items() if k != "groups"})
    failed = [c for c in CHECKS if not c[1]]
    for n, ok, d in CHECKS:
        print(("PASS " if ok else "FAIL ") + n + (f"  [{d}]" if d else ""))
    print(f"{len(CHECKS) - len(failed)} of {len(CHECKS)} checks pass")
    return 1 if failed else 0


if __name__ == "__main__":
    sys.exit(main())
