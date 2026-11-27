# Accuracy review: Spark, Visually · Episode 1: Who Does the Work

You are reviewing the script of one episode of *Spark, Visually*, an animated video series that shows what an Apache
Spark cluster does when PySpark code runs. Its viewers write PySpark on Databricks but have never watched a job run.
Another model wrote the script. You are the independent second opinion on technical accuracy, after the series'
owner approved it. The episode is already voiced and animated, so keep each fix to the words that must change. Review accuracy only, not style or pacing.

## Ground rules

- Check every claim against Apache Spark 4.3.0 as the run below used it: PySpark in local mode, default settings
  unless stated, adaptive query execution on. If your knowledge rests on an earlier release, say which. Where
  Databricks Runtime would behave differently in a way a Databricks user would notice, say so.
- The measured facts below were captured from Spark's own plans, event log and results, through Spark Connect as well as a classic session, and, for the driver's memory, from Python's own allocation tracking (`tracemalloc`) of the collected rows, by the episode's numbers
  file, and every check listed there passed. Treat them as what this run did. If one looks wrong to you, explain why
  and how to check it.
- Judge each sentence as a viewer would take it. A claim that is true of this example but sounds like a general rule
  (or the reverse) is an issue.
- The "On screen" notes describe the animation. Check them too: the picture must not show Spark doing something it
  doesn't do.
- The series uses plain words on purpose (see "Deliberate choices"). Flag one only if it would leave a viewer with a
  wrong idea.
- Back each finding with Spark's source code (name the class or method), its configuration reference or its official
  documentation, and say how confident you are. If only running code would settle it, give the PySpark snippet to run.
- Don't rewrite clips that are correct, and don't propose new content beyond what a fix needs.

## What to check

1. Every number said or shown traces to the measured facts, with the right meaning (for example jobs against stages
   against tasks, or stages that ran against stages that were skipped).
2. Every statement about Spark is correct as worded: workers, executors and cores; who starts executors; the driver's role; Spark Connect as a thin client and what it leaves out; a read that runs no job; the count's partial counts and final task; `collect()` and the driver's memory; where DataFrame work runs; Python UDFs and their batches.
3. The code on screen does what the narration says it does.
4. Nothing contradicts what earlier episodes taught, and nothing reinforces the misconceptions this episode targets.
5. The open questions at the end.

## How to report

**A. Findings**, most serious first, each labelled Wrong (false as worded), Misleading (true, but leaves a wrong idea)
or Imprecise (worth a tighter wording). For each: the clip id; the sentence or visual, quoted; the problem; a suggested
wording in the script's style (short, plain, written to be spoken); your evidence and confidence.

**B. Open questions**: your answer to each, in a sentence or two.

**C. Verified**: what you checked and found correct, briefly.

**D. Unverified**: anything you couldn't settle, with the snippet to run.

## The episode

Part I, Episode 1: *Who Does the Work* (driver, executors, cores), module M1 · Who does the work.

- **Question:** When I run a PySpark cell, which machine does what?
- **Learning objective:** The viewer can name the driver, executors, cores and cluster manager, say which one runs their notebook code and which ones touch the data, and predict where `count` and `collect` send rows.
- **Core concept:** One driver plans and schedules; many executors do the work, each with a few cores. Data flows from storage straight to executors. Only plans, tasks and small results pass through the driver, unless the code asks for rows.

### Misconceptions this episode targets

- The driver loads the data and hands it out. (Executors read storage themselves.)
- My Python runs on the cluster. (Only UDFs do; DataFrame operations run in the JVM.)
- Executor and worker are the same thing. (A worker is a machine; executors are processes on it.)
- More executors always means faster. (Only with enough partitions, and a driver that keeps up.)

### What earlier episodes taught

None: this is the series' first episode.

### The data

The cast: 103 simulated patients in five clinics (Alder, Birch, Cedar, Dune, Elm), written as four Parquet files that Spark reads as four piles of 28, 25, 25 and 25 rows. Column x = 1 marks a higher-risk patient (46 of the 103); column y marks a readmission (42). The animation always shows a fixed cluster of one driver and four executors with two cores each; the numbers come from one machine in local mode. The table is registered as `cast`. For collect at scale, the cast is repeated 10,000 times (1,030,000 rows). The Spark Connect run used a local Spark Connect server.

### Deliberate choices (not errors)

- The hook names a national registry with no size; the measured ten-thousand-times figure appears in S07, because about a million rows and 533 MB would still fit on a laptop.
- One Spark Connect beat (S04), checked by running the whole code panel through Spark Connect; `df.rdd` and `sparkContext` fail there, as recorded.
- The code panel has two lines more than the blueprint, a Python UDF and its use, so S08 shows a real UDF and its plan node.
- The count's four partial counts go to one final task, not to the driver; only the sum goes up.
- Bytes on the wire are recorded but not said, because they mislead at this size: every task reports back to the driver, and collected rows travel compressed, so on the cast the count's task results outweigh the collect's. The episode speaks of rows, and of the memory they take on the driver.
- In Spark 4.3 a plain Python UDF is Arrow-optimized by default, so its plan node is `ArrowEvalPython`; S08 says "in batches", which holds either way.
- With adaptive execution on, the count runs as 2 jobs; this episode doesn't count jobs, which Episode 4 takes up.
- Plain words on purpose: a pile is a partition, a wall is the boundary between stages, the cast is the example data. In the On screen notes, a chip is one record, coloured by its clinic; a tray is a pile as drawn; a bench is an executor; the driver desk is the driver; the shelf is storage.

## Code on screen

These lines run verbatim in the numbers file, in this order.

```python
df = spark.read.table("cast")
df.count()
rows = df.collect()
flip = F.udf(lambda y: 1 - y, "int")
df.select(F.sum(flip("y"))).collect()
```

## Measured facts

### The count's plan (explain("formatted"))

```text
== Physical Plan ==
AdaptiveSparkPlan (5)
+- HashAggregate (4)
   +- Exchange (3)
      +- HashAggregate (2)
         +- Scan parquet  (1)


(1) Scan parquet 
Output: []
Batched: true
Location: InMemoryFileIndex [file:/tmp/sv_e01/cast]
ReadSchema: struct<>

(2) HashAggregate
Input: []
Keys: []
Functions [1]: [partial_count(1)]
Aggregate Attributes [1]: [count#55L]
Results [1]: [count#56L]

(3) Exchange
Input [1]: [count#56L]
Arguments: SinglePartition, ENSURE_REQUIREMENTS, [plan_id=251]

(4) HashAggregate
Input [1]: [count#56L]
Keys: []
Functions [1]: [count(1)]
Aggregate Attributes [1]: [count(1)#54L]
Results [1]: [count(1)#54L AS count#49L]

(5) AdaptiveSparkPlan
Output [1]: [count#49L]
Arguments: isFinalPlan=false
```

### The collect's plan (explain("formatted"))

```text
== Physical Plan ==
* ColumnarToRow (2)
+- Scan parquet  (1)


(1) Scan parquet 
Output [4]: [patient#4, clinic#5, x#6, y#7]
Batched: true
Location: InMemoryFileIndex [file:/tmp/sv_e01/cast]
ReadSchema: struct<patient:int,clinic:string,x:int,y:int>

(2) ColumnarToRow [codegen id : 1]
Input [4]: [patient#4, clinic#5, x#6, y#7]
```

### The UDF line's plan (explain("formatted"))

```text
== Physical Plan ==
AdaptiveSparkPlan (7)
+- HashAggregate (6)
   +- Exchange (5)
      +- HashAggregate (4)
         +- Project (3)
            +- ArrowEvalPython (2)
               +- Scan parquet  (1)


(1) Scan parquet 
Output [1]: [y#7]
Batched: true
Location: InMemoryFileIndex [file:/tmp/sv_e01/cast]
ReadSchema: struct<y:int>

(2) ArrowEvalPython
Input [1]: [y#7]
Arguments: [<lambda>(y#7)#57], [pythonUDF0#60], 101

(3) Project
Output [1]: [pythonUDF0#60]
Input [2]: [y#7, pythonUDF0#60]

(4) HashAggregate
Input [1]: [pythonUDF0#60]
Keys: []
Functions [1]: [partial_sum(pythonUDF0#60)]
Aggregate Attributes [1]: [sum#61L]
Results [1]: [sum#62L]

(5) Exchange
Input [1]: [sum#62L]
Arguments: SinglePartition, ENSURE_REQUIREMENTS, [plan_id=272]

(6) HashAggregate
Input [1]: [sum#62L]
Keys: []
Functions [1]: [sum(pythonUDF0#60)]
Aggregate Attributes [1]: [sum(pythonUDF0#60)#58L]
Results [1]: [sum(pythonUDF0#60)#58L AS sum(<lambda>(y))#59L]

(7) AdaptiveSparkPlan
Output [1]: [sum(<lambda>(y))#59L]
Arguments: isFinalPlan=false
```

### Everything else the numbers file recorded

Ids, counts and orders are exact; fields in milliseconds come from a one-CPU machine and matter only as order.

```json
{
 "series": "Spark, Visually",
 "part": "I · How Spark runs a job",
 "episode": 1,
 "title": "Who Does the Work",
 "engine": {"spark": "4.3.0", "master": "local[8]", "connect_master": "local[4]", "settings": {"spark.sql.adaptive.enabled": "true", "spark.sql.files.maxPartitionBytes": "134217728b", "spark.sql.files.openCostInBytes": "4194304b", "spark.sql.autoBroadcastJoinThreshold": "10485760b", "spark.sql.execution.pythonUDF.arrow.enabled": "true"}, "note": "local mode: exact tasks, rows and bytes; no network"},
 "cast": {"patients": 103, "readmitted": 42, "files": 4, "seed": 2026},
 "trays": {"0": {"Alder": 9, "Birch": 6, "Cedar": 10, "Dune": 2, "Elm": 1}, "1": {"Alder": 5, "Birch": 12, "Cedar": 2, "Dune": 3, "Elm": 3}, "2": {"Alder": 8, "Birch": 8, "Cedar": 3, "Dune": 3, "Elm": 3}, "3": {"Alder": 8, "Birch": 4, "Cedar": 5, "Dune": 4, "Elm": 4}},
 "tray_sizes": [28, 25, 25, 25],
 "slots": 8,
 "count": {"result": 103, "jobs": 2, "tasks": 4, "rows_per_task": [28, 25, 25, 25], "partials_written": 4, "final_tasks": 1, "final_task_reads": 4},
 "collect": {"rows": 103, "tasks": 4},
 "bytes_to_driver": {"cast": {"collect_python_bytes": 56104, "count_bytes_to_driver": 12690, "collect_bytes_to_driver": 8896}, "scaled": {"factor": 10000, "rows": 1030000, "collect_python_bytes": 533420943, "count_bytes_to_driver": 27202, "collect_bytes_to_driver": 6473488, "count_tasks": 13, "collect_tasks": 12, "collect_python_megabytes": 533}},
 "udf": {"result": 61, "pruned_in_a_count": true, "runs_in_line5": true, "plan_node": "ArrowEvalPython", "tasks": 4, "arrow": "true"},
 "connect": {"mode": "pyspark.sql.connect.session", "count": 103, "collect_rows": 103, "udf_sum": 61, "rdd": "PySparkNotImplementedError: [NOT_IMPLEMENTED] rdd is not implemented.", "sparkContext": "PySparkAttributeError: [JVM_ATTRIBUTE_NOT_SUPPORTED] Attribute `sparkContext` is not supported in Spark Connect as it depends on the JVM."},
 "checks": [
  {"name": "pinned engine: Apache Spark 4.3.0", "ok": true, "detail": "4.3.0"},
  {"name": "eight task slots, as on the home shot (four executors, two cores each)", "ok": true, "detail": "8"},
  {"name": "the cast is written as four files", "ok": true, "detail": "4 files"},
  {"name": "the four files read as four trays holding all 103 patients", "ok": true, "detail": "[28, 25, 25, 25]"},
  {"name": "df.count() is 103", "ok": true, "detail": "103"},
  {"name": "collect() brings all 103 rows to the driver", "ok": true, "detail": "103"},
  {"name": "the UDF line adds 1 - y over every patient: the patients not readmitted", "ok": true, "detail": "61 = 103 - 42"},
  {"name": "a UDF whose value a count doesn't use never runs (so line 5 sums instead)", "ok": true, "detail": ""},
  {"name": "in the line on screen the UDF does run, in Python workers", "ok": true, "detail": ""},
  {"name": "the UDF's plan sends rows out to Python (an EvalPython node)", "ok": true, "detail": "ArrowEvalPython"},
  {"name": "the plain count's plan has no Python in it", "ok": true, "detail": ""},
  {"name": "the scaled cast has 1,030,000 rows", "ok": true, "detail": "1030000"},
  {"name": "the scaled collect brings every row into the driver's Python", "ok": true, "detail": "1030000"},
  {"name": "reading the table runs no job", "ok": true, "detail": "[]"},
  {"name": "defining the UDF runs no job", "ok": true, "detail": ""},
  {"name": "count: four tasks, one per file", "ok": true, "detail": "4"},
  {"name": "count: each task reads its own tray from storage", "ok": true, "detail": "[28, 25, 25, 25] vs [28, 25, 25, 25]"},
  {"name": "count: each task passes on one partial count", "ok": true, "detail": "[1, 1, 1, 1]"},
  {"name": "count: one final task reads the four partial counts", "ok": true, "detail": "1 task(s), 4 records"},
  {"name": "collect: four tasks read the same four trays", "ok": true, "detail": ""},
  {"name": "the UDF count reads the same four trays", "ok": true, "detail": ""},
  {"name": "collected rows cost about the same Python memory each, at both sizes", "ok": true, "detail": "545 and 518 bytes per row"},
  {"name": "at scale, the collected rows take hundreds of megabytes in the driver's Python", "ok": true, "detail": "509 MiB"},
  {"name": "Spark Connect: the same code panel gives the same count", "ok": true, "detail": "103"},
  {"name": "Spark Connect: collect brings the same 103 rows", "ok": true, "detail": ""},
  {"name": "Spark Connect: the UDF line runs too", "ok": true, "detail": "61"},
  {"name": "Spark Connect: df.rdd is not available", "ok": true, "detail": "PySparkNotImplementedError: [NOT_IMPLEMENTED] rdd is not implemented."},
  {"name": "Spark Connect: the session is a client (no SparkContext)", "ok": true, "detail": "PySparkAttributeError: [JVM_ATTRIBUTE_NOT_SUPPORTED] Attribute `sparkContext` is not supported in Spark Connect as it depends on the JVM."}
 ]
}
```

## The script

Each clip is one stretch of voiceover. "On screen" describes the animation; "Narration" is spoken.

### S01 · Cold open

- **s1_1**. On screen: A laptop card holds the cast: 103 chips in the clinic hues, its memory bar barely touched.
  Narration: “Here are our 103 patients. They fit on one laptop, with plenty of room to spare. Now picture a national registry: every patient, every visit, for years.”
- **s1_2**. On screen: The registry pours in: the laptop's memory bar floods red and chips spill over its edge.
  Narration: “The laptop's memory fills, and then it overflows. One machine isn't enough. So we spread the work across many machines. But then, which machine does what?”

### S02 · Title

- **s2_1**. On screen: Series wordmark, Spark, Visually; Part I, How Spark runs a job; the episode title.
  Narration: “Welcome to Spark, Visually. Today: who does the work.”

### S03 · The parts

- **s3_1**. On screen: The home shot builds piece by piece. First the storage shelf along the bottom, with the cast written on it as four files.
  Narration: “Let's build the cluster, piece by piece. At the bottom: storage. Our cast is written there as 4 files.”
- **s3_2**. On screen: Four executor benches rise above the shelf, each with two core slots. Labels name the parts once: worker (the machine), executor (the process on it), core (a slot that runs one task).
  Narration: “Above it, four machines, called workers. On each one runs a process called an executor. Each executor has cores, and a core runs one task at a time. 4 executors with 2 cores each: 8 tasks can run at once.”
- **s3_3**. On screen: The network band joins the benches, and the driver desk appears above it and lights up. A small cluster-manager card starts the four executors when the driver asks.
  Narration: “And above them all, the driver. It holds the Spark session, plans the work and hands it out. The executors were started by the cluster manager, when the driver asked for them.”

### S04 · Your notebook

- **s4_1**. On screen: The code panel slides in on the left, wired to the driver desk.
  Narration: “On a classic cluster, your notebook's Python runs on the driver machine itself.”
- **s4_2**. On screen: Variant: the code panel detaches into a small client card, joined to the driver by a thin line. A plan travels down the line; one result travels back.
  Narration: “On some Databricks compute, it doesn't. With Spark Connect, the notebook is a small client: it sends each query to a driver on the cluster, and gets back only the result.”
- **s4_3**. On screen: Both set-ups run the same code panel. A small card lists what Spark Connect leaves out: df.rdd and the SparkContext.
  Narration: “Everything in this series runs both ways. What doesn't run on Spark Connect is the old, low-level route through the SparkContext, like df.rdd. So we won't use it.”

### S05 · Nothing moves

- **s5_1**. On screen: Code panel, line 1 glows: df = spark.read.table("cast"). Then it dims. Nothing moves anywhere on the home shot.
  Narration: “Now, some code. Line one reads our table. It returns at once, and nothing moves. No machine touches a single row. We'll see why in episode 3.”

### S06 · Counting

- **s6_1**. On screen: Line 2 glows: df.count(). The driver turns the plan into four tasks, one per file, and sends them down to the benches.
  Narration: “Line two counts the rows. The driver turns that into 4 tasks, one for each file, and sends them down to the executors.”
- **s6_2**. On screen: Each bench pulls its own file up from the shelf, and its chips flow up into its tray. Nothing passes through the driver.
  Narration: “Each executor reads its own file, straight from storage. The data never passes through the driver.”
- **s6_3**. On screen: Each tray shows its count: 28, 25, 25, 25. The four numbers travel to one final task on one bench, which adds them; a single number, 103, rises to the driver.
  Narration: “Each task counts its own rows: 28, 25, 25 and 25. The 4 partial counts go to one final task, which adds them up. And just one number goes up to the driver: 103.”

### S07 · The wrong way

- **s7_1**. On screen: Line 3 glows: rows = df.collect(). Every chip flies up from the trays into the driver, and its memory gauge rises.
  Narration: “Line three asks for every row: collect. Now all 103 patients travel up to the driver.”
- **s7_2**. On screen: A scale card: the cast ten thousand times over, 1,030,000 rows. Count sends up one number; collect fills the driver's gauge: 533 MB of Python objects.
  Narration: “For our cast, that's nothing. Now make it 10,000 times bigger: about a million rows. Count still sends up one number. Collect lands every row on the driver: over 500 megabytes of memory.”
- **s7_3**. On screen: The registry again: the driver's gauge floods red. Then the shot rewinds: the chips drop back to their trays.
  Narration: “For a whole registry, that would swamp the driver. Rows should stay down, with the executors.”

### S08 · Where Python runs

- **s8_1**. On screen: The code panel's Python feeds the plan on the driver desk; the benches wear a small JVM badge.
  Narration: “One more surprise. The Python you write mostly just builds a plan. The real work, reading, counting, adding, runs inside the executors, in Java, not Python.”
- **s8_2**. On screen: Lines 4 and 5 glow: a Python function, flip, summed in a select. Beside each bench a small Python worker appears, and batches of chips shuttle out to it and back. The plan shows ArrowEvalPython.
  Narration: “Unless you write your own Python function, a UDF. Then each executor starts Python workers beside it, and ships the rows out to them in batches, and back. That takes time, and Spark can't see inside your function.”

### S09 · Recap

- **s9_1**. On screen: Three recap cards: the driver plans; the executors work; rows stay down.
  Narration: “Let's gather it up. The driver plans the work and hands out tasks. The executors read the data, and do the work. And rows stay down, unless you ask for them.”
- **s9_2**. On screen: The home shot during the count: four of the eight core slots lit, four dark. Next episode title: Piles of records.
  Narration: “One loose end: our count read the table with 4 tasks, but we have 8 cores. Why only four? Next time: piles of records.”
- **s9_3**. On screen: Series end card with the next episode title.
  Narration: “This has been Spark, Visually. Thanks for watching the cluster with us.”

## Open questions

1. S08 says the real work runs "in Java, not Python". Strictly it is the JVM (Spark is written mostly in Scala, and whole-stage code generation emits Java). Is "in Java" fair for this audience, or misleading?
2. S04: "With Spark Connect, the notebook is a small client: it sends each query to a driver on the cluster, and gets back only the result." Is that accurate for the Databricks compute that uses Spark Connect (serverless, and standard access mode)?
3. S07: "Collect lands every row on the driver: over 500 megabytes of memory" for about a million rows. The figure is Python's memory for the collected rows, measured here. Is the claim stated fairly, and does a Databricks driver differ in a way worth saying?
