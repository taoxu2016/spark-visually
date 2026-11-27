# Accuracy review: Spark, Visually · Episode 5: Everyone Sends to Everyone

You are reviewing the script of one episode of *Spark, Visually*, an animated video series that shows what an Apache
Spark cluster does when PySpark code runs. Its viewers write PySpark on Databricks but have never watched a job run.
Another model wrote the script. You are the independent second opinion on technical accuracy, after the series'
owner approved it. The episode is already voiced and animated, so keep each fix to the words that must change. Review accuracy only, not style or pacing.

## Ground rules

- Check every claim against Apache Spark 4.3.0 as the run below used it: PySpark in local mode, default settings
  unless stated, adaptive query execution on. If your knowledge rests on an earlier release, say which. Where
  Databricks Runtime would behave differently in a way a Databricks user would notice, say so.
- The measured facts below were captured from Spark's own plans, event log and hash function by the episode's numbers
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
2. Every statement about Spark is correct as worded: the hash and its remainder, bins written to local disk, the wall, fetching only bins that hold rows, partial aggregation, `collect_list`, shuffle bytes and their fixed overhead, the number of reducers and adaptive coalescing.
3. The code on screen does what the narration says it does.
4. Nothing contradicts what earlier episodes taught, and nothing reinforces the misconceptions this episode targets.
5. The open questions at the end.
6. The five problems earlier reviews of this series kept finding. Look for each directly:
   - a setting the picture relies on that the screen doesn't show (adaptive execution, a broadcast threshold,
     filter pushdown);
   - a comparison or a check that sets two different queries against each other, or passes for the wrong reason;
   - task counts that leave out a final task, or stage counts that leave out a skipped stage;
   - a claim true of this small example but said as a rule (or the reverse);
   - behaviour that differs over Spark Connect: when errors appear, what `explain()` shows, where the plan lives.

## How to report

**A. Findings**, most serious first, each labelled Wrong (false as worded), Misleading (true, but leaves a wrong idea)
or Imprecise (worth a tighter wording). For each: the clip id; the sentence or visual, quoted; the problem; a suggested
wording in the script's style (short, plain, written to be spoken); your evidence and confidence.

**B. Open questions**: your answer to each, in a sentence or two.

**C. Verified**: what you checked and found correct, briefly.

**D. Unverified**: anything you couldn't settle, with the snippet to run.

## The episode

Part I, Episode 5: *Everyone Sends to Everyone* (the shuffle), module M5 · Everyone sends to everyone.

- **Question:** What happens when rows with the same key sit on different machines?
- **Learning objective:** The viewer can describe both halves of a shuffle, count its blocks, explain why it costs serialisation, disk, network and a wall, and show how partial aggregation shrinks it.
- **Core concept:** A shuffle re-deals the whole table by key. Every map task writes one bin per destination to its local disk; every reduce task fetches its bin from every map task.

### Misconceptions this episode targets

- Shuffle data goes straight over the network. (It is written to local disk first, then fetched.)
- `groupBy` moves every row. (Not when the partial results are small.)
- Each key gets its own reduce partition. (Many keys share one, and some partitions get none.)
- More shuffle partitions are always better. (Each one is a task with a fixed cost and a small file.)

### What earlier episodes taught

- Episode 1, Who does the work: the driver runs the notebook's code, plans and schedules; executors read their own files from storage, never through the driver. In a count, each task counts its file's rows and the partial counts go to one final task, which adds them and sends one number (103) to the driver. collect() brings every row to the driver. DataFrame work runs inside the executors; a Python UDF starts Python workers beside each executor.
- Episode 2, Piles of records: a partition, called a pile, is the rows one task processes on one core. Reads cut files into piles; data built in code starts as one pile per core; after a shuffle there are 200 piles by default, unless adaptive execution merges small ones. coalesce merges piles where they stand; repartition re-deals every row. A stage ends when its slowest task ends.
- Episode 3, Nothing happens until you ask: transformations add steps to a plan and run nothing; an action runs the whole plan from storage, in one pass per pile, and every action starts again from storage unless something is cached. Its closing line: a count of the filtered rows was one action, yet Spark ran it as 2 jobs and 2 stages.
- Episode 4, Cut at the walls (written after this episode, which was made first as the pilot): Spark cuts a plan into stages where rows are exchanged; one task per pile; between stages stands a wall, and nothing beyond it starts until everything before it is done; with adaptive execution one action can run as several jobs.

### The data

The cast: 103 simulated patients in five clinics (Alder, Birch, Cedar, Dune, Elm), written as four Parquet files that Spark reads as four piles of 28, 25, 25 and 25 rows. Column x = 1 marks a higher-risk patient (46 of the 103); column y marks a readmission (42). The animation always shows a fixed cluster of one driver and four executors with two cores each; the numbers come from one machine in local mode. The shuffles run with 5 reduce partitions, then 3; the default of 200 appears on a settings card.

### Deliberate choices (not errors)

- Adaptive coalescing is switched off for the cast's shuffles, so every bin stays visible; S08 says so, and the one run with it on merges the 200 piles into a single task.
- Local mode has no network, so the figures for what crosses between machines (9 bins, 79 patients) are worked out from the drawn placement (pile k on executor k, reducer r on executor r mod 4), and S06 labels its picture "as placed on the home shot". Every other figure comes from Spark's event log or its own hash.
- A bin is the part of a map task's shuffle output bound for one reducer. Spark writes each map task's bins as one sorted file with an index; the series draws the bins.
- The blueprint's scale beat on the crowd (a large simulated registry) is left out until the crowd exists.
- This episode was made first, as the pilot, so it recaps the wall itself in S03.
- Plain words on purpose: a pile is a partition, a wall is the boundary between stages, the cast is the example data. In the On screen notes, a chip is one record, coloured by its clinic; a tray is a pile as drawn; a bench is an executor; the driver desk is the driver; the shelf is storage.

## Code on screen

These lines run verbatim in the numbers file, in this order.

```python
spark.conf.set("spark.sql.shuffle.partitions", 5)
df.repartition("clinic").collect()
df.groupBy("clinic").count().collect()
df.groupBy("clinic").agg(F.collect_list("y")).collect()
spark.conf.set("spark.sql.shuffle.partitions", 3)
```

## Measured facts

### `df.groupBy("clinic").count()`: its plan (explain("formatted"))

```text
== Physical Plan ==
AdaptiveSparkPlan (5)
+- HashAggregate (4)
   +- Exchange (3)
      +- HashAggregate (2)
         +- Scan parquet  (1)


(1) Scan parquet 
Output [1]: [clinic#5]
Batched: true
Location: InMemoryFileIndex [file:/tmp/sv_e05/cast]
ReadSchema: struct<clinic:string>

(2) HashAggregate
Input [1]: [clinic#5]
Keys [1]: [clinic#5]
Functions [1]: [partial_count(1)]
Aggregate Attributes [1]: [count#98L]
Results [2]: [clinic#5, count#99L]

(3) Exchange
Input [2]: [clinic#5, count#99L]
Arguments: hashpartitioning(clinic#5, 5), ENSURE_REQUIREMENTS, [plan_id=235]

(4) HashAggregate
Input [2]: [clinic#5, count#99L]
Keys [1]: [clinic#5]
Functions [1]: [count(1)]
Aggregate Attributes [1]: [count(1)#45L]
Results [2]: [clinic#5, count(1)#45L AS count#40L]

(5) AdaptiveSparkPlan
Output [2]: [clinic#5, count#40L]
Arguments: isFinalPlan=false
```

### `df.groupBy("clinic").agg(F.collect_list("y"))`: its plan (explain("formatted"))

```text
== Physical Plan ==
AdaptiveSparkPlan (5)
+- ObjectHashAggregate (4)
   +- Exchange (3)
      +- ObjectHashAggregate (2)
         +- Scan parquet  (1)


(1) Scan parquet 
Output [2]: [clinic#5, y#7]
Batched: true
Location: InMemoryFileIndex [file:/tmp/sv_e05/cast]
ReadSchema: struct<clinic:string,y:int>

(2) ObjectHashAggregate
Input [2]: [clinic#5, y#7]
Keys [1]: [clinic#5]
Functions [1]: [partial_collect_list(y#7, 0, 0, true)]
Aggregate Attributes [1]: [buf#100]
Results [2]: [clinic#5, buf#101]

(3) Exchange
Input [2]: [clinic#5, buf#101]
Arguments: hashpartitioning(clinic#5, 5), ENSURE_REQUIREMENTS, [plan_id=250]

(4) ObjectHashAggregate
Input [2]: [clinic#5, buf#101]
Keys [1]: [clinic#5]
Functions [1]: [collect_list(y#7, 0, 0, true)]
Aggregate Attributes [1]: [collect_list(y#7)#50]
Results [2]: [clinic#5, collect_list(y#7)#50 AS collect_list(y)#51]

(5) AdaptiveSparkPlan
Output [2]: [clinic#5, collect_list(y)#51]
Arguments: isFinalPlan=false
```

### `df.repartition("clinic")`: its plan (explain("formatted"))

```text
== Physical Plan ==
AdaptiveSparkPlan (3)
+- Exchange (2)
   +- Scan parquet  (1)


(1) Scan parquet 
Output [4]: [patient#4, clinic#5, x#6, y#7]
Batched: true
Location: InMemoryFileIndex [file:/tmp/sv_e05/cast]
ReadSchema: struct<patient:int,clinic:string,x:int,y:int>

(2) Exchange
Input [4]: [patient#4, clinic#5, x#6, y#7]
Arguments: hashpartitioning(clinic#5, 5), REPARTITION_BY_COL, [plan_id=260]

(3) AdaptiveSparkPlan
Output [4]: [patient#4, clinic#5, x#6, y#7]
Arguments: isFinalPlan=false
```

### Everything else the numbers file recorded

Ids, counts and orders are exact; fields in milliseconds come from a one-CPU machine and matter only as order.

```json
{
 "series": "Spark, Visually",
 "part": "I · How Spark runs a job",
 "episode": 5,
 "title": "Everyone Sends to Everyone",
 "engine": {"spark": "4.3.0", "master": "local[8]", "settings": {"spark.sql.adaptive.enabled": "true", "spark.sql.adaptive.coalescePartitions.enabled": "false (true only for count_r200_coalesced)", "spark.sql.shuffle.partitions": "5 then 3 (code panel), then 200 (the default, on a settings card)", "spark.sql.files.maxPartitionBytes": "134217728b", "spark.sql.files.openCostInBytes": "4194304b"}, "note": "local mode: exact plans, partitions, records and bytes; no network"},
 "partial_aggregation": {"scale": 10000, "cast": {"with": {"records": 20, "bytes": 998}, "without": {"records": 103, "bytes": 1075}}, "scaled": {"with": {"records": 40, "bytes": 2048}, "without": {"records": 1030000, "bytes": 1042872}}},
 "cast": {"patients": 103, "readmitted": 42, "per_clinic": {"Birch": 30, "Alder": 30, "Elm": 11, "Dune": 12, "Cedar": 20}, "seed": 2026},
 "trays": {"0": {"Alder": 9, "Birch": 6, "Cedar": 10, "Dune": 2, "Elm": 1}, "1": {"Alder": 5, "Birch": 12, "Cedar": 2, "Dune": 3, "Elm": 3}, "2": {"Alder": 8, "Birch": 8, "Cedar": 3, "Dune": 3, "Elm": 3}, "3": {"Alder": 8, "Birch": 4, "Cedar": 5, "Dune": 4, "Elm": 4}},
 "tray_sizes": [28, 25, 25, 25],
 "hash": {"Alder": {"hash": -2141637979, "3": 2, "4": 1, "5": 1, "8": 5, "200": 21}, "Birch": {"hash": 194916515, "3": 2, "4": 3, "5": 0, "8": 3, "200": 115}, "Cedar": {"hash": -788428916, "3": 1, "4": 0, "5": 4, "8": 4, "200": 84}, "Dune": {"hash": -315170486, "3": 1, "4": 2, "5": 4, "8": 2, "200": 114}, "Elm": {"hash": 1141522469, "3": 2, "4": 1, "5": 4, "8": 5, "200": 69}},
 "occupancy": {"3": {"used": 2, "empty": 1, "shared": {"1": ["Cedar", "Dune"], "2": ["Alder", "Birch", "Elm"]}}, "4": {"used": 4, "empty": 0, "shared": {"1": ["Alder", "Elm"]}}, "5": {"used": 3, "empty": 2, "shared": {"4": ["Cedar", "Dune", "Elm"]}}, "8": {"used": 4, "empty": 4, "shared": {"5": ["Alder", "Elm"]}}, "200": {"used": 5, "empty": 195, "shared": {}}},
 "r3_patients_per_reducer": {"1": 32, "2": 71},
 "shuffles": {"all_rows": {"reduce_partitions": 5, "map_tasks": 4, "reduce_tasks": 5, "blocks_possible": 20, "blocks_nonempty": 12, "blocks_fetched": 12, "records_written": [28, 25, 25, 25], "bytes_written": [551, 471, 547, 523], "records_read": [30, 30, 0, 0, 43], "bytes_read": [615, 640, 0, 0, 837], "bins": {"0,0": 6, "0,1": 9, "0,4": 13, "1,0": 12, "1,1": 5, "1,4": 8, "2,0": 8, "2,1": 8, "2,4": 9, "3,0": 4, "3,1": 8, "3,4": 13}, "home_shot_blocks_crossing_network": 9, "home_shot_records_crossing_network": 79, "total_bytes_written": 2092, "total_records_written": 103}, "count": {"reduce_partitions": 5, "map_tasks": 4, "reduce_tasks": 5, "blocks_possible": 20, "blocks_nonempty": 12, "blocks_fetched": 12, "records_written": [5, 5, 5, 5], "bytes_written": [248, 254, 243, 253], "records_read": [4, 4, 0, 0, 12], "bytes_read": [284, 283, 0, 0, 431], "bins": {"0,0": 1, "0,1": 1, "0,4": 3, "1,0": 1, "1,1": 1, "1,4": 3, "2,0": 1, "2,1": 1, "2,4": 3, "3,0": 1, "3,1": 1, "3,4": 3}, "home_shot_blocks_crossing_network": 9, "home_shot_records_crossing_network": 15, "total_bytes_written": 998, "total_records_written": 20}, "collect_list": {"reduce_partitions": 5, "map_tasks": 4, "reduce_tasks": 5, "blocks_possible": 20, "blocks_nonempty": 12, "blocks_fetched": 12, "records_written": [5, 5, 5, 5], "bytes_written": [387, 378, 361, 376], "records_read": [4, 4, 0, 0, 12], "bytes_read": [430, 427, 0, 0, 645], "bins": {"0,0": 1, "0,1": 1, "0,4": 3, "1,0": 1, "1,1": 1, "1,4": 3, "2,0": 1, "2,1": 1, "2,4": 3, "3,0": 1, "3,1": 1, "3,4": 3}, "home_shot_blocks_crossing_network": 9, "home_shot_records_crossing_network": 15, "total_bytes_written": 1502, "total_records_written": 20}, "count_r3": {"reduce_partitions": 3, "map_tasks": 4, "reduce_tasks": 3, "blocks_possible": 12, "blocks_nonempty": 8, "blocks_fetched": 8, "records_written": [5, 5, 5, 5], "bytes_written": [193, 201, 193, 195], "records_read": [0, 8, 12], "bytes_read": [0, 356, 426], "bins": {"0,1": 2, "0,2": 3, "1,1": 2, "1,2": 3, "2,1": 2, "2,2": 3, "3,1": 2, "3,2": 3}, "home_shot_blocks_crossing_network": 6, "home_shot_records_crossing_network": 15, "total_bytes_written": 782, "total_records_written": 20}, "count_r200": {"reduce_partitions": 200, "map_tasks": 4, "reduce_tasks": 200, "blocks_possible": 800, "blocks_nonempty": 20, "blocks_fetched": 20, "records_written": [5, 5, 5, 5], "bytes_written": [355, 353, 354, 353], "records_read": [0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 4, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 4, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 4, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 4, 4, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0], "bytes_read": [0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 283, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 282, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 283, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 283, 284, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0], "bins": {"0,21": 1, "0,69": 1, "0,84": 1, "0,114": 1, "0,115": 1, "1,21": 1, "1,69": 1, "1,84": 1, "1,114": 1, "1,115": 1, "2,21": 1, "2,69": 1, "2,84": 1, "2,114": 1, "2,115": 1, "3,21": 1, "3,69": 1, "3,84": 1, "3,114": 1, "3,115": 1}, "home_shot_blocks_crossing_network": 15, "home_shot_records_crossing_network": 15, "total_bytes_written": 1415, "total_records_written": 20}},
 "count_r200_coalesced_reduce_tasks": 1,
 "driver": {"collect_result_bytes": 8681, "count_result_bytes": 3895, "count_partials_to_final_task": 4},
 "checks": [
  {"name": "pinned engine: Apache Spark 4.3.0", "ok": true, "detail": "4.3.0"},
  {"name": "the cast is four files and reads as four trays", "ok": true, "detail": "4 files, 4 partitions"},
  {"name": "trays hold all 103 patients", "ok": true, "detail": "[28, 25, 25, 25]"},
  {"name": "every clinic's rows are spread over the trays as the cast says", "ok": true, "detail": ""},
  {"name": "Alder's chips sit on all four benches (the hook)", "ok": true, "detail": "[9, 5, 8, 8]"},
  {"name": "count() is 103", "ok": true, "detail": "103"},
  {"name": "line 1 sets five reducers", "ok": true, "detail": ""},
  {"name": "lines 2 to 4 are actions, and run as typed", "ok": true, "detail": ""},
  {"name": "the count plan has one Exchange, hash-partitioned by clinic into 5", "ok": true, "detail": ""},
  {"name": "the count plan aggregates twice: partial before the wall, final after it", "ok": true, "detail": ""},
  {"name": "groupBy count matches the cast", "ok": true, "detail": "{'Birch': 30, 'Alder': 30, 'Elm': 11, 'Dune': 12, 'Cedar': 20}"},
  {"name": "each clinic lands where pmod(hash, 5) says (count)", "ok": true, "detail": ""},
  {"name": "each clinic lands where pmod(hash, 5) says (every chip)", "ok": true, "detail": ""},
  {"name": "each clinic lands where pmod(hash, 5) says (collect_list)", "ok": true, "detail": ""},
  {"name": "collect_list carries every value", "ok": true, "detail": "{'Birch': 30, 'Alder': 30, 'Elm': 11, 'Dune': 12, 'Cedar': 20}"},
  {"name": "each clinic lands where pmod(hash, 3) says", "ok": true, "detail": ""},
  {"name": "each clinic lands where pmod(hash, 200) says", "ok": true, "detail": ""},
  {"name": "all_rows: one map task per tray", "ok": true, "detail": "4"},
  {"name": "all_rows: one reduce task per reduce partition", "ok": true, "detail": "5"},
  {"name": "all_rows: records written per bin agree with the event log", "ok": true, "detail": "103 written, 103 read"},
  {"name": "all_rows: reducers fetch exactly the non-empty bins", "ok": true, "detail": "12 fetched, 12 non-empty"},
  {"name": "count: one map task per tray", "ok": true, "detail": "4"},
  {"name": "count: one reduce task per reduce partition", "ok": true, "detail": "5"},
  {"name": "count: records written per bin agree with the event log", "ok": true, "detail": "20 written, 20 read"},
  {"name": "count: reducers fetch exactly the non-empty bins", "ok": true, "detail": "12 fetched, 12 non-empty"},
  {"name": "collect_list: one map task per tray", "ok": true, "detail": "4"},
  {"name": "collect_list: one reduce task per reduce partition", "ok": true, "detail": "5"},
  {"name": "collect_list: records written per bin agree with the event log", "ok": true, "detail": "20 written, 20 read"},
  {"name": "collect_list: reducers fetch exactly the non-empty bins", "ok": true, "detail": "12 fetched, 12 non-empty"},
  {"name": "count_r3: one map task per tray", "ok": true, "detail": "4"},
  {"name": "count_r3: one reduce task per reduce partition", "ok": true, "detail": "3"},
  {"name": "count_r3: records written per bin agree with the event log", "ok": true, "detail": "20 written, 20 read"},
  {"name": "count_r3: reducers fetch exactly the non-empty bins", "ok": true, "detail": "8 fetched, 8 non-empty"},
  {"name": "count_r200: one map task per tray", "ok": true, "detail": "4"},
  {"name": "count_r200: one reduce task per reduce partition", "ok": true, "detail": "200"},
  {"name": "count_r200: records written per bin agree with the event log", "ok": true, "detail": "20 written, 20 read"},
  {"name": "count_r200: reducers fetch exactly the non-empty bins", "ok": true, "detail": "20 fetched, 20 non-empty"},
  {"name": "every chip crosses the wall when nothing is combined", "ok": true, "detail": ""},
  {"name": "partial counts cut the rows crossing the wall (the same count, with and without them)", "ok": true, "detail": "{'with': {'records': 20, 'bytes': 998}, 'without': {'records': 103, 'bytes': 1075}}"},
  {"name": "on the cast, bytes barely fall: each bin's fixed overhead outweighs the rows saved", "ok": true, "detail": "{'with': {'records': 20, 'bytes': 998}, 'without': {'records': 103, 'bytes': 1075}}"},
  {"name": "at ten thousand times the cast, partial counts cut the bytes by over 99 percent", "ok": true, "detail": "{'with': {'records': 40, 'bytes': 2048}, 'without': {'records': 1030000, 'bytes': 1042872}}"},
  {"name": "collect_list sends as many records as count, but more bytes", "ok": true, "detail": ""},
  {"name": "adaptive coalescing merges the 200 tiny reduce partitions", "ok": true, "detail": "1 reduce tasks"},
  {"name": "count(): four partial counts meet in one final task, which returns one number", "ok": true, "detail": ""}
 ]
}
```

## The script

Each clip is one stretch of voiceover. "On screen" describes the animation; "Narration" is spoken.

### S01 · Cold open

- **s1_1**. On screen: The home shot. Four executor benches, each holding a tray of chips in the clinic hues; the trays read 28, 25, 25 and 25 rows. The code panel is empty.
  Narration: “Here's a simple question: how many patients does each clinic have? Our 103 patients are spread across four machines, in four piles.”
- **s1_2**. On screen: Alder's orange chips pulse on every bench, with counts 9, 5, 8 and 8.
  Narration: “Take Alder. It has 9 patients on this machine, 5 on this one, and 8 on each of the last two. No machine can count Alder by itself. Alder's rows have to meet.”
- **s1_3**. On screen: Wrong method, sketched once: every chip streams up to the driver and its memory gauge rises. The shot rewinds.
  Narration: “We could send every row to the driver and count there. For 103 rows, fine. For a whole registry, the driver chokes. So Spark does something smarter: it moves rows sideways, between the machines.”

### S02 · Title

- **s2_1**. On screen: Series wordmark, Spark, Visually; Part I, How Spark runs a job; the episode title.
  Narration: “Welcome to Spark, Visually. Today: everyone sends to everyone.”

### S03 · The wall

- **s3_1**. On screen: The plan tree on the driver desk: scan, partial count, Exchange, final count. A dashed wall drops across the executors' lanes at the Exchange.
  Narration: “Every Spark plan is cut wherever rows must change machines. At each cut stands a wall: no work on the far side starts until all the work on the near side is done.”
- **s3_2**. On screen: The camera pushes in on the wall.
  Narration: “Today, we walk through that wall, and watch what crosses it.”

### S04 · Into bins

- **s4_1**. On screen: Code panel, line 1 glows: spark.conf.set("spark.sql.shuffle.partitions", 5). Five reducer slots, 0 to 4, appear ghosted beyond the wall. Beside each bench, five empty bins numbered 0 to 4.
  Narration: “On the far side of the wall, we'll have five reduce tasks. Five clinics, five reducers: one each, you'd hope. So every pile gets sorted into five bins, one for each reducer.”
- **s4_2**. On screen: Line 2 glows: df.repartition("clinic").collect(). One chip is lifted; its clinic name passes through a hash box; a number comes out; the chip drops into that bin. Over the box: pmod(hash(clinic), 5).
  Narration: “Which bin? Spark turns the clinic's name into a big number, called a hash, and keeps the positive remainder after dividing by five. The same name gives the same bin, on every machine. That's how Alder's rows will meet.”
- **s4_3**. On screen: All four benches sort at once. Birch's chips drop into bin 0, Alder's into bin 1, and Cedar's, Dune's and Elm's all into bin 4. Bins 2 and 3 stay empty on every bench.
  Narration: “Now watch where the clinics go. Birch, bin 0. Alder, bin 1. And Cedar, Dune and Elm all land in bin 4. Two bins get nothing at all. A hash scatters keys. It doesn't deal them out evenly.”
- **s4_4**. On screen: Each bench writes its bins to a small disk beside it, labelled local disk. The network band stays dark.
  Narration: “Each task writes its bins to its own machine's disk. Notice that nothing has crossed the network yet.”

### S05 · Everyone waits

- **s5_1**. On screen: The wall glows. The map tasks finish one by one; each finished bench shows a tick and waits.
  Narration: “So why the wall? Why can't a reducer start as soon as one pile is sorted? Because every reducer needs a piece of every pile. Until the last pile is sorted, no reducer has all of its rows.”
- **s5_2**. On screen: The last bench ticks, and the wall opens.
  Narration: “So the fastest machines wait for the slowest. Remember that wait. It comes back.”

### S06 · Everyone fetches

- **s6_1**. On screen: Five reduce tasks take core slots: reducers 0 and 4 on the first bench, 1 on the second, 2 on the third, 3 on the fourth. Each reaches into every bench's disk for its bin; beams cross the network band. Reducers 2 and 3 have nothing to fetch: Spark knows their bins are empty, so they stay put.
  Narration: “Now the second half. Each reducer visits every machine and fetches its bin. Four piles and five reducers make 20 possible bins. Only 12 hold any rows, and only those 12 are fetched.”
- **s6_2**. On screen: Beams that leave their own bench turn yellow, with a running total: 9 of 12 bins, 79 of 103 rows. The 3 bins read on their own bench stay grey. Small label: reducers as placed on the home shot.
  Narration: “With our reducers placed like this, 9 of those bins cross the network: 79 of our 103 patients, on the move.”
- **s6_3**. On screen: Results land. Reducer 0 holds Birch's 30, reducer 1 Alder's 30, reducer 4 Cedar's 20, Dune's 12 and Elm's 11, 43 together. Reducers 2 and 3 are empty. Each clinic's count appears beside it.
  Narration: “Now each clinic is in one place. Birch's 30 on one reducer, Alder's 30 on another, and 43 patients from three clinics, crowded onto a third. And two reducers did nothing at all.”

### S07 · Send less

- **s7_1**. On screen: Line 3 glows: df.groupBy("clinic").count().collect(). Rewind to the map side. Each tray's chips collapse into small number cards, one per clinic; Alder's cards read 9, 5, 8 and 8.
  Narration: “But to count patients, do we really need to move every patient? Each machine can count its own pile first, and send just the numbers.”
- **s7_2**. On screen: The bins now hold number cards. A ledger for the same count with and without partial counts: rows crossing the wall, 103 to 20; bytes written, 1,075 to 998; and at ten thousand times the cast, 1,042,872 bytes to 2,048.
  Narration: “Now each machine sends one number per clinic: 20 small rows instead of 103. On a cast this small, bytes barely fall, from 1,075 to 998, because every bin carries a fixed overhead. Scale the cast up ten thousand times, and the count writes about 2,000 bytes instead of over a million.”
- **s7_3**. On screen: The plan tree: a partial count before the Exchange, a final count after it.
  Narration: “Spark does this by itself. Its plan counts twice: a partial count before the wall, and a final count after it.”
- **s7_4**. On screen: Line 4 glows: df.groupBy("clinic").agg(F.collect_list("y")).collect(). The number cards turn back into bundles of chips. Ledger: 20 rows, 1,502 bytes.
  Narration: “Not every question shrinks like that. Ask for the list of every patient's outcome, and every value still has to travel. Still 20 rows, but each one carries a whole list, and the bytes climb to 1,502.”

### S08 · Collisions

- **s8_1**. On screen: Line 5 glows: spark.conf.set("spark.sql.shuffle.partitions", 3). Three reducers. Bin 2 fills with Alder, Birch and Elm (71 patients), bin 1 with Cedar and Dune (32), and bin 0 stays empty.
  Narration: “What if we ask for three reducers instead? Alder, Birch and Elm all hash to the same one: 71 patients. Cedar and Dune share another: 32. The third gets nothing.”
- **s8_2**. On screen: The three reducer trays stand side by side as columns of 71, 32 and 0 patients, labelled: patients, if every row moved.
  Narration: “A stage ends when its busiest task ends. On real data, one crowded reducer can hold up a whole job.”
- **s8_3**. On screen: Settings card: spark.sql.shuffle.partitions, default 200. The five clinics light 5 of 200 reducer slots; then all 200, busy and empty alike, merge into one task.
  Narration: “And Spark's default is 200 reducers. For five clinics, that's 195 tasks with nothing to read. Normally, adaptive execution sees that the bins are tiny, and merges them into a single task. We switched that off today, so you could see every bin.”

### S09 · Recap

- **s9_1**. On screen: Three recap cards with icons: bins on a disk, a wall, number cards.
  Narration: “Let's gather it up. A shuffle sorts every pile into bins by key, and writes them to local disk. Then a wall, and each reducer fetches its bin from every machine. And Spark sends less when it can: partial results instead of every row.”
- **s9_2**. On screen: The hash box again, sending the five clinics into bins 0, 1 and 4.
  Narration: “Along the way, the hash put five clinics into three bins, and left two reducers idle.”
- **s9_3**. On screen: Teaser: the code panel and the plan tree side by side, a question mark over hashpartitioning. Next episode title: Say what, not how.
  Narration: “Our code never said hash, or bins. We asked a question, and Spark chose how to answer it. Next time: who makes that choice, and how.”
- **s9_4**. On screen: Series end card with the next episode title.
  Narration: “This has been Spark, Visually. Thanks for watching the cluster with us.”

## Open questions

1. Spark places reduce tasks differently on every run, so S06 labels its picture "as placed on the home shot", and its crossing figures follow that placement. Is the label enough?
2. S08: "Normally, adaptive execution spots the empty bins and merges them into a single task." Is that accurate for Spark 4.3's coalescing, which merges small post-shuffle partitions toward an advisory size?
3. S07: "Rows fall further than bytes, because every bin carries a small fixed overhead." Is the reason right, given what Spark's shuffle write metrics count?
