# Accuracy review: Spark, Visually · Episode 6: Say What, Not How

You are reviewing the script of one episode of *Spark, Visually*, an animated video series that shows what an Apache
Spark cluster does when PySpark code runs. Its viewers write PySpark on Databricks but have never watched a job run.
Another model wrote the script. You are the independent second opinion on technical accuracy, on draft 2. A first
review found six things, and all six were changed; check the new wording as closely as the rest. Review accuracy only, not style or pacing.

## Ground rules

- Check every claim against Apache Spark 4.3.0 as the run below used it: PySpark in local mode, default settings
  unless stated, adaptive query execution on. If your knowledge rests on an earlier release, say which. Where
  Databricks Runtime would behave differently in a way a Databricks user would notice, say so.
- The measured facts below were captured from Spark's own plans and event log, through Spark Connect as well as a classic session, and, for the column data a query needs, from the Parquet file's own metadata (the sizes of its column chunks), by the episode's numbers
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
2. Every statement about Spark is correct as worded: the plan at each phase (parsed, analysed, optimised, physical), DataFrame and SQL reaching the same plan, analysis of names and when a misspelt name fails (classic and over Spark Connect), the optimiser's rewrites (pushdown, the inferred `isnotnull`, column pruning), Parquet row-group skipping and column chunks, the split aggregate, `explain("formatted")`, and a Python UDF blocking pushdown.
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

Part I, Episode 6: *Say What, Not How* (from your code to Spark's plan), module M6 · Say what, not how.

- **Question:** How does a DataFrame or SQL query become the plan Spark runs?
- **Learning objective:** The viewer can follow a query from parsed to analysed, optimised and physical plan, show that DataFrame and SQL forms give the same plan, and spot column pruning, filter pushdown and the chosen aggregate in `explain("formatted")`.
- **Core concept:** You describe the result; Catalyst chooses the steps. Because a DataFrame carries a schema and a plan, Spark can move filters, drop unused columns and pick operators, which it cannot do inside a Python function.

### Misconceptions this episode targets

- SQL and DataFrames run differently. (Same optimiser, same plan.)
- Spark runs steps in the order I wrote them. (It reorders whatever is safe to reorder.)
- A Python UDF costs only its own work. (It blocks optimisation and moves rows to Python and back; vectorised pandas UDFs reduce that cost but do not remove it.)
- `explain()` shows exactly what will run. (With adaptive execution it shows the starting plan; Episode 12.)

### What earlier episodes taught

- Episode 1, Who does the work: the driver runs the notebook's code, plans and schedules; executors read their own files from storage, never through the driver. In a count, each task counts its file's rows and the partial counts go to one final task, which adds them and sends one number (103) to the driver. collect() brings every row to the driver. DataFrame work runs inside the executors; a Python UDF starts Python workers beside each executor.
- Episode 2, Piles of records: a partition, called a pile, is the rows one task processes on one core. Reads cut files into piles; data built in code starts as one pile per core; after a shuffle there are 200 piles by default, unless adaptive execution merges small ones. coalesce merges piles where they stand; repartition re-deals every row. A stage ends when its slowest task ends.
- Episode 3, Nothing happens until you ask: transformations add steps to a plan and run nothing; an action runs the whole plan from storage, in one pass per pile, and every action starts again from storage unless something is cached. Its closing line: a count of the filtered rows was one action, yet Spark ran it as 2 jobs and 2 stages.
- Episode 4, Cut at the walls (written after this episode, which was made first as the pilot): Spark cuts a plan into stages where rows are exchanged; one task per pile; between stages stands a wall, and nothing beyond it starts until everything before it is done; with adaptive execution one action can run as several jobs.
- Episode 5, Everyone sends to everyone: in a shuffle, each map task writes its output into bins by destination on its executor's local disk; after the wall, each reduce task fetches its bin from every map task. Partial aggregation sends one count per clinic per pile instead of every row; at scale that cuts the bytes sharply.

### The data

The cast: 103 simulated patients in five clinics (Alder, Birch, Cedar, Dune, Elm), written as four Parquet files that Spark reads as four piles of 28, 25, 25 and 25 rows. Column x = 1 marks a higher-risk patient (46 of the 103); column y marks a readmission (42). The animation always shows a fixed cluster of one driver and four executors with two cores each; the numbers come from one machine in local mode. The table is reached as `cast` (a temporary view over the cast's Parquet files). S05 and S07 also use the cast ten thousand times over (1,030,000 rows), sorted by risk and written as one Parquet file in 512 KB row groups (25 of them).

### Deliberate choices (not errors)

- The blueprint's crowd (a wide simulated registry) doesn't exist yet, so the storage scene uses the cast ten thousand times over, sorted by risk; "three of twenty columns" becomes two of four. The narration says the sizes belong to this sorted file.
- The storage scene's sizes are the column data the query needs, from the file's own metadata: the column chunks of the columns in the scan's ReadSchema, in the row groups it reads. They are labelled so on screen. The reader itself fetches more on this tiny file, because it reads through a 64 KiB buffer; the event log's bytes read record that, and are kept in the facts and not said.
- The SQL names its count (`AS count`) so both forms name the column alike; the plans are compared with Spark's expression ids and plan ids set aside.
- The blueprint's constant-folding example is replaced by a rewrite this plan really shows: the inferred `isnotnull(x)`.
- Pushdown switched off appears on a settings card (`spark.conf.set("spark.sql.parquet.filterPushdown", False)`), and the reordering example on another card (`df.groupBy("clinic").count().filter(F.col("clinic") == "Birch")`); both run verbatim. The code panel prints both formatted plans and the four phases (`q1.explain(True)`).
- The physical-plan scene comes before the storage scene, because handing the filter to the read happens in physical planning, not in the optimiser.
- No durations: the UDF's cost is told in rows read and rows sent to Python.
- Plain words on purpose: a pile is a partition, a wall is the boundary between stages, the cast is the example data. In the On screen notes, a chip is one record, coloured by its clinic; a tray is a pile as drawn; a bench is an executor; the driver desk is the driver; the shelf is storage.

## Code on screen

These lines run verbatim in the numbers file, in this order.

```python
q1 = df.filter(F.col("x") == 1).groupBy("clinic").count()
q2 = spark.sql("""SELECT clinic, count(*) AS count
                 FROM cast WHERE x = 1 GROUP BY clinic""")
q1.explain("formatted"); q2.explain("formatted")
q1.explain(True)
df.filter(F.col("xx") == 1)
hi = F.udf(lambda x: x == 1, "boolean")
df.filter(hi("x")).groupBy("clinic").count().collect()
```

## Measured facts

### q1.explain("formatted"), before any run (the code panel's line 4; q2's is the same, ids aside)

```text
== Physical Plan ==
AdaptiveSparkPlan (7)
+- HashAggregate (6)
   +- Exchange (5)
      +- HashAggregate (4)
         +- Project (3)
            +- Filter (2)
               +- Scan parquet  (1)


(1) Scan parquet 
Output [2]: [clinic#5, x#6]
Batched: true
Location: InMemoryFileIndex [file:/tmp/sv_e06/cast]
PushedFilters: [IsNotNull(x), EqualTo(x,1)]
ReadSchema: struct<clinic:string,x:int>

(2) Filter
Input [2]: [clinic#5, x#6]
Condition : (isnotnull(x#6) AND (x#6 = 1))

(3) Project
Output [1]: [clinic#5]
Input [2]: [clinic#5, x#6]

(4) HashAggregate
Input [1]: [clinic#5]
Keys [1]: [clinic#5]
Functions [1]: [partial_count(1)]
Aggregate Attributes [1]: [count#17L]
Results [2]: [clinic#5, count#18L]

(5) Exchange
Input [2]: [clinic#5, count#18L]
Arguments: hashpartitioning(clinic#5, 200), ENSURE_REQUIREMENTS, [plan_id=32]

(6) HashAggregate
Input [2]: [clinic#5, count#18L]
Keys [1]: [clinic#5]
Functions [1]: [count(1)]
Aggregate Attributes [1]: [count(1)#14L]
Results [2]: [clinic#5, count(1)#14L AS count#9L]

(7) AdaptiveSparkPlan
Output [2]: [clinic#5, count#9L]
Arguments: isFinalPlan=false
```

### q2's parsed plan: the SQL before analysis

```text
'Aggregate ['clinic], ['clinic, 'count(1) AS count#15]
+- 'Filter ('x = 1)
   +- 'UnresolvedRelation [cast], [], false
```

### q1's analysed plan

```text
Aggregate [clinic#5], [clinic#5, count(1) AS count#9L]
+- Filter (x#6 = 1)
   +- Relation [patient#4,clinic#5,x#6,y#7] parquet
```

### q1's optimised plan

```text
Aggregate [clinic#5], [clinic#5, count(1) AS count#9L]
+- Project [clinic#5]
   +- Filter (isnotnull(x#6) AND (x#6 = 1))
      +- Relation [patient#4,clinic#5,x#6,y#7] parquet
```

### the reordering card's optimised plan

```text
Aggregate [clinic#5], [clinic#5, count(1) AS count#31L]
+- Project [clinic#5]
   +- Filter (isnotnull(clinic#5) AND (clinic#5 = Birch))
      +- Relation [patient#4,clinic#5,x#6,y#7] parquet
```

### the UDF version's physical plan

```text
AdaptiveSparkPlan isFinalPlan=false
+- HashAggregate(keys=[clinic#5], functions=[count(1)], output=[clinic#5, count#40L])
   +- Exchange hashpartitioning(clinic#5, 200), ENSURE_REQUIREMENTS, [plan_id=283]
      +- HashAggregate(keys=[clinic#5], functions=[partial_count(1)], output=[clinic#5, count#48L])
         +- Project [clinic#5]
            +- Filter pythonUDF0#46: boolean
               +- ArrowEvalPython [<lambda>(x#6)#39], [pythonUDF0#46], 101
                  +- FileScan parquet [clinic#5,x#6] Batched: true, DataFilters: [], Format: Parquet, Location: InMemoryFileIndex(1 paths)[file:/tmp/sv_e06/cast], PartitionFilters: [], PushedFilters: [], ReadSchema: struct<clinic:string,x:int>
```

### Everything else the numbers file recorded

Ids, counts and orders are exact; fields in milliseconds come from a one-CPU machine and matter only as order.

```json
{
 "series": "Spark, Visually",
 "part": "I · How Spark runs a job",
 "episode": 6,
 "title": "Say What, Not How",
 "engine": {"spark": "4.3.0", "master": "local[8]", "cpus": 1, "settings": {"spark.sql.adaptive.enabled": "true", "spark.sql.parquet.filterPushdown": "true", "spark.sql.shuffle.partitions": "200"}, "note": "local mode: exact plans, rows, bytes and row groups; no durations"},
 "statements": [[0, 1], [1, 3], [3, 4], [4, 5], [5, 6], [6, 7], [7, 8]],
 "card_pushdown_off": "spark.conf.set(\"spark.sql.parquet.filterPushdown\", False)",
 "card_reorder": "df.groupBy(\"clinic\").count().filter(F.col(\"clinic\") == \"Birch\")",
 "cast": {"patients": 103, "files": 4, "seed": 2026, "columns": 4},
 "counts": {"Alder": 6, "Cedar": 10, "Dune": 6, "Birch": 24},
 "same_plan": true,
 "read_schema": "struct<clinic:string,x:int>",
 "pushed_filters": "IsNotNull(x), EqualTo(x,1)",
 "formatted_nodes": ["Scan parquet", "Filter", "Project", "HashAggregate", "Exchange", "HashAggregate", "AdaptiveSparkPlan"],
 "misspelt": {"error": "AnalysisException", "unresolved_column": true},
 "connect": {"dataframe_at_its_line": null, "dataframe_when_columns_are_asked": "AnalysisException: UNRESOLVED_COLUMN", "dataframe_at_count": "AnalysisException: UNRESOLVED_COLUMN", "sql_at_its_call": "AnalysisException: UNRESOLVED_COLUMN"},
 "udf": {"node": "ArrowEvalPython", "pushed_filters": "", "rows_to_python": 103},
 "storage": {"scale": 10000, "rows": 1030000, "row_group_bytes": 524288, "row_groups": 25, "row_groups_that_may_hold_x1": 12, "groups": [{"rows": 38976, "x_min": 0, "x_max": 0, "chunk_bytes": {"patient": 156083, "clinic": 108, "x": 103, "y": 103}}, {"rows": 38976, "x_min": 0, "x_max": 0, "chunk_bytes": {"patient": 156099, "clinic": 108, "x": 103, "y": 103}}, {"rows": 38976, "x_min": 0, "x_max": 0, "chunk_bytes": {"patient": 156081, "clinic": 108, "x": 103, "y": 111}}, {"rows": 38976, "x_min": 0, "x_max": 0, "chunk_bytes": {"patient": 156080, "clinic": 108, "x": 103, "y": 103}}, {"rows": 38976, "x_min": 0, "x_max": 0, "chunk_bytes": {"patient": 156087, "clinic": 108, "x": 103, "y": 103}}, {"rows": 38976, "x_min": 0, "x_max": 0, "chunk_bytes": {"patient": 156090, "clinic": 108, "x": 103, "y": 103}}, {"rows": 38976, "x_min": 0, "x_max": 0, "chunk_bytes": {"patient": 156095, "clinic": 121, "x": 103, "y": 114}}, {"rows": 38976, "x_min": 0, "x_max": 0, "chunk_bytes": {"patient": 156085, "clinic": 121, "x": 103, "y": 110}}, {"rows": 38976, "x_min": 0, "x_max": 0, "chunk_bytes": {"patient": 156094, "clinic": 108, "x": 103, "y": 112}}, {"rows": 38976, "x_min": 0, "x_max": 0, "chunk_bytes": {"patient": 156085, "clinic": 108, "x": 103, "y": 103}}, {"rows": 57901, "x_min": 0, "x_max": 0, "chunk_bytes": {"patient": 231871, "clinic": 162, "x": 142, "y": 155}}, {"rows": 58312, "x_min": 0, "x_max": 0, "chunk_bytes": {"patient": 233522, "clinic": 159, "x": 142, "y": 142}}, {"rows": 58372, "x_min": 0, "x_max": 0, "chunk_bytes": {"patient": 233766, "clinic": 145, "x": 142, "y": 142}}, {"rows": 39078, "x_min": 0, "x_max": 1, "chunk_bytes": {"patient": 156501, "clinic": 120, "x": 111, "y": 111}}, {"rows": 38976, "x_min": 1, "x_max": 1, "chunk_bytes": {"patient": 156082, "clinic": 121, "x": 103, "y": 115}}, {"rows": 38976, "x_min": 1, "x_max": 1, "chunk_bytes": {"patient": 156077, "clinic": 108, "x": 103, "y": 103}}, {"rows": 38976, "x_min": 1, "x_max": 1, "chunk_bytes": {"patient": 156084, "clinic": 108, "x": 103, "y": 103}}, {"rows": 38976, "x_min": 1, "x_max": 1, "chunk_bytes": {"patient": 156082, "clinic": 108, "x": 103, "y": 103}}, {"rows": 38976, "x_min": 1, "x_max": 1, "chunk_bytes": {"patient": 156091, "clinic": 108, "x": 103, "y": 111}}, {"rows": 38976, "x_min": 1, "x_max": 1, "chunk_bytes": {"patient": 156092, "clinic": 108, "x": 103, "y": 103}}, {"rows": 38976, "x_min": 1, "x_max": 1, "chunk_bytes": {"patient": 156085, "clinic": 121, "x": 103, "y": 110}}, {"rows": 38976, "x_min": 1, "x_max": 1, "chunk_bytes": {"patient": 156085, "clinic": 108, "x": 103, "y": 103}}, {"rows": 38976, "x_min": 1, "x_max": 1, "chunk_bytes": {"patient": 156097, "clinic": 108, "x": 103, "y": 111}}, {"rows": 57792, "x_min": 1, "x_max": 1, "chunk_bytes": {"patient": 231440, "clinic": 160, "x": 142, "y": 150}}, {"rows": 18001, "x_min": 1, "x_max": 1, "chunk_bytes": {"patient": 72096, "clinic": 68, "x": 64, "y": 72}}], "reads": {"query": {"rows": 465655, "bytes": 767762}, "no_pushdown": {"rows": 1030000, "bytes": 16148}, "all_columns": {"rows": 465655, "bytes": 767762}, "udf": {"rows": 1030000, "bytes": 16148}}, "columns_read": ["clinic", "x"], "column_bytes_needed": {"query": 2590, "no_pushdown": 5618, "all_columns": 1868697, "everything": 4133267}, "note": "column_bytes_needed come from the file's metadata; reads[...].bytes are what the reader fetched (64 KiB buffered reads), kept and not said"},
 "reorder": {"card": "df.groupBy(\"clinic\").count().filter(F.col(\"clinic\") == \"Birch\")", "rows": {"Birch": 30}},
 "checks": [
  {"name": "pinned engine: Apache Spark 4.3.0", "ok": true, "detail": "4.3.0"},
  {"name": "the panel prints both formatted plans, and they match with ids set aside", "ok": true, "detail": ""},
  {"name": "explain(True) prints all four phases", "ok": true, "detail": ""},
  {"name": "the DataFrame and SQL forms give the same answer", "ok": true, "detail": "{'Alder': 6, 'Cedar': 10, 'Dune': 6, 'Birch': 24}"},
  {"name": "SQL and DataFrame code reach the same optimised plan", "ok": true, "detail": ""},
  {"name": "and the same physical plan", "ok": true, "detail": ""},
  {"name": "the SQL's parsed plan names its table unresolved; analysis finds it", "ok": true, "detail": ""},
  {"name": "DataFrame code is analysed as each line is written: its plan already carries types and ids (x#n)", "ok": true, "detail": ""},
  {"name": "a misspelt column fails at analysis, on its own line, before any job", "ok": true, "detail": "{'error': 'AnalysisException', 'unresolved_column': True}"},
  {"name": "the optimiser adds a step nobody wrote: isnotnull(x)", "ok": true, "detail": ""},
  {"name": "the optimised plan still has the Filter over the whole relation: pushdown and the two-column read are decided in physical planning", "ok": true, "detail": ""},
  {"name": "the optimiser keeps only what the query needs: a Project of clinic over the filter", "ok": true, "detail": ""},
  {"name": "written after the count, the filter runs before it: the optimiser moves it below the aggregate", "ok": true, "detail": "{'Birch': 30}"},
  {"name": "the scan reads 2 of the table's 4 columns (pruning)", "ok": true, "detail": "struct<clinic:string,x:int>"},
  {"name": "the filter is pushed into the scan", "ok": true, "detail": "IsNotNull(x), EqualTo(x,1)"},
  {"name": "the physical plan splits the count around an Exchange: partial, then final", "ok": true, "detail": "['Scan parquet', 'Filter', 'Project', 'HashAggregate', 'Exchange', 'HashAggregate', 'AdaptiveSparkPlan']"},
  {"name": "explain() before any action shows the starting plan", "ok": true, "detail": ""},
  {"name": "the UDF version gives the same answer", "ok": true, "detail": "{'Alder': 6, 'Cedar': 10, 'Dune': 6, 'Birch': 24}"},
  {"name": "with the UDF, a Python node evaluates the filter and nothing about x is pushed down", "ok": true, "detail": "ArrowEvalPython; PushedFilters: []"},
  {"name": "over Spark Connect, DataFrame code waits until its plan is first needed; a SQL string still fails at once", "ok": true, "detail": "{'dataframe_at_its_line': None, 'dataframe_when_columns_are_asked': 'AnalysisException: UNRESOLVED_COLUMN', 'dataframe_at_count': 'AnalysisException: UNRESOLVED_COLUMN', 'sql_at_its_call': 'AnalysisException: UNRESOLVED_COLUMN'}"},
  {"name": "the panel's first five statements run no job: building, comparing and explaining plans moves no rows", "ok": true, "detail": ""},
  {"name": "the UDF line reads every patient and sends each one to Python", "ok": true, "detail": "{'rows': 103, 'bytes': 5224}"},
  {"name": "with pushdown, only the row groups whose statistics allow x = 1 are read", "ok": true, "detail": "465,655 of 1,030,000"},
  {"name": "without pushdown every row group is read", "ok": true, "detail": ""},
  {"name": "the column data the query needs is a small part of the file's", "ok": true, "detail": "{'query': 2590, 'no_pushdown': 5618, 'all_columns': 1868697, 'everything': 4133267}"},
  {"name": "the UDF version reads every row: its filter can't be pushed down", "ok": true, "detail": ""}
 ]
}
```

## The script

Each clip is one stretch of voiceover. "On screen" describes the animation; "Narration" is spoken.

### S01 · Cold open

- **s1_1**. On screen: The code panel types one question two ways: the DataFrame line, then the same question in SQL.
  Narration: “Here's one question, written two ways: as DataFrame code, and as SQL. Higher-risk patients, counted by clinic.”
- **s1_2**. On screen: Line 4 glows: both formatted plans appear side by side, node by node; every node matches, and the pairs light together.
  Narration: “Which one runs faster? Neither. Spark turns both into the same plan, step for step.”

### S02 · Title

- **s2_1**. On screen: Series wordmark, Spark, Visually; Part I, How Spark runs a job; the episode title.
  Narration: “Welcome to Spark, Visually. Today: say what, not how.”

### S03 · Analysis

- **s3_1**. On screen: Line 5 glows: q1.explain(True) prints all four phases. The SQL's first plan on the driver desk: plain names, the table unresolved. Analysis looks each one up in the catalog, and each column gains its type.
  Narration: “First, Spark reads what you wrote and looks up every name: the table, and each column, with its type. In classic PySpark, that happens line by line, as you write.”
- **s3_2**. On screen: Line 6 types: df.filter(F.col("xx") == 1). It fails at once with an unresolved column; no job ran. A caption: over Spark Connect, the same DataFrame line passes, and fails when its plan is first needed (at .count(), say); a SQL string still fails at once.
  Narration: “So a misspelt column fails right there, on its own line, before a single task runs. Over Spark Connect, DataFrame code waits until its plan is first needed, but a SQL string still fails at once.”

### S04 · The optimiser

- **s4_1**. On screen: The analysed plan on the desk: count per clinic, filter, read. A new step appears beside the filter, isnotnull(x), and a projection of clinic slides in above it.
  Narration: “Then the optimiser rewrites the plan. It adds a check you never wrote: that x isn't null. And it keeps only what the query needs: clinic, for the count.”
- **s4_2**. On screen: A card runs df.groupBy("clinic").count().filter(F.col("clinic") == "Birch"): in the plan, the filter written last slides below the count.
  Narration: “It reorders what's safe to reorder, too. Write a filter on clinic after the count, and the optimiser moves it before: fewer rows to count. You said what you wanted. Spark chose the steps.”

### S05 · The physical plan

- **s5_1**. On screen: The physical plan on the desk: the read is handed a copy of the filter and reads 2 of the 4 columns (patient and y fade from it); the filter stays above the read; the single count splits into a partial count, an Exchange and a final count, with Episode 4's wall between.
  Narration: “Last, Spark picks real operators. It hands the read a copy of the filter, so the read can skip what can't match, and has it read only 2 of the 4 columns. The filter still runs after the read, to be sure. And the one count becomes two: a partial count on each pile, an exchange, and a final count. That's Episode 4's wall.”
- **s5_2**. On screen: Line 4 glows again: q1.explain("formatted"). Its nodes light from the bottom: Scan parquet with its PushedFilters and ReadSchema, Filter, Project, HashAggregate, Exchange, HashAggregate, AdaptiveSparkPlan.
  Narration: “Explain, formatted, shows the physical plan, node by node: the scan, with the filters it was handed and the two columns it reads; then the filter, the partial count, the exchange, and the final count. Before a run, it's the starting plan: adaptive execution can still change it. That's episode 12.”

### S06 · Down to storage

- **s6_1**. On screen: The cast scaled up ten thousand times, sorted by risk, written as one Parquet file of 25 row groups; each group shows the smallest and largest x it holds.
  Narration: “Here's why handing the filter down pays. Take the cast ten thousand times over, sorted by risk, in one file of 25 row groups. Each group records the smallest and largest x it holds.”
- **s6_2**. On screen: With pushdown, the 13 groups whose largest x is 0 grey out, and 12 are read. Only clinic and x are needed: about 2.6 kB of column data, against 1.9 MB for every column of those groups, labelled: column sizes, from the file's metadata. A settings card turns pushdown off, and all 25 groups are read.
  Narration: “With the filter handed down, the reader skips the 13 groups that can't hold a higher-risk patient. Of the 12 it reads, it needs just two columns: about 2.6 kilobytes of column data, where all four would be nearly 2 megabytes. Those sizes belong to this sorted file. Unsorted, every group holds both kinds, and nothing is skipped. Turn pushdown off, and every group is read.”

### S07 · A black box

- **s7_1**. On screen: Lines 7 and 8 type: the same filter as a Python function. The read's pushed filters are now empty; a Python worker appears beside each bench. The rows stay on the bench: only x goes out to the worker, and a tick comes back.
  Narration: “Now write the same filter as a Python function. To Spark, it's a black box. It can't hand the filter to the read, so every row is read, and for every row the value it checks, x, travels to Python and back, just to be checked.”
- **s7_2**. On screen: On the scaled file: all 25 row groups read, against 12.
  Narration: “On our sorted file, that's all 25 row groups instead of 12. Keep your logic where the optimiser can see it.”

### S08 · Recap

- **s8_1**. On screen: Three recap cards: one plan for DataFrame code and SQL; the optimiser reads only what it needs; a Python function hides its logic.
  Narration: “Let's gather it up. Say what you want, in DataFrame code or in SQL: it's the same plan. Spark checks the names, rewrites the plan, reads only what it needs, and picks the operators. A Python function hides its logic, and the optimiser can't help.”
- **s8_2**. On screen: The plan on the desk, with a price tag beside it: blank. Next: Part II, Counting walls.
  Narration: “The plan is chosen. But is it cheap? Next time, Part Two begins: counting walls.”
- **s8_3**. On screen: Series end card with the next episode title.
  Narration: “This has been Spark, Visually. Thanks for watching the cluster with us.”

## Open questions

1. s5_1: "It hands the read a copy of the filter, so the read can skip what can't match, and has it read only 2 of the 4 columns. The filter still runs after the read, to be sure." Accurate for Spark 4.3's file sources?
2. s4_2: "Write a filter on clinic after the count, and the optimiser moves it before: fewer rows to count." Accurate as worded?
3. s3_2: "Over Spark Connect, DataFrame code waits until its plan is first needed, but a SQL string still fails at once." Accurate as worded?
