# Accuracy review: Spark, Visually · Episode 3: Nothing Happens Until You Ask

You are reviewing the script of one episode of *Spark, Visually*, an animated video series that shows what an Apache
Spark cluster does when PySpark code runs. Its viewers write PySpark on Databricks but have never watched a job run.
Another model wrote the script. You are the independent second opinion on technical accuracy, after the series'
owner approved it. The episode is already voiced and animated, so keep each fix to the words that must change. Review accuracy only, not style or pacing.

## Ground rules

- Check every claim against Apache Spark 4.3.0 as the run below used it: PySpark in local mode, default settings
  unless stated, adaptive query execution on. If your knowledge rests on an earlier release, say which. Where
  Databricks Runtime would behave differently in a way a Databricks user would notice, say so.
- The measured facts below were captured from Spark's own plans, event log and results by the episode's numbers
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
2. Every statement about Spark is correct as worded: transformations and actions, eager analysis but lazy execution, column pruning for a count, whole-stage code generation as one pass, `show()` re-reading storage, `cache()` being lazy, Parquet filter pushdown and file skipping, CSV schema inference, and a count running as two jobs with adaptive execution.
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

Part I, Episode 3: *Nothing Happens Until You Ask* (transformations and actions), module M3 · Nothing happens until you ask.

- **Question:** Why does one line of PySpark return instantly and the next take minutes?
- **Learning objective:** The viewer can sort operations into transformations and actions, explain that a DataFrame holds a plan rather than rows, see that narrow steps run as one pass, and predict that each action recomputes from storage unless something is cached.
- **Core concept:** A DataFrame is a recipe. Transformations add steps; an action runs the whole recipe from storage, in one pass per partition. Seeing the whole recipe first is what lets Spark optimise it.

### Misconceptions this episode targets

- Each transformation is a pass over the data. (Narrow steps are fused into one pass.)
- `cache()` stores the data now. (It is lazy too.)
- A second action reuses the first one's result. (Not without a cache.)
- Lazy means no work before an action. (Schema inference and some reads run early.)

### What earlier episodes taught

- Episode 1, Who does the work: the driver runs the notebook's code, plans and schedules; executors read their own files from storage, never through the driver. In a count, each task counts its file's rows and the partial counts go to one final task, which adds them and sends one number (103) to the driver. collect() brings every row to the driver. DataFrame work runs inside the executors; a Python UDF starts Python workers beside each executor.
- Episode 2, Piles of records: a partition, called a pile, is the rows one task processes on one core. Reads cut files into piles; data built in code starts as one pile per core; after a shuffle there are 200 piles by default, unless adaptive execution merges small ones. coalesce merges piles where they stand; repartition re-deals every row. A stage ends when its slowest task ends.

### The data

The cast: 103 simulated patients in five clinics (Alder, Birch, Cedar, Dune, Elm), written as four Parquet files that Spark reads as four piles of 28, 25, 25 and 25 rows. Column x = 1 marks a higher-risk patient (46 of the 103); column y marks a readmission (42). The animation always shows a fixed cluster of one driver and four executors with two cores each; the numbers come from one machine in local mode. The hook repeats the cast 10,000 times (1,030,000 rows). S06 writes the cast again sorted by risk, so two of its four files hold no higher-risk patient. S07 also writes the cast as CSV.

### Deliberate choices (not errors)

- The hook is told in rows read (0, 0, 0, then every row), not in time: times depend on the machine, rows read don't. The hook's wall-clock times are kept in the facts below and never said.
- The count computes only what it needs: its scan reads only column x and never computes the new column. The one-pass-through-every-step beat belongs to `show()`, which needs all three steps.
- Pushdown is shown on files sorted by risk, because on the cast's four mixed files every file holds higher-risk patients and a pushed filter skips nothing.
- `show()` asks for 21 rows, so it reads files only until it has them: 53 rows here, from two of the four files. It still starts again from storage, which is all S05 claims.
- The CSV exception is shown both ways: inferring the schema runs 2 jobs at read time; a given schema keeps the read lazy.
- Plain words on purpose: a pile is a partition, a wall is the boundary between stages, the cast is the example data. In the On screen notes, a chip is one record, coloured by its clinic; a tray is a pile as drawn; a bench is an executor; the driver desk is the driver; the shelf is storage.

## Code on screen

These lines run verbatim in the numbers file, in this order.

```python
hi = df.filter(F.col("x") == 1)
hi = hi.withColumn("readmitted", F.col("y") == 1)
hi = hi.select("clinic", "readmitted")
hi.count()
hi.show()
```

## Measured facts

### The physical plan of `hi` after line 3, before any action

```text
== Physical Plan ==
*(1) Project [clinic#5, (y#7 = 1) AS readmitted#19]
+- *(1) Filter (isnotnull(x#6) AND (x#6 = 1))
   +- *(1) ColumnarToRow
      +- FileScan parquet [clinic#5,x#6,y#7] Batched: true, DataFilters: [isnotnull(x#6), (x#6 = 1)], Format: Parquet, Location: InMemoryFileIndex(1 paths)[file:/tmp/sv_e03/cast], PartitionFilters: [], PushedFilters: [IsNotNull(x), EqualTo(x,1)], ReadSchema: struct<clinic:string,x:int,y:int>
```

### `hi.count()`: the plan it runs (explain() before the run)

```text
== Physical Plan ==
AdaptiveSparkPlan isFinalPlan=false
+- HashAggregate(keys=[], functions=[count(1)])
   +- Exchange SinglePartition, ENSURE_REQUIREMENTS, [plan_id=97]
      +- HashAggregate(keys=[], functions=[partial_count(1)])
         +- Project
            +- Filter (isnotnull(x#6) AND (x#6 = 1))
               +- FileScan parquet [x#6] Batched: true, DataFilters: [isnotnull(x#6), (x#6 = 1)], Format: Parquet, Location: InMemoryFileIndex(1 paths)[file:/tmp/sv_e03/cast], PartitionFilters: [], PushedFilters: [IsNotNull(x), EqualTo(x,1)], ReadSchema: struct<x:int>
```

### `hi.show()`: the plan it runs

```text
== Physical Plan ==
CollectLimit 21
+- *(1) Project [clinic#5, (y#7 = 1) AS readmitted#19]
   +- *(1) Filter (isnotnull(x#6) AND (x#6 = 1))
      +- *(1) ColumnarToRow
         +- FileScan parquet [clinic#5,x#6,y#7] Batched: true, DataFilters: [isnotnull(x#6), (x#6 = 1)], Format: Parquet, Location: InMemoryFileIndex(1 paths)[file:/tmp/sv_e03/cast], PartitionFilters: [], PushedFilters: [IsNotNull(x), EqualTo(x,1)], ReadSchema: struct<clinic:string,x:int,y:int>
```

### Everything else the numbers file recorded

Ids, counts and orders are exact; fields in milliseconds come from a one-CPU machine and matter only as order.

```json
{
 "series": "Spark, Visually",
 "part": "I · How Spark runs a job",
 "episode": 3,
 "title": "Nothing Happens Until You Ask",
 "engine": {"spark": "4.3.0", "master": "local[8]", "cpus": 1, "settings": {"spark.sql.adaptive.enabled": "true", "spark.sql.parquet.filterPushdown": "true", "spark.sql.codegen.wholeStage": "true"}, "note": "local mode: exact plans, jobs, tasks and rows; the hook's times are wall-clock on this machine"},
 "cast": {"patients": 103, "higher_risk": 46, "seed": 2026},
 "plans": {"analyzed_after_each_line": [["Filter", "Relation"], ["Project", "Filter", "Relation"], ["Project", "Project", "Filter", "Relation"]], "optimized": ["Project", "Filter", "Relation"], "count_reads": "x:int"},
 "count": {"result": 46, "jobs": 2, "stages": 2, "tasks": 4, "rows_read_per_task": [28, 25, 25, 25]},
 "show": {"jobs": 2, "rows_read": 53, "printed": "+------+----------+\n|clinic|readmitted|\n+------+----------+\n| Alder|      true|\n|  Dune|      true|\n| Birch|     false|\n| Birch|      true|\n| Cedar|     false|\n| Birch|     false|\n| Cedar|      true|\n| Cedar|      true|\n| Cedar|      true|\n| Birch|     false|\n| Cedar|      true|\n| Cedar|     false|\n| Cedar|     false|\n| Alder|      true|\n| Birch|      true|\n| Alder|     false|\n| Birch|     false|\n| Birch|     false|\n| Birch|     false|\n| Birch|      true|\n+------+----------+\nonly showing top 20 rows\n"},
 "hook": {"rows": 1030000, "scale": 10000, "rows_read_per_line": [0, 0, 0, 1030000], "seconds_per_line": [0.0214, 0.0173, 0.0147, 0.4945], "ms_lines_1_3": [21.4, 17.3, 14.7], "seconds_line_4": 0.49},
 "pushdown": {"pushed_filters": "IsNotNull(x), EqualTo(x,1)", "rows_read_without": 103, "rows_read_with": 53, "higher_risk_per_file": [0, 0, 18, 28], "sorted_files": [{"rows": 28, "higher_risk": 28}, {"rows": 25, "higher_risk": 0}, {"rows": 25, "higher_risk": 18}, {"rows": 25, "higher_risk": 0}]},
 "kinds": {"transformation": ["DataFrame", "DataFrame", "DataFrame"], "count": "int", "show": "NoneType", "collect": "list", "write": "NoneType"},
 "trays": {"0": {"Alder": 9, "Birch": 6, "Cedar": 10, "Dune": 2, "Elm": 1}, "1": {"Alder": 5, "Birch": 12, "Cedar": 2, "Dune": 3, "Elm": 3}, "2": {"Alder": 8, "Birch": 8, "Cedar": 3, "Dune": 3, "Elm": 3}, "3": {"Alder": 8, "Birch": 4, "Cedar": 5, "Dune": 4, "Elm": 4}},
 "csv": {"jobs_with_inference": 2, "jobs_with_schema": 0, "parquet_by_path_jobs": 1, "json_by_path_jobs": 1, "connect": {"jobs_after_read": 0, "jobs_when_schema_needed": 2}},
 "show_jobs": [
  {"tasks": 1, "rows": 28},
  {"tasks": 1, "rows": 25}
 ],
 "pushdown_rows_per_task": [28, 0, 25, 0],
 "cache_jobs": 0,
 "checks": [
  {"name": "pinned engine: Apache Spark 4.3.0", "ok": true, "detail": "4.3.0"},
  {"name": "the count is the cast's higher-risk patients", "ok": true, "detail": "46 of 103"},
  {"name": "each transformation adds one step to the plan", "ok": true, "detail": "[['Filter', 'Relation'], ['Project', 'Filter', 'Relation'], ['Project', 'Project', 'Filter', 'Relation']]"},
  {"name": "line 1 runs no job", "ok": true, "detail": ""},
  {"name": "line 2 runs no job", "ok": true, "detail": ""},
  {"name": "line 3 runs no job", "ok": true, "detail": ""},
  {"name": "line 4: four tasks, one per file, each reading its file once", "ok": true, "detail": "[28, 25, 25, 25]"},
  {"name": "line 4 runs as two jobs and two stages", "ok": true, "detail": "2 jobs, 2 stages"},
  {"name": "filter, new column and select run as one fused pass", "ok": true, "detail": "['1', '1']"},
  {"name": "the two Projects collapse into one before it runs", "ok": true, "detail": "['Project', 'Project', 'Filter', 'Relation'] -> ['Project', 'Filter', 'Relation']"},
  {"name": "the count never computes the new column, and reads only the column it filters on", "ok": true, "detail": "x:int"},
  {"name": "show runs all three steps, as one fused pass", "ok": true, "detail": "['1', '1']"},
  {"name": "line 5 runs its own job and reads the files again", "ok": true, "detail": "53 rows read"},
  {"name": "in the hook, only the fourth line runs a job", "ok": true, "detail": "{1: False, 2: False, 3: False, 4: True}"},
  {"name": "the hook's count reads every row", "ok": true, "detail": "1,030,000"},
  {"name": "the count takes longer than any of the transformations (times are kept, not said)", "ok": true, "detail": "[21.4, 17.3, 14.7, 494.5] ms"},
  {"name": "the pushed filter is in the scan", "ok": true, "detail": "IsNotNull(x), EqualTo(x,1)"},
  {"name": "with the files sorted by risk, the pushed filter skips files", "ok": true, "detail": "53 rows read instead of 103"},
  {"name": "transformations hand back a DataFrame; count a number; show nothing (it prints)", "ok": true, "detail": ""},
  {"name": "reading Parquet or JSON by path runs one job each, to learn the schema", "ok": true, "detail": "parquet 1, json 1"},
  {"name": "over Spark Connect the CSV read runs no job, and the inference jobs run when the schema is first needed", "ok": true, "detail": "{'before': 0, 'after_read': 0, 'after_columns': 2}"},
  {"name": "show() reads one file at a time, in growing batches: two jobs of one task", "ok": true, "detail": "[{'tasks': 1, 'rows': 28}, {'tasks': 1, 'rows': 25}]"},
  {"name": "with pushdown all four tasks still run; the files with no higher-risk patient give them nothing to read", "ok": true, "detail": "[28, 0, 25, 0]"},
  {"name": "reading CSV with schema inference runs jobs at read time", "ok": true, "detail": "2 jobs"},
  {"name": "reading CSV with a schema runs no job", "ok": true, "detail": ""},
  {"name": "cache() runs no job by itself", "ok": true, "detail": ""}
 ]
}
```

## The script

Each clip is one stretch of voiceover. "On screen" describes the animation; "Narration" is spoken.

### S01 · Cold open

- **s1_1**. On screen: The code panel holds four lines; the shelf holds the cast scaled ten thousand times, about a million rows, as four files. Beside each line, a counter: rows read.
  Narration: “You've seen it happen: a few lines of PySpark come back instantly, and then one line takes minutes. Here are four lines, run on our cast scaled up to about a million rows.”
- **s1_2**. On screen: The counters fill as each line runs: 0, 0, 0, then 1,030,000 for the fourth.
  Narration: “The first three read nothing. Not one row. The fourth reads every single row. So why do the first three read nothing?”

### S02 · Title

- **s2_1**. On screen: Series wordmark, Spark, Visually; Part I, How Spark runs a job; the episode title.
  Narration: “Welcome to Spark, Visually. Today: nothing happens until you ask.”

### S03 · A recipe

- **s3_1**. On screen: Back to the home shot and the cast's four files; the benches stay empty. Line 1 types; a box, filter, appears on the driver desk. Nothing moves.
  Narration: “Back to our 103 patients. Line one filters them down to the higher-risk patients. Or rather, it writes that down. A step is added to a plan: filter. Not one row moves.”
- **s3_2**. On screen: Lines 2 and 3 type; two more boxes join the chain: new column, select. The rows-read counter stays at 0.
  Narration: “Line two adds a column, and line three keeps just two columns. Each one adds a step. Three lines, and not a single row has been read.”
- **s3_3**. On screen: A card: a DataFrame is a recipe, its source and its steps.
  Narration: “A DataFrame isn't a table of rows. It's a recipe: where the rows come from, and the steps to follow.”

### S04 · One pass

- **s4_1**. On screen: Line 4 glows: hi.count(). Four tasks drop from the driver to the benches, one per file.
  Narration: “Line four asks a question: how many rows are left? That's an action, and an action runs the recipe. The driver sends 4 tasks down, one per file.”
- **s4_2**. On screen: Each bench reads its file; its chips pass a filter gate as they arrive, in one stream, and the chips without a risk ring drop away. The new-column and select boxes stay dark; only the x column leaves the shelf.
  Narration: “Each task reads its file and filters the rows as they arrive, in a single pass. And the new column? A count doesn't need it, so Spark never computes it. It even reads just the one column the filter needs.”
- **s4_3**. On screen: 46 chips remain; four partial counts go to one final task; 46 rises to the driver.
  Narration: “46 higher-risk patients make it through, and one number goes up to the driver: 46.”

### S05 · Again

- **s5_1**. On screen: The trays are empty again. Line 5 glows: hi.show(). One task reads the first file, then a second task the next, and it stops there: show() needs only 21 rows. This time the chips pass all three steps in one stream: the filter, the new column stamped on, the risk ring dropped by select. Rows print in the code panel.
  Narration: “Line five asks to see the rows. Another action, so the recipe runs again, from the files. This time it needs every step, and it still makes one pass: filter, add the column, keep two.”
- **s5_2**. On screen: A card: normally, every action starts from storage; keeping a result is a promise for Episode 10.
  Narration: “Spark didn't keep the count's work. Normally, every action starts again from storage, unless you ask Spark to keep a result. And even that request waits for an action. That's episode 10.”

### S06 · Why wait?

- **s6_1**. On screen: The whole chain on the driver desk lights at once.
  Narration: “So why be lazy? Because when the action arrives, Spark sees the whole recipe at once, and can plan it. Here's our cast again, written with its files sorted by risk.”
- **s6_2**. On screen: A copy of the filter box slides down into the read; the filter stays on the desk. All four tasks run: the two files holding no higher-risk patients grey out and are skipped, and their tasks read nothing; a counter: rows read, 53 instead of 103.
  Narration: “Spark pushes the filter into the read itself. Files that hold no higher-risk patients are skipped, and it reads 53 rows instead of 103. Episode 6 shows how.”

### S07 · Two kinds of line

- **s7_1**. On screen: A sorting card: transformations (filter, withColumn, select) hand back a DataFrame; actions (count, show, collect, write) hand back a number, rows, or files.
  Narration: “So, two kinds of line. A transformation hands back a new DataFrame: a longer recipe, and no work. An action hands back a result: a number, rows on screen or in your hands, or files written. Actions do the work.”
- **s7_2**. On screen: A CSV file on the shelf: with inferSchema, two jobs run to look at the data; with a schema given, none. Beneath: reading Parquet or JSON by path runs 1 job, for the schema; over Spark Connect these jobs wait until the schema is first needed.
  Narration: “A common exception: read a CSV file and ask Spark to guess its column types, and it runs 2 jobs, to look at the data. Give it the schema, and the read is lazy again.”

### S08 · Recap

- **s8_1**. On screen: Three recap cards: transformations write the recipe; actions run it, one pass per pile; normally, every action starts from storage.
  Narration: “Let's gather it up. Transformations write the recipe. Actions run it, in one pass over each pile. And normally, every action starts again from storage.”
- **s8_2**. On screen: The count's job and stage tally: 2 jobs, 2 stages. Next episode title: Cut at the walls.
  Narration: “One loose end: our count was one action, yet Spark ran it as 2 jobs and 2 stages. How does Spark cut up the work? Next time: cut at the walls.”
- **s8_3**. On screen: Series end card with the next episode title.
  Narration: “This has been Spark, Visually. Thanks for watching the cluster with us.”

## Open questions

1. The hook's first sentence ("a few lines of PySpark come back instantly, and then one line takes minutes") describes a common experience, not this run: the count took 0.35 seconds here. Is it fair framing?
2. S06: "Files that hold no higher-risk patients are skipped." Spark 4.3 skips Parquet row groups whose statistics rule them out, and here each file is one row group. Is "files are skipped" accurate as worded, or should it say row groups?
3. S04 and S05 together: a count never computes the new column, while `show()` makes one pass through filter, new column and select. Are these fair readings of the plans below?
