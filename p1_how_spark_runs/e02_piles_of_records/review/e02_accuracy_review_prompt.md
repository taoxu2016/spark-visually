# Accuracy review: Spark, Visually · Episode 2: Piles of Records

You are reviewing the script of one episode of *Spark, Visually*, an animated video series that shows what an Apache
Spark cluster does when PySpark code runs. Its viewers write PySpark on Databricks but have never watched a job run.
Another model wrote the script. You are the independent second opinion on technical accuracy, after the series'
owner approved it. The episode is already voiced and animated, so keep each fix to the words that must change. Review accuracy only, not style or pacing.

## Ground rules

- Check every claim against Apache Spark 4.3.0 as the run below used it: PySpark in local mode, default settings
  unless stated, adaptive query execution on. If your knowledge rests on an earlier release, say which. Where
  Databricks Runtime would behave differently in a way a Databricks user would notice, say so.
- The measured facts below were captured from Spark's own event log and results, and, for the fixed cost per task, from executor CPU time in the event log of warm runs on this one-CPU machine, by the episode's numbers
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
2. Every statement about Spark is correct as worded: partitions and tasks, task slots and rounds, the fixed cost per task, how files become partitions (`maxPartitionBytes`, `openCostInBytes`, Parquet row groups), the default parallelism for data built in code, `spark.sql.shuffle.partitions` and adaptive coalescing, `coalesce` against `repartition`, and uneven partitions.
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

Part I, Episode 2: *Piles of Records* (partitions, tasks and cores), module M2 · Piles of records.

- **Question:** How does Spark split a table so many cores can work at once?
- **Learning objective:** The viewer can explain a partition as the unit of parallel work, predict roughly how many partitions a read produces, read waves and idle slots on a task timeline, and say what too few, too many and uneven partitions cost.
- **Core concept:** A partition is a pile of rows one task processes on one core. Parallelism is the smaller of partitions and task slots. Too few partitions idle cores, too many pay a fixed cost per task, and uneven ones leave cores waiting for the biggest.

### Misconceptions this episode targets

- A partition is a file. (Large files split into several partitions; small files can share one.)
- More partitions are always faster. (Past a few per core, fixed costs win.)
- Partitioning is set once. (Every shuffle re-cuts it.)
- `coalesce` and `repartition` are interchangeable. (One moves no rows across the network; the other moves them all.)

### What earlier episodes taught

- Episode 1, Who does the work: the driver runs the notebook's code, plans and schedules; executors read their own files from storage, never through the driver. In a count, each task counts its file's rows and the partial counts go to one final task, which adds them and sends one number (103) to the driver. collect() brings every row to the driver. DataFrame work runs inside the executors; a Python UDF starts Python workers beside each executor.

### The data

The cast: 103 simulated patients in five clinics (Alder, Birch, Cedar, Dune, Elm), written as four Parquet files that Spark reads as four piles of 28, 25, 25 and 25 rows. Column x = 1 marks a higher-risk patient (46 of the 103); column y marks a readmission (42). The animation always shows a fixed cluster of one driver and four executors with two cores each; the numbers come from one machine in local mode. The episode also writes: the cast as one file; the cast as 32 small files; one 61 MB Parquet file of 8 million random numbers in 8 MB row groups; and the cast as one big file and three small ones (70, 11, 11 and 11 rows). The fixed cost is measured on the cast repeated 10,000 times as well as on the cast.

### Deliberate choices (not errors)

- Line 2 of the code panel ends with `.show()`, an action, so the pile counts on screen are Spark's own output.
- S05 gives the full rule for cutting files, from the run: a piece is at most 128 MB, and smaller when that gives every core one. The 61 MB file became 8 piles, one 8 MB row group each.
- Thirty-two piles hold 2 to 4 rows each, not exactly 3 or 4: `repartition` starts each input pile's round-robin deal at a random place.
- The fixed cost per task is measured in executor CPU time from warm runs, because the machine has one CPU. The grey cap drawn on each timeline bar is the share of the task's CPU time not explained by per-row work.
- The timelines in S04 and S07 are drawn from this run's real task start and end times, labelled on screen as from one CPU (tasks running together share it). An eight-core Databricks run will replace them; the task timings are left out of the facts below.
- S04 says "tens of thousands of rows": warm runs on this machine put the break-even between about 31,000 and 54,000 rows (cold first runs gave about 17,000), so only the robust wording is said.
- Plain words on purpose: a pile is a partition, a wall is the boundary between stages, the cast is the example data. In the On screen notes, a chip is one record, coloured by its clinic; a tray is a pile as drawn; a bench is an executor; the driver desk is the driver; the shelf is storage.

## Code on screen

These lines run verbatim in the numbers file, in this order.

```python
piles = df.withColumn("pile", F.spark_partition_id())
piles.groupBy("pile").count().show()
df.coalesce(2)
df.repartition(8)
```

## Measured facts

### Everything else the numbers file recorded

Ids, counts and orders are exact; fields in milliseconds come from a one-CPU machine and matter only as order.

```json
{
 "series": "Spark, Visually",
 "part": "I · How Spark runs a job",
 "episode": 2,
 "title": "Piles of Records",
 "engine": {"spark": "4.3.0", "master": "local[8]", "cpus": 1, "settings": {"spark.sql.files.maxPartitionBytes": "134217728b", "spark.sql.files.openCostInBytes": "4194304b", "spark.sql.shuffle.partitions": "200", "spark.sql.adaptive.enabled": "true", "spark.sql.adaptive.coalescePartitions.enabled": "true"}, "note": "local mode: exact piles, tasks and rows; times are not used here (see the docstring)"},
 "cast": {"patients": 103, "seed": 2026},
 "slots": 8,
 "piles": {"one_file": 1, "four_files": [28, 25, 25, 25], "eight": [13, 12, 14, 13, 13, 13, 13, 12], "thirty_two": [4, 3, 3, 3, 2, 2, 2, 2, 4, 4, 4, 4, 4, 4, 4, 4, 4, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 4], "rows_per_pile_at_32": [2, 4], "mean_rows_at_32": 3.2},
 "tasks": {"one": 1, "four": 4, "eight": 8, "thirty_two": 32},
 "rounds_at_32": 4,
 "max_tasks_at_once": 8,
 "fixed_cost": {"cpu_ms_per_task": 2.878, "cpu_ns_per_row": 51.5, "share_at_32_piles_of_the_cast": 0.9999, "rows_whose_work_equals_the_fixed_cost": 55832, "measured_from": "median executor CPU time (incl. deserialization) over every task of 4 warm runs of a 32-pile count, on the cast and on the cast x 10000"},
 "from_files": {"big_file_mb": 61.1, "big_file_piles": 8, "big_rows_per_pile": [1048431, 1048431, 1048431, 1048431, 1048431, 1048431, 1048431, 660983], "big_row_group_mb": 8, "big_default_row_groups_rows_per_pile": [0, 0, 0, 8000000, 0, 0, 0, 0], "min_piece_mb": 4, "small_files": 32, "small_file_piles": 8, "max_partition_mb": 128},
 "created_piles": 8,
 "after_shuffle": {"setting": 200, "adaptive": 1},
 "coalesce": {"piles": [53, 50], "rows_re_dealt": 0},
 "lines_3_4_jobs": 0,
 "repartition": {"piles": [13, 12, 14, 13, 13, 13, 13, 12], "rows_re_dealt": 103},
 "uneven": {"piles": [70, 11, 11, 11], "ratio_biggest_to_others": 6.4, "scaled_piles": [700000, 110000, 110000, 110000]},
 "trays": {"0": {"Alder": 9, "Birch": 6, "Cedar": 10, "Dune": 2, "Elm": 1}, "1": {"Alder": 5, "Birch": 12, "Cedar": 2, "Dune": 3, "Elm": 3}, "2": {"Alder": 8, "Birch": 8, "Cedar": 3, "Dune": 3, "Elm": 3}, "3": {"Alder": 8, "Birch": 4, "Cedar": 5, "Dune": 4, "Elm": 4}},
 "checks": [
  {"name": "pinned engine: Apache Spark 4.3.0", "ok": true, "detail": "4.3.0"},
  {"name": "eight task slots, as on the home shot", "ok": true, "detail": "8"},
  {"name": "the cast as one file is one pile", "ok": true, "detail": "1"},
  {"name": "the cast as four files is four piles: 28, 25, 25, 25", "ok": true, "detail": "[28, 25, 25, 25]"},
  {"name": "the code panel's show() prints the four piles", "ok": true, "detail": ""},
  {"name": "repartition(8) makes eight piles", "ok": true, "detail": "[13, 12, 14, 13, 13, 13, 13, 12]"},
  {"name": "repartition(32) makes thirty-two small piles, about three rows each", "ok": true, "detail": "2 to 4 rows"},
  {"name": "the big file is one file", "ok": true, "detail": "1"},
  {"name": "with Spark's default row groups the same rows make the same pieces, but only one piece holds rows", "ok": true, "detail": "[0, 0, 0, 8000000, 0, 0, 0, 0]"},
  {"name": "one pile: the count runs one task", "ok": true, "detail": "1"},
  {"name": "eight piles: eight tasks", "ok": true, "detail": "8"},
  {"name": "thirty-two piles: thirty-two tasks", "ok": true, "detail": "32"},
  {"name": "never more than eight tasks run at once", "ok": true, "detail": "8"},
  {"name": "the scaled cast runs the same thirty-two tasks", "ok": true, "detail": "32"},
  {"name": "a task's fixed cost is positive and the work per row is positive", "ok": true, "detail": "fixed 2.88 ms CPU, 52 ns a row"},
  {"name": "with about three rows a pile, nearly all of a task is fixed cost", "ok": true, "detail": "0.9999"},
  {"name": "a pile needs at least tens of thousands of rows before its work matches the fixed cost (a floor, not an estimate)", "ok": true, "detail": "55,832 rows"},
  {"name": "the big file is cut into one piece per core, each holding rows", "ok": true, "detail": "[1048431, 1048431, 1048431, 1048431, 1048431, 1048431, 1048431, 660983]"},
  {"name": "thirty-two small files are packed into eight piles", "ok": true, "detail": "32 files, 8 piles"},
  {"name": "data created in code starts as one pile per core", "ok": true, "detail": "8"},
  {"name": "after a shuffle, the setting decides: 200 piles", "ok": true, "detail": "200"},
  {"name": "adaptive execution merges the cast's 200 tiny piles", "ok": true, "detail": "1"},
  {"name": "coalesce(2): two piles, each made of two whole piles, nothing re-dealt", "ok": true, "detail": "[53, 50]"},
  {"name": "repartition(8): every row is re-dealt", "ok": true, "detail": "103 rows"},
  {"name": "coalesce(2): no row is re-dealt", "ok": true, "detail": "0 rows re-dealt; tasks read [53, 50]"},
  {"name": "the coalesce and repartition lines run no job by themselves", "ok": true, "detail": ""},
  {"name": "uneven files read as uneven piles", "ok": true, "detail": "[70, 11, 11, 11]"},
  {"name": "the uneven work runs four tasks, the biggest pile's the longest", "ok": true, "detail": "4"}
 ]
}
```

## The script

Each clip is one stretch of voiceover. "On screen" describes the animation; "Narration" is spoken.

### S01 · Cold open

- **s1_1**. On screen: The home shot. The cast lies on the storage shelf as a single file. A count runs: one task drops into one core slot on the first bench; the other seven slots stay dark.
  Narration: “Last time, our count read the table with 4 tasks on 8 cores. Here's a worse case: the same 103 patients, written as one file. Spark reads it with 1 task. One core works, and 7 sit idle.”
- **s1_2**. On screen: The single tray glows; the seven idle slots pulse once.
  Narration: “Nothing is wrong with the data. It's how the rows are piled up. Spark works pile by pile, and one file made one pile.”

### S02 · Title

- **s2_1**. On screen: Series wordmark, Spark, Visually; Part I, How Spark runs a job; the episode title.
  Narration: “Welcome to Spark, Visually. Today: piles of records.”

### S03 · One pile, one task

- **s3_1**. On screen: A tray lifts and is labelled: partition, a pile of rows. One task carries it into one core slot.
  Narration: “Spark calls a pile of rows a partition. Each partition is worked on by one task, on one core, from start to finish.”
- **s3_2**. On screen: Code lines 1 and 2 type in; the four files become four trays, and the output table shows 28, 25, 25, 25. Four slots light; four stay dark.
  Narration: “Our 4 files make 4 piles. These two lines number each row's pile, and count them: 28, 25, 25 and 25. Four piles: four cores work, and four sit idle.”
- **s3_3**. On screen: The rows are dealt into eight trays, two per bench, one beside each core slot. All eight slots light.
  Narration: “Deal the rows into eight piles, and every core gets one. All eight work at once.”

### S04 · Rounds

- **s4_1**. On screen: Thirty-two small trays stack beside the benches. The timeline replaces the storage shelf: eight lanes, one per core; bars appear in four rounds. Bars drawn from this run's real task times on one CPU, labelled so on screen (tasks running together share it); an eight-core Databricks run (e02_timings.json) will replace them.
  Narration: “So why not 32 piles? The eight cores take the first eight. Whenever a core finishes, it takes the next pile. Thirty-two piles run in four rounds.”
- **s4_2**. On screen: The timeline is named: a lane per core, a bar per task. Each bar's grey cap lights up.
  Narration: “This is a timeline: a lane for each core, and a bar for each task. Every bar carries the same grey cap: the fixed cost of starting a task and wrapping it up.”
- **s4_3**. On screen: Zoom on one bar: the cap is nearly the whole bar. A card: more than 99% fixed cost at about 3 rows a pile; at least tens of thousands of rows before the work matches it.
  Narration: “Our piles hold about 3 patients each, so the cap is nearly the whole bar: more than 99 percent of each task is overhead. For a simple count, a pile needs at least tens of thousands of rows before its real work matches that fixed cost.”

### S05 · Where piles come from

- **s5_1**. On screen: The shelf returns. One big file, written in 8 MB row groups, is cut into eight pieces, one per tray; then thirty-two small files are gathered four at a time into eight trays.
  Narration: “So where do piles come from? When Spark reads files, it cuts big files into pieces, and packs small files together. One big file of 61 megabytes, written in 8-megabyte row groups, became 8 piles, one for each core. And 32 small files became 8 piles too. A piece is never bigger than 128 megabytes, and smaller when that gives every core a piece, but never under 4 megabytes.”
- **s5_2**. On screen: Two cards: data built in code, eight piles; after a shuffle, the setting's 200 piles, with the adaptive merge pointing to Episode 4, where it is shown.
  Narration: “Data you build in code starts as one pile per core. And after a shuffle, a setting decides: 200 piles by default, unless Spark merges the small ones. That's episode 5.”

### S06 · Merge or re-deal

- **s6_1**. On screen: Code line 3 glows: df.coalesce(2). Trays merge in pairs where they stand: 28 and 25 become 53, 25 and 25 become 50. Nothing crosses the network band.
  Narration: “To change the number of piles, there are two tools. The first, coalesce, merges piles where they stand. Ours become two piles, of 53 and 50, and not one row is re-dealt.”
- **s6_2**. On screen: Code line 4 glows: df.repartition(8). Every chip leaves its tray, goes through the shuffle across the network band, and lands in one of eight new trays (13, 12, 14, 13, 13, 13, 13, 12).
  Narration: “The second, repartition, re-deals every row. All 103 go through a shuffle into eight new piles. A shuffle has a real cost. We'll open one up in episode 5.”

### S07 · Uneven piles

- **s7_1**. On screen: The cast is rewritten as four files of 70, 11, 11 and 11 rows; four uneven trays. On the timeline, labelled with its data (the cast ten thousand times over: piles of 700,000 and 110,000 rows, every row hashed), one long bar and three short ones; three lanes go dark while the long bar runs. Bars drawn from this run's real task times on one CPU, labelled so on screen (tasks running together share it); an eight-core Databricks run (e02_timings.json) will replace them.
  Narration: “Piles don't have to be even. Write our cast as one big file and three small ones: piles of 70, 11, 11 and 11. The big pile has over 6 times the rows. At scale, its core works on, while the others finish and wait.”
- **s7_2**. On screen: The end of the long bar is marked: the stage ends here.
  Narration: “A stage ends when its last task ends. One big pile holds up the whole job. Remember that. It comes back.”

### S08 · How many piles?

- **s8_1**. On screen: A rule-of-thumb card: a few piles per core, each big enough that its work dwarfs the fixed cost. Three small icons: idle cores, overhead caps, a long bar.
  Narration: “So how many piles? A few per core, each one big enough that its work dwarfs the fixed cost. Too few, and cores sit idle. Too many tiny ones, and you pay for overhead. Uneven, and everyone waits for the biggest.”

### S09 · Recap

- **s9_1**. On screen: Three recap cards: one pile, one task, one core; where piles come from; merge or re-deal.
  Narration: “Let's gather it up. A partition is a pile of rows, and one task works on one pile, on one core. Files, code and shuffles decide the piles. Coalesce merges them, and repartition re-deals them.”
- **s9_2**. On screen: The code panel: lines 3 and 4 flash; a stopwatch reads zero. Next episode title: Nothing happens until you ask.
  Narration: “One loose end: the coalesce and repartition lines returned instantly. Nothing ran until something asked for a result. So when does Spark actually do the work? Next time: nothing happens until you ask.”
- **s9_3**. On screen: Series end card with the next episode title.
  Narration: “This has been Spark, Visually. Thanks for watching the cluster with us.”

## Open questions

1. S05: "A piece is never bigger than 128 megabytes, and smaller when that gives every core a piece." Is this a fair summary of how Spark 4.3 sizes file splits (`maxPartitionBytes`, `openCostInBytes`, total bytes per core)?
2. S04: "For a simple count, a pile needs tens of thousands of rows before its real work matches that fixed cost." Is that reasonable, given how the fixed cost is measured (median executor CPU time per task, warm runs)?
3. S05: "Data you build in code starts as one pile per core." Is that true as worded for `createDataFrame` from local data and for `spark.range`, in local mode and on a cluster?
