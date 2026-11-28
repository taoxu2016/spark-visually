# Accuracy review: Spark, Visually · Episode 7: Counting Walls

You are reviewing the script of one episode of *Spark, Visually*, an animated video series that shows what an Apache
Spark cluster does when PySpark code runs. Its viewers write PySpark on Databricks but have never watched a job run.
Another model wrote the script. You are the independent second opinion on technical accuracy, on draft 2. A first
review found six things, and all six were changed; check the new wording as closely as the rest. Review accuracy only, not style or pacing.

## Ground rules

- Check every claim against Apache Spark 4.3.0 as the run below used it: PySpark in local mode, default settings
  unless stated, adaptive query execution on. If your knowledge rests on an earlier release, say which. Where
  Databricks Runtime would behave differently in a way a Databricks user would notice, say so.
- The measured facts below were captured from Spark's own plans and event log, by the episode's numbers
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
2. Every statement about Spark is correct as worded: narrow and wide operations, walls counted as shuffle Exchanges in a plan, `union` and `coalesce` as narrow, the join's two walls, `orderBy` against `sortWithinPartitions`, a pointless `repartition(200)`, a deal by the grouping key that the aggregation reuses, and rows and bytes crossing each wall.
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

Part I, Episode 7: *Counting Walls* (reading a job's cost from its code), module M7 · Counting walls.

- **Question:** Can you predict a job's cost by reading its code?
- **Learning objective:** The viewer can classify common operations as narrow or wide, count the shuffles in a pipeline before running it, and remove the avoidable ones.
- **Core concept:** A narrow operation builds each output partition from one input partition: no wall. A wide one needs rows from many partitions: a wall and a shuffle. A pipeline's cost is mostly its number of walls and what crosses each.

### Misconceptions this episode targets

- `orderBy` just sorts each partition. (That is `sortWithinPartitions`; `orderBy` is a global sort: a wall plus a sampling job.)
- `union` shuffles. (It is narrow: it stacks partitions.)
- A wide operation always shuffles. (Not when rows are already partitioned that way, or when a broadcast replaces the shuffle; Episode 8.)
- Repartitioning first speeds up later steps. (Only steps that need that exact partitioning.)

### What earlier episodes taught

- Episode 1, Who does the work: the driver runs the notebook's code, plans and schedules; executors read their own files from storage, never through the driver. In a count, each task counts its file's rows and the partial counts go to one final task, which adds them and sends one number (103) to the driver. collect() brings every row to the driver. DataFrame work runs inside the executors; a Python UDF starts Python workers beside each executor.
- Episode 2, Piles of records: a partition, called a pile, is the rows one task processes on one core. Reads cut files into piles; data built in code starts as one pile per core; after a shuffle there are 200 piles by default, unless adaptive execution merges small ones. coalesce merges piles where they stand; repartition re-deals every row. A stage ends when its slowest task ends.
- Episode 3, Nothing happens until you ask: transformations add steps to a plan and run nothing; an action runs the whole plan from storage, in one pass per pile, and every action starts again from storage unless something is cached. Its closing line: a count of the filtered rows was one action, yet Spark ran it as 2 jobs and 2 stages.
- Episode 4, Cut at the walls (written after this episode, which was made first as the pilot): Spark cuts a plan into stages where rows are exchanged; one task per pile; between stages stands a wall, and nothing beyond it starts until everything before it is done; with adaptive execution one action can run as several jobs.
- Episode 5, Everyone sends to everyone: in a shuffle, each map task writes its output into bins by destination on its executor's local disk; after the wall, each reduce task fetches its bin from every map task. Partial aggregation sends one count per clinic per pile instead of every row; at scale that cuts the bytes sharply.
- Episode 6, Say what, not how: DataFrame code and SQL reach the same plan. Analysis resolves names; the optimiser adds checks, keeps only what is needed and reorders what is safe; physical planning hands the read a copy of the filter and the columns it needs, and splits an aggregate around an Exchange. A Python UDF hides its logic.

### The data

The cast: 103 simulated patients in five clinics (Alder, Birch, Cedar, Dune, Elm), written as four Parquet files that Spark reads as four piles of 28, 25, 25 and 25 rows. Column x = 1 marks a higher-risk patient (46 of the 103); column y marks a readmission (42). The animation always shows a fixed cluster of one driver and four executors with two cores each; the numbers come from one machine in local mode. The three versions also run on the cast ten thousand times over (1,030,000 rows, written as Parquet), for the race. `clinics` is a five-row Parquet table, one row per clinic.

### Deliberate choices (not errors)

- The blueprint groups by provider; the cast's key is `clinic`, so the episode uses that.
- The race is told in walls, rows and bytes crossing, all exact. It has no times: this sandbox has one CPU, and the crowd, the blueprint's large simulated table, doesn't exist yet, so the cast ten thousand times over stands in.
- A wall is a shuffle Exchange in the plan; a BroadcastExchange is not counted (Episode 8). The join card uses `clinics.hint("merge")` so that both sides are dealt.
- Each gallery card runs verbatim, with a no-op write as its action, so its jobs and stages are measured.
- Measured for draft 2: orderBy's sampling job reads every row, so the table is read twice; SQL's UNION ALL builds no wall while plain UNION deals on every column; one deal by clinic serves both a count and a window keyed by clinic.
- S05 carries one Databricks caption, by the owner's decision: on Photon compute a wall can also appear as a PhotonShuffleExchangeSink line (taken from an explain plan in a Databricks talk on Photon, where Spark and Photon nodes can mix; not run here). The narration itself describes open-source Spark.
- The byte figures are this table's: its columns compress very well, so the bytes depend on the data; the rows crossing do not.
- Plain words on purpose: a pile is a partition, a wall is the boundary between stages, the cast is the example data. In the On screen notes, a chip is one record, coloured by its clinic; a tray is a pile as drawn; a bench is an executor; the driver desk is the driver; the shelf is storage.

## Code on screen

These lines run verbatim in the numbers file, in this order.

```python
slow = df.repartition(200).groupBy("clinic").count()
fast = df.groupBy("clinic").count()
kept = df.repartition("clinic").groupBy("clinic").count()
slow.explain()
[q.collect() for q in (slow, fast, kept)]
```

## Measured facts

### slow.explain(), as printed (the code panel's line 4)

```text
== Physical Plan ==
AdaptiveSparkPlan isFinalPlan=false
+- HashAggregate(keys=[clinic#7], functions=[count(1)])
   +- Exchange hashpartitioning(clinic#7, 200), ENSURE_REQUIREMENTS, [plan_id=802]
      +- HashAggregate(keys=[clinic#7], functions=[partial_count(1)])
         +- Exchange RoundRobinPartitioning(200), REPARTITION_BY_NUM, [plan_id=798]
            +- FileScan parquet [clinic#7] Batched: true, DataFilters: [], Format: Parquet, Location: InMemoryFileIndex(1 paths)[file:/tmp/sv_e07/cast], PartitionFilters: [], PushedFilters: [], ReadSchema: struct<clinic:string>
```

### Everything else the numbers file recorded

Ids, counts and orders are exact; fields in milliseconds come from a one-CPU machine and matter only as order.

```json
{
 "series": "Spark, Visually",
 "part": "II · Why jobs are slow",
 "episode": 7,
 "title": "Counting Walls",
 "engine": {"spark": "4.3.0", "master": "local[8]", "cpus": 1, "settings": {"spark.sql.adaptive.enabled": "true", "spark.sql.shuffle.partitions": "200", "spark.sql.autoBroadcastJoinThreshold": "10485760b"}, "note": "local mode: exact plans, walls, jobs, stages, rows and bytes; no durations"},
 "cast": {"patients": 103, "files": 4, "seed": 2026},
 "answers": {"Alder": 30, "Elm": 11, "Cedar": 20, "Dune": 12, "Birch": 30},
 "narrow": [
  {"code": "df.filter(F.col(\"x\") == 1)", "walls": [], "broadcasts": 0, "jobs": 1, "stages_run": 1},
  {"code": "df.select(\"clinic\", \"y\")", "walls": [], "broadcasts": 0, "jobs": 1, "stages_run": 1},
  {"code": "df.withColumn(\"z\", F.col(\"y\") + 1)", "walls": [], "broadcasts": 0, "jobs": 1, "stages_run": 1},
  {"code": "df.union(df)", "walls": [], "broadcasts": 0, "jobs": 1, "stages_run": 1},
  {"code": "df.coalesce(2)", "walls": [], "broadcasts": 0, "jobs": 1, "stages_run": 1}
 ],
 "wide": [
  {"code": "df.groupBy(\"clinic\").count()", "walls": ["hashpartitioning(clinic#7, 200)"], "broadcasts": 0, "jobs": 2, "stages_run": 2},
  {"code": "df.distinct()", "walls": ["hashpartitioning(patient#6, clinic#7, x#8, y#9, 200)"], "broadcasts": 0, "jobs": 2, "stages_run": 2},
  {"code": "df.join(clinics.hint(\"merge\"), \"clinic\")", "walls": ["hashpartitioning(clinic#7, 200)", "hashpartitioning(clinic#10, 200)"], "broadcasts": 0, "jobs": 3, "stages_run": 3},
  {"code": "df.repartition(4)", "walls": ["RoundRobinPartitioning(4)"], "broadcasts": 0, "jobs": 2, "stages_run": 2},
  {"code": "df.withColumn(\"n\", F.count(\"*\").over(Window.partitionBy(\"clinic\")))", "walls": ["hashpartitioning(clinic#7, 200)"], "broadcasts": 0, "jobs": 2, "stages_run": 2},
  {"code": "df.orderBy(\"patient\")", "walls": ["rangepartitioning(patient#6 ASC NULLS FIRST, 200)"], "broadcasts": 0, "jobs": 3, "stages_run": 3, "rows_read_per_job": [103, 103, 0]}
 ],
 "sort_within": {"code": "df.sortWithinPartitions(\"patient\")", "walls": [], "broadcasts": 0, "jobs": 1},
 "union_sql": [
  {"code": "spark.sql(\"SELECT * FROM cast UNION ALL SELECT * FROM cast\")", "walls": [], "broadcasts": 0},
  {"code": "spark.sql(\"SELECT * FROM cast UNION SELECT * FROM cast\")", "walls": ["hashpartitioning(patient#6, clinic#7, x#8, y#9, 200)"], "broadcasts": 0}
 ],
 "reuse": {"lines": ["d = df.repartition(\"clinic\")", "d.groupBy(\"clinic\").count()", "d.withColumn(\"n\", F.count(\"*\").over(Window.partitionBy(\"clinic\")))"], "walls": [["hashpartitioning(clinic#7, 200)"], ["hashpartitioning(clinic#7, 200)"]]},
 "walls": {"slow": ["hashpartitioning(clinic#7, 200)", "RoundRobinPartitioning(200)"], "fast": ["hashpartitioning(clinic#7, 200)"], "kept": ["hashpartitioning(clinic#7, 200)"]},
 "race": {"cast": {"slow": [{"rows": 103, "bytes": 7004, "tasks": 4}, {"rows": 90, "bytes": 6390, "tasks": 200}], "fast": [{"rows": 20, "bytes": 1415, "tasks": 4}], "kept": [{"rows": 103, "bytes": 1503, "tasks": 4}]}, "big": {"slow": [{"rows": 1030000, "bytes": 331200, "tasks": 8}, {"rows": 1000, "bytes": 72800, "tasks": 200}], "fast": [{"rows": 40, "bytes": 2880, "tasks": 8}], "kept": [{"rows": 1030000, "bytes": 163832, "tasks": 8}]}},
 "scale": 10000,
 "checks": [
  {"name": "pinned engine: Apache Spark 4.3.0", "ok": true, "detail": "4.3.0"},
  {"name": "all three give the same answer", "ok": true, "detail": "{'Alder': 30, 'Elm': 11, 'Cedar': 20, 'Dune': 12, 'Birch': 30}"},
  {"name": "every narrow card has no wall, and runs as one stage", "ok": true, "detail": "[('df.filter(F.col(\"x\") == 1)', [], 1), ('df.select(\"clinic\", \"y\")', [], 1), ('df.withColumn(\"z\", F.col(\"y\") + 1)', [], 1), ('df.union(df)', [], 1), ('df.coalesce(2)', [], 1)]"},
  {"name": "every wide card has at least one wall", "ok": true, "detail": "[('df.groupBy(\"clinic\").count()', ['hashpartitioning(clinic#7, 200)']), ('df.distinct()', ['hashpartitioning(patient#6, clinic#7, x#8, y#9, 200)']), ('df.join(clinics.hint(\"merge\"), \"clinic\")', ['hashpartitioning(clinic#7, 200)', 'hashpartitioning(clinic#10, 200)']), ('df.repartition(4)', ['RoundRobinPartitioning(4)']), ('df.withColumn(\"n\", F.count(\"*\").over(Window.partitionBy(\"clinic\")))', ['hashpartitioning(clinic#7, 200)']), ('df.orderBy(\"patient\")', ['rangepartitioning(patient#6 ASC NULLS FIRST, 200)'])]"},
  {"name": "the sort-merge join has a wall on each side", "ok": true, "detail": "['hashpartitioning(clinic#7, 200)', 'hashpartitioning(clinic#10, 200)']"},
  {"name": "orderBy is a range wall, and runs an extra job to sample the ranges", "ok": true, "detail": "['rangepartitioning(patient#6 ASC NULLS FIRST, 200)'], 3 jobs"},
  {"name": "sortWithinPartitions sorts each pile where it lies: no wall", "ok": true, "detail": "{'code': 'df.sortWithinPartitions(\"patient\")', 'walls': [], 'broadcasts': 0, 'jobs': 1}"},
  {"name": "orderBy's extra job reads the whole table to sample it: the table is read twice", "ok": true, "detail": "[103, 103, 0]"},
  {"name": "SQL's UNION ALL stacks piles (no wall); plain UNION removes duplicates, and that is a wall", "ok": true, "detail": "[[], ['hashpartitioning(patient#6, clinic#7, x#8, y#9, 200)']]"},
  {"name": "one deal by clinic serves two later steps keyed by clinic: no new wall for either", "ok": true, "detail": "[['hashpartitioning(clinic#7, 200)'], ['hashpartitioning(clinic#7, 200)']]"},
  {"name": "slow has two walls: a round-robin deal, then the hash deal the count needs", "ok": true, "detail": "['hashpartitioning(clinic#7, 200)', 'RoundRobinPartitioning(200)']"},
  {"name": "fast has one wall", "ok": true, "detail": "['hashpartitioning(clinic#7, 200)']"},
  {"name": "kept has one wall: the count finds its rows already dealt by clinic", "ok": true, "detail": "['hashpartitioning(clinic#7, 200)']"},
  {"name": "explain() shows slow's two Exchange lines", "ok": true, "detail": ""},
  {"name": "cast: slow's first wall carries every row, and its second the partial counts", "ok": true, "detail": "[{'rows': 103, 'bytes': 7004, 'tasks': 4}, {'rows': 90, 'bytes': 6390, 'tasks': 200}]"},
  {"name": "cast: fast's one wall carries only partial counts", "ok": true, "detail": "[{'rows': 20, 'bytes': 1415, 'tasks': 4}]"},
  {"name": "cast: kept's one wall carries every row", "ok": true, "detail": "[{'rows': 103, 'bytes': 1503, 'tasks': 4}]"},
  {"name": "big: slow's first wall carries every row, and its second the partial counts", "ok": true, "detail": "[{'rows': 1030000, 'bytes': 331200, 'tasks': 8}, {'rows': 1000, 'bytes': 72800, 'tasks': 200}]"},
  {"name": "big: fast's one wall carries only partial counts", "ok": true, "detail": "[{'rows': 40, 'bytes': 2880, 'tasks': 8}]"},
  {"name": "big: kept's one wall carries every row", "ok": true, "detail": "[{'rows': 1030000, 'bytes': 163832, 'tasks': 8}]"},
  {"name": "at scale, fast moves the fewest bytes, then kept, then slow", "ok": true, "detail": "{'slow': 404000, 'fast': 2880, 'kept': 163832}"}
 ]
}
```

## The script

Each clip is one stretch of voiceover. "On screen" describes the animation; "Narration" is spoken.

### S01 · Cold open

- **s1_1**. On screen: The code panel types three lines, slow, fast and kept: one count per clinic, written three ways. A wall counter sits in the corner, blank.
  Narration: “Here's one question written three ways: patients per clinic. Every version gives the same answer. Which one runs fastest? Make a guess before they run.”
- **s1_2**. On screen: Three cards, one per line; each card's wall counter fills in from Spark's own plan: two, one, one.
  Narration: “Here's a clue you can read straight from Spark's plan: how many walls each one builds. Two, one, and one.”

### S02 · Title

- **s2_1**. On screen: Series wordmark, Spark, Visually; Part II, Why jobs are slow; the episode title.
  Narration: “Welcome to Spark, Visually. Part Two: why jobs are slow. Today: counting walls.”

### S03 · No wall

- **s3_1**. On screen: Five cards: filter, select, withColumn, union, coalesce. For each, arrows run straight down, tray to tray; the wall counter stays at 0, and each runs as one stage. The union card notes: in SQL that is UNION ALL; plain UNION also removes duplicates, and that is a wall.
  Narration: “Some steps send each input pile whole to one output pile. A filter, a select, a new column: each pile is worked where it lies. Union just stacks two tables' piles, and coalesce merges neighbours where they stand. No wall, so each runs as a single stage.”

### S04 · A wall each

- **s4_1**. On screen: Six cards: groupBy, distinct, join, repartition, a window over each clinic, orderBy. Each lights Episode 5's all-to-all lattice across the benches; the wall counter ticks: 1, 1, 2 for the join, 1, 1, 1. The join card shows its hint, .hint("merge"): copying the small table instead is Episode 8.
  Narration: “Other steps need rows from every pile: a count per clinic, distinct rows, a join, a re-deal, a window over each clinic, a sort. Each one builds a wall. A join that deals out both tables, as this hint asks, builds a wall on each side.”
- **s4_2**. On screen: orderBy's card: the shelf is read twice, once by the sampling job and once by the map stage, then one wall. Beside it, sortWithinPartitions: no wall.
  Narration: “Sorting needs care. orderBy sorts the whole table: one wall, plus an extra job that reads the table once more, to sample the rows and choose the ranges. To sort each pile where it lies, there's sortWithinPartitions, with no wall at all.”

### S05 · Count from the plan

- **s5_1**. On screen: Line 4 types: slow.explain(). Two Exchange lines light: RoundRobinPartitioning(200), then hashpartitioning(clinic, 200). Slow's wall counter: two. A caption: on Databricks Photon compute, a wall can also appear as a PhotonShuffleExchangeSink line; count those too.
  Narration: “You don't have to guess. Ask for the plan, and count the lines that begin with Exchange. Each one is a wall. Slow has two: a re-deal into 200 piles, then the deal by clinic that the count needs anyway.”
- **s5_2**. On screen: Fast's and kept's plans: one Exchange each. Kept's count needs no second wall: its rows are already dealt by clinic.
  Narration: “Fast has one. So does kept: it deals by clinic up front, so the count finds each clinic's rows already in one place.”

### S06 · The race

- **s6_1**. On screen: Line 5 glows: all three run, on the cast ten thousand times over. A ledger per version: its walls, and the rows crossing each.
  Narration: “Now run all three on the cast ten thousand times over, just over a million rows. Slow sends every row across its first wall, then 1,000 partial counts across its second. Fast sends only partial counts: 40 small rows, one per clinic per pile. Kept sends every row, once.”
- **s6_2**. On screen: Bytes written per version, on this table: fast 2.9 kB, kept 164 kB, slow 404 kB.
  Narration: “In bytes, fast wins by far, then kept, then slow. Fast wins because there are only five clinics, so the partial counts are tiny. With many more groups, they shrink less.”

### S07 · Already in place

- **s7_1**. On screen: Kept's deal by clinic, with later steps that need clinic lining up behind it, no new wall for them. Beside it, slow's round-robin deal, which no later step needs.
  Narration: “So why deal up front at all? Only when several later steps need rows dealt by the same key: then one wall serves them all. That's episode 17. A re-deal into 200 piles, just to be safe, helps no step here.”
- **s7_2**. On screen: A wide step with no new wall: rows already dealt the right way. A small table copied to every bench, a hint of Episode 8.
  Narration: “And a wide step doesn't always build a new wall: not when the rows are already dealt the right way, and, as you'll see, not when a small table is copied instead.”

### S08 · Recap

- **s8_1**. On screen: Three recap cards: narrow steps work each pile where it lies; wide steps build a wall; count the Exchange lines.
  Narration: “Let's gather it up. Narrow steps work each pile where it lies. Wide steps build a wall, unless the rows are already in place, and the plan's Exchange lines count them. Fewer walls, and less across each: that's a cost you can read from the plan.”
- **s8_2**. On screen: A join's two walls; the next episode's title: Send the small table.
  Narration: “A join is the commonest wall of all. Must it be one? Next time: send the small table.”
- **s8_3**. On screen: Series end card with the next episode title.
  Narration: “This has been Spark, Visually. Thanks for watching the cluster with us.”

## Open questions

1. s4_2: "orderBy sorts the whole table: one wall, plus an extra job that reads the table once more, to sample the rows and choose the ranges." Accurate as worded?
2. s7_1: "Only when several later steps need rows dealt by the same key: then one wall serves them all." Accurate as worded, given that a deal on a subset of a step's keys also serves it?
3. s6_1: "Fast sends only partial counts: 40 small rows, one per clinic per pile." Accurate for this table (8 piles, 5 clinics)?
