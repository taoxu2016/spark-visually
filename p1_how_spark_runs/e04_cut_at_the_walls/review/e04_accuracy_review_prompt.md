# Accuracy review: Spark, Visually · Episode 4: Cut at the Walls

You are reviewing the script of one episode of *Spark, Visually*, an animated video series that shows what an Apache
Spark cluster does when PySpark code runs. Its viewers write PySpark on Databricks but have never watched a job run.
Another model wrote the script. You are the independent second opinion on technical accuracy, on draft 2. A first
review found ten things, and all ten were changed; check the new wording as closely as the rest. Review accuracy only, not style or pacing.

## Ground rules

- Check every claim against Apache Spark 4.3.0 as the run below used it: PySpark in local mode, default settings
  unless stated, adaptive query execution on. If your knowledge rests on an earlier release, say which. Where
  Databricks Runtime would behave differently in a way a Databricks user would notice, say so.
- The measured facts below were captured from Spark's own `explain()` output and event log by the episode's numbers
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
2. Every statement about Spark is correct as worded: where and why Spark cuts a plan into stages, tasks and partitions, the wall between stages, adaptive execution's extra jobs and skipped stages, `explain()` before and after a run, whole-stage code generation ids, and the stages of a join, and `explain()` over Spark Connect.
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

Part I, Episode 4: *Cut at the Walls* (jobs, stages and tasks), module M4 · Cut at the walls.

- **Question:** How does one action become jobs, stages and tasks?
- **Learning objective:** Given a query, the viewer can draw its plan, place a stage boundary at each shuffle, count each stage's tasks from its partitions, and find the same structure in `explain()` and the Spark UI.
- **Core concept:** An action submits a job. The scheduler cuts the job's plan into stages wherever rows must be exchanged. Inside a stage tasks are independent, one per partition, and every task feeding the next stage must finish before it starts.

### Misconceptions this episode targets

- One action is one job. (Several jobs are common, especially with adaptive execution.)
- A stage is a line of code. (Stages span lines, and one line, such as a join, can span stages.)
- `*(2)` in `explain()` means stage 2. (It is a code-generation id.)
- Stages always run one after another. (Independent stages, such as the two sides of a join, can run at once.)

### What earlier episodes taught

- Episode 1, Who does the work: the driver runs the notebook's code, plans and schedules; executors read their own files from storage, never through the driver. In a count, each task counts its file's rows and the partial counts go to one final task, which adds them and sends one number (103) to the driver. collect() brings every row to the driver.
- Episode 2, Piles of records: a partition, called a pile, is the rows one task processes on one core. Reads cut files into piles; after a shuffle there are 200 piles by default, unless adaptive execution merges small ones. coalesce merges piles where they stand; repartition re-deals every row. A stage ends when its slowest task ends.
- Episode 3, Nothing happens until you ask: transformations add steps to a plan and run nothing; an action runs the whole plan from storage, in one pass per pile. Its closing line, which Episode 4 answers: a count of the filtered rows was one action, yet Spark ran it as 2 jobs and 2 stages.
- Episode 5, Everyone sends to everyone (made first, as the pilot, and already approved): in a shuffle, each map task writes its output into bins by destination on its executor's local disk; after the wall, each reduce task fetches its bin from every map task. Partial aggregation sends one count per clinic instead of every row.

### The data

The cast: 103 simulated patients in five clinics (Alder, Birch, Cedar, Dune, Elm), written as four Parquet files that Spark reads as four piles of 28, 25, 25 and 25 rows. Column x = 1 marks a higher-risk patient (46 of the 103); column y marks a readmission. clinics is a five-row DataFrame, one row per clinic. The animation always shows a fixed cluster of one driver and four executors with two cores each; the numbers come from one machine in local mode.

### Deliberate choices (not errors)

- The code panel asks for `explain()` after `collect()`. With adaptive execution on, `explain()` before the run prints the starting plan (`isFinalPlan=false`), which has no `*(n)` markers; after the run it prints the plan as it ran. The screen shows that printout cropped to its final plan.
- The skipped stage is shown where it arises on its own, in adaptive execution's second job; its own number, 3, explains the UI's jump from stage 2 to stage 4.
- The join line carries `clinics.hint("merge")`: by default Spark broadcasts the clinic table (recorded below), and the hint asks for a sort-merge join so that both sides are shuffled. The clinic table is stored as Parquet, so Spark knows its size; the narration points to Episode 8.
- Adaptive execution off appears as a settings card in S07 that runs its two lines verbatim: `spark.conf.set("spark.sql.adaptive.enabled", False)` and the count as a new DataFrame (a DataFrame that has already run keeps its plan).
- S06 says that on Spark Connect `explain()` shows the starting plan even after the run; this was checked on a local Spark Connect server (recorded below).
- A finished task frees its core; the wall holds the next stage, not the finished tasks.
- Time is used only as order (stage two starts after stage one's last task), never as a duration, because the run is on a one-CPU machine.
- Stage and job numbers are this run's: they count from the start of the application, and setup jobs ran first. S06 captions them so.
- Plain words on purpose: a pile is a partition, a wall is the boundary between stages, the cast is the example data. In the On screen notes, a chip is one record, coloured by its clinic; a tray is a pile as drawn; a bench is an executor; the driver desk is the driver.

## Code on screen

These lines run verbatim in the numbers file, in this order.

```python
hi = df.filter(F.col("x") == 1)
per_clinic = hi.groupBy("clinic").count()
per_clinic.collect()
per_clinic.explain()
df.join(clinics.hint("merge"), "clinic").collect()
```

## Measured facts

### explain() after the run, as shown on screen (the code panel's line 4; the printout's Initial Plan section is cropped)

```text
== Physical Plan ==
AdaptiveSparkPlan isFinalPlan=true
+- == Final Plan ==
   ResultQueryStage 1
   +- *(2) HashAggregate(keys=[clinic#5], functions=[count(1)])
      +- AQEShuffleRead coalesced
         +- ShuffleQueryStage 0
            +- Exchange hashpartitioning(clinic#5, 200), ENSURE_REQUIREMENTS, [plan_id=53]
               +- *(1) HashAggregate(keys=[clinic#5], functions=[partial_count(1)])
                  +- *(1) Project [clinic#5]
                     +- *(1) Filter (isnotnull(x#6) AND (x#6 = 1))
                        +- *(1) ColumnarToRow
                           +- FileScan parquet [clinic#5,x#6] Batched: true, DataFilters: [isnotnull(x#6), (x#6 = 1)], Format: Parquet, Location: InMemoryFileIndex(1 paths)[file:/tmp/sv_e04/cast], PartitionFilters: [], PushedFilters: [IsNotNull(x), EqualTo(x,1)], ReadSchema: struct<clinic:string,x:int>
```

### explain() before the run, as printed (not on screen)

```text
== Physical Plan ==
AdaptiveSparkPlan isFinalPlan=false
+- HashAggregate(keys=[clinic#5], functions=[count(1)])
   +- Exchange hashpartitioning(clinic#5, 200), ENSURE_REQUIREMENTS, [plan_id=29]
      +- HashAggregate(keys=[clinic#5], functions=[partial_count(1)])
         +- Project [clinic#5]
            +- Filter (isnotnull(x#6) AND (x#6 = 1))
               +- FileScan parquet [clinic#5,x#6] Batched: true, DataFilters: [isnotnull(x#6), (x#6 = 1)], Format: Parquet, Location: InMemoryFileIndex(1 paths)[file:/tmp/sv_e04/cast], PartitionFilters: [], PushedFilters: [IsNotNull(x), EqualTo(x,1)], ReadSchema: struct<clinic:string,x:int>
```

### explain() after the run through Spark Connect, as printed (not on screen)

```text
== Physical Plan ==
AdaptiveSparkPlan isFinalPlan=false
+- HashAggregate(keys=[clinic#2], functions=[count(1)])
   +- Exchange hashpartitioning(clinic#2, 200), ENSURE_REQUIREMENTS, [plan_id=76]
      +- HashAggregate(keys=[clinic#2], functions=[partial_count(1)])
         +- Project [clinic#2]
            +- Filter (isnotnull(x#3) AND (x#3 = 1))
               +- FileScan parquet [clinic#2,x#3] Batched: true, DataFilters: [isnotnull(x#3), (x#3 = 1)], Format: Parquet, Location: InMemoryFileIndex(1 paths)[file:/tmp/sv_e04/cast], PartitionFilters: [], PushedFilters: [IsNotNull(x), EqualTo(x,1)], ReadSchema: struct<clinic:string,x:int>
```

### Everything else the numbers file recorded

Ids, counts and orders are exact; fields in milliseconds come from a one-CPU machine and matter only as order.

```json
{
 "series": "Spark, Visually",
 "part": "I · How Spark runs a job",
 "episode": 4,
 "title": "Cut at the Walls",
 "engine": {"spark": "4.3.0", "master": "local[8]", "cpus": 1, "settings": {"spark.sql.adaptive.enabled": "true", "spark.sql.shuffle.partitions": "200", "spark.sql.adaptive.coalescePartitions.enabled": "true", "spark.sql.autoBroadcastJoinThreshold": "10485760b"}, "note": "local mode: exact plans, jobs, stages and tasks; order in time, not durations"},
 "cast": {"patients": 103, "files": 4, "seed": 2026},
 "counts": {"Alder": 6, "Cedar": 10, "Dune": 6, "Birch": 24},
 "query_stage_ids": [0, 1],
 "exchanges": ["hashpartitioning(clinic#5, 200)"],
 "card_aqe_off": ["spark.conf.set(\"spark.sql.adaptive.enabled\", False)", "hi.groupBy(\"clinic\").count().collect()"],
 "counted_piles_per_clinic": {"Alder": 3, "Cedar": 3, "Dune": 4, "Birch": 4},
 "trays": {"0": {"Alder": 9, "Birch": 6, "Cedar": 10, "Dune": 2, "Elm": 1}, "1": {"Alder": 5, "Birch": 12, "Cedar": 2, "Dune": 3, "Elm": 3}, "2": {"Alder": 8, "Birch": 8, "Cedar": 3, "Dune": 3, "Elm": 3}, "3": {"Alder": 8, "Birch": 4, "Cedar": 5, "Dune": 4, "Elm": 4}},
 "connect": {"explain_after_run_is_final": false},
 "codegen_ids": [1, 2],
 "main": {"jobs": 2, "job_ids": [2, 3], "stages_run": 2, "stages_per_job": [1, 1], "skipped_per_job": [0, 1], "stage_ids": [2, 4], "skipped_ids": [3], "stage1_finish_order": [3, 2, 0, 1], "partial_counts_crossing": 14, "tasks": [4, 1], "skipped_in_second_job": 1, "total_tasks": 5, "stage1_finish_offsets_ms": [0, 9, 11, 12], "stage2_after_last_stage1_ms": 289},
 "again": {"stages_run": 1, "stages_skipped": 1},
 "aqe_off": {"jobs": 1, "stages_run": 2, "tasks": [4, 200], "total_tasks": 204, "nonempty_piles": [21, 84, 114, 115]},
 "join": {"stages_run": 3, "input_tasks": [4, 1], "join_tasks": 1, "inputs_overlap": true, "stages_in_order": [{"tasks": 4, "start_ms": 0, "end_ms": 161}, {"tasks": 1, "start_ms": 12, "end_ms": 159}, {"tasks": 1, "start_ms": 482, "end_ms": 706}], "note_order": "the two input stages are submitted together; which gets the lower stage number varies", "windows_ms": [[0, 161], [12, 159], [482, 706]], "default_strategy": "BroadcastHashJoin, broadcasting the clinic table", "note": "the merge hint asks for a sort-merge join, so both sides are dealt by clinic"},
 "checks": [
  {"name": "pinned engine: Apache Spark 4.3.0", "ok": true, "detail": "4.3.0"},
  {"name": "the per-clinic counts of higher-risk patients", "ok": true, "detail": "{'Alder': 6, 'Cedar': 10, 'Dune': 6, 'Birch': 24}"},
  {"name": "the join keeps every patient", "ok": true, "detail": "103"},
  {"name": "the settings card's count gives the same answer", "ok": true, "detail": ""},
  {"name": "by default Spark broadcasts the clinic table (the shortcut the hint turns down)", "ok": true, "detail": "+- FileScan parquet [clinic#44,card#45] Batched: true, DataFilters: [i"},
  {"name": "through Spark Connect, explain() after the run still prints the starting plan, without codegen markers", "ok": true, "detail": "pyspark.sql.connect.session"},
  {"name": "lines 1 and 2 and explain() run no job", "ok": true, "detail": ""},
  {"name": "before the run, explain() shows the starting plan, with no codegen markers", "ok": true, "detail": ""},
  {"name": "the printed plan as it ran has one Exchange line, and adaptive execution's own query stages 0 and 1", "ok": true, "detail": "[0, 1]"},
  {"name": "after the run, explain() shows the plan as it ran, with codegen markers *(1) and *(2)", "ok": true, "detail": "[1, 2]"},
  {"name": "the action's jobs belong to one SQL execution", "ok": true, "detail": "{'1'}"},
  {"name": "the plan as it ran (the UI's SQL tab) is final and carries codegen ids 1 and 2", "ok": true, "detail": "[1, 2], 16 numbered operators"},
  {"name": "explain() shows one Exchange, hash-partitioned by clinic into 200", "ok": true, "detail": "[('hashpartitioning', 'clinic#5, 200')]"},
  {"name": "with adaptive execution: two jobs, two stages that run, four tasks then one", "ok": true, "detail": "2 jobs, stages [2, 4], tasks [4, 1]"},
  {"name": "the second job lists the first stage's work again, as a skipped stage (new number, same four tasks)", "ok": true, "detail": "skipped [3]"},
  {"name": "the wall: stage 2 starts only after stage 1's last task has finished", "ok": true, "detail": "stage 2 at 289 ms after the last stage-1 task"},
  {"name": "14 partial counts cross the wall: one per clinic present in each pile", "ok": true, "detail": "14 records written; piles per clinic {'Alder': 3, 'Cedar': 3, 'Dune': 4, 'Birch': 4}"},
  {"name": "stage 1's tasks finish at different moments, and wait", "ok": true, "detail": "[0, 9, 11, 12]"},
  {"name": "codegen ids are not stage numbers", "ok": true, "detail": "codegen [1, 2], stages [2, 4]"},
  {"name": "running the action again skips its first stage: the shuffle files are reused", "ok": true, "detail": "ran [15], skipped [14]"},
  {"name": "adaptive execution off: only the piles holding a clinic's counts have rows", "ok": true, "detail": "[21, 84, 114, 115]"},
  {"name": "without adaptive execution: one job, two stages, four tasks then 200", "ok": true, "detail": "1 job, tasks [4, 200]"},
  {"name": "the join line runs as three stages", "ok": true, "detail": "{20: 4, 21: 1, 24: 1}"},
  {"name": "its two input stages run at the same time", "ok": true, "detail": "{20: (0, 161), 21: (12, 159), 24: (482, 706)}"}
 ]
}
```

## The script

Each clip is one stretch of voiceover. "On screen" describes the animation; "Narration" is spoken.

### S01 · Cold open

- **s1_1**. On screen: The code panel holds Episode 3's filter and a new line, a count per clinic. Beside the driver, last episode's tally: 1 action, 2 jobs, 2 stages.
  Narration: “Last time, a single count ran as 2 jobs and 2 stages. Here's a new question for our cast: how many higher-risk patients does each clinic have?”
- **s1_2**. On screen: Line 3 glows: collect. A schematic of Spark's UI fills in from this run: 2 jobs, 2 stages and 5 tasks that ran, with one more stage beside them, greyed: skipped.
  Narration: “One action answers it. Spark ran it as 2 jobs, 2 stages and 5 tasks. Where do those numbers come from?”

### S02 · Title

- **s2_1**. On screen: Series wordmark, Spark, Visually; Part I, How Spark runs a job; the episode title.
  Narration: “Welcome to Spark, Visually. Today: cut at the walls.”

### S03 · The plan

- **s3_1**. On screen: Lines 1 and 2 type. The plan on the driver desk reads: read, filter; then one more box: count per clinic. The benches stay empty.
  Narration: “Line one is last episode's filter. Line two asks for a count per clinic, and the driver adds it to the plan. Nothing has run yet. But this step is different.”
- **s3_2**. On screen: The cast's four piles; Alder's higher-risk patients glow in the three piles that hold them. The count box splits into the plan Spark will run: partial count, exchange, count; the exchange lights.
  Narration: “A clinic's patients sit in different piles. So when it runs, Spark counts in two steps. First, each pile counts its own patients, clinic by clinic. Then those partial counts must meet, so each clinic's numbers move to one place. That step is an exchange.”

### S04 · Cut at the walls

- **s4_1**. On screen: A cut through the plan at the exchange; two tinted bands: stage 1 (read, filter, partial count) and stage 2 (count).
  Narration: “Spark cuts the plan at every exchange, and each piece is a stage. Stage one reads, filters and counts each pile. Stage two finishes the counts.”
- **s4_2**. On screen: A wall drops across the executors' lanes, between stage 1 and stage 2.
  Narration: “Between the two stands a wall: nothing beyond it starts until everything before it has finished. Stage two can't start early, because it needs a piece from every pile.”
- **s4_3**. On screen: Stage 1's band gains four task tiles, one per pile.
  Narration: “Inside a stage, the work is split one task per pile. Four piles, so stage one runs 4 tasks.”

### S05 · Running the job

- **s5_1**. On screen: Line 3 glows: per_clinic.collect(). Four tasks drop. Each tray's chips stream through the filter and settle as small partial counts, clinic by clinic. As each task finishes, in this run's order, its core goes dark; the wall holds stage two.
  Narration: “Line three is the action. Four tasks go down and run side by side. Each one finishes when its own pile is done, and stage two can't begin until the last one does.”
- **s5_2**. On screen: The last tick; the wall opens. 14 partial counts cross to one task; the final counts appear: Alder 6, Birch 24, Cedar 10, Dune 6. Four rows rise to the driver.
  Narration: “When the last one finishes, the wall opens. The partial counts cross, and stage two adds them up. Alder has 6 higher-risk patients, Birch 24, Cedar 10 and Dune 6. Elm has none, so it gets no row at all.”
- **s5_3**. On screen: Stage 2's 200 piles, almost all empty, merge into one; one task. The tally reads 4 tasks, then 1.
  Narration: “And stage two's tasks? The exchange deals into 200 piles by default. These are tiny, so Spark merges them into one: one pile, one task. Four tasks, then one: those are our 5.”

### S06 · Reading the plan

- **s6_1**. On screen: Line 4 types: per_clinic.explain(). The printed plan appears, cropped to the final plan, the one that ran; read from the bottom up: scan, filter, partial count; then the Exchange line lights, with its 200.
  Narration: “Line four prints the plan Spark ran. Read it from the bottom up: the scan, the filter, the partial count. Then a line that starts with Exchange. That's the wall, and the 200 piles it deals into.”
- **s6_2**. On screen: The *(1) and *(2) markers light, then the query stages 0 and 1; beside them, the UI's stage numbers, captioned as this run's: 2 and 4.
  Narration: “Beside the steps sit star-one and star-two. They aren't stage numbers: they number the chains of steps Spark compiles into one function. Adaptive execution counts its query stages, 0 and 1, its own way too. Spark's UI numbered our two stages 2 and 4.”
- **s6_3**. On screen: A caption under the plan: on Spark Connect, explain() shows the starting plan, even after the run.
  Narration: “One catch: on Spark Connect, which some Databricks compute uses, explain() shows the starting plan, even after the run. To see the plan that ran, use the Spark UI, or the query profile.”

### S07 · Reading the UI

- **s7_1**. On screen: A schematic Jobs page from this run: job 2, one stage, 4 tasks; job 3, one stage plus one skipped, 1 task.
  Narration: “Now, why two jobs? With adaptive execution on, as it is by default, Spark runs stage one as a job of its own. It looks at how much stage one actually wrote, and only then plans the rest. That's episode 12.”
- **s7_2**. On screen: The second job's skipped stage greys out, with its own number: 3.
  Narration: “The second job lists that first stage again, marked skipped. Its work was done, and its output was still there to reuse. The skipped copy even took a number of its own: 3.”
- **s7_3**. On screen: A settings card runs its two lines: adaptive execution off, and the same count again. The UI schematic: 1 job, the same two stages, 4 tasks, then 200.
  Narration: “Switch adaptive execution off and count again, and you get 1 job with the same two stages. But now stage two runs 200 tasks, one per pile, and most of those piles are empty.”

### S08 · Not always in line

- **s8_1**. On screen: Line 5 types: df.join(clinics.hint("merge"), "clinic").collect(). The plan: two reads, each with its own exchange, meeting at a join; three stages.
  Narration: “One last line: join the cast to a small table of clinics. Spark would normally take a shortcut with a table this small, and the hint asks it not to. That's episode 8. So both sides are dealt out by clinic: each side gets its own stage, and a third stage joins them.”
- **s8_2**. On screen: A timeline from this run: the two input stages' bars overlap; a wall; then the join stage.
  Narration: “Neither input needs the other, so there's no wall between them, and they run at the same time. One line of code, three stages: a stage isn't a line of code.”

### S09 · Recap

- **s9_1**. On screen: Three recap cards: an action starts one job or several; the plan is cut into stages at every exchange; one task per pile, and a wall wherever one stage needs another's output.
  Narration: “Let's gather it up. An action starts one job or several, and adaptive execution is one reason for several. Spark cuts its plan at every exchange, and each piece is a stage. A stage runs one task per pile, and a wall stands wherever one stage needs another's output.”
- **s9_2**. On screen: The wall alone across the lanes; the next episode's title: Everyone sends to everyone.
  Narration: “But what exactly crosses the wall? Next time: everyone sends to everyone.”
- **s9_3**. On screen: Series end card with the next episode title.
  Narration: “This has been Spark, Visually. Thanks for watching the cluster with us.”

## Open questions

1. Databricks: on Photon compute the plan shows Photon's operator names and no `*(n)` markers, `spark.sql.shuffle.partitions` may be `auto`, and serverless compute has a query profile instead of the Spark UI. Would the episode mislead a viewer on such compute, and what would one caption in S06 need to say?
2. S09: "a wall stands wherever one stage needs another's output." Is that accurate as a general rule, broadcast joins included?
3. S06: "Adaptive execution counts its query stages, 0 and 1, its own way too." Accurate as worded for Spark 4.3?
