# Episode 6 · second-model review of script draft 1

Pasted by the owner on 28 November 2026. The reviewer worked from `e06_accuracy_review_prompt.md` (draft 1). Draft 2
takes all six findings; see the pull request for what changed where.

---

I ran Episode 6's lines on PySpark 4.3.0, classic and through a local Spark Connect server. I rebuilt the sorted file
(cast × 10,000, Spark's writer, 512 KB blocks). Because the bytes needed it, I traced the real file reads with
`strace`. I also spot-checked 3.5.10 and 4.0.5. The plans, the same-plan claim, analysis, row-group skipping and the
UDF blocking pushdown all hold. The problems are the byte figures in S05, which phase does what, what travels to
Python, and Connect. I did not run Databricks.

## A. Findings

**1. Misleading · s5_2**
“Of the 12 it reads, it fetches just two columns: about 2.6 kilobytes, where all four would be nearly 2 megabytes.”
(On screen: “about 2.6 kB, against 1.9 MB”.)
- **Problem.** 2.6 kB is the size of the column chunks the reader needs, from the file's metadata. It isn't what the
  reader fetches.
  - **What I measured.** With `strace` on my rebuilt file (13 row groups), the query with pushdown on read 362,533
    bytes from the file. 321 KB of that was the unneeded `patient` column. With pushdown off it read 46,446 bytes.
    The same filter on all four columns read 2.26 MB.
  - **Cause.** The filtered read path goes through Spark's 64 KiB Hadoop buffer (`spark.buffer.size` feeds
    `io.file.buffer.size`). Each surviving group's tiny `clinic` chunk is read as a 64 KiB block, which drags in 61 KB
    of the next group's `patient` data. With `spark.buffer.size=4096` the same query read 62 KB.
  - **Your own figures show it.** The event log's 767,762 bytes with pushdown against 16,148 without is 12 groups ×
    64 KiB. The metric wasn't inconsistent; it was reporting what the reader did.
  - **Footer.** The footer isn't in the 2.6 kB either, about 11 KB on the 25-group file.
- **Consequence.** A viewer who checks Input Size in the Spark UI will see pushdown increase the bytes on this file.
  The rows and groups are right: 13 groups skipped, 465,655 of 1,030,000 rows read.
- **Suggest.** “Of the 12 it reads, it needs just two columns: about 2.6 kilobytes of column data, where all four would
  be nearly 2 megabytes.” On screen, label it “column sizes from the file's metadata”. “Nearly 2 megabytes” is 99% the
  patient column, so the label could say so.
- **Evidence.** The traces and the buffer test above; your event-log figures. High on 4.3.0 with local files. Moderate
  for a cloud reader.

**2. Misleading · s4_1, s4_2 and s6_2**
“It pushes the filter down into the read…” (On screen: the filter slides down into the read; patient and y fade out of
the read.)
- **Problem.** Your own facts disagree with the picture.
  - **The optimised plan.** It still has `Filter` above a `Relation` listing all four columns. `PushedFilters` and
    `ReadSchema: struct<clinic,x>` appear only in the physical plan's Scan.
  - **The Filter stays.** Spark hands the scan a copy of the filters during physical planning and re-applies the filter
    after the scan. The formatted plan shows both.
  - **The mismatch.** S04 says the filter moves into the read at the optimiser phase, and S06 then lists “the filters
    it took” followed by “the filter”.
- **Suggest.**
  - **s4_1.** “It adds a check you never wrote: that x isn't null. And it keeps only what the query needs.”
  - **s6_2.** “The scan shows what it was handed: a copy of the filters, and the two columns it reads. The filter still
    runs after it, to be sure.”
- **Evidence.** The optimised and physical plans in your facts; `FileSourceStrategy` (`pushedFilters`,
  `afterScanFilters`); `InferFiltersFromConstraints`, `ColumnPruning`. High.

**3. Imprecise · s7_1**
“…every row travels to Python and back, just to be checked.” (On screen: chips go out to Python and back.)
- **Problem.** Only the function's argument crosses. The rest of the row waits in the JVM and is joined with the
  boolean that comes back. The Python workers read 4.2 bytes per row for `UDF(x)`, against 208 bytes per row when a
  200-byte column was also an argument. Values travel in Arrow batches of up to 10,000.
- **Suggest.** “…and the value it checks, x, travels to Python and back for every row, just to be checked.” On screen,
  the chips stay on the bench; only x goes and a tick returns.
- **Evidence.** `EvalPythonEvaluatorFactory` (`HybridRowQueue`, argument projection); my test. High.

**4. Imprecise · s3_2 (Spark Connect)**
“Over Spark Connect, the same error waits until the first action.” (Caption likewise.)
- **Problem.** Three corrections.
  - **Surfacing.** The DataFrame error surfaced on `columns`, `explain("formatted")` and `count()`, not only on actions.
  - **SQL differs.** `spark.sql("… WHERE xx = 1")` raised `AnalysisException` at the call even over Connect, and a
    syntax error raised `ParseException`. So the two forms differ here.
  - **The panel.** As typed, line 5 builds a DataFrame nobody uses, so over Connect it never errors at all.
- **Suggest.** “Over Spark Connect, DataFrame code waits until the plan is first needed; a SQL string still fails at
  once.” Make the caption's example use the DataFrame, for instance with `.count()`.
- **Evidence.** My Connect run on 4.3.0. High.

**5. Imprecise · s6_2 and s1_2 (code on screen)**
“Explain, formatted, shows it all, node by node.”
- **Problem.** `explain("formatted")` prints only the physical plan. The parsed, analysed and optimised plans of S03 and
  S04 come from `explain(True)`, which the panel never shows. s1_2's side-by-side plans need a `q2.explain("formatted")`
  line the panel doesn't have.
- **Suggest.** “Explain, formatted, shows the physical plan, node by node.” Add `q1.explain(True)` and
  `q2.explain("formatted")` to the panel.
- **Evidence.** My run. High.

**6. Imprecise · s4_2**
“Spark chose the steps, and their order.”
- **Problem.** Nothing is reordered in this query. The filter was written first and runs first; the `isnotnull` and
  Project are insertions. The misconception “runs steps in the order I wrote them” goes unrebutted.
- **Suggest.** Drop “and their order”, or show a real reordering. For
  `df.groupBy("clinic").count().filter(F.col("clinic") == "Birch")`, the optimised order is Aggregate, Project, Filter,
  Relation, so the filter moves below the count.
- **Evidence.** My run. High.

## B. Open questions

1. **s3_1.** Accurate for both forms in classic. DataFrame code is analysed as each line builds its DataFrame, and
   `spark.sql()` parses and analyses at the call; a typo raised there with no job. Over Connect only SQL stays eager
   (finding 4).
2. **s5_2.** Fair as the size of the column data the reader needs. It is not a measure of what it fetches (finding 1).
3. **s7_1.** Not as worded. The rows don't travel; the argument column does (finding 3).

## C. Verified

- **Plans.** Parsed (`'UnresolvedRelation`), analysed, optimised and physical plans match your facts. The optimiser adds
  `isnotnull(x)` and a Project. The count splits into partial and final around the Exchange. Building the queries and
  explaining them ran no job.
- **Same plan.** The SQL and DataFrame forms give identical optimised and physical plans with ids set aside.
- **Row groups.** Pushdown skipped every group whose x maximum is 0. On my 13-group file that left 471,519 of 1,030,000
  rows read; with pushdown off, and with the UDF, all rows. The unsorted file skipped nothing.
- **UDF.** The plan puts `Filter pythonUDF0` over `ArrowEvalPython`, with empty `PushedFilters`. On 3.5.10 and 4.0.5
  the node is `BatchEvalPython`, so Databricks users on those releases will see that name.
- **The five recurring problems.**
  - **(a) Hidden settings.** Pushdown has its switch on screen. The hidden one is the 64 KiB read buffer (finding 1).
  - **(b) Wrong comparison.** Found in the bytes check, which uses a metadata model instead of reads (finding 1). The
    same-plan check is sound.
  - **(c) Counts.** None in this script.
  - **(d) Rules from one example.** Found in findings 2 and 6.
  - **(e) Connect.** Found in finding 4. `explain` is identical over Connect before a run.
- **Columnar reads.** “Reads only what it needs” (s8_1) is true of the plan's read schema everywhere. It shrinks bytes
  for columnar formats, while CSV and JSON files are still read whole.

## D. Unverified

1. **Databricks.** I'm unsure about Photon plan nodes (`PhotonScan` and friends) and Delta's file-level skipping at
   planning, and about how a cloud reader's read-ahead behaves on tiny chunks.
   ```python
   d = spark.read.parquet(path_to_sorted_file)
   d.filter("x = 1").groupBy("clinic").count().collect()   # Spark UI > Input Size, with filterPushdown true, then false
   ```
   Your numbers file should show 767,762 against 16,148, as in your facts.
2. **Your own file.** I rebuilt it (13 groups rather than 25), so the figures I measured are mine.
