# Episode 6: Say What, Not How · script for review

Draft 2, revised after the second-model review (see review/e06_review_report.md): for the owner's review. Not voiced.

8 scenes, 16 clips, 524 words. Every number comes from `e06_numbers.json`.

## S01 · Cold open

| Clip | On screen | Narration |
| --- | --- | --- |
| s1_1 | The code panel types one question two ways: the DataFrame line, then the same question in SQL. | Here's one question, written two ways: as DataFrame code, and as SQL. Higher-risk patients, counted by clinic. |
| s1_2 | Line 4 glows: both formatted plans appear side by side, node by node; every node matches, and the pairs light together. | Which one runs faster? Neither. Spark turns both into the same plan, step for step. |

## S02 · Title

| Clip | On screen | Narration |
| --- | --- | --- |
| s2_1 | Series wordmark, Spark, Visually; Part I, How Spark runs a job; the episode title. | Welcome to Spark, Visually. Today: say what, not how. |

## S03 · Analysis

| Clip | On screen | Narration |
| --- | --- | --- |
| s3_1 | Line 5 glows: q1.explain(True) prints all four phases. The SQL's first plan on the driver desk: plain names, the table unresolved. Analysis looks each one up in the catalog, and each column gains its type. | First, Spark reads what you wrote and looks up every name: the table, and each column, with its type. In classic PySpark, that happens line by line, as you write. |
| s3_2 | Line 6 types: df.filter(F.col("xx") == 1). It fails at once with an unresolved column; no job ran. A caption: over Spark Connect, the same DataFrame line passes, and fails when its plan is first needed (at .count(), say); a SQL string still fails at once. | So a misspelt column fails right there, on its own line, before a single task runs. Over Spark Connect, DataFrame code waits until its plan is first needed, but a SQL string still fails at once. |

## S04 · The optimiser

| Clip | On screen | Narration |
| --- | --- | --- |
| s4_1 | The analysed plan on the desk: count per clinic, filter, read. A new step appears beside the filter, isnotnull(x), and a projection of clinic slides in above it. | Then the optimiser rewrites the plan. It adds a check you never wrote: that x isn't null. And it keeps only what the query needs: clinic, for the count. |
| s4_2 | A card runs df.groupBy("clinic").count().filter(F.col("clinic") == "Birch"): in the plan, the filter written last slides below the count. | It reorders what's safe to reorder, too. Write a filter on clinic after the count, and the optimiser moves it before: fewer rows to count. You said what you wanted. Spark chose the steps. |

## S05 · The physical plan

| Clip | On screen | Narration |
| --- | --- | --- |
| s5_1 | The physical plan on the desk: the read is handed a copy of the filter and reads 2 of the 4 columns (patient and y fade from it); the filter stays above the read; the single count splits into a partial count, an Exchange and a final count, with Episode 4's wall between. | Last, Spark picks real operators. It hands the read a copy of the filter, so the read can skip what can't match, and has it read only 2 of the 4 columns. The filter still runs after the read, to be sure. And the one count becomes two: a partial count on each pile, an exchange, and a final count. That's Episode 4's wall. |
| s5_2 | Line 4 glows again: q1.explain("formatted"). Its nodes light from the bottom: Scan parquet with its PushedFilters and ReadSchema, Filter, Project, HashAggregate, Exchange, HashAggregate, AdaptiveSparkPlan. | Explain, formatted, shows the physical plan, node by node: the scan, with the filters it was handed and the two columns it reads; then the filter, the partial count, the exchange, and the final count. Before a run, it's the starting plan: adaptive execution can still change it. That's episode 12. |

## S06 · Down to storage

| Clip | On screen | Narration |
| --- | --- | --- |
| s6_1 | The cast scaled up ten thousand times, sorted by risk, written as one Parquet file of 25 row groups; each group shows the smallest and largest x it holds. | Here's why handing the filter down pays. Take the cast ten thousand times over, sorted by risk, in one file of 25 row groups. Each group records the smallest and largest x it holds. |
| s6_2 | With pushdown, the 13 groups whose largest x is 0 grey out, and 12 are read. Only clinic and x are needed: about 2.6 kB of column data, against 1.9 MB for every column of those groups, labelled: column sizes, from the file's metadata. A settings card turns pushdown off, and all 25 groups are read. | With the filter handed down, the reader skips the 13 groups that can't hold a higher-risk patient. Of the 12 it reads, it needs just two columns: about 2.6 kilobytes of column data, where all four would be nearly 2 megabytes. Those sizes belong to this sorted file. Unsorted, every group holds both kinds, and nothing is skipped. Turn pushdown off, and every group is read. |

## S07 · A black box

| Clip | On screen | Narration |
| --- | --- | --- |
| s7_1 | Lines 7 and 8 type: the same filter as a Python function. The read's pushed filters are now empty; a Python worker appears beside each bench. The rows stay on the bench: only x goes out to the worker, and a tick comes back. | Now write the same filter as a Python function. To Spark, it's a black box. It can't hand the filter to the read, so every row is read, and for every row the value it checks, x, travels to Python and back, just to be checked. |
| s7_2 | On the scaled file: all 25 row groups read, against 12. | On our sorted file, that's all 25 row groups instead of 12. Keep your logic where the optimiser can see it. |

## S08 · Recap

| Clip | On screen | Narration |
| --- | --- | --- |
| s8_1 | Three recap cards: one plan for DataFrame code and SQL; the optimiser reads only what it needs; a Python function hides its logic. | Let's gather it up. Say what you want, in DataFrame code or in SQL: it's the same plan. Spark checks the names, rewrites the plan, reads only what it needs, and picks the operators. A Python function hides its logic, and the optimiser can't help. |
| s8_2 | The plan on the desk, with a price tag beside it: blank. Next: Part II, Counting walls. | The plan is chosen. But is it cheap? Next time, Part Two begins: counting walls. |
| s8_3 | Series end card with the next episode title. | This has been Spark, Visually. Thanks for watching the cluster with us. |
