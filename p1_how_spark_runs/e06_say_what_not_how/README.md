# Spark, Visually · Part I · Episode 6: Say What, Not How

Module M6 of the series blueprint: how a DataFrame or SQL query becomes the plan Spark runs. It closes Part I.

**Status: draft 1 of the script, for review: a second model checks Spark accuracy, then the owner. Not voiced.**

| File | What it is |
| --- | --- |
| `e06_numbers.py` | Runs the code panel verbatim on Apache Spark 4.3.0 and captures the plan at every phase; checks that the DataFrame and SQL forms reach the same plan, when a misspelt column fails (classic and over Spark Connect), pushdown and pruning in the scan, the split aggregate, and the UDF that blocks pushdown; reads the scaled, risk-sorted Parquet file's row groups and column chunks. Writes `e06_numbers.json`. |
| `narration/build_script.py` | Builds `narration/narration.json` (8 scenes, 16 clips) and `review/e06_script_for_review.md`, tracing every spoken number to the numbers file. |
| `review/` | The script for review, the review brief, and the review prompt built from them. |

The scenes come once the script is approved.
