# Spark, Visually · Part I · Episode 6: Say What, Not How

Module M6 of the series blueprint: how a DataFrame or SQL query becomes the plan Spark runs. It closes Part I.

**Status: script approved by the owner (draft 2, after the second-model review in `review/e06_review_report.md`), voiced with am_fenrir (3:13 of speech) and animated; the cut (3:31) is approved.**

| File | What it is |
| --- | --- |
| `e06_numbers.py` | Runs the code panel verbatim on Apache Spark 4.3.0 and captures the plan at every phase; checks that the DataFrame and SQL forms reach the same plan, when a misspelt column fails (classic and over Spark Connect), pushdown and pruning in the scan, the split aggregate, and the UDF that blocks pushdown; reads the scaled, risk-sorted Parquet file's row groups, and from its metadata the column data the query needs (the reader's own fetches, in 64 KiB buffered reads, are kept and not said). Writes `e06_numbers.json`. |
| `narration/build_script.py` | Builds `narration/narration.json` (8 scenes, 16 clips) and `review/e06_script_for_review.md`, tracing every spoken number to the numbers file. |
| `review/` | The script for review, the review brief, and the review prompt built from them. |

`episode/episode06.py` holds the eight scenes; `audio/manifest.json` the voiceover's timings. Build the cut from the Actions tab (build episode, `p1_how_spark_runs/e06_say_what_not_how`).
