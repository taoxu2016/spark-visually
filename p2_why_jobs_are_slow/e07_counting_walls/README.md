# Spark, Visually · Part II · Episode 7: Counting Walls

Module M7 of the series blueprint: reading a job's cost from its code, by counting its walls. It opens Part II.

**Status: draft 2 of the script, revised after the second-model review (`review/e07_review_report.md`); for the owner's review. Not voiced.**

| File | What it is |
| --- | --- |
| `e07_numbers.py` | Runs the code panel and every gallery card verbatim on Apache Spark 4.3.0; counts each plan's walls (shuffle Exchanges), the jobs and stages each card runs, and the rows and bytes crossing each wall for the three versions, on the cast and on the cast ten thousand times over. Writes `e07_numbers.json`. |
| `narration/build_script.py` | Builds `narration/narration.json` (8 scenes, 15 clips) and `review/e07_script_for_review.md`, tracing every spoken number. |
| `review/` | The script for review, the review brief, and the review prompt built from them. |
