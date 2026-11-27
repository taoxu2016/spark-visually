# Spark, Visually · Part I · Episode 2: Piles of Records

**Status: script approved by the owner (one line later changed for accuracy) and voiced with am_fenrir; animation cut
at 720p (3:53) and with the owner for review (26 Nov 2026). Timelines from this episode's own run until the Databricks
run arrives.**

**Revised 26 Nov 2026, after the second-model review:** s1_1 now says the count "read the table with 4 tasks" (and the one-file case "reads it with 1 task"), matching Episode 1; s4_3 says "at least tens of thousands of rows", a floor the numbers file now checks; s5_1 names the 8 MB row groups behind the 61 MB file's 8 piles (with Spark's default row groups the same rows land in one pile of eight, also checked) and the 4 MB floor on a piece; s7_1 says "at scale", and its timeline is titled with the data it measures. The merge pointer in S05 now names Episode 4. Only those four clips were re-voiced; 28 of 28 checks pass.

Blueprint module M2: a partition is a pile of rows that one task works on, on one core; parallelism is the smaller of
partitions and task slots; too few piles idle cores, too many pay a fixed cost per task, uneven ones make everyone wait.

| File | What it is |
|---|---|
| `e02_numbers.py` | Runs the code panel's four lines verbatim on Apache Spark 4.3.0 (local[8]) and computes every number the episode says (24 checks): piles from one file, four files, a big file and 32 small files; piles of created data and after a shuffle; tasks at 1, 4, 8 and 32 piles and how many ever run at once; the fixed cost per task in CPU time; what coalesce and repartition move; uneven piles. Writes `e02_numbers.json`. |
| `e02_timings_databricks.py` | A Databricks notebook that measures the timelines' real task start and end times on eight real cores. Run it on a single-node 8-core cluster (dedicated access mode) and send back the JSON it prints; it becomes `e02_timings.json`. |
| `narration/narration.json` | The approved script in the series format: 9 scenes, 19 clips, 574 words, voiced with am_fenrir at 0.92. s4_3's "about 17,000 rows" became "tens of thousands of rows" after approval: warm runs measure 31,000 to 40,000. |
| `audio/` | The voiceover the scenes are cut to (3:34 of speech, 161 words a minute). |
| `episode/episode02.py` | The animation: nine scenes cut to the voice. Every re-deal moves each patient to the pile Spark put them in; the 32 small piles sit on the benches in the lanes and rounds of their timeline bars; the timelines read `e02_timings.json` when it is present, else this run's real task times, labelled with the configuration. |

Why two files of numbers. The sandbox this was written in has one CPU: its eight local slots share one processor, so
eight tasks take as long as one, and a timeline measured there would show no gain from parallelism. Counts are exact
anywhere; the fixed cost is measured in CPU time, which sharing a processor doesn't inflate; the timelines need eight
real cores.

    pip install pyspark==4.3.0      # needs Java 17 or 21
    python e02_numbers.py           # from this folder: prints the ledger, exits 1 if a check fails

The episode (720p review cut, captions burned in; `build/` is not committed):

    MIM_REPO=path/to/model-in-motion
    for S in S01_ColdOpen S02_Title S03_Piles S04_Rounds S05_Where S06_Merge S07_Uneven S08_Rule S09_Recap; do
      manim -qm --disable_caching --media_dir build/media episode/episode02.py $S; done
    python $MIM_REPO/tools/assemble.py . --quality 720p30

When the Databricks JSON arrives, save it as `e02_timings.json` here and re-render S04_Rounds and S07_Uneven, then
re-assemble. Still to do: the owner's review, then a watch-through with sound.
