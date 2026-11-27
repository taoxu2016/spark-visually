# Spark, Visually · Part I · Episode 1: Who Does the Work

**Status: script approved by the owner and voiced with am_fenrir; animation cut at 720p (3:33) and with the owner for
review (26 Nov 2026). Built straight from the script, with no separate animatic (the owner's call).**

**Revised 26 Nov 2026, after the second-model review:** code line 5 now uses the UDF's value, `df.select(F.sum(flip("y"))).collect()` (61), because a count prunes a UDF whose value it doesn't use and never calls it; the numbers file proves this with a UDF that always fails. s9_2 now says the count "read the table with 4 tasks" (only that clip was re-voiced). 28 of 28 checks pass; the cut runs 3:34.

Blueprint module M1: one driver plans and hands out the work; executors, each with a few cores, read the data and do
it; rows stay with the executors unless the code asks for them. With the Spark Connect beat the decision list agreed.

| File | What it is |
|---|---|
| `e01_numbers.py` | Runs the code panel's five lines verbatim on Apache Spark 4.3.0, in classic Spark (local[8]) and again through a local Spark Connect server, and computes every number the episode shows or says (26 checks). Writes `e01_numbers.json`. |
| `e01_numbers.json` | Tasks, rows per task, partial counts and the final task from the event log; the collect's rows and the Python memory they take on the driver, for the cast and the cast ten thousand times over; the UDF's plan node; what Spark Connect returns; every check. |
| `narration/narration.json` | The approved script in the series format: 9 scenes, 21 clips, 518 words, voiced with am_fenrir at 0.92. |
| `audio/` | The voiceover the scenes are cut to: one WAV per clip in `segments/` and `manifest.json` with exact sentence timings (3:16 of speech, 159 words a minute). |
| `episode/episode01.py` | The animation: nine scenes cut to the voice. A laptop floods; the home shot is built piece by piece; the notebook meets the driver and, with Spark Connect, leaves it; four tasks read their files and count, and one final task adds; a collect brings every row up; a Python UDF ships rows out to Python workers. Every action reads its files again: nothing stays in the trays (M3). |

The script is reviewed in the Claude Doc "Spark, Visually · Episode 1: Who Does the Work · Script for Review":
first by the spark-reviewer agent, on a different model from the writer's, then by the owner.

    pip install "pyspark[connect]==4.3.0"     # needs Java 17 or 21; the extras run the Spark Connect check
    python e01_numbers.py                     # from this folder: prints the ledger, exits 1 if a check fails

The cast and the event-log reader are shared with every episode in `sv/cast.py` and `sv/runlog.py`. Episode 5's
numbers file now uses them too; its output is unchanged (41 of 41 checks, no value different).

What the run changed in the plan: bytes on the wire mislead at this size (every task reports back, and collected rows
travel compressed), so the episode measures a collect by the rows and the Python memory they take on the driver. And
in Spark 4.3 a plain Python UDF runs Arrow-optimized: its plan node is `ArrowEvalPython`.

The episode, built with Model in Motion's tools (720p review cut with captions burned in; `build/` is not committed):

    MIM_REPO=path/to/model-in-motion
    for S in S01_ColdOpen S02_Title S03_Parts S04_Notebook S05_Read S06_Count S07_Collect S08_Python S09_Recap; do
      manim -qm --disable_caching --media_dir build/media episode/episode01.py $S; done
    python $MIM_REPO/tools/assemble.py . --quality 720p30        # build/final/: the cut, captions, timed script

Still to do: the owner's review of the animation, then a watch-through with sound. The second-model review is not
recorded; it can still run on the script. 1080p60 when the owner asks for it.
