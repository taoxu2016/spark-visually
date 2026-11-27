# Spark, Visually · Part I · Episode 4: Cut at the Walls

Module M4 of the series blueprint: how one action becomes jobs, stages and tasks.

**Status: script approved by the owner (draft 2, after the second-model review) and voiced with am_fenrir; animation
cut at 720p (4:33) and with the owner for review (26 Nov 2026).**

| File | What it is |
| --- | --- |
| `e04_numbers.py` | Runs every line of the code panel verbatim on Apache Spark 4.3.0 (local mode) and reads jobs, stages, skipped stages, tasks and their order from the event log; writes `e04_numbers.json` (23 of 23 checks pass, and every count repeats exactly on a second run). Also runs S07's settings card verbatim, and the code panel through a local Spark Connect server. |
| `e04_numbers.json` | Every number the episode shows or says: the per-clinic counts, the plan before and after the run, the codegen ids, jobs and stages with adaptive execution on and off, the skipped stage, the 14 partial counts that cross the wall, and the join's three stages. |
| `narration/narration.json` | The approved script in the series format: 9 scenes, 22 clips, 692 words, voiced with am_fenrir at 0.92. |
| `audio/` | The voiceover the scenes are cut to (4:13 of speech, 164 words a minute). |
| `episode/episode04.py` | The animation: nine scenes cut to the voice. The plan grows on the driver desk and is cut at the exchange into two tinted stages; the cut drops a wall across the lanes. Stage one runs four tasks whose cores go dark as they finish; the wall opens for the 14 partial counts. Then the printed plan read bottom-up, the UI's jobs and skipped stage, a settings card, and a join's three stages on this run's timeline. |
| `review/` | The second-model review's brief and prompt (`tools/review_prompt.py` builds the prompt from the episode's files); the prompt is rebuilt for draft 2. |

What the run shows (adaptive execution on, as by default): `per_clinic.collect()` runs as 2 jobs. The first runs stage 2
(4 tasks, one per file); the second lists that stage's work again as skipped stage 3 and runs stage 4 (1 task, the 200
post-exchange piles merged into one). Stage 4 starts only after stage 2's last task finishes. With adaptive execution
off: 1 job, 4 tasks then 200. `explain()` prints the starting plan before the run (no `*(n)` markers) and the plan as it
ran afterwards (`*(1)`, `*(2)`), which is why the code panel asks for it after `collect()`. The join line carries a `merge`
hint (by default Spark broadcasts the clinic table, stored as Parquet so its size is known) and runs as three stages,
its two input stages overlapping in time. Over Spark Connect, `explain()` after the run still prints the starting plan.

Numbers:

    python e04_numbers.py

The episode (720p review cut, captions burned in; `build/` is not committed):

    MIM_REPO=path/to/model-in-motion
    for S in S01_ColdOpen S02_Title S03_Plan S04_Cut S05_Run S06_Explain S07_UI S08_Join S09_Recap; do
      manim -qm --disable_caching --media_dir build/media episode/episode04.py $S; done
    python $MIM_REPO/tools/assemble.py . --quality 720p30

Still to do: the owner's review of the cut, then a watch-through with sound.
