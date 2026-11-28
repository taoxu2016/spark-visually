# Spark, Visually

An animated series that shows what an Apache Spark cluster does when PySpark code runs: who does the work, how a
table is cut into piles, why nothing happens until you ask, where a job is cut into stages, and what crosses the wall
between them. It is styled after [Model in Motion](https://github.com/taoxu2016/model-in-motion), whose tools it
shares. The blueprint plans 19 modules in three parts. Every dataset is simulated; no patient data is used anywhere.

## Episodes

| Part I · How Spark runs a job | Status |
| --- | --- |
| 1 · Who Does the Work | Approved and reviewed by a second model; cut 3:34 |
| 2 · Piles of Records | Approved and reviewed; cut 3:59. The timelines use this run's task times until the Databricks run arrives (below) |
| 3 · Nothing Happens Until You Ask | Approved and reviewed; cut 3:15 |
| 4 · Cut at the Walls | Approved and reviewed; cut 4:33 |
| 5 · Everyone Sends to Everyone | Approved and reviewed; cut 4:54 (the pilot) |
| 6 · Say What, Not How | Approved and reviewed; cut 3:31 |

The review cuts, built in the cloud from this repository, are on the [Review cuts](../../releases/tag/review-cuts) release.

## How an episode is made

1. **Numbers.** `eNN_numbers.py` runs every line of the code panel verbatim on Apache Spark 4.3.0 and writes
   `eNN_numbers.json`. Every number the episode shows or says comes from that file, and every check in it must pass.
   Counts, plans and orders are exact anywhere; durations come only from a named run.
2. **Script.** `narration/build_script.py` writes `narration/narration.json`. It traces every spoken number to the
   numbers file and refuses to build otherwise.
3. **Review.** A second model checks Spark accuracy, then the owner reviews. `tools/review_prompt.py` builds a
   self-contained prompt from the episode's own files (`review/eNN_accuracy_review_prompt.md`).
4. **Voice, animation, cut.** Model in Motion's tools voice the script (Kokoro, am_fenrir at 0.92), the scenes in
   `episode/episodeNN.py` are rendered with Manim to the voice's cues, and the cut is assembled with captions burned in.

## Layout

```
sv/                          the series' shared library: the cast, the home shot, the board, the event-log reader
tools/review_prompt.py       builds an episode's review prompt
external/model-in-motion     Model in Motion's tools, pinned (a submodule)
p1_how_spark_runs/eNN_*/     one folder per episode:
    eNN_numbers.py / .json     the measured facts
    narration/                 build_script.py and narration.json
    episode/episodeNN.py       the scenes
    audio/manifest.json        the voiceover's timings (the audio itself is rebuilt, not committed)
    review/                    the review brief and prompt; reports go here too
```

## Working in the browser

Nothing needs installing.

- **Checks** run on every push and pull request (Actions → checks): every numbers file's checks, and every script and
  review prompt rebuilt from its builder, which must change nothing.
- **Building a cut**: Actions → build episode → Run workflow, with the episode's folder, for example
  `p1_how_spark_runs/e04_cut_at_the_walls`. The 720p cut goes to the [Review cuts](../../releases/tag/review-cuts) release.
- **Reviewing**: a reviewer reads an episode's folder directly, or pastes its `review/eNN_accuracy_review_prompt.md`.
  Its report goes in `review/eNN_review_report.md`, and the fixes come as a pull request that cites the findings.

## The Databricks timings (Episode 2)

`p1_how_spark_runs/e02_piles_of_records/e02_timings_databricks.py` measures real task times on an eight-core cluster.
Import it into Databricks (Workspace → Import, from this file's raw URL or a download), run it on a single-node
cluster with 8 cores in dedicated access mode, and add the JSON it prints to the episode folder as `e02_timings.json`
(Add file → Upload files, or paste it into the chat). Episode 2's timeline scenes then re-render from it.
