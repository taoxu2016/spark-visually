# Spark, Visually · Part I · Episode 5: Everyone Sends to Everyone

**Status: script approved; look approved on the key frames; voiced with am_fenrir (chosen from the second audition);
animation pass cut at 720p (4:47) and with the owner for review (26 Nov 2026). Next: the 1080p60 final render.** The pilot: this episode set the series' look.

**Revised 26 Nov 2026, after the second-model review:** s7_2 now compares the same count with and without partial counts (Spark 4.3's bypassPartialAggregation): on the cast, rows fall from 103 to 20 but bytes only from 1,075 to 998, because each bin carries a fixed overhead; at ten thousand times the cast, from 1,042,872 bytes to 2,048. s4_2 says the "positive" remainder (pmod); s8_3 says adaptive execution merges the "tiny" bins. The code panel's lines 2 to 4 end in .collect(), so they run as typed, and a note under it says adaptive merging is off. S06 marks reducers 2 and 3 "nothing to fetch", and S08 labels its columns "patients, if every row moved". Only those three clips were re-voiced; 44 of 44 checks pass. (The script builder's voice setting was also brought in line with the approved am_fenrir.)

Blueprint module M5: a shuffle re-deals the table by key. Map tasks sort their rows into bins on local disk, a wall,
then every reducer fetches its bin from every machine; partial aggregation sends less.

| File | What it is |
|---|---|
| `e05_numbers.py` | Runs the code panel's five lines verbatim on Apache Spark 4.3.0 (local mode) and computes every number the episode shows or says (41 checks). Writes `e05_numbers.json`. |
| `e05_numbers.json` | Plans from `explain()`, Spark's own hash assignments, task, record and byte counts from the event log, and every check. |
| `narration/narration.json` | The approved script in the series format, voiced with am_fenrir at 0.92. |
| `audio/` | The voiceover the scenes are cut to: one WAV per clip in `segments/` and `manifest.json` with exact sentence timings (4:25 of speech, 161 words a minute). |
| `episode/episode05.py` | The animation: nine scenes cut to the voice, every motion on its spoken cue. The cast is dealt, sorted into bins by Spark's hash, fetched across the network into the reducers' piles and gathered into partial counts; tasks run against the wall. Scenes S03 to S08 stand on one frame and hand it on, so their cuts don't show. |
| `e05_cast.py` | The cast as the episode draws it: trays, bins and reducer piles rebuilt from the numbers file's deal and Spark's hash, checked against `e05_numbers.json`. Shared by the animatic and the key frames. |
| `audition_voices.py`, `audition/` | S01 and S02 voiced at the series' settings in every candidate voice or blend (`am_michael:0.6+am_onyx:0.4`), a one-file sampler, and each take's pitch and pace (`measure.json`). Round 1: am_michael, am_fenrir, am_puck (chosen, then turned down). Round 2: the other ten male voices but am_santa, and four blends; the owner chose am_fenrir. |
| `preview_stills.py`, `stills/` | Four key frames (S01, S04, S06, S07) rendered in Manim on `sv/stage.py`, every chip and number from `e05_numbers.json`; review only. |

The script is reviewed in the Claude Doc "Spark, Visually · Episode 5: Everyone Sends to Everyone · Script for Review":
first by the spark-reviewer agent, on a different model from the writer's, then by the owner.

    pip install pyspark==4.3.0          # needs Java 17 or 21
    python e05_numbers.py               # from this folder: prints the ledger, exits 1 if a check fails

The run corrected two things in the blueprint, now fixed there:

- With five reducers, Spark's hash sends Cedar, Dune and Elm to the same one. Two reducers get nothing, and only 12 of
  the 20 possible bins hold rows.
- `count()` does not add its partial counts on the driver: they cross a wall to one final task, which sends one number up.

Local mode has no network, so the figures for bins crossing it come from the home shot's placement (tray k on bench k,
reducer r on bench r mod 4) and are labelled that way on screen.

The episode, built with Model in Motion's tools (720p review cut with captions burned in; `-qh` and `--quality 1080p60`
for the final; `build/` is not committed):

    MIM_REPO=path/to/model-in-motion
    for S in S01_ColdOpen S02_Title S03_Wall S04_Bins S05_Wait S06_Fetch S07_SendLess S08_Collide S09_Recap; do
      manim -qm --disable_caching --media_dir build/media episode/episode05.py $S; done
    python $MIM_REPO/tools/assemble.py . --quality 720p30        # build/final/: the cut, captions, timed script

Still to do, in order: the owner's review of the animation, the 1080p60 final render, a watch-through with sound.
The second-model review is not recorded; it can still run on the script.

    MIM_REPO=/path/to/model-in-motion manim -s -qh --media_dir build/media preview_stills.py \
        FrameA_ColdOpen FrameB_IntoBins FrameC_Fetch FrameD_SendLess
    KOKORO_MODELS=~/tts_models python audition_voices.py                   # the three auditions
    KOKORO_MODELS=~/tts_models python path/to/model-in-motion/tools/make_voiceover.py .   # the voiceover
