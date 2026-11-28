# Handoff · Spark, Visually

For the next working session (written 28 November 2026). Read this, then `README.md`.

## Where things stand

- **Part I (Episodes 1 to 6) is complete** and merged into `main`. Every episode was reviewed by a second model; all
  findings were fixed; the owner approved every script and cut. The cuts are on the
  [Review cuts](https://github.com/taoxu2016/spark-visually/releases/tag/review-cuts) release.
- **Episode 7, Counting Walls** (Part II, blueprint M7) is
  [pull request #2](https://github.com/taoxu2016/spark-visually/pull/2), branch `episode-7`: numbers (28 of 28
  checks), script **draft 3** (after two second-model reviews; both reports in its `review/` folder), and one Photon
  caption in S05 that the owner asked for, still to be run on Photon compute before voicing. **Next step: the owner's
  approval of draft 3**, then voice, scenes, cloud build, merge.
- Then Episode 8, **Send the small table** (M8), and on through the blueprint.

## How an episode is made (the loop that worked)

1. **Branch** `episode-N`. Folder `pK_…/eNN_title/` (Part II lives in `p2_why_jobs_are_slow/`).
2. **Numbers**: `eNN_numbers.py` runs every code-panel line and every on-screen card verbatim on Spark 4.3.0
   (local[8]), checks every claim, writes `eNN_numbers.json`. Exact things only (plans, jobs, stages, rows, bytes);
   no durations unless from a named run (a Databricks notebook, as Episode 2 has).
3. **Script**: `narration/build_script.py` writes `narration.json` and `review/eNN_script_for_review.md`; it traces
   every spoken digit to the JSON and asserts the key facts. Voice settings live in the builder (am_fenrir, 0.92):
   keep builder and JSON in step (Episode 5 once drifted).
4. **Review brief** `review/brief.json` → `python tools/review_prompt.py <episode folder>` builds the self-contained
   review prompt. The owner sends its raw GitHub URL to the second model, then pastes the report; save it verbatim as
   `review/eNN_review_report.md`, verify each runnable claim before changing anything, fix, comment on the PR.
5. **Owner approves the script** (in chat). Mark the builder's status approved; update the blueprint module (a Claude
   Doc, below) with the departures.
6. **Voice** (`make_voiceover.py`, cached per sentence), **scenes** (`episode/episodeNN.py`, Manim, cues are words of
   the clip's text), **frame review** (Model in Motion's `tools/review.py`), then commit and run **Actions → build
   episode** on the branch; it publishes the cut to the Review cuts release. Check frames of the published file.
7. **Owner approves the cut** → merge the PR.

## Rules the reviews taught (check before every review)

1. Show on screen every setting a picture relies on (adaptive execution, broadcast hints, pushdown).
2. Compare like with like; make every check test the claim it names.
3. Task counts include final tasks; stage counts say "ran" (skipped stages exist).
4. Say what holds for this small example as this example's; say rules only where they hold.
5. Say where Spark Connect differs (when errors appear, what `explain()` shows, where the plan lives).
6. Count walls in a plan asked for before a run: once a query has run, classic Spark's `explain()` prints the final plan
   above the starting plan, so each Exchange appears twice (over Connect it is always the starting plan).
7. Measure a reviewer's suggested wording as well: Episode 7's second review proposed a case where a deal up front
   "pays"; it saved a wall but sent every row instead of partial counts.
Also: Databricks differences are not captioned (owner's decision), with one exception, Episode 7's Photon caption.

## Owner preferences

- **Ask before anything that could cost money on GitHub.** The repo is public and uses only standard runners (free);
  builds attach no artifacts; cuts go to a release.
- The owner reviews scripts in the PR (`review/eNN_script_for_review.md`) and approves in chat. 720p review cuts.
- No local installs on the owner's laptop; Databricks has no GitHub integration (import notebooks by raw URL).

## Setting up a fresh sandbox

```sh
git clone --recurse-submodules https://github.com/taoxu2016/spark-visually.git && cd spark-visually
apt-get install -y openjdk-21-jre-headless ffmpeg libcairo2-dev libpango1.0-dev pkg-config   # if missing
pip install --break-system-packages -r requirements-numbers.txt -r requirements-build.txt
# fonts and Kokoro voice models: copy the two steps from .github/workflows/build-episode.yml
export MIM_REPO=$PWD/external/model-in-motion KOKORO_MODELS=$HOME/tts_models
```

Pushing needs a token from the owner (fine-grained, this repo only: Contents, Workflows, Pull requests, Issues,
Actions read and write). Keep it in a private file and give git a credential helper that reads it; never commit it.
The sandbox reaches github.com, api.github.com and release downloads, but not Actions artifact storage.

## Open items

- Episode 2's Databricks timing notebook (`e02_timings_databricks.py`) has not been run; its JSON would replace S04
  and S07's one-CPU timelines.
- The "crowd" (a wide simulated registry) does not exist; Episodes 5 to 7 use the cast ten thousand times over.
  Several later modules need it: decide its size and columns before Part III.
- No license chosen yet for the public repo.
- Episode 7's Photon caption: run `slow.explain()` on Photon compute and copy the exact shuffle line before voicing.
- For M17: `explain()` of a query on a cached DataFrame also prints the plan that built the cache, so the cache's deal
  shows as Exchange lines although the query builds no wall.
- Blueprint open decisions (in the doc): crowd sizes, covariate groups, Databricks timings for M12 and M19,
  pprof_spark loop check before M16, a reviewer for Part III's statistics.

## Links

- Blueprint (Claude Doc): https://claude.ai/artifact/6MkXggJ5Wixy1dqBJCA2RD
- Episode review docs (Claude Docs, Episodes 1 to 5; Episode 6 onward review in the PR): E1
  https://claude.ai/artifact/DeMfH82B1ETkVHmPe9HqPm · E2 https://claude.ai/artifact/3pqsmtDR89czETThQr4BvS ·
  E3 https://claude.ai/artifact/VD4VaFnW2vqpsDN1oUttMP · E4 https://claude.ai/artifact/PQbBEoj81pkhmPUs383zmn ·
  E5 https://claude.ai/artifact/Nc6sekvaUDEoeV8QPsAYgb
- Model in Motion (tools, pinned as `external/model-in-motion`): https://github.com/taoxu2016/model-in-motion

## Gotchas worth knowing

- The sandbox's `sh` has no brace expansion: make folders one argument at a time.
- Running the same DataFrame twice reuses its shuffle files (skipped stages): measure on fresh DataFrames.
- After rewording a clip, update any cue that waits on the old words (the render reports `cue not found`).
- Episode panels hold up to about 8 lines; put long code on cards over the benches, laid out on one backing card.
- Parquet bytes: the event log's bytes read are what the reader fetched (64 KiB buffered reads), not the column data.
