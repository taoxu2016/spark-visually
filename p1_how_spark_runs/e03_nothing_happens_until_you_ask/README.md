# Spark, Visually · Part I · Episode 3: Nothing Happens Until You Ask

**Status: script approved by the owner and voiced with am_fenrir; animation cut at 720p (3:14) and with the owner for
review (26 Nov 2026).**

**Revised 26 Nov 2026, after the second-model review:** s3_1 says "a step is added to a plan" (over Spark Connect the client holds the plan); s5_2 and s8_1 say every action "normally" starts again from storage; s6_2 says Spark "pushes" the filter into the read (a copy goes down; the filter stays); s7_2 calls CSV inference "a common exception", and the card adds that Parquet or JSON by path runs 1 job and that over Spark Connect the jobs wait until the schema is first needed (all measured). S05 now shows show() reading one file, then the next; S06 shows all four tasks under pushdown, two of them reading nothing. Only those five clips were re-voiced; 26 of 26 checks pass.

Blueprint module M3: a DataFrame is a recipe; transformations add steps and run nothing; an action runs the whole
recipe from storage, in one pass per pile; seeing the whole recipe first is what lets Spark plan it.

| File | What it is |
|---|---|
| `e03_numbers.py` | Runs the code panel's five lines verbatim on Apache Spark 4.3.0 (local[8]) and computes every number the episode says (22 checks): rows read per line of the hook, the plan after each line, the count's tasks, the columns it reads and what it never computes, show() re-reading storage as one fused pass, cache() running no job, filter pushdown on the cast written sorted by risk, what each kind of line hands back, CSV schema inference, and the count's jobs and stages. Writes `e03_numbers.json`. |
| `narration/narration.json` | The approved script in the series format: 8 scenes, 18 clips, 496 words, voiced with am_fenrir at 0.92. |
| `audio/` | The voiceover the scenes are cut to (2:56 of speech). It reads as 169 words a minute because the words are short (1.32 syllables each); in syllables a minute it is the series' usual pace. |
| `episode/episode03.py` | The animation: eight scenes cut to the voice. Transformations grow the plan on the driver desk while the benches stay empty; an action streams every row up from its file through the steps in one motion, so one pass looks like one pass (a chip drops at the filter, takes its stamp at the new column and loses its risk ring at the select, mid-flight); a count streams through the filter alone and reads only column x. |

    pip install pyspark==4.3.0      # needs Java 17 or 21
    python e03_numbers.py           # from this folder: prints the ledger, exits 1 if a check fails

What the run changed in the plan: the hook is told in rows read, not time (the count over a million rows took 0.35 s
on this one-CPU sandbox, because it reads only one column); a count never computes the new column, so the
one-pass-through-every-step beat belongs to show(); and pushdown skips files only when the files are sorted by risk.

The episode (720p review cut, captions burned in; `build/` is not committed):

    MIM_REPO=path/to/model-in-motion
    for S in S01_ColdOpen S02_Title S03_Recipe S04_Count S05_Again S06_WhyWait S07_Kinds S08_Recap; do
      manim -qm --disable_caching --media_dir build/media episode/episode03.py $S; done
    python $MIM_REPO/tools/assemble.py . --quality 720p30

Still to do: the owner's review, then a watch-through with sound.
