"""Builds Episode 3's draft narration.json and the review doc's tables from one source, and checks its numbers."""
import json
import re
from pathlib import Path

EP = Path(__file__).resolve().parents[1]                 # the episode folder
N = json.loads((EP / "e03_numbers.json").read_text())
T = lambda text, say=None: ({"text": text, "say": say} if say else {"text": text})

S = [
 ("S01_ColdOpen", None, [
  ("s1_1", "The code panel holds four lines; the shelf holds the cast scaled ten thousand times, about a million rows, as four files. Beside each line, a counter: rows read.",
   [T("You've seen it happen: a few lines of PySpark come back instantly, and then one line takes minutes."),
    T("Here are four lines, run on our cast scaled up to about a million rows.")]),
  ("s1_2", "The counters fill as each line runs: 0, 0, 0, then 1,030,000 for the fourth.",
   [T("The first three read nothing. Not one row."),
    T("The fourth reads every single row. So why do the first three read nothing?")]),
 ]),
 ("S02_Title", None, [
  ("s2_1", "Series wordmark, Spark, Visually; Part I, How Spark runs a job; the episode title.",
   [T("Welcome to Spark, Visually."), T("Today: nothing happens until you ask.")]),
 ]),
 ("S03_Recipe", "A recipe", [
  ("s3_1", "Back to the home shot and the cast's four files; the benches stay empty. Line 1 types; a box, filter, appears on the driver desk. Nothing moves.",
   [T("Back to our 103 patients. Line one filters them down to the higher-risk patients. Or rather, it writes that down.",
      "Back to our hundred and three patients. Line one filters them down to the higher-risk patients. Or rather, it writes that down."),
    T("A step is added to a plan: filter. Not one row moves.")]),
  ("s3_2", "Lines 2 and 3 type; two more boxes join the chain: new column, select. The rows-read counter stays at 0.",
   [T("Line two adds a column, and line three keeps just two columns. Each one adds a step."),
    T("Three lines, and not a single row has been read.")]),
  ("s3_3", "A card: a DataFrame is a recipe, its source and its steps.",
   [T("A DataFrame isn't a table of rows. It's a recipe: where the rows come from, and the steps to follow.")]),
 ]),
 ("S04_Count", "One pass", [
  ("s4_1", "Line 4 glows: hi.count(). Four tasks drop from the driver to the benches, one per file.",
   [T("Line four asks a question: how many rows are left? That's an action, and an action runs the recipe."),
    T("The driver sends 4 tasks down, one per file.", "The driver sends four tasks down, one per file.")]),
  ("s4_2", "Each bench reads its file; its chips pass a filter gate as they arrive, in one stream, and the chips without a risk ring drop away. The new-column and select boxes stay dark; only the x column leaves the shelf.",
   [T("Each task reads its file and filters the rows as they arrive, in a single pass."),
    T("And the new column? A count doesn't need it, so Spark never computes it. It even reads just the one column the filter needs.")]),
  ("s4_3", "46 chips remain; four partial counts go to one final task; 46 rises to the driver.",
   [T("46 higher-risk patients make it through, and one number goes up to the driver: 46.",
      "Forty-six higher-risk patients make it through, and one number goes up to the driver: forty-six.")]),
 ]),
 ("S05_Again", "Again", [
  ("s5_1", "The trays are empty again. Line 5 glows: hi.show(). One task reads the first file, then a second task the next, and it stops there: show() needs only 21 rows. This time the chips pass all three steps in one stream: the filter, the new column stamped on, the risk ring dropped by select. Rows print in the code panel.",
   [T("Line five asks to see the rows. Another action, so the recipe runs again, from the files."),
    T("This time it needs every step, and it still makes one pass: filter, add the column, keep two.")]),
  ("s5_2", "A card: normally, every action starts from storage; keeping a result is a promise for Episode 10.",
   [T("Spark didn't keep the count's work. Normally, every action starts again from storage, unless you ask Spark to keep a result."),
    T("And even that request waits for an action. That's episode 10.", "And even that request waits for an action. That's episode ten.")]),
 ]),
 ("S06_WhyWait", "Why wait?", [
  ("s6_1", "The whole chain on the driver desk lights at once.",
   [T("So why be lazy? Because when the action arrives, Spark sees the whole recipe at once, and can plan it."),
    T("Here's our cast again, written with its files sorted by risk.")]),
  ("s6_2", "A copy of the filter box slides down into the read; the filter stays on the desk. All four tasks run: the two files holding no higher-risk patients grey out and are skipped, and their tasks read nothing; a counter: rows read, 53 instead of 103.",
   [T("Spark pushes the filter into the read itself. Files that hold no higher-risk patients are skipped, and it reads 53 rows instead of 103.",
      "Spark pushes the filter into the read itself. Files that hold no higher-risk patients are skipped, and it reads fifty-three rows instead of a hundred and three."),
    T("Episode 6 shows how.", "Episode six shows how.")]),
 ]),
 ("S07_Kinds", "Two kinds of line", [
  ("s7_1", "A sorting card: transformations (filter, withColumn, select) hand back a DataFrame; actions (count, show, collect, write) hand back a number, rows, or files.",
   [T("So, two kinds of line. A transformation hands back a new DataFrame: a longer recipe, and no work."),
    T("An action hands back a result: a number, rows on screen or in your hands, or files written. Actions do the work.")]),
  ("s7_2", "A CSV file on the shelf: with inferSchema, two jobs run to look at the data; with a schema given, none. Beneath: reading Parquet or JSON by path runs 1 job, for the schema; over Spark Connect these jobs wait until the schema is first needed.",
   [T("A common exception: read a CSV file and ask Spark to guess its column types, and it runs 2 jobs, to look at the data.",
      "A common exception: read a CSV file and ask Spark to guess its column types, and it runs two jobs, to look at the data."),
    T("Give it the schema, and the read is lazy again.")]),
 ]),
 ("S08_Recap", "Recap", [
  ("s8_1", "Three recap cards: transformations write the recipe; actions run it, one pass per pile; normally, every action starts from storage.",
   [T("Let's gather it up."), T("Transformations write the recipe. Actions run it, in one pass over each pile."),
    T("And normally, every action starts again from storage.")]),
  ("s8_2", "The count's job and stage tally: 2 jobs, 2 stages. Next episode title: Cut at the walls.",
   [T("One loose end: our count was one action, yet Spark ran it as 2 jobs and 2 stages.",
      "One loose end: our count was one action, yet Spark ran it as two jobs and two stages."),
    T("How does Spark cut up the work? Next time: cut at the walls.")]),
  ("s8_3", "Series end card with the next episode title.",
   [T("This has been Spark, Visually. Thanks for watching the cluster with us.")]),
 ]),
]

# ---- every number in the narration must trace to the numbers JSON
allowed = {"103": "cast.patients; pushdown.rows_read_without", "4": "count.tasks (one per file)", "46": "count.result",
           "10": "episode 10 (M10, Keep it or rebuild it)", "53": "pushdown.rows_read_with", "6": "episode 6 (M6, Say what, not how)",
           "2": "csv.jobs_with_inference; count.jobs; count.stages"}
assert N["cast"]["patients"] == 103 == N["pushdown"]["rows_read_without"] and N["count"]["tasks"] == 4
assert N["count"]["result"] == 46 == N["cast"]["higher_risk"] and N["pushdown"]["rows_read_with"] == 53
assert N["csv"]["jobs_with_inference"] == 2 and N["csv"]["jobs_with_schema"] == 0 and N["count"]["jobs"] == 2 == N["count"]["stages"]
assert N["hook"]["rows_read_per_line"] == [0, 0, 0, 1_030_000] and N["cache_jobs"] == 0
assert N["plans"]["count_reads"] == "x:int" and "readmitted" not in N["plans"]["count"] and "readmitted" in N["plans"]["show"]
assert N["kinds"]["transformation"] == ["DataFrame"] * 3 and N["kinds"]["count"] == "int"
assert all(c["ok"] for c in N["checks"])
found = set()
for _, _, segs in S:
    for _, _, sents in segs:
        for s in sents:
            for num in re.findall(r"\d[\d,]*", s["text"]):
                num = num.rstrip(",")
                assert num in allowed, f"untraced number {num!r} in: {s['text']}"
                found.add(num)

narr = {
 "series": "Spark, Visually", "arc": "Part I · How Spark Runs a Job", "episode": 3,
 "title": "Nothing Happens Until You Ask", "subtitle": "transformations and actions",
 "name": "SparkVisually_P1_E03_NothingHappensUntilYouAsk",
 "status": "Script approved by the owner on 26 Nov 2026; revised after the second-model review (s3_1, s5_2, s6_2, s7_2 and s8_1, and the pictures in S05 and S06; see the review doc). Voiced with am_fenrir.",
 "voice": {"engine": "kokoro-onnx v1.0", "name": "am_fenrir", "speed": 0.92, "lang": "en-us",
           "pause_between_sentences": 0.32, "tail_pad": 0.15},
 "pronunciations": {},
 "scenes": [{"id": sid, "chapter": ch, "segments": [{"id": gid, "visual": vis, "sentences": sents} for gid, vis, sents in segs]}
            for sid, ch, segs in S],
}
(EP / "narration" / "narration.json").write_text(json.dumps(narr, indent=1, ensure_ascii=False) + "\n")
words = sum(len(s["text"].split()) for _, _, segs in S for _, _, sents in segs for s in sents)
clips = sum(len(segs) for _, _, segs in S)
print("scenes", len(S), "clips", clips, "words", words, f"(~{words / 160:.1f} min of speech)",
      "numbers traced", sorted(found, key=lambda x: int(x.replace(',', ''))))


def esc(x):
    return x.replace("|", "\\|")


md = {}
for sid, ch, segs in S:
    head = f"### {sid.split('_')[0]} · {ch or ('Cold open' if sid.endswith('ColdOpen') else 'Title')}"
    rows = ["| Clip | On screen | Narration |", "| --- | --- | --- |"]
    for gid, vis, sents in segs:
        rows.append(f"| {gid} | {esc(vis)} | {esc(' '.join(s['text'] for s in sents))} |")
    md[sid] = head + "\n\n" + "\n".join(rows)
Path("/tmp/e03_md.json").write_text(json.dumps(md))
