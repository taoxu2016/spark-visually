"""Builds Episode 6's narration.json and its script-for-review table from one source, and checks its numbers."""
import json
import re
from pathlib import Path

EP = Path(__file__).resolve().parents[1]                 # the episode folder
N = json.loads((EP / "e06_numbers.json").read_text())
T = lambda text, say=None: ({"text": text, "say": say} if say else {"text": text})

S = [
 ("S01_ColdOpen", None, [
  ("s1_1", "The code panel types one question two ways: the DataFrame line, then the same question in SQL.",
   [T("Here's one question, written two ways: as DataFrame code, and as SQL. Higher-risk patients, counted by clinic.")]),
  ("s1_2", "Line 4 glows: both formatted plans appear side by side, node by node; every node matches, and the pairs light together.",
   [T("Which one runs faster? Neither. Spark turns both into the same plan, step for step.")]),
 ]),
 ("S02_Title", None, [
  ("s2_1", "Series wordmark, Spark, Visually; Part I, How Spark runs a job; the episode title.",
   [T("Welcome to Spark, Visually."), T("Today: say what, not how.")]),
 ]),
 ("S03_Analyse", "Analysis", [
  ("s3_1", "Line 5 glows: q1.explain(True) prints all four phases. The SQL's first plan on the driver desk: plain names, the table unresolved. Analysis looks each one up in the catalog, and each column gains its type.",
   [T("First, Spark reads what you wrote and looks up every name: the table, and each column, with its type."),
    T("In classic PySpark, that happens line by line, as you write.")]),
  ("s3_2", "Line 6 types: df.filter(F.col(\"xx\") == 1). It fails at once with an unresolved column; no job ran. A caption: over Spark Connect, the same DataFrame line passes, and fails when its plan is first needed (at .count(), say); a SQL string still fails at once.",
   [T("So a misspelt column fails right there, on its own line, before a single task runs."),
    T("Over Spark Connect, DataFrame code waits until its plan is first needed, but a SQL string still fails at once.")]),
 ]),
 ("S04_Optimise", "The optimiser", [
  ("s4_1", "The analysed plan on the desk: count per clinic, filter, read. A new step appears beside the filter, isnotnull(x), and a projection of clinic slides in above it.",
   [T("Then the optimiser rewrites the plan. It adds a check you never wrote: that x isn't null. And it keeps only what the query needs: clinic, for the count.")]),
  ("s4_2", "A card runs df.groupBy(\"clinic\").count().filter(F.col(\"clinic\") == \"Birch\"): in the plan, the filter written last slides below the count.",
   [T("It reorders what's safe to reorder, too. Write a filter on clinic after the count, and the optimiser moves it before: fewer rows to count."),
    T("You said what you wanted. Spark chose the steps.")]),
 ]),
 ("S05_Physical", "The physical plan", [
  ("s5_1", "The physical plan on the desk: the read is handed a copy of the filter and reads 2 of the 4 columns (patient and y fade from it); the filter stays above the read; the single count splits into a partial count, an Exchange and a final count, with Episode 4's wall between.",
   [T("Last, Spark picks real operators. It hands the read a copy of the filter, so the read can skip what can't match, and has it read only 2 of the 4 columns. The filter still runs after the read, to be sure.",
      "Last, Spark picks real operators. It hands the read a copy of the filter, so the read can skip what can't match, and has it read only two of the four columns. The filter still runs after the read, to be sure."),
    T("And the one count becomes two: a partial count on each pile, an exchange, and a final count. That's Episode 4's wall.",
      "And the one count becomes two: a partial count on each pile, an exchange, and a final count. That's Episode four's wall.")]),
  ("s5_2", "Line 4 glows again: q1.explain(\"formatted\"). Its nodes light from the bottom: Scan parquet with its PushedFilters and ReadSchema, Filter, Project, HashAggregate, Exchange, HashAggregate, AdaptiveSparkPlan.",
   [T("Explain, formatted, shows the physical plan, node by node: the scan, with the filters it was handed and the two columns it reads; then the filter, the partial count, the exchange, and the final count."),
    T("Before a run, it's the starting plan: adaptive execution can still change it. That's episode 12.",
      "Before a run, it's the starting plan: adaptive execution can still change it. That's episode twelve.")]),
 ]),
 ("S06_Storage", "Down to storage", [
  ("s6_1", "The cast scaled up ten thousand times, sorted by risk, written as one Parquet file of 25 row groups; each group shows the smallest and largest x it holds.",
   [T("Here's why handing the filter down pays. Take the cast ten thousand times over, sorted by risk, in one file of 25 row groups.",
      "Here's why handing the filter down pays. Take the cast ten thousand times over, sorted by risk, in one file of twenty-five row groups."),
    T("Each group records the smallest and largest x it holds.")]),
  ("s6_2", "With pushdown, the 13 groups whose largest x is 0 grey out, and 12 are read. Only clinic and x are needed: about 2.6 kB of column data, against 1.9 MB for every column of those groups, labelled: column sizes, from the file's metadata. A settings card turns pushdown off, and all 25 groups are read.",
   [T("With the filter handed down, the reader skips the 13 groups that can't hold a higher-risk patient. Of the 12 it reads, it needs just two columns: about 2.6 kilobytes of column data, where all four would be nearly 2 megabytes.",
      "With the filter handed down, the reader skips the thirteen groups that can't hold a higher-risk patient. Of the twelve it reads, it needs just two columns: about two point six kilobytes of column data, where all four would be nearly two megabytes."),
    T("Those sizes belong to this sorted file. Unsorted, every group holds both kinds, and nothing is skipped."),
    T("Turn pushdown off, and every group is read.")]),
 ]),
 ("S07_UDF", "A black box", [
  ("s7_1", "Lines 7 and 8 type: the same filter as a Python function. The read's pushed filters are now empty; a Python worker appears beside each bench. The rows stay on the bench: only x goes out to the worker, and a tick comes back.",
   [T("Now write the same filter as a Python function. To Spark, it's a black box."),
    T("It can't hand the filter to the read, so every row is read, and for every row the value it checks, x, travels to Python and back, just to be checked.")]),
  ("s7_2", "On the scaled file: all 25 row groups read, against 12.",
   [T("On our sorted file, that's all 25 row groups instead of 12. Keep your logic where the optimiser can see it.",
      "On our sorted file, that's all twenty-five row groups instead of twelve. Keep your logic where the optimiser can see it.")]),
 ]),
 ("S08_Recap", "Recap", [
  ("s8_1", "Three recap cards: one plan for DataFrame code and SQL; the optimiser reads only what it needs; a Python function hides its logic.",
   [T("Let's gather it up."),
    T("Say what you want, in DataFrame code or in SQL: it's the same plan. Spark checks the names, rewrites the plan, reads only what it needs, and picks the operators."),
    T("A Python function hides its logic, and the optimiser can't help.")]),
  ("s8_2", "The plan on the desk, with a price tag beside it: blank. Next: Part II, Counting walls.",
   [T("The plan is chosen. But is it cheap? Next time, Part Two begins: counting walls.")]),
  ("s8_3", "Series end card with the next episode title.",
   [T("This has been Spark, Visually. Thanks for watching the cluster with us.")]),
 ]),
]

# ---- every number in the narration must trace to the numbers JSON
St, B = N["storage"], N["storage"]["column_bytes_needed"]
groups, may = St["row_groups"], St["row_groups_that_may_hold_x1"]
allowed = {"2": "columns read (read_schema); nearly 2 megabytes (column_bytes_needed.all_columns, 1,868,697 bytes)",
           "4": "the table's columns; Episode 4", "25": "storage.row_groups",
           "13": "row groups skipped (row_groups - row_groups_that_may_hold_x1)", "12": "row groups read; episode 12",
           "2.6": "column_bytes_needed.query (2,590 bytes), said as about 2.6 kilobytes of column data"}
assert N["read_schema"] == "struct<clinic:string,x:int>" and N["cast"]["columns"] == 4
assert groups == 25 and may == 12 and groups - may == 13 and St["scale"] == 10_000
assert 2550 <= B["query"] < 2650 and 1_800_000 < B["all_columns"] < 2_000_000
assert N["misspelt"]["unresolved_column"] and N["connect"]["dataframe_at_its_line"] is None
assert N["connect"]["sql_at_its_call"].endswith("UNRESOLVED_COLUMN") and N["reorder"]["rows"] == {"Birch": 30}
assert N["udf"]["pushed_filters"] == "" and N["storage"]["reads"]["udf"]["rows"] == St["rows"]
assert all(c["ok"] for c in N["checks"])
found = set()
for _, _, segs in S:
    for _, _, sents in segs:
        for s in sents:
            for num in re.findall(r"\d[\d,.]*\d|\d", s["text"]):
                assert num in allowed, f"untraced number {num!r} in: {s['text']}"
                found.add(num)

narr = {
 "series": "Spark, Visually", "arc": "Part I · How Spark Runs a Job", "episode": 6,
 "title": "Say What, Not How", "subtitle": "from your code to Spark's plan",
 "name": "SparkVisually_P1_E06_SayWhatNotHow",
 "status": "Draft 2, revised after the second-model review (see review/e06_review_report.md): for the owner's review. Not voiced.",
 "voice": {"engine": "kokoro-onnx v1.0", "name": "am_fenrir", "speed": 0.92, "lang": "en-us",
           "pause_between_sentences": 0.32, "tail_pad": 0.15},
 "pronunciations": {},
 "scenes": [{"id": sid, "chapter": ch, "segments": [{"id": gid, "visual": vis, "sentences": sents} for gid, vis, sents in segs]}
            for sid, ch, segs in S],
}
(EP / "narration" / "narration.json").write_text(json.dumps(narr, indent=1, ensure_ascii=False) + "\n")
words = sum(len(s["text"].split()) for _, _, segs in S for _, _, sents in segs for s in sents)
clips = sum(len(segs) for _, _, segs in S)
print("scenes", len(S), "clips", clips, "words", words, f"(~{words / 165:.1f} min of speech)", "numbers traced", sorted(found))

# ---- the script for review, as one markdown file beside the review prompt
lines = [f"# Episode {narr['episode']}: {narr['title']} · script for review", "", narr["status"], "",
         f"{len(S)} scenes, {clips} clips, {words} words. Every number comes from `e06_numbers.json`.", ""]
for sid, ch, segs in S:
    lines += [f"## {sid.split('_')[0]} · {ch or ('Cold open' if sid.endswith('ColdOpen') else 'Title')}", "",
              "| Clip | On screen | Narration |", "| --- | --- | --- |"]
    lines += [f"| {gid} | {vis.replace('|', '/')} | {' '.join(s['text'] for s in sents)} |" for gid, vis, sents in segs]
    lines.append("")
(EP / "review" / "e06_script_for_review.md").write_text("\n".join(lines))
