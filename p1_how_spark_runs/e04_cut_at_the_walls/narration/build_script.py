"""Builds Episode 4's draft narration.json and the review doc's tables from one source, and checks its numbers."""
import json
import re
from pathlib import Path

EP = Path(__file__).resolve().parents[1]                 # the episode folder
N = json.loads((EP / "e04_numbers.json").read_text())
T = lambda text, say=None: ({"text": text, "say": say} if say else {"text": text})

S = [
 ("S01_ColdOpen", None, [
  ("s1_1", "The code panel holds Episode 3's filter and a new line, a count per clinic. Beside the driver, last episode's tally: 1 action, 2 jobs, 2 stages.",
   [T("Last time, a single count ran as 2 jobs and 2 stages.", "Last time, a single count ran as two jobs and two stages."),
    T("Here's a new question for our cast: how many higher-risk patients does each clinic have?")]),
  ("s1_2", "Line 3 glows: collect. A schematic of Spark's UI fills in from this run: 2 jobs, 2 stages and 5 tasks that ran, with one more stage beside them, greyed: skipped.",
   [T("One action answers it. Spark ran it as 2 jobs, 2 stages and 5 tasks.",
      "One action answers it. Spark ran it as two jobs, two stages and five tasks."),
    T("Where do those numbers come from?")]),
 ]),
 ("S02_Title", None, [
  ("s2_1", "Series wordmark, Spark, Visually; Part I, How Spark runs a job; the episode title.",
   [T("Welcome to Spark, Visually."), T("Today: cut at the walls.")]),
 ]),
 ("S03_Plan", "The plan", [
  ("s3_1", "Lines 1 and 2 type. The plan on the driver desk reads: read, filter; then one more box: count per clinic. The benches stay empty.",
   [T("Line one is last episode's filter. Line two asks for a count per clinic, and the driver adds it to the plan."),
    T("Nothing has run yet. But this step is different.")]),
  ("s3_2", "The cast's four piles; Alder's higher-risk patients glow in the three piles that hold them. The count box splits into the plan Spark will run: partial count, exchange, count; the exchange lights.",
   [T("A clinic's patients sit in different piles. So when it runs, Spark counts in two steps."),
    T("First, each pile counts its own patients, clinic by clinic. Then those partial counts must meet, so each clinic's numbers move to one place. That step is an exchange.")]),
 ]),
 ("S04_Cut", "Cut at the walls", [
  ("s4_1", "A cut through the plan at the exchange; two tinted bands: stage 1 (read, filter, partial count) and stage 2 (count).",
   [T("Spark cuts the plan at every exchange, and each piece is a stage."),
    T("Stage one reads, filters and counts each pile. Stage two finishes the counts.")]),
  ("s4_2", "A wall drops across the executors' lanes, between stage 1 and stage 2.",
   [T("Between the two stands a wall: nothing beyond it starts until everything before it has finished."),
    T("Stage two can't start early, because it needs a piece from every pile.")]),
  ("s4_3", "Stage 1's band gains four task tiles, one per pile.",
   [T("Inside a stage, the work is split one task per pile. Four piles, so stage one runs 4 tasks.",
      "Inside a stage, the work is split one task per pile. Four piles, so stage one runs four tasks.")]),
 ]),
 ("S05_Run", "Running the job", [
  ("s5_1", "Line 3 glows: per_clinic.collect(). Four tasks drop. Each tray's chips stream through the filter and settle as small partial counts, clinic by clinic. As each task finishes, in this run's order, its core goes dark; the wall holds stage two.",
   [T("Line three is the action. Four tasks go down and run side by side."),
    T("Each one finishes when its own pile is done, and stage two can't begin until the last one does.")]),
  ("s5_2", "The last tick; the wall opens. 14 partial counts cross to one task; the final counts appear: Alder 6, Birch 24, Cedar 10, Dune 6. Four rows rise to the driver.",
   [T("When the last one finishes, the wall opens. The partial counts cross, and stage two adds them up."),
    T("Alder has 6 higher-risk patients, Birch 24, Cedar 10 and Dune 6. Elm has none, so it gets no row at all.",
      "Alder has six higher-risk patients, Birch twenty-four, Cedar ten and Dune six. Elm has none, so it gets no row at all.")]),
  ("s5_3", "Stage 2's 200 piles, almost all empty, merge into one; one task. The tally reads 4 tasks, then 1.",
   [T("And stage two's tasks? The exchange deals into 200 piles by default. These are tiny, so Spark merges them into one: one pile, one task.",
      "And stage two's tasks? The exchange deals into two hundred piles by default. These are tiny, so Spark merges them into one: one pile, one task."),
    T("Four tasks, then one: those are our 5.", "Four tasks, then one: those are our five.")]),
 ]),
 ("S06_Explain", "Reading the plan", [
  ("s6_1", "Line 4 types: per_clinic.explain(). The printed plan appears, cropped to the final plan, the one that ran; read from the bottom up: scan, filter, partial count; then the Exchange line lights, with its 200.",
   [T("Line four prints the plan Spark ran. Read it from the bottom up: the scan, the filter, the partial count."),
    T("Then a line that starts with Exchange. That's the wall, and the 200 piles it deals into.",
      "Then a line that starts with Exchange. That's the wall, and the two hundred piles it deals into.")]),
  ("s6_2", "The *(1) and *(2) markers light, then the query stages 0 and 1; beside them, the UI's stage numbers, captioned as this run's: 2 and 4.",
   [T("Beside the steps sit star-one and star-two. They aren't stage numbers: they number the chains of steps Spark compiles into one function."),
    T("Adaptive execution counts its query stages, 0 and 1, its own way too.",
      "Adaptive execution counts its query stages, zero and one, its own way too."),
    T("Spark's UI numbered our two stages 2 and 4.", "Spark's UI numbered our two stages two and four.")]),
  ("s6_3", "A caption under the plan: on Spark Connect, explain() shows the starting plan, even after the run.",
   [T("One catch: on Spark Connect, which some Databricks compute uses, explain() shows the starting plan, even after the run."),
    T("To see the plan that ran, use the Spark UI, or the query profile.")]),
 ]),
 ("S07_UI", "Reading the UI", [
  ("s7_1", "A schematic Jobs page from this run: job 2, one stage, 4 tasks; job 3, one stage plus one skipped, 1 task.",
   [T("Now, why two jobs? With adaptive execution on, as it is by default, Spark runs stage one as a job of its own."),
    T("It looks at how much stage one actually wrote, and only then plans the rest. That's episode 12.",
      "It looks at how much stage one actually wrote, and only then plans the rest. That's episode twelve.")]),
  ("s7_2", "The second job's skipped stage greys out, with its own number: 3.",
   [T("The second job lists that first stage again, marked skipped. Its work was done, and its output was still there to reuse."),
    T("The skipped copy even took a number of its own: 3.", "The skipped copy even took a number of its own: three.")]),
  ("s7_3", "A settings card runs its two lines: adaptive execution off, and the same count again. The UI schematic: 1 job, the same two stages, 4 tasks, then 200.",
   [T("Switch adaptive execution off and count again, and you get 1 job with the same two stages. But now stage two runs 200 tasks, one per pile, and most of those piles are empty.",
      "Switch adaptive execution off and count again, and you get one job with the same two stages. But now stage two runs two hundred tasks, one per pile, and most of those piles are empty.")]),
 ]),
 ("S08_Join", "Not always in line", [
  ("s8_1", "Line 5 types: df.join(clinics.hint(\"merge\"), \"clinic\").collect(). The plan: two reads, each with its own exchange, meeting at a join; three stages.",
   [T("One last line: join the cast to a small table of clinics."),
    T("Spark would normally take a shortcut with a table this small, and the hint asks it not to. That's episode 8.",
      "Spark would normally take a shortcut with a table this small, and the hint asks it not to. That's episode eight."),
    T("So both sides are dealt out by clinic: each side gets its own stage, and a third stage joins them.")]),
  ("s8_2", "A timeline from this run: the two input stages' bars overlap; a wall; then the join stage.",
   [T("Neither input needs the other, so there's no wall between them, and they run at the same time."),
    T("One line of code, three stages: a stage isn't a line of code.")]),
 ]),
 ("S09_Recap", "Recap", [
  ("s9_1", "Three recap cards: an action starts one job or several; the plan is cut into stages at every exchange; one task per pile, and a wall wherever one stage needs another's output.",
   [T("Let's gather it up."),
    T("An action starts one job or several, and adaptive execution is one reason for several. Spark cuts its plan at every exchange, and each piece is a stage."),
    T("A stage runs one task per pile, and a wall stands wherever one stage needs another's output.")]),
  ("s9_2", "The wall alone across the lanes; the next episode's title: Everyone sends to everyone.",
   [T("But what exactly crosses the wall? Next time: everyone sends to everyone.")]),
  ("s9_3", "Series end card with the next episode title.",
   [T("This has been Spark, Visually. Thanks for watching the cluster with us.")]),
 ]),
]

# ---- every number in the narration must trace to the numbers JSON
M, C = N["main"], N["counts"]
allowed = {"2": "main.jobs; main.stages_run; stage id main.stage_ids[0]", "5": "main.total_tasks (4 + 1)",
           "4": "main.tasks[0] (one per pile); stage id main.stage_ids[1]", "6": "counts.Alder; counts.Dune", "24": "counts.Birch",
           "10": "counts.Cedar", "200": "exchanges: hashpartitioning(clinic, 200); aqe_off.tasks[1]",
           "1": "aqe_off.jobs; query_stage_ids[1]", "12": "episode 12 (M12, A plan that changes its mind)",
           "8": "episode 8 (M8, Send the small table)", "0": "query_stage_ids[0]", "3": "main.skipped_ids[0] (the skipped copy's number)"}
assert M["jobs"] == 2 == M["stages_run"] and M["total_tasks"] == 5 and M["tasks"] == [4, 1] and M["stage_ids"] == [2, 4]
assert M["skipped_per_job"] == [0, 1] and M["stage2_after_last_stage1_ms"] >= 0 and M["partial_counts_crossing"] == 14
assert C == {"Alder": 6, "Birch": 24, "Cedar": 10, "Dune": 6} and "Elm" not in C
assert N["exchanges"] == ["hashpartitioning(clinic#5, 200)"] and N["codegen_ids"] == [1, 2]
assert {k: N["aqe_off"][k] for k in ("jobs", "stages_run", "tasks", "total_tasks")} == {"jobs": 1, "stages_run": 2, "tasks": [4, 200], "total_tasks": 204}
assert N["join"]["stages_run"] == 3 and N["join"]["inputs_overlap"] and N["again"] == {"stages_run": 1, "stages_skipped": 1}
assert N["join"]["default_strategy"].startswith("BroadcastHashJoin") and N["code_on_screen"][4].startswith('df.join(clinics.hint("merge")')
assert N["query_stage_ids"] == [0, 1] and M["skipped_ids"] == [3] and N["connect"]["explain_after_run_is_final"] is False
assert N["counted_piles_per_clinic"]["Alder"] == 3 and N["card_aqe_off"][0].startswith('spark.conf.set("spark.sql.adaptive.enabled"')
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
 "series": "Spark, Visually", "arc": "Part I · How Spark Runs a Job", "episode": 4,
 "title": "Cut at the Walls", "subtitle": "jobs, stages and tasks",
 "name": "SparkVisually_P1_E04_CutAtTheWalls",
 "status": "Script approved by the owner on 26 Nov 2026 (draft 2, revised after the second-model review). Voiced with am_fenrir.",
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
Path("/tmp/e04_md.json").write_text(json.dumps(md))
