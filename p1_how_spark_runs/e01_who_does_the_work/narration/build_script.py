"""Builds Episode 1's draft narration.json and the review doc's tables from one source, and checks its numbers."""
import json
import re
from pathlib import Path

EP = Path(__file__).resolve().parents[1]                 # the episode folder
N = json.loads((EP / "e01_numbers.json").read_text())
T = lambda text, say=None: ({"text": text, "say": say} if say else {"text": text})
SC = N["bytes_to_driver"]["scaled"]

S = [
 ("S01_ColdOpen", None, [
  ("s1_1", "A laptop card holds the cast: 103 chips in the clinic hues, its memory bar barely touched.",
   [T("Here are our 103 patients. They fit on one laptop, with plenty of room to spare.",
      "Here are our hundred and three patients. They fit on one laptop, with plenty of room to spare."),
    T("Now picture a national registry: every patient, every visit, for years.")]),
  ("s1_2", "The registry pours in: the laptop's memory bar floods red and chips spill over its edge.",
   [T("The laptop's memory fills, and then it overflows. One machine isn't enough."),
    T("So we spread the work across many machines. But then, which machine does what?")]),
 ]),
 ("S02_Title", None, [
  ("s2_1", "Series wordmark, Spark, Visually; Part I, How Spark runs a job; the episode title.",
   [T("Welcome to Spark, Visually."), T("Today: who does the work.")]),
 ]),
 ("S03_Parts", "The parts", [
  ("s3_1", "The home shot builds piece by piece. First the storage shelf along the bottom, with the cast written on it as four files.",
   [T("Let's build the cluster, piece by piece. At the bottom: storage. Our cast is written there as 4 files.",
      "Let's build the cluster, piece by piece. At the bottom: storage. Our cast is written there as four files.")]),
  ("s3_2", "Four executor benches rise above the shelf, each with two core slots. Labels name the parts once: worker (the machine), executor (the process on it), core (a slot that runs one task).",
   [T("Above it, four machines, called workers. On each one runs a process called an executor."),
    T("Each executor has cores, and a core runs one task at a time. 4 executors with 2 cores each: 8 tasks can run at once.",
      "Each executor has cores, and a core runs one task at a time. Four executors with two cores each: eight tasks can run at once.")]),
  ("s3_3", "The network band joins the benches, and the driver desk appears above it and lights up. A small cluster-manager card starts the four executors when the driver asks.",
   [T("And above them all, the driver. It holds the Spark session, plans the work and hands it out."),
    T("The executors were started by the cluster manager, when the driver asked for them.")]),
 ]),
 ("S04_Notebook", "Your notebook", [
  ("s4_1", "The code panel slides in on the left, wired to the driver desk.",
   [T("On a classic cluster, your notebook's Python runs on the driver machine itself.")]),
  ("s4_2", "Variant: the code panel detaches into a small client card, joined to the driver by a thin line. A plan travels down the line; one result travels back.",
   [T("On some Databricks compute, it doesn't. With Spark Connect, the notebook is a small client: it sends each query to a driver on the cluster, and gets back only the result.")]),
  ("s4_3", "Both set-ups run the same code panel. A small card lists what Spark Connect leaves out: df.rdd and the SparkContext.",
   [T("Everything in this series runs both ways. What doesn't run on Spark Connect is the old, low-level route through the SparkContext, like df.rdd. So we won't use it.",
      "Everything in this series runs both ways. What doesn't run on Spark Connect is the old, low-level route through the Spark context, like D F dot R D D. So we won't use it.")]),
 ]),
 ("S05_Read", "Nothing moves", [
  ("s5_1", "Code panel, line 1 glows: df = spark.read.table(\"cast\"). Then it dims. Nothing moves anywhere on the home shot.",
   [T("Now, some code. Line one reads our table."),
    T("It returns at once, and nothing moves. No machine touches a single row. We'll see why in episode 3.",
      "It returns at once, and nothing moves. No machine touches a single row. We'll see why in episode three.")]),
 ]),
 ("S06_Count", "Counting", [
  ("s6_1", "Line 2 glows: df.count(). The driver turns the plan into four tasks, one per file, and sends them down to the benches.",
   [T("Line two counts the rows."),
    T("The driver turns that into 4 tasks, one for each file, and sends them down to the executors.",
      "The driver turns that into four tasks, one for each file, and sends them down to the executors.")]),
  ("s6_2", "Each bench pulls its own file up from the shelf, and its chips flow up into its tray. Nothing passes through the driver.",
   [T("Each executor reads its own file, straight from storage. The data never passes through the driver.")]),
  ("s6_3", "Each tray shows its count: 28, 25, 25, 25. The four numbers travel to one final task on one bench, which adds them; a single number, 103, rises to the driver.",
   [T("Each task counts its own rows: 28, 25, 25 and 25.",
      "Each task counts its own rows: twenty-eight, twenty-five, twenty-five and twenty-five."),
    T("The 4 partial counts go to one final task, which adds them up. And just one number goes up to the driver: 103.",
      "The four partial counts go to one final task, which adds them up. And just one number goes up to the driver: a hundred and three.")]),
 ]),
 ("S07_Collect", "The wrong way", [
  ("s7_1", "Line 3 glows: rows = df.collect(). Every chip flies up from the trays into the driver, and its memory gauge rises.",
   [T("Line three asks for every row: collect."),
    T("Now all 103 patients travel up to the driver.", "Now all one hundred and three patients travel up to the driver.")]),
  ("s7_2", "A scale card: the cast ten thousand times over, 1,030,000 rows. Count sends up one number; collect fills the driver's gauge: 533 MB of Python objects.",
   [T("For our cast, that's nothing. Now make it 10,000 times bigger: about a million rows.",
      "For our cast, that's nothing. Now make it ten thousand times bigger: about a million rows."),
    T("Count still sends up one number. Collect lands every row on the driver: over 500 megabytes of memory.",
      "Count still sends up one number. Collect lands every row on the driver: over five hundred megabytes of memory.")]),
  ("s7_3", "The registry again: the driver's gauge floods red. Then the shot rewinds: the chips drop back to their trays.",
   [T("For a whole registry, that would swamp the driver. Rows should stay down, with the executors.")]),
 ]),
 ("S08_Python", "Where Python runs", [
  ("s8_1", "The code panel's Python feeds the plan on the driver desk; the benches wear a small JVM badge.",
   [T("One more surprise. The Python you write mostly just builds a plan."),
    T("The real work, reading, counting, adding, runs inside the executors, in Java, not Python.")]),
  ("s8_2", "Lines 4 and 5 glow: a Python function, flip, summed in a select. Beside each bench a small Python worker appears, and batches of chips shuttle out to it and back. The plan shows ArrowEvalPython.",
   [T("Unless you write your own Python function, a UDF. Then each executor starts Python workers beside it, and ships the rows out to them in batches, and back.",
      "Unless you write your own Python function, a U D F. Then each executor starts Python workers beside it, and ships the rows out to them in batches, and back."),
    T("That takes time, and Spark can't see inside your function.")]),
 ]),
 ("S09_Recap", "Recap", [
  ("s9_1", "Three recap cards: the driver plans; the executors work; rows stay down.",
   [T("Let's gather it up."), T("The driver plans the work and hands out tasks."),
    T("The executors read the data, and do the work."), T("And rows stay down, unless you ask for them.")]),
  ("s9_2", "The home shot during the count: four of the eight core slots lit, four dark. Next episode title: Piles of records.",
   [T("One loose end: our count read the table with 4 tasks, but we have 8 cores.",
      "One loose end: our count read the table with four tasks, but we have eight cores."),
    T("Why only four? Next time: piles of records.")]),
  ("s9_3", "Series end card with the next episode title.",
   [T("This has been Spark, Visually. Thanks for watching the cluster with us.")]),
 ]),
]

# ---- every number in the narration must trace to the numbers JSON (or the home shot it is drawn on)
allowed = {
 "103": "cast.patients; count.result; collect.rows", "4": "cast.files; count.tasks; partials; executors (home shot)",
 "2": "cores per executor (home shot; 4 x 2 = slots)", "8": "slots", "28": "count.rows_per_task[0]",
 "25": "count.rows_per_task[1:]", "3": "episode 3 (M3, Nothing happens until you ask)", "10,000": "bytes_to_driver.scaled.factor",
 "500": "bytes_to_driver.scaled.collect_python_megabytes (533), said as 'over 500'",
}
assert N["cast"]["patients"] == 103 == N["count"]["result"] == N["collect"]["rows"]
assert N["cast"]["files"] == 4 == N["count"]["tasks"] == N["count"]["partials_written"] and N["count"]["final_tasks"] == 1
assert N["slots"] == 8 == 4 * 2 and N["count"]["rows_per_task"] == [28, 25, 25, 25]
assert SC["factor"] == 10_000 and SC["rows"] == 1_030_000 and 500 < SC["collect_python_megabytes"] < 600
assert N["udf"]["plan_node"] == "ArrowEvalPython" and N["connect"]["rdd"].startswith("PySparkNotImplementedError")
found = set()
for _, _, segs in S:
    for _, _, sents in segs:
        for s in sents:
            for num in re.findall(r"\d[\d,]*", s["text"]):
                num = num.rstrip(",")
                assert num in allowed, f"untraced number {num!r} in: {s['text']}"
                found.add(num)

narr = {
 "series": "Spark, Visually", "arc": "Part I · How Spark Runs a Job", "episode": 1,
 "title": "Who Does the Work", "subtitle": "driver, executors, cores",
 "name": "SparkVisually_P1_E01_WhoDoesTheWork",
 "status": "Script approved by the owner on 26 Nov 2026; revised after the second-model review (line 5 now sums the UDF, which a count would prune; s9_2 says the count read the table with 4 tasks). Voiced with am_fenrir.",
 "voice": {"engine": "kokoro-onnx v1.0", "name": "am_fenrir", "speed": 0.92, "lang": "en-us",
           "pause_between_sentences": 0.32, "tail_pad": 0.15},
 "pronunciations": {},
 "scenes": [{"id": sid, "chapter": ch, "segments": [{"id": gid, "visual": vis, "sentences": sents} for gid, vis, sents in segs]}
            for sid, ch, segs in S],
}
(EP / "narration").mkdir(exist_ok=True)
(EP / "narration" / "narration.json").write_text(json.dumps(narr, indent=1, ensure_ascii=False) + "\n")

words = sum(len(s["text"].split()) for _, _, segs in S for _, _, sents in segs for s in sents)
clips = sum(len(segs) for _, _, segs in S)
print("scenes", len(S), "clips", clips, "words", words, f"(~{words / 161:.1f} min of speech at 161 wpm)",
      "numbers traced", sorted(found, key=lambda x: int(x.replace(',', ''))))


# ---- markdown tables for the review doc, one table per scene
def esc(x):
    return x.replace("|", "\\|")


md = {}
for sid, ch, segs in S:
    head = f"### {sid.split('_')[0]} · {ch or ('Cold open' if sid.endswith('ColdOpen') else 'Title')}"
    rows = ["| Clip | On screen | Narration |", "| --- | --- | --- |"]
    for gid, vis, sents in segs:
        rows.append(f"| {gid} | {esc(vis)} | {esc(' '.join(s['text'] for s in sents))} |")
    md[sid] = head + "\n\n" + "\n".join(rows)
Path("/tmp/e01_md.json").write_text(json.dumps(md))
