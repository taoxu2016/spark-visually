"""Builds Episode 2's draft narration.json and the review doc's tables from one source, and checks its numbers."""
import json
import re
from pathlib import Path

EP = Path(__file__).resolve().parents[1]                 # the episode folder
N = json.loads((EP / "e02_numbers.json").read_text())
T = lambda text, say=None: ({"text": text, "say": say} if say else {"text": text})
TL = "Bars drawn from this run's real task times on one CPU, labelled so on screen (tasks running together share it); an eight-core Databricks run (e02_timings.json) will replace them."

S = [
 ("S01_ColdOpen", None, [
  ("s1_1", "The home shot. The cast lies on the storage shelf as a single file. A count runs: one task drops into one core slot on the first bench; the other seven slots stay dark.",
   [T("Last time, our count read the table with 4 tasks on 8 cores. Here's a worse case: the same 103 patients, written as one file.",
      "Last time, our count read the table with four tasks on eight cores. Here's a worse case: the same hundred and three patients, written as one file."),
    T("Spark reads it with 1 task. One core works, and 7 sit idle.", "Spark reads it with one task. One core works, and seven sit idle.")]),
  ("s1_2", "The single tray glows; the seven idle slots pulse once.",
   [T("Nothing is wrong with the data. It's how the rows are piled up."),
    T("Spark works pile by pile, and one file made one pile.")]),
 ]),
 ("S02_Title", None, [
  ("s2_1", "Series wordmark, Spark, Visually; Part I, How Spark runs a job; the episode title.",
   [T("Welcome to Spark, Visually."), T("Today: piles of records.")]),
 ]),
 ("S03_Piles", "One pile, one task", [
  ("s3_1", "A tray lifts and is labelled: partition, a pile of rows. One task carries it into one core slot.",
   [T("Spark calls a pile of rows a partition. Each partition is worked on by one task, on one core, from start to finish.")]),
  ("s3_2", "Code lines 1 and 2 type in; the four files become four trays, and the output table shows 28, 25, 25, 25. Four slots light; four stay dark.",
   [T("Our 4 files make 4 piles. These two lines number each row's pile, and count them: 28, 25, 25 and 25.",
      "Our four files make four piles. These two lines number each row's pile, and count them: twenty-eight, twenty-five, twenty-five and twenty-five."),
    T("Four piles: four cores work, and four sit idle.")]),
  ("s3_3", "The rows are dealt into eight trays, two per bench, one beside each core slot. All eight slots light.",
   [T("Deal the rows into eight piles, and every core gets one. All eight work at once.")]),
 ]),
 ("S04_Rounds", "Rounds", [
  ("s4_1", "Thirty-two small trays stack beside the benches. The timeline replaces the storage shelf: eight lanes, one per core; bars appear in four rounds. " + TL,
   [T("So why not 32 piles? The eight cores take the first eight. Whenever a core finishes, it takes the next pile.",
      "So why not thirty-two piles? The eight cores take the first eight. Whenever a core finishes, it takes the next pile."),
    T("Thirty-two piles run in four rounds.")]),
  ("s4_2", "The timeline is named: a lane per core, a bar per task. Each bar's grey cap lights up.",
   [T("This is a timeline: a lane for each core, and a bar for each task."),
    T("Every bar carries the same grey cap: the fixed cost of starting a task and wrapping it up.")]),
  ("s4_3", "Zoom on one bar: the cap is nearly the whole bar. A card: more than 99% fixed cost at about 3 rows a pile; at least tens of thousands of rows before the work matches it.",
   [T("Our piles hold about 3 patients each, so the cap is nearly the whole bar: more than 99 percent of each task is overhead.",
      "Our piles hold about three patients each, so the cap is nearly the whole bar: more than ninety-nine percent of each task is overhead."),
    T("For a simple count, a pile needs at least tens of thousands of rows before its real work matches that fixed cost.")]),
 ]),
 ("S05_Where", "Where piles come from", [
  ("s5_1", "The shelf returns. One big file, written in 8 MB row groups, is cut into eight pieces, one per tray; then thirty-two small files are gathered four at a time into eight trays.",
   [T("So where do piles come from? When Spark reads files, it cuts big files into pieces, and packs small files together."),
    T("One big file of 61 megabytes, written in 8-megabyte row groups, became 8 piles, one for each core. And 32 small files became 8 piles too.",
      "One big file of sixty-one megabytes, written in eight-megabyte row groups, became eight piles, one for each core. And thirty-two small files became eight piles too."),
    T("A piece is never bigger than 128 megabytes, and smaller when that gives every core a piece, but never under 4 megabytes.",
      "A piece is never bigger than a hundred and twenty-eight megabytes, and smaller when that gives every core a piece, but never under four megabytes.")]),
  ("s5_2", "Two cards: data built in code, eight piles; after a shuffle, the setting's 200 piles, with the adaptive merge pointing to Episode 4, where it is shown.",
   [T("Data you build in code starts as one pile per core."),
    T("And after a shuffle, a setting decides: 200 piles by default, unless Spark merges the small ones. That's episode 5.",
      "And after a shuffle, a setting decides: two hundred piles by default, unless Spark merges the small ones. That's episode five.")]),
 ]),
 ("S06_Merge", "Merge or re-deal", [
  ("s6_1", "Code line 3 glows: df.coalesce(2). Trays merge in pairs where they stand: 28 and 25 become 53, 25 and 25 become 50. Nothing crosses the network band.",
   [T("To change the number of piles, there are two tools."),
    T("The first, coalesce, merges piles where they stand. Ours become two piles, of 53 and 50, and not one row is re-dealt.",
      "The first, coalesce, merges piles where they stand. Ours become two piles, of fifty-three and fifty, and not one row is re-dealt.")]),
  ("s6_2", "Code line 4 glows: df.repartition(8). Every chip leaves its tray, goes through the shuffle across the network band, and lands in one of eight new trays (13, 12, 14, 13, 13, 13, 13, 12).",
   [T("The second, repartition, re-deals every row. All 103 go through a shuffle into eight new piles.",
      "The second, repartition, re-deals every row. All one hundred and three go through a shuffle into eight new piles."),
    T("A shuffle has a real cost. We'll open one up in episode 5.", "A shuffle has a real cost. We'll open one up in episode five.")]),
 ]),
 ("S07_Uneven", "Uneven piles", [
  ("s7_1", "The cast is rewritten as four files of 70, 11, 11 and 11 rows; four uneven trays. On the timeline, labelled with its data (the cast ten thousand times over: piles of 700,000 and 110,000 rows, every row hashed), one long bar and three short ones; three lanes go dark while the long bar runs. " + TL,
   [T("Piles don't have to be even. Write our cast as one big file and three small ones: piles of 70, 11, 11 and 11.",
      "Piles don't have to be even. Write our cast as one big file and three small ones: piles of seventy, eleven, eleven and eleven."),
    T("The big pile has over 6 times the rows. At scale, its core works on, while the others finish and wait.",
      "The big pile has over six times the rows. At scale, its core works on, while the others finish and wait.")]),
  ("s7_2", "The end of the long bar is marked: the stage ends here.",
   [T("A stage ends when its last task ends. One big pile holds up the whole job. Remember that. It comes back.")]),
 ]),
 ("S08_Rule", "How many piles?", [
  ("s8_1", "A rule-of-thumb card: a few piles per core, each big enough that its work dwarfs the fixed cost. Three small icons: idle cores, overhead caps, a long bar.",
   [T("So how many piles? A few per core, each one big enough that its work dwarfs the fixed cost."),
    T("Too few, and cores sit idle. Too many tiny ones, and you pay for overhead. Uneven, and everyone waits for the biggest.")]),
 ]),
 ("S09_Recap", "Recap", [
  ("s9_1", "Three recap cards: one pile, one task, one core; where piles come from; merge or re-deal.",
   [T("Let's gather it up."), T("A partition is a pile of rows, and one task works on one pile, on one core."),
    T("Files, code and shuffles decide the piles. Coalesce merges them, and repartition re-deals them.")]),
  ("s9_2", "The code panel: lines 3 and 4 flash; a stopwatch reads zero. Next episode title: Nothing happens until you ask.",
   [T("One loose end: the coalesce and repartition lines returned instantly. Nothing ran until something asked for a result."),
    T("So when does Spark actually do the work? Next time: nothing happens until you ask.")]),
  ("s9_3", "Series end card with the next episode title.",
   [T("This has been Spark, Visually. Thanks for watching the cluster with us.")]),
 ]),
]

# ---- every number in the narration must trace to the numbers JSON (or the home shot it is drawn on)
P, FC, FF = N["piles"], N["fixed_cost"], N["from_files"]
allowed = {
 "4": "piles.four_files (count); E01's four tasks; four idle; rounds_at_32", "8": "slots; piles; created_piles; big/small file piles",
 "103": "cast.patients", "1": "tasks.one", "7": "slots - tasks.one", "28": "piles.four_files[0]", "25": "piles.four_files[1:]",
 "32": "tasks.thirty_two; from_files.small_files", "3": "piles.mean_rows_at_32 (3.2), said 'about 3'",
 "99": "fixed_cost.share_at_32_piles_of_the_cast (0.9998), said 'more than 99 percent'",
 "61": "from_files.big_file_mb (61.1)",
 "128": "from_files.max_partition_mb", "200": "after_shuffle.setting", "5": "episode 5 (M5)", "53": "coalesce.piles[0]",
 "50": "coalesce.piles[1]", "70": "uneven.piles[0]", "11": "uneven.piles[1:]", "6": "uneven.ratio_biggest_to_others (6.4), said 'over 6'",
}
assert N["cast"]["patients"] == 103 and N["slots"] == 8 and N["tasks"] == {"one": 1, "four": 4, "eight": 8, "thirty_two": 32}
assert P["one_file"] == 1 and P["four_files"] == [28, 25, 25, 25] and N["rounds_at_32"] == 4 and N["max_tasks_at_once"] == 8
assert round(P["mean_rows_at_32"]) == 3 and FC["share_at_32_piles_of_the_cast"] > 0.99
assert 10_000 <= FC["rows_whose_work_equals_the_fixed_cost"] < 100_000      # said "tens of thousands of rows"
assert round(FF["big_file_mb"]) == 61 and FF["big_file_piles"] == 8 and FF["small_files"] == 32 and FF["small_file_piles"] == 8
assert FF["max_partition_mb"] == 128 and N["created_piles"] == 8 and N["after_shuffle"]["setting"] == 200
assert sorted(N["coalesce"]["piles"]) == [50, 53] and N["coalesce"]["rows_re_dealt"] == 0
assert N["repartition"]["rows_re_dealt"] == 103 and len(N["repartition"]["piles"]) == 8
assert N["uneven"]["piles"] == [70, 11, 11, 11] and N["uneven"]["ratio_biggest_to_others"] > 6
found = set()
for _, _, segs in S:
    for _, _, sents in segs:
        for s in sents:
            for num in re.findall(r"\d[\d,]*", s["text"]):
                num = num.rstrip(",")
                assert num in allowed, f"untraced number {num!r} in: {s['text']}"
                found.add(num)

narr = {
 "series": "Spark, Visually", "arc": "Part I · How Spark Runs a Job", "episode": 2,
 "title": "Piles of Records", "subtitle": "partitions, tasks and cores",
 "name": "SparkVisually_P1_E02_PilesOfRecords",
 "status": "Script approved by the owner on 26 Nov 2026. Changed for accuracy afterwards: s4_3's 'about 17,000 rows' became 'tens of thousands', and the second-model review then revised s1_1, s4_3, s5_1 and s7_1 (see the review doc). Voiced with am_fenrir. Timelines from this run until e02_timings.json (eight real cores) arrives.",
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
Path("/tmp/e02_md.json").write_text(json.dumps(md))
