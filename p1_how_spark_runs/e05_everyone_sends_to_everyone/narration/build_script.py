"""Builds Episode 5's draft narration.json and the review doc's tables from one source, and checks its numbers."""
import json, re
from pathlib import Path

EP = Path(__file__).resolve().parents[1]                 # the episode folder
N = json.loads((EP / "e05_numbers.json").read_text())
sh = N["shuffles"]
T = lambda text, say=None: ({"text": text, "say": say} if say else {"text": text})

S = [
 ("S01_ColdOpen", None, [
  ("s1_1", "The home shot. Four executor benches, each holding a tray of chips in the clinic hues; the trays read 28, 25, 25 and 25 rows. The code panel is empty.",
   [T("Here's a simple question: how many patients does each clinic have?"),
    T("Our 103 patients are spread across four machines, in four piles.", "Our hundred and three patients are spread across four machines, in four piles.")]),
  ("s1_2", "Alder's orange chips pulse on every bench, with counts 9, 5, 8 and 8.",
   [T("Take Alder. It has 9 patients on this machine, 5 on this one, and 8 on each of the last two.",
      "Take Alder. It has nine patients on this machine, five on this one, and eight on each of the last two."),
    T("No machine can count Alder by itself. Alder's rows have to meet.")]),
  ("s1_3", "Wrong method, sketched once: every chip streams up to the driver and its memory gauge rises. The shot rewinds.",
   [T("We could send every row to the driver and count there. For 103 rows, fine. For a whole registry, the driver chokes.",
      "We could send every row to the driver and count there. For a hundred and three rows, fine. For a whole registry, the driver chokes."),
    T("So Spark does something smarter: it moves rows sideways, between the machines.")]),
 ]),
 ("S02_Title", None, [
  ("s2_1", "Series wordmark, Spark, Visually; Part I, How Spark runs a job; the episode title.",
   [T("Welcome to Spark, Visually."), T("Today: everyone sends to everyone.")]),
 ]),
 ("S03_Wall", "The wall", [
  ("s3_1", "The plan tree on the driver desk: scan, partial count, Exchange, final count. A dashed wall drops across the executors' lanes at the Exchange.",
   [T("Every Spark plan is cut wherever rows must change machines."),
    T("At each cut stands a wall: no work on the far side starts until all the work on the near side is done.")]),
  ("s3_2", "The camera pushes in on the wall.",
   [T("Today, we walk through that wall, and watch what crosses it.")]),
 ]),
 ("S04_Bins", "Into bins", [
  ("s4_1", "Code panel, line 1 glows: spark.conf.set(\"spark.sql.shuffle.partitions\", 5). Five reducer slots, 0 to 4, appear ghosted beyond the wall. Beside each bench, five empty bins numbered 0 to 4.",
   [T("On the far side of the wall, we'll have five reduce tasks. Five clinics, five reducers: one each, you'd hope."),
    T("So every pile gets sorted into five bins, one for each reducer.")]),
  ("s4_2", "Line 2 glows: df.repartition(\"clinic\").collect(). One chip is lifted; its clinic name passes through a hash box; a number comes out; the chip drops into that bin. Over the box: pmod(hash(clinic), 5).",
   [T("Which bin? Spark turns the clinic's name into a big number, called a hash, and keeps the positive remainder after dividing by five."),
    T("The same name gives the same bin, on every machine. That's how Alder's rows will meet.")]),
  ("s4_3", "All four benches sort at once. Birch's chips drop into bin 0, Alder's into bin 1, and Cedar's, Dune's and Elm's all into bin 4. Bins 2 and 3 stay empty on every bench.",
   [T("Now watch where the clinics go. Birch, bin 0. Alder, bin 1.", "Now watch where the clinics go. Birch, bin zero. Alder, bin one."),
    T("And Cedar, Dune and Elm all land in bin 4. Two bins get nothing at all.", "And Cedar, Dune and Elm all land in bin four. Two bins get nothing at all."),
    T("A hash scatters keys. It doesn't deal them out evenly.")]),
  ("s4_4", "Each bench writes its bins to a small disk beside it, labelled local disk. The network band stays dark.",
   [T("Each task writes its bins to its own machine's disk."), T("Notice that nothing has crossed the network yet.")]),
 ]),
 ("S05_Wait", "Everyone waits", [
  ("s5_1", "The wall glows. The map tasks finish one by one; each finished bench shows a tick and waits.",
   [T("So why the wall? Why can't a reducer start as soon as one pile is sorted?"),
    T("Because every reducer needs a piece of every pile. Until the last pile is sorted, no reducer has all of its rows.")]),
  ("s5_2", "The last bench ticks, and the wall opens.",
   [T("So the fastest machines wait for the slowest. Remember that wait. It comes back.")]),
 ]),
 ("S06_Fetch", "Everyone fetches", [
  ("s6_1", "Five reduce tasks take core slots: reducers 0 and 4 on the first bench, 1 on the second, 2 on the third, 3 on the fourth. Each reaches into every bench's disk for its bin; beams cross the network band. Reducers 2 and 3 have nothing to fetch: Spark knows their bins are empty, so they stay put.",
   [T("Now the second half. Each reducer visits every machine and fetches its bin."),
    T("Four piles and five reducers make 20 possible bins. Only 12 hold any rows, and only those 12 are fetched.",
      "Four piles and five reducers make twenty possible bins. Only twelve hold any rows, and only those twelve are fetched.")]),
  ("s6_2", "Beams that leave their own bench turn yellow, with a running total: 9 of 12 bins, 79 of 103 rows. The 3 bins read on their own bench stay grey. Small label: reducers as placed on the home shot.",
   [T("With our reducers placed like this, 9 of those bins cross the network: 79 of our 103 patients, on the move.",
      "With our reducers placed like this, nine of those bins cross the network: seventy-nine of our hundred and three patients, on the move.")]),
  ("s6_3", "Results land. Reducer 0 holds Birch's 30, reducer 1 Alder's 30, reducer 4 Cedar's 20, Dune's 12 and Elm's 11, 43 together. Reducers 2 and 3 are empty. Each clinic's count appears beside it.",
   [T("Now each clinic is in one place. Birch's 30 on one reducer, Alder's 30 on another, and 43 patients from three clinics, crowded onto a third.",
      "Now each clinic is in one place. Birch's thirty on one reducer, Alder's thirty on another, and forty-three patients from three clinics, crowded onto a third."),
    T("And two reducers did nothing at all.")]),
 ]),
 ("S07_SendLess", "Send less", [
  ("s7_1", "Line 3 glows: df.groupBy(\"clinic\").count().collect(). Rewind to the map side. Each tray's chips collapse into small number cards, one per clinic; Alder's cards read 9, 5, 8 and 8.",
   [T("But to count patients, do we really need to move every patient?"),
    T("Each machine can count its own pile first, and send just the numbers.")]),
  ("s7_2", "The bins now hold number cards. A ledger for the same count with and without partial counts: rows crossing the wall, 103 to 20; bytes written, 1,075 to 998; and at ten thousand times the cast, 1,042,872 bytes to 2,048.",
   [T("Now each machine sends one number per clinic: 20 small rows instead of 103.",
      "Now each machine sends one number per clinic: twenty small rows instead of a hundred and three."),
    T("On a cast this small, bytes barely fall, from 1,075 to 998, because every bin carries a fixed overhead.",
      "On a cast this small, bytes barely fall, from a thousand and seventy-five, to nine hundred and ninety-eight, because every bin carries a fixed overhead."),
    T("Scale the cast up ten thousand times, and the count writes about 2,000 bytes instead of over a million.",
      "Scale the cast up ten thousand times, and the count writes about two thousand bytes instead of over a million.")]),
  ("s7_3", "The plan tree: a partial count before the Exchange, a final count after it.",
   [T("Spark does this by itself. Its plan counts twice: a partial count before the wall, and a final count after it.")]),
  ("s7_4", "Line 4 glows: df.groupBy(\"clinic\").agg(F.collect_list(\"y\")).collect(). The number cards turn back into bundles of chips. Ledger: 20 rows, 1,502 bytes.",
   [T("Not every question shrinks like that. Ask for the list of every patient's outcome, and every value still has to travel."),
    T("Still 20 rows, but each one carries a whole list, and the bytes climb to 1,502.",
      "Still twenty rows, but each one carries a whole list, and the bytes climb to fifteen hundred and two.")]),
 ]),
 ("S08_Collide", "Collisions", [
  ("s8_1", "Line 5 glows: spark.conf.set(\"spark.sql.shuffle.partitions\", 3). Three reducers. Bin 2 fills with Alder, Birch and Elm (71 patients), bin 1 with Cedar and Dune (32), and bin 0 stays empty.",
   [T("What if we ask for three reducers instead?"),
    T("Alder, Birch and Elm all hash to the same one: 71 patients. Cedar and Dune share another: 32. The third gets nothing.",
      "Alder, Birch and Elm all hash to the same one: seventy-one patients. Cedar and Dune share another: thirty-two. The third gets nothing.")]),
  ("s8_2", "The three reducer trays stand side by side as columns of 71, 32 and 0 patients, labelled: patients, if every row moved.",
   [T("A stage ends when its busiest task ends. On real data, one crowded reducer can hold up a whole job.")]),
  ("s8_3", "Settings card: spark.sql.shuffle.partitions, default 200. The five clinics light 5 of 200 reducer slots; then all 200, busy and empty alike, merge into one task.",
   [T("And Spark's default is 200 reducers. For five clinics, that's 195 tasks with nothing to read.",
      "And Spark's default is two hundred reducers. For five clinics, that's a hundred and ninety-five tasks with nothing to read."),
    T("Normally, adaptive execution sees that the bins are tiny, and merges them into a single task. We switched that off today, so you could see every bin.")]),
 ]),
 ("S09_Recap", "Recap", [
  ("s9_1", "Three recap cards with icons: bins on a disk, a wall, number cards.",
   [T("Let's gather it up."),
    T("A shuffle sorts every pile into bins by key, and writes them to local disk."),
    T("Then a wall, and each reducer fetches its bin from every machine."),
    T("And Spark sends less when it can: partial results instead of every row.")]),
  ("s9_2", "The hash box again, sending the five clinics into bins 0, 1 and 4.",
   [T("Along the way, the hash put five clinics into three bins, and left two reducers idle.")]),
  ("s9_3", "Teaser: the code panel and the plan tree side by side, a question mark over hashpartitioning. Next episode title: Say what, not how.",
   [T("Our code never said hash, or bins. We asked a question, and Spark chose how to answer it."),
    T("Next time: who makes that choice, and how.")]),
  ("s9_4", "Series end card with the next episode title.",
   [T("This has been Spark, Visually. Thanks for watching the cluster with us.")]),
 ]),
]

# ---- every number in the narration must trace to the numbers JSON (or the structure it records)
allowed = {
 "103": "cast.patients", "9": "trays[0].Alder; home-shot crossing bins", "5": "trays[1].Alder; reducers (code panel)",
 "8": "trays[2,3].Alder", "0": "Birch's bin (hash r5)", "1": "Alder's bin (hash r5)", "4": "Cedar, Dune, Elm's bin (hash r5)",
 "20": "possible bins (4 x 5); partial rows written", "12": "bins non-empty and fetched",
 "79": "home-shot rows crossing", "30": "per_clinic Alder, Birch", "43": "reducer 4 records read",
 "1,075": "partial_aggregation.cast.without.bytes", "2,000": "partial_aggregation.scaled.with.bytes (2,048), said as about 2,000", "998": "count bytes written", "1,502": "collect_list bytes written",
 "71": "r3 reducer 2 patients", "32": "r3 reducer 1 patients", "200": "default shuffle partitions", "195": "occupancy[200].empty",
}
assert N["cast"]["patients"] == 103 and [N["trays"][str(t)]["Alder"] for t in range(4)] == [9, 5, 8, 8]
assert (N["hash"]["Birch"]["5"], N["hash"]["Alder"]["5"], N["hash"]["Cedar"]["5"], N["hash"]["Dune"]["5"], N["hash"]["Elm"]["5"]) == (0, 1, 4, 4, 4)
assert sh["all_rows"]["blocks_possible"] == 20 and sh["all_rows"]["blocks_fetched"] == 12 == sh["all_rows"]["blocks_nonempty"]
assert sh["all_rows"]["home_shot_blocks_crossing_network"] == 9 and sh["all_rows"]["home_shot_records_crossing_network"] == 79
assert sh["all_rows"]["records_read"] == [30, 30, 0, 0, 43]
assert sh["all_rows"]["total_bytes_written"] == 2092 and sh["count"]["total_bytes_written"] == 998 and sh["collect_list"]["total_bytes_written"] == 1502
assert sh["count"]["total_records_written"] == 20 == sh["collect_list"]["total_records_written"]
assert N["r3_patients_per_reducer"] == {"1": 32, "2": 71} and N["occupancy"]["200"]["empty"] == 195
assert N["count_r200_coalesced_reduce_tasks"] == 1
PA = N["partial_aggregation"]
assert PA["cast"]["without"] == {"records": 103, "bytes": 1075} and PA["cast"]["with"] == {"records": 20, "bytes": 998}
assert 1950 <= PA["scaled"]["with"]["bytes"] <= 2050 and PA["scaled"]["without"]["bytes"] > 1_000_000 and PA["scale"] == 10_000
found = set()
for _, _, segs in S:
    for _, _, sents in segs:
        for s in sents:
            for num in re.findall(r"\d[\d,]*", s["text"]):
                num = num.rstrip(",")
                assert num in allowed, f"untraced number {num!r} in: {s['text']}"
                found.add(num)

narr = {
 "series": "Spark, Visually", "arc": "Part I · How Spark Runs a Job", "episode": 5,
 "title": "Everyone Sends to Everyone", "subtitle": "the shuffle",
 "name": "SparkVisually_P1_E05_EveryoneSendsToEveryone",
 "status": "Script approved by the owner on 26 Nov 2026; revised after the second-model review (s4_2, s7_2 and s8_3, the code panel's actions and setting note, and the pictures in S06 to S08; see the review doc). Voiced with am_fenrir.",
 "voice": {"engine": "kokoro-onnx v1.0", "name": "am_fenrir", "audition": ["am_michael", "am_fenrir", "am_echo", "am_eric", "am_liam", "am_adam", "am_onyx", "bm_george", "bm_fable", "bm_lewis", "bm_daniel", "am_fenrir:0.5+am_michael:0.5", "am_michael:0.6+am_onyx:0.4", "bm_george:0.5+bm_fable:0.5", "am_michael:0.7+af_heart:0.3"], "speed": 0.92, "lang": "en-us", "pause_between_sentences": 0.32, "tail_pad": 0.15},
 "pronunciations": {},
 "scenes": [{"id": sid, "chapter": ch, "segments": [{"id": gid, "visual": vis, "sentences": sents} for gid, vis, sents in segs]}
            for sid, ch, segs in S],
}
(EP / "narration" / "narration.json").write_text(json.dumps(narr, indent=1, ensure_ascii=False) + "\n")

words = sum(len(s["text"].split()) for _, _, segs in S for _, _, sents in segs for s in sents)
clips = sum(len(segs) for _, _, segs in S)
print("scenes", len(S), "clips", clips, "words", words, "numbers traced", sorted(found, key=lambda x: int(x.replace(',', ''))))

# ---- markdown tables for the review doc, one table per scene
def esc(x): return x.replace("|", "\\|")
md = {}
for sid, ch, segs in S:
    head = f"### {sid.split('_')[0]} · {ch or ('Cold open' if sid.endswith('ColdOpen') else 'Title')}"
    rows = ["| Clip | On screen | Narration |", "| --- | --- | --- |"]
    for gid, vis, sents in segs:
        rows.append(f"| {gid} | {esc(vis)} | {esc(' '.join(s['text'] for s in sents))} |")
    md[sid] = head + "\n\n" + "\n".join(rows)
Path("/tmp/e05_md.json").write_text(json.dumps(md))
