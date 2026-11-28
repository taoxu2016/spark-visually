"""Builds Episode 7's narration.json and its script-for-review table from one source, and checks its numbers."""
import json
import re
from pathlib import Path

EP = Path(__file__).resolve().parents[1]                 # the episode folder
N = json.loads((EP / "e07_numbers.json").read_text())
T = lambda text, say=None: ({"text": text, "say": say} if say else {"text": text})

S = [
 ("S01_ColdOpen", None, [
  ("s1_1", "The code panel types three lines, slow, fast and kept: one count per clinic, written three ways. A wall counter sits in the corner, blank.",
   [T("Here's one question written three ways: patients per clinic. Every version gives the same answer."),
    T("Which one runs fastest? Make a guess before they run.")]),
  ("s1_2", "Three cards, one per line; each card's wall counter fills in from Spark's own plan: two, one, one.",
   [T("Here's a clue you can read straight from the code: how many walls each one builds. Two, one, and one.")]),
 ]),
 ("S02_Title", None, [
  ("s2_1", "Series wordmark, Spark, Visually; Part II, Why jobs are slow; the episode title.",
   [T("Welcome to Spark, Visually. Part Two: why jobs are slow."), T("Today: counting walls.")]),
 ]),
 ("S03_Narrow", "No wall", [
  ("s3_1", "Five cards: filter, select, withColumn, union, coalesce. For each, arrows run straight down, tray to tray; the wall counter stays at 0, and each runs as one stage.",
   [T("Some steps build each output pile from one input pile. A filter, a select, a new column: each pile is worked where it lies."),
    T("Union just stacks two tables' piles, and coalesce merges neighbours where they stand. No wall, so each runs as a single stage.")]),
 ]),
 ("S04_Wide", "A wall each", [
  ("s4_1", "Six cards: groupBy, distinct, join, repartition, a window over each clinic, orderBy. Each lights Episode 5's all-to-all lattice across the benches; the wall counter ticks: 1, 1, 2 for the join, 1, 1, 1.",
   [T("Other steps need rows from every pile: a count per clinic, distinct rows, a join, a re-deal, a window over each clinic, a sort. Each one builds a wall."),
    T("A join that deals out both tables builds a wall on each side.")]),
  ("s4_2", "orderBy's card: its wall, plus an extra job that samples the rows to choose the ranges. Beside it, sortWithinPartitions: no wall.",
   [T("Sorting needs care. orderBy sorts the whole table: one wall, plus a small extra job that samples the rows to choose the ranges."),
    T("To sort each pile where it lies, there's sortWithinPartitions, with no wall at all.")]),
 ]),
 ("S05_Count", "Count from the plan", [
  ("s5_1", "Line 4 types: slow.explain(). Two Exchange lines light: RoundRobinPartitioning(200), then hashpartitioning(clinic, 200). Slow's wall counter: two.",
   [T("You don't have to guess. Ask for the plan, and count the lines that begin with Exchange. Each one is a wall."),
    T("Slow has two: a re-deal into 200 piles, then the deal by clinic that the count needs anyway.",
      "Slow has two: a re-deal into two hundred piles, then the deal by clinic that the count needs anyway.")]),
  ("s5_2", "Fast's and kept's plans: one Exchange each. Kept's count needs no second wall: its rows are already dealt by clinic.",
   [T("Fast has one. So does kept: it deals by clinic up front, so the count finds each clinic's rows already in one place.")]),
 ]),
 ("S06_Race", "The race", [
  ("s6_1", "Line 5 glows: all three run, on the cast ten thousand times over. A ledger per version: its walls, and the rows crossing each.",
   [T("Now run all three on the cast ten thousand times over, just over a million rows."),
    T("Slow sends every row across its first wall, then 1,000 partial counts across its second.",
      "Slow sends every row across its first wall, then a thousand partial counts across its second."),
    T("Fast sends only partial counts: 40 small rows. Kept sends every row, once.",
      "Fast sends only partial counts: forty small rows. Kept sends every row, once.")]),
  ("s6_2", "Bytes written per version, on this table: fast 2.9 kB, kept 164 kB, slow 404 kB.",
   [T("In bytes, fast wins by far, then kept, then slow."),
    T("Fast wins because there are only five clinics, so the partial counts are tiny. With many more groups, they shrink less.")]),
 ]),
 ("S07_Kept", "Already in place", [
  ("s7_1", "Kept's deal by clinic, with later steps that need clinic lining up behind it, no new wall for them. Beside it, slow's round-robin deal, which no later step needs.",
   [T("So why deal up front at all? Only when several later steps need the same dealing: then one wall serves them all. That's episode 17.",
      "So why deal up front at all? Only when several later steps need the same dealing: then one wall serves them all. That's episode seventeen."),
    T("A re-deal into 200 piles, just to be safe, helps no step at all.",
      "A re-deal into two hundred piles, just to be safe, helps no step at all.")]),
  ("s7_2", "A wide step with no new wall: rows already dealt the right way. A small table copied to every bench, a hint of Episode 8.",
   [T("And a wide step doesn't always build a new wall: not when the rows are already dealt the right way, and, as you'll see, not when a small table is copied instead.")]),
 ]),
 ("S08_Recap", "Recap", [
  ("s8_1", "Three recap cards: narrow steps work each pile where it lies; wide steps build a wall; count the Exchange lines.",
   [T("Let's gather it up."),
    T("Narrow steps work each pile where it lies. Wide steps build a wall, and the plan's Exchange lines count them."),
    T("Fewer walls, and less across each: that's a cost you can read from the code.")]),
  ("s8_2", "A join's two walls; the next episode's title: Send the small table.",
   [T("A join is the commonest wall of all. Must it be one? Next time: send the small table.")]),
  ("s8_3", "Series end card with the next episode title.",
   [T("This has been Spark, Visually. Thanks for watching the cluster with us.")]),
 ]),
]

# ---- every number in the narration must trace to the numbers JSON
R, W = N["race"]["big"], N["walls"]
allowed = {"0": "narrow cards' walls", "1": "walls of fast, kept and most wide cards", "2": "slow's walls; the join's walls",
           "200": "slow's RoundRobinPartitioning(200)", "1,000": "race.big.slow[1].rows (partial counts across slow's second wall)",
           "40": "race.big.fast[0].rows (fast's partial counts)", "17": "episode 17 (M17)", "5": "Episode 5", "8": "Episode 8"}
assert [len(W[k]) for k in ("slow", "fast", "kept")] == [2, 1, 1] and W["slow"][1].startswith("RoundRobinPartitioning(200)")
assert R["slow"][0]["rows"] == N["cast"]["patients"] * N["scale"] and R["slow"][1]["rows"] == 1000 and R["fast"][0]["rows"] == 40
assert R["kept"][0]["rows"] == N["cast"]["patients"] * N["scale"] and len(N["answers"]) == 5
assert all(c["ok"] for c in N["checks"])
found = set()
for _, _, segs in S:
    for _, _, sents in segs:
        for s in sents:
            for num in re.findall(r"\d[\d,.]*\d|\d", s["text"]):
                assert num in allowed, f"untraced number {num!r} in: {s['text']}"
                found.add(num)

narr = {
 "series": "Spark, Visually", "arc": "Part II · Why Jobs Are Slow", "episode": 7,
 "title": "Counting Walls", "subtitle": "reading a job's cost from its code",
 "name": "SparkVisually_P2_E07_CountingWalls",
 "status": "Draft 1, for review: a second model checks Spark accuracy, then the owner. Not voiced.",
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
         f"{len(S)} scenes, {clips} clips, {words} words. Every number comes from `e07_numbers.json`.", ""]
for sid, ch, segs in S:
    lines += [f"## {sid.split('_')[0]} · {ch or ('Cold open' if sid.endswith('ColdOpen') else 'Title')}", "",
              "| Clip | On screen | Narration |", "| --- | --- | --- |"]
    lines += [f"| {gid} | {vis.replace('|', '/')} | {' '.join(s['text'] for s in sents)} |" for gid, vis, sents in segs]
    lines.append("")
(EP / "review" / "e07_script_for_review.md").write_text("\n".join(lines))
