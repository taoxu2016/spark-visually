"""Builds a self-contained accuracy-review prompt for one episode of Spark, Visually, to paste into a different model.

    python tools/review_prompt.py p1_how_spark_runs/e04_cut_at_the_walls      (from the series root)

Reads the episode's narration/narration.json, its eNN_numbers.json and review/brief.json, and writes
review/eNN_accuracy_review_prompt.md. The instructions are the same for every episode and follow the spark-reviewer
agent's checks, adapted for a model that cannot open files; the materials come from the episode's own files, so no
number is retyped.
"""
from __future__ import annotations

import json
import sys
from pathlib import Path

INSTRUCTIONS = """\
# Accuracy review: Spark, Visually · Episode {episode}: {title}

You are reviewing the script of one episode of *Spark, Visually*, an animated video series that shows what an Apache
Spark cluster does when PySpark code runs. Its viewers write PySpark on Databricks but have never watched a job run.
Another model wrote the script. You are the independent second opinion on technical accuracy, {stage} Review accuracy only, not style or pacing.

## Ground rules

- Check every claim against Apache Spark {spark} as the run below used it: PySpark in local mode, default settings
  unless stated, adaptive query execution on. If your knowledge rests on an earlier release, say which. Where
  Databricks Runtime would behave differently in a way a Databricks user would notice, say so.
- The measured facts below were captured {sources} by the episode's numbers
  file, and every check listed there passed. Treat them as what this run did. If one looks wrong to you, explain why
  and how to check it.
- Judge each sentence as a viewer would take it. A claim that is true of this example but sounds like a general rule
  (or the reverse) is an issue.
- The "On screen" notes describe the animation. Check them too: the picture must not show Spark doing something it
  doesn't do.
- The series uses plain words on purpose (see "Deliberate choices"). Flag one only if it would leave a viewer with a
  wrong idea.
- Back each finding with Spark's source code (name the class or method), its configuration reference or its official
  documentation, and say how confident you are. If only running code would settle it, give the PySpark snippet to run.
- Don't rewrite clips that are correct, and don't propose new content beyond what a fix needs.

## What to check

1. Every number said or shown traces to the measured facts, with the right meaning (for example jobs against stages
   against tasks, or stages that ran against stages that were skipped).
2. Every statement about Spark is correct as worded{focus}.
3. The code on screen does what the narration says it does.
4. Nothing contradicts what earlier episodes taught, and nothing reinforces the misconceptions this episode targets.
5. The open questions at the end.

## How to report

**A. Findings**, most serious first, each labelled Wrong (false as worded), Misleading (true, but leaves a wrong idea)
or Imprecise (worth a tighter wording). For each: the clip id; the sentence or visual, quoted; the problem; a suggested
wording in the script's style (short, plain, written to be spoken); your evidence and confidence.

**B. Open questions**: your answer to each, in a sentence or two.

**C. Verified**: what you checked and found correct, briefly.

**D. Unverified**: anything you couldn't settle, with the snippet to run.
"""


def fence(lang: str, body: str) -> str:
    return f"```{lang}\n{body.rstrip()}\n```"


def dig(obj: dict, path: str):
    for part in path.split("."):
        obj = obj[part]
    return obj


def drop(obj: dict, path: str) -> None:
    """Remove a key, or a nested key written with dots, and any parent it leaves empty."""
    *parents, last = path.split(".")
    chain = [obj]
    for part in parents:
        chain.append(chain[-1][part])
    del chain[-1][last]
    for parent, part in zip(reversed(chain[:-1]), reversed(parents)):
        if parent[part] == {}:
            del parent[part]


def compact_json(obj: dict) -> str:
    """A JSON object with one line per key (and one per check), so a reader sees each fact whole."""
    lines = []
    for k, v in obj.items():
        if isinstance(v, list) and v and all(isinstance(x, dict) for x in v):
            inner = ",\n".join("  " + json.dumps(x, ensure_ascii=False) for x in v)
            lines.append(f" {json.dumps(k)}: [\n{inner}\n ]")
        else:
            lines.append(f" {json.dumps(k)}: {json.dumps(v, ensure_ascii=False)}")
    return "{\n" + ",\n".join(lines) + "\n}"


def build(ep: Path) -> Path:
    narr = json.loads((ep / "narration" / "narration.json").read_text())
    nums_path = next(ep.glob("e*_numbers.json"))
    N = json.loads(nums_path.read_text())
    B = json.loads((ep / "review" / "brief.json").read_text())
    stem = nums_path.name.split("_")[0]                      # e04

    out = [INSTRUCTIONS.format(episode=narr["episode"], title=narr["title"], spark=N["engine"]["spark"],
                               focus=(": " + B["focus"]) if B.get("focus") else "",
                               sources=B.get("sources", "from Spark's own `explain()` output and event log"),
                               stage=B.get("stage", "before the series' owner\nreviews it."))]
    out += ["## The episode", "",
            f"Part I, Episode {narr['episode']}: *{narr['title']}* ({narr['subtitle']}), module {B['module']}.", "",
            f"- **Question:** {B['question']}", f"- **Learning objective:** {B['objective']}",
            f"- **Core concept:** {B['core_concept']}", "",
            "### Misconceptions this episode targets", ""] + [f"- {m}" for m in B["misconceptions"]]
    out += ["", "### What earlier episodes taught", ""] + ([f"- {e}" for e in B["earlier_episodes"]]
                                                           or ["None: this is the series' first episode."])
    out += ["", "### The data", "", B["data"], "", "### Deliberate choices (not errors)", ""]
    out += [f"- {d}" for d in B["deliberate_choices"]]

    out += ["", "## Code on screen", "", "These lines run verbatim in the numbers file, in this order.", "",
            fence("python", "\n".join(N["code_on_screen"]))]

    out += ["", "## Measured facts", ""]
    rest = json.loads(json.dumps(N))
    for key, label in B.get("plan_blocks", []):
        out += [f"### {label}", "", fence("text", dig(N, key)), ""]
    for key in [k for k, _ in B.get("plan_blocks", [])] + B.get("omit_keys", []) + ["code_on_screen"]:
        drop(rest, key)
    out += ["### Everything else the numbers file recorded", "",
            "Ids, counts and orders are exact; fields in milliseconds come from a one-CPU machine and matter only as "
            "order.", "", fence("json", compact_json(rest))]

    out += ["", "## The script", "",
            "Each clip is one stretch of voiceover. \"On screen\" describes the animation; \"Narration\" is spoken."]
    for sc in narr["scenes"]:
        sid = sc["id"]
        name = sc["chapter"] or ("Cold open" if sid.endswith("ColdOpen") else "Title")
        out += ["", f"### {sid.split('_')[0]} · {name}", ""]
        for g in sc["segments"]:
            said = " ".join(s["text"] for s in g["sentences"])
            out += [f"- **{g['id']}**. On screen: {g['visual']}", f"  Narration: \u201c{said}\u201d"]

    out += ["", "## Open questions", ""] + [f"{k}. {q}" for k, q in enumerate(B["open_questions"], 1)]
    path = ep / "review" / f"{stem}_accuracy_review_prompt.md"
    path.write_text("\n".join(out) + "\n")
    return path


if __name__ == "__main__":
    p = build(Path(sys.argv[1]))
    text = p.read_text()
    print(p, f"{len(text.split())} words, {len(text)} characters")
