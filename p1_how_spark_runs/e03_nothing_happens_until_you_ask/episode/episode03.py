"""Spark, Visually · Part I · Episode 3: Nothing Happens Until You Ask (transformations and actions).

ANIMATION PASS, cut to the am_fenrir voiceover. Transformations grow the plan on the driver desk while the benches
stay empty; an action streams every row up from its file through the steps in a single motion, never stopping at a
step, so one pass looks like one pass: a chip drops away at the filter, takes its stamp as it passes the new column,
and loses its risk ring as it passes the select. A count streams through the filter alone (it needs nothing else).

Render one scene:   manim -qm --media_dir build/media episode/episode03.py S04_Count      (run from the episode folder)
Every number comes from e03_numbers.json; none is typed here.
All content stays above y = -3.0 so that burned-in captions never cover it.
Every action reads its files again: nothing stays in the trays between actions.
"""
from __future__ import annotations

import json
import os
import sys
from pathlib import Path

EP = Path(__file__).resolve().parents[1]                     # this episode's folder
SERIES = EP.parents[1]                                        # the series root, for the shared sv package
os.environ["MIM_EPISODE"] = str(EP)
sys.path[:0] = [str(Path(os.environ.get("MIM_REPO", SERIES / "external" / "model-in-motion"))), str(SERIES), str(EP)]
from mim.common import *  # noqa: E402,F403
from sv.stage import *  # noqa: E402,F403
from sv.board import Board, chapter, fmt, lag_for, note  # noqa: E402
from sv.cast import TRAYS, cast_rows, trays_from_counts, write_slices  # noqa: E402

N = json.loads((EP / "e03_numbers.json").read_text())
CODE = N["code_on_screen"]
ROWS = cast_rows()
REC = {pid: (c, bool(y)) for pid, c, _, y in ROWS}
RISK = {pid: x for pid, _, x, _ in ROWS}
SIZES = N["count"]["rows_read_per_task"]
NEXT = "Cut at the walls"                                     # the next episode's title (blueprint M4)
PLAN = ["read", "filter", "new column", "select"]
Y_FILTER, Y_COLUMN, Y_SELECT = -1.12, -0.84, -0.56            # where a streaming row meets each step, inside a bench


def _sorted_files():
    """The cast written sorted by risk: four files, matched to Spark's piles by their rows and higher-risk counts."""
    slices = write_slices(sorted(cast_rows(), key=lambda r: (r[2], r[0])))
    out, used = {}, set()
    for f, want in enumerate(N["pushdown"]["sorted_files"]):
        k = next(i for i, sl in enumerate(slices) if i not in used and len(sl) == want["rows"]
                 and sum(r[2] for r in sl) == want["higher_risk"])
        used.add(k)
        out[f] = [r[0] for r in slices[k]]
    return out


PILES = {t: [pid for pid, _, _ in recs] for t, recs in trays_from_counts(N["trays"], with_ids=True).items()}
SORTED = _sorted_files()
KEPT = {t: [p for p in PILES[t] if RISK[p] == 1] for t in range(TRAYS)}
assert sum(len(v) for v in KEPT.values()) == N["count"]["result"]


# ---------------------------------------------------------------- pieces
def pchip(pid: int, r: float = TRAY_R, ring: bool = True, stamp: bool = False):
    """A patient: the series chip (filled if readmitted), a pale ring if higher-risk (Model in Motion's mark), and the
    new column's stamp. Always three parts, so a chip can gain a stamp or lose its ring mid-flight."""
    c, e = REC[pid]
    body = chip(CLINIC_COLOR[c], e, r=r)
    halo = Circle(radius=r * 1.55, stroke_color=PAPER, stroke_width=1.5).set_fill(opacity=0).move_to(body)
    halo.set_stroke(opacity=0.9 if (ring and RISK[pid] == 1) else 0)
    dot = Dot(radius=r * 0.42, color=ACCENT).move_to(body).set_opacity(1 if stamp else 0)
    return VGroup(body, halo, dot)


def gates(steps: tuple):
    """The steps a streaming row passes on each bench, drawn as faint lines with their names."""
    g = VGroup()
    for step, name, y in (("filter", "filter", Y_FILTER), ("new column", "+ column", Y_COLUMN), ("select", "select", Y_SELECT)):
        if step not in steps:
            continue
        for t in range(TRAYS):
            g.add(DashedLine([BENCH_XS[t] - 0.95, y, 0], [BENCH_XS[t] + 0.95, y, 0], dash_length=0.06, stroke_width=1.2,
                             color=MUTED))
            g.add(txt(name, 10, MUTED, font=SERIF, slant=ITALIC).move_to([BENCH_XS[t] - 0.78, y + 0.09, 0]))
    return g


class E03Board(Board):
    CODE, SHELF = CODE, None

    def stream(self, key: str, sources: dict, steps: tuple = ("filter",), rt: float = 2.6, r: float = 0.065):
        """Every row of each source file streams up through the steps in one motion: a lower-risk row drops away at
        the filter; a kept row takes its stamp at the new column and loses its ring at the select, mid-flight, then
        lands in its bench's tray. sources: {bench: [patients in the file]}."""
        movers, anims, kept_all = [], [], []
        for t, pids in sources.items():
            src = file_card(t, "")[0].get_center()
            kept = [p for p in pids if RISK[p] == 1]
            for k, pid in enumerate(pids):
                g = pchip(pid, r=r).move_to(src + RIGHT * ((k % 9) - 4) * 0.1)
                lane = BENCH_XS[t] + ((k % 13) - 6) * 0.13
                movers.append(g)
                if RISK[pid] == 1:
                    end = tray_pos(t, kept.index(pid))
                    path = VMobject().set_points_as_corners([g.get_center(), [lane, Y_FILTER, 0], [lane, Y_COLUMN, 0],
                                                             [lane, Y_SELECT, 0], end])
                    if "new column" in steps or "select" in steps:
                        def upd(m, steps=steps):
                            y = m[0].get_y()
                            if "new column" in steps:
                                m[2].set_opacity(1 if y >= Y_COLUMN - 0.01 else 0)
                            if "select" in steps:
                                m[1].set_stroke(opacity=0 if y >= Y_SELECT - 0.01 else 0.9)
                        g.add_updater(upd)
                    anims.append(MoveAlongPath(g, path, suspend_mobject_updating=False))
                    kept_all.append(g)
                else:
                    path = VMobject().set_points_as_corners([g.get_center(), [lane, Y_FILTER, 0]])
                    anims.append(Succession(MoveAlongPath(g, path), FadeOut(g, scale=0.2, run_time=0.25)))
        self.add(*movers)
        self.play(LaggedStart(*anims, lag_ratio=lag_for(len(anims), 0.35)), run_time=rt)
        for g in kept_all:
            g.clear_updaters()
        self.remove(*[m for m in movers if m not in kept_all])
        self.adopt(key, VGroup(*kept_all))


# =================================================================================== S01
class S01_ColdOpen(E03Board):
    def construct(self):
        H = N["hook"]
        big_labels = [f"{fmt(n * H['scale'])} rows" for n in SIZES]
        rows_read = H["rows_read_per_line"]
        with self.nar("s1_1") as seg:
            self.frame(fade=1.0, files=files(big_labels), store=storage_label())
            seg.wait_until(seg.s(1))
            for k in range(4):
                self.code_to(k, rt=0.55)
        left = CODE_C[0] - CODE_W / 2 + 0.24
        with self.nar("s1_2", gap=0.8) as seg:
            head = txt("rows read", 15, MUTED, font=SERIF, slant=ITALIC).move_to([left, 0.25, 0], aligned_edge=LEFT)
            self.play(FadeIn(head), run_time=0.4)
            self.p["head"] = head
            for k in range(3):
                row = readout([(f"line {k + 1}", str(rows_read[k]), PAPER)], top=-0.2 - 0.42 * k, size=22)
                self.play(FadeIn(row, shift=RIGHT * 0.1), run_time=0.35)
                self.p[f"r{k}"] = row
            seg.wait_until(seg.s(1))
            lab = txt("line 4", 17, MUTED).move_to([left, -0.2 - 0.42 * 3, 0], aligned_edge=LEFT)
            vt = ValueTracker(0)
            val = always_redraw(lambda: txt(fmt(round(vt.get_value())), 22, ACCENT, weight=SEMIBOLD, font=DISPLAY)
                                .move_to([CODE_C[0] + CODE_W / 2 - 0.24, -0.2 - 0.42 * 3, 0], aligned_edge=RIGHT))
            tiles_ = task_tiles(range(TRAYS))
            self.play(FadeIn(lab), FadeIn(val), FadeIn(tiles_), run_time=0.4)
            self.p["tasks"] = tiles_
            stream = []
            for t in range(TRAYS):
                src = file_card(t, "")[0].get_center()
                for k in range(36):
                    pid = PILES[t][k % len(PILES[t])]
                    c = pchip(pid, r=0.045).move_to(src + RIGHT * ((k % 9) - 4) * 0.1)
                    stream.append((c, [BENCH_XS[t] + ((k % 12) - 5.5) * 0.15, 0.6 - (k // 12) * 0.35, 0]))
            self.add(*[c for c, _ in stream])
            self.play(LaggedStart(*[c.animate.move_to(p).set_opacity(0) for c, p in stream], lag_ratio=lag_for(len(stream), 0.3)),
                      vt.animate.set_value(rows_read[3]), run_time=2.4)
            self.remove(*[c for c, _ in stream])
            val.clear_updaters()
            self.adopt("r3", VGroup(lab, val))
        self.fade_all()


# =================================================================================== S02
class S02_Title(E03Board):
    def construct(self):
        head, line, name, sub = title_card(SPEC["arc"], SPEC["episode"], SPEC["title"], SPEC["subtitle"])
        with self.nar("s2_1", gap=0.6) as seg:
            self.play(Write(head), run_time=1.2)
            seg.wait_until(seg.s(1) - 0.2)
            self.play(LaggedStart(FadeIn(line, shift=UP * 0.15), FadeIn(name, shift=UP * 0.2),
                                  FadeIn(sub, shift=UP * 0.15), lag_ratio=0.25), run_time=1.6)
        self.wait(0.8)
        self.fade_all()


# =================================================================================== S03
class S03_Recipe(E03Board):
    def construct(self):
        with self.nar("s3_1") as seg:
            self.frame(fade=1.0, files=files(), store=storage_label(), tag=chapter("S03_Recipe"))
            self.plan_to(PLAN[:1], rt=0.5)
            seg.wait_until(seg.word(0, "Line one"))
            self.code_to(0, rt=0.9)
            seg.wait_until(seg.word(1, "A step is added"))
            self.plan_to(PLAN[:2])
            seg.wait_until(seg.word(1, "Not one row moves"))
            self.play(*[Indicate(b, color=MUTED, scale_factor=1.02) for b in self.p["home"][4:]], run_time=0.9)
        with self.nar("s3_2") as seg:
            self.code_to(1, rt=0.8)
            self.plan_to(PLAN[:3])
            seg.wait_until(seg.word(0, "line three"))
            self.code_to(2, rt=0.8)
            self.plan_to(PLAN)
            seg.wait_until(seg.s(1))
            self.show(rt=0.6, ro=readout([("rows read", "0", PAPER), ("jobs run", "0", PAPER)], top=0.6, size=24))
        with self.nar("s3_3", gap=0.8) as seg:
            card_ = VGroup(txt("a DataFrame is a recipe", 20, ACCENT, weight=SEMIBOLD),
                           txt("where the rows come from: the files", 15, PAPER),
                           txt("the steps: filter, new column, select", 15, PAPER)).arrange(DOWN, buff=0.16, aligned_edge=LEFT)
            if card_.width > CODE_W - 0.4:
                card_.scale_to_fit_width(CODE_W - 0.4)
            card_.move_to([CODE_C[0], -1.2, 0])
            self.play(FadeIn(card_, shift=UP * 0.1), Indicate(self.p["plan"], color=ACCENT, scale_factor=1.06), run_time=1.0)
            self.p["card"] = card_
        self.drop("tag", "ro", "card")


# =================================================================================== S04
class S04_Count(E03Board):
    def construct(self):
        self.frame(code=(3, 2), files=files(), store=storage_label(), plan=driver_plan(PLAN))
        with self.nar("s4_1") as seg:
            self.show(rt=0.6, tag=chapter("S04_Count"))
            seg.wait_until(seg.word(0, "Line four"))
            self.code_to(3, rt=0.9)
            seg.wait_until(seg.s(1))
            self.drop_tasks(range(TRAYS))
        with self.nar("s4_2") as seg:
            self.show(rt=0.5, gates=gates(("filter",)))
            self.stream("kept", {t: PILES[t] for t in range(TRAYS)}, steps=("filter",), rt=3.0)
            seg.wait_until(seg.s(1))
            self.plan_to(PLAN, dim=("new column", "select"))
            seg.wait_until(seg.word(1, "the one column"))
            reads = txt("reads column x only", 13, ACCENT).move_to([CX, SHELF_Y + SHELF_H / 2 + 0.16, 0])
            self.show(rt=0.6, reads=reads)
        counts = [txt(str(len(KEPT[t])), 26, PAPER, weight=SEMIBOLD, font=DISPLAY).move_to([BENCH_XS[t] + 0.72, BIN_Y, 0])
                  for t in range(TRAYS)]
        with self.nar("s4_3", gap=0.8) as seg:
            self.play(LaggedStart(*[FadeIn(c, scale=1.4) for c in counts], lag_ratio=0.2), FadeOut(self.p.pop("gates")),
                      run_time=0.8)
            fin = slot_tile(0, 1, "+")
            self.play(GrowFromCenter(fin), FadeOut(self.p.pop("tasks")), run_time=0.4)
            spot = fin.get_center() + UP * 0.55
            self.play(*[c.animate(path_arc=0.6).move_to(spot).scale(0.6).set_opacity(0) for c in counts], run_time=0.9)
            self.remove(*counts)
            total = txt(str(N["count"]["result"]), 30, ACCENT, weight=SEMIBOLD, font=DISPLAY).move_to(spot)
            self.play(FadeIn(total, scale=1.4), run_time=0.4)
            up = txt(f"count: {N['count']['result']}", 22, ACCENT, weight=SEMIBOLD, font=DISPLAY).move_to(
                [DRIVER_C[0] + DRIVER_W / 2 + 0.95, DRIVER_C[1], 0])
            self.play(ReplacementTransform(total, up, path_arc=-0.5), FadeOut(fin), run_time=1.0)
            self.p["up"] = up
        self.drop("tag", "kept", "reads", "up")                       # nothing stays in the trays


# =================================================================================== S05
class S05_Again(E03Board):
    def construct(self):
        self.frame(code=(4, 3), files=files(), store=storage_label(), plan=driver_plan(PLAN))
        printed = [ln for ln in N["show"]["printed"].splitlines() if ln.strip()]
        read_files = list(range(len(N["show_jobs"])))                # show() reads file by file: a job of one task each
        assert [j["tasks"] for j in N["show_jobs"]] == [1] * len(read_files)
        assert [j["rows"] for j in N["show_jobs"]] == [SIZES[t] for t in read_files] and sum(SIZES[t] for t in read_files) == N["show"]["rows_read"]
        with self.nar("s5_1") as seg:
            self.show(rt=0.6, tag=chapter("S05_Again"))
            seg.wait_until(seg.word(0, "Line five"))
            self.code_to(4, rt=0.9)
            seg.wait_until(seg.word(0, "runs again"))
            self.drop_tasks(read_files[:1])                                # one file first: show() needs only 21 rows
            seg.wait_until(seg.s(1))
            self.show(rt=0.4, gates=gates(("filter", "new column", "select")))
            self.stream("kept", {read_files[0]: PILES[read_files[0]]}, steps=("filter", "new column", "select"), rt=1.8)
            self.drop_tasks(read_files[1:2], key="tasks2")                 # not 21 rows yet: one more file, then it stops
            self.stream("kept2", {read_files[1]: PILES[read_files[1]]}, steps=("filter", "new column", "select"), rt=1.6)
            out = VGroup(*[txt(ln, 11, PAPER if k > 2 else MUTED, font=MONO) for k, ln in enumerate(printed[:9])]).arrange(
                DOWN, buff=0.05, aligned_edge=LEFT)
            out.move_to([CODE_C[0], -1.05, 0])
            self.show(rt=0.6, out=out)
        with self.nar("s5_2", gap=0.8) as seg:
            self.drop("out", "gates", "tasks", "tasks2", "kept", "kept2", rt=0.5)
            c = VGroup(txt("normally, every action starts from storage", 17, ACCENT, weight=SEMIBOLD),
                       txt(f"cache() runs {N['cache_jobs']} jobs:", 15, PAPER),
                       txt("it waits for an action too (Episode 10)", 15, MUTED, font=SERIF, slant=ITALIC)).arrange(
                DOWN, buff=0.14, aligned_edge=LEFT)
            if c.width > CODE_W - 0.4:
                c.scale_to_fit_width(CODE_W - 0.4)
            c.move_to([CODE_C[0], -1.1, 0])
            self.play(FadeIn(c[0], shift=UP * 0.1), Indicate(self.p["files"], color=ACCENT, scale_factor=1.04), run_time=0.9)
            seg.wait_until(seg.s(1))
            self.play(FadeIn(c[1:], shift=UP * 0.1), run_time=0.7)
            self.adopt("card", c)
        self.drop("tag", "card")


# =================================================================================== S06
class S06_WhyWait(E03Board):
    def construct(self):
        P = N["pushdown"]
        self.frame(code=(5, 4), files=files(), store=storage_label(), plan=driver_plan(PLAN))
        sf = P["sorted_files"]
        notes = VGroup(*[txt(f"{f['higher_risk']} of {f['rows']} higher-risk", 13, PAPER).move_to([BENCH_XS[t], SHELF_Y + SHELF_H / 2 + 0.16, 0])
                         for t, f in enumerate(sf)])
        with self.nar("s6_1") as seg:
            self.show(rt=0.6, tag=chapter("S06_WhyWait"))
            self.play(Indicate(self.p["plan"], color=ACCENT, scale_factor=1.08), run_time=1.0)
            seg.wait_until(seg.s(1))
            self.morph(rt=0.8, same=False, files=files([f"file {t + 1}" for t in range(TRAYS)]))
            self.show(rt=0.6, store=None, notes=notes)
        with self.nar("s6_2", gap=0.8) as seg:
            flt = self.p["plan"][0][1].copy()
            self.add(flt)
            self.play(flt.animate.move_to([CX, SHELF_Y + SHELF_H / 2 + 0.42, 0]).scale(1.8).set_color(ACCENT), run_time=1.1)
            self.p["flt"] = flt
            skipped = [t for t, f in enumerate(sf) if f["higher_risk"] == 0]
            greys = VGroup(*[VGroup(self.p["files"][t].copy().set_opacity(0.25),
                                    txt("skipped", 12, ALERT, weight=SEMIBOLD).move_to([BENCH_XS[t], SHELF_Y, 0])) for t in skipped])
            self.play(*[FadeOut(self.p["files"][t]) for t in skipped], FadeIn(greys), run_time=0.7)
            self.p["greys"] = greys
            push_rows = N["pushdown_rows_per_task"]
            assert {t for t, n in enumerate(push_rows) if n == 0} == set(skipped)
            self.drop_tasks(range(TRAYS))                                  # all four tasks run; two have nothing to read
            done = VGroup(*[tick(t) for t in skipped])
            self.play(Create(done), run_time=0.4)
            self.p["done"] = done
            self.stream("kept", {t: SORTED[t] for t in range(TRAYS) if t not in skipped}, steps=("filter",), rt=2.2)
            self.show(rt=0.6, ro=readout([("rows read", f"{P['rows_read_with']} instead of {P['rows_read_without']}", ACCENT)],
                                         top=-0.5, size=26, stacked=True))
            seg.wait_until(seg.word(1, "Episode six"))
            self.play(Indicate(self.p["ro"], color=ACCENT, scale_factor=1.05), run_time=0.8)
        self.fade_all()


# =================================================================================== S07
class S07_Kinds(E03Board):
    def construct(self):
        K = N["kinds"]
        colx = (-3.4, 3.4)
        hdr = [VGroup(txt("Transformations", 30, PAPER, weight=SEMIBOLD, font=DISPLAY),
                      txt("hand back a DataFrame: a longer recipe, no work", 17, MUTED, font=SERIF, slant=ITALIC)).arrange(DOWN, buff=0.15)
               .move_to([colx[0], 2.3, 0]),
               VGroup(txt("Actions", 30, ACCENT, weight=SEMIBOLD, font=DISPLAY),
                      txt("hand back a result, and do the work", 17, MUTED, font=SERIF, slant=ITALIC)).arrange(DOWN, buff=0.15)
               .move_to([colx[1], 2.3, 0])]
        left = VGroup(*[txt(s, 22, PAPER, font=MONO) for s in ("filter", "withColumn", "select")]).arrange(DOWN, buff=0.35).move_to([colx[0], 0.4, 0])
        rows = [("count", f"a number ({K['count']})"), ("show", "rows on screen"), ("collect", f"rows in your hands ({K['collect']})"),
                ("write", "files written")]
        right = VGroup(*[VGroup(txt(a, 22, ACCENT, font=MONO), txt(f"\u2192 {b}", 18, PAPER)).arrange(RIGHT, buff=0.3) for a, b in rows])
        right.arrange(DOWN, buff=0.3, aligned_edge=LEFT).move_to([colx[1], 0.25, 0])
        divider = DashedLine([0, 2.9, 0], [0, -1.6, 0], dash_length=0.1, color=FAINT)
        with self.nar("s7_1") as seg:
            self.show(rt=0.6, tag=chapter("S07_Kinds"))
            self.play(FadeIn(hdr[0], shift=DOWN * 0.1), Create(divider), run_time=0.8)
            self.play(LaggedStart(*[FadeIn(t, shift=RIGHT * 0.1) for t in left], lag_ratio=0.3), run_time=1.0)
            seg.wait_until(seg.s(1))
            self.play(FadeIn(hdr[1], shift=DOWN * 0.1), run_time=0.6)
            self.play(LaggedStart(*[FadeIn(r, shift=LEFT * 0.1) for r in right], lag_ratio=0.3), run_time=1.4)
            self.adopt("sorting", VGroup(*hdr, left, right, divider))
        C = N["csv"]
        with self.nar("s7_2", gap=0.8) as seg:
            self.drop("sorting", rt=0.6)
            a = VGroup(txt('spark.read.csv(path, header=True, inferSchema=True)', 18, PAPER, font=MONO),
                       txt(f"\u2192 {C['jobs_with_inference']} jobs, to look at the data", 22, ALERT, weight=SEMIBOLD)).arrange(DOWN, buff=0.2)
            b = VGroup(txt('spark.read.csv(path, header=True, schema=schema)', 18, PAPER, font=MONO),
                       txt(f"\u2192 {C['jobs_with_schema']} jobs: lazy again", 22, ACCENT, weight=SEMIBOLD)).arrange(DOWN, buff=0.2)
            a.move_to([0, 1.2, 0])
            b.move_to([0, -0.9, 0])
            self.play(FadeIn(a, shift=UP * 0.1), run_time=0.8)
            self.p["a"] = a
            seg.wait_until(seg.s(1))
            self.play(FadeIn(b, shift=UP * 0.1), run_time=0.8)
            self.p["b"] = b
            assert C["parquet_by_path_jobs"] == C["json_by_path_jobs"] and C["connect"]["jobs_after_read"] == 0
            notes = VGroup(txt(f"Parquet or JSON by path: {C['parquet_by_path_jobs']} job, for the schema", 16, MUTED, font=SERIF, slant=ITALIC),
                           txt("Spark Connect: these jobs wait until the schema is first needed", 16, MUTED, font=SERIF,
                               slant=ITALIC)).arrange(DOWN, buff=0.12).move_to([0, -2.35, 0])
            self.play(FadeIn(notes, shift=UP * 0.1), run_time=0.7)
            self.p["notes"] = notes
        self.fade_all()


# =================================================================================== S08
class S08_Recap(E03Board):
    def construct(self):
        recipe = driver_plan(PLAN).scale(1.0)
        onepass = VGroup(*[pchip(p, r=0.07, stamp=True, ring=False) for p in KEPT[0][:6]]).arrange(RIGHT, buff=0.08)
        storage = VGroup(file_card(0, "file 1"), Arrow(DOWN * 0.1, UP * 0.6, buff=0, color=ACCENT, stroke_width=4)).scale(1.0)
        storage[1].next_to(storage[0], UP, buff=0.1)
        cards = [recap_card(0, "Transformations", "write the recipe", recipe),
                 recap_card(1, "Actions run it", "in one pass over each pile", onepass),
                 recap_card(2, "Every action", "normally starts again from storage", storage)]
        with self.nar("s8_1") as seg:
            self.show(rt=0.7, tag=chapter("S08_Recap"))
            seg.wait_until(seg.s(1))
            self.play(FadeIn(cards[0], shift=UP * 0.3), run_time=0.7)
            seg.wait_until(seg.word(1, "Actions run it"))
            self.play(FadeIn(cards[1], shift=UP * 0.3), run_time=0.7)
            seg.wait_until(seg.s(2))
            self.play(FadeIn(cards[2], shift=UP * 0.3), run_time=0.7)
            self.p.update(r0=cards[0], r1=cards[1], r2=cards[2])
        with self.nar("s8_2") as seg:
            self.show(rt=0.6, r0=None, r1=None, r2=None, tag=None)
            tally = VGroup(txt("hi.count()", 28, PAPER, font=MONO),
                           txt(f"1 action \u2192 {N['count']['jobs']} jobs, {N['count']['stages']} stages", 30, ACCENT, weight=SEMIBOLD,
                               font=DISPLAY)).arrange(DOWN, buff=0.4).move_to([0, 0.6, 0])
            self.play(FadeIn(tally, shift=UP * 0.15), run_time=0.9)
            self.p["tally"] = tally
            seg.wait_until(seg.s(1))
            nxt = txt(f"Next: {NEXT}", 30, PAPER, weight=SEMIBOLD, font=DISPLAY).move_to([0, -1.4, 0])
            self.play(FadeIn(nxt, shift=UP * 0.15), run_time=0.8)
        with self.nar("s8_3", gap=1.2) as seg:
            self.play(*[FadeOut(m) for m in self.mobjects], run_time=0.8)
            self.p.clear()
            end = end_card(NEXT)
            self.play(Write(end[0]), FadeIn(end[1], shift=UP * 0.1), run_time=1.2)
        self.wait(0.6)
        self.fade_all()
