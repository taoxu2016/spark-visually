"""Spark, Visually · Part I · Episode 6: Say What, Not How (from your code to Spark's plan).

ANIMATION PASS, cut to the am_fenrir voiceover. One question written as DataFrame code and as SQL reaches the same
plan. The plan changes in place on the driver desk, phase by phase: analysis finds the names, the optimiser adds a
check, keeps only what is needed and reorders, physical planning hands the read a copy of the filter and two columns
and splits the count around Episode 4's wall. A scaled, risk-sorted Parquet file shows what a read skips; a Python
function hides the filter from all of it. Every number comes from e06_numbers.json.

Render one scene:   manim -qm --media_dir build/media episode/episode06.py S05_Physical      (from the episode folder)
All content stays above y = -3.0 so that burned-in captions never cover it.
"""
from __future__ import annotations

import json
import os
import re
import sys
from pathlib import Path

EP = Path(__file__).resolve().parents[1]                     # this episode's folder
SERIES = EP.parents[1]                                        # the series root, for the shared sv package
os.environ["MIM_EPISODE"] = str(EP)
sys.path[:0] = [str(Path(os.environ.get("MIM_REPO", SERIES / "external" / "model-in-motion"))), str(SERIES), str(EP)]
from mim.common import *  # noqa: E402,F403
from sv.stage import *  # noqa: E402,F403
from sv.board import Board, chapter, lag_for  # noqa: E402
from sv.cast import TRAYS  # noqa: E402

N = json.loads((EP / "e06_numbers.json").read_text())
CODE = N["code_on_screen"]
NEXT = "Counting walls"                                       # the next episode's title (blueprint M7)
NODE_RE = r"^\((\d+)\) (\w[\w ]*?)\s*(?:\[codegen id : \d+\])?$"
NODES = N["formatted_nodes"]
assert [n for _, n in re.findall(NODE_RE, N["formatted_q2"], flags=re.M)] == NODES     # the same plan, both forms
READ_COLS = re.findall(r"(\w+):(\w+)", N["read_schema"])                                 # [(clinic, string), (x, int)]
TABLE_COLS = ["patient", "clinic", "x", "y"]
assert [c for c, _ in READ_COLS] == ["clinic", "x"] and N["cast"]["columns"] == len(TABLE_COLS)
OPT = N["plans"]["q1"]["optimized"]
assert "isnotnull(x" in OPT and re.search(r"Project \[clinic#\d+\]", OPT)
RO = N["reorder"]["optimized"]
assert RO.find("Aggregate") < RO.find("Filter")                                         # the late filter runs first
ST = N["storage"]
GROUPS = ST["groups"]
SKIPPED = [k for k, g in enumerate(GROUPS) if g["x_max"] < 1]
assert len(GROUPS) - len(SKIPPED) == ST["row_groups_that_may_hold_x1"]
NEED = ST["column_bytes_needed"]
C = N["connect"]
assert C["dataframe_at_its_line"] is None and C["dataframe_at_count"] and C["sql_at_its_call"]

PLAN_A = ["read", "filter", "count per clinic"]
PLAN_B = ["read", "filter", "isnotnull(x)", "count per clinic"]
PLAN_C = ["read", "filter", "isnotnull(x)", "keep clinic", "count per clinic"]
PLAN_P = ["scan", "filter", "partial count", "Exchange", "count"]


# ---------------------------------------------------------------- pieces
def mono_block(lines, size=13, color=PAPER):
    return VGroup(*[txt(ln, size, color, font=MONO) for ln in lines]).arrange(DOWN, buff=0.1, aligned_edge=LEFT)


def node_stack(names, x, title, colour=PAPER):
    """A plan's nodes as Spark numbers them, the scan at the bottom."""
    rows = VGroup(*[VGroup(txt(f"({k + 1})", 12, MUTED, font=MONO), txt(n, 15, colour, weight=SEMIBOLD)).arrange(RIGHT, buff=0.15)
                    for k, n in enumerate(names)])
    rows.arrange(UP, buff=0.16, aligned_edge=LEFT)
    head = txt(title, 15, MUTED, font=SERIF, slant=ITALIC)
    g = VGroup(head, rows).arrange(DOWN, buff=0.25, aligned_edge=LEFT)
    box = card(g.width + 0.6, g.height + 0.5)
    box.move_to([x, 0.05, 0])
    g.move_to(box)
    return VGroup(box, g)


def group_grid(lit: set, x0=-2.45, y0=1.35, cols=5, w=1.0, h=0.55):
    """The file's row groups as blocks, five to a row, each showing the smallest and largest x it holds."""
    g = VGroup()
    for k, gr in enumerate(GROUPS):
        r, c = divmod(k, cols)
        on = k in lit
        box = RoundedRectangle(corner_radius=0.05, width=w, height=h, stroke_width=1.4,
                               stroke_color=ACCENT if on else FAINT, fill_color=ACCENT if on else INK,
                               fill_opacity=0.25 if on else 1)
        lab = txt(f"x {gr['x_min']}\u2013{gr['x_max']}", 12, PAPER if on else FAINT, font=MONO)
        g.add(VGroup(box, lab.move_to(box)).move_to([x0 + c * (w + 0.12) + w / 2, y0 - r * (h + 0.12), 0]))
    return g


def column_chips(dim=()):
    return VGroup(*[badge(c, FAINT if c in dim else PAPER, size=13) for c in TABLE_COLS]).arrange(RIGHT, buff=0.12)


def backing(h: float = 3.8, y: float = -0.05):
    """One card over the cluster's benches, for this episode's plans and figures."""
    return card(X1 - X0 - 0.15, h).move_to([CX, y, 0])


def chain(steps, marked=(), y: float = 1.0, width: float = 8.4):
    """A plan's steps, large, on the backing card."""
    c = driver_plan(steps, marked=marked)
    return c.scale(min(2.4, width / c.width)).move_to([CX, y, 0])


class E06Board(Board):
    CODE, SHELF = CODE, None


# =================================================================================== S01
class S01_ColdOpen(E06Board):
    def construct(self):
        with self.nar("s1_1") as seg:
            self.frame(fade=1.0, files=files(), store=storage_label())
            seg.wait_until(seg.word(0, "as DataFrame code"))
            self.code_to(0, rt=0.8)
            seg.wait_until(seg.word(0, "and as SQL"))
            self.code_to(1, rt=0.6)
            self.code_to(2, rt=0.6)
        left, right = node_stack(NODES, CX - 2.3, "q1 \u00b7 DataFrame"), node_stack(NODES, CX + 2.3, "q2 \u00b7 SQL")
        with self.nar("s1_2", gap=0.8) as seg:
            self.code_to(3, rt=0.7)
            self.play(FadeIn(left, shift=UP * 0.1), FadeIn(right, shift=UP * 0.1), run_time=0.8)
            self.p.update(left=left, right=right)
            seg.wait_until(seg.word(0, "Neither"))
            eq = txt("=", 34, ACCENT, weight=SEMIBOLD).move_to([CX, 0.05, 0])
            self.play(FadeIn(eq), *[Indicate(VGroup(a, b), color=ACCENT, scale_factor=1.04)
                                    for a, b in zip(left[1][1], right[1][1])], run_time=1.4)
            self.p["eq"] = eq
        self.fade_all()


# =================================================================================== S02
class S02_Title(E06Board):
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
class S03_Analyse(E06Board):
    def construct(self):
        parsed = [ln for ln in N["plans"]["q2_parsed"].splitlines() if ln.strip()][:4]
        with self.nar("s3_1") as seg:
            self.frame(code=(4, None), files=files(), store=storage_label(), tag=chapter("S03_Analyse"))
            self.code_to(4, rt=0.8)
            back = backing()
            head = txt("the SQL, parsed: plain names, the table unresolved", 15, MUTED, font=SERIF, slant=ITALIC)
            blk = VGroup(head, mono_block(parsed, size=14)).arrange(DOWN, buff=0.2, aligned_edge=LEFT).move_to([CX, 0.95, 0])
            self.play(FadeIn(back), FadeIn(blk, shift=UP * 0.1), run_time=0.9)
            self.p.update(back=back, parsed=blk)
            seg.wait_until(seg.word(0, "looks up every name"))
            found = VGroup(txt("analysed:", 15, ACCENT, font=SERIF, slant=ITALIC),
                           txt("cast \u2192 Relation [patient, clinic, x, y] parquet", 15, PAPER, font=MONO),
                           txt("  \u00b7  ".join(f"{c}: {t}" for c, t in READ_COLS), 15, PAPER, font=MONO)).arrange(
                DOWN, buff=0.14, aligned_edge=LEFT).move_to([CX, -0.4, 0])
            self.play(LaggedStart(*[FadeIn(m, shift=RIGHT * 0.1) for m in found], lag_ratio=0.3), run_time=1.2)
            self.p["found"] = found
            seg.wait_until(seg.s(1))
            self.show(rt=0.6, note=txt("classic PySpark: checked line by line, as you write", 15, PAPER, font=SERIF,
                                       slant=ITALIC).move_to([CX, -1.45, 0]))
        with self.nar("s3_2", gap=0.8) as seg:
            self.code_to(5, rt=0.8)
            err = VGroup(txt(N["misspelt"]["error"], 16, ALERT, weight=SEMIBOLD), txt("UNRESOLVED_COLUMN \u00b7 no job ran", 14, ALERT)
                         ).arrange(DOWN, buff=0.08, aligned_edge=LEFT).move_to([CODE_C[0], -1.6, 0])
            self.show(rt=0.5, err=err)
            seg.wait_until(seg.s(1))
            cap = VGroup(txt("Spark Connect: the DataFrame line passes, and fails when its plan is first needed (.count(), say);",
                             13, ACCENT, font=SERIF, slant=ITALIC),
                         txt("a SQL string still fails at once", 13, ACCENT, font=SERIF, slant=ITALIC)).arrange(DOWN, buff=0.06)
            self.show(rt=0.7, note=None, cap=cap.move_to([CX, -1.45, 0]))
        self.fade_all()


# =================================================================================== S04
class S04_Optimise(E06Board):
    def construct(self):
        with self.nar("s4_1") as seg:
            self.frame(code=(6, 4), files=files(), store=storage_label(), tag=chapter("S04_Optimise"))
            back = backing()
            head = txt("the plan, optimised", 15, MUTED, font=SERIF, slant=ITALIC).move_to([CX, 1.55, 0])
            first = chain(PLAN_A)
            self.play(FadeIn(back), FadeIn(head), FadeIn(first), run_time=0.8)
            self.p.update(back=back, head=head, chain=first)
            seg.wait_until(seg.word(0, "adds a check"))
            self.morph(rt=0.8, chain=chain(PLAN_B, marked=("isnotnull(x)",)))
            seg.wait_until(seg.word(0, "keeps only what"))
            self.morph(rt=0.8, chain=chain(PLAN_C, marked=("keep clinic",)))
        with self.nar("s4_2", gap=0.8) as seg:
            code = txt(N["card_reorder"], 15, PAPER, font=MONO)
            written = txt("as written:   read \u2192 count per clinic \u2192 filter Birch", 16, MUTED)
            chosen = txt("optimised:   read \u2192 filter Birch \u2192 count per clinic", 16, ACCENT, weight=SEMIBOLD)
            g = VGroup(code, written, chosen).arrange(DOWN, buff=0.28, aligned_edge=LEFT).move_to([CX, -0.75, 0])
            self.play(FadeIn(code), FadeIn(written), run_time=0.8)
            seg.wait_until(seg.word(0, "moves it before"))
            self.play(FadeIn(chosen, shift=UP * 0.15), run_time=0.8)
            self.p["reorder"] = g
            seg.wait_until(seg.s(1))
            self.play(Indicate(self.p["chain"], color=ACCENT, scale_factor=1.04), run_time=1.0)
        self.fade_all()


# =================================================================================== S05
class S05_Physical(E06Board):
    def construct(self):
        with self.nar("s5_1") as seg:
            self.frame(code=(6, None), files=files(), store=storage_label(), tag=chapter("S05_Physical"))
            back = backing()
            head = txt("the physical plan", 15, MUTED, font=SERIF, slant=ITALIC).move_to([CX, 1.55, 0])
            first = chain(PLAN_C)
            self.play(FadeIn(back), FadeIn(head), FadeIn(first), run_time=0.7)
            self.p.update(back=back, head=head, chain=first)
            seg.wait_until(seg.word(0, "picks real operators"))
            self.morph(rt=0.9, chain=chain(PLAN_P, marked=("scan",)))
            seg.wait_until(seg.word(0, "a copy of the filter"))
            handed = VGroup(txt("the scan is handed:", 15, MUTED, font=SERIF, slant=ITALIC),
                            badge("filter (a copy)", ACCENT, size=14)).arrange(RIGHT, buff=0.25).move_to([CX - 1.9, 0.05, 0])
            chips = VGroup(txt("and reads:", 15, MUTED, font=SERIF, slant=ITALIC), column_chips()).arrange(RIGHT, buff=0.25)
            chips.move_to([CX + 2.3, 0.05, 0])
            self.show(rt=0.6, handed=handed, chips=chips)
            seg.wait_until(seg.word(0, "read only"))
            dim = [c for c in TABLE_COLS if c not in dict(READ_COLS)]
            self.morph(rt=0.7, chips=VGroup(chips[0].copy(), column_chips(dim=dim).move_to(chips[1])))
            seg.wait_until(seg.word(0, "still runs after"))
            self.show(rt=0.5, after=txt("and the filter still runs after the read", 15, PAPER, font=SERIF,
                                        slant=ITALIC).move_to([CX, -0.75, 0]))
            seg.wait_until(seg.s(1))
            self.show(rt=0.8, wall=wall("wall"))
        with self.nar("s5_2", gap=0.8) as seg:
            self.drop("handed", "chips", "after", "chain", "head", rt=0.4)
            self.code_to(3, rt=0.6)
            rows = VGroup(*[VGroup(txt(f"({k + 1})", 12, MUTED, font=MONO), txt(n, 15, PAPER, weight=SEMIBOLD)).arrange(RIGHT, buff=0.15)
                            for k, n in enumerate(NODES)]).arrange(UP, buff=0.12, aligned_edge=LEFT)
            rows.move_to([CX - 2.5, -0.05, 0])
            detail = VGroup(txt(f"PushedFilters: [{N['pushed_filters']}]", 13, ACCENT, font=MONO),
                            txt(f"ReadSchema: {N['read_schema']}", 13, ACCENT, font=MONO)).arrange(DOWN, buff=0.08, aligned_edge=LEFT)
            detail.next_to(rows[0], RIGHT, buff=0.5)
            head = txt("q1.explain(\"formatted\"): the physical plan, node by node", 15, MUTED, font=SERIF,
                       slant=ITALIC).move_to([CX, 1.55, 0])
            self.play(FadeIn(head), LaggedStart(*[FadeIn(r) for r in rows], lag_ratio=0.1), run_time=0.9)
            self.p.update(head=head, rows=rows)
            cues = [("the scan", 0), ("then the filter", 1), ("the partial count", 3), ("the exchange", 4), ("the final count", 5)]
            for cue, k in cues:
                seg.wait_until(seg.word(0, cue))
                extra = [FadeIn(detail)] if k == 0 else []
                self.play(Indicate(rows[k], color=ACCENT, scale_factor=1.12), *extra, run_time=0.6)
            self.p["detail"] = detail
            seg.wait_until(seg.s(1))
            self.show(rt=0.6, start=txt("AdaptiveSparkPlan isFinalPlan=false: the starting plan", 14, MUTED, font=SERIF,
                                        slant=ITALIC).move_to([CX + 1.6, -1.45, 0]))
        self.fade_all()


# =================================================================================== S06
class S06_Storage(E06Board):
    def construct(self):
        all_groups = set(range(len(GROUPS)))
        RX = X1 - 1.95                                               # the figures' column, right of the grid
        with self.nar("s6_1") as seg:
            self.frame(code=(8, None), tag=chapter("S06_Storage"))
            back = backing(h=3.9)
            title = txt(f"the cast \u00d7 {ST['scale']:,}, sorted by risk: one file, {len(GROUPS)} row groups", 16, PAPER,
                        weight=SEMIBOLD).move_to([CX, 1.6, 0])
            grid = group_grid(all_groups, x0=X0 + 0.2, y0=1.05, h=0.5, w=0.9)
            self.play(FadeIn(back), FadeIn(title), LaggedStart(*[FadeIn(b, scale=0.8) for b in grid], lag_ratio=0.04), run_time=1.6)
            self.p.update(back=back, title=title, grid=grid)
            seg.wait_until(seg.s(1))
            self.play(*[Indicate(b[1], color=ACCENT, scale_factor=1.2) for b in grid], run_time=1.0)
        with self.nar("s6_2", gap=0.8) as seg:
            seg.wait_until(seg.word(0, "skips the"))
            self.morph(rt=0.9, grid=group_grid(all_groups - set(SKIPPED), x0=X0 + 0.2, y0=1.05, h=0.5, w=0.9))
            read = VGroup(txt("groups read", 14, MUTED), txt(f"{len(GROUPS) - len(SKIPPED)} of {len(GROUPS)}", 26, ACCENT,
                                                            weight=SEMIBOLD, font=DISPLAY)).arrange(DOWN, buff=0.06).move_to([RX, 0.95, 0])
            self.show(rt=0.5, read=read)
            seg.wait_until(seg.word(0, "it needs just two columns"))
            sizes = VGroup(txt("column data needed", 14, MUTED),
                           txt(f"clinic + x: {NEED['query'] / 1000:.1f} kB", 17, ACCENT, weight=SEMIBOLD),
                           txt(f"all four: {NEED['all_columns'] / 1e6:.1f} MB", 17, PAPER, weight=SEMIBOLD),
                           txt("sizes from the file's metadata", 12, MUTED, font=SERIF, slant=ITALIC)).arrange(
                DOWN, buff=0.1).move_to([RX, -0.2, 0])
            self.show(rt=0.7, sizes=sizes)
            seg.wait_until(seg.s(1))
            unsorted = VGroup(txt("unsorted, every group", 13, MUTED, font=SERIF, slant=ITALIC),
                              txt("holds both kinds:", 13, MUTED, font=SERIF, slant=ITALIC),
                              txt("nothing is skipped", 13, MUTED, font=SERIF, slant=ITALIC)).arrange(DOWN, buff=0.04).move_to([RX, -1.35, 0])
            self.show(rt=0.6, unsorted=unsorted)
            seg.wait_until(seg.s(2))
            code = N["card_pushdown_off"]
            cut = code.index("(") + 1
            setting = VGroup(txt(code[:cut], 11, PAPER, font=MONO), txt("  " + code[cut:], 11, PAPER, font=MONO)).arrange(
                DOWN, buff=0.06, aligned_edge=LEFT)
            setting = VGroup(card(setting.width + 0.3, setting.height + 0.3), setting)
            setting[1].move_to(setting[0])
            self.show(rt=0.5, unsorted=None, setting=setting.move_to([RX, -1.35, 0]))
            self.morph(rt=0.8, grid=group_grid(all_groups, x0=X0 + 0.2, y0=1.05, h=0.5, w=0.9),
                       read=VGroup(txt("groups read", 14, MUTED), txt(f"{len(GROUPS)} of {len(GROUPS)}", 26, PAPER,
                                                                     weight=SEMIBOLD, font=DISPLAY)).arrange(DOWN, buff=0.06).move_to([RX, 0.95, 0]))
        self.fade_all()


# =================================================================================== S07
class S07_UDF(E06Board):
    def construct(self):
        with self.nar("s7_1") as seg:
            self.frame(code=(6, None), files=files(), store=storage_label(), tag=chapter("S07_UDF"))
            seg.wait_until(seg.word(0, "as a Python function"))
            self.code_to(6, rt=0.7)
            self.code_to(7, rt=0.7)
            seg.wait_until(seg.word(0, "a black box"))
            workers = VGroup(*[badge("Python", ACCENT).move_to([BENCH_XS[t] - 0.62, BIN_Y, 0]) for t in range(TRAYS)])
            empty = txt(f"PushedFilters: [{N['udf']['pushed_filters']}]", 15, ALERT, font=MONO).move_to(
                [DRIVER_C[0] + DRIVER_W / 2 + 1.35, DRIVER_C[1], 0])
            self.show(rt=0.7, workers=workers, empty=empty)
            seg.wait_until(seg.word(1, "the value it checks"))
            trips = []
            for t in range(TRAYS):
                for k in range(3):
                    x = txt("x", 16, PAPER, font=MONO).move_to([BENCH_XS[t] + 0.3 + 0.25 * k, 0.1, 0])
                    back = txt("\u2713", 18, ACCENT).move_to([BENCH_XS[t] + 0.3 + 0.25 * k, 0.1, 0])
                    trips.append(Succession(FadeIn(x, run_time=0.1), x.animate(run_time=0.5).move_to(workers[t]),
                                            Transform(x, back.copy().move_to(workers[t]), run_time=0.1),
                                            x.animate(run_time=0.5).move_to(back), FadeOut(x, run_time=0.2)))
            self.play(LaggedStart(*trips, lag_ratio=0.08), run_time=3.2)
        with self.nar("s7_2", gap=0.8) as seg:
            grid = group_grid(set(range(len(GROUPS)))).scale(0.7).move_to([CX, 0.25, 0])
            note = txt(f"all {len(GROUPS)} row groups read, against {len(GROUPS) - len(SKIPPED)}", 16, PAPER,
                       weight=SEMIBOLD).next_to(grid, DOWN, buff=0.2)
            self.show(rt=0.6, workers=None, grid=VGroup(card(grid.width + 0.5, grid.height + 0.9).move_to(
                VGroup(grid, note)), grid, note))
        self.fade_all()


# =================================================================================== S08
class S08_Recap(E06Board):
    def construct(self):
        icon1 = VGroup(badge("DataFrame", PAPER, 14), txt("=", 24, ACCENT, weight=SEMIBOLD), badge("SQL", PAPER, 14)).arrange(RIGHT, buff=0.18)
        icon2 = column_chips(dim=[c for c in TABLE_COLS if c not in dict(READ_COLS)]).scale(0.85)
        icon3 = VGroup(badge("Python", ACCENT, 14), txt("?", 26, ALERT, weight=SEMIBOLD)).arrange(RIGHT, buff=0.15)
        cards = [recap_card(0, "One plan", "same question, same plan", icon1),
                 recap_card(1, "Rewritten for you", "names checked, only what's needed", icon2),
                 recap_card(2, "A black box", "the optimiser can't see inside", icon3)]
        with self.nar("s8_1") as seg:
            self.show(rt=0.7, tag=chapter("S08_Recap"))
            seg.wait_until(seg.s(1))
            self.play(FadeIn(cards[0], shift=UP * 0.3), run_time=0.7)
            seg.wait_until(seg.word(1, "Spark checks the names"))
            self.play(FadeIn(cards[1], shift=UP * 0.3), run_time=0.7)
            seg.wait_until(seg.s(2))
            self.play(FadeIn(cards[2], shift=UP * 0.3), run_time=0.7)
            self.p.update(r0=cards[0], r1=cards[1], r2=cards[2])
        with self.nar("s8_2") as seg:
            self.show(rt=0.6, r0=None, r1=None, r2=None, tag=None)
            chain = driver_plan(PLAN_P).scale(1.6).move_to([-0.8, 0.6, 0])
            tag = VGroup(RoundedRectangle(corner_radius=0.08, width=1.0, height=0.6, stroke_color=ACCENT, stroke_width=2),
                         txt("?", 26, ACCENT, weight=SEMIBOLD))
            tag[1].move_to(tag[0])
            tag.next_to(chain, RIGHT, buff=0.4)
            self.play(FadeIn(chain), FadeIn(tag, scale=1.3), run_time=0.9)
            self.p.update(chain=chain, price=tag)
            seg.wait_until(seg.word(0, "Next time"))
            nxt = txt(f"Next: Part II \u00b7 {NEXT}", 30, PAPER, weight=SEMIBOLD, font=DISPLAY).move_to([0, -1.3, 0])
            self.play(FadeIn(nxt, shift=UP * 0.15), run_time=0.8)
            self.p["nxt"] = nxt
        with self.nar("s8_3", gap=1.2) as seg:
            self.play(*[FadeOut(m) for m in self.mobjects], run_time=0.8)
            self.p.clear()
            end = end_card(NEXT)
            self.play(Write(end[0]), FadeIn(end[1], shift=UP * 0.1), run_time=1.2)
        self.wait(0.6)
        self.fade_all()
