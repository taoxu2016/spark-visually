"""Spark, Visually · Part I · Episode 4: Cut at the Walls (jobs, stages and tasks).

ANIMATION PASS, cut to the am_fenrir voiceover. The plan grows on the driver desk; Spark cuts it at the exchange into
two tinted stages, and the cut drops a wall across the executors' lanes. Stage one runs four tasks; a finished task's
core goes dark, and only when the last one finishes does the wall open for the partial counts to cross. The printed
plan, the UI's jobs and a join's three stages follow, all drawn from e04_numbers.json.

Render one scene:   manim -qm --media_dir build/media episode/episode04.py S05_Run      (run from the episode folder)
Every number comes from e04_numbers.json; none is typed here.
All content stays above y = -3.0 so that burned-in captions never cover it.
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
from sv.board import Board, chapter, lag_for  # noqa: E402
from sv.cast import NAMES, TRAYS, cast_rows, trays_from_counts  # noqa: E402

N = json.loads((EP / "e04_numbers.json").read_text())
CODE = N["code_on_screen"]
M = N["main"]
REC = {pid: (c, bool(y)) for pid, c, _, y in cast_rows()}
RISK = {pid: x for pid, _, x, _ in cast_rows()}
PILES = {t: [pid for pid, _, _ in recs] for t, recs in trays_from_counts(N["trays"], with_ids=True).items()}
KEPT = {t: [p for p in PILES[t] if RISK[p] == 1] for t in range(TRAYS)}
PART = {t: {k: sum(1 for p in KEPT[t] if REC[p][0] == k) for k in NAMES} for t in range(TRAYS)}
PART = {t: {k: n for k, n in v.items() if n} for t, v in PART.items()}
assert sum(len(v) for v in PART.values()) == M["partial_counts_crossing"]
assert {k: sum(PART[t].get(k, 0) for t in PART) for k in N["counts"]} == N["counts"]
NEXT = "Everyone sends to everyone"                          # the next episode's title (blueprint M5)
PLAN_A = ["read", "filter", "count per clinic"]
PLAN_B = ["read", "filter", "partial count", "Exchange", "count"]
S1_FILL, S2_FILL = MUTED, ACCENT                             # the two stages' tints


# ---------------------------------------------------------------- pieces
def stage_bands(plan):
    """Tinted bands behind the desk's plan: stage 1 up to the middle of the Exchange, stage 2 after it."""
    steps = plan[0]
    ex = steps[PLAN_B.index("Exchange")]
    y, h = steps.get_center()[1], steps.height + 0.16
    x0, xm, x1 = steps[0].get_left()[0] - 0.08, ex.get_center()[0], steps[-1].get_right()[0] + 0.08
    b1 = Rectangle(width=xm - x0, height=h, stroke_width=0, fill_color=S1_FILL, fill_opacity=0.22).move_to([(x0 + xm) / 2, y, 0])
    b2 = Rectangle(width=x1 - xm, height=h, stroke_width=0, fill_color=S2_FILL, fill_opacity=0.18).move_to([(xm + x1) / 2, y, 0])
    l1 = txt("stage 1", 10, MUTED, font=SERIF, slant=ITALIC).move_to([(x0 + xm) / 2, y - h / 2 - 0.1, 0])
    l2 = txt("stage 2", 10, ACCENT, font=SERIF, slant=ITALIC).move_to([(xm + x1) / 2, y - h / 2 - 0.1, 0])
    return VGroup(b1, b2, l1, l2)


def exchange_x():
    return driver_plan(PLAN_B)[0][PLAN_B.index("Exchange")].get_center()[0]


def cut_line(down_to: float | None = None):
    """The cut through the Exchange: across the desk, and on down to the wall if asked."""
    x = exchange_x()
    top = DRIVER_C[1] + DRIVER_H / 2 - 0.06
    bottom = DRIVER_C[1] - DRIVER_H / 2 + 0.04 if down_to is None else down_to
    return DashedLine([x, top, 0], [x, bottom, 0], dash_length=0.07, stroke_width=3, color=ACCENT)


def pchip(pid: int, r: float = TRAY_R, opacity: float = 1.0):
    c, e = REC[pid]
    return chip(CLINIC_COLOR[c], e, r=r).set_opacity(opacity)


def card_grid(t: int):
    """Where bench t's partial counts sit: a small grid in the middle of its tray."""
    ks = list(PART[t])
    pos = {}
    for j, k in enumerate(ks):
        col, row = j % 2, j // 2
        pos[k] = [BENCH_XS[t] - 0.32 + 0.64 * col, TRAY_Y0 - 0.12 - 0.42 * row, 0]
    return pos


def partial_card(t: int, k: str, at):
    n = PART[t][k]
    return VGroup(txt(k, 11, CLINIC_COLOR[k]), txt(str(n), 15, CLINIC_COLOR[k], weight=SEMIBOLD, font=DISPLAY)).arrange(
        RIGHT, buff=0.08).move_to(at)


def final_row(k: str, n: int):
    return VGroup(txt(k, 14, CLINIC_COLOR[k]), txt(str(n), 20, CLINIC_COLOR[k], weight=SEMIBOLD, font=DISPLAY)).arrange(RIGHT, buff=0.12)


def stage_box(label: str, sub: str, skipped: bool = False, w: float = 2.35):
    col = FAINT if skipped else PAPER
    body = RoundedRectangle(corner_radius=0.08, width=w, height=0.5, stroke_width=0, fill_color=INK, fill_opacity=1)
    edge = RoundedRectangle(corner_radius=0.08, width=w, height=0.5, stroke_color=FAINT if skipped else MUTED, stroke_width=1.6)
    if skipped:
        edge = DashedVMobject(edge, num_dashes=34)
    t = VGroup(txt(label, 14, col, weight=SEMIBOLD), txt(sub, 12, FAINT if skipped else MUTED)).arrange(RIGHT, buff=0.16)
    return VGroup(body, edge, t)


def ui_card(rows: list, title: str, footer: str | None = None, height: float = 2.6, y: float = -0.25,
            rows_y: float | None = None):
    """A schematic of the Spark UI's Jobs page, drawn from this run: one row per job, one box per stage it lists."""
    box = card(X1 - X0 - 0.2, height).move_to([CX, y, 0])
    head = txt(title, 14, MUTED, font=SERIF, slant=ITALIC).move_to(box.get_corner(UL) + RIGHT * 0.25 + DOWN * 0.25, aligned_edge=UL)
    g = VGroup(box, head)
    lines = VGroup()
    for j, (job, stages) in enumerate(rows):
        lab = txt(job, 16, PAPER, weight=SEMIBOLD)
        boxes = VGroup(*[stage_box(*s) for s in stages]).arrange(RIGHT, buff=0.25)
        lines.add(VGroup(lab, boxes).arrange(RIGHT, buff=0.45))
    lines.arrange(DOWN, buff=0.28, aligned_edge=LEFT).move_to([box.get_left()[0] + 0.35, y + 0.12 if rows_y is None else rows_y, 0],
                                                              aligned_edge=LEFT)
    g.add(lines)
    if footer:
        g.add(txt(footer, 15, ACCENT, weight=SEMIBOLD).move_to([CX, box.get_bottom()[1] + 0.3, 0]))
    return g


def piles_strip(n: int = 200, lit=(), y: float = -0.35):
    """The exchange's n piles, as a strip of small squares; the lit ones hold rows."""
    cols = 25
    sq = VGroup()
    for i in range(n):
        r, c = divmod(i, cols)
        s = Square(side_length=0.2, stroke_width=1, stroke_color=FAINT, fill_color=ACCENT if i in lit else INK,
                   fill_opacity=1 if i in lit else 0.0)
        sq.add(s.move_to([CX - (cols - 1) * 0.13 + c * 0.26, y + 0.9 - r * 0.26, 0]))
    return sq


MONO_W = None


def mono_cw(size: int) -> float:
    return txt("M" * 40, size, PAPER, font=MONO).width / 40


def plan_print(lines: list[str], size: int = 11, width: float = X1 - X0 + 0.1, top: float = 1.38, max_chars: int = 84,
               foot: float = 0.75):
    """Spark's printed plan, line by line in the code font, each line at its true indentation. Returns the card, the
    line mobjects, a function giving a highlight box for a span of a line, and the line strings as shown."""
    cw = mono_cw(size)
    dy = 0.24
    shown = [ln if len(ln) <= max_chars else ln[:max_chars - 1] + "\u2026" for ln in lines]
    h = dy * len(shown) + 0.4 + foot                          # room below the lines for the captions
    box = card(width, h).move_to([CX, top - h / 2, 0])
    x_left = box.get_left()[0] + 0.3
    mobs = VGroup()
    for i, ln in enumerate(shown):
        lead = len(ln) - len(ln.lstrip(" "))
        t = txt(ln.strip(), size, PAPER if i > 1 else MUTED, font=MONO)
        t.move_to([x_left + lead * cw + t.width / 2, top - 0.3 - i * dy, 0])
        mobs.add(t)

    def span(i: int, s: str, color=ACCENT, occurrence: int = 0):
        ln = shown[i]
        j = -1
        for _ in range(occurrence + 1):
            j = ln.index(s, j + 1)
        r = Rectangle(width=len(s) * cw + 0.06, height=dy * 0.92, stroke_width=1.6, stroke_color=color, fill_color=color,
                      fill_opacity=0.18)
        return r.move_to([x_left + (j + len(s) / 2) * cw, mobs[i].get_center()[1], 0])
    return box, mobs, span, shown


def find_line(shown: list[str], needle: str) -> int:
    return next(i for i, ln in enumerate(shown) if needle in ln)


class E04Board(Board):
    CODE, SHELF = CODE, None

    def bands_on(self, rt: float = 0.8):
        b = stage_bands(self.p["plan"])
        self.play(FadeIn(b), run_time=rt)
        self.p["bands"] = b
        self.bring_to_front(self.p["plan"])


# =================================================================================== S01
class S01_ColdOpen(E04Board):
    def construct(self):
        tally = VGroup(txt("Episode 3", 13, MUTED, font=SERIF, slant=ITALIC),
                       txt("1 action \u2192 2 jobs, 2 stages", 16, PAPER, weight=SEMIBOLD)).arrange(DOWN, buff=0.08)
        tally.move_to([DRIVER_C[0] + DRIVER_W / 2 + 1.2, DRIVER_C[1], 0])
        with self.nar("s1_1") as seg:
            self.frame(fade=1.0, code=(2, 1), files=files(), store=storage_label())
            self.show(rt=0.6, tally=tally)
            seg.wait_until(seg.s(1))
            self.play(Indicate(self.p["files"], color=ACCENT, scale_factor=1.03), run_time=1.0)
        (j1, j2), (s1, s2), sk, (t1, t2) = M["job_ids"], M["stage_ids"], M["skipped_ids"][0], M["tasks"]
        ui = ui_card([(f"Job {j1}", [(f"stage {s1}", f"{t1} tasks")]),
                      (f"Job {j2}", [(f"stage {sk}", "skipped", True), (f"stage {s2}", f"{t2} task" + ("s" if t2 != 1 else ""))])],
                     "Spark UI \u00b7 Jobs, from this run",
                     footer=f"ran: {M['jobs']} jobs \u00b7 {M['stages_run']} stages \u00b7 {M['total_tasks']} tasks")
        with self.nar("s1_2", gap=0.8) as seg:
            self.code_to(2, rt=0.8)
            self.show(rt=0.9, ui=ui)
            seg.wait_until(seg.s(1))
            self.play(Indicate(ui[-1], color=ACCENT, scale_factor=1.08), run_time=1.0)
        self.fade_all()


# =================================================================================== S02
class S02_Title(E04Board):
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
class S03_Plan(E04Board):
    def construct(self):
        with self.nar("s3_1") as seg:
            self.frame(fade=1.0, files=files(), store=storage_label(), tag=chapter("S03_Plan"))
            self.plan_to(["read"], rt=0.5)
            seg.wait_until(seg.word(0, "Line one"))
            self.code_to(0, rt=0.8)
            self.plan_to(["read", "filter"])
            seg.wait_until(seg.word(0, "Line two"))
            self.code_to(1, rt=0.8)
            self.plan_to(PLAN_A)
            seg.wait_until(seg.s(1))
            self.play(*[Indicate(b, color=MUTED, scale_factor=1.02) for b in self.p["home"][4:]], run_time=0.9)
        with self.nar("s3_2") as seg:
            chips = VGroup()
            for t in range(TRAYS):
                for k, pid in enumerate(KEPT[t]):
                    chips.add(pchip(pid, opacity=1.0 if REC[pid][0] == "Alder" else 0.28).move_to(tray_pos(t, k)))
            self.play(LaggedStart(*[FadeIn(c, scale=0.6) for c in chips], lag_ratio=lag_for(len(chips), 0.4)), run_time=1.4)
            self.adopt("chips", chips)
            alder = VGroup(*[c for c, pid in zip(chips, [p for t in range(TRAYS) for p in KEPT[t]]) if REC[pid][0] == "Alder"])
            self.play(Indicate(alder, color=ALDER, scale_factor=1.25), run_time=1.0)
            seg.wait_until(seg.word(0, "So when it runs"))
            self.plan_to(PLAN_B)
            seg.wait_until(seg.word(1, "That step is an exchange"))
            self.plan_to(PLAN_B, marked=("Exchange",), rt=0.5)
        self.drop("tag", "chips")


# =================================================================================== S04
class S04_Cut(E04Board):
    def construct(self):
        self.frame(code=(2, 1), files=files(), store=storage_label(), plan=driver_plan(PLAN_B, marked=("Exchange",)))
        with self.nar("s4_1") as seg:
            self.show(rt=0.6, tag=chapter("S04_Cut"))
            cut = cut_line()
            self.play(Create(cut), run_time=0.8)
            self.p["cut"] = cut
            seg.wait_until(seg.word(0, "each piece is a stage"))
            self.bands_on()
            seg.wait_until(seg.word(1, "Stage one reads"))
            self.play(Indicate(self.p["bands"][0], color=PAPER, scale_factor=1.04), run_time=0.8)
            seg.wait_until(seg.word(1, "Stage two finishes"))
            self.play(Indicate(self.p["bands"][1], color=ACCENT, scale_factor=1.08), run_time=0.8)
        with self.nar("s4_2") as seg:
            down = cut_line(down_to=WALL_Y)
            self.play(Transform(self.p["cut"], down), run_time=0.7)
            self.show(rt=0.8, wall=wall("wall"))
            seg.wait_until(seg.s(1))
            self.play(Indicate(self.p["wall"], color=ACCENT, scale_factor=1.02), run_time=1.0)
        with self.nar("s4_3", gap=0.8) as seg:
            seg.wait_until(seg.word(0, "Four piles"))
            tiles = task_tiles(range(TRAYS))
            ro = readout([("stage 1", f"{M['tasks'][0]} tasks", PAPER)], top=-0.2, size=22)
            self.play(LaggedStart(*[GrowFromCenter(t) for t in tiles], lag_ratio=0.2), FadeIn(ro), run_time=0.9)
            self.p.update(tiles=tiles, ro=ro)
        self.drop("tag", "tiles", "ro")


# =================================================================================== S05
class S05_Run(E04Board):
    def construct(self):
        plan = driver_plan(PLAN_B, marked=("Exchange",))
        self.frame(code=(2, 1), files=files(), store=storage_label(), plan=plan, bands=stage_bands(plan),
                   cut=cut_line(down_to=WALL_Y), wall=wall("wall"))
        self.bring_to_front(self.p["plan"])
        order = M["stage1_finish_order"]
        with self.nar("s5_1") as seg:
            self.show(rt=0.6, tag=chapter("S05_Run"))
            seg.wait_until(seg.word(0, "Line three"))
            self.code_to(2, rt=0.8)
            seg.wait_until(seg.word(0, "Four tasks go down"))
            self.drop_tasks(range(TRAYS))
            movers, anims, kept_mobs = [], [], {t: [] for t in range(TRAYS)}
            for t in range(TRAYS):
                src = file_card(t, "")[0].get_center()
                kept = KEPT[t]
                for k, pid in enumerate(PILES[t]):
                    c = pchip(pid, r=0.06).move_to(src + RIGHT * ((k % 9) - 4) * 0.1)
                    movers.append(c)
                    if RISK[pid] == 1:
                        anims.append(c.animate.move_to(tray_pos(t, kept.index(pid))).scale(TRAY_R / 0.06))
                        kept_mobs[t].append((pid, c))
                    else:
                        mid = [BENCH_XS[t] + ((k % 11) - 5) * 0.16, -1.05, 0]
                        anims.append(Succession(c.animate(run_time=1).move_to(mid), FadeOut(c, scale=0.2, run_time=0.3)))
            self.add(*movers)
            self.play(LaggedStart(*anims, lag_ratio=lag_for(len(anims), 0.4)), run_time=2.6)
            cards = VGroup()
            groups = []
            for t in range(TRAYS):
                pos = card_grid(t)
                for k in PART[t]:
                    chips_k = VGroup(*[c for pid, c in kept_mobs[t] if REC[pid][0] == k])
                    pc = partial_card(t, k, pos[k])
                    groups.append(ReplacementTransform(chips_k, pc))
                    cards.add(pc)
            self.play(LaggedStart(*groups, lag_ratio=0.05), run_time=1.2)
            self.adopt("cards", cards)
            seg.wait_until(seg.s(1))
            ticks = VGroup()
            for t in order:
                tk = tick(t)
                dark = slot_tile(t, 0, color=FAINT)
                self.play(Create(tk), Transform(self.p["tasks"][t], dark), run_time=0.45)
                ticks.add(tk)
            self.p["ticks"] = ticks
            seg.wait_until(seg.word(1, "stage two can't begin"))
            self.play(Indicate(self.p["wall"], color=ACCENT, scale_factor=1.02), run_time=1.0)
        with self.nar("s5_2") as seg:
            open_wall = self.p["wall"].copy().set_opacity(0.18)
            self.play(Transform(self.p["wall"], open_wall), run_time=0.8)
            seg.wait_until(seg.word(0, "The partial counts cross"))
            reducer = slot_tile(0, 1, color=ACCENT)
            self.play(GrowFromCenter(reducer), FadeOut(self.p.pop("ticks")), run_time=0.5)
            self.p["reducer"] = reducer
            finals = {k: final_row(k, n) for k, n in N["counts"].items()}
            ks = [k for k in NAMES if k in finals]
            for j, k in enumerate(ks):
                finals[k].move_to([BENCH_XS[0] - 0.42 + 0.84 * (j % 2), TRAY_Y0 - 0.1 - 0.5 * (j // 2), 0])
            by_clinic = {k: VGroup() for k in ks}
            i = 0
            for t in range(TRAYS):
                for k in PART[t]:
                    by_clinic[k].add(self.p["cards"][i])
                    i += 1
            self.play(LaggedStart(*[ReplacementTransform(by_clinic[k], finals[k], path_arc=0.5) for k in ks], lag_ratio=0.2),
                      run_time=2.0)
            self.p.pop("cards")
            self.adopt("finals", VGroup(*[finals[k] for k in ks]))
            for k in ks:
                seg.wait_until(seg.word(1, k))
                self.play(Indicate(finals[k], color=CLINIC_COLOR[k], scale_factor=1.2), run_time=0.5)
            seg.wait_until(seg.word(1, "Elm has none"))
            elm = VGroup(txt("Elm", 14, CLINIC_COLOR["Elm"]), txt("no row", 13, FAINT, font=SERIF, slant=ITALIC)).arrange(RIGHT, buff=0.12)
            elm.next_to(self.p["finals"], DOWN, buff=0.22)
            self.play(FadeIn(elm, shift=UP * 0.1), run_time=0.5)
            self.wait(0.6)
            self.play(FadeOut(elm), run_time=0.5)
            up = self.p["finals"].copy().arrange_in_grid(2, 2, buff=(0.4, 0.12)).scale(0.85)
            up.move_to([DRIVER_C[0] + DRIVER_W / 2 + 1.1, DRIVER_C[1], 0])
            self.play(Transform(self.p["finals"], up), FadeOut(self.p.pop("reducer")), run_time=1.0)
        with self.nar("s5_3", gap=0.8) as seg:
            seg.wait_until(seg.word(0, "piles by default"))
            strip = piles_strip(200, lit=set(N["aqe_off"]["nonempty_piles"]))
            backing = card(X1 - X0 - 0.2, 2.6).move_to([CX, -0.35, 0])
            self.play(FadeIn(backing), LaggedStart(*[FadeIn(s) for s in strip], lag_ratio=0.004), run_time=1.2)
            self.p.update(backing=backing, strip=strip)
            seg.wait_until(seg.word(0, "merges them into one"))
            one = Square(side_length=0.5, stroke_width=1.5, stroke_color=ACCENT, fill_color=ACCENT, fill_opacity=1).move_to([CX, -0.35, 0])
            self.play(ReplacementTransform(strip, one), run_time=1.0)
            self.p["strip"] = one
            ro = readout([("stage 1", f"{M['tasks'][0]} tasks", PAPER), ("stage 2", f"{M['tasks'][1]} task", PAPER)], top=-0.2, size=22)
            self.show(rt=0.6, ro=ro)
            seg.wait_until(seg.s(1))
            tot = readout([("tasks", str(M["total_tasks"]), ACCENT)], top=-1.25, size=26)
            self.show(rt=0.6, tot=tot)
        self.fade_all()


# =================================================================================== S06
class S06_Explain(E04Board):
    def construct(self):
        self.frame(code=(3, 2), plan=driver_plan(PLAN_B, marked=("Exchange",)))
        lines = N["explain_shown"].splitlines()
        box, mobs, span, shown = plan_print(lines)
        scan, filt = find_line(shown, "FileScan"), find_line(shown, "Filter (")
        part, exch = find_line(shown, "partial_count"), find_line(shown, "Exchange ")
        with self.nar("s6_1") as seg:
            self.show(rt=0.6, tag=chapter("S06_Explain"))
            seg.wait_until(seg.word(0, "Line four"))
            self.code_to(3, rt=0.8)
            self.play(FadeIn(box), LaggedStart(*[FadeIn(m) for m in mobs], lag_ratio=0.05), run_time=1.2)
            self.adopt("print", VGroup(box, mobs))
            marks = {}
            for cue, i, needle in (("the scan", scan, "FileScan parquet"), ("the filter", filt, "Filter"),
                                   ("the partial count", part, "partial_count(1)")):
                seg.wait_until(seg.word(0, cue))
                r = span(i, needle, color=MUTED)
                self.play(FadeIn(r), run_time=0.4)
                marks[needle] = r
            seg.wait_until(seg.word(1, "Exchange"))
            ex = span(exch, "Exchange hashpartitioning", color=ACCENT)
            tag = txt("the wall", 14, ACCENT, font=SERIF, slant=ITALIC).next_to(mobs[exch], RIGHT, buff=0.25)
            self.play(FadeIn(ex), FadeIn(tag), run_time=0.6)
            seg.wait_until(seg.word(1, "piles it deals"))
            n200 = span(exch, "200", color=ACCENT)
            self.play(Create(n200), Indicate(mobs[exch], color=ACCENT, scale_factor=1.02), run_time=0.8)
            self.adopt("marks", VGroup(*marks.values(), ex, tag, n200))
        with self.nar("s6_2") as seg:
            seg.wait_until(seg.word(0, "Beside the steps"))
            ones = VGroup(*[span(i, "*(1)") for i, ln in enumerate(shown) if "*(1)" in ln])
            self.play(FadeIn(ones), run_time=0.6)
            self.wait(0.4)
            twos = VGroup(*[span(i, "*(2)") for i, ln in enumerate(shown) if "*(2)" in ln])
            self.play(FadeIn(twos), run_time=0.6)
            seg.wait_until(seg.s(1))
            qs = VGroup(*[span(i, f"QueryStage {q}", color=PAPER) for q in N["query_stage_ids"]
                          for i, ln in enumerate(shown) if f"QueryStage {q}" in ln])
            self.play(FadeIn(qs), run_time=0.6)
            seg.wait_until(seg.s(2))
            ui = VGroup(txt("Spark UI", 14, MUTED, font=SERIF, slant=ITALIC),
                        txt(f"stage {M['stage_ids'][0]} \u00b7 stage {M['stage_ids'][1]}", 18, PAPER, weight=SEMIBOLD),
                        txt("this run's numbers", 12, MUTED, font=SERIF, slant=ITALIC)).arrange(DOWN, buff=0.08)
            side = VGroup(card(ui.width + 0.4, ui.height + 0.3), ui)
            side.move_to([box.get_right()[0] - side.width / 2 - 0.2, box.get_top()[1] - side.height / 2 - 0.2, 0])
            side[1].move_to(side[0])
            self.play(FadeIn(side, shift=LEFT * 0.1), run_time=0.7)
            self.adopt("nums", VGroup(ones, twos, qs, side))
        with self.nar("s6_3", gap=0.8) as seg:
            cap1 = txt("On Spark Connect, explain() shows the starting plan, even after the run.", 15, ACCENT, font=SERIF, slant=ITALIC)
            cap1.move_to([CX, box.get_bottom()[1] + 0.5, 0])
            self.play(FadeIn(cap1, shift=UP * 0.1), run_time=0.7)
            seg.wait_until(seg.s(1))
            cap2 = txt("The plan that ran: the Spark UI, or the query profile.", 14, MUTED, font=SERIF, slant=ITALIC)
            cap2.next_to(cap1, DOWN, buff=0.12)
            self.play(FadeIn(cap2, shift=UP * 0.1), run_time=0.6)
            self.p.update(cap1=cap1, cap2=cap2)
        self.fade_all()


# =================================================================================== S07
class S07_UI(E04Board):
    def construct(self):
        self.frame(code=(4, 2))
        (j1, j2), (s1, s2), sk, (t1, t2) = M["job_ids"], M["stage_ids"], M["skipped_ids"][0], M["tasks"]
        ui = ui_card([(f"Job {j1}", [(f"stage {s1}", f"{t1} tasks")]),
                      (f"Job {j2}", [(f"stage {sk}", "skipped", True), (f"stage {s2}", f"{t2} task" + ("s" if t2 != 1 else ""))])],
                     "Spark UI \u00b7 Jobs, from this run", height=2.9)
        rows = ui[2]
        with self.nar("s7_1") as seg:
            self.show(rt=0.6, tag=chapter("S07_UI"))
            self.play(FadeIn(ui[0]), FadeIn(ui[1]), FadeIn(rows[0]), run_time=0.8)
            seg.wait_until(seg.word(0, "a job of its own"))
            self.play(Indicate(rows[0], color=ACCENT, scale_factor=1.04), run_time=0.9)
            seg.wait_until(seg.word(1, "actually wrote"))
            wrote = txt(f"wrote {M['partial_counts_crossing']} partial counts", 14, MUTED, font=SERIF, slant=ITALIC)
            wrote.next_to(rows[0], RIGHT, buff=0.4)
            self.play(FadeIn(wrote, shift=LEFT * 0.1), run_time=0.6)
            seg.wait_until(seg.word(1, "plans the rest"))
            self.play(FadeIn(rows[1], shift=DOWN * 0.1), run_time=0.8)
            self.adopt("ui", VGroup(ui, wrote))
        skipped_box = rows[1][1][0]
        with self.nar("s7_2") as seg:
            self.play(Indicate(skipped_box, color=FAINT, scale_factor=1.08), run_time=0.9)
            seg.wait_until(seg.s(1))
            num = skipped_box[2][0]
            self.play(num.animate.set_color(ACCENT).scale(1.2), run_time=0.6)
        with self.nar("s7_3", gap=0.8) as seg:
            self.drop("ui", rt=0.5)
            setting = VGroup(*[txt(ln, 13, PAPER, font=MONO) for ln in N["card_aqe_off"]]).arrange(DOWN, buff=0.1, aligned_edge=LEFT)
            sbox = VGroup(card(setting.width + 0.5, setting.height + 0.4), setting)
            sbox.move_to([CX, 1.05, 0])
            sbox[1].move_to(sbox[0])
            ta, tb = N["aqe_off"]["tasks"]
            off = ui_card([("1 job", [("stage", f"{ta} tasks"), ("stage", f"{tb} tasks")])], "Spark UI \u00b7 adaptive execution off",
                          height=2.3, y=-0.85, rows_y=-0.45)
            self.play(FadeIn(sbox, shift=DOWN * 0.1), run_time=0.7)
            self.play(FadeIn(off), run_time=0.8)
            self.p.update(sbox=sbox, off=off)
            seg.wait_until(seg.word(0, "most of those piles"))
            strip = piles_strip(200, lit=set(N["aqe_off"]["nonempty_piles"]), y=-1.0).scale(0.56).move_to([CX, -1.33, 0])
            self.play(LaggedStart(*[FadeIn(s) for s in strip], lag_ratio=0.003), run_time=1.0)
            self.p["strip"] = strip
        self.fade_all()


# =================================================================================== S08
class S08_Join(E04Board):
    def construct(self):
        self.frame(code=(4, None))
        J = N["join"]
        ins, jt = J["input_tasks"], J["join_tasks"]
        xa, xb, y0 = CX - 2.3, CX + 2.3, -1.45

        def node(s, x, y, col=PAPER):
            t = txt(s, 15, col, weight=SEMIBOLD)
            return VGroup(RoundedRectangle(corner_radius=0.08, width=t.width + 0.4, height=0.46, stroke_color=MUTED, stroke_width=1.4,
                                           fill_color=INK, fill_opacity=1), t).move_to([x, y, 0])
        ra, rb = node("read cast", xa, y0), node("read clinics", xb, y0)
        ea, eb = node("Exchange", xa, y0 + 1.0, PAPER), node("Exchange", xb, y0 + 1.0, PAPER)
        jn = node("sort-merge join", CX, y0 + 2.15)
        arrows = VGroup(Arrow(ra.get_top(), ea.get_bottom(), buff=0.06, stroke_width=2, color=MUTED),
                        Arrow(rb.get_top(), eb.get_bottom(), buff=0.06, stroke_width=2, color=MUTED),
                        Arrow(ea.get_top(), jn.get_bottom() + LEFT * 0.5, buff=0.06, stroke_width=2, color=MUTED),
                        Arrow(eb.get_top(), jn.get_bottom() + RIGHT * 0.5, buff=0.06, stroke_width=2, color=MUTED))

        def band(group, label, col):
            r = SurroundingRectangle(group, buff=0.14, corner_radius=0.1, stroke_width=0, fill_color=col, fill_opacity=0.16)
            return VGroup(r, txt(label, 12, col, font=SERIF, slant=ITALIC).next_to(r, DOWN, buff=0.06))
        ba = band(VGroup(ra, ea), f"stage \u00b7 {ins[0]} tasks", MUTED)
        bb = band(VGroup(rb, eb), f"stage \u00b7 {ins[1]} task" + ("s" if ins[1] != 1 else ""), MUTED)
        bj = band(jn, f"stage \u00b7 {jt} task" + ("s" if jt != 1 else ""), ACCENT)
        backing = card(X1 - X0 - 0.2, 3.5).move_to([CX, -0.4, 0])
        with self.nar("s8_1") as seg:
            self.show(rt=0.6, tag=chapter("S08_Join"))
            seg.wait_until(seg.word(0, "One last line"))
            self.code_to(4, rt=0.9)
            self.play(FadeIn(backing), LaggedStart(FadeIn(ra), FadeIn(rb), Create(arrows[0]), Create(arrows[1]), FadeIn(ea), FadeIn(eb),
                                                   Create(arrows[2]), Create(arrows[3]), FadeIn(jn), lag_ratio=0.15), run_time=1.6)
            self.adopt("tree", VGroup(backing, ra, rb, ea, eb, jn, arrows))
            seg.wait_until(seg.s(1))
            hint = txt("hint(\"merge\"): no broadcast", 14, ACCENT, font=MONO).move_to([CX, y0 + 2.75, 0])
            self.play(FadeIn(hint, shift=DOWN * 0.1), run_time=0.6)
            self.p["hint"] = hint
            seg.wait_until(seg.s(2))
            self.play(FadeIn(ba), FadeIn(bb), run_time=0.7)
            self.play(FadeIn(bj), run_time=0.5)
            self.p["bands8"] = VGroup(ba, bb, bj)
            self.bring_to_front(self.p["tree"][1:])
        with self.nar("s8_2", gap=0.8) as seg:
            self.drop("tree", "bands8", "hint", rt=0.5)
            st = J["stages_in_order"]
            t_end = max(s["end_ms"] for s in st)
            x_a, x_b = X0 + 2.3, X1 - 0.5
            sx = lambda ms: x_a + (x_b - x_a) * ms / t_end
            tl = card(X1 - X0 - 0.2, 3.0).move_to([CX, -0.45, 0])
            labels = ["cast \u00b7 " + f"{st[0]['tasks']} task" + ("s" if st[0]["tasks"] != 1 else ""),
                      "clinics \u00b7 " + f"{st[1]['tasks']} task" + ("s" if st[1]["tasks"] != 1 else ""),
                      "join \u00b7 " + f"{st[2]['tasks']} task" + ("s" if st[2]["tasks"] != 1 else "")]
            if st[0]["tasks"] < st[1]["tasks"]:                       # the cast's stage has more tasks, whichever started first
                labels[0], labels[1] = labels[1], labels[0]
            bars = VGroup()
            for j, s in enumerate(st):
                y = 0.45 - j * 0.65
                lab = txt(labels[j], 14, PAPER if j < 2 else ACCENT).move_to([X0 + 1.15, y, 0])
                w = max(0.06, sx(s["end_ms"]) - sx(s["start_ms"]))
                bar = Rectangle(width=w, height=0.34, stroke_width=0, fill_color=ACCENT if j == 2 else MUTED, fill_opacity=1)
                bars.add(VGroup(lab, bar.move_to([sx(s["start_ms"]) + w / 2, y, 0])))
            wl = DashedLine([sx(st[2]["start_ms"]), 0.85, 0], [sx(st[2]["start_ms"]), -1.15, 0], dash_length=0.08, stroke_width=3, color=ACCENT)
            wtag = txt("wall", 13, ACCENT, font=SERIF, slant=ITALIC).next_to(wl, UP, buff=0.05)
            src = txt("this run's times, on one CPU", 12, MUTED, font=SERIF, slant=ITALIC).move_to([CX, -1.65, 0])
            self.play(FadeIn(tl), LaggedStart(*[FadeIn(b[0]) for b in bars], lag_ratio=0.2), run_time=0.8)
            self.play(LaggedStart(*[GrowFromEdge(b[1], LEFT) for b in bars[:2]], lag_ratio=0.1), run_time=1.0)
            self.play(Create(wl), FadeIn(wtag), run_time=0.6)
            self.play(GrowFromEdge(bars[2][1], LEFT), FadeIn(src), run_time=0.8)
            self.adopt("timeline", VGroup(tl, bars, wl, wtag, src))
            seg.wait_until(seg.s(1))
            ro = readout([("1 line", "3 stages", ACCENT)], top=-0.6, size=24)
            self.show(rt=0.6, ro=ro)
        self.fade_all()


# =================================================================================== S09
class S09_Recap(E04Board):
    def construct(self):
        jobs = VGroup(*[RoundedRectangle(corner_radius=0.06, width=0.9, height=0.34, stroke_color=MUTED, stroke_width=1.5) for _ in range(2)]
                      ).arrange(RIGHT, buff=0.15)
        icon1 = VGroup(txt("1 action", 16, PAPER, weight=SEMIBOLD), Arrow(UP * 0.2, DOWN * 0.2, buff=0, stroke_width=3, color=ACCENT),
                       jobs).arrange(DOWN, buff=0.1)
        mini = driver_plan(PLAN_B).scale(0.85)
        icon2 = VGroup(mini, DashedLine(UP * 0.3, DOWN * 0.3, dash_length=0.06, stroke_width=3, color=ACCENT).move_to(
            mini[0][PLAN_B.index("Exchange")]))
        tiles = VGroup(*[RoundedRectangle(corner_radius=0.04, width=0.22, height=0.22, stroke_width=0, fill_color=ACCENT, fill_opacity=1)
                         for _ in range(4)]).arrange(RIGHT, buff=0.12)
        icon3 = VGroup(tiles, DashedLine(LEFT * 0.75, RIGHT * 0.75, dash_length=0.08, stroke_width=3, color=ACCENT)).arrange(DOWN, buff=0.18)
        cards = [recap_card(0, "An action", "starts one job, or several", icon1),
                 recap_card(1, "Cut at each exchange", "each piece is a stage", icon2),
                 recap_card(2, "A task per pile", "and a wall between stages", icon3)]
        with self.nar("s9_1") as seg:
            self.show(rt=0.7, tag=chapter("S09_Recap"))
            seg.wait_until(seg.s(1))
            self.play(FadeIn(cards[0], shift=UP * 0.3), run_time=0.7)
            seg.wait_until(seg.word(1, "Spark cuts"))
            self.play(FadeIn(cards[1], shift=UP * 0.3), run_time=0.7)
            seg.wait_until(seg.s(2))
            self.play(FadeIn(cards[2], shift=UP * 0.3), run_time=0.7)
            self.p.update(r0=cards[0], r1=cards[1], r2=cards[2])
        with self.nar("s9_2") as seg:
            self.show(rt=0.6, r0=None, r1=None, r2=None, tag=None)
            w = wall("wall")
            q = txt("What crosses it?", 26, PAPER, weight=SEMIBOLD, font=DISPLAY).move_to([CX - 2.2, 0.4, 0])
            self.play(Create(w[0]), FadeIn(w[1]), FadeIn(q, shift=UP * 0.1), run_time=1.0)
            self.p.update(w=w, q=q)
            seg.wait_until(seg.word(0, "Next time"))
            nxt = txt(f"Next: {NEXT}", 30, PAPER, weight=SEMIBOLD, font=DISPLAY).move_to([0, -1.2, 0])
            self.play(FadeIn(nxt, shift=UP * 0.15), run_time=0.8)
            self.p["nxt"] = nxt
        with self.nar("s9_3", gap=1.2) as seg:
            self.play(*[FadeOut(m) for m in self.mobjects], run_time=0.8)
            self.p.clear()
            end = end_card(NEXT)
            self.play(Write(end[0]), FadeIn(end[1], shift=UP * 0.1), run_time=1.2)
        self.wait(0.6)
        self.fade_all()
