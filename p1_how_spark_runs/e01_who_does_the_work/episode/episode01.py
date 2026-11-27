"""Spark, Visually · Part I · Episode 1: Who Does the Work (driver, executors, cores).

ANIMATION PASS, cut to the am_fenrir voiceover (no separate animatic: the owner's call, with the look and the scene
machinery set by Episode 5). The cold open floods a laptop; the home shot is then built piece by piece, the notebook
meets the driver (and, with Spark Connect, leaves it), a count runs as four tasks and one final task, a collect brings
every row up, and a Python UDF ships rows out to Python workers.

Render one scene:   manim -qm --media_dir build/media episode/episode01.py S06_Count      (run from the episode folder)
Each `with self.nar("sX_Y") as seg:` block starts that voiceover clip and holds the scene until it has finished;
motion starts on the spoken cue it illustrates.
Every number comes from e01_numbers.json; none is typed here.
All content stays above y = -3.0 so that burned-in captions never cover it.
Every action reads its files from storage again: nothing stays in the trays between actions (blueprint M3).
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
from sv.board import Board, chapter, fmt, lag_for, note  # noqa: E402
from sv.cast import NAMES, TRAYS, cast_rows, trays_from_counts  # noqa: E402

N = json.loads((EP / "e01_numbers.json").read_text())
CODE = N["code_on_screen"]
TRAY = trays_from_counts(N["trays"])
SIZES = N["tray_sizes"]
PATIENTS = N["cast"]["patients"]
SCALED = N["bytes_to_driver"]["scaled"]
NEXT = "Piles of records"                                     # the next episode's title (blueprint M2)
FINAL_BENCH = 0                                              # where the home shot draws the count's final task
assert N["count"]["rows_per_task"] == SIZES and N["count"]["tasks"] == TRAYS == len(SIZES)
assert N["slots"] == 2 * TRAYS                               # four benches, two core slots each

WIRE_Y = DRIVER_C[1]                                          # the notebook's line to the driver, above the network band
WIRE_L, WIRE_R = CODE_C[0] + CODE_W / 2, DRIVER_C[0] - DRIVER_W / 2


# ---------------------------------------------------------------- pieces
def trays(opacity: float = 1.0):
    return VGroup(*[tray(t, TRAY[t], opacity=opacity) for t in range(TRAYS)])


def files():
    return VGroup(*[file_card(t, f"file {t + 1}") for t in range(TRAYS)])


def storage_label():
    return txt("storage", 13, MUTED, font=SERIF, slant=ITALIC).move_to([X0 + 0.42, SHELF_Y + SHELF_H / 2 + 0.16, 0])


def read_moves(t: int):
    """Bench t's task reads its file: a chip leaves the file on the shelf for each record and lands in the tray."""
    src = file_card(t, "")[0].get_center()
    pairs = []
    for k, (c, e) in enumerate(TRAY[t]):
        s = chip(CLINIC_COLOR[c], e, r=0.05).move_to(src + RIGHT * ((k % 9) - 4) * 0.1)
        pairs.append((s, chip(CLINIC_COLOR[c], e, r=TRAY_R).move_to(tray_pos(t, k))))
    return pairs


def task_in_desk(k: int):
    sq = RoundedRectangle(corner_radius=0.05, width=0.22, height=0.22, stroke_width=0, fill_color=ACCENT, fill_opacity=1)
    return sq.move_to([DRIVER_C[0] - 0.5 + 0.33 * k, DRIVER_C[1] - 0.2, 0])


def count_card(t: int):
    return txt(str(SIZES[t]), 28, PAPER, weight=SEMIBOLD, font=DISPLAY).move_to([BENCH_XS[t] + 0.72, BIN_Y, 0])


def desk_text(s: str, color=PAPER, size: int = 20):
    return txt(s, size, color, weight=SEMIBOLD, font=DISPLAY).move_to([DRIVER_C[0] + 0.25, DRIVER_C[1] - 0.2, 0])


def wire(dashed: bool = False):
    a, b = np.array([WIRE_L, WIRE_Y, 0]), np.array([WIRE_R, WIRE_Y, 0])
    return (DashedLine(a, b, dash_length=0.1, stroke_width=2, color=ACCENT) if dashed
            else Line(a, b, stroke_width=3, color=MUTED))


def packet(label: str, color=ACCENT):
    t = txt(label, 13, INK, weight=SEMIBOLD)
    box = RoundedRectangle(corner_radius=0.06, width=t.width + 0.2, height=0.28, stroke_width=0, fill_color=color, fill_opacity=1)
    return VGroup(box, t.move_to(box))


def laptop():
    """S01's laptop: a screen holding the cast, a memory meter along its top, and a keyboard base."""
    screen = RoundedRectangle(corner_radius=0.14, width=5.4, height=3.2, stroke_color=MUTED, stroke_width=3,
                              fill_color=INK_2, fill_opacity=1).move_to([0, 0.65, 0])
    base = Polygon([-3.25, -1.2, 0], [3.25, -1.2, 0], [2.85, -0.95, 0], [-2.85, -0.95, 0], stroke_color=MUTED, stroke_width=3,
                   fill_color=INK_2, fill_opacity=1)
    track = Rectangle(width=3.4, height=0.12, stroke_width=0, fill_color=FAINT, fill_opacity=1).move_to([0.45, 1.95, 0])
    mem = txt("memory", 16, MUTED, font=SERIF, slant=ITALIC).next_to(track, LEFT, buff=0.2)
    return VGroup(screen, base, track, mem)


def laptop_fill(frac: float, color=MUTED):
    w = max(0.02, 3.4 * frac)
    return Rectangle(width=w, height=0.12, stroke_width=0, fill_color=color, fill_opacity=1).move_to([0.45 - 1.7 + w / 2, 1.95, 0])


def laptop_cast():
    rows = cast_rows()
    return VGroup(*[chip(CLINIC_COLOR[c], bool(y), r=0.075).move_to([(k % 15 - 7) * 0.24, 1.35 - (k // 15) * 0.24, 0])
                    for k, (_, c, _, y) in enumerate(rows)])


def registry(n: int = 360):
    """S01's registry: many more patients than the laptop holds, falling in and spilling over."""
    rng = np.random.default_rng(2026)
    cols = list(CLINIC_COLOR.values())
    out = []
    for k in range(n):
        x, y = rng.uniform(-3.6, 3.6), rng.uniform(-0.9, 2.3)
        if k > n * 0.7:                                       # the overflow spills past the screen's edges
            x, y = rng.choice([-1, 1]) * rng.uniform(2.9, 4.6), rng.uniform(-1.6, 2.0)
        c = chip(cols[k % len(cols)], bool(rng.random() < 0.4), r=0.07).move_to([x, 4.4 + rng.uniform(0, 1.5), 0])
        out.append((c, np.array([x, y, 0])))
    return out


def cluster_manager():
    box = card(2.15, 0.62).move_to([-1.45, DRIVER_C[1], 0])
    return VGroup(box, txt("cluster manager", 16, PAPER, weight=SEMIBOLD).move_to(box))


def callouts():
    """S03's labels for the parts, on the empty left of the frame, each with a leader line to bench 1."""
    x0 = CODE_C[0] - 1.6
    rows = [("worker", "a machine", np.array([BENCH_XS[0] - BENCH_W / 2, TRAY_Y0 + 0.55, 0]), 0.95),
            ("executor", "a process on it", np.array([BENCH_XS[0] - BENCH_W / 2 + 0.25, NAME_Y, 0]), -0.15),
            ("core", "runs one task at a time", np.array([BENCH_XS[0] + 0.66, NAME_Y - 0.13, 0]), -1.15)]
    out = []
    for name, what, tip, y in rows:
        a = txt(name, 22, PAPER, weight=SEMIBOLD, font=DISPLAY)
        b = txt(what, 16, MUTED, font=SERIF, slant=ITALIC)
        g = VGroup(a, b).arrange(DOWN, buff=0.08, aligned_edge=LEFT).move_to([x0, y, 0], aligned_edge=LEFT)
        start = np.array([g.get_right()[0] + 0.12, a.get_y(), 0])
        line = Line(start, tip, stroke_width=1.6, color=MUTED)
        out.append(VGroup(g, line))
    return out


def connect_card():
    """S04: what runs both ways, and what Spark Connect leaves out (from the Spark Connect run)."""
    rdd_err = re.search(r"\[(\w+)\]", N["connect"]["rdd"]).group(1)
    rows = VGroup(
        txt("The same code, both ways:", 18, MUTED, font=SERIF, slant=ITALIC),
        txt(f"count {N['connect']['count']} \u00b7 collect {N['connect']['collect_rows']} \u00b7 UDF line {N['connect']['udf_sum']}",
            19, PAPER, weight=SEMIBOLD),
        txt("Not on Spark Connect:", 18, MUTED, font=SERIF, slant=ITALIC),
        txt("df.rdd", 19, ALERT, font=MONO),
        txt(rdd_err, 15, ALERT, font=MONO),
        txt("spark.sparkContext", 19, ALERT, font=MONO))
    rows.arrange(DOWN, buff=0.22, aligned_edge=LEFT)
    rows[3:].shift(RIGHT * 0.25)
    rows[4].shift(UP * 0.1 + RIGHT * 0.2)
    if rows.width > CODE_W - 0.4:
        rows.scale_to_fit_width(CODE_W - 0.4)
    return rows.move_to([CODE_C[0], -0.75, 0])


def scale_card():
    box = card(X1 - X0 + 0.12, 3.25).move_to([CX, -0.25, 0])
    head = txt(f"the cast \u00d7 {fmt(SCALED['factor'])} = {fmt(SCALED['rows'])} rows", 24, PAPER, weight=SEMIBOLD,
               font=DISPLAY).move_to([CX, 0.95, 0])
    r1 = VGroup(txt("count()", 20, PAPER, font=MONO), txt("\u2192  one number up to the driver", 20, MUTED)).arrange(RIGHT, buff=0.3)
    r2 = VGroup(txt("collect()", 20, PAPER, font=MONO),
                txt(f"\u2192  every row: {SCALED['collect_python_megabytes']} MB of Python on the driver", 20, ALERT)).arrange(RIGHT, buff=0.3)
    r1.move_to([CX, 0.15, 0], aligned_edge=LEFT).set_x(CX - 3.6 + r1.width / 2)
    r2.move_to([CX, -0.55, 0]).set_x(CX - 3.6 + r2.width / 2)
    track = Rectangle(width=7.2, height=0.16, stroke_width=0, fill_color=FAINT, fill_opacity=1).move_to([CX, -1.2, 0])
    cap = txt("what lands on the driver", 14, MUTED, font=SERIF, slant=ITALIC).move_to([CX, -1.5, 0])
    return box, head, r1, r2, VGroup(track, cap)


def scale_fill(frac: float, color):
    w = max(0.02, 7.2 * frac)
    return Rectangle(width=w, height=0.16, stroke_width=0, fill_color=color, fill_opacity=1).move_to([CX - 3.6 + w / 2, -1.2, 0])


def udf_plan():
    """S08: line 5's physical plan as Spark printed it, around its Python node: the partial sum, the project, the
    node (marked) and the scan."""
    lines = N["plans"]["udf"].split("\n")
    tree = lines[1:lines.index("", 1)] if "" in lines[1:] else lines[1:]
    node = N["udf"]["plan_node"]
    k = next(i for i, ln in enumerate(tree) if node in ln)
    part = tree[max(0, k - 2): k + 2]
    base = min((len(ln) - len(ln.lstrip())) // 3 for ln in part)
    g, y = VGroup(), -0.2
    for line in part:
        depth = (len(line) - len(line.lstrip())) // 3 - base
        s = line.strip()
        t = txt(s, 14, ACCENT if node in s else MUTED, font=MONO)
        g.add(t.move_to([CODE_C[0] - CODE_W / 2 + 0.3 + 0.28 * depth + t.width / 2, y, 0]))
        y -= 0.36
    return g


def recap_card(k: int, title: str, caption: str, icon):
    x = (-4.35, 0.0, 4.35)[k]
    box = card(3.9, 3.5).move_to([x, 0.1, 0])
    icon.move_to([x, 0.8, 0])
    t = txt(title, 26, PAPER, weight=SEMIBOLD, font=DISPLAY).move_to([x, -0.45, 0])
    cap = txt(caption, 17, MUTED, font=SERIF, slant=ITALIC).move_to([x, -1.05, 0])
    return VGroup(box, icon, t, cap)


class E01Board(Board):
    CODE, SHELF = CODE, None

    def read(self, t_list=range(TRAYS), rt: float = 1.8):
        """Each listed bench's task reads its file from storage into its tray."""
        self.fly("trays", [p for t in t_list for p in read_moves(t)], rt=rt, share=0.25, arc=0.5)


# =================================================================================== S01
class S01_ColdOpen(E01Board):
    def construct(self):
        lap, cast_chips = laptop(), laptop_cast()
        fill = laptop_fill(PATIENTS / 2000)
        with self.nar("s1_1") as seg:
            self.play(FadeIn(lap, shift=UP * 0.2), run_time=1.0)
            self.play(LaggedStart(*[GrowFromCenter(c) for c in cast_chips], lag_ratio=lag_for(len(cast_chips), 0.15)),
                      FadeIn(fill), run_time=1.8)
            self.p.update(lap=lap, cast=cast_chips, fill=fill)
            seg.wait_until(seg.word(0, "plenty of room"))
            self.play(Indicate(fill, color=PAPER, scale_factor=1.6), run_time=0.8)
            seg.wait_until(seg.s(1))
            reg = txt("a national registry", 30, PAPER, weight=SEMIBOLD, font=DISPLAY).move_to([0, 3.4, 0])
            self.show(rt=0.7, reg=reg)
        flood = registry()
        with self.nar("s1_2") as seg:
            drops = [c for c, _ in flood]
            self.add(*drops)
            full = laptop_fill(1.0, ALERT)
            self.play(LaggedStart(*[c.animate(rate_func=rush_into).move_to(p) for c, p in flood], lag_ratio=lag_for(len(flood), 0.12)),
                      ReplacementTransform(self.p.pop("fill"), full), run_time=3.0)
            self.p["fill"] = full
            self.play(Wiggle(self.p["lap"], scale_value=1.03, rotation_angle=0.01 * TAU), run_time=0.9)
            seg.wait_until(seg.s(1))
            self.play(*[FadeOut(m) for m in self.mobjects], run_time=0.8)
            self.p.clear()
            machines = VGroup(*[card(1.6, 1.1) for _ in range(4)]).arrange(RIGHT, buff=0.45).move_to([0, 0.4, 0])
            q = txt("?", 72, ACCENT, weight=SEMIBOLD, font=DISPLAY).next_to(machines, UP, buff=0.4)
            self.play(LaggedStart(*[FadeIn(m, shift=UP * 0.2) for m in machines], lag_ratio=0.2), run_time=1.0)
            seg.wait_until(seg.word(1, "which machine does what"))
            self.play(FadeIn(q, scale=1.4), run_time=0.6)
        self.fade_all()


# =================================================================================== S02
class S02_Title(E01Board):
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
class S03_Parts(E01Board):
    def construct(self):
        H = home_parts(None)
        with self.nar("s3_1") as seg:
            self.show(rt=0.6, tag=chapter("S03_Parts"))
            seg.wait_until(seg.word(0, "At the bottom"))
            lab = storage_label()
            self.play(FadeIn(H["shelf"], shift=UP * 0.2), FadeIn(lab), run_time=0.8)
            self.adopt("shelf", VGroup(H["shelf"]))
            self.p["store"] = lab
            seg.wait_until(seg.word(0, "four files"))
            F = files()
            self.play(LaggedStart(*[FadeIn(f, shift=DOWN * 0.15) for f in F], lag_ratio=0.25), run_time=1.2)
            self.adopt("files", F)

        labels = callouts()
        with self.nar("s3_2") as seg:
            self.play(LaggedStart(*[FadeIn(b, shift=UP * 0.3) for b in H["benches"]], lag_ratio=0.2),
                      Create(H["to_shelf"]), run_time=1.6)
            self.adopt("benches", VGroup(H["benches"], H["to_shelf"]))
            self.play(FadeIn(labels[0]), run_time=0.6)
            seg.wait_until(seg.word(0, "called an executor"))
            self.play(FadeIn(labels[1]), Indicate(H["benches"][0][1], color=PAPER), run_time=0.8)
            seg.wait_until(seg.s(1))
            self.play(FadeIn(labels[2]), Indicate(H["benches"][0][2], color=ACCENT, scale_factor=1.3), run_time=0.8)
            self.p.update(lab0=labels[0], lab1=labels[1], lab2=labels[2])
            seg.wait_until(seg.word(1, "eight tasks can run"))
            slots8 = VGroup(*[slot_tile(i, k, color=ACCENT) for i in range(TRAYS) for k in range(2)])
            tally = txt(f"{N['slots']} task slots", 26, ACCENT, weight=SEMIBOLD, font=DISPLAY).move_to([CODE_C[0], -2.3, 0])
            self.play(LaggedStart(*[GrowFromCenter(s) for s in slots8], lag_ratio=0.15), FadeIn(tally), run_time=1.2)
            self.play(FadeOut(slots8), run_time=0.6)
            self.p["tally"] = tally

        with self.nar("s3_3") as seg:
            self.show(rt=0.5, lab0=None, lab1=None, lab2=None, tally=None)
            tag = band_tag()
            self.play(FadeIn(H["band"]), Create(H["to_band"]), Create(H["to_driver"]), FadeIn(tag), run_time=0.9)
            self.adopt("net", VGroup(H["band"], H["to_band"], H["to_driver"]))
            self.p["bandtag"] = tag
            desk, gauge = driver_box(), driver_gauge()
            self.play(FadeIn(desk, shift=DOWN * 0.2), FadeIn(gauge), run_time=0.8)
            self.p.update(desk=desk, gauge=gauge)
            self.play(Indicate(desk, color=ACCENT, scale_factor=1.04), run_time=0.8)
            seg.wait_until(seg.word(0, "holds the Spark session"))
            self.show(rt=0.5, session=desk_text("SparkSession", MUTED, 16))
            seg.wait_until(seg.word(0, "hands it out"))
            hand = VGroup(*[Arrow(DRIVER_C + DOWN * 0.45, [BENCH_XS[t], TOP + 0.05, 0], buff=0, stroke_width=3, color=ACCENT,
                                  max_tip_length_to_length_ratio=0.08) for t in range(TRAYS)])
            self.play(LaggedStart(*[Create(a) for a in hand], lag_ratio=0.15), run_time=0.9)
            self.play(FadeOut(hand), run_time=0.4)
            seg.wait_until(seg.s(1))
            cm = cluster_manager()
            ask = Arrow([DRIVER_C[0] - DRIVER_W / 2, DRIVER_C[1], 0], cm.get_right(), buff=0.05, stroke_width=3, color=MUTED,
                        max_tip_length_to_length_ratio=0.25)
            self.play(FadeIn(cm, shift=RIGHT * 0.2), run_time=0.6)
            self.play(Create(ask), run_time=0.5)
            starts = VGroup(*[Arrow(cm.get_bottom(), [BENCH_XS[t] - 0.4, TOP + 0.05, 0], buff=0.05, stroke_width=2.5, color=MUTED,
                                    max_tip_length_to_length_ratio=0.08) for t in range(TRAYS)])
            self.play(LaggedStart(*[Create(a) for a in starts], lag_ratio=0.2),
                      LaggedStart(*[Indicate(b, color=PAPER, scale_factor=1.03) for b in self.p["benches"][0]], lag_ratio=0.2),
                      run_time=1.6)
            self.p.update(cm=cm, ask=ask, starts=starts)
        self.drop("tag", "session", "cm", "ask", "starts")             # S04 opens on the finished home shot


# =================================================================================== S04
class S04_Notebook(E01Board):
    def construct(self):
        self.frame(panel=False, files=files(), store=storage_label())
        box = code_box()
        badge_py = badge("Python", ACCENT).move_to([DRIVER_C[0] - 1.05, DRIVER_C[1] - 0.2, 0])
        with self.nar("s4_1") as seg:
            self.show(rt=0.6, tag=chapter("S04_Notebook"))
            self.play(FadeIn(box, shift=RIGHT * 0.6), run_time=0.9)
            self.p["codebox"] = box
            w = wire()
            self.play(Create(w), FadeIn(badge_py, scale=1.3), run_time=0.9)
            self.p.update(wire=w, py=badge_py)
            self.show(rt=0.5, wnote=note("classic cluster", y=WIRE_Y + 0.25, x=(WIRE_L + WIRE_R) / 2))

        client = badge("client", ACCENT).move_to([CODE_C[0] + 0.75, CODE_C[1] + CODE_H / 2 - 0.3, 0])
        with self.nar("s4_2") as seg:
            seg.wait_until(seg.word(0, "With Spark Connect"))
            dw, cn = wire(dashed=True), note("Spark Connect", y=WIRE_Y + 0.25, x=(WIRE_L + WIRE_R) / 2)
            self.play(ReplacementTransform(self.p.pop("py"), client), FadeTransform(self.p.pop("wire"), dw),
                      FadeTransform(self.p.pop("wnote"), cn), run_time=1.0)
            self.p.update(client=client, wire=dw, wnote=cn)
            seg.wait_until(seg.word(0, "sends each query"))
            plan = packet("plan").move_to([WIRE_L + 0.3, WIRE_Y, 0])
            self.play(FadeIn(plan, scale=0.6), run_time=0.3)
            self.play(plan.animate.move_to([WIRE_R - 0.35, WIRE_Y, 0]), run_time=1.2)
            self.play(FadeOut(plan), Indicate(self.p["desk"], color=ACCENT, scale_factor=1.03), run_time=0.6)
            seg.wait_until(seg.word(0, "gets back only the result"))
            res = packet("result", PAPER).move_to([WIRE_R - 0.35, WIRE_Y, 0])
            self.play(FadeIn(res, scale=0.6), run_time=0.3)
            self.play(res.animate.move_to([WIRE_L + 0.4, WIRE_Y, 0]), run_time=1.2)
            self.play(FadeOut(res), run_time=0.4)

        with self.nar("s4_3", gap=0.8) as seg:
            cc = connect_card()
            self.play(LaggedStart(*[FadeIn(r, shift=UP * 0.1) for r in cc[:2]], lag_ratio=0.3), run_time=0.9)
            seg.wait_until(seg.word(0, "doesn't run on Spark Connect"))
            self.play(LaggedStart(*[FadeIn(r, shift=UP * 0.1) for r in cc[2:]], lag_ratio=0.3), run_time=1.0)
            self.adopt("ccard", cc)
            seg.wait_until(seg.word(0, "So we won't use it"))
            self.play(Indicate(cc[3]), Indicate(cc[4]), Indicate(cc[5]), run_time=0.9)
        self.drop("tag", "client", "wire", "wnote", "ccard")


# =================================================================================== S05
class S05_Read(E01Board):
    def construct(self):
        self.frame(files=files(), store=storage_label())
        with self.nar("s5_1") as seg:
            self.show(rt=0.6, tag=chapter("S05_Read"))
            seg.wait_until(seg.word(0, "Line one"))
            self.code_to(0, rt=1.2)
            seg.wait_until(seg.s(1))
            self.code_rest()
            jobs = 0 if all(c["ok"] for c in N["checks"] if c["name"] == "reading the table runs no job") else None
            assert jobs == 0
            self.show(rt=0.6, ro=readout([("jobs run", str(jobs), PAPER), ("rows read", "0", PAPER)], top=1.0, size=24))
            seg.wait_until(seg.word(1, "No machine touches"))
            self.play(*[Indicate(b, color=MUTED, scale_factor=1.02) for b in self.p["home"][4:]], run_time=1.0)
        self.drop("tag", "ro")


# =================================================================================== S06
class S06_Count(E01Board):
    def construct(self):
        self.frame(code=(1, None), files=files(), store=storage_label())
        with self.nar("s6_1") as seg:
            self.show(rt=0.6, tag=chapter("S06_Count"))
            self.code_to(1, rt=0.9)
            seg.wait_until(seg.s(1))
            tasks = [task_in_desk(k) for k in range(TRAYS)]
            self.play(LaggedStart(*[GrowFromCenter(t) for t in tasks], lag_ratio=0.25), run_time=0.8)
            tiles = VGroup(*[slot_tile(t, 0) for t in range(TRAYS)])
            self.play(LaggedStart(*[ReplacementTransform(a, b, path_arc=-0.8) for a, b in zip(tasks, tiles)], lag_ratio=0.15),
                      run_time=1.5)
            self.adopt("tiles", tiles)
            self.show(rt=0.5, ro1=readout([("tasks", f"{N['count']['tasks']}, one per file", PAPER)], top=0.45, size=22))

        with self.nar("s6_2") as seg:
            self.read(rt=2.6)
            seg.wait_until(seg.word(0, "never passes through"))
            self.play(Indicate(self.p["desk"], color=MUTED, scale_factor=1.02), run_time=0.9)

        cards = [count_card(t) for t in range(TRAYS)]
        with self.nar("s6_3", gap=0.8) as seg:
            for t, (word, nth) in enumerate((("twenty-eight", 1), ("twenty-five", 1), ("twenty-five", 2), ("twenty-five", 3))):
                seg.wait_until(seg.word(0, word, nth))
                self.play(FadeIn(cards[t], scale=1.5), run_time=0.4)
            self.p.update(**{f"c{t}": cards[t] for t in range(TRAYS)})
            seg.wait_until(seg.s(1))
            fin = slot_tile(FINAL_BENCH, 1, "+")
            self.play(FadeOut(self.p.pop("tiles")), FadeOut(self.p.pop("trays")), GrowFromCenter(fin), run_time=0.6)
            spot = fin.get_center() + UP * 0.55
            cs = [self.p.pop(f"c{t}") for t in range(TRAYS)]
            self.play(*[c.animate(path_arc=0.6).move_to(spot).scale(0.6).set_opacity(0) for c in cs], run_time=1.2)
            self.remove(*cs)
            total = txt(str(N["count"]["result"]), 30, ACCENT, weight=SEMIBOLD, font=DISPLAY).move_to(spot)
            self.play(FadeIn(total, scale=1.4), run_time=0.5)
            seg.wait_until(seg.word(1, "just one number goes up"))
            up = desk_text(f"count: {N['count']['result']}", ACCENT, 20)
            self.play(ReplacementTransform(total, up, path_arc=-0.5), FadeOut(fin), run_time=1.3)
            self.p["up"] = up
            self.show(rt=0.5, ro2=readout([("per task", " \u00b7 ".join(str(n) for n in SIZES), PAPER),
                                           ("to the driver", "1 number", ACCENT)], top=-0.05, size=22))
        self.drop("tag", "ro1", "ro2", "up")                          # nothing stays in the trays (blueprint M3)


# =================================================================================== S07
class S07_Collect(E01Board):
    def construct(self):
        self.frame(code=(2, 1), files=files(), store=storage_label())
        with self.nar("s7_1") as seg:
            self.show(rt=0.6, tag=chapter("S07_Collect"))
            self.code_to(2, rt=1.0)
            seg.wait_until(seg.s(1))
            self.read(rt=1.4)                                         # collect reads the files again
            flyers = self.p.pop("trays")
            sink = DRIVER_C + DOWN * 0.18
            g1 = driver_gauge(0.2)
            self.play(LaggedStart(*[c.animate(path_arc=0.6).move_to(sink + RIGHT * ((k % 11) - 5) * 0.1).scale(0.35).set_opacity(0)
                                    for k, c in enumerate(flyers)], lag_ratio=lag_for(len(flyers), 0.3)),
                      ReplacementTransform(self.p.pop("gauge"), g1), run_time=2.0)
            self.remove(flyers)
            self.p["gauge"] = g1

        box, head, r1, r2, track = scale_card()
        with self.nar("s7_2") as seg:
            seg.wait_until(seg.word(0, "ten thousand times bigger"))
            self.play(FadeIn(box), FadeIn(head, shift=UP * 0.1), run_time=0.8)
            seg.wait_until(seg.s(1))
            self.play(FadeIn(r1, shift=RIGHT * 0.1), FadeIn(track), FadeIn(small := scale_fill(0.004, PAPER)), run_time=0.8)
            seg.wait_until(seg.word(1, "Collect lands every row"))
            big = scale_fill(1.0, ALERT)
            self.play(FadeIn(r2, shift=RIGHT * 0.1), ReplacementTransform(small, big), run_time=1.6)
            self.p.update(sbox=box, shead=head, sr1=r1, sr2=r2, strack=track, sfill=big)

        with self.nar("s7_3", gap=0.8) as seg:
            self.drop("sbox", "shead", "sr1", "sr2", "strack", "sfill")
            g2 = driver_gauge(1.0, ALERT)
            self.play(ReplacementTransform(self.p.pop("gauge"), g2), Wiggle(self.p["desk"], scale_value=1.05, rotation_angle=0.012 * TAU),
                      run_time=1.1)
            self.p["gauge"] = g2
            seg.wait_until(seg.word(0, "Rows should stay down"))
            self.morph(rt=0.7, gauge=driver_gauge())
            self.play(*[Indicate(b, color=PAPER, scale_factor=1.02) for b in self.p["home"][4:]], run_time=0.9)
        self.drop("tag")


# =================================================================================== S08
class S08_Python(E01Board):
    def construct(self):
        self.frame(code=(3, 2), files=files(), store=storage_label())
        jvm = VGroup(*[badge("JVM", MUTED).move_to([BENCH_XS[t] + 0.72, BIN_Y, 0]) for t in range(TRAYS)])
        with self.nar("s8_1") as seg:
            self.show(rt=0.6, tag=chapter("S08_Python"))
            py = badge("Python", ACCENT).move_to([CODE_C[0] + 0.75, CODE_C[1] + CODE_H / 2 - 0.3, 0])
            self.play(FadeIn(py, scale=1.3), run_time=0.5)
            self.p["py"] = py
            seg.wait_until(seg.word(0, "builds a plan"))
            plan = packet("plan").move_to([WIRE_L + 0.3, WIRE_Y, 0])
            self.play(FadeIn(plan, scale=0.6), run_time=0.3)
            self.play(plan.animate.move_to([WIRE_R - 0.35, WIRE_Y, 0]), run_time=1.1)
            self.play(FadeOut(plan), Indicate(self.p["desk"], color=ACCENT, scale_factor=1.03), run_time=0.5)
            seg.wait_until(seg.s(1))
            self.play(LaggedStart(*[FadeIn(b, scale=1.3) for b in jvm], lag_ratio=0.2), run_time=1.0)
            self.adopt("jvm", jvm)
            seg.wait_until(seg.word(1, "in Java"))
            self.play(*[Indicate(b, color=PAPER, scale_factor=1.25) for b in jvm], run_time=0.9)

        workers = VGroup(*[badge("Python", ACCENT).move_to([BENCH_XS[t] - 0.62, BIN_Y, 0]) for t in range(TRAYS)])
        with self.nar("s8_2") as seg:
            self.code_to(3, rt=1.0)
            self.code_to(4, rt=0.8)
            seg.wait_until(seg.word(0, "starts Python workers"))
            self.play(LaggedStart(*[GrowFromCenter(w) for w in workers], lag_ratio=0.2), run_time=1.0)
            self.adopt("workers", workers)
            seg.wait_until(seg.word(0, "ships the rows out"))
            self.read(rt=1.2)
            batch = [c for t in range(TRAYS) for c in self.p["trays"][sum(SIZES[:t]):sum(SIZES[:t]) + 6]]
            homes = [c.get_center() for c in batch]
            dests = [workers[t].get_center() + RIGHT * ((j % 6) - 2.5) * 0.06 for t in range(TRAYS) for j in range(6)]
            self.play(*[c.animate.move_to(d).scale(0.6) for c, d in zip(batch, dests)], run_time=0.8)
            self.play(*[c.animate.move_to(h).scale(1 / 0.6) for c, h in zip(batch, homes)], run_time=0.8)
            self.show(rt=0.6, plan=udf_plan())
            seg.wait_until(seg.s(1))
            self.play(*[Indicate(w, color=ALERT, scale_factor=1.25) for w in workers],
                      Indicate(self.p["plan"], color=ALERT, scale_factor=1.04), run_time=1.2)
        self.fade_all()


# =================================================================================== S09
class S09_Recap(E01Board):
    def construct(self):
        desk_icon = VGroup(driver_box(), desk_text("plan", ACCENT, 20)).scale(0.62)
        bench_icon = VGroup(bench(0), tray(0, TRAY[0])).scale(0.62)
        stay = VGroup(VGroup(*[chip(CLINIC_COLOR[c], e, r=0.08) for c, e in TRAY[1][:6]]).arrange(RIGHT, buff=0.08),
                      Arrow(UP * 0.55, DOWN * 0.05, buff=0, stroke_width=5, color=ACCENT, max_tip_length_to_length_ratio=0.3))
        stay[1].next_to(stay[0], UP, buff=0.1)
        cards = [recap_card(0, "The driver plans", "and hands out tasks", desk_icon),
                 recap_card(1, "The executors work", "reading the data where it lies", bench_icon),
                 recap_card(2, "Rows stay down", "unless you ask for them", stay)]
        with self.nar("s9_1") as seg:
            self.show(rt=0.7, tag=chapter("S09_Recap"))
            for k in range(3):
                seg.wait_until(seg.s(k + 1))
                self.play(FadeIn(cards[k], shift=UP * 0.3), run_time=0.8)
                self.p[f"r{k}"] = cards[k]

        with self.nar("s9_2") as seg:
            self.show(rt=0.6, r0=None, r1=None, r2=None, tag=None)
            self.frame(code=(5, None), fade=0.8, files=files(), store=storage_label())
            lit = VGroup(*[slot_tile(t, 0) for t in range(TRAYS)])
            dark = VGroup(*[RoundedRectangle(corner_radius=0.05, width=0.3, height=0.3, stroke_color=ALERT, stroke_width=2)
                            .move_to([BENCH_XS[t] + 0.96, NAME_Y, 0]) for t in range(TRAYS)])
            self.play(LaggedStart(*[GrowFromCenter(s) for s in lit], lag_ratio=0.2), run_time=0.8)
            self.play(Create(dark), run_time=0.7)
            self.p.update(lit=lit, dark=dark)
            self.show(rt=0.5, ro=readout([("tasks", str(N["count"]["tasks"]), ACCENT), ("cores", str(N["slots"]), PAPER)], top=-0.2,
                                         size=26))
            seg.wait_until(seg.s(1))
            nxt = txt(f"Next: {NEXT}", 30, PAPER, weight=SEMIBOLD, font=DISPLAY).move_to([CX, 3.5, 0])
            self.play(FadeIn(nxt, shift=UP * 0.15), run_time=0.8)

        with self.nar("s9_3", gap=1.2) as seg:
            self.play(*[FadeOut(m) for m in self.mobjects], run_time=0.8)
            self.p.clear()
            end = end_card(NEXT)
            self.play(Write(end[0]), FadeIn(end[1], shift=UP * 0.1), run_time=1.2)
        self.wait(0.6)
        self.fade_all()
