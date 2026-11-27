"""Spark, Visually: the home shot, drawn on Model in Motion's identity.

Every episode is filmed on one fixed frame (blueprint, "The home shot"): the code panel on the left; the driver
desk, the network band, four executor benches and the storage shelf on the right; nothing below y = -3.0, where
captions are burned in. This module draws that frame and the pieces that live on it: trays of record chips,
core slots, shuffle bins on each bench's local disk, reducer piles, transfer beams and the side readout.

Colours, type, chips and cards come from mim.common, so the two series share one identity. Import this module
only after MIM_EPISODE is set, as mim.common requires.
"""
from __future__ import annotations

import numpy as np
from manim import (DOWN, LEFT, RIGHT, UP, UL, Circle, CubicBezier, DashedLine, DashedVMobject, Ellipse, Line,
                   Rectangle, RoundedRectangle, Triangle, VGroup, VMobject, ITALIC, MEDIUM, NORMAL, SEMIBOLD)
from mim.common import (ACCENT, ALDER, BIRCH, CEDAR, DUNE, ELM, FAINT, INK, INK_2, MUTED, PAPER, DISPLAY,
                        SANS, SERIF, card, chapter_tag, chip, txt)

MONO = "JetBrains Mono"                     # code panel only; the rest of the type is Model in Motion's
CLINIC_COLOR = {"Alder": ALDER, "Birch": BIRCH, "Cedar": CEDAR, "Dune": DUNE, "Elm": ELM}

# ---------------------------------------------------------------- fixed geometry (frame 14.22 x 8 units)
CODE_C, CODE_W, CODE_H = np.array([-4.9, 0.05, 0.0]), 3.9, 5.7
X0, X1 = -2.75, 6.85                        # the cluster's columns
CX = (X0 + X1) / 2
DRIVER_C, DRIVER_W, DRIVER_H = np.array([CX, 2.55, 0.0]), 3.4, 0.95
BAND_Y, BAND_H = 1.62, 0.26
BENCH_Y, BENCH_W, BENCH_H = -0.22, 2.225, 2.75
BENCH_XS = [X0 + BENCH_W / 2 + k * (BENCH_W + (X1 - X0 - 4 * BENCH_W) / 3) for k in range(4)]
SHELF_Y, SHELF_H = -2.25, 0.6
TOP, BOTTOM = BENCH_Y + BENCH_H / 2, BENCH_Y - BENCH_H / 2
BIN_Y, BIN_W, BIN_H = TOP - 0.39, 0.30, 0.50    # the local-disk strip along each bench's top, facing the network
TRAY_Y0, TRAY_DY, TRAY_R = 0.17, 0.235, 0.082   # the partition (or a reducer's pile) in the middle
NAME_Y = BOTTOM + 0.2                           # the bench's name plate and core slots, at the bottom
WALL_Y = (TOP + BAND_Y - BAND_H / 2) / 2         # a stage's wall runs across every bench, just under the network


def tray_pos(i: int, k: int):
    """Where record k of bench i's partition sits: six to a row, in the bench's middle."""
    return np.array([BENCH_XS[i] + (k % 6 - 2.5) * TRAY_DY, TRAY_Y0 - (k // 6) * TRAY_DY, 0])


def pile_pos(x: float, k: int, cols: int = 6, dy: float = 0.11):
    """Where record k of a reducer's pile, centred at x, sits."""
    return np.array([x + (k % cols - (cols - 1) / 2) * dy, TRAY_Y0 - (k // cols) * dy, 0])


def bin_geom(n: int = 5) -> tuple[float, float, int]:
    """Width, pitch and chips per row of a bench's bins when it has n of them (five narrow bins, or three wide)."""
    return (BIN_W, 0.36, 3) if n > 3 else (0.50, 0.56, 5)


def bin_x(bench: int, b: int, n: int = 5) -> float:
    w, pitch, _ = bin_geom(n)
    return BENCH_XS[bench] - 0.70 + w / 2 + b * pitch


def bin_pos(i: int, b: int, k: int, n: int = 5):
    """Where the k-th record in bin b of bench i sits (bins fill from the top, a few to a row)."""
    cols = bin_geom(n)[2]
    return np.array([bin_x(i, b, n) + (k % cols - (cols - 1) / 2) * 0.088, BIN_Y + BIN_H / 2 - 0.07 - (k // cols) * 0.088, 0])


def card_pos(i: int, b: int, k: int, n: int = 5):
    """Where the k-th partial result (a number card) in bin b of bench i sits."""
    return np.array([bin_x(i, b, n), BIN_Y + BIN_H / 2 - 0.1 - k * 0.15, 0])


# ---------------------------------------------------------------- the frame
CODE_TOP, CODE_DY = CODE_C[1] + CODE_H / 2 - 0.78, 0.44     # the first line's row, and the line pitch


def code_box():
    """The code panel's card and its language label."""
    box = card(CODE_W, CODE_H).move_to(CODE_C)
    head = txt("PySpark", 17, MUTED, font=SERIF, slant=ITALIC).move_to(box.get_corner(UL) + np.array([0.62, -0.3, 0]))
    return VGroup(box, head)


def code_lines(lines: list[str], size: int = 15):
    """Every line of the episode's code in the mono face, muted, all at one size (set by the longest line, so it
    never changes between scenes)."""
    widest = max(txt(line, size, font=MONO).width for line in lines)
    fit = min(1.0, (CODE_W - 0.42) / widest)
    out = []
    for k, line in enumerate(lines):
        t = txt(line, size, MUTED, font=MONO).scale(fit)
        out.append(t.move_to([CODE_C[0] - CODE_W / 2 + 0.24 + t.width / 2, CODE_TOP - CODE_DY * k, 0]))
    return out


def code_glow(k: int):
    """The glow behind the active line k, with its bar at the panel's edge."""
    y = CODE_TOP - CODE_DY * k
    glow = Rectangle(width=CODE_W - 0.14, height=0.34, stroke_width=0, fill_color=ACCENT, fill_opacity=0.13).move_to([CODE_C[0], y, 0])
    bar = Rectangle(width=0.05, height=0.34, stroke_width=0, fill_color=ACCENT, fill_opacity=1).move_to(
        [CODE_C[0] - CODE_W / 2 + 0.1, y, 0])
    return VGroup(glow, bar)


def code_panel(lines: list[str], active: int | None = None, shown: int | None = None, size: int = 15):
    """The code panel: lines after `shown` are not yet written; the active line glows."""
    g = code_box()
    shown = len(lines) if shown is None else shown
    for k, t in enumerate(code_lines(lines, size)[:shown]):
        if k == active:
            g.add(code_glow(k))
            t.set_color(PAPER)
        g.add(t)
    return g


def readout(rows: list[tuple], top: float = 0.45, dy: float = 0.5, size: int = 26, stacked: bool = False):
    """The panel's lower half: rows of (label, value, value colour[, label colour]); label in Inter, value in
    Inter Display. Side by side, or stacked (label above a larger value) for long values."""
    g = VGroup()
    y = top
    left = CODE_C[0] - CODE_W / 2 + 0.24
    for row in rows:
        label, value, color = row[:3]
        lab = txt(label, 17, row[3] if len(row) > 3 else MUTED)
        val = txt(value, size, color, weight=SEMIBOLD, font=DISPLAY)
        lab.move_to([left + lab.width / 2, y, 0])
        if stacked:
            val.move_to([left + val.width / 2, y - 0.42, 0])
            y -= 0.42
        else:
            val.move_to([CODE_C[0] + CODE_W / 2 - 0.24 - val.width / 2, y, 0])
        g.add(lab, val)
        y -= dy
    return g


DRIVER_ROW = DRIVER_C[1] + DRIVER_H / 2 - 0.28                # the driver's name row, where its memory gauge sits
GAUGE_X0, GAUGE_W = DRIVER_C[0] + DRIVER_W / 2 - 1.1, 0.9         # the gauge's left end and full width


def driver_box():
    box = card(DRIVER_W, DRIVER_H).move_to(DRIVER_C)
    name = txt("Driver", 22, PAPER, weight=SEMIBOLD, font=DISPLAY)
    name.move_to([box.get_left()[0] + 0.24 + name.width / 2, DRIVER_ROW, 0])
    track = Rectangle(width=GAUGE_W, height=0.09, stroke_width=0, fill_color=FAINT, fill_opacity=1)
    track.move_to([GAUGE_X0 + GAUGE_W / 2, DRIVER_ROW, 0])
    mem = txt("memory", 13, MUTED, font=SERIF, slant=ITALIC).next_to(track, LEFT, buff=0.12)
    return VGroup(box, name, track, mem)


def driver_gauge(fraction: float = 0.155, color=MUTED):
    """How full the driver's memory is."""
    w = max(0.02, GAUGE_W * fraction)
    return Rectangle(width=w, height=0.09, stroke_width=0, fill_color=color, fill_opacity=1).move_to([GAUGE_X0 + w / 2, DRIVER_ROW, 0])


def driver_plan(plan: list[str], marked: tuple = (), dim: tuple = ()):
    """The plan on the driver desk, as a chain of steps. The Exchange step is always set bright; steps in
    `marked` glow in the accent; steps in `dim` (left out of what runs) fade back."""
    steps = VGroup(*[txt(s, 12, FAINT if s in dim else ACCENT if s in marked else PAPER if s == "Exchange" else MUTED,
                         weight=SEMIBOLD if s == "Exchange" or s in marked else NORMAL) for s in plan])
    steps.arrange(RIGHT, buff=0.3).move_to([DRIVER_C[0], DRIVER_C[1] - 0.2, 0])
    arrows = VGroup(*[Line(a.get_right() + RIGHT * 0.07, b.get_left() + LEFT * 0.07, stroke_width=2, color=MUTED)
                      for a, b in zip(steps[:-1], steps[1:])])
    g = VGroup(steps, arrows)
    if g.width > DRIVER_W - 0.3:                     # a long plan shrinks to stay on the desk
        g.scale_to_fit_width(DRIVER_W - 0.3).move_to([DRIVER_C[0], DRIVER_C[1] - 0.2, 0])
    return g


def driver_desk(plan: list[str] | None = None, gauge: float = 0.155, gauge_color=MUTED, marked: tuple = ()):
    """The driver: name, memory gauge and, optionally, the plan."""
    g = VGroup(driver_box(), driver_gauge(gauge, gauge_color))
    if plan:
        g.add(driver_plan(plan, marked))
    return g


def bench(i: int):
    box = card(BENCH_W, BENCH_H).move_to([BENCH_XS[i], BENCH_Y, 0])
    name = txt(f"Executor {i + 1}", 15, MUTED, weight=MEDIUM)
    name.move_to([BENCH_XS[i] - BENCH_W / 2 + 0.2 + name.width / 2, NAME_Y, 0])
    slots = VGroup(*[RoundedRectangle(corner_radius=0.05, width=0.22, height=0.22, stroke_color=FAINT,
                                      stroke_width=2).move_to([BENCH_XS[i] + 0.66 + 0.3 * k, NAME_Y, 0])
                     for k in range(2)])
    return VGroup(box, name, slots)


def home_parts(shelf_note: str | None = "storage") -> dict:
    """The home shot's pieces by name, for an episode that builds the frame piece by piece."""
    band = Rectangle(width=X1 - X0, height=BAND_H, stroke_width=0, fill_color=FAINT, fill_opacity=0.55).move_to([CX, BAND_Y, 0])
    shelf = card(X1 - X0, SHELF_H).move_to([CX, SHELF_Y, 0])
    shelf_lab = (txt(shelf_note, 15, MUTED, font=SERIF, slant=ITALIC).move_to([CX, SHELF_Y, 0]) if shelf_note else VGroup())
    to_driver = Line([CX, DRIVER_C[1] - DRIVER_H / 2, 0], [CX, BAND_Y + BAND_H / 2, 0], stroke_width=2, color=FAINT)
    to_band = VGroup(*[Line([x, BAND_Y - BAND_H / 2, 0], [x, TOP, 0], stroke_width=2, color=FAINT) for x in BENCH_XS])
    to_shelf = VGroup(*[Line([x, SHELF_Y + SHELF_H / 2, 0], [x, BENCH_Y - BENCH_H / 2, 0], stroke_width=2, color=FAINT)
                        for x in BENCH_XS])
    return {"links": VGroup(to_driver, *[VGroup(a, b) for a, b in zip(to_band, to_shelf)]), "to_driver": to_driver,
            "to_band": to_band, "to_shelf": to_shelf, "band": band, "shelf": shelf, "shelf_label": shelf_lab,
            "benches": VGroup(*[bench(i) for i in range(4)])}


def home_shot(shelf_note: str | None = "storage"):
    p = home_parts(shelf_note)
    return VGroup(p["links"], p["band"], p["shelf"], p["shelf_label"], *p["benches"])


def file_card(i: int, label: str):
    """A file on the storage shelf, under bench i: a page with a folded corner and its name."""
    w, h, f = 1.25, 0.38, 0.11
    x, y = BENCH_XS[i], SHELF_Y
    page = VMobject().set_points_as_corners([[x - w / 2, y - h / 2, 0], [x - w / 2, y + h / 2, 0], [x + w / 2 - f, y + h / 2, 0],
                                             [x + w / 2, y + h / 2 - f, 0], [x + w / 2, y - h / 2, 0], [x - w / 2, y - h / 2, 0]])
    page.set_stroke(MUTED, 1.6).set_fill(INK, 1)
    fold = VMobject().set_points_as_corners([[x + w / 2 - f, y + h / 2, 0], [x + w / 2 - f, y + h / 2 - f, 0],
                                             [x + w / 2, y + h / 2 - f, 0]]).set_stroke(MUTED, 1.2)
    return VGroup(page, fold, txt(label, 13, MUTED, font=MONO).move_to([x - 0.05, y, 0]))


def badge(text: str, color=MUTED, size: int = 12):
    """A small outlined tag (JVM, Python, client)."""
    t = txt(text, size, color, weight=SEMIBOLD)
    box = RoundedRectangle(corner_radius=0.06, width=t.width + 0.18, height=t.height + 0.12, stroke_color=color,
                           stroke_width=1.4, fill_color=INK, fill_opacity=1)
    return VGroup(box, t.move_to(box))


def band_tag():
    """The network band's name, drawn last so beams never cover it."""
    t = txt("network", 14, MUTED, font=SERIF, slant=ITALIC)
    bg = RoundedRectangle(corner_radius=0.06, width=t.width + 0.2, height=0.24, stroke_width=0, fill_color=INK_2,
                          fill_opacity=1)
    return VGroup(bg, t).move_to([X0 + 0.5, BAND_Y, 0]).set_z_index(10)


# ---------------------------------------------------------------- data on the frame
def tray(i: int, rows: list[tuple[str, bool]], opacity: float = 1.0, highlight: str | None = None):
    """A partition: one chip per record, six to a row, filled if the event happened (Model in Motion's chip)."""
    g = VGroup()
    for k, (clinic, event) in enumerate(rows):
        c = chip(CLINIC_COLOR[clinic], event, r=TRAY_R)
        c.move_to(tray_pos(i, k))
        o = opacity if highlight is None or clinic == highlight else 0.16
        c.set_stroke(opacity=o)
        c.set_fill(opacity=o if event else 0)
        g.add(c)
    return g


def disk_icon(i: int):
    x, y = BENCH_XS[i] - 0.93, BIN_Y
    top = Ellipse(width=0.2, height=0.07, stroke_color=MUTED, stroke_width=1.6).move_to([x, y + 0.1, 0])
    bot = Ellipse(width=0.2, height=0.07, stroke_color=MUTED, stroke_width=1.6).move_to([x, y - 0.1, 0])
    sides = VGroup(Line([x - 0.1, y + 0.1, 0], [x - 0.1, y - 0.1, 0]), Line([x + 0.1, y + 0.1, 0], [x + 0.1, y - 0.1, 0]))
    sides.set_stroke(MUTED, 1.6)
    return VGroup(top, bot, sides)


def bin_frames(i: int, n: int = 5, dashed: tuple = (), lit: tuple = ()):
    """Bench i's disk with n empty bins; bins in `dashed` are drawn dashed (they stay empty), bins in `lit` glow."""
    g = VGroup(disk_icon(i))
    w = bin_geom(n)[0]
    for b in range(n):
        x = bin_x(i, b, n)
        frame = RoundedRectangle(corner_radius=0.05, width=w, height=BIN_H,
                                 stroke_color=ACCENT if b in lit else FAINT if b in dashed else MUTED,
                                 stroke_width=2.4 if b in lit else 1.6, fill_color=INK, fill_opacity=0.6).move_to([x, BIN_Y, 0])
        g.add(DashedVMobject(frame, num_dashes=18) if b in dashed else frame)
        g.add(txt(str(b), 12, MUTED).move_to([x, BIN_Y - BIN_H / 2 - 0.12, 0]))
    return g


def bin_items(i: int, contents: list[list], mode: str = "chips", only=None):
    """What bench i's bins hold: (clinic, event) records as chips, or (clinic, count) partial results as number
    cards. With `only`, just those clinics, each still in its final place."""
    g = VGroup()
    n = len(contents)
    for b, items in enumerate(contents):
        for k, item in enumerate(items):
            if only is not None and item[0] not in only:
                continue
            if mode == "chips":
                g.add(chip(CLINIC_COLOR[item[0]], item[1], r=0.036).move_to(bin_pos(i, b, k, n)))
            else:
                g.add(number_card(item[0], item[1]).move_to(card_pos(i, b, k, n)))
    return g


def number_card(clinic: str, n: int):
    """A partial result: one clinic's count, in its colour."""
    return txt(str(n), 13, CLINIC_COLOR[clinic], weight=SEMIBOLD, font=DISPLAY)


def bins(i: int, contents: list[list], mode: str = "chips"):
    """Bench i's shuffle bins on its local disk, filled; an empty bin is drawn dashed."""
    return VGroup(bin_frames(i, len(contents), dashed=tuple(b for b, it in enumerate(contents) if not it)),
                  bin_items(i, contents, mode))


def task_bar(i: int, frac: float, color=ACCENT):
    """How far bench i's running task has got: a thin bar above its name plate."""
    w = BENCH_W - 0.4
    y = BOTTOM + 0.45
    track = Rectangle(width=w, height=0.05, stroke_width=0, fill_color=FAINT, fill_opacity=1).move_to([BENCH_XS[i], y, 0])
    fw = max(0.001, w * min(1.0, max(0.0, frac)))
    fill = Rectangle(width=fw, height=0.05, stroke_width=0, fill_color=color, fill_opacity=1)
    return VGroup(track, fill.move_to([BENCH_XS[i] - w / 2 + fw / 2, y, 0]))


def wall(label: str = "wall"):
    """A stage's wall: a dashed line across every bench, just under the network band, with its sign on the right."""
    line = DashedLine([X0 - 0.05, WALL_Y, 0], [X1 + 0.05, WALL_Y, 0], dash_length=0.13, dashed_ratio=0.6,
                      stroke_width=4, color=ACCENT)
    t = txt(label, 14, ACCENT, font=SERIF, slant=ITALIC)
    bg = RoundedRectangle(corner_radius=0.06, width=t.width + 0.22, height=0.26, stroke_width=0, fill_color=INK,
                          fill_opacity=1)
    sign = VGroup(bg, t).move_to([X1 - 0.45, WALL_Y, 0]).set_z_index(3)
    return VGroup(line, sign)


def slot_tile(i: int, k: int, label: str | None = None, color=ACCENT):
    """A task in bench i's core slot k: a filled slot, optionally numbered (a reducer)."""
    sq = RoundedRectangle(corner_radius=0.05, width=0.22, height=0.22, stroke_width=0, fill_color=color, fill_opacity=1)
    g = VGroup(sq.move_to([BENCH_XS[i] + 0.66 + 0.3 * k, NAME_Y, 0]))
    if label is not None:
        g.add(txt(label, 12, INK, weight=SEMIBOLD).move_to(sq))
    return g


def tick(i: int):
    """Bench i's map task is done: a tick beside its name plate."""
    x, y = BENCH_XS[i] + 0.36, NAME_Y
    m = VMobject().set_points_as_corners([[x - 0.09, y, 0], [x - 0.03, y - 0.07, 0], [x + 0.1, y + 0.09, 0]])
    return m.set_stroke(PAPER, 3.2)


def waiting_reducers(n: int, label: str = "reduce tasks, waiting"):
    """Reduce tasks the driver has planned but cannot start yet: numbered ghost tiles beside the driver desk."""
    tiles = VGroup()
    for r in range(n):
        box = RoundedRectangle(corner_radius=0.05, width=0.3, height=0.3, stroke_color=MUTED, stroke_width=1.6)
        tiles.add(VGroup(DashedVMobject(box, num_dashes=12), txt(str(r), 13, MUTED)).move_to([4.25 + 0.42 * r, DRIVER_C[1] - 0.12, 0]))
    cap = txt(label, 13, MUTED, font=SERIF, slant=ITALIC).next_to(tiles, UP, buff=0.13)
    return VGroup(tiles, cap)


# ---------------------------------------------------------------- the task timeline
TL_C, TL_W, TL_H = np.array([CX, -0.25, 0]), X1 - X0 + 0.12, 3.25    # a card over the benches when time matters


def timeline(tasks: list[dict], lanes: int = 8, title: str = "", source: str = "", span: float | None = None) -> dict:
    """A task timeline: a lane per core, a bar per task from its real start to its end (ms), and the bar's fixed-cost
    share (`cap`) as a grey cap at its start. Each task takes the lane that has been free the longest, as a free core
    takes the next task, so the lanes show the rounds the scheduler made. Returns the parts by name:
    card, title, labels, guides, foot, bars {pile: VGroup(bar, cap)}, lane_of {pile: lane}, x (ms -> x), lane_y."""
    box = card(TL_W, TL_H).move_to(TL_C)
    left, right = TL_C[0] - TL_W / 2 + 1.1, TL_C[0] + TL_W / 2 - 0.3
    top, bottom = TL_C[1] + TL_H / 2 - 0.62, TL_C[1] - TL_H / 2 + 0.45
    lane_h = (top - bottom) / lanes
    span = span or max(t["end_ms"] for t in tasks) * 1.04
    x = lambda ms: left + (right - left) * ms / span
    lane_y = lambda k: top - (k + 0.5) * lane_h
    free, lane_of = [0.0] * lanes, {}
    for t in sorted(tasks, key=lambda t: (t["start_ms"], t.get("pile", 0))):
        idle = [k for k in range(lanes) if free[k] <= t["start_ms"] + 1]
        k = min(idle, key=lambda k: (free[k], k)) if idle else min(range(lanes), key=lambda k: (free[k], k))
        lane_of[t.get("pile", len(lane_of))], free[k] = k, t["end_ms"]
    labels = VGroup(*[txt(f"core {k + 1}", 12, MUTED).move_to([box.get_left()[0] + 0.55, lane_y(k), 0]) for k in range(lanes)])
    guides = VGroup(*[Line([left, top - k * lane_h, 0], [right, top - k * lane_h, 0], stroke_width=1, color=FAINT)
                      for k in range(lanes + 1)])
    bars = {}
    for t in tasks:
        pile = t.get("pile", len(bars))
        k = lane_of[pile]
        x0, x1 = x(t["start_ms"]), x(t["end_ms"])
        w = max(0.02, x1 - x0)
        bar = Rectangle(width=w, height=lane_h * 0.62, stroke_width=0, fill_color=PAPER, fill_opacity=0.85)
        bar.move_to([x0 + w / 2, lane_y(k), 0])
        cw = max(0.015, w * t.get("cap", 0))
        cap = Rectangle(width=cw, height=lane_h * 0.62, stroke_width=0, fill_color=MUTED, fill_opacity=1)
        cap.move_to([x0 + cw / 2, lane_y(k), 0])
        bars[pile] = VGroup(bar, cap)
    head = txt(title, 18, PAPER, weight=SEMIBOLD).move_to([left + 0.05, top + 0.3, 0], aligned_edge=LEFT) if title else VGroup()
    foot = VGroup(txt(source, 12, MUTED, font=SERIF, slant=ITALIC).move_to([left, bottom - 0.22, 0], aligned_edge=LEFT),
                  txt("time \u2192", 12, MUTED).move_to([right, bottom - 0.22, 0], aligned_edge=RIGHT))
    return {"card": box, "title": head, "labels": labels, "guides": guides, "foot": foot, "bars": bars,
            "lane_of": lane_of, "x": x, "lane_y": lane_y, "lane_h": lane_h, "span": span}


# ---------------------------------------------------------------- title and end cards
def wordmark(size: int = 64):
    """The series wordmark: Spark in Inter Display, Visually in Lora Italic, the accent."""
    a = txt("Spark,", size, PAPER, weight=SEMIBOLD, font=DISPLAY)
    b = txt("Visually", size, ACCENT, font=SERIF, slant=ITALIC)
    g = VGroup(a, b).arrange(RIGHT, buff=0.22 * size / 64)
    b.align_to(a, DOWN)
    return g


def title_card(arc: str, episode: int, title: str, subtitle: str):
    head = wordmark(64).move_to(UP * 1.15)
    line = txt(f"{arc.upper()}  \u00b7  EPISODE {episode}", 22, MUTED, weight=MEDIUM).next_to(head, DOWN, buff=0.75)
    name = txt(title, 66, PAPER, weight=SEMIBOLD, font=DISPLAY).next_to(line, DOWN, buff=0.32)
    sub = txt(subtitle, 30, MUTED, font=SERIF, slant=ITALIC).next_to(name, DOWN, buff=0.26)
    return head, line, name, sub


def end_card(next_title: str):
    head = wordmark(58).move_to(UP * 0.7)
    nxt = txt(f"Next: {next_title}", 30, MUTED, font=SERIF, slant=ITALIC).next_to(head, DOWN, buff=0.7)
    return VGroup(head, nxt)


def pile(x: float, rows: list[tuple[str, bool]], label: str, count: int, cols: int = 6, r: float = 0.04,
         dy: float = 0.11):
    """What a reducer holds after the fetch: its records gathered in one place, named underneath with its count."""
    g = VGroup()
    if rows:
        for k, (clinic, event) in enumerate(rows):
            g.add(chip(CLINIC_COLOR[clinic], event, r=r).move_to(pile_pos(x, k, cols, dy)))
    else:
        box = RoundedRectangle(corner_radius=0.06, width=cols * dy, height=0.5, stroke_color=FAINT, stroke_width=1.6)
        g.add(DashedVMobject(box.move_to([x, TRAY_Y0 - 0.25, 0]), num_dashes=22))
    ly = TRAY_Y0 - 7 * dy - 0.2
    g.add(txt(label, 12, MUTED).move_to([x, ly, 0]),
          txt(str(count), 17, PAPER if count else MUTED, weight=SEMIBOLD, font=DISPLAY).move_to([x, ly - 0.21, 0]))
    return g


def beam(src_x: float, dst_x: float, weight: float, crossing: bool, lift: float = 0.0, color=None):
    """A transfer: from a bin, up over the network band, down to the receiving bench (crossing), or a short
    drop on the same bench (local). Width grows with the records carried; crossing beams are the accent unless
    `color` says otherwise."""
    color = color or (ACCENT if crossing else MUTED)
    w = 1.2 + 0.2 * weight
    if crossing:
        start, end = np.array([src_x, BIN_Y + BIN_H / 2, 0]), np.array([dst_x, TOP + 0.12, 0])
        hy = BAND_Y + lift
        path = CubicBezier(start, [src_x, hy + 0.25, 0], [dst_x, hy + 0.25, 0], end)
    else:
        start, end = np.array([src_x, BIN_Y - BIN_H / 2 - 0.2, 0]), np.array([dst_x, TRAY_Y0 + 0.1, 0])
        path = CubicBezier(start, start + DOWN * 0.08, end + UP * 0.08, end)
    path.set_stroke(color, w, opacity=0.85)
    tip = Triangle(stroke_width=0, fill_color=color, fill_opacity=0.95).scale(0.06).rotate(np.pi).move_to(end + UP * 0.02)
    return VGroup(path, tip)




# ---------------------------------------------------------------- pieces shared by episodes 3 and on
def files(labels=None):
    """The four files on the storage shelf, labelled file 1 to file 4 unless labels are given."""
    return VGroup(*[file_card(t, labels[t] if labels else f"file {t + 1}") for t in range(4)])


def storage_label():
    return txt("storage", 13, MUTED, font=SERIF, slant=ITALIC).move_to([X0 + 0.42, SHELF_Y + SHELF_H / 2 + 0.16, 0])


def task_tiles(benches, slot: int = 0):
    """One running task on each listed bench, in core slot `slot`."""
    return VGroup(*[slot_tile(t, slot) for t in benches])


def recap_card(k: int, title: str, caption: str, icon):
    """One of a recap's three cards, left to right, with an icon above its title and caption."""
    x = (-4.35, 0.0, 4.35)[k]
    box = card(3.9, 3.5).move_to([x, 0.1, 0])
    icon.move_to([x, 0.8, 0])
    t = txt(title, 24, PAPER, weight=SEMIBOLD, font=DISPLAY).move_to([x, -0.45, 0])
    cap = txt(caption, 16, MUTED, font=SERIF, slant=ITALIC).move_to([x, -1.05, 0])
    return VGroup(box, icon, t, cap)


__all__ = [n for n in dir() if not n.startswith("_")]
