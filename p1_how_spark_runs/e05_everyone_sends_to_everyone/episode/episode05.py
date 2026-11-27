"""Spark, Visually · Part I · Episode 5: Everyone Sends to Everyone (the shuffle).

ANIMATION PASS, cut to the am_fenrir voiceover. The animatic's boards, now in motion: the cast is dealt onto the
benches, every record flies into the bin Spark's hash picks, map tasks run against the wall, every reducer's records
fly across the network into its pile, each clinic's records gather into a partial count, and the counters run.

Render one scene:   manim -qm --media_dir build/media episode/episode05.py S04_Bins      (run from the episode folder)
Each `with self.nar("sX_Y") as seg:` block starts that voiceover clip and holds the scene until it has finished;
motion starts on the spoken cue it illustrates.
Every number, chip and flight comes from e05_numbers.json, through e05_cast.py; none is typed here.
All content stays above y = -3.0 so that burned-in captions never cover it.
Scenes S03 to S08 stand on one frame: each ends on the frame the next one opens with, so the cuts between them don't show.
"""
from __future__ import annotations

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
from e05_cast import (ALL, CODE, COUNT, HASH, HASH_VALUE, HOME, LIST, N, NAMES, PATIENTS, TRAY, TRAYS,  # noqa: E402
                      bin_contents, crossing, partial_cards, reducer_rows)
from sv.board import Board, chapter, fmt, lag_for, note  # noqa: E402

SHELF = "storage · the cast, four files"
NEXT = "Say what, not how"                                    # the next episode's title (blueprint M6)
H5, H3, H200 = ({c: HASH[c][r] for c in NAMES} for r in (5, 3, 200))
EMPTY5 = tuple(b for b in range(5) if b not in H5.values())
EMPTY3 = tuple(b for b in range(3) if b not in H3.values())
assert all(HASH_VALUE[c] % 5 == H5[c] for c in NAMES)         # the remainder on screen is Spark's own bin
DONE = sorted(range(TRAYS), key=lambda t: len(TRAY[t]))       # map tasks finish smallest pile first (illustrative)


# ---------------------------------------------------------------- pieces built from the cast
def trays(opacity: float = 1.0, highlight: str | None = None):
    return VGroup(*[tray(t, TRAY[t], opacity=opacity, highlight=highlight) for t in range(TRAYS)])


def frames(n: int = 5, dashed: tuple = (), lit: tuple = ()):
    return VGroup(*[bin_frames(t, n, dashed, lit) for t in range(TRAYS)])


def items(mode: str = "chips", n: int = 5, only=None):
    src = partial_cards if mode == "cards" else bin_contents
    return VGroup(*[bin_items(t, src(t, n), mode, only) for t in range(TRAYS)])


def _pile_x() -> dict[int, float]:
    out = {}
    for b in range(TRAYS):
        mine = [r for r in range(5) if HOME[r] == b]
        for j, r in enumerate(mine):
            out[r] = BENCH_XS[b] + (0.0 if len(mine) == 1 else (-0.53 if j == 0 else 0.53))
    return out


PILE_X = _pile_x()
_SEQ = iter([-0.06, -0.03, 0.0, 0.03, 0.06, 0.09, -0.09, 0.12, -0.12])
LIFT = {(t, r): next(_SEQ) for t, r, n, cross in crossing()[0] if cross}   # each crossing beam's height over the band


def beams(colored: bool = True):
    """Every fetched bin as a beam; crossing beams turn the accent once the placement is shown."""
    return VGroup(*[beam(bin_x(t, r), PILE_X[r], n, cross, LIFT.get((t, r), 0.0), color=None if colored else MUTED)
                    for t, r, n, cross in crossing()[0]])


def piles():
    """Each reducer's pile, split into its chips and its name plate (an empty pile's dashed box goes with its plate)."""
    got = reducer_rows(5)
    chips_, plates = VGroup(), VGroup()
    for r in range(5):
        p = pile(PILE_X[r], got[r], f"reducer {r}", len(got[r]), cols=6 if len(got[r]) > 30 else 5)
        chips_.add(VGroup(*p[:-2]) if got[r] else VGroup())
        plates.add(VGroup(*p[-2:]) if got[r] else VGroup(*p))
    return chips_, plates


def flight(start, end, lift: float | None):
    """A record's way from its bin to a pile: up over the network band (crossing) or a short drop on its own bench."""
    if lift is None:
        return CubicBezier(start, start + DOWN * 0.12, end + UP * 0.12, end)
    hy = BAND_Y + lift + 0.25
    return CubicBezier(start, np.array([start[0], hy, 0]), np.array([end[0], hy, 0]), end)


def fetch_flights():
    """Every record: its chip in its bin, and the path it flies to its place in its reducer's pile."""
    out = []
    for r in range(5):
        got = [(t, j, c, e) for t in range(TRAYS) for j, (c, e) in enumerate(bin_contents(t)[r])]
        order = sorted(range(len(got)), key=lambda q: NAMES.index(got[q][2]))     # the pile's order (stable)
        cols = 6 if len(got) > 30 else 5
        for pos, q in enumerate(order):
            t, j, c, e = got[q]
            start, end = bin_pos(t, r, j), pile_pos(PILE_X[r], pos, cols)
            out.append((chip(CLINIC_COLOR[c], e, r=0.036).move_to(start), flight(start, end, LIFT.get((t, r)) if HOME[r] != t else None)))
    assert len(out) == PATIENTS
    return out


def sort_moves(n: int = 5, only=None):
    """For every record of the given clinics: a copy of its tray chip, and the chip it becomes in its bin."""
    srcs, dsts = VGroup(), VGroup()
    for t in range(TRAYS):
        fill = [0] * n
        for k, (c, e) in enumerate(TRAY[t]):
            b = HASH[c][n]
            j, fill[b] = fill[b], fill[b] + 1
            if only is None or c in only:
                srcs.add(chip(CLINIC_COLOR[c], e, r=TRAY_R).move_to(tray_pos(t, k)))
                dsts.add(chip(CLINIC_COLOR[c], e, r=0.036).move_to(bin_pos(t, b, j, n)))
    return srcs, dsts


def collapse_groups():
    """Each clinic's records in each bin, grouped, with the count card they gather into."""
    out = []
    for t in range(TRAYS):
        cont, cards = bin_contents(t), partial_cards(t)
        for b in range(5):
            for k, (c, n) in enumerate(cards[b]):
                g = VGroup(*[chip(CLINIC_COLOR[c], e, r=0.036).move_to(bin_pos(t, b, j)) for j, (c2, e) in enumerate(cont[b]) if c2 == c])
                assert len(g) == n
                out.append((g, number_card(c, n).move_to(card_pos(t, b, k))))
    return out


def reducer_slot(r: int):
    """Reducer r running in a core slot of its home bench."""
    mates = [q for q in range(5) if HOME[q] == HOME[r]]
    return slot_tile(HOME[r], mates.index(r), str(r))


def hash_box():
    rows = [txt('hash("Alder")', 15, PAPER, font=MONO),
            txt(f"= {fmt(HASH_VALUE['Alder'])}".replace("-", "\u2212"), 24, PAPER, weight=SEMIBOLD, font=DISPLAY),
            txt("remainder after dividing by 5", 15, MUTED, font=SERIF, slant=ITALIC),
            txt(f"bin {H5['Alder']}", 30, ALDER, weight=SEMIBOLD, font=DISPLAY)]
    return VGroup(*rows).arrange(DOWN, buff=0.22).move_to([CODE_C[0] + 0.25, -0.75, 0])


def bin_rings(b: int, color):
    return VGroup(*[RoundedRectangle(corner_radius=0.06, width=BIN_W + 0.08, height=BIN_H + 0.08, stroke_color=color,
                                     stroke_width=3).move_to([bin_x(t, b), BIN_Y, 0]) for t in range(TRAYS)])


def ledger(label: str, value: str, row: int, top: float = 0.55, size: int = 34):
    """One row of the panel's stacked ledger (row 0 or 1), starting below the last written code line."""
    return readout([(label, value, ACCENT)], top=top - row * (0.42 + 0.62), size=size, stacked=True)


def columns():
    """The three reducers' loads at three reducers: bars stacked by clinic, and their labels. The bars count patients
    as if every row moved (a repartition); a count would hand the reducers partial counts instead."""
    got = reducer_rows(3)
    base, unit = -2.2, 1.9 / max(len(v) for v in got.values())
    bars, labels = VGroup(), VGroup()
    labels.add(txt("patients, if every row moved", 13, MUTED, font=SERIF, slant=ITALIC).move_to([CODE_C[0], base + 1.9 + 0.32, 0]))
    for r in range(3):
        x, col, y = CODE_C[0] - 1.15 + 1.15 * r, VGroup(), base
        for c in NAMES:
            n = sum(1 for q in got[r] if q[0] == c)
            if n:
                col.add(Rectangle(width=0.55, height=n * unit, stroke_width=0, fill_color=CLINIC_COLOR[c],
                                  fill_opacity=0.9).move_to([x, y + n * unit / 2, 0]))
                y += n * unit
        lab = VGroup()
        if not got[r]:
            lab.add(DashedVMobject(Rectangle(width=0.55, height=0.3, stroke_color=FAINT, stroke_width=1.6), num_dashes=14)
                    .move_to([x, base + 0.15, 0]))
            y = base + 0.3
        lab.add(txt(str(len(got[r])), 22, PAPER if got[r] else MUTED, weight=SEMIBOLD, font=DISPLAY).move_to([x, y + 0.22, 0]),
                txt(f"reducer {r}", 13, MUTED).move_to([x, base - 0.2, 0]))
        bars.add(col)
        labels.add(lab)
    return bars, labels


def settings_card():
    """Spark's default: 200 reduce partitions, drawn as 200 slots; the five clinics light five of them."""
    box = card(X1 - X0 + 0.12, 3.25).move_to([CX, -0.25, 0])
    head = txt("spark.sql.shuffle.partitions = 200", 16, PAPER, font=MONO)
    tag = txt("the default", 14, MUTED, font=SERIF, slant=ITALIC)
    VGroup(head, tag).arrange(RIGHT, buff=0.3).move_to([CX, box.get_top()[1] - 0.32, 0])
    lit = {p: c for c, p in H200.items()}
    grid = VGroup()
    for p in range(200):
        row, col = divmod(p, 25)
        sq = RoundedRectangle(corner_radius=0.03, width=0.22, height=0.22, stroke_width=1.2,
                              stroke_color=CLINIC_COLOR[lit[p]] if p in lit else FAINT,
                              fill_color=CLINIC_COLOR[lit[p]] if p in lit else INK, fill_opacity=1 if p in lit else 0)
        grid.add(sq.move_to([CX + (col - 12) * 0.3, 0.7 - row * 0.3, 0]))
    occ = N["occupancy"]["200"]
    cap = txt(f"{occ['used']} of 200 reducers get any rows; {occ['empty']} read nothing", 14, MUTED, font=SERIF,
              slant=ITALIC).move_to([CX, -1.68, 0])
    return VGroup(box, head, tag), grid, cap


def coalesced():
    k = N["count_r200_coalesced_reduce_tasks"]
    tile = RoundedRectangle(corner_radius=0.08, width=2.2, height=0.62, stroke_color=ACCENT, stroke_width=2.4,
                            fill_color=INK, fill_opacity=1).move_to([CX, -0.35, 0])
    t = txt(f"{k} reduce task", 24, PAPER, weight=SEMIBOLD, font=DISPLAY).move_to(tile)
    cap = txt("adaptive execution merges the tiny bins into one task (switched off in this episode)", 14, MUTED, font=SERIF,
              slant=ITALIC).move_to([CX, -1.68, 0])
    return tile, t, cap


def recap_card(k: int, title: str, caption: list[str], icon):
    x = (-4.35, 0.0, 4.35)[k]
    box = card(3.9, 3.5).move_to([x, 0.1, 0])
    icon.move_to([x, 0.85, 0])
    t = txt(title, 26, PAPER, weight=SEMIBOLD, font=DISPLAY).move_to([x, -0.35, 0])
    cap = VGroup(*[txt(s, 17, MUTED, font=SERIF, slant=ITALIC) for s in caption]).arrange(DOWN, buff=0.1).move_to([x, -1.0, 0])
    return VGroup(box, icon, t, cap)


def recap_icons():
    disk = VGroup(bin_frames(0, 5, dashed=EMPTY5), bin_items(0, bin_contents(0))).scale(1.7)
    line = DashedLine([-1.2, 0, 0], [1.2, 0, 0], dash_length=0.13, dashed_ratio=0.6, stroke_width=4, color=ACCENT)
    ups = VGroup(*[Arrow([x, -0.45, 0], [x, 0.45, 0], buff=0, stroke_width=4, color=MUTED, max_tip_length_to_length_ratio=0.2)
                   for x in (-0.7, 0.0, 0.7)])
    alder = VGroup(*[txt(str(N["trays"][str(t)]["Alder"]), 34, ALDER, weight=SEMIBOLD, font=DISPLAY) for t in range(TRAYS)])
    return disk, VGroup(line, ups), alder.arrange(RIGHT, buff=0.4)


def hash_map():
    ys = [1.5 - 0.6 * k for k in range(5)]
    names = VGroup(*[txt(c, 26, CLINIC_COLOR[c], weight=SEMIBOLD, font=DISPLAY).move_to([-3.4, ys[k], 0])
                     for k, c in enumerate(NAMES)])
    boxes, extra = VGroup(), VGroup()
    for b in range(5):
        fr = RoundedRectangle(corner_radius=0.08, width=1.2, height=0.48, stroke_color=FAINT if b in EMPTY5 else MUTED,
                              stroke_width=2).move_to([1.6, ys[b], 0])
        boxes.add(VGroup(DashedVMobject(fr, num_dashes=24) if b in EMPTY5 else fr,
                         txt(f"bin {b}", 18, PAPER if b not in EMPTY5 else MUTED).move_to(fr)))
        if b in EMPTY5:
            extra.add(txt("idle", 16, MUTED, font=SERIF, slant=ITALIC).next_to(fr, RIGHT, buff=0.25))
    arrows = VGroup(*[Arrow(names[k].get_right() + RIGHT * 0.25, boxes[H5[c]][0].get_left() + LEFT * 0.1, buff=0, stroke_width=3,
                            color=CLINIC_COLOR[c], max_tip_length_to_length_ratio=0.05) for k, c in enumerate(NAMES)])
    occ = N["occupancy"]["5"]
    head = txt(f"{len(NAMES)} clinics, {occ['used']} bins, {occ['empty']} idle reducers", 28, PAPER, weight=SEMIBOLD,
               font=DISPLAY).move_to([-0.6, 2.55, 0])
    return VGroup(head, names, arrows, boxes, extra)


def plan_card():
    """The count's physical plan as Spark printed it, with the choice it made that our code never asked for."""
    plan = N["plans"]["count"]
    tree = plan.split("\n")[1:6]
    hp = re.search(r"hashpartitioning\([^)]*\)", plan).group(0)
    box = card(7.6, 4.1).move_to([2.75, 0.55, 0])
    lines, y = VGroup(), 2.15
    mark = None
    for line in tree:
        depth = (len(line) - len(line.lstrip())) // 3
        s = line.strip()
        is_x = "Exchange" in s
        t = txt(s, 17, PAPER if is_x else MUTED, font=MONO)
        lines.add(t.move_to([-0.7 + 0.32 * depth + t.width / 2, y, 0]))
        y -= 0.5
        if is_x:
            a = txt(hp, 17, ACCENT, font=MONO)
            lines.add(a.move_to([-0.7 + 0.32 * (depth + 1) + 0.38 + a.width / 2, y, 0]))
            mark = txt("?", 54, ACCENT, weight=SEMIBOLD, font=DISPLAY).next_to(a, RIGHT, buff=0.35)
            y -= 0.5
    return box, lines, mark


# ---------------------------------------------------------------- a scene that keeps its pieces by name
class E05Board(Board):
    PANEL_NOTE = "adaptive merging off, to show every bin"
    CODE, SHELF = CODE, SHELF

    def sort(self, key: str, n: int = 5, only=None, rt: float = 1.4, **extra):
        """Copies of the trays' chips fly up into the bins Spark's hash picks; extra pieces fade in alongside."""
        srcs, dsts = sort_moves(n, only)
        self.fly(key, zip(srcs, dsts), rt=rt, share=0.35, arc=-0.7, **extra)


# =================================================================================== S01
class S01_ColdOpen(E05Board):
    def construct(self):
        q = txt("How many patients does each clinic have?", 30, PAPER, weight=SEMIBOLD, font=DISPLAY).move_to([CX, 3.55, 0])
        sizes = VGroup(*[txt(f"{len(TRAY[t])} rows", 15, MUTED).move_to([BENCH_XS[t] + 0.62, BIN_Y, 0]) for t in range(TRAYS)])
        alder = [txt(str(N["trays"][str(t)]["Alder"]), 30, ALDER, weight=SEMIBOLD, font=DISPLAY).move_to([BENCH_XS[t] + 0.75, BIN_Y, 0])
                 for t in range(TRAYS)]
        T = trays()
        with self.nar("s1_1") as seg:
            self.frame(fade=1.0)
            self.play(FadeIn(q, shift=DOWN * 0.15), run_time=0.8)
            self.p["q"] = q
            seg.wait_until(seg.s(1))
            dealt = [c for tr in T for c in tr]
            self.play(LaggedStart(*[GrowFromCenter(c) for c in dealt], lag_ratio=lag_for(len(dealt), 0.15)), run_time=2.0)
            self.adopt("trays", T)
            self.show(rt=0.6, sizes=sizes)

        with self.nar("s1_2") as seg:
            self.morph(rt=0.9, trays=trays(highlight="Alder"))
            self.show(rt=0.5, sizes=None, alder_tag=txt("Alder", 18, ALDER, font=SERIF, slant=ITALIC).move_to(
                [BENCH_XS[0] + 0.15, BIN_Y - 0.04, 0]))
            for cue, ts in (("nine patients", (0,)), ("five on this one", (1,)), ("eight on each", (2, 3))):
                seg.wait_until(seg.word(0, cue))
                self.play(*[FadeIn(alder[t], scale=1.6) for t in ts], run_time=0.5)
                for t in ts:
                    self.p[f"a{t}"] = alder[t]
            seg.wait_until(seg.word(1, "have to meet"))
            self.play(*[Indicate(alder[t], color=PAPER, scale_factor=1.25) for t in range(TRAYS)], run_time=0.9)

        with self.nar("s1_3", gap=0.8) as seg:
            self.show(rt=0.5, alder_tag=None, **{f"a{t}": None for t in range(TRAYS)})
            self.morph(rt=0.6, trays=trays())
            flyers = VGroup(*[c.copy() for tr in self.p["trays"] for c in tr])
            self.add(flyers)
            sink = DRIVER_C + DOWN * 0.18
            g1 = driver_gauge(0.45)
            self.play(LaggedStart(*[c.animate(path_arc=0.6).move_to(sink + RIGHT * ((k % 11) - 5) * 0.1).scale(0.35).set_opacity(0)
                                    for k, c in enumerate(flyers)], lag_ratio=lag_for(len(flyers), 0.3)),
                      ReplacementTransform(self.p.pop("gauge"), g1), run_time=2.2)
            self.p["gauge"] = g1
            self.remove(flyers)
            seg.wait_until(seg.word(0, "For a whole registry"))
            g2 = driver_gauge(1.0, ALERT)
            self.play(ReplacementTransform(self.p.pop("gauge"), g2), Wiggle(self.p["desk"], scale_value=1.05, rotation_angle=0.012 * TAU),
                      run_time=1.1)
            self.p["gauge"] = g2
            seg.wait_until(seg.s(1))
            self.morph(rt=0.6, gauge=driver_gauge())
            seg.wait_until(seg.word(1, "it moves rows sideways"))
            big = sorted([o for o in crossing()[0] if o[3]], key=lambda o: -o[2])[:3]   # three real crossings, previewed
            side = VGroup(*[beam(bin_x(t, r), PILE_X[r], n, True, LIFT[(t, r)]) for t, r, n, _ in big])
            packets = [VGroup(*[chip(CLINIC_COLOR[c], e, r=0.05) for c, e in bin_contents(t)[r][:4]]).arrange(RIGHT, buff=0.03)
                       .move_to(b[0].get_start()) for (t, r, n, _), b in zip(big, side)]
            self.play(LaggedStart(*[Create(b) for b in side], lag_ratio=0.25), run_time=0.8)
            self.play(*[MoveAlongPath(p, b[0]) for p, b in zip(packets, side)], run_time=1.2)
        self.fade_all()


# =================================================================================== S02
class S02_Title(E05Board):
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
class S03_Wall(E05Board):
    def construct(self):
        with self.nar("s3_1") as seg:
            self.frame(fade=1.0, trays=trays(), tag=chapter("S03_Wall"))
            plan = driver_plan(["scan", "partial count", "Exchange", "final count"])
            steps, arrows = plan
            self.play(LaggedStart(*[FadeIn(s, shift=RIGHT * 0.1) for s in steps], lag_ratio=0.4),
                      LaggedStart(*[Create(a) for a in arrows], lag_ratio=0.4), run_time=1.8)
            self.adopt("plan", plan)
            seg.wait_until(seg.s(1))
            w = wall()
            self.play(LaggedStart(*[Create(d) for d in w[0]], lag_ratio=lag_for(len(w[0]), 0.2)), run_time=1.2)
            self.play(FadeIn(w[1], scale=1.3), run_time=0.4)
            self.adopt("wall", w)
            seg.wait_until(seg.word(1, "until all the work"))
            self.play(Indicate(self.p["trays"], color=PAPER, scale_factor=1.02), run_time=1.0)
        with self.nar("s3_2", gap=0.8) as seg:
            self.play(Indicate(self.p["wall"], color=ACCENT, scale_factor=1.03), run_time=1.4)
        self.drop("wall", "tag", "plan")                       # S04 opens on the bare frame


# =================================================================================== S04
class S04_Bins(E05Board):
    def construct(self):
        self.frame(trays=trays())
        with self.nar("s4_1") as seg:
            self.show(rt=0.6, tag=chapter("S04_Bins"))
            self.code_to(0, rt=1.3)
            seg.wait_until(seg.word(0, "five reduce tasks"))
            gh = waiting_reducers(5)
            self.play(LaggedStart(*[GrowFromCenter(t) for t in gh[0]], lag_ratio=0.3), FadeIn(gh[1]), run_time=1.2)
            self.adopt("ghosts", gh)
            seg.wait_until(seg.s(1))
            F = frames()
            self.play(LaggedStart(*[FadeIn(f, shift=DOWN * 0.25) for f in F], lag_ratio=0.25), run_time=1.3)
            self.adopt("frames", F)

        k0 = next(k for k, (c, _) in enumerate(TRAY[0]) if c == "Alder")
        hb = hash_box()
        with self.nar("s4_2") as seg:
            self.code_to(1, rt=1.0)
            seg.wait_until(seg.word(0, "turns the clinic's name"))
            demo = chip(ALDER, TRAY[0][k0][1], r=TRAY_R).move_to(tray_pos(0, k0))
            self.add(demo)
            self.play(demo.animate(path_arc=0.9).move_to(hb[0].get_left() + LEFT * 0.35).scale(1.5), FadeIn(hb[0]), run_time=1.1)
            seg.wait_until(seg.word(0, "called a hash"))
            self.play(FadeIn(hb[1], shift=DOWN * 0.1), run_time=0.6)
            seg.wait_until(seg.word(0, "keeps the positive remainder"))
            self.play(FadeIn(hb[2]), run_time=0.5)
            self.play(FadeIn(hb[3], scale=1.5), run_time=0.6)
            self.adopt("hashbox", hb)
            seg.wait_until(seg.s(1))
            self.play(demo.animate(path_arc=-0.9).move_to(tray_pos(0, k0)).scale(1 / 1.5), run_time=0.9)
            self.remove(demo)
            self.morph(rt=0.7, trays=trays(highlight="Alder"))
            self.show(rt=0.6, rings=bin_rings(H5["Alder"], ALDER))

        order = sorted(NAMES, key=lambda c: (H5[c], NAMES.index(c)))
        table = readout([(c, f"bin {H5[c]}", PAPER, CLINIC_COLOR[c]) for c in order], top=0.45, dy=0.42, size=24)
        rows = {c: table[2 * k:2 * k + 2] for k, c in enumerate(order)}
        first = [c for c in order if H5[c] == H5["Birch"]], [c for c in order if H5[c] == H5["Alder"]]
        rest = [c for c in order if c not in first[0] + first[1]]
        with self.nar("s4_3") as seg:
            self.show(rt=0.5, hashbox=None, rings=None)
            self.morph(rt=0.6, trays=trays(opacity=0.16))
            self.show(rt=0.4, head=txt("pmod(hash(clinic), 5)", 15, PAPER, font=MONO).move_to([CODE_C[0], 0.95, 0]))
            for cue, group in (("Birch, bin zero", first[0]), ("Alder, bin one", first[1])):
                seg.wait_until(seg.word(0, cue))
                self.sort(f"chips_{group[0]}", only=set(group), rt=1.0, **{f"row_{c}": rows[c] for c in group})
            seg.wait_until(seg.s(1))
            self.sort("chips_rest", only=set(rest), rt=1.5, **{f"row_{c}": rows[c] for c in rest})
            seg.wait_until(seg.word(1, "Two bins get nothing"))
            self.morph(rt=0.7, same=False, frames=frames(dashed=EMPTY5))
            self.show(rt=0.5, empty=note(f"bins {EMPTY5[0]} and {EMPTY5[1]} stay empty", y=-1.75))
            seg.wait_until(seg.s(2))                                      # the crowded bin, and the empty ones
            flash = VGroup(bin_rings(H5["Cedar"], ACCENT), *[bin_rings(b, MUTED) for b in EMPTY5])
            self.play(FadeIn(flash), run_time=0.4)
            self.play(FadeOut(flash), run_time=0.8)

        with self.nar("s4_4", gap=0.8) as seg:
            self.play(*[Indicate(f[0], color=ACCENT, scale_factor=1.4) for f in self.p["frames"]], run_time=1.2)
            seg.wait_until(seg.s(1))
            band = Rectangle(width=X1 - X0, height=BAND_H, stroke_color=ACCENT, stroke_width=2).move_to([CX, BAND_Y, 0])
            self.show(rt=0.6, netring=band, netnote=txt("nothing on the network yet", 14, MUTED, font=SERIF, slant=ITALIC)
                      .move_to([X1 - 1.15, BAND_Y + 0.3, 0]))
        self.drop("tag", "head", "empty", "netring", "netnote", *[f"row_{c}" for c in NAMES])


# =================================================================================== S05
class S05_Wait(E05Board):
    def construct(self):
        self.frame(code=(2, 1), trays=trays(opacity=0.16), frames=frames(dashed=EMPTY5), chips=items(),
                   ghosts=waiting_reducers(5))
        V = [ValueTracker(0.0) for _ in range(TRAYS)]
        bars = VGroup(*[always_redraw(lambda t=t: task_bar(t, V[t].get_value())) for t in range(TRAYS)])
        with self.nar("s5_1") as seg:
            self.show(rt=0.6, tag=chapter("S05_Wait"))
            w = wall()
            self.play(LaggedStart(*[Create(d) for d in w[0]], lag_ratio=lag_for(len(w[0]), 0.2)), run_time=1.0)
            self.play(FadeIn(w[1], scale=1.3), run_time=0.3)
            self.adopt("wall", w)
            seg.wait_until(seg.word(0, "as soon as one pile"))
            self.show(rt=0.4, bars=bars, **{f"run{t}": slot_tile(t, 0) for t in range(TRAYS)})
            start = self.nar.now()                                        # every map task is sorting its pile
            ends = {DONE[j]: seg.t0 + seg.s(1) + 1.6 + 1.5 * j for j in range(3)}
            ends[DONE[3]] = seg.t0 + seg.dur + seg.gap + 1.3              # the slowest finishes in the next clip

            def frac(t, at):
                return min(1.0, (at - start) / (ends[t] - start))

            for j in range(3):
                b = DONE[j]
                dt = ends[b] - self.nar.now()
                if dt > 1 / 30:
                    self.play(*[V[t].animate.set_value(frac(t, ends[b])) for t in range(TRAYS)], run_time=dt, rate_func=linear)
                tk, at = tick(b), self.nar.now() + 0.3
                self.play(FadeOut(self.p.pop(f"run{b}")), GrowFromCenter(tk),
                          *[V[t].animate.set_value(frac(t, at)) for t in range(TRAYS)], run_time=0.3, rate_func=linear)
                self.p[f"tick{b}"] = tk
            last = DONE[3]
            dt = seg.t0 + seg.dur + seg.gap - self.nar.now()
            if dt > 1 / 30:
                self.play(V[last].animate.set_value(frac(last, self.nar.now() + dt)), run_time=dt, rate_func=linear)

        with self.nar("s5_2", gap=0.8) as seg:
            dt = ends[last] - self.nar.now()
            if dt > 1 / 30:
                self.play(V[last].animate.set_value(1.0), run_time=dt, rate_func=linear)
            tk = tick(last)
            self.play(FadeOut(self.p.pop(f"run{last}")), GrowFromCenter(tk), run_time=0.3)
            self.p[f"tick{last}"] = tk
            wl = self.p.pop("wall")                                       # the wall opens
            left = VGroup(*[d for d in wl[0] if d.get_center()[0] < CX])
            right = VGroup(*[d for d in wl[0] if d.get_center()[0] >= CX])
            self.play(left.animate.shift(LEFT * 1.4).set_opacity(0), right.animate.shift(RIGHT * 1.4).set_opacity(0),
                      FadeOut(wl[1]), run_time=1.0)
            self.remove(wl, left, right)
            seg.wait_until(seg.word(0, "Remember that wait"))
            self.show(rt=0.7, slow=txt("the fastest wait for the slowest", 15, MUTED, font=SERIF, slant=ITALIC)
                      .move_to([CX, WALL_Y, 0]))
        for b in bars:
            b.clear_updaters()
        self.drop("tag", "slow", "bars", *[f"tick{t}" for t in range(TRAYS)])


# =================================================================================== S06
class S06_Fetch(E05Board):
    def construct(self):
        self.frame(code=(2, 1), trays=trays(opacity=0.16), frames=frames(dashed=EMPTY5), chips=items(),
                   ghosts=waiting_reducers(5))
        _, bins_x, rows_x = crossing()
        pchips, plates = piles()
        with self.nar("s6_1") as seg:
            self.show(rt=0.6, tag=chapter("S06_Fetch"))
            gh = self.p.pop("ghosts")                                     # the reduce tasks leave the driver's queue
            self.remove(gh)
            self.add(*gh[0], gh[1])
            tiles = VGroup(*[reducer_slot(r) for r in range(5)])
            self.play(LaggedStart(*[ReplacementTransform(gh[0][r], tiles[r], path_arc=-1.0) for r in range(5)], lag_ratio=0.15),
                      FadeOut(gh[1]), FadeOut(self.p.pop("trays")), run_time=1.5)
            self.adopt("tiles", tiles)
            seg.wait_until(seg.word(0, "fetches its bin"))
            B = beams(colored=False)
            self.play(LaggedStart(*[Create(b) for b in B], lag_ratio=lag_for(len(B), 0.3)), run_time=1.6)
            self.adopt("beams", B)
            seg.wait_until(seg.s(1))
            self.show(rt=0.7, ro1=readout([("possible bins", str(ALL["blocks_possible"]), PAPER),
                                           ("bins holding rows", str(ALL["blocks_nonempty"]), PAPER)], top=0.45, size=24))
            seg.wait_until(seg.word(1, "only those"))                    # empty bins are never asked for
            idle = VGroup(*[txt("nothing to fetch", 11, MUTED, font=SERIF, slant=ITALIC).next_to(tiles[r], UP, buff=0.08)
                            for r in EMPTY5])
            self.show(rt=0.5, idle=idle)

        flights = fetch_flights()
        with self.nar("s6_2") as seg:
            self.morph(rt=0.8, beams=beams())
            seg.wait_until(seg.word(0, "nine of those bins"))
            self.show(rt=0.6, ro2=readout([("cross the network", str(bins_x), ACCENT),
                                           ("patients moving", f"{rows_x} of {PATIENTS}", ACCENT)], top=-0.55, size=24),
                      placed=note("reducers as placed on the home shot"))
            seg.wait_until(seg.word(0, "seventy-nine"))
            self.remove(self.p.pop("chips"))                              # every bin's records take off
            fly = [c for c, _ in flights]
            self.add(*fly)
            self.play(LaggedStart(*[MoveAlongPath(c, path) for c, path in flights], lag_ratio=lag_for(len(flights), 0.45)),
                      run_time=2.8)
            self.remove(*fly)
            self.put(pchips=pchips)

        counts = readout([(c, str(N["cast"]["per_clinic"][c]), PAPER, CLINIC_COLOR[c]) for c in NAMES], top=0.45, dy=0.42, size=24)
        with self.nar("s6_3", gap=0.8) as seg:
            self.show(rt=0.6, ro1=None, ro2=None, placed=None, beams=None, idle=None)
            self.show(rt=0.8, plates=plates, counts=counts)
            for cue, clinic in (("Birch's thirty", "Birch"), ("Alder's thirty", "Alder"), ("forty-three", "Elm")):
                seg.wait_until(seg.word(0, cue))
                self.play(Indicate(pchips[H5[clinic]], color=PAPER, scale_factor=1.12), run_time=0.8)
            seg.wait_until(seg.s(1))
            self.play(*[Indicate(plates[b], color=MUTED) for b in EMPTY5], run_time=1.2)
        self.drop("tag", "counts", "pchips", "plates", "tiles")
        self.show(rt=0.7, trays=trays(opacity=0.16), chips=items())     # S07 rewinds to the map side


# =================================================================================== S07
class S07_SendLess(E05Board):
    def construct(self):
        groups = collapse_groups()
        self.frame(code=(2, 1), trays=trays(opacity=0.16), frames=frames(dashed=EMPTY5),
                   **{f"cg{k}": g for k, (g, _) in enumerate(groups)})
        with self.nar("s7_1") as seg:
            self.show(rt=0.6, tag=chapter("S07_SendLess"))
            self.code_to(2, rt=1.1)
            seg.wait_until(seg.s(1))                                      # each clinic's records gather into a count
            cards = VGroup(*[c for _, c in groups])
            self.play(LaggedStart(*[ReplacementTransform(self.p.pop(f"cg{k}"), c) for k, (_, c) in enumerate(groups)],
                                  lag_ratio=lag_for(len(groups), 0.4)), run_time=1.8)
            self.adopt("cards", cards)

        with self.nar("s7_2") as seg:
            PA = N["partial_aggregation"]                                 # the same count, without and with partial counts
            a, b = PA["cast"]["without"]["records"], PA["cast"]["with"]["records"]
            self.count_row("led0", "rows crossing the wall", a, b, 0, rt=1.6, final=f"{a} \u2192 {b}")
            seg.wait_until(seg.s(1))
            a, b = PA["cast"]["without"]["bytes"], PA["cast"]["with"]["bytes"]
            self.count_row("led1", "bytes written", a, b, 1, rt=1.6, final=f"{fmt(a)} \u2192 {fmt(b)}")
            seg.wait_until(seg.word(1, "fixed overhead"))
            self.show(rt=0.6, overhead=note("every bin carries a fixed overhead", y=-1.75))
            seg.wait_until(seg.s(2))
            a, b = PA["scaled"]["without"]["bytes"], PA["scaled"]["with"]["bytes"]
            self.show(rt=0.7, overhead=None, led2=ledger(f"bytes, cast \u00d7 {fmt(PA['scale'])}", f"{fmt(a)} \u2192 {fmt(b)}", 2, size=26))

        steps = ["partial count", "Exchange", "final count"]
        with self.nar("s7_3") as seg:
            self.show(rt=0.9, plan=driver_plan(steps))
            seg.wait_until(seg.word(0, "a partial count"))
            self.morph(rt=0.6, plan=driver_plan(steps, marked=("partial count",)))
            seg.wait_until(seg.word(0, "a final count"))
            self.morph(rt=0.6, plan=driver_plan(steps, marked=("partial count", "final count")))

        with self.nar("s7_4", gap=0.8) as seg:
            self.show(rt=0.6, led0=None, led1=None, led2=None, overhead=None, plan=None)
            self.code_to(3, rt=1.3)
            seg.wait_until(seg.word(0, "every value still has to travel"))
            back = [g for g, _ in collapse_groups()]                      # the counts open back into every value
            cards = self.p.pop("cards")
            self.remove(cards)
            self.add(*cards)
            self.play(LaggedStart(*[ReplacementTransform(c, g) for c, g in zip(cards, back)], lag_ratio=lag_for(len(back), 0.4)),
                      run_time=1.5)
            self.adopt("chips", VGroup(*back))
            seg.wait_until(seg.s(1))
            top = CODE_TOP - CODE_DY * 3 - 0.62                           # under the fourth line, now written
            self.show(rt=0.6, led0=ledger("rows crossing the wall", str(LIST["total_records_written"]), 0, top))
            self.count_row("led1", "bytes written", COUNT["total_bytes_written"], LIST["total_bytes_written"], 1, top=top, rt=1.6)
        self.drop("tag", "led0", "led1")


# =================================================================================== S08
class S08_Collide(E05Board):
    def construct(self):
        self.frame(code=(4, 3), trays=trays(opacity=0.16), frames=frames(dashed=EMPTY5), chips=items())
        crowd, pair = ({c for c in NAMES if H3[c] == b} for b in sorted({H3[c] for c in NAMES},
                                                                        key=lambda b: -sum(1 for c in NAMES if H3[c] == b)))
        with self.nar("s8_1") as seg:
            self.show(rt=0.6, tag=chapter("S08_Collide"))
            self.code_to(4, rt=1.2)
            self.show(rt=0.5, chips=None)
            self.morph(rt=0.8, same=False, frames=frames(n=3))
            seg.wait_until(seg.s(1))
            self.sort("crowd", n=3, only=crowd, rt=1.6)
            seg.wait_until(seg.word(1, "Cedar and Dune share"))
            self.sort("pair", n=3, only=pair, rt=1.3)
            seg.wait_until(seg.word(1, "The third gets nothing"))
            self.morph(rt=0.6, same=False, frames=frames(n=3, dashed=EMPTY3))

        bars, labels = columns()
        with self.nar("s8_2") as seg:
            self.play(LaggedStart(*[GrowFromEdge(s, DOWN) for col in bars for s in col], lag_ratio=0.35), run_time=1.3)
            self.play(FadeIn(labels, shift=UP * 0.1), run_time=0.4)
            self.adopt("cols", VGroup(bars, labels))
            seg.wait_until(seg.word(0, "On real data"))
            busiest = max(range(3), key=lambda r: len(reducer_rows(3)[r]))
            self.play(Indicate(bars[busiest], color=ACCENT, scale_factor=1.08), run_time=1.2)

        box, grid, cap = settings_card()
        tile, word, cap2 = coalesced()
        with self.nar("s8_3", gap=0.8) as seg:
            self.play(FadeIn(box), run_time=0.5)
            self.play(LaggedStart(*[FadeIn(sq, scale=0.5) for sq in grid], lag_ratio=lag_for(len(grid), 0.15)), run_time=1.6)
            lit = [grid[p] for p in H200.values()]
            self.play(*[Indicate(m, scale_factor=1.8, color=PAPER) for m in lit], FadeIn(cap), run_time=1.0)
            self.adopt("grid", grid)
            seg.wait_until(seg.s(1))                                      # adaptive execution merges the empty bins
            self.play(ReplacementTransform(self.p.pop("grid"), tile), FadeTransform(cap, cap2), run_time=1.4)
            self.play(FadeIn(word, scale=1.2), run_time=0.5)
        self.fade_all()


# =================================================================================== S09
class S09_Recap(E05Board):
    def construct(self):
        disk, wall_icon, cards_icon = recap_icons()
        cards = [recap_card(0, "Into bins", ["every pile sorted by key,", "written to local disk"], disk),
                 recap_card(1, "A wall, then a fetch", ["each reducer fetches its bin", "from every machine"], wall_icon),
                 recap_card(2, "Send less", ["partial results", "instead of every row"], cards_icon)]
        with self.nar("s9_1") as seg:
            self.show(rt=0.7, tag=chapter("S09_Recap"))
            for k in range(3):
                seg.wait_until(seg.s(k + 1))
                self.play(FadeIn(cards[k], shift=UP * 0.3), run_time=0.8)
                self.p[f"r{k}"] = cards[k]

        hm = hash_map()
        head, names, arrows, boxes, extra = hm
        with self.nar("s9_2") as seg:
            self.show(rt=0.6, r0=None, r1=None, r2=None)
            self.play(FadeIn(head), LaggedStart(*[FadeIn(n, shift=RIGHT * 0.1) for n in names], lag_ratio=0.15), FadeIn(boxes),
                      run_time=1.0)
            self.play(LaggedStart(*[GrowArrow(a) for a in arrows], lag_ratio=0.2), run_time=1.2)
            self.play(FadeIn(extra), run_time=0.5)
            self.adopt("hmap", hm)

        pbox, plines, mark = plan_card()
        with self.nar("s9_3") as seg:
            self.show(rt=0.6, hmap=None, tag=None)
            self.play(FadeIn(code_panel(CODE)), FadeIn(pbox), run_time=0.6)
            self.play(LaggedStart(*[FadeIn(t, shift=RIGHT * 0.1) for t in plines], lag_ratio=0.3), run_time=1.6)
            self.play(GrowFromCenter(mark), run_time=0.5)
            seg.wait_until(seg.s(1))
            nxt = txt(f"Next: {NEXT}", 30, PAPER, weight=SEMIBOLD, font=DISPLAY).move_to([2.75, -2.15, 0])
            self.play(FadeIn(nxt, shift=UP * 0.15), run_time=0.8)

        with self.nar("s9_4", gap=1.2) as seg:
            self.play(*[FadeOut(m) for m in self.mobjects], run_time=0.8)
            self.p.clear()
            end = end_card(NEXT)
            self.play(Write(end[0]), FadeIn(end[1], shift=UP * 0.1), run_time=1.2)
        self.wait(0.6)
        self.fade_all()
