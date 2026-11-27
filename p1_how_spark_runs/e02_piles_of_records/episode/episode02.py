"""Spark, Visually · Part I · Episode 2: Piles of Records (partitions, tasks and cores).

ANIMATION PASS, cut to the am_fenrir voiceover. Every re-deal moves each patient to the pile Spark put them in
(e02_numbers.json, pile_of); the 32 small piles sit on the benches in the lanes and rounds of their timeline bars.

The timelines are drawn from real task times: from e02_timings.json (a Databricks run on eight real cores) when it is
present, otherwise from e02_numbers.json's own run, labelled with its configuration on screen.

Render one scene:   manim -qm --media_dir build/media episode/episode02.py S04_Rounds      (run from the episode folder)
Every number comes from e02_numbers.json (or e02_timings.json); none is typed here.
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
from sv.board import Board, chapter, fmt, lag_for, note  # noqa: E402
from sv.cast import TRAYS, cast_rows, trays_from_counts  # noqa: E402

N = json.loads((EP / "e02_numbers.json").read_text())
CODE = N["code_on_screen"]
REC = {pid: (c, bool(y)) for pid, c, _, y in cast_rows()}
TRAYID = trays_from_counts(N["trays"], with_ids=True)        # tray -> [(patient, clinic, readmitted)]
PILE = {k: {int(p): v for p, v in m.items()} for k, m in N["pile_of"].items()}
UNEVEN_PILE = {int(p): v for p, v in N["uneven"]["pile_of"].items()}
SLOTS = N["slots"]
NEXT = "Nothing happens until you ask"                        # the next episode's title (blueprint M3)
assert all(PILE["four"][pid] == t for t in range(TRAYS) for pid, _, _ in TRAYID[t])
assert SLOTS == 2 * TRAYS


def uneven_label(scale: int, sizes: list) -> str:
    """What the uneven timeline measures: the cast scaled up into uneven piles, with real work on every row."""
    return f"the cast \u00d7 {scale:,}: piles of {max(sizes):,} and {min(sizes):,} rows, every row hashed"


def timelines():
    """(tiny_32, uneven, label, uneven data): from the eight-core Databricks run if it is here, else from this
    episode's own run."""
    f = EP / "e02_timings.json"
    if f.exists():
        D = json.loads(f.read_text())
        ex = D["experiments"]
        fixed = sorted(t["duration_ms"] for t in ex["tiny_32"])[len(ex["tiny_32"]) // 2]   # a tiny pile's task is all fixed cost

        def conv(ts):
            return [{"pile": t["index"], "start_ms": t["start_ms"], "end_ms": t["start_ms"] + t["duration_ms"],
                     "cap": min(1.0, fixed / max(t["duration_ms"], 1))} for t in ts]
        c = D["cluster"]
        return (conv(ex["tiny_32"]), conv(ex["uneven_4"]), f"measured on Databricks: {c['runtime']}, {c['cpus']} cores",
                uneven_label(D["scale"], D["pile_sizes"]["uneven_4"]))
    T = N["timelines"]
    U = N["uneven"]
    return (T["tiny_32"], T["uneven"], f"measured in {T['source'].replace('this run: ', 'this episode’s run: ')} (tasks running together share it)",
            uneven_label(U["scaled_piles"][0] // U["piles"][0], U["scaled_piles"]))


TINY, UNEVEN_TL, TL_SOURCE, UNEVEN_DATA = timelines()


# ---------------------------------------------------------------- positions
def dense_pos(i: int, k: int, cols: int = 12, dy: float = 0.105, top: float = 0.45):
    return np.array([BENCH_XS[i] + (k % cols - (cols - 1) / 2) * dy, top - (k // cols) * dy, 0])


def half_pos(i: int, side: int, k: int):
    return np.array([BENCH_XS[i] + (-0.5 if side == 0 else 0.5) + (k % 3 - 1) * 0.2, TRAY_Y0 + 0.12 - (k // 3) * 0.2, 0])


def mini_pos(i: int, side: int, rnd: int, k: int):
    return np.array([BENCH_XS[i] + (-0.5 if side == 0 else 0.5) + (k - 1.5) * 0.105, 0.5 - rnd * 0.3, 0])


def bench_of(pile: int):                                        # eight piles: pile p on bench p // 2, beside core p % 2
    return pile // 2, pile % 2


def chips_at(pids, pos_fn, r=0.07):
    return VGroup(*[chip(CLINIC_COLOR[REC[p][0]], REC[p][1], r=r).move_to(pos_fn(k, p)) for k, p in enumerate(pids)])


# ---------------------------------------------------------------- pieces
def four_files():
    return VGroup(*[file_card(t, f"file {t + 1}") for t in range(TRAYS)])


def one_file():
    return file_card(0, "1 file")


def trays4(opacity: float = 1.0):
    return VGroup(*[tray(t, [(c, e) for _, c, e in TRAYID[t]], opacity=opacity) for t in range(TRAYS)])


def tiles(slots_: list[tuple[int, int]], color=ACCENT):
    return VGroup(*[slot_tile(i, k, color=color) for i, k in slots_])


def idle_marks(slots_: list[tuple[int, int]]):
    return VGroup(*[RoundedRectangle(corner_radius=0.05, width=0.3, height=0.3, stroke_color=ALERT, stroke_width=2)
                    .move_to([BENCH_XS[i] + 0.66 + 0.3 * k, NAME_Y, 0]) for i, k in slots_])


ALL_SLOTS = [(i, k) for i in range(TRAYS) for k in range(2)]


def members(mapping: dict, pile: int):
    """The patients in a pile, in the order of the four files they came from."""
    order = [pid for t in range(TRAYS) for pid, _, _ in TRAYID[t]]
    return [p for p in order if mapping[p] == pile]


def read_pairs(dest_of):
    """Every patient leaves its file on the shelf for its place in a pile: dest_of(pid) -> position."""
    pairs = []
    for t in range(TRAYS):
        src = file_card(t, "")[0].get_center()
        for k, (pid, c, e) in enumerate(TRAYID[t]):
            s = chip(CLINIC_COLOR[c], e, r=0.05).move_to(src + RIGHT * ((k % 9) - 4) * 0.1)
            pairs.append((s, chip(CLINIC_COLOR[c], e, r=TRAY_R).move_to(dest_of(pid))))
    return pairs


def tray_dest(pid):
    t = PILE["four"][pid]
    return tray_pos(t, [p for p, _, _ in TRAYID[t]].index(pid))


def eight_dest():
    idx = {p: members(PILE["repartition8"], PILE["repartition8"][p]).index(p) for p in REC}
    return lambda pid: half_pos(*bench_of(PILE["repartition8"][pid]), idx[pid])


def redeal(src_pos, dest_pos, r_from=TRAY_R, r_to=0.07):
    return [(chip(CLINIC_COLOR[REC[p][0]], REC[p][1], r=r_from).move_to(src_pos(p)),
             chip(CLINIC_COLOR[REC[p][0]], REC[p][1], r=r_to).move_to(dest_pos(p))) for p in REC]


def output_rows():
    """The code panel's show() output: one row per pile."""
    rows = [(f"pile {t}", str(n), PAPER) for t, n in enumerate(N["piles"]["four_files"])]
    return readout(rows, top=0.95, dy=0.4, size=22)


def zoom_bar(cap_share: float, y: float = 0.95):
    """S04: one tiny pile's bar, enlarged: nearly all grey cap. Returns the callout and its second line, unshown."""
    w = 4.2
    x0 = TL_C[0] + TL_W / 2 - 0.45 - w
    bar = Rectangle(width=w, height=0.34, stroke_width=0, fill_color=PAPER, fill_opacity=0.85).move_to([x0 + w / 2, y, 0])
    cap = Rectangle(width=w * cap_share, height=0.34, stroke_width=0, fill_color=MUTED, fill_opacity=1).move_to(
        [x0 + w * cap_share / 2, y, 0])
    lab = txt(f"fixed cost: more than {int(cap_share * 100)}% of the task", 15, PAPER).next_to(bar, DOWN, buff=0.12)
    line2 = VGroup(txt("a count's real work matches it", 14, ACCENT),
                   txt("at least tens of thousands of rows a pile", 14, ACCENT)).arrange(DOWN, buff=0.06).next_to(lab, DOWN, buff=0.1)
    box = RoundedRectangle(corner_radius=0.1, width=w + 0.4, height=1.75, stroke_color=ACCENT, stroke_width=2, fill_color=INK_2,
                           fill_opacity=1).move_to([x0 + w / 2, y - 0.5, 0])
    return VGroup(box, bar, cap, lab), line2


def rule_card():
    head = txt("How many piles?", 34, PAPER, weight=SEMIBOLD, font=DISPLAY).move_to([0, 2.3, 0])
    rule = txt("a few per core, each big enough that its work dwarfs the fixed cost", 22, ACCENT).move_to([0, 1.5, 0])
    rows = []
    specs = [("too few", "cores sit idle"), ("too many tiny ones", "you pay for overhead"), ("uneven", "everyone waits for the biggest")]
    for k, (a, b) in enumerate(specs):
        y = 0.45 - k * 1.0
        icon = VGroup()
        if k == 0:
            icon = VGroup(*[RoundedRectangle(corner_radius=0.04, width=0.24, height=0.24, stroke_width=0 if j < 2 else 2,
                                             stroke_color=ALERT, fill_color=ACCENT, fill_opacity=1 if j < 2 else 0)
                            for j in range(8)]).arrange(RIGHT, buff=0.08)
        elif k == 1:
            icon = VGroup(*[VGroup(Rectangle(width=0.3, height=0.16, stroke_width=0, fill_color=PAPER, fill_opacity=0.85),
                                   Rectangle(width=0.28, height=0.16, stroke_width=0, fill_color=MUTED, fill_opacity=1))
                            for _ in range(6)]).arrange(RIGHT, buff=0.06)
            for g in icon:
                g[1].align_to(g[0], LEFT)
        else:
            icon = VGroup(Rectangle(width=2.4, height=0.16, stroke_width=0, fill_color=PAPER, fill_opacity=0.85),
                          *[Rectangle(width=0.5, height=0.16, stroke_width=0, fill_color=PAPER, fill_opacity=0.85) for _ in range(3)])
            icon[0].move_to(ORIGIN)
            for j in range(3):
                icon[j + 1].next_to(icon[0], DOWN, buff=0.06 + j * 0.22, aligned_edge=LEFT)
            icon.scale(0.8)
        icon.move_to([-3.6, y, 0])
        t1 = txt(a, 24, PAPER, weight=SEMIBOLD).move_to([-1.2, y, 0], aligned_edge=LEFT)
        t2 = txt(b, 20, MUTED, font=SERIF, slant=ITALIC).move_to([2.2, y, 0], aligned_edge=LEFT)
        rows.append(VGroup(icon, t1, t2))
    return head, rule, rows


def recap_card(k: int, title: str, caption: str, icon):
    x = (-4.35, 0.0, 4.35)[k]
    box = card(3.9, 3.5).move_to([x, 0.1, 0])
    icon.move_to([x, 0.8, 0])
    t = txt(title, 24, PAPER, weight=SEMIBOLD, font=DISPLAY).move_to([x, -0.45, 0])
    cap = txt(caption, 16, MUTED, font=SERIF, slant=ITALIC).move_to([x, -1.05, 0])
    return VGroup(box, icon, t, cap)


class E02Board(Board):
    CODE, SHELF = CODE, None

    def show_timeline(self, key: str, tasks, title: str, rt: float = 2.0):
        """The timeline card rises over the benches; bars appear in the order their tasks started."""
        tl = timeline(tasks, lanes=SLOTS, title=title, source=TL_SOURCE)
        frame_ = VGroup(tl["card"], tl["title"], tl["labels"], tl["guides"], tl["foot"])
        self.play(FadeIn(frame_, shift=UP * 0.3), run_time=0.8)
        order = sorted(tasks, key=lambda t: t["start_ms"])
        bars = [tl["bars"][t.get("pile", i)] for i, t in enumerate(order)]
        self.play(LaggedStart(*[GrowFromEdge(b, LEFT) for b in bars], lag_ratio=lag_for(len(bars), 0.3)), run_time=rt)
        self.adopt(key, VGroup(frame_, *bars))
        return tl


# =================================================================================== S01
class S01_ColdOpen(E02Board):
    def construct(self):
        dense = chips_at([pid for t in range(TRAYS) for pid, _, _ in TRAYID[t]], lambda k, p: dense_pos(0, k), r=0.045)
        with self.nar("s1_1") as seg:
            self.frame(fade=1.0, files=one_file(), store=txt("storage", 13, MUTED, font=SERIF, slant=ITALIC)
                       .move_to([X0 + 0.42, SHELF_Y + SHELF_H / 2 + 0.16, 0]))
            last, marks = tiles([(i, 0) for i in range(TRAYS)]), idle_marks([(i, 1) for i in range(TRAYS)])
            self.play(FadeIn(last), Create(marks), run_time=0.8)
            self.adopt("last", VGroup(last, marks))
            seg.wait_until(seg.word(0, "worse case"))
            self.drop("last", rt=0.5)
            seg.wait_until(seg.s(1))
            task = RoundedRectangle(corner_radius=0.05, width=0.22, height=0.22, stroke_width=0, fill_color=ACCENT,
                                    fill_opacity=1).move_to(DRIVER_C + DOWN * 0.2)
            tile = slot_tile(0, 0)
            self.play(GrowFromCenter(task), run_time=0.3)
            self.play(ReplacementTransform(task, tile, path_arc=-0.6), run_time=0.8)
            self.p["tile"] = tile
            src = one_file()[0].get_center()
            self.fly("pile", [(chip(CLINIC_COLOR[REC[p][0]], REC[p][1], r=0.04).move_to(src + RIGHT * ((k % 9) - 4) * 0.1), d)
                              for k, (p, d) in enumerate(zip([pid for t in range(TRAYS) for pid, _, _ in TRAYID[t]], dense))],
                     rt=1.6, share=0.2)
            seg.wait_until(seg.word(1, "seven sit idle"))
            idle = idle_marks([s for s in ALL_SLOTS if s != (0, 0)])
            self.play(Create(idle), run_time=0.8)
            self.p["idle"] = idle

        with self.nar("s1_2", gap=0.8) as seg:
            seg.wait_until(seg.word(0, "piled up"))
            self.play(Indicate(self.p["pile"], color=PAPER, scale_factor=1.04), run_time=0.9)
            seg.wait_until(seg.s(1))
            lab = txt("1 file \u2192 1 pile", 18, ACCENT, weight=SEMIBOLD).move_to([BENCH_XS[0], BIN_Y, 0])
            self.show(rt=0.6, lab=lab)
        self.fade_all()


# =================================================================================== S02
class S02_Title(E02Board):
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
def storage_label():
    return txt("storage", 13, MUTED, font=SERIF, slant=ITALIC).move_to([X0 + 0.42, SHELF_Y + SHELF_H / 2 + 0.16, 0])


class S03_Piles(E02Board):
    def construct(self):
        allp = [pid for t in range(TRAYS) for pid, _, _ in TRAYID[t]]
        dense = chips_at(allp, lambda k, p: dense_pos(0, k), r=0.045)
        with self.nar("s3_1") as seg:
            self.frame(fade=1.0, files=one_file(), store=storage_label(), pile=dense, tile=slot_tile(0, 0),
                       tag=chapter("S03_Piles"))
            seg.wait_until(seg.word(0, "a partition"))
            box = SurroundingRectangle(self.p["pile"], buff=0.08, color=ACCENT, corner_radius=0.06)
            lab = txt("partition: a pile of rows", 16, ACCENT, weight=SEMIBOLD).next_to(box, UP, buff=0.1)
            self.play(Create(box), FadeIn(lab), run_time=0.8)
            self.p["plab"] = VGroup(box, lab)
            seg.wait_until(seg.word(0, "one task"))
            tl = txt("one task, one core", 14, PAPER).move_to([BENCH_XS[0] + 0.3, NAME_Y + 0.32, 0])
            self.play(FadeIn(tl), Indicate(self.p["tile"], color=PAPER, scale_factor=1.4), run_time=0.8)
            self.p["tlab"] = tl

        with self.nar("s3_2") as seg:
            self.show(rt=0.5, plab=None, tlab=None, pile=None, tile=None)
            self.morph(rt=0.7, same=False, files=four_files())
            seg.wait_until(seg.word(0, "These two lines"))
            self.code_to(0, rt=0.9)
            self.code_to(1, rt=0.7)
            self.fly("trays", read_pairs(tray_dest), rt=1.6, share=0.25)
            out = output_rows()
            for t, (word, nth) in enumerate((("twenty-eight", 1), ("twenty-five", 1), ("twenty-five", 2), ("twenty-five", 3))):
                seg.wait_until(seg.word(0, word, nth))
                self.play(FadeIn(out[2 * t:2 * t + 2], shift=RIGHT * 0.1), run_time=0.35)
            self.adopt("out", out)
            seg.wait_until(seg.s(1))
            lit, idle = tiles([(i, 0) for i in range(TRAYS)]), idle_marks([(i, 1) for i in range(TRAYS)])
            self.play(LaggedStart(*[GrowFromCenter(s) for s in lit], lag_ratio=0.2), Create(idle), run_time=1.0)
            self.p.update(lit=lit, idle=idle)

        with self.nar("s3_3", gap=0.8) as seg:
            self.drop("idle", "out", rt=0.4)
            src = {pid: tray_dest(pid) for pid in REC}
            dest = eight_dest()
            self.remove(self.p.pop("trays"))
            self.fly("trays8", redeal(lambda p: src[p], dest), rt=1.6, share=0.3, arc=-0.6)
            self.morph(rt=0.6, lit=tiles(ALL_SLOTS))
        self.drop("tag")


# =================================================================================== S04
class S04_Rounds(E02Board):
    def construct(self):
        dest8 = eight_dest()
        trays8 = VGroup(*[chip(CLINIC_COLOR[REC[p][0]], REC[p][1], r=0.07).move_to(dest8(p)) for p in REC])
        self.frame(code=(2, 1), files=four_files(), store=storage_label(), trays8=trays8, lit=tiles(ALL_SLOTS))
        lane_of = timeline(TINY, lanes=SLOTS)["lane_of"]
        rounds = {}
        for t in sorted(TINY, key=lambda t: t["start_ms"]):
            k = lane_of[t["pile"]]
            rounds[t["pile"]] = sum(1 for q in rounds if lane_of[q] == k)
        idx32 = {p: members(PILE["repartition32"], PILE["repartition32"][p]).index(p) for p in REC}

        def mini(p):
            pile = PILE["repartition32"][p]
            i, side = bench_of(lane_of[pile])
            return mini_pos(i, side, rounds[pile], idx32[p])

        with self.nar("s4_1") as seg:
            self.show(rt=0.6, tag=chapter("S04_Rounds"))
            self.remove(self.p.pop("trays8"))
            self.fly("minis", redeal(dest8, mini, r_from=0.07, r_to=0.045), rt=1.6, share=0.3, arc=-0.5)
            seg.wait_until(seg.word(0, "Whenever a core finishes"))
            by_round = [VGroup(*[c for c, p in zip(self.p["minis"], REC) if rounds[PILE["repartition32"][p]] == r]) for r in range(4)]
            for r in range(4):
                self.play(Indicate(by_round[r], color=PAPER, scale_factor=1.15), run_time=0.45)
            seg.wait_until(seg.s(1))
            self.show_timeline("tl", TINY, f"{N['tasks']['thirty_two']} piles on {SLOTS} cores", rt=2.0)

        tl = timeline(TINY, lanes=SLOTS)
        with self.nar("s4_2") as seg:
            seg.wait_until(seg.word(0, "a lane for each core"))
            self.play(Indicate(self.p["tl"][0][2], color=ACCENT, scale_factor=1.15), run_time=0.9)
            seg.wait_until(seg.word(0, "a bar for each task"))
            self.play(Indicate(VGroup(*self.p["tl"][1:]), color=ACCENT, scale_factor=1.03), run_time=0.9)
            seg.wait_until(seg.s(1))
            caps = VGroup(*[b[1] for b in self.p["tl"][1:]])
            self.play(Indicate(caps, color=PAPER, scale_factor=1.15), run_time=1.0)

        share = N["fixed_cost"]["share_at_32_piles_of_the_cast"]
        with self.nar("s4_3", gap=0.8) as seg:
            z, t2 = zoom_bar(share, y=0.95)
            self.play(FadeIn(z, shift=UP * 0.2), run_time=0.8)
            self.p["zoom"] = z
            seg.wait_until(seg.s(1))
            self.show(rt=0.6, zoom2=t2)
        self.drop("tag", "tl", "zoom", "zoom2", "minis", "lit")


# =================================================================================== S05
class S05_Where(E02Board):
    def construct(self):
        FF = N["from_files"]
        self.frame(code=(2, 1), store=storage_label())
        big = VGroup(RoundedRectangle(corner_radius=0.06, width=X1 - X0 - 0.5, height=0.4, stroke_color=MUTED, stroke_width=1.6,
                                      fill_color=INK, fill_opacity=1).move_to([CX, SHELF_Y, 0]),
                     txt(f"one file \u00b7 {round(FF['big_file_mb'])} MB \u00b7 {FF['big_row_group_mb']} MB row groups", 14, MUTED, font=MONO).move_to([CX, SHELF_Y, 0]))
        with self.nar("s5_1") as seg:
            self.show(rt=0.6, tag=chapter("S05_Where"))
            self.play(FadeIn(big, shift=UP * 0.15), run_time=0.6)
            self.p["big"] = big
            seg.wait_until(seg.s(1))
            w = (X1 - X0 - 0.5) / SLOTS
            pieces = [Rectangle(width=w - 0.04, height=0.4, stroke_color=ACCENT, stroke_width=1.6, fill_color=INK, fill_opacity=1)
                      .move_to([X0 + 0.25 + w * (k + 0.5), SHELF_Y, 0]) for k in range(SLOTS)]
            self.play(FadeOut(self.p.pop("big")), *[FadeIn(p) for p in pieces], run_time=0.6)
            lit = tiles(ALL_SLOTS)
            self.play(LaggedStart(*[ReplacementTransform(p, s, path_arc=-0.4) for p, s in zip(pieces, lit)], lag_ratio=0.12), run_time=1.4)
            self.adopt("lit", lit)
            self.show(rt=0.5, r1=readout([(f"1 file, {round(FF['big_file_mb'])} MB", f"{FF['big_file_piles']} piles", ACCENT)],
                                         top=0.9, size=22))
            seg.wait_until(seg.word(1, "And thirty-two small files"))
            self.drop("lit", rt=0.4)
            small = VGroup(*[RoundedRectangle(corner_radius=0.03, width=0.26, height=0.15, stroke_color=MUTED, stroke_width=1.2)
                             .move_to([X0 + 0.45 + (k % 16) * 0.56, SHELF_Y + (0.1 if k < 16 else -0.1), 0]) for k in range(FF["small_files"])])
            self.play(LaggedStart(*[FadeIn(f, scale=0.5) for f in small], lag_ratio=0.04), run_time=0.8)
            lit = tiles(ALL_SLOTS)
            groups = [VGroup(*small[4 * k:4 * k + 4]) for k in range(SLOTS)]
            self.play(LaggedStart(*[ReplacementTransform(g, s, path_arc=-0.4) for g, s in zip(groups, lit)], lag_ratio=0.12), run_time=1.4)
            self.remove(small)
            self.adopt("lit", lit)
            self.show(rt=0.5, r2=readout([(f"{FF['small_files']} small files", f"{FF['small_file_piles']} piles", ACCENT)],
                                         top=0.4, size=22))
            seg.wait_until(seg.s(2))
            self.show(rt=0.6, rule=VGroup(txt(f"a piece: at most {FF['max_partition_mb']} MB,", 17, PAPER),
                                          txt("smaller to give every core one", 17, PAPER),
                                          txt(f"never under {FF['min_piece_mb']} MB", 17, PAPER)).arrange(DOWN, buff=0.1,
                                                                                                  aligned_edge=LEFT)
                      .move_to([CODE_C[0], -0.45, 0]))

        with self.nar("s5_2", gap=0.8) as seg:
            self.drop("r1", "r2", "rule", "lit", rt=0.5)
            c1 = VGroup(txt("data built in code", 18, MUTED), txt("spark.range(1000)", 16, PAPER, font=MONO),
                        txt(f"\u2192 {N['created_piles']} piles, one per core", 20, ACCENT, weight=SEMIBOLD)).arrange(DOWN, buff=0.14,
                                                                                                                aligned_edge=LEFT)
            c2 = VGroup(txt("after a shuffle", 18, MUTED), txt("spark.sql.shuffle.partitions", 15, PAPER, font=MONO),
                        txt(f"\u2192 {N['after_shuffle']['setting']} piles by default", 20, ACCENT, weight=SEMIBOLD),
                        txt("small ones merged (Episode 4)", 16, MUTED, font=SERIF, slant=ITALIC)).arrange(DOWN, buff=0.12,
                                                                                                 aligned_edge=LEFT)
            for c in (c1, c2):
                if c.width > CODE_W - 0.4:
                    c.scale_to_fit_width(CODE_W - 0.4)
            c1.move_to([CODE_C[0], 0.55, 0])
            c2.move_to([CODE_C[0], -1.0, 0])
            self.play(FadeIn(c1, shift=UP * 0.1), Indicate(self.p["home"][4:], color=ACCENT, scale_factor=1.0), run_time=0.8)
            self.p["c1"] = c1
            seg.wait_until(seg.s(1))
            self.show(rt=0.7, c2=c2)
        self.drop("tag", "c1", "c2")


# =================================================================================== S06
class S06_Merge(E02Board):
    def construct(self):
        self.frame(code=(2, 1), files=four_files(), store=storage_label())
        coal = PILE["coalesce2"]
        sizes = N["coalesce"]["piles"]
        with self.nar("s6_1") as seg:
            self.show(rt=0.6, tag=chapter("S06_Merge"))
            self.fly("trays", read_pairs(tray_dest), rt=1.3, share=0.25)
            seg.wait_until(seg.s(1))
            self.code_to(2, rt=0.8)
            self.drop("trays", rt=0.4)
            idx = {p: members(coal, coal[p]).index(p) for p in REC}
            pairs = []
            for t in range(TRAYS):
                src = file_card(t, "")[0].get_center()
                for k, (pid, c, e) in enumerate(TRAYID[t]):
                    s = chip(CLINIC_COLOR[c], e, r=0.05).move_to(src + RIGHT * ((k % 9) - 4) * 0.1)
                    pairs.append((s, chip(CLINIC_COLOR[c], e, r=0.06).move_to(dense_pos(coal[pid], idx[pid], cols=9, dy=0.15, top=0.45))))
            lit = tiles([(0, 0), (1, 0)])
            self.play(LaggedStart(*[GrowFromCenter(s) for s in lit], lag_ratio=0.3), run_time=0.6)
            self.p["lit"] = lit
            self.fly("merged", pairs, rt=1.8, share=0.25, arc=0.4)
            labels = VGroup(*[txt(str(sizes[p]), 26, ACCENT, weight=SEMIBOLD, font=DISPLAY).move_to([BENCH_XS[p] + 0.72, BIN_Y, 0])
                              for p in range(len(sizes))])
            nb = txt("nothing crosses the network", 14, MUTED, font=SERIF, slant=ITALIC).move_to([X1 - 1.35, BAND_Y + 0.3, 0])
            self.show(rt=0.6, labels=labels, nb=nb)

        dest8 = eight_dest()
        with self.nar("s6_2", gap=0.8) as seg:
            self.drop("merged", "labels", "nb", "lit", rt=0.5)
            self.code_to(3, rt=0.8)
            self.fly("trays", read_pairs(tray_dest), rt=1.1, share=0.25)
            src = {pid: tray_dest(pid) for pid in REC}
            self.remove(self.p.pop("trays"))
            band = Rectangle(width=X1 - X0, height=BAND_H, stroke_width=0, fill_color=ACCENT, fill_opacity=0.35).move_to([CX, BAND_Y, 0])
            self.play(FadeIn(band), run_time=0.3)
            pairs = [(chip(CLINIC_COLOR[REC[p][0]], REC[p][1], r=TRAY_R).move_to(src[p]),
                      chip(CLINIC_COLOR[REC[p][0]], REC[p][1], r=0.07).move_to(dest8(p))) for p in REC]
            self.add(*[s for s, _ in pairs])
            paths = [CubicBezier(s.get_center(), [s.get_x(), BAND_Y + 0.2, 0], [d.get_x(), BAND_Y + 0.2, 0], d.get_center())
                     for s, d in pairs]
            self.play(LaggedStart(*[MoveAlongPath(s, path) for (s, _), path in zip(pairs, paths)], lag_ratio=lag_for(len(pairs), 0.3)),
                      run_time=2.2)
            self.remove(*[s for s, _ in pairs])
            self.put(trays8=VGroup(*[d for _, d in pairs]))
            self.play(FadeOut(band), run_time=0.4)
            self.show(rt=0.6, lit=tiles(ALL_SLOTS),
                      ro=readout([("rows re-dealt", str(N["repartition"]["rows_re_dealt"]), ACCENT),
                                  ("new piles", str(len(N["repartition"]["piles"])), PAPER)], top=-0.2, size=24))
        self.drop("tag", "trays8", "lit", "ro")


# =================================================================================== S07
class S07_Uneven(E02Board):
    def construct(self):
        sizes = N["uneven"]["piles"]
        counts = {p: sum(1 for v in UNEVEN_PILE.values() if v == p) for p in range(TRAYS)}
        order_piles = sorted(range(TRAYS), key=lambda p: -counts[p])     # the big pile on bench 1
        bench_for = {p: i for i, p in enumerate(order_piles)}
        files = VGroup(*[file_card(i, f"{counts[p]} rows") for i, p in enumerate(order_piles)])
        self.frame(code=(4, 3), files=files, store=storage_label())
        with self.nar("s7_1") as seg:
            self.show(rt=0.6, tag=chapter("S07_Uneven"))
            seg.wait_until(seg.word(0, "piles of seventy"))
            idx = {}
            for p in range(TRAYS):
                for k, pid in enumerate([q for q, v in sorted(UNEVEN_PILE.items()) if v == p]):
                    idx[pid] = k
            pairs = []
            for pid, p in sorted(UNEVEN_PILE.items()):
                i = bench_for[p]
                c, e = REC[pid]
                src = file_card(i, "")[0].get_center()
                d = dense_pos(i, idx[pid], cols=10, dy=0.13, top=0.45) if counts[p] > 20 else tray_pos(i, idx[pid])
                pairs.append((chip(CLINIC_COLOR[c], e, r=0.05).move_to(src), chip(CLINIC_COLOR[c], e, r=0.055 if counts[p] > 20 else TRAY_R).move_to(d)))
            self.fly("piles", pairs, rt=1.6, share=0.25)
            lit = tiles([(i, 0) for i in range(TRAYS)])
            self.play(FadeIn(lit), run_time=0.5)
            self.p["lit"] = lit
            seg.wait_until(seg.s(1))
            tl = self.show_timeline("tl", UNEVEN_TL, UNEVEN_DATA, rt=1.6)
            seg.wait_until(seg.word(1, "the others finish and wait"))
            end = max(t["end_ms"] for t in UNEVEN_TL)
            used = {tl["lane_of"][t["pile"]] for t in UNEVEN_TL}
            waits = VGroup()
            for t in UNEVEN_TL:
                if t["end_ms"] < end * 0.97:
                    k = tl["lane_of"][t["pile"]]
                    x0, x1 = tl["x"](t["end_ms"]), tl["x"](end)
                    waits.add(DashedVMobject(Rectangle(width=x1 - x0, height=tl["lane_h"] * 0.62, stroke_color=ALERT, stroke_width=2)
                                             .move_to([(x0 + x1) / 2, tl["lane_y"](k), 0]), num_dashes=24))
            for k in range(SLOTS):
                if k not in used:
                    x0, x1 = tl["x"](0), tl["x"](end)
                    waits.add(DashedVMobject(Rectangle(width=x1 - x0, height=tl["lane_h"] * 0.62, stroke_color=ALERT,
                                                       stroke_width=1.2, stroke_opacity=0.6).move_to([(x0 + x1) / 2, tl["lane_y"](k), 0]),
                                             num_dashes=30))
            self.play(Create(waits), run_time=1.0)
            self.p["waits"] = waits
            self.uneven_tl = tl

        with self.nar("s7_2", gap=0.8) as seg:
            tl = self.uneven_tl
            end = max(t["end_ms"] for t in UNEVEN_TL)
            x = tl["x"](end)
            mark = DashedLine([x, TL_C[1] + TL_H / 2 - 0.5, 0], [x, TL_C[1] - TL_H / 2 + 0.4, 0], dash_length=0.08, color=ACCENT, stroke_width=3)
            t_ = txt("the stage ends here", 15, ACCENT, weight=SEMIBOLD)
            bg = RoundedRectangle(corner_radius=0.06, width=t_.width + 0.24, height=t_.height + 0.16, stroke_width=0,
                                  fill_color=INK, fill_opacity=1)
            lab = VGroup(bg, t_.move_to(bg))                         # over the idle lanes, clear of the title and footer
            lab.move_to([x - 0.15 - lab.width / 2, (tl["lane_y"](4) + tl["lane_y"](5)) / 2, 0])
            self.play(Create(mark), FadeIn(lab), run_time=0.9)
            self.p["mark"] = VGroup(mark, lab)
        self.fade_all()


# =================================================================================== S08
class S08_Rule(E02Board):
    def construct(self):
        head, rule, rows = rule_card()
        with self.nar("s8_1", gap=0.8) as seg:
            self.show(rt=0.6, tag=chapter("S08_Rule"))
            self.play(FadeIn(head, shift=UP * 0.15), run_time=0.7)
            seg.wait_until(seg.word(0, "A few per core"))
            self.play(FadeIn(rule, shift=UP * 0.1), run_time=0.8)
            for k, cue in enumerate(("Too few", "Too many tiny ones", "Uneven")):
                seg.wait_until(seg.word(1, cue))
                self.play(FadeIn(rows[k], shift=RIGHT * 0.15), run_time=0.6)
        self.fade_all()


# =================================================================================== S09
class S09_Recap(E02Board):
    def construct(self):
        one = VGroup(bench(0), tray(0, [(c, e) for _, c, e in TRAYID[0]]), slot_tile(0, 0)).scale(0.6)
        where = VGroup(*[file_card(t, "") for t in range(2)], *[slot_tile(t, 0) for t in range(2)]).scale(0.7)
        merge = VGroup(txt("coalesce", 20, PAPER, font=MONO), txt("repartition", 20, PAPER, font=MONO)).arrange(DOWN, buff=0.25)
        cards = [recap_card(0, "One pile, one task", "on one core", one),
                 recap_card(1, "Files, code, shuffles", "decide the piles", where),
                 recap_card(2, "Merge or re-deal", "coalesce, or repartition", merge)]
        with self.nar("s9_1") as seg:
            self.show(rt=0.7, tag=chapter("S09_Recap"))
            seg.wait_until(seg.s(1))
            self.play(FadeIn(cards[0], shift=UP * 0.3), run_time=0.8)
            seg.wait_until(seg.s(2))
            self.play(FadeIn(cards[1], shift=UP * 0.3), run_time=0.8)
            seg.wait_until(seg.word(2, "Coalesce merges"))
            self.play(FadeIn(cards[2], shift=UP * 0.3), run_time=0.8)
            self.p.update(r0=cards[0], r1=cards[1], r2=cards[2])

        with self.nar("s9_2") as seg:
            self.show(rt=0.6, r0=None, r1=None, r2=None, tag=None)
            self.frame(code=(4, None), fade=0.8, files=four_files(), store=storage_label())
            flash = VGroup(code_glow(2), code_glow(3))
            self.play(FadeIn(flash), run_time=0.4)
            self.show(rt=0.5, ro=readout([("jobs run", str(N["lines_3_4_jobs"]), ACCENT)], top=-0.2, size=26))
            self.play(FadeOut(flash), run_time=0.4)
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
