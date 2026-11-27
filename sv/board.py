"""Spark, Visually: the scene machinery every episode builds on.

Board is an EpisodeScene (Model in Motion's narration sync) that keeps its pieces by name, so a clip can bring a piece
in, swap it for its next version, morph it in place or drop it, and so a scene can open on exactly the frame the last
one ended on. An episode subclasses Board with its code panel's lines (CODE) and its shelf label (SHELF).
Import this module only after MIM_EPISODE is set, as mim.common requires.
"""
from __future__ import annotations

from manim import (AddTextLetterByLetter, DOWN, FadeIn, FadeOut, FadeTransform, GrowFromCenter, ITALIC, LEFT, LaggedStart, RIGHT, ReplacementTransform, RoundedRectangle, SEMIBOLD, UP, VGroup, ValueTracker, always_redraw, smooth)
from mim.common import ACCENT, MUTED, PAPER, DISPLAY, SERIF, SPEC, EpisodeScene, chapter_tag, txt
from sv.stage import (CODE_C, CODE_H, CODE_W, DRIVER_C, band_tag, code_box, code_glow, code_lines, driver_box, driver_gauge, driver_plan, home_shot, task_tiles)

CHAPTERS = [sc for sc in SPEC["scenes"] if sc.get("chapter")]


def chapter(scene_id: str):
    """The chapter tag for a scene, numbered and named from the episode's narration.json."""
    k = next(i for i, sc in enumerate(CHAPTERS) if sc["id"] == scene_id)
    return chapter_tag(k + 1, CHAPTERS[k]["chapter"])


def fmt(n: int) -> str:
    return f"{n:,}"


def note(s: str, y: float = -1.6, x: float | None = None, size: int = 14):
    """A quiet italic note, by default in the code panel's lower half."""
    return txt(s, size, MUTED, font=SERIF, slant=ITALIC).move_to([CODE_C[0] if x is None else x, y, 0])


def lag_for(n: int, share: float = 0.35) -> float:
    """The lag ratio that makes each of n staggered animations last `share` of the whole."""
    return 0.0 if n < 2 else (1 / share - 1) / (n - 1)


class Board(EpisodeScene):
    CODE: list[str] = []                 # the episode's code panel, in the order its lines glow
    PANEL_NOTE: str | None = None        # a setting the panel's lines rely on but don't show, noted under them
    SHELF: str | None = None             # the storage shelf's label

    def setup(self):
        super().setup()
        self.p, self.L, self.active = {}, [], None

    def put(self, **parts):
        """Pieces placed at once."""
        for k, m in parts.items():
            if k in self.p:
                self.remove(self.p.pop(k))
            if m is not None:
                self.add(m)
                self.p[k] = m

    def adopt(self, key: str, group):
        """Gather mobjects that arrived one by one (animation targets) into one named piece."""
        fam = set(group.get_family())
        self.remove(*[m for m in self.mobjects if m in fam and m is not group])
        self.add(group)
        self.p[key] = group

    def show(self, rt: float = 0.7, lag: float = 0.0, **parts):
        """Bring pieces in, or swap a piece for its next version, in one cross-fade. None takes a piece away."""
        anims = []
        for k, m in parts.items():
            if k in self.p:
                anims.append(FadeOut(self.p.pop(k)))
            if m is not None:
                anims.append(FadeIn(m))
                self.p[k] = m
        if anims:
            self.play(LaggedStart(*anims, lag_ratio=lag), run_time=rt)

    def morph(self, rt: float = 0.9, same: bool = True, **parts):
        """Turn pieces into their next versions in place (point by point when they are built alike)."""
        anims = []
        for k, m in parts.items():
            old = self.p.pop(k, None)
            anims.append(FadeIn(m) if old is None else ReplacementTransform(old, m) if same else FadeTransform(old, m))
            self.p[k] = m
        self.play(*anims, run_time=rt)

    def drop(self, *keys, rt: float = 0.6):
        gone = [self.p.pop(k) for k in keys if k in self.p]
        if gone:
            self.play(*[FadeOut(m) for m in gone], run_time=rt)

    def fade_all(self, rt: float = 0.9):
        if self.mobjects:
            self.play(*[FadeOut(m) for m in self.mobjects], run_time=rt)
        self.p.clear()

    def frame(self, code: tuple = (0, None), fade: float = 0.0, panel: bool = True, **pieces):
        """The home shot, with the code panel as the last scene left it, plus this scene's opening pieces: placed
        at once (the cut from the last scene doesn't show) or faded in."""
        shown, active = code
        self.L, self.active = code_lines(self.CODE), active
        for t in self.L:
            t.set_z_index(1)
        base = dict(home=home_shot(self.SHELF), desk=driver_box(), gauge=driver_gauge())
        if panel:
            base["codebox"] = code_box()
            for k in range(shown):
                if k == active:
                    self.L[k].set_color(PAPER)
                base[f"line{k}"] = self.L[k]
            if active is not None:
                base["glow"] = code_glow(active)
            if self.PANEL_NOTE:
                base["panel_note"] = txt(self.PANEL_NOTE, 12, MUTED, font=SERIF, slant=ITALIC).move_to(
                    [CODE_C[0], CODE_C[1] - CODE_H / 2 + 0.22, 0])
        base.update(pieces)
        base["band"] = band_tag()
        self.show(rt=fade, **base) if fade else self.put(**base)

    def code_to(self, k: int, rt: float = 0.9):
        """Line k becomes the active line: typed if it is new, and the glow moves to it."""
        anims = []
        if self.active is not None and self.active != k:
            anims.append(self.L[self.active].animate.set_color(MUTED))
        if f"line{k}" not in self.p:
            self.L[k].set_color(PAPER)
            anims.append(AddTextLetterByLetter(self.L[k]))
            self.p[f"line{k}"] = self.L[k]
        else:
            anims.append(self.L[k].animate.set_color(PAPER))
        g = code_glow(k)
        anims.append(ReplacementTransform(self.p.pop("glow"), g) if "glow" in self.p else FadeIn(g))
        self.p["glow"], self.active = g, k
        self.play(*anims, run_time=rt)

    def fly(self, key: str, pairs, rt: float = 1.8, share: float = 0.25, arc: float = 0.5, **extra):
        """Each source flies to its landing place and becomes it; the landed targets become one named piece.
        Extra pieces fade in alongside."""
        pairs = list(pairs)
        self.add(*[s for s, _ in pairs])
        anims = [LaggedStart(*[ReplacementTransform(s, d, path_arc=arc) for s, d in pairs], lag_ratio=lag_for(len(pairs), share))]
        for k, m in extra.items():
            anims.append(FadeIn(m))
            self.p[k] = m
        self.play(*anims, run_time=rt)
        self.adopt(key, VGroup(*[d for _, d in pairs]))

    def plan_to(self, steps, rt: float = 0.7, **style):
        """The plan on the driver desk becomes `steps` (driver_plan's styles pass through)."""
        p = driver_plan(steps, **style)
        if "plan" in self.p:
            self.play(FadeTransform(self.p.pop("plan"), p), run_time=rt)
        else:
            self.play(FadeIn(p, shift=RIGHT * 0.1), run_time=rt)
        self.p["plan"] = p

    def drop_tasks(self, benches, key: str = "tasks"):
        """The driver hands one task to each listed bench: small squares leave the desk for a core slot."""
        src = [RoundedRectangle(corner_radius=0.05, width=0.22, height=0.22, stroke_width=0, fill_color=ACCENT, fill_opacity=1)
               .move_to(DRIVER_C + DOWN * 0.2 + RIGHT * (k - 1.5) * 0.33) for k in range(len(benches))]
        dst = task_tiles(benches)
        self.play(LaggedStart(*[GrowFromCenter(s) for s in src], lag_ratio=0.2), run_time=0.5)
        self.play(LaggedStart(*[ReplacementTransform(a, b, path_arc=-0.8) for a, b in zip(src, dst)], lag_ratio=0.15), run_time=1.2)
        self.adopt(key, dst)

    def code_rest(self, rt: float = 0.6):
        """The active line dims and its glow goes: the code has run, and nothing is running now."""
        anims = [FadeOut(self.p.pop("glow"))] if "glow" in self.p else []
        if self.active is not None:
            anims.append(self.L[self.active].animate.set_color(MUTED))
        self.active = None
        if anims:
            self.play(*anims, run_time=rt)

    def count_row(self, key: str, label: str, start: int, end: int, row: int, top: float = 0.55, rt: float = 1.6,
                  final: str | None = None):
        """A ledger row whose value runs from start to end, then (if given) settles as the `final` text."""
        left = CODE_C[0] - CODE_W / 2 + 0.24
        y = top - row * (0.42 + 0.62)
        lab = txt(label, 17, MUTED)
        lab.move_to([left + lab.width / 2, y, 0])
        vt = ValueTracker(start)

        def draw():
            m = txt(fmt(round(vt.get_value())), 34, ACCENT, weight=SEMIBOLD, font=DISPLAY)
            return m.move_to([left + m.width / 2, y - 0.42, 0])

        val = always_redraw(draw)
        self.play(FadeIn(lab), FadeIn(val), run_time=0.45)
        self.play(vt.animate.set_value(end), run_time=rt, rate_func=smooth)
        val.clear_updaters()
        if final is not None:
            fin = txt(final, 34, ACCENT, weight=SEMIBOLD, font=DISPLAY)
            fin.move_to([left + fin.width / 2, y - 0.42, 0])
            self.play(FadeTransform(val, fin), run_time=0.5)
            val = fin
        self.adopt(key, VGroup(lab, val))


__all__ = ["Board", "chapter", "fmt", "note", "lag_for"]
