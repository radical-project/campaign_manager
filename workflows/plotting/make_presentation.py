#!/usr/bin/env python3
"""
Generate SPHERICAL benchmark PowerPoint presentation.

Usage:
    python make_presentation.py [--out spherical_benchmark.pptx]
"""

import argparse
from pathlib import Path

import matplotlib
matplotlib.use("Agg")
import matplotlib.patches as mpatches
import matplotlib.pyplot as plt
import numpy as np
from matplotlib.patches import FancyBboxPatch

from pptx import Presentation
from pptx.util import Inches, Pt
from pptx.dml.color import RGBColor
from pptx.enum.text import PP_ALIGN
from pptx.enum.shapes import MSO_SHAPE

# ── Colour constants ──────────────────────────────────────────────────────────

WHITE       = RGBColor(0xFF, 0xFF, 0xFF)
BLACK       = RGBColor(0x00, 0x00, 0x00)
DARK_BG     = RGBColor(0x1A, 0x1A, 0x2E)
ACCENT      = RGBColor(0x0F, 0x3C, 0x78)
LIGHT_PANEL = RGBColor(0xF0, 0xF4, 0xFF)
GRAY_TEXT   = RGBColor(0x55, 0x55, 0x55)
BASELINE_C  = RGBColor(0x9E, 0x9E, 0x9E)
SHARD_C     = RGBColor(0x4C, 0xAF, 0x50)
BANDIT_C    = RGBColor(0x9C, 0x27, 0xB0)
ALLOPT_C    = RGBColor(0xF4, 0x43, 0x36)
HIGHLIGHT   = RGBColor(0xFF, 0xC1, 0x07)
TEAL_C      = RGBColor(0x00, 0x83, 0x8F)
ORANGE_C    = RGBColor(0xE6, 0x51, 0x00)

# Campaign-Manager intro palette (ported from cm_presentation_v2.js)
CM_NAVY     = RGBColor(0x0D, 0x1B, 0x3E)
CM_TEAL     = RGBColor(0x0F, 0x71, 0x73)
CM_TEALLT   = RGBColor(0x14, 0xA0, 0xA3)
CM_ICE      = RGBColor(0xD5, 0xE8, 0xF0)
CM_OFFWHITE = RGBColor(0xF4, 0xF8, 0xFB)
CM_SLATE    = RGBColor(0x2C, 0x4A, 0x6E)
CM_GOLD     = RGBColor(0xD9, 0x6E, 0x12)  # orange (was gold; low contrast on off-white)
CM_MUTED    = RGBColor(0x6B, 0x8F, 0xAB)
CM_CARDNAVY = RGBColor(0x13, 0x27, 0x48)
CM_CARDTXT  = RGBColor(0x8B, 0xAA, 0xC4)

SLIDE_W = Inches(13.33)
SLIDE_H = Inches(7.5)


# ── Low-level helpers ─────────────────────────────────────────────────────────

def _bg(slide, color: RGBColor):
    fill = slide.background.fill
    fill.solid()
    fill.fore_color.rgb = color


def _box(slide, l, t, w, h, text="", font_size=18, bold=False,
         color=WHITE, bg=None, align=PP_ALIGN.LEFT,
         font_name="Calibri", italic=False, wrap=True):
    txBox = slide.shapes.add_textbox(l, t, w, h)
    tf    = txBox.text_frame
    tf.word_wrap = wrap
    p  = tf.paragraphs[0]
    p.alignment = align
    run = p.add_run()
    run.text = text
    run.font.size      = Pt(font_size)
    run.font.bold      = bold
    run.font.italic    = italic
    run.font.color.rgb = color
    run.font.name      = font_name
    if bg is not None:
        txBox.fill.solid()
        txBox.fill.fore_color.rgb = bg
    return txBox


def _rect(slide, l, t, w, h, fill_color: RGBColor, line_color=None, line_width=0):
    shape = slide.shapes.add_shape(1, l, t, w, h)
    shape.fill.solid()
    shape.fill.fore_color.rgb = fill_color
    if line_color:
        shape.line.color.rgb = line_color
        shape.line.width     = Pt(line_width)
    else:
        shape.line.fill.background()
    return shape


def _img(slide, path, l, t, w, h=None):
    if h is not None:
        slide.shapes.add_picture(str(path), l, t, w, h)
    else:
        slide.shapes.add_picture(str(path), l, t, w)


def _title_bar(slide, title: str, subtitle: str = ""):
    _rect(slide, Inches(0), Inches(0), SLIDE_W, Inches(1.1), ACCENT)
    _box(slide, Inches(0.25), Inches(0.08), Inches(12.5), Inches(0.6),
         title, font_size=28, bold=True, color=WHITE, align=PP_ALIGN.LEFT)
    if subtitle:
        _box(slide, Inches(0.25), Inches(0.65), Inches(12.5), Inches(0.35),
             subtitle, font_size=13, color=RGBColor(0xBB, 0xCC, 0xFF),
             align=PP_ALIGN.LEFT)


def _stat_box(slide, l, t, w, h, value, label, val_color=HIGHLIGHT,
              lbl_color=WHITE, bg=ACCENT):
    _rect(slide, l, t, w, h, bg)
    _box(slide, l, t + Inches(0.05), w, Inches(0.55),
         value, font_size=36, bold=True, color=val_color, align=PP_ALIGN.CENTER)
    _box(slide, l, t + Inches(0.6), w, Inches(0.35),
         label, font_size=11, color=lbl_color, align=PP_ALIGN.CENTER)


def _section_panel(slide, l, t, w, h, title, bullets, title_bg, title_color=WHITE,
                   body_bg=None, bullet_color=None, title_size=11, bullet_size=10):
    """Titled panel with bullet items."""
    if body_bg is None:
        body_bg = RGBColor(0xF8, 0xF8, 0xF8)
    if bullet_color is None:
        bullet_color = BLACK
    _rect(slide, l, t, w, Inches(0.32), title_bg)
    _box(slide, l + Inches(0.06), t + Inches(0.02), w - Inches(0.12), Inches(0.30),
         title, font_size=title_size, bold=True, color=title_color)
    _rect(slide, l, t + Inches(0.32), w, h - Inches(0.32), body_bg)
    y = t + Inches(0.36)
    per = (h - Inches(0.40)) / max(len(bullets), 1)
    for b in bullets:
        _box(slide, l + Inches(0.1), y, w - Inches(0.15), per,
             f"• {b}", font_size=bullet_size, color=bullet_color)
        y += per


# ── Slide builders ────────────────────────────────────────────────────────────

PLOT_DIR = Path(__file__).parent / "plots" / "optimizations"


def _cm_header(slide, title: str):
    """Header bar for the CM-intro slides — same ACCENT colour as _title_bar
    so every slide's top section matches."""
    _rect(slide, Inches(0), Inches(0), SLIDE_W, Inches(0.95), ACCENT)
    _box(slide, Inches(0.55), Inches(0.18), Inches(12), Inches(0.6),
         title, font_size=26, bold=True, color=WHITE)


def slide_cm_core_idea(prs):
    """CM intro 1 — what the Campaign Manager is, in one statement + 3 pillars."""
    slide = prs.slides.add_slide(prs.slide_layouts[6])
    _bg(slide, CM_OFFWHITE)
    _cm_header(slide, "Campaign Manager: Core Concept")

    # Big statement band (navy with gold left accent)
    _rect(slide, Inches(0.7), Inches(1.25), Inches(11.9), Inches(2.0), CM_NAVY)
    _rect(slide, Inches(0.7), Inches(1.25), Inches(0.14), Inches(2.0), CM_GOLD)
    _box(slide, Inches(1.1), Inches(1.45), Inches(11.2), Inches(1.6),
         "The Campaign Manager maintains and continuously refines an execution plan "
         "for multiple concurrent, heterogeneous workflows, coordinating their "
         "activities, adapting to incoming results, and optimizing decisions across "
         "multiple dimensions to achieve user-defined campaign objectives.",
         font_size=22, color=WHITE)

    pillars = [
        ("Orchestrate",
         "Coordinate many interdependent workflows as a single campaign, "
         "automatically managing ordering, data flow, and shared resources.",
         CM_TEAL),
        ("Adapt",
         "Continuously learn from incoming results and adjust the plan in real time, "
         "rather than committing to a static execution plan upfront.", CM_NAVY),
        ("Optimize for objectives",
         "Dynamically refine decisions across multiple dimensions — including cost, "
         "uncertainty, throughput, etc. — throughout execution to achieve "
         "campaign objectives.", CM_GOLD),
    ]
    for i, (label, body, col) in enumerate(pillars):
        x = Inches(0.7 + i * 4.05)
        y = Inches(3.7)
        _rect(slide, x, y, Inches(3.8), Inches(2.9), WHITE,
              line_color=RGBColor(0xD0, 0xDF, 0xE8), line_width=0.5)
        _rect(slide, x, y, Inches(3.8), Inches(0.12), col)
        _box(slide, x + Inches(0.25), y + Inches(0.3), Inches(3.3), Inches(0.5),
             label, font_size=18, bold=True, color=CM_NAVY)
        _box(slide, x + Inches(0.25), y + Inches(0.95), Inches(3.35), Inches(1.8),
             body, font_size=16, color=CM_SLATE)


def slide_cm_closed_loop(prs):
    """CM intro 2 — the 5-step adaptive loop."""
    slide = prs.slides.add_slide(prs.slide_layouts[6])
    _bg(slide, CM_OFFWHITE)
    _cm_header(slide, "Campaign Manager: Continuous Replanning")

    steps = [
     #    ("1", "Define\nObjectives", " Identify high-quality N leads while"
     #     " minimizing time-to-solution and uncertainty in candidate selection."),
     #    ("2", "Plan\nExecution", "The Campaign Manager constructs an initial execution plan — which workflows run, "
     #     "how many instances, in what order, and the resource budget each gets."),
     #    ("3", "Run\nInstances", "Concurrent workflow instances execute, consuming "
     #     "resources according to the current plan."),
     #    ("4", "Observe\nOutcomes", "Results, metrics and queue signals flow back "
     #     "to CM as each instance completes."),
     #    ("5", "Replan\n& Adapt", "CM updates the plan — reallocating resources, "
     #     "re-prioritising workflows, and nudging the budget-control score cutoffs."),

         ("1", "Define\nObjectives", 
          "Identify high-quality N leads while minimizing time-to-solution and uncertainty "
          "in candidate selection (Antigen Prediction Workflow)."),

          ("2", "Plan\nExecution", 
          "The Campaign Manager constructs an initial execution plan — defining which workflows"
          " to run, how many instances are launched, their ordering, and allocated resource budgets."),

          ("3", "Run\nInstances", "Concurrent workflow instances execute, consuming "
          "resources according to the current plan."),

          ("4", "Observe\nOutcomes", 
          "Results, metrics, and queue signals flow back to the Campaign Manager as each" 
          " instance completes."),

          ("5", "Replan\n& Adapt", 
          "The Campaign Manager updates the execution plan by reallocating resources, "
          "reprioritizing workflows, and adjusting control parameters to maintain budget constraints.")

    ]
    box_w, box_h, gap, y0 = 2.32, 5.0, 0.22, 1.55
    x0 = 0.55
    for i, (num, head, detail) in enumerate(steps):
        x = x0 + i * (box_w + gap)
        accent = CM_GOLD if i == 4 else CM_TEAL
        _rect(slide, Inches(x), Inches(y0), Inches(box_w), Inches(box_h), WHITE,
              line_color=RGBColor(0xD0, 0xDF, 0xE8), line_width=0.5)
        _rect(slide, Inches(x), Inches(y0), Inches(box_w), Inches(0.1), accent)
        _box(slide, Inches(x), Inches(y0 + 0.2), Inches(box_w), Inches(0.7),
             num, font_size=34, bold=True, color=accent, align=PP_ALIGN.CENTER)
        _box(slide, Inches(x + 0.1), Inches(y0 + 1.05), Inches(box_w - 0.2), Inches(0.9),
             head, font_size=15, bold=True, color=CM_NAVY, align=PP_ALIGN.CENTER)
        _box(slide, Inches(x + 0.12), Inches(y0 + 1.95), Inches(box_w - 0.24), Inches(1.9),
             detail, font_size=15, color=CM_SLATE, align=PP_ALIGN.CENTER)
        if i < len(steps) - 1:
            _rect(slide, Inches(x + box_w + 0.04), Inches(y0 + box_h / 2 - 0.04),
                  Inches(gap - 0.08), Inches(0.08), CM_TEAL)

    # Return arrow: box #5 (Replan & Adapt) → box #2 (Plan Execution).
    # Each step centre-x = x0 + i*(box_w+gap) + box_w/2.
    cx2 = x0 + 1 * (box_w + gap) + box_w / 2   # Plan Execution (i=1)
    cx5 = x0 + 4 * (box_w + gap) + box_w / 2   # Replan & Adapt (i=4)
    bot = y0 + box_h                            # box bottom
    ylo = bot + 0.34                            # horizontal run y
    # down stub under box 5
    _rect(slide, Inches(cx5 - 0.04), Inches(bot), Inches(0.08), Inches(0.34 + 0.04), CM_GOLD)
    # horizontal run from box5 back to box2
    _rect(slide, Inches(cx2), Inches(ylo), Inches(cx5 - cx2), Inches(0.08), CM_GOLD)
    # up stub into box 2
    _rect(slide, Inches(cx2 - 0.04), Inches(bot + 0.17), Inches(0.08), Inches(0.17 + 0.04), CM_GOLD)
    # arrowhead pointing up into box 2
    head = slide.shapes.add_shape(
        MSO_SHAPE.ISOSCELES_TRIANGLE, Inches(cx2 - 0.13), Inches(bot - 0.02),
        Inches(0.26), Inches(0.20))
    head.fill.solid(); head.fill.fore_color.rgb = CM_GOLD
    head.line.fill.background()
    # label below the return run
    _box(slide, Inches(cx2), Inches(ylo + 0.12), Inches(cx5 - cx2), Inches(0.3),
         "re-plan: results reshape the schedule", font_size=11, italic=True,
         color=CM_GOLD, align=PP_ALIGN.CENTER)

#     _box(slide, Inches(0.55), Inches(6.65), Inches(12), Inches(0.4),
#          "↺  continuous loop — the plan updates on every instance completion",
#          font_size=12, italic=True, color=CM_TEAL, align=PP_ALIGN.CENTER)


def slide_cm_adaptation(prs):
    """CM intro 3 — the four mechanisms that reshape the plan."""
    slide = prs.slides.add_slide(prs.slide_layouts[6])
    _bg(slide, CM_OFFWHITE)
    _cm_header(slide, "Campaign Manager: Adaptation Mechanisms")
    _box(slide, Inches(0.7), Inches(1.1), Inches(12), Inches(0.45),
         "Every completed workflow returns data that reshapes the execution plan:",
         font_size=15, italic=True, color=CM_SLATE)

#     mechs = [
#         ("Reprioritisation", "Each freed compute resource goes to the highest-value stage, "
#          "re-ranked every cycle  →  Scheduling Bandit.", CM_TEAL),
#         ("Dynamic Spawning", "Results spawn new downstream work, dispatched "
#          "best-first  →  Sharder.", CM_TEAL),
#         ("Flow Control", "Dispatch throttles back when a downstream queue floods, "
#          "and widens when it drains  →  Backpressure.", CM_GOLD),
#         ("Selective Execution", "A surrogate decides whether each candidate is worth "
#          "running — skip or fast-forward the confident ones  →  Triage + BudgetController.", CM_GOLD),
#     ]

    mechs = [
     ("Reprioritization",
          "Freed compute resources are continuously assigned to the highest-value workflow, "
          "with ranks updated each cycle.",
          CM_TEAL),

     ("Dynamic Spawning",
          "Completed results trigger generation of downstream work, dispatched "
          "in best-first order.",
          CM_TEAL),

     ("Flow Control",
          "Dispatch is throttled when downstream queues saturate and expanded as they drain.",
          CM_GOLD),

     ("Selective Execution",
          "A surrogate model evaluates whether each candidate should run, "
          "skipping or fast-tracking high-confidence cases.",
          CM_GOLD),
     ]
    
    pos = [(0.7, 1.7), (6.95, 1.7), (0.7, 4.25), (6.95, 4.25)]
    for (label, body, col), (x, y) in zip(mechs, pos):
        _rect(slide, Inches(x), Inches(y), Inches(5.9), Inches(2.35), WHITE,
              line_color=RGBColor(0xD0, 0xDF, 0xE8), line_width=0.5)
        _rect(slide, Inches(x), Inches(y), Inches(0.12), Inches(2.35), col)
        _box(slide, Inches(x + 0.3), Inches(y + 0.2), Inches(5.4), Inches(0.5),
             label, font_size=19, bold=True, color=CM_NAVY)
        _box(slide, Inches(x + 0.3), Inches(y + 0.8), Inches(5.4), Inches(1.4),
             body, font_size=16, color=CM_SLATE)

#     _box(slide, Inches(0.7), Inches(6.75), Inches(12), Inches(0.45),
#          "The rest of this deck benchmarks three of these as explicit optimisations on a "
#          "5-workflow drug-discovery pipeline.",
#          font_size=12.5, italic=True, color=CM_SLATE, align=PP_ALIGN.CENTER)


def slide_cm_architecture(prs):
    """High-level CM architecture — the main building blocks (Scheduler,
    Executor, Monitor, Resource Pool, Workflows), not the optional optimizers."""
    slide = prs.slides.add_slide(prs.slide_layouts[6])
    _bg(slide, CM_OFFWHITE)
    _cm_header(slide, "Campaign Manager: Core Components")

    def varrow(cx, y, h, color, label=None, lx=None):
        st = slide.shapes.add_shape(MSO_SHAPE.DOWN_ARROW,
                                    Inches(cx - 0.13), Inches(y), Inches(0.26), Inches(h))
        st.fill.solid(); st.fill.fore_color.rgb = color; st.line.fill.background()
        if label:
            _box(slide, Inches((lx if lx is not None else cx) + 0.15), Inches(y + h / 2 - 0.18),
                 Inches(3.0), Inches(0.36), label, font_size=10, italic=True, color=CM_SLATE)

    # 1) Plan / objective (top)
    _rect(slide, Inches(4.0), Inches(1.15), Inches(5.33), Inches(0.82), CM_NAVY)
    _box(slide, Inches(4.0), Inches(1.24), Inches(5.33), Inches(0.4),
         "Plan ", font_size=16, bold=True, color=WHITE, align=PP_ALIGN.CENTER)
    _box(slide, Inches(4.0), Inches(1.61), Inches(5.33), Inches(0.32),
         "workflows, resources, budget, dependencies",
         font_size=10, color=RGBColor(0xBB, 0xCC, 0xFF), align=PP_ALIGN.CENTER)
    varrow(6.665, 2.0, 0.32, CM_TEAL)

    # 2) Campaign Manager container — the core blocks
    cx0, cy0, cw, ch = 0.7, 2.4, 11.93, 3.45
    _rect(slide, Inches(cx0), Inches(cy0), Inches(cw), Inches(ch),
          RGBColor(0xEE, 0xF3, 0xF9), line_color=CM_NAVY, line_width=1.0)
    _box(slide, Inches(cx0 + 0.2), Inches(cy0 + 0.1), Inches(8), Inches(0.4),
         "Campaign Manager", font_size=15, bold=True, color=CM_NAVY)

    # Three core blocks (the mixins), each with the optional optimizers it drives.
    blocks = [
        ("Scheduler", "decides which workflows run next, and how many at once, "
         "based on their priority",
         "Batching · Flow control · Learning", "tunes how much work is dispatched"),
        ("Executor", "starts each instance, gives it resources, and passes "
         "results to the next workflow when it finishes",
         "Budget Controller · Surrogate Model", "skips confident candidates, keeps spend on budget"),
        ("Monitor", "periodic health checks, stall detection & drift "
         "alerts while the campaign runs",
         "Replanning controller", "reacts to drift events"),
    ]
    bw, bgap, bx0, by = 3.55, 0.22, cx0 + 0.25, cy0 + 0.6
    for i, (name, body, opt, optdesc) in enumerate(blocks):
        x = bx0 + i * (bw + bgap)
        # core block
        _rect(slide, Inches(x), Inches(by), Inches(bw), Inches(1.1), CM_TEAL)
        _box(slide, Inches(x), Inches(by + 0.08), Inches(bw), Inches(0.38),
             name, font_size=15, bold=True, color=WHITE, align=PP_ALIGN.CENTER)
        _box(slide, Inches(x + 0.16), Inches(by + 0.46), Inches(bw - 0.32), Inches(0.6),
             body, font_size=9, color=WHITE, align=PP_ALIGN.CENTER)
        # attached optional-optimizer chip (distinct orange accent)
        opt_c = RGBColor(0xD9, 0x6E, 0x12)
        oy = by + 1.18
        _rect(slide, Inches(x), Inches(oy), Inches(bw), Inches(0.72), WHITE,
              line_color=opt_c, line_width=1.25)
        _rect(slide, Inches(x), Inches(oy), Inches(bw), Inches(0.1), opt_c)
        _box(slide, Inches(x + 0.1), Inches(oy + 0.14), Inches(bw - 0.2), Inches(0.32),
             opt, font_size=10, bold=True, color=CM_NAVY, align=PP_ALIGN.CENTER)
        _box(slide, Inches(x + 0.1), Inches(oy + 0.44), Inches(bw - 0.2), Inches(0.26),
             optdesc, font_size=8, italic=True, color=CM_SLATE, align=PP_ALIGN.CENTER)

    # Resource Pool — shared state the blocks read/update
    rpy = by + 2.05
    _rect(slide, Inches(bx0), Inches(rpy), Inches(3 * bw + 2 * bgap), Inches(0.5), RGBColor(0xE3, 0xEC, 0xF4),
          line_color=CM_NAVY, line_width=0.75)
    _box(slide, Inches(bx0), Inches(rpy + 0.04), Inches(3 * bw + 2 * bgap), Inches(0.42),
         "Resource Pool   ·   workflow state & stats   (resources tracked, reserved, released)",
         font_size=10.5, bold=True, color=CM_NAVY, align=PP_ALIGN.CENTER)
    _box(slide, Inches(cx0 + cw - 3.0), Inches(cy0 + 0.12), Inches(2.85), Inches(0.32),
         "orange = optional optimizers", font_size=10, italic=True, bold=True,
         color=RGBColor(0xD9, 0x6E, 0x12), align=PP_ALIGN.RIGHT)

    # 3) Execution engine (bottom)
    eng_y = 6.45
    cm_bottom = cy0 + ch
    aw, ay, ah = 0.28, cm_bottom + 0.05, eng_y - (cm_bottom + 0.05) - 0.05

    # Down arrow (left of centre) — CM dispatches work to the engine
    dn = slide.shapes.add_shape(MSO_SHAPE.DOWN_ARROW,
                                Inches(6.1 - aw / 2), Inches(ay), Inches(aw), Inches(ah))
    dn.fill.solid(); dn.fill.fore_color.rgb = CM_TEAL; dn.line.fill.background()
    _box(slide, Inches(4.0), Inches(ay + ah / 2 - 0.18), Inches(1.9), Inches(0.36),
         "run instances", font_size=10, italic=True, color=CM_SLATE, align=PP_ALIGN.RIGHT)

    # Up arrow (right of centre) — results & metrics feed back into CM
    up = slide.shapes.add_shape(MSO_SHAPE.UP_ARROW,
                                Inches(7.25 - aw / 2), Inches(ay), Inches(aw), Inches(ah))
    up.fill.solid(); up.fill.fore_color.rgb = RGBColor(0xD9, 0x6E, 0x12); up.line.fill.background()
    _box(slide, Inches(7.55), Inches(ay + ah / 2 - 0.18), Inches(3.0), Inches(0.36),
         "results & metrics feedback", font_size=10, italic=True, color=RGBColor(0xD9, 0x6E, 0x12),
         align=PP_ALIGN.LEFT)

    _rect(slide, Inches(2.6), Inches(eng_y), Inches(8.13), Inches(0.82), CM_SLATE)
    _box(slide, Inches(2.6), Inches(eng_y + 0.08), Inches(8.13), Inches(0.38),
         "Workflows  →  Execution Engine", font_size=15, bold=True,
         color=WHITE, align=PP_ALIGN.CENTER)
    _box(slide, Inches(2.6), Inches(eng_y + 0.46), Inches(8.13), Inches(0.32),
         "Run as async tasks via RADICAL AsyncFlow / RHAPSODY — local or Dragon (HPC)",
         font_size=10, color=RGBColor(0xDD, 0xE6, 0xF0), align=PP_ALIGN.CENTER)


def slide_bandit_learning(prs):
    """Bandit learning curve — priorities redistribute from a uniform start."""
    slide = prs.slides.add_slide(prs.slide_layouts[6])
    _bg(slide, WHITE)
    _title_bar(slide, "Scheduling Bandit — Learning Priority From Scratch",
               "Starting from uniform priors, the bandit discovers downstream-first "
               "scheduling purely from the reward signal")

    _img(slide, PLOT_DIR / "6_bandit_convergence.png",
         Inches(0.15), Inches(1.15), Inches(8.7), Inches(5.6))

    RW, RX = Inches(4.3), Inches(8.95)
    _rect(slide, RX, Inches(1.15), RW, Inches(0.35), BANDIT_C)
    _box(slide, RX + Inches(0.08), Inches(1.21), RW - Inches(0.15), Inches(0.32),
         "What the curves show", font_size=12, bold=True, color=WHITE)

    points = [
        (BASELINE_C, "Uniform start",
         "All five workflows begin at priority 0.50 — the bandit has no built-in "
         "preference for any workflow."),
        (SHARD_C, "Reward drives learning",
         "Each finished instance is scored by how much its downstream still needs "
         "work; the terminal workflow always scores high."),
        (ALLOPT_C, "Priorities redistribute",
         "Downstream workflows climb toward ~0.8–0.95 as the bandit learns to feed the "
         "final stages, while initial screening is held near 0.5 so its 10 000 inputs "
         "don't starve the pipeline."),
        (TEAL_C, "No hand-tuning",
         "The downstream-first schedule emerges automatically — the same ordering "
         "the warm-start priors encode, but discovered from data."),
    ]
    for i, (col, title, body) in enumerate(points):
        y = Inches(1.65 + i * 1.32)
        _rect(slide, RX, y, RW, Inches(0.3), col)
        _box(slide, RX + Inches(0.08), y + Inches(0.02), RW - Inches(0.15), Inches(0.28),
             title, font_size=11, bold=True, color=WHITE)
        _box(slide, RX + Inches(0.08), y + Inches(0.33), RW - Inches(0.15), Inches(0.92),
             body, font_size=10, color=GRAY_TEXT)


def slide_title(prs):
    """Conceptual title slide — what the Campaign Manager is, no benchmark
    numbers or implementation specifics."""
    slide = prs.slides.add_slide(prs.slide_layouts[6])
    _bg(slide, WHITE)

    # Title band
    _rect(slide, Inches(0), Inches(0), SLIDE_W, Inches(2.6), ACCENT)
    _rect(slide, Inches(0), Inches(2.6), SLIDE_W, Inches(0.08), CM_GOLD)
    _box(slide, Inches(0.7), Inches(0.62), Inches(12), Inches(1.1),
         "Campaign Manager", font_size=46, bold=True,
         color=WHITE, align=PP_ALIGN.LEFT)
    _box(slide, Inches(0.72), Inches(1.78), Inches(12), Inches(0.7),
         "Adaptive, Objective-Driven Orchestration of Heterogeneous Scientific Workflows",
         font_size=19, color=RGBColor(0xBB, 0xCC, 0xFF), align=PP_ALIGN.LEFT)

    # Three conceptual word-chips — the design in three words.
    chips = [("Orchestrate", CM_TEAL),
             ("Adapt", CM_NAVY),
             ("Optimize for Objectives", CM_GOLD)]
    cw, gap, y0 = 3.8, 0.25, 4.1
    x0 = 0.72
    for i, (label, col) in enumerate(chips):
        x = Inches(x0 + i * (cw + gap))
        _rect(slide, x, Inches(y0), Inches(cw), Inches(1.5), col)
        _box(slide, x, Inches(y0 + 0.48), Inches(cw), Inches(0.6),
             label, font_size=22, bold=True, color=WHITE, align=PP_ALIGN.CENTER)


def slide_pipeline_overview(prs):
    """5-workflow drug-discovery pipeline — no redundant optimization-axis preview."""
    slide = prs.slides.add_slide(prs.slide_layouts[6])
    _bg(slide, WHITE)
    _title_bar(slide, "Antigen Prediction Workflow",
          "5-stage screening workflow — each screening "
          "progressively refines quality and filters candidates")

    workflows = [
        ("Initial\nScreening",     "~3,200 start\n(10,000 queued)", "#42a5f5", "score > 0.60"),
        ("Active\nLearning",       "~312 enter",                    "#66bb6a", "score > 0.65"),
        ("Structural\nModeling",   "~85 enter",                     "#ffa726", "score > 0.70"),
        ("Refinement\nSimulation", "~25 enter",                     "#ef5350", "score > 0.75"),
        ("Affinity\nRanking",      "5 hit target",                  "#ab47bc", "score > 0.80"),
    ]
    bw, bh, gap, x0, top = 1.95, 2.15, 0.62, 0.36, 1.55
    for i, (name, count, col, filt) in enumerate(workflows):
        x = x0 + i * (bw + gap)
        c = RGBColor(int(col[1:3], 16), int(col[3:5], 16), int(col[5:7], 16))
        _rect(slide, Inches(x), Inches(top), Inches(bw), Inches(bh), c)
     #    _box(slide, Inches(x), Inches(top + 0.1), Inches(bw), Inches(0.24),
     #         f"Workflow {i + 1}", font_size=9, bold=True,
     #         color=RGBColor(0xEE, 0xEE, 0xEE), align=PP_ALIGN.CENTER)
        _box(slide, Inches(x), Inches(top + 0.36), Inches(bw), Inches(0.6),
             name, font_size=13, bold=True, color=WHITE, align=PP_ALIGN.CENTER)
        _box(slide, Inches(x), Inches(top + 1.02), Inches(bw), Inches(0.5),
             count, font_size=10, color=WHITE, align=PP_ALIGN.CENTER)
        _box(slide, Inches(x), Inches(top + 1.56), Inches(bw), Inches(0.5),
             filt, font_size=9.5, italic=True,
             color=RGBColor(0xEE, 0xEE, 0xEE), align=PP_ALIGN.CENTER)
        # Flow arrow + label between consecutive workflows
        if i < len(workflows) - 1:
            ax = x + bw + 0.04
            aw = gap - 0.08
            ay = top + bh / 2 - 0.18
            arr = slide.shapes.add_shape(
                MSO_SHAPE.RIGHT_ARROW, Inches(ax), Inches(ay),
                Inches(aw), Inches(0.36))
            arr.fill.solid(); arr.fill.fore_color.rgb = ACCENT
            arr.line.fill.background()
            _box(slide, Inches(ax - 0.25), Inches(ay - 0.5), Inches(aw + 0.5), Inches(0.45),
                 "filter\n& rank", font_size=8, color=GRAY_TEXT, align=PP_ALIGN.CENTER)

    # Footnote: where counts come from
    _box(slide, Inches(0.25), Inches(4.1), Inches(12.8), Inches(0.22),
         "† Counts are averages from baseline benchmark runs (5 independent runs).  "
         "The campaign stops as soon as the 5th final lead is found — so most of the "
         "10,000 starting candidates are never run.  Counts vary by configuration; "
         "see the Cascade Funnel slide.",
         font_size=8.5, italic=True, color=GRAY_TEXT)

    # Adaptation in the use case — the four mechanisms from the previous slide,
    # illustrated on this antigen cascade.
    _rect(slide, Inches(0.25), Inches(4.35), Inches(12.85), Inches(2.8),
          RGBColor(0xF3, 0xF4, 0xFF))
    _box(slide, Inches(0.4), Inches(4.42), Inches(12.5), Inches(0.38),
         "How the Campaign Manager adapts this cascade  —  the four mechanisms in action",
         font_size=14, bold=True, color=ACCENT)

    # (title, header colour, bullets) — titles match slide 4's mechanism names.
    cols = [
        ("Reprioritization", CM_TEAL,
         ["Freed resources goes to the highest-value workflow each cycle.",
          "CM learns to start Affinity Ranking as soon as candidates arrive.",
          "Initial Screening is capped so it can't hog every GPU."]),
        ("Dynamic Spawning", CM_TEAL,
         ["Survivors of one workflow launch the next workflow's runs.",
          "Top-scoring candidates advance first.",
          "Work grows from results, not a fixed schedule."]),
        ("Flow Control", CM_GOLD,
         ["Dispatch throttles when a downstream queue backs up.",
          "It widens again once that queue drains.",
          "Keeps every workflow busy without flooding."]),
        ("Selective Execution", CM_GOLD,
         ["Surrogate scores each candidate: RUN / DISCARD / ADVANCE.",
          "Confident leads skip expensive calculations (ADVANCE).",
          "Budget controller keeps spend on plan."]),
    ]
    cw, step = 3.0, 3.2
    for ci, (title, hcol, bullets) in enumerate(cols):
        x = Inches(0.35 + ci * step)
        _rect(slide, x, Inches(4.83), Inches(cw), Inches(0.05), hcol)
        _box(slide, x, Inches(4.9), Inches(cw), Inches(0.32),
             title, font_size=11.5, bold=True, color=hcol)
        for bi, b in enumerate(bullets):
            _box(slide, x + Inches(0.08), Inches(5.32 + bi * 0.58), Inches(cw - 0.1), Inches(0.55),
                 f"• {b}", font_size=9, color=GRAY_TEXT)


def slide_spherical_architecture(prs, diag_dir: Path):
    """Merged architecture slide: CM class hierarchy + scheduler description."""
    slide = prs.slides.add_slide(prs.slide_layouts[6])
    _bg(slide, WHITE)
    _title_bar(slide, "SPHERICAL — System Architecture",
               "AsyncCampaignManager: mixin-based design with optional feature flags")

    _img(slide, diag_dir / "cm_architecture.png",
         Inches(0.15), Inches(1.1), Inches(8.7), Inches(5.9))

    # Right: Scheduler + key design notes
    _rect(slide, Inches(9.05), Inches(1.1), Inches(4.1), Inches(5.9),
          RGBColor(0xF5, 0xF5, 0xF5))
    _box(slide, Inches(9.15), Inches(1.15), Inches(3.9), Inches(0.38),
         "Scheduling Algorithm", font_size=13, bold=True, color=BLACK)

    sched_items = [
        (RGBColor(0x2E, 0x7D, 0x32), "Pass 1 — Fairness",
         "For every eligible group: allocate until running == concurrency_floor.  "
         "Highest priority first.  Prevents Initial Screening from monopolising all resources."),
        (ORANGE_C, "Pass 2 — Throughput",
         "After concurrency_floor satisfied: fill remaining capacity up to concurrency_cap.  "
         "Highest priority (or bandit-ranked) workflow gets extras first."),
        (BANDIT_C, "Bandit override",
         "When bandit=true: Thompson-sample Beta arm per workflow to replace "
         "static priority sort.  Learns downstream-first allocation."),
        (RGBColor(0x01, 0x57, 0x9B), "Dependency eligibility",
         "Group eligible when: deps called _signal_done()  OR  "
         "dep.finished_replicas ≥ dep_threshold."),
    ]
    for i, (col, title, body) in enumerate(sched_items):
        y = Inches(1.6 + i * 1.35)
        _rect(slide, Inches(9.05), y, Inches(4.1), Inches(0.3), col)
        _box(slide, Inches(9.1), y + Inches(0.02), Inches(4.0), Inches(0.28),
             title, font_size=10, bold=True, color=WHITE)
        _box(slide, Inches(9.1), y + Inches(0.33), Inches(4.0), Inches(0.88),
             body, font_size=9, color=GRAY_TEXT)

    _box(slide, Inches(0.15), Inches(7.1), Inches(13.1), Inches(0.3),
         "Feature flags: cm.features.sharder / backpressure / bandit / monitor — all disabled by default",
         font_size=9, italic=True, color=GRAY_TEXT, align=PP_ALIGN.CENTER)


def slide_stage_profiles(prs, diag_dir: Path):
    """NEW: Candidate ranking profiles used by the sharder."""
    slide = prs.slides.add_slide(prs.slide_layouts[6])
    _bg(slide, WHITE)
    _title_bar(slide, "Candidate Ranking Profiles",
               "The sharder scores candidates as: priority = Σ(weight × signal) — choose profile per campaign stage")

    # Width only — preserve the heatmap's native aspect (1.24:1) so it isn't stretched.
    _img(slide, diag_dir / "profiles.png",
         Inches(0.5), Inches(1.4), Inches(6.95))

    # Right panel: when to use each profile
    _rect(slide, Inches(8.35), Inches(1.1), Inches(4.8), Inches(5.9),
          RGBColor(0xF3, 0xF4, 0xFF))
    _box(slide, Inches(8.45), Inches(1.15), Inches(4.6), Inches(0.38),
         "When to use each profile", font_size=13, bold=True, color=ACCENT)

    profiles_guide = [
        (SHARD_C,   "pure_promise",
         "Best when upstream scores are reliable.\nPure quality routing — top candidates only.\nUsed in this benchmark (sharding+bp config)."),
        (BANDIT_C,  "active_learning",
         "Early on, when the model needs varied data.\nMaximises uncertainty reduction.\nTrades short-term quality for model accuracy."),
        (TEAL_C,    "explore_exploit",
         "Once the model is well-calibrated.\nBalances score, prediction, uncertainty.\nA sensible default for most campaigns."),
        (ALLOPT_C,  "diverse_top",
         "When you want a varied result set.\nQuality plus a novelty bonus.\nAvoids over-sampling one type of candidate."),
        (BASELINE_C, "round_robin",
         "When breadth matters more than quality.\nCycles evenly across candidate categories.\nIgnores score — maximises diversity."),
    ]
    for i, (col, name, desc) in enumerate(profiles_guide):
        y = Inches(1.6 + i * 1.07)
        _rect(slide, Inches(8.35), y, Inches(4.8), Inches(0.28), col)
        _box(slide, Inches(8.42), y + Inches(0.02), Inches(4.6), Inches(0.26),
             name, font_size=10, bold=True, color=WHITE)
        _box(slide, Inches(8.42), y + Inches(0.30), Inches(4.65), Inches(0.72),
             desc, font_size=9, color=GRAY_TEXT)

    _box(slide, Inches(0.15), Inches(7.1), Inches(13.1), Inches(0.3),
         "Profiles are configured per-group in the YAML.  The sharder evaluates all buffered candidates each dispatch cycle.",
         font_size=9, italic=True, color=GRAY_TEXT, align=PP_ALIGN.CENTER)


def slide_benchmark_design(prs):
    slide = prs.slides.add_slide(prs.slide_layouts[6])
    _bg(slide, WHITE)
    _title_bar(slide, "Evaluation of Adaptive Replanning",
               "Same workload, different adaptive mechanisms — 5 independent runs per configuration")

    configs = [
        ("baseline",          BASELINE_C, "Static plan",
         ["Pipelined FIFO — no quality routing", "No resource allocation learning",
          "Initial screening monopolises resources; downstream starved", "Result: 98 s ± 12 s"]),
        ("sharding",       SHARD_C,    "Quality Routing",
         ["Sharder: highest-score candidates dispatched first",
          "Backpressure: THROTTLE/WIDEN queue control",
          "concurrency_floor guarantees running slots",
          "Result: 17 s ± 2 s  (5.8× faster)"]),
        ("scheduling", BANDIT_C,   "Adaptive Scheduling",
         ["Thompson-sampling bandit per stage",
          "Learns downstream-first resource allocation",
          "No quality routing — FIFO dispatch",
          "Result: 28 s ± 11 s  (3.5× faster)"]),
        ("surrogate",         TEAL_C,     "Skip-on-Confident",
         ["Surrogate-driven ADVANCE for confident leads",
          "Candidate skips expensive compute when score is high",
          "BudgetController nudges cutoffs to stay on budget",
          "Result: 16 s ± 0.6 s  (6.1× faster)"]),
        ("all optimizations", ALLOPT_C,   "All Three Combined",
         ["Sharding + BP + sharding bandit (quality)",
          "Scheduling bandit (resource allocation)",
          "Surrogate + BudgetController (skip + budget constraint)",
          "Result: 7 s ± 0.3 s  (14× faster)"]),
    ]

    # 5 cards across — fits the slide width with small gaps.
    card_w = Inches(2.55)
    gap    = Inches(0.08)
    margin = Inches(0.15)
    for i, (name, color, tag, bullets) in enumerate(configs):
        x = margin + i * (card_w + gap)
        _rect(slide, x, Inches(1.8), card_w, Inches(0.45), color)
        _box(slide, x, Inches(1.82), card_w, Inches(0.42),
             f"{name}", font_size=12, bold=True, color=WHITE, align=PP_ALIGN.CENTER)
        _rect(slide, x, Inches(2.25), card_w, Inches(0.3),
              RGBColor(0xEE, 0xEE, 0xEE))
        _box(slide, x, Inches(2.26), card_w, Inches(0.3),
             tag, font_size=10, italic=True, color=GRAY_TEXT, align=PP_ALIGN.CENTER)
        _rect(slide, x, Inches(2.55), card_w, Inches(2.5),
              RGBColor(0xF8, 0xF8, 0xF8))
        for j, b in enumerate(bullets):
            bold_last = (j == len(bullets) - 1)
            col = color if bold_last else BLACK
            _box(slide, x + Inches(0.05), Inches(2.6 + j * 0.58),
                 card_w - Inches(0.10), Inches(0.55),
                 f"{'→' if bold_last else '•'} {b}",
                 font_size=10, bold=bold_last, color=col)

    _rect(slide, Inches(0.25), Inches(5.25), Inches(12.8), Inches(0.65),
          RGBColor(0xE3, 0xF2, 0xFD))
    _box(slide, Inches(0.35), Inches(5.30), Inches(12.5), Inches(0.55),
         "Setup:  10,000 initial candidates  ·  early termination when 5 high-quality leads found  "
         "·  same random seed per run index across all configs  ·  Concurrent AsyncFlow backend (no real GPU hardware)",
         font_size=11, color=RGBColor(0x0D, 0x47, 0xA1))

    _box(slide, Inches(0.25), Inches(6.02), Inches(12.8), Inches(0.75),
         "Note: 'baseline' runs a fixed execution plan — each workflow's concurrency floor/cap and "
         "priority are defined up front and never change. Workflows still overlap (a downstream "
         "workflow starts once its first upstream instance completes), but dispatch is first-come-"
         "first-served with no quality routing, learning, or runtime adaptation. This is a strong "
         "baseline, so the measured speedups are conservative.",
         font_size=11, italic=True, color=GRAY_TEXT)


def slide_cascade_funnel(prs):
    """Standalone cascade funnel — shows pipeline compute cost per config."""
    slide = prs.slides.add_slide(prs.slide_layouts[6])
    _bg(slide, WHITE)
    _title_bar(slide, "Cascade Funnel — Total Compute to Target",
               "Instances launched at each workflow to reach 5 final leads  ·  5 runs per config")

    _img(slide, PLOT_DIR / "3_cascade_funnel.png",
         Inches(0.15), Inches(1.70), Inches(8.7), Inches(5.0))

    # Right: key numbers
    _rect(slide, Inches(9.05), Inches(1.70), Inches(4.1), Inches(5.0),
          RGBColor(0xF5, 0xF5, 0xF5))
    _box(slide, Inches(9.15), Inches(1.75), Inches(3.9), Inches(0.38),
         "Total instances launched", font_size=12, bold=True, color=BLACK)

    funnel_stats = [
        (BASELINE_C,  "baseline",          "3,615 instances\nInitial Screening monopolises resources"),
        (SHARD_C,     "sharding",       "628 instances\n5.8× less compute"),
        (BANDIT_C,    "scheduling", "1,157 instances\n3.1× less compute"),
        (TEAL_C,      "surrogate",         "566 instances\n6.4× less compute"),
        (ALLOPT_C,    "all optimizations", "214 instances\n16.9× less compute"),
    ]
    for i, (col, name, stat) in enumerate(funnel_stats):
        y = Inches(2.17 + i * 0.90)
        _rect(slide, Inches(9.05), y, Inches(4.1), Inches(0.82), col)
        _box(slide, Inches(9.12), y + Inches(0.03), Inches(3.9), Inches(0.30),
             name, font_size=11, bold=True, color=WHITE)
        _box(slide, Inches(9.12), y + Inches(0.33), Inches(3.9), Inches(0.46),
             stat, font_size=12, bold=True, color=WHITE, align=PP_ALIGN.CENTER)

#     _box(slide, Inches(0.15), Inches(6.2), Inches(13.0), Inches(1.0),
#          "Left: stacked bars show absolute instance counts per config — w1 dominates baseline.  "
#          "Right: log scale reveals all 5 stages.  Sharding dispatches only the top-scoring ~20% of w1 "
#          "results downstream — drastically shrinking every subsequent stage.",
#          font_size=9.5, italic=True, color=GRAY_TEXT)


def slide_main_result(prs):
    slide = prs.slides.add_slide(prs.slide_layouts[6])
    _bg(slide, WHITE)
    _title_bar(slide, "Main Result — Wall Time to Target",
               "Time from campaign start until 5th final lead  ·  5 runs per config")

    _img(slide, PLOT_DIR / "1_wall_time.png",
         Inches(0.2), Inches(1.15), Inches(7.8), Inches(5.5))

    stats = [
        ("98 s",   "baseline",                       BASELINE_C),
        ("17 s",   "sharding\n5.8× faster",        SHARD_C),
        ("28 s",   "scheduling\n3.5× faster",  BANDIT_C),
        ("16 s",   "surrogate\n6.1× faster",          TEAL_C),
        ("7 s",    "all optimizations\n14× faster", ALLOPT_C),
    ]
    for i, (val, lbl, col) in enumerate(stats):
        y = Inches(1.2 + i * 1.13)
        _rect(slide, Inches(8.3), y, Inches(4.8), Inches(1.0), col)
        _box(slide, Inches(8.3), y + Inches(0.06), Inches(4.8), Inches(0.55),
             val, font_size=32, bold=True, color=WHITE, align=PP_ALIGN.CENTER)
        _box(slide, Inches(8.3), y + Inches(0.60), Inches(4.8), Inches(0.38),
             lbl, font_size=11, color=WHITE, align=PP_ALIGN.CENTER)

#     _box(slide, Inches(8.3), Inches(7.1), Inches(4.8), Inches(0.3),
#          "Lower is better  ·  white dots = individual runs",
#          font_size=9, italic=True, color=GRAY_TEXT, align=PP_ALIGN.CENTER)


def slide_sharding_bp(prs, diag_dir: Path = None):
    """Optimisation 1 — fully pptx-native layout (no embedded image)."""
    slide = prs.slides.add_slide(prs.slide_layouts[6])
    _bg(slide, WHITE)
    _title_bar(slide, "Optimisation 1 — Quality Routing",
               "Sharder + BackpressureNegotiator + Shard Bandit  ·  17 s ± 2 s  (5.8× faster)")

    # ── Left column: visual diagrams ──────────────────────────────────────

    # --- Data flow section label ---
    _rect(slide, Inches(0.15), Inches(1.18), Inches(8.55), Inches(0.28),
          RGBColor(0xE8, 0xF5, 0xE9))
    _box(slide, Inches(0.22), Inches(1.20), Inches(8.4), Inches(0.25),
         "SHARDER  —  upstream trigger  →  buffer  →  rank  →  dispatch",
         font_size=9.5, bold=True, color=RGBColor(0x1B, 0x5E, 0x20))

    # Data flow boxes (4 boxes + arrows)
    flow_y  = Inches(1.52)
    flow_h  = Inches(1.6)
    box_w   = Inches(1.88)
    arr_w   = Inches(0.36)
    gap     = arr_w
    flow_items = [
        (Inches(0.15), "Upstream Instance\nDone",    SHARD_C,
         "trigger(cid,\nscore, surr,\nuncertainty,\nscaffold)"),
        (Inches(0.15) + 2*(box_w + gap), "Ranking\nEngine",  BANDIT_C,
         "priority =\nΣ weight\n× signal\n(profile)"),
        (Inches(0.15) + (box_w + gap), "BUFFER",    RGBColor(0x1B, 0x5E, 0x20),
         "candidates that\npassed\nscore_threshold\ngate"),
        (Inches(0.15) + 3*(box_w + gap), "Queue", RGBColor(0x01, 0x57, 0x9B),
         "highest-score\ncandidates\ndispatched\nfirst"),
    ]
    for i, (x, name, col, sub) in enumerate(flow_items):
        _rect(slide, x, flow_y, box_w, flow_h, col)
        _box(slide, x + Inches(0.05), flow_y + Inches(0.07),
             box_w - Inches(0.1), Inches(0.44),
             name, font_size=11, bold=True, color=WHITE, align=PP_ALIGN.CENTER)
        _box(slide, x + Inches(0.05), flow_y + Inches(0.52),
             box_w - Inches(0.1), Inches(0.98),
             sub, font_size=9, color=RGBColor(0xDD, 0xFF, 0xDD), align=PP_ALIGN.CENTER)
        if i < 3:
            arr_x = x + box_w
            _box(slide, arr_x, flow_y + Inches(0.65), arr_w, Inches(0.35),
                 "▶", font_size=18, color=GRAY_TEXT, align=PP_ALIGN.CENTER)

    # Adaptive batch sizing note (below flow)
    _rect(slide, Inches(0.15), Inches(3.18), Inches(8.55), Inches(0.28),
          RGBColor(0x00, 0x60, 0x64))
    _box(slide, Inches(0.22), Inches(3.20), Inches(8.4), Inches(0.25),
         "Adaptive batch size: shard bandit tunes target_size (5 arms [0.5–1.5×]).  "
         "soft = partial batches OK;  strict = hold until full.",
         font_size=9, color=WHITE)

    # --- Backpressure state machine ---
    _rect(slide, Inches(0.15), Inches(3.55), Inches(8.55), Inches(0.28),
          RGBColor(0xFF, 0xE0, 0xB2))
    _box(slide, Inches(0.22), Inches(3.57), Inches(8.4), Inches(0.25),
         "BACKPRESSURE NEGOTIATOR  —  hysteresis state machine controlling dispatch rate",
         font_size=9.5, bold=True, color=RGBColor(0xE6, 0x51, 0x00))

    # Three state boxes
    BP_Y   = Inches(3.9)
    BP_H   = Inches(1.55)
    BP_W   = Inches(2.35)
    states = [
        (Inches(0.15),   "HOLD",     "normal operation",
         "dispatch proceeds\nat normal rate",    RGBColor(0x43, 0xA0, 0x47)),
        (Inches(2.97),   "THROTTLE", "queue ≥ high_water",
         "dispatch = 0\npipeline paused",         RGBColor(0xE5, 0x39, 0x35)),
        (Inches(5.8),    "WIDEN",    "queue ≤ low_water",
         "dispatch × mult\nqueue drained",        RGBColor(0x1E, 0x88, 0xE5)),
    ]
    for x, title, cond, action, col in states:
        _rect(slide, x, BP_Y, BP_W, BP_H, col)
        _box(slide, x + Inches(0.05), BP_Y + Inches(0.06),
             BP_W - Inches(0.1), Inches(0.38),
             title, font_size=14, bold=True, color=WHITE, align=PP_ALIGN.CENTER)
        _box(slide, x + Inches(0.05), BP_Y + Inches(0.44),
             BP_W - Inches(0.1), Inches(0.3),
             cond, font_size=8.5, italic=True,
             color=RGBColor(0xFF, 0xFF, 0xCC), align=PP_ALIGN.CENTER)
        _box(slide, x + Inches(0.05), BP_Y + Inches(0.78),
             BP_W - Inches(0.1), Inches(0.6),
             action, font_size=10, color=WHITE, align=PP_ALIGN.CENTER)

    # Transition arrows between states (text-based)
    _box(slide, Inches(2.52), BP_Y + Inches(0.58), Inches(0.45), Inches(0.4),
         "▶", font_size=18, color=RGBColor(0xE5, 0x39, 0x35), align=PP_ALIGN.CENTER)
    _box(slide, Inches(5.35), BP_Y + Inches(0.58), Inches(0.45), Inches(0.4),
         "▶", font_size=18, color=RGBColor(0x1E, 0x88, 0xE5), align=PP_ALIGN.CENTER)

    # Return path label
    _rect(slide, Inches(0.15), BP_Y + BP_H + Inches(0.05),
          Inches(8.55), Inches(0.3), RGBColor(0x43, 0xA0, 0x47))
    _box(slide, Inches(0.22), BP_Y + BP_H + Inches(0.07),
         Inches(8.4), Inches(0.25),
         "◀─── when queue returns to normal range, state resets to HOLD ───────────────────────",
         font_size=9, color=WHITE)

    # Config params
    _box(slide, Inches(0.15), BP_Y + BP_H + Inches(0.42), Inches(8.55), Inches(0.28),
         "Config:  backpressure_high (high_water mark)  ·  backpressure_low (low_water mark)  "
         "·  queue_depth = group.replicas − group.started_count",
         font_size=8.5, italic=True, color=GRAY_TEXT)

    # ── Right column: bullet descriptions ─────────────────────────────────
    RW = Inches(4.3)
    RX = Inches(8.95)

    _section_panel(
        slide, RX, Inches(1.18), RW, Inches(1.85),
        "Sharder",
        ["Buffer upstream trigger results (score, surrogate, uncertainty, scaffold)",
         "Rank candidates: priority = Σ(weight × signal) via ProfileWeights",
         "Dispatch best candidates to w2 first  (stratify=soft: partial batches allowed)",
         "Flush buffer when upstream workflow completes"],
        title_bg=SHARD_C,
        body_bg=RGBColor(0xE8, 0xF5, 0xE9),
        bullet_color=RGBColor(0x1B, 0x5E, 0x20),
        bullet_size=9.5,
    )

    _section_panel(
        slide, RX, Inches(3.13), RW, Inches(1.95),
        "BackpressureNegotiator",
        ["HOLD  → queue depth between thresholds → dispatch normal",
         "THROTTLE  → depth ≥ high_water → pause dispatch (return 0)",
         "WIDEN  → depth ≤ low_water → dispatch × multiplier (> 1)",
         "Hysteresis prevents rapid oscillation between states",
         "Config: backpressure_high / backpressure_low per group"],
        title_bg=RGBColor(0xE6, 0x51, 0x00),
        body_bg=RGBColor(0xFF, 0xF3, 0xE0),
        bullet_color=RGBColor(0x7F, 0x3B, 0x00),
        bullet_size=9.5,
    )

    _section_panel(
        slide, RX, Inches(5.18), RW, Inches(1.42),
        "Shard Bandit",
        ["Arms = [0.5, 0.75, 1.0, 1.25, 1.5]  (dispatch multipliers)",
         "Thompson-samples arm to adjust batch size each dispatch cycle",
         "Reward = throughput improvement over last window",
         "Learns optimal batch size for current pipeline state"],
        title_bg=BANDIT_C,
        body_bg=RGBColor(0xF3, 0xE5, 0xF5),
        bullet_color=RGBColor(0x4A, 0x14, 0x8C),
        bullet_size=9.5,
    )

    # Result callout
    _rect(slide, RX, Inches(6.7), RW, Inches(0.68), SHARD_C)
    _box(slide, RX + Inches(0.08), Inches(6.73), RW - Inches(0.15), Inches(0.28),
         "Result:  17 s ± 2 s  ·  5.8× faster wall time",
         font_size=11, bold=True, color=WHITE)
    _box(slide, RX + Inches(0.08), Inches(7.01), RW - Inches(0.15), Inches(0.28),
         "5.8× fewer total instances launched  (3,615 → 628)", font_size=11, color=WHITE)


def slide_bandit(prs):
    """Optimisation 2 — Scheduling Bandit: resource utilization + algorithm description."""
    slide = prs.slides.add_slide(prs.slide_layouts[6])
    _bg(slide, WHITE)
    _title_bar(slide, "Optimisation 2 — Adaptive Scheduling  (Thompson-sampling Bandit)",
               "Learns to allocate freed resources to the highest-value workflow  ·  28 s ± 11 s  (3.5× faster)")

    # Left: GPU utilization plot — full height
    _img(slide, PLOT_DIR / "4_gpu_utilization.png",
         Inches(0.15), Inches(1.15), Inches(8.6), Inches(5.85))

    # Right: Algorithm description
    RW = Inches(4.3)
    RX = Inches(8.95)

    _rect(slide, RX, Inches(1.15), RW, Inches(0.35), BANDIT_C)
    _box(slide, RX + Inches(0.08), Inches(1.21), RW - Inches(0.15), Inches(0.32),
         "Thompson Sampling  (1 arm per stage)", font_size=11, bold=True, color=WHITE)

    algo_steps = [
        "1.  Resource freed → collect eligible stages",
        "2.  Sample θᵢ ~ Beta(αᵢ, βᵢ) for each stage",
        "3.  Assign resource to workflow with highest θ",
        "4.  Instance runs → measure downstream BP state",
        "5.  Compute reward r ∈ [0,1] (see below)",
        "6.  Update posterior:  αᵢ += r,  βᵢ += (1-r)",
    ]
    _rect(slide, RX, Inches(1.5), RW, Inches(2.2), RGBColor(0xF3, 0xE5, 0xF5))
    for j, step in enumerate(algo_steps):
        _box(slide, RX + Inches(0.1), Inches(1.53 + j * 0.35), RW - Inches(0.15), Inches(0.34),
             step, font_size=9.5, color=RGBColor(0x4A, 0x14, 0x8C))

    # Reward signal
    _rect(slide, RX, Inches(4.05), RW, Inches(0.3), RGBColor(0x4A, 0x14, 0x8C))
    _box(slide, RX + Inches(0.08), Inches(4.11), RW - Inches(0.15), Inches(0.28),
         "Reward signal (from downstream BP state)", font_size=10, bold=True, color=WHITE)
    _rect(slide, RX, Inches(4.35), RW, Inches(1.05), RGBColor(0xED, 0xE7, 0xF6))
    for j, (label, r, col) in enumerate([
        ("THROTTLE (queue flooded)", "r = 0.2", RGBColor(0xC6, 0x28, 0x28)),
        ("HOLD (queue healthy)",      "r = 0.5-0.8", RGBColor(0x2E, 0x7D, 0x32)),
        ("WIDEN (queue drained)",     "r = 0.8", RGBColor(0x15, 0x65, 0xC0)),
    ]):
        _box(slide, RX + Inches(0.1), Inches(4.45 + j * 0.33), RW - Inches(0.2), Inches(0.3),
             f"• {label}  →  {r}", font_size=9.5, color=col)

    # Result callout
    _rect(slide, RX, Inches(5.75), RW, Inches(0.9), BANDIT_C)
    _box(slide, RX + Inches(0.08), Inches(5.85), RW - Inches(0.15), Inches(0.32),
         "Affinity Ranking first start:\nbaseline 30.1 s  →  bandit 15.4 s  (2×)", font_size=11,
         bold=True, color=WHITE)
    _box(slide, RX + Inches(0.08), Inches(6.25), RW - Inches(0.15), Inches(0.32),
         "Wall time 3.5× faster  ·  3.1× fewer total instances", font_size=11, color=WHITE)


def slide_all_opt(prs):
    slide = prs.slides.add_slide(prs.slide_layouts[6])
    _bg(slide, WHITE)
    _title_bar(slide, "Optimisation 4 — Combined  (all optimizations)",
               "Quality routing + adaptive scheduling + budget-adaptive surrogate gating — all three axes stacked")

    _img(slide, PLOT_DIR / "7_time_to_target.png",
         Inches(0.15), Inches(1.7), Inches(8.0), Inches(5.1))

    _rect(slide, Inches(8.3), Inches(1.7), Inches(4.8), Inches(5.1),
          RGBColor(0xFF, 0xEB, 0xEE))
    _box(slide, Inches(8.4), Inches(1.8), Inches(4.6), Inches(0.4),
         "Why all three together win", font_size=14, bold=True,
         color=RGBColor(0xB7, 0x1C, 0x1C))

    for j, (head, body) in enumerate([
        ("Sharder", "routes the best candidates to the next workflow first"),
        ("Scheduler",  "fills final-step workflow GPUs from t ≈ 1 s"),
        ("Surrogate", "skips compute on the most confident leads"),
    ]):
        y = Inches(2.3 + j * 0.62)
        _box(slide, Inches(8.45), y, Inches(4.5), Inches(0.55),
             f"• {head} — {body}", font_size=11.5, color=RGBColor(0xB7, 0x1C, 0x1C))

    _box(slide, Inches(8.45), Inches(4.25), Inches(4.55), Inches(0.95),
         "→ The best candidates reach a well-resourced final step workflow almost "
         "immediately — so all 5 leads land at the far left of the curve.",
         font_size=11, italic=True, color=RGBColor(0x7F, 0x14, 0x14))

    _rect(slide, Inches(8.3), Inches(5.4), Inches(4.8), Inches(1.0), ALLOPT_C)
    _box(slide, Inches(8.35), Inches(5.47), Inches(4.7), Inches(0.45),
         "14× faster · 17× less compute", font_size=20, bold=True,
         color=WHITE, align=PP_ALIGN.CENTER)
    _box(slide, Inches(8.35), Inches(5.95), Inches(4.7), Inches(0.38),
         "median 7 s ± 0.3 s — the most consistent config",
         font_size=11, color=WHITE, align=PP_ALIGN.CENTER)


def slide_budget_control(prs, diag_dir: Path):
    """Optimisation 3 — Surrogate gate with BudgetController.

    The surrogate gate ADVANCEs high-confidence candidates,
    letting workflows skip expensive compute.  BudgetController nudges
    the surrogate cutoffs to keep spend within the plan envelope.  Plot is
    generated from real benchmark_results.json by plot_budget_control.py.
    """
    slide = prs.slides.add_slide(prs.slide_layouts[6])
    _bg(slide, WHITE)
    _title_bar(slide, "Optimisation 3 — Surrogate  (skip compute on confident leads)",
               "Surrogate-driven ADVANCE skips expensive workflows ·  BudgetController keeps spend on track")

    # Plot is near-square (stacked panels), so it must be sized by HEIGHT to fit
    # the slide — width omitted's natural height would overflow.  ~5.8×5.9 at
    # 0.98:1, centred in the left zone (the right panel starts at 8.3).
    _img(slide, diag_dir / "budget_control_illustration.png",
         Inches(1.2), Inches(1.4), Inches(5.8))

    # Right column: explanation
    RX = Inches(8.30)
    RW = Inches(4.85)

    # The "story" panel
    _rect(slide, RX, Inches(1.15), RW, Inches(0.4), ACCENT)
    _box(slide, RX + Inches(0.10), Inches(1.18), RW - Inches(0.20), Inches(0.34),
         "Why the controller matters", font_size=12, bold=True, color=WHITE)
    _rect(slide, RX, Inches(1.55), RW, Inches(2.05),
          RGBColor(0xE3, 0xF2, 0xFD))
    story_lines = [
        ("Budget = hard constraint",
         "Planner sets a per-workflow budget envelope.  The CM never exceeds it."),
        ("Cutoff = soft lever",
         "Surrogate cutoffs (score, uncertainty) adapt within Planner-set bounds."),
        ("Burn ratio drives the loop",
         "burn_ratio = actual / (budget × progress).  Outside the band → nudge."),
        ("Escalates when stuck",
         "Bound-locked for K cycles → BUDGET_LOCKED drift → replan signal."),
    ]
    for i, (title, body) in enumerate(story_lines):
        y = Inches(1.62 + i * 0.50)
        _box(slide, RX + Inches(0.10), y, RW - Inches(0.20), Inches(0.24),
             f"• {title}", font_size=10, bold=True, color=RGBColor(0x0D, 0x47, 0xA1))
        _box(slide, RX + Inches(0.20), y + Inches(0.20), RW - Inches(0.30), Inches(0.26),
             body, font_size=9, color=RGBColor(0x0D, 0x47, 0xA1))

    # The control law panel
    _rect(slide, RX, Inches(3.70), RW, Inches(0.40), BANDIT_C)
    _box(slide, RX + Inches(0.10), Inches(3.73), RW - Inches(0.20), Inches(0.34),
         "How the controller adjusts (each cycle)", font_size=12, bold=True, color=WHITE)
    _rect(slide, RX, Inches(4.10), RW, Inches(1.40),
          RGBColor(0xF3, 0xE5, 0xF5))
    law_lines = [
        "1.  Compare spend so far against the plan.",
        "2.  Within the allowed band → leave the cutoff alone.",
        "3.  Spending too fast → raise the cutoff (run only the best).",
        "4.  Spending too slow → lower it (explore more widely).",
        "5.  Can't recover after several cycles → ask for a replan.",
    ]
    for i, line in enumerate(law_lines):
        _box(slide, RX + Inches(0.12), Inches(4.18 + i * 0.26),
             RW - Inches(0.24), Inches(0.25),
             line, font_size=9.5, color=RGBColor(0x4A, 0x14, 0x8C))

    # Result callout
    _rect(slide, RX, Inches(5.60), RW, Inches(0.40), ALLOPT_C)
    _box(slide, RX + Inches(0.10), Inches(5.63), RW - Inches(0.20), Inches(0.34),
         "What the controller delivers", font_size=12, bold=True, color=WHITE)
    _rect(slide, RX, Inches(6.00), RW, Inches(1.05),
          RGBColor(0xFF, 0xEB, 0xEE))
    deliverables = [
        "Spend stays in the planned band (both directions)",
        "Threshold relaxes when budget has slack → broader exploration",
        "Threshold tightens when burn is too fast → budget honoured",
        "Replan is triggered automatically when the envelope can't be held",
    ]
    for i, b in enumerate(deliverables):
        _box(slide, RX + Inches(0.12), Inches(6.05 + i * 0.24),
             RW - Inches(0.24), Inches(0.23),
             f"✓ {b}", font_size=10, color=RGBColor(0xB7, 0x1C, 0x1C))

#     _box(slide, Inches(0.15), Inches(7.1), Inches(13.1), Inches(0.3),
#          "Illustration uses real Triage + BudgetController; the cost-cutoff coupling is a heuristic "
#          "for visualisation.  Production wiring uses surrogate predictions + measured node-hours.",
#          font_size=9, italic=True, color=GRAY_TEXT, align=PP_ALIGN.CENTER)


def slide_gantt(prs):
    slide = prs.slides.add_slide(prs.slide_layouts[6])
    _bg(slide, WHITE)
    _title_bar(slide, "Workflows Overlap — Gantt View",
               "Average first-start to last-finish per stage; more overlap = better pipeline utilisation")

    _img(slide, PLOT_DIR / "2_pipeline_gantt.png",
         Inches(0.15), Inches(1.15), Inches(9.5), Inches(5.8))

    _rect(slide, Inches(9.8), Inches(1.15), Inches(3.3), Inches(5.8),
          RGBColor(0xF5, 0xF5, 0xF5))
#     _box(slide, Inches(9.9), Inches(1.2), Inches(3.1), Inches(0.4),
#          "What to look for", font_size=13, bold=True, color=BLACK)

    insights = [
        (BASELINE_C,  "baseline",
         "downstream workflows start late and stay starved (~98 s)."),
        (SHARD_C,     "sharding",
         "Quality routing sends the best candidates downstream first while backpressure paces dispatch — downstream workflows start within seconds (~17 s)."),
        (BANDIT_C,    "scheduling",
         "Bandit starts the final workflow earlier than baseline, but with no quality routing it still runs many candidates (~28 s)."),
        (TEAL_C,      "surrogate",
         "Surrogate ADVANCE lets confident leads skip expensive workflows, so fewer instances run and the cascade reaches 5 leads in ~16 s."),
        (ALLOPT_C,    "all optimizations",
         "All five workflows overlap from the start — campaign ends at ~7 s."),
    ]
    for j, (col, name, desc) in enumerate(insights):
        y = Inches(1.62 + j * 1.04)
        _rect(slide, Inches(9.85), y, Inches(0.18), Inches(0.26), col)
        _box(slide, Inches(10.1), y - Inches(0.02), Inches(2.9), Inches(0.30),
             name, font_size=11, bold=True, color=col)
        _box(slide, Inches(10.0), y + Inches(0.28), Inches(3.0), Inches(0.72),
             desc, font_size=9.5, color=GRAY_TEXT)


def slide_planner_execution(prs, diag_dir: Path):
    """Execution model — four plain-language concepts, no dense diagram panel."""
    slide = prs.slides.add_slide(prs.slide_layouts[6])
    _bg(slide, CM_OFFWHITE)
    _title_bar(slide, "How a Campaign Runs",
               "From YAML config to running instances — the plan adapts as work completes")

    cards = [
        ("You declare the plan",
         "Groups, their instance counts, resource budgets, priorities and "
         "dependencies — written in the config. This is the starting point.", TEAL_C),
        ("The engine executes",
         "Instances run as async tasks on the chosen backend — local for testing, "
         "Dragon for HPC — while CM keeps steering.", ALLOPT_C),
        ("Work grows with results",
         "As instances finish, their results can spawn new downstream work. How much "
         "work there is gets discovered at runtime, not fixed up front.", SHARD_C),
        ("Resources re-allocate live",
         "Each time a resource frees, CM re-decides which group should get it — "
         "driven by priorities, budgets and the learned bandit.", BANDIT_C),
    ]
    pos = [(0.7, 1.5), (6.95, 1.5), (0.7, 4.2), (6.95, 4.2)]
    for (title, body, col), (x, y) in zip(cards, pos):
        _rect(slide, Inches(x), Inches(y), Inches(5.9), Inches(2.45), WHITE,
              line_color=RGBColor(0xD0, 0xDF, 0xE8), line_width=0.5)
        _rect(slide, Inches(x), Inches(y), Inches(0.12), Inches(2.45), col)
        _box(slide, Inches(x + 0.3), Inches(y + 0.22), Inches(5.4), Inches(0.5),
             title, font_size=18, bold=True, color=CM_NAVY)
        _box(slide, Inches(x + 0.3), Inches(y + 0.9), Inches(5.4), Inches(1.4),
             body, font_size=13.5, color=CM_SLATE)


def slide_components(prs, diag_dir: Path):
    """How a campaign runs: the component diagram + the four-step narrative."""
    slide = prs.slides.add_slide(prs.slide_layouts[6])
    _bg(slide, CM_OFFWHITE)
    _title_bar(slide, "Running the Antigen Prediction — Step by Step",
               "Scheduler is the hub; optional modules guide each decision; results feed back")

    # Diagram on the left (scaled down to leave room for the narrative)
    _img(slide, diag_dir / "components.png",
         Inches(0.25), Inches(1.55), Inches(8.55))

    # Four-step narrative on the right — tied to the antigen cascade
    steps = [
        ("1  Declare the cascade",
         "The 5 workflows (Initial Screening → Affinity Ranking), their resources, "
         "priorities and dependencies are set in the config.", ACCENT),
        ("2  Best candidates advance",
         "Each workflow's top-scoring candidates are ranked and passed to the next "
         "workflow first, so quality flows down the cascade. Sharder also learns the optimal "
         "batch size to dispatch.", SHARD_C),
        ("3  Resources follow value",
         "Freed slots go to the highest-value workflow; "
         "a surrogate model lets confident candidates skip expensive calculations.", BANDIT_C),
        ("4  The engine executes",
         "Instances run as async tasks — local or HPC — while CM keeps steering "
         "toward the target leads.", ALLOPT_C),
    ]
    x = Inches(9.0)
    for i, (title, body, col) in enumerate(steps):
        y = Inches(1.55 + i * 1.40)
        _rect(slide, x, y, Inches(4.1), Inches(1.25), WHITE,
              line_color=RGBColor(0xD0, 0xDF, 0xE8), line_width=0.5)
        _rect(slide, x, y, Inches(0.1), Inches(1.25), col)
        _box(slide, x + Inches(0.22), y + Inches(0.1), Inches(3.7), Inches(0.4),
             title, font_size=13, bold=True, color=CM_NAVY)
        _box(slide, x + Inches(0.22), y + Inches(0.52), Inches(3.75), Inches(0.7),
             body, font_size=10.5, color=CM_SLATE)


def slide_methodology(prs):
    slide = prs.slides.add_slide(prs.slide_layouts[6])
    _bg(slide, WHITE)
    _title_bar(slide, "Methodology — What's Real vs What to Watch",
               "Verified findings and known caveats")

    confirmed = [
        "14× wall-time speedup is correctly measured (time from start to 5th w5 finish)",
        "Cascade funnel reduction (15× less total work) uses n_started — accurate",
        "Same random seed per run index ensures consistent score distributions across configs",
        "All 5 runs per config completed successfully (no timeouts or failures)",
        "all_optimizations has the lowest run-to-run variance (σ/mean = 12% vs 23% for baseline)",
    ]
    caveats = [
        "Baseline label says 'waterfall' but dep_threshold_override is commented out — "
        "baseline is actually pipelined FIFO, making speedups MORE conservative",
        "all_optimizations over-provisions w5: ~10 w5 instances start to find 5 hits "
        "(bandit warm-start Beta(5,1) is aggressive — 2× w5 waste)",
        "BP fractions show 100% WIDEN for all runs — queue never hit high-water mark "
        "(BP controlled dispatch rate but never fully throttled in these short runs)",
        "Runs use asyncio concurrent backend (no real GPU hardware) — timing models "
        "workflow durations with simulated sleep + jitter, not actual compute",
    ]

    _rect(slide, Inches(0.25), Inches(1.15), Inches(6.2), Inches(5.5),
          RGBColor(0xE8, 0xF5, 0xE9))
    _box(slide, Inches(0.35), Inches(1.2), Inches(5.9), Inches(0.4),
         "✓  Confirmed — results are real", font_size=13, bold=True,
         color=RGBColor(0x1B, 0x5E, 0x20))
    for j, txt in enumerate(confirmed):
        _box(slide, Inches(0.4), Inches(1.65 + j * 0.95), Inches(5.9), Inches(0.9),
             f"✓  {txt}", font_size=10.5, color=RGBColor(0x1B, 0x5E, 0x20))

    _rect(slide, Inches(6.7), Inches(1.15), Inches(6.4), Inches(5.5),
          RGBColor(0xFF, 0xF9, 0xC4))
    _box(slide, Inches(6.8), Inches(1.2), Inches(6.1), Inches(0.4),
         "⚠  Caveats — known limitations", font_size=13, bold=True,
         color=RGBColor(0xE6, 0x5C, 0x00))
    for j, txt in enumerate(caveats):
        _box(slide, Inches(6.85), Inches(1.65 + j * 1.22), Inches(6.1), Inches(1.1),
             f"⚠  {txt}", font_size=10.5, color=RGBColor(0x7F, 0x3B, 0x00))


def slide_recent_additions(prs):
    """Features added beyond the benchmark scope + roadmap.

    Goes between methodology and summary so readers see the full surface
    area of the CM, not just what these benchmark runs exercise.
    """
    slide = prs.slides.add_slide(prs.slide_layouts[6])
    _bg(slide, WHITE)
    _title_bar(slide, "Recent Additions — Beyond This Benchmark",
               "Features built into the CM but not exercised by these runs")

    # Header band — why this slide exists
    _rect(slide, Inches(0.25), Inches(1.18), Inches(12.85), Inches(0.55),
          RGBColor(0xE3, 0xF2, 0xFD))
    _box(slide, Inches(0.35), Inches(1.22), Inches(12.55), Inches(0.48),
         "These pieces extend the runtime beyond what the benchmark measures — "
         "structured plan validation, surrogate-driven candidate gating, and budget-adaptive "
         "thresholds.  Listed here so readers see the full surface area available for follow-on work.",
         font_size=11, color=RGBColor(0x0D, 0x47, 0xA1))

    # ── Implemented features (top row of cards) ──────────────────────────
    _rect(slide, Inches(0.25), Inches(1.85), Inches(12.85), Inches(0.32),
          RGBColor(0xE8, 0xF5, 0xE9))
    _box(slide, Inches(0.35), Inches(1.88), Inches(12.55), Inches(0.28),
         "✓ Implemented this session — in main, not yet benchmarked",
         font_size=11, bold=True, color=RGBColor(0x1B, 0x5E, 0x20))

    cards_top = [
        ("Structured Plan Schema", SHARD_C,
         ["Typed CampaignPlan: StageSpec, EdgeSpec, PilotSpec, SurrogateSpec",
          "Cross-reference validation: unique IDs, edges, deps resolve",
          "signature field — ready for Planner → CM trust handshake",
          "Back-compat: legacy flat workflows: dict still loads"]),
        ("Surrogate gate  (per-workflow)", BANDIT_C,
         ["RUN / ADVANCE / DISCARD on score + surrogate uncertainty",
          "Runs at trigger time — before any compute is spent",
          "Cutoffs nudgeable within plan-set bounds",
          "ADVANCE reserved for confident-high (off by default)"]),
        ("BudgetController (per-stage)", ALLOPT_C,
         ["Proportional feedback loop on burn_ratio vs plan budget",
          "Nudges surrogate-gate cutoffs to keep spend within ±band",
          "Clamped to plan-set nudge_bounds — never exceeds envelope",
          "Bound-locked → BUDGET_LOCKED drift → replan signal"]),
        ("Memory + CampaignState contract", TEAL_C,
         ["ResourcePool tracks total_memory_gb alongside cpus + gpus",
          "required_memory_gb per workflow; can_fit / allocate aware",
          "cm.state exposes plan, surrogate gates, controllers, BP, sharders",
          "Same field references — tests use structured names"]),
    ]
    for i, (title, col, bullets) in enumerate(cards_top):
        x = Inches(0.25 + i * 3.27)
        _rect(slide, x, Inches(2.25), Inches(3.1), Inches(0.38), col)
        _box(slide, x, Inches(2.27), Inches(3.1), Inches(0.35),
             title, font_size=11, bold=True, color=WHITE, align=PP_ALIGN.CENTER)
        _rect(slide, x, Inches(2.63), Inches(3.1), Inches(1.85),
              RGBColor(0xF8, 0xF8, 0xF8))
        for j, b in enumerate(bullets):
            _box(slide, x + Inches(0.08), Inches(2.68 + j * 0.42),
                 Inches(2.95), Inches(0.4),
                 f"• {b}", font_size=9, color=BLACK)

    # ── Roadmap (deferred) ──────────────────────────────────────────────
    _rect(slide, Inches(0.25), Inches(4.65), Inches(12.85), Inches(0.32),
          RGBColor(0xFF, 0xE0, 0xB2))
    _box(slide, Inches(0.35), Inches(4.68), Inches(12.55), Inches(0.28),
         "⌛ Roadmap — pieces still on the design list",
         font_size=11, bold=True, color=RGBColor(0xE6, 0x51, 0x00))

    roadmap = [
        ("ReplanningController",
         "Subscribes to BUDGET_LOCKED + other drift events.  Orchestrates "
         "DRIFT → DRAIN → RESUME handshake with the Planner."),
        ("Surrogate machinery",
         "Surrogate interface (predict_batch, update_with_results, redeploy).  "
         "Freezes BudgetController when recall drifts below the plan floor."),
        ("Aggregator (downstream gate)",
         "Streaming top-fraction quantile on the downstream side of each stage.  "
         "Today's gate runs only on the upstream score signal."),
        ("Retry policy execution",
         "Per-workflow RetryPolicy is in the schema; the executor doesn't act on "
         "max_attempts / backoff_s yet — failures are terminal."),
        ("Plan signing / verification",
         "signature field exists; sign / verify helpers and key management "
         "(Planner private key, CM public key) not yet implemented."),
        ("Persistent provenance",
         "CampaignMetrics is in-memory.  Production audit needs a "
         "Parquet / OpenLineage writer for replay-able lineage."),
    ]
    for i, (title, body) in enumerate(roadmap):
        row, col = i // 3, i % 3
        x = Inches(0.25 + col * 4.3)
        y = Inches(5.07 + row * 1.05)
        _rect(slide, x, y, Inches(4.1), Inches(0.32),
              RGBColor(0xE6, 0x51, 0x00))
        _box(slide, x + Inches(0.08), y + Inches(0.02),
             Inches(4.0), Inches(0.3),
             title, font_size=10, bold=True, color=WHITE)
        _rect(slide, x, y + Inches(0.32), Inches(4.1), Inches(0.65),
              RGBColor(0xFF, 0xF3, 0xE0))
        _box(slide, x + Inches(0.08), y + Inches(0.36),
             Inches(4.0), Inches(0.62),
             body, font_size=8.5, color=RGBColor(0x7F, 0x3B, 0x00))

    _box(slide, Inches(0.25), Inches(7.2), Inches(13.0), Inches(0.25),
         "Plan-side YAML drives every implemented field — budgets, cutoffs, "
         "nudge bounds, retry policy.  The Planner stays in charge of strategy; "
         "the CM stays in charge of tactics inside the plan-allowed envelope.",
         font_size=8.5, italic=True, color=GRAY_TEXT, align=PP_ALIGN.CENTER)


def slide_summary(prs):
    slide = prs.slides.add_slide(prs.slide_layouts[6])
    _bg(slide, WHITE)
    _rect(slide, Inches(0), Inches(0), SLIDE_W, Inches(1.1), ACCENT)
    _box(slide, Inches(0.3), Inches(0.12), Inches(12.5), Inches(0.85),
         "Summary", font_size=32, bold=True, color=WHITE)

    # 4 stat boxes + 4 takeaway cards share the same x-grid (centred on the slide).
    # Three headline numbers — all about the combined config's overall result.
    # (Per-axis gains live in the takeaway cards below, so no individual-axis box.)
    # Three headline numbers — unified navy cards with amber values (matches the
    # title bar); orange is reserved for the "Next step" call-to-action below.
    col_w, step = 3.05, 3.18
    stats = [
        ("14×",  "wall-time speedup\nvs baseline"),
        ("17×",  "less total compute\n(instances launched)"),
        ("7 s",  "median time-to-5-hits\n(±0.3 s)"),
    ]
    x0 = (13.33 - (len(stats) * col_w + (len(stats) - 1) * (step - col_w))) / 2
    for i, (val, lbl) in enumerate(stats):
        x = Inches(x0 + i * step)
        _rect(slide, x, Inches(1.45), Inches(col_w), Inches(1.5), ACCENT)
        _box(slide, x, Inches(1.55), Inches(col_w), Inches(0.85),
             val, font_size=34, bold=True, color=HIGHLIGHT, align=PP_ALIGN.CENTER)
        _box(slide, x, Inches(2.4), Inches(col_w), Inches(0.5),
             lbl, font_size=11, color=RGBColor(0xCC, 0xDD, 0xFF), align=PP_ALIGN.CENTER)

    # Next step — transition CM decision-making to the RADICAL agent layer
    # (moved up, just under the headline numbers — per-optimization detail removed).
    nx, nw = Inches(0.37), Inches(12.59)
    _rect(slide, nx, Inches(3.25), nw, Inches(0.5), ORANGE_C)
    _box(slide, nx + Inches(0.15), Inches(3.3), nw - Inches(0.3), Inches(0.42),
         "Next step", font_size=16, bold=True, color=WHITE)
    _rect(slide, nx, Inches(3.75), nw, Inches(1.35), RGBColor(0xFF, 0xF3, 0xE0))
    _box(slide, nx + Inches(0.25), Inches(3.95), nw - Inches(0.5), Inches(1.0),
         "Move the Campaign Manager's decision-making into the RADICAL Agentic "
         "Adaptive Decision Layer — allowing CM to take advantage of LLM models and improving "
         "adaptive decision capability.",
         font_size=16, color=RGBColor(0x7F, 0x3B, 0x00))


# ── Architecture diagram generators ──────────────────────────────────────────

DIAG_DIR = Path(__file__).parent / "plots" / "diagrams"

_BG   = "#F4F6FB"
_NAVY = "#1A237E"
_BLUE = "#1565C0"
_GRN  = "#2E7D32"
_PRP  = "#6A1B9A"
_ORG  = "#E65100"
_RED  = "#B71C1C"
_GRY  = "#37474F"
_LBL  = "#546E7A"
_TEAL = "#00838F"


def _fbox(ax, x, y, w, h, label, sublabel="", fc="#1565C0", tc="white",
          fs=11, sfs=8.5, radius=0.05, lw=1.5):
    ax.add_patch(FancyBboxPatch((x, y), w, h,
                                boxstyle=f"round,pad={radius}",
                                facecolor=fc, edgecolor="white", linewidth=lw, zorder=3))
    ly = y + h * (0.62 if sublabel else 0.5)
    ax.text(x + w / 2, ly, label, ha="center", va="center",
            fontsize=fs, fontweight="bold", color=tc, zorder=4)
    if sublabel:
        ax.text(x + w / 2, y + h * 0.28, sublabel, ha="center", va="center",
                fontsize=sfs, color=tc, alpha=0.88, zorder=4, fontstyle="italic")


def _arrow(ax, x0, y0, x1, y1, color="#555", lw=1.5, style="->"):
    ax.annotate("", xy=(x1, y1), xytext=(x0, y0),
                arrowprops=dict(arrowstyle=style, color=color,
                                lw=lw, connectionstyle="arc3,rad=0"))


def _label(ax, x, y, text, fs=9, color="#333", ha="center", va="center",
           bold=False, italic=False):
    ax.text(x, y, text, ha=ha, va=va, fontsize=fs, color=color,
            fontweight="bold" if bold else "normal",
            fontstyle="italic" if italic else "normal", zorder=5)


def _diag_save(fig, path, facecolor=_BG):
    fig.patch.set_facecolor(facecolor)
    plt.savefig(path, dpi=150, bbox_inches="tight", facecolor=facecolor)
    plt.close()


# ── Diagram: high-level component relations ───────────────────────────────────

def make_components_diagram(path: Path) -> None:
    """High-level block diagram: how Config, Scheduler, the feature modules
    (Sharder/BP, Scheduling Bandit, Surrogate), the engine, and the feedback
    loop relate."""
    _TEAL = "#B8860B"  # yellowish (was teal — too close to the green Sharder box)
    fig, ax = plt.subplots(figsize=(13.5, 7.5))
    ax.set_xlim(0, 13.5); ax.set_ylim(0, 7.5); ax.axis("off")

    # ── Decision modules that guide each scheduling cycle (top row) ──────────
    _fbox(ax, 0.5, 5.2, 3.6, 1.3, "Sharder + Backpressure",
          "buffer & rank candidates\nthrottle queue depth", fc=_GRN, fs=12, sfs=8.5)
    _fbox(ax, 4.9, 5.2, 3.6, 1.3, "Scheduling Bandit",
          "which workflow gets the\nnext freed resource", fc=_PRP, fs=12, sfs=8.5)
    _fbox(ax, 9.3, 5.2, 3.6, 1.3, "Surrogate + BudgetController",
          "RUN / DISCARD / ADVANCE\nnudge cutoffs to stay on budget", fc=_TEAL, fs=12, sfs=8.5)

    # ── Core: Scheduler (hub) ────────────────────────────────────────────────
    _fbox(ax, 4.6, 2.85, 4.3, 1.5, "SCHEDULER",
          "two-pass greedy, every state change\nPass 1 floor · Pass 2 cap", fc=_NAVY, fs=15, sfs=9)

    # ── Config → scheduler ───────────────────────────────────────────────────
    _fbox(ax, 0.4, 2.95, 3.3, 1.3, "Campaign Config", "workflows· deps\nresources · priorities",
          fc=_GRY, fs=12, sfs=8.5)
    _arrow(ax, 3.7, 3.6, 4.6, 3.6, color=_NAVY, lw=2.2)

    # modules guide the scheduler (down arrows into the hub)
    for mx in (2.3, 6.7, 11.1):
        _arrow(ax, mx, 5.2, mx if mx == 6.7 else 6.7, 4.35, color="#888", lw=1.6)
    #_label(ax, 6.75, 4.62, "guide each decision", fs=9, color="#666", italic=True)
    # Triage gates candidates before they reach the sharder buffer — a clean
    # hump that clears the Scheduling Bandit box top (6.5) without going through it.
    ax.annotate("", xy=(2.3, 6.55), xytext=(11.1, 6.55),
                arrowprops=dict(arrowstyle="->", color=_TEAL, lw=2.0,
                                connectionstyle="arc3,rad=-0.40"))
#     _label(ax, 6.7, 7.35, "gate candidates → Sharder buffer", fs=8.5,
#            color=_TEAL, italic=True)

    # ── Scheduler → engine → instances ───────────────────────────────────────
    _arrow(ax, 8.9, 3.6, 9.8, 3.6, color=_GRN, lw=2.4)
    #_label(ax, 9.35, 3.85, "create_task()", fs=8.5, color=_GRN, italic=True)
    _fbox(ax, 9.8, 2.85, 3.3, 1.5, "asyncflow Engine",
          "Concurrent (local)\nor Dragon (HPC)", fc="#00838F", fs=12, sfs=9)

    # ── Feedback loop (bottom) ───────────────────────────────────────────────
    _fbox(ax, 4.6, 0.55, 4.3, 1.3, "CandidateLog · Metrics · Monitor",
          "scores, rewards, drift signals", fc=_BLUE, fs=11, sfs=8.5)
    # engine results down into feedback store
    ax.annotate("", xy=(8.9, 1.2), xytext=(11.45, 2.85),
                arrowprops=dict(arrowstyle="->", color=_BLUE, lw=1.8,
                                connectionstyle="arc3,rad=0.25"))
    #_label(ax, 10.6, 1.9, "results", fs=8.5, color=_BLUE, italic=True)
    # feedback up to the decision modules: candidate scores → Sharder (left arc),
    # burn-rate / drift → Triage's BudgetController (right arc).
    ax.annotate("", xy=(1.5, 5.2), xytext=(4.6, 1.0),
                arrowprops=dict(arrowstyle="->", color=_BLUE, lw=1.8,
                                connectionstyle="arc3,rad=0.45"))
    #_label(ax, 0.55, 4.4, "scores →\nSharder", fs=8.5, color=_BLUE, italic=True, ha="left")
    # right feedback swings around the OUTSIDE (right) of the engine box to Triage
    ax.annotate("", xy=(12.6, 5.2), xytext=(8.9, 1.0),
                arrowprops=dict(arrowstyle="->", color=_BLUE, lw=1.8,
                                connectionstyle="arc3,rad=-0.55"))
#     _label(ax, 13.35, 3.0, "drift / burn →\nBudgetController", fs=8.5,
#            color=_BLUE, italic=True, ha="right")

    # Numbered badges tying each box to the 1–4 narrative steps on the slide.
    # Colours match the right-panel step accents (teal / green / purple / red).
    def _badge(cx, cy, n, color):
        ax.add_patch(plt.Circle((cx, cy), 0.30, facecolor=color,
                                 edgecolor="white", lw=2.0, zorder=20))
        ax.text(cx, cy, str(n), ha="center", va="center",
                fontsize=13, fontweight="bold", color="white", zorder=21)

    _badge(0.72, 4.25, 1, "#0F3C78")   # 1 → Campaign Config (declare the plan)
    _badge(0.82, 6.50, 2, "#4caf50")   # 2 → Sharder + Backpressure (best candidates advance)
    _badge(5.22, 6.50, 3, "#9c27b0")   # 3 → Scheduling Bandit (resources follow value)
    _badge(9.62, 6.50, 3, "#9c27b0")   # 3 → Surrogate gate (same step)
    _badge(10.12, 4.35, 4, "#f44336")  # 4 → asyncflow Engine (the engine executes)

    ax.set_title("Campaign Manager — How the Pieces Fit Together",
                 fontsize=15, fontweight="bold", color=_NAVY, pad=12)
    _diag_save(fig, path)


# ── Diagram 1: CM Architecture ────────────────────────────────────────────────

def make_cm_arch_diagram(path: Path) -> None:
    fig, ax = plt.subplots(figsize=(14, 8.5))
    ax.set_xlim(0, 14); ax.set_ylim(0, 8.5); ax.axis("off")

    _fbox(ax, 0.3, 7.0, 13.4, 1.2,
          "AsyncCampaignManager",
          "from_config(yaml, registry, asyncflow)  ·  start()  ·  wait()  ·  close()  ·  metrics()",
          fc=_NAVY, fs=18, sfs=10)

    mixin_specs = [
        ("SchedulerMixin", "Two-pass greedy scheduler\nPass 1: guarantee concurrency_floor\nPass 2: fill to concurrency_cap\nPriority / bandit ordering", _GRN,  0.3),
        ("ExecutorMixin",  "Instance lifecycle\nLaunch → monitor → complete\nGPU ID assignment\nEarly termination", _ORG, 4.85),
        ("MonitorMixin",   "Periodic health checks\nDrift detection\nStall alerting\nBP transitions", _PRP, 9.4),
    ]
    for name, desc, fc, x in mixin_specs:
        _fbox(ax, x, 4.8, 4.2, 2.0, name, desc, fc=fc, fs=13, sfs=9.5)
        _arrow(ax, x + 2.1, 7.0, x + 2.1, 6.8, color="white")

    struct_specs = [
        ("_WorkflowInfo",    "replicas · concurrency_floor/cap\npriority · dependencies · status\nrunning_count (derived)", "#01579B"),
        ("ResourcePool",     "total_cpus · total_gpus\ntotal_memory_gb\ncan_fit() / allocate() / release()", "#01579B"),
        ("CampaignMetrics",  "replica_events\nscheduling_events\nbp_fractions · shard_events", "#01579B"),
        ("BaseWorkflow",     "_signal_done()\n_trigger_dependent()\nrun()  or  start()", _GRY),
    ]
    for i, (name, desc, fc) in enumerate(struct_specs):
        x = 0.3 + i * 3.42
        _fbox(ax, x, 2.7, 3.1, 1.85, name, desc, fc=fc, fs=10.5, sfs=8.5)
        if i < 3:
            _arrow(ax, x + 1.55, 4.8, x + 1.55, 4.55, color="#aaa")

    ax.text(7.0, 2.55, "Core data structures", ha="center", va="center",
            fontsize=9, style="italic", color=_LBL)

    feat_specs = [
        ("Sharder",                 "Buffer → Rank → Dispatch\nAdaptive batch sizing\nShard Bandit dispatch multiplier"),
        ("BackpressureNegotiator",  "HOLD / THROTTLE / WIDEN\nHysteresis queue control\nThrottles dispatch rate"),
        ("SchedulingBandit",        "Thompson sampling\nCross-workflow GPU allocation\nBeta arm per stage"),
        ("CandidateLog",            "Sharder signal store\nScore / surrogate / uncertainty\nScaffold diversity for ranking"),
    ]
    for i, (name, desc) in enumerate(feat_specs):
        x = 0.3 + i * 3.42
        _fbox(ax, x, 0.4, 3.1, 1.9, name, desc, fc=_GRN, fs=10.5, sfs=8.5)

    ax.text(7.0, 0.22, "Optional features — enabled via feature flags in config YAML",
            ha="center", va="center", fontsize=9, style="italic", color=_GRN)

    for i, flag in enumerate(["sharder=true", "backpressure=true", "bandit=true", ""]):
        if flag:
            ax.text(0.3 + i * 3.42 + 1.55, 2.45, flag,
                    ha="center", va="center", fontsize=7.5, color=_GRN, style="italic",
                    bbox=dict(boxstyle="round,pad=0.2", fc="#E8F5E9", ec=_GRN, lw=0.8))

    ax.set_title("AsyncCampaignManager — Class Architecture", fontsize=14,
                 fontweight="bold", color=_NAVY, pad=6)
    _diag_save(fig, path)


# ── Diagram 2: Sharder + Backpressure ─────────────────────────────────────────

def make_sharder_diagram(path: Path) -> None:
    fig, ax = plt.subplots(figsize=(14, 8))
    ax.set_xlim(0, 14); ax.set_ylim(0, 8); ax.axis("off")

    _fbox(ax, 0.2, 4.5, 2.5, 2.4,
          "Upstream\nworkflow  (w1)",
          "on_replica_done()\n→ score, surr_pred,\n   surr_unc computed\n→ _trigger_dependent()", fc=_BLUE, fs=12, sfs=9)

    _arrow(ax, 2.7, 5.7, 3.2, 5.7, color=_GRN, lw=2)
    ax.text(2.95, 5.95, "trigger(candidate_id,\n  score, surr_pred,\n  surr_unc, scaffold)",
            ha="center", va="bottom", fontsize=7.5, color=_GRN)

    buf_fc = "#E8F5E9"
    ax.add_patch(FancyBboxPatch((3.2, 3.5), 2.8, 4.2,
                                boxstyle="round,pad=0.12",
                                facecolor=buf_fc, edgecolor=_GRN, linewidth=2, zorder=2))
    ax.text(4.6, 7.4, "BUFFER", ha="center", va="center",
            fontsize=13, fontweight="bold", color=_GRN, zorder=5)
    ax.text(4.6, 7.05, "candidate queue", ha="center", va="center",
            fontsize=9, color=_GRN, style="italic", zorder=5)

    for yi, (score, lbl) in enumerate([
        (0.94, "0.94"), (0.88, "0.88"), (0.81, "0.81"),
        (0.75, "0.75"), (0.68, "0.68"), (0.61, "0.61"),
    ]):
        y = 6.55 - yi * 0.48
        c = plt.cm.RdYlGn(score)
        ax.add_patch(plt.Circle((3.95, y), 0.21, color=c, zorder=6))
        ax.text(4.25, y, f"score={lbl}", va="center", fontsize=7.5, color=_GRY, zorder=6)

    _fbox(ax, 6.3, 4.5, 3.5, 2.4,
          "Ranking Engine",
          "priority =\nw_score × score\n+ w_surr × surr_pred\n+ w_unc × surr_unc\n+ w_age × age",
          fc=_PRP, fs=12, sfs=8.5)
    _arrow(ax, 6.0, 5.7, 6.3, 5.7, color=_PRP, lw=2)
    ax.text(6.15, 5.95, "dispatch()", ha="center", va="bottom", fontsize=8, color=_PRP)

    _fbox(ax, 10.1, 4.5, 3.0, 2.4,
          "Downstream\nworkflow  (w2)",
          "receives candidates\nin priority order\n(highest score first)\nvia _pending_candidates",
          fc=_BLUE, fs=12, sfs=9)
    _arrow(ax, 9.8, 5.7, 10.1, 5.7, color=_NAVY, lw=2)
    ax.text(9.95, 5.95, "priority-ranked\ninstances", ha="center", va="bottom",
            fontsize=7.5, color=_NAVY)

    _fbox(ax, 6.3, 2.3, 3.5, 1.9,
          "Adaptive Batch Sizing",
          "target_size = base × BP_multiplier\nstratify=soft: tail dispatch allowed\nstratify=strict: hold until full\nshard bandit learns multiplier",
          fc="#006064", fs=11, sfs=8)

    ax.add_patch(FancyBboxPatch((0.2, 0.3), 5.6, 3.8,
                                boxstyle="round,pad=0.1",
                                facecolor="#FFF9C4", edgecolor="#F57F21", linewidth=2, zorder=2))
    ax.text(3.0, 3.8, "BackpressureNegotiator", ha="center", va="center",
            fontsize=12, fontweight="bold", color="#E65100", zorder=5)

    states = [("HOLD\n(normal)", 1.0, 2.6, "#43A047"),
              ("THROTTLE\n(queue too deep)", 3.0, 2.6, "#E53935"),
              ("WIDEN\n(queue drained)", 5.0, 2.6, "#1E88E5")]
    for lbl, x, y, c in states:
        ax.add_patch(plt.Circle((x, y), 0.55, color=c, zorder=4))
        ax.text(x, y, lbl, ha="center", va="center", fontsize=7.5,
                fontweight="bold", color="white", zorder=5)

    for (x0, y0), (x1, y1), lbl, c in [
        ((1.55, 2.85), (2.45, 2.85), "q ≥ high_water", "#E53935"),
        ((3.55, 2.35), (4.45, 2.35), "q ≤ low_water",  "#1E88E5"),
        ((4.45, 2.85), (2.6,  2.85), "q in range → HOLD", "#43A047"),
    ]:
        _arrow(ax, x0, y0, x1, y1, color=c)
        ax.text((x0 + x1) / 2, (y0 + y1) / 2 + 0.18, lbl,
                ha="center", fontsize=7, color=c)

    for x, lbl in [(1.0, "dispatch\nnormal"), (3.0, "dispatch\n= 0"), (5.0, "dispatch\n× mult")]:
        ax.text(x, 1.8, lbl, ha="center", va="center", fontsize=7.5, color=_GRY,
                bbox=dict(boxstyle="round,pad=0.2", fc="white", ec="#ccc", lw=0.8))

    ax.text(3.0, 0.6, "queue_depth = group.replicas − group.started_count",
            ha="center", fontsize=8, color=_GRY, style="italic")

    _arrow(ax, 5.8, 2.6, 6.3, 2.8, color="#F57F21", lw=1.5)
    ax.text(6.1, 2.85, "BP state\n→ multiplier", ha="center", fontsize=7.5, color="#E65100")

    ax.set_title("Sharder Module — Buffering, Priority Ranking, and Dispatch Control",
                 fontsize=13, fontweight="bold", color=_NAVY, pad=6)
    _diag_save(fig, path)


# ── Diagram 3: Thompson-Sampling Bandit ───────────────────────────────────────

def make_bandit_diagram(path: Path) -> None:
    fig = plt.figure(figsize=(14, 9))
    fig.patch.set_facecolor(_BG)

    fig.text(0.5, 0.97, "Scheduling Bandit — Thompson Sampling per Stage",
             ha="center", va="top", fontsize=14, fontweight="bold", color=_NAVY)

    arm_specs = [
        ("w1\nInitial filter", 1, 1,  "#42a5f5", "Beta(1,1)\n(uniform prior)"),
        ("w2\nActive Learning",   2, 1,  "#66bb6a", "Beta(2,1)"),
        ("w3\nStructural Modeling",       3, 1,  "#ffa726", "Beta(3,1)"),
        ("w4\nRefinement Simulation",     4, 1,  "#ef5350", "Beta(4,1)"),
        ("w5\nAffinity Ranking",      5, 1,  "#ab47bc", "Beta(5,1)\n(warm-start: prefers w5)"),
    ]
    x_positions = np.linspace(0.06, 0.88, 5)
    ax_width, ax_height = 0.155, 0.26
    ax_y = 0.65

    xs = np.linspace(0.001, 0.999, 300)
    for i, (stage_lbl, a, b, color, prior_lbl) in enumerate(arm_specs):
        ax = fig.add_axes([x_positions[i], ax_y, ax_width, ax_height])
        log_pdf = (a - 1) * np.log(xs) + (b - 1) * np.log(1 - xs)
        pdf = np.exp(log_pdf - log_pdf.max())
        pdf = pdf / (np.trapz(pdf, xs) if hasattr(np, "trapz") else np.trapezoid(pdf, xs))
        ax.fill_between(xs, pdf, alpha=0.5, color=color)
        ax.plot(xs, pdf, color=color, linewidth=2)
        ax.set_xlim(0, 1); ax.set_ylim(0)
        ax.set_xlabel("θ (priority)", fontsize=7)
        ax.set_title(stage_lbl, fontsize=8, fontweight="bold", color=color, pad=2)
        ax.tick_params(labelsize=6)
        ax.text(0.5, ax.get_ylim()[1] * 0.75, prior_lbl,
                ha="center", fontsize=6.5, color=color, style="italic")
        sample_theta = a / (a + b)
        ax.axvline(sample_theta, color=color, linestyle="--", linewidth=1.5, alpha=0.8)
        ax.text(sample_theta, ax.get_ylim()[1] * 0.12, f"θ={sample_theta:.2f}",
                ha="center", fontsize=6, color=color,
                bbox=dict(boxstyle="round,pad=0.15", fc="white", ec=color, lw=0.8))

    main_ax = fig.add_axes([0.0, 0.0, 1.0, 1.0], facecolor="none")
    main_ax.set_xlim(0, 14); main_ax.set_ylim(0, 9); main_ax.axis("off")

    for xi in x_positions:
        main_ax.annotate("", xy=(xi * 14, 5.7), xytext=(xi * 14, 5.95),
                         arrowprops=dict(arrowstyle="->", color="#888", lw=1.2))

    _fbox(main_ax, 2.5, 4.95, 9.0, 0.65,
          "Sample θᵢ ~ Betaᵢ(αᵢ, βᵢ)  for each eligible stage",
          "Thompson sample — exploration/exploitation trade-off", fc="#37474F", fs=12, sfs=9)
    _arrow(main_ax, 7.0, 4.95, 7.0, 4.65, color="#555")

    _fbox(main_ax, 2.5, 4.0, 9.0, 0.65,
          "Rank workflows by θ  →  highest θ gets next freed resource",
          "deterministic tie-breaking by registration order", fc=_PRP, fs=12, sfs=9)
    _arrow(main_ax, 7.0, 4.0, 7.0, 3.7, color="#555")

    _fbox(main_ax, 2.5, 3.05, 9.0, 0.65,
          "Replica executes  →  on_replica_done  →  compute reward r ∈ [0, 1]",
          "", fc=_BLUE, fs=12)
    _arrow(main_ax, 7.0, 3.05, 7.0, 2.75, color="#555")

    reward_specs = [
        (1.8,  "THROTTLE\n(downstream queue full)", 0.2,  "#E53935"),
        (5.5,  "HOLD\n(queue healthy)",             "1 − 0.5×util", "#43A047"),
        (9.5,  "WIDEN\n(queue drained)",             0.8,  "#1E88E5"),
    ]
    for rx, lbl, r, c in reward_specs:
        main_ax.add_patch(FancyBboxPatch((rx - 1.5, 1.6), 3.0, 0.9,
                                         boxstyle="round,pad=0.08",
                                         facecolor=c, edgecolor="white", lw=1.5, zorder=3, alpha=0.9))
        main_ax.text(rx, 2.22, lbl, ha="center", va="center",
                     fontsize=9, fontweight="bold", color="white", zorder=4)
        main_ax.text(rx, 1.85, f"r = {r}", ha="center", va="center",
                     fontsize=9, color="white", style="italic", zorder=4)
        _arrow(main_ax, rx, 2.75, rx, 2.5, color=c)

    _fbox(main_ax, 2.5, 0.85, 9.0, 0.65,
          "Bayesian update:  αᵢ ← αᵢ + r    βᵢ ← βᵢ + (1 − r)",
          "positive reward → arm shifts right (higher priority in next sample)", fc=_GRN, fs=12, sfs=9)

    for rx in [1.8, 5.5, 9.5]:
        _arrow(main_ax, rx, 1.6, rx, 1.5, color="#888")
        _arrow(main_ax, rx, 1.5, 7.0, 1.5, color="#888")
    _arrow(main_ax, 7.0, 1.5, 7.0, 0.85, color="#888")

    main_ax.annotate("", xy=(0.5, 6.4), xytext=(0.5, 0.85),
                     arrowprops=dict(arrowstyle="->", color=_GRN, lw=2,
                                     connectionstyle="arc3,rad=0.0"))
    main_ax.text(0.18, 3.5, "update\nposterior", ha="center", va="center",
                 fontsize=9, color=_GRN, fontweight="bold", rotation=90)

    _diag_save(fig, path)


# ── Diagram 4: Candidate Profiles ─────────────────────────────────────────────

def make_profiles_diagram(path: Path) -> None:
    # Only the weight matrix — the per-profile "when to use" guidance lives in
    # the slide's right panel, so a "Use Cases" sub-panel here would duplicate it.
    fig, ax_heat = plt.subplots(figsize=(8.5, 7))
    fig.patch.set_facecolor(_BG)
    ax_heat.set_facecolor(_BG)

    profiles = ["pure_promise", "active_learning", "explore_exploit",
                "diverse_top", "round_robin"]
    weights = np.array([
        [1.0, 0.0, 0.0, 0.05, 0.0],
        [0.0, 0.0, 1.0, 0.05, 0.0],
        [0.5, 0.3, 0.4, 0.05, 0.0],
        [0.6, 0.0, 0.1, 0.05, 0.3],
        [0.0, 0.0, 0.0, 0.05, 1.0],
    ])
    signal_names = ["Score", "Surrogate", "Uncertainty", "Age", "Diversity"]
    row_colors   = ["#4caf50", "#9c27b0", "#00838f", "#f44336", "#78909c"]

    im = ax_heat.imshow(weights, cmap="YlGn", vmin=0, vmax=1.0, aspect="auto")
    ax_heat.set_xticks(range(5))
    ax_heat.set_xticklabels(signal_names, fontsize=11, fontweight="bold", color=_NAVY)
    ax_heat.set_yticks(range(5))
    ax_heat.set_yticklabels(profiles, fontsize=10.5, fontweight="bold")

    for tick_lbl, col in zip(ax_heat.get_yticklabels(), row_colors):
        tick_lbl.set_color(col)

    for i in range(5):
        for j in range(5):
            v = weights[i, j]
            txt_col = "white" if v > 0.55 else ("black" if v > 0.15 else "#aaaaaa")
            ax_heat.text(j, i, f"{v:.2f}", ha="center", va="center",
                         fontsize=12, fontweight="bold", color=txt_col)

    ax_heat.set_title("Weight Matrix  (greener = higher weight)",
                      fontsize=12, fontweight="bold", color=_NAVY, pad=10)
    plt.colorbar(im, ax=ax_heat, fraction=0.046, pad=0.04)

    plt.tight_layout(pad=2.5)
    _diag_save(fig, path)


# ── Diagram 5: Planner / Execution Model ──────────────────────────────────────

def make_planner_diagram(path: Path) -> None:
    fig, ax = plt.subplots(figsize=(14, 8.5))
    ax.set_xlim(0, 14); ax.set_ylim(0, 8.5); ax.axis("off")

    # ── Column 1: User inputs ────────────────────────────────────────────────
    ax.text(1.95, 8.3, "User Inputs", ha="center", fontsize=12,
            fontweight="bold", color=_NAVY)

    _fbox(ax, 0.2, 6.0, 3.5, 2.1,
          "Campaign Config  (YAML)",
          "workflows:\n  w1: replicas=10000, priority=10\n  w2: dependencies=[w1]\n       concurrency_floor=1\n  ...",
          fc=_BLUE, fs=11, sfs=8.5)

    _fbox(ax, 0.2, 3.5, 3.5, 2.2,
          "Workflow Classes",
          "class SimWorkflow(BaseWorkflow):\n  async def run(self, rid):\n    await do_work()\n    await self._signal_done()",
          fc=_GRY, fs=10, sfs=8)

    _fbox(ax, 0.2, 1.2, 3.5, 2.0,
          "Resource Spec",
          "engine: concurrent  # or dragon\ntotal_gpus: 4\ntotal_cpus: 128\nfeatures:\n  sharder: true",
          fc="#455A64", fs=10, sfs=8.5)

    # ── Arrows → CM ──────────────────────────────────────────────────────────
    for y_mid in [7.05, 4.6, 2.2]:
        _arrow(ax, 3.7, y_mid, 4.5, y_mid, color=_NAVY, lw=2)
    ax.text(4.1, 4.9, "from_config()", ha="center", fontsize=8.5,
            color=_NAVY, style="italic",
            bbox=dict(boxstyle="round,pad=0.2", fc=_BG, ec=_NAVY, lw=0.8))

    # ── Column 2: AsyncCampaignManager ───────────────────────────────────────
    ax.add_patch(FancyBboxPatch((4.5, 0.5), 5.2, 7.75,
                                boxstyle="round,pad=0.15",
                                facecolor="#E3F2FD", edgecolor=_NAVY,
                                linewidth=2.5, zorder=2))
    ax.text(7.1, 8.1, "AsyncCampaignManager", ha="center",
            fontsize=13, fontweight="bold", color=_NAVY)

    # Dependency graph box
    ax.add_patch(FancyBboxPatch((4.7, 5.65), 4.8, 2.4,
                                boxstyle="round,pad=0.1",
                                facecolor="#BBDEFB", edgecolor=_BLUE, linewidth=1.5, zorder=3))
    ax.text(7.1, 7.85, "Dependency Graph", ha="center",
            fontsize=9, fontweight="bold", color=_BLUE, zorder=4)

    stage_cols = ["#42a5f5", "#66bb6a", "#ffa726", "#ef5350", "#ab47bc"]
    stage_names = ["w1", "w2", "w3", "w4", "w5"]
    for i, (sn, sc) in enumerate(zip(stage_names, stage_cols)):
        nx = 5.1 + i * 0.95
        ax.add_patch(plt.Circle((nx, 6.85), 0.32, color=sc, zorder=5))
        ax.text(nx, 6.85, sn, ha="center", va="center",
                fontsize=9, fontweight="bold", color="white", zorder=6)
        if i < 4:
            _arrow(ax, nx + 0.32, 6.85, nx + 0.63, 6.85, color=_GRY, lw=1.5)
    ax.text(7.1, 6.2, "group.status: eligible → scheduling → running → done",
            ha="center", fontsize=7.5, color=_GRY, zorder=4, style="italic")

    # Scheduler box
    _fbox(ax, 4.7, 3.85, 4.8, 1.7,
          "Scheduler (per state change)",
          "1. Flush sharder buffers + refresh BP\n"
          "2. Collect eligible groups\n"
          "3. Pass 1: guarantee concurrency_floor\n"
          "4. Pass 2: fill to concurrency_cap",
          fc=_GRN, fs=10, sfs=8.5)

    # Optional features box
    _fbox(ax, 4.7, 2.15, 4.8, 1.5,
          "Optional Features (feature flags)",
          "sharder=true  ·  backpressure=true\nbandit=true  ·  monitor=true\nAll disabled by default",
          fc=_PRP, fs=10, sfs=8.5)

    # Metrics box
    _fbox(ax, 4.7, 0.65, 4.8, 1.3,
          "CampaignMetrics",
          "replica_events  ·  scheduling_events\nbp_fractions  ·  shard_events",
          fc=_GRY, fs=9.5, sfs=8)

    # ── Arrow → asyncflow ────────────────────────────────────────────────────
    _arrow(ax, 9.7, 4.5, 10.4, 4.5, color=_GRN, lw=2.5)
    ax.text(10.05, 4.85, "create_task()", ha="center", fontsize=8.5,
            color=_GRN, style="italic",
            bbox=dict(boxstyle="round,pad=0.2", fc=_BG, ec=_GRN, lw=0.8))

    # ── Column 3: asyncflow Engine ───────────────────────────────────────────
    ax.text(12.0, 8.3, "Execution Engine", ha="center", fontsize=12,
            fontweight="bold", color=_NAVY)

    _fbox(ax, 10.4, 5.5, 3.3, 2.7,
          "asyncflow\nWorkflowEngine",
          "ConcurrentBackend\n(asyncio — local)\n── or ──\nDragonBackend\n(HPC multi-node)",
          fc=_ORG, fs=12, sfs=9)

    _fbox(ax, 10.4, 3.1, 3.3, 2.2,
          "Running Instances",
          "SimWorkflow.run(replica_0)\nSimWorkflow.run(replica_1)\n...\n(up to concurrency_cap concurrent)",
          fc=_GRY, fs=10, sfs=8)

    _arrow(ax, 12.05, 5.5, 12.05, 5.3, color=_GRY, lw=2)

    _fbox(ax, 10.4, 1.2, 3.3, 1.75,
          "Results",
          "on_replica_done() callbacks\n_signal_done() / _trigger_dependent()\nCampaignMetrics updated",
          fc=_BLUE, fs=10, sfs=8.5)

    _arrow(ax, 12.05, 3.1, 12.05, 2.95, color=_GRY, lw=2)

    # Feedback arrow
    ax.annotate("", xy=(7.1, 3.85), xytext=(10.4, 1.8),
                arrowprops=dict(arrowstyle="->", color=_BLUE, lw=1.8,
                                connectionstyle="arc3,rad=-0.3"))
    ax.text(9.5, 2.6, "signal / trigger\nstate update", ha="center", fontsize=8,
            color=_BLUE, style="italic")

    ax.set_title("SPHERICAL — From Config to Execution  (asyncio event-loop model)",
                 fontsize=14, fontweight="bold", color=_NAVY, pad=6)
    _diag_save(fig, path)


def generate_diagrams() -> Path:
    DIAG_DIR.mkdir(parents=True, exist_ok=True)
    print("  Generating architecture diagrams...")
    make_sharder_diagram(DIAG_DIR / "sharder.png")
    print("    sharder.png")
    make_bandit_diagram(DIAG_DIR / "bandit.png")
    print("    bandit.png")
    make_profiles_diagram(DIAG_DIR / "profiles.png")
    print("    profiles.png")
    make_planner_diagram(DIAG_DIR / "planner.png")
    print("    planner.png")
    make_components_diagram(DIAG_DIR / "components.png")
    print("    components.png")
    return DIAG_DIR


# ── Assemble ──────────────────────────────────────────────────────────────────

def build(out_path: str) -> None:
    prs = Presentation()
    prs.slide_width  = SLIDE_W
    prs.slide_height = SLIDE_H

    diag_dir = generate_diagrams()

    print("Building slides...")
    n = 0
    def step(label):
        nonlocal n; n += 1; print(f"  {n:2}. {label}")

    # ── Part 1: What the Campaign Manager is ──────────────────────────────────
    slide_title(prs);                              step("Title")
    slide_cm_core_idea(prs);                       step("CM — The core idea")
    slide_cm_closed_loop(prs);                     step("CM — The closed loop")
    slide_cm_adaptation(prs);                      step("CM — How adaptation works")
    slide_cm_architecture(prs);                    step("CM — Conceptual architecture")

    # ── Part 2: The funnel pipeline in detail ─────────────────────────────────
    slide_pipeline_overview(prs);                  step("Funnel pipeline overview")
    slide_components(prs, diag_dir);               step("How a campaign runs")
    slide_benchmark_design(prs);                   step("Benchmark design")
    slide_cascade_funnel(prs);                     step("Cascade funnel")
    slide_main_result(prs);                        step("Main result (wall time)")
    slide_gantt(prs);                              step("Pipeline Gantt")
    slide_sharding_bp(prs, diag_dir);              step("Optimisation 1 — Sharder+BP")
    slide_stage_profiles(prs, diag_dir);           step("workflow profiles")
    slide_bandit(prs);                             step("Optimisation 2 — Scheduling bandit")
    slide_bandit_learning(prs);                    step("Bandit — learning curve")
    slide_budget_control(prs, diag_dir);           step("Optimisation 3 — Surrogate")
    slide_all_opt(prs);                            step("Optimisation 4 — Combined")
    slide_summary(prs);                            step("Summary")

    prs.save(out_path)
    print(f"\nSaved: {out_path}  ({len(prs.slides)} slides)")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--out", default="spherical_benchmark.pptx")
    args = parser.parse_args()
    build(args.out)
