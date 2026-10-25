#!/usr/bin/env python3
"""Draw docs/figures/aucam-architecture.png (AuCaM components and data flow).

python docs/figures/src/architecture.py
"""

from pathlib import Path

import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt  # noqa: E402
from matplotlib.patches import FancyArrowPatch, FancyBboxPatch  # noqa: E402

OUT = Path(__file__).resolve().parents[1] / "aucam-architecture.png"

BOX, EDGE, TEXT, FRAME, LABEL = "#dbe9f6", "#4a6fa5", "#1f2d3d", "#8a99b0", "#3d4b5c"
# Outside AuCaM: drivers and the execution engine.
EXT_BOX, EXT_EDGE = "#eceff1", "#7b8794"

fig, ax = plt.subplots(figsize=(16, 11))
ax.set_xlim(0, 16)
ax.set_ylim(0, 11.2)
ax.axis("off")


def frame(x0, y0, x1, y1, title, fc):
    ax.add_patch(
        FancyBboxPatch(
            (x0, y0),
            x1 - x0,
            y1 - y0,
            boxstyle="round,pad=0.02,rounding_size=0.15",
            fc=fc,
            ec=FRAME,
            lw=1.2,
            ls="--",
        )
    )
    ax.text(
        x0 + 0.2, y1 - 0.25, title, ha="left", va="top", fontsize=13, weight="bold", color=LABEL
    )


def box(cx, cy, w, h, title, sub="", fc=BOX, ec=EDGE):
    ax.add_patch(
        FancyBboxPatch(
            (cx - w / 2, cy - h / 2),
            w,
            h,
            boxstyle="round,pad=0.02,rounding_size=0.12",
            fc=fc,
            ec=ec,
            lw=1.5,
        )
    )
    ax.text(
        cx,
        cy + (0.17 if sub else 0),
        title,
        ha="center",
        va="center",
        fontsize=13,
        weight="bold",
        color=TEXT,
    )
    if sub:
        ax.text(cx, cy - 0.25, sub, ha="center", va="center", fontsize=10.5, color=TEXT)
    return {"l": cx - w / 2, "r": cx + w / 2, "t": cy + h / 2, "b": cy - h / 2, "x": cx, "y": cy}


def arrow(p, q, text="", tx=None, ty=None, both=False, rad=0.0, ha="center"):
    ax.add_patch(
        FancyArrowPatch(
            p,
            q,
            arrowstyle="<|-|>" if both else "-|>",
            mutation_scale=16,
            lw=1.4,
            color=LABEL,
            connectionstyle=f"arc3,rad={rad}",
            shrinkA=2,
            shrinkB=2,
        )
    )
    if text:
        ax.text(
            tx if tx is not None else (p[0] + q[0]) / 2,
            ty if ty is not None else (p[1] + q[1]) / 2,
            text,
            ha=ha,
            va="center",
            fontsize=10.5,
            style="italic",
            color=LABEL,
            bbox={"fc": "white", "ec": "none", "pad": 1.5},
        )


def path(points, text="", tx=None, ty=None, ha="center"):
    xs, ys = zip(*points, strict=True)
    ax.plot(xs[:-1], ys[:-1], color=LABEL, lw=1.4)
    arrow(points[-2], points[-1])
    if text:
        ax.text(
            tx,
            ty,
            text,
            ha=ha,
            va="center",
            fontsize=10.5,
            style="italic",
            color=LABEL,
            bbox={"fc": "white", "ec": "none", "pad": 1.5},
        )


drv = box(8, 10.45, 5.4, 0.9, "Drivers", "decide which campaigns to run", fc=EXT_BOX, ec=EXT_EDGE)

frame(0.4, 2.55, 15.6, 9.55, "CampaignManager (AuCaM)", "#f7f9fc")
db = box(3.0, 8.45, 4.0, 1.0, "DataBackend", "Local · Redis · Dragon")
di = box(8, 8.45, 3.9, 1.0, "Driver interface", "admit or reject · return outcomes")
ba = box(
    13.15,
    8.45,
    4.1,
    1.0,
    "BudgetAllocator",
    "spending ledger: limits · spent · in-flight",
)

frame(1.0, 2.9, 15.0, 7.45, "", "#ffffff")
ax.text(14.8, 7.2, "Campaign 1 … N", ha="right", va="top", fontsize=13, weight="bold", color=LABEL)
pl = box(4.0, 6.2, 3.0, 1.1, "Policy loop", "each interval and completion")
cc = box(9.0, 6.2, 4.2, 1.1, "CampaignController", "observe · decide (rules, code, LLM, ADR)")
q = box(5.4, 3.85, 2.6, 1.1, "WorkflowQueue", "priority · aging")
sc = box(12.4, 3.85, 3.0, 1.1, "Scheduler", "+ scheduling policy")

ex = box(
    8,
    1.25,
    11.0,
    1.1,
    "AsyncFlow engine on RHAPSODY",
    "each workflow = one block; its tasks run on the HPC allocation",
    fc=EXT_BOX,
    ec=EXT_EDGE,
)

arrow(
    (drv["x"] - 0.6, drv["b"]),
    (di["x"] - 0.6, di["t"]),
    "submit(spec)",
    tx=drv["x"] - 0.65,
    ha="right",
)
arrow(
    (di["x"] + 0.6, di["t"]),
    (drv["x"] + 0.6, drv["b"]),
    "CampaignOutcome",
    tx=drv["x"] + 0.65,
    ha="left",
)
arrow((di["r"], di["y"]), (ba["l"], ba["y"]), "admission", both=True)
arrow((4.0, db["b"]), (4.0, pl["t"]), "latest reports", tx=4.1, ha="left")
arrow((pl["r"], pl["y"] + 0.2), (cc["l"], cc["y"] + 0.2), "observation", ty=pl["y"] + 0.48)
arrow((cc["l"], cc["y"] - 0.2), (pl["r"], pl["y"] - 0.2), "PolicyResult", ty=pl["y"] - 0.48)
arrow((4.6, pl["b"]), (q["x"] - 0.3, q["t"]), "launch requests", tx=5.15, ty=5.1, ha="left")
arrow((q["r"], q["y"]), (sc["l"], sc["y"]))
arrow(
    (sc["x"] - 0.6, sc["t"]),
    (sc["x"] - 0.6, ba["b"]),
    "limit check",
    tx=sc["x"] - 0.5,
    ty=6.6,
    both=True,
    ha="left",
)
arrow((2.9, pl["b"]), (2.9, ex["t"]), "cancel", tx=2.8, ty=4.6, ha="right")
arrow((sc["x"], sc["b"]), (sc["x"], ex["t"]), "launch", tx=sc["x"] + 0.1, ty=2.3, ha="left")
path(
    [(ex["l"], ex["y"]), (0.7, ex["y"]), (0.7, db["y"]), (db["l"], db["y"])],
    "reports:\nscore, metrics",
    tx=0.78,
    ty=4.6,
    ha="left",
)
path(
    [(ex["r"], ex["y"]), (15.35, ex["y"]), (15.35, ba["y"]), (ba["r"], ba["y"])],
    "task\ncharges",
    tx=15.27,
    ty=4.6,
    ha="right",
)

fig.tight_layout()
fig.savefig(OUT, dpi=150, facecolor="white")
print(f"figure → {OUT}")
