"""Genera la carta Gantt y el tablero Kanban a partir de docs/plan.csv."""
from pathlib import Path
import pandas as pd
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
from matplotlib.patches import FancyBboxPatch

HERE = Path(__file__).parent
df = pd.read_csv(HERE / "plan.csv", parse_dates=["inicio", "fin"])
colors = {"Líder de proyecto": "#1f5f99", "Arquitecto cloud": "#6d4fb3",
          "Ingeniero de datos": "#2a9d8f", "Científico de datos": "#e76f51"}

# ---------- Gantt ----------
fig, ax = plt.subplots(figsize=(12, 6.2))
for i, r in df.iloc[::-1].reset_index(drop=True).iterrows():
    start, end = mdates.date2num(r.inicio), mdates.date2num(r.fin)
    ax.barh(i, end - start, left=start, color=colors[r.rol], alpha=1 if r.estado == "Hecho" else 0.35,
            edgecolor=colors[r.rol], hatch=None if r.estado == "Hecho" else "//", height=0.62)
    if isinstance(r.hito, str):
        ax.plot(end, i, marker="D", color="#111827", ms=6)
        ax.text(end + 0.0004, i, r.hito, va="center", fontsize=8, color="#111827")
ax.set_yticks(range(len(df)))
ax.set_yticklabels([f"{r.id}  {r.tarea}" for _, r in df.iloc[::-1].iterrows()], fontsize=8.5)
ax.xaxis.set_major_formatter(mdates.DateFormatter("%H:%M"))
ax.xaxis.set_major_locator(mdates.MinuteLocator(byminute=range(0, 60, 10)))
ax.grid(axis="x", alpha=.3)
ax.set_title("Carta Gantt — Pipeline Big Data NYC Taxi (19-10-2026)", fontsize=12, loc="left")
handles = [plt.Rectangle((0, 0), 1, 1, color=c) for c in colors.values()]
handles += [plt.Rectangle((0, 0), 1, 1, facecolor="#9ca3af", alpha=.35, hatch="//", edgecolor="#6b7280")]
ax.legend(handles, list(colors) + ["Pendiente"], loc="lower left", fontsize=8, ncol=5, frameon=False,
          bbox_to_anchor=(0, -0.13))
for s in ["top", "right"]:
    ax.spines[s].set_visible(False)
fig.tight_layout()
fig.savefig(HERE / "img" / "gantt.png", dpi=160)

# ---------- Kanban ----------
cols = ["Por hacer", "En progreso", "Hecho"]
fig, ax = plt.subplots(figsize=(12, 7.2))
ax.set_xlim(0, 3); ax.set_ylim(0, 1); ax.axis("off")
maxn = max(len(df[df.estado == c]) for c in cols)
h = min(0.075, 0.84 / max(maxn, 1) - 0.008)
for j, c in enumerate(cols):
    ax.add_patch(FancyBboxPatch((j + .02, .0), .96, .95, boxstyle="round,pad=0,rounding_size=.02",
                                fc="#f3f4f6", ec="#d1d5db"))
    sub = df[df.estado == c]
    ax.text(j + .5, .925, f"{c}  ({len(sub)})", ha="center", va="center", fontsize=11, weight="bold")
    for k, (_, r) in enumerate(sub.iterrows()):
        y = .87 - (k + 1) * (h + .006)
        ax.add_patch(FancyBboxPatch((j + .05, y), .9, h, boxstyle="round,pad=0,rounding_size=.01",
                                    fc="white", ec=colors[r.rol], lw=1.4))
        ax.add_patch(plt.Rectangle((j + .05, y), .012, h, color=colors[r.rol]))
        ax.text(j + .08, y + h * .62, f"{r.id} · " + (r.tarea if len(r.tarea) <= 50 else r.tarea[:49] + "…"), fontsize=7.4, va="center")
        ax.text(j + .08, y + h * .25, f"{r.rol} · {r.fase}", fontsize=6.6, va="center", color="#6b7280")
ax.set_title("Tablero Kanban (estado al cierre del informe)", loc="left", fontsize=12)
fig.tight_layout()
fig.savefig(HERE / "img" / "kanban.png", dpi=160)
print("ok")
