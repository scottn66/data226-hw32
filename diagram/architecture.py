"""HW 3.2 architecture diagram (writeup section 1). Run: python3 architecture.py -> architecture.png

Same style as the Nov 22 original: Airflow tasks on top, Snowflake objects below, dashed arrows for
"which task touches which object", solid arrows for data flow. Updated Nov 24 for the current DAG.
Box widths come from the measured text, and the script warns if any text spills out of its box.
"""
from pathlib import Path

import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt  # noqa: E402
from matplotlib.patches import FancyBboxPatch  # noqa: E402

W, H = 2950, 1225                 # drawing units; 130 units per inch
Y0 = 60                           # bottom of the visible area (crops the empty band)
FIG_W, DPI = 22.65, 150
OUT = Path(__file__).with_name("architecture.png")

# Colors taken from the original figure.
AIR_EDGE, AIR_FILL, AIR_TITLE = "#1C7ED6", "#EEF5FC", "#0B5CAD"
SF_EDGE, SF_FILL, SF_TITLE = "#29B5E8", "#F0F9FD", "#1479A6"
DBT_EDGE, DBT_FILL = "#F26B4F", "#FFF2EE"
CSV_EDGE, CSV_FILL = "#D97706", "#FFF8E7"
INK, BODY, BLUE, GRAY = "#1F2937", "#374151", "#1565C0", "#6B7280"
TESTS_INK, CLONES_INK = "#9A3412", "#0E7490"

fig = plt.figure(figsize=(FIG_W, FIG_W * (H - Y0) / W), dpi=DPI)
ax = fig.add_axes([0, 0, 1, 1])
ax.set_xlim(0, W)
ax.set_ylim(Y0, H)
ax.axis("off")
renderer = fig.canvas.get_renderer()
PT = W / (FIG_W * 72)             # drawing units per typographic point
checks = []                       # (text artist, (x0, y0, x1, y1) it must stay inside)


def text_width(s, size, weight="normal", style="normal"):
    t = ax.text(0, 0, s, fontsize=size, fontweight=weight, fontstyle=style)
    w = t.get_window_extent(renderer).width * W / (FIG_W * DPI)
    t.remove()
    return w


def line(s, size=12.5, weight="normal", color=BODY, style="normal", gap_after=0.0):
    return dict(s=s, size=size, weight=weight, color=color, style=style, gap_after=gap_after)


def title(s, size=15):
    return line(s, size=size, weight="bold", color=INK, gap_after=0.45)


def content_width(lines):
    return max(text_width(l["s"], l["size"], l["weight"], l["style"]) for l in lines)


def draw_box(x0, y0, x1, y1, lines, edge, fill, lw=2.4, dashed=False, zorder=2):
    ax.add_patch(FancyBboxPatch((x0, y0), x1 - x0, y1 - y0,
                                boxstyle="round,pad=0,rounding_size=16", lw=lw, ec=edge, fc=fill,
                                ls=(0, (6, 3)) if dashed else "-", zorder=zorder))
    heights = [l["size"] * PT * 1.28 + l["gap_after"] * l["size"] * PT for l in lines]
    y = (y0 + y1) / 2 + sum(heights) / 2
    for l, h in zip(lines, heights):
        y_mid = y - l["size"] * PT * 1.28 / 2
        t = ax.text((x0 + x1) / 2, y_mid, l["s"], ha="center", va="center", fontsize=l["size"],
                    fontweight=l["weight"], color=l["color"], fontstyle=l["style"], zorder=zorder + 4)
        checks.append((t, (x0, y0, x1, y1)))
        y -= h
    return dict(x0=x0, y0=y0, x1=x1, y1=y1)


def layout_row(specs, x_start, x_end, min_gap, pad=22):
    """Widths from measured text; leftover space is shared by the boxes in proportion to their width."""
    widths = [content_width(s["lines"]) + 2 * pad for s in specs]
    spare = (x_end - x_start) - sum(widths) - min_gap * (len(specs) - 1)
    print("row widths:", [round(w) for w in widths], "spare:", round(spare))
    if spare < 0:
        print(f"WARNING: row overflows by {-spare:.0f} units")
    widths = [w + max(spare, 0) * w / sum(widths) for w in widths]
    xs, x = [], x_start
    for w in widths:
        xs.append((x, x + w))
        x += w + min_gap
    return xs


def at(box, fx, side):
    """Point on a box edge: fx = 0..1 along the width, side = 'top' or 'bottom'."""
    return (box["x0"] + fx * (box["x1"] - box["x0"]), box["y1"] if side == "top" else box["y0"])


def solid_arrow(p0, p1, label=None, label_lines=None):
    ax.annotate("", xy=p1, xytext=p0, zorder=5,
                arrowprops=dict(arrowstyle="-|>", color=BODY, lw=2.4, shrinkA=0, shrinkB=0,
                                mutation_scale=20))
    texts = label_lines or ([label] if label else [])
    for i, s in enumerate(reversed(texts)):
        ax.text((p0[0] + p1[0]) / 2, p0[1] + 16 + i * 25, s, ha="center", va="bottom",
                fontsize=10.5, fontstyle="italic", color=BODY, zorder=7)


def dashed_arrow(p0, p1, color):
    (x0, y0), (x1, y1) = p0, p1
    length = ((x1 - x0) ** 2 + (y1 - y0) ** 2) ** 0.5
    ux, uy = (x1 - x0) / length, (y1 - y0) / length
    head = 24
    ax.plot([x0, x1 - ux * head * 0.7], [y0, y1 - uy * head * 0.7], color=color, lw=2.4,
            ls=(0, (5, 3)), solid_capstyle="butt", zorder=5)
    ax.annotate("", xy=p1, xytext=(x1 - ux * head, y1 - uy * head), zorder=5,
                arrowprops=dict(arrowstyle="-|>", color=color, lw=2.4, shrinkA=0, shrinkB=0,
                                mutation_scale=20))


# ---------------------------------------------------------------- Airflow
AIR = (40, 865, W - 40, 1195)
ax.add_patch(FancyBboxPatch((AIR[0], AIR[1]), AIR[2] - AIR[0], AIR[3] - AIR[1],
                            boxstyle="round,pad=0,rounding_size=22", lw=3, ec=AIR_EDGE, fc=AIR_FILL,
                            zorder=1))
ax.text(75, 1153, "Apache Airflow 3 (Astro CLI + Docker)  ·  DAG: hw32_products_pipeline  ·  "
        "@daily, midnight Pacific", fontsize=16, fontweight="bold", color=AIR_TITLE, va="center",
        zorder=6)
ax.text(75, 1109, "retries=2, retry_delay=1 min, on_failure_callback logs the failed task  ·  "
        "catchup off", fontsize=12.5, color=BLUE, va="center", zorder=6)

tasks = [
    dict(name="extract", lines=[title("extract"), line("validate CSV (rows > 0),"),
                                line("PUT to internal stage")]),
    dict(name="load_staging", lines=[title("load_staging"), line("COPY INTO in one transaction;"),
                                     line("rollback on any error")]),
    dict(name="dbt_transform", lines=[
        title("dbt_transform"), line("Cosmos DbtTaskGroup: one Airflow task per dbt node", gap_after=0.2),
        line("stg_products run/test → products_snapshot → dim_products run/test", size=11, color=BLUE),
        line("→ scd2_matches_snapshot test (runs after both)", size=11, color=BLUE)]),
    dict(name="verify", lines=[title("verify"), line("one current row per product,"),
                               line("logs the history count")]),
    dict(name="clone_dev_database", lines=[title("clone_dev_database"),
                                           line("CREATE OR REPLACE DATABASE"),
                                           line("HW32_DEV CLONE HW32  (9.1)")]),
]
TASK_Y = (890, 1070)
for spec, (x0, x1) in zip(tasks, layout_row(tasks, 80, W - 80, min_gap=62)):
    spec["box"] = draw_box(x0, TASK_Y[0], x1, TASK_Y[1], spec["lines"], AIR_EDGE, "white")
T = {s["name"]: s["box"] for s in tasks}
mid_y = (TASK_Y[0] + TASK_Y[1]) / 2
for a, b in zip(tasks, tasks[1:]):
    solid_arrow((a["box"]["x1"], mid_y), (b["box"]["x0"], mid_y))

# ---------------------------------------------------------------- Snowflake
SF = (360, 110, 2380, 700)
ax.add_patch(FancyBboxPatch((SF[0], SF[1]), SF[2] - SF[0], SF[3] - SF[1],
                            boxstyle="round,pad=0,rounding_size=22", lw=3, ec=SF_EDGE, fc=SF_FILL,
                            zorder=1))
# The title masks the dashed arrows behind it instead of being crossed by them.
ax.text(385, 662, "Snowflake  ·  database HW32  ·  warehouse HW32_WH (X-Small, auto-suspend 60 s)",
        fontsize=15.5, fontweight="bold", color=SF_TITLE, va="center", zorder=7,
        bbox=dict(boxstyle="square,pad=0.12", fc=SF_FILL, ec="none"))

OBJ_Y = (330, 600)
objects = [
    dict(name="stage", edge=SF_EDGE, fill="white",
         lines=[title("Internal stage"), line("RAW.PRODUCTS_STAGE"), line("file format CSV_FMT")]),
    dict(name="raw", edge=SF_EDGE, fill="white",
         lines=[title("Raw table"), line("RAW.STG_PRODUCTS_RAW"), line("2,551 rows, 14 columns")]),
    dict(name="stg", edge=DBT_EDGE, fill=DBT_FILL,
         lines=[title("dbt: stg_products"), line("ANALYTICS · view"), line("cast price, trim title,"),
                line("clean SKU / GTIN")]),
    dict(name="snapshot", edge=DBT_EDGE, fill=DBT_FILL,
         lines=[line("dbt: products_", size=15, weight="bold", color=INK),
                title("snapshot"), line("SNAPSHOTS · SCD2"), line("check: title, price,"),
                line("availability, brand")]),
    dict(name="dim", edge=DBT_EDGE, fill=DBT_FILL,
         lines=[title("dbt: dim_products"), line("ANALYTICS · table"), line("version · change_type"),
                line("previous_price · rating"), line("valid_from / valid_to"), line("is_current")]),
]
for spec, (x0, x1) in zip(objects, layout_row(objects, 385, 2355, min_gap=100)):
    spec["box"] = draw_box(x0, OBJ_Y[0], x1, OBJ_Y[1], spec["lines"], spec["edge"], spec["fill"])
O = {s["name"]: s["box"] for s in objects}

csv = draw_box(40, OBJ_Y[0], 330, OBJ_Y[1],
               [title("CSV source"), line("home_depot_data_"), line("1_2021_12.csv"),
                line("2,551 rows × 14 cols"), line("airflow/include/data")], CSV_EDGE, CSV_FILL)

rating_lines = [line("RAW.PRODUCT_RATINGS", size=13.5, weight="bold", color=INK, gap_after=0.35),
                line("9.1 feature, promoted", size=12), line("from HW32_DEV", size=12)]
dim_cx = (O["dim"]["x0"] + O["dim"]["x1"]) / 2
rw = max(O["dim"]["x1"] - O["dim"]["x0"], content_width(rating_lines) + 44)
r_x1 = min(dim_cx + rw / 2, SF[2] - 28)          # keep it inside the HW32 container
ratings = draw_box(r_x1 - rw, 140, r_x1, 258, rating_lines, SF_EDGE, "white")

dev = draw_box(2470, OBJ_Y[0], W - 40, OBJ_Y[1],
               [line("database HW32_DEV", size=15, weight="bold", color=SF_TITLE, gap_after=0.45),
                line("zero-copy clone of HW32,"), line("rebuilt every day;"),
                line("dbt target: dev (9.1)")], SF_EDGE, SF_FILL, dashed=True)

# Data flow (solid).
row_y = (OBJ_Y[0] + OBJ_Y[1]) / 2
solid_arrow((csv["x1"], row_y), (O["stage"]["x0"], row_y), label="PUT")
solid_arrow((O["stage"]["x1"], row_y), (O["raw"]["x0"], row_y), label_lines=["COPY", "INTO"])
solid_arrow((O["raw"]["x1"], row_y), (O["stg"]["x0"], row_y), label="source()")
solid_arrow((O["stg"]["x1"], row_y), (O["snapshot"]["x0"], row_y), label="snapshot")
solid_arrow((O["snapshot"]["x1"], row_y), (O["dim"]["x0"], row_y), label="ref()")
solid_arrow((SF[2], row_y), (dev["x0"], row_y), label="CLONE")
solid_arrow((dim_cx, ratings["y1"]), (dim_cx, O["dim"]["y0"]))
ax.text(dim_cx + 14, (ratings["y1"] + O["dim"]["y0"]) / 2, "left join", fontsize=10.5,
        fontstyle="italic", color=BODY, va="center", zorder=7)

# Which task touches which object (dashed): blue = writes or runs, gray = reads.
dashed_arrow(at(T["extract"], 0.3, "bottom"), at(csv, 0.5, "top"), GRAY)
dashed_arrow(at(T["extract"], 0.72, "bottom"), at(O["stage"], 0.5, "top"), AIR_EDGE)
dashed_arrow(at(T["load_staging"], 0.6, "bottom"), at(O["raw"], 0.5, "top"), AIR_EDGE)
dashed_arrow(at(T["dbt_transform"], 0.2, "bottom"), at(O["stg"], 0.5, "top"), AIR_EDGE)
dashed_arrow(at(T["dbt_transform"], 0.55, "bottom"), at(O["snapshot"], 0.62, "top"), AIR_EDGE)
dashed_arrow(at(T["dbt_transform"], 0.8, "bottom"), at(O["dim"], 0.35, "top"), AIR_EDGE)
dashed_arrow(at(T["verify"], 0.5, "bottom"), at(O["dim"], 0.72, "top"), GRAY)
dashed_arrow(at(T["clone_dev_database"], 0.55, "bottom"), at(dev, 0.5, "top"), AIR_EDGE)

# Notes inside the HW32 container, left of the ratings table.
notes = [
    ("dbt tests (12): unique / not_null on keys · accepted_values(change_type) · "
     "one_current_row_per_product ·", TESTS_INK),
    ("no_overlapping_validity · scd2_matches_snapshot (dim windows = dbt_valid_to) · rating_in_range",
     TESTS_INK),
    ("Other zero-copy clones: year-end PRODUCTS_SNAPSHOT AT(TIMESTAMP) → price_yoy (9.2) ·",
     CLONES_INK),
    ("PROTOTYPE schema → price_bands (9.3) · daily HW32_BACKUP_YYYYMMDD, keep 7, "
     "restore by schema swap (9.4)", CLONES_INK),
]
for i, (s, color) in enumerate(notes):
    y = 285 - i * 31 - (14 if i >= 2 else 0)
    t = ax.text(385, y, s, fontsize=11.5, color=color, va="center", zorder=6)
    checks.append((t, (SF[0], SF[1], ratings["x0"] - 20, SF[3])))

# Legend, under the CSV source.
for i, (label, color, style) in enumerate([("task writes or runs", AIR_EDGE, (0, (5, 3))),
                                           ("task reads", GRAY, (0, (5, 3))),
                                           ("data flow", BODY, "-")]):
    y = 240 - i * 40
    ax.plot([50, 110], [y, y], color=color, lw=2.4, ls=style, zorder=6)
    t = ax.text(122, y, label, fontsize=11.5, color=color if color != BODY else BODY, va="center",
                zorder=6)
    checks.append((t, (0, 0, SF[0] - 8, H)))
ax.text(50, 282, "Arrows:", fontsize=11.5, fontweight="bold", color=INK, va="center", zorder=6)

# Warn about any text that spills out of its box.
fig.canvas.draw()
to_data = ax.transData.inverted()
problems = 0
for t, (x0, y0, x1, y1) in checks:
    bb = t.get_window_extent(renderer)
    (bx0, by0), (bx1, by1) = to_data.transform([(bb.x0, bb.y0), (bb.x1, bb.y1)])
    if bx0 < x0 + 8 or bx1 > x1 - 8 or by0 < y0 + 4 or by1 > y1 - 4:
        problems += 1
        print(f"OVERFLOW: {t.get_text()!r} spans x {bx0:.0f}-{bx1:.0f}, y {by0:.0f}-{by1:.0f}; "
              f"box x {x0:.0f}-{x1:.0f}, y {y0:.0f}-{y1:.0f}")
fig.savefig(OUT, dpi=DPI, facecolor="white")
print(f"wrote {OUT} ({problems} overflow warnings)")
