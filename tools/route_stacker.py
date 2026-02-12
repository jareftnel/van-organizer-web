# route_stacker.py
from __future__ import annotations

# stdlib
import json
import math
import re
import time
from collections import OrderedDict
from pathlib import Path
from typing import Any

# third-party
import numpy as np
import pandas as pd
import pdfplumber
from PIL import Image, ImageDraw, ImageFont


# =========================
# CONFIG
# =========================
DPI: int = 200
BASE_DPI: int = 200
SCALE: float = 1.0


def spx(x: float) -> int:
    return int(round(x * SCALE))  # keep it consistent


LETTER_W_IN: float = 8.5
LETTER_H_IN: float = 11.0
MARGIN_IN: float = 0.5

PAGE_W_PX = int(LETTER_W_IN * DPI)
PAGE_H_PX = int(LETTER_H_IN * DPI)
MARGIN_PX = int(MARGIN_IN * DPI)
CONTENT_W_PX = PAGE_W_PX - 2 * MARGIN_PX

TOP_MARGIN_PX = MARGIN_PX
BOTTOM_MARGIN_PX = MARGIN_PX

GAP_IN: float = 0.07
GAP_PX: int = round(GAP_IN * DPI)

ROWS_GRID = 3  # tote rows

# Table columns: Bag | Zones | Total
COLS_BASE = [spx(325), spx(850), spx(325)]
COLS_SUM = sum(COLS_BASE)
w1 = int(CONTENT_W_PX * COLS_BASE[0] / COLS_SUM)
w3 = int(CONTENT_W_PX * COLS_BASE[2] / COLS_SUM)
side = max(w1, w3)
w2 = CONTENT_W_PX - 2 * side
COLS_W = [side, w2, side]


STYLE = {
    "banner_bg": (211, 211, 211),
    "meta_grey": (85, 85, 85),
    "royal_blue": (0, 32, 194),
    "purple": (75, 0, 130),
    "lavender": (236, 232, 255),
    "bright_red": (210, 40, 40),
    "row_fill_teal": (238, 247, 247),
    "divider_teal": (0, 140, 140),
    "divider_grey": (170,170, 170),
    "row_divider_h": spx(2),
    "table_cell_height": spx(64),
    "banner_height": spx(54),
    "table_margin": spx(22),
    "bag_colors": {
        "yellow": (246, 217, 74),
        "green": (83, 182, 53),
        "orange": (234, 99, 43),
        "black": (12, 10, 11),
        "navy": (57, 128, 240),
    },
}

# Overflow pairing
PAIR_MAP = {"A": "T", "B": "U", "C": "W", "D": "X", "E": "Y", "G": "Z"}
INVERSE_PAIR = {v: k for k, v in PAIR_MAP.items()}

ZONE_RE = re.compile(r"^(?:[A-Z]-[0-9.]*[A-Z]+|99\.[A-Z0-9]+)$")
SPLIT_RE = re.compile(r"^([0-9.]*)([A-Z]+)$")
TIME_RE = re.compile(r"\b(\d{1,2}:\d{2}\s*(?:AM|PM))\b", re.I)

BAG_COLORS_ALLOWED = {
    "Yellow", "Green", "Orange", "Black", "Navy",
    "Blue", "Brown", "Grey", "Gray", "Purple"
}


# =========================
# FONTS
# =========================
_FONT_CACHE: dict[int, ImageFont.FreeTypeFont] = {}

def get_font(size: int):
    size = int(size)
    if size in _FONT_CACHE:
        return _FONT_CACHE[size]
    for name in ("DejaVuSansCondensed-Bold.ttf", "DejaVuSans-Bold.ttf"):
        try:
            f = ImageFont.truetype(name, size)
            _FONT_CACHE[size] = f
            return f
        except Exception:
            continue
    f = ImageFont.load_default()
    _FONT_CACHE[size] = f
    return f

FONT_BANNER = get_font(spx(40))
FONT_TABLE = get_font(spx(32))
FONT_SUMMARY = get_font(spx(32))
FONT_TOTE_NUM = get_font(spx(40))
FONT_TOTE_TAG_BASE = get_font(spx(22))
FONT_TOTE_TAG_MIN = get_font(spx(14))
FONT_TOTE_PKGS = get_font(spx(22))
FONT_STYLE_TAG = get_font(spx(22))
FONT_DATE = get_font(spx(22))
FONT_ZONE = get_font(spx(16))

_CHIP_DUMMY_IMG = Image.new("RGB", (spx(10), spx(10)), "white")
_CHIP_D = ImageDraw.Draw(_CHIP_DUMMY_IMG)


# =========================
# HELPERS
# =========================
def warn(msg: str):
    print("[WARN]", msg, flush=True)

def is_zone(token: str) -> bool:
    return bool(ZONE_RE.match(token))

def parse_int_safe(token, context: str = "", route_title: str = ""):
    s = str(token).strip()
    cleaned = re.sub(r"[^\d]", "", s)
    if cleaned == "":
        warn(f"Failed to parse int from {token!r} in {context} [{route_title}]")
        return None
    try:
        return int(cleaned)
    except Exception:
        warn(f"Exception parsing int from {token!r} in {context} [{route_title}]")
        return None

def extract_bag_num_str(token, context: str = "", route_title: str = ""):
    # Preserve leading zeros
    s = str(token).strip()
    digits = re.sub(r"[^\d]", "", s)
    if digits == "":
        warn(f"Failed to parse bag number from {token!r} in {context} [{route_title}]")
        return None
    return digits

def is_99_tag(label: str) -> bool:
    clean = str(label).strip()
    first = clean.split()[0] if clean else ""
    return first.startswith("99.")

def infer_style_label(text: str, rs: str | None) -> str:
    t = (text or "").lower()
    if "on-road experience" in t or "on road experience" in t:
        return "Standard: On-Road Experience (Driver)"
    if rs and rs.upper().startswith("K.7"):
        return "Standard: On-Road Experience (Driver)"
    if "nursery route level 3" in t or "nursery lvl 3" in t:
        return "Nursery LVL 3"
    if "nursery route level 2" in t or "nursery lvl 2" in t:
        return "Nursery LVL 2"
    if "nursery route level 1" in t or "nursery lvl 1" in t:
        return "Nursery LVL 1"
    if "nursery route" in t:
        return "Nursery"
    return "Standard"

def extract_time_label(text: str):
    head = "\n".join((text or "").splitlines()[:10])
    m = TIME_RE.search(head)
    if m:
        return m.group(1).upper().replace("  ", " ")
    return None

def extract_declared_counts(lines, route_title: str = ""):
    bag_ct = ov_ct = None
    for l in lines:
        if "Sort Zone Bag Pkgs" in l:
            break
        m = re.search(r"(\d+)\s+bags?\s+(\d+)\s+over", l.lower())
        if m:
            bag_ct = parse_int_safe(m.group(1), "Declared bags", route_title)
            ov_ct = parse_int_safe(m.group(2), "Declared overflow", route_title)
            break
    return bag_ct, ov_ct

def extract_pkg_summaries(lines, route_title: str = ""):
    commercial = total = None
    for l in lines:
        s = l.strip().lower()
        if s.startswith("commercial packages"):
            for tok in reversed(l.split()):
                v = parse_int_safe(tok, "Commercial packages", route_title)
                if v is not None:
                    commercial = v
                    break
        if s.startswith("total packages"):
            for tok in reversed(l.split()):
                v = parse_int_safe(tok, "Total packages", route_title)
                if v is not None:
                    total = v
                    break
    return commercial, total


# =========================
# PARSE ROUTE PAGE (ORDER BY PRINTED INDEX)
# =========================
def parse_route_page(text: str):
    """
    Parse a single route page into bag + overflow structures.

    Bags are ordered by their printed index number (leftmost digit on each bag row).
    """
    text = text or ""
    m = re.search(r"STG\.([A-Z]+\.\d+)", text)
    rs = m.group(1) if m else None

    m2 = re.search(r"((?:CX|TX)\d{1,3})", text)
    cx = m2.group(1) if m2 else None

    route_title = f"{rs} ({cx})" if rs and cx else (rs or cx or "")

    lines = text.splitlines()
    decl_bags, decl_over = extract_declared_counts(lines, route_title)
    comm_pkgs, total_pkgs = extract_pkg_summaries(lines, route_title)

    try:
        hdr_idx = next(i for i, l in enumerate(lines) if "Sort Zone Bag Pkgs" in l)
    except StopIteration:
        return None

    data_lines = [l.strip() for l in lines[hdr_idx + 1:] if l.strip()]

    bags: list[dict[str, Any]] = []
    overs: list[tuple[str, int]] = []

    for ln in data_lines:
        if ln.startswith("Total Packages") or ln.startswith("Commercial Packages"):
            continue

        toks = ln.split()
        if not toks:
            continue

        ptr = 0
        while ptr < len(toks):
            # Bag row with sort zone: idx zone color bag pkgs
            if (
                ptr + 4 < len(toks)
                and toks[ptr].isdigit()
                and is_zone(toks[ptr + 1])
                and toks[ptr + 2] in BAG_COLORS_ALLOWED
            ):
                idx_val = parse_int_safe(toks[ptr], "Bag index", route_title)
                bag_num_str = extract_bag_num_str(toks[ptr + 3], "Bag number (with zone)", route_title)
                pk = parse_int_safe(toks[ptr + 4], "Bag pkgs", route_title)
                if idx_val is not None and bag_num_str is not None and pk is not None:
                    bags.append({
                        "idx": idx_val,
                        "sort_zone": toks[ptr + 1],
                        "bag": f"{toks[ptr + 2]} {bag_num_str}",
                        "pkgs": pk,
                    })
                ptr += 5
                continue

            # Bag row without sort zone: idx color bag pkgs
            if (
                ptr + 3 < len(toks)
                and toks[ptr].isdigit()
                and toks[ptr + 1] in BAG_COLORS_ALLOWED
            ):
                idx_val = parse_int_safe(toks[ptr], "Bag index (no zone)", route_title)
                bag_num_str = extract_bag_num_str(toks[ptr + 2], "Bag number (no zone)", route_title)
                pk = parse_int_safe(toks[ptr + 3], "Bag pkgs (no zone)", route_title)
                if idx_val is not None and bag_num_str is not None and pk is not None:
                    bags.append({
                        "idx": idx_val,
                        "sort_zone": None,
                        "bag": f"{toks[ptr + 1]} {bag_num_str}",
                        "pkgs": pk,
                    })
                ptr += 4
                continue

            # Overflow row: idx zone pkgs
            if (
                ptr + 2 < len(toks)
                and toks[ptr].isdigit()
                and is_zone(toks[ptr + 1])
            ):
                pk_val = parse_int_safe(toks[ptr + 2], "Overflow line", route_title)
                if pk_val is not None:
                    overs.append((toks[ptr + 1], pk_val))
                ptr += 3
                continue

            ptr += 1

    if not bags:
        return None

    bags.sort(key=lambda b: b.get("idx", 10**6))
    style_label = infer_style_label(text, rs)
    time_label = extract_time_label(text)

    return (
        rs,
        cx,
        style_label,
        time_label,
        bags,
        overs,
        decl_bags,
        decl_over,
        comm_pkgs,
        total_pkgs,
    )


# =========================
# OVERFLOW ASSIGNMENT
# =========================
def split_zone_for_index(z: str):
    if "-" not in z:
        return z[:-1], z[-1]
    prefix, tail = z.split("-", 1)
    m = SPLIT_RE.match(tail)
    if m:
        num, letters = m.groups()
        core = f"{prefix}-{num}" if num else prefix
        return core, letters[-1]
    return z[:-1], z[-1]

def assign_overflows(bags, overs):
    bag_idx = {}
    for i, b in enumerate(bags):
        sz = b.get("sort_zone")
        if not sz:
            continue
        core, L = split_zone_for_index(sz)
        bag_idx.setdefault((core, L), []).append(i)

    texts = [[] for _ in bags]
    totals = [0 for _ in bags]
    last_assigned_bag = None

    for zone, count in overs:
        core, L = split_zone_for_index(zone)
        label_core = zone.split("-", 1)[1] if "-" in zone else zone
        is99 = is_99_tag(label_core)

        bi = None

        if is99:
            # ✅ RULE: If this is the first overflow row (no previous), attach to Bag #1.
            if last_assigned_bag is None:
                bi = 0 if len(bags) else None
            else:
                bi = last_assigned_bag
        else:
            # Normal overflow pairing (A↔T, B↔U, C↔W, D↔X, E↔Y, G↔Z)
            need = INVERSE_PAIR.get(L)
            if need and bag_idx.get((core, need)):
                bi = bag_idx[(core, need)][0]
            elif bag_idx.get((core, L)):
                bi = bag_idx[(core, L)][0]

        # Final fallback: if we still couldn’t map it, keep continuity if possible,
        # otherwise dump it to first bag.
        if bi is None:
            if last_assigned_bag is not None:
                bi = last_assigned_bag
            elif bags:
                bi = 0

        if bi is not None:
            texts[bi].append(f"{label_core} ({count})")
            totals[bi] += int(count)
            last_assigned_bag = bi

    return texts, totals


# =========================
# DATAFRAME
# =========================
def df_from(bags, texts, totals):
    rows = []
    for b, tags, tot in zip(bags, texts, totals):
        mid = "; ".join(tags)
        tot_disp = int(tot) if (mid and tot) else ""  # blank if no overflow
        rows.append([b["bag"], mid, tot_disp])
    df = pd.DataFrame(rows, columns=["Bag", "Overflow Zone(s)", "Overflow Pkgs (total)"])
    df["Overflow Zone(s)"] = df["Overflow Zone(s)"].replace({np.nan: ""})
    return df


# =========================
# CHIP RENDERING
# =========================
def draw_chip_fullwidth(draw, text, tile_w):
    outer = spx(6)
    max_w = tile_w - 2 * outer
    clean = str(text).strip()
    is99 = is_99_tag(clean)
    txt_color = STYLE["purple"] if is99 else (0, 0, 0)
    bg_color = STYLE["lavender"] if is99 else (245, 245, 245)

    size = FONT_TOTE_TAG_BASE.size
    while size >= FONT_TOTE_TAG_MIN.size:
        fnt = get_font(size)
        bbox = draw.textbbox((0, 0), clean, font=fnt)
        tw, th = bbox[2] - bbox[0], bbox[3] - bbox[1]
        if tw + spx(12) <= max_w:
            chip_w = max_w
            chip_h = th + spx(8)
            chip = Image.new("RGBA", (chip_w, chip_h), (0, 0, 0, 0))
            cd = ImageDraw.Draw(chip)
            try:
                cd.rounded_rectangle([0, 0, chip_w - spx(1), chip_h - spx(1)], radius=spx(6), fill=bg_color)
            except Exception:
                cd.rectangle([0, 0, chip_w - spx(1), chip_h - spx(1)], fill=bg_color)
            cd.text((chip_w // 2, chip_h // 2), clean, anchor="mm", font=fnt, fill=txt_color)
            return chip, chip_w, chip_h, outer
        size -= max(1, spx(1))

    fnt = FONT_TOTE_TAG_MIN
    bbox = draw.textbbox((0, 0), clean, font=fnt)
    th = bbox[3] - bbox[1]
    chip_w = max_w
    chip_h = th + spx(8)
    chip = Image.new("RGBA", (chip_w, chip_h), (0, 0, 0, 0))
    cd = ImageDraw.Draw(chip)
    cd.rectangle([0, 0, chip_w - spx(1), chip_h - spx(1)], fill=bg_color)
    cd.text((chip_w // 2, chip_h // 2), clean, anchor="mm", font=fnt, fill=txt_color)
    return chip, chip_w, chip_h, outer

def measure_tile_heights(df, tile_w):
    base_h = int(tile_w * 0.55)
    heights = []
    cache = []

    for i in range(len(df)):
        mid = str(df.iat[i, 1] or "")
        toks = [t.strip() for t in re.split(r"[;|]+", mid) if t.strip()]
        chips = []
        total_h = 0

        for t in toks:
            chip, cw, ch, margin = draw_chip_fullwidth(_CHIP_D, t, tile_w)
            chips.append((chip, cw, ch, margin))
            total_h += ch

        if toks:
            total_h += spx(4) * (len(toks) - 1)

        heights.append(base_h + (spx(4) if toks else 0) + total_h + spx(10))
        cache.append(chips)

    return heights, cache


# =========================
# TOTE RENDERING
# =========================
def draw_tote(df, bags):
    n = len(df)
    if n == 0:
        return Image.new("RGB", (CONTENT_W_PX, spx(10)), "white")

    def fmt_zone(sz):
        if not sz:
            return ""
        return sz.split("-", 1)[1] if "-" in sz else sz

    zone_display = []
    last = None
    for b in bags:
        sz = b.get("sort_zone")
        if sz:
            last = sz
            zone_display.append(fmt_zone(sz))
        else:
            zone_display.append(fmt_zone(last) if last else "")

    cols = max(1, math.ceil(n / ROWS_GRID))
    pad_x, pad_y = spx(6), spx(8)
    inner_w = CONTENT_W_PX - (cols - 1) * pad_x
    base_w = inner_w // cols
    extra = inner_w - base_w * cols
    # First `extra` columns get +1 px
    col_ws = [base_w + (1 if i < extra else 0) for i in range(cols)]

    col_x0 = []
    x = 0
    for w in col_ws:
        col_x0.append(x)
        x += w + pad_x

    tile_w = max(col_ws)

    base_h = int(tile_w * 0.55)
    heights, cache = measure_tile_heights(df, tile_w)

    # Right-to-left, 3-row fill
    positions = []
    for col in range(cols - 1, -1, -1):
        for row in range(ROWS_GRID):
            positions.append((col, row))

    row_heights = [0] * ROWS_GRID
    for i, h in enumerate(heights):
        if i >= len(positions):
            break
        _, row = positions[i]
        row_heights[row] = max(row_heights[row], h)

    img_h = sum(row_heights) + pad_y * (ROWS_GRID - 1)
    img = Image.new("RGB", (CONTENT_W_PX, img_h), "white")
    d = ImageDraw.Draw(img)

    row_y = [0] * ROWS_GRID
    for r in range(1, ROWS_GRID):
        row_y[r] = row_y[r - 1] + row_heights[r - 1] + pad_y

    def color_for_bag(label):
        base = str(label).split()[0].lower()
        return STYLE["bag_colors"].get(base, (200, 200, 200))

    for i in range(n):
        col, row = positions[i]
        x0 = col_x0[col]
        y0 = row_y[row]
        tile_h = row_heights[row]
        x1 = x0 + col_ws[col]

        bg = color_for_bag(df.iat[i, 0])
        d.rectangle([x0, y0, x1, y0 + tile_h], fill=bg, outline="black", width=spx(2))

        label = df.iat[i, 0]
        num = str(label).split()[-1]

        r, g, b = bg
        lum = r * 0.299 + g * 0.587 + b * 0.114
        num_fill = (255, 255, 255) if lum < 140 else (0, 0, 0)
        halo_center = (0, 0, 0) if lum < 140 else (255, 255, 255)

        num_x = (x0 + x1) // 2
        num_y = y0 + base_h // 2 + spx(14)  # your “14” vertical center shift

        try:
            d.text(
                (num_x, num_y),
                num,
                anchor="mm",
                font=FONT_TOTE_NUM,
                fill=num_fill,
                stroke_width=spx(3),
                stroke_fill=halo_center,
            )
        except TypeError:
            bbox = d.textbbox((0, 0), num, font=FONT_TOTE_NUM)
            tw, th = bbox[2] - bbox[0], bbox[3] - bbox[1]
            pad = spx(3)
            d.rectangle(
                (num_x - tw // 2 - pad, num_y - th // 2 - pad, num_x + tw // 2 + pad, num_y + th // 2 + pad),
                fill=halo_center,
            )
            d.text((num_x, num_y), num, anchor="mm", font=FONT_TOTE_NUM, fill=num_fill)

        # Top-left zone with white halo
        zdisp = zone_display[i] if i < len(zone_display) else ""
        if zdisp:
            try:
                d.text(
                    (x0 + spx(6), y0 + spx(4)),
                    zdisp,
                    anchor="la",
                    font=FONT_TOTE_PKGS,
                    fill=(70, 70, 70),
                    stroke_width=spx(2),
                    stroke_fill=(255, 255, 255),
                )
            except TypeError:
                bbox = d.textbbox((0, 0), zdisp, font=FONT_TOTE_PKGS)
                tw, th = bbox[2] - bbox[0], bbox[3] - bbox[1]
                pad = spx(1)
                d.rectangle((x0 + spx(6) - pad, y0 + spx(4) - pad, x0 + spx(6) + tw + pad, y0 + spx(4) + th + pad), fill=(255, 255, 255))
                d.text((x0 + spx(6), y0 + spx(4)), zdisp, anchor="la", font=FONT_TOTE_PKGS, fill=(70, 70, 70))

        # Top-right pkgs with white halo
        binfo = bags[i]
        if binfo.get("sort_zone") and binfo.get("pkgs") not in ("", None):
            pk_txt = str(int(binfo["pkgs"]))
            try:
                d.text(
                    (x1 - spx(6), y0 + spx(4)),
                    pk_txt,
                    anchor="ra",
                    font=FONT_TOTE_PKGS,
                    fill=STYLE["bright_red"],
                    stroke_width=spx(2),
                    stroke_fill=(255, 255, 255),
                )
            except TypeError:
                bbox = d.textbbox((0, 0), pk_txt, font=FONT_TOTE_PKGS)
                tw, th = bbox[2] - bbox[0], bbox[3] - bbox[1]
                pad = spx(1)
                d.rectangle((x1 - spx(6) - tw - pad, y0 + spx(4) - pad, x1 - spx(6) + pad, y0 + spx(4) + th + pad), fill=(255, 255, 255))
                d.text((x1 - spx(6), y0 + spx(4)), pk_txt, anchor="ra", font=FONT_TOTE_PKGS, fill=STYLE["bright_red"])

        # Overflow chips (BOTTOM-ALIGNED)
        chips = cache[i]
        if chips:
            top_pad = spx(8)     # space below the big number area
            bot_pad = spx(10)    # space above the tile border
            gap = spx(4)

            stack_h = sum(ch for _, _, ch, _ in chips) + gap * (len(chips) - 1)

            # Try to place the whole stack flush-ish to the bottom...
            cy = (y0 + tile_h) - bot_pad - stack_h

            # ...but never let it rise into the number zone.
            min_cy = y0 + base_h + top_pad
            if cy < min_cy:
                cy = min_cy

            for chip_img, cw, ch, margin in chips:
                img.paste(chip_img, (x0 + margin, cy), mask=chip_img)
                cy += ch + gap

    return img


# =========================
# TABLE RENDERING
# =========================
def render_table(
    df,
    title,
    style_label,
    date_label,
    time_label,
    bag_count,
    declared_overflow,
    commercial_pkgs,
    total_pkgs,
    bags,
):
    cell_h = STYLE["table_cell_height"]
    margin = STYLE["table_margin"]
    banner_h = STYLE["banner_height"]
    total_rows = len(df) + 2  # summary + rows + bottom totals
    width = CONTENT_W_PX
    height = banner_h + total_rows * cell_h + margin * 2

    im = Image.new("RGB", (width, height), "white")
    d = ImageDraw.Draw(im)

    # Banner
    d.rectangle([0, 0, width, banner_h], fill=STYLE["banner_bg"])

    left = ""
    if date_label and time_label:
        left = f"{date_label} [{time_label}]"
    elif date_label:
        left = str(date_label)

    if left:
        d.text((spx(12), banner_h // 2), left, anchor="lm", font=FONT_DATE, fill=STYLE["meta_grey"])

    d.text((width // 2, banner_h // 2), title, anchor="mm", font=FONT_BANNER, fill="black")

    if style_label:
        d.text((width - spx(12), banner_h // 2), str(style_label).upper(), anchor="rm", font=FONT_STYLE_TAG, fill=STYLE["meta_grey"])

    x = margin
    y0 = banner_h + margin
    right = width - margin

    def _tw(font, s) -> int:
        s = "" if s is None else str(s)
        if not s:
            return 0
        try:
            box = d.textbbox((0, 0), s, font=font)
            return int(box[2] - box[0])
        except Exception:
            try:
                return int(d.textlength(s, font=font))
            except Exception:
                return int(len(s) * (getattr(font, "size", 12) * 0.6))

    pad_lr = spx(10)
    zone_gap = spx(6)
    pkg_gap = spx(6)

    min_mid = spx(520)
    min_side = spx(240)
    max_side = max(0, (right - x - min_mid) // 2)

    max_w = 0
    last_zone_for_measure = None

    for df_idx in range(len(df)):
        label = str(df.iat[df_idx, 0] or "")
        bag_w = _tw(FONT_TABLE, label)

        zone_display = ""
        pkg_txt = ""

        if df_idx < len(bags):
            binfo = bags[df_idx]
            actual_sz = binfo.get("sort_zone")

            if actual_sz:
                last_zone_for_measure = actual_sz
                zone_display = actual_sz.split("-", 1)[1] if "-" in actual_sz else actual_sz
            elif last_zone_for_measure:
                zone_display = last_zone_for_measure.split("-", 1)[1] if "-" in last_zone_for_measure else last_zone_for_measure

            if actual_sz and binfo.get("pkgs") not in ("", None):
                try:
                    pkg_txt = f" ({int(binfo['pkgs'])})"
                except Exception:
                    pkg_txt = ""

        zone_w = _tw(FONT_ZONE, zone_display) + (zone_gap if zone_display else 0)
        pkg_w = _tw(FONT_TOTE_PKGS, pkg_txt) + (pkg_gap if pkg_txt else 0)

        max_w = max(max_w, zone_w + bag_w + pkg_w + pad_lr * 2)
        if max_w >= max_side:
            max_w = max_side
            break

    target_mid = spx(120)
    side = int(min(max_w, max_side))
    if (right - x) - 2 * min_side >= target_mid:
        side = max(side, min_side)
    mid = (right - x) - 2 * side

    # safety net
    if mid < target_mid:
        # compute max possible side that still leaves target_mid
        max_side_for_target = max(0, ((right - x) - target_mid) // 2)
        side = max(0, min(side, max_side_for_target))
        # still honor min_side ONLY if it fits
        if (right - x) - 2 * min_side >= target_mid:
            side = max(side, min_side)
        mid = (right - x) - 2 * side

    col_w = [side, mid, side]

    # Top summary row
    top = y0
    bot = top + cell_h
    d.rectangle([x, top, right, bot], outline="black", width=spx(2))
    d.text((x + spx(10), (top + bot) // 2), f"{bag_count} bags", anchor="lm", font=FONT_SUMMARY, fill=STYLE["royal_blue"])
    d.text((right - spx(10), (top + bot) // 2), f"{declared_overflow} overflow", anchor="rm", font=FONT_SUMMARY, fill=STYLE["royal_blue"])
    d.line([x, bot, right, bot], fill=STYLE["royal_blue"], width=spx(5))

    # Bag rows
    last_zone_for_display = None

    for r in range(1, len(df) + 1):
        top = y0 + r * cell_h
        bot = top + cell_h

        # --- teal/white rhythm: 3 teal rows, 3 white rows, repeat ---
        block = (r - 1) // 3
        teal_block = (block % 2) == 0

        # fill first
        if teal_block:
            d.rectangle([x, top, right, bot], fill=STYLE["row_fill_teal"])
        else:
            d.rectangle([x, top, right, bot], fill="white")

        # then outline on top of fill
        d.rectangle([x, top, right, bot], outline="black", width=spx(2))

        # divider under each 3-row block
        if r % 3 == 0:
            h = STYLE["row_divider_h"]
            div_color = STYLE["divider_teal"] if teal_block else STYLE["divider_grey"]
            d.rectangle([x + spx(2), bot - h, right - spx(2), bot], fill=div_color)

        cx = x
        df_idx = r - 1

        for c_idx, w in enumerate(col_w):
            val = df.iat[df_idx, c_idx]
            if val == "" or pd.isna(val):
                cx += w
                continue

            text = str(val)
            ym = (top + bot) // 2

            # Bag column
            if c_idx == 0:
                label = text
                pkg_txt = ""
                zone_display = ""

                if df_idx < len(bags):
                    binfo = bags[df_idx]
                    actual_sz = binfo.get("sort_zone")

                    if actual_sz:
                        last_zone_for_display = actual_sz
                        zone_display = actual_sz.split("-", 1)[1] if "-" in actual_sz else actual_sz
                    elif last_zone_for_display:
                        zone_display = last_zone_for_display.split("-", 1)[1] if "-" in last_zone_for_display else last_zone_for_display

                    if actual_sz and binfo.get("pkgs") not in ("", None):
                        try:
                            pkg_txt = f" ({int(binfo['pkgs'])})"
                        except Exception:
                            pkg_txt = ""

                start_x = cx + spx(10)

                if zone_display:
                    d.text((start_x, ym), zone_display, anchor="lm", font=FONT_ZONE, fill=STYLE["meta_grey"])
                    zb = d.textbbox((0, 0), zone_display, font=FONT_ZONE)
                    start_x += (zb[2] - zb[0]) + spx(6)

                d.text((start_x, ym), label, anchor="lm", font=FONT_TABLE, fill="black")

                if pkg_txt:
                    lb = d.textbbox((0, 0), label, font=FONT_TABLE)
                    d.text((start_x + (lb[2] - lb[0]) + spx(6), ym), pkg_txt, anchor="lm", font=FONT_TOTE_PKGS, fill=STYLE["bright_red"])

            # Overflow zones column
            elif c_idx == 1:
                toks = [t.strip() for t in re.split(r"[;|]+", text) if t.strip()]
                if not toks:
                    cx += w
                    continue

                segs = []
                first = True
                for tok in toks:
                    prefix = "" if first else "; "
                    first = False
                    color = STYLE["purple"] if is_99_tag(tok) else (0, 0, 0)
                    segs.append((prefix + tok, color))

                total_w = 0
                for seg, _ in segs:
                    bb = d.textbbox((0, 0), seg, font=FONT_TABLE)
                    total_w += bb[2] - bb[0]

                sx = cx + max((w - total_w) // 2, spx(4))
                for seg, color in segs:
                    bb = d.textbbox((0, 0), seg, font=FONT_TABLE)
                    sw = bb[2] - bb[0]
                    d.text((sx, ym), seg, anchor="lm", font=FONT_TABLE, fill=color)
                    sx += sw

            # Overflow totals column
            else:
                d.text((cx + w - spx(10), ym), text, anchor="rm", font=FONT_TABLE, fill="black")

            cx += w

    # Bottom totals row
    br_top = y0 + (len(df) + 1) * cell_h
    br_bot = br_top + cell_h
    d.rectangle([x, br_top, right, br_bot], outline="black", width=spx(4))

    if commercial_pkgs is not None:
        d.text((x + spx(10), (br_top + br_bot) // 2), f"{int(commercial_pkgs)} Commercial", anchor="lm", font=FONT_TABLE, fill=STYLE["bright_red"])

    if total_pkgs is not None:
        d.text((right - spx(10), (br_top + br_bot) // 2), f"{int(total_pkgs)} Total", anchor="rm", font=FONT_TABLE, fill=STYLE["bright_red"])

    # Outer border
    d.rectangle([x, y0, right, y0 + total_rows * cell_h], outline="black", width=spx(2))

    return im


# =========================================================
# GROUPED ROUTE STACKING (RS11.30 STANDARD)
# =========================================================
_RE_STG  = re.compile(r"\bSTG\.[A-Z]\.\d+\b", re.I)
_RE_CX   = re.compile(r"\b(?:CX|TX)\d+\b", re.I)
_RE_BAGS = re.compile(r"\b(\d+)\s+bags\b", re.I)

def _is_header_page(t: str) -> bool:
    t = t or ""
    return bool(_RE_STG.search(t) and _RE_CX.search(t))

def _is_tableish_page(t: str) -> bool:
    t = t or ""
    return ("Sort Zone Bag" in t) or ("Sort Zone Pkgs" in t) or bool(_RE_BAGS.search(t))

def _group_pages(page_texts):
    groups = []
    i, n = 0, len(page_texts)
    while i < n:
        if _is_header_page(page_texts[i]):
            g = [i]
            i += 1
            while i < n and (not _is_header_page(page_texts[i])) and (_is_tableish_page(page_texts[i]) or not page_texts[i].strip()):
                g.append(i)
                i += 1
            groups.append(g)
        else:
            i += 1
    return groups


# =========================
# SUMMARY PAGES (AUTO-SPLIT + LINKS)
# =========================
def render_summary_pages(
    mismatches,
    routes_over_30=None,
    routes_over_50_overflow=None,
    top10_heavy_totals=None,
    top10_commercial=None,
):
    """
    Sections (order):
      "Verification"
      "Routes with 30+ Bags"
      "Routes with 50+ Overflow"
      "Routes with Heaviest Package Counts"
      "Routes with Heaviest Commercial"

    Row layout:
      - Route (left, hyperlink style; true PDF link added later)
      - Metric (middle, left-aligned)
      - Pg (right-aligned)
    """
    header_font = FONT_BANNER
    body_font = get_font(spx(24))
    label_font = get_font(spx(18))

    x_left = MARGIN_PX
    x_mid = MARGIN_PX + int(CONTENT_W_PX * 0.62)
    x_right = PAGE_W_PX - MARGIN_PX
    bottom_guard = spx(80)

    pages = []
    specs_pages = []

    def _start_page():
        page = Image.new("RGB", (PAGE_W_PX, PAGE_H_PX), "white")
        d = ImageDraw.Draw(page)
        d.text((x_right, spx(46)), "Pg", anchor="ra", font=label_font, fill=(90, 90, 90))
        return page, d, []

    page, d, link_specs = _start_page()
    y = spx(70)
    current_section = None

    def _push_page():
        nonlocal page, d, link_specs, y
        pages.append(page)
        specs_pages.append(link_specs)
        page, d, link_specs = _start_page()
        y = spx(70)

    def _ensure_space(needed_h, repeat_section=False):
        nonlocal y, current_section
        if y + needed_h > (PAGE_H_PX - bottom_guard):
            _push_page()
            if repeat_section and current_section:
                y_new = _section(current_section, y)
                return y_new
        return y

    def _section(title, y_in):
        nonlocal current_section
        current_section = title
        d.text((x_left, y_in), title, anchor="la", font=header_font, fill="black")
        return y_in + spx(44)

    def _row(route, metric, page_no, y_in, *, color="black", clickable=True):
        link_color = (0, 0, 238) if clickable else color
        d.text((x_left, y_in), route, anchor="la", font=body_font, fill=link_color)
        d.text((x_mid, y_in), metric, anchor="la", font=body_font, fill=color)
        d.text((x_right, y_in), str(page_no), anchor="ra", font=body_font, fill=color)

        if clickable:
            try:
                bbox = body_font.getbbox(route)
                w = bbox[2] - bbox[0]
                h = bbox[3] - bbox[1]
                uy = y_in + h + spx(1)
                d.line([(x_left, uy), (x_left + w, uy)], fill=link_color, width=spx(2))
                rect = (x_left, y_in - spx(2), x_left + w + spx(6), y_in + h + spx(2))
                link_specs.append({"rect": rect, "page": int(page_no)})
            except Exception:
                pass
        return y_in + spx(26)

    # 1) Verification
    y = _ensure_space(spx(44) + spx(12))
    y = _section("Verification", y)
    y += spx(6)

    if mismatches:
        for m in mismatches:
            y = _ensure_space(spx(26), repeat_section=True)
            route = m.get("title", "Route")
            page_no = int(m.get("output_page") or 0)

            parts = []
            if m.get("overflow_mismatch"):
                parts.append(f"Overflow {m.get('declared_overflow')}→{m.get('computed_overflow')}")
            if m.get("total_mismatch"):
                parts.append(f"Total {m.get('declared_total')}→{m.get('computed_total')}")
            metric = " | ".join(parts) if parts else "Mismatch"

            y = _row(route, metric, page_no, y, color=(220, 0, 0))
        y += spx(12)
    else:
        y = _ensure_space(spx(44))
        d.text((x_left, y), "OK (NO MISMATCHES)", anchor="la", font=body_font, fill=(0, 140, 0))
        y += spx(44)

    # 2) Routes with 30+ Bags
    y = _ensure_space(spx(44) + spx(12))
    y = _section("Routes with 30+ Bags", y)
    y += spx(6)
    for bag_count, title, output_page in (routes_over_30 or []):
        y = _ensure_space(spx(26), repeat_section=True)
        y = _row(title, f"{bag_count} bags", output_page, y)
    y += spx(12)

    # 3) Routes with 50+ Overflow
    y = _ensure_space(spx(44) + spx(12))
    y = _section("Routes with 50+ Overflow", y)
    y += spx(6)
    for overflow_count, title, output_page in (routes_over_50_overflow or []):
        y = _ensure_space(spx(26), repeat_section=True)
        y = _row(title, f"{overflow_count} overflow", output_page, y)
    y += spx(12)

    # 4) Heaviest Totals (Top 10)
    y = _ensure_space(spx(44) + spx(12))
    y = _section("Routes with Heaviest Package Counts", y)
    y += spx(6)
    for total_pkgs, title, output_page in (top10_heavy_totals or []):
        y = _ensure_space(spx(26), repeat_section=True)
        y = _row(title, f"{total_pkgs} total", output_page, y)
    y += spx(12)

    # 5) Heaviest Commercial (Top 10)
    y = _ensure_space(spx(44) + spx(12))
    y = _section("Routes with Heaviest Commercial", y)
    y += spx(6)
    for comm_pkgs, title, output_page in (top10_commercial or []):
        y = _ensure_space(spx(26), repeat_section=True)
        y = _row(title, f"{comm_pkgs} commercial", output_page, y)

    pages.append(page)
    specs_pages.append(link_specs)
    return pages, specs_pages


# =========================
# TOC (COVER PAGE) + LINKS
# =========================
def _wave_label(time_label: str) -> str:
    if not time_label:
        return "Wave: ??:??"
    m = re.search(r"(\d{1,2}):(\d{2})", str(time_label))
    if not m:
        return "Wave: ??:??"
    hh = int(m.group(1))
    mm = int(m.group(2))
    return f"Wave: {hh:02d}:{mm:02d}"

def render_toc_page(date_label: str, route_entries):
    """
    1-page cover + TOC:
      - 3 EQUAL columns spanning full content width
      - columns are CENTER-aligned (header + route rows)
      - wave sections arranged row-major (wave1 col1, wave2 col2, wave3 col3, wave4 next row col1, ...)
      - dynamic list sizing to fill page height better
      - route titles are true internal PDF hyperlinks (added post-save)
    """
    page = Image.new("RGB", (PAGE_W_PX, PAGE_H_PX), "white")
    d = ImageDraw.Draw(page)

    # Stable title block
    title_font = get_font(spx(72))
    date_font = get_font(spx(34))
    sub_font = get_font(spx(28))

    x_center = PAGE_W_PX // 2
    y = spx(120)

    d.text((x_center, y), "Route Sheets", anchor="ma", font=title_font, fill="black")
    y += spx(82)
    d.text((x_center, y), str(date_label), anchor="ma", font=date_font, fill="black")
    y += spx(46)
    n = len(route_entries or [])
    d.text((x_center, y), f"({n} Routes)", anchor="ma", font=sub_font, fill=(60, 60, 60))
    y += spx(40)

    d.line([(MARGIN_PX, y), (PAGE_W_PX - MARGIN_PX, y)], fill=(0, 0, 0), width=spx(3))
    y += spx(22)

    entries = list(route_entries or [])

    def wave_sort_key(e):
        tl = e.get("time_label") or ""
        m = re.search(r"(\d{1,2}):(\d{2})", tl)
        if not m:
            return (999, 99, e.get("title", ""))
        return (int(m.group(1)), int(m.group(2)), e.get("title", ""))

    entries.sort(key=wave_sort_key)

    grouped = OrderedDict()
    for e in entries:
        label = _wave_label(e.get("time_label", ""))
        grouped.setdefault(label, []).append(e)

    # Sort each wave section: alphabetical first, then numeric
    def _natural_key(s: str):
        parts = re.findall(r"\d+|\D+", s)
        key = []
        for p in parts:
            if p.isdigit():
                key.append((1, int(p)))
            else:
                key.append((0, p.lower()))
        return key

    def _toc_item_key(e):
        t = str(e.get("title", ""))
        alpha_first = 0 if (t[:1].isalpha()) else 1
        return (alpha_first, _natural_key(t))

    for k in list(grouped.keys()):
        grouped[k].sort(key=_toc_item_key)

    wave_blocks = [(label, items) for label, items in grouped.items()]
    if not wave_blocks:
        return page, []

    cols = 3
    gap_x = spx(36)
    gap_y = spx(22)
    col_w = (CONTENT_W_PX - gap_x * (cols - 1)) // cols
    col_x = [MARGIN_PX + i * (col_w + gap_x) for i in range(cols)]
    col_mid = [x + col_w // 2 for x in col_x]

    bottom_limit = PAGE_H_PX - spx(110)
    available_h = max(spx(10), bottom_limit - y)

    def _text_w(font, s):
        try:
            b = font.getbbox(s)
            return b[2] - b[0]
        except Exception:
            return d.textlength(s, font=font)

    def _text_h(font):
        try:
            b = font.getbbox("Hg")
            return b[3] - b[1]
        except Exception:
            return int(font.size * 1.2)

    def col_of_wave(idx):
        return idx % cols

    best = None
    for row_size in range(spx(34), spx(16), -max(1, spx(1))):
        wave_size = max(spx(24), int(round(row_size * 1.22)))
        row_font = get_font(row_size)
        wave_font = get_font(wave_size)

        line_h = _text_h(row_font) + spx(6)
        header_h = _text_h(wave_font) + spx(8)
        pad_top = spx(6)
        pad_bottom = spx(4)

        heights = []
        for label, items in wave_blocks:
            heights.append(pad_top + header_h + max(1, len(items)) * line_h + pad_bottom)

        rows = (len(wave_blocks) + cols - 1) // cols
        row_heights = []
        for r in range(rows):
            chunk = heights[r * cols:(r + 1) * cols]
            row_heights.append(max(chunk) if chunk else 0)

        total_grid_h = sum(row_heights) + gap_y * max(0, rows - 1)
        if total_grid_h > available_h:
            continue

        ok = True
        for c in range(cols):
            max_wc = 0
            for idx, (label, items) in enumerate(wave_blocks):
                if col_of_wave(idx) != c:
                    continue
                max_wc = max(max_wc, _text_w(wave_font, label))
                for e in items:
                    max_wc = max(max_wc, _text_w(row_font, str(e.get("title", "Route"))))
            if max_wc > col_w:
                ok = False
                break
        if not ok:
            continue

        best = {
            "row_font": row_font,
            "wave_font": wave_font,
            "line_h": line_h,
            "header_h": header_h,
            "pad_top": pad_top,
            "gap_y": gap_y,
            "row_heights": row_heights,
            "rows": rows,
            "total_grid_h": total_grid_h,
        }
        break

    if best is None:
        row_font = get_font(spx(22))
        wave_font = get_font(spx(28))
        line_h = _text_h(row_font) + spx(6)
        header_h = _text_h(wave_font) + spx(8)
        pad_top = spx(6)
        rows = (len(wave_blocks) + cols - 1) // cols
        row_heights = [0] * rows
        best = {
            "row_font": row_font,
            "wave_font": wave_font,
            "line_h": line_h,
            "header_h": header_h,
            "pad_top": pad_top,
            "gap_y": gap_y,
            "row_heights": row_heights,
            "rows": rows,
            "total_grid_h": min(available_h, available_h),
        }

    row_font = best["row_font"]
    wave_font = best["wave_font"]
    line_h = best["line_h"]
    header_h = best["header_h"]
    pad_top = best["pad_top"]
    row_heights = best["row_heights"]
    rows = best["rows"]
    total_grid_h = best["total_grid_h"]

    extra = max(0, available_h - total_grid_h)
    grid_start_y = y + int(extra * 0.50)

    link_specs = []
    link_color = (0, 0, 238)

    cur_y = grid_start_y
    idx = 0
    for r in range(rows):
        if cur_y + row_heights[r] > bottom_limit:
            break

        for c in range(cols):
            if idx >= len(wave_blocks):
                break

            label, items = wave_blocks[idx]
            idx += 1

            xm = col_mid[c]
            yy = cur_y + pad_top

            d.text((xm, yy), label, anchor="ma", font=wave_font, fill="black")
            yy += header_h

            for e in items:
                t = e.get("title", "Route")
                pg = int(e.get("output_page", 0))
                d.text((xm, yy), t, anchor="ma", font=row_font, fill=link_color)
                try:
                    bbox = row_font.getbbox(t)
                    w = bbox[2] - bbox[0]
                    htxt = bbox[3] - bbox[1]
                    x0 = xm - w / 2.0
                    x1 = xm + w / 2.0
                    uy = yy + htxt + spx(1)
                    d.line([(x0, uy), (x1, uy)], fill=link_color, width=spx(2))
                    rect = (x0 - spx(3), yy - spx(2), x1 + spx(3), yy + htxt + spx(2))
                    link_specs.append({"rect": rect, "page": pg, "page_num": 1})
                except Exception:
                    pass
                yy += line_h

        cur_y += row_heights[r] + gap_y

    return page, link_specs


# =========================
# PDF LINK INSERTION (PyMuPDF) - ROBUST (full save, no saveIncr)
# =========================
def _try_add_all_links(
    pdf_path: str,
    toc_link_specs,
    *,
    dpi: int,
    default_from_page: int = 1,
    summary_start_page: int | None = None,
    summary_link_specs_pages=None,
):
    """
    Adds TOC + Summary internal GOTO links to an already-written PDF.
    Saves to a temp file then replaces the original (more reliable than saveIncr()).
    """
    try:
        import fitz  # PyMuPDF
    except Exception as e:
        print("[WARN] PyMuPDF not available; links will NOT be added:", repr(e), flush=True)
        return False

    import os

    pdf_path = str(pdf_path)
    tmp_path = str(Path(pdf_path).with_suffix(".linked.pdf"))

    doc = None
    try:
        doc = fitz.open(pdf_path)
        scale = 72.0 / float(dpi)

        added = 0

        # --- TOC links
        for spec in (toc_link_specs or []):
            from_page_1 = int(spec.get("page_num", default_from_page))
            to_page_1 = int(spec.get("page", 0))
            if to_page_1 <= 0:
                continue

            fp = from_page_1 - 1
            tp = to_page_1 - 1
            if fp < 0 or fp >= doc.page_count:
                continue
            if tp < 0 or tp >= doc.page_count:
                continue

            x0, y0, x1, y1 = spec["rect"]
            rect = fitz.Rect(float(x0) * scale, float(y0) * scale, float(x1) * scale, float(y1) * scale)
            doc[fp].insert_link({"kind": fitz.LINK_GOTO, "from": rect, "page": tp})
            added += 1

        # --- Summary links
        if summary_start_page is not None and summary_link_specs_pages:
            for idx, specs in enumerate(summary_link_specs_pages):
                sp = int(summary_start_page) - 1 + int(idx)
                if sp < 0 or sp >= doc.page_count:
                    continue

                page = doc[sp]
                for spec in (specs or []):
                    to_page_1 = int(spec.get("page", 0))
                    if to_page_1 <= 0:
                        continue
                    tp = to_page_1 - 1
                    if tp < 0 or tp >= doc.page_count:
                        continue

                    x0, y0, x1, y1 = spec["rect"]
                    rect = fitz.Rect(float(x0) * scale, float(y0) * scale, float(x1) * scale, float(y1) * scale)
                    page.insert_link({"kind": fitz.LINK_GOTO, "from": rect, "page": tp})
                    added += 1

        # Full save to temp then replace original
        doc.save(tmp_path, garbage=4, deflate=True)
        doc.close()
        doc = None

        os.replace(tmp_path, pdf_path)

        print(f"[LINKS] Added {added} internal links", flush=True)
        return added > 0

    except Exception as e:
        print("[WARN] Link insertion failed:", repr(e), flush=True)
        try:
            if doc:
                doc.close()
        except Exception:
            pass
        try:
            if os.path.exists(tmp_path):
                os.remove(tmp_path)
        except Exception:
            pass
        return False


# =========================
# MAIN BUILDER (Grouped + TOC + Summary)
# =========================
def build_stacked_pdf_with_summary_grouped(input_pdf: str, output_pdf: str, date_label: str, progress_cb=None):
    # If caller doesn't provide a progress callback, write _job_status.json next to output_pdf.
    if progress_cb is None:
        out_dir = Path(output_pdf).resolve().parent
        status_path = out_dir / "_job_status.json"
        last_write = {"t": 0.0, "sig": None}

        def _atomic_write(path: Path, payload: dict):
            tmp = path.with_suffix(path.suffix + ".tmp")
            tmp.write_text(json.dumps(payload), encoding="utf-8")
            tmp.replace(path)

        def progress_cb(**data):
            now = time.monotonic()
            sig = (
                int(data.get("pages_total", 0)),
                int(data.get("pages_done", 0)),
                int(data.get("current_page", 0)),
                str(data.get("stage", "")),
                str(data.get("detail", "")),
                int(data.get("percent", 0)),
            )
            if last_write["sig"] == sig and (now - last_write["t"]) < 0.75:
                return
            last_write["t"] = now
            last_write["sig"] = sig

            payload = {"ok": True, "ts": int(time.time()), **data}
            try:
                _atomic_write(status_path, payload)
            except Exception:
                pass

    def _cb(pages_total, pages_done, current_page, stage, detail):
        if not progress_cb:
            return
        # percent from route counters (stays < 100 until ready)
        pct = 25
        if pages_total > 0:
            pct = int(round((max(0, min(pages_total, pages_done)) / float(pages_total)) * 100))
            pct = max(2, min(99, pct))
        progress_cb(
            pages_total=int(pages_total),
            pages_done=int(pages_done),
            current_page=int(current_page),
            stage=str(stage),
            detail=str(detail),
            percent=int(pct),
        )

    # Placeholder TOC page at front
    pages = [Image.new("RGB", (PAGE_W_PX, PAGE_H_PX), "white")]

    toc_entries = []
    mismatches = []
    routes_over_30 = []
    routes_over_50_overflow = []
    combined_routes = []
    route_total_pkgs = []  # (total_pkgs, title, output_page)
    route_comm_pkgs = []   # (comm_pkgs, title, output_page)

    _cb(0, 0, 0, "Reading", "Extracting text…")

    with pdfplumber.open(input_pdf) as pdf:
        page_texts = [(p.extract_text() or "") for p in pdf.pages]

    groups = _group_pages(page_texts)
    if not groups:
        raise RuntimeError("No header pages detected (no STG.* + CX*).")

    total_routes = len(groups)
    done_routes = 0
    _cb(total_routes, 0, 0, "Processing", f"Found {total_routes} routes…")

    for g_idx, g in enumerate(groups, start=1):
        _cb(total_routes, done_routes, g_idx, "Processing", f"Route {g_idx}/{total_routes}…")

        combined_text = "\n\n".join(page_texts[i] for i in g).strip()
        parsed = parse_route_page(combined_text) if combined_text else None
        if not parsed:
            done_routes += 1
            _cb(total_routes, done_routes, g_idx, "Processing", f"Skipped unreadable route {g_idx}/{total_routes}")
            continue

        (
            rs,
            cx,
            style_label,
            time_label,
            bags,
            overs,
            decl_bags,
            decl_over,
            comm_pkgs,
            total_pkgs,
        ) = parsed

        title = f"{rs} ({cx})" if (rs and cx) else (rs or cx or "Route")
        pages_used = [i + 1 for i in g]

        bag_count = int(decl_bags) if decl_bags is not None else len(bags)
        declared_overflow = int(decl_over) if decl_over is not None else int(sum(pk for _, pk in overs))

        if len(g) > 1:
            combined_routes.append((title, pages_used, bag_count))

        texts, totals = assign_overflows(bags, overs)
        df = df_from(bags, texts, totals)

        bag_pk_total = int(sum(int(b.get("pkgs") or 0) for b in bags))
        computed_overflow_total = int(sum(int(t or 0) for t in totals if str(t).strip() != ""))
        sum_plus_overflow = int(bag_pk_total + computed_overflow_total)

        overflow_mismatch = (decl_over is not None and int(decl_over) != computed_overflow_total)
        total_mismatch = (total_pkgs is not None and int(total_pkgs) != int(sum_plus_overflow))

        mismatch_payload = None
        if overflow_mismatch or total_mismatch:
            mismatch_payload = {
                "title": title,
                "declared_overflow": decl_over,
                "computed_overflow": computed_overflow_total,
                "declared_total": total_pkgs,
                "computed_total": sum_plus_overflow,
                "overflow_mismatch": overflow_mismatch,
                "total_mismatch": total_mismatch,
            }

        table_img = render_table(
            df=df,
            title=title,
            style_label=style_label,
            date_label=date_label,
            time_label=time_label,
            bag_count=bag_count,
            declared_overflow=declared_overflow,
            commercial_pkgs=comm_pkgs,
            total_pkgs=total_pkgs,
            bags=bags,
        )
        tote_img = draw_tote(df, bags)

        available_h = PAGE_H_PX - TOP_MARGIN_PX - BOTTOM_MARGIN_PX
        needed_h = table_img.height + GAP_PX + tote_img.height
        if needed_h > available_h and needed_h > 0:
            scale = available_h / float(needed_h)
            warn(f"{title}: content too tall for letter page, scaling to {scale:.3f}")
            new_w_tbl = max(1, int(table_img.width * scale))
            new_h_tbl = max(1, int(table_img.height * scale))
            new_w_tote = max(1, int(tote_img.width * scale))
            new_h_tote = max(1, int(tote_img.height * scale))
            table_img = table_img.resize((new_w_tbl, new_h_tbl), Image.Resampling.LANCZOS)
            tote_img = tote_img.resize((new_w_tote, new_h_tote), Image.Resampling.LANCZOS)

        canvas = Image.new("RGB", (PAGE_W_PX, PAGE_H_PX), "white")

        x_tbl = MARGIN_PX + (CONTENT_W_PX - table_img.width) // 2
        x_tote = MARGIN_PX + (CONTENT_W_PX - tote_img.width) // 2

        y_tbl = TOP_MARGIN_PX
        canvas.paste(table_img, (x_tbl, y_tbl))
        canvas.paste(tote_img, (x_tote, y_tbl + table_img.height + GAP_PX))

        output_page = len(pages) + 1
        pages.append(canvas.convert("RGB"))

        toc_entries.append({"title": title, "output_page": int(output_page), "time_label": time_label or ""})

        if total_pkgs is not None:
            total_pkgs_value = int(total_pkgs)
        else:
            declared_overflow_fallback = int(decl_over) if decl_over is not None else int(sum(pk for _, pk in overs))
            total_pkgs_value = int(bag_pk_total + declared_overflow_fallback)

        route_total_pkgs.append((total_pkgs_value, title, output_page))
        route_comm_pkgs.append((int(comm_pkgs) if comm_pkgs is not None else 0, title, output_page))

        if bag_count >= 30:
            routes_over_30.append((bag_count, title, output_page))
        if declared_overflow >= 50:
            routes_over_50_overflow.append((declared_overflow, title, output_page))

        if mismatch_payload is not None:
            mismatch_payload["output_page"] = output_page
            mismatches.append(mismatch_payload)

        done_routes += 1
        _cb(total_routes, done_routes, g_idx, "Processing", f"Done: {title}")

        print(f"[ROUTE] {g_idx}/{len(groups)} => Pg {output_page}: {title}", flush=True)

    _cb(total_routes, done_routes, done_routes, "Summary", "Building summary & TOC…")

    routes_over_30.sort(key=lambda x: (-x[0], x[1]))
    routes_over_50_overflow.sort(key=lambda x: (-x[0], x[1]))
    combined_routes.sort(key=lambda x: x[1][0])

    route_total_pkgs.sort(key=lambda x: (-x[0], x[1]))
    route_comm_pkgs.sort(key=lambda x: (-x[0], x[1]))
    top10_heavy_totals = route_total_pkgs[:10]
    top10_commercial = route_comm_pkgs[:10]

    summary_pages, summary_link_specs_pages = render_summary_pages(
        mismatches,
        routes_over_30=routes_over_30,
        routes_over_50_overflow=routes_over_50_overflow,
        top10_heavy_totals=top10_heavy_totals,
        top10_commercial=top10_commercial,
    )

    summary_start_page = len(pages) + 1
    for sp in summary_pages:
        pages.append(sp.convert("RGB"))

    toc_img, toc_link_specs = render_toc_page(date_label, toc_entries)
    pages[0] = toc_img.convert("RGB")

    _cb(total_routes, done_routes, done_routes, "Saving", "Writing PDF…")
    pages[0].save(output_pdf, save_all=True, append_images=pages[1:], resolution=DPI)

    _cb(total_routes, done_routes, done_routes, "Linking", "Adding TOC + summary links…")
    _try_add_all_links(
        output_pdf,
        toc_link_specs,
        dpi=DPI,
        default_from_page=1,
        summary_start_page=summary_start_page,
        summary_link_specs_pages=summary_link_specs_pages,
    )

    _cb(total_routes, done_routes, done_routes, "Done", "Complete.")

    return {
        "output_pdf": output_pdf,
        "group_count": len(groups),
        "mismatch_count": len(mismatches),
        "mismatches": mismatches,
        "routes_over_30": routes_over_30,
        "routes_over_50_overflow": routes_over_50_overflow,
        "top10_heavy_totals": top10_heavy_totals,
        "top10_commercial": top10_commercial,
        "combined_routes": combined_routes,
        "toc_entries": toc_entries,
        "date_label": date_label,
    }


# Back-compat wrapper name (older processor imports)
def build_stacked_pdf_with_summary(input_pdf: str, output_pdf: str, date_label: str):
    return build_stacked_pdf_with_summary_grouped(input_pdf, output_pdf, date_label)
