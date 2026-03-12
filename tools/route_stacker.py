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
SCALE: float = 1.0
STRICT_TOTE_DATA: bool = False  # set True to hard-fail the run if any route has no bags parsed


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


STYLE = {
    "banner_bg": (211, 211, 211),
    "meta_grey": (85, 85, 85),
    "royal_blue": (0, 32, 194),
    "purple": (75, 0, 130),
    "lavender": (236, 232, 255),
    "bright_red": (210, 40, 40),
    "row_fill_teal": (238, 247, 247),
    "divider_teal": (0, 140, 140),
    "divider_grey": (170, 170, 170),
    "bag_colors": {
        "yellow": (246, 217, 74),
        "green": (83, 182, 53),
        "orange": (234, 99, 43),
        "black": (12, 10, 11),
        "navy": (57, 128, 240),
    },
}

LAYOUT = {
    "row_divider_h": spx(2),
    "table_cell_height": spx(64),
    "banner_height": spx(54),
    "table_margin": spx(22),
}

# Overflow pairing
PAIR_MAP = {"A": "T", "B": "U", "C": "W", "D": "X", "E": "Y", "G": "Z"}
INVERSE_PAIR = {v: k for k, v in PAIR_MAP.items()}

ZONE_RE = re.compile(r"^(?:[A-Z]-[0-9.]*[A-Z]+|99\.[A-Z0-9]+)$")  # Matches normal zone tags (e.g., A-12.3BC) and 99.* overflow tags (e.g., 99.A1).
SPLIT_RE = re.compile(r"^(\d+(?:\.\d+)*)?([A-Z]+)$")
TIME_RE = re.compile(r"\b(\d{1,2}:\d{2}\s*(?:AM|PM))\b", re.I)
_WS_RE = re.compile(r"\s+")
HEADER_RE = re.compile(r"\bsort\s+zone\s+(?:bag\s+)?pkgs?\b", re.I)
STG_RE = re.compile(r"\bSTG\.([A-Z0-9]+(?:\.[A-Z0-9]+)*\.\d+)\b", re.I)  # Captures the code after STG. for staging/route values like STG.ABC.12 or STG.A1.B2.34.
CX_RE  = re.compile(r"\b(?:CX|TX)\d{1,3}\b", re.I)


def _norm_line(s: str) -> str:
    return _WS_RE.sub(" ", (s or "")).strip()

BAG_COLORS_ALLOWED = {"Yellow", "Green", "Orange", "Black", "Navy"}


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
FONT_TOTE_TAG_BASE = get_font(spx(26))
FONT_TOTE_TAG_MIN = get_font(spx(18))
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
    print(f"[WARN {time.strftime('%H:%M:%S')}]", msg, flush=True)

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

def infer_style_label(text: str) -> str:
    t = (text or "").lower()

    if "on-road experience" in t or "on road experience" in t:
        return "Standard: On-Road Experience (Driver)"

    if "nursery" in t:
        for lvl in (3, 2, 1):
            if f"level {lvl}" in t or f"lvl {lvl}" in t:
                return f"Nursery LVL {lvl}"
        return "Nursery"

    return "Standard"

def extract_time_label(text: str):
    head = "\n".join((text or "").splitlines()[:10])
    m = TIME_RE.search(head)
    if not m:
        return None
    return " ".join(m.group(1).upper().split())

def extract_declared_counts(lines, route_title: str = ""):
    bag_ct = ov_ct = None
    for l in lines:
        if HEADER_RE.search(_norm_line(l)):
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

        if commercial is None and s.startswith("commercial packages"):
            for tok in reversed(l.split()):
                v = parse_int_safe(tok, "Commercial packages", route_title)
                if v is not None:
                    commercial = v
                    break

        if total is None and s.startswith("total packages"):
            for tok in reversed(l.split()):
                v = parse_int_safe(tok, "Total packages", route_title)
                if v is not None:
                    total = v
                    break

        if commercial is not None and total is not None:
            break

    return commercial, total


def extract_route_identity(text: str):
    text = text or ""
    lines = text.splitlines()
    head = "\n".join(lines[:40])

    m = STG_RE.search(head) or STG_RE.search(text)
    rs = m.group(1).upper() if m else None

    m2 = CX_RE.search(head) or CX_RE.search(text)
    cx = m2.group(0).upper() if m2 else None

    title = f"{rs} ({cx})" if (rs and cx) else (rs or cx or "Route")
    return rs, cx, title


# =========================
# PARSE ROUTE PAGE (ORDER BY PRINTED INDEX)
# =========================
def parse_route_page(text: str):
    """
    Parse route text into
    (rs, cx, style_label, time_label, bags, overs, decl_bags, decl_over, comm_pkgs, total_pkgs).

    Bags are ordered by their printed index number (the leftmost index token on each bag row).
    """
    text = text or ""
    lines = text.splitlines()
    rs, cx, route_title = extract_route_identity(text)

    decl_bags, decl_over = extract_declared_counts(lines, route_title)
    comm_pkgs, total_pkgs = extract_pkg_summaries(lines, route_title)
    
    try:
        hdr_idx = next(i for i, l in enumerate(lines) if HEADER_RE.search(_norm_line(l)))
    except StopIteration:
        return None


    bags: list[dict[str, Any]] = []
    overs: list[tuple[str, int]] = []

    for line in lines[hdr_idx + 1:]:
        norm = _norm_line(line)
        if not norm:
            continue
        if HEADER_RE.search(norm):
            continue

        low = norm.lower()
        if low.startswith(("total packages", "commercial packages")):
            continue

        toks = norm.split()
        ptr = 0

        while ptr < len(toks):
            tok0 = toks[ptr]
            if not tok0 or not tok0[0].isdigit():  # first character must be a digit (e.g. "1.")
                ptr += 1
                continue

            # 1) Bag row WITH sort zone: idx zone color bag pkgs
            if ptr + 4 < len(toks):
                zone = toks[ptr + 1].upper()
                color = toks[ptr + 2].capitalize()
                if is_zone(zone) and color in BAG_COLORS_ALLOWED:
                    idx_val = parse_int_safe(tok0, "Bag index", route_title)
                    bag_num_str = extract_bag_num_str(toks[ptr + 3], "Bag number (with zone)", route_title)
                    pk = parse_int_safe(toks[ptr + 4], "Bag pkgs", route_title)
                    if idx_val is not None and bag_num_str is not None and pk is not None:
                        bags.append({
                            "idx": idx_val,
                            "sort_zone": zone,
                            "bag": f"{color} {bag_num_str}",
                            "pkgs": pk,
                        })
                    ptr += 5
                    continue

            # 2) Bag row WITHOUT sort zone: idx color bag pkgs
            if ptr + 3 < len(toks):
                color = toks[ptr + 1].capitalize()
                if color in BAG_COLORS_ALLOWED:
                    idx_val = parse_int_safe(tok0, "Bag index (no zone)", route_title)
                    bag_num_str = extract_bag_num_str(toks[ptr + 2], "Bag number (no zone)", route_title)
                    pk = parse_int_safe(toks[ptr + 3], "Bag pkgs (no zone)", route_title)

                    if idx_val is not None and bag_num_str is not None and pk is not None:
                        # RULE: this is usually a second bag with the SAME sort zone as the bag above,
                        # but the PDF text dropped the zone. We do NOT "merge" bags.
                        # We:
                        # 1) inherit the previous bag's zone so it doesn't display as "no zone"
                        # 2) roll THIS bag's pkgs up into the previous bag (doubling effect)
                        # 3) hide pkgs on THIS bag (keep bag entry)
                        inherited_zone = None
                        pk_out = pk

                        if bags and bags[-1].get("sort_zone"):
                            inherited_zone = bags[-1]["sort_zone"]
                            bags[-1]["pkgs"] = int(bags[-1].get("pkgs") or 0) + int(pk or 0)
                            pk_out = None  # hide pkgs on the "no-zone" bag

                        bags.append({
                            "idx": idx_val,
                            "sort_zone": inherited_zone,
                            "bag": f"{color} {bag_num_str}",
                            "pkgs": pk_out,
                        })

                    ptr += 4
                    continue

            # 3) Overflow row: idx zone pkgs
            if ptr + 2 < len(toks):
                zone = toks[ptr + 1].upper()
                pk_tok = toks[ptr + 2]
                if is_zone(zone) and any(ch.isdigit() for ch in pk_tok):
                    pk_val = parse_int_safe(pk_tok, "Overflow line", route_title)
                    if pk_val is not None:
                        overs.append((zone, pk_val))
                    ptr += 3
                    continue

            ptr += 1
      

    # DO NOT return None just because bags is empty
    # (leave bags empty and let the builder handle it)

    bags.sort(key=lambda b: b.get("idx", 10**6))
    style_label = infer_style_label(text)
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
    z = str(z or "").strip().upper()
    if len(z) < 2:
        return z, ""

    if "-" not in z:
        return z[:-1], z[-1]

    prefix, tail = z.split("-", 1)
    tail = tail.strip()
    m = SPLIT_RE.fullmatch(tail)
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
    used_fallback = False
    fallback_events = []

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
            used_fallback = True
            fallback_events.append({"label_core": label_core, "count": int(count)})
            if last_assigned_bag is not None:
                bi = last_assigned_bag
            elif bags:
                bi = 0

        if bi is not None:
            texts[bi].append(f"{label_core} ({count})")
            totals[bi] += int(count)
            last_assigned_bag = bi

    return texts, totals, used_fallback, fallback_events


# =========================
# DATAFRAME
# =========================
def df_from(bags, texts, totals):
    assert len(bags) == len(texts) == len(totals), "Length mismatch in df_from inputs"
    rows = []
    for b, tags, tot in zip(bags, texts, totals):
        mid = "; ".join(tags)
        tot_disp = int(tot) if mid else ""  # blank if no overflow
        rows.append([b["bag"], mid, tot_disp])
    df = pd.DataFrame(rows, columns=["Bag", "Overflow Zone(s)", "Overflow Pkgs (total)"])
    df["Overflow Zone(s)"] = df["Overflow Zone(s)"].replace({np.nan: ""})
    return df


# =========================
# CHIP RENDERING
# =========================
CHIP_PAD_Y_PX = 6
CHIP_PAD_X_PX = 6
CHIP_GAP_PX = 4
CHIP_RADIUS_PX = 6
CHIP_OUTER_MAX_PX = 12

# Tote top-section tuning: keep existing structure, but tighten number→chip spacing
# and avoid a hard bottom-aligned chip stack.
TOTE_NUM_BASE_HEIGHT_RATIO = 0.54
TOTE_NUM_TO_CHIP_GAP_PX = 6
TOTE_CHIP_BOTTOM_PAD_PX = 10


def draw_chip_fitwidth(draw, text, max_w, *, font_size=None, forced_h=None):
    clean = "" if text is None else str(text).strip()
    if clean.lower() == "nan":
        clean = ""
    is99 = is_99_tag(clean)
    txt_color = STYLE["purple"] if is99 else (0, 0, 0)
    bg_color = STYLE["lavender"] if is99 else (245, 245, 245)

    pad_y = CHIP_PAD_Y_PX      # per-edge (top + bottom); total vertical padding = 2*pad_y
    pad_x = CHIP_PAD_X_PX      # per-edge (left + right); total horizontal padding = 2*pad_x

    max_w = max(1, int(max_w))
    avail_text_w = max(1, max_w - 2 * pad_x)

    _wcache: dict[tuple[int, str], int] = {}

    def _text_w(font, s: str) -> int:
        # Include font size in the cache key since this helper is called across size fallbacks.
        fs = int(getattr(font, "size", 0))
        key = (fs, s)
        if key in _wcache:
            return _wcache[key]
        bb = draw.textbbox((0, 0), s, font=font)
        w = bb[2] - bb[0]
        _wcache[key] = w
        return w

    def _fit_text(font, s: str) -> str:
        if not s:
            return ""
        ell = "…"
        ell_w = _text_w(font, ell)
        if ell_w > avail_text_w:
            return ell
        if _text_w(font, s) <= avail_text_w:
            return s
        # Binary search for the longest prefix that fits with the ellipsis.
        lo, hi = 0, len(s)
        best = 0
        while lo <= hi:
            mid = (lo + hi) // 2
            if _text_w(font, s[:mid]) + ell_w <= avail_text_w:
                best = mid
                lo = mid + 1
            else:
                hi = mid - 1

        cut = s[:best]
        return (cut + ell) if cut else ell

    if font_size is None:
        size = int(getattr(FONT_TOTE_TAG_BASE, "size", spx(26)))
        min_size = int(getattr(FONT_TOTE_TAG_MIN, "size", spx(18)))
        chosen = None
        while size >= min_size:
            f = get_font(size)
            # fit test with full text; only truncate later if needed at min size
            if _text_w(f, clean) <= avail_text_w:
                chosen = f
                break
            size -= 1
        font = chosen if chosen is not None else get_font(min_size)
    else:
        font = get_font(int(font_size))

    fitted = _fit_text(font, clean)
    bbox = draw.textbbox((0, 0), fitted, font=font)
    th = bbox[3] - bbox[1]

    chip_w = max_w
    natural_h = max(1, int(th + 2 * pad_y))
    chip_h = int(forced_h) if forced_h is not None else natural_h
    chip_h = max(1, chip_h)

    chip = Image.new("RGBA", (chip_w, chip_h), (0, 0, 0, 0))
    cd = ImageDraw.Draw(chip)
    try:
        cd.rounded_rectangle([0, 0, chip_w - 1, chip_h - 1], radius=CHIP_RADIUS_PX, fill=bg_color)
    except (AttributeError, TypeError):
        cd.rectangle([0, 0, chip_w - 1, chip_h - 1], fill=bg_color)

    cd.text((chip_w // 2, chip_h // 2), fitted, anchor="mm", font=font, fill=txt_color)
    return chip, chip_w, chip_h

def compute_base_h(tile_w: int) -> int:
    return int(tile_w * TOTE_NUM_BASE_HEIGHT_RATIO)


def compute_chip_stack_y(chip_area_top: int, chip_area_h: int, stack_h: int, chip_count: int) -> int:
    free_h = max(0, int(chip_area_h) - int(stack_h))
    if chip_count <= 0:
        return int(chip_area_top)

    if chip_count == 1:
        y_offset = free_h // 2 + spx(2)
    elif chip_count == 2:
        y_offset = free_h // 2 + spx(4)
    else:
        y_offset = free_h - spx(6)

    y_offset = max(0, min(free_h, y_offset))
    return int(chip_area_top) + y_offset


def plan_overflow_chips(draw, toks, tile_w):
    toks = [t.strip() for t in (toks or []) if t and str(t).strip()]
    if not toks:
        return {"mode": "none", "chips": [], "stack_h": 0, "outer": 0, "gap": CHIP_GAP_PX}

    # Consistent outer margin (no dynamic variability)
    outer = CHIP_OUTER_MAX_PX

    # Use base font metrics to derive a consistent chip height.
    fs = int(getattr(FONT_TOTE_TAG_BASE, "size", spx(26)))

    # Fixed chip height for all chips (consistency)
    pad_y = CHIP_PAD_Y_PX
    gap = CHIP_GAP_PX

    f = get_font(fs)
    bb = draw.textbbox((0, 0), "Ag", font=f)
    th = bb[3] - bb[1]
    target_h = max(1, int(th + 2 * pad_y))

    max_w = max(1, tile_w - 2 * outer)

    chips = []
    for t in toks:
        chip, cw, ch = draw_chip_fitwidth(
            draw,
            t,
            max_w,
            forced_h=target_h,
        )
        chips.append((chip, cw, ch, outer))

    stack_h = len(chips) * target_h + gap * max(0, len(chips) - 1)

    # We do not use "+N more" or 2-column layout; chips may still truncate as a last resort after shrinking to min size.
    # The tote tiles will grow to fit worst-case instead.
    return {
        "mode": "1col",
        "chips": chips,
        "gap": gap,
        "outer": outer,
        "stack_h": stack_h,
    }


# =========================
# TOTE RENDERING
# =========================
def draw_tote(df: pd.DataFrame, bags: list[dict[str, Any]], max_h: int | None = None) -> Image.Image:
    """Render the tote-board image from the tote dataframe and parsed bag metadata."""
    n = len(df)
    if n == 0:
        warn("draw_tote(): empty df (no tote rows). Rendering MISSING TOTE DATA placeholder.")
        return render_missing_tote_placeholder("")

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

    # Right-to-left, 3-row fill
    positions = []
    for col in range(cols - 1, -1, -1):
        for row in range(ROWS_GRID):
            positions.append((col, row))

    if max_h is not None:
        usable = max(1, int(max_h) - pad_y * (ROWS_GRID - 1))
        fixed = max(1, usable // ROWS_GRID)
        row_heights = [fixed] * ROWS_GRID
        img_h = fixed * ROWS_GRID + pad_y * (ROWS_GRID - 1)
    else:
        tile_ws_for_items = [col_ws[col] for col, _row in positions[:n]]

        # MUST match chip placement padding used when rendering inside each tile.
        top_pad = spx(TOTE_NUM_TO_CHIP_GAP_PX)
        bot_pad = spx(TOTE_CHIP_BOTTOM_PAD_PX)
        heights = []
        for i in range(n):
            tile_w_i = int(tile_ws_for_items[i]) if i < len(tile_ws_for_items) else int(tile_ws_for_items[-1])
            base_h = compute_base_h(tile_w_i)
            cell = df.iat[i, 1]
            mid = "" if pd.isna(cell) else str(cell)

            toks = [t.strip() for t in re.split(r";+", mid) if t.strip()]
            if toks:
                plan = plan_overflow_chips(_CHIP_D, toks, tile_w_i)
                planned_chip_stack_h = int(plan.get("stack_h", 0))
                tile_h = base_h + top_pad + planned_chip_stack_h + bot_pad
            else:
                # Minimal height when no chips
                tile_h = base_h + bot_pad

            heights.append(tile_h)

        # ONE RULE: all tiles match the worst-case tile height
        max_tile_h = max([1] + [int(h) for h in heights])

        row_heights = [max_tile_h] * ROWS_GRID
        img_h = max_tile_h * ROWS_GRID + pad_y * (ROWS_GRID - 1)

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
        tile_w_i = col_ws[col]
        x1 = x0 + tile_w_i
        base_h = compute_base_h(tile_w_i)
        if max_h is not None:
            min_chip_area_h = spx(54)
            # In fixed-height mode, cap the number zone so overflow chips always have room.
            base_h = min(base_h, max(0, tile_h - spx(TOTE_NUM_TO_CHIP_GAP_PX) - spx(TOTE_CHIP_BOTTOM_PAD_PX) - min_chip_area_h))

        bg = color_for_bag(df.iat[i, 0])
        d.rectangle([x0, y0, x1, y0 + tile_h], fill=bg, outline="black", width=spx(2))

        label = df.iat[i, 0]
        num = str(label).split()[-1]

        is_black_tote = bg == STYLE["bag_colors"]["black"]
        num_fill = (255, 255, 255) if is_black_tote else (0, 0, 0)
        halo_center = (150, 150, 150)

        num_x = (x0 + x1) // 2
        num_y = y0 + base_h // 2 + spx(14)  # your “14” vertical center shift

        try:
            d.text(
                (num_x, num_y),
                num,
                anchor="mm",
                font=FONT_TOTE_NUM,
                fill=num_fill,
                stroke_width=spx(1),
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
                    stroke_width=spx(1),
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

        cell = df.iat[i, 1]
        mid = "" if pd.isna(cell) else str(cell)
        toks = [t.strip() for t in re.split(r";+", mid) if t.strip()]

        top_pad = spx(TOTE_NUM_TO_CHIP_GAP_PX)
        bot_pad = spx(TOTE_CHIP_BOTTOM_PAD_PX)
        chip_area_top = y0 + base_h + top_pad
        chip_area_bot = y0 + tile_h - bot_pad
        chip_area_h = max(0, chip_area_bot - chip_area_top)

        plan = plan_overflow_chips(d, toks, tile_w_i)
        if plan.get("mode") == "1col":
            chips = plan.get("chips", [])
            gap = plan.get("gap", CHIP_GAP_PX)
            stack_h = sum(ch for _, _, ch, _ in chips) + gap * max(0, len(chips) - 1)
            cy = compute_chip_stack_y(chip_area_top, chip_area_h, stack_h, len(chips))
            for chip_img, _cw, ch, margin in chips:
                img.paste(chip_img, (x0 + margin, cy), mask=chip_img)
                cy += ch + gap

    return img

def render_missing_tote_placeholder(title: str, target_h: int | None = None) -> Image.Image:
    h = max(1, int(target_h)) if target_h is not None else spx(220)
    dy = max(spx(28), int(h * 0.18))
    img = Image.new("RGB", (CONTENT_W_PX, h), "white")
    d = ImageDraw.Draw(img)
    d.rectangle([0, 0, CONTENT_W_PX - 1, h - 1], outline=(220, 0, 0), width=spx(4))
    d.text((CONTENT_W_PX // 2, h // 2), "MISSING TOTE DATA", anchor="mm", font=FONT_TOTE_PKGS, fill=(220, 0, 0))
    d.text((CONTENT_W_PX // 2, h // 2 + dy), str(title), anchor="mm", font=get_font(spx(18)), fill=(80, 80, 80))
    return img


# =========================
# TABLE RENDERING
# =========================
def render_table_scaled(
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
    render_scale: float,
):
    render_scale = max(0.05, float(render_scale))

    cell_h = max(1, int(round(LAYOUT["table_cell_height"] * render_scale)))
    margin = max(1, int(round(LAYOUT["table_margin"] * render_scale)))
    banner_h = max(1, int(round(LAYOUT["banner_height"] * render_scale)))
    row_divider_h = max(1, int(round(LAYOUT["row_divider_h"] * render_scale)))

    base_banner_size = spx(40)
    base_table_size = spx(32)
    base_summary_size = spx(32)
    base_date_size = spx(22)
    base_style_size = spx(22)
    base_zone_size = spx(16)
    base_pkgs_size = spx(22)

    font_banner = get_font(max(1, int(round(base_banner_size * render_scale))))
    font_table = get_font(max(1, int(round(base_table_size * render_scale))))
    font_summary = get_font(max(1, int(round(base_summary_size * render_scale))))
    font_date = get_font(max(1, int(round(base_date_size * render_scale))))
    font_style = get_font(max(1, int(round(base_style_size * render_scale))))
    font_zone = get_font(max(1, int(round(base_zone_size * render_scale))))
    font_pkgs = get_font(max(1, int(round(base_pkgs_size * render_scale))))

    def sp(v: float) -> int:
        return max(1, int(round(spx(v) * render_scale)))

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
        d.text((sp(12), banner_h // 2), left, anchor="lm", font=font_date, fill=STYLE["meta_grey"])

    d.text((width // 2, banner_h // 2), title, anchor="mm", font=font_banner, fill="black")

    if style_label:
        d.text((width - sp(12), banner_h // 2), str(style_label).upper(), anchor="rm", font=font_style, fill=STYLE["meta_grey"])

    x = margin
    y0 = banner_h + margin
    right = width - margin

    # --- Dynamic column widths:
    # col1 fits longest Bag cell (zone + bag + pkgs), col3 matches col1, col2 gets the rest.
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

    pad_lr = sp(10)
    zone_gap = sp(6)
    pkg_gap = sp(6)

    available_w = right - x
    # Don't scale these with render_scale. The table width is fixed (CONTENT_W_PX),
    # so scaling the minimums can crush the side columns at larger scales.
    min_mid = int(available_w * 0.54)   # middle gets ~54% minimum
    min_side = int(available_w * 0.18)  # each side gets ~18% minimum
    max_side = max(0, (available_w - min_mid) // 2)

    max_w = 0
    last_zone_for_measure = None

    for df_idx in range(len(df)):
        label = str(df.iat[df_idx, 0] or "")
        bag_w = _tw(font_table, label)

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

        zone_w = _tw(font_zone, zone_display) + (zone_gap if zone_display else 0)
        pkg_w = _tw(font_pkgs, pkg_txt) + (pkg_gap if pkg_txt else 0)

        max_w = max(max_w, zone_w + bag_w + pkg_w + pad_lr * 2)
        if max_w >= max_side:
            max_w = max_side
            break

    # Start with measured width clamped to what can fit
    side = int(min(max_w, max_side))

    # Only enforce min_side if it doesn't violate min_mid
    if available_w - 2 * min_side >= min_mid:
        side = max(side, min_side)

    mid = available_w - 2 * side

    # Safety net: if mid got squeezed too far, reduce sides
    if mid < min_mid:
        max_side_for_target = max(0, (available_w - min_mid) // 2)
        side = max(0, min(side, max_side_for_target))
        if available_w - 2 * min_side >= min_mid:
            side = max(side, min_side)
        mid = available_w - 2 * side

    col_w = [side, mid, side]

    # Top summary row
    top = y0
    bot = top + cell_h
    d.rectangle([x, top, right, bot], outline="black", width=sp(2))
    d.text((x + sp(10), (top + bot) // 2), f"{bag_count} bags", anchor="lm", font=font_summary, fill=STYLE["royal_blue"])
    d.text((right - sp(10), (top + bot) // 2), f"{declared_overflow} overflow", anchor="rm", font=font_summary, fill=STYLE["royal_blue"])
    d.line([x, bot, right, bot], fill=STYLE["royal_blue"], width=sp(5))

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
        d.rectangle([x, top, right, bot], outline="black", width=sp(2))

        # divider under each 3-row block
        if r % 3 == 0:
            h = row_divider_h
            div_color = STYLE["divider_teal"] if teal_block else STYLE["divider_grey"]
            d.rectangle([x + sp(2), bot - h, right - sp(2), bot], fill=div_color)

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

                start_x = cx + sp(10)

                if zone_display:
                    d.text((start_x, ym), zone_display, anchor="lm", font=font_zone, fill=STYLE["meta_grey"])
                    zb = d.textbbox((0, 0), zone_display, font=font_zone)
                    start_x += (zb[2] - zb[0]) + sp(6)

                d.text((start_x, ym), label, anchor="lm", font=font_table, fill="black")

                if pkg_txt:
                    lb = d.textbbox((0, 0), label, font=font_table)
                    d.text((start_x + (lb[2] - lb[0]) + sp(6), ym), pkg_txt, anchor="lm", font=font_pkgs, fill=STYLE["bright_red"])

            # Overflow zones column
            elif c_idx == 1:
                toks = [t.strip() for t in re.split(r";+", text) if t.strip()]
                if not toks:
                    cx += w
                    continue

                # Build colored segments ("; " separators included)
                segs = []
                first = True
                for tok in toks:
                    prefix = "" if first else "; "
                    first = False
                    color = STYLE["purple"] if is_99_tag(tok) else (0, 0, 0)
                    segs.append((prefix + tok, color))

                pad = sp(8)
                max_w = max(0, w - 2 * pad)

                def seg_width(font, s: str) -> int:
                    bb = d.textbbox((0, 0), s, font=font)
                    return bb[2] - bb[0]

                def total_width(font) -> int:
                    return sum(seg_width(font, s) for s, _ in segs)

                # Try font sizes from normal down to a minimum
                start_size = int(getattr(font_table, "size", sp(32)))
                min_size = sp(18)  # floor so it doesn't become microscopic
                font = font_table

                tw = total_width(font)
                if tw > max_w:
                    chosen = None
                    for sz in range(start_size - 1, min_size - 1, -1):
                        f = get_font(sz)
                        if total_width(f) <= max_w:
                            chosen = f
                            break
                    font = chosen if chosen is not None else get_font(min_size)
                    tw = total_width(font)

                # If it fits now, center and draw
                if tw <= max_w:
                    sx = cx + (w - tw) // 2
                    for seg, color in segs:
                        sw = seg_width(font, seg)
                        d.text((sx, ym), seg, anchor="lm", font=font, fill=color)
                        sx += sw
                    cx += w
                    continue

                # Last-resort fallback: truncate with ellipsis at min font (never overlap)
                ell = "…"
                ell_w = seg_width(font, ell)
                sx = cx + pad
                remaining = max_w

                for seg, color in segs:
                    if remaining <= ell_w:
                        d.text((sx, ym), ell, anchor="lm", font=font, fill=(0, 0, 0))
                        break

                    sw = seg_width(font, seg)
                    if sw <= remaining:
                        d.text((sx, ym), seg, anchor="lm", font=font, fill=color)
                        sx += sw
                        remaining -= sw
                        continue

                    cut = seg
                    while cut and seg_width(font, cut) + ell_w > remaining:
                        cut = cut[:-1]
                    if cut:
                        d.text((sx, ym), cut + ell, anchor="lm", font=font, fill=color)
                    else:
                        d.text((sx, ym), ell, anchor="lm", font=font, fill=(0, 0, 0))
                    break

            # Overflow totals column
            else:
                d.text((cx + w - sp(10), ym), text, anchor="rm", font=font_table, fill="black")

            cx += w

    # Bottom totals row
    br_top = y0 + (len(df) + 1) * cell_h
    br_bot = br_top + cell_h
    d.rectangle([x, br_top, right, br_bot], outline="black", width=sp(4))

    if commercial_pkgs is not None:
        d.text((x + sp(10), (br_top + br_bot) // 2), f"{int(commercial_pkgs)} Commercial", anchor="lm", font=font_table, fill=STYLE["bright_red"])

    if total_pkgs is not None:
        d.text((right - sp(10), (br_top + br_bot) // 2), f"{int(total_pkgs)} Total", anchor="rm", font=font_table, fill=STYLE["bright_red"])

    # Outer border
    d.rectangle([x, y0, right, y0 + total_rows * cell_h], outline="black", width=sp(2))

    return im


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
    return render_table_scaled(
        df=df,
        title=title,
        style_label=style_label,
        date_label=date_label,
        time_label=time_label,
        bag_count=bag_count,
        declared_overflow=declared_overflow,
        commercial_pkgs=commercial_pkgs,
        total_pkgs=total_pkgs,
        bags=bags,
        render_scale=1.0,
    )


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
            if m.get("declared_counts_not_found"):
                parts.append("DECLARED COUNTS NOT FOUND")
            if m.get("missing_stg"):
                parts.append("MISSING STG")
            if m.get("missing_cx"):
                parts.append("MISSING CX")
            if m.get("skipped_no_header"):
                parts.append("SKIPPED ROUTE (NO HEADER)")
            if m.get("tote_missing"):
                parts.append("NO TOTE DATA")
            metric = " | ".join(parts) if parts else "Mismatch"

            y = _row(route, metric, page_no, y, color=(220, 0, 0), clickable=(page_no > 0))
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
    routes_missing_tote_data = []
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
        rs_guess, cx_guess, title_guess = extract_route_identity(combined_text)
        parsed = parse_route_page(combined_text) if combined_text else None
        if not parsed:
            mismatches.append({
                "title": title_guess,
                "missing_stg": rs_guess is None,
                "missing_cx": cx_guess is None,
                "skipped_no_header": True,
                "output_page": 0,
            })
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
        tote_missing = (len(bags) == 0)

        if tote_missing:
            routes_missing_tote_data.append(title)
            warn(f"{title}: NO BAG/TOTE DATA PARSED (source pages {pages_used})")
            if STRICT_TOTE_DATA:
                raise RuntimeError(f"{title}: NO BAG/TOTE DATA PARSED (pages {pages_used})")

        bag_count = int(decl_bags) if decl_bags is not None else len(bags)
        declared_overflow = int(decl_over) if decl_over is not None else int(sum(pk for _, pk in overs))

        if len(g) > 1:
            combined_routes.append((title, pages_used, bag_count))

        overflow_fallback_used = False
        fallback_events = []

        CONTENT_H = PAGE_H_PX - TOP_MARGIN_PX - BOTTOM_MARGIN_PX
        target_table_h = max(1, CONTENT_H - GAP_PX)

        def _render_table_to_target(df_local):
            table_local = render_table_scaled(
                df=df_local,
                title=title,
                style_label=style_label,
                date_label=date_label,
                time_label=time_label,
                bag_count=bag_count,
                declared_overflow=declared_overflow,
                commercial_pkgs=comm_pkgs,
                total_pkgs=total_pkgs,
                bags=bags,
                render_scale=1.0,
            )

            if table_local.height <= 0:
                return table_local

            s = target_table_h / float(table_local.height)
            table_local = render_table_scaled(
                df=df_local,
                title=title,
                style_label=style_label,
                date_label=date_label,
                time_label=time_label,
                bag_count=bag_count,
                declared_overflow=declared_overflow,
                commercial_pkgs=comm_pkgs,
                total_pkgs=total_pkgs,
                bags=bags,
                render_scale=s,
            )
            diff = abs(table_local.height - target_table_h)
            if diff > 2 or abs(s - 1.0) > 0.01:
                warn(f"{title}: rerender table scale={s:.3f} to hit {target_table_h}px (got {table_local.height}px)")

            for _ in range(2):
                if table_local.height <= 0:
                    break
                if table_local.height == target_table_h:
                    break
                s *= target_table_h / float(table_local.height)
                table_local = render_table_scaled(
                    df=df_local,
                    title=title,
                    style_label=style_label,
                    date_label=date_label,
                    time_label=time_label,
                    bag_count=bag_count,
                    declared_overflow=declared_overflow,
                    commercial_pkgs=comm_pkgs,
                    total_pkgs=total_pkgs,
                    bags=bags,
                    render_scale=s,
                )
                diff = abs(table_local.height - target_table_h)
                if diff > 2 or abs(s - 1.0) > 0.01:
                    warn(f"{title}: rerender table scale={s:.3f} to hit {target_table_h}px (got {table_local.height}px)")

            return table_local

        if tote_missing:
            texts, totals = [], []
            df = df_from([], [], [])
            tote_img = render_missing_tote_placeholder(title)
            target_table_h = max(1, CONTENT_H - GAP_PX - tote_img.height)
            table_img = _render_table_to_target(df)
        else:
            texts, totals, overflow_fallback_used, fallback_events = assign_overflows(bags, overs)
            df = df_from(bags, texts, totals)
            tote_img = draw_tote(df, bags, max_h=None)
            target_table_h = max(1, CONTENT_H - GAP_PX - tote_img.height)
            table_img = _render_table_to_target(df)

            if fallback_events:
                warn(
                    f"{title}: Fallback overflow assignment used => "
                    + ", ".join(f"{e['label_core']}({e['count']})" for e in fallback_events)
                )

        bag_pk_total = int(sum(int(b.get("pkgs") or 0) for b in bags))
        computed_overflow_total = int(sum(int(t or 0) for t in totals))
        sum_plus_overflow = int(bag_pk_total + computed_overflow_total)

        overflow_mismatch = (decl_over is not None and int(decl_over) != computed_overflow_total)
        total_mismatch = (total_pkgs is not None and int(total_pkgs) != int(sum_plus_overflow))

        declared_counts_not_found = (decl_bags is None or decl_over is None)
        missing_stg = (rs is None)
        missing_cx = (cx is None)

        mismatch_payload = None
        if (
            overflow_mismatch
            or total_mismatch
            or declared_counts_not_found
            or missing_stg
            or missing_cx
            or tote_missing
        ):
            mismatch_payload = {
                "title": title,
                "declared_overflow": decl_over,
                "computed_overflow": computed_overflow_total,
                "declared_total": total_pkgs,
                "computed_total": sum_plus_overflow,
                "overflow_mismatch": overflow_mismatch,
                "total_mismatch": total_mismatch,
                "declared_counts_not_found": declared_counts_not_found,
                "missing_stg": missing_stg,
                "missing_cx": missing_cx,
            }

        if tote_missing and mismatch_payload is not None:
            mismatch_payload["tote_missing"] = True

        available_h = PAGE_H_PX - TOP_MARGIN_PX - BOTTOM_MARGIN_PX
        needed_h = table_img.height + GAP_PX + tote_img.height
        if needed_h > available_h and needed_h > 0:
            corrected_target_table_h = max(1, available_h - GAP_PX - tote_img.height)
            table_natural = render_table_scaled(
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
                render_scale=1.0,
            )
            s = corrected_target_table_h / float(max(1, table_natural.height))
            for _ in range(3):
                table_img = render_table_scaled(
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
                    render_scale=s,
                )
                warn(f"{title}: rerender table scale={s:.3f} to hit {corrected_target_table_h}px (got {table_img.height}px)")
                if table_img.height == corrected_target_table_h or table_img.height <= 0:
                    break
                s *= corrected_target_table_h / float(table_img.height)

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

    if routes_missing_tote_data and STRICT_TOTE_DATA:
        missing = ", ".join(routes_missing_tote_data)
        raise RuntimeError(f"Strict tote data mode enabled: missing tote data for route(s): {missing}")

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
        "routes_missing_tote_data": routes_missing_tote_data,
        "toc_entries": toc_entries,
        "date_label": date_label,
    }


# Back-compat wrapper name (older processor imports)
def build_stacked_pdf_with_summary(input_pdf: str, output_pdf: str, date_label: str):
    return build_stacked_pdf_with_summary_grouped(input_pdf, output_pdf, date_label)
