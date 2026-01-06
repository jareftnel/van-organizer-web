#!/usr/bin/env python3
"""
Build Van Organizer HTML (v14 behavior) from:
- Excel: bag→overflow mapping (Bags_with_Overflow)
- PDF : Sort Zone + Bag Pkgs + wave time (merged by bag index)

Optimizations (no output/UX changes):
- Workbook opened in read_only mode
- PDF opened once (title + parsing in one pass)
- Optional on-disk cache for PDF parse (huge speedup on repeat runs)
"""

from __future__ import annotations
import argparse
import datetime as _dt
import json
import os
import re
from pathlib import Path
from typing import Dict, List, Tuple, Optional

import openpyxl
import pdfplumber


CACHE_VERSION_PDF = 2
CACHE_VERSION_ROUTES = 2

# ----------------------------- Regex (precompiled) -----------------------------
PAT_HEADER = re.compile(r'\b(DDF\d+)\s*·\s*([A-Z]{3},\s*[A-Z]{3}\s+\d{1,2},\s+\d{4})\b')
PAT_DATE_ONLY = re.compile(r'\b([A-Z]{3},\s*[A-Z]{3}\s+\d{1,2},\s+\d{4})\b')
PAT_FILE_DATE = re.compile(r'(\d{2})_(\d{2})_(\d{4})')

PAT_ROW_FULL = re.compile(r'^\s*(\d+)\s+([A-Z]-\d+(?:\.\d+)?[A-Z]?)\s+([A-Za-z]+)\s+([0-9A-Za-z]+)\s+(\d+)(?:\s+|$)')
PAT_ROW_NOSZ = re.compile(r'^\s*(\d+)\s+([A-Za-z]+)\s+([0-9A-Za-z]+)\s+(\d+)(?:\s+|$)')
PAT_TIME = re.compile(r'\b(\d{1,2}:\d{2}\s*[AP]M)\b')

PAT_OV_ZONE_CNT = re.compile(r'^([0-9]+\.[0-9]+[A-Z])\s*\((\d+)\)\s*$')
PAT_OV_ZONE = re.compile(r'^([0-9]+\.[0-9]+[A-Z])')
SHEET_RE = re.compile(r'^([A-Z]\.\d+)_?(CX\d+)$')


# ----------------------------- Helpers -----------------------------
def _time_to_minutes(t: str) -> Optional[int]:
    t = (t or "").strip().upper()
    m = re.match(r'^(\d{1,2}):(\d{2})\s*([AP]M)$', t)
    if not m:
        return None
    hh = int(m.group(1))
    mm = int(m.group(2))
    ap = m.group(3)
    if ap == "AM":
        if hh == 12:
            hh = 0
    else:
        if hh != 12:
            hh += 12
    return hh * 60 + mm


def _sort_route_short(rs: str) -> Tuple[str, int]:
    m = re.match(r'^([A-Z]+)\.(\d+)$', rs or "")
    if not m:
        return (rs or "", 0)
    return (m.group(1), int(m.group(2)))


def _parse_zone_counts(zones_str: str) -> List[Tuple[str, int]]:
    if not zones_str:
        return []
    parts = [p.strip() for p in str(zones_str).split(";") if p and str(p).strip()]
    out: List[Tuple[str, int]] = []
    for p in parts:
        m = PAT_OV_ZONE_CNT.match(p)
        if m:
            out.append((m.group(1), int(m.group(2))))
            continue
        m2 = PAT_OV_ZONE.match(p)
        if m2:
            out.append((m2.group(1), 0))
    return out


def _cache_path_for(pdf_path: str) -> Path:
    p = Path(pdf_path)
    return p.with_suffix(p.suffix + ".vanorg_cache.json")


def _load_pdf_cache(pdf_path: str) -> Optional[dict]:
    cache_path = _cache_path_for(pdf_path)
    if not cache_path.exists():
        return None
    try:
        st = os.stat(pdf_path)
        with cache_path.open("r", encoding="utf-8") as f:
            obj = json.load(f)
        meta = obj.get("meta", {})
        if meta.get("v") != CACHE_VERSION_PDF:
            return None
        if meta.get("size") != st.st_size:
            return None
        if meta.get("mtime") != int(st.st_mtime):
            return None
        data = obj.get("data")
        if not data or not data.get("pdf_meta"):
            return None
        return data
    except Exception:
        return None


def _save_pdf_cache(pdf_path: str, data: dict) -> None:
    try:
        st = os.stat(pdf_path)
        cache_path = _cache_path_for(pdf_path)
        payload = {
            "meta": {"v": CACHE_VERSION_PDF, "size": st.st_size, "mtime": int(st.st_mtime)},
            "data": data,
        }
        cache_path.write_text(json.dumps(payload, ensure_ascii=False), encoding="utf-8")
    except Exception:
        pass
def _routes_cache_path_for(xlsx_path: str) -> Path:
    p = Path(xlsx_path)
    return p.with_suffix(p.suffix + ".vanorg_routes_cache.json")


def _load_routes_cache(pdf_path: str, xlsx_path: str) -> Optional[dict]:
    cache_path = _routes_cache_path_for(xlsx_path)
    if not cache_path.exists():
        return None
    try:
        pst = os.stat(pdf_path)
        xst = os.stat(xlsx_path)
        with cache_path.open("r", encoding="utf-8") as f:
            obj = json.load(f)
        meta = obj.get("meta", {})
        if meta.get("v") != CACHE_VERSION_ROUTES:
            return None
        if meta.get("pdf_size") != pst.st_size or meta.get("pdf_mtime") != int(pst.st_mtime):
            return None
        if meta.get("xlsx_size") != xst.st_size or meta.get("xlsx_mtime") != int(xst.st_mtime):
            return None
        data = obj.get("data")
        if not data or not data.get("routes"):
            return None
        return data
    except Exception:
        return None


def _save_routes_cache(pdf_path: str, xlsx_path: str, data: dict) -> None:
    try:
        pst = os.stat(pdf_path)
        xst = os.stat(xlsx_path)
        cache_path = _routes_cache_path_for(xlsx_path)
        payload = {
            "meta": {
                "v": CACHE_VERSION_ROUTES,
                "pdf_size": pst.st_size, "pdf_mtime": int(pst.st_mtime),
                "xlsx_size": xst.st_size, "xlsx_mtime": int(xst.st_mtime),
            },
            "data": data,
        }
        cache_path.write_text(json.dumps(payload, ensure_ascii=False), encoding="utf-8")
    except Exception:
        pass



# ----------------------------- Parsing -----------------------------
def parse_pdf_meta(pdf_path: str, use_cache: bool = True) -> Tuple[str, str, Dict[str, Dict[int, dict]], Dict[str, str]]:
    """
    Returns:
      header_title, route_code, pdf_meta[route_short][idx] = {sort_zone, pkgs}, route_time[route_short] = "11:20 AM"
    """
    if use_cache:
        cached = _load_pdf_cache(pdf_path)
        if cached:
            return cached["header_title"], cached["route_code"], cached["pdf_meta"], cached["route_time"]

    header_title = ""
    route_code = "DDF5"
    date_str = ""

    pdf_meta: Dict[str, Dict[int, dict]] = {}
    route_time: Dict[str, str] = {}

    def _group_words_into_lines(words: List[dict], y_tol: float = 2.0) -> List[str]:
        if not words:
            return []
        words = sorted(words, key=lambda w: (float(w.get("top", 0.0)), float(w.get("x0", 0.0))))
        lines: List[List[dict]] = []
        cur: List[dict] = []
        cur_y: Optional[float] = None

        for w in words:
            y = float(w.get("top", 0.0))
            if cur_y is None:
                cur_y = y
                cur = [w]
                continue
            if abs(y - cur_y) <= y_tol:
                cur.append(w)
            else:
                lines.append(cur)
                cur = [w]
                cur_y = y
        if cur:
            lines.append(cur)

        out_lines: List[str] = []
        for ln in lines:
            ln_sorted = sorted(ln, key=lambda w: float(w.get("x0", 0.0)))
            text = " ".join((w.get("text") or "").strip() for w in ln_sorted if (w.get("text") or "").strip())
            text = re.sub(r"\s+", " ", text).strip()
            if text:
                out_lines.append(text)
        return out_lines

    with pdfplumber.open(pdf_path) as pdf:
        t0 = (pdf.pages[0].extract_text() or "")
        m = PAT_HEADER.search(t0)
        if m:
            route_code = m.group(1)
            date_str = m.group(2).upper()
        else:
            m2 = PAT_DATE_ONLY.search(t0)
            if m2:
                date_str = m2.group(1).upper()
            else:
                m3 = PAT_FILE_DATE.search(pdf_path)
                if m3:
                    mm, dd, yyyy = map(int, m3.groups())
                    dt = _dt.date(yyyy, mm, dd)
                    date_str = dt.strftime("%a, %b %d, %Y").upper()
        header_title = f"{route_code} • {date_str}".strip(" •")

        for page in pdf.pages:
            page_text = page.extract_text() or ""
            if "Sort Zone" not in page_text or "Pkgs" not in page_text:
                continue

            lines_quick = [ln.strip() for ln in page_text.splitlines() if ln and ln.strip()]
            route_short = ""
            for ln in lines_quick[:20]:
                if ln.startswith("STG."):
                    route_short = ln.replace("STG.", "").strip()
                    break
            if not route_short:
                words0 = page.extract_words(use_text_flow=True, keep_blank_chars=False) or []
                for w in words0:
                    t = (w.get("text") or "").strip()
                    if t.startswith("STG."):
                        route_short = t.replace("STG.", "").strip()
                        break
            if not route_short:
                continue

            if route_short not in route_time:
                tm = PAT_TIME.search(page_text)
                if tm:
                    route_time[route_short] = tm.group(1).upper()

            meta_by_idx = pdf_meta.get(route_short)
            if meta_by_idx is None:
                meta_by_idx = {}
                pdf_meta[route_short] = meta_by_idx

            words = page.extract_words(use_text_flow=True, keep_blank_chars=False) or []
            line_texts = _group_words_into_lines(words, y_tol=2.0)

            for ln in line_texts:
                m = PAT_ROW_FULL.match(ln)
                if m:
                    idx = int(m.group(1))
                    meta_by_idx[idx] = {"sort_zone": m.group(2), "pkgs": int(m.group(5))}
                    continue
                m2 = PAT_ROW_NOSZ.match(ln)
                if m2:
                    idx = int(m2.group(1))
                    if idx not in meta_by_idx:
                        meta_by_idx[idx] = {"sort_zone": "", "pkgs": int(m2.group(4))}

    if use_cache and pdf_meta:
        _save_pdf_cache(pdf_path, {
            "header_title": header_title,
            "route_code": route_code,
            "pdf_meta": pdf_meta,
            "route_time": route_time
        })

    return header_title, route_code, pdf_meta, route_time


def parse_excel_routes(xlsx_path: str, pdf_meta: Dict[str, Dict[int, dict]], route_time: Dict[str, str]) -> List[dict]:
    wb = openpyxl.load_workbook(xlsx_path, data_only=True, read_only=True)

    routes: List[dict] = []
    for sheet_name in wb.sheetnames:
        if sheet_name == "INDEX":
            continue
        m = SHEET_RE.match(sheet_name)
        if not m:
            continue
        rs, cx = m.group(1), m.group(2)
        ws = wb[sheet_name]

        combined: List[dict] = []
        bags: List[str] = []
        overflow_total = 0
        ov_agg: Dict[str, int] = {}

        ov_seq: List[dict] = []
        # Rows: Bag | Overflow Zone(s) | Overflow Pkgs (total)
        for row in ws.iter_rows(values_only=True):
            if not row:
                continue
            bag = row[0] if len(row) > 0 else None
            zones = row[1] if len(row) > 1 else None
            total_cell = row[2] if len(row) > 2 else None
            if bag is None:
                continue
            bag_s = str(bag).strip()
            if not bag_s:
                continue

            zones_s = "" if zones is None else str(zones).strip()

            total_val = None
            if total_cell is not None:
                tc_s = str(total_cell).strip()
                if tc_s:
                    # Excel might store as float
                    total_val = int(float(total_cell))

            combined.append({"bag": bag_s, "zones": zones_s, "total": "" if total_val is None else str(total_val)})
            bags.append(bag_s)

            if total_val is not None:
                overflow_total += total_val

            for z, cnt in _parse_zone_counts(zones_s):
                ov_seq.append({"zone": z, "count": cnt, "bag_idx": len(bags)})
                ov_agg[z] = ov_agg.get(z, 0) + cnt

        overflow_agg = [{"zone": k, "count": v} for k, v in sorted(ov_agg.items(), key=lambda x: x[0])]

        meta_by_idx = pdf_meta.get(rs, {})
        bags_detail: List[dict] = []
        for i, bag in enumerate(bags, start=1):
            bag_id = bag.split(" ", 1)[1] if " " in bag else bag
            meta = meta_by_idx.get(i)
            bags_detail.append({
                "idx": i,
                "bag": bag,
                "bag_id": bag_id,
                "sort_zone": meta["sort_zone"] if meta else "",
                "pkgs": meta["pkgs"] if meta else None
            })

        routes.append({
            "route_short": rs,
            "cx": cx,
            "wave_time": route_time.get(rs, ""),
            "bags_count": len(bags),
            "overflow_total": overflow_total,
            "bags_detail": bags_detail,
            "overflow_agg": overflow_agg,
            "overflow_seq": ov_seq,
            "combined": combined
        })

    # Sort: wave time group order then alpha+numeric route_short
    times_sorted = sorted({r.get("wave_time", "") for r in routes if r.get("wave_time", "")}, key=lambda x: _time_to_minutes(x) or 10**9)
    wave_rank = {t: i for i, t in enumerate(times_sorted, start=1)}

    def _route_sort_key(r: dict):
        wt = r.get("wave_time", "")
        return (wave_rank.get(wt, 999),) + _sort_route_short(r.get("route_short", ""))

    routes.sort(key=_route_sort_key)
    return routes


def build_wave_labels(routes: List[dict]) -> dict:
    times_sorted = sorted({r.get("wave_time", "") for r in routes if r.get("wave_time", "")}, key=lambda x: _time_to_minutes(x) or 10**9)
    suffix = {1: "st", 2: "nd", 3: "rd"}
    out = {}
    for i, t in enumerate(times_sorted, start=1):
        out[t] = f"{i}{suffix.get(i, 'th')} wave"
    return out


# ----------------------------- HTML template (unchanged behavior) -----------------------------
HTML_TEMPLATE = r"""<!doctype html>
<html lang="en">
<head>
<meta charset="utf-8"/>
<meta name="viewport" content="width=device-width, initial-scale=1"/>
<title>__HEADER_TITLE__</title>
<style>
:root{--bg:#0b0f14;--panel:#0f1722;--text:#e8eef6;--muted:#97a7bd;--border:#1c2a3a;--accent:#3fa7ff;}
*, *::before, *::after{box-sizing:border-box}
html,body{height:100%;width:100%}
body{margin:0;min-height:100vh;overflow:hidden;font-family:system-ui,-apple-system,Segoe UI,Roboto,Arial,sans-serif;background:radial-gradient(1400px 800px at 20% 0%, #101826, var(--bg));color:var(--text);}
.organizerPage{width:100%;max-width:none;min-width:0;margin:0;padding:16px 24px;height:100vh;display:flex;flex-direction:column;overflow:hidden;}
.organizerHeader{flex:0 0 auto;display:flex;flex-direction:column;gap:12px;min-width:0}
.organizerBody{flex:1 1 auto;min-height:0;width:100%;max-width:100%;overflow:hidden;padding-left:24px;padding-right:24px;padding-top:24px;padding-bottom:24px}
.organizerRoot{width:100%;max-width:none;min-width:0;margin:0}
.controls{display:flex;flex-direction:column;gap:12px;min-width:0;width:100%}
.header{display:flex;flex-direction:column;gap:12px;min-width:0}
.topbar{display:flex;gap:10px;align-items:center;flex-wrap:wrap;background:rgba(0,0,0,.25);border:1px solid var(--border);border-radius:14px;padding:12px 12px;min-width:0}
.topbar > *{min-width:0}
.brand{font-weight:900}
.sel{margin-left:10px}
select,input{background:rgba(255,255,255,.04);border:1px solid var(--border);color:var(--text);border-radius:12px;padding:10px 12px}
select{min-width:240px; color-scheme: dark;}
/* Make native dropdown readable (Chrome/Windows) */
select option{background:#0f1722;color:#e8eef6;}
select optgroup{background:#0b0f14;color:#97a7bd;font-weight:900;}

input{min-width:260px;flex:1}
.subHeaderRow{
  display:flex;
  justify-content:space-between;
  align-items:flex-start;
  gap:16px;
  width:100%;
  margin-top:10px;
}
.subHeaderLeft{
  display:flex;
  flex-direction:column;
  gap:8px;
  min-width:420px;
}
.subHeaderRight{
  display:flex;
  justify-content:flex-end;
  align-items:flex-start;
  flex:1;
  padding-top:2px;
}
.routeTitle{
  height:34px;
  display:flex;
  align-items:center;
  font-weight:900;
}
.toggleRow{
  display:flex;
  align-items:center;
  gap:10px;
  flex-wrap:wrap;
}
.tabsRow{display:flex;gap:10px;margin:0 !important;}
.tab{padding:8px 12px;border:1px solid var(--border);border-radius:999px;background:rgba(255,255,255,.03);cursor:pointer;font-weight:700;user-select:none}
.tab.active{background:rgba(255,255,255,.10)}
.card{margin-top:14px;border:1px solid var(--border);border-radius:18px;background:rgba(0,0,0,.22);padding:14px;min-width:0}
.content{margin-top:0;width:100%;max-width:none;min-width:0;overflow:hidden;height:100%;display:flex;flex-direction:column;gap:16px}
.card.plain{background:transparent;border:none;padding:0;}
.hint{color:var(--muted);font-size:12px;margin-top:4px}
.badge{display:inline-flex;align-items:center;gap:6px;font-size:12px;color:var(--muted)}
.dot{width:8px;height:8px;border-radius:99px;background:var(--accent)}

/* tote cards */

/* tote cards */
.toteWrap{width:100%;min-width:0;display:flex;flex:1 1 auto;min-height:0}
.toteBoard{
  display:grid;
  width:100%;
  height:100%;
  gap:var(--tileGap, 16px);
  padding:0;
  min-width:0;
  grid-template-rows: repeat(3, var(--tileH, 1fr));
  grid-template-columns: repeat(var(--cols), var(--tileW, 1fr));
  grid-auto-flow: column;
  justify-content: center;
  align-content: center;
  justify-items: center;
  align-items: center;
}
.bagsGrid{width:100%;max-width:100%;height:100%;overflow:hidden}
.toteBoard{flex:1 1 auto}
.toteCol{display:flex;flex-direction:column;gap:14px;}

.toteCard{
  position:relative;
  width:var(--tileW, 100%);
  min-width:0;
  height:var(--tileH, auto);
  aspect-ratio:4/3;
  max-width:100%;
  max-height:100%;
  border-radius:18px;
  background:rgba(10,14,20,.72);
  border:1px solid rgba(255,255,255,.08);
  box-shadow: 0 10px 28px rgba(0,0,0,.35), 0 2px 0 rgba(0,0,0,.10);
  overflow:hidden;
  cursor:pointer;
  container-type:inline-size;
  direction:ltr;
  display:grid;
  grid-template-rows:auto 1fr auto;
  row-gap:10px;
  padding:14px 16px;
  min-height:160px;
}
.toteCard *{box-sizing:border-box;}
.toteCard.draggable{cursor:grab;}
.toteCard.dragging{opacity:.25;}
.toteCard.dropTarget{outline:2px dashed rgba(90,170,255,.85); outline-offset:2px;}
.toteCard.loaded{filter:grayscale(.85) brightness(.72);}

.toteTopRow{
  display:flex;
  justify-content:space-between;
  align-items:center;
  gap:10px;
}
.toteMetaRight{
  display:flex;
  align-items:center;
  gap:6px;
}
.toteBar{
  flex:1;
  height:8px;
  border-radius:999px;
  background: linear-gradient(90deg, var(--chipL, #2a74ff) 0 50%, var(--chipR, var(--chipL, #2a74ff)) 50% 100%);
  box-shadow: inset 0 0 0 1px rgba(255,255,255,.10);
  pointer-events:none;
  min-width:40px;
}

.toteIdx{
  width:26px; height:26px;
  display:flex; align-items:center; justify-content:center;
  border-radius:999px;
  background:rgba(0,0,0,.72);
  border:1px solid rgba(255,255,255,.16);
  font-weight:900;
  font-size:12px;
  flex-shrink:0;
}

.totePkg{
  font-weight:900;
  font-size:11px;
  color:#ff4b4b;
  flex-shrink:0;
}

.toteStar{
  width:22px; height:22px;
  display:flex; align-items:center; justify-content:center;
  border-radius:8px;
  background:rgba(0,0,0,.55);
  border:1px solid rgba(255,255,255,.10);
  font-weight:900;
  font-size:16px;
  color:#ff4b4b;
  line-height:1;
  user-select:none;
  flex-shrink:0;
}
.toteStar.combine{
  color:#FFD400;
  border-color: rgba(255,212,0,.45);
  background: rgba(255,212,0,.12);
}
.toteStar.on{
  border-color: rgba(255,75,75,.45);
  background: rgba(255,75,75,.08);
}
.toteBigNumber{
  display:flex;
  align-items:center;
  justify-content:center;
  text-align:center;
  font-weight:800;
  line-height:1;
  font-size: clamp(44px, 5vw, 64px);
  margin:0;
  letter-spacing:1px;
  white-space:nowrap;
  position:static !important;
}
.toteBigNumberStack{
  flex-direction:column;
  gap: clamp(2px, 1.2cqi, 10px);
}
.toteBigNumberLine{
  font-weight:900;
  letter-spacing:1px;
  line-height:1;
  max-width:100%;
  white-space:nowrap;
  font-size: clamp(32px, 4.5vw, 56px);
}
.toteBottomRow{
  display:flex;
  justify-content:center;
  gap:10px;
  align-items:center;
  flex-wrap:wrap;
  line-height:1.1;
  text-align:center;
  font-weight:800;
  letter-spacing:.2px;
  opacity:.92;
  font-size: clamp(11px, 7cqi, 14px);
  position:static !important;
}
.ovZone{color:inherit;}
.ovZone99{color:#b46bff;}
.toteBottomRow .ovLine{line-height:1.25;}

@container (max-width: 240px){
  .toteBigNumber{ font-size: clamp(36px, 6vw, 52px); }
  .toteBigNumberLine{ font-size: clamp(24px, 5vw, 40px); }
  .toteBottomRow{ font-size: clamp(10px, 8cqi, 13px); }
}

/* tables */
table{width:100%;border-collapse:separate;border-spacing:0}
th,td{padding:10px 10px;border-bottom:1px solid rgba(255,255,255,.06)}

/* Overflow layout */
.ovTable{width:100%;table-layout:fixed}
.ovTable td:nth-child(2){white-space:nowrap}
.ovTable td:nth-child(4){white-space:nowrap}

.ovWrap{width:100%;max-width:none;margin:0;padding:0 18px;}
.controlsRow{min-width:0;display:flex;justify-content:space-between;align-items:center;gap:12px;flex-wrap:wrap}
.controlsRow > *{min-width:0}
.ovHeader{display:flex;justify-content:space-between;align-items:center;gap:12px;flex-wrap:wrap}
.ovHeader > *{min-width:0}
.ovHeaderRight{display:flex;align-items:center;gap:10px;flex-wrap:wrap}
.ovTitleRow{display:flex;align-items:center;gap:10px;flex-wrap:wrap}
.downloadRow{flex-basis:100%;display:flex;justify-content:flex-end}
.downloadBtn{
  display:inline-flex;align-items:center;justify-content:center;
  padding:6px 12px;border-radius:999px;
  border:1px solid rgba(140,170,200,.6);
  background:#3fa7ff;color:#001018;
  font-weight:900;text-decoration:none;letter-spacing:.02em;
}
.downloadBtn:hover{filter:brightness(1.08)}
.downloadBtn:active{transform:translateY(1px)}
.syncBtn{
  padding:6px 12px;
  border-radius:999px;
  border:1px solid rgba(140,170,200,.6);
  background:#5f7fa6;
  color:#0b0f14;
  font-weight:900;
  cursor:pointer;
}
.syncBtn:hover{filter:brightness(1.08)}
.syncBtn:active{transform:translateY(1px)}

/* Overflow checklist cells */
.ovChecks{display:flex;flex-wrap:wrap;gap:8px;align-items:center;justify-content:flex-start;min-height:22px}
.ovBox{
  width:18px;height:18px;border-radius:5px;
  border:1px solid rgba(255,255,255,.22);
  background:rgba(0,0,0,.25);
  box-shadow: inset 0 0 0 1px rgba(0,0,0,.25);
  cursor:pointer;
}
.ovBox.on{
  background:rgba(255,255,255,.22);
  border-color:rgba(255,255,255,.38);
}
tr.ovDone td{
  color:#aab6c6;
  background:rgba(255,255,255,.10);
}
tr.ovDone{box-shadow: inset 0 0 0 2px rgba(255,255,255,.10);}
tr.ovDone td{filter: grayscale(1);}
tr.ovDone td .ovBox{opacity:.85}
.ovLoadedPill{margin-left:10px;display:inline-flex;align-items:center;gap:6px;padding:4px 10px;border-radius:999px;font-weight:900;font-size:12px;letter-spacing:.02em;background:rgba(255,255,255,.14);border:1px solid rgba(255,255,255,.14);color:var(--fg)}
.ovMode{
  display:inline-flex;align-items:center;gap:0;
  background:rgba(255,255,255,.06);
  border:1px solid rgba(255,255,255,.10);
  border-radius:999px;
  overflow:hidden;
}
.ovMode button{
  padding:8px 12px;border:0;background:transparent;color:var(--muted);font-weight:800;cursor:pointer;
}
.ovMode button.on{background:rgba(255,255,255,.10);color:var(--fg)}
tr.ovDrag{cursor:grab}
tr.ovDrag.dragging{opacity:.55}
tr.ovDrag.dropTarget{outline:2px dashed rgba(255,255,255,.25); outline-offset:-6px}
th{color:var(--muted);font-size:12px;text-align:left}
td:last-child,th:last-child{text-align:right}

@media (max-width: 1100px){
  .organizerPage{padding:16px;height:100vh;}
}

/* FULL-WIDTH OVERRIDE */
.organizerPage,
.organizerRoot{
  width:100% !important;
  max-width:none !important;
  margin:0 !important;
}

/* Combined tab is shown by default */

</style>
</head>
<body>
<div class="organizerRoot organizerPage">
  <div class="organizerHeader">
    <div class="controls">
      <div class="header">
        <div class="topbar">
          <div class="brand">__HEADER_TITLE__</div>
          <div class="sel"><select id="routeSel"></select></div>
          <input id="q" placeholder="Search bags / overflow (ex: 16.3X)"/>
          <div class="badge"><span class="dot"></span><span id="bagsCount">0</span>&nbsp;bags</div>
          <div class="badge"><span class="dot"></span><span id="ovCount">0</span>&nbsp;overflow</div>
          <div class="downloadRow">
            <a class="downloadBtn" href="download/STACKED.pdf">DOWNLOAD PDF</a>
          </div>
        </div>

        <div class="subHeaderRow">
          <div class="subHeaderLeft">
            <div class="routeTitle" id="routeTitle"></div>
            <div class="toggleRow" id="toggleRow"></div>
          </div>
          <div class="subHeaderRight">
            <div class="tabsRow">
              <div class="tab active" data-tab="combined">Bags + Overflow</div>
              <div class="tab" data-tab="bags">Bags</div>
              <div class="tab" data-tab="overflow">Overflow</div>
            </div>
          </div>
        </div>
      </div>
    </div>
  </div>

  <div class="organizerBody">
    <div id="content" class="card content"></div>
  </div>
</div>

<script>
const ROUTES = __ROUTES_JSON__;
const WAVE_LABEL_BY_TIME = __WAVE_JSON__;
const organizerRoot = document.querySelector(".organizerRoot");
const organizerBody = document.querySelector(".organizerBody");

let layoutWidth = 0;
let measureRaf = 0;
let renderRaf = 0;
const TOTE_ASPECT = 4 / 3;
const TOTE_GAP_PX = 16;

function applyLayoutWidth(nextWidth){
  if(!nextWidth || !isFinite(nextWidth)) return;
  if(Math.abs(nextWidth - layoutWidth) < 0.5) return;
  layoutWidth = nextWidth;
  document.documentElement.style.setProperty("--layoutWidth", Math.round(layoutWidth) + "px");
}

function measureLayout(){
  if(!organizerRoot) return;
  const w = organizerRoot.getBoundingClientRect().width;
  applyLayoutWidth(w);
  if(!organizerBody) return;
  document.querySelectorAll(".toteWrap").forEach((wrap)=>{
    const board = wrap.querySelector(".toteBoard");
    if(!board) return;
    const colsRaw = getComputedStyle(board).getPropertyValue("--cols");
    const cols = Math.max(1, parseInt(colsRaw, 10) || 1);
    const availW = Math.max(0, wrap.clientWidth);
    const availH = Math.max(0, wrap.clientHeight);
    const gap = TOTE_GAP_PX;
    const tileWFromW = (availW - (cols - 1) * gap) / cols;
    const tileWFromH = ((availH - (TOTE_ROWS - 1) * gap) * TOTE_ASPECT) / TOTE_ROWS;
    const tileW = Math.floor(Math.min(tileWFromW, tileWFromH));
    const tileH = Math.floor(tileW / TOTE_ASPECT);
    if(!isFinite(tileW) || tileW <= 0 || !isFinite(tileH) || tileH <= 0) return;
    board.style.setProperty("--tileW", `${tileW}px`);
    board.style.setProperty("--tileH", `${tileH}px`);
    board.style.setProperty("--tileGap", `${gap}px`);
  });
}

function scheduleMeasure(){
  if(measureRaf) cancelAnimationFrame(measureRaf);
  measureRaf = requestAnimationFrame(()=>{
    measureLayout();
    requestAnimationFrame(measureLayout);
  });
}

function scheduleRender(){
  if(renderRaf) return;
  renderRaf = requestAnimationFrame(()=>{
    renderRaf = 0;
    render();
  });
}

// --- Persistent state ---
const STORAGE_KEY = "vanorg_loaded_v1";
const MODE_KEY = "vanorg_bagmode_v1";
const ORDER_KEY = "vanorg_bagorder_v1";
const COMBINE_KEY = "vanorg_combined_v1";

function readJSON(key, fallback){ try { return JSON.parse(localStorage.getItem(key) || JSON.stringify(fallback)); } catch(e){ return fallback; } }
function writeJSON(key, obj){ try { localStorage.setItem(key, JSON.stringify(obj)); } catch(e){} }

let LOADED = readJSON(STORAGE_KEY, {});
let BAGMODE = readJSON(MODE_KEY, {});
let BAGORDER = readJSON(ORDER_KEY, {});
let COMBINED = readJSON(COMBINE_KEY, {});

// Overflow checklist + ordering
const OVKEY = "vanorg_overflow_checks_v1";
const OVMODE_KEY = "vanorg_overflow_mode_v1";
const OVORDER_KEY = "vanorg_overflow_order_v1";

let OVCHK = readJSON(OVKEY, {});
let OVMODE = readJSON(OVMODE_KEY, {});
let OVORDER = readJSON(OVORDER_KEY, {});

function getOvMode(routeShort){ return OVMODE[routeShort] || "normal"; }
function setOvMode(routeShort, mode){ OVMODE[routeShort] = mode; writeJSON(OVMODE_KEY, OVMODE); }

function getOvOrder(routeShort){ return (OVORDER[routeShort] || []).slice(); }
function setOvOrder(routeShort, arr){ OVORDER[routeShort] = arr.slice(); writeJSON(OVORDER_KEY, OVORDER); }

function isOvChecked(routeShort, rowId, k){
  return !!(OVCHK[routeShort] && OVCHK[routeShort][rowId] && OVCHK[routeShort][rowId][String(k)]);
}
function toggleOvChecked(routeShort, rowId, k){
  if(!OVCHK[routeShort]) OVCHK[routeShort] = {};
  if(!OVCHK[routeShort][rowId]) OVCHK[routeShort][rowId] = {};
  const kk = String(k);
  if(OVCHK[routeShort][rowId][kk]) delete OVCHK[routeShort][rowId][kk];
  else OVCHK[routeShort][rowId][kk] = true;
  writeJSON(OVKEY, OVCHK);
}
function clearOvRoute(routeShort){
  OVCHK[routeShort] = {};
  OVMODE[routeShort] = "normal";
  OVORDER[routeShort] = [];
  writeJSON(OVKEY, OVCHK);
  writeJSON(OVMODE_KEY, OVMODE);
  writeJSON(OVORDER_KEY, OVORDER);
}

function isLoaded(routeShort, idx){ return !!(LOADED[routeShort] && LOADED[routeShort][String(idx)]); }
function toggleLoaded(routeShort, idx){
  if(!LOADED[routeShort]) LOADED[routeShort] = {};
  const k = String(idx);
  if(LOADED[routeShort][k]) delete LOADED[routeShort][k];
  else LOADED[routeShort][k] = true;
  writeJSON(STORAGE_KEY, LOADED);
}
function clearLoaded(routeShort){ LOADED[routeShort] = {}; writeJSON(STORAGE_KEY, LOADED); }

function getMode(routeShort){ return BAGMODE[routeShort] || "normal"; }
function setMode(routeShort, mode){ BAGMODE[routeShort] = mode; writeJSON(MODE_KEY, BAGMODE); }

function isCombinedSecond(routeShort, secondIdx){ return !!(COMBINED[routeShort] && COMBINED[routeShort][String(secondIdx)]); }
function setCombined(routeShort, secondIdx, val){
  if(!COMBINED[routeShort]) COMBINED[routeShort] = {};
  const k = String(secondIdx);
  if(val) COMBINED[routeShort][k] = true;
  else delete COMBINED[routeShort][k];
  writeJSON(COMBINE_KEY, COMBINED);
}
function clearCombined(routeShort){
  COMBINED[routeShort] = {};
  writeJSON(COMBINE_KEY, COMBINED);
}
function resetBagsPage(routeShort, baseOrderArr){
  // Unpress all totes + uncombine everything
  clearLoaded(routeShort);
  clearCombined(routeShort);
  // If in custom mode, restore order to base (so nothing stays missing)
  if(getMode(routeShort)==="custom"){
    setCustomOrder(routeShort, baseOrderArr.slice());
  }
}


let activeRouteIndex = 0;
let activeTab = "combined";


const routeSel = document.getElementById("routeSel");
const qBox = document.getElementById("q");
const bagsCount = document.getElementById("bagsCount");
const ovCount = document.getElementById("ovCount");
const content = document.getElementById("content");
const routeTitleEl = document.getElementById("routeTitle");
const toggleRow = document.getElementById("toggleRow");

function routeTitle(r){ return (r.route_short||"") + (r.cx ? ` (${r.cx})` : ""); }
function baseOrder(r){ return (r.bags_detail||[]).map(x=>x.idx); }
function subHeaderTitle(r){
  if(activeTab==="bags") return `${routeTitle(r)} — Bags`;
  if(activeTab==="overflow") return `${routeTitle(r)} — Overflow`;
  return `${routeTitle(r)} — Bags + Overflow`;
}
function updateSubHeader(r){
  if(routeTitleEl) routeTitleEl.textContent = subHeaderTitle(r);
  if(!toggleRow) return;
  if(activeTab === "overflow"){
    toggleRow.innerHTML = "";
    toggleRow.style.display = "none";
    return;
  }
  const mode = getMode(r.route_short);
  toggleRow.style.display = "flex";
  toggleRow.innerHTML = `
    <div class="modeToggle" role="tablist" aria-label="Bag order mode">
      <button class="modeBtn ${mode==="normal" ? "active":""}" data-bagmode="normal">Normal</button>
      <button class="modeBtn ${mode==="reversed" ? "active":""}" data-bagmode="reversed">Reversed</button>
      <button class="modeBtn ${mode==="custom" ? "active":""}" data-bagmode="custom">Custom</button>
    </div>
  `;
}

// Saturated chips (high-contrast)
function bagColorChip(label){
  const s = (label||"").toLowerCase();
  if (s.includes("yellow")) return "#FFD400";
  if (s.includes("orange")) return "#FF6A00";
  if (s.includes("green"))  return "#00D26A";
  if (s.includes("navy"))   return "#1E5BFF";
  if (s.includes("black"))  return "#0B0B0B";
  return "#34B3FF";
}

function normZone(z){
  if(!z) return "";
  let s = String(z).trim();
  s = s.replace(/^[A-Za-z]-/, "");
  const m = s.match(/(\d+\.\d+[A-Za-z])/);
  return m ? m[1] : s;
}
function parseZoneCounts(zones){
  if(!zones) return [];
  return String(zones)
    .split(';')
    .map(p=>p.trim())
    .filter(Boolean)
    .map((p)=>{
      const cnt = p.match(/(\d+\.\d+[A-Za-z])\s*\((\d+)\)/);
      if(cnt) return { zone: cnt[1], count: parseInt(cnt[2],10) || 0 };
      const zon = p.match(/(\d+\.\d+[A-Za-z])/);
      if(zon) return { zone: zon[1], count: 0 };
      return { zone: p, count: 0 };
    });
}
function match(text,q){ return !q || (text||"").toLowerCase().includes(q.toLowerCase()); }

// Custom order helpers
function getCustomOrder(routeShort, base){
  const arr = (BAGORDER[routeShort] || []).slice();
  const baseSet = new Set(base.map(String));
  const cleaned = arr.map(String).filter(x=>baseSet.has(x));
  const cleanedSet = new Set(cleaned);
  base.forEach(i=>{ const s=String(i); if(!cleanedSet.has(s)) cleaned.push(s); });
  BAGORDER[routeShort] = cleaned.map(x=>parseInt(x,10));
  writeJSON(ORDER_KEY, BAGORDER);
  return BAGORDER[routeShort];
}
function setCustomOrder(routeShort, orderArr){ BAGORDER[routeShort] = orderArr.map(x=>parseInt(x,10)); writeJSON(ORDER_KEY, BAGORDER); }

function removeCombinedSecondsFromOrder(routeShort, orderArr){
  const sec = COMBINED[routeShort] || {};
  const secSet = new Set(Object.keys(sec).map(x=>parseInt(x,10)));
  if(secSet.size===0) return orderArr;
  return orderArr.filter(i=>!secSet.has(i));
}

function buildOrder(r){
  const routeShort = r.route_short;
  const base = baseOrder(r);
  const mode = getMode(routeShort);
  let ord = base.slice();
  if(mode==="reversed") ord.reverse();
  if(mode==="custom") ord = getCustomOrder(routeShort, base).slice();
  if(mode==="custom") ord = removeCombinedSecondsFromOrder(routeShort, ord);
  return ord;
}

function buildDisplayItems(r, q){
  const routeShort = r.route_short;
  const byIdx = Object.fromEntries((r.bags_detail||[]).map(x=>[x.idx, x]));
  const ord = buildOrder(r);
  const items = [];
  for(const idx of ord){
    if(isCombinedSecond(routeShort, idx)) continue;
    const cur = byIdx[idx];
    if(!cur) continue;
    const secondIdx = idx + 1;
    const second = isCombinedSecond(routeShort, secondIdx) ? byIdx[secondIdx] : null;
    const eligibleCombine = (!cur.sort_zone) && idx > 1;
    const text = `${cur.idx} ${cur.bag} ${cur.sort_zone||""} ${cur.pkgs||""}` + (second ? ` ${second.bag}` : "");
    if(!match(text, q)) continue;
    items.push({ idx, cur, secondIdx: second ? secondIdx : null, second, eligibleCombine });
  }
  return items;
}

const TOTE_ROWS = 3;

function getToteColumnCount(itemCount){
  const count = Math.max(1, itemCount|0);
  return Math.max(1, Math.ceil(count / TOTE_ROWS));
}

function orderForToteSweep(items, columns){
  if(items.length <= 1) return items.slice();
  return items.slice();
}

function orderFromToteSweep(items, columns){
  if(items.length <= 1) return items.slice();
  return items.slice();
}

function buildToteLayout(items, routeShort, getSubLine, getBadgeText, getPkgCount){
  const cols = getToteColumnCount(items.length);
  const orderedItems = orderForToteSweep(items, cols);
  const cardsHtml = orderedItems.map((it, i)=>{
    const cur = it.cur;
    const second = it.second;
    const main1 = (cur.bag_id || cur.bag || "").toString();
    const chip1 = bagColorChip(cur.bag);
    const loadedClass = isLoaded(routeShort, it.idx) ? "loaded" : "";
    const badgeText = getBadgeText ? getBadgeText(cur, second, it.idx) : it.idx;
    const pkgText = getPkgCount ? getPkgCount(cur, second) : "";
    const badgeHtml = badgeText ? `<div class="toteIdx">${badgeText}</div>` : ``;
    const pkgHtml = pkgText ? `<div class="totePkg">${pkgText}</div>` : ``;
    const pkgClass = pkgText ? "hasPkg" : "";
    const row = (i % TOTE_ROWS) + 1;
    const colFromRight = Math.floor(i / TOTE_ROWS);
    const col = cols - colFromRight;
    const posStyle = `grid-row:${row};grid-column:${col};`;

    if(second){
      const main2 = (second.bag_id || second.bag || "").toString();
      const chip2 = bagColorChip(second.bag);
      const sub = getSubLine(cur, second);
      const topNum = (cur.sort_zone ? main1 : main2);
      const botNum = (cur.sort_zone ? main2 : main1);
      return `<div class="toteCard ${loadedClass} ${pkgClass}" data-idx="${it.idx}" style="--chipL:${chip1};--chipR:${chip2};${posStyle}">
        <div class="toteTopRow">
          ${badgeHtml}
          <div class="toteBar"></div>
          <div class="toteMetaRight">
            ${pkgHtml}
            <div class="toteStar on" data-action="uncombine" data-second="${it.secondIdx}" title="Uncombine">-</div>
          </div>
        </div>
        <div class="toteBigNumber toteBigNumberStack">
          <div class="toteBigNumberLine">${topNum}</div>
          <div class="toteBigNumberLine">${botNum}</div>
        </div>
        ${sub ? `<div class="toteBottomRow">${sub}</div>` : ``}
      </div>`;
    }

    const sub = getSubLine(cur, null);
    const starHtml = it.eligibleCombine ? `<div class="toteStar combine" data-action="combine" data-second="${it.idx}" title="Combine with previous">+</div>` : ``;

    return `<div class="toteCard ${loadedClass} ${pkgClass}" data-idx="${it.idx}" style="--chipL:${chip1};--chipR:${chip1};${posStyle}">
      <div class="toteTopRow">
        ${badgeHtml}
        <div class="toteBar"></div>
        <div class="toteMetaRight">
          ${pkgHtml}
          ${starHtml}
        </div>
      </div>
      <div class="toteBigNumber">${main1}</div>
      ${sub ? `<div class="toteBottomRow">${sub}</div>` : ``}
    </div>`;
  }).join("");

  return { cardsHtml, cols };
}

function buildOverflowMap(r){
  const map = new Map();
  (r.combined || []).forEach((x)=>{
    const bag = String(x.bag || "").trim();
    if(!bag) return;
    let entry = map.get(bag);
    if(!entry){
      entry = [];
      map.set(bag, entry);
    }
    parseZoneCounts(x.zones || "").forEach(z=>entry.push(z));
  });
  return map;
}

function overflowSummary(bagLabel, ovMap){
  if(!bagLabel || !ovMap) return "";
  const entry = ovMap.get(bagLabel);
  if(!entry || !entry.length) return "";
  return entry.map((item)=>{
    const label = normZone(item.zone);
    if(!label) return "";
    const count = item.count ? ` (${item.count})` : "";
    const cls = label.startsWith("99.") ? "ovZone ovZone99" : "ovZone";
    return `<div class="ovLine"><span class="${cls}">${label}${count}</span></div>`;
  }).filter(Boolean).join("");
}


function fitToteText(){
  const cards = document.querySelectorAll('#bagsBoard .toteCard');
  for(const card of cards){
    const main = card.querySelector('.toteBigNumber:not(.toteBigNumberStack)');
    const stack = card.querySelector('.toteBigNumberStack');
    const innerW = card.clientWidth - 32;
    if(main){
      const el = main;
      const max = parseFloat(getComputedStyle(el).fontSize);
      let lo = 10, hi = max, best = 10;
      const fits = (fs)=>{
        el.style.fontSize = fs+'px';
        return el.scrollWidth <= innerW + 1;
      };
      if(fits(max)){ el.style.fontSize=''; }
      else{
        while(lo<=hi){
          const mid=(lo+hi)/2;
          if(fits(mid)){ best=mid; lo=mid+0.5; } else hi=mid-0.5;
        }
        el.style.fontSize = best+'px';
      }
    }
    if(stack){
      const lines = stack.querySelectorAll('.toteBigNumberLine');
      for(const el of lines){
        const max = parseFloat(getComputedStyle(el).fontSize);
        let lo = 10, hi = max, best = 10;
        const fits = (fs)=>{ el.style.fontSize = fs+'px'; return el.scrollWidth <= innerW + 1; };
        if(fits(max)){ el.style.fontSize=''; }
        else{
          while(lo<=hi){
            const mid=(lo+hi)/2;
            if(fits(mid)){ best=mid; lo=mid+0.5; } else hi=mid-0.5;
          }
          el.style.fontSize = best+'px';
        }
      }
    }
  }
}

function attachBagHandlers(routeShort, allowDrag){
  // click to mark loaded (ignore star clicks)
  document.querySelectorAll('.toteCard[data-idx]').forEach(el=>{
    el.addEventListener('click', (e)=>{
      if(e.target && e.target.classList && e.target.classList.contains('toteStar')) return;
      if(el.classList.contains('dragging')) return;
      const idx = parseInt(el.getAttribute('data-idx')||"0",10);
      if(!idx) return;
      toggleLoaded(routeShort, idx);
      el.classList.toggle('loaded', isLoaded(routeShort, idx));
    });
  });

  // combine/uncombine
  document.querySelectorAll('.toteStar[data-action]').forEach(btn=>{
    btn.addEventListener('click', (e)=>{
      e.preventDefault(); e.stopPropagation();
      const act = btn.getAttribute('data-action');
      const second = parseInt(btn.getAttribute('data-second')||"0",10);
      if(!second) return;
      const r = ROUTES[activeRouteIndex];
      const base = baseOrder(r);

      if(act==="combine"){
        setCombined(routeShort, second, true);
        if(getMode(routeShort)==="custom"){
          const ord = getCustomOrder(routeShort, base).slice().filter(i=>i!==second);
          setCustomOrder(routeShort, ord);
        }
      } else if(act==="uncombine"){
        setCombined(routeShort, second, false);
        if(getMode(routeShort)==="custom"){
          const ord = getCustomOrder(routeShort, base).slice();
          const first = second-1;
          if(!ord.includes(second)){
            const pos = ord.indexOf(first);
            if(pos>=0) ord.splice(pos+1,0,second);
            else ord.push(second);
            setCustomOrder(routeShort, ord);
          }
        }
      }
      render();
    });
  });

  // clear loaded
  const btn = document.getElementById('clearLoadedBtn');
  if(btn) btn.addEventListener('click', ()=>{ clearLoaded(routeShort); render(); });
  const rbtn = document.getElementById('resetBagsBtn');
  if(rbtn){
    rbtn.addEventListener('click', ()=>{
      const r = ROUTES[activeRouteIndex];
      const base = baseOrder(r);
      resetBagsPage(routeShort, base);
      render();
    });
  }

  // mode buttons
  document.querySelectorAll('[data-bagmode]').forEach(b=>{
    b.addEventListener('click', ()=>{ setMode(routeShort, b.getAttribute('data-bagmode')); render(); });
  });

  // drag/drop for custom: reorder displayed cards
  if(!allowDrag) return;
  let dragIdx = null;

  document.querySelectorAll('.toteCard[data-idx]').forEach(el=>{
    el.setAttribute('draggable', 'true');
    el.classList.add('draggable');

    el.addEventListener('dragstart', (e)=>{
      dragIdx = el.getAttribute('data-idx');
      el.classList.add('dragging');
      try { e.dataTransfer.setData('text/plain', dragIdx); } catch(_) {}
      e.dataTransfer.effectAllowed = 'move';
    });

    el.addEventListener('dragend', ()=>{
      dragIdx = null;
      document.querySelectorAll('.toteCard').forEach(x=>x.classList.remove('dragging','dropTarget'));
    });

    el.addEventListener('dragover', (e)=>{
      e.preventDefault();
      el.classList.add('dropTarget');
      e.dataTransfer.dropEffect = 'move';
    });

    el.addEventListener('dragleave', ()=>{ el.classList.remove('dropTarget'); });

    el.addEventListener('drop', (e)=>{
      e.preventDefault();
      el.classList.remove('dropTarget');
      const targetIdx = el.getAttribute('data-idx');
      const src = dragIdx || (function(){ try { return e.dataTransfer.getData('text/plain'); } catch(_){ return null; } })();
      if(!src || !targetIdx || src === targetIdx) return;

      const s = parseInt(src,10);
      const t = parseInt(targetIdx,10);

      const r = ROUTES[activeRouteIndex];
      const base = baseOrder(r);
      let ord = removeCombinedSecondsFromOrder(routeShort, getCustomOrder(routeShort, base).slice());

      const cols = getToteColumnCount(ord.length);
      let domOrder = orderForToteSweep(ord, cols);
      const from = domOrder.indexOf(s);
      const to = domOrder.indexOf(t);
      if(from === -1 || to === -1) return;
      domOrder.splice(from, 1);
      domOrder.splice(to, 0, s);
      const sweepOrder = orderFromToteSweep(domOrder, cols);
      setCustomOrder(routeShort, sweepOrder);
      setMode(routeShort, "custom");
      render();
    });
  });
}



function buildOverflowSyncOrder(r){
  const base = (r.overflow_seq || []).map((x,i)=>({
    zone: x.zone,
    count: x.count||0,
    bag_idx: x.bag_idx || 0,
    _i: i,
    _id: `${x.bag_idx||0}|${normZone(x.zone)}|${i}`,
  }));
  const buckets = new Map();
  base.forEach(item=>{
    const key = item.bag_idx || 0;
    if(!buckets.has(key)) buckets.set(key, []);
    buckets.get(key).push(item);
  });
  const ids = [];
  const seen = new Set();
  const bagOrder = buildOrder(r);
  bagOrder.forEach(idx=>{
    const items = buckets.get(idx) || [];
    items.forEach(it=>{
      if(seen.has(it._id)) return;
      seen.add(it._id);
      ids.push(it._id);
    });
    buckets.delete(idx);
  });
  base.forEach(it=>{
    if(seen.has(it._id)) return;
    seen.add(it._id);
    ids.push(it._id);
  });
  return ids;
}

function attachOverflowHandlers(routeShort, allowDrag, r){
  // mode toggle
  document.querySelectorAll('[data-ovmode]').forEach(btn=>{
    btn.addEventListener('click', ()=>{
      const m = btn.getAttribute('data-ovmode')||"normal";
      setOvMode(routeShort, m);
      if(m !== "custom") setOvOrder(routeShort, []); // keep custom order only in custom
      render();
    });
  });

  const ovSync = document.getElementById('ovSync');
  if(ovSync){
    ovSync.addEventListener('click', ()=>{
      const ids = buildOverflowSyncOrder(r);
      setOvOrder(routeShort, ids);
      setOvMode(routeShort, "custom");
      render();
    });
  }

  // checkbox toggles (click + keyboard)
  document.querySelectorAll('.ovBox[data-rowid][data-k]').forEach(box=>{
    const fire = ()=>{
      const rowId = box.getAttribute('data-rowid');
      const k = parseInt(box.getAttribute('data-k')||"0",10);
      if(!rowId || !k) return;
      toggleOvChecked(routeShort, rowId, k);
      render();
    };
    box.addEventListener('click', (e)=>{ e.preventDefault(); e.stopPropagation(); fire(); });
    box.addEventListener('keydown', (e)=>{
      if(e.key === "Enter" || e.key === " "){ e.preventDefault(); fire(); }
    });
  });

  // clear overflow checks for this route only
  const ovClear = document.getElementById('ovClear');
  if(ovClear){
    ovClear.addEventListener('click', ()=>{
      OVCHK[routeShort] = {};
      writeJSON(OVKEY, OVCHK);
      render();
    });
  }

  if(!allowDrag) return;

  // drag reorder rows (custom mode)
  let dragId = null;
  document.querySelectorAll('tr.ovDrag[data-rowid]').forEach(tr=>{
    tr.addEventListener('dragstart', (e)=>{
      dragId = tr.getAttribute('data-rowid');
      tr.classList.add('dragging');
      try{ e.dataTransfer.setData('text/plain', dragId); }catch(_){}
      e.dataTransfer.effectAllowed = 'move';
    });
    tr.addEventListener('dragend', ()=>{
      dragId = null;
      document.querySelectorAll('tr.ovDrag').forEach(x=>x.classList.remove('dragging','dropTarget'));
    });
    tr.addEventListener('dragover', (e)=>{
      e.preventDefault();
      tr.classList.add('dropTarget');
      e.dataTransfer.dropEffect = 'move';
    });
    tr.addEventListener('dragleave', ()=>tr.classList.remove('dropTarget'));
    tr.addEventListener('drop', (e)=>{
      e.preventDefault();
      tr.classList.remove('dropTarget');
      const targetId = tr.getAttribute('data-rowid');
      const srcId = dragId || (function(){ try{ return e.dataTransfer.getData('text/plain'); }catch(_){ return null; } })();
      if(!srcId || !targetId || srcId === targetId) return;

      // Build current ordered ids from DOM
      const ids = Array.from(document.querySelectorAll('tr.ovDrag[data-rowid]')).map(x=>x.getAttribute('data-rowid'));
      const from = ids.indexOf(srcId);
      const to = ids.indexOf(targetId);
      if(from === -1 || to === -1) return;
      ids.splice(from,1);
      ids.splice(to,0,srcId);
      setOvOrder(routeShort, ids);
      setOvMode(routeShort, "custom");
      render();
    });
  });
}

function renderBags(r, q){
  const routeShort = r.route_short;
  const mode = getMode(routeShort);

  const items = buildDisplayItems(r, q);

  function subLine(anchor, other){
    const src = anchor.sort_zone ? anchor : (other && other.sort_zone ? other : anchor);
    const sz = normZone(src.sort_zone);
    const pk = (src.pkgs===undefined || src.pkgs===null) ? "" : String(src.pkgs);
    if(!sz && !pk) return "";
    if(sz && pk) return `${sz} (${pk})`;
    if(sz) return sz;
    return `(${pk})`;
  }

  function bagBadgeText(anchor, other, idx){
    const src = anchor.sort_zone ? anchor : (other && other.sort_zone ? other : anchor);
    return src.sort_zone ? idx : "";
  }

  const layout = buildToteLayout(items, routeShort, subLine, bagBadgeText);

  content.innerHTML = `
    <div class="controlsRow">
      <div class="hint">Tap to mark loaded. ${
        mode==="custom" ? "Drag to reorder (badge numbers stay the same)." :
        mode==="reversed" ? "Showing last bag → first bag." :
        "Showing first bag → last bag."
      }</div>
      <div class="badge"><span class="dot"></span>${items.length} bags</div>
    </div>
    <div class="toteWrap">
      <div class="toteBoard bagsGrid" style="--cols:${layout.cols}">${layout.cardsHtml}</div>
    </div>
    <div class="clearRow">
      <button id="clearLoadedBtn" class="clearBtn">Clear</button>
      <button id="resetBagsBtn" class="clearBtn">Reset</button>
    </div>
  `;

  const allowDrag = (mode === "custom") && !q;
  attachBagHandlers(routeShort, allowDrag);
}

function renderOverflow(r,q){
const routeShort = r.short || r.route_short || "";
  const mode = getOvMode(routeShort);

  // Build ordered list (same order as Excel by default)
  const base = (r.overflow_seq || []).map((x,i)=>({
    zone: x.zone,
    count: x.count||0,
    bag_idx: x.bag_idx || 0,
    _i: i,
    _id: `${x.bag_idx||0}|${normZone(x.zone)}|${i}`
  }));

  // Apply ordering mode
  let ordered = base.slice();
  if(mode === "reversed"){
    ordered.reverse();
  } else if(mode === "custom"){
    const saved = getOvOrder(routeShort);
    if(saved && saved.length){
      const map = new Map(ordered.map(o=>[o._id,o]));
      const out = [];
      saved.forEach(id=>{ const it = map.get(id); if(it){ out.push(it); map.delete(id); } });
      // append any new rows not in saved
      map.forEach(v=>out.push(v));
      ordered = out;
    }
  }

  // Search filter keeps current order
  if(q) ordered = ordered.filter(x=>match(`${x.bag_idx} ${x.zone} ${x.count}`, q));

  const allowDrag = (mode==="custom") && !q;

  const modeHtml = `
    <div class="ovMode" role="tablist" aria-label="Overflow order mode">
      <button class="${mode==="normal"?"on":""}" data-ovmode="normal">Normal</button>
      <button class="${mode==="reversed"?"on":""}" data-ovmode="reversed">Reversed</button>
      <button class="${mode==="custom"?"on":""}" data-ovmode="custom">Custom</button>
    </div>
  `;

  content.innerHTML = `
    <div class="ovWrap">
    <div class="ovHeader">
      <div>
        <div class="ovTitleRow">
          <div style="font-weight:900">Overflow</div>
          <button class="syncBtn" id="ovSync" type="button">Sync</button>
        </div>
      </div>
      <div class="ovHeaderRight">
        ${modeHtml}
        <div class="badge"><span class="dot"></span>${r.overflow_total||0} overflow</div>
      </div>
    </div>

    <table class="ovTable">
      <colgroup>
        <col style="width:80px">
        <col style="width:140px">
        <col>
        <col style="width:90px">
      </colgroup>
      <thead>
        <tr>
          <th style="width:70px">Bag #</th>
          <th>Overflow Zone</th>
          <th style="width:38%">Load</th>
          <th style="text-align:right;width:80px">Pkgs</th>
        </tr>
      </thead>
      <tbody>
        ${ordered.length ? ordered.map((x,idx)=>{
          const rowId = x._id;
          const total = Math.max(0, parseInt(x.count||0,10)||0);
          let done = true;
          for(let k=1;k<=total;k++){ if(!isOvChecked(routeShort,rowId,k)){ done=false; break; } }
          const trCls = `${allowDrag?'ovDrag':''} ${done && total>0 ? 'ovDone':''}`.trim();
          const checks = total ? Array.from({length: total}, (_,i)=>{
            const k=i+1;
            const on = isOvChecked(routeShort,rowId,k);
            return `<div class="ovBox ${on?'on':''}" role="checkbox" aria-checked="${on?'true':'false'}" tabindex="0" data-rowid="${rowId}" data-k="${k}"></div>`;
          }).join('') : '';
          return `
            <tr class="${trCls}" draggable="${allowDrag?'true':'false'}" data-rowid="${rowId}">
              <td style="font-weight:900">${x.bag_idx||""}</td>
              <td><span class="${normZone(x.zone).startsWith("99.") ? "ovZone ovZone99" : "ovZone"}">${normZone(x.zone)}</span></td>
              <td>${total ? `<div class="ovChecks">${checks}</div>${(done&&total>0)?`<span class="ovLoadedPill">LOADED</span>`:""}` : `<span style="color:var(--muted)">—</span>`}</td>
              <td style="text-align:right;font-weight:900">${total||""}</td>
            </tr>
          `;
        }).join("") : `<tr><td colspan="4" style="color:var(--muted)">No overflow</td></tr>`}
      </tbody>
    </table>

    <div class="rowActions">
      <button class="btn" id="ovClear">Clear</button>
    </div>
  `;

  attachOverflowHandlers(routeShort, allowDrag, r);
}

function renderCombined(r,q){
  const routeShort = r.route_short;
  const mode = getMode(routeShort);
  const items = buildDisplayItems(r, q);
  const ovMap = buildOverflowMap(r);

  function combinedBadgeText(anchor, other){
    const src = anchor.sort_zone ? anchor : (other && other.sort_zone ? other : anchor);
    return normZone(src.sort_zone);
  }

  function combinedPkgCount(anchor, other){
    const src = anchor.sort_zone ? anchor : (other && other.sort_zone ? other : anchor);
    const pk = (src.pkgs===undefined || src.pkgs===null) ? "" : String(src.pkgs);
    return pk;
  }

  function combinedSubLine(anchor, other){
    const first = overflowSummary(anchor.bag, ovMap);
    const second = other ? overflowSummary(other.bag, ovMap) : "";
    const parts = [first, second].filter(Boolean);
    if(!parts.length) return "";
    return parts.join("");
  }

  const layout = buildToteLayout(items, routeShort, combinedSubLine, combinedBadgeText, combinedPkgCount);
  content.innerHTML = `
    <div class="controlsRow">
      <div class="hint">Overflow zones + pkgs under each tote.</div>
      <div class="badge"><span class="dot"></span>${items.length} bags</div>
    </div>
    <div class="toteWrap">
      <div class="toteBoard bagsGrid" style="--cols:${layout.cols}">${layout.cardsHtml}</div>
    </div>
    <div class="clearRow">
      <button id="clearLoadedBtn" class="clearBtn">Clear</button>
      <button id="resetBagsBtn" class="clearBtn">Reset</button>
    </div>
  `;

  const allowDrag = (mode === "custom") && !q;
  attachBagHandlers(routeShort, allowDrag);
}

function render(){
  const r = ROUTES[activeRouteIndex];
  if(!r){ content.innerHTML = "<div style='color:var(--muted)'>No routes found.</div>"; return; }
  bagsCount.textContent = r.bags_count ?? 0;
  ovCount.textContent = r.overflow_total ?? 0;
  updateSubHeader(r);
  const q = qBox.value.trim();
  content.classList.toggle('plain', activeTab==='bags' || activeTab==='combined');
  if(activeTab==="bags") renderBags(r,q);
  if(activeTab==="overflow") renderOverflow(r,q);
  if(activeTab==="combined") renderCombined(r,q);
  scheduleMeasure();
}

function buildRouteDropdown(){
  routeSel.innerHTML = "";

  const groups = new Map(); // label -> [{r,i}]
  ROUTES.forEach((r,i)=>{
    const t = (r.wave_time||"").trim();
    const wave = t ? (WAVE_LABEL_BY_TIME[t] || "Wave") : "Other";
    const label = t ? `${wave} (${t})` : "Other";
    if(!groups.has(label)) groups.set(label, []);
    groups.get(label).push({r,i});
  });

  function groupKey(label){
    const m = label.match(/^(\d+)/);
    return m ? parseInt(m[1],10) : 999;
  }
  const groupLabels = Array.from(groups.keys()).sort((a,b)=>groupKey(a)-groupKey(b) || a.localeCompare(b));

  function routeKey(rs){
    const m = rs.match(/^([A-Z]+)\.(\d+)$/);
    if(!m) return [rs, 0];
    return [m[1], parseInt(m[2],10)];
  }

  groupLabels.forEach(gl=>{
    const og = document.createElement("optgroup");
    og.label = gl;
    const arr = groups.get(gl);
    arr.sort((A,B)=>{
      const [a1,a2]=routeKey(A.r.route_short||"");
      const [b1,b2]=routeKey(B.r.route_short||"");
      if(a1!==b1) return a1.localeCompare(b1);
      return (a2-b2);
    });
    arr.forEach(({r,i})=>{
      const opt = document.createElement("option");
      opt.value = String(i);
      opt.textContent = routeTitle(r);
      og.appendChild(opt);
    });
    routeSel.appendChild(og);
  });
}

function init(){
  buildRouteDropdown();
  routeSel.addEventListener("change", ()=>{ activeRouteIndex=parseInt(routeSel.value,10)||0; render(); });
  qBox.addEventListener("input", ()=>render());
  document.querySelectorAll(".tab").forEach(t=>{
    t.addEventListener("click", ()=>{
      document.querySelectorAll(".tab").forEach(x=>x.classList.remove("active"));
      t.classList.add("active");
      activeTab = t.dataset.tab;
      render();
    });
  });
  if(organizerRoot && "ResizeObserver" in window){
    const ro = new ResizeObserver(()=>{
      measureLayout();
      scheduleRender();
    });
    ro.observe(organizerRoot);
  }
  if(document.fonts && document.fonts.ready){
    document.fonts.ready.then(()=>{
      scheduleMeasure();
      scheduleRender();
    });
  }
  window.addEventListener('orientationchange', ()=>{
    scheduleMeasure();
    scheduleRender();
  });
  window.addEventListener('resize', ()=>{
    scheduleMeasure();
    scheduleRender();
  });
  scheduleMeasure();
  render();
}
init();
</script>
</body>
</html>
"""


def build_html(header_title: str, routes: List[dict], wave_map: dict) -> str:
    # Keep JSON dumps settings identical to previous: no indent, ensure_ascii False.
    routes_json = json.dumps(routes, ensure_ascii=False)
    wave_json = json.dumps(wave_map, ensure_ascii=False)
    return (HTML_TEMPLATE
            .replace("__HEADER_TITLE__", header_title)
            .replace("__ROUTES_JSON__", routes_json)
            .replace("__WAVE_JSON__", wave_json))


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--pdf", required=True, help="Route Sheets PDF")
    ap.add_argument("--xlsx", required=True, help="Bags_with_Overflow Excel")
    ap.add_argument("--out", required=True, help="Output HTML path")
    ap.add_argument("--no-cache", action="store_true", help="Disable PDF parse cache")
    args = ap.parse_args()

    header_title, _, pdf_meta, route_time = parse_pdf_meta(args.pdf, use_cache=not args.no_cache)
    if args.no_cache:
        routes = parse_excel_routes(args.xlsx, pdf_meta, route_time)
        wave_map = build_wave_labels(routes)
    else:
        cached = _load_routes_cache(args.pdf, args.xlsx)
        if cached:
            routes = cached["routes"]
            wave_map = cached["wave_map"]
        else:
            routes = parse_excel_routes(args.xlsx, pdf_meta, route_time)
            wave_map = build_wave_labels(routes)
            _save_routes_cache(args.pdf, args.xlsx, {"routes": routes, "wave_map": wave_map})
    html = build_html(header_title, routes, wave_map)
    Path(args.out).write_text(html, encoding="utf-8")
    print(args.out)


if __name__ == "__main__":
    main()
