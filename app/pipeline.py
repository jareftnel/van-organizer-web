from __future__ import annotations

import colorsys
import json
import os
import re
import threading
import time
import uuid
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, Any, Optional, Callable

import numpy as np
import pdfplumber
import pandas as pd
from PIL import Image

# we vendor these scripts into /tools
import sys
tools_dir = Path(__file__).resolve().parents[1] / "tools"
tools_dir_str = str(tools_dir)
if tools_dir_str not in sys.path:
    sys.path.insert(0, tools_dir_str)

from route_stacker import (
    parse_route_page,
    assign_overflows,
    df_from,
    build_stacked_pdf_with_summary_grouped,
)

DATE_RE = re.compile(r"\b(?:MON|TUE|WED|THU|FRI|SAT|SUN),\s+[A-Z]{3}\s+\d{1,2},\s+\d{4}\b")

STAGE_WEIGHTS = {
    "parse_pdf": 0.3333333333,
    "excel": 0.2,
    "build_html": 0.4666666667,
}
STAGE_TEXT = {
    "parse_pdf": "Processing File…",
    "excel": "Generating Data…",
    "build_html": "Building Organizer + OptiSheets…",
}
DEFAULT_STAGE_SECONDS = {
    "parse_pdf": 25.0,
    "excel": 15.0,
    "build_html": 35.0,
}
PROGRESS_SLACK = 1.5
STAGE_PROGRESS_CAP = 0.98
EMA_ALPHA = 0.25


def auto_detect_date_label(pdf_path: str) -> str:
    try:
        with pdfplumber.open(pdf_path) as pdf:
            for p in pdf.pages[:4]:
                t = (p.extract_text() or "").upper()
                m = DATE_RE.search(t)
                if m:
                    return m.group(0)
    except Exception:
        pass
    return "DATE UNKNOWN"


def _clamp_pct(x: int) -> int:
    try:
        x = int(x)
    except Exception:
        x = 0
    return max(0, min(100, x))


def _monotonic_seconds() -> float:
    return time.monotonic()


def _normalize_time_label(label: str, require_ampm: bool = False) -> str:
    match = re.search(
        r"(\d{1,2})\s*[:.]\s*(\d{2})\s*([AaPp])?\s*([Mm])?",
        label or "",
    )
    if not match:
        return ""
    hh = int(match.group(1))
    mm = int(match.group(2))
    if require_ampm and not (match.group(3) and match.group(4)):
        return ""
    ampm = ""
    if match.group(3) and match.group(4):
        ampm = f"{match.group(3)}{match.group(4)}".upper()

    if ampm == "PM" and hh != 12:
        hh += 12
    if ampm == "AM" and hh == 12:
        hh = 0

    return f"{hh:02d}:{mm:02d}"


def _time_sort_key(label: str) -> int:
    key = _normalize_time_label(label)
    if not key:
        return 0
    hh, mm = key.split(":")
    return int(hh) * 60 + int(mm)


def _rgb_to_hex(rgb: np.ndarray) -> str:
    r, g, b = [int(round(v)) for v in rgb]
    return f"#{r:02x}{g:02x}{b:02x}"


@dataclass(frozen=True)
class WaveBand:
    y_start: int
    y_end: int
    rgb: np.ndarray
    color_name: str


def _margin_samples(arr: np.ndarray) -> np.ndarray:
    width = arr.shape[1]
    margin = max(1, int(width * 0.12))
    left = arr[:, :margin, :]
    right = arr[:, max(width - margin, 0) :, :]
    if left.size == 0 and right.size == 0:
        return arr
    if left.size == 0:
        return right
    if right.size == 0:
        return left
    return np.concatenate([left, right], axis=1)


def _median_color(block: np.ndarray) -> np.ndarray:
    return np.median(block.reshape(-1, 3), axis=0)


def _classify_color(rgb: np.ndarray) -> str:
    r, g, b = [float(v) / 255.0 for v in rgb]
    h, s, v = colorsys.rgb_to_hsv(r, g, b)

    if v >= 0.9 and s <= 0.12:
        return "white"

    hue = h * 360.0
    if 250 <= hue < 310:
        return "purple"
    if 210 <= hue < 250:
        return "blue"
    if 120 <= hue < 180:
        return "green"
    if 40 <= hue < 70:
        return "yellow"
    if hue < 20 or hue >= 330:
        return "red"
    return "unknown"


def _detect_color_bands(image: Image.Image) -> list[tuple[int, int, np.ndarray]]:
    original = image.convert("RGB")
    width, height = original.size
    if width < 2 or height < 2:
        return []

    target_width = min(300, width)
    work_img = original
    if width != target_width:
        work_img = original.resize((target_width, height))
    work_arr = np.array(work_img)
    row_samples = _margin_samples(work_arr)
    row_colors = np.median(row_samples, axis=1)

    threshold = 20.0
    segments: list[tuple[int, int]] = []
    start = 0
    current = row_colors[0]
    for i in range(1, len(row_colors)):
        if np.linalg.norm(row_colors[i] - current) > threshold:
            segments.append((start, i - 1))
            start = i
            current = row_colors[i]
    segments.append((start, len(row_colors) - 1))

    merged: list[tuple[int, int]] = []
    prev_color = None
    for seg_start, seg_end in segments:
        seg_color = np.median(row_colors[seg_start : seg_end + 1], axis=0)
        if not merged:
            merged.append((seg_start, seg_end))
            prev_color = seg_color
            continue
        if prev_color is not None and np.linalg.norm(seg_color - prev_color) < threshold:
            merged[-1] = (merged[-1][0], seg_end)
            prev_color = np.median(row_colors[merged[-1][0] : seg_end + 1], axis=0)
        else:
            merged.append((seg_start, seg_end))
            prev_color = seg_color

    min_height = max(6, int(len(row_colors) * 0.01))
    bands: list[tuple[int, int, np.ndarray]] = []
    orig_arr = np.array(original)
    margin_samples = _margin_samples(orig_arr)
    for seg_start, seg_end in merged:
        if (seg_end - seg_start + 1) < min_height:
            continue
        band_height = seg_end - seg_start + 1
        band_y0 = seg_start + int(band_height * 0.3)
        band_y1 = seg_start + max(int(band_height * 0.7), int(band_height * 0.3) + 1)
        band_y1 = min(band_y1, seg_end + 1)
        band_patch = margin_samples[band_y0:band_y1, :, :]
        if band_patch.size == 0:
            continue
        bands.append((seg_start, seg_end, _median_color(band_patch)))
    return bands


def extract_wave_color_map(image_paths: list[Path], toc_entries: list[dict]) -> dict[str, str]:
    if not image_paths or not toc_entries:
        return {}

    time_items: list[tuple[str, int]] = []
    seen: set[str] = set()
    for entry in toc_entries:
        raw = entry.get("time_label", "") or ""
        key = _normalize_time_label(raw)
        if not key or key in seen:
            continue
        seen.add(key)
        time_items.append((key, _time_sort_key(raw)))

    time_items.sort(key=lambda item: item[1])
    time_labels = [key for key, _ in time_items]

    if not time_labels:
        return {}

    detected_bands: list[WaveBand] = []

    for image_path in sorted(image_paths, key=lambda p: p.name):
        try:
            with Image.open(image_path) as img:
                img = img.convert("RGB")
                bands = sorted(_detect_color_bands(img), key=lambda band: band[0])
                for y_start, y_end, rgb in bands:
                    color_name = _classify_color(rgb)
                    detected_bands.append(
                        WaveBand(
                            y_start=y_start,
                            y_end=y_end,
                            rgb=rgb,
                            color_name=color_name,
                        )
                    )
        except Exception as exc:
            print(f"[wave-colors] Failed to process {image_path.name}: {exc}")

    if not detected_bands:
        return {}

    if len(detected_bands) != len(time_labels):
        print(
            "[wave-colors] Band/time count mismatch; mapping by index overlap.",
            f"bands={len(detected_bands)}",
            f"toc_times={len(time_labels)}",
        )

    count = min(len(time_labels), len(detected_bands))
    mapping = {time_labels[i]: detected_bands[i] for i in range(count)}

    print("[wave-colors] Final mapping:")
    print("timeKey | colorName | rgb | bandY")
    for time_key in time_labels[:count]:
        band = mapping[time_key]
        rgb = [int(round(v)) for v in band.rgb]
        print(f"{time_key} | {band.color_name} | {rgb} | {band.y_start}-{band.y_end}")

    return {time_key: _rgb_to_hex(mapping[time_key].rgb) for time_key in time_labels[:count]}


def generate_bags_xlsx_from_routesheets(
    pdf_path: str,
    out_xlsx: str,
    progress_cb: Optional[Callable[..., None]] = None,
) -> Dict[str, Any]:
    """
    Creates multi-sheet xlsx where each sheet name matches builder expectation:
      <RS>_<CX>  e.g. H.7_CX92
    Each sheet rows: Bag | Overflow Zone(s) | Overflow Pkgs (total)
    """
    out = {"routes": 0, "errors": []}
    pdf_path = str(pdf_path)
    out_xlsx = str(out_xlsx)

    wb_routes = []

    def report(stage: str, msg: str, extra: Optional[dict] = None):
        if not progress_cb:
            return
        payload = {
            "stage": stage,
            "msg": msg,
        }
        if extra:
            payload.update(extra)
        progress_cb(**payload)

    report("parse_pdf", "Parsing PDF…")

    with pdfplumber.open(pdf_path) as pdf:
        total_pages = len(pdf.pages) or 1

        for i, page in enumerate(pdf.pages, start=1):
            # progress update
            report(
                "parse_pdf",
                f"Parsing PDF… ({i}/{total_pages})",
                {"page": i, "pages": total_pages},
            )

            text = page.extract_text() or ""
            parsed = parse_route_page(text)
            if not parsed:
                continue

            rs, cx, *_rest = parsed
            if not rs or not cx:
                continue

            bags = parsed[4]
            overs = parsed[5]

            texts, totals = assign_overflows(bags, overs)
            df = df_from(bags, texts, totals)

            sheet_name = f"{rs}_{cx}"  # matches builder SHEET_RE
            wb_routes.append((sheet_name, df))
            out["routes"] += 1

    if out["routes"] == 0:
        raise RuntimeError("No routes were parsed from the uploaded PDF.")

    report("excel", f"Generating Excel… ({out['routes']} routes)")

    Path(out_xlsx).parent.mkdir(parents=True, exist_ok=True)
    with pd.ExcelWriter(out_xlsx, engine="openpyxl") as writer:
        for sheet_name, df in wb_routes:
            safe = sheet_name[:31]  # Excel sheet name max length
            df.to_excel(writer, sheet_name=safe, index=False, header=False)

        idx = pd.DataFrame([[name] for name, _df in wb_routes], columns=["Sheets"])
        idx.to_excel(writer, sheet_name="INDEX", index=False)

    report("excel", "Excel ready.")
    return out


def run_builder_html(
    pdf_path: str,
    xlsx_path: str,
    out_html: str,
    progress_cb: Optional[Callable[..., None]] = None,
) -> None:
    """
    Calls your v21 builder script.
    """
    import subprocess

    def report(msg: str, extra: Optional[dict] = None):
        if not progress_cb:
            return
        payload = {
            "stage": "build_html",
            "msg": msg,
        }
        if extra:
            payload.update(extra)
        progress_cb(**payload)

    report("Building organizer…")

    builder = Path(__file__).resolve().parents[1] / "tools" / "build_van_organizer_v21_hide_combined_ORIGPDF.py"
    cmd = [
        "python",
        str(builder),
        "--pdf", str(pdf_path),
        "--xlsx", str(xlsx_path),
        "--out", str(out_html),
        "--no-cache",
    ]

    # quick mid-progress marker (this step is usually short)
    report("Building organizer…")
    subprocess.check_call(cmd)
    report("Organizer ready.")


def run_stacker(
    pdf_path: str,
    out_pdf: str,
    date_label: str,
    progress_cb: Optional[Callable[..., None]] = None,
) -> Dict[str, Any]:
    return build_stacked_pdf_with_summary_grouped(
        str(pdf_path),
        str(out_pdf),
        date_label,
        progress_cb=progress_cb
    )


class ProgressEmaStore:
    def __init__(self, path: Path, alpha: float = EMA_ALPHA):
        self.path = path
        self.alpha = alpha
        self._lock = threading.Lock()
        self._data: Dict[str, float] = {}
        self._load()

    def _load(self) -> None:
        if not self.path.exists():
            return
        try:
            raw = json.loads(self.path.read_text(encoding="utf-8"))
        except Exception:
            return
        if isinstance(raw, dict):
            for key, value in raw.items():
                try:
                    self._data[str(key)] = float(value)
                except Exception:
                    continue

    def _save(self) -> None:
        self.path.parent.mkdir(parents=True, exist_ok=True)
        payload = {k: round(v, 3) for k, v in self._data.items()}
        self.path.write_text(json.dumps(payload), encoding="utf-8")

    def expected(self, stage: str) -> float:
        if stage in self._data:
            return float(self._data[stage])
        return float(DEFAULT_STAGE_SECONDS.get(stage, 10.0))

    def update(self, stage: str, observed: float) -> None:
        if observed <= 0:
            return
        with self._lock:
            old = self._data.get(stage, DEFAULT_STAGE_SECONDS.get(stage, observed))
            new = (self.alpha * observed) + ((1 - self.alpha) * old)
            self._data[stage] = float(new)
            self._save()


class JobStore:
    """
    Persist jobs to disk. For persistence across Render restarts/deploys, set VANORG_STATE_DIR to a path on a Render Persistent Disk (e.g. /var/data/vanorg_jobs).
    """
    def __init__(self, root_dir: Optional[str] = None):
        root_dir = root_dir or os.environ.get("VANORG_STATE_DIR") or "/tmp/vanorg_jobs"
        self.root = Path(root_dir)
        self.root.mkdir(parents=True, exist_ok=True)
        self._lock = threading.Lock()
        self._jobs: Dict[str, Dict[str, Any]] = {}
        self._ema = ProgressEmaStore(self.root / "progress_ema.json")

    def _job_dir(self, jid: str) -> Path:
        return self.root / jid

    def _job_json(self, jid: str) -> Path:
        return self._job_dir(jid) / "job.json"

    def create(self) -> str:
        jid = uuid.uuid4().hex[:10]
        d = self._job_dir(jid)
        d.mkdir(parents=True, exist_ok=True)
        payload = {"status": "queued", "progress": {"pct": 0, "stage": "queued", "msg": "Queued"}, "error": None, "outputs": None}
        self._job_json(jid).write_text(json.dumps(payload), encoding="utf-8")
        with self._lock:
            self._jobs[jid] = payload
        return jid

    def path(self, jid: str) -> Path:
        return self._job_dir(jid)

    def get(self, jid: str) -> Dict[str, Any]:
        with self._lock:
            if jid in self._jobs:
                return dict(self._jobs[jid])

        jfile = self._job_json(jid)
        if jfile.exists():
            try:
                payload = json.loads(jfile.read_text(encoding="utf-8"))
            except Exception:
                payload = {"status": "error", "progress": {}, "error": "Corrupt job.json", "outputs": None}

            # If outputs exist, mark done (even after restart)
            d = self._job_dir(jid)
            outs = {
                "xlsx": "Bags_with_Overflow.xlsx",
                "html": "van_organizer.html",
                "stacked": "STACKED.pdf",
            }
            if all((d / v).exists() for v in outs.values()):
                payload["status"] = "done"
                payload["outputs"] = outs

            with self._lock:
                self._jobs[jid] = payload
            return dict(payload)

        return {"status": "missing"}

    def set(self, jid: str, **patch):
        payload = self.get(jid)
        payload.update(patch)
        d = self._job_dir(jid)
        d.mkdir(parents=True, exist_ok=True)
        self._job_json(jid).write_text(json.dumps(payload), encoding="utf-8")
        with self._lock:
            self._jobs[jid] = payload

    def set_progress(self, jid: str, payload: Dict[str, Any]):
        existing = (self.get(jid).get("progress") or {})
        merged = dict(existing)
        merged.update(payload)

        new_stage = merged.get("stage")
        old_stage = existing.get("stage")
        if new_stage and new_stage != old_stage:
            now = _monotonic_seconds()
            old_started = existing.get("stage_started_at")
            if old_stage and old_started is not None:
                self._ema.update(old_stage, max(0.0, now - float(old_started)))
                completed = list(existing.get("completed_stages") or [])
                if old_stage not in completed and old_stage in STAGE_WEIGHTS:
                    completed.append(old_stage)
                merged["completed_stages"] = completed
            merged["stage_started_at"] = now
        elif "stage_started_at" not in merged and existing.get("stage_started_at") is not None:
            merged["stage_started_at"] = existing.get("stage_started_at")

        if "completed_stages" not in merged and existing.get("completed_stages") is not None:
            merged["completed_stages"] = existing.get("completed_stages")

        if "last_reported_percent" not in merged and existing.get("last_reported_percent") is not None:
            merged["last_reported_percent"] = existing.get("last_reported_percent")

        # Ensure pct exists and stays 0..100 (legacy fields)
        if "pct" in merged:
            merged["pct"] = _clamp_pct(merged["pct"])
        self.set(jid, progress=merged)

    def complete_current_stage(self, jid: str) -> None:
        progress = self.get(jid).get("progress") or {}
        stage = progress.get("stage")
        started = progress.get("stage_started_at")
        if not stage or started is None:
            return
        now = _monotonic_seconds()
        self._ema.update(stage, max(0.0, now - float(started)))
        completed = list(progress.get("completed_stages") or [])
        if stage not in completed and stage in STAGE_WEIGHTS:
            completed.append(stage)
        progress["completed_stages"] = completed
        progress["stage_started_at"] = started
        self.set(jid, progress=progress)

    def compute_progress_percent(self, jid: str) -> tuple[int, str]:
        job = self.get(jid)
        status = job.get("status")
        progress = job.get("progress") or {}
        stage = progress.get("stage")
        if status == "error":
            stage_text = "Error"
        else:
            stage_text = STAGE_TEXT.get(stage, "Working…")

        if status == "done":
            return 100, "Done"

        completed = progress.get("completed_stages") or []
        completed_weight = sum(STAGE_WEIGHTS.get(s, 0.0) for s in completed)
        stage_weight = STAGE_WEIGHTS.get(stage, 0.0)

        stage_progress = 0.0
        started_at = progress.get("stage_started_at")
        if stage and started_at is not None and stage_weight > 0:
            elapsed = max(0.0, _monotonic_seconds() - float(started_at))
            expected = max(0.1, self._ema.expected(stage))
            stage_progress = min(elapsed / (expected * PROGRESS_SLACK), STAGE_PROGRESS_CAP)

        total = 100 * (completed_weight + (stage_weight * stage_progress))
        last_reported = float(progress.get("last_reported_percent") or 0.0)
        total = max(total, last_reported)
        total = min(total, 99.0)
        return int(total), stage_text


def process_job(store: JobStore, jid: str) -> None:
    try:
        store.set(
            jid,
            status="running",
            progress={
                "stage": "parse_pdf",
                "stage_started_at": _monotonic_seconds(),
                "msg": STAGE_TEXT["parse_pdf"],
                "last_reported_percent": 0,
                "completed_stages": [],
            },
        )

        job_dir = store.path(jid)
        pdf_path = job_dir / "routesheets.pdf"
        xlsx_path = job_dir / "Bags_with_Overflow.xlsx"
        html_path = job_dir / "van_organizer.html"
        stacked_pdf = job_dir / "STACKED.pdf"

        # unified progress writer
        def cb(**payload):
            store.set_progress(jid, payload)

        # 1) Excel
        generate_bags_xlsx_from_routesheets(str(pdf_path), str(xlsx_path), progress_cb=cb)

        # 2) HTML
        run_builder_html(str(pdf_path), str(xlsx_path), str(html_path), progress_cb=cb)

        # 3) Stacked PDF (with progress callback)
        cb(stage="build_html", msg=STAGE_TEXT["build_html"])
        date_label = auto_detect_date_label(str(pdf_path))

        def stack_cb(**payload):
            payload["stage"] = "build_html"
            payload.setdefault("msg", STAGE_TEXT["build_html"])
            store.set_progress(jid, payload)

        stack_results = run_stacker(str(pdf_path), str(stacked_pdf), date_label, progress_cb=stack_cb)
        toc_entries = (stack_results or {}).get("toc_entries", [])
        wave_images = list(job_dir.glob("wave_image_*"))
        wave_colors = extract_wave_color_map(wave_images, toc_entries)

        store.complete_current_stage(jid)
        store.set(
            jid,
            status="done",
            progress={"pct": 100, "stage": "done", "msg": "Done"},
            outputs={
                "xlsx": "Bags_with_Overflow.xlsx",
                "html": "van_organizer.html",
                "stacked": "STACKED.pdf",
            },
            toc={
                "date_label": (stack_results or {}).get("date_label", date_label),
                "routes": toc_entries,
                "wave_colors": wave_colors,
                "mismatch_count": (stack_results or {}).get("mismatch_count"),
            },
            summary={
                "mismatches": (stack_results or {}).get("mismatches") or [],
                "routes_over_30": (stack_results or {}).get("routes_over_30") or [],
                "routes_over_50_overflow": (stack_results or {}).get("routes_over_50_overflow") or [],
                "top10_heavy_totals": (stack_results or {}).get("top10_heavy_totals") or [],
                "top10_commercial": (stack_results or {}).get("top10_commercial") or [],
            },
        )
    except Exception as e:
        store.set(jid, status="error", error=str(e))
        store.set_progress(jid, {"stage": "error", "msg": "Error"})
