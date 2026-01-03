from __future__ import annotations

import json
import re
import threading
import uuid
from pathlib import Path
from typing import Dict, Any, Optional, Callable

import pdfplumber
import pandas as pd

# we vendor these scripts into /tools
import sys
sys.path.append(str(Path(__file__).resolve().parents[1] / "tools"))

from route_stacker import (
    parse_route_page,
    assign_overflows,
    df_from,
    build_stacked_pdf_with_summary_grouped,
)

DATE_RE = re.compile(r"\b(?:MON|TUE|WED|THU|FRI|SAT|SUN),\s+[A-Z]{3}\s+\d{1,2},\s+\d{4}\b")

# Pipeline progress allocation (overall bar)
# Excel:   0% -> 20%
# HTML:   20% -> 30%
# Stacker:30% -> 100%
P_EXCEL_START, P_EXCEL_END = 0, 20
P_HTML_START, P_HTML_END = 20, 30
P_STACK_START, P_STACK_END = 30, 100


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


def _map_stage_pct(stage_pct: int, a: int, b: int) -> int:
    """
    Map stage_pct (0..100) into overall [a..b]
    """
    stage_pct = _clamp_pct(stage_pct)
    return int(a + (b - a) * (stage_pct / 100.0))


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

    def report(stage_pct: int, msg: str, extra: Optional[dict] = None):
        if not progress_cb:
            return
        payload = {
            "stage": "excel",
            "msg": msg,
            "pct": _map_stage_pct(stage_pct, P_EXCEL_START, P_EXCEL_END),
        }
        if extra:
            payload.update(extra)
        progress_cb(**payload)

    report(0, "Reading PDF pages for bags/overflow…")

    with pdfplumber.open(pdf_path) as pdf:
        total_pages = len(pdf.pages) or 1

        for i, page in enumerate(pdf.pages, start=1):
            # progress update
            stage_pct = int((i / total_pages) * 100)
            report(stage_pct, f"Parsing routesheets… ({i}/{total_pages})", {"page": i, "pages": total_pages})

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

    report(95, f"Writing Excel… ({out['routes']} routes)")

    Path(out_xlsx).parent.mkdir(parents=True, exist_ok=True)
    with pd.ExcelWriter(out_xlsx, engine="openpyxl") as writer:
        for sheet_name, df in wb_routes:
            safe = sheet_name[:31]  # Excel sheet name max length
            df.to_excel(writer, sheet_name=safe, index=False, header=False)

        idx = pd.DataFrame([[name] for name, _df in wb_routes], columns=["Sheets"])
        idx.to_excel(writer, sheet_name="INDEX", index=False)

    report(100, "Excel ready.")
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

    def report(stage_pct: int, msg: str, extra: Optional[dict] = None):
        if not progress_cb:
            return
        payload = {
            "stage": "html",
            "msg": msg,
            "pct": _map_stage_pct(stage_pct, P_HTML_START, P_HTML_END),
        }
        if extra:
            payload.update(extra)
        progress_cb(**payload)

    report(0, "Building Van Organizer HTML…")

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
    report(40, "Running builder…")
    subprocess.check_call(cmd)
    report(100, "HTML ready.")


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


class JobStore:
    """
    Persist jobs to disk so they survive Render restarts.
    Each job folder contains job.json.
    """
    def __init__(self, root_dir: str):
        self.root = Path(root_dir)
        self.root.mkdir(parents=True, exist_ok=True)
        self._lock = threading.Lock()
        self._jobs: Dict[str, Dict[str, Any]] = {}

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
        # Ensure pct exists and stays 0..100
        if "pct" in payload:
            payload["pct"] = _clamp_pct(payload["pct"])
        self.set(jid, progress=payload)


def process_job(store: JobStore, jid: str) -> None:
    try:
        store.set(jid, status="running", progress={"pct": 1, "stage": "start", "msg": "Starting…"})

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
        cb(stage="stacked", msg="Detecting date label…", pct=_map_stage_pct(0, P_STACK_START, P_STACK_END))
        date_label = auto_detect_date_label(str(pdf_path))

        def stack_cb(**payload):
            # route_stacker likely sends pct 0..100 — map into 30..100
            sp = payload.get("pct", 0)
            mapped = _map_stage_pct(sp, P_STACK_START, P_STACK_END)
            payload["stage"] = payload.get("stage") or "stacked"
            payload["pct"] = mapped
            store.set_progress(jid, payload)

        run_stacker(str(pdf_path), str(stacked_pdf), date_label, progress_cb=stack_cb)

        store.set(
            jid,
            status="done",
            progress={"pct": 100, "stage": "done", "msg": "Done"},
            outputs={
                "xlsx": "Bags_with_Overflow.xlsx",
                "html": "van_organizer.html",
                "stacked": "STACKED.pdf",
            }
        )
    except Exception as e:
        store.set(jid, status="error", error=str(e), progress={"pct": 100, "stage": "error", "msg": "Error"})
