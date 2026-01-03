from __future__ import annotations

import re
import uuid
import threading
from pathlib import Path
from typing import Dict, Any

import pdfplumber
import pandas as pd

import sys
sys.path.append(str(Path(__file__).resolve().parents[1] / "tools"))

from route_stacker import (
    parse_route_page,
    assign_overflows,
    df_from,
    build_stacked_pdf_with_summary_grouped,
)

DATE_RE = re.compile(r"\b(?:MON|TUE|WED|THU|FRI|SAT|SUN),\s+[A-Z]{3}\s+\d{1,2},\s+\d{4}\b")


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


def generate_bags_xlsx_from_routesheets(pdf_path: str, out_xlsx: str) -> Dict[str, Any]:
    """
    Creates multi-sheet xlsx where each sheet name matches builder expectation:
      <RS>_<CX>  e.g. H.7_CX92
    Each sheet rows: Bag | Overflow Zone(s) | Overflow Pkgs (total)
    """
    out: Dict[str, Any] = {"routes": 0}
    pdf_path = str(pdf_path)
    out_xlsx = str(out_xlsx)

    wb_routes = []

    with pdfplumber.open(pdf_path) as pdf:
        for page in pdf.pages:
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

            sheet_name = f"{rs}_{cx}"
            wb_routes.append((sheet_name, df))
            out["routes"] += 1

    if out["routes"] == 0:
        raise RuntimeError("No routes were parsed from the uploaded PDF.")

    Path(out_xlsx).parent.mkdir(parents=True, exist_ok=True)
    with pd.ExcelWriter(out_xlsx, engine="openpyxl") as writer:
        for sheet_name, df in wb_routes:
            safe = sheet_name[:31]
            # builder expects no header row
            df.to_excel(writer, sheet_name=safe, index=False, header=False)

        idx = pd.DataFrame([[name] for name, _df in wb_routes], columns=["Sheets"])
        idx.to_excel(writer, sheet_name="INDEX", index=False)

    return out


def run_builder_html(pdf_path: str, xlsx_path: str, out_html: str) -> None:
    import subprocess
    builder = Path(__file__).resolve().parents[1] / "tools" / "build_van_organizer_v21_hide_combined_ORIGPDF.py"
    cmd = [
        "python",
        str(builder),
        "--pdf", str(pdf_path),
        "--xlsx", str(xlsx_path),
        "--out", str(out_html),
        "--no-cache",
    ]
    subprocess.check_call(cmd)


class JobStore:
    def __init__(self, root_dir: str):
        self.root = Path(root_dir)
        self.root.mkdir(parents=True, exist_ok=True)
        self._lock = threading.Lock()
        self._jobs: Dict[str, Dict[str, Any]] = {}

    def create(self) -> str:
        jid = uuid.uuid4().hex[:10]
        with self._lock:
            self._jobs[jid] = {"status": "queued", "progress": {}, "error": None, "outputs": {}}
        (self.root / jid).mkdir(parents=True, exist_ok=True)
        return jid

    def path(self, jid: str) -> Path:
        return self.root / jid

    def get(self, jid: str) -> Dict[str, Any]:
        with self._lock:
            return dict(self._jobs.get(jid, {"status": "missing"}))

    def set(self, jid: str, **patch):
        with self._lock:
            self._jobs.setdefault(jid, {})
            self._jobs[jid].update(patch)

    def set_progress(self, jid: str, payload: Dict[str, Any]):
        with self._lock:
            self._jobs.setdefault(jid, {})
            self._jobs[jid]["progress"] = payload


def process_job(store: JobStore, jid: str) -> None:
    try:
        store.set(jid, status="running")

        job_dir = store.path(jid)
        pdf_path = job_dir / "routesheets.pdf"
        xlsx_path = job_dir / "Bags_with_Overflow.xlsx"
        html_path = job_dir / "van_organizer.html"
        stacked_pdf = job_dir / "STACKED.pdf"

        # 1) Excel from original PDF
        generate_bags_xlsx_from_routesheets(str(pdf_path), str(xlsx_path))

        # 2) HTML from (original PDF + Excel)
        run_builder_html(str(pdf_path), str(xlsx_path), str(html_path))

        # 3) Stacked PDF
        date_label = auto_detect_date_label(str(pdf_path))

        def cb(**payload):
            # optional: route_stacker may pass progress info
            store.set_progress(jid, payload)

        build_stacked_pdf_with_summary_grouped(
            str(pdf_path),
            str(stacked_pdf),
            date_label,
            progress_cb=cb
        )

        store.set(
            jid,
            status="done",
            outputs={
                "xlsx": "Bags_with_Overflow.xlsx",
                "html": "van_organizer.html",
                "stacked": "STACKED.pdf",
            }
        )
    except Exception as e:
        store.set(jid, status="error", error=str(e))
