from __future__ import annotations

import json
import re
import threading
import uuid
from pathlib import Path
from typing import Dict, Any

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
    out = {"routes": 0, "errors": []}
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

            sheet_name = f"{rs}_{cx}"  # matches builder SHEET_RE
            wb_routes.append((sheet_name, df))
            out["routes"] += 1

    if out["routes"] == 0:
        raise RuntimeError("No routes were parsed from the uploaded PDF.")

    Path(out_xlsx).parent.mkdir(parents=True, exist_ok=True)
    with pd.ExcelWriter(out_xlsx, engine="openpyxl") as writer:
        for sheet_name, df in wb_routes:
            safe = sheet_name[:31]  # Excel sheet name max length
            df.to_excel(writer, sheet_name=safe, index=False, header=False)

        idx = pd.DataFrame([[name] for name, _df in wb_routes], columns=["Sheets"])
        idx.to_excel(writer, sheet_name="INDEX", index=False)

    return out


def run_builder_html(pdf_path: str, xlsx_path: str, out_html: str) -> None:
    """
    Calls your v21 builder script.
    """
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


def run_stacker(pdf_path: str, out_pdf: str, date_label: str, progress_cb=None) -> Dict[str, Any]:
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
        payload = {"status": "queued", "progress": {}, "error": None, "outputs": None}
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
        self.set(jid, progress=payload)


def process_job(store: JobStore, jid: str) -> None:
    try:
        store.set(jid, status="running")

        job_dir = store.path(jid)
        pdf_path = job_dir / "routesheets.pdf"
        xlsx_path = job_dir / "Bags_with_Overflow.xlsx"
        html_path = job_dir / "van_organizer.html"
        stacked_pdf = job_dir / "STACKED.pdf"

        # 1) Excel
        generate_bags_xlsx_from_routesheets(str(pdf_path), str(xlsx_path))

        # 2) HTML
        run_builder_html(str(pdf_path), str(xlsx_path), str(html_path))

        # 3) Stacked PDF (with progress callback)
        date_label = auto_detect_date_label(str(pdf_path))

        def cb(**payload):
            store.set_progress(jid, payload)

        run_stacker(str(pdf_path), str(stacked_pdf), date_label, progress_cb=cb)

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
