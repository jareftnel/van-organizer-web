from __future__ import annotations

import threading
from pathlib import Path

from fastapi import FastAPI, UploadFile, File, Response
from fastapi.responses import HTMLResponse, FileResponse, RedirectResponse

from .pipeline import JobStore, process_job

JOBS_DIR = Path("/tmp/vanorg_jobs")  # Render-friendly ephemeral storage
store = JobStore(str(JOBS_DIR))

app = FastAPI()


@app.get("/health")
def health():
    return {"ok": True}


@app.head("/health")
def head_health():
    return Response(status_code=200)


@app.head("/")
def head_root():
    return Response(status_code=200)


@app.get("/", response_class=HTMLResponse)
def home():
    return """
<!doctype html>
<html>
<head>
<meta name="viewport" content="width=device-width, initial-scale=1"/>
<title>Van Organizer Builder</title>
<style>
body{font-family:system-ui,-apple-system,Segoe UI,Roboto,Arial,sans-serif;margin:0;padding:18px;background:#0b0f14;color:#e8eef6}
.card{max-width:720px;margin:0 auto;background:#101826;border:1px solid #1c2a3a;border-radius:16px;padding:16px}
h1{margin:0 0 10px;font-size:22px}
p{color:#97a7bd;margin:0 0 14px}
input{width:100%;padding:14px;border-radius:12px;border:1px solid #1c2a3a;background:#0f1722;color:#e8eef6}
button{width:100%;margin-top:12px;padding:14px;border-radius:12px;border:0;background:#3fa7ff;color:#001018;font-weight:800;font-size:16px}
.small{margin-top:10px;font-size:13px;color:#97a7bd}
</style>
</head>
<body>
  <div class="card">
    <h1>Upload RouteSheets (ORIGINAL PDF)</h1>
    <p>This generates: Van Organizer (mobile page) + STACKED PDF + Bags Excel.</p>
    <form action="/upload" method="post" enctype="multipart/form-data">
      <input type="file" name="file" accept="application/pdf" required />
      <button type="submit">Build</button>
    </form>
    <div class="small">Tip: open this link in Safari for best results.</div>
  </div>
</body>
</html>
"""


@app.post("/upload")
async def upload(file: UploadFile = File(...)):
    jid = store.create()
    job_dir = store.path(jid)
    pdf_path = job_dir / "routesheets.pdf"

    data = await file.read()
    pdf_path.write_bytes(data)

    t = threading.Thread(target=process_job, args=(store, jid), daemon=True)
    t.start()

    return RedirectResponse(url=f"/job/{jid}", status_code=303)


@app.get("/job/{jid}", response_class=HTMLResponse)
def job_page(jid: str):
    j = store.get(jid)
    if j.get("status") == "missing":
        return HTMLResponse("<h3>Job not found</h3>", status_code=404)

    status = j.get("status")
    err = j.get("error")
    prog = j.get("progress") or {}

    links = ""
    if status == "done":
        links = f"""
        <div style="display:grid;gap:10px;margin-top:14px">
          <a href="/job/{jid}/organizer" style="padding:14px;border-radius:12px;background:#3fa7ff;color:#001018;font-weight:900;text-decoration:none;text-align:center;">Open Van Organizer</a>
          <a href="/job/{jid}/download/STACKED.pdf" style="padding:14px;border-radius:12px;border:1px solid #1c2a3a;background:#0f1722;color:#e8eef6;text-decoration:none;text-align:center;">Download STACKED.pdf</a>
          <a href="/job/{jid}/download/Bags_with_Overflow.xlsx" style="padding:14px;border-radius:12px;border:1px solid #1c2a3a;background:#0f1722;color:#e8eef6;text-decoration:none;text-align:center;">Download Excel</a>
        </div>
        """

    if status == "error":
        links = f"<pre style='white-space:pre-wrap;background:#0f1722;border:1px solid #1c2a3a;padding:12px;border-radius:12px;margin-top:12px'>{err}</pre>"

    pct = int(prog.get("pct", 0) or 0)
    pct = max(0, min(100, pct))

    return f"""
<!doctype html><html>
<head>
<meta name="viewport" content="width=device-width, initial-scale=1"/>
<title>Job {jid}</title>
<style>
body{{font-family:system-ui,-apple-system,Segoe UI,Roboto,Arial,sans-serif;margin:0;padding:18px;background:#0b0f14;color:#e8eef6}}
.card{{max-width:720px;margin:0 auto;background:#101826;border:1px solid #1c2a3a;border-radius:16px;padding:16px}}
.muted{{color:#97a7bd}}
.bar{{height:10px;background:#0f1722;border:1px solid #1c2a3a;border-radius:999px;overflow:hidden}}
.fill{{height:100%;width:{pct}%;background:#3fa7ff}}
</style>
</head>
<body>
  <div class="card">
    <div style="font-weight:900">Job {jid}</div>
    <div class="muted" style="margin-top:6px">Status: <b>{status}</b></div>
    <div style="margin-top:12px" class="bar"><div class="fill"></div></div>
    <div class="muted" style="margin-top:10px">Progress: {prog}</div>
    {links}
    <div class="muted" style="margin-top:14px;font-size:13px">Leave this page open; refresh if needed.</div>
  </div>
</body>
</html>
"""


@app.get("/job/{jid}/organizer")
def organizer(jid: str):
    job_dir = store.path(jid)
    html_path = job_dir / "van_organizer.html"
    if not html_path.exists():
        return HTMLResponse("Organizer not ready yet.", status_code=404)

    html = html_path.read_text(encoding="utf-8")

    # Force scrolling on mobile Safari even if the organizer CSS locks it
    scroll_fix = """
<style>
html, body {
  height: auto !important;
  min-height: 100% !important;
  overflow-y: auto !important;
  overflow-x: hidden !important;
  -webkit-overflow-scrolling: touch !important;
}

/* common wrappers that often get set to 100vh/hidden */
#app, .app, .page, .screen, .content, .container, main {
  height: auto !important;
  min-height: 100% !important;
  overflow-y: auto !important;
  -webkit-overflow-scrolling: touch !important;
}

/* if anything is position: fixed and trapping scroll, loosen it */
body { position: static !important; }
</style>
"""

    # Inject right after <head> (or at top if not found)
    if "<head>" in html:
        html = html.replace("<head>", "<head>" + scroll_fix, 1)
    else:
        html = scroll_fix + html

    return HTMLResponse(html)


@app.get("/job/{jid}/download/{name}")
def download(jid: str, name: str):
    job_dir = store.path(jid)
    f = job_dir / name
    if not f.exists():
        return HTMLResponse("File not ready yet.", status_code=404)
    return FileResponse(str(f), filename=name)
