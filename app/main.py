from __future__ import annotations

import json
import threading
import time
from pathlib import Path

from fastapi import FastAPI, UploadFile, File, Response
from fastapi.staticfiles import StaticFiles
from fastapi.responses import (
    HTMLResponse,
    FileResponse,
    RedirectResponse,
    JSONResponse,
)

from .pipeline import JobStore, process_job

JOBS_DIR = Path("/tmp/vanorg_jobs")
store = JobStore(str(JOBS_DIR))

app = FastAPI()

REPO_ROOT = Path(__file__).resolve().parents[1]
STATIC_DIR = Path(__file__).parent / "static"
app.mount("/static", StaticFiles(directory=str(STATIC_DIR)), name="static")


# ---------------------------
# No-cache middleware (important on Render + phones)
# ---------------------------
@app.middleware("http")
async def no_cache_mw(request, call_next):
    resp = await call_next(request)
    # Avoid stale status/progress + stale PDFs/HTML behind mobile caches
    resp.headers["Cache-Control"] = "no-store"
    resp.headers["Pragma"] = "no-cache"
    resp.headers["Expires"] = "0"
    return resp


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
body{
  font-family:system-ui,-apple-system,Segoe UI,Roboto,Arial,sans-serif;
  margin:0;
  background:#0b0f14;
  color:#e8eef6;
}
.page{
  min-height:100vh;
  display:flex;
  align-items:center;
  justify-content:center;
  padding:18px;
}
.brandBlock{
  display:flex;
  flex-direction:column;
  align-items:center;
  gap:0;
  width:100%;
  max-width:780px;
}
.bannerWrap{
  width:100%;
  max-width:780px;
  margin:0 auto 28px;
}
.siteBanner{
  width:100%;
  height:auto;
  max-width:100%;
  object-fit:contain;
  display:block;
}
.card{
  width:100%;
  max-width:780px;
  background:#101826;
  border:1px solid #1c2a3a;
  border-radius:18px;
  padding:32px;
  box-shadow:0 14px 36px rgba(0,0,0,0.35);
}
form{display:flex;flex-direction:column;gap:16px}
input[type="file"]{
  width:100%;
  padding:12px;
  border-radius:12px;
  border:1px solid #1c2a3a;
  background:#0f1722;
  color:#e8eef6;
}
input[type="file"]::file-selector-button{
  margin-right:12px;
  padding:10px 14px;
  border-radius:10px;
  border:1px solid #2a3a4d;
  background:#111c2b;
  color:#e8eef6;
  font-weight:600;
}
button{
  width:100%;
  padding:14px;
  border-radius:12px;
  border:0;
  background:#3fa7ff;
  color:#001018;
  font-weight:800;
  font-size:16px;
  cursor:pointer;
}
.helper{
  text-align:center;
  font-size:12px;
  color:#9aa6b2;
  margin-top:-4px;
}
</style>
</head>
<body>
  <div class="page">
    <div class="brandBlock">
      <div class="bannerWrap">
        <img class="siteBanner" src="/banner.png" alt="Van Organizer Banner" />
      </div>
      <div class="card">
        <form action="/upload" method="post" enctype="multipart/form-data">
          <input type="file" name="file" accept="application/pdf" required />
          <button type="submit">Build</button>
          <div class="helper">PDF only.</div>
        </form>
      </div>
    </div>
  </div>
</body>
</html>
"""


@app.get("/banner.png")
def banner_png():
    banner_path = REPO_ROOT / "banner.png"
    return FileResponse(str(banner_path))


@app.post("/upload")
async def upload(file: UploadFile = File(...)):
    jid = store.create()
    job_dir = store.path(jid)
    pdf_path = job_dir / "routesheets.pdf"
    pdf_path.write_bytes(await file.read())

    t = threading.Thread(target=process_job, args=(store, jid), daemon=True)
    t.start()

    return RedirectResponse(url=f"/job/{jid}", status_code=303)


# ---------------------------
# Status endpoint for polling (no refresh needed)
# ---------------------------
@app.get("/job/{jid}/status")
def job_status(jid: str):
    j = store.get(jid)
    if j.get("status") == "missing":
        return JSONResponse({"status": "missing"}, status_code=404)

    job_dir = store.path(jid)
    out_pdf = job_dir / "STACKED.pdf"
    out_xlsx = job_dir / "Bags_with_Overflow.xlsx"
    out_html = job_dir / "van_organizer.html"

    return {
        "status": j.get("status", ""),
        "error": j.get("error"),
        "progress": j.get("progress") or {},
        "has_pdf": out_pdf.exists(),
        "has_xlsx": out_xlsx.exists(),
        "has_html": out_html.exists(),
        # stable URLs (client will cache-bust with ?v=)
        "organizer_url": f"/job/{jid}/organizer",
        "pdf_url": f"/job/{jid}/download/STACKED.pdf",
        "xlsx_url": f"/job/{jid}/download/Bags_with_Overflow.xlsx",
        "ts": int(time.time()),
    }


@app.get("/job/{jid}", response_class=HTMLResponse)
def job_page(jid: str):
    """
    IMPORTANT: This page contains lots of JS { } braces, so we avoid Python f-strings here.
    We use a plain template string + .replace() so JS doesn't break Python formatting.
    """
    j = store.get(jid)
    if j.get("status") == "missing":
        return HTMLResponse("<h3>Job not found</h3>", status_code=404)

    status = j.get("status", "")
    prog = j.get("progress") or {}

    pct = int(prog.get("pct", 0) or 0)
    pct = max(0, min(100, pct))

    status_line = ""
    if prog.get("page") is not None and prog.get("pages") is not None:
        status_line = f"Processing route {prog.get('page')} of {prog.get('pages')}"
    elif prog.get("msg"):
        status_line = str(prog.get("msg"))
    elif status:
        status_line = str(status)
    else:
        status_line = "Working…"

    html = """<!doctype html><html>
<head>
<meta name="viewport" content="width=device-width, initial-scale=1"/>
<title>Building…</title>
<style>
body{font-family:system-ui,-apple-system,Segoe UI,Roboto,Arial,sans-serif;margin:0;min-height:100vh;display:flex;align-items:center;justify-content:center;background:#0b0f14;color:#e8eef6;padding:24px}
.card{width:min(540px,100%);background:#101826;border:1px solid #1c2a3a;border-radius:18px;padding:24px 22px;box-shadow:0 18px 40px rgba(5,9,14,.45)}
.title{font-size:24px;font-weight:800;letter-spacing:.2px}
.muted{color:#97a7bd}
.status{margin-top:14px;font-size:15px;font-weight:600}
.subtle{margin-top:6px;font-size:13px}
.bar{margin-top:16px;height:10px;background:#0f1722;border:1px solid #1c2a3a;border-radius:999px;overflow:hidden}
.fill{height:100%;background:linear-gradient(90deg,#3fa7ff,#66b6ff);transition:width .25s ease}
.error{margin-top:12px;color:#ffb4b4;background:#291414;border:1px solid #3a1c1c;padding:10px 12px;border-radius:10px;font-size:13px}
</style>
</head>
<body>
  <div class="card">
    <div class="title">Building…</div>

    <div class="bar">
      <div class="fill" id="fill" style="width: __PCT__%"></div>
    </div>

    <div class="status" id="statusLine">__STATUS_LINE__</div>
    <div class="muted subtle">This page updates automatically.</div>

    <div class="error" id="err" style="display:none"></div>
  </div>

<script>
(function(){
  var jid = "__JID__";
  var fill = document.getElementById("fill");
  var statusLine = document.getElementById("statusLine");
  var err = document.getElementById("err");

  function setPct(p){
    p = Math.max(0, Math.min(100, p|0));
    fill.style.width = p + "%";
  }

  function showErr(msg){
    err.textContent = msg || "Unknown error";
    err.style.display = "block";
  }

  async function tick(){
    try{
      var r = await fetch("/job/" + jid + "/status", { cache: "no-store" });
      if(!r.ok) return;
      var s = await r.json();

      var nextLine = "";
      if(s.progress && typeof s.progress.page !== "undefined" && typeof s.progress.pages !== "undefined"){
        nextLine = "Processing route " + s.progress.page + " of " + s.progress.pages;
      }else if(s.progress && s.progress.msg){
        nextLine = s.progress.msg;
      }else if(s.status){
        nextLine = s.status;
      }else{
        nextLine = "Working…";
      }
      statusLine.textContent = nextLine;

      var pct = 0;
      if(s.progress && typeof s.progress.pct !== "undefined") pct = parseInt(s.progress.pct, 10) || 0;
      setPct(pct);

      var stage = s.progress ? s.progress.stage : "";
      if(s.status === "done" || s.has_html){
        // Cache-bust so mobile browsers don't show old files
        var bust = "v=" + Date.now();
        window.location.replace(s.organizer_url + "?" + bust);
        clearInterval(timer);
      } else if(s.status === "error"){
        showErr(s.error);
        clearInterval(timer);
      }
    }catch(e){
      // Ignore transient network errors; next poll will recover.
    }
  }

  tick();
  var timer = setInterval(tick, 1000);
})();
</script>
</body>
</html>
"""

    html = (
        html.replace("__JID__", jid)
            .replace("__PCT__", str(pct))
            .replace("__STATUS_LINE__", status_line)
    )
    return HTMLResponse(html)


@app.get("/job/{jid}/organizer_raw", response_class=HTMLResponse)
def organizer_raw(jid: str):
    job_dir = store.path(jid)
    html_path = job_dir / "van_organizer.html"
    if not html_path.exists():
        return HTMLResponse("Organizer not ready yet.", status_code=404)

    html = html_path.read_text(encoding="utf-8")
    # Patch older organizer HTML so the combined tab is visible and default.
    old_tabs = """  <div class="pills">
    <div class="tab active" data-tab="bags">Bags</div>
    <div class="tab" data-tab="overflow">Overflow</div>
    <div class="tab" data-tab="combined">Bags + Overflow</div>
  </div>
"""
    new_tabs = """  <div class="pills">
    <div class="tab active" data-tab="combined">Bags + Overflow</div>
    <div class="tab" data-tab="bags">Bags</div>
    <div class="tab" data-tab="overflow">Overflow</div>
  </div>
"""
    if old_tabs in html and new_tabs not in html:
        html = html.replace(old_tabs, new_tabs)
    html = html.replace('.tab[data-tab="combined"]{display:none !important;}', "")
    html = html.replace('#combinedPanel, .combinedPanel, [data-panel="combined"]{display:none !important;}', "")
    if 'let activeTab = "bags";' in html:
        html = html.replace('let activeTab = "bags";', 'let activeTab = "combined";')
    html = html.replace('  if(activeTab==="combined") activeTab="bags";', "")
    if ".pills{display:flex;gap:8px;margin-top:12px}" in html:
        html = html.replace(
            ".pills{display:flex;gap:8px;margin-top:12px}",
            ".pills{display:flex;gap:8px;margin-top:12px;flex-wrap:wrap}",
        )

    # Explicit no-cache for embedded content too
    resp = HTMLResponse(html)
    resp.headers["Cache-Control"] = "no-store"
    resp.headers["Pragma"] = "no-cache"
    resp.headers["Expires"] = "0"
    return resp


@app.get("/job/{jid}/organizer", response_class=HTMLResponse)
def organizer_wrapper(jid: str):
    """
    Wrapper that:
    - loads organizer_raw in an iframe
    - measures true content span (minLeft..maxRight) inside the iframe
    - parent page scrolls normally

    IMPORTANT: do NOT use JS template literals (`...${}...`) inside this Python f-string.
    """
    return HTMLResponse(f"""
<!doctype html>
<html>
<head>
<meta name="viewport" content="width=device-width, initial-scale=1"/>
<title>Van Organizer</title>
<style>
html,body{{margin:0;padding:0;background:#0b0f14;color:#e8eef6;font-family:system-ui,-apple-system,Segoe UI,Roboto,Arial,sans-serif}}
.topbar{{position:sticky;top:0;z-index:10;background:#101826;border-bottom:1px solid #1c2a3a;padding:10px 12px}}
.topbar a{{color:#3fa7ff;text-decoration:none;font-weight:800}}
.wrap{{padding:10px}}
iframe{{border:0; display:block; width:100%}}
</style>
</head>
<body>
  <div class="topbar">
    <a href="/job/{jid}">← Back</a>
  </div>

  <div class="wrap">
    <iframe id="orgFrame" src="/job/{jid}/organizer_raw?v=1" scrolling="no"></iframe>
  </div>

<script>
(function () {{
  var frame = document.getElementById("orgFrame");

  // cache-bust iframe so it always pulls the newest organizer without manual refresh
  frame.src = "/job/{jid}/organizer_raw?v=" + Date.now();

  function sizeFrame() {{
    try {{
      var doc = frame.contentDocument || frame.contentWindow.document;
      if (!doc) return;

      var b = doc.body;
      var h = doc.documentElement;
      var height = Math.max(
        b.scrollHeight,
        h.scrollHeight,
        b.offsetHeight,
        h.offsetHeight,
        b.clientHeight,
        h.clientHeight
      );
      frame.style.height = height + "px";
    }} catch (e) {{}}
  }}

  var burstTimer = null;
  function startBurst() {{
    if (burstTimer) {{
      clearInterval(burstTimer);
      burstTimer = null;
    }}
    var start = Date.now();
    burstTimer = setInterval(function () {{
      sizeFrame();
      if (Date.now() - start > 2500) {{
        clearInterval(burstTimer);
        burstTimer = null;
      }}
    }}, 200);
  }}

  frame.addEventListener("load", function () {{
    sizeFrame();
    startBurst();
  }});

  window.addEventListener("resize", function () {{
    sizeFrame();
    startBurst();
  }});
}})();
</script>
</body>
</html>
""")


@app.get("/job/{jid}/download/{name}")
def download(jid: str, name: str):
    job_dir = store.path(jid)
    f = job_dir / name
    if not f.exists():
        return HTMLResponse("File not ready yet.", status_code=404)

    # FileResponse is fine; we keep no-store via middleware, but also set here explicitly
    return FileResponse(
        str(f),
        filename=name,
        headers={"Cache-Control": "no-store", "Pragma": "no-cache", "Expires": "0"},
    )
