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


@app.get("/van.png")
def van_icon():
    return FileResponse(REPO_ROOT / "van.png", media_type="image/png")


@app.get("/banner.png")
def banner_image():
    return FileResponse(
        REPO_ROOT / "banner_clean_1600x400 (1).png",
        media_type="image/png",
    )


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
html, body{
  height:100%;
  overflow:hidden;
}
body{
  font-family:system-ui,-apple-system,Segoe UI,Roboto,Arial,sans-serif;
  margin:0;
  background:#0b0f14;
  color:#e8eef6;
}
:root{
  --r:22px;
  --glass:rgba(255,255,255,0.06);
  --glassBorder:rgba(255,255,255,0.10);
}
.uploadPage{
  height:100svh;
  display:flex;
  align-items:center;
  justify-content:center;
  padding:36px 18px 18px;
  box-sizing:border-box;
}
.heroWrap{
  width:100%;
  max-width:1100px;
  margin:0 auto;
  display:flex;
  flex-direction:column;
  align-items:stretch;
  gap:0;
}
.heroWrap > *{
  width:100%;
}
.brandBanner{
  display:block;
  width:100%;
  height:auto;
  object-fit:contain;
  object-position:center;
  box-sizing:border-box;
  border:1px solid var(--glassBorder);
  border-radius:var(--r) var(--r) 0 0;
  box-shadow:0 18px 45px rgba(0,0,0,0.40);
}
.tagGlass{
  width:100%;
  margin-top:-12px;
  padding:14px 0;
  background:rgba(255,255,255,0.06);
  border:1px solid rgba(255,255,255,0.10);
  backdrop-filter:blur(10px);
  -webkit-backdrop-filter:blur(10px);
  border-radius:0;
  box-shadow:0 16px 40px rgba(0,0,0,0.35);
}
.taglineText{
  text-align:center;
  letter-spacing:2px;
  font-size:13px;
  opacity:0.85;
}
.heroWrap, .tagGlass, .uploadCard, .fileRow, .fileNameLabel, .fileBtn, .buildBtn{
  box-sizing:border-box;
}
.uploadCard{
  width:100%;
  max-width:100%;
  background:rgba(10,16,26,0.55);
  border:1px solid var(--glassBorder);
  border-radius:0 0 var(--r) var(--r);
  padding:22px;
  margin-top:0;
  box-shadow:0 18px 45px rgba(0,0,0,0.35);
}
form{display:flex;flex-direction:column;gap:16px}
.fileRow{
  width:100%;
  max-width:100%;
  display:flex;
  align-items:center;
  gap:16px;
  padding:14px;
  background:rgba(255,255,255,0.04);
  border:1px solid rgba(255,255,255,0.08);
  border-radius:14px;
  position:relative;
}
.fileBtn .fileIcon{
  width:18px;
  height:18px;
  fill:currentColor;
}
.fileBtn{
  position:relative;
  display:inline-flex;
  align-items:center;
  justify-content:center;
  height:40px;
  padding:0 16px;
  border-radius:12px;
  cursor:pointer;
  border:1px solid rgba(255,255,255,0.10);
  background:rgba(0,0,0,0.18);
  color:#e8eef6;
  font-weight:600;
  flex:0 0 auto;
  transition:transform 120ms ease, box-shadow 120ms ease, background 120ms ease;
}
.fileBtn .fileIcon{
  position:absolute;
  left:16px;
  top:50%;
  transform:translateY(-50%);
  pointer-events:none;
}
.uploadText{
  font-size:clamp(10px, 1.6vh, 12px);
  max-width:160px;
  white-space:nowrap;
  overflow:hidden;
  text-overflow:ellipsis;
}
.fileBtn:hover{
  transform:translateY(-1px);
  box-shadow:0 8px 18px rgba(0,0,0,0.25);
}
.fileBtn:focus-visible{
  outline:2px solid rgba(63,167,255,0.6);
  outline-offset:2px;
}
.fileNameLabel{
  color:rgba(255,255,255,0.85);
  white-space:nowrap;
  overflow:hidden;
  text-overflow:ellipsis;
  font-size:clamp(12px, 1.8vh, 14px);
  flex:0 1 auto;
  min-width:0;
  text-align:left;
  opacity:0.9;
  font-weight:600;
}
.fileInfo{
  display:flex;
  flex-direction:column;
  gap:4px;
  flex:1 1 auto;
  min-width:0;
}
.fileHint{
  color:rgba(255,255,255,0.55);
  font-size:clamp(10px, 1.4vh, 12px);
  font-weight:500;
  white-space:nowrap;
  overflow:hidden;
  text-overflow:ellipsis;
}
.waveBadge{
  position:absolute;
  right:10px;
  bottom:10px;
  width:28px;
  height:28px;
  border-radius:50%;
  border:none;
  background:#f39c12;
  color:#1b1b1b;
  display:flex;
  align-items:center;
  justify-content:center;
  cursor:pointer;
  box-shadow:0 10px 18px rgba(0,0,0,0.3);
  transition:transform 120ms ease, box-shadow 120ms ease, filter 120ms ease;
}
.waveBadge .plusIcon{
  font-size:18px;
  font-weight:700;
  line-height:1;
  color:#111111;
}
.waveBadge:hover{
  transform:translateY(-1px);
  box-shadow:0 14px 24px rgba(0,0,0,0.35);
  filter:brightness(1.02);
}
.waveBadge:focus-visible{
  outline:2px solid rgba(63,167,255,0.6);
  outline-offset:2px;
}
.page, .container, .shell{
  max-width:none !important;
  width:100% !important;
  padding-left:24px;
  padding-right:24px;
}
.uploadBtn,
.buildBtn{
  height:clamp(48px, 7vh, 64px);
  font-size:clamp(16px, 2.4vh, 20px);
  border-radius:16px;
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
.buildBtn{
  width:100%;
  max-width:100%;
  transition:transform 120ms ease, box-shadow 120ms ease, filter 120ms ease;
}
.buildBtn:hover{
  transform:translateY(-1px);
  box-shadow:0 14px 30px rgba(0,0,0,0.35);
  filter:brightness(1.03);
}
.buildBtn:active{
  transform:translateY(1px);
  box-shadow:0 8px 18px rgba(0,0,0,0.25);
}
@media (orientation: landscape) and (max-height: 560px){
  html, body{
    height:100%;
    overflow:hidden;
  }
  .brandBanner{
    height:auto;
    max-height:min(180px, 28svh);
    object-fit:contain;
    object-position:center;
  }
  .uploadPage{
    height:100svh;
    align-items:center;
    padding-top:8px;
    padding-bottom:calc(8px + env(safe-area-inset-bottom, 0px));
  }
  .heroWrap{
    width:100%;
  }
  .tagGlass{
    padding:6px 0;
  }
  .uploadCard{
    padding:10px;
  }
  form{
    gap:6px;
  }
  .fileRow{
    padding:6px;
    gap:6px;
  }
  .uploadBtn,
  .buildBtn{
    height:36px;
    font-size:14px;
    border-radius:12px;
  }
}
@media (orientation: portrait) and (max-height: 560px){
  html, body{
    height:auto;
    min-height:100%;
    overflow:auto;
  }
  .uploadPage{
    height:auto;
    min-height:100svh;
    align-items:flex-start;
    padding-top:20px;
  }
}
</style>
</head>
<body>
  <div class="uploadPage">
    <div class="heroWrap">
      <img class="brandBanner" src="/banner.png" alt="Van Organizer Banner" />
      <div class="tagGlass">
        <div class="taglineText">OPTIMIZE YOUR ROUTE</div>
      </div>
      <div class="uploadCard">
        <form action="/upload" method="post" enctype="multipart/form-data">
          <div class="fileRow">
            <button class="fileBtn uploadBtn" type="button">
              <svg class="fileIcon" viewBox="0 0 24 24" role="img" focusable="false" aria-hidden="true">
                <path d="M6 2h7l5 5v13a2 2 0 0 1-2 2H6a2 2 0 0 1-2-2V4a2 2 0 0 1 2-2zm7 1.5V8h4.5L13 3.5zM8 12h8v2H8v-2zm0 4h8v2H8v-2z"/>
              </svg>
              <span class="uploadText">Upload</span>
            </button>
            <div class="fileInfo">
              <div class="fileNameLabel" id="fileLabel">Choose file</div>
              <div class="fileHint" id="waveLabel">Wave images (optional)</div>
            </div>
            <button class="waveBadge" type="button" id="waveBtn" aria-label="Add wave images">
              <span class="plusIcon" aria-hidden="true">+</span>
            </button>
            <input id="fileInput" class="fileInput" type="file" name="file" accept="application/pdf" hidden required />
            <input id="waveInput" class="fileInput" type="file" name="wave_images" accept="image/*" multiple hidden />
          </div>
          <button class="buildBtn" type="submit">Build</button>
        </form>
      </div>
    </div>
  </div>
  <script>
    const fileInput = document.getElementById("fileInput");
    const fileLabel = document.getElementById("fileLabel");
    const fileBtn = document.querySelector(".fileBtn");
    const uploadText = document.querySelector(".uploadText");
    const waveInput = document.getElementById("waveInput");
    const waveLabel = document.getElementById("waveLabel");
    const waveBtn = document.getElementById("waveBtn");

    if (fileBtn && fileInput) {
      fileBtn.addEventListener("click", () => fileInput.click());
    }

    if (fileInput && fileLabel) {
      fileInput.addEventListener("change", () => {
        const name = fileInput.files && fileInput.files.length > 0
          ? fileInput.files[0].name
          : "Choose file";
        fileLabel.textContent = name;
        if (uploadText) {
          uploadText.textContent = fileInput.files && fileInput.files.length > 0
            ? fileInput.files[0].name
            : "Upload";
        }
      });
    }

    if (waveBtn && waveInput) {
      waveBtn.addEventListener("click", () => waveInput.click());
    }

    if (waveInput && waveLabel) {
      waveInput.addEventListener("change", () => {
        const count = waveInput.files ? waveInput.files.length : 0;
        if (!count) {
          waveLabel.textContent = "Wave images (optional)";
          return;
        }
        waveLabel.textContent = count === 1
          ? waveInput.files[0].name
          : `Wave images: ${count} selected`;
      });
    }
  </script>
</body>
</html>
"""


@app.get("/banner.png")
def banner_png():
    banner_path = REPO_ROOT / "banner.png"
    return FileResponse(str(banner_path))


@app.post("/upload")
async def upload(
    file: UploadFile = File(...),
    wave_images: list[UploadFile] | None = File(None),
):
    jid = store.create()
    job_dir = store.path(jid)
    pdf_path = job_dir / "routesheets.pdf"
    pdf_path.write_bytes(await file.read())

    if wave_images:
        for idx, image in enumerate(wave_images, start=1):
            if not image or not image.filename:
                continue
            suffix = Path(image.filename).suffix or ".png"
            dest = job_dir / f"wave_image_{idx}{suffix.lower()}"
            dest.write_bytes(await image.read())

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
        "has_toc": bool(j.get("toc")),
        # stable URLs (client will cache-bust with ?v=)
        "organizer_url": f"/job/{jid}/organizer",
        "pdf_url": f"/job/{jid}/download/STACKED.pdf",
        "xlsx_url": f"/job/{jid}/download/Bags_with_Overflow.xlsx",
        "toc_url": f"/job/{jid}/toc",
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
        status_line = "Running . . ." if status == "running" else str(status)
    else:
        status_line = "Working…"

    html = """<!doctype html><html>
<head>
<meta name="viewport" content="width=device-width, initial-scale=1"/>
<title>Building…</title>
<style>
html, body{
  height:100%;
  overflow:hidden;
}
body{
  font-family:system-ui,-apple-system,Segoe UI,Roboto,Arial,sans-serif;
  margin:0;
  background:#0b0f14;
  color:#e8eef6;
}
.page{
  height:100svh;
  display:flex;
  justify-content:center;
  align-items:center;
  padding:env(safe-area-inset-top) env(safe-area-inset-right) env(safe-area-inset-bottom) env(safe-area-inset-left);
}
.card{
  width:min(92vw, 760px);
  max-height:100svh;
  display:flex;
  flex-direction:column;
  gap:16px;
  border-radius:22px;
  overflow:hidden;
  background:#101826;
  border:1px solid #1c2a3a;
  padding:26px 24px;
  box-shadow:0 18px 40px rgba(5,9,14,.45);
}
.title{font-size:24px;font-weight:800;letter-spacing:.2px}
.muted{color:#97a7bd}
.status{margin-top:14px;font-size:15px;font-weight:600}
.subtle{margin-top:6px;font-size:12px;opacity:0.7}
.auto-refresh{
  display:flex;
  align-items:center;
  justify-content:center;
  gap:8px;
  text-align:center;
  width:100%;
}
.auto-refresh::before{
  content:"";
  width:8px;
  height:8px;
  border-radius:50%;
  background:#38bdf8;
  box-shadow:0 0 0 0 rgba(56,189,248,.6);
  animation:autoPulse 1.6s ease-out infinite;
}
@keyframes autoPulse{
  0%{ box-shadow:0 0 0 0 rgba(56,189,248,.6); opacity:1; }
  70%{ box-shadow:0 0 0 8px rgba(56,189,248,0); opacity:.6; }
  100%{ box-shadow:0 0 0 12px rgba(56,189,248,0); opacity:.4; }
}
.error{margin-top:12px;color:#ffb4b4;background:#291414;border:1px solid #3a1c1c;padding:10px 12px;border-radius:10px;font-size:13px}

:root{
  --edv-blue:#2b6f9c;
  --edv-blue-dark:#1e4f72;
  --edv-glass:#9ec6df;
  --edv-wheel:#0b0f14;
}
.job-progress{ width:100%; margin:10px 0 0; }
.road{
  position:relative;
  height:20px;
  border-radius:999px;
  background:linear-gradient(180deg,#5b5f66,#3f434a);
  box-shadow:inset 0 0 0 1px rgba(0,0,0,.35);
  overflow:visible;
}
.lane{
  position:absolute; left:0; right:0; top:50%;
  height:3px; transform:translateY(-50%);
  background:repeating-linear-gradient(90deg,#f6c945 0 12px,transparent 12px 24px);
  background-size:24px 100%;
  opacity:.9;
  animation:laneMove 1.2s linear infinite;
}
@keyframes laneMove{ from{background-position:0 0;} to{background-position:24px 0;} }
.van{
  position:absolute;
  top:0;
  left:0%;
  transform:translate(-50%,-80%);
  transition:left .35s ease;
  filter:drop-shadow(0 6px 14px rgba(0,0,0,.45));
  pointer-events:none;
}
.van img{
  width:64px;
  height:auto;
  display:block;
  filter:drop-shadow(0 0 10px rgba(255,152,0,.85));
}
.van.moving img{ animation:vanBob 1.2s ease-in-out infinite; }
@keyframes vanBob{ 0%,100%{ transform:translateY(0); } 50%{ transform:translateY(-2px); } }
.van.parsing{ filter:drop-shadow(0 6px 14px rgba(0,0,0,.45)) grayscale(0.5); }
.van.building{ filter:drop-shadow(0 6px 14px rgba(0,0,0,.45)); }
.van.organizing{ filter:drop-shadow(0 6px 14px rgba(0,0,0,.45)) saturate(1.2); }
.van.complete{ filter:drop-shadow(0 0 12px rgba(34,197,94,.6)); }
.job-complete .lane{ animation:none; opacity:.25; }
.progress-meta{
  display:flex;
  justify-content:space-between;
  margin-top:14px;
  font-weight:600;
  opacity:.9;
}
</style>
</head>
<body>
  <div class="page">
    <div class="card">
      <div class="title">Building…</div>

      <div class="job-progress" id="jobProgress">
        <div class="road" aria-hidden="true">
          <div class="lane"></div>
          <div class="van building moving" id="vanIcon" style="left: __PCT__%">
            <img src="/van.png" alt="" aria-hidden="true" />
          </div>
        </div>
        <div class="progress-meta">
          <div id="statusText">__STATUS_LINE__</div>
          <div id="pctText">__PCT__%</div>
        </div>
      </div>

      <div class="muted subtle auto-refresh">This page updates automatically.</div>

      <div class="error" id="err" style="display:none"></div>
    </div>
  </div>

<script>
(function(){
  var jid = "__JID__";
  var err = document.getElementById("err");
  var van = document.getElementById("vanIcon");
  var root = document.getElementById("jobProgress");
  var pctEl = document.getElementById("pctText");
  var statusEl = document.getElementById("statusText");

  function setProgress(pct, statusText){
    var clamped = Math.max(0, Math.min(100, Number(pct) || 0));
    van.style.left = clamped + "%";
    pctEl.textContent = clamped.toFixed(0) + "%";
    if (typeof statusText === "string") statusEl.textContent = statusText;

    van.className = "van";
    root.classList.remove("job-complete");

    if (clamped < 100) van.classList.add("moving");

    if (clamped < 25) van.classList.add("parsing");
    else if (clamped < 60) van.classList.add("building");
    else if (clamped < 95) van.classList.add("organizing");
    else van.classList.add("complete");

    if (clamped >= 100){
      root.classList.add("job-complete");
      van.classList.remove("moving");

      setTimeout(function(){
        if (typeof openVanOrganizer === "function") openVanOrganizer();
      }, 600);
    }
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
        nextLine = s.status === "running" ? "Running . . ." : s.status;
      }else{
        nextLine = "Working…";
      }

      var pct = 0;
      if(s.progress && typeof s.progress.pct !== "undefined") pct = parseInt(s.progress.pct, 10) || 0;
      setProgress(pct, nextLine);

      var stage = s.progress ? s.progress.stage : "";
      if(s.status === "done" || s.has_toc){
        // Cache-bust so mobile browsers don't show old files
        var bust = "v=" + Date.now();
        var nextUrl = s.has_toc ? s.toc_url : s.organizer_url;
        window.location.replace(nextUrl + "?" + bust);
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
    html = html.replace("overflow-x:visible", "overflow-x:auto")
    if "function scrollTotesToRight()" in html and "detectRtlScrollType" not in html:
        html = html.replace(
            "function scrollTotesToRight(){",
            "let rtlScrollType = null;\n\n"
            "function detectRtlScrollType(){\n"
            "  if(rtlScrollType) return rtlScrollType;\n"
            "  const probe = document.createElement(\"div\");\n"
            "  probe.dir = \"rtl\";\n"
            "  probe.style.width = \"100px\";\n"
            "  probe.style.height = \"100px\";\n"
            "  probe.style.overflow = \"scroll\";\n"
            "  probe.style.position = \"absolute\";\n"
            "  probe.style.top = \"-9999px\";\n"
            "  probe.style.visibility = \"hidden\";\n"
            "  const inner = document.createElement(\"div\");\n"
            "  inner.style.width = \"200px\";\n"
            "  inner.style.height = \"1px\";\n"
            "  probe.appendChild(inner);\n"
            "  document.body.appendChild(probe);\n"
            "  probe.scrollLeft = 0;\n"
            "  const start = probe.scrollLeft;\n"
            "  probe.scrollLeft = 1;\n"
            "  const after = probe.scrollLeft;\n"
            "  document.body.removeChild(probe);\n"
            "  if(start === 0 && after === 0){\n"
            "    rtlScrollType = \"negative\";\n"
            "  }else if(start === 0 && after === 1){\n"
            "    rtlScrollType = \"default\";\n"
            "  }else{\n"
            "    rtlScrollType = \"reverse\";\n"
            "  }\n"
            "  return rtlScrollType;\n"
            "}\n\n"
            "function setRtlAwareScrollLeft(el, logicalLeft){\n"
            "  const type = detectRtlScrollType();\n"
            "  if(type === \"default\"){ el.scrollLeft = logicalLeft; return; }\n"
            "  if(type === \"negative\"){ el.scrollLeft = -logicalLeft; return; }\n"
            "  el.scrollLeft = el.scrollWidth - el.clientWidth - logicalLeft;\n"
            "}\n\n"
            "function scrollTotesToRight(){",
        )
        html = html.replace(
            "    wrap.scrollLeft = maxScroll;",
            "    setRtlAwareScrollLeft(wrap, maxScroll);",
        )
    if "</style>" in html and "tab-align-patch" not in html:
        html = html.replace(
            "</style>",
            "/* tab-align-patch */"
            ".sectionRight{display:contents !important;}"
            ".tabsRow{grid-column:1 !important;grid-row:1 !important;justify-self:start !important;}"
            ".topCounts{grid-column:3 !important;grid-row:1 !important;justify-self:end !important;}"
            "</style>",
        )
    if "</style>" in html and "grid-height-patch" not in html:
        html = html.replace(
            "</style>",
            "/* grid-height-patch */"
            ".organizer-grid,"
            ".tote-grid,"
            ".cards-grid{"
            "height:auto !important;"
            "max-height:none !important;"
            "overflow:visible !important;"
            "}"
            "</style>",
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
html,body{{margin:0;padding:0;height:100%;background:#0b0f14;color:#e8eef6;font-family:system-ui,-apple-system,Segoe UI,Roboto,Arial,sans-serif;overflow:auto}}
body{{display:flex;flex-direction:column;height:100dvh}}
.banner{{position:relative;flex:0 0 auto;background:#0b0f14;border-bottom:1px solid #1c2a3a}}
.banner img{{display:block;width:100%;height:auto;max-height:160px;object-fit:contain;transition:max-height 220ms ease, opacity 220ms ease}}
.bannerMin .banner{{border-bottom:1px solid rgba(28,42,58,0.7);min-height:86px;overflow:hidden}}
.bannerMin .banner img{{max-height:none;height:100%;object-fit:cover;object-position:center bottom;opacity:0.95;transform:scale(1.12);transform-origin:center bottom}}
.wrap{{flex:1 1 auto;padding:0 calc(10px + env(safe-area-inset-right, 0px)) calc(10px + env(safe-area-inset-bottom, 0px)) calc(10px + env(safe-area-inset-left, 0px));min-height:0}}
iframe{{border:0; display:block; width:100%; height:100%}}

#bannerHUD{{
  position:absolute;
  inset:0;
  display:none;
  grid-template-columns:repeat(3, minmax(0, 1fr));
  align-items:center;
  gap:12px;
  padding:10px 14px;
}}
.bannerMin #bannerHUD{{ display:grid; padding:14px 16px; }}
#bannerHUD::before{{
  content:"";
  position:absolute;
  inset:0;
  background:rgba(0,0,0,.60);
  backdrop-filter: blur(2px);
}}
#bannerHUD > *{{ position:relative; z-index:1; pointer-events:auto; }}
.hudLeft{{ display:flex; gap:8px; z-index:1; justify-self:stretch; justify-content:flex-start; }}
.hudTab{{
  background:rgba(255,255,255,.08);
  border:1px solid rgba(255,255,255,.14);
  color:#eaf2ff;
  padding:6px 10px;
  border-radius:999px;
  font-weight:800;
  cursor:pointer;
  user-select:none;
}}
.hudTab.active{{
  background:rgba(255,255,255,.16);
  border-color:rgba(255,255,255,.28);
}}
.hudTitle{{
  display:flex;
  align-items:center;
  justify-content:center;
  font-size:32px;
  font-weight:900;
  color:#fff;
  white-space:nowrap;
  z-index:1;
  pointer-events:none;
}}
.hudRight{{ display:flex; gap:10px; z-index:1; justify-self:stretch; justify-content:flex-end; }}
.pill{{
  background:rgba(0,0,0,.35);
  border:1px solid rgba(255,255,255,.14);
  color:#eaf2ff;
  padding:6px 10px;
  border-radius:999px;
  font-weight:850;
  white-space:nowrap;
}}

@media (max-width: 900px){{
  .wrap{{padding:0}}
}}
@media (max-height: 560px){{
  .banner img{{max-height:110px}}
}}
@media (max-width: 600px) and (orientation: portrait){{
  .banner img{{max-height:130px}}
}}
@media (prefers-reduced-motion: reduce){{
  .banner img{{ transition:none; }}
}}
</style>
</head>
<body>
  <div class="banner">
    <img src="/banner.png" alt="Van organizer banner" />
    <div id="bannerHUD">
      <div class="hudLeft">
        <button class="hudTab" data-tab="bags_overflow">Bags + Overflow</button>
        <button class="hudTab" data-tab="bags">Bags</button>
        <button class="hudTab" data-tab="overflow">Overflow</button>
      </div>

      <div class="hudTitle" id="hudTitle">—</div>

      <div class="hudRight">
        <span class="pill" id="hudPill1">—</span>
        <span class="pill" id="hudPill2" style="display:none">—</span>
      </div>
    </div>
  </div>
  <div class="wrap">
    <iframe id="orgFrame" src="/job/{jid}/organizer_raw?v=1" scrolling="no"></iframe>
  </div>

<script>
(function () {{
  var frame = document.getElementById("orgFrame");
  var params = new URLSearchParams(window.location.search);
  var routeParam = params.get("route");
  // cache-bust iframe so it always pulls the newest organizer without manual refresh
  var src = "/job/{jid}/organizer_raw?v=" + Date.now();
  if(routeParam){{
    src += "&route=" + encodeURIComponent(routeParam);
  }}
  frame.src = src;
}})();
(function(){{
  var root = document.documentElement;
  setTimeout(function(){{
    root.classList.add("bannerMin");
  }}, 3000);

  var hudTitle = document.getElementById("hudTitle");
  var pill1 = document.getElementById("hudPill1");
  var pill2 = document.getElementById("hudPill2");
  var iframe = document.getElementById("orgFrame");

  document.querySelectorAll(".hudTab").forEach(function(btn){{
    btn.addEventListener("click", function(){{
      document.querySelectorAll(".hudTab").forEach(function(b){{ b.classList.remove("active"); }});
      btn.classList.add("active");
      if(iframe && iframe.contentWindow){{
        iframe.contentWindow.postMessage({{ type:"setTab", tab: btn.dataset.tab }}, "*");
      }}
    }});
  }});
  var defaultTab = document.querySelector('.hudTab[data-tab="bags_overflow"]');
  if(defaultTab) defaultTab.classList.add("active");

  window.addEventListener("message", function(ev){{
    var d = ev.data || {{}};
    if(d.type !== "routeMeta") return;

    if(hudTitle) hudTitle.textContent = d.title || "—";

    var bags = (d.bags !== undefined && d.bags !== null) ? d.bags : "—";
    var ov = (d.overflow !== undefined && d.overflow !== null) ? d.overflow : "—";
    if(pill1) pill1.textContent = bags + " bags • " + ov + " overflow";

    var hasExtra = (d.commercial !== undefined && d.commercial !== null) || (d.total !== undefined && d.total !== null);
    if(pill2){{
      if(hasExtra){{
        var c = (d.commercial !== undefined && d.commercial !== null) ? d.commercial : "—";
        var t = (d.total !== undefined && d.total !== null) ? d.total : "—";
        pill2.style.display = "";
        pill2.textContent = c + " commercial • " + t + " total";
      }}else{{
        pill2.style.display = "none";
      }}
    }}
  }});
}})();
</script>
</body>
</html>
""")


@app.get("/job/{jid}/toc-data")
def toc_data(jid: str):
    j = store.get(jid)
    if j.get("status") == "missing":
        return JSONResponse({"status": "missing"}, status_code=404)

    toc = j.get("toc") or {}
    routes = toc.get("routes") or []
    date_label = toc.get("date_label") or ""

    if not routes:
        return JSONResponse({"status": "not_ready"}, status_code=404)

    return JSONResponse(
        {
            "status": "ok",
            "date_label": date_label,
            "routes": routes,
            "route_count": len(routes),
            "wave_colors": toc.get("wave_colors") or {},
            "mismatch_count": toc.get("mismatch_count"),
        }
    )


@app.get("/job/{jid}/summary-data")
def summary_data(jid: str):
    j = store.get(jid)
    if j.get("status") == "missing":
        return JSONResponse({"status": "missing"}, status_code=404)

    summary = j.get("summary") or {}
    return JSONResponse(
        {
            "status": "ok",
            "mismatches": summary.get("mismatches") or [],
            "routes_over_30": summary.get("routes_over_30") or [],
            "routes_over_50_overflow": summary.get("routes_over_50_overflow") or [],
            "top10_heavy_totals": summary.get("top10_heavy_totals") or [],
            "top10_commercial": summary.get("top10_commercial") or [],
        }
    )


@app.get("/job/{jid}/verification", response_class=HTMLResponse)
def verification_page(jid: str):
    return HTMLResponse(f"""
<!doctype html>
<html>
<head>
<meta name="viewport" content="width=device-width, initial-scale=1"/>
<title>Verification Summary</title>
<style>
html, body{{
  height:100%;
  overflow:auto;
}}
body{{
  font-family:system-ui,-apple-system,Segoe UI,Roboto,Arial,sans-serif;
  margin:0;
  background:#0b0f14;
  color:#e8eef6;
}}
:root{{
  --r:22px;
  --glass:rgba(255,255,255,0.06);
  --glassBorder:rgba(255,255,255,0.10);
}}
.page{{
  min-height:100svh;
  display:flex;
  align-items:flex-start;
  justify-content:center;
  padding:36px 18px 28px;
  box-sizing:border-box;
}}
.card{{
  width:100%;
  max-width:1100px;
  background:rgba(10,16,26,0.55);
  border:1px solid var(--glassBorder);
  border-radius:var(--r);
  padding:22px;
  box-shadow:0 18px 45px rgba(0,0,0,0.35);
}}
.headerRow{{
  display:flex;
  align-items:center;
  justify-content:space-between;
  flex-wrap:wrap;
  gap:12px;
  margin-bottom:14px;
}}
.titleBlock{{
  display:flex;
  flex-direction:column;
  gap:4px;
}}
.title{{
  font-size:26px;
  font-weight:800;
  letter-spacing:0.6px;
}}
.subtitle{{
  font-size:14px;
  opacity:0.75;
}}
.backBtn{{
  display:inline-flex;
  align-items:center;
  justify-content:center;
  height:40px;
  padding:0 16px;
  border-radius:999px;
  border:1px solid rgba(255,255,255,0.18);
  color:#e8eef6;
  background:rgba(255,255,255,0.04);
  text-decoration:none;
  font-weight:700;
}}
.section{{
  margin-top:18px;
  border:1px solid rgba(255,255,255,0.08);
  border-radius:18px;
  padding:16px;
  background:rgba(255,255,255,0.03);
}}
.sectionTitle{{
  font-size:16px;
  letter-spacing:1px;
  text-transform:uppercase;
  font-weight:700;
  opacity:0.8;
  margin-bottom:12px;
}}
.routeList{{
  display:flex;
  flex-direction:column;
  gap:8px;
}}
.routeRow{{
  width:100%;
  display:flex;
  align-items:center;
  justify-content:space-between;
  gap:12px;
  background:rgba(255,255,255,0.04);
  border:1px solid rgba(255,255,255,0.08);
  border-radius:12px;
  padding:10px 12px;
  text-align:left;
  color:inherit;
  cursor:pointer;
}}
.routeRow:focus-visible{{
  outline:2px solid rgba(63,167,255,0.6);
  outline-offset:2px;
}}
.routeName{{
  font-weight:700;
}}
.routeMetric{{
  font-weight:700;
  opacity:0.8;
  white-space:nowrap;
}}
.emptyState{{
  font-size:13px;
  opacity:0.7;
}}
.alertRow{{
  color:#ffb4b4;
  border-color:rgba(248,113,113,0.4);
  background:rgba(248,113,113,0.08);
}}
</style>
</head>
<body>
  <div class="page">
    <div class="card">
      <div class="headerRow">
        <div class="titleBlock">
          <div class="title">Verification Summary</div>
          <div class="subtitle">Review routes with the heaviest counts and overflow.</div>
        </div>
        <a class="backBtn" href="/job/{jid}/toc">Back to routes</a>
      </div>

      <section class="section" id="verificationSection">
        <div class="sectionTitle">Verification</div>
        <div class="routeList" id="verificationList"></div>
      </section>

      <section class="section" id="bagsSection">
        <div class="sectionTitle">Routes with 30+ Bags</div>
        <div class="routeList" id="bagsList"></div>
        <div class="emptyState" id="bagsEmpty" hidden>No routes with 30+ bags.</div>
      </section>

      <section class="section" id="overflowSection">
        <div class="sectionTitle">Routes with 50+ Overflow</div>
        <div class="routeList" id="overflowList"></div>
        <div class="emptyState" id="overflowEmpty" hidden>No routes with 50+ overflow.</div>
      </section>

      <section class="section" id="totalSection">
        <div class="sectionTitle">Routes with Heaviest Package Counts</div>
        <div class="routeList" id="totalList"></div>
        <div class="emptyState" id="totalEmpty" hidden>No route totals available.</div>
      </section>

      <section class="section" id="commercialSection">
        <div class="sectionTitle">Routes with Heaviest Commercial</div>
        <div class="routeList" id="commercialList"></div>
        <div class="emptyState" id="commercialEmpty" hidden>No commercial counts available.</div>
      </section>
    </div>
  </div>

<script>
(function(){{
  var jid = "{jid}";
  var verificationList = document.getElementById("verificationList");
  var bagsSection = document.getElementById("bagsSection");
  var bagsList = document.getElementById("bagsList");
  var bagsEmpty = document.getElementById("bagsEmpty");
  var overflowSection = document.getElementById("overflowSection");
  var overflowList = document.getElementById("overflowList");
  var overflowEmpty = document.getElementById("overflowEmpty");
  var totalList = document.getElementById("totalList");
  var totalEmpty = document.getElementById("totalEmpty");
  var commercialList = document.getElementById("commercialList");
  var commercialEmpty = document.getElementById("commercialEmpty");

  function openRoute(routeName){{
    if(!routeName) return;
    var url = "/job/" + jid + "/organizer?route=" + encodeURIComponent(routeName);
    window.location.href = url;
  }}

  function makeRow(routeName, metric, isAlert){{
    var row = document.createElement("button");
    row.type = "button";
    row.className = "routeRow" + (isAlert ? " alertRow" : "");
    row.addEventListener("click", function(){{ openRoute(routeName); }});
    var nameSpan = document.createElement("span");
    nameSpan.className = "routeName";
    nameSpan.textContent = routeName || "Route";
    var metricSpan = document.createElement("span");
    metricSpan.className = "routeMetric";
    metricSpan.textContent = metric || "";
    row.appendChild(nameSpan);
    row.appendChild(metricSpan);
    return row;
  }}

  function renderVerification(mismatches){{
    verificationList.innerHTML = "";
    if(!mismatches || !mismatches.length){{
      verificationList.appendChild(makeRow("All routes verified", "No mismatches", false));
      return;
    }}
    mismatches.forEach(function(item){{
      var parts = [];
      if(item.overflow_mismatch){{
        parts.push("Overflow " + item.declared_overflow + "→" + item.computed_overflow);
      }}
      if(item.total_mismatch){{
        parts.push("Total " + item.declared_total + "→" + item.computed_total);
      }}
      var metric = parts.length ? parts.join(" | ") : "Mismatch";
      verificationList.appendChild(makeRow(item.title || "Route", metric, true));
    }});
  }}

  function renderList(listEl, items, metricLabel, emptyEl, sectionEl, hideWhenEmpty){{
    listEl.innerHTML = "";
    if(!items || !items.length){{
      if(hideWhenEmpty && sectionEl){{
        sectionEl.hidden = true;
        return;
      }}
      if(emptyEl) emptyEl.hidden = false;
      return;
    }}
    if(emptyEl) emptyEl.hidden = true;
    if(sectionEl) sectionEl.hidden = false;
    items.forEach(function(item){{
      listEl.appendChild(makeRow(item.route || item.title || "Route", item.metric || metricLabel(item), false));
    }});
  }}

  function adaptItems(items, formatter){{
    return (items || []).map(function(raw){{
      return formatter(raw);
    }});
  }}

  fetch("/job/" + jid + "/summary-data", {{ cache: "no-store" }})
    .then(function(r){{ return r.json(); }})
    .then(function(data){{
      if(!data || data.status !== "ok") return;

      renderVerification(data.mismatches || []);

      var bagsItems = adaptItems(data.routes_over_30, function(row){{
        return {{
          route: row[1],
          metric: row[0] + " bags"
        }};
      }});
      renderList(bagsList, bagsItems, function(item){{ return item.metric; }}, bagsEmpty, bagsSection, true);

      var overflowItems = adaptItems(data.routes_over_50_overflow, function(row){{
        return {{
          route: row[1],
          metric: row[0] + " overflow"
        }};
      }});
      renderList(overflowList, overflowItems, function(item){{ return item.metric; }}, overflowEmpty, overflowSection, true);

      var totalItems = adaptItems(data.top10_heavy_totals, function(row){{
        return {{
          route: row[1],
          metric: row[0] + " total"
        }};
      }});
      renderList(totalList, totalItems, function(item){{ return item.metric; }}, totalEmpty, null, false);

      var commercialItems = adaptItems(data.top10_commercial, function(row){{
        return {{
          route: row[1],
          metric: row[0] + " commercial"
        }};
      }});
      renderList(commercialList, commercialItems, function(item){{ return item.metric; }}, commercialEmpty, null, false);
    }})
    .catch(function(){{}});
}})();
</script>
</body>
</html>
""")


@app.get("/job/{jid}/toc", response_class=HTMLResponse)
def toc_page(jid: str):
    return HTMLResponse(f"""
<!doctype html>
<html>
<head>
<meta name="viewport" content="width=device-width, initial-scale=1"/>
<title>Route Sheets</title>
<style>
html, body{{
  height:100%;
  overflow:hidden;
}}
body{{
  font-family:system-ui,-apple-system,Segoe UI,Roboto,Arial,sans-serif;
  margin:0;
  background:#0b0f14;
  color:#e8eef6;
}}
:root{{
  --r:22px;
  --glass:rgba(255,255,255,0.06);
  --glassBorder:rgba(255,255,255,0.10);
}}
.uploadPage{{
  height:100svh;
  display:flex;
  align-items:center;
  justify-content:center;
  padding:36px 18px 18px;
  box-sizing:border-box;
}}
.heroWrap{{
  width:100%;
  max-width:1100px;
  margin:0 auto;
  display:flex;
  flex-direction:column;
  align-items:stretch;
  gap:0;
}}
.heroWrap > *{{width:100%;}}
.brandBanner{{
  display:block;
  width:100%;
  height:auto;
  object-fit:contain;
  object-position:center;
  box-sizing:border-box;
  border:1px solid var(--glassBorder);
  border-radius:var(--r) var(--r) 0 0;
  box-shadow:0 18px 45px rgba(0,0,0,0.40);
}}
.tagGlass{{
  width:100%;
  margin-top:-12px;
  padding:14px 0;
  background:rgba(255,255,255,0.06);
  border:1px solid rgba(255,255,255,0.10);
  backdrop-filter:blur(10px);
  -webkit-backdrop-filter:blur(10px);
  border-radius:0;
  box-shadow:0 16px 40px rgba(0,0,0,0.35);
}}
.taglineText{{
  text-align:center;
  letter-spacing:2px;
  font-size:13px;
  opacity:0.85;
}}
.heroWrap, .tagGlass, .uploadCard, .buildBtn{{
  box-sizing:border-box;
}}
.uploadCard{{
  width:100%;
  max-width:100%;
  background:rgba(10,16,26,0.55);
  border:1px solid var(--glassBorder);
  border-radius:0 0 var(--r) var(--r);
  padding:22px;
  margin-top:0;
  position:relative;
  box-shadow:0 18px 45px rgba(0,0,0,0.35);
}}
.tocHeader{{
  text-align:center;
  padding:6px 0 12px;
  display:flex;
  flex-direction:column;
  align-items:center;
  gap:6px;
}}
.tocMetaRow{{
  display:flex;
  align-items:center;
  justify-content:center;
  gap:10px;
}}
.tocTitle{{
  font-size:26px;
  font-weight:800;
  letter-spacing:1px;
}}
.tocDate{{
  margin-top:0;
  font-size:16px;
  opacity:0.85;
}}
.tocCount{{
  margin-top:0;
  font-size:12px;
  letter-spacing:1.4px;
  text-transform:uppercase;
  opacity:0.8;
  background:rgba(255,255,255,0.08);
  border:1px solid rgba(255,255,255,0.12);
  padding:4px 12px;
  border-radius:999px;
  color:inherit;
  font:inherit;
  line-height:1.2;
}}
.tocCount--button{{
  cursor:pointer;
  transition:opacity 0.2s ease, background 0.2s ease, border-color 0.2s ease;
}}
.mismatchIndicator{{
  display:inline-flex;
  align-items:center;
  justify-content:center;
  width:32px;
  height:32px;
  border-radius:999px;
  font-size:18px;
  font-weight:800;
  box-shadow:0 6px 16px rgba(0,0,0,0.18);
  position:absolute;
  top:16px;
  right:16px;
  cursor:pointer;
}}
.mismatchIndicator:focus-visible{{
  outline:2px solid rgba(255,255,255,0.7);
  outline-offset:2px;
}}
.mismatchIndicator--ok{{
  background:rgba(22, 185, 78, 0.16);
  border:1px solid rgba(22, 185, 78, 0.7);
  color:#16b94e;
}}
.mismatchIndicator--error{{
  background:rgba(248, 113, 113, 0.16);
  border:1px solid rgba(248, 113, 113, 0.7);
  color:#f87171;
}}
.tocCount--button:hover{{
  opacity:1;
  background:rgba(255,255,255,0.12);
  border-color:rgba(255,255,255,0.2);
}}
.tocCount--button:focus-visible{{
  outline:2px solid rgba(255,255,255,0.6);
  outline-offset:2px;
}}
.divider{{
  height:1px;
  background:rgba(255,255,255,0.12);
  margin:12px 0 18px;
}}
.selectRow{{
  display:flex;
  flex-direction:column;
  gap:8px;
  margin-bottom:14px;
  align-items:center;
}}
.selectRow--dual{{
  flex-direction:row;
  align-items:flex-end;
  gap:16px;
}}
.selectGroup{{
  display:flex;
  flex-direction:column;
  gap:8px;
  align-items:center;
  flex:1;
}}
.selectGroup[hidden]{{
  display:none;
}}
.selectLabel{{
  font-size:clamp(12px, 1.6vw, 13px);
  letter-spacing:1px;
  text-transform:uppercase;
  opacity:0.7;
  text-align:center;
}}
.selectInput{{
  height:46px;
  width:100%;
  max-width:420px;
  border-radius:12px;
  border:1px solid rgba(255,255,255,0.12);
  background:rgba(255,255,255,0.04);
  color:#e8eef6;
  padding:0 12px;
  font-size:clamp(14px, 2.1vw, 16px);
  font-weight:600;
  text-align:center;
}}
.selectInput option{{
  background:#0b0f14;
  color:#e8eef6;
}}
.selectInput option:disabled{{
  color:rgba(232,238,246,0.6);
}}
.selectInput:disabled{{
  opacity:0.5;
}}
.actionRow{{
  display:flex;
  flex-direction:column;
  gap:10px;
  margin-top:16px;
}}
.buildBtn{{
  height:clamp(48px, 7vh, 64px);
  font-size:clamp(16px, 2.4vh, 20px);
  border-radius:16px;
  border:0;
  background:#3fa7ff;
  color:#001018;
  font-weight:800;
  cursor:pointer;
}}
.buildBtn:disabled{{
  opacity:0.6;
  cursor:not-allowed;
}}
.secondaryBtn,
.ghostBtn{{
  display:inline-flex;
  align-items:center;
  justify-content:center;
  height:44px;
  border-radius:14px;
  text-decoration:none;
  font-weight:700;
  border:1px solid rgba(255,255,255,0.12);
  color:#e8eef6;
  background:rgba(255,255,255,0.04);
}}
.ghostBtn{{
  background:transparent;
}}
.statusLine{{
  margin-top:12px;
  font-size:13px;
  opacity:0.7;
  text-align:center;
}}
@media (orientation: landscape) and (max-height: 560px){{
  .uploadPage{{padding-top:8px; padding-bottom:calc(8px + env(safe-area-inset-bottom, 0px));}}
  .uploadCard{{padding:12px;}}
}}
@media (orientation: portrait) and (max-height: 560px){{
  html, body{{height:auto; min-height:100%; overflow:auto;}}
  .uploadPage{{height:auto; min-height:100svh; align-items:flex-start; padding-top:20px;}}
}}
</style>
</head>
<body>
  <div class="uploadPage">
    <div class="heroWrap">
      <img class="brandBanner" src="/banner.png" alt="Van Organizer Banner" />
      <div class="tagGlass">
        <div class="taglineText">OPTIMIZE YOUR ROUTE</div>
      </div>
      <div class="uploadCard">
        <div class="tocHeader">
          <div class="tocTitle" id="tocDate">Date</div>
          <div class="tocMetaRow">
            <button class="tocCount tocCount--button" id="tocCount" type="button" title="Open stacked PDF">0 Routes</button>
          </div>
        </div>
        <span class="mismatchIndicator mismatchIndicator--ok" id="mismatchIndicator" role="button" tabindex="0" title="No mismatches reported" hidden>✓</span>
        <div class="divider"></div>
        <div class="selectRow selectRow--dual">
          <div class="selectGroup">
            <label class="selectLabel" for="waveSelect">Wave</label>
            <select id="waveSelect" class="selectInput">
              <option value="">Loading…</option>
            </select>
          </div>
          <div class="selectGroup" id="routeGroup" hidden>
            <label class="selectLabel" for="routeSelect">Route</label>
            <select id="routeSelect" class="selectInput" disabled>
              <option value="">Select a wave first</option>
            </select>
          </div>
        </div>
        <div class="actionRow">
          <button class="buildBtn" id="openRoute" type="button" disabled>Open Route</button>
        </div>
        <div class="statusLine" id="statusLine">Loading table of contents…</div>
      </div>
    </div>
  </div>
<script>
(function(){{
  var jid = "{jid}";
  var waveSelect = document.getElementById("waveSelect");
  var routeSelect = document.getElementById("routeSelect");
  var routeGroup = document.getElementById("routeGroup");
  var openRoute = document.getElementById("openRoute");
  var tocDate = document.getElementById("tocDate");
  var tocCount = document.getElementById("tocCount");
  var mismatchIndicator = document.getElementById("mismatchIndicator");
  var statusLine = document.getElementById("statusLine");
  var groupedRoutes = {{}};
  var routeIndex = {{}};
  var waveColors = {{}};
  var stackedUrl = "/job/" + jid + "/download/STACKED.pdf";
  var verificationUrl = "/job/" + jid + "/verification";

  tocCount.addEventListener("click", function(){{
    window.open(stackedUrl, "_blank", "noopener");
  }});
  if(mismatchIndicator){{
    mismatchIndicator.addEventListener("click", function(){{
      window.location.href = verificationUrl;
    }});
    mismatchIndicator.addEventListener("keydown", function(event){{
      if(event.key === "Enter" || event.key === " "){{
        event.preventDefault();
        window.location.href = verificationUrl;
      }}
    }});
  }}

  function timeKey(timeLabel){{
    if(!timeLabel) return "";
    var match = String(timeLabel).match(/(\\d{{1,2}}):(\\d{{2}})/);
    if(!match) return "";
    var hh = match[1].padStart(2, "0");
    var mm = match[2];
    return hh + ":" + mm;
  }}

  function waveLabel(timeLabel){{
    var key = timeKey(timeLabel);
    return key ? "Wave: " + key : "Wave: ??:??";
  }}

  function setStatus(msg){{
    if(statusLine) statusLine.textContent = msg;
  }}

  function setMismatchIndicator(count){{
    if(!mismatchIndicator) return;
    if(typeof count !== "number"){{
      mismatchIndicator.hidden = true;
      return;
    }}
    mismatchIndicator.hidden = false;
    if(count === 0){{
      mismatchIndicator.textContent = "✓";
      mismatchIndicator.title = "No mismatches reported";
      mismatchIndicator.classList.remove("mismatchIndicator--error");
      mismatchIndicator.classList.add("mismatchIndicator--ok");
    }} else {{
      mismatchIndicator.textContent = "✕";
      mismatchIndicator.title = "Mismatches reported";
      mismatchIndicator.classList.remove("mismatchIndicator--ok");
      mismatchIndicator.classList.add("mismatchIndicator--error");
    }}
  }}

  function ordinalize(num){{
    var mod100 = num % 100;
    if(mod100 >= 11 && mod100 <= 13) return num + "th";
    switch(num % 10){{
      case 1: return num + "st";
      case 2: return num + "nd";
      case 3: return num + "rd";
      default: return num + "th";
    }}
  }}

  function populateWaves(){{
    waveSelect.innerHTML = "";
    var labels = Object.keys(groupedRoutes);
    if(!labels.length){{
      waveSelect.innerHTML = "<option value=''>No waves found</option>";
      waveSelect.disabled = true;
      return;
    }}
    labels.sort();
    waveSelect.appendChild(new Option("Select Wave", ""));
    labels.forEach(function(label, index){{
      var opt = new Option(ordinalize(index + 1) + " " + label, label);
      var key = label.replace("Wave: ", "");
      var color = waveColors[key];
      if(color){{
        opt.style.color = color;
        opt.dataset.color = color;
      }}
      waveSelect.appendChild(opt);
    }});
    waveSelect.disabled = false;
  }}

  function populateRoutes(label){{
    routeSelect.innerHTML = "";
    openRoute.disabled = true;
    if(!label || !groupedRoutes[label]){{
      routeSelect.appendChild(new Option("Select a wave first", ""));
      routeSelect.disabled = true;
      if(routeGroup) routeGroup.hidden = true;
      return;
    }}
    if(routeGroup) routeGroup.hidden = false;
    routeSelect.disabled = false;
    routeSelect.appendChild(new Option("Select route", ""));
    var waveColor = waveColors[label.replace("Wave: ", "")] || "";
    groupedRoutes[label].forEach(function(route){{
      var opt = new Option(route.title, route.key);
      if(waveColor){{
        opt.style.color = waveColor;
      }}
      routeSelect.appendChild(opt);
    }});
  }}

  function applyWaveColor(){{
    var selected = waveSelect.options[waveSelect.selectedIndex];
    var color = selected && selected.dataset ? selected.dataset.color : "";
    waveSelect.style.color = color || "";
    routeSelect.style.color = color || "";
  }}

  waveSelect.addEventListener("change", function(){{
    populateRoutes(waveSelect.value);
    applyWaveColor();
  }});

  routeSelect.addEventListener("change", function(){{
    openRoute.disabled = !routeSelect.value;
  }});

  openRoute.addEventListener("click", function(){{
    var key = routeSelect.value;
    if(!key || !routeIndex[key]) return;
    var routeTitle = routeIndex[key].title || "";
    var url = "/job/" + jid + "/organizer?route=" + encodeURIComponent(routeTitle);
    window.location.href = url;
  }});

  fetch("/job/" + jid + "/toc-data", {{ cache: "no-store" }})
    .then(function(r){{ return r.json(); }})
    .then(function(data){{
      if(!data || data.status !== "ok"){{
        setStatus("Table of contents not ready yet.");
        return;
      }}
      tocDate.textContent = data.date_label || "Date";
      var n = data.route_count ?? 0;
      tocCount.textContent = n + " Route" + (n === 1 ? "" : "s");
      waveColors = data.wave_colors ?? {{}};
      setMismatchIndicator(data.mismatch_count);

      var routes = data.routes || [];
      groupedRoutes = {{}};
      routeIndex = {{}};

      routes.forEach(function(route, idx){{
        var label = waveLabel(route.time_label || "");
        var timeKeyValue = timeKey(route.time_label || "");
        var key = label + "::" + route.title + "::" + idx;
        var payload = {{
          title: route.title || "Route",
          output_page: route.output_page || 1,
          time_label: route.time_label || "",
          time_key: timeKeyValue,
          key: key
        }};
        if(!groupedRoutes[label]) groupedRoutes[label] = [];
        groupedRoutes[label].push(payload);
        routeIndex[key] = payload;
      }});

      Object.keys(groupedRoutes).forEach(function(label){{
        groupedRoutes[label].sort(function(a, b){{
          var numberA = String(a.title || "").match(/(\d+)/);
          var numberB = String(b.title || "").match(/(\d+)/);
          if(numberA && numberB){{
            var intA = parseInt(numberA[1], 10);
            var intB = parseInt(numberB[1], 10);
            if(intA !== intB) return intA - intB;
          }}
          return String(a.title || "").localeCompare(String(b.title || ""), undefined, {{ numeric: true }});
        }});
      }});

      populateWaves();
      setStatus("");
    }})
    .catch(function(){{
      setStatus("Unable to load table of contents.");
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
