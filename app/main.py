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
  min-height:100svh;
  min-height:100dvh;
  height:auto;
  display:flex;
  align-items:center;
  justify-content:center;
  padding:calc(36px + env(safe-area-inset-top, 0px)) 18px calc(18px + env(safe-area-inset-bottom, 0px));
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
  display:flex;
  align-items:center;
  justify-content:center;
  height:40px;
  padding:0 48px;
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
  position:absolute;
  left:50%;
  transform:translateX(-50%);
  font-size:clamp(10px, 1.6vh, 12px);
  max-width:calc(100% - 96px);
  white-space:nowrap;
  overflow:hidden;
  text-overflow:ellipsis;
  pointer-events:none;
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
  display:flex;
  align-items:center;
  justify-content:center;
  width:100%;
  height:100%;
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
@media (min-width: 768px){
  .uploadBtn{
    height:clamp(56px, 8vh, 72px);
  }
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
  display:flex;
  align-items:center;
  justify-content:center;
  line-height:1;
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
    overflow:auto;
  }
  .brandBanner{
    height:auto;
    max-height:120px;
    object-fit:contain;
    object-position:center;
  }
  .uploadPage{
    min-height:100svh;
    min-height:100dvh;
    height:auto;
    align-items:center;
    padding-top:calc(6px + env(safe-area-inset-top, 0px));
    padding-bottom:calc(6px + env(safe-area-inset-bottom, 0px));
  }
  .heroWrap{
    width:100%;
  }
  .tagGlass{
    padding:6px 0;
  }
  .uploadCard{
    padding:12px;
  }
  form{
    gap:8px;
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
    min-height:100dvh;
    align-items:flex-start;
    padding-top:calc(20px + env(safe-area-inset-top, 0px));
    padding-bottom:calc(18px + env(safe-area-inset-bottom, 0px));
  }
}
@media (max-width: 480px){
  html, body{
    height:100%;
  }
  .uploadPage{
    min-height:100svh;
    min-height:100dvh;
    display:flex;
    flex-direction:column;
    align-items:center;
    justify-content:center;
    padding:16px;
    padding-left:max(16px, env(safe-area-inset-left));
    padding-right:max(16px, env(safe-area-inset-right));
    padding-top:calc(16px + env(safe-area-inset-top, 0px));
    padding-bottom:calc(16px + env(safe-area-inset-bottom, 0px));
    box-sizing:border-box;
  }
  .heroWrap{
    min-height:100svh;
    min-height:100dvh;
    display:flex;
    flex-direction:column;
    justify-content:center;
    align-items:center;
    width:100% !important;
    max-width:390px;
    margin:0 auto;
    box-sizing:border-box;
    transform:translateY(-14px);
  }
  .brandBanner{
    width:100%;
    max-width:100%;
    max-height:54px;
    height:auto;
    object-fit:contain;
    display:block;
    margin:0 auto;
  }
  .tagGlass{
    padding:4px 0;
    margin-top:-6px;
    background:rgba(255,255,255,0.05);
  }
  .taglineText{
    letter-spacing:1.2px;
    font-size:12px;
    line-height:1.2;
    margin-top:4px;
    margin-bottom:0;
    opacity:0.8;
  }
  .uploadCard{
    width:100%;
    max-width:390px;
    padding-top:12px;
    margin-top:0;
    margin-bottom:0;
  }
  form{
    gap:10px;
  }
  .fileRow{
    margin-top:10px;
    margin-bottom:6px;
  }
  .buildBtn{
    font-size:18px;
    line-height:1.1;
    padding-top:12px;
    padding-bottom:12px;
    border-radius:18px;
    font-weight:600;
  }
  .uploadPage,
  .heroWrap,
  .uploadCard,
  .brandBanner{
    width:100% !important;
  }
  .uploadCard *,
  .heroWrap *{
    max-width:100%;
    box-sizing:border-box;
  }
  .fileRow{
    display:flex;
    align-items:center;
    gap:10px;
  }
  .fileBtn{
    display:flex;
    align-items:center;
    justify-content:flex-start;
    flex-shrink:0;
  }
  .uploadText{
    max-width:calc(100% - 96px);
  }
  .fileIcon,
  .plusIcon{
    flex-shrink:0;
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

    progress_percent, stage_text = store.compute_progress_percent(jid)
    progress = j.get("progress") or {}
    last_reported = progress.get("last_reported_percent") or 0
    if progress_percent > last_reported and j.get("status") != "done":
        store.set_progress(jid, {"last_reported_percent": progress_percent})

    job_dir = store.path(jid)
    out_pdf = job_dir / "STACKED.pdf"
    out_xlsx = job_dir / "Bags_with_Overflow.xlsx"
    out_html = job_dir / "van_organizer.html"

    return {
        "status": j.get("status", ""),
        "done": j.get("status") == "done",
        "error": j.get("error"),
        "progress": j.get("progress") or {},
        "progress_percent": progress_percent,
        "stage_text": stage_text,
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
    pct, stage_text = store.compute_progress_percent(jid)
    pct = max(0, min(100, pct))

    status_line = stage_text
    if status == "error":
        status_line = str(j.get("error") or "Error")

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
  transform:translate3d(-50%,-80%,0);
  transition:left .35s ease;
  filter:drop-shadow(0 6px 14px rgba(0,0,0,.45));
  will-change:left, transform;
  backface-visibility:hidden;
  pointer-events:none;
}
.van img{
  width:64px;
  height:auto;
  display:block;
  filter:drop-shadow(0 0 10px rgba(255,152,0,.85));
  transform:translateZ(0);
  backface-visibility:hidden;
}
.van.moving img{ animation:vanBob 1.2s ease-in-out infinite; }
@keyframes vanBob{ 0%,100%{ transform:translate3d(0,0,0); } 50%{ transform:translate3d(0,-2px,0); } }
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
@media (max-width: 480px){
  .page{
    padding-left:max(16px, env(safe-area-inset-left));
    padding-right:max(16px, env(safe-area-inset-right));
    padding-top:16px;
    box-sizing:border-box;
  }
  .card{
    width:100%;
    max-width:520px;
    margin:0 auto;
  }
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
  var doneHandled = false;
  var minLeftPct = 0;
  var maxLeftPct = 100;

  function updateVanBounds(){
    var road = root.querySelector(".road");
    if (!road || !van) return;
    var roadWidth = road.getBoundingClientRect().width || 0;
    var vanWidth = van.getBoundingClientRect().width || 0;
    if (roadWidth <= 0 || vanWidth <= 0) return;
    var halfPct = (vanWidth / roadWidth) * 50;
    minLeftPct = halfPct;
    maxLeftPct = 100 - halfPct;
  }

  function setProgress(pct, statusText){
    updateVanBounds();
    var rawPct = Number(pct) || 0;
    var displayPct = Math.max(0, Math.min(100, rawPct));
    var positionPct = minLeftPct + ((maxLeftPct - minLeftPct) * (displayPct / 100));
    van.style.left = positionPct + "%";
    pctEl.textContent = displayPct.toFixed(0) + "%";
    if (typeof statusText === "string") statusEl.textContent = statusText;

    van.className = "van";
    root.classList.remove("job-complete");

    if (displayPct < 100) van.classList.add("moving");

    if (displayPct < 25) van.classList.add("parsing");
    else if (displayPct < 60) van.classList.add("building");
    else if (displayPct < 95) van.classList.add("organizing");
    else van.classList.add("complete");

    if (displayPct >= 100){
      root.classList.add("job-complete");
      van.classList.remove("moving");
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

      var nextLine = s.stage_text || (s.progress && s.progress.msg) || "Working…";
      var pct = Number(s.progress_percent || 0);
      setProgress(pct, nextLine);

      if((s.done || s.status === "done" || s.has_toc) && !doneHandled){
        doneHandled = true;
        van.style.transition = "left 0.5s ease";
        setProgress(100, "Done");
        // Cache-bust so mobile browsers don't show old files
        var bust = "v=" + Date.now();
        var nextUrl = s.has_toc ? s.toc_url : s.organizer_url;
        setTimeout(function(){
          window.location.replace(nextUrl + "?" + bust);
        }, 500);
        clearInterval(timer);
      } else if(s.status === "error"){
        showErr(s.error);
        clearInterval(timer);
      }
    }catch(e){
      // Ignore transient network errors; next poll will recover.
    }
  }

  updateVanBounds();
  window.addEventListener("resize", updateVanBounds);
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
    if "</style>" in html and "grid-width-fit-patch" not in html:
        html = html.replace(
            "</style>",
            "/* grid-width-fit-patch */"
            ".organizer-grid,"
            ".tote-grid,"
            ".cards-grid{"
            "width:100% !important;"
            "grid-template-columns:repeat(auto-fit, minmax(160px, 1fr)) !important;"
            "grid-auto-columns:minmax(160px, 1fr) !important;"
            "grid-auto-rows:minmax(160px, auto) !important;"
            "align-items:stretch !important;"
            "justify-items:stretch !important;"
            "box-sizing:border-box;"
            "padding-inline:4px;"
            "}"
            ".organizer-grid > *,"
            ".tote-grid > *,"
            ".cards-grid > *{"
            "min-width:0 !important;"
            "min-height:0 !important;"
            "width:100% !important;"
            "height:100% !important;"
            "box-sizing:border-box;"
            "}"
            "</style>",
        )
    if "</style>" in html and "grid-right-fit-patch" not in html:
        html = html.replace(
            "</style>",
            "/* grid-right-fit-patch */"
            ".toteWrap,"
            ".cardsWrap,"
            ".organizer-grid,"
            ".tote-grid{"
            "position:relative;"
            "overflow-x:hidden !important;"
            "overflow-y:hidden !important;"
            "}"
            ".toteGridFrame .toteWrap{"
            "overflow:hidden !important;"
            "}"
            "</style>",
        )
    if "</style>" in html and "footer-counts-spacing-patch" not in html:
        html = html.replace(
            "</style>",
            "/* footer-counts-spacing-patch */"
            ".footerCounts{"
            "padding-bottom:6px;"
            "}"
            "</style>",
        )
    if "</style>" in html and "card-spacing-patch" not in html:
        html = html.replace(
            "</style>",
            "/* card-spacing-patch */"
            ":where(.cards-grid,.tote-grid) :where(.card,.cell){"
            "  padding-bottom:14px !important;"
            "}"
            ":where(.cards-grid,.tote-grid) :where(.card,.cell)>:last-child{"
            "  margin-top:8px !important;"
            "  margin-bottom:2px !important;"
            "  line-height:1.25 !important;"
            "}"
            "</style>",
        )
    if "</body>" in html and "combined-search-patch" not in html:
        html = html.replace(
            "</body>",
            "<style>"
            "/* combined-search-patch */"
            ".card[hidden], [data-card][hidden], .toteCard[hidden] { display: none !important; }"
            "</style>"
            "<script>"
            "/* combined-search-patch */"
            "(function(){"
            "  // --- Helpers ---"
            "  function $(sel, root){ return (root||document).querySelector(sel); }"
            "  function $all(sel, root){ return Array.prototype.slice.call((root||document).querySelectorAll(sel)); }"
            "  function norm(s){"
            "    return String(s||\"\").toLowerCase().replace(/\\s+/g,\" \").trim();"
            "  }"
            "  function buildSearchText(card){"
            "    return norm(collectSearchText(card));"
            "  }"
            ""
            "  // Find the search input & combined panel"
            "  function findSearchInput(){"
            "    return $('input[type=\"search\"]')"
            "        || $('input[placeholder*=\"Search\"]')"
            "        || $('input[placeholder*=\"bag\"]')"
            "        || $('input[placeholder*=\"overflow\"]');"
            "  }"
            "  function findCombinedCards(){"
            "    // Works across older/newer organizer builds"
            "    const roots = ["
            "      $('#combinedPanel'),"
            "      $('[data-panel=\"combined\"]'),"
            "      $('.combinedPanel')"
            "    ].filter(Boolean);"
            "    const cards = [];"
            "    roots.forEach(r => {"
            "      cards.push.apply(cards, $all('.card, [data-card], .toteCard', r));"
            "    });"
            "    return cards;"
            "  }"
            ""
            "  let cards = [];"
            "  let searchEl = null;"
            "  let indexedVersion = 0;"
            ""
            "  function collectSearchText(card){"
            "    const parts = [];"
            "    parts.push(card.getAttribute('data-search-source') || \"\");"
            "    parts.push(card.innerText || \"\");"
            "    parts.push(card.textContent || \"\");"
            "    const nodes = [card].concat($all('*', card));"
            "    nodes.forEach(node => {"
            "      if (node.dataset){"
            "        Object.keys(node.dataset).forEach(key => {"
            "          const val = node.dataset[key];"
            "          if (val) parts.push(val);"
            "        });"
            "      }"
            "      const title = node.getAttribute('title');"
            "      if (title) parts.push(title);"
            "      const aria = node.getAttribute('aria-label');"
            "      if (aria) parts.push(aria);"
            "    });"
            "    return parts.join(' ');"
            "  }"
            ""
            "  function buildIndex(){"
            "    cards = findCombinedCards();"
            "    cards.forEach(card => {"
            "      const text = buildSearchText(card);"
            "      card.dataset.search = text;"
            "    });"
            "    indexedVersion++;"
            "  }"
            ""
            "  function filterNow(){"
            "    if (!searchEl) return;"
            "    const q = norm(searchEl.value);"
            "    if (!q){"
            "      cards.forEach(c => c.hidden = false);"
            "      return;"
            "    }"
            "    const qTokens = q.split(/\\s+/).filter(Boolean);"
            "    cards.forEach(card => {"
            "      const hay = card.dataset.search || \"\";"
            "      // Must match ALL query tokens (AND semantics). Each token is substring match."
            "      const ok = qTokens.every(t => hay.indexOf(t) !== -1);"
            "      card.hidden = !ok;"
            "    });"
            "  }"
            ""
            "  function init(){"
            "    searchEl = findSearchInput();"
            "    if (!searchEl) return;"
            ""
            "    buildIndex();"
            "    filterNow();"
            ""
            "    // Re-index when the DOM changes (route/tab switches re-render)"
            "    const combinedRoot = $('#combinedPanel') || $('[data-panel=\"combined\"]') || document.body;"
            "    const mo = new MutationObserver(() => {"
            "      buildIndex();"
            "      filterNow();"
            "    });"
            "    mo.observe(combinedRoot, { childList: true, subtree: true });"
            ""
            "    // Hook to any existing render() the page might call"
            "    if (typeof window.render === 'function'){"
            "      const orig = window.render;"
            "      window.render = function(){"
            "        const r = orig.apply(this, arguments);"
            "        buildIndex();"
            "        filterNow();"
            "        return r;"
            "      };"
            "    }"
            ""
            "    searchEl.addEventListener('input', filterNow);"
            "  }"
            ""
            "  window.addEventListener('load', init);"
            "})();"
            "</script>"
            "</body>",
        )
    if "</body>" in html and "bags-footer-pill-filter-patch" not in html:
        html = html.replace(
            "</body>",
            "<script>"
            "/* bags-footer-pill-filter-patch */"
            "(function(){"
            "  function getActiveTab(){"
            "    var active = document.querySelector('.tab.active[data-tab]');"
            "    return active ? active.getAttribute('data-tab') : null;"
            "  }"
            "  function isTargetPill(pill, label){"
            "    if(!pill) return false;"
            "    var parts = ["
            "      pill.textContent,"
            "      pill.getAttribute('aria-label'),"
            "      pill.getAttribute('title'),"
            "      pill.id,"
            "      pill.className"
            "    ];"
            "    if(pill.dataset){"
            "      Object.keys(pill.dataset).forEach(function(key){"
            "        parts.push(pill.dataset[key]);"
            "      });"
            "    }"
            "    var hay = parts.filter(Boolean).join(' ').toLowerCase();"
            "    return hay.indexOf(label) !== -1;"
            "  }"
            "  function ensureLabel(pill, label){"
            "    if(!pill) return;"
            "    var text = (pill.textContent || '').trim();"
            "    if(text.toLowerCase().indexOf(label) !== -1) return;"
            "    var countText = text.replace(/\\s+/g, ' ').trim();"
            "    pill.textContent = countText ? (label.charAt(0).toUpperCase() + label.slice(1) + ' ' + countText) : (label.charAt(0).toUpperCase() + label.slice(1));"
            "  }"
            "  function findFooterPillEls(){"
            "    var footer = document.querySelector('.footerCounts') || document.querySelector('[data-footer-counts]');"
            "    var scope = footer || document;"
            "    var pills = Array.prototype.slice.call(scope.querySelectorAll('.pill'));"
            "    return pills.filter(function(pill){"
            "      return isTargetPill(pill, 'commercial') || isTargetPill(pill, 'packages');"
            "    });"
            "  }"
            "  function elementMatchesLabel(el, label){"
            "    if(!el) return false;"
            "    var parts = ["
            "      el.textContent,"
            "      el.getAttribute && el.getAttribute('aria-label'),"
            "      el.getAttribute && el.getAttribute('title'),"
            "      el.id,"
            "      el.className"
            "    ];"
            "    if(el.dataset){"
            "      Object.keys(el.dataset).forEach(function(key){"
            "        parts.push(el.dataset[key]);"
            "      });"
            "    }"
            "    var hay = parts.filter(Boolean).join(' ').toLowerCase();"
            "    return hay.indexOf(label) !== -1;"
            "  }"
            "  function columnHasTarget(column){"
            "    if(!column) return false;"
            "    if(elementMatchesLabel(column, 'commercial') || elementMatchesLabel(column, 'packages')) return true;"
            "    var descendants = Array.prototype.slice.call(column.querySelectorAll('*'));"
            "    return descendants.some(function(el){"
            "      return elementMatchesLabel(el, 'commercial') || elementMatchesLabel(el, 'packages');"
            "    });"
            "  }"
            "  function updateFooterPills(){"
            "    var activeTab = getActiveTab();"
            "    var shouldHide = activeTab === 'bags';"
            "    var pills = findFooterPillEls();"
            "    pills.forEach(function(pill){"
            "      if(shouldHide){"
            "        pill.setAttribute('hidden', 'hidden');"
            "        pill.setAttribute('aria-hidden', 'true');"
            "        pill.style.display = 'none';"
            "      }else{"
            "        pill.removeAttribute('hidden');"
            "        pill.removeAttribute('aria-hidden');"
            "        pill.style.removeProperty('display');"
            "        if(activeTab === 'combined'){"
            "          if(isTargetPill(pill, 'commercial')) ensureLabel(pill, 'commercial');"
            "          if(isTargetPill(pill, 'packages')) ensureLabel(pill, 'packages');"
            "        }"
            "      }"
            "    });"
            "    var footer = document.querySelector('.footerCounts') || document.querySelector('[data-footer-counts]');"
            "    if(footer){"
            "      var columns = Array.prototype.slice.call(footer.children);"
            "      columns.forEach(function(column){"
            "        if(columnHasTarget(column)){"
            "          if(shouldHide){"
            "            column.setAttribute('hidden', 'hidden');"
            "            column.setAttribute('aria-hidden', 'true');"
            "            column.style.display = 'none';"
            "          }else{"
            "            column.removeAttribute('hidden');"
            "            column.removeAttribute('aria-hidden');"
            "            column.style.removeProperty('display');"
            "          }"
            "        }"
            "      });"
            "    }"
            "  }"
            "  document.addEventListener('click', function(ev){"
            "    var target = ev.target;"
            "    if(target && target.classList && target.classList.contains('tab')){"
            "      setTimeout(updateFooterPills, 0);"
            "    }"
            "  });"
            "  var observer = new MutationObserver(function(){ updateFooterPills(); });"
            "  observer.observe(document.body, { subtree: true, childList: true, attributes: true, attributeFilter: ['class'] });"
            "  updateFooterPills();"
            "})();"
            "</script>"
            "</body>",
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
.bannerMin .banner img{{max-height:70px;opacity:0.95}}
.bannerMin .banner{{border-bottom:1px solid rgba(28,42,58,0.7);min-height:86px;display:flex;align-items:center}}
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
.bannerMin #bannerHUD{{ display:grid; padding:7px 16px; }}
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
.hudRight.stacked{{
  flex-direction:column;
  align-items:flex-end;
  gap:6px;
}}
.hudRight.stacked .pill{{
  width:var(--hud-pill-target-width, auto);
  padding:4px 10px;
  line-height:1.1;
  text-align:center;
}}
.pill{{
  --pill-bg: rgba(0,0,0,.35);
  display:inline-flex;
  align-items:center;
  justify-content:center;
  background:var(--pill-bg);
  border:1px solid rgba(255,255,255,.14);
  color:#eaf2ff;
  padding:6px 10px;
  border-radius:999px;
  font-weight:850;
  white-space:nowrap;
  text-align:center;
}}
.progressPill{{
  background:linear-gradient(90deg, var(--pill-fill) 0 var(--pill-progress, 0%), var(--pill-bg) var(--pill-progress, 0%));
}}
.pillBags{{ --pill-fill: rgba(126, 201, 255, 0.65); }}
.pillOverflow{{ --pill-fill: rgba(255, 186, 93, 0.7); }}

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
        <span class="pill progressPill pillBags" id="hudPillBags">—</span>
        <span class="pill progressPill pillOverflow" id="hudPillOverflow">—</span>
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
  var pillBags = document.getElementById("hudPillBags");
  var pillOverflow = document.getElementById("hudPillOverflow");
  var hudRight = document.querySelector(".hudRight");
  var hudWrap = document.getElementById("bannerHUD");
  var iframe = document.getElementById("orgFrame");
  var lastBags = null;
  var lastBagsLoaded = 0;
  var lastOverflow = null;
  var lastOverflowLoaded = 0;
  var lastFooterWidth = 0;
  var activeHudTab = "bags_overflow";

  document.querySelectorAll(".hudTab").forEach(function(btn){{
    btn.addEventListener("click", function(){{
      document.querySelectorAll(".hudTab").forEach(function(b){{ b.classList.remove("active"); }});
      btn.classList.add("active");
      activeHudTab = btn.dataset.tab || "bags_overflow";
      updateHudPillVisibility();
      if(iframe && iframe.contentWindow){{
        iframe.contentWindow.postMessage({{ type:"setTab", tab: btn.dataset.tab }}, "*");
      }}
    }});
  }});
  var defaultTab = document.querySelector('.hudTab[data-tab="bags_overflow"]');
  if(defaultTab) defaultTab.classList.add("active");
  updateHudPillVisibility();

  function shouldShowBags(){{
    return activeHudTab === "bags" || activeHudTab === "bags_overflow";
  }}

  function shouldShowOverflow(){{
    return activeHudTab === "overflow" || activeHudTab === "bags_overflow";
  }}

  function applyHudStacking(){{
    var selectedCount = parseInt(lastBagsLoaded || 0, 10);
    var footerWidth = parseInt(lastFooterWidth || 0, 10);
    var shouldStack = selectedCount > 0;
    var allowStack = activeHudTab === "bags" || activeHudTab === "bags_overflow";
    if(hudRight) hudRight.classList.toggle("stacked", shouldStack && allowStack);
    if(hudWrap){{
      if(shouldStack && allowStack && footerWidth > 0){{
        hudWrap.style.setProperty("--hud-pill-target-width", footerWidth + "px");
      }}else{{
        hudWrap.style.removeProperty("--hud-pill-target-width");
      }}
    }}
  }}

  function updateHudPillVisibility(){{
    if(pillBags) pillBags.style.display = shouldShowBags() ? "inline-flex" : "none";
    if(pillOverflow) pillOverflow.style.display = shouldShowOverflow() ? "inline-flex" : "none";
    applyHudStacking();
  }}

  window.addEventListener("message", function(ev){{
    var d = ev.data || {{}};
    if(d.type !== "routeMeta") return;

    if(hudTitle) hudTitle.textContent = d.title || "—";

    function formatProgress(total, selected, label){{
      if(total === undefined || total === null) return "—";
      var totalNum = parseInt(total, 10);
      if(Number.isNaN(totalNum)) return "—";
      var selectedNum = parseInt(selected, 10);
      if(Number.isNaN(selectedNum)) selectedNum = 0;
      var suffix = label ? " " + label : "";
      if(selectedNum <= 0){{
        return totalNum + suffix;
      }}
      var remaining = Math.max(totalNum - selectedNum, 0);
      return selectedNum + "/" + totalNum + suffix + " (" + remaining + " left)";
    }}

    function setPillProgress(pill, total, selected){{
      if(!pill) return;
      var totalNum = parseInt(total, 10);
      if(Number.isNaN(totalNum) || totalNum <= 0){{
        pill.style.setProperty("--pill-progress", "0%");
        return;
      }}
      var selectedNum = parseInt(selected, 10);
      if(Number.isNaN(selectedNum)) selectedNum = 0;
      var pct = Math.max(0, Math.min(selectedNum / totalNum, 1));
      pill.style.setProperty("--pill-progress", (pct * 100).toFixed(1) + "%");
    }}

    if(d.bags !== undefined && d.bags !== null) lastBags = d.bags;
    if(d.bags_loaded !== undefined && d.bags_loaded !== null) lastBagsLoaded = d.bags_loaded;
    if(d.overflow !== undefined && d.overflow !== null) lastOverflow = d.overflow;
    if(d.overflow_loaded !== undefined && d.overflow_loaded !== null) lastOverflowLoaded = d.overflow_loaded;
    if(d.footer_pill_width !== undefined && d.footer_pill_width !== null) lastFooterWidth = d.footer_pill_width;

    var bags = lastBags;
    var bagsLoaded = lastBagsLoaded;
    if(pillBags) pillBags.textContent = formatProgress(bags, bagsLoaded, "bags");
    setPillProgress(pillBags, bags, bagsLoaded);
    var overflow = lastOverflow;
    var overflowLoaded = lastOverflowLoaded;
    if(pillOverflow) pillOverflow.textContent = formatProgress(overflow, overflowLoaded, "overflow");
    setPillProgress(pillOverflow, overflow, overflowLoaded);
    applyHudStacking();

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
  padding:14px 12px;
  text-align:left;
  color:inherit;
  cursor:pointer;
  transition:background 0.2s ease, border-color 0.2s ease;
}}
.routeRow:hover{{
  background:rgba(255,255,255,0.08);
  border-color:rgba(255,255,255,0.16);
}}
.routeRow:active{{
  background:rgba(255,255,255,0.12);
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
  min-height:100dvh;
  height:auto;
  display:flex;
  align-items:stretch;
  justify-content:center;
  padding:calc(24px + env(safe-area-inset-top, 0px)) 18px calc(24px + env(safe-area-inset-bottom, 0px));
  box-sizing:border-box;
}}
.tocPage{{
  align-items:stretch;
}}
.heroWrap{{
  width:100%;
  max-width:1100px;
  margin:0 auto;
  display:flex;
  flex-direction:column;
  align-items:stretch;
  gap:0;
  flex:1;
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
  position:relative;
  z-index:1;
}}
.tocBanner{{
  position:relative;
  border-radius:var(--r);
  overflow:visible;
}}
.tocBanner .brandBanner{{
  border-radius:inherit;
}}
.tocDateBanner{{
  position:absolute;
  inset:0;
  display:grid;
  place-items:center;
  padding:10px 12px;
  min-height:52px;
  font-family:'Segoe UI','Inter','Helvetica Neue',Arial,sans-serif;
  text-transform:uppercase;
  color:#fff;
  border-radius:inherit;
  z-index:2;
}}
.tocDateBanner::before{{
  content:"";
  position:absolute;
  inset:0;
  background:rgba(0,0,0,0.6);
  backdrop-filter: blur(3px);
  border-radius:inherit;
}}
.tocDateText{{
  position:relative;
  z-index:1;
  white-space:nowrap;
  display:inline-block;
  font-size:19px;
  font-weight:800;
  letter-spacing:0.14em;
  transform-origin:center;
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
.taglineText--mobile{{
  display:none;
}}
.taglineText--desktop{{
  display:block;
}}
.heroWrap, .tagGlass, .uploadCard, .buildBtn{{
  box-sizing:border-box;
}}
.glassCard{{
  background:rgba(10,16,26,0.55);
  border:1px solid var(--glassBorder);
  box-shadow:0 18px 45px rgba(0,0,0,0.35);
}}
.glassField{{
  background:rgba(255,255,255,0.04);
  border:1px solid rgba(255,255,255,0.08);
  border-radius:14px;
}}
.uploadCard{{
  width:100%;
  max-width:100%;
  border-radius:0 0 var(--r) var(--r);
  padding:22px;
  margin-top:0;
  position:relative;
}}
.tocCard{{
  padding:0;
  border-radius:var(--r);
  overflow:hidden;
  display:grid;
  grid-template-rows:auto minmax(0, 1fr) auto;
  min-height:calc(100dvh - 96px);
}}
.tocTop{{
  padding:20px 22px 8px;
  display:flex;
  flex-direction:column;
  gap:6px;
}}
.metaRow{{
  display:flex;
  align-items:center;
  justify-content:flex-start;
  position:relative;
  min-height:28px;
  margin-top:10px;
}}
.tocStatusRow{{
  display:flex;
  align-items:center;
  justify-content:flex-start;
}}
.tocMiddle{{
  padding:0 22px;
  display:flex;
  flex-direction:column;
  align-items:center;
  justify-content:center;
  gap:8px;
}}
.tocMiddleInner{{
  width:100%;
  max-width:100%;
  width:min(560px, 100%);
  margin:0 auto;
  display:flex;
  flex-direction:column;
  gap:8px;
}}
.tocBottom{{
  padding:0 22px 22px;
}}
.tocBottom .actionRow{{
  margin-top:0;
}}
.tocCount{{
  margin-top:0;
  display:inline-flex;
  align-items:center;
  height:30px;
  padding:0 12px;
  border-radius:999px;
  background:#f39c12;
  border:1px solid rgba(243, 156, 18, 0.85);
  color:#111111;
  font-size:12px;
  font-weight:700;
  letter-spacing:0.08em;
  text-transform:uppercase;
  font-family:inherit;
  opacity:0.9;
  backdrop-filter:blur(8px);
  -webkit-backdrop-filter:blur(8px);
  box-shadow:0 12px 30px rgba(0,0,0,0.28);
}}
.tocCount--button{{
  cursor:pointer;
  transition:opacity 0.2s ease, background 0.2s ease, border-color 0.2s ease;
}}
.summaryPill{{
  display:inline-flex;
  align-items:center;
  gap:8px;
  height:30px;
  padding:0 12px;
  border-radius:999px;
  font-size:12px;
  font-weight:700;
  letter-spacing:0.08em;
  text-transform:uppercase;
  color:#e8eef6;
  cursor:pointer;
  transition:transform 0.15s ease, box-shadow 0.15s ease, background 0.2s ease;
  margin-left:auto;
}}
.summaryPill:active{{
  transform:translateY(1px) scale(0.98);
}}
.summaryPill:focus-visible{{
  outline:2px solid rgba(255,255,255,0.6);
  outline-offset:2px;
}}
.summaryBadge{{
  display:inline-flex;
  align-items:center;
  justify-content:center;
  width:16px;
  height:16px;
  border-radius:999px;
  font-size:10px;
  font-weight:800;
  box-shadow:0 6px 16px rgba(0,0,0,0.18);
}}
.summaryBadge--ok{{
  background:rgba(0, 0, 0, 0.12);
  border:1px solid #000000;
  color:#16b94e;
}}
.summaryBadge--error{{
  background:rgba(248, 113, 113, 0.16);
  border:1px solid rgba(248, 113, 113, 0.7);
  color:#f87171;
}}
.tocCount--button:hover{{
  opacity:1;
  background:rgba(243, 156, 18, 0.32);
  border-color:rgba(243, 156, 18, 0.7);
}}
.tocCount--button:focus-visible{{
  outline:2px solid rgba(255,255,255,0.6);
  outline-offset:2px;
}}
.tocSelectorsPanel{{
  width:100%;
  border-radius:18px;
  padding:0;
  min-height:200px;
  background:transparent;
  border:0;
  display:flex;
  flex-direction:column;
  justify-content:center;
  gap:12px;
  max-width:100%;
  box-sizing:border-box;
}}
.selectionCard{{
  width:100%;
  padding:20px;
  display:flex;
  flex-direction:column;
  gap:12px;
  box-sizing:border-box;
  border-radius:18px;
  background:rgba(255,255,255,0.047);
  border:1px solid rgba(255,255,255,0.07);
  box-shadow:0 10px 30px rgba(0,0,0,0.35);
  backdrop-filter:blur(14px);
  -webkit-backdrop-filter:blur(14px);
  transform:translateY(-12px);
}}
.fieldRow{{
  display:flex;
  flex-direction:column;
  gap:6px;
}}
.fieldRow[hidden]{{
  display:none;
}}
.fieldDivider{{
  height:1px;
  width:100%;
  background:rgba(255,255,255,0.12);
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
.selectRow--spaced{{
  margin-top:22px;
}}
.selectGroup{{
  display:flex;
  flex-direction:column;
  gap:6px;
  align-items:flex-start;
  flex:1;
}}
.selectGroup[hidden]{{
  display:none;
}}
.selectLabel{{
  font-size:12px;
  letter-spacing:0.12em;
  text-transform:uppercase;
  opacity:0.75;
  text-align:left;
  margin-left:4px;
}}
.selectInput{{
  height:46px;
  width:100%;
  max-width:100%;
  border-radius:14px;
  border:0;
  background:transparent;
  color:#e8eef6;
  padding:0 12px;
  font-size:clamp(14px, 2.1vw, 16px);
  font-weight:600;
  text-align:center;
  line-height:1;
}}
.selectInput--hidden{{
  position:absolute;
  width:1px;
  height:1px;
  padding:0;
  margin:-1px;
  overflow:hidden;
  clip:rect(0, 0, 0, 0);
  border:0;
  opacity:0;
  pointer-events:none;
}}
.customSelect{{
  position:relative;
  width:100%;
  max-width:100%;
}}
.customSelectControl{{
  width:100%;
  cursor:pointer;
  padding:0 44px 0 16px;
  position:relative;
  display:flex;
  align-items:center;
  justify-content:center;
}}
.fieldSurface{{
  background:rgba(255,255,255,0.06);
  border:1px solid rgba(255,255,255,0.08);
  border-radius:14px;
  display:flex;
  align-items:center;
}}
.fieldSurface[data-has-value="true"]{{
  border-color:var(--wave-border, rgba(255,255,255,0.12));
  background:linear-gradient(180deg, var(--wave-fill, rgba(255,255,255,0.16)) 0%, rgba(255,255,255,0.04) 100%);
  box-shadow:0 10px 24px rgba(0,0,0,0.28);
  transform:translateY(-1px);
}}
.customSelectControl::after{{
  content:"";
  position:absolute;
  right:18px;
  top:50%;
  transform:translateY(-50%);
  border-left:6px solid transparent;
  border-right:6px solid transparent;
  border-top:7px solid rgba(232,238,246,0.9);
}}
.customSelectControl:disabled{{
  opacity:0.5;
  cursor:default;
}}
.pickerBackdrop{{
  position:fixed;
  inset:0;
  background:rgba(8,12,18,0.5);
  backdrop-filter:blur(10px);
  -webkit-backdrop-filter:blur(10px);
  z-index:9000;
}}
.pickerBackdrop[hidden],
.pickerModal[hidden]{{
  display:none;
}}
.pickerModal{{
  position:fixed;
  top:50%;
  left:50%;
  transform:translate(-50%, -50%);
  width:min(92vw, 460px);
  background:rgba(12,16,24,0.95);
  border-radius:20px;
  border:1px solid rgba(255,255,255,0.16);
  box-shadow:0 24px 50px rgba(0,0,0,0.4);
  z-index:9001;
  display:flex;
  flex-direction:column;
  max-height:min(94svh, 680px);
  overflow:hidden;
}}
.pickerHeader{{
  display:flex;
  align-items:center;
  justify-content:space-between;
  height:clamp(40px, 7vh, 52px);
  padding:clamp(6px, 1.6vh, 12px) clamp(10px, 2.6vw, 16px);
  border-bottom:1px solid rgba(255,255,255,0.06);
}}
.pickerTitle{{
  font-weight:750;
  font-size:clamp(13px, 2.2vh, 17px);
  text-align:left;
}}
.pickerClose{{
  background:transparent;
  border:0;
  color:#e8eef6;
  font-size:clamp(15px, 2.4vh, 18px);
  cursor:pointer;
  padding:0;
  width:clamp(26px, 5.4vh, 32px);
  height:clamp(26px, 5.4vh, 32px);
  display:flex;
  align-items:center;
  justify-content:center;
}}
.pickerList{{
  max-height:none;
  overflow:hidden;
  -webkit-overflow-scrolling:touch;
  padding:clamp(4px, 1.2vh, 10px) clamp(6px, 2vw, 12px) clamp(6px, 1.6vh, 12px);
  scrollbar-color:rgba(116,136,168,0.6) rgba(255,255,255,0.06);
  scrollbar-width:thin;
  display:flex;
  flex-direction:column;
  align-items:stretch;
  overflow-x:hidden;
}}
.pickerList::-webkit-scrollbar{{
  width:8px;
}}
.pickerList::-webkit-scrollbar-track{{
  background:rgba(255,255,255,0.06);
  border-radius:999px;
}}
.pickerList::-webkit-scrollbar-thumb{{
  background:rgba(116,136,168,0.65);
  border-radius:999px;
  border:2px solid rgba(12,16,24,0.95);
}}
.pickerRow{{
  width:100%;
  border:0;
  background:transparent;
  color:#e8eef6;
  height:clamp(40px, 6vh, 52px);
  font-size:clamp(12px, 2vh, 16px);
  font-weight:600;
  cursor:pointer;
  text-align:center;
  display:flex;
  align-items:center;
  justify-content:center;
  padding:6px 12px;
  margin:clamp(2px, 0.5vh, 4px) clamp(4px, 1vw, 6px);
  border-radius:12px;
  pointer-events:auto;
  -webkit-tap-highlight-color:transparent;
  overflow:hidden;
  text-overflow:ellipsis;
}}
.pickerRow:hover{{
  background:rgba(255,255,255,0.08);
}}
.pickerRow:active{{
  background:rgba(255,255,255,0.14);
}}
.pickerRow--active{{
  background:rgba(255,255,255,0.12);
  font-weight:700;
}}
.pickerRow--disabled{{
  opacity:0.5;
  cursor:default;
}}
@media (hover:none){{
  .pickerRow:hover,
  .pickerRow:active,
  .pickerRow--active{{
    background:transparent;
  }}
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
  height:clamp(40px, 5.2vh, 50px);
  font-size:clamp(16px, 2.4vh, 20px);
  border-radius:18px;
  border:0;
  background:#3fa7ff;
  color:#001018;
  font-weight:800;
  cursor:pointer;
  box-shadow:0 12px 24px rgba(0,0,0,0.25);
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
  margin-top:10px;
  font-size:13px;
  opacity:0.7;
  text-align:center;
}}
@media (orientation: landscape) and (max-height: 560px){{
  html, body{{height:auto; min-height:100%; overflow:auto;}}
  .uploadPage{{height:auto; min-height:100svh; align-items:flex-start; padding-top:8px; padding-bottom:calc(8px + env(safe-area-inset-bottom, 0px));}}
  .uploadCard{{padding:12px;}}
  .tocCard{{min-height:0; height:auto;}}
}}
@media (orientation: portrait) and (max-height: 560px){{
  html, body{{height:auto; min-height:100%; overflow:auto;}}
  .uploadPage{{height:auto; min-height:100svh; align-items:flex-start; padding-top:20px;}}
}}
@media (max-width: 480px){{
  html, body{{height:auto; min-height:100%; overflow:auto;}}
  .tocPage{{
    min-height:100dvh;
    display:flex;
    justify-content:center;
    align-items:center;
    padding:12px;
    overflow-x:hidden;
  }}
  .heroWrap{{
    max-width:100%;
    margin:0;
  }}
  .tocCard{{
    width:min(94vw, 380px);
    max-height:calc(100dvh - 24px);
    display:flex;
    flex-direction:column;
    box-sizing:border-box;
    max-width:100%;
  }}
  .tocTop{{
    padding:12px 14px 8px;
    gap:8px;
    box-sizing:border-box;
    max-width:100%;
  }}
  .tocTop .bannerImg{{
    width:100%;
    max-height:120px;
    object-fit:cover;
    border-radius:12px;
  }}
  .tocTop .tocBanner{{
    border-radius:12px;
  }}
  .summaryPill{{
    height:28px;
    padding:0 10px;
    font-size:11px;
  }}
  .summaryBadge{{
    width:14px;
    height:14px;
    font-size:9px;
  }}
  .tocCount{{
    font-size:15px;
    letter-spacing:1.3px;
    padding:6px 14px;
  }}
  .tocMiddle{{
    flex:0;
    display:flex;
    flex-direction:column;
    justify-content:flex-start;
    padding:10px 14px;
    min-height:0;
    box-sizing:border-box;
    max-width:100%;
  }}
  .tocMiddleInner{{
    width:100%;
    max-width:100%;
    box-sizing:border-box;
    display:flex;
    flex-direction:column;
    gap:12px;
  }}
  .tocSelectorsPanel{{
    width:100%;
    border-radius:18px;
    padding:0;
    min-height:auto;
    background:transparent;
    border:0;
    display:flex;
    flex-direction:column;
    justify-content:flex-start;
    gap:12px;
    max-width:100%;
    box-sizing:border-box;
  }}
  .selectionCard{{
    width:100%;
    padding:16px 14px;
    display:flex;
    flex-direction:column;
    box-sizing:border-box;
  }}
  .fieldRow{{
    gap:6px;
  }}
  .selectGroup{{
    gap:6px;
    align-items:flex-start;
  }}
  .selectLabel{{
    font-size:11px;
    text-align:left;
    margin-bottom:0;
    letter-spacing:0.12em;
  }}
  .selectInput{{
    height:44px;
    font-size:14px;
    max-width:100%;
  }}
  .customSelect{{
    max-width:100%;
  }}
  .taglineText--desktop{{
    display:none;
  }}
  .taglineText--mobile{{
    display:block;
    font-weight:600;
    letter-spacing:0.6px;
  }}
  .tocBottom{{
    padding:14px 14px calc(14px + env(safe-area-inset-bottom));
    border-top:1px solid rgba(255,255,255,0.06);
    box-sizing:border-box;
    max-width:100%;
  }}
  .tocBottom button{{
    width:100%;
    height:48px;
  }}
  .statusLine{{
    margin-top:4px;
  }}
}}
</style>
</head>
<body>
  <div class="uploadPage tocPage">
    <div class="heroWrap">
      <div class="uploadCard tocCard glassCard">
        <div class="tocTop">
          <div class="tocBanner">
            <img class="brandBanner bannerImg" src="/banner.png" alt="Van Organizer Banner" />
            <div class="tocDateBanner" id="tocDateBanner"><span class="tocDateText">Date</span></div>
          </div>
          <div class="metaRow">
            <button class="tocCount tocCount--button glassField" id="tocCount" type="button" title="Open stacked PDF">0 Routes</button>
            <button class="summaryPill glassField" id="summaryPill" type="button" title="View verification summary">
              <span class="summaryLabel">Summary</span>
              <span class="summaryBadge summaryBadge--ok" id="summaryBadge" aria-hidden="true">✓</span>
            </button>
          </div>
        </div>
        <div class="tocMiddle">
          <div class="tocMiddleInner">
            <div class="tocSelectorsPanel">
              <div class="selectionCard glassCard">
                <div class="fieldRow">
                  <div class="selectGroup">
                    <label class="selectLabel" for="waveSelect">Wave</label>
                    <div class="customSelect" id="waveDropdown">
                      <button class="selectInput customSelectControl fieldSurface" id="waveControl" type="button" aria-expanded="false">Loading…</button>
                    </div>
                    <select id="waveSelect" class="selectInput selectInput--hidden" aria-hidden="true" tabindex="-1">
                      <option value="">Loading…</option>
                    </select>
                  </div>
                </div>
                <div class="fieldDivider" id="routeDivider" aria-hidden="true" hidden></div>
                <div class="fieldRow" id="routeRow" hidden>
                  <div class="selectGroup">
                    <label class="selectLabel" for="routeSelect">Route</label>
                    <div class="customSelect" id="routeDropdown">
                      <button class="selectInput customSelectControl fieldSurface" id="routeControl" type="button" aria-expanded="false" disabled>Select route</button>
                    </div>
                    <select id="routeSelect" class="selectInput selectInput--hidden" aria-hidden="true" tabindex="-1" disabled>
                      <option value="">Select a wave first</option>
                    </select>
                  </div>
                </div>
              </div>
              <div class="statusLine" id="statusLine">Loading table of contents…</div>
            </div>
          </div>
        </div>
        <div class="tocBottom">
          <div class="actionRow">
            <button class="buildBtn" id="openRoute" type="button" disabled>Open Route</button>
          </div>
        </div>
      </div>
    </div>
  </div>
  <div id="pickerBackdrop" class="pickerBackdrop" hidden></div>
  <div id="pickerModal" class="pickerModal" role="dialog" aria-modal="true" hidden>
    <div class="pickerHeader">
      <div id="pickerTitle" class="pickerTitle">Select</div>
      <button id="pickerClose" class="pickerClose" type="button" aria-label="Close">✕</button>
    </div>
    <div id="pickerList" class="pickerList" role="listbox"></div>
  </div>
<script>
(function(){{
  var jid = "{jid}";
  var waveSelect = document.getElementById("waveSelect");
  var waveControl = document.getElementById("waveControl");
  var routeControl = document.getElementById("routeControl");
  var routeSelect = document.getElementById("routeSelect");
  var routeRow = document.getElementById("routeRow");
  var routeDivider = document.getElementById("routeDivider");
  var openRoute = document.getElementById("openRoute");
  var tocCount = document.getElementById("tocCount");
  var tocDateBanner = document.getElementById("tocDateBanner");
  var tocDateText = tocDateBanner ? tocDateBanner.querySelector(".tocDateText") : null;
  var tocDateObserver = null;
  var summaryPill = document.getElementById("summaryPill");
  var summaryBadge = document.getElementById("summaryBadge");
  var statusLine = document.getElementById("statusLine");
  var pickerBackdrop = document.getElementById("pickerBackdrop");
  var pickerModal = document.getElementById("pickerModal");
  var pickerTitle = document.getElementById("pickerTitle");
  var pickerClose = document.getElementById("pickerClose");
  var pickerList = document.getElementById("pickerList");
  var mobileMedia = window.matchMedia ? window.matchMedia("(max-width: 600px) and (orientation: portrait)") : null;
  var groupedRoutes = {{}};
  var routeIndex = {{}};
  var waveColors = {{}};
  var stackedUrl = "/job/" + jid + "/download/STACKED.pdf";
  var summaryUrl = "/job/" + jid + "/verification";

  tocCount.addEventListener("click", function(){{
    window.open(stackedUrl, "_blank", "noopener");
  }});
  if(summaryPill){{
    summaryPill.addEventListener("click", function(){{
      window.location.href = summaryUrl;
    }});
    summaryPill.addEventListener("keydown", function(event){{
      if(event.key === "Enter" || event.key === " "){{
        event.preventDefault();
        window.location.href = summaryUrl;
      }}
    }});
  }}

  function updateDateScale(){{
    if(!tocDateBanner || !tocDateText) return;
    var bannerStyle = window.getComputedStyle(tocDateBanner);
    var paddingLeft = parseFloat(bannerStyle.paddingLeft) || 0;
    var paddingRight = parseFloat(bannerStyle.paddingRight) || 0;
    var availableWidth = tocDateBanner.clientWidth - paddingLeft - paddingRight;
    var textWidth = tocDateText.scrollWidth;
    if(!availableWidth || !textWidth) return;
    var scale = Math.min(1, Math.max(0.72, availableWidth / textWidth));
    tocDateText.style.transform = "scale(" + scale.toFixed(3) + ")";
  }}

  function setupDateObserver(){{
    if(!tocDateBanner || !tocDateText || typeof ResizeObserver === "undefined") return;
    if(tocDateObserver) tocDateObserver.disconnect();
    tocDateObserver = new ResizeObserver(function(){{
      updateDateScale();
    }});
    tocDateObserver.observe(tocDateBanner);
    tocDateObserver.observe(tocDateText);
  }}

  setupDateObserver();
  updateDateScale();
  if(document.fonts && document.fonts.ready){{
    document.fonts.ready.then(function(){{
      updateDateScale();
    }});
  }}

  function timeKey(timeLabel){{
    if(!timeLabel) return "";
    var match = String(timeLabel).match(/(\\d{{1,2}})\\s*[:.]\\s*(\\d{{2}})\\s*([AaPp])?\\s*([Mm])?/);
    if(!match) return "";
    var hh = parseInt(match[1], 10);
    var mm = match[2];
    var ampm = "";
    if(match[3] && match[4]){{
      ampm = (match[3] + match[4]).toUpperCase();
    }}
    if(ampm === "PM" && hh !== 12){{
      hh += 12;
    }}
    if(ampm === "AM" && hh === 12){{
      hh = 0;
    }}
    return String(hh).padStart(2, "0") + ":" + mm;
  }}

  function waveLabel(timeLabel){{
    var key = timeKey(timeLabel);
    return key ? "Wave: " + key : "Wave: ??:??";
  }}

  function setStatus(msg){{
    if(statusLine) statusLine.textContent = msg;
  }}

  function isVerticalMobile(){{
    if(mobileMedia) return mobileMedia.matches;
    return window.innerWidth <= 600 && window.innerHeight >= window.innerWidth;
  }}

  function setRouteGroupVisibility(hasWave){{
    if(!routeRow) return;
    if(hasWave){{
      routeRow.hidden = false;
      if(routeDivider) routeDivider.hidden = false;
      return;
    }}
    var hideRow = !isVerticalMobile();
    routeRow.hidden = hideRow;
    if(routeDivider) routeDivider.hidden = hideRow;
  }}

  function setMismatchIndicator(count){{
    if(!summaryPill || !summaryBadge) return;
    if(typeof count !== "number"){{
      summaryPill.hidden = false;
      summaryPill.title = "View verification summary";
      summaryBadge.textContent = "✓";
      summaryBadge.hidden = false;
      summaryBadge.classList.remove("summaryBadge--error");
      summaryBadge.classList.add("summaryBadge--ok");
      return;
    }}
    if(count === 0){{
      summaryPill.title = "No mismatches reported";
      summaryBadge.textContent = "✓";
      summaryBadge.hidden = false;
      summaryBadge.classList.remove("summaryBadge--error");
      summaryBadge.classList.add("summaryBadge--ok");
    }} else {{
      summaryPill.title = "Mismatches reported";
      summaryBadge.textContent = "!";
      summaryBadge.hidden = false;
      summaryBadge.classList.remove("summaryBadge--ok");
      summaryBadge.classList.add("summaryBadge--error");
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

  function getSelectedWaveColor(){{
    var selected = waveSelect.options[waveSelect.selectedIndex];
    return selected && selected.dataset ? selected.dataset.color : "";
  }}

  function toRgba(color, alpha){{
    if(!color) return "";
    var trimmed = String(color).trim();
    var hex = trimmed.match(/^#([0-9a-f]{{3}}|[0-9a-f]{{6}})$/i);
    if(hex){{
      var value = hex[1];
      if(value.length === 3){{
        value = value.split("").map(function(ch){{ return ch + ch; }}).join("");
      }}
      var r = parseInt(value.slice(0, 2), 16);
      var g = parseInt(value.slice(2, 4), 16);
      var b = parseInt(value.slice(4, 6), 16);
      return "rgba(" + r + ", " + g + ", " + b + ", " + alpha + ")";
    }}
    var rgb = trimmed.match(/^rgba?\\(([^)]+)\\)$/i);
    if(rgb){{
      var parts = rgb[1].split(",").map(function(part){{ return parseFloat(part); }});
      if(parts.length >= 3){{
        return "rgba(" + parts[0] + ", " + parts[1] + ", " + parts[2] + ", " + alpha + ")";
      }}
    }}
    return trimmed;
  }}

  function syncWaveControl(){{
    if(!waveControl) return;
    var selected = waveSelect.options[waveSelect.selectedIndex];
    var display = selected ? selected.textContent : "Select Wave";
    var color = selected && selected.dataset ? selected.dataset.color : "";
    waveControl.textContent = display || "Select Wave";
    waveControl.style.color = color || "";
    waveControl.style.setProperty("--wave-accent", color || "transparent");
    waveControl.style.setProperty("--wave-border", color ? toRgba(color, 0.65) : "");
    waveControl.style.setProperty("--wave-fill", color ? toRgba(color, 0.2) : "");
    waveControl.dataset.hasValue = selected && selected.value ? "true" : "false";
  }}

  function syncRouteControl(){{
    if(!routeControl) return;
    var selected = routeSelect.options[routeSelect.selectedIndex];
    var display = selected ? selected.textContent : "Select route";
    var color = selected ? selected.style.color : "";
    var waveColor = getSelectedWaveColor();
    var accent = color || waveColor || "";
    routeControl.textContent = display || "Select route";
    routeControl.style.color = accent || "";
    routeControl.style.setProperty("--wave-border", accent ? toRgba(accent, 0.65) : "");
    routeControl.style.setProperty("--wave-fill", accent ? toRgba(accent, 0.2) : "");
    routeControl.dataset.hasValue = selected && selected.value ? "true" : "false";
  }}

  function closePicker(){{
    if(pickerModal) pickerModal.hidden = true;
    if(pickerBackdrop) pickerBackdrop.hidden = true;
    if(pickerList) pickerList.innerHTML = "";
    if(waveControl) waveControl.setAttribute("aria-expanded", "false");
    if(routeControl) routeControl.setAttribute("aria-expanded", "false");
  }}

  function openPicker(kind){{
    if(!pickerModal || !pickerList) return;
    var selectEl = kind === "route" ? routeSelect : waveSelect;
    if(!selectEl || selectEl.disabled) return;
    if(pickerTitle){{
      pickerTitle.textContent = kind === "route" ? "Select Route" : "Select Wave";
    }}
    pickerList.innerHTML = "";
    var selectedValue = selectEl.value;
    var waveColor = getSelectedWaveColor();
    Array.prototype.forEach.call(selectEl.options, function(option){{
      if(!option.value || option.disabled){{
        return;
      }}
      var btn = document.createElement("button");
      btn.type = "button";
      btn.className = "pickerRow";
      btn.textContent = option.textContent;
      btn.dataset.value = option.value;
      if(option.value === selectedValue){{
        btn.classList.add("pickerRow--active");
      }}
      if(option.disabled){{
        btn.classList.add("pickerRow--disabled");
        btn.disabled = true;
      }}
      var color = "";
      if(kind === "wave"){{
        color = option.dataset ? option.dataset.color : "";
      }} else {{
        color = option.style && option.style.color ? option.style.color : "";
        if(!color) color = waveColor;
      }}
      if(color) btn.style.color = color;
      if(!option.disabled){{
        btn.addEventListener("click", function(){{
          if(kind === "wave"){{
            selectWaveValue(option.value);
          }} else {{
            selectRouteValue(option.value);
          }}
          closePicker();
        }});
      }}
      pickerList.appendChild(btn);
    }});
    if(pickerBackdrop) pickerBackdrop.hidden = false;
    pickerModal.hidden = false;
    if(waveControl) waveControl.setAttribute("aria-expanded", kind === "wave" ? "true" : "false");
    if(routeControl) routeControl.setAttribute("aria-expanded", kind === "route" ? "true" : "false");
  }}

  function selectWaveValue(value){{
    waveSelect.value = value;
    waveSelect.dispatchEvent(new Event("change", {{ bubbles: true }}));
    syncWaveControl();
  }}

  function selectRouteValue(value){{
    routeSelect.value = value;
    routeSelect.dispatchEvent(new Event("change", {{ bubbles: true }}));
    syncRouteControl();
  }}

  function populateWaves(){{
    waveSelect.innerHTML = "";
    var labels = Object.keys(groupedRoutes);
    if(!labels.length){{
      waveSelect.innerHTML = "<option value=''>No waves found</option>";
      waveSelect.disabled = true;
      if(waveControl) {{
        waveControl.disabled = true;
        waveControl.textContent = "No waves found";
      }}
      closePicker();
      return;
    }}
    labels.sort();
    var placeholder = new Option("Select Wave", "");
    placeholder.disabled = true;
    placeholder.selected = true;
    waveSelect.appendChild(placeholder);
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
    if(waveControl) waveControl.disabled = false;
    syncWaveControl();
  }}

  function populateRoutes(label){{
    routeSelect.innerHTML = "";
    openRoute.disabled = true;
    if(!label || !groupedRoutes[label]){{
      var placeholder = new Option("Select a wave first", "");
      placeholder.disabled = true;
      routeSelect.appendChild(placeholder);
      routeSelect.disabled = true;
      setRouteGroupVisibility(false);
      if(routeControl) {{
        routeControl.disabled = true;
        routeControl.textContent = "Select a wave first";
        routeControl.style.color = "";
      }}
      closePicker();
      return;
    }}
    setRouteGroupVisibility(true);
    routeSelect.disabled = false;
    if(routeControl) routeControl.disabled = false;
    var waveColor = waveColors[label.replace("Wave: ", "")] || "";
    var placeholder = new Option("Select route", "");
    if(waveColor) placeholder.style.color = waveColor;
    placeholder.disabled = true;
    placeholder.selected = true;
    routeSelect.appendChild(placeholder);
    groupedRoutes[label].forEach(function(route){{
      var opt = new Option(route.title, route.key);
      if(waveColor){{
        opt.style.color = waveColor;
      }}
      routeSelect.appendChild(opt);
    }});
    routeSelect.value = "";
    if(routeControl) routeControl.style.color = waveColor || "";
    syncRouteControl();
  }}

  function applyWaveColor(){{
    var selected = waveSelect.options[waveSelect.selectedIndex];
    var color = selected && selected.dataset ? selected.dataset.color : "";
    waveSelect.style.color = color || "";
    if(waveControl) waveControl.style.color = color || "";
    routeSelect.style.color = color || "";
  }}

  if(waveControl){{
    waveControl.addEventListener("click", function(){{
      openPicker("wave");
    }});
  }}

  if(routeControl){{
    routeControl.addEventListener("click", function(){{
      openPicker("route");
    }});
  }}
  if(pickerBackdrop){{
    pickerBackdrop.addEventListener("click", function(){{
      closePicker();
    }});
  }}
  if(pickerClose){{
    pickerClose.addEventListener("click", function(){{
      closePicker();
    }});
  }}
  if(pickerModal){{
    pickerModal.addEventListener("click", function(event){{
      event.stopPropagation();
    }});
  }}
  if(mobileMedia){{
    mobileMedia.addEventListener("change", function(){{
      setRouteGroupVisibility(Boolean(waveSelect && waveSelect.value));
    }});
  }} else {{
    window.addEventListener("resize", function(){{
      setRouteGroupVisibility(Boolean(waveSelect && waveSelect.value));
    }});
  }}

  document.addEventListener("keydown", function(event){{
    if(event.key === "Escape"){{
      closePicker();
    }}
  }});

  waveSelect.addEventListener("change", function(){{
    populateRoutes(waveSelect.value);
    applyWaveColor();
    syncWaveControl();
    syncRouteControl();
  }});

  routeSelect.addEventListener("change", function(){{
    openRoute.disabled = !routeSelect.value;
    syncRouteControl();
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
      if(tocDateText){{
        tocDateText.textContent = data.date_label || "Date";
        updateDateScale();
      }}
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
      populateRoutes("");
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
