from __future__ import annotations

import json
import threading
import time
from pathlib import Path

from fastapi import FastAPI, UploadFile, File, Response
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

    prog_pretty = json.dumps(prog, indent=2)

    html = """<!doctype html><html>
<head>
<meta name="viewport" content="width=device-width, initial-scale=1"/>
<title>Job __JID__</title>
<style>
body{font-family:system-ui,-apple-system,Segoe UI,Roboto,Arial,sans-serif;margin:0;padding:18px;background:#0b0f14;color:#e8eef6}
.card{max-width:720px;margin:0 auto;background:#101826;border:1px solid #1c2a3a;border-radius:16px;padding:16px}
.muted{color:#97a7bd}
.bar{height:10px;background:#0f1722;border:1px solid #1c2a3a;border-radius:999px;overflow:hidden}
.fill{height:100%;background:#3fa7ff;transition:width .25s ease}
.grid{display:grid;gap:10px;margin-top:14px}
.btnA{padding:14px;border-radius:12px;background:#3fa7ff;color:#001018;font-weight:900;text-decoration:none;text-align:center}
.btnB{padding:14px;border-radius:12px;border:1px solid #1c2a3a;background:#0f1722;color:#e8eef6;text-decoration:none;text-align:center}
pre{white-space:pre-wrap;background:#0f1722;border:1px solid #1c2a3a;padding:12px;border-radius:12px;margin-top:12px}
</style>
</head>
<body>
  <div class="card">
    <div style="font-weight:900">Job __JID__</div>

    <div class="muted" style="margin-top:6px">
      Status: <b id="st">__STATUS__</b>
    </div>

    <div style="margin-top:12px" class="bar">
      <div class="fill" id="fill" style="width: __PCT__%"></div>
    </div>

    <div class="muted" style="margin-top:10px">Progress:</div>
    <pre id="prog">__PROG__</pre>

    <div class="grid" id="links" style="display:none">
      <a id="aOrg" class="btnA" href="/job/__JID__/organizer">Open Van Organizer</a>
      <a id="aPdf" class="btnB" href="/job/__JID__/download/STACKED.pdf">Download STACKED.pdf</a>
      <a id="aXlsx" class="btnB" href="/job/__JID__/download/Bags_with_Overflow.xlsx">Download Excel</a>
    </div>

    <pre id="err" style="display:none"></pre>

    <div class="muted" style="margin-top:14px;font-size:13px">
      Leave this page open — it will update automatically.
    </div>
  </div>

<script>
(function(){
  var jid = "__JID__";
  var stEl = document.getElementById("st");
  var fill = document.getElementById("fill");
  var prog = document.getElementById("prog");
  var links = document.getElementById("links");
  var err = document.getElementById("err");

  var aOrg = document.getElementById("aOrg");
  var aPdf = document.getElementById("aPdf");
  var aXlsx = document.getElementById("aXlsx");

  function setPct(p){
    p = Math.max(0, Math.min(100, p|0));
    fill.style.width = p + "%";
  }

  function showDone(s){
    // Cache-bust so mobile browsers don't show old files
    var bust = "v=" + Date.now();
    aOrg.href  = s.organizer_url + "?" + bust;
    aPdf.href  = s.pdf_url + "?" + bust;
    aXlsx.href = s.xlsx_url + "?" + bust;

    links.style.display = "grid";
    err.style.display = "none";
  }

  function showErr(msg){
    err.textContent = msg || "Unknown error";
    err.style.display = "block";
    links.style.display = "none";
  }

  async function tick(){
    try{
      var r = await fetch("/job/" + jid + "/status", { cache: "no-store" });
      if(!r.ok) return;
      var s = await r.json();

      stEl.textContent = s.status || "";
      prog.textContent = JSON.stringify(s.progress || {}, null, 2);

      var pct = 0;
      if(s.progress && typeof s.progress.pct !== "undefined") pct = parseInt(s.progress.pct, 10) || 0;
      setPct(pct);

      if(s.status === "done"){
        showDone(s);
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
            .replace("__STATUS__", str(status))
            .replace("__PCT__", str(pct))
            .replace("__PROG__", prog_pretty)
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
    - shifts content into view (fixes left cutoff)
    - scales to fit phone width
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
.scale-shell{{width:100%; overflow:visible}}
.scale-inner{{transform-origin: top left; will-change: transform}}
iframe{{border:0; display:block}}
.hint{{color:#97a7bd;font-size:12px;padding:6px 0 0}}
</style>
</head>
<body>
  <div class="topbar">
    <a href="/job/{jid}">← Back</a>
  </div>

  <div class="wrap">
    <div class="scale-shell" id="shell">
      <div class="scale-inner" id="inner">
        <iframe id="orgFrame" src="/job/{jid}/organizer_raw?v=1" scrolling="no"></iframe>
      </div>
    </div>
    <div class="hint">Auto-fit width + shift into view. Scroll this page.</div>
  </div>

<script>
(function () {{
  var frame = document.getElementById("orgFrame");
  var inner = document.getElementById("inner");
  var shell = document.getElementById("shell");

  // cache-bust iframe so it always pulls the newest organizer without manual refresh
  frame.src = "/job/{jid}/organizer_raw?v=" + Date.now();

  function measureSpan(doc) {{
    var els = Array.from(doc.querySelectorAll("body *"));
    var minLeft = Infinity;
    var maxRight = -Infinity;
    var maxBottom = 0;

    for (var i = 0; i < els.length; i++) {{
      var el = els[i];
      if (!el.getBoundingClientRect) continue;
      var r = el.getBoundingClientRect();
      if (r.width === 0 || r.height === 0) continue;
      if (r.left < minLeft) minLeft = r.left;
      if (r.right > maxRight) maxRight = r.right;
      if (r.bottom > maxBottom) maxBottom = r.bottom;
    }}

    var b = doc.body;
    var h = doc.documentElement;
    var docRight = Math.max(b.scrollWidth, h.scrollWidth, b.offsetWidth, h.offsetWidth, b.clientWidth, h.clientWidth);
    var docBottom = Math.max(b.scrollHeight, h.scrollHeight, b.offsetHeight, h.offsetHeight, b.clientHeight, h.clientHeight);

    if (!isFinite(minLeft) || !isFinite(maxRight)) {{
      minLeft = 0;
      maxRight = docRight;
    }}

    maxBottom = Math.max(maxBottom, docBottom);

    var width = (maxRight - minLeft) + 20;
    var height = maxBottom + 20;
    return {{ minLeft: minLeft, width: width, height: height }};
  }}

  function sizeAndScale() {{
    try {{
      var doc = frame.contentDocument || frame.contentWindow.document;
      if (!doc) return;

      var span = measureSpan(doc);
      var available = document.documentElement.clientWidth - 20;

      var scale = 1;
      if (span.width > 0) {{
        scale = Math.min(0.985, available / span.width);
      }}

      var shiftX = (span.minLeft < 0) ? (-span.minLeft) : 0;

      // NO template literals here (avoid Python f-string conflicts)
      inner.style.transform = "translateX(" + shiftX + "px) scale(" + scale.toFixed(4) + ")";
      inner.style.width = span.width + "px";

      frame.style.width = span.width + "px";
      frame.style.height = span.height + "px";

      shell.style.height = (span.height * scale) + "px";
    }} catch (e) {{}}
  }}

  frame.addEventListener("load", function () {{
    sizeAndScale();
    setTimeout(sizeAndScale, 150);
    setTimeout(sizeAndScale, 700);
    setTimeout(sizeAndScale, 1600);
    setTimeout(sizeAndScale, 3000);
  }});

  window.addEventListener("resize", sizeAndScale);
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
