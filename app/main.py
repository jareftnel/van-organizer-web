from __future__ import annotations

import threading
from pathlib import Path

from fastapi import FastAPI, UploadFile, File, Response
from fastapi.responses import HTMLResponse, FileResponse, RedirectResponse

from .pipeline import JobStore, process_job

JOBS_DIR = Path("/tmp/vanorg_jobs")
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
    pdf_path.write_bytes(await file.read())

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


@app.get("/job/{jid}/organizer_raw", response_class=HTMLResponse)
def organizer_raw(jid: str):
    job_dir = store.path(jid)
    html_path = job_dir / "van_organizer.html"
    if not html_path.exists():
        return HTMLResponse("Organizer not ready yet.", status_code=404)
    return HTMLResponse(html_path.read_text(encoding="utf-8"))


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
        <iframe id="orgFrame" src="/job/{jid}/organizer_raw" scrolling="no"></iframe>
      </div>
    </div>
    <div class="hint">Auto-fit width + shift into view. Scroll this page.</div>
  </div>

<script>
(function () {{
  var frame = document.getElementById("orgFrame");
  var inner = document.getElementById("inner");
  var shell = document.getElementById("shell");

  function measureSpan(doc) {{
    var els = Array.from(doc.querySelectorAll("body *"));
    var minLeft = Infinity;
    var maxRight = -Infinity;
    var maxBottom = 0;

    for (var i = 0; i < els.length; i++) {{
      var el = els[i];
      if (!el.getBoundingClientRect) continue;
      var r = el.getBoundingClientRect();
      if (r.width < 40 || r.height < 40) continue;
      if (r.left < minLeft) minLeft = r.left;
      if (r.right > maxRight) maxRight = r.right;
      if (r.bottom > maxBottom) maxBottom = r.bottom;
    }}

    if (!isFinite(minLeft) || !isFinite(maxRight)) {{
      var b = doc.body;
      var h = doc.documentElement;
      minLeft = 0;
      maxRight = Math.max(b.scrollWidth, h.scrollWidth, b.offsetWidth, h.offsetWidth, b.clientWidth, h.clientWidth);
      maxBottom = Math.max(b.scrollHeight, h.scrollHeight, b.offsetHeight, h.offsetHeight, b.clientHeight, h.clientHeight);
    }}

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
    return FileResponse(str(f), filename=name)
