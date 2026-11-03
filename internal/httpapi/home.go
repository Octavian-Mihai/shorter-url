package httpapi

import "net/http"

// handleHome serves a one-page demo UI: shorten a link, open it, see its clicks.
// It only calls the public API, so it exercises the same path as any client.
func handleHome(w http.ResponseWriter, _ *http.Request) {
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	_, _ = w.Write([]byte(homeHTML))
}

const homeHTML = `<!doctype html>
<html lang="en"><head><meta charset="utf-8">
<meta name="viewport" content="width=device-width,initial-scale=1">
<title>URL Shortener</title>
<style>
  body{font:16px/1.5 system-ui,sans-serif;max-width:560px;margin:60px auto;padding:0 16px;color:#1a1a1a}
  h1{margin:0 0 4px} .sub{color:#666;margin:0 0 28px}
  input{width:100%;box-sizing:border-box;padding:10px;font-size:16px;border:1px solid #ccc;border-radius:6px;margin:4px 0 12px}
  button{padding:10px 18px;font-size:16px;border:0;border-radius:6px;background:#2563eb;color:#fff;cursor:pointer}
  button.alt{background:#e5e7eb;color:#111}
  #result{margin-top:28px;padding:16px;border:1px solid #ddd;border-radius:8px;display:none}
  #short{font-size:20px;font-weight:600;word-break:break-all}
  .err{color:#b91c1c} .muted{color:#666;font-size:14px}
  footer{margin-top:40px;font-size:14px;color:#666} a{color:#2563eb}
</style></head><body>
<h1>URL Shortener</h1>
<p class="sub">Paste a long link, get a short one, and watch the clicks come in.</p>

<label>Long URL</label>
<input id="url" placeholder="https://example.com/some/very/long/path">
<label>Custom name <span class="muted">(optional)</span></label>
<input id="alias" placeholder="hello">
<button id="go">Shorten</button>
<p id="error" class="err"></p>

<div id="result">
  <div class="muted">Your short link</div>
  <div><a id="short" target="_blank"></a></div>
  <p>Clicks: <b id="clicks">0</b>
     <button class="alt" id="refresh">Refresh</button></p>
  <p class="muted">Open the short link, then press Refresh. Clicks are counted in the
  background (Kafka), so the number can lag by a second or two.</p>
</div>

<footer>
  <a href="/docs">API docs</a> · <a href="http://localhost:3000" target="_blank">Metrics dashboard</a>
</footer>

<script>
const KEY = "dev-key-change-me"; // demo key seeded by docker compose
const $ = id => document.getElementById(id);
let slug = "";

$("go").onclick = async () => {
  $("error").textContent = "";
  const body = {url: $("url").value.trim()};
  if ($("alias").value.trim()) body.alias = $("alias").value.trim();
  const res = await fetch("/v1/links", {
    method: "POST",
    headers: {"X-API-Key": KEY, "Content-Type": "application/json"},
    body: JSON.stringify(body)
  });
  const data = await res.json();
  if (!res.ok) { $("error").textContent = (data.error && data.error.message) || "Something went wrong"; return; }
  slug = data.slug;
  $("short").textContent = data.short_url;
  $("short").href = data.short_url;
  $("result").style.display = "block";
  refresh();
};

async function refresh() {
  if (!slug) return;
  const res = await fetch("/v1/links/" + slug + "/stats", {headers: {"X-API-Key": KEY}});
  if (res.ok) $("clicks").textContent = (await res.json()).total_clicks;
}
$("refresh").onclick = refresh;
</script>
</body></html>`
