// WikiPulse dashboard. The panels read the exported Parquet snapshots directly with hyparquet
// (~50 KB of JS), so the page renders in well under 3 s. DuckDB-WASM (~7 MB compressed, plus
// seconds of compile time) loads only when someone opens "Explore with SQL" (ADR 0007).
import { parquetReadObjects } from "https://cdn.jsdelivr.net/npm/hyparquet@1.31.2/+esm";
import * as Plot from "https://cdn.jsdelivr.net/npm/@observablehq/plot@0.6.16/+esm";

// Where the snapshots live: ?data=<url> overrides (e.g. a Cloudflare R2 bucket).
const DATA = (() => {
  const base = new URLSearchParams(location.search).get("data") || "./data/";
  return base.endsWith("/") ? base : base + "/";
})();
const STALE_MINUTES = 30; // PRD §9: banner turns amber past 30 minutes

const $ = (id) => document.getElementById(id);
const fmt = new Intl.NumberFormat();
const pct = new Intl.NumberFormat(undefined, { style: "percent", maximumFractionDigits: 1 });
const esc = (s) => String(s ?? "").replace(/[&<>"']/g, (c) => ({ "&": "&amp;", "<": "&lt;", ">": "&gt;", '"': "&quot;", "'": "&#39;" })[c]);
const css = (name) => getComputedStyle(document.documentElement).getPropertyValue(name).trim();
const num = (v) => (typeof v === "bigint" ? Number(v) : v);
const clockUTC = (d) => d.toLocaleTimeString([], { hour: "2-digit", minute: "2-digit", timeZone: "UTC" }) + " UTC";
const ago = (d) => {
  const m = Math.round((Date.now() - d.getTime()) / 60000);
  return m < 1 ? "just now" : m < 60 ? `${m} min ago` : `${Math.round(m / 60)} h ago`;
};
const done = (el) => el.removeAttribute("aria-busy");

const buffers = {}; // file name -> ArrayBuffer, shared with the SQL explorer

async function readParquet(file) {
  const res = await fetch(DATA + file, { cache: "no-cache" });
  if (!res.ok) throw new Error(`${file}: HTTP ${res.status}`);
  buffers[file] = await res.arrayBuffer();
  const rows = await parquetReadObjects({ file: buffers[file] });
  return rows.map((r) => Object.fromEntries(Object.entries(r).map(([k, v]) => [k, num(v)])));
}

// ---- Panels ----

function renderFreshness(manifest) {
  const asOf = manifest.data_as_of ? new Date(manifest.data_as_of) : null;
  const stale = !asOf || (Date.now() - asOf.getTime()) / 60000 > STALE_MINUTES;
  $("asof").dataset.state = stale ? "stale" : "fresh";
  $("asof-text").textContent = asOf ? `Data as of ${clockUTC(asOf)} (${ago(asOf)})${stale ? ": stale" : ""}` : "No data timestamp";
}

function renderTiles(manifest, epm, spikes) {
  const newest = epm.length ? epm.at(-1).t_ms : 0;
  const hour = epm.filter((d) => d.t_ms > newest - 3600_000);
  const total = hour.reduce((a, d) => a + d.total, 0), bot = hour.reduce((a, d) => a + d.bot, 0);
  $("k-edits").textContent = fmt.format(total);
  $("k-bots").textContent = total ? pct.format(bot / total) : "–";
  $("k-spikes").textContent = fmt.format(new Set(spikes.map((d) => d.wiki + "|" + d.title)).size);
  const p95 = manifest.latency_last_hour_seconds?.p95;
  $("k-latency").textContent = p95 != null ? `≤ ${p95} s` : "–";
  const dbt = manifest.last_dbt_run || {};
  if (dbt.passed != null) {
    $("k-tests").innerHTML = dbt.failed ? `<span class="bad">${dbt.failed} failed</span>` : `<span class="ok">${dbt.passed} pass</span>`;
    $("k-tests-hint").textContent = `last dbt ${dbt.command || "run"}, ${ago(new Date(dbt.generated_at))}`;
  }
}

function renderEditsChart(data) {
  const el = $("chart-epm");
  done(el);
  if (!data.length) { el.innerHTML = `<div class="empty">No data yet</div>`; return; }
  // Drop the newest minute: it's still filling up and would read as a sudden drop.
  const complete = data.slice(0, -1);
  const long = complete.flatMap((d) => [
    { t: new Date(d.t_ms), series: "Human", value: d.human },
    { t: new Date(d.t_ms), series: "Bot", value: d.bot },
  ]);
  const last = complete.at(-1);
  el.replaceChildren(Plot.plot({
    height: 260, marginLeft: 48, marginRight: 52, width: el.clientWidth,
    style: { fontFamily: "var(--mono)", fontSize: "11px", color: css("--faint"), background: "transparent" },
    x: { type: "time", label: null },
    y: { label: "edits / min", grid: true, nice: true },
    color: { domain: ["Human", "Bot"], range: [css("--human"), css("--bot")] },
    marks: [
      Plot.ruleY([0], { stroke: css("--line-strong") }),
      Plot.lineY(long, { x: "t", y: "value", stroke: "series", strokeWidth: 2, curve: "monotone-x" }),
      Plot.text(last ? [
        { t: new Date(last.t_ms), value: last.human, series: "Human" },
        { t: new Date(last.t_ms), value: last.bot, series: "Bot" },
      ] : [], { x: "t", y: "value", text: "series", dx: 8, textAnchor: "start", fill: css("--ink-2") }),
      Plot.tip(long, Plot.pointerX({ x: "t", y: "value", stroke: "series",
        title: (d) => `${d.series}: ${fmt.format(d.value)} edits\n${d.t.toLocaleTimeString([], { hour: "2-digit", minute: "2-digit" })}` })),
    ],
  }));
}

function renderTrending(data) {
  const t = $("t-trending");
  done(t);
  t.innerHTML = data.length
    ? `<tr><th class="r">#</th><th>Article</th><th>Wiki</th><th class="r">Edits</th><th class="r">Editors</th></tr>` +
      data.map((d, i) => `<tr><td class="r num">${i + 1}</td><td class="title"><a href="${esc(d.url)}" target="_blank" rel="noopener" dir="auto">${esc(d.title)}</a></td><td><span class="chip">${esc(d.wiki)}</span></td><td class="r num">${d.edits}</td><td class="r num">${d.editors}</td></tr>`).join("")
    : `<tr><td class="empty">No article edits in the last 15 minutes</td></tr>`;
}

function languageShares(data) {
  const sorted = [...data].sort((a, b) => b.edits - a.edits);
  const top = sorted.slice(0, 9), rest = sorted.slice(9);
  if (rest.length) top.push({ language: "Other", edits: rest.reduce((a, d) => a + d.edits, 0), share: rest.reduce((a, d) => a + d.share, 0) });
  return top;
}

function renderLanguages(data) {
  const el = $("chart-lang");
  done(el);
  if (!data.length) { el.innerHTML = `<div class="empty">No edits today yet</div>`; return; }
  el.replaceChildren(Plot.plot({
    height: Math.max(180, data.length * 26), marginLeft: 110, marginRight: 56, width: el.clientWidth,
    style: { fontFamily: "var(--sans)", fontSize: "12px", color: css("--ink-2"), background: "transparent" },
    x: { axis: null }, y: { label: null, domain: data.map((d) => d.language) },
    marks: [
      Plot.barX(data, { x: "share", y: "language", rx: 3, insetTop: 3, insetBottom: 3,
        fill: (d) => (d.language === "Other" ? css("--line-strong") : css("--bar")),
        title: (d) => `${d.language}: ${fmt.format(d.edits)} edits (${pct.format(d.share)})`, tip: true }),
      Plot.text(data, { x: "share", y: "language", text: (d) => pct.format(d.share), dx: 6, textAnchor: "start",
        fill: css("--ink-2"), fontFamily: "var(--mono)" }),
    ],
  }));
}

function spikesByPage(data) {
  const pages = new Map();
  for (const d of data) {
    const key = d.wiki + "|" + d.title;
    const p = pages.get(key) || { wiki: d.wiki, title: d.title, url: d.url, peak: 0, editors: 0, z: 0, last: 0 };
    p.peak = Math.max(p.peak, d.window_count); p.editors = Math.max(p.editors, d.unique_editors);
    p.z = Math.max(p.z, d.z_score); p.last = Math.max(p.last, d.t_ms);
    pages.set(key, p);
  }
  return [...pages.values()].sort((a, b) => b.last - a.last).slice(0, 15);
}

function renderSpikes(data) {
  const t = $("t-spikes");
  done(t);
  t.innerHTML = data.length
    ? `<tr><th>Article</th><th>Wiki</th><th class="r">Peak edits</th><th class="r">Editors</th><th class="r">z</th><th class="r">Last seen</th></tr>` +
      data.map((d) => `<tr><td class="title"><a href="${esc(d.url)}" target="_blank" rel="noopener" dir="auto">${esc(d.title)}</a></td><td><span class="chip">${esc(d.wiki)}</span></td><td class="r num">${d.peak}</td><td class="r num">${d.editors}</td><td class="r num">${d.z.toFixed(1)}</td><td class="r num">${esc(ago(new Date(d.last)))}</td></tr>`).join("")
    : `<tr><td class="empty">No spikes in the last 24 hours</td></tr>`;
}

function renderVandalism(data) {
  const t = $("t-vandalism");
  done(t);
  const cls = (s) => (s >= 60 ? "hi" : s >= 40 ? "mid" : "lo");
  const rows = [...data].sort((a, b) => b.vandalism_score - a.vandalism_score || b.t_ms - a.t_ms).slice(0, 15);
  t.innerHTML = rows.length
    ? `<tr><th class="r">Score</th><th>Article</th><th>Wiki</th><th>Editor</th><th class="r">Bytes</th><th>Signals</th><th>Diff</th></tr>` +
      rows.map((d) => {
        const signals = [d.is_anonymous && "anonymous", d.is_blanking && "blanking",
          d.page_reverts_last_hour >= 2 && "revert war", d.user_edits_last_10min >= 10 && "burst"].filter(Boolean);
        return `<tr><td class="r"><span class="score ${cls(d.vandalism_score)}">${d.vandalism_score}</span></td><td class="title" dir="auto">${esc(d.title)}</td><td><span class="chip">${esc(d.wiki)}</span></td><td class="num" dir="auto">${esc(d.user_name)}</td><td class="r num">${d.byte_delta > 0 ? "+" : ""}${fmt.format(d.byte_delta)}</td><td>${signals.map((s) => `<span class="sig">${s}</span>`).join("")}</td><td><a href="${esc(d.diff_url)}" target="_blank" rel="noopener">view diff</a></td></tr>`;
      }).join("")
    : `<tr><td class="empty">No flagged edits in the last hour</td></tr>`;
}

function renderHealth(manifest) {
  const lat = manifest.latency_last_hour_seconds || {};
  const dbt = manifest.last_dbt_run || {};
  const asOf = manifest.data_as_of ? new Date(manifest.data_as_of) : null;
  const items = [
    ["Data freshness", asOf ? ago(asOf) : "unknown"],
    ["Latency p50 / p95 / p99", lat.events ? `${lat.p50} / ${lat.p95} / ${lat.p99} s` : "no events"],
    ["Events measured (1 h)", lat.events ? fmt.format(lat.events) : "0"],
    ["Last dbt run", dbt.passed != null ? `<span class="${dbt.failed ? "bad" : "ok"}">${dbt.passed} pass, ${dbt.failed} fail</span>` : "unknown"],
    ["Exported", manifest.exported_at ? clockUTC(new Date(manifest.exported_at)) : "unknown"],
    ["Snapshot size", `${fmt.format(Object.values(manifest.files).reduce((a, f) => a + f.bytes, 0) / 1024 | 0)} KB`],
  ];
  $("health").innerHTML = items.map(([k, v]) => `<dt>${k}</dt><dd>${v}</dd>`).join("");
}

// ---- Explore with SQL: DuckDB-WASM, loaded on demand ----

let duckdbConn = null;

async function duckdb() {
  if (duckdbConn) return duckdbConn;
  const lib = await import("https://cdn.jsdelivr.net/npm/@duckdb/duckdb-wasm@1.29.0/+esm");
  // unpkg serves the ~36 MB wasm gzip-compressed (~7 MB); jsDelivr served it uncompressed in testing.
  const dist = "https://unpkg.com/@duckdb/duckdb-wasm@1.29.0/dist/";
  const bundle = await lib.selectBundle({
    mvp: { mainModule: dist + "duckdb-mvp.wasm", mainWorker: dist + "duckdb-browser-mvp.worker.js" },
    eh: { mainModule: dist + "duckdb-eh.wasm", mainWorker: dist + "duckdb-browser-eh.worker.js" },
  });
  const workerUrl = URL.createObjectURL(new Blob([`importScripts("${bundle.mainWorker}");`], { type: "text/javascript" }));
  const db = new lib.AsyncDuckDB(new lib.ConsoleLogger(lib.LogLevel.WARNING), new Worker(workerUrl));
  await db.instantiate(bundle.mainModule, bundle.pthreadWorker);
  URL.revokeObjectURL(workerUrl);
  for (const [file, buf] of Object.entries(buffers)) await db.registerFileBuffer(file, new Uint8Array(buf));
  duckdbConn = await db.connect();
  return duckdbConn;
}

function setupExplorer() {
  const form = $("sql-form"), out = $("sql-out"), status = $("sql-status");
  form.addEventListener("submit", async (e) => {
    e.preventDefault();
    const button = form.querySelector("button");
    button.disabled = true;
    try {
      status.textContent = duckdbConn ? "Running…" : "Loading DuckDB-WASM (first query only)…";
      const t0 = performance.now();
      const conn = await duckdb();
      const result = await conn.query($("sql").value);
      const rows = result.toArray().slice(0, 200).map((r) => r.toJSON());
      const cols = result.schema.fields.map((f) => f.name);
      status.textContent = `${fmt.format(result.numRows)} rows in ${Math.round(performance.now() - t0)} ms` +
        (result.numRows > 200 ? " (showing 200)" : "");
      out.innerHTML = `<tr>${cols.map((c) => `<th>${esc(c)}</th>`).join("")}</tr>` +
        rows.map((r) => `<tr>${cols.map((c) => `<td class="num">${esc(num(r[c]))}</td>`).join("")}</tr>`).join("");
    } catch (err) {
      status.innerHTML = `<span class="bad">${esc(err.message.split("\n")[0])}</span>`;
    } finally {
      button.disabled = false;
    }
  });
}

function fail(err) {
  console.error(err);
  $("asof").dataset.state = "stale";
  $("asof-text").textContent = `Couldn't load data: ${err.message}`;
  document.querySelectorAll("[aria-busy]").forEach((el) => { done(el); el.innerHTML = `<div class="empty error">Data unavailable</div>`; });
}

async function main() {
  const manifestRes = await fetch(DATA + "manifest.json", { cache: "no-cache" });
  if (!manifestRes.ok) throw new Error(`manifest.json: HTTP ${manifestRes.status}`);
  const manifest = await manifestRes.json();
  renderFreshness(manifest);
  renderHealth(manifest);

  const [epm, trending, spikes, langs, vandalism] = await Promise.all(
    ["edits_per_minute", "trending", "spikes", "language_share", "vandalism"].map((n) => readParquet(`${n}.parquet`)));
  epm.sort((a, b) => a.t_ms - b.t_ms);
  const langShares = languageShares(langs);

  renderTiles(manifest, epm, spikes);
  renderEditsChart(epm);
  renderTrending(trending);
  renderLanguages(langShares);
  renderSpikes(spikesByPage(spikes));
  renderVandalism(vandalism);
  setupExplorer();
  window.addEventListener("resize", () => { renderEditsChart(epm); renderLanguages(langShares); });
  document.body.dataset.loadMs = Math.round(performance.now()); // PRD §10: load time < 3 s
}

main().catch(fail);
