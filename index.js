// index.js (2026-02-06 v6.3)
// ✅ persist hot news + close top30 + market cap + industry/sub
// ✅ hot news schedule 10/11/12/13 (Mon-Fri)
// ✅ realtime quotes 09:00-13:30 (interval) + /api/tw/realtime
// ✅ US close TOP30 dollar volume (Polygon grouped daily) 05:10 + /api/us/top30-dollar-volume
// ✅ US market cap TOP30 (Polygon ticker overview) 05:10 + /api/us/marketcap-top30
// ✅ /api/hot/refresh (manual refresh for Hot News)
// ✅ FIX: hot news keep 31 days on API (even if frontend passes smaller days)
// ✅ FIX: do NOT fallback publishedAtISO to "now" (avoid weird retention/sorting)

import express from "express";
import * as cheerio from "cheerio";
import cron from "node-cron";
import fs from "fs";
import path from "path";

const app = express();
const PORT = process.env.PORT || 3000;
process.env.TZ = process.env.TZ || "Asia/Taipei";

console.log("### RUNNING INDEX.JS VERSION: 2026-02-06 v6.3 (hot retention + api min days fix) ###");

app.use(express.static("public"));

// ===== In-memory stores =====
let hotNewsStore = [];
let closeStore = null;
let realtimeStore = null; // { updatedAt, market, items: [...] }
let usTop30Store = null;  // { updatedAt, tradingDate, items: [...] }  (Dollar Volume Top30)
let usMarketCapStore = null; // { updatedAt, tradingDate, items: [...] } (Market Cap Top30)

// ===== Data files =====
const DATA_DIR = path.join(process.cwd(), "data");
const HISTORY_FILE = path.join(DATA_DIR, "top30_history.json");
const SECTOR_FILE = path.join(DATA_DIR, "sector_map.json");

// ✅ Hot news persistent store
const HOT_FILE = path.join(DATA_DIR, "hot_news_store.json");

// ✅ Close persistent store
const CLOSE_FILE = path.join(DATA_DIR, "close_store.json");

// ✅ Realtime persistent store
const REALTIME_FILE = path.join(DATA_DIR, "realtime_quotes.json");

// ✅ US dollar volume persistent store
const US_TOP30_FILE = path.join(DATA_DIR, "us_top30_dollar_volume.json");

// ✅ US market cap persistent store
const US_MKCAP_FILE = path.join(DATA_DIR, "us_marketcap_top30.json");

// 用來算市值：發行股數（上市/上櫃）
const SHARES_CACHE_FILE = path.join(DATA_DIR, "shares_cache.json");

// ===== Hot retention config =====
const HOT_KEEP_DAYS = Number(process.env.HOT_KEEP_DAYS || 90);
const HOT_API_DEFAULT_DAYS = Number(process.env.HOT_API_DEFAULT_DAYS || 30);
// ✅ NEW: API minimum days (avoid frontend passing too small days)
const HOT_API_MIN_DAYS = Number(process.env.HOT_API_MIN_DAYS || 31);

// ===== Realtime config =====
const REALTIME_INTERVAL_MS = Number(process.env.REALTIME_INTERVAL_MS || 30000);

// ===== US config =====
const POLYGON_API_KEY = process.env.POLYGON_API_KEY || "";

// ===== Helpers =====
function ensureDataDir() {
  if (!fs.existsSync(DATA_DIR)) fs.mkdirSync(DATA_DIR, { recursive: true });
}
function loadJsonSafe(file, fallback) {
  try {
    if (!fs.existsSync(file)) return fallback;
    return JSON.parse(fs.readFileSync(file, "utf-8"));
  } catch {
    return fallback;
  }
}
function saveJsonSafe(file, obj) {
  ensureDataDir();
  fs.writeFileSync(file, JSON.stringify(obj, null, 2), "utf-8");
}
function sleep(ms) { return new Promise(r => setTimeout(r, ms)); }
function nowTaipei() { return new Date(); }
function pad2(n) { return String(n).padStart(2, "0"); }
function toISODate(d) { return `${d.getFullYear()}-${pad2(d.getMonth() + 1)}-${pad2(d.getDate())}`; }
function yyyymmdd(iso) { return String(iso).replace(/-/g, ""); }
function parseNumber(str) { return Number(String(str ?? "").replace(/,/g, "").trim()) || 0; }
function stripTags(s = "") { return String(s).replace(/<[^>]*>/g, "").trim(); }
function normalizeTitle(t = "") { return t.replace(/\s+/g, " ").trim(); }
function isHotTitle(title = "") { return title.includes("熱門族群"); }

// ✅ FIX: guard null/empty ISO
function withinLastNDays(iso, days = 30) {
  if (!iso) return false;
  const t = new Date(iso).getTime();
  if (!Number.isFinite(t)) return false;
  return (nowTaipei().getTime() - t) <= days * 24 * 60 * 60 * 1000;
}

function signFromCell(cell = "") {
  const t = String(cell || "");
  if (t.includes("color:red") || t.includes(">+<") || t.includes("+")) return "+";
  if (t.includes("color:green") || t.includes(">-<") || t.includes("-")) return "-";
  return "";
}
function fmtPctFromNumbers(change, prev) {
  if (!Number.isFinite(change) || !Number.isFinite(prev) || prev === 0) return "--";
  const pct = (change / prev) * 100;
  return (pct > 0 ? "+" : "") + pct.toFixed(2) + "%";
}
function yahooUrl(code, market) {
  const suffix = market === "TPEx" ? "TWO" : "TW";
  return `https://tw.stock.yahoo.com/quote/${code}.${suffix}`;
}
function marketLabel(market) {
  return market === "TPEx" ? "上櫃" : "上市";
}

// ===== Time helpers =====
function isMarketOpenTaipei(now = new Date()) {
  const fmt = new Intl.DateTimeFormat("en-US", {
    timeZone: "Asia/Taipei",
    hour: "2-digit",
    minute: "2-digit",
    hour12: false,
    weekday: "short",
  });
  const parts = fmt.formatToParts(now);
  const weekday = parts.find(p => p.type === "weekday")?.value;
  const hour = Number(parts.find(p => p.type === "hour")?.value);
  const minute = Number(parts.find(p => p.type === "minute")?.value);

  const isWeekday = ["Mon", "Tue", "Wed", "Thu", "Fri"].includes(weekday);
  if (!isWeekday) return false;

  const afterOpen = (hour > 9) || (hour === 9 && minute >= 0);
  const beforeClose = (hour < 13) || (hour === 13 && minute <= 30);
  return afterOpen && beforeClose;
}

function getDateStringInNY(date = new Date()) {
  const formatter = new Intl.DateTimeFormat("en-CA", {
    timeZone: "America/New_York",
    year: "numeric",
    month: "2-digit",
    day: "2-digit",
  });
  return formatter.format(date);
}

function shiftDateStr(dateStr, daysBack = 1) {
  const [y, m, d] = dateStr.split("-").map(Number);
  const dt = new Date(Date.UTC(y, m - 1, d));
  dt.setUTCDate(dt.getUTCDate() - daysBack);
  const yyyy = dt.getUTCFullYear();
  const mm = String(dt.getUTCMonth() + 1).padStart(2, "0");
  const dd = String(dt.getUTCDate()).padStart(2, "0");
  return `${yyyy}-${mm}-${dd}`;
}

// ===== Store persistence =====
function loadHotStoreFromDisk() {
  ensureDataDir();
  const arr = loadJsonSafe(HOT_FILE, []);
  if (!Array.isArray(arr)) return [];
  return arr
    .map(x => ({
      title: String(x.title || "").trim(),
      link: String(x.link || "").trim(),
      // ✅ FIX: do NOT fallback to now()
      publishedAtISO: x.publishedAtISO ? String(x.publishedAtISO) : null,
    }))
    .filter(x => x.title && x.link);
}
function saveHotStoreToDisk() {
  saveJsonSafe(HOT_FILE, hotNewsStore);
}

function loadCloseStoreFromDisk() {
  ensureDataDir();
  const obj = loadJsonSafe(CLOSE_FILE, null);
  if (!obj || typeof obj !== "object") return null;
  return obj;
}
function saveCloseStoreToDisk() {
  if (!closeStore) return;
  saveJsonSafe(CLOSE_FILE, closeStore);
}

function loadRealtimeStoreFromDisk() {
  ensureDataDir();
  const obj = loadJsonSafe(REALTIME_FILE, null);
  if (!obj || typeof obj !== "object") return null;
  return obj;
}
function saveRealtimeStoreToDisk() {
  if (!realtimeStore) return;
  saveJsonSafe(REALTIME_FILE, realtimeStore);
}

function loadUsTop30FromDisk() {
  ensureDataDir();
  const obj = loadJsonSafe(US_TOP30_FILE, null);
  if (!obj || typeof obj !== "object") return null;
  return obj;
}
function saveUsTop30ToDisk() {
  if (!usTop30Store) return;
  saveJsonSafe(US_TOP30_FILE, usTop30Store);
}

function loadUsMarketCapFromDisk() {
  ensureDataDir();
  const obj = loadJsonSafe(US_MKCAP_FILE, null);
  if (!obj || typeof obj !== "object") return null;
  return obj;
}
function saveUsMarketCapToDisk() {
  if (!usMarketCapStore) return;
  saveJsonSafe(US_MKCAP_FILE, usMarketCapStore);
}

// ===== Sector map =====
function defaultSectorMap() {
  return {
    "2330": { industry: "半導體", sub: "晶圓代工" },
    "2454": { industry: "半導體", sub: "IC設計" },
    "0050": { industry: "ETF", sub: "ETF" },
  };
}
function loadSectorMap() {
  ensureDataDir();
  if (!fs.existsSync(SECTOR_FILE)) {
    saveJsonSafe(SECTOR_FILE, defaultSectorMap());
  }
  const m = loadJsonSafe(SECTOR_FILE, defaultSectorMap());
  return m && typeof m === "object" ? m : defaultSectorMap();
}

function industrySubOf(code, name, sectorMap) {
  const c = String(code || "").trim();
  const hit = sectorMap?.[c];

  if (hit && typeof hit === "object") {
    const industry = String(hit.industry || "").trim();
    const sub = String(hit.sub || "").trim();
    return { industry: industry || "—", sub: sub || "—" };
  }

  if (/^00/.test(c)) return { industry: "ETF", sub: "ETF" };
  if (String(name || "").includes("投控")) return { industry: "控股/投控", sub: "投控" };

  return { industry: "—", sub: "—" };
}

// ===== Streak history =====
function loadHistory() {
  return loadJsonSafe(HISTORY_FILE, {});
}
function updateHistory(dateISO, codes) {
  const hist = loadHistory();
  hist[dateISO] = Array.from(new Set(codes.map(String)));

  const dates = Object.keys(hist).sort();
  const keep = 60;
  if (dates.length > keep) {
    const toDrop = dates.slice(0, dates.length - keep);
    for (const d of toDrop) delete hist[d];
  }
  saveJsonSafe(HISTORY_FILE, hist);
}
function calcStreakMap(latestDateISO) {
  const hist = loadHistory();
  const datesDesc = Object.keys(hist).sort((a, b) => b.localeCompare(a));
  if (!datesDesc.includes(latestDateISO)) return {};

  const streak = {};
  const codesToday = new Set((hist[latestDateISO] || []).map(String));
  for (const code of codesToday) streak[code] = 0;

  for (const d of datesDesc) {
    const set = new Set((hist[d] || []).map(String));
    for (const code of Object.keys(streak)) {
      if (set.has(code)) streak[code] += 1;
      else delete streak[code];
    }
    if (Object.keys(streak).length === 0) break;
  }

  const full = {};
  for (const code of codesToday) full[code] = streak[code] || 1;
  return full;
}

// ===== HTTP fetch =====
async function fetchHtml(url) {
  const res = await fetch(url, {
    headers: {
      "User-Agent": "Mozilla/5.0 (compatible; tw-stock-dashboard/1.0)",
      "Accept-Language": "zh-TW,zh;q=0.9,en;q=0.8",
    },
  });
  if (!res.ok) throw new Error(`Fetch failed ${res.status} ${res.statusText}: ${url}`);
  return await res.text();
}
async function fetchJson(url, extraHeaders = {}) {
  const res = await fetch(url, {
    headers: {
      "User-Agent": "Mozilla/5.0 (compatible; tw-stock-dashboard/1.0)",
      "Accept-Language": "zh-TW,zh;q=0.9,en;q=0.8",
      ...extraHeaders,
    },
  });
  if (!res.ok) throw new Error(`Fetch failed ${res.status} ${res.statusText}: ${url}`);
  return await res.json();
}

// ===== 1) Hot news =====
async function fetchMoneyLinkPublishedTimeISO(detailUrl) {
  const html = await fetchHtml(detailUrl);
  const $ = cheerio.load(html);
  const text = $("body").text().replace(/\s+/g, " ");

  let m = text.match(/(20\d{2})[\/\-](\d{1,2})[\/\-](\d{1,2})\s+(\d{1,2}):(\d{2})/);
  if (m) {
    const y = Number(m[1]), mo = Number(m[2]), d = Number(m[3]), hh = Number(m[4]), mm = Number(m[5]);
    return new Date(y, mo - 1, d, hh, mm, 0).toISOString();
  }

  m = text.match(/(\d{2,3})\/(\d{1,2})\/(\d{1,2})\s+(\d{1,2}):(\d{2})/);
  if (m) {
    const y = Number(m[1]) + 1911, mo = Number(m[2]), d = Number(m[3]), hh = Number(m[4]), mm = Number(m[5]);
    return new Date(y, mo - 1, d, hh, mm, 0).toISOString();
  }

  throw new Error("published time not found");
}

async function scanMoneyLinkHotNews({ maxPages = 60, limit = 300, delayMs = 220 } = {}) {
  const ntypes = ["0002", "2002"];
  const results = [];
  const seen = new Set();

  for (const ntype of ntypes) {
    const base = `https://ww2.money-link.com.tw/RealtimeNews/Index.aspx?NType=${ntype}`;

    for (let pg = 1; pg <= maxPages; pg++) {
      const url = new URL(base);
      url.searchParams.set("PGNum", String(pg));

      const html = await fetchHtml(url.toString());
      const $ = cheerio.load(html);

      const items = $('a[href*="NewsContent.aspx"]').map((_, el) => {
        const a = $(el);
        const rawHref = a.attr("href") || "";
        const link = new URL(rawHref, url.toString()).toString();
        const title = normalizeTitle(a.text());
        return { title, link };
      }).get();

      for (const it of items) {
        if (!it.title || !it.link) continue;
        if (seen.has(it.link)) continue;
        seen.add(it.link);
        if (!isHotTitle(it.title)) continue;

        const publishedAtISO = await fetchMoneyLinkPublishedTimeISO(it.link).catch(() => null);

        results.push({
          title: it.title,
          link: it.link,
          // ✅ FIX: do NOT fallback to now()
          publishedAtISO: publishedAtISO || null,
        });

        if (results.length >= limit) return results;
      }

      await sleep(delayMs);
    }
  }

  return results;
}

// ===== 2) Close data =====
function pickTWSETurnoverTableFromMIIndex(j) {
  if (j?.fields9 && j?.data9) return { fields: j.fields9, data: j.data9 };
  if (Array.isArray(j?.tables)) {
    for (const t of j.tables) {
      const fields = t.fields || [];
      const hasCode = fields.some(f => String(f).includes("證券代號"));
      const hasAmt = fields.some(f => String(f).includes("成交金額") || String(f).includes("成交值"));
      if (hasCode && hasAmt) return { fields, data: t.data || [] };
    }
  }
  return null;
}

async function fetchTAIEXClose() {
  const url = "https://openapi.twse.com.tw/v1/exchangeReport/MI_INDEX";
  const data = await fetchJson(url);
  const row = (data || []).find(x => String(x["指數"] || "").includes("發行量加權股價指數"));
  if (!row) throw new Error("TAIEX row not found");

  const signRaw = row["漲跌(+/-)"] || row["漲跌"] || "";
  const sign =
    String(signRaw).includes("color:green") || String(signRaw).includes("-") ? "-" :
      String(signRaw).includes("color:red") || String(signRaw).includes("+") ? "+" : "";

  return {
    name: row["指數"],
    close: String(row["收盤指數"] ?? "").replace(/,/g, "").trim(),
    change: String(row["漲跌點數"] ?? "").replace(/,/g, "").trim(),
    pct: row["漲跌百分比(%)"] || row["漲跌百分比"] || "",
    sign,
  };
}

async function fetchTPEXCloseAligned(isoDate) {
  const target = yyyymmdd(isoDate);
  const urls = [
    "https://www.tpex.org.tw/openapi/v1/tpex_index?s=Date,desc,0",
    "https://www.tpex.org.tw/openapi/v1/tpex_index",
  ];

  let arr = null;
  for (const u of urls) {
    try {
      const j = await fetchJson(u);
      if (Array.isArray(j) && j.length > 0) { arr = j; break; }
    } catch { }
  }
  if (!Array.isArray(arr) || arr.length === 0) throw new Error("TPEX index empty");

  const rows = arr
    .map(x => ({ Date: String(x.Date || "").trim(), Close: x.Close, Change: x.Change }))
    .filter(x => /^\d{8}$/.test(x.Date));

  let best = rows.filter(x => x.Date <= target).sort((a, b) => b.Date.localeCompare(a.Date))[0];
  if (!best) best = rows.sort((a, b) => b.Date.localeCompare(a.Date))[0];
  if (!best) throw new Error("TPEX best row not found");

  return { name: "櫃買指數", date: best.Date, close: best.Close, change: best.Change ?? "--" };
}

async function fetchTWSETop30WithPricePct(isoDate) {
  const url = `https://www.twse.com.tw/exchangeReport/MI_INDEX?response=json&date=${yyyymmdd(isoDate)}&type=ALLBUT0999`;
  const j = await fetchJson(url);

  const picked = pickTWSETurnoverTableFromMIIndex(j);
  if (!picked) throw new Error("TWSE turnover table not found");

  const fields = picked.fields || [];
  const data = picked.data || [];

  const idxCode = fields.findIndex(f => String(f).includes("證券代號"));
  const idxName = fields.findIndex(f => String(f).includes("證券名稱"));
  const idxAmt = fields.findIndex(f => String(f).includes("成交金額") || String(f).includes("成交值"));
  const idxClose = fields.findIndex(f => String(f).includes("收盤價"));
  const idxDiff = fields.findIndex(f => String(f).includes("漲跌價差"));
  const idxSign = fields.findIndex(f => String(f).includes("漲跌(+/-)"));
  const idxPct = fields.findIndex(f => String(f).includes("漲跌幅"));

  if (idxCode < 0 || idxAmt < 0) throw new Error("TWSE fields mismatch");

  const rows = data.map(r => {
    const code = String(r[idxCode] || "").trim();
    const name = idxName >= 0 ? String(r[idxName] || "").trim() : "";
    const turnover = parseNumber(r[idxAmt]);

    const close = idxClose >= 0 ? parseNumber(stripTags(r[idxClose])) : NaN;

    let diff = idxDiff >= 0 ? parseNumber(stripTags(r[idxDiff])) : NaN;
    const sign = idxSign >= 0 ? signFromCell(r[idxSign]) : "";
    if (Number.isFinite(diff) && sign === "-") diff = -Math.abs(diff);
    if (Number.isFinite(diff) && sign === "+") diff = Math.abs(diff);

    let pctText = "--";
    if (idxPct >= 0) {
      const raw = stripTags(r[idxPct]);
      if (raw) {
        const cleaned = raw.includes("%") ? raw : `${raw}%`;
        pctText = (cleaned.startsWith("-") || cleaned.startsWith("+")) ? cleaned : `+${cleaned}`;
      }
    } else if (Number.isFinite(close) && Number.isFinite(diff)) {
      pctText = fmtPctFromNumbers(diff, close - diff);
    }

    return {
      code,
      name,
      market: "TWSE",
      turnover,
      close: Number.isFinite(close) ? close : null,
      change: Number.isFinite(diff) ? diff : null,
      pctChange: pctText,
      yahooUrl: yahooUrl(code, "TWSE"),
    };
  });

  rows.sort((a, b) => b.turnover - a.turnover);
  return rows.slice(0, 30);
}

async function fetchTPEXTop30WithPricePct(isoDate) {
  const y = Number(isoDate.slice(0, 4)) - 1911;
  const m = isoDate.slice(5, 7);
  const d = isoDate.slice(8, 10);
  const roc = `${y}/${m}/${d}`;

  const url = `https://www.tpex.org.tw/openapi/v1/tpex_mainboard_daily_close_quotes?l=zh-tw&d=${encodeURIComponent(roc)}&s=0,asc,0`;
  const arr = await fetchJson(url);
  if (!Array.isArray(arr) || arr.length === 0) throw new Error("TPEX daily close quotes empty");

  const keys = Object.keys(arr[0] || {});
  const amtKey = keys.includes("TransactionAmount") ? "TransactionAmount" : null;
  const codeKey = keys.includes("SecuritiesCompanyCode") ? "SecuritiesCompanyCode" : null;
  const nameKey = keys.includes("CompanyName") ? "CompanyName" : null;
  if (!amtKey || !codeKey || !nameKey) throw new Error(`TPEX fields mismatch. keys=${keys.join(",")}`);

  const rows = arr.map(x => {
    const code = String(x[codeKey] || "").trim();
    const name = String(x[nameKey] || "").trim();
    const turnover = parseNumber(x[amtKey]);

    const close = parseNumber(x.Close);
    const chg = parseNumber(x.Change);
    const prev = close - chg;

    return {
      code,
      name,
      market: "TPEx",
      turnover,
      close: Number.isFinite(close) ? close : null,
      change: Number.isFinite(chg) ? chg : null,
      pctChange: fmtPctFromNumbers(chg, prev),
      yahooUrl: yahooUrl(code, "TPEx"),
    };
  });

  rows.sort((a, b) => b.turnover - a.turnover);
  return rows.slice(0, 30);
}

async function findLatestTradingDateISO(tryDaysBack = 14) {
  const now = nowTaipei();
  for (let i = 0; i < tryDaysBack; i++) {
    const d = new Date(now);
    d.setDate(now.getDate() - i);
    const iso = toISODate(d);
    try {
      const top = await fetchTWSETop30WithPricePct(iso);
      if (top && top.length > 0) return iso;
    } catch { }
  }
  return toISODate(now);
}

// ===== Shares (for market cap) =====
function parseIssuedShares(v) {
  const n = Number(String(v ?? "").replace(/,/g, "").trim());
  return Number.isFinite(n) ? n : 0;
}

async function fetchSharesFromTWSEOpenDataListed() {
  const url = "https://openapi.twse.com.tw/v1/opendata/t187ap03_L";
  const arr = await fetchJson(url);
  const map = {};
  if (Array.isArray(arr)) {
    for (const x of arr) {
      const code = String(x["公司代號"] || "").trim();
      const shares = parseIssuedShares(x["已發行普通股數或TDR原股發行股數"]);
      if (code && shares) map[code] = shares;
    }
  }
  return map;
}

async function loadSharesMapCached() {
  ensureDataDir();
  const cache = loadJsonSafe(SHARES_CACHE_FILE, { updatedAtISO: "", shares: {} });
  const updatedAt = cache.updatedAtISO ? new Date(cache.updatedAtISO).getTime() : 0;
  const ageMs = Date.now() - updatedAt;

  if (cache.shares && typeof cache.shares === "object" && ageMs < 24 * 60 * 60 * 1000) {
    return cache.shares;
  }

  const listed = await fetchSharesFromTWSEOpenDataListed().catch(() => ({}));
  const merged = { ...(cache.shares || {}), ...listed };

  saveJsonSafe(SHARES_CACHE_FILE, { updatedAtISO: new Date().toISOString(), shares: merged });
  return merged;
}

function marketCapBillionNTD(closePrice, shares) {
  const p = Number(closePrice);
  const s = Number(shares);
  if (!Number.isFinite(p) || !Number.isFinite(s) || p <= 0 || s <= 0) return null;
  return (p * s) / 1e9;
}

async function buildClosePackage() {
  const iso = await findLatestTradingDateISO(14);

  const [taiex, tpex] = await Promise.all([
    fetchTAIEXClose(),
    fetchTPEXCloseAligned(iso),
  ]);

  const [twseTop, tpexTop] = await Promise.all([
    fetchTWSETop30WithPricePct(iso),
    fetchTPEXTop30WithPricePct(iso),
  ]);

  const codesToday = [...twseTop, ...tpexTop].map(x => String(x.code));
  updateHistory(iso, codesToday);

  const streakMap = calcStreakMap(iso);
  const sectorMap = loadSectorMap();
  const sharesMap = await loadSharesMapCached();

  const merged = [...twseTop, ...tpexTop]
    .sort((a, b) => b.turnover - a.turnover)
    .slice(0, 30)
    .map(x => {
      const { industry, sub } = industrySubOf(x.code, x.name, sectorMap);

      const shares = sharesMap[String(x.code)] || 0;
      const mcapB = marketCapBillionNTD(x.close, shares);

      return {
        ...x,
        marketLabel: marketLabel(x.market),
        industry,
        sub,
        sector: industry || "—",
        streakDays: streakMap[String(x.code)] || 1,
        marketCapB: mcapB,
      };
    });

  return { dateISO: iso, taiex, tpex, top30: merged };
}

// ===== 3) Realtime quotes =====
function buildMisExChListFromCloseStore() {
  if (!closeStore?.top30 || !Array.isArray(closeStore.top30)) return [];

  return closeStore.top30.map(x => {
    const code = String(x.code || "").trim();
    const market = String(x.market || "").trim();
    if (!code) return null;
    const prefix = market === "TPEx" ? "otc" : "tse";
    return `${prefix}_${code}.tw`;
  }).filter(Boolean);
}

async function fetchTWRealtimeQuotesForCloseTop30() {
  const exChList = buildMisExChListFromCloseStore();
  if (exChList.length === 0) return [];

  const ex_ch = exChList.join("|");
  const url = `https://mis.twse.com.tw/stock/api/getStockInfo.jsp?ex_ch=${encodeURIComponent(ex_ch)}&json=1&delay=0&_=${Date.now()}`;

  const j = await fetchJson(url, {
    "Referer": "https://mis.twse.com.tw/stock/index.jsp",
  });

  const arr = Array.isArray(j?.msgArray) ? j.msgArray : [];
  const out = arr.map(r => {
    const code = String(r.c || "").trim();
    const name = String(r.n || "").trim();

    const last = parseNumber(r.z);
    const yClose = parseNumber(r.y);
    const open = parseNumber(r.o);
    const high = parseNumber(r.h);
    const low = parseNumber(r.l);
    const vol = parseNumber(r.v);

    const change = (Number.isFinite(last) && Number.isFinite(yClose) && yClose > 0) ? (last - yClose) : null;
    const pct = (Number.isFinite(change) && Number.isFinite(yClose) && yClose > 0)
      ? ((change / yClose) * 100)
      : null;

    return {
      code,
      name,
      last: Number.isFinite(last) ? last : null,
      change: Number.isFinite(change) ? change : null,
      pctChange: Number.isFinite(pct) ? (pct > 0 ? "+" : "") + pct.toFixed(2) + "%" : "--",
      open: Number.isFinite(open) ? open : null,
      high: Number.isFinite(high) ? high : null,
      low: Number.isFinite(low) ? low : null,
      volume: Number.isFinite(vol) ? vol : null,
      updatedAt: new Date().toISOString(),
    };
  }).filter(x => x.code);

  return out;
}

let realtimeIntervalId = null;
let realtimeUpdating = false;

async function refreshRealtimeQuotesOnce() {
  if (!isMarketOpenTaipei()) return;
  if (realtimeUpdating) return;
  realtimeUpdating = true;

  try {
    if (!closeStore) {
      closeStore = loadCloseStoreFromDisk() || closeStore;
    }

    const items = await fetchTWRealtimeQuotesForCloseTop30();
    realtimeStore = {
      updatedAt: new Date().toISOString(),
      market: "TW",
      intervalMs: REALTIME_INTERVAL_MS,
      items,
    };
    saveRealtimeStoreToDisk();

    console.log(`[JOB] realtime updated ✅ items=${items.length}`);
  } catch (e) {
    console.log("[JOB] realtime update failed ❌", e.message);
  } finally {
    realtimeUpdating = false;
  }
}

function startRealtimeUpdater() {
  if (realtimeIntervalId) return;
  realtimeIntervalId = setInterval(async () => {
    if (!isMarketOpenTaipei()) return;
    await refreshRealtimeQuotesOnce();
  }, REALTIME_INTERVAL_MS);

  console.log(`[JOB] realtime updater started ✅ interval=${REALTIME_INTERVAL_MS}ms`);
}

function stopRealtimeUpdater() {
  if (!realtimeIntervalId) return;
  clearInterval(realtimeIntervalId);
  realtimeIntervalId = null;
  console.log("[JOB] realtime updater stopped 🛑");
}

// ===== 4) US Top30 Dollar Volume =====
async function fetchPolygonGroupedDaily(tradingDate) {
  if (!POLYGON_API_KEY) throw new Error("Missing env var POLYGON_API_KEY");

  const url = `https://api.polygon.io/v2/aggs/grouped/locale/us/market/stocks/${tradingDate}?adjusted=true&apiKey=${POLYGON_API_KEY}`;
  return await fetchJson(url);
}

function buildUsTop30DollarVolume(groupedJson) {
  const results = groupedJson?.results || [];
  if (!Array.isArray(results) || results.length === 0) return [];

  return results
    .map(r => {
      const ticker = r.T;
      const close = Number(r.c);
      const volume = Number(r.v);
      if (!ticker || !Number.isFinite(close) || !Number.isFinite(volume)) return null;
      return { ticker, close, volume, dollarVolume: close * volume };
    })
    .filter(Boolean)
    .sort((a, b) => b.dollarVolume - a.dollarVolume)
    .slice(0, 30);
}

async function resolveLatestUsTradingDate() {
  const base = getDateStringInNY(new Date());
  for (let i = 0; i < 7; i++) {
    const tryDate = i === 0 ? base : shiftDateStr(base, i);
    try {
      const payload = await fetchPolygonGroupedDaily(tryDate);
      const items = buildUsTop30DollarVolume(payload);
      if (items.length > 0) return tryDate;
    } catch { }
  }
  throw new Error("No US trading data found in last 7 days.");
}

// ===== 5) US Market Cap Top30 =====
async function fetchPolygonTickerOverview(ticker) {
  if (!POLYGON_API_KEY) throw new Error("Missing env var POLYGON_API_KEY");
  const url = `https://api.polygon.io/v3/reference/tickers/${encodeURIComponent(ticker)}?apiKey=${POLYGON_API_KEY}`;
  return await fetchJson(url);
}

function extractMarketCapFromTickerOverview(j) {
  const r = j?.results || j?.result || j || {};
  const candidates = [
    r?.market_cap,
    r?.marketCap,
    r?.market_capitalization,
    r?.marketCapitalization,
  ];
  for (const v of candidates) {
    const n = Number(v);
    if (Number.isFinite(n) && n > 0) return n;
  }
  return null;
}

async function buildUsMarketCapTop30FromTickers(tickers = []) {
  const uniq = Array.from(new Set(tickers.map(String))).filter(Boolean).slice(0, 60);
  const out = [];

  const batchSize = 5;
  for (let i = 0; i < uniq.length; i += batchSize) {
    const batch = uniq.slice(i, i + batchSize);

    const res = await Promise.allSettled(
      batch.map(async (t) => {
        const j = await fetchPolygonTickerOverview(t);
        const mc = extractMarketCapFromTickerOverview(j);
        return { ticker: t, marketCap: mc };
      })
    );

    for (const r of res) {
      if (r.status === "fulfilled" && r.value?.marketCap) {
        out.push(r.value);
      }
    }

    await sleep(250);
  }

  out.sort((a, b) => (b.marketCap || 0) - (a.marketCap || 0));
  return out.slice(0, 30);
}

// ===== 6) Top30 History Packs =====
async function buildTop30PackForDate(isoDate) {
  const [twseTop, tpexTop] = await Promise.all([
    fetchTWSETop30WithPricePct(isoDate),
    fetchTPEXTop30WithPricePct(isoDate),
  ]);

  const merged = [...twseTop, ...tpexTop]
    .sort((a, b) => (b.turnover || 0) - (a.turnover || 0))
    .slice(0, 30)
    .map(x => ({
      code: x.code,
      name: x.name,
      market: x.market,
      marketLabel: marketLabel(x.market),
      turnover: x.turnover,
      close: x.close,
      change: x.change,
      pctChange: x.pctChange,
      yahooUrl: x.yahooUrl,
    }));

  return { dateISO: isoDate, top30: merged };
}

async function buildTop30HistoryPacks(tradingDays = 5, tryBackCalendarDays = 30) {
  const now = nowTaipei();
  const packs = [];

  for (let i = 0; i < tryBackCalendarDays; i++) {
    const d = new Date(now);
    d.setDate(now.getDate() - i);
    const iso = toISODate(d);

    try {
      const pack = await buildTop30PackForDate(iso);
      if (pack?.top30?.length > 0) packs.push(pack);
    } catch {
      // skip
    }

    if (packs.length >= tradingDays) break;
    await sleep(120);
  }

  return packs;
}

// ===== Jobs =====
async function refreshHotNews() {
  console.log("[JOB] refreshHotNews start");

  let scanned = [];
  try {
    scanned = await scanMoneyLinkHotNews({ maxPages: 60, limit: 300 });
  } catch (e) {
    console.log("[JOB] refreshHotNews scan failed (keep old):", e.message);
    scanned = [];
  }

  const map = new Map(hotNewsStore.map(x => [x.link, x]));
  for (const it of scanned) map.set(it.link, it);

  // ✅ FIX: keep only items with a valid publishedAtISO
  hotNewsStore = [...map.values()]
    .filter(x => x.publishedAtISO && withinLastNDays(x.publishedAtISO, HOT_KEEP_DAYS))
    .sort((a, b) => new Date(b.publishedAtISO) - new Date(a.publishedAtISO));

  saveHotStoreToDisk();
  console.log("[JOB] refreshHotNews done ✅", hotNewsStore.length);
}

async function refreshCloseDataWithRetry() {
  console.log("[JOB] refreshCloseData start");
  const maxTry = 3;
  for (let i = 0; i < maxTry; i++) {
    try {
      closeStore = await buildClosePackage();
      saveCloseStoreToDisk();
      console.log("[JOB] refreshCloseData done ✅", closeStore.dateISO);

      if (isMarketOpenTaipei()) await refreshRealtimeQuotesOnce();
      return;
    } catch (e) {
      console.log("[JOB] refreshCloseData failed, retry:", i + 1, e.message);
      await sleep(3 * 60 * 1000);
    }
  }
}

async function refreshUsDollarVolumeAndMarketCap() {
  if (!POLYGON_API_KEY) {
    console.log("[JOB] US jobs skipped: POLYGON_API_KEY missing");
    return;
  }

  console.log("[JOB] US refresh start (dollar volume + market cap)");

  const tradingDate = await resolveLatestUsTradingDate();
  const grouped = await fetchPolygonGroupedDaily(tradingDate);
  const top30 = buildUsTop30DollarVolume(grouped);

  usTop30Store = {
    updatedAt: new Date().toISOString(),
    tradingDate,
    source: "Polygon Grouped Daily",
    items: top30,
  };
  saveUsTop30ToDisk();

  const tickers = top30.map(x => x.ticker);
  const mkTop30 = await buildUsMarketCapTop30FromTickers(tickers);

  usMarketCapStore = {
    updatedAt: new Date().toISOString(),
    tradingDate,
    source: "Polygon Ticker Overview (market cap)",
    items: mkTop30,
  };
  saveUsMarketCapToDisk();

  console.log("[JOB] US refresh done ✅", tradingDate, `dollarTop=${top30.length}`, `mcapTop=${mkTop30.length}`);
}

function scheduleJobs() {
  // ✅ 熱門族群新聞：10/11/12/13（週一到週五）
  cron.schedule("0 10-13 * * 1-5", async () => {
    try { await refreshHotNews(); } catch (e) { console.error(e); }
  }, { timezone: "Asia/Taipei" });

  // ✅ 台股收盤：17:00（週一到週五）
  cron.schedule("0 17 * * 1-5", async () => {
    try { await refreshCloseDataWithRetry(); } catch (e) { console.error(e); }
  }, { timezone: "Asia/Taipei" });

  // ✅ US：05:10（週一到週五）
  cron.schedule("10 5 * * 1-5", async () => {
    try { await refreshUsDollarVolumeAndMarketCap(); } catch (e) { console.error(e); }
  }, { timezone: "Asia/Taipei" });

  // ✅ 盤前啟動即時（08:59）
  cron.schedule("59 8 * * 1-5", async () => {
    try {
      startRealtimeUpdater();
      await refreshRealtimeQuotesOnce();
    } catch (e) { console.error(e); }
  }, { timezone: "Asia/Taipei" });

  // ✅ 盤後停止即時（13:31）
  cron.schedule("31 13 * * 1-5", async () => {
    try { stopRealtimeUpdater(); } catch (e) { console.error(e); }
  }, { timezone: "Asia/Taipei" });

  console.log("[JOB] cron scheduled ✅");
}

(async () => {
  ensureDataDir();

  hotNewsStore = loadHotStoreFromDisk();
  console.log("[BOOT] hotNewsStore loaded:", hotNewsStore.length);

  closeStore = loadCloseStoreFromDisk();
  console.log("[BOOT] closeStore loaded:", closeStore ? closeStore.dateISO : "null");

  realtimeStore = loadRealtimeStoreFromDisk();
  console.log("[BOOT] realtimeStore loaded:", realtimeStore ? realtimeStore.updatedAt : "null");

  usTop30Store = loadUsTop30FromDisk();
  console.log("[BOOT] usTop30Store loaded:", usTop30Store ? usTop30Store.tradingDate : "null");

  usMarketCapStore = loadUsMarketCapFromDisk();
  console.log("[BOOT] usMarketCapStore loaded:", usMarketCapStore ? usMarketCapStore.tradingDate : "null");

  if (isMarketOpenTaipei()) {
    startRealtimeUpdater();
    await refreshRealtimeQuotesOnce();
  }

  scheduleJobs();
})();

// ===== API =====

// /api/hot?limit=200&days=30&force=1
app.get("/api/hot", async (req, res) => {
  try {
    const limit = Math.min(parseInt(req.query.limit || "200", 10), 500);

    const daysRaw = req.query.days;
    let days = Number.isFinite(Number(daysRaw))
      ? Math.min(Math.max(parseInt(daysRaw, 10), 1), 365)
      : HOT_API_DEFAULT_DAYS;

    // ✅ FIX: force never less than HOT_API_MIN_DAYS
    days = Math.max(days, HOT_API_MIN_DAYS);

    const force = String(req.query.force || "") === "1";
    if (force) {
      await refreshHotNews();
    }

    const filtered = hotNewsStore
      .filter(x => x.publishedAtISO && withinLastNDays(x.publishedAtISO, days))
      .slice(0, limit);

    res.json(filtered);
  } catch (e) {
    console.error("[/api/hot] failed:", e);
    res.status(500).json({ error: "hot_failed", message: e.message });
  }
});

// ✅ NEW：手動刷新熱門族群快訊
// GET /api/hot/refresh
app.get("/api/hot/refresh", async (req, res) => {
  try {
    await refreshHotNews();
    res.json({
      ok: true,
      count: hotNewsStore.length,
      latest: hotNewsStore?.[0] || null,
      savedFile: "data/hot_news_store.json",
    });
  } catch (e) {
    console.error("[/api/hot/refresh] failed:", e);
    res.status(500).json({ ok: false, message: e.message });
  }
});

let closeLoading = null;
app.get("/api/close", async (req, res) => {
  try {
    const force = String(req.query.force || "") === "1";
    if (!force && closeStore) return res.json(closeStore);

    if (!closeLoading) {
      closeLoading = (async () => {
        await refreshCloseDataWithRetry();
        closeLoading = null;
      })().catch((e) => {
        closeLoading = null;
        throw e;
      });
    }

    await closeLoading;
    if (!closeStore) return res.status(503).json({ error: "close_not_ready" });
    return res.json(closeStore);
  } catch (e) {
    console.error("[/api/close] build failed:", e);
    return res.status(500).json({ error: "close_build_failed", message: e.message });
  }
});

// ✅ 盤中即時報價
app.get("/api/tw/realtime", async (req, res) => {
  try {
    const force = String(req.query.force || "") === "1";
    if (force) {
      await refreshRealtimeQuotesOnce();
    }

    if (!realtimeStore) realtimeStore = loadRealtimeStoreFromDisk() || realtimeStore;
    if (!realtimeStore) return res.status(404).json({ error: "realtime_not_ready" });

    res.json(realtimeStore);
  } catch (e) {
    console.error("[/api/tw/realtime] failed:", e);
    res.status(500).json({ error: "realtime_failed", message: e.message });
  }
});

// ✅ US 成交值 Top30
app.get("/api/us/top30-dollar-volume", async (req, res) => {
  try {
    const force = String(req.query.force || "") === "1";
    if (force) {
      await refreshUsDollarVolumeAndMarketCap();
    }

    if (!usTop30Store) usTop30Store = loadUsTop30FromDisk() || usTop30Store;
    if (!usTop30Store) return res.status(404).json({ error: "us_top30_not_ready" });

    res.json(usTop30Store);
  } catch (e) {
    console.error("[/api/us/top30-dollar-volume] failed:", e);
    res.status(500).json({ error: "us_top30_failed", message: e.message });
  }
});

// ✅ US 市值 Top30
app.get("/api/us/marketcap-top30", async (req, res) => {
  try {
    const force = String(req.query.force || "") === "1";
    if (force) {
      await refreshUsDollarVolumeAndMarketCap();
    }

    if (!usMarketCapStore) usMarketCapStore = loadUsMarketCapFromDisk() || usMarketCapStore;
    if (!usMarketCapStore) return res.status(404).json({ error: "us_marketcap_not_ready" });

    res.json(usMarketCapStore);
  } catch (e) {
    console.error("[/api/us/marketcap-top30] failed:", e);
    res.status(500).json({ error: "us_marketcap_failed", message: e.message });
  }
});

// ✅ 近 N 個交易日 Top30 成交值（for charts）
// /api/top30-history?days=5&force=1
let top30HistoryCache = { key: "", updatedAt: 0, data: null };

app.get("/api/top30-history", async (req, res) => {
  try {
    const days = Math.min(Math.max(parseInt(req.query.days || "5", 10), 1), 15);
    const force = String(req.query.force || "") === "1";

    const key = `days=${days}`;
    const now = Date.now();
    const cacheTTL = 10 * 60 * 1000;

    if (!force && top30HistoryCache.key === key && top30HistoryCache.data && (now - top30HistoryCache.updatedAt) < cacheTTL) {
      return res.json(top30HistoryCache.data);
    }

    const packs = await buildTop30HistoryPacks(days, 30);
    top30HistoryCache = { key, updatedAt: now, data: packs };

    res.json(packs);
  } catch (e) {
    console.error("[/api/top30-history] failed:", e);
    res.status(500).json({ error: "top30_history_failed", message: e.message });
  }
});

app.get("/api/health", (req, res) => res.json({
  ok: true,
  time: new Date().toISOString(),
  marketOpen: isMarketOpenTaipei(),
  realtimeIntervalMs: REALTIME_INTERVAL_MS,
  closeDateISO: closeStore?.dateISO || null,
  usTradingDate: usTop30Store?.tradingDate || null,
  hotCount: hotNewsStore?.length || 0,
  hotKeepDays: HOT_KEEP_DAYS,
  hotApiDefaultDays: HOT_API_DEFAULT_DAYS,
  hotApiMinDays: HOT_API_MIN_DAYS,
}));

app.get("/health", (req, res) => {
  res.status(200).json({ ok: true, time: new Date().toISOString() });
});

app.listen(PORT, () => console.log(`Server running on ${PORT}`));




