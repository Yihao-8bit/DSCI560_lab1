from pathlib import Path
from bs4 import BeautifulSoup
import csv, re, sys, json, time
import requests
from urllib.parse import urljoin

ROOT = Path(__file__).resolve().parents[1]
RAW_HTML = ROOT / "data" / "raw_data" / "web_data.html"
OUT_DIR  = ROOT / "data" / "processed_data"
MARKET_CSV = OUT_DIR / "market_data.csv"
NEWS_CSV   = OUT_DIR / "news_data.csv"
BASE = "https://www.cnbc.com"

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0 Safari/537.36",
    "Accept-Language": "en-US,en;q=0.9",
    "Referer": "https://www.cnbc.com/",
}

SYMBOLS = (".DJI", ".SPX", ".IXIC", ".RUT", "VIX")

def T(x): return x.get_text(strip=True) if x else ""

def pos_from_pct(pct_text, cls=""):
    txt = (pct_text or "").strip()
    if re.search(r"[-−]\s*\d", txt): return "down"
    if "+" in txt: return "up"
    if "down" in cls: return "down"
    if "up" in cls: return "up"
    return "flat"

def symbol_from_href(href):
    if not href: return ""
    m = re.search(r"/quotes/([^/?#]+)", href)
    return m.group(1) if m else ""

def parse_markets_dom(soup):
    out = []
    cards = soup.select('a[class*="MarketCard-container"]')
    if not cards:
        cards = soup.select('a[href*="/quotes/"]')
    for a in cards:
        href = (a.get("href") or "").strip()
        sym = symbol_from_href(href)
        pct_el = a.select_one('[class*="changesPct"], [class*="changePct"], [class*="Pct"], [class*="pct"]')
        if not pct_el:
            for el in a.select('[class*="change"]'):
                t = el.get_text(" ", strip=True)
                if "%" in t:
                    pct_el = el; break
        pct = T(pct_el)
        if sym and pct:
            cls = " ".join(a.get("class", []))
            out.append({
                "marketCard_symbol": sym,
                "marketCard_stockPosition": pos_from_pct(pct, cls),
                "marketCard-changePct": pct
            })
    seen, dedup = set(), []
    for r in out:
        if r["marketCard_symbol"] not in seen:
            dedup.append(r); seen.add(r["marketCard_symbol"])
    return dedup

def _norm_pct(val):
    if isinstance(val, (int, float)): return f"{val:+.2f}%"
    s = str(val or "").strip()
    if s and not s.endswith("%"): s += "%"
    return s

def _pack(sym, pct_txt):
    pos = "down" if str(pct_txt).startswith("-") else ("up" if str(pct_txt).startswith("+") else "flat")
    return {
        "marketCard_symbol": sym,
        "marketCard_stockPosition": pos,
        "marketCard-changePct": pct_txt
    }

def _try_variant(url):
    r = requests.get(url, headers=HEADERS, timeout=20)
    r.raise_for_status()
    txt = r.text
    rows = []
    try:
        data = r.json()
    except Exception:
        data = None
    if isinstance(data, dict):
        fq = data.get("FormattedQuoteResult", {}).get("FormattedQuote") if "FormattedQuoteResult" in data else None
        if isinstance(fq, list) and fq:
            for it in fq:
                sym = (it.get("symbol") or "").strip()
                pct = _norm_pct(it.get("change_pct") or it.get("changePercent") or it.get("change_percentage"))
                if sym and pct: rows.append(_pack(sym, pct))
        qq = data.get("QuickQuoteResult", {}).get("QuickQuote")
        if isinstance(qq, list) and qq:
            for it in qq:
                sym = (it.get("symbol") or "").strip()
                pct = _norm_pct(it.get("change_pct") or it.get("changePercent") or it.get("change_percentage"))
                if sym and pct: rows.append(_pack(sym, pct))
        res = data.get("quoteResponse", {}).get("result") or data.get("result")
        if isinstance(res, list) and res:
            for it in res:
                sym = (it.get("symbol") or "").strip()
                pct = _norm_pct(it.get("change_pct") or it.get("changePercent") or it.get("change_percentage"))
                if sym and pct: rows.append(_pack(sym, pct))
    if not rows:
        m = re.search(r'\{(?:.|\n)*\}', txt)
        if m:
            try:
                j = json.loads(m.group(0))
            except Exception:
                j = None
            if isinstance(j, dict):
                fq = j.get("FormattedQuoteResult", {}).get("FormattedQuote")
                if isinstance(fq, list):
                    for it in fq:
                        sym = (it.get("symbol") or "").strip()
                        pct = _norm_pct(it.get("change_pct") or it.get("changePercent") or it.get("change_percentage"))
                        if sym and pct: rows.append(_pack(sym, pct))
    seen, dedup = set(), []
    for r in rows:
        k = r["marketCard_symbol"]
        if k and k not in seen:
            dedup.append(r); seen.add(k)
    return dedup

def markets_from_cnbc_multi(symbols=SYMBOLS):
    syms = "|".join(symbols)
    t = int(time.time()*1000)
    variants = [
        f"https://quote.cnbc.com/quote-html-webservice/quote.htm?symbols={syms}&noform=1&extended=1&random={t}&output=json",
        f"https://quote.cnbc.com/quote-html-webservice/quote.htm?symbols={syms}&requestMethod=quick&noform=1&partnerId=2&exthrs=1&random={t}&output=jsonp&callback=cb",
        f"https://quote.cnbc.com/quote-html-webservice/quote.htm?symbols={syms}&requestMethod=quick&noform=1&extended=1&random={t}&output=json",
        f"https://quote.cnbc.com/quote-html-webservice/quote.htm?symbols={syms}&noform=1&extended=1&random={t}&output=jsonp&callback=cb",
    ]
    for url in variants:
        try:
            rows = _try_variant(url)
            if rows:
                return rows
        except requests.RequestException:
            continue
        except Exception:
            continue
    return []

def parse_latest_news(soup):
    out=[]
    sec = soup.select_one('[class*="LatestNews"], [id*="LatestNews"]')
    if not sec: return out
    items = sec.select('li[class*="LatestNews-item"], li[data-test*="LatestNews"]')
    for li in items:
        a = li.select_one('a[class*="LatestNews-headline"], a[href]')
        title = T(a)
        link = urljoin(BASE,(a.get("href") or "").strip()) if a else ""
        t = li.find("time") or li.select_one('[class*="timestamp"], [class*="Timestamp"]')
        ts = T(t)
        if not ts:
            raw = li.get_text(" ", strip=True)
            m = re.search(r"\b\d+\s+(?:MINUTES?|HOURS?|DAYS?)\s+AGO\b", raw, re.I)
            ts = m.group(0) if m else ""
        if title and link.startswith("http"):
            out.append({"LatestNews-timestamp":ts,"title":title,"link":link})
    return out

def main():
    if not RAW_HTML.exists():
        print(f"[ERR] not found: {RAW_HTML}\nRun: python scripts/web_scraper.py", file=sys.stderr)
        sys.exit(1)
    OUT_DIR.mkdir(parents=True, exist_ok=True)
    html_text = RAW_HTML.read_text(encoding="utf-8", errors="ignore")
    soup = BeautifulSoup(html_text, "html.parser")
    markets = parse_markets_dom(soup)
    if not markets:
        markets = markets_from_cnbc_multi()
    if markets:
        with MARKET_CSV.open("w", newline="", encoding="utf-8") as f:
            w = csv.DictWriter(f, fieldnames=["marketCard_symbol","marketCard_stockPosition","marketCard-changePct"])
            w.writeheader(); w.writerows(markets)
        print(f"[OK] Market CSV created: {MARKET_CSV} (rows={len(markets)})")
    else:
        print("[ERROR] Could not obtain Market data from DOM or API.")
    print("[INFO] Filtering fields: Latest News")
    news = parse_latest_news(soup)
    if news:
        with NEWS_CSV.open("w", newline="", encoding="utf-8") as f:
            w = csv.DictWriter(f, fieldnames=["LatestNews-timestamp","title","link"])
            w.writeheader(); w.writerows(news)
        print(f"[OK] News CSV created: {NEWS_CSV} (rows={len(news)})")
    else:
        print("[WARN] Latest News section not found.")
    print("[DONE] success!!!")

if __name__ == "__main__":
    main()
