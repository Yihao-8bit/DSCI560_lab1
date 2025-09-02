from pathlib import Path
import time, requests

URL = "https://www.cnbc.com/world/?region=world"

ROOT = Path(__file__).resolve().parents[1]
RAW_DIR = ROOT / "data" / "raw_data"
PROC_DIR = ROOT / "data" / "processed_data"
RAW_HTML = RAW_DIR / "web_data.html"

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0 Safari/537.36",
    "Accept-Language": "en-US,en;q=0.9",
}

def fetch(url, retries=3, backoff=1.6):
    last = None
    for i in range(retries):
        try:
            r = requests.get(url, headers=HEADERS, timeout=25)
            r.raise_for_status()
            r.encoding = r.encoding or "utf-8"
            return r.text
        except requests.RequestException as e:
            last = e
            if i < retries - 1:
                time.sleep(backoff * (2 ** i))
    raise last

def main():
    RAW_DIR.mkdir(parents=True, exist_ok=True)
    PROC_DIR.mkdir(parents=True, exist_ok=True)
    print("[INFO] Downloading HTML ...")
    html = fetch(URL)
    RAW_HTML.write_text(html, encoding="utf-8")
    print(f"[OK] Saved to: {RAW_HTML}")
    print("\n[INFO] First 10 lines of web_data.html:")
    for i, line in enumerate(html.splitlines()[:10], 1):
        print(f"{i:>2}: {line}")

if __name__ == "__main__":
    main()
