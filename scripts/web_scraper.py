from pathlib import Path
import requests

URL = "https://www.cnbc.com/world/?region=world"
OUT = Path(__file__).resolve().parents[1] / "data"/ "raw_data" / "web_data.html"

def main():
    OUT.parent.mkdir(parents=True, exist_ok=True)
    headers = {"User-Agent":"Mozilla/5.0"}
    r = requests.get(URL, headers=headers, timeout=20)
    r.raise_for_status()
    OUT.write_text(r.text, encoding="utf-8")
    print(f"Save:{OUT}")

if __name__ == "__main__":
    main()

