"""Verify: URL với year=2026 có data trong cells không?"""
import requests
from bs4 import BeautifulSoup

HEADERS = {'User-Agent': 'Mozilla/5.0', 'Accept': 'text/html, */*'}

for year in [2026, 2025]:
    url = f"https://s.cafef.vn/bao-cao-tai-chinh/VNM/IncSta/{year}/0/0/0/ket-qua-hoat-dong-kinh-doanh-.chn"
    r = requests.get(url, headers=HEADERS, timeout=15)
    soup = BeautifulSoup(r.text, 'html.parser')
    tables = soup.find_all('table')
    best = max(tables, key=lambda t: len(t.find_all('tr')))
    rows = best.find_all('tr')
    
    print(f"\n=== year={year} ===")
    for row in rows:
        cells = row.find_all('td')
        if cells and 'doanh thu thuần' in cells[0].get_text(strip=True).lower():
            print(f"Found! {len(cells)} cells")
            for j, c in enumerate(cells[:6]):
                print(f"  [{j}] '{c.get_text(strip=True)[:50]}'")
            break
