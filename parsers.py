import re
from bs4 import BeautifulSoup

MONTHS = ["Jan","Feb","Mar","Apr","May","Jun","Jul","Aug","Sep","Oct","Nov","Dec"]

def parse_amount(amount_text: str):
    if not amount_text:
        return None, None, None
    m = re.search(r'(?P<cur>[$€£])?\s*(?P<num>[\d\.]+)\s*(?P<unit>M|B|million|billion)', amount_text, re.I)
    if not m:
        return None, None, amount_text
    cur = m.group("cur") or "$"
    num = float(m.group("num"))
    unit = m.group("unit").lower()
    multiplier = 1e6 if unit.startswith("m") else 1e9
    amount_value = num * multiplier
    currency = {"$":"USD","€":"EUR","£":"GBP"}[cur]
    return amount_value, currency, amount_text

def parse_date_company(line: str, year: int):
    m = re.match(r'^(?P<mon>Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec)\.? ?(?P<day>\d{1,2})—(?P<co>.+)$', line)
    if not m:
        return None, None
    mon = m.group("mon"); day = int(m.group("day"))
    month_num = MONTHS.index(mon) + 1
    date_iso = f"{year:04d}-{month_num:02d}-{day:02d}"
    return date_iso, m.group("co").strip()

def extract_entries(html: str, year: int, tracker_url: str):
    soup = BeautifulSoup(html, "lxml")
    container = soup.select_one("article") or soup
    blocks = [el.get_text(" ", strip=True) for el in container.select("h2, h3, p, li")]

    entries = []
    i = 0
    while i < len(blocks):
        line = blocks[i]
        if re.match(r'^(Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec)\.? \d{1,2}—', line):
            date_iso, company = parse_date_company(line, year)
            series = amount_text = investors = blurb = None
            source_type = source_url = None

            details = []
            j = i + 1
            while j < len(blocks) and not re.match(r'^(Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec)\.? \d{1,2}—', blocks[j]):
                details.append(blocks[j]); j += 1

            for d in details:
                lower = d.lower()
                if lower.startswith("series:"):
                    series = d.split(":",1)[1].strip()
                elif lower.startswith("amount:"):
                    amount_text = d.split(":",1)[1].strip()
                elif lower.startswith("investors:"):
                    investors = d.split(":",1)[1].strip()
                elif d in ("Story", "Release"):
                    source_type = d
                else:
                    if blurb is None and len(d.split()) > 4:
                        blurb = d

            amount_value, amount_currency, normalized_amount_text = parse_amount(amount_text or "")

            # Try to find Story/Release anchors (best effort)
            anchors = container.find_all("a", string=re.compile(r"^(Story|Release)$"))
            if anchors:
                source_type = anchors[-1].get_text(strip=True)
                source_url = anchors[-1].get("href")

            entries.append({
                "year": year,
                "date": date_iso,
                "company": company,
                "series": series,
                "amount_value": amount_value,
                "amount_currency": amount_currency,
                "amount_text": normalized_amount_text,
                "investors": investors,
                "blurb": blurb,
                "source_type": source_type,
                "source_url": source_url,
                "tracker_url": tracker_url
            })
            i = j
        else:
            i += 1
    return entries
