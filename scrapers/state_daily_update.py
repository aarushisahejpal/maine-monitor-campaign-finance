#!/usr/bin/env python3
"""
Daily incremental scraper for Maine Campaign Finance Disclosure.
Appends new transactions since the last scrape date.
Verifies total against site to detect gaps.
"""

import csv
import time
import os
import sys
from datetime import date, timedelta
import requests
from bs4 import BeautifulSoup

BASE_URL = "https://www.mainecampaignfinancedisclosure.com/public/activities"
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
OUTPUT_FILE = os.path.join(SCRIPT_DIR, "..", "data", "state_2026", "transactions.csv")
LAST_SCRAPE_FILE = os.path.join(SCRIPT_DIR, "..", "data", "state_2026", "last_scrape_date.txt")

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
}

FIELDNAMES = ["filer_name", "filer_url", "transaction_type", "source_payee", "date", "amount"]


def build_params(page, start_date, end_date):
    params = [("q[public_search_i_cont]", "")]
    for t in ["", "contribution", "loan", "loan_forgiveness",
              "returned_expenditure", "returned_independent_expenditure",
              "", "expenditure", "independent_expenditure",
              "debt_payment", "loan_payment", "returned_contribution",
              "", "debt"]:
        params.append(("q[transaction_type_in][]", t))
    params.append(("q[filer_type_key_eq]", ""))
    params.append(("q[amount_cents_gteq]", ""))
    params.append(("q[amount_cents_lteq]", ""))
    params.append(("q[date_gteq]", start_date))
    params.append(("q[date_lteq]", end_date))
    params.append(("q[s]", "date asc"))
    params.append(("commit", "Create Search"))
    params.append(("limit", "50"))
    params.append(("page", str(page)))
    return params


def parse_page(html):
    soup = BeautifulSoup(html, "html.parser")
    total = None
    displaying = soup.find(string=lambda t: t and "Displaying items" in t)
    if displaying:
        parts = displaying.split()
        try:
            total = int(parts[parts.index("of") + 1].replace(",", ""))
        except (ValueError, IndexError):
            pass
    rows = []
    table = soup.find("table")
    if not table:
        return rows, total
    for tr in table.find_all("tr")[1:]:
        tds = tr.find_all("td")
        if len(tds) < 5:
            continue
        a_tag = tds[0].find("a")
        rows.append({
            "filer_name": tds[0].get_text(strip=True),
            "filer_url": a_tag["href"] if a_tag else "",
            "transaction_type": tds[1].get_text(strip=True),
            "source_payee": tds[2].get_text(strip=True),
            "date": tds[3].get_text(strip=True),
            "amount": tds[4].get_text(strip=True),
        })
    return rows, total


def get_site_total(session):
    """Get the total record count from the site."""
    params = build_params(1, "2025-01-01", date.today().isoformat())
    try:
        resp = session.get(BASE_URL, params=params, timeout=30)
        _, total = parse_page(resp.text)
        return total or 0
    except Exception as e:
        print(f"Warning: could not get site total: {e}")
        return 0


def scrape_window(session, start_date, end_date, seen_urls):
    """Scrape all pages for a date window. Returns new rows."""
    new_rows = []
    page = 1
    total_pages = None
    total_records = None
    max_retries = 5
    empty_retries = 0

    while True:
        params = build_params(page, start_date, end_date)
        retries = 0
        while retries < max_retries:
            try:
                resp = session.get(BASE_URL, params=params, timeout=30)
                resp.raise_for_status()
                break
            except requests.RequestException as e:
                retries += 1
                if retries >= max_retries:
                    print(f"    FAILED page {page} after {max_retries} retries: {e}")
                    return new_rows, total_records or 0
                print(f"    Error ({retries}/{max_retries}): {e}. Retrying in 10s...")
                time.sleep(10)

        rows, total = parse_page(resp.text)

        if total and total_records is None:
            total_records = total
            total_pages = (total + 49) // 50

        if not rows:
            empty_retries += 1
            if empty_retries <= 2:
                time.sleep(3)
                continue
            break

        empty_retries = 0

        for row in rows:
            url = row["filer_url"]
            if not url:
                new_rows.append(row)
                continue
            if url not in seen_urls:
                seen_urls.add(url)
                new_rows.append(row)

        if total_pages and page >= total_pages:
            break

        page += 1
        time.sleep(0.5)

    return new_rows, total_records or 0


def main():
    today = date.today()

    # Determine start date: last scrape date - 3 days (overlap for safety)
    if os.path.exists(LAST_SCRAPE_FILE):
        with open(LAST_SCRAPE_FILE) as f:
            last_date = date.fromisoformat(f.read().strip())
        start = last_date - timedelta(days=3)
    else:
        # First run: scrape everything
        start = date(2025, 1, 1)

    print(f"Daily update: {start} to {today}")

    session = requests.Session()
    session.headers.update(HEADERS)

    # Load existing URLs for dedup
    seen_urls = set()
    existing_count = 0
    if os.path.exists(OUTPUT_FILE):
        with open(OUTPUT_FILE, "r") as f:
            reader = csv.DictReader(f)
            for r in reader:
                seen_urls.add(r["filer_url"])
                existing_count += 1
    print(f"Existing records: {existing_count:,}")

    # Scrape day by day for the window
    all_new = []
    current = start
    while current <= today:
        new_rows, expected = scrape_window(
            session, current.isoformat(), current.isoformat(), seen_urls
        )
        if new_rows:
            all_new.extend(new_rows)
            print(f"  {current}: {len(new_rows):,} new rows")
        current += timedelta(days=1)

    # Append new rows
    if all_new:
        with open(OUTPUT_FILE, "a", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=FIELDNAMES)
            writer.writerows(all_new)
        print(f"\nAppended {len(all_new):,} new rows")
    else:
        print("\nNo new rows found")

    # Verify against site total
    site_total = get_site_total(session)
    our_total = existing_count + len(all_new)
    gap = site_total - our_total

    print(f"\n=== VERIFICATION ===")
    print(f"Site total:  {site_total:,}")
    print(f"Our total:   {our_total:,}")
    print(f"Gap:         {gap:,}")

    if gap > 50:
        print(f"WARNING: Gap of {gap:,} records. Consider a full re-scrape.")
        sys.exit(1)
    elif gap < 0:
        print(f"WARNING: We have more records than the site. Possible duplicates.")
        sys.exit(1)
    else:
        print("OK — counts match (within tolerance)")

    # Save last scrape date
    with open(LAST_SCRAPE_FILE, "w") as f:
        f.write(today.isoformat())

    print(f"\nDone! Total records: {our_total:,}")


if __name__ == "__main__":
    main()
