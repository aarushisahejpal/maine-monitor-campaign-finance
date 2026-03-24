#!/usr/bin/env python3
"""
Maine Campaign Finance Disclosure scraper (new site)
Scrapes all transactions from 2025-01-01 to today.
Used by GitHub Action weekly to refresh 2026 cycle state + governor data.
"""

import csv
import time
import os
from datetime import date
import requests
from bs4 import BeautifulSoup

BASE_URL = "https://www.mainecampaignfinancedisclosure.com/public/activities"
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
OUTPUT_FILE = os.path.join(SCRIPT_DIR, "..", "data", "state_2026", "transactions.csv")

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
}

FIELDNAMES = ["filer_name", "filer_url", "transaction_type", "source_payee", "date", "amount"]


def build_params(page, end_date):
    params = []
    params.append(("q[public_search_i_cont]", ""))
    for t in ["", "contribution", "loan", "loan_forgiveness",
              "returned_expenditure", "returned_independent_expenditure",
              "", "expenditure", "independent_expenditure",
              "debt_payment", "loan_payment", "returned_contribution",
              "", "debt"]:
        params.append(("q[transaction_type_in][]", t))
    params.append(("q[filer_type_key_eq]", ""))
    params.append(("q[amount_cents_gteq]", ""))
    params.append(("q[amount_cents_lteq]", ""))
    params.append(("q[date_gteq]", "2025-01-01"))
    params.append(("q[date_lteq]", end_date))
    params.append(("q[s]", "filer_name asc"))
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


def main():
    end_date = date.today().isoformat()
    print(f"Scraping state 2026 data: 2025-01-01 to {end_date}")

    session = requests.Session()
    session.headers.update(HEADERS)
    os.makedirs(os.path.dirname(OUTPUT_FILE), exist_ok=True)

    with open(OUTPUT_FILE, "w", newline="", encoding="utf-8") as csvfile:
        writer = csv.DictWriter(csvfile, fieldnames=FIELDNAMES)
        writer.writeheader()

        page = 1
        total_pages = None
        total_records = None

        while True:
            params = build_params(page, end_date)
            try:
                resp = session.get(BASE_URL, params=params, timeout=30)
                resp.raise_for_status()
            except requests.RequestException as e:
                print(f"  Error on page {page}: {e}. Retrying in 10s...")
                time.sleep(10)
                continue

            rows, total = parse_page(resp.text)

            if total and total_records is None:
                total_records = total
                total_pages = (total + 49) // 50
                print(f"Total records: {total_records:,} | Total pages: {total_pages:,}")

            if not rows:
                print(f"Page {page}: no rows found. Stopping.")
                break

            writer.writerows(rows)
            csvfile.flush()

            if page % 100 == 0 or page == total_pages:
                pct = page / total_pages * 100 if total_pages else 0
                print(f"Page {page}/{total_pages or '?'} | {pct:.1f}% done")

            if total_pages and page >= total_pages:
                break

            page += 1
            time.sleep(0.5)

    print(f"\nDone! Data saved to {OUTPUT_FILE}")


if __name__ == "__main__":
    main()
