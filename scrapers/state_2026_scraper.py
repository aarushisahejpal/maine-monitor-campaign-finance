#!/usr/bin/env python3
"""
Maine Campaign Finance Disclosure scraper (new site)
Scrapes all transactions from 2025-01-01 to today.
Uses monthly date windows to avoid pagination instability.
Deduplicates by transaction URL.
"""

import csv
import time
import os
from datetime import date, timedelta
from dateutil.relativedelta import relativedelta
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


def build_params(page, start_date, end_date):
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


def generate_month_windows(start, end):
    """Generate (start_date, end_date) tuples for each month."""
    windows = []
    current = start
    while current < end:
        window_end = min(current + relativedelta(months=1) - timedelta(days=1), end)
        windows.append((current, window_end))
        current = current + relativedelta(months=1)
    return windows


def split_into_weeks(start, end):
    """Split a date range into weekly windows."""
    windows = []
    current = start
    while current <= end:
        window_end = min(current + timedelta(days=6), end)
        windows.append((current, window_end))
        current = window_end + timedelta(days=1)
    return windows


def scrape_window(session, start_date, end_date, seen_urls):
    """Scrape all pages for a single date window. Returns new rows."""
    new_rows = []
    page = 1
    total_pages = None
    total_records = None

    while True:
        params = build_params(page, start_date, end_date)
        try:
            resp = session.get(BASE_URL, params=params, timeout=30)
            resp.raise_for_status()
        except requests.RequestException as e:
            print(f"    Error on page {page}: {e}. Retrying in 10s...")
            time.sleep(10)
            continue

        rows, total = parse_page(resp.text)

        if total and total_records is None:
            total_records = total
            total_pages = (total + 49) // 50

        if not rows:
            break

        for row in rows:
            url = row["filer_url"]
            if url not in seen_urls:
                seen_urls.add(url)
                new_rows.append(row)

        if total_pages and page >= total_pages:
            break

        page += 1
        time.sleep(0.5)

    return new_rows, total_records or 0


def main():
    end = date.today()
    start = date(2025, 1, 1)
    print(f"Scraping state 2026 data: {start} to {end}")
    print(f"Using monthly date windows to avoid pagination issues\n")

    session = requests.Session()
    session.headers.update(HEADERS)
    os.makedirs(os.path.dirname(OUTPUT_FILE), exist_ok=True)

    MAX_PER_WINDOW = 8000  # if a window exceeds this, split smaller

    windows = generate_month_windows(start, end)
    all_rows = []
    seen_urls = set()

    for i, (w_start, w_end) in enumerate(windows):
        label = f"[{i+1}/{len(windows)}] {w_start} to {w_end}"

        # First, check how many records this window has
        params = build_params(1, w_start.isoformat(), w_end.isoformat())
        resp = session.get(BASE_URL, params=params, timeout=30)
        _, window_total = parse_page(resp.text)
        window_total = window_total or 0

        if window_total > MAX_PER_WINDOW:
            # Too many — split into weekly windows
            weeks = split_into_weeks(w_start, w_end)
            print(f"{label} ({window_total:,} records — splitting into {len(weeks)} weeks)")
            for j, (ws, we) in enumerate(weeks):
                # Check if this week also needs splitting into days
                params_check = build_params(1, ws.isoformat(), we.isoformat())
                resp_check = session.get(BASE_URL, params=params_check, timeout=30)
                _, week_total = parse_page(resp_check.text)
                week_total = week_total or 0

                if week_total > MAX_PER_WINDOW:
                    # Split into individual days
                    print(f"  Week {j+1}/{len(weeks)}: {ws} to {we} ({week_total:,} — splitting into days)")
                    d = ws
                    while d <= we:
                        print(f"    {d}...", end=" ", flush=True)
                        new_rows, expected = scrape_window(session, d.isoformat(), d.isoformat(), seen_urls)
                        all_rows.extend(new_rows)
                        print(f"{len(new_rows):,} rows (site: {expected:,})")
                        d += timedelta(days=1)
                else:
                    print(f"  Week {j+1}/{len(weeks)}: {ws} to {we}...", end=" ", flush=True)
                    new_rows, expected = scrape_window(session, ws.isoformat(), we.isoformat(), seen_urls)
                    all_rows.extend(new_rows)
                    print(f"{len(new_rows):,} rows (site: {expected:,})")
        else:
            print(f"{label}...", end=" ", flush=True)
            new_rows, expected = scrape_window(session, w_start.isoformat(), w_end.isoformat(), seen_urls)
            all_rows.extend(new_rows)
            print(f"{len(new_rows):,} new rows (site: {expected:,})")

    # Write results
    with open(OUTPUT_FILE, "w", newline="", encoding="utf-8") as csvfile:
        writer = csv.DictWriter(csvfile, fieldnames=FIELDNAMES)
        writer.writeheader()
        writer.writerows(all_rows)

    print(f"\nDone! {len(all_rows):,} unique rows saved to {OUTPUT_FILE}")


if __name__ == "__main__":
    main()
