#!/usr/bin/env python3
"""
Daily incremental scraper for Maine Campaign Finance Disclosure.
Appends new transactions since the last scrape date.
Uses Playwright to handle Cloudflare protection.
"""

import csv
import time
import os
import sys
from datetime import date, timedelta

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
OUTPUT_FILE = os.path.join(SCRIPT_DIR, "..", "data", "state_2026", "transactions.csv")
LAST_SCRAPE_FILE = os.path.join(SCRIPT_DIR, "..", "data", "state_2026", "last_scrape_date.txt")
BASE_URL = "https://www.mainecampaignfinancedisclosure.com/public/activities"

FIELDNAMES = ["filer_name", "filer_url", "transaction_type", "source_payee", "date", "amount"]


def build_url(page, start_date, end_date):
    """Build the search URL with query params."""
    types = ["", "contribution", "loan", "loan_forgiveness",
             "returned_expenditure", "returned_independent_expenditure",
             "", "expenditure", "independent_expenditure",
             "debt_payment", "loan_payment", "returned_contribution",
             "", "debt"]
    type_params = "&".join(f"q%5Btransaction_type_in%5D%5B%5D={t}" for t in types)
    return (
        f"{BASE_URL}?"
        f"q%5Bpublic_search_i_cont%5D="
        f"&{type_params}"
        f"&q%5Bfiler_type_key_eq%5D="
        f"&q%5Bamount_cents_gteq%5D="
        f"&q%5Bamount_cents_lteq%5D="
        f"&q%5Bdate_gteq%5D={start_date}"
        f"&q%5Bdate_lteq%5D={end_date}"
        f"&q%5Bs%5D=date+asc"
        f"&commit=Create+Search"
        f"&limit=50"
        f"&page={page}"
    )


def parse_page(page):
    """Parse the current page for transaction rows and total count."""
    total = None
    displaying = page.query_selector("text=Displaying items")
    if displaying:
        text = displaying.text_content()
        parts = text.split()
        try:
            total = int(parts[parts.index("of") + 1].replace(",", ""))
        except (ValueError, IndexError):
            pass

    rows = []
    table = page.query_selector("table")
    if not table:
        return rows, total

    trs = table.query_selector_all("tr")
    for tr in trs[1:]:  # skip header
        tds = tr.query_selector_all("td")
        if len(tds) < 5:
            continue
        a_tag = tds[0].query_selector("a")
        rows.append({
            "filer_name": tds[0].text_content().strip(),
            "filer_url": a_tag.get_attribute("href") if a_tag else "",
            "transaction_type": tds[1].text_content().strip(),
            "source_payee": tds[2].text_content().strip(),
            "date": tds[3].text_content().strip(),
            "amount": tds[4].text_content().strip(),
        })
    return rows, total


def scrape_window(pw_page, start_date, end_date, seen_urls):
    """Scrape all pages for a date window. Returns new rows."""
    new_rows = []
    page_num = 1
    total_pages = None
    total_records = None
    max_retries = 5
    empty_retries = 0

    while True:
        url = build_url(page_num, start_date, end_date)
        retries = 0
        while retries < max_retries:
            try:
                pw_page.goto(url, wait_until="networkidle", timeout=60000)
                # Wait for table or "No results" to appear
                try:
                    pw_page.wait_for_selector("table, .no-results", timeout=15000)
                except:
                    pass  # Table might not exist if no results
                break
            except Exception as e:
                retries += 1
                if retries >= max_retries:
                    print(f"    FAILED page {page_num} after {max_retries} retries: {e}")
                    return new_rows, total_records or 0
                print(f"    Error ({retries}/{max_retries}): {e}. Retrying in 10s...")
                time.sleep(10)

        rows, total = parse_page(pw_page)

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
            filer_url = row["filer_url"]
            if not filer_url:
                new_rows.append(row)
                continue
            if filer_url not in seen_urls:
                seen_urls.add(filer_url)
                new_rows.append(row)

        if total_pages and page_num >= total_pages:
            break

        page_num += 1
        time.sleep(0.5)

    return new_rows, total_records or 0


def get_site_total(pw_page):
    """Get the total record count from the site."""
    url = build_url(1, "2025-01-01", date.today().isoformat())
    try:
        pw_page.goto(url, wait_until="networkidle", timeout=60000)
        try:
            pw_page.wait_for_selector("table", timeout=15000)
        except:
            pass
        _, total = parse_page(pw_page)
        return total or 0
    except Exception as e:
        print(f"Warning: could not get site total: {e}")
        return 0


def main():
    from playwright.sync_api import sync_playwright

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

    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        context = browser.new_context(
            user_agent="Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
        )
        pw_page = context.new_page()

        # Initial page load to handle any Cloudflare challenge
        print("Loading site (handling Cloudflare)...")
        pw_page.goto(BASE_URL, wait_until="networkidle", timeout=60000)
        # Wait for Cloudflare to clear and page to render
        try:
            pw_page.wait_for_selector("table", timeout=30000)
            print("Site loaded — table found")
        except:
            # Might be on a Cloudflare challenge page, wait longer
            print("Waiting for Cloudflare challenge...")
            time.sleep(10)
            pw_page.reload(wait_until="networkidle", timeout=60000)
            try:
                pw_page.wait_for_selector("table", timeout=30000)
                print("Site loaded after retry — table found")
            except:
                print("WARNING: Could not find table after Cloudflare retry")

        # Scrape day by day for the window
        all_new = []
        current = start
        while current <= today:
            new_rows, expected = scrape_window(
                pw_page, current.isoformat(), current.isoformat(), seen_urls
            )
            if new_rows:
                all_new.extend(new_rows)
                print(f"  {current}: {len(new_rows):,} new rows")
            current += timedelta(days=1)

        # Verify against site total
        site_total = get_site_total(pw_page)

        browser.close()

    # Append new rows
    if all_new:
        with open(OUTPUT_FILE, "a", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=FIELDNAMES)
            writer.writerows(all_new)
        print(f"\nAppended {len(all_new):,} new rows")
    else:
        print("\nNo new rows found")

    our_total = existing_count + len(all_new)

    print(f"\n=== VERIFICATION ===")
    print(f"Site total:  {site_total:,}")
    print(f"Our total:   {our_total:,}")
    gap = site_total - our_total
    print(f"Gap:         {gap:,}")

    if gap > 500:
        print(f"ERROR: Gap of {gap:,} records is too large. Full re-scrape needed.")
        sys.exit(1)
    elif gap > 50:
        print(f"WARNING: Gap of {gap:,} records. Will be caught up over time.")
    elif gap < 0:
        print(f"WARNING: We have more records than the site. Possible duplicates.")
    else:
        print("OK — counts match (within tolerance)")

    # Save last scrape date
    with open(LAST_SCRAPE_FILE, "w") as f:
        f.write(today.isoformat())

    print(f"\nDone! Total records: {our_total:,}")


if __name__ == "__main__":
    main()
