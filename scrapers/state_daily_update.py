#!/usr/bin/env python3
"""
Daily incremental scraper for Maine Campaign Finance Disclosure.
Appends new transactions since the last scrape date.
Uses stealth Playwright with fresh browser per page to bypass Cloudflare.
"""

import csv
import time
import os
import sys
import random
from datetime import date, timedelta

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
OUTPUT_FILE = os.path.join(SCRIPT_DIR, "..", "data", "state_2026", "transactions.csv")
LAST_SCRAPE_FILE = os.path.join(SCRIPT_DIR, "..", "data", "state_2026", "last_scrape_date.txt")
BASE_URL = "https://www.mainecampaignfinancedisclosure.com/public/activities"

FIELDNAMES = ["filer_name", "filer_url", "transaction_type", "source_payee", "date", "amount"]

BROWSER_ARGS = ['--disable-blink-features=AutomationControlled', '--no-sandbox']
USER_AGENT = 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36'
ANTI_DETECT_SCRIPT = 'Object.defineProperty(navigator, "webdriver", {get: () => undefined})'


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


def fetch_page(playwright, url):
    """Fetch a single page using a fresh stealth browser instance."""
    browser = playwright.chromium.launch(headless=True, args=BROWSER_ARGS)
    context = browser.new_context(
        user_agent=USER_AGENT,
        viewport={'width': 1920, 'height': 1080},
        locale='en-US',
    )
    page = context.new_page()
    page.add_init_script(ANTI_DETECT_SCRIPT)

    try:
        page.goto(url, wait_until="networkidle", timeout=60000)
        time.sleep(random.uniform(2, 4))
        page.wait_for_selector("table", timeout=15000)
        rows, total = parse_page(page)
        return rows, total
    except Exception:
        return [], None
    finally:
        browser.close()


def scrape_date_range(playwright, start_date, end_date, seen_urls):
    """Scrape all pages for a date range using fresh browser per page."""
    all_rows = []
    page_num = 1

    while True:
        url = build_url(page_num, start_date, end_date)
        rows, total = fetch_page(playwright, url)

        if not rows:
            break

        for row in rows:
            filer_url = row["filer_url"]
            if not filer_url or filer_url not in seen_urls:
                if filer_url:
                    seen_urls.add(filer_url)
                all_rows.append(row)

        if total:
            total_pages = (total + 49) // 50
            if page_num >= total_pages:
                break

        page_num += 1
        time.sleep(random.uniform(3, 6))

    return all_rows, total or 0


def main():
    from playwright.sync_api import sync_playwright

    today = date.today()

    # Wider window: last scrape date - 30 days to catch backfilled data
    if os.path.exists(LAST_SCRAPE_FILE):
        with open(LAST_SCRAPE_FILE) as f:
            last_date = date.fromisoformat(f.read().strip())
        start = last_date - timedelta(days=30)
    else:
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

    grand_total = 0

    with sync_playwright() as p:
        # Scrape week by week for the window
        current = start
        while current <= today:
            week_end = min(current + timedelta(days=6), today)
            week_str = f"{current} to {week_end}"

            new_rows, site_total = scrape_date_range(
                p, current.isoformat(), week_end.isoformat(), seen_urls
            )

            # Save after each week so we don't lose progress
            if new_rows:
                with open(OUTPUT_FILE, "a", newline="", encoding="utf-8") as f:
                    writer = csv.DictWriter(f, fieldnames=FIELDNAMES)
                    writer.writerows(new_rows)
                grand_total += len(new_rows)
                print(f"  {week_str}: {len(new_rows)} new — SAVED (total: {grand_total:,})")
            else:
                print(f"  {week_str}: ok")

            current = week_end + timedelta(days=1)
            time.sleep(random.uniform(2, 4))

    our_total = existing_count + grand_total
    print(f"\nDone! {grand_total:,} new rows. Total records: {our_total:,}")

    # Save last scrape date
    with open(LAST_SCRAPE_FILE, "w") as f:
        f.write(today.isoformat())


if __name__ == "__main__":
    main()
