#!/usr/bin/env python3
"""
Scrape filing summary totals from Maine Campaign Finance Disclosure filer pages.
Uses Playwright stealth to bypass Cloudflare, same approach as state_daily_update.py.

Reads filer URLs from data/state_2026/all_filer_urls_2026.csv (filtered to
Representative, Senator, Governor) and outputs data/state_2026/filing_summaries.csv.
"""

import csv
import html
import time
import os
import sys
import random
import re
from datetime import date

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
FILER_URLS_FILE = os.path.join(SCRIPT_DIR, "..", "data", "state_2026", "all_filer_urls_2026.csv")
OUTPUT_FILE = os.path.join(SCRIPT_DIR, "..", "data", "state_2026", "filing_summaries.csv")

BROWSER_ARGS = ['--disable-blink-features=AutomationControlled', '--no-sandbox']
USER_AGENT = 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36'
ANTI_DETECT_SCRIPT = 'Object.defineProperty(navigator, "webdriver", {get: () => undefined})'

FILING_PERIOD_ENDS = {
    "42-Day Post-Primary Report": "07/14/2026",
    "42-Day Post-Primary MCEA Report": "07/14/2026",
    "11-Day Pre-Primary Report": "05/22/2026",
    "11-Day Pre-Primary MCEA Report": "05/22/2026",
    "42-Day Pre-Primary Report": "04/21/2026",
    "42-Day Pre-Primary MCEA Report": "04/21/2026",
}

STATE_OFFICES = {"Representative", "Senator", "Governor"}

FIELDNAMES = [
    "filer_name", "filer_url",
    "total_contributions", "total_monetary_contributions", "total_inkind_contributions",
    "total_expenditures", "current_cash_balance", "current_loan_balance", "current_debt_balance",
    "latest_filing_name", "latest_filing_date", "filing_period_end",
    "scrape_date"
]


def parse_dollar(text):
    cleaned = re.sub(r'[^\d.\-]', '', text)
    try:
        return float(cleaned)
    except ValueError:
        return None


def scrape_filer_page(page, url, filer_name):
    try:
        page.goto(url, wait_until="networkidle", timeout=60000)
        time.sleep(random.uniform(1.5, 3))

        result = {
            "filer_name": filer_name,
            "filer_url": url,
            "scrape_date": date.today().isoformat(),
        }

        body_text = page.text_content("body") or ""

        summary_fields = {
            "Total Contributions": "total_contributions",
            "Total Monetary Contributions": "total_monetary_contributions",
            "Total In-Kind Contributions": "total_inkind_contributions",
            "Total Expenditures": "total_expenditures",
            "Current Cash Balance": "current_cash_balance",
            "Current Loan Balance": "current_loan_balance",
            "Current Debt Balance": "current_debt_balance",
        }

        for label, key in summary_fields.items():
            pattern = re.compile(re.escape(label) + r'\s*\$?([\d,]+\.?\d*)')
            match = pattern.search(body_text)
            if match:
                result[key] = parse_dollar(match.group(1))
            else:
                result[key] = None

        filing_rows = page.query_selector_all("table tr")
        latest_filing_name = None
        latest_filing_date = None

        for tr in filing_rows[1:]:
            tds = tr.query_selector_all("td")
            if len(tds) >= 2:
                fname = tds[0].text_content().strip()
                fdate = tds[1].text_content().strip()
                if fname and fdate and html.unescape(fname) != html.unescape(filer_name):
                    latest_filing_name = fname
                    latest_filing_date = fdate
                    break

        result["latest_filing_name"] = latest_filing_name
        result["latest_filing_date"] = latest_filing_date
        result["filing_period_end"] = FILING_PERIOD_ENDS.get(latest_filing_name, "")

        return result

    except Exception as e:
        print(f"  Error scraping {filer_name}: {e}", file=sys.stderr)
        return None


def main():
    from playwright.sync_api import sync_playwright

    if not os.path.exists(FILER_URLS_FILE):
        print(f"Error: {FILER_URLS_FILE} not found.", file=sys.stderr)
        sys.exit(1)

    filers = []
    with open(FILER_URLS_FILE) as f:
        reader = csv.DictReader(f)
        for row in reader:
            if row.get("office", "").strip() in STATE_OFFICES:
                filers.append(row)

    already_done = set()
    if os.path.exists(OUTPUT_FILE):
        with open(OUTPUT_FILE) as f:
            reader = csv.DictReader(f)
            for row in reader:
                already_done.add(row.get("filer_url", ""))
        print(f"Resuming — {len(already_done)} already scraped")

    remaining = [f for f in filers if f["filer_url"] not in already_done]
    print(f"Scraping filing summaries for {len(remaining)} of {len(filers)} state filers...")

    write_header = not os.path.exists(OUTPUT_FILE) or os.path.getsize(OUTPUT_FILE) == 0
    outfile = open(OUTPUT_FILE, "a", newline="")
    writer = csv.DictWriter(outfile, fieldnames=FIELDNAMES)
    if write_header:
        writer.writeheader()

    success_count = len(already_done)
    failed = []

    BATCH_SIZE = 5

    with sync_playwright() as pw:
        browser = None
        for i, filer in enumerate(remaining):
            name = filer["name"]
            url = filer["filer_url"]

            if i % BATCH_SIZE == 0:
                if browser:
                    browser.close()
                browser = pw.chromium.launch(headless=True, args=BROWSER_ARGS)

            print(f"  [{i+1}/{len(remaining)}] {name}...", flush=True)

            context = browser.new_context(
                user_agent=USER_AGENT,
                viewport={'width': 1920, 'height': 1080},
                locale='en-US',
            )
            page = context.new_page()
            page.add_init_script(ANTI_DETECT_SCRIPT)

            result = scrape_filer_page(page, url, name)

            if result and result.get("total_contributions") is not None:
                writer.writerow(result)
                outfile.flush()
                success_count += 1
                tc = result.get('total_contributions', 0) or 0
                print(f"    ${tc:,.2f}", flush=True)
            elif result:
                writer.writerow(result)
                outfile.flush()
                success_count += 1
                print(f"    No financial data", flush=True)
            else:
                failed.append(name)
                print(f"    FAILED", flush=True)

            context.close()
            time.sleep(random.uniform(1, 2))

        if browser:
            browser.close()

    outfile.close()
    print(f"\nDone — {success_count} total filing summaries in {OUTPUT_FILE}")
    if failed:
        print(f"Failed: {len(failed)} — {', '.join(failed)}")


if __name__ == "__main__":
    main()
