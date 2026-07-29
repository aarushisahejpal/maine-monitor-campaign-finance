#!/usr/bin/env python3
"""
Scrape filing summary totals from Maine Campaign Finance Disclosure filer pages.
Uses Playwright stealth to bypass Cloudflare, same approach as state_daily_update.py.

Reads filer URLs from data/state_2026/filer_urls.csv and outputs
data/state_2026/filing_summaries.csv with the financial summary from each filer's page.
"""

import csv
import time
import os
import sys
import random
import re
from datetime import date

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
FILER_URLS_FILE = os.path.join(SCRIPT_DIR, "..", "data", "state_2026", "filer_urls.csv")
OUTPUT_FILE = os.path.join(SCRIPT_DIR, "..", "data", "state_2026", "filing_summaries.csv")

BROWSER_ARGS = ['--disable-blink-features=AutomationControlled', '--no-sandbox']
USER_AGENT = 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36'
ANTI_DETECT_SCRIPT = 'Object.defineProperty(navigator, "webdriver", {get: () => undefined})'

FIELDNAMES = [
    "filer_name", "filer_url",
    "total_contributions", "total_monetary_contributions", "total_inkind_contributions",
    "total_expenditures", "current_cash_balance", "current_loan_balance", "current_debt_balance",
    "latest_filing_name", "latest_filing_date",
    "scrape_date"
]


def parse_dollar(text):
    """Parse a dollar string like '$2,929,306.73' to float."""
    cleaned = re.sub(r'[^\d.\-]', '', text)
    try:
        return float(cleaned)
    except ValueError:
        return None


def scrape_filer_page(playwright, url, filer_name):
    """Scrape a single filer page for financial summary and latest filing info."""
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

        result = {
            "filer_name": filer_name,
            "filer_url": url,
            "scrape_date": date.today().isoformat(),
        }

        # Extract financial summary - look for the summary section
        # The page has labels like "Total Contributions" followed by dollar amounts
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

        # Extract latest filing info from the filings table
        # Look for filing rows - typically a table with Filing Name, Date Filed, Version
        filing_rows = page.query_selector_all("table tr")
        latest_filing_name = None
        latest_filing_date = None

        for tr in filing_rows[1:]:  # skip header
            tds = tr.query_selector_all("td")
            if len(tds) >= 2:
                fname = tds[0].text_content().strip()
                fdate = tds[1].text_content().strip()
                if fname and fdate and not latest_filing_name:
                    latest_filing_name = fname
                    latest_filing_date = fdate
                    break

        result["latest_filing_name"] = latest_filing_name
        result["latest_filing_date"] = latest_filing_date

        return result

    except Exception as e:
        print(f"  Error scraping {filer_name}: {e}", file=sys.stderr)
        return None
    finally:
        browser.close()


def main():
    from playwright.sync_api import sync_playwright

    if not os.path.exists(FILER_URLS_FILE):
        print(f"Error: {FILER_URLS_FILE} not found.", file=sys.stderr)
        print("Create it with columns: filer_name,filer_url", file=sys.stderr)
        sys.exit(1)

    # Read filer URLs
    filers = []
    with open(FILER_URLS_FILE) as f:
        reader = csv.DictReader(f)
        for row in reader:
            filers.append(row)

    print(f"Scraping filing summaries for {len(filers)} filers...")

    results = []
    with sync_playwright() as pw:
        for i, filer in enumerate(filers):
            name = filer["filer_name"]
            url = filer["filer_url"]
            print(f"  [{i+1}/{len(filers)}] {name}...")

            result = scrape_filer_page(pw, url, name)
            if result:
                results.append(result)
                print(f"    Total contributions: ${result.get('total_contributions', 'N/A')}")
            else:
                print(f"    FAILED")

            time.sleep(random.uniform(3, 6))

    # Write results
    with open(OUTPUT_FILE, "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=FIELDNAMES)
        writer.writeheader()
        writer.writerows(results)

    print(f"\nWrote {len(results)} filing summaries to {OUTPUT_FILE}")


if __name__ == "__main__":
    main()
