#!/usr/bin/env python3
"""
Scrape MCEA filing reports from the Maine Campaign Finance Disclosure site.
Pulls MCEA payment data and seed money details from each MCEA candidate's filings.
Uses stealth Playwright with fresh browser per page to bypass Cloudflare.
"""

import csv
import json
import time
import os
import sys
import random
from playwright.sync_api import sync_playwright

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
FINANCE_FILE = os.path.join(SCRIPT_DIR, "..", "data", "all_candidates_finance_type.csv")
OUTPUT_FILE = os.path.join(SCRIPT_DIR, "..", "data", "mcea_filings.csv")

BROWSER_ARGS = ['--disable-blink-features=AutomationControlled', '--no-sandbox']
USER_AGENT = 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36'
ANTI_DETECT_SCRIPT = 'Object.defineProperty(navigator, "webdriver", {get: () => undefined})'

FILING_FIELDS = [
    'candidate_name', 'filer_url', 'filing_name', 'filing_url', 'date_filed',
    'report_type', 'reporting_period', 'total_contributions', 'total_monetary_contributions',
    'total_inkind_contributions', 'total_expenditures', 'total_mcea_payments',
    'return_of_mcea_funds', 'balance_beginning', 'balance_end', 'debt_balance',
    'unitemized_total'
]


def fetch_page(playwright, url):
    """Fetch a single page using a fresh stealth browser instance. Returns page text or None."""
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
        text = page.text_content('body') or ''
        if 'Just a moment' in text:
            return None, None
        # Also get links for filing URLs
        links = page.query_selector_all('a[href*="/filings/"]')
        filing_links = []
        for link in links:
            href = link.get_attribute('href')
            link_text = link.text_content().strip()
            if href and '/filings/' in href:
                full_url = f'https://www.mainecampaignfinancedisclosure.com{href}' if href.startswith('/') else href
                filing_links.append((link_text, full_url))
        return text, filing_links
    except Exception as e:
        return None, None
    finally:
        browser.close()


def parse_filing(text):
    """Parse a filing report page for key financial data."""
    lines = [l.strip() for l in text.split('\n') if l.strip()]
    data = {}

    def find_value(label):
        """Find dollar value after a label."""
        for i, line in enumerate(lines):
            if label.lower() in line.lower():
                for j in range(i + 1, min(i + 3, len(lines))):
                    val = lines[j].strip()
                    if val.startswith('$'):
                        return val
                    if val.replace(',', '').replace('.', '').replace('-', '').isdigit():
                        return f'${val}'
        return ''

    def find_text(label):
        """Find text value after a label."""
        for i, line in enumerate(lines):
            if label.lower() in line.lower():
                for j in range(i + 1, min(i + 3, len(lines))):
                    val = lines[j].strip()
                    if val and val != label and not val.startswith('$'):
                        return val
        return ''

    data['report_type'] = find_text('Report Type')
    data['reporting_period'] = find_text('Reporting Period')
    data['total_contributions'] = find_value('Total Contributions')
    data['total_monetary_contributions'] = find_value('Total Monetary Contributions')
    data['total_inkind_contributions'] = find_value('Total In-Kind Contributions')
    data['total_expenditures'] = find_value('Total Expenditures')
    data['total_mcea_payments'] = find_value('Total MCEA Payments')
    data['return_of_mcea_funds'] = find_value('Return of MCEA Funds')
    data['balance_beginning'] = find_value('Balance at the Beginning')
    data['balance_end'] = find_value('Balance at the End')
    data['debt_balance'] = find_value('Debt Balance')
    data['unitemized_total'] = find_value('Unitemized Total')

    return data


def main():
    # Load MCEA candidates
    mcea_candidates = []
    with open(FINANCE_FILE) as f:
        for r in csv.DictReader(f):
            if r.get('finance_type') == 'MCEA' and r.get('url'):
                mcea_candidates.append(r)

    print(f"MCEA candidates with URLs: {len(mcea_candidates)}")

    all_filings = []

    with sync_playwright() as p:
        for i, cand in enumerate(mcea_candidates):
            name = cand['name']
            filer_url = cand['url']

            print(f"\n[{i+1}/{len(mcea_candidates)}] {name}")

            # Get filer profile page to find filing links
            text, filing_links = fetch_page(p, filer_url)
            if text is None:
                print(f"  BLOCKED")
                continue

            if not filing_links:
                print(f"  No filings found")
                continue

            # Check for pagination — might have more filings
            if 'Displaying items' in text:
                # Try page 2 of filings
                page2_url = filer_url + '?page=2'
                text2, more_links = fetch_page(p, page2_url)
                if more_links:
                    filing_links.extend(more_links)
                time.sleep(random.uniform(2, 4))

            print(f"  {len(filing_links)} filings")

            # Scrape each filing
            for fname, furl in filing_links:
                time.sleep(random.uniform(3, 6))

                filing_text, _ = fetch_page(p, furl)
                if filing_text is None:
                    print(f"    {fname}: BLOCKED")
                    continue

                data = parse_filing(filing_text)
                data['candidate_name'] = name
                data['filer_url'] = filer_url
                data['filing_name'] = fname
                data['filing_url'] = furl
                data['date_filed'] = ''  # Will be on the profile page

                mcea_amt = data.get('total_mcea_payments', '')
                contrib = data.get('total_contributions', '')
                exp = data.get('total_expenditures', '')
                rtype = data.get('report_type', '')

                print(f"    {fname}: type={rtype}, mcea={mcea_amt}, contrib={contrib}, exp={exp}")

                all_filings.append(data)

            # Save after each candidate
            with open(OUTPUT_FILE, 'w', newline='', encoding='utf-8') as f:
                writer = csv.DictWriter(f, fieldnames=FILING_FIELDS)
                writer.writeheader()
                writer.writerows(all_filings)

            time.sleep(random.uniform(2, 4))

    print(f"\nDone! {len(all_filings)} filings scraped from {len(mcea_candidates)} candidates")
    print(f"Saved to {OUTPUT_FILE}")

    # Summary
    mcea_total = 0
    for f in all_filings:
        amt = f.get('total_mcea_payments', '').replace('$', '').replace(',', '')
        if amt:
            try:
                mcea_total += float(amt)
            except:
                pass
    print(f"Total MCEA payments found: ${mcea_total:,.2f}")


if __name__ == '__main__':
    main()
