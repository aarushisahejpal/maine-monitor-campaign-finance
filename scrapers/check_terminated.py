#!/usr/bin/env python3
"""
Check for newly terminated candidates on the disclosure site.
Updates candidate_status.csv to hide them.
"""

import csv
import os
import time
import requests
from bs4 import BeautifulSoup

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
STATUS_FILE = os.path.join(SCRIPT_DIR, "..", "data", "candidate_status.csv")
FINANCE_FILE = os.path.join(SCRIPT_DIR, "..", "data", "all_candidates_finance_type.csv")
BASE_URL = "https://www.mainecampaignfinancedisclosure.com/public/filers"

HEADERS = {"User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36"}


def scrape_all_candidates():
    """Scrape the filer list for all candidate statuses."""
    session = requests.Session()
    session.headers.update(HEADERS)

    params = {"q[filer_type_key_cont_any][]": "candidate", "limit": "50"}
    all_filers = []
    page = 1

    while True:
        try:
            resp = session.get(BASE_URL, params={**params, "page": str(page)}, timeout=30)
            soup = BeautifulSoup(resp.text, "html.parser")
        except Exception as e:
            print(f"  Error on page {page}: {e}")
            break

        table = soup.find("table")
        if not table:
            break

        rows = table.find_all("tr")[1:]
        if not rows:
            break

        for tr in rows:
            tds = tr.find_all("td")
            a = tr.find("a")
            if a and len(tds) >= 3:
                all_filers.append({
                    "name": tds[0].get_text(strip=True),
                    "status": tds[2].get_text(strip=True),
                    "url": a["href"],
                })

        # Check if there are more pages
        disp = soup.find(string=lambda t: t and "Displaying" in str(t))
        if disp:
            parts = disp.strip().split()
            total = int(parts[parts.index("of") + 1].replace(",", ""))
            if page * 50 >= total:
                break
        else:
            break

        page += 1
        time.sleep(0.5)

    return all_filers


def main():
    print("Checking for terminated candidates...")

    # Scrape current statuses
    filers = scrape_all_candidates()
    print(f"  Scraped {len(filers)} candidates from disclosure site")

    # Build lookup — a candidate is only truly terminated if they have
    # NO active committees (they may have old terminated ones + a new active one)
    active_names = set()
    terminated_names = set()
    for f in filers:
        name = f["name"].lower().strip()
        if f["status"] == "Active":
            active_names.add(name)
        else:
            terminated_names.add(name)

    # Only truly terminated = terminated AND NOT active
    truly_terminated = terminated_names - active_names
    print(f"  Active: {len(active_names)}, Terminated only: {len(truly_terminated)}, Has both: {len(terminated_names & active_names)}")
    terminated = truly_terminated
    active = active_names

    # Update the finance type file
    if os.path.exists(FINANCE_FILE):
        with open(FINANCE_FILE) as f:
            finance_rows = list(csv.DictReader(f))

        updated = 0
        for r in finance_rows:
            name_lower = r["name"].lower().strip()
            # Only mark terminated if NOT in active list
            has_active = any(name_lower in a or a in name_lower for a in active)
            if not has_active and name_lower in terminated and r["status"] == "Active":
                r["status"] = "Terminated"
                updated += 1
                print(f"  NEWLY TERMINATED: {r['name']}")

        if updated:
            with open(FINANCE_FILE, "w", newline="") as f:
                writer = csv.DictWriter(f, fieldnames=finance_rows[0].keys())
                writer.writeheader()
                writer.writerows(finance_rows)
            print(f"  Updated {updated} candidates to Terminated")

    # Update candidate_status.csv
    if os.path.exists(STATUS_FILE):
        with open(STATUS_FILE) as f:
            status_rows = list(csv.DictReader(f))

        hidden = 0
        for r in status_rows:
            # Never override editorial decisions (candidates already set to "no")
            if r.get("show_on_page") == "no":
                continue
            cand_lower = r["candidate"].lower().strip()
            # Check if candidate matches any ACTIVE name — if so, skip
            has_active = any(cand_lower in a or a in cand_lower for a in active)
            if has_active:
                continue
            # Only hide if matches a terminated name AND has no active match
            has_terminated = any(cand_lower in t or t in cand_lower for t in terminated)
            if has_terminated:
                r["show_on_page"] = "no"
                hidden += 1
                print(f"  HIDDEN: {r['candidate']} ({r['race']} dist {r['district']})")

        if hidden:
            with open(STATUS_FILE, "w", newline="") as f:
                writer = csv.DictWriter(f, fieldnames=status_rows[0].keys())
                writer.writeheader()
                writer.writerows(status_rows)
            print(f"  Hidden {hidden} candidates from pages")

    if not updated and not hidden:
        print("  No changes — all candidates still active")

    print("Done!")


if __name__ == "__main__":
    main()
