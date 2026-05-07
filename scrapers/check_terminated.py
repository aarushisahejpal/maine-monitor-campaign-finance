#!/usr/bin/env python3
"""
Check for newly terminated candidates on the disclosure site.
Downloads the filers CSV from the disclosure site (no scraping needed),
then updates candidate_status.csv and all_candidates_finance_type.csv.
"""

import csv
import io
import os
import re
import requests

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
STATUS_FILE = os.path.join(SCRIPT_DIR, "..", "data", "candidate_status.csv")
FINANCE_FILE = os.path.join(SCRIPT_DIR, "..", "data", "all_candidates_finance_type.csv")

FILERS_CSV_URL = (
    "https://www.mainecampaignfinancedisclosure.com/public/filers.csv"
    "?q%5Bsearch_i_cont%5D="
    "&q%5Bfiler_type_key_cont_any%5D%5B%5D=candidate"
    "&commit=Create+Search"
)

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36"
}


def clean_name(name):
    """Lowercase, remove all punctuation, squish spaces (matches R pipeline)."""
    n = name.strip().lower()
    n = re.sub(r"[^\w\s]", "", n)
    n = re.sub(r"\s+", " ", n).strip()
    return n


def download_filers_csv():
    """Download the candidate filers CSV from the disclosure site."""
    print("  Downloading filers CSV from disclosure site...")
    resp = requests.get(FILERS_CSV_URL, headers=HEADERS, timeout=30)
    resp.raise_for_status()

    reader = csv.DictReader(io.StringIO(resp.text))
    filers = list(reader)
    print(f"  Downloaded {len(filers)} filer records")
    return filers


def main():
    print("Checking for terminated candidates...")

    filers = download_filers_csv()

    # Build active/terminated lookups
    # A candidate is only truly terminated if ALL their filings are terminated
    active_names = set()
    terminated_names = set()
    for f in filers:
        name = clean_name(f["name"])
        if f["status"] == "Active":
            active_names.add(name)
        else:
            terminated_names.add(name)

    truly_terminated = terminated_names - active_names
    print(
        f"  Active: {len(active_names)}, "
        f"Terminated only: {len(truly_terminated)}, "
        f"Has both: {len(terminated_names & active_names)}"
    )

    # Update all_candidates_finance_type.csv
    updated_finance = 0
    if os.path.exists(FINANCE_FILE):
        with open(FINANCE_FILE) as f:
            finance_rows = list(csv.DictReader(f))

        for r in finance_rows:
            name = clean_name(r["name"])
            has_active = name in active_names
            if not has_active and name in truly_terminated and r["status"] == "Active":
                r["status"] = "Terminated"
                updated_finance += 1
                print(f"  NEWLY TERMINATED: {r['name']}")

        if updated_finance:
            with open(FINANCE_FILE, "w", newline="") as f:
                writer = csv.DictWriter(f, fieldnames=finance_rows[0].keys())
                writer.writeheader()
                writer.writerows(finance_rows)
            print(f"  Updated {updated_finance} candidates to Terminated in finance file")

    # Update candidate_status.csv
    hidden = 0
    if os.path.exists(STATUS_FILE):
        with open(STATUS_FILE) as f:
            status_rows = list(csv.DictReader(f))

        for r in status_rows:
            if r.get("show_on_page") == "no":
                continue
            name = clean_name(r["candidate"])
            if name in active_names:
                continue
            if name in truly_terminated:
                r["show_on_page"] = "no"
                hidden += 1
                print(f"  HIDDEN: {r['candidate']} ({r['race']} dist {r['district']})")

        if hidden:
            with open(STATUS_FILE, "w", newline="") as f:
                writer = csv.DictWriter(f, fieldnames=status_rows[0].keys())
                writer.writeheader()
                writer.writerows(status_rows)
            print(f"  Hidden {hidden} candidates from pages")

    if not updated_finance and not hidden:
        print("  No changes — all candidates still active")

    # Report any new active filers not in our data
    if os.path.exists(FINANCE_FILE):
        our_names = set(clean_name(r["name"]) for r in finance_rows)
        new_active = active_names - our_names
        if new_active:
            print(f"\n  NEW active filers not in our data ({len(new_active)}):")
            for name in sorted(new_active):
                print(f"    {name}")
            print("  (These may need finance type checked manually)")

    print("Done!")


if __name__ == "__main__":
    main()
