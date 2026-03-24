#!/usr/bin/env python3
"""
FEC scraper for Maine federal candidates.
Pulls Schedule A (receipts/contributions) and Schedule B (disbursements/expenditures).
Supports resuming from where it left off.

Usage (run from repo root):
    python scrapers/fec_scraper.py 2026 a        # receipts
    python scrapers/fec_scraper.py 2026 b        # expenditures
    python scrapers/fec_scraper.py 2024 a
    python scrapers/fec_scraper.py 2024 b
"""

import json, csv, time, os, sys
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

API_KEY = os.environ.get('FEC_API_KEY', 'DEMO_KEY')

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
DATA_DIR = os.path.join(SCRIPT_DIR, "..", "data")

RECEIPT_FIELDS = [
    'candidate_name', 'candidate_id', 'office', 'district', 'party', 'committee_id',
    'contributor_name', 'contributor_city', 'contributor_state', 'contributor_zip',
    'contributor_employer', 'contributor_occupation',
    'contribution_receipt_date', 'contribution_receipt_amount',
    'receipt_type_description', 'memo_text', 'line_number_label'
]

DISBURSEMENT_FIELDS = [
    'candidate_name', 'candidate_id', 'office', 'district', 'party', 'committee_id',
    'recipient_name', 'recipient_city', 'recipient_state', 'recipient_zip',
    'disbursement_description', 'disbursement_purpose_category',
    'disbursement_date', 'disbursement_amount',
    'disbursement_type_description', 'memo_text', 'line_number_label'
]


def make_session():
    s = requests.Session()
    retries = Retry(total=5, backoff_factor=2, status_forcelist=[429, 500, 502, 503, 504])
    s.mount('https://', HTTPAdapter(max_retries=retries))
    return s


def load_checkpoint(path):
    if os.path.exists(path):
        with open(path) as f:
            return json.load(f)
    return {'done_committees': []}


def save_checkpoint(path, data):
    with open(path, 'w') as f:
        json.dump(data, f)


def row_from_receipt(cand, r):
    return {
        'candidate_name': cand['name'],
        'candidate_id': cand['candidate_id'],
        'office': cand['office'],
        'district': cand['district'],
        'party': cand['party'],
        'committee_id': cand['committee_id'],
        'contributor_name': r.get('contributor_name', ''),
        'contributor_city': r.get('contributor_city', ''),
        'contributor_state': r.get('contributor_state', ''),
        'contributor_zip': r.get('contributor_zip', ''),
        'contributor_employer': r.get('contributor_employer', ''),
        'contributor_occupation': r.get('contributor_occupation', ''),
        'contribution_receipt_date': r.get('contribution_receipt_date', ''),
        'contribution_receipt_amount': r.get('contribution_receipt_amount', ''),
        'receipt_type_description': r.get('receipt_type_description', ''),
        'memo_text': r.get('memo_text', ''),
        'line_number_label': r.get('line_number_label', ''),
    }


def row_from_disbursement(cand, r):
    return {
        'candidate_name': cand['name'],
        'candidate_id': cand['candidate_id'],
        'office': cand['office'],
        'district': cand['district'],
        'party': cand['party'],
        'committee_id': cand['committee_id'],
        'recipient_name': r.get('recipient_name', ''),
        'recipient_city': r.get('recipient_city', ''),
        'recipient_state': r.get('recipient_state', ''),
        'recipient_zip': r.get('recipient_zip', ''),
        'disbursement_description': r.get('disbursement_description', ''),
        'disbursement_purpose_category': r.get('disbursement_purpose_category', ''),
        'disbursement_date': r.get('disbursement_date', ''),
        'disbursement_amount': r.get('disbursement_amount', ''),
        'disbursement_type_description': r.get('disbursement_type_description', ''),
        'memo_text': r.get('memo_text', ''),
        'line_number_label': r.get('line_number_label', ''),
    }


SCHEDULE_CONFIG = {
    'a': {
        'endpoint': 'https://api.open.fec.gov/v1/schedules/schedule_a/',
        'fields': RECEIPT_FIELDS,
        'file_name': 'receipts.csv',
        'sort_field': '-contribution_receipt_date',
        'date_key': 'last_contribution_receipt_date',
        'row_fn': row_from_receipt,
        'label': 'receipts',
    },
    'b': {
        'endpoint': 'https://api.open.fec.gov/v1/schedules/schedule_b/',
        'fields': DISBURSEMENT_FIELDS,
        'file_name': 'expenditures.csv',
        'sort_field': '-disbursement_date',
        'date_key': 'last_disbursement_date',
        'row_fn': row_from_disbursement,
        'label': 'expenditures',
    },
}


def pull(cycle, schedule):
    cfg = SCHEDULE_CONFIG[schedule]
    candidates_file = os.path.join(SCRIPT_DIR, f'me_candidates_{cycle}.json')
    out_dir = os.path.join(DATA_DIR, f'federal_{cycle}')
    output_file = os.path.join(out_dir, cfg['file_name'])
    checkpoint_path = os.path.join(out_dir, f'checkpoint_{schedule}.json')

    os.makedirs(out_dir, exist_ok=True)

    with open(candidates_file) as f:
        candidates = json.load(f)

    seen_comms = {}
    for c in candidates:
        cid = c['committee_id']
        if cid != 'NONE' and cid not in seen_comms:
            seen_comms[cid] = c

    checkpoint = load_checkpoint(checkpoint_path)
    done = set(checkpoint['done_committees'])
    remaining = {k: v for k, v in seen_comms.items() if k not in done}
    print(f"Cycle {cycle} schedule {schedule.upper()}: {len(seen_comms)} committees, {len(done)} done, {len(remaining)} remaining")

    file_exists = os.path.exists(output_file) and len(done) > 0
    session = make_session()

    with open(output_file, 'a' if file_exists else 'w', newline='', encoding='utf-8') as csvfile:
        writer = csv.DictWriter(csvfile, fieldnames=cfg['fields'])
        if not file_exists:
            writer.writeheader()

        total_rows = 0
        for comm_id, cand in remaining.items():
            print(f"\n  {cand['name']} ({comm_id})...", end='', flush=True)

            last_index = None
            last_date = None
            cand_rows = 0

            while True:
                params = {
                    'api_key': API_KEY,
                    'committee_id': comm_id,
                    'two_year_transaction_period': cycle,
                    'per_page': 100,
                    'sort': cfg['sort_field'],
                }
                if last_index:
                    params['last_index'] = last_index
                    params[cfg['date_key']] = last_date

                try:
                    resp = session.get(cfg['endpoint'], params=params, timeout=60)
                    resp.raise_for_status()
                    data = resp.json()
                except Exception as e:
                    print(f"\n    Error: {e}. Waiting 30s and retrying...")
                    time.sleep(30)
                    continue

                results = data.get('results', [])
                if not results:
                    break

                for r in results:
                    writer.writerow(cfg['row_fn'](cand, r))

                cand_rows += len(results)

                pagination = data.get('pagination', {})
                last_index = pagination.get('last_indexes', {}).get('last_index')
                last_date = pagination.get('last_indexes', {}).get(cfg['date_key'])

                if not last_index:
                    break
                time.sleep(0.5)

            csvfile.flush()
            total_rows += cand_rows
            print(f" {cand_rows} {cfg['label']}")

            checkpoint['done_committees'].append(comm_id)
            save_checkpoint(checkpoint_path, checkpoint)

    print(f"\nDone! {total_rows} new {cfg['label']}. Total in {output_file}: {sum(1 for _ in open(output_file)) - 1}")
    if os.path.exists(checkpoint_path):
        os.remove(checkpoint_path)


if __name__ == '__main__':
    cycle = int(sys.argv[1]) if len(sys.argv) > 1 else 2026
    schedule = sys.argv[2] if len(sys.argv) > 2 else 'a'
    pull(cycle, schedule)
