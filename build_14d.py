"""
build_14d.py — 14-day GM productivity rebuild with gap backfill
Austin Proper · Anis Khoury

Stage 1 (--gaps): classify the freshly-pulled 14-day calendar, detect blank
                  8AM–8PM windows on working days, and emit them for backfill.
Stage 2 (--build): merge evidence-based "inferred" activity blocks (recovered
                   from Email/Slack/Notion) and regenerate the dashboard data.

Reuses the classification engine from categorize.py — no duplication.
"""

import json
import sys
from datetime import datetime, timedelta, date

import categorize as cz

# Working-hours window for gap detection: 8 AM – 8 PM (user spec)
cz.WORK_START_H = 8
cz.WORK_END_H = 20
cz.MIN_GAP_MINUTES = 45  # ignore sub-45-min slivers

RAW_14D   = 'calendar_raw_14d.json'
GAPS_OUT  = 'calendar_gaps_14d.json'
TIMELINE  = 'calendar_timeline_14d.json'

TODAY     = date(2026, 6, 26)
WINDOWS   = {
    '7d':  TODAY - timedelta(days=7),
    '14d': TODAY - timedelta(days=14),
}


def classify_all(raw):
    """Classify every raw event (mirrors categorize.run step 1)."""
    events = []
    for e in raw:
        block_type = cz.classify_block_type(e)
        category   = cz.classify_category(e, block_type) if block_type not in ('cancelled', 'context_block', 'personal') else block_type
        override   = cz.get_override(e)
        dur_h      = override.get('dur_hours_override') if override and 'dur_hours_override' in override else cz.duration_hours(e)

        start_utc  = cz.parse_dt(e.get('start'))
        end_utc    = cz.parse_dt(e.get('end'))
        start_local = cz.to_local(start_utc) if start_utc else None
        end_local   = cz.to_local(end_utc)   if end_utc   else None

        events.append({
            **e,
            'block_type':   block_type,
            'category':     category,
            'dur_hours':    round(dur_h, 2) if dur_h is not None else None,
            'start_local':  start_local.isoformat() if start_local else None,
            'end_local':    end_local.isoformat()   if end_local   else None,
            'date_local':   start_local.strftime('%Y-%m-%d') if start_local else None,
            'day_name':     start_local.strftime('%a %b %d') if start_local else None,
        })
    events.sort(key=lambda x: x.get('start') or '')
    return events


def group_by_date(events):
    by = {}
    for e in events:
        d = e.get('date_local')
        if d:
            by.setdefault(d, []).append(e)
    return by


def stage_gaps():
    raw = json.load(open(RAW_14D))
    events = classify_all(raw)
    by_date = group_by_date(events)

    gaps = cz.find_gaps(by_date)

    # Per-day timeline of what's ALREADY logged (busy blocks, local time) —
    # gives the backfill step context so it doesn't double-count.
    timeline = {}
    for day, evs in sorted(by_date.items()):
        d = date.fromisoformat(day)
        if d.weekday() in cz.DAYS_OFF:
            continue
        blocks = []
        for e in evs:
            if e['block_type'] in ('meeting', 'solo_work', 'tentative') and not e.get('isAllDay'):
                if e.get('start_local') and e.get('end_local'):
                    sl = datetime.fromisoformat(e['start_local'])
                    el = datetime.fromisoformat(e['end_local'])
                    blocks.append({
                        'start': sl.strftime('%H:%M'),
                        'end':   el.strftime('%H:%M'),
                        'subject': e.get('subject', '?'),
                        'category': e.get('category'),
                    })
        timeline[day] = blocks

    json.dump(gaps, open(GAPS_OUT, 'w'), indent=2)
    json.dump(timeline, open(TIMELINE, 'w'), indent=2)

    work_gaps = [g for g in gaps if g['duration_min'] >= cz.MIN_GAP_MINUTES]
    print(f"Classified {len(events)} events across {len(by_date)} days.")
    print(f"Working days (Sun–Thu): {sorted(timeline.keys())}")
    print(f"Found {len(work_gaps)} blank 8AM–8PM windows on working days:\n")
    for g in work_gaps:
        print(f"  {g['date']}  {g['start']}–{g['end']}  ({g['duration_min']} min)  | {g.get('context','')}")


if __name__ == '__main__':
    mode = sys.argv[1] if len(sys.argv) > 1 else '--gaps'
    if mode == '--gaps':
        stage_gaps()
    else:
        print('stage 2 (--build) added after backfill')
