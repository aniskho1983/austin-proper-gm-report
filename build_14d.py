"""
build_14d.py — 14-day GM productivity rebuild with evidence-based gap backfill
Austin Proper · Anis Khoury

Stage 1 (--gaps):  classify the freshly-pulled 14-day calendar, detect blank
                   8AM–8PM windows on working days, emit them for backfill.
Stage 2 (--build): merge evidence-based "inferred" activity blocks (recovered
                   from Email/Slack/Notion), recompute 7d/14d summaries, and
                   emit the dashboard data payload (build_out_14d.json).

Reuses the classification engine from categorize.py — no duplication.
Logged calendar time and recovered (inferred) time are tracked separately so
the report stays honest about what was on the calendar vs. reconstructed.
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
INFERRED  = ['inferred_E1.json', 'inferred_E2.json']
BUILD_OUT = 'build_out_14d.json'

TODAY      = date(2026, 6, 26)
WIN_START  = date(2026, 6, 12)   # 14-day window start
WINDOWS    = {
    '7d':  TODAY - timedelta(days=7),    # 2026-06-19
    '14d': TODAY - timedelta(days=14),   # 2026-06-12
}
UTC_OFFSET_H = 5  # CDT in June: local = UTC − 5  →  UTC = local + 5


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
            'source':       'logged',
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

    timeline = {}
    for day, evs in sorted(by_date.items()):
        d = date.fromisoformat(day)
        if d.weekday() in cz.DAYS_OFF or d < WIN_START:
            continue
        blocks = []
        for e in evs:
            if e['block_type'] in ('meeting', 'solo_work', 'tentative') and not e.get('isAllDay'):
                if e.get('start_local') and e.get('end_local'):
                    sl = datetime.fromisoformat(e['start_local'])
                    el = datetime.fromisoformat(e['end_local'])
                    blocks.append({'start': sl.strftime('%H:%M'), 'end': el.strftime('%H:%M'),
                                   'subject': e.get('subject', '?'), 'category': e.get('category')})
        timeline[day] = blocks

    # Only real gaps within the window, on working days, >= threshold
    real_gaps = [g for g in gaps
                 if g['duration_min'] >= cz.MIN_GAP_MINUTES
                 and date.fromisoformat(g['date']) >= WIN_START
                 and date.fromisoformat(g['date']).weekday() not in cz.DAYS_OFF]

    json.dump(real_gaps, open(GAPS_OUT, 'w'), indent=2)
    json.dump(timeline, open(TIMELINE, 'w'), indent=2)
    print(f"Classified {len(events)} events. Working days: {sorted(timeline.keys())}")
    print(f"Found {len(real_gaps)} blank 8AM–8PM windows on working days:\n")
    for g in real_gaps:
        print(f"  {g['date']}  {g['start']}–{g['end']}  ({g['duration_min']} min) | {g.get('context','')}")


def inferred_to_event(b):
    """Convert a recovered/inferred block into an engine-compatible event."""
    sl = datetime.fromisoformat(b['start_local'])
    el = datetime.fromisoformat(b['end_local'])
    dur = max(0.0, (el - sl).total_seconds() / 3600)
    su = sl + timedelta(hours=UTC_OFFSET_H)
    eu = el + timedelta(hours=UTC_OFFSET_H)
    return {
        'subject':     b.get('subject', 'Recovered activity'),
        'start':       su.strftime('%Y-%m-%dT%H:%M:%SZ'),
        'end':         eu.strftime('%Y-%m-%dT%H:%M:%SZ'),
        'isAllDay':    False, 'isCancelled': False, 'showAs': 'busy',
        'attendees':   [], 'categories': [],
        'block_type':  'solo_work',
        'category':    b.get('category', 'solo_work_other'),
        'dur_hours':   round(dur, 2),
        'start_local': sl.isoformat(), 'end_local': el.isoformat(),
        'date_local':  sl.strftime('%Y-%m-%d'), 'day_name': sl.strftime('%a %b %d'),
        'source':      'inferred',
        'confidence':  b.get('confidence', 'med'),
        'evidence':    b.get('evidence', []),
    }


def summarize(events, start_date):
    """Hours per category in [start_date, TODAY], plus logged/recovered split."""
    cat_hours = {k: 0.0 for k in cz.CATEGORY_META}
    cat_count = {k: 0   for k in cz.CATEGORY_META}
    tot_meet = tot_solo = recovered = 0.0
    total_events = 0
    for e in events:
        d = e.get('date_local')
        if not d:
            continue
        dd = date.fromisoformat(d)
        if dd < start_date or dd > TODAY:
            continue
        if e['block_type'] in ('cancelled', 'context_block', 'personal'):
            continue
        h = e.get('dur_hours') or 0
        cat = e.get('category', 'uncategorized')
        if cat in cat_hours:
            cat_hours[cat] = round(cat_hours[cat] + h, 2)
            cat_count[cat] += 1
        meta = cz.CATEGORY_META.get(cat, {})
        if meta.get('counts_as') == 'meeting':
            tot_meet += h
        elif meta.get('counts_as') == 'solo':
            tot_solo += h
        if e.get('source') == 'inferred':
            recovered += h
        total_events += 1

    days = (TODAY - start_date).days
    weeks = max(days / 7, 1)
    return {
        'start_date': start_date.isoformat(), 'end_date': TODAY.isoformat(),
        'days': days, 'weeks': round(weeks, 1), 'total_events': total_events,
        'total_meeting_h': round(tot_meet, 1), 'total_solo_h': round(tot_solo, 1),
        'total_h': round(tot_meet + tot_solo, 1),
        'recovered_h': round(recovered, 1),
        'meeting_h_per_wk': round(tot_meet / weeks, 1),
        'solo_h_per_wk': round(tot_solo / weeks, 1),
        'pct_of_60h': round((tot_meet + tot_solo) / (weeks * cz.GM_HOURS_PER_WEEK) * 100, 1),
        'by_category': {k: {'hours': cat_hours[k], 'count': cat_count[k],
                            'hrs_per_wk': round(cat_hours[k] / weeks, 2)} for k in cz.CATEGORY_META},
    }


def stage_build():
    raw = json.load(open(RAW_14D))
    events = classify_all(raw)

    # Merge recovered/inferred blocks
    inferred = []
    for f in INFERRED:
        try:
            inferred += json.load(open(f))
        except FileNotFoundError:
            print(f"  (note: {f} not found — skipping)")
    inf_events = [inferred_to_event(b) for b in inferred]
    events += inf_events
    events.sort(key=lambda x: x.get('start') or '')

    # Keep only window events for the report
    win_events = [e for e in events
                  if e.get('date_local') and date.fromisoformat(e['date_local']) >= WIN_START
                  and date.fromisoformat(e['date_local']) <= TODAY]

    summaries = {k: summarize(win_events, sd) for k, sd in WINDOWS.items()}

    # Dashboard EVENTS rows: [date, "HH:MM-HH:MM", "Xh", subject, category, block_type, source]
    rows = []
    for e in win_events:
        if e['block_type'] in ('cancelled', 'context_block', 'personal'):
            continue
        if e.get('isAllDay') or not e.get('start_local'):
            continue
        sl = datetime.fromisoformat(e['start_local'])
        el = datetime.fromisoformat(e['end_local']) if e.get('end_local') else sl
        rows.append([
            e['date_local'], f"{sl.strftime('%H:%M')}-{el.strftime('%H:%M')}",
            f"{e.get('dur_hours', 0)}h", e.get('subject', '?'),
            e.get('category', 'uncategorized'), e['block_type'], e.get('source', 'logged'),
        ])

    out = {
        'generated': TODAY.isoformat(),
        'window_start': WIN_START.isoformat(),
        'summaries': summaries,
        'events': rows,
        'inferred_detail': [
            {'date': e['date_local'], 'time': f"{datetime.fromisoformat(e['start_local']).strftime('%H:%M')}-{datetime.fromisoformat(e['end_local']).strftime('%H:%M')}",
             'subject': e['subject'], 'category': e['category'], 'confidence': e.get('confidence'),
             'hours': e['dur_hours'], 'evidence': e.get('evidence', [])}
            for e in inf_events
        ],
    }
    json.dump(out, open(BUILD_OUT, 'w'), indent=2)

    s14 = summaries['14d']
    print(f"Merged {len(inf_events)} recovered blocks ({s14['recovered_h']}h).")
    print(f"14-day totals: {s14['total_h']}h logged+recovered "
          f"({s14['total_meeting_h']}h meetings, {s14['total_solo_h']}h solo) "
          f"across {s14['total_events']} blocks, {s14['pct_of_60h']}% of 120h.")
    print(f"Wrote {BUILD_OUT}")


if __name__ == '__main__':
    mode = sys.argv[1] if len(sys.argv) > 1 else '--gaps'
    if mode == '--gaps':
        stage_gaps()
    elif mode == '--build':
        stage_build()
