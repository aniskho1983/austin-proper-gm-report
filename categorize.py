"""
categorize.py — GM Calendar Categorization Engine
Austin Proper Hotel · Anis Khoury

Takes raw M365 calendar JSON (calendar_raw.json) and outputs:
  - calendar_categorized.json  : every event with type + category assigned
  - calendar_report.txt        : human-readable full event list
  - calendar_gaps.json         : gaps in working hours that need review
  - calendar_summary.json      : hours per category per time window (7d, 30d, YTD)

Block Types:
  context_block   — all-day, free (vendor visits, FOP guests, OOO, reminders)
  meeting         — busy/tentative, 2+ attendees, structured meeting
  solo_work       — busy, 0–1 attendees, deep work / prep / email / admin
  cancelled       — isCancelled=True, excluded from counts
  personal        — keyword-flagged personal blocks (not counted)

Categories (maps to dashboard):
  daily_ops       — AM Ops, Rate Review, DBR, PM Shift
  direct_reports  — 1:1s with direct reports (Green category + keywords)
  weekly_ops      — ExCom, GM Meeting, Property Walk, Pool Plan, Service Mtg, etc.
  project_dev     — Purple category + Dallas/Red Bluff/Minneapolis/Tampa project work
  ai_innovation   — Yellow category + Claude/AI/Perplexity blocks
  monthly_fin     — Monthly financial reviews, P&L, Gabe 1:1, Proper AI Monthly
  payroll         — Payroll cycle meetings
  sales_mktg      — Sales Blitz, Marketing, BEO, Travel Agents, PR
  guest_relations — Owner dinners, FOP interactions, floor time, site visits (external)
  hr_talent       — Resume meetings, interviews, FMLA, assessments
  vendor_ops      — Jani-King, Kappo, vendor walkthroughs
  solo_work       — Email, prep, admin, deep work blocks (0-attendee busy)
  uncategorized   — Needs manual review
"""

import json
import re
from datetime import datetime, timedelta, timezone, date

# ── Configuration ─────────────────────────────────────────────────────────────

# Austin timezone offsets (CDT from Mar 8, CST before)
DST_START_2026 = date(2026, 3, 8)

# Working hours for gap detection (local time)
WORK_START_H = 7   # 7:00 AM
WORK_END_H   = 20  # 8:00 PM (hotel GM, not 9-5)
MIN_GAP_MINUTES = 45  # gaps smaller than this are ignored

# Scheduled days off — no gap detection on these days (0=Mon … 4=Fri, 5=Sat, 6=Sun)
DAYS_OFF = {4, 5}  # Friday, Saturday

# Known direct reports (for keyword matching)
DIRECT_REPORTS = [
    "natascha", "anna", "lalaine", "ryan", "ash", "jasmine",
    "kinsey", "bryan", "edwin", "lena"
]

# ── Manual overrides ────────────────────────────────────────────────────────────
# Subject substring (lowercase) → {block_type, category, dur_hours_override}
# These take full precedence over all automatic classification rules.
MANUAL_OVERRIDES = {
    # Disregard — visibility/awareness blocks, GM does not attend
    "anna mayevska and jason hammons":          {"block_type": "context_block"},
    "proper ai 10 week challenge - office hours": {"block_type": "context_block"},
    "austineater visit":                         {"block_type": "context_block"},
    "filming":                                   {"block_type": "context_block"},  # on-property filming added for visibility only

    # HR / Talent
    "max houston - peacock runner":              {"block_type": "meeting",  "category": "hr_talent",    "dur_hours_override": 10/60},
    "anis jess and kinsey":                      {"block_type": "meeting",  "category": "hr_talent"},
    "wage conversation - atxp":                  {"block_type": "meeting",  "category": "hr_talent"},

    # Legal counsel
    "shannon and anis":                          {"block_type": "meeting",  "category": "legal_external"},

    # AI / Innovation
    "perplexity!":                               {"block_type": "meeting",  "category": "ai_innovation"},

    # Monthly Financial — Forecast review (calendar shows 5min, actual = 1h)
    "forecast 4 deadline":                       {"block_type": "meeting",  "category": "monthly_fin",  "dur_hours_override": 1.0},

    # Monthly Proper Welcome — recurring onboarding session, always count
    "proper story, leadership, brand & purpose": {"block_type": "meeting",  "category": "hr_talent"},
}

# ── Timezone helper ────────────────────────────────────────────────────────────

def to_local(dt_utc):
    """Convert UTC datetime to Austin local time."""
    if dt_utc.date() >= DST_START_2026:
        offset = timedelta(hours=-5)  # CDT
    else:
        offset = timedelta(hours=-6)  # CST
    return dt_utc + offset

def parse_dt(s):
    if not s:
        return None
    return datetime.fromisoformat(s.replace('Z', '+00:00'))

def duration_hours(e):
    """Return duration in hours, or None if all-day."""
    if e.get('isAllDay'):
        return None
    s = parse_dt(e.get('start'))
    en = parse_dt(e.get('end'))
    if not s or not en:
        return None
    return max(0, (en - s).total_seconds() / 3600)

# ── Classification ─────────────────────────────────────────────────────────────

def get_override(e):
    """Return the manual override dict for this event, or None."""
    subj = (e.get('subject') or '').lower().strip()
    for key, override in MANUAL_OVERRIDES.items():
        if key in subj:
            return override
    return None


def classify_block_type(e):
    """
    Returns one of: context_block, cancelled, meeting, solo_work, tentative, personal
    """
    override = get_override(e)
    if override:
        return override['block_type']

    if e.get('isCancelled'):
        return 'cancelled'
    if e.get('isAllDay') and e.get('showAs') == 'free':
        return 'context_block'
    if e.get('isAllDay') and e.get('showAs') == 'busy':
        # All-day busy = committed full-day block (offsite, OOO)
        return 'solo_work'

    subj = (e.get('subject') or '').strip()
    n_att = len(e.get('attendees') or [])
    show = e.get('showAs', '')

    # Personal / non-work
    personal_keywords = ['home close', 'daughter from school', 'lieu', 'ooo', 'pto']
    if any(k in subj.lower() for k in personal_keywords):
        return 'personal'

    if show == 'tentative':
        return 'tentative'

    # Solo work: 0 attendees OR 1 attendee but clearly a solo block
    solo_keywords = [
        'send excom', 'email', 'prep', 'notes', 'agenda', 'report',
        'debrief', 'continuation', 'fmla', 'wage conversation',
        'complete ', 'read through', 'study', 'coding', 'claude code',
        'open claw', 'perplexity', 'biz dev', 'lobby ambassador',
        'drink with', 'dinner at eberly', 'dallas work', 'red bluff prep',
        'minneapolis continued', 'fop site and lunch', 'home close',
        'deal with public noise', 'gm report', 'stewarding follow up'
    ]
    if n_att == 0:
        if any(k in subj.lower() for k in solo_keywords):
            return 'solo_work'
        return 'solo_work'  # 0 attendees = always solo

    if n_att == 1:
        # Could be a 1:1 — check subject
        if any(dr in subj.lower() for dr in DIRECT_REPORTS):
            return 'meeting'
        if any(k in subj.lower() for k in ['1 on 1', '1:1', 'catch up', 'touch base', 'x anis', 'anis x', 'and anis']):
            return 'meeting'
        # Otherwise treat as solo
        return 'solo_work'

    return 'meeting'


def classify_category(e, block_type):
    """
    Returns a category string for the event.
    Manual overrides → colors → keyword rules.
    """
    override = get_override(e)
    if override and 'category' in override:
        return override['category']

    cats = e.get('categories') or []
    subj = (e.get('subject') or '').lower()
    n_att = len(e.get('attendees') or [])

    if block_type == 'solo_work':
        # Sub-classify solo work
        if any(k in subj for k in ['claude', 'ai coding', 'open claw', 'perplexity', 'biz dev']):
            return 'ai_innovation'
        if any(k in subj for k in ['dallas', 'red bluff', 'minneapolis', 'tampa', '2500', 'prep for dallas']):
            return 'project_dev'
        if any(k in subj for k in ['email', 'inbox']):
            return 'solo_email'
        if any(k in subj for k in ['send excom', 'agenda', 'notes', 'report', 'debrief', 'stewarding', 'continuation', 'wage']):
            return 'solo_admin'
        if any(k in subj for k in ['complete ', 'assessment', 'fmla', 'read through', 'study']):
            return 'solo_admin'
        if any(k in subj for k in ['lobby ambassador', 'drink with', 'dinner at eberly', 'fop site and lunch', 'home close']):
            return 'guest_floor'
        return 'solo_work_other'

    # Color category rules
    if 'Purple category' in cats:
        return 'project_dev'
    if 'Green category' in cats:
        return 'direct_reports'
    if 'Yellow category' in cats:
        return 'ai_innovation'

    # Keyword rules for no-color meetings
    # Daily ops
    if any(k in subj for k in ['am ops meeting', '90-day rate review', 'daily 90-day', 'dbr review', 'pm shift', 'eng. dept. debrief', 'eng dept debrief']):
        return 'daily_ops'

    # Weekly ops
    if any(k in subj for k in ['excom meeting', 'gm meeting', 'notion tracking', 'property walk', 'pool plan', 'proper service meeting', 'revenue optimization', 'radio implementation', 'beo meeting', 'ec outing', 'sxsw planning']):
        return 'weekly_ops'

    # Direct reports (by name, no color tag)
    if any(dr in subj for dr in DIRECT_REPORTS):
        if any(k in subj for k in ['x anis', 'anis x', 'and anis', '1 on 1', '1:1', 'catch up', 'touch base']):
            return 'direct_reports'

    # Monthly financial
    if any(k in subj for k in ['financial review', 'p&l', 'midmonth', 'manager\'s financial', 'managers financial', 'february 2026', 'capex', 'budget review', 'suites budget', 'anis x gabe', 'anis x jacob', 'review increases']):
        return 'monthly_fin'

    # Proper AI Monthly / corporate meetings
    if any(k in subj for k in ['proper ai monthly', 'anis x gabe']):
        return 'monthly_fin'

    # Payroll
    if 'payroll' in subj:
        return 'payroll'

    # Sales & Marketing
    if any(k in subj for k in ['sales blitz', 'marketing meeting', 'travel agent', 'monthly marketing', 'tenfold', 'alyssa', 'women in hosp', 'advertising', 'pr ']):
        return 'sales_mktg'

    # Guest relations / owner / external
    if any(k in subj for k in ['dinner at peacock', 'dinner at', 'site visit', 'spacex', 'filming', 'ford commercial', 'adidas', 'bank of america', 'tour', 'site with', 'wilks', 'tea with', 'proper welcome', 'farewell', 'toast to']):
        return 'guest_relations'

    # External partner / vendor
    if any(k in subj for k in ['mml', 'jani-king', 'kappo', 'openclaw', 'openable', 'austin proper x autumn', 'autumn communications', 'ketchum', 'aipac', 'deloitte', 'tenfold']):
        return 'vendor_ops'

    # Karen site visits = external/sales
    if 'karen site' in subj:
        return 'sales_mktg'

    # HR / Talent
    if any(k in subj for k in ['resume meeting', 'interview', 'fmla', 'chief engineer position', 'assessment', 'overnight hotel manager', 'catch up - chief']):
        return 'hr_talent'

    # Guest experience / ops
    if any(k in subj for k in ['guest review', 'guest recovery', 'spa', 'f&b meeting', 'f&b weekly', 'la pisina', 'lobby']):
        return 'guest_relations'

    # Meaningful performance reviews = HR
    if 'meaningful performance' in subj:
        return 'hr_talent'

    # Catch-all for named catch-ups
    if any(k in subj for k in ['catch up', 'touch base', 'connect']):
        return 'direct_reports' if n_att <= 3 else 'weekly_ops'

    return 'uncategorized'


# ── Gap Detection ──────────────────────────────────────────────────────────────

def find_gaps(events_by_date):
    """
    For each working day, find windows > MIN_GAP_MINUTES with no busy blocks.
    Returns list of {date, start, end, duration_min, context}.
    """
    gaps = []
    for day_str, day_events in sorted(events_by_date.items()):
        day = date.fromisoformat(day_str)
        if day.weekday() in DAYS_OFF:
            continue  # Scheduled day off — skip gap detection

        # Get all busy blocks for the day (local time)
        busy_blocks = []
        for e in day_events:
            if e['block_type'] in ('meeting', 'solo_work', 'tentative'):
                if not e.get('isAllDay'):
                    s_utc = parse_dt(e.get('start'))
                    en_utc = parse_dt(e.get('end'))
                    if s_utc and en_utc:
                        s_local = to_local(s_utc)
                        en_local = to_local(en_utc)
                        busy_blocks.append((s_local, en_local, e.get('subject', '?')))

        if not busy_blocks:
            gaps.append({
                'date': day_str,
                'start': f'{WORK_START_H:02d}:00',
                'end': f'{WORK_END_H:02d}:00',
                'duration_min': (WORK_END_H - WORK_START_H) * 60,
                'type': 'empty_day',
                'context': 'No calendar blocks found for this day'
            })
            continue

        # Sort by start
        busy_blocks.sort(key=lambda x: x[0])

        # Check gap before first meeting
        work_start = busy_blocks[0][0].replace(hour=WORK_START_H, minute=0, second=0, microsecond=0)
        first_start = busy_blocks[0][0]
        if (first_start - work_start).total_seconds() / 60 > MIN_GAP_MINUTES:
            gaps.append({
                'date': day_str,
                'start': work_start.strftime('%H:%M'),
                'end': first_start.strftime('%H:%M'),
                'duration_min': int((first_start - work_start).total_seconds() / 60),
                'type': 'gap',
                'context': f'Before: {busy_blocks[0][2][:50]}'
            })

        # Check gaps between meetings
        for i in range(len(busy_blocks) - 1):
            end_of_current = busy_blocks[i][1]
            start_of_next = busy_blocks[i + 1][0]
            gap_min = (start_of_next - end_of_current).total_seconds() / 60
            if gap_min > MIN_GAP_MINUTES:
                gaps.append({
                    'date': day_str,
                    'start': end_of_current.strftime('%H:%M'),
                    'end': start_of_next.strftime('%H:%M'),
                    'duration_min': int(gap_min),
                    'type': 'gap',
                    'context': f'After: {busy_blocks[i][2][:40]} | Before: {busy_blocks[i+1][2][:40]}'
                })

        # Check gap after last meeting
        last_end = busy_blocks[-1][1]
        work_end = last_end.replace(hour=WORK_END_H, minute=0, second=0, microsecond=0)
        if (work_end - last_end).total_seconds() / 60 > MIN_GAP_MINUTES:
            gaps.append({
                'date': day_str,
                'start': last_end.strftime('%H:%M'),
                'end': work_end.strftime('%H:%M'),
                'duration_min': int((work_end - last_end).total_seconds() / 60),
                'type': 'gap',
                'context': f'After: {busy_blocks[-1][2][:50]}'
            })

    return gaps


# ── Category display config ────────────────────────────────────────────────────

CATEGORY_META = {
    'daily_ops':       {'label': 'Daily Ops Rhythm',          'color': '#2c5f9e', 'counts_as': 'meeting'},
    'direct_reports':  {'label': 'Direct Report 1:1s',         'color': '#27ae60', 'counts_as': 'meeting'},
    'weekly_ops':      {'label': 'Weekly Ops Meetings',        'color': '#3a7fc1', 'counts_as': 'meeting'},
    'project_dev':     {'label': 'Project Development',        'color': '#8e44ad', 'counts_as': 'meeting'},
    'ai_innovation':   {'label': 'AI / Innovation',            'color': '#d4a017', 'counts_as': 'meeting'},
    'monthly_fin':     {'label': 'Monthly & Financial',        'color': '#95a5a6', 'counts_as': 'meeting'},
    'payroll':         {'label': 'Payroll Cycles',             'color': '#7f8c8d', 'counts_as': 'meeting'},
    'sales_mktg':      {'label': 'Sales & Marketing',          'color': '#e67e22', 'counts_as': 'meeting'},
    'guest_relations': {'label': 'Guest & Owner Relations',    'color': '#c0392b', 'counts_as': 'meeting'},
    'hr_talent':       {'label': 'HR & Talent',                'color': '#16a085', 'counts_as': 'meeting'},
    'vendor_ops':      {'label': 'Vendor / External Partners', 'color': '#8e6e53', 'counts_as': 'meeting'},
    'legal_external':  {'label': 'Legal Counsel',              'color': '#5d6d7e', 'counts_as': 'meeting'},
    'solo_email':      {'label': 'Email & Comms (solo)',        'color': '#bdc3c7', 'counts_as': 'solo'},
    'solo_admin':      {'label': 'Admin / Prep (solo)',         'color': '#95a5a6', 'counts_as': 'solo'},
    'guest_floor':     {'label': 'Guest / Floor Time (solo)',   'color': '#e8a87c', 'counts_as': 'solo'},
    'solo_work_other': {'label': 'Deep Work / Other (solo)',    'color': '#7f8c8d', 'counts_as': 'solo'},
    'uncategorized':   {'label': '⚠ Needs Review',             'color': '#e74c3c', 'counts_as': 'unknown'},
}

BLOCK_TYPE_LABEL = {
    'context_block': 'CONTEXT',
    'cancelled':     'CANCELLED',
    'meeting':       'MEETING',
    'solo_work':     'SOLO',
    'tentative':     'TENTATIVE',
    'personal':      'PERSONAL',
}


# ── Main ───────────────────────────────────────────────────────────────────────

def run(input_file='calendar_raw.json',
        report_file='calendar_report.txt',
        gaps_file='calendar_gaps.json',
        categorized_file='calendar_categorized.json',
        summary_file='calendar_summary.json'):

    raw = json.load(open(input_file))

    # ── Step 1: Classify every event ─────────────────────────────────────────
    events = []
    for e in raw:
        block_type = classify_block_type(e)
        category   = classify_category(e, block_type) if block_type not in ('cancelled', 'context_block', 'personal') else block_type
        override   = get_override(e)
        dur_h      = override.get('dur_hours_override') if override and 'dur_hours_override' in override else duration_hours(e)

        start_utc  = parse_dt(e.get('start'))
        end_utc    = parse_dt(e.get('end'))
        start_local = to_local(start_utc) if start_utc else None
        end_local   = to_local(end_utc)   if end_utc   else None

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

    # Sort by start time
    events.sort(key=lambda x: x.get('start') or '')

    # ── Step 2: Group by date ─────────────────────────────────────────────────
    events_by_date = {}
    for e in events:
        d = e.get('date_local')
        if d:
            events_by_date.setdefault(d, []).append(e)

    # ── Step 3: Find gaps ─────────────────────────────────────────────────────
    gaps = find_gaps(events_by_date)

    # ── Step 4: Summary by time window ───────────────────────────────────────
    today = date(2026, 4, 1)  # Report date
    windows = {
        '7d':  today - timedelta(days=7),
        '30d': today - timedelta(days=30),
        'ytd': date(2026, 1, 1),
    }

    summaries = {}
    for window_key, start_date in windows.items():
        cat_hours = {k: 0.0 for k in CATEGORY_META}
        cat_count = {k: 0   for k in CATEGORY_META}
        total_meeting_h = 0.0
        total_solo_h    = 0.0
        total_events    = 0

        for e in events:
            d = e.get('date_local')
            if not d:
                continue
            if date.fromisoformat(d) < start_date or date.fromisoformat(d) > today:
                continue
            if e['block_type'] in ('cancelled', 'context_block', 'personal'):
                continue

            h = e.get('dur_hours') or 0
            cat = e.get('category', 'uncategorized')
            if cat in cat_hours:
                cat_hours[cat] = round(cat_hours[cat] + h, 2)
                cat_count[cat] += 1

            meta = CATEGORY_META.get(cat, {})
            if meta.get('counts_as') == 'meeting':
                total_meeting_h += h
            elif meta.get('counts_as') == 'solo':
                total_solo_h += h
            total_events += 1

        # Days in window
        days_in_window = (today - start_date).days
        weeks = max(days_in_window / 7, 1)

        summaries[window_key] = {
            'window':           window_key,
            'start_date':       start_date.isoformat(),
            'end_date':         today.isoformat(),
            'days':             days_in_window,
            'weeks':            round(weeks, 1),
            'total_events':     total_events,
            'total_meeting_h':  round(total_meeting_h, 1),
            'total_solo_h':     round(total_solo_h, 1),
            'total_h':          round(total_meeting_h + total_solo_h, 1),
            'meeting_h_per_wk': round(total_meeting_h / weeks, 1),
            'solo_h_per_wk':    round(total_solo_h / weeks, 1),
            'pct_of_40h':       round((total_meeting_h + total_solo_h) / (weeks * 40) * 100, 1),
            'by_category':      {k: {'hours': cat_hours[k], 'count': cat_count[k], 'hrs_per_wk': round(cat_hours[k] / weeks, 2)} for k in CATEGORY_META},
        }

    # ── Step 5: Human-readable report ────────────────────────────────────────
    lines = []
    lines.append("=" * 120)
    lines.append("GM CALENDAR AUDIT REPORT — Austin Proper Hotel · Anis Khoury")
    lines.append("Period: March 2 – April 1, 2026  |  Generated: 2026-04-01")
    lines.append("=" * 120)
    lines.append("")

    current_day = None
    for e in events:
        d = e.get('day_name') or e.get('date_local') or '?'
        if d != current_day:
            lines.append("")
            lines.append("─" * 120)
            lines.append(f"  {d}")
            lines.append("─" * 120)
            current_day = d

        block_type  = e.get('block_type', '?')
        category    = e.get('category', '?')
        dur         = e.get('dur_hours')
        subj        = (e.get('subject') or '?')[:70]
        n_att       = len(e.get('attendees') or [])
        show        = e.get('showAs', '')
        is_all_day  = e.get('isAllDay', False)
        cat_label   = CATEGORY_META.get(category, {}).get('label', category)
        type_label  = BLOCK_TYPE_LABEL.get(block_type, block_type.upper())

        if is_all_day:
            time_str = 'ALL-DAY'
        elif e.get('start_local'):
            sl = datetime.fromisoformat(e['start_local'])
            el = datetime.fromisoformat(e['end_local']) if e.get('end_local') else None
            if el:
                time_str = f"{sl.strftime('%H:%M')}–{el.strftime('%H:%M')}"
            else:
                time_str = sl.strftime('%H:%M')
        else:
            time_str = '?'

        dur_str = f"{dur:.1f}h" if dur is not None else ('all-day' if is_all_day else '?')
        att_str = f"att:{n_att}"

        prefix = f"  {time_str:<14} {dur_str:<7} {att_str:<7} [{type_label:<9}] [{cat_label:<30}]"
        lines.append(f"{prefix} {subj}")

    # Gaps section
    lines.append("")
    lines.append("=" * 120)
    lines.append("GAPS IN WORKING HOURS (>45 min, no calendar block)")
    lines.append("  → These need email cross-reference or manual fill-in")
    lines.append("=" * 120)
    for g in gaps:
        lines.append(f"  {g['date']}  {g['start']}–{g['end']}  ({g['duration_min']} min)  |  {g['context']}")

    # Uncategorized
    uncat = [e for e in events if e.get('category') == 'uncategorized']
    if uncat:
        lines.append("")
        lines.append("=" * 120)
        lines.append("⚠  UNCATEGORIZED EVENTS — Needs Manual Review")
        lines.append("=" * 120)
        for e in uncat:
            lines.append(f"  {e.get('day_name'):<14} {e.get('subject','?')[:70]}")

    # Summary
    lines.append("")
    lines.append("=" * 120)
    lines.append("30-DAY SUMMARY BY CATEGORY")
    lines.append("=" * 120)
    s30 = summaries['30d']
    lines.append(f"  Total events tracked:     {s30['total_events']}")
    lines.append(f"  Meeting hours (30d):      {s30['total_meeting_h']}h  ({s30['meeting_h_per_wk']}h/wk)")
    lines.append(f"  Solo work hours (30d):    {s30['total_solo_h']}h  ({s30['solo_h_per_wk']}h/wk)")
    lines.append(f"  Total committed (30d):    {s30['total_h']}h  (~{s30['pct_of_40h']}% of 40h week)")
    lines.append("")
    lines.append(f"  {'Category':<35} {'Hours':>8}  {'Count':>6}  {'Hrs/Wk':>8}")
    lines.append(f"  {'-'*35} {'-'*8}  {'-'*6}  {'-'*8}")
    for cat, meta in CATEGORY_META.items():
        data = s30['by_category'][cat]
        if data['hours'] > 0:
            lines.append(f"  {meta['label']:<35} {data['hours']:>7.1f}h  {data['count']:>6}  {data['hrs_per_wk']:>7.1f}h")

    report_text = '\n'.join(lines)

    # ── Step 6: Write outputs ──────────────────────────────────────────────────
    with open(report_file, 'w') as f:
        f.write(report_text)

    with open(categorized_file, 'w') as f:
        json.dump(events, f, indent=2, default=str)

    with open(gaps_file, 'w') as f:
        json.dump(gaps, f, indent=2)

    with open(summary_file, 'w') as f:
        json.dump(summaries, f, indent=2)

    print(report_text)
    print(f"\n✓  Outputs written:")
    print(f"   {categorized_file}  — every event with block_type + category")
    print(f"   {report_file}       — full human-readable audit")
    print(f"   {gaps_file}         — working-hour gaps to review")
    print(f"   {summary_file}      — hours by category for 7d/30d/YTD dashboard")


if __name__ == '__main__':
    run()
