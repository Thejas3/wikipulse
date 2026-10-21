# 5. Spike definition: PRD z-score rule plus at least 2 distinct editors

Date: 2026-10-20 · Status: accepted

## Context

PRD §6.4: flag a page when its current 5-minute edit count has z > 3 against its rolling 24-hour baseline, and the count is at least 5.

Measured over 20.9 h of Silver article edits, 5-minute page windows with at least 5 edits:

| Windows with ≥ 5 edits | per hour |
|---|---|
| all | 17 |
| single editor | 15 |
| ≥ 2 distinct editors | 2 |
| all bot | < 1 |

Almost 9 in 10 are one person saving repeatedly while writing or fixing an article. Those aren't "the world is editing this page" events, and they'd bury real bursts on the dashboard.

## Decision

A window is a spike when **all** of these hold:

- `edit_count >= 5` (PRD)
- `unique_editors >= 2` (added)
- `z = (count - mean) / max(std, 1.0) > 3` (PRD), where mean and std come from the page's tumbling 5-minute buckets over the previous 24 h, with buckets that had no edits counted as zeros. Buckets overlapping the current window are excluded.
- At least 12 buckets (1 h) of history exist. Before that, nothing is flagged.

The std floor of 1.0 handles pages with no prior edits (std 0): 5 edits from 2 people on a quiet page gives z ≈ 5, a spike. A busy page needs a real jump.

## Consequences

- About 2 candidate windows per hour, before the z-score filter, instead of 17.
- One editor's burst of saves isn't a spike. If that's vandalism, the vandalism score (PRD §6.4, "edit burst from one user") is where it belongs.
- A burst lasting several minutes appears in several overlapping sliding windows, as one row each (keyed by page and `window_start`). The dashboard groups them by page.
- Thresholds live as constants at the top of `streaming/gold_job.py`.
