"""Tests for scheduler helpers."""

from __future__ import annotations

from src.scheduler import build_scheduled_jobs, render_job_listing


def test_build_scheduled_jobs_uses_daily_time_as_weekly_fallback():
    config = {
        "schedule": {
            "daily_briefing": "07:30",
            "weekly_briefing": "saturday",
            "macro_update": "daily",
        }
    }

    jobs = build_scheduled_jobs(config)
    by_id = {job.job_id: job for job in jobs}

    assert by_id["daily_briefing"].trigger_kwargs == {"trigger": "cron", "hour": 7, "minute": 30}
    assert by_id["weekly_briefing"].trigger_kwargs == {
        "trigger": "cron",
        "day_of_week": "sat",
        "hour": 7,
        "minute": 30,
    }
    assert by_id["macro_update"].trigger_kwargs == {"trigger": "cron", "hour": 6, "minute": 45}


def test_render_job_listing_includes_one_off_jobs():
    config = {"schedule": {"daily_briefing": "07:30", "weekly_briefing": "saturday 09:00"}}
    listing = render_job_listing(build_scheduled_jobs(config))

    assert "## Configured" in listing
    assert "`daily_briefing` | 每日晨报 | 每日 07:30" in listing
    assert "`weekly_briefing` | 每周周报 | 每周 sat 09:00" in listing
    assert "## One-off Only" in listing
    assert "`noon_briefing`" in listing
