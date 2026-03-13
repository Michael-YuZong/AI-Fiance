"""Lightweight scheduler for recurring research workflows."""

from __future__ import annotations

import argparse
import subprocess
import sys
from dataclasses import dataclass
from datetime import datetime
from typing import Any, Iterable, Mapping, Sequence

from apscheduler.schedulers.blocking import BlockingScheduler

from src.utils.config import PROJECT_ROOT, load_config, resolve_project_path

DEFAULT_JOB_TIMES = {
    "daily_briefing": "07:30",
    "weekly_briefing": "07:30",
    "macro_update": "06:45",
    "market_update": "08:45",
}

WEEKDAY_ALIASES = {
    "monday": "mon",
    "mon": "mon",
    "tuesday": "tue",
    "tue": "tue",
    "wednesday": "wed",
    "wed": "wed",
    "thursday": "thu",
    "thu": "thu",
    "friday": "fri",
    "fri": "fri",
    "saturday": "sat",
    "sat": "sat",
    "sunday": "sun",
    "sun": "sun",
}

JOB_CATALOG = {
    "daily_briefing": {
        "label": "每日晨报",
        "command": ("-m", "src.commands.briefing", "daily"),
        "schedule_key": "daily_briefing",
    },
    "weekly_briefing": {
        "label": "每周周报",
        "command": ("-m", "src.commands.briefing", "weekly"),
        "schedule_key": "weekly_briefing",
    },
    "noon_briefing": {
        "label": "午间盘中简报",
        "command": ("-m", "src.commands.briefing", "noon"),
        "schedule_key": "noon_briefing",
    },
    "evening_briefing": {
        "label": "收盘晚报",
        "command": ("-m", "src.commands.briefing", "evening"),
        "schedule_key": "evening_briefing",
    },
    "macro_update": {
        "label": "宏观体制更新",
        "command": ("-m", "src.commands.regime"),
        "schedule_key": "macro_update",
    },
    "market_update": {
        "label": "市场机会扫描",
        "command": ("-m", "src.commands.discover"),
        "schedule_key": "market_update",
    },
}


@dataclass(frozen=True)
class ScheduledJob:
    """A resolved job ready to register with APScheduler."""

    job_id: str
    label: str
    schedule_label: str
    command: tuple[str, ...]
    trigger_kwargs: dict[str, Any]


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Run or schedule recurring research jobs.")
    parser.add_argument("--config", default="", help="Optional path to config YAML")
    subparsers = parser.add_subparsers(dest="command")

    subparsers.add_parser("list", help="List configured and one-off scheduler jobs")

    run_parser = subparsers.add_parser("run", help="Run a job immediately")
    run_parser.add_argument("job_id", choices=tuple(JOB_CATALOG), help="Job id to execute once")

    subparsers.add_parser("serve", help="Start the blocking scheduler with configured jobs")
    return parser


def _parse_time_token(raw: str) -> tuple[int, int]:
    try:
        hour_text, minute_text = raw.split(":", 1)
        hour = int(hour_text)
        minute = int(minute_text)
    except (AttributeError, TypeError, ValueError) as exc:
        raise ValueError(f"无法识别时间配置: {raw}") from exc
    if not 0 <= hour <= 23 or not 0 <= minute <= 59:
        raise ValueError(f"时间超出范围: {raw}")
    return hour, minute


def _normalize_weekday(raw: str) -> str:
    weekday = WEEKDAY_ALIASES.get(str(raw).strip().lower())
    if not weekday:
        raise ValueError(f"无法识别周度调度配置: {raw}")
    return weekday


def _schedule_command(job_id: str, config_path: str = "") -> tuple[str, ...]:
    command = [sys.executable, *JOB_CATALOG[job_id]["command"]]
    if config_path:
        command.extend(["--config", str(resolve_project_path(config_path))])
    return tuple(command)


def _resolve_daily_job(job_id: str, raw: str, config_path: str = "") -> ScheduledJob:
    hour, minute = _parse_time_token(raw)
    return ScheduledJob(
        job_id=job_id,
        label=str(JOB_CATALOG[job_id]["label"]),
        schedule_label=f"每日 {hour:02d}:{minute:02d}",
        command=_schedule_command(job_id, config_path),
        trigger_kwargs={"trigger": "cron", "hour": hour, "minute": minute},
    )


def _resolve_weekly_job(job_id: str, raw: str, fallback_time: str, config_path: str = "") -> ScheduledJob:
    parts = str(raw).strip().lower().split()
    if not parts:
        raise ValueError(f"周度调度不能为空: {job_id}")
    weekday = _normalize_weekday(parts[0])
    time_token = parts[1] if len(parts) > 1 else fallback_time
    hour, minute = _parse_time_token(time_token)
    return ScheduledJob(
        job_id=job_id,
        label=str(JOB_CATALOG[job_id]["label"]),
        schedule_label=f"每周 {weekday} {hour:02d}:{minute:02d}",
        command=_schedule_command(job_id, config_path),
        trigger_kwargs={"trigger": "cron", "day_of_week": weekday, "hour": hour, "minute": minute},
    )


def build_scheduled_jobs(config: Mapping[str, Any], config_path: str = "") -> list[ScheduledJob]:
    schedule_cfg = dict(config.get("schedule", {}) or {})
    daily_time = str(schedule_cfg.get("daily_briefing", DEFAULT_JOB_TIMES["daily_briefing"]))
    jobs: list[ScheduledJob] = []

    for job_id, meta in JOB_CATALOG.items():
        schedule_key = str(meta.get("schedule_key", ""))
        raw = schedule_cfg.get(schedule_key)
        if raw in (None, "", False):
            if job_id == "daily_briefing":
                raw = daily_time
            elif job_id == "weekly_briefing":
                raw = schedule_cfg.get("weekly_briefing", "saturday")
            else:
                continue

        normalized = str(raw).strip().lower()
        if job_id == "weekly_briefing":
            jobs.append(_resolve_weekly_job(job_id, str(raw), daily_time, config_path))
            continue

        if normalized == "daily":
            normalized = DEFAULT_JOB_TIMES.get(job_id, daily_time)
        jobs.append(_resolve_daily_job(job_id, normalized, config_path))

    return jobs


def _scheduled_job_ids(jobs: Sequence[ScheduledJob]) -> set[str]:
    return {job.job_id for job in jobs}


def _run_command(command: Sequence[str], label: str) -> int:
    started_at = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    print(f"[{started_at}] 开始执行 {label}: {' '.join(command)}")
    completed = subprocess.run(command, cwd=PROJECT_ROOT, check=False)
    finished_at = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    print(f"[{finished_at}] 完成 {label}: exit={completed.returncode}")
    return int(completed.returncode)


def run_job_once(job_id: str, config_path: str = "") -> int:
    if job_id not in JOB_CATALOG:
        raise SystemExit(f"未知任务: {job_id}")
    return _run_command(_schedule_command(job_id, config_path), str(JOB_CATALOG[job_id]["label"]))


def render_job_listing(jobs: Sequence[ScheduledJob]) -> str:
    scheduled = list(jobs)
    scheduled_ids = _scheduled_job_ids(scheduled)
    lines = ["# Scheduler Jobs", ""]

    if scheduled:
        lines.extend(["## Configured", ""])
        for job in scheduled:
            lines.append(f"- `{job.job_id}` | {job.label} | {job.schedule_label} | `{' '.join(job.command)}`")
    else:
        lines.extend(["## Configured", "", "- 当前没有解析出可调度任务。"])

    one_off = [job_id for job_id in JOB_CATALOG if job_id not in scheduled_ids]
    if one_off:
        lines.extend(["", "## One-off Only", ""])
        for job_id in one_off:
            command = " ".join(_schedule_command(job_id))
            lines.append(f"- `{job_id}` | {JOB_CATALOG[job_id]['label']} | `scheduler run {job_id}` | `{command}`")

    return "\n".join(lines)


def serve_scheduler(jobs: Iterable[ScheduledJob]) -> None:
    resolved_jobs = list(jobs)
    if not resolved_jobs:
        raise SystemExit("当前配置没有可启动的调度任务。")

    scheduler = BlockingScheduler(timezone=datetime.now().astimezone().tzinfo)
    for job in resolved_jobs:
        scheduler.add_job(
            _run_command,
            id=job.job_id,
            replace_existing=True,
            kwargs={"command": job.command, "label": job.label},
            **job.trigger_kwargs,
        )

    print(render_job_listing(resolved_jobs))
    print("")
    print("Scheduler started. Press Ctrl+C to stop.")
    scheduler.start()


def main() -> None:
    parser = build_parser()
    args = parser.parse_args()
    config_path = args.config or ""
    config = load_config(config_path or None)
    jobs = build_scheduled_jobs(config, config_path)
    command = args.command or "list"

    if command == "list":
        print(render_job_listing(jobs))
        return
    if command == "run":
        raise SystemExit(run_job_once(args.job_id, config_path))
    if command == "serve":
        serve_scheduler(jobs)
        return
    parser.error(f"Unsupported command: {command}")


if __name__ == "__main__":
    main()
