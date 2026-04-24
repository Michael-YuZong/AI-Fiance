"""Natural-language command router for non-technical users."""

from __future__ import annotations

import argparse
import re
import subprocess
import sys

from src.collectors import AssetLookupCollector
from src.processors.request_router import route_request
from src.storage.portfolio import PortfolioRepository
from src.utils.config import PROJECT_ROOT, load_config
from src.utils.data import load_watchlist


NEWS_SOURCE_ALIASES = {
    "Reuters": ["reuters", "路透"],
    "Bloomberg": ["bloomberg", "彭博"],
    "Financial Times": ["financial times", "ft", "金融时报"],
    "华尔街见闻": ["华尔街见闻"],
    "财联社": ["财联社"],
}

COMMAND_TIMEOUTS = {
    "discover": 45,
    "scan": 90,
    "compare": 45,
    "briefing": 45,
    "intel": 45,
    "backtest": 60,
    "risk": 30,
}


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Route natural-language requests to the right command.")
    parser.add_argument("request", nargs="+", help="Natural-language request")
    parser.add_argument("--config", default="", help="Optional path to config YAML")
    return parser


def main() -> None:
    args = build_parser().parse_args()
    request = " ".join(args.request).strip()
    config = load_config(args.config or None)

    watchlist_symbols = [item["symbol"] for item in load_watchlist()]
    portfolio_symbols = [item["symbol"] for item in PortfolioRepository().list_holdings()]
    explicit_symbol = _has_explicit_symbol(request)
    resolved_assets = AssetLookupCollector(config).search(request, limit=6) if _should_resolve_assets(request) and not explicit_symbol else []
    resolved_symbols = [item["symbol"] for item in resolved_assets]
    routed = route_request(
        request,
        candidate_symbols=watchlist_symbols + portfolio_symbols,
        resolved_symbols=resolved_symbols,
    )

    cmd = [sys.executable, "-m", f"src.commands.{routed.module}", *routed.args]
    if routed.module == "briefing":
        for source in _detect_news_sources(request):
            cmd.extend(["--news-source", source])
    if args.config:
        cmd.extend(["--config", args.config])

    try:
        result = subprocess.run(
            cmd,
            cwd=str(PROJECT_ROOT),
            capture_output=True,
            text=True,
            timeout=COMMAND_TIMEOUTS.get(routed.module, 15),
        )
        timed_out = False
    except subprocess.TimeoutExpired as exc:
        timed_out = True
        stdout = exc.stdout.decode("utf-8", errors="ignore") if isinstance(exc.stdout, bytes) else (exc.stdout or "")
        stderr = exc.stderr.decode("utf-8", errors="ignore") if isinstance(exc.stderr, bytes) else (exc.stderr or "")
        result = subprocess.CompletedProcess(
            cmd,
            returncode=124,
            stdout=stdout,
            stderr=stderr + "\nCommand timed out.",
        )

    lines = [
        "# 智能入口",
        "",
        f"- 请求: {request}",
        f"- 路由: `{routed.display}`",
        f"- 原因: {routed.reason}",
        "",
    ]
    if resolved_assets:
        lines.extend(
            [
                "## 识别到的标的",
                *[
                    f"- {item['name']} -> `{item['symbol']}` ({item.get('match_type', 'unknown')})"
                    for item in resolved_assets[:4]
                ],
                "",
            ]
        )

    stdout = result.stdout.strip()
    stderr = result.stderr.strip()
    if result.returncode == 0 and stdout:
        lines.append(stdout)
    elif result.returncode == 0:
        lines.append("命令执行成功，但没有返回正文。")
    elif timed_out:
        lines.append("## 执行超时")
        lines.append("- 下游数据源响应过慢，已停止继续等待。")
        if resolved_assets:
            lines.append("- 已经完成标的识别，你可以先用上面的代码直接跑 `scan`，或先用 `lookup` 看候选列表。")
    else:
        lines.append("## 执行失败")
        lines.append(f"- 返回码: {result.returncode}")
        if stderr:
            lines.append(f"- 错误: {stderr.splitlines()[-1]}")
        if routed.module != "research":
            fallback = subprocess.run(
                [sys.executable, "-m", "src.commands.research", request, *(["--config", args.config] if args.config else [])],
                cwd=str(PROJECT_ROOT),
                capture_output=True,
                text=True,
            )
            if fallback.returncode == 0 and fallback.stdout.strip():
                lines.extend(["", "## 回退研究回答", fallback.stdout.strip()])

    print("\n".join(lines))


def _detect_news_sources(request: str) -> list[str]:
    lowered = request.lower()
    matched: list[str] = []
    for canonical, aliases in NEWS_SOURCE_ALIASES.items():
        if any(alias.lower() in lowered for alias in aliases):
            matched.append(canonical)
    return matched


def _should_resolve_assets(request: str) -> bool:
    if re.search(r"\b[A-Z]{1,5}\b|\b\d{5,6}\b|\b[A-Z]{1,2}\d\b", request.upper()):
        return True
    keywords = [
        "etf",
        "基金",
        "指数",
        "标的",
        "代码",
        "编号",
        "黄金",
        "芯片",
        "光伏",
        "医药",
        "有色",
        "电网",
        "中概互联",
        "恒生科技",
        "纳斯达克",
        "纳指",
        "沪深300",
        "中证a500",
        "a500",
        "商业航天",
        "航天",
        "卫星",
        "军工",
    ]
    lowered = request.lower()
    return any(keyword.lower() in lowered for keyword in keywords)


def _has_explicit_symbol(request: str) -> bool:
    return bool(re.search(r"\b[A-Z]{1,5}\b|(?<!\d)\d{5,6}(?!\d)|\b[A-Z]{1,2}\d\b", request.upper()))


if __name__ == "__main__":
    main()
