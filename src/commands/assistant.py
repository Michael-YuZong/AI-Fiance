"""Natural-language command router for non-technical users."""

from __future__ import annotations

import argparse
import subprocess
import sys

from src.processors.request_router import route_request
from src.storage.portfolio import PortfolioRepository
from src.utils.config import PROJECT_ROOT, load_config
from src.utils.data import load_watchlist


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Route natural-language requests to the right command.")
    parser.add_argument("request", nargs="+", help="Natural-language request")
    parser.add_argument("--config", default="", help="Optional path to config YAML")
    return parser


def main() -> None:
    args = build_parser().parse_args()
    request = " ".join(args.request).strip()
    load_config(args.config or None)

    watchlist_symbols = [item["symbol"] for item in load_watchlist()]
    portfolio_symbols = [item["symbol"] for item in PortfolioRepository().list_holdings()]
    routed = route_request(request, candidate_symbols=watchlist_symbols + portfolio_symbols)

    cmd = [sys.executable, "-m", f"src.commands.{routed.module}", *routed.args]
    if args.config:
        cmd.extend(["--config", args.config])

    result = subprocess.run(
        cmd,
        cwd=str(PROJECT_ROOT),
        capture_output=True,
        text=True,
    )

    lines = [
        "# 智能入口",
        "",
        f"- 请求: {request}",
        f"- 路由: `{routed.display}`",
        f"- 原因: {routed.reason}",
        "",
    ]

    stdout = result.stdout.strip()
    stderr = result.stderr.strip()
    if result.returncode == 0 and stdout:
        lines.append(stdout)
    elif result.returncode == 0:
        lines.append("命令执行成功，但没有返回正文。")
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


if __name__ == "__main__":
    main()
