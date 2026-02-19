from __future__ import annotations

import argparse
import asyncio
import inspect
import json
from typing import Any, Callable

from real_estate.mcp_server import server as server_module
from real_estate.mcp_server.tools import finance, onbid, rent, subscription, trade


def _discover_tools() -> dict[str, Callable[..., Any]]:
    modules = [server_module, finance, onbid, rent, subscription, trade]
    tools: dict[str, Callable[..., Any]] = {}

    for module in modules:
        for name, value in vars(module).items():
            if not inspect.isfunction(value):
                continue
            if value.__module__ != module.__name__:
                continue
            if name.startswith("_") or name == "main":
                continue
            tools[name] = value

    return tools


def _parse_key_value(items: list[str]) -> dict[str, Any]:
    parsed: dict[str, Any] = {}
    for item in items:
        if "=" not in item:
            raise ValueError(f"Invalid --arg format: {item}. Expected key=value")
        key, value = item.split("=", 1)
        value = value.strip()
        if value.lower() in {"true", "false"}:
            parsed[key] = value.lower() == "true"
            continue
        if value.isdigit() or (value.startswith("-") and value[1:].isdigit()):
            parsed[key] = int(value)
            continue
        try:
            parsed[key] = float(value)
        except ValueError:
            parsed[key] = value
    return parsed


async def _run_tool(func: Callable[..., Any], kwargs: dict[str, Any]) -> Any:
    result = func(**kwargs)
    if inspect.isawaitable(result):
        return await result
    return result


def main() -> None:
    parser = argparse.ArgumentParser(description="Run real-estate MCP tool implementation locally.")
    parser.add_argument("tool", nargs="?", help="Tool name to execute")
    parser.add_argument(
        "--arg",
        action="append",
        default=[],
        help="Tool argument in key=value form; repeatable",
    )
    parser.add_argument(
        "--args-json",
        default="",
        help='JSON object containing tool args, e.g. {"region_code":"11710","year_month":"202601"}',
    )
    parser.add_argument(
        "--list",
        action="store_true",
        help="List available tools and exit",
    )
    args = parser.parse_args()

    tools = _discover_tools()

    if args.list:
        for name in sorted(tools):
            print(name)
        return

    if not args.tool:
        raise SystemExit("Tool name is required unless --list is used")
    if args.tool not in tools:
        raise SystemExit(f"Unknown tool: {args.tool}")

    kwargs: dict[str, Any] = {}
    if args.args_json:
        loaded = json.loads(args.args_json)
        if not isinstance(loaded, dict):
            raise SystemExit("--args-json must be a JSON object")
        kwargs.update(loaded)
    kwargs.update(_parse_key_value(args.arg))

    result = asyncio.run(_run_tool(tools[args.tool], kwargs))
    print(json.dumps(result, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
