from __future__ import annotations

import argparse
import ast
import json
from pathlib import Path
from typing import Any


def _is_mcp_tool(decorator: ast.expr) -> bool:
    target = decorator.func if isinstance(decorator, ast.Call) else decorator
    return isinstance(target, ast.Attribute) and target.attr == "tool"


def _annotation_to_str(node: ast.expr | None) -> str | None:
    if node is None:
        return None
    return ast.unparse(node)


def _expr_to_str(node: ast.expr) -> str:
    return ast.unparse(node)


def _default_list(function_node: ast.FunctionDef | ast.AsyncFunctionDef) -> list[ast.expr | None]:
    args_count = len(function_node.args.args)
    defaults_count = len(function_node.args.defaults)
    return [None] * (args_count - defaults_count) + list(function_node.args.defaults)


def _requires_env(tool_name: str) -> list[str]:
    if tool_name in {
        "get_region_code",
        "get_current_year_month",
        "calculate_loan_payment",
        "calculate_compound_growth",
        "calculate_monthly_cashflow",
    }:
        return []

    if tool_name.startswith("get_apt_subscription_"):
        return [
            "ODCLOUD_API_KEY or ODCLOUD_SERVICE_KEY",
            "DATA_GO_KR_API_KEY (fallback)",
        ]

    if tool_name.startswith("get_onbid_") or tool_name.startswith("get_public_auction_"):
        return [
            "ONBID_API_KEY",
            "DATA_GO_KR_API_KEY (fallback)",
        ]

    if tool_name.startswith("get_") and (
        "trade" in tool_name
        or "rent" in tool_name
        or tool_name == "get_commercial_trade"
    ):
        return ["DATA_GO_KR_API_KEY"]

    return []


def _tool_domain(module_path: str, tool_name: str) -> str:
    if module_path == "server.py":
        if tool_name in {"get_region_code", "get_current_year_month"}:
            return "core"
        return "server"
    return Path(module_path).stem


def _extract_tools(server_dir: Path) -> list[dict[str, Any]]:
    files = [server_dir / "server.py", *sorted((server_dir / "tools").glob("*.py"))]
    tools: list[dict[str, Any]] = []

    for file_path in files:
        tree = ast.parse(file_path.read_text(encoding="utf-8"))
        module_rel = file_path.relative_to(server_dir).as_posix()

        for node in tree.body:
            if not isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)):
                continue

            if not any(_is_mcp_tool(dec) for dec in node.decorator_list):
                continue

            defaults = _default_list(node)
            params: list[dict[str, str | None]] = []
            for arg_node, default_node in zip(node.args.args, defaults, strict=True):
                params.append(
                    {
                        "name": arg_node.arg,
                        "annotation": _annotation_to_str(arg_node.annotation),
                        "default": _expr_to_str(default_node) if default_node is not None else None,
                    }
                )

            doc = ast.get_docstring(node) or ""
            first_line = doc.strip().splitlines()[0] if doc.strip() else ""

            tools.append(
                {
                    "name": node.name,
                    "domain": _tool_domain(module_rel, node.name),
                    "module": module_rel,
                    "kind": "async" if isinstance(node, ast.AsyncFunctionDef) else "sync",
                    "description": first_line,
                    "parameters": params,
                    "returns": _annotation_to_str(node.returns),
                    "requires_env": _requires_env(node.name),
                }
            )

    tools.sort(key=lambda item: (item["domain"], item["name"]))
    return tools


def _build_report(tools: list[dict[str, Any]]) -> dict[str, Any]:
    domain_counts: dict[str, int] = {}
    for tool in tools:
        domain = tool["domain"]
        domain_counts[domain] = domain_counts.get(domain, 0) + 1

    return {
        "summary": {
            "tool_count": len(tools),
            "async_tool_count": sum(1 for t in tools if t["kind"] == "async"),
            "sync_tool_count": sum(1 for t in tools if t["kind"] == "sync"),
            "domain_counts": dict(sorted(domain_counts.items())),
            "required_env_vars": [
                "DATA_GO_KR_API_KEY",
                "ODCLOUD_API_KEY",
                "ODCLOUD_SERVICE_KEY",
                "ONBID_API_KEY",
            ],
        },
        "recommended_call_sequences": [
            {
                "goal": "Trade and rent market check",
                "sequence": [
                    "get_region_code",
                    "get_current_year_month",
                    "get_apartment_trades or get_officetel_trades or get_villa_trades or get_single_house_trades",
                    "get_apartment_rent or get_officetel_rent or get_villa_rent or get_single_house_rent",
                ],
            },
            {
                "goal": "Subscription analysis",
                "sequence": [
                    "get_apt_subscription_info",
                    "get_apt_subscription_results",
                ],
            },
            {
                "goal": "Public auction analysis",
                "sequence": [
                    "get_onbid_top_code_info / get_onbid_addr1_info",
                    "get_onbid_middle_code_info / get_onbid_addr2_info / get_onbid_addr3_info",
                    "get_onbid_thing_info_list or get_public_auction_items",
                    "get_public_auction_item_detail (optional)",
                ],
            },
        ],
        "tools": tools,
    }


def main() -> None:
    parser = argparse.ArgumentParser(description="Analyze real-estate MCP tools and emit JSON manifest.")
    parser.add_argument(
        "--project-root",
        default=str(Path(__file__).resolve().parents[1]),
        help="Path to the real-estate-mcp project root",
    )
    parser.add_argument(
        "--output",
        default="resources/mcp_tool_manifest.json",
        help="Output file path relative to project root",
    )
    args = parser.parse_args()

    project_root = Path(args.project_root).resolve()
    server_dir = project_root / "src" / "real_estate" / "mcp_server"
    output_path = project_root / args.output

    tools = _extract_tools(server_dir)
    report = _build_report(tools)

    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(json.dumps(report, indent=2, ensure_ascii=False) + "\n", encoding="utf-8")

    print(f"Wrote {output_path}")
    print(f"Detected {report['summary']['tool_count']} MCP tools")


if __name__ == "__main__":
    main()
