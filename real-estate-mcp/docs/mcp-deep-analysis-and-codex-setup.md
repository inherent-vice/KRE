# MCP Deep Analysis and Codex Setup

This document summarizes the current `real-estate-mcp` codebase and the setup required so Codex can use it immediately.

## 1) Server Architecture

Core structure:

- `src/real_estate/mcp_server/__init__.py`
  - Creates `FastMCP("real-estate")`
  - Loads `.env` from project root
- `src/real_estate/mcp_server/server.py`
  - Entry point
  - Registers core tools: `get_region_code`, `get_current_year_month`
  - Imports all tool modules so `@mcp.tool()` functions are registered
  - Supports `stdio` and `http` transport
- `src/real_estate/mcp_server/tools/*.py`
  - Domain tools: `trade`, `rent`, `subscription`, `onbid`, `finance`
- `src/real_estate/mcp_server/_helpers.py`
  - API endpoints
  - API key guards
  - HTTP fetch and XML/JSON parse wrappers
  - Shared summary builders and standardized error responses
- `src/real_estate/mcp_server/parsers/*.py`
  - Domain-specific XML parsing
- `src/real_estate/mcp_server/_region.py`
  - Region code lookup from `src/real_estate/resources/region_codes.txt`

Execution model:

1. Tool validates required API key(s).
2. URL is built with encoded key.
3. HTTP request executes via async `httpx`.
4. Response is parsed and normalized.
5. Result returns in standard dict shape or error dict.

## 2) Tool Inventory

Generated from source via `scripts/analyze_mcp.py`.

- Total tools: `26`
- Async tools: `21`
- Sync tools: `5`
- Domain distribution:
  - `core`: 2
  - `finance`: 3
  - `onbid`: 10
  - `rent`: 4
  - `subscription`: 2
  - `trade`: 5

Machine-readable manifest:

- `resources/mcp_tool_manifest.json`

## 3) Environment Variable Matrix

- Trade/Rent/Commercial tools:
  - `DATA_GO_KR_API_KEY` required
- Onbid tools:
  - `ONBID_API_KEY` preferred
  - fallback: `DATA_GO_KR_API_KEY`
- Subscription tools:
  - `ODCLOUD_API_KEY` or `ODCLOUD_SERVICE_KEY` preferred
  - fallback: `DATA_GO_KR_API_KEY`
- Core/Finance tools:
  - no API key required

## 4) Codex-Usable Setup

Project script:

- `scripts/setup_codex_mcp.ps1`

Usage:

```powershell
pwsh ./scripts/setup_codex_mcp.ps1 `
  -ProjectRoot "C:\Devs\KRE\real-estate-mcp" `
  -DataGoKrApiKey "<YOUR_KEY>"
```

Optional additional keys:

- `-OdcloudApiKey`
- `-OdcloudServiceKey`
- `-OnbidApiKey`

What the script does:

1. Validates `codex` and `uv` are installed.
2. Registers `real-estate` server through `codex mcp add`.
3. Prints `codex mcp get real-estate --json` for verification.

## 5) Local Tool Runner for Agent Workflows

Project script:

- `scripts/run_local_mcp_tool.py`

Use this to call MCP tool implementations directly from shell when needed.

Examples:

```bash
uv run --directory C:/Devs/KRE/real-estate-mcp python scripts/run_local_mcp_tool.py --list
```

```bash
uv run --directory C:/Devs/KRE/real-estate-mcp python scripts/run_local_mcp_tool.py get_region_code --arg query=송파구
```

```bash
uv run --directory C:/Devs/KRE/real-estate-mcp python scripts/run_local_mcp_tool.py get_apartment_trades --arg region_code=11710 --arg year_month=202601 --arg num_of_rows=5
```

## 6) Regeneration Command

If tool signatures or docs change, regenerate manifest:

```bash
uv run --directory C:/Devs/KRE/real-estate-mcp python scripts/analyze_mcp.py
```
