# Marketplace Connectors MCP

MCP servers that accelerate ecommerce marketplace connector development for Amazon, eBay, and Rakuten.

Each server encodes marketplace-specific knowledge (auth flows, rate limits, endpoint quirks, error codes) so developer agents can consume it directly without reading raw documentation.

## Architecture

```
servers/
├── marketplace_api/      ← live: API specs, auth, rate limits, endpoints, error codes
├── schema_mapping/       ← planned: field mapping between marketplace and internal models
├── codegen/              ← planned: connector scaffolding and code generation
├── testing/              ← planned: test generation and sandbox interaction
└── compliance/           ← planned: marketplace listing rules and validation
```

## Requirements

- Python 3.12+
- [uv](https://docs.astral.sh/uv/getting-started/installation/) (recommended) or pip

## Setup

### 1. Clone

```bash
git clone <repo-url>
cd marketplace-connectors-mcp
```

### 2. Install dependencies

```bash
uv sync
```

This installs the exact versions pinned in `uv.lock` — guaranteed reproducible on any machine.

### 3. Configure credentials

```bash
cp .env.example .env
```

Open `.env` and fill in the credentials for the marketplaces you work with. The server works without credentials — you only need them when connector code calls the marketplace APIs at runtime.

See `.env.example` for the full list and where to obtain each credential.

### 4. Run the MCP server

**Development mode** (with the MCP Inspector UI):
```bash
uv run mcp dev servers/marketplace_api/server.py
```

**Production / stdio mode** (used by Claude Desktop and Claude Code):
```bash
uv run marketplace-api-server
```

---

## Connecting to Claude

### Claude Code (CLI)

Register the server once:
```bash
claude mcp add marketplace-api -- uv --directory /absolute/path/to/marketplace-connectors-mcp run marketplace-api-server
```

Verify:
```bash
claude mcp list
```

### Claude Desktop

Add to your Claude Desktop config file:

| OS | Config path |
|---|---|
| macOS | `~/Library/Application Support/Claude/claude_desktop_config.json` |
| Windows | `%APPDATA%\Claude\claude_desktop_config.json` |
| Linux | `~/.config/Claude/claude_desktop_config.json` |

See `mcp_config.example.json` for the exact block to add.

---

## Available Tools

| Tool | Description |
|---|---|
| `list_supported_marketplaces()` | List available marketplaces |
| `get_api_spec(marketplace)` | API style, groups, base URL, common gotchas |
| `get_auth_config(marketplace)` | Auth scheme, token flows, required env vars, sandbox details |
| `list_endpoints(marketplace, category)` | Endpoints with params, pagination, and usage notes |
| `get_rate_limits(marketplace, endpoint?)` | Rate limit table and throttling/retry strategy |
| `get_error_codes(marketplace, http_status?)` | Error codes with retryable flag and recommended action |

**Categories for `list_endpoints`:** `orders` · `catalog` · `inventory` · `fulfillment` · `reports` · `returns`

## Available Prompts

| Prompt | Description |
|---|---|
| `connector_kickoff(marketplace)` | Structured discovery sequence — run before writing any code |
| `auth_implementation_guide(marketplace)` | Auth module spec including token refresh and edge cases |
| `rate_limit_strategy(marketplace)` | Designs the HTTP client rate limiter and retry logic |

---

## Project structure

```
.
├── .env.example                        # Credential template — copy to .env
├── mcp_config.example.json             # Claude Desktop / Claude Code MCP config
├── pyproject.toml                      # Project metadata and dependencies
├── uv.lock                             # Pinned dependency versions
└── servers/
    └── marketplace_api/
        ├── server.py                   # FastMCP server (tools, resources, prompts)
        ├── specs/                      # High-level API overviews per marketplace
        └── fixtures/                   # Detailed reference data per marketplace
            ├── amazon/  (auth | rate_limits | error_codes | endpoints)
            ├── ebay/
            └── rakuten/
```
