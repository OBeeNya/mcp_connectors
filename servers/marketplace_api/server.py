from mcp.server.fastmcp import FastMCP
import json
from pathlib import Path
from typing import Literal
from dotenv import load_dotenv

load_dotenv()

FIXTURES_DIR = Path(__file__).parent / "fixtures"
SPECS_DIR = Path(__file__).parent / "specs"

SUPPORTED_MARKETPLACES = ["amazon", "ebay", "rakuten"]

mcp = FastMCP(
    "marketplace-api-server",
    instructions="""
    Authoritative reference server for marketplace API data (Amazon, eBay, Rakuten).
    Use this server at the very start of any connector project — before writing code.
    Call get_api_spec first, then get_auth_config, then get_rate_limits, then list_endpoints.
    """,
)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _require_marketplace(marketplace: str) -> str:
    normalized = marketplace.lower()
    if normalized not in SUPPORTED_MARKETPLACES:
        raise ValueError(
            f"Unsupported marketplace '{marketplace}'. "
            f"Supported: {', '.join(SUPPORTED_MARKETPLACES)}"
        )
    return normalized


def _load(marketplace: str, fixture: str) -> dict | list:
    path = FIXTURES_DIR / marketplace / f"{fixture}.json"
    if not path.exists():
        raise FileNotFoundError(f"Fixture not found: {path}")
    return json.loads(path.read_text())


def _load_spec(marketplace: str) -> dict:
    path = SPECS_DIR / f"{marketplace}.json"
    if not path.exists():
        raise FileNotFoundError(f"Spec not found: {path}")
    return json.loads(path.read_text())


# ---------------------------------------------------------------------------
# Tools
# ---------------------------------------------------------------------------


@mcp.tool()
def list_supported_marketplaces() -> list[str]:
    """Return the list of marketplaces this server has data for."""
    return SUPPORTED_MARKETPLACES


@mcp.tool()
def get_api_spec(marketplace: str) -> dict:
    """
    Return a high-level overview of the marketplace API: base URL, API style (REST/SOAP/mixed),
    available API groups, versioning strategy, and documentation links.

    Use this as your FIRST call when starting a new connector.
    """
    m = _require_marketplace(marketplace)
    return _load_spec(m)


@mcp.tool()
def list_endpoints(
    marketplace: str,
    category: Literal[
        "orders", "catalog", "inventory", "fulfillment", "reports", "returns"
    ] = "orders",
) -> list[dict]:
    """
    List available API endpoints for a given category with method, path, parameters,
    pagination strategy, and usage guidance.

    Categories: orders | catalog | inventory | fulfillment | reports | returns
    """
    m = _require_marketplace(marketplace)
    endpoints: dict = _load(m, "endpoints")
    if category not in endpoints:
        available = list(endpoints.keys())
        raise ValueError(
            f"Category '{category}' not available for {marketplace}. Available: {available}"
        )
    return endpoints[category]


@mcp.tool()
def get_auth_config(marketplace: str) -> dict:
    """
    Return the complete authentication configuration for a marketplace.

    Includes: auth scheme, required credentials, token exchange flows,
    request signing requirements, required environment variables, and sandbox details.

    Always implement auth before any other connector module.
    """
    m = _require_marketplace(marketplace)
    return _load(m, "auth")


@mcp.tool()
def get_rate_limits(marketplace: str, endpoint: str | None = None) -> dict:
    """
    Return rate limit data for a marketplace.

    If endpoint is provided (e.g. "getOrders"), return limits for that specific endpoint.
    If omitted, return the full rate limit table and throttling/retry strategy.

    Use this to design the HTTP client's rate limiter and retry logic.
    """
    m = _require_marketplace(marketplace)
    data: dict = _load(m, "rate_limits")

    if endpoint is not None:
        ep_data = data.get("endpoints", {})
        if endpoint not in ep_data:
            return {
                "warning": f"No specific rate limit data for endpoint '{endpoint}'",
                "available_endpoints": list(ep_data.keys()),
                "throttling_behavior": data.get("throttling_behavior"),
            }
        return {
            "endpoint": endpoint,
            "limits": ep_data[endpoint],
            "throttling_behavior": data.get("throttling_behavior"),
        }
    return data


@mcp.tool()
def get_error_codes(marketplace: str, http_status: int | None = None) -> list[dict]:
    """
    Return documented error codes with descriptions, retry guidance, and recommended actions.

    If http_status is provided (e.g. 429, 403, 500), filter to codes matching that status.
    Use this to build the error handling matrix for the connector.
    """
    m = _require_marketplace(marketplace)
    codes: list = _load(m, "error_codes")
    if http_status is not None:
        return [c for c in codes if c.get("http_status") == http_status]
    return codes


# ---------------------------------------------------------------------------
# Resources
# ---------------------------------------------------------------------------


@mcp.resource("marketplace://amazon/spec")
def amazon_spec() -> str:
    """Amazon SP-API structured specification."""
    return json.dumps(_load_spec("amazon"), indent=2)


@mcp.resource("marketplace://ebay/spec")
def ebay_spec() -> str:
    """eBay REST API structured specification."""
    return json.dumps(_load_spec("ebay"), indent=2)


@mcp.resource("marketplace://rakuten/spec")
def rakuten_spec() -> str:
    """Rakuten RMS API structured specification."""
    return json.dumps(_load_spec("rakuten"), indent=2)


@mcp.resource("marketplace://{marketplace}/auth")
def marketplace_auth(marketplace: str) -> str:
    """Authentication configuration for a supported marketplace."""
    m = _require_marketplace(marketplace)
    return json.dumps(_load(m, "auth"), indent=2)


@mcp.resource("marketplace://{marketplace}/rate-limits")
def marketplace_rate_limits(marketplace: str) -> str:
    """Full rate limit table for a supported marketplace."""
    m = _require_marketplace(marketplace)
    return json.dumps(_load(m, "rate_limits"), indent=2)


# ---------------------------------------------------------------------------
# Prompts
# ---------------------------------------------------------------------------


@mcp.prompt()
def connector_kickoff(marketplace: str) -> str:
    return f"""
You are starting a new ecommerce connector for **{marketplace}**.

Follow these steps in order before writing any code:

1. Call `get_api_spec("{marketplace}")` — understand the API style, versioning, and base URL.
2. Call `get_auth_config("{marketplace}")` — authentication is always the first module implemented.
   Note every required environment variable and credential setup step.
3. Call `get_rate_limits("{marketplace}")` — rate limits drive the HTTP client design.
   Note the throttling strategy (token bucket vs daily quota) and retry behavior.
4. Call `list_endpoints("{marketplace}", category="orders")` then repeat for
   "catalog", "inventory", "fulfillment", and "reports".
5. Call `get_error_codes("{marketplace}")` — classify errors as retryable, dead-letter, or alert.

After gathering all information, produce a **Connector Implementation Plan** covering:
- **Auth module**: credential loading, token refresh lifecycle, signing if required
- **HTTP client**: base class with auth injection, rate limiting, retry with backoff
- **Order ingestion**: endpoint sequence, pagination strategy, polling interval
- **Catalog sync**: create/update/delete flow, batching strategy
- **Inventory sync**: delta vs full sync recommendation based on rate limits
- **Error handling matrix**: retryable / dead-letter / alert classification per error code
- **Open questions**: anything requiring sandbox verification before implementation

Flag any requirements that differ significantly from a standard REST connector.
"""


@mcp.prompt()
def auth_implementation_guide(marketplace: str) -> str:
    return f"""
You are implementing the authentication module for the {marketplace} connector.

1. Call `get_auth_config("{marketplace}")` to get the full auth specification.
2. Implement the auth module with:
   - Credential loading from environment variables (never hardcode secrets)
   - Token exchange / refresh logic with proper error handling
   - Token caching with TTL respect (avoid unnecessary token requests)
   - Request signing if required (e.g. AWS SigV4 for Amazon)
3. Call `get_error_codes("{marketplace}", http_status=401)` and
   `get_error_codes("{marketplace}", http_status=403)` to handle auth errors correctly.
4. Write unit tests covering:
   - Successful token acquisition
   - Transparent token refresh on expiry
   - Retry on transient auth failure
   - Hard failure on invalid credentials (no infinite retry)

Output the complete auth module code following the project's coding standards.
"""


@mcp.prompt()
def rate_limit_strategy(marketplace: str) -> str:
    return f"""
You are designing the rate limiting and retry strategy for the {marketplace} connector.

1. Call `get_rate_limits("{marketplace}")` to get the full rate limit table.
2. Call `get_error_codes("{marketplace}", http_status=429)` to get throttling error details.
3. Based on the data, implement:
   - A rate limiter appropriate to the marketplace's model
     (token bucket for per-second limits, quota tracker for daily limits)
   - Exponential backoff with jitter for retryable errors
   - Per-endpoint rate limit awareness for endpoints with different quotas
4. Output the HTTP client base class with rate limiting and retry baked in.
   The class should be reusable by all domain modules (orders, catalog, inventory).
"""


def main():
    mcp.run()


if __name__ == "__main__":
    main()
