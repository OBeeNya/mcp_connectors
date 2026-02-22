from mcp.server.fastmcp import FastMCP
import json
import re
from pathlib import Path
from typing import Literal
from dotenv import load_dotenv

load_dotenv()

INTERNAL_MODELS_DIR = Path(__file__).parent / "internal_models"
MARKETPLACE_SCHEMAS_DIR = Path(__file__).parent / "marketplace_schemas"
SAVED_MAPPINGS_DIR = Path(__file__).parent / "saved_mappings"

SUPPORTED_ENTITIES = ["Order", "Product", "Inventory"]
SUPPORTED_MARKETPLACES = ["amazon", "ebay", "rakuten"]

VALID_TRANSFORMATIONS = {
    "direct",
    "constant",
    "str_to_decimal",
    "int_to_decimal",
    "datetime_iso",
    "datetime_ymd_hms",
    "enum_map",
    "nested_object",
    "list_transform",
    "pass_raw",
}

mcp = FastMCP(
    "schema-mapping-server",
    instructions="""
    Maps marketplace-specific data models (Amazon, eBay, Rakuten) to the company's internal
    canonical schemas. Use this server to build and validate field mappings, then generate
    Python transformer code. Always start with get_internal_schema + get_marketplace_schema,
    then suggest_field_mapping for ambiguous fields, then validate_mapping before generating code.
    """,
)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

_ENTITY_TO_FILE = {
    "order": "order",
    "product": "product",
    "inventory": "inventory",
}


def _require_marketplace(marketplace: str) -> str:
    n = marketplace.lower()
    if n not in SUPPORTED_MARKETPLACES:
        raise ValueError(f"Unsupported marketplace '{marketplace}'. Supported: {SUPPORTED_MARKETPLACES}")
    return n


def _require_entity(entity: str) -> str:
    normalized = entity.strip().capitalize()
    if normalized not in SUPPORTED_ENTITIES:
        raise ValueError(f"Unsupported entity '{entity}'. Supported: {SUPPORTED_ENTITIES}")
    return normalized


def _entity_filename(entity: str) -> str:
    return _ENTITY_TO_FILE.get(entity.lower(), entity.lower())


def _load(path: Path) -> dict | list:
    if not path.exists():
        raise FileNotFoundError(f"File not found: {path}")
    return json.loads(path.read_text())


# ---------------------------------------------------------------------------
# Field suggestion engine
# ---------------------------------------------------------------------------

_SYNONYMS: dict[str, set[str]] = {
    "id":          {"identifier", "number", "num", "code", "ref"},
    "amount":      {"price", "cost", "value", "total", "sum"},
    "date":        {"time", "datetime", "timestamp", "at"},
    "status":      {"state", "progress", "condition"},
    "quantity":    {"qty", "count", "units", "stock", "inventory"},
    "name":        {"title", "label", "caption"},
    "description": {"text", "content", "detail", "caption", "body"},
    "email":       {"mail"},
    "phone":       {"tel", "telephone"},
    "postal":      {"zip", "postcode"},
    "channel":     {"type", "method"},
    "address":     {"location", "destination"},
}


def _tokenize(field_name: str) -> set[str]:
    """Normalize a dot-path field name into a set of lowercase words."""
    name = field_name.rsplit(".", 1)[-1].rstrip("]").split("[")[0]
    name = re.sub(r"([a-z])([A-Z])", r"\1 \2", name)
    return {w for w in re.split(r"[^a-z0-9]+", name.lower()) if len(w) > 1}


def _expand(words: set[str]) -> set[str]:
    expanded = set(words)
    for word in words:
        for canonical, synonyms in _SYNONYMS.items():
            if word == canonical or word in synonyms:
                expanded.add(canonical)
                expanded.update(synonyms)
    return expanded


def _similarity(source: str, target: str) -> float:
    a = _expand(_tokenize(source))
    b = _expand(_tokenize(target))
    if not a or not b:
        return 0.0
    return len(a & b) / len(a | b)


# ---------------------------------------------------------------------------
# Code generator
# ---------------------------------------------------------------------------

def _nested_get(path: str, root: str = "raw", default: str = "None") -> str:
    """Build a Python expression for nested dict access from a dot-path."""
    parts = path.split(".")
    if len(parts) == 1:
        return f'{root}.get("{parts[0]}", {default})'
    expr = f'{root}.get("{parts[0]}", {{}})'
    for part in parts[1:-1]:
        expr = f'{expr}.get("{part}", {{}})'
    expr = f'{expr}.get("{parts[-1]}", {default})'
    return expr


def _emit_field(m: dict, indent: str, enum_var_map: dict[str, str]) -> list[str]:
    """Emit one or more lines for a single mapping entry."""
    field = m["internal_field"]
    src = m.get("source_path", "")
    transform = m.get("transformation", "direct")
    default = m.get("default_value")
    default_repr = json.dumps(default) if default is not None else "None"
    lines = []

    if transform == "constant":
        val = json.dumps(m.get("value"))
        lines.append(f'{indent}"{field}": {val},')

    elif transform == "pass_raw":
        lines.append(f'{indent}"{field}": raw,')

    elif transform == "direct":
        expr = _nested_get(src, default=default_repr)
        lines.append(f'{indent}"{field}": {expr},')

    elif transform == "str_to_decimal":
        expr = _nested_get(src)
        lines.append(f'{indent}"{field}": Decimal({expr} or "0"),')

    elif transform == "int_to_decimal":
        expr = _nested_get(src, default="0")
        lines.append(f'{indent}"{field}": Decimal(str({expr})),')

    elif transform == "datetime_iso":
        expr = _nested_get(src)
        lines.append(f'{indent}"{field}": _dt_iso({expr}),')

    elif transform == "datetime_ymd_hms":
        expr = _nested_get(src)
        lines.append(f'{indent}"{field}": _dt_rkt({expr}),')

    elif transform == "enum_map":
        var = enum_var_map.get(field, "_ENUM_MAP")
        expr = _nested_get(src)
        lines.append(f'{indent}"{field}": {var}.get({expr}),')

    elif transform == "nested_object":
        nested = m.get("nested_mapping", {})
        obj_src = src or ""
        lines.append(f'{indent}"{field}": {{')
        for int_sub, src_sub in nested.items():
            full = f"{obj_src}.{src_sub}" if obj_src else src_sub
            lines.append(f'{indent}    "{int_sub}": {_nested_get(full)},')
        lines.append(f'{indent}}},')

    elif transform == "list_transform":
        item_mapping = m.get("item_mapping", [])
        lines.append(f'{indent}"{field}": [')
        lines.append(f'{indent}    {{')
        for im in item_mapping:
            lines.extend(_emit_field(im, indent + "        ", enum_var_map))
        lines.append(f'{indent}    }}')
        lines.append(f'{indent}    for {src.rstrip("[]").lower().replace(".", "_")} in raw.get("{src.rstrip("[]")}", [])')
        lines.append(f'{indent}],')
        # Patch: list comprehensions need "item" variable in nested gets
        # Rebuild with correct item variable name
        item_var = src.rstrip("[]").split(".")[-1].lower()
        lines = [f'{indent}"{field}": [']
        lines.append(f'{indent}    {{')
        for im in item_mapping:
            sub_src = im.get("source_path", "")
            sub_transform = im.get("transformation", "direct")
            sub_field = im["internal_field"]
            sub_default = im.get("default_value")
            sub_default_repr = json.dumps(sub_default) if sub_default is not None else "None"
            if sub_transform == "direct":
                expr = f'{item_var}.get("{sub_src}", {sub_default_repr})'
            elif sub_transform == "str_to_decimal":
                expr = f'Decimal({item_var}.get("{sub_src}") or "0")'
            elif sub_transform == "int_to_decimal":
                expr = f'Decimal(str({item_var}.get("{sub_src}", 0)))'
            elif sub_transform == "datetime_iso":
                expr = f'_dt_iso({item_var}.get("{sub_src}"))'
            elif sub_transform == "nested_get":
                # Handle dot-paths within item
                parts = sub_src.split(".")
                expr = f'{item_var}.get("{parts[0]}", {"{}"})'
                for p in parts[1:]:
                    expr = f'{expr}.get("{p}")'
            elif sub_transform == "str_to_decimal_nested":
                parts = sub_src.split(".")
                acc = f'{item_var}.get("{parts[0]}", {{}})'
                for p in parts[1:-1]:
                    acc = f'{acc}.get("{p}", {{}})'
                acc = f'{acc}.get("{parts[-1]}")'
                expr = f'Decimal({acc} or "0")'
            else:
                expr = f'{item_var}.get("{sub_src}", {sub_default_repr})'
            lines.append(f'{indent}        "{sub_field}": {expr},')
        lines.append(f'{indent}    }}')
        lines.append(f'{indent}    for {item_var} in raw.get("{src.rstrip("[]")}", [])')
        lines.append(f'{indent}],')

    else:
        lines.append(f'{indent}"{field}": None,  # TODO: unsupported transformation "{transform}"')

    return lines


def _generate_python(marketplace: str, entity: str, mapping: list[dict]) -> str:
    func_name = f"transform_{marketplace}_{entity.lower()}"

    # Collect enum maps
    enum_var_map: dict[str, str] = {}
    enum_defs: list[str] = []
    for m in mapping:
        if m.get("transformation") == "enum_map":
            var = f'_{m["internal_field"].upper()}_MAP'
            enum_var_map[m["internal_field"]] = var
            enum_defs.append(f"{var} = {{")
            for k, v in m.get("enum_map", {}).items():
                key_repr = json.dumps(k)
                enum_defs.append(f'    {key_repr}: "{v}",')
            enum_defs.append("}")
            enum_defs.append("")

    has_rakuten_dt = any(m.get("transformation") == "datetime_ymd_hms" for m in mapping)
    has_decimal = any(
        m.get("transformation") in ("str_to_decimal", "int_to_decimal")
        or any(
            im.get("transformation") in ("str_to_decimal", "str_to_decimal_nested", "int_to_decimal")
            for im in m.get("item_mapping", [])
        )
        for m in mapping
    )

    out: list[str] = []

    # Imports
    if has_decimal:
        out.append("from decimal import Decimal")
    out.append("from datetime import datetime")
    out.append("")

    # Enum map constants
    out.extend(enum_defs)

    # Function definition
    out.append(f"def {func_name}(raw: dict) -> dict:")
    out.append(f'    """')
    out.append(f'    Transform {marketplace} {entity} payload to the internal canonical model.')
    out.append(f'    Auto-generated from saved_mappings/{marketplace}/{entity.lower()}.json.')
    out.append(f'    Regenerate with: generate_transformer("{marketplace}", "{entity}")')
    out.append(f'    """')

    # Datetime helpers
    out.append("    def _dt_iso(s: str | None):")
    out.append("        if not s:")
    out.append("            return None")
    out.append('        return datetime.fromisoformat(s.replace("Z", "+00:00"))')
    if has_rakuten_dt:
        out.append("")
        out.append("    def _dt_rkt(s: str | None):")
        out.append("        if not s:")
        out.append("            return None")
        out.append('        return datetime.strptime(s, "%Y-%m-%d %H:%M:%S")')

    out.append("")
    out.append("    return {")

    for m in mapping:
        out.extend(_emit_field(m, "        ", enum_var_map))

    out.append("    }")
    out.append("")

    return "\n".join(out)


# ---------------------------------------------------------------------------
# Tools
# ---------------------------------------------------------------------------

@mcp.tool()
def list_entities() -> list[str]:
    """Return the list of supported canonical entity types."""
    return SUPPORTED_ENTITIES


@mcp.tool()
def get_internal_schema(entity: str) -> dict:
    """
    Return the internal canonical schema for an entity type.
    This is the target model that all marketplace connectors must produce.
    Entity types: Order, Product, Inventory
    """
    e = _require_entity(entity)
    return _load(INTERNAL_MODELS_DIR / f"{_entity_filename(e)}.json")


@mcp.tool()
def get_marketplace_schema(marketplace: str, entity: str) -> dict:
    """
    Return the raw marketplace API schema for an entity type, with exact field names
    as returned by the marketplace API, types, and notes on quirks.
    """
    m = _require_marketplace(marketplace)
    e = _require_entity(entity)
    return _load(MARKETPLACE_SCHEMAS_DIR / m / f"{_entity_filename(e)}.json")


@mcp.tool()
def get_saved_mapping(marketplace: str, entity: str) -> dict:
    """
    Return the pre-validated field mapping for a marketplace/entity pair.
    These are team-approved mappings ready for use in transformer generation.
    """
    m = _require_marketplace(marketplace)
    e = _require_entity(entity)
    path = SAVED_MAPPINGS_DIR / m / f"{_entity_filename(e)}.json"
    if not path.exists():
        return {
            "found": False,
            "marketplace": m,
            "entity": e,
            "note": "No saved mapping yet. Use suggest_field_mapping + validate_mapping to build one.",
        }
    data = _load(path)
    data["found"] = True
    return data


@mcp.tool()
def list_saved_mappings() -> list[dict]:
    """List all available pre-validated mappings with their marketplace, entity, and validation status."""
    results = []
    for path in sorted(SAVED_MAPPINGS_DIR.rglob("*.json")):
        try:
            data = _load(path)
            results.append({
                "marketplace": data.get("marketplace"),
                "entity": data.get("entity"),
                "version": data.get("version"),
                "validated": data.get("validated", False),
            })
        except Exception:
            pass
    return results


@mcp.tool()
def suggest_field_mapping(
    marketplace: str,
    entity: str,
    source_field: str,
    top_k: int = 3,
) -> list[dict]:
    """
    Suggest the top candidate internal fields for a given marketplace source field.

    Uses token-based name similarity with semantic synonym expansion.
    Returns a ranked list of {internal_field, type, required, confidence, rationale}.

    Use this for marketplace fields that have no obvious direct match in the internal schema.
    """
    e = _require_entity(entity)
    internal_schema = _load(INTERNAL_MODELS_DIR / f"{_entity_filename(e)}.json")
    source_tokens = _expand(_tokenize(source_field))

    results = []
    for field in internal_schema["fields"]:
        int_field = field["name"]
        score = _similarity(source_field, int_field)
        if score > 0:
            int_tokens = _expand(_tokenize(int_field))
            shared = sorted(source_tokens & int_tokens)
            results.append({
                "internal_field": int_field,
                "type": field["type"],
                "required": field.get("required", False),
                "confidence": round(score, 3),
                "rationale": f"Shared tokens: {shared}" if shared else "Low overlap",
                "description": field.get("description", ""),
            })

    results.sort(key=lambda x: x["confidence"], reverse=True)
    return results[:top_k]


@mcp.tool()
def validate_mapping(
    marketplace: str,
    entity: str,
    mapping: list[dict],
) -> dict:
    """
    Validate a proposed field mapping against the internal canonical schema.

    Checks:
    - All required internal fields are covered
    - No duplicate internal field assignments
    - All transformation types are valid
    - Type compatibility (e.g. decimal fields should not use 'direct' on string sources)

    Returns {valid, missing_required, duplicate_fields, invalid_transformations, type_warnings, summary}
    """
    e = _require_entity(entity)
    internal_schema = _load(INTERNAL_MODELS_DIR / f"{_entity_filename(e)}.json")

    required_fields = {f["name"] for f in internal_schema["fields"] if f.get("required")}
    field_types = {f["name"]: f["type"] for f in internal_schema["fields"]}

    mapped = [m["internal_field"] for m in mapping]
    mapped_set = set(mapped)

    missing_required = sorted(required_fields - mapped_set)

    seen: set[str] = set()
    duplicates = [f for f in mapped if f in seen or seen.add(f)]  # type: ignore[func-returns-value]

    invalid_transforms = [
        m["internal_field"]
        for m in mapping
        if m.get("transformation", "direct") not in VALID_TRANSFORMATIONS
    ]

    type_warnings = []
    for m in mapping:
        f = m.get("internal_field", "")
        transform = m.get("transformation", "direct")
        expected = field_types.get(f, "")
        if expected == "decimal" and transform == "direct":
            type_warnings.append(
                f"'{f}': type is decimal but transformation is 'direct'. "
                "Use str_to_decimal or int_to_decimal depending on the source format."
            )
        if expected == "datetime" and transform == "direct":
            type_warnings.append(
                f"'{f}': type is datetime but transformation is 'direct'. "
                "Use datetime_iso (ISO 8601) or datetime_ymd_hms (Rakuten format)."
            )

    valid = not missing_required and not duplicates and not invalid_transforms

    return {
        "valid": valid,
        "missing_required": missing_required,
        "duplicate_fields": duplicates,
        "invalid_transformations": invalid_transforms,
        "type_warnings": type_warnings,
        "summary": (
            "Mapping is valid and ready for code generation."
            if valid else
            f"{len(missing_required)} required field(s) missing: {missing_required}"
        ),
    }


@mcp.tool()
def generate_transformer(
    marketplace: str,
    entity: str,
    mapping: list[dict] | None = None,
    language: Literal["python"] = "python",
) -> str:
    """
    Generate a transformation function from a marketplace payload to the internal model.

    If mapping is None, uses the pre-validated saved mapping for marketplace/entity.
    Currently only Python is supported.

    The generated function takes a single `raw: dict` argument (the merged marketplace
    API response) and returns an internal model dict.
    """
    m = _require_marketplace(marketplace)
    e = _require_entity(entity)

    if mapping is None:
        saved = get_saved_mapping(marketplace, entity)
        if not saved.get("found"):
            raise ValueError(
                f"No saved mapping for {marketplace}/{entity}. "
                "Provide a mapping explicitly or build one with suggest_field_mapping + validate_mapping."
            )
        mapping = saved["mappings"]

    if language != "python":
        raise ValueError(f"Language '{language}' not supported yet. Only 'python' is available.")

    return _generate_python(m, e, mapping)


# ---------------------------------------------------------------------------
# Resources
# ---------------------------------------------------------------------------

@mcp.resource("internal://data-model/{entity}")
def internal_model_resource(entity: str) -> str:
    """Internal canonical schema for an entity type (Order, Product, Inventory)."""
    e = _require_entity(entity)
    return (INTERNAL_MODELS_DIR / f"{_entity_filename(e)}.json").read_text()


@mcp.resource("internal://mapping/{marketplace}/{entity}")
def saved_mapping_resource(marketplace: str, entity: str) -> str:
    """Pre-validated field mapping for a marketplace/entity pair."""
    m = _require_marketplace(marketplace)
    e = _require_entity(entity)
    path = SAVED_MAPPINGS_DIR / m / f"{_entity_filename(e)}.json"
    if not path.exists():
        return json.dumps({"found": False, "note": f"No saved mapping for {marketplace}/{entity}"})
    return path.read_text()


# ---------------------------------------------------------------------------
# Prompts
# ---------------------------------------------------------------------------

@mcp.prompt()
def map_entity(marketplace: str, entity: str) -> str:
    return f"""
You are building the field mapping for **{marketplace} {entity}**.

Follow this exact sequence:

1. Call `get_marketplace_schema("{marketplace}", "{entity}")` — get all source fields with types and notes.
2. Call `get_internal_schema("{entity}")` — get the target canonical schema with required fields.
3. Map each marketplace field to an internal field:
   - Obvious matches (same name or clear synonym): map directly.
   - Ambiguous fields: call `suggest_field_mapping("{marketplace}", "{entity}", "<field>")`.
   - Fields with no internal equivalent: add to `raw` or flag for schema review.
4. Assign the correct `transformation` type for each mapping:
   - Amounts: `str_to_decimal` (Amazon/eBay) or `int_to_decimal` (Rakuten)
   - Datetimes: `datetime_iso` (ISO 8601) or `datetime_ymd_hms` (Rakuten "YYYY-MM-DD HH:MM:SS")
   - Status values: `enum_map` with the status mapping dict
   - Nested objects (e.g., shipping_address): `nested_object` with `nested_mapping`
   - Arrays of items: `list_transform` with `item_mapping`
5. Call `validate_mapping("{marketplace}", "{entity}", <mapping>)` — fix all issues.
6. Once valid, call `generate_transformer("{marketplace}", "{entity}", <mapping>)` — get the Python code.
"""


@mcp.prompt()
def review_mapping(marketplace: str, entity: str) -> str:
    return f"""
Review and update the saved mapping for **{marketplace} {entity}**.

1. Call `get_saved_mapping("{marketplace}", "{entity}")` to load the current mapping.
2. Call `get_marketplace_schema("{marketplace}", "{entity}")` to check for new or changed fields.
3. Call `get_internal_schema("{entity}")` to check for schema additions since the mapping was created.
4. Re-run `validate_mapping("{marketplace}", "{entity}", <current_mappings>)`.
5. Identify:
   - New required internal fields without a source mapping.
   - Marketplace fields added since the last mapping version.
   - Fields using `direct` transformation that should use typed transforms.
   - Deprecated marketplace fields still in the mapping.
6. Output the diff of proposed changes with reasoning.
"""


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

def main():
    mcp.run()


if __name__ == "__main__":
    main()
