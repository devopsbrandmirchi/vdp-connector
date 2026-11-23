"""
VDP MCP connector — plain-language access to Smart Analytics V2 (Supabase).

Users ask in natural language ("VDP views for Moix RV last week").
They do NOT need table names. Tools resolve dealer names → client IDs
and query the right tables behind the scenes:

  smart_hoot_config       → dealer list / ga4_customer_id ↔ client_id
  smart_final_data        → VDP KPIs, daily trend, make/model/location breakdowns
  smart_ga4_page_data     → all page views, channels, page titles
  smart_dealer_locations  → location filter options
  smart_ga4_config        → GA4 sync status
  smart_vdp_logic         → VDP / SRP URL filtration patterns
  smart_hoot_inventory_live → live Hoot feed (SmartAnalytics Inventory)
  smart_hoot_inventory    → Hoot warehouse inventory
  smart_scrap_inventory   → scrap inventory
  smart_vdp_logic         → URL rules (read + update)
  smart_dealer_locations  → rooftops (read + CRUD)
  smart_user_* / roles    → access (read + CRUD)

Auth: SUPABASE_URL + SUPABASE_SERVICE_ROLE_KEY (RLS is on for most tables).

Run:
  python vdp_mcp_server.py           # stdio (Claude Desktop / Cursor)
  python vdp_mcp_server.py --http    # http://127.0.0.1:8001/mcp
  Cloud Run: MCP_TRANSPORT=http + PORT (8080) + HOST=0.0.0.0
  Secrets via env: SUPABASE_URL, SUPABASE_SERVICE_ROLE_KEY (Secret Manager)
"""

from __future__ import annotations

import os
import sys
from collections import defaultdict
from datetime import date, datetime, timedelta
from typing import Any, Optional

try:
    from dotenv import load_dotenv
except ImportError:  # optional
    def load_dotenv(*_a, **_k):  # type: ignore
        return False

try:
    from mcp.server.fastmcp import FastMCP
except ImportError:
    from mcp.server import MCPServer as FastMCP

from supabase import Client, create_client

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------

_SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
load_dotenv(os.path.join(_SCRIPT_DIR, ".env"))

DEFAULT_URL = "https://rllwmeqingvuohyctddg.supabase.co"
SUPABASE_URL = (
    os.environ.get("SUPABASE_URL", "").strip()
    or os.environ.get("VDP_SUPABASE_URL", "").strip()
    or DEFAULT_URL
)
SUPABASE_KEY = (
    os.environ.get("SUPABASE_SERVICE_ROLE_KEY", "").strip()
    or os.environ.get("SUPABASE_KEY", "").strip()
    or os.environ.get("VDP_SUPABASE_KEY", "").strip()
)

sb: Optional[Client] = None

# Claude.ai custom connectors need OAuth DCR. Enable when MCP_PUBLIC_URL is set
# (Cloud Run deploy script sets this to https://SERVICE.run.app).
def _resolve_public_url() -> str:
    for key in ("MCP_PUBLIC_URL", "BASE_URL", "SERVICE_URL"):
        raw = os.environ.get(key, "").strip().rstrip("/")
        if raw:
            return raw
    # Cloud Run from Git often omits MCP_PUBLIC_URL — build the known URL shape.
    service = os.environ.get("K_SERVICE", "").strip()
    if not service:
        return ""
    region = (
        os.environ.get("CLOUD_RUN_REGION", "").strip()
        or os.environ.get("GOOGLE_CLOUD_REGION", "").strip()
    )
    project_number = (
        os.environ.get("CLOUD_RUN_PROJECT_NUMBER", "").strip()
        or os.environ.get("GCP_PROJECT_NUMBER", "").strip()
        or os.environ.get("GOOGLE_CLOUD_PROJECT_NUMBER", "").strip()
    )
    if service and region and project_number:
        return f"https://{service}-{project_number}.{region}.run.app"
    # Known production service (Git → europe-west1) until env is set explicitly.
    if service == "vdp-connector-git":
        return "https://vdp-connector-git-573223329822.europe-west1.run.app"
    return ""


MCP_PUBLIC_URL = _resolve_public_url()
MCP_OAUTH_PASSWORD = os.environ.get("MCP_OAUTH_PASSWORD", "").strip() or None


def _build_mcp():
    if not MCP_PUBLIC_URL:
        return FastMCP("vdp")

    from pydantic import AnyHttpUrl

    from mcp.server.auth.settings import AuthSettings, ClientRegistrationOptions

    from vdp_oauth import ClaudeOAuthProvider

    provider = ClaudeOAuthProvider(
        base_url=MCP_PUBLIC_URL,
        password=MCP_OAUTH_PASSWORD,
    )
    auth = AuthSettings(
        issuer_url=AnyHttpUrl(MCP_PUBLIC_URL),
        resource_server_url=AnyHttpUrl(MCP_PUBLIC_URL),
        client_registration_options=ClientRegistrationOptions(
            enabled=True,
            valid_scopes=["vdp"],
            default_scopes=["vdp"],
        ),
        required_scopes=["vdp"],
    )
    return FastMCP(
        "vdp",
        auth_server_provider=provider,
        auth=auth,
    )


mcp = _build_mcp()


def _client() -> Client:
    global sb
    if sb is not None:
        return sb
    if not SUPABASE_KEY:
        raise RuntimeError(
            "Missing Supabase service-role key.\n"
            "Local: set SUPABASE_SERVICE_ROLE_KEY in .env\n"
            "Cloud Run: inject via Secret Manager (--set-secrets).\n"
            "Dashboard: https://supabase.com/dashboard/project/rllwmeqingvuohyctddg/settings/api\n"
            "Anon/publishable keys are not enough — most VDP tables use RLS."
        )
    sb = create_client(SUPABASE_URL, SUPABASE_KEY)
    return sb


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _parse_date(value: str, *, default: Optional[date] = None) -> date:
    raw = (value or "").strip()
    if not raw:
        if default is None:
            raise ValueError("date required (YYYY-MM-DD)")
        return default
    lowered = raw.lower()
    today = date.today()
    if lowered in ("today",):
        return today
    if lowered in ("yesterday",):
        return today - timedelta(days=1)
    return datetime.strptime(raw[:10], "%Y-%m-%d").date()


def _fmt_int(n: Any) -> str:
    try:
        return f"{int(n):,}"
    except (TypeError, ValueError):
        return "0"


def _sum_field(rows: list[dict], field: str) -> int:
    total = 0
    for r in rows:
        v = r.get(field)
        if v is None:
            continue
        try:
            total += int(v)
        except (TypeError, ValueError):
            pass
    return total


def _table_lines(headers: list[str], rows: list[list[Any]]) -> str:
    if not rows:
        return "No rows matched."
    lines = [" | ".join(headers)]
    lines.append(" | ".join("---" for _ in headers))
    for row in rows:
        lines.append(" | ".join(str(c) for c in row))
    return "\n".join(lines)


def _prefer_dealer_with_data(candidates: list[dict]) -> dict:
    """When names collide (e.g. Moix Rv vs Moix RV), pick the one with recent VDP rows."""
    best = candidates[0]
    best_hits = -1
    for row in candidates:
        cid = row.get("ga4_customer_id")
        if not cid:
            continue
        try:
            sample = (
                _client()
                .table("smart_final_data")
                .select("id")
                .eq("client_id", cid)
                .limit(1)
                .execute()
                .data
                or []
            )
            hits = 1 if sample else 0
        except Exception:
            hits = 0
        if hits > best_hits:
            best_hits = hits
            best = row
    return best


def _resolve_dealer(dealer: str) -> dict[str, Any]:
    """
    Accept dealer display name OR numeric client_id / ga4_customer_id.
    Returns {client_id, customer_name, hoot_id, website_platform, dealer_category, ...}
    """
    q = (dealer or "").strip()
    if not q:
        raise ValueError("dealer is required (name or client id)")

    # Exact client id
    if q.isdigit():
        by_id = (
            _client().table("smart_hoot_config")
            .select(
                "id, customer_name, ga4_customer_id, hoot_id, website_platform, "
                "dealer_category, is_active, hoot_url"
            )
            .eq("ga4_customer_id", q)
            .limit(5)
            .execute()
            .data
            or []
        )
        if by_id:
            row = by_id[0]
            return {
                "client_id": row.get("ga4_customer_id") or q,
                "customer_name": row.get("customer_name") or q,
                "hoot_id": row.get("hoot_id"),
                "website_platform": row.get("website_platform"),
                "dealer_category": row.get("dealer_category"),
                "is_active": row.get("is_active"),
                "hoot_url": row.get("hoot_url"),
                "config_id": row.get("id"),
            }
        # Still allow raw client_id even if not in hoot_config
        return {
            "client_id": q,
            "customer_name": q,
            "hoot_id": None,
            "website_platform": None,
            "dealer_category": None,
            "is_active": None,
            "hoot_url": None,
            "config_id": None,
        }

    # Fuzzy name match
    rows = (
        _client().table("smart_hoot_config")
        .select(
            "id, customer_name, ga4_customer_id, hoot_id, website_platform, "
            "dealer_category, is_active, hoot_url"
        )
        .ilike("customer_name", f"%{q}%")
        .order("customer_name")
        .limit(10)
        .execute()
        .data
        or []
    )
    if not rows:
        raise ValueError(
            f"No dealer matched '{q}'. Try list_dealers(search='{q}') first."
        )

    # Prefer: exact case → case-insensitive exact (with data if duplicates) → first fuzzy
    exact_case = [r for r in rows if (r.get("customer_name") or "") == q]
    exact_ci = [
        r for r in rows if (r.get("customer_name") or "").lower() == q.lower()
    ]
    if exact_case:
        chosen = exact_case[0]
    elif len(exact_ci) == 1:
        chosen = exact_ci[0]
    elif len(exact_ci) > 1:
        chosen = _prefer_dealer_with_data(exact_ci)
    else:
        chosen = rows[0]

    also = None
    if len(rows) > 1:
        also = ", ".join(
            f"{r.get('customer_name')} ({r.get('ga4_customer_id')})" for r in rows[:5]
        )

    result = {
        "client_id": chosen.get("ga4_customer_id"),
        "customer_name": chosen.get("customer_name"),
        "hoot_id": chosen.get("hoot_id"),
        "website_platform": chosen.get("website_platform"),
        "dealer_category": chosen.get("dealer_category"),
        "is_active": chosen.get("is_active"),
        "hoot_url": chosen.get("hoot_url"),
        "config_id": chosen.get("id"),
    }
    if also:
        result["also_matched"] = also
    return result


def _fetch_paged(
    table: str,
    columns: str,
    *,
    filters: list[tuple[str, str, Any]],
    order: Optional[str] = None,
    page_size: int = 1000,
    max_rows: int = 20000,
) -> list[dict]:
    """Paginate PostgREST results for one dealer / date window."""
    out: list[dict] = []
    start = 0
    while start < max_rows:
        end = min(start + page_size - 1, max_rows - 1)
        q = _client().table(table).select(columns)
        for op, col, val in filters:
            if op == "eq":
                q = q.eq(col, val)
            elif op == "gte":
                q = q.gte(col, val)
            elif op == "lte":
                q = q.lte(col, val)
            elif op == "ilike":
                q = q.ilike(col, val)
            elif op == "is":
                # PostgREST expects 'true'/'false'/'null' strings for IS
                if isinstance(val, bool):
                    q = q.eq(col, val)
                else:
                    q = q.is_(col, val)
        if order:
            q = q.order(order)
        batch = q.range(start, end).execute().data or []
        out.extend(batch)
        if len(batch) < page_size:
            break
        start += page_size
    return out


def _try_aggregate_sum(
    table: str,
    *,
    metrics: list[str],
    filters: list[tuple[str, str, Any]],
    group_by: Optional[str] = None,
) -> Optional[list[dict]]:
    """
    Prefer PostgREST aggregates (views.sum()). Fall back to None on failure
    so callers can paginate + sum in Python.
    """
    if group_by:
        select = group_by + ", " + ", ".join(f"{m}.sum()" for m in metrics)
    else:
        select = ", ".join(f"{m}.sum()" for m in metrics)
    try:
        q = _client().table(table).select(select)
        for op, col, val in filters:
            if op == "eq":
                q = q.eq(col, val)
            elif op == "gte":
                q = q.gte(col, val)
            elif op == "lte":
                q = q.lte(col, val)
            elif op == "is":
                if isinstance(val, bool):
                    q = q.eq(col, val)
                else:
                    q = q.is_(col, val)
        data = q.execute().data
        return data or []
    except Exception:
        return None


def _csv_list(value: str) -> Optional[list[str]]:
    """Comma-separated filter list → list, or None when empty / BOTH / all."""
    raw = (value or "").strip()
    if not raw or raw.lower() in ("all", "both", "*"):
        return None
    parts = [p.strip() for p in raw.split(",") if p.strip()]
    return parts or None


def _csv_int_list(value: str) -> Optional[list[int]]:
    parts = _csv_list(value)
    if not parts:
        return None
    out: list[int] = []
    for p in parts:
        try:
            out.append(int(str(p)[:4]))
        except ValueError:
            continue
    return out or None


def _rpc(name: str, params: dict[str, Any]) -> Any:
    """Call a Smart Analytics Supabase RPC (same functions the web app uses)."""
    return _client().rpc(name, params).execute().data


def _vdp_rpc_filters(
    *,
    make: str = "",
    model: str = "",
    location: str = "",
    year: str = "",
    type_: str = "",
    condition: str = "BOTH",
    channel: str = "",
) -> dict[str, Any]:
    cond = (condition or "BOTH").strip().upper() or "BOTH"
    if cond in ("ALL", "USED+NEW", "NEW+USED", ""):
        cond = "BOTH"
    return {
        "p_types": _csv_list(type_),
        "p_makes": _csv_list(make),
        "p_models": _csv_list(model),
        "p_locations": _csv_list(location),
        "p_years": _csv_int_list(year),
        "p_condition": cond,
        "p_channels": _csv_list(channel),
    }


def _inventory_dealer_or_filter(name: str) -> str:
    """PostgREST or_ clause matching SmartAnalytics dealer inventory naming."""
    safe = name.replace(",", " ").replace("%", "").strip()
    return f"customer_name.ilike.%{safe}%,advertiser.ilike.%{safe}%"


def _count_inventory(table: str, *, dealer_name: str = "", **filters: str) -> int:
    q = _client().table(table).select("sk", count="exact")
    if dealer_name:
        q = q.or_(_inventory_dealer_or_filter(dealer_name))
    for col, val in filters.items():
        if val and val.strip():
            q = q.ilike(col, f"%{val.strip()}%")
    res = q.limit(1).execute()
    return int(res.count or 0)


# ---------------------------------------------------------------------------
# Tools — plain-language descriptions so the model never needs table names
# ---------------------------------------------------------------------------


@mcp.tool()
def help_vdp() -> str:
    """Explain what this VDP connector can answer in simple language.

    Call this first if the user asks what they can do, or how to phrase questions.
    No table names required from the user.
    """
    return """You can ask things like:

DEALERS
- "List my dealers" / "Find Beaver Coach"
- "What locations does Moix RV have?"
- "Add location Boise to Moix RV" / "Remove location …"

VDP PERFORMANCE (same numbers as SmartAnalytics VDP report)
- "VDP views for Moix RV last 7 days"
- "Daily VDP trend for Zoomers RV from 2026-08-01 to 2026-08-17"
- "Break down VDP by make / model / year / location / condition for …"
- Optional filters: make, model, location, year, type, condition, channel

TRAFFIC & CHANNELS
- "Which channels drove VDP traffic for A&L RV?"
- "Top VDP pages for Gerzeny’s RV last week"

INVENTORY (same Hoot feed / daily snapshot as SmartAnalytics Inventory report)
- "Inventory report for Moix RV"  ← preferred full report (units + $ + mix)
- "Inventory summary by make for Beaver Coach"
- "Search inventory for Forest River at Moix" (paginated — use offset)
- "Find VIN … in live Hoot inventory"
- source: live (default, matches SmartAnalytics) | hoot | scrap | both

SETUP / RULES / ACCESS (CRUD where safe)
- "Show / update VDP URL rules for Beaver Coach"
- "Is GA4 sync healthy for Moix RV?"
- "Who has access?" / "Grant user access" / "Remove user access"

Use dealer *names* — you do not need client IDs or table names.
Dates are YYYY-MM-DD (or yesterday / today).
Inventory writes are NOT available (feeds overwrite them); use SmartAnalytics sync jobs."""


@mcp.tool()
def list_dealers(
    search: str = "",
    category: str = "",
    active_only: bool = True,
    limit: int = 50,
) -> str:
    """List dealerships the user can analyze.

    Use when the user says: list dealers, find a dealer, search dealers by name,
    or filter by category (RV, Marine, Powersports, Boats, etc.).

    search: optional name fragment (e.g. "Moix", "Beaver").
    category: optional dealer_category filter.
    active_only: default True — only active dealers.
    """
    q = _client().table("smart_hoot_config").select(
        "customer_name, ga4_customer_id, dealer_category, website_platform, is_active"
    )
    if active_only:
        q = q.eq("is_active", True)
    if search.strip():
        q = q.ilike("customer_name", f"%{search.strip()}%")
    if category.strip():
        q = q.ilike("dealer_category", f"%{category.strip()}%")
    rows = q.order("customer_name").limit(max(1, min(limit, 200))).execute().data or []
    if not rows:
        return "No dealers matched."
    body = [
        [
            r.get("customer_name") or "",
            r.get("ga4_customer_id") or "",
            r.get("dealer_category") or "",
            r.get("website_platform") or "",
            "active" if r.get("is_active") else "inactive",
        ]
        for r in rows
    ]
    return (
        f"Found {len(rows)} dealer(s).\n"
        + _table_lines(
            ["Dealer", "Client ID", "Category", "Platform", "Status"], body
        )
    )


@mcp.tool()
def list_locations(dealer: str) -> str:
    """List location / rooftop options for a dealer.

    Use when the user asks for dealer locations, rooftops, or location filter values.
    dealer: dealer name or client id.
    """
    info = _resolve_dealer(dealer)
    cid = info["client_id"]
    rows = (
        _client().table("smart_dealer_locations")
        .select("location_name, customer_id")
        .eq("customer_id", cid)
        .order("location_name")
        .execute()
        .data
        or []
    )
    header = f"Locations for {info['customer_name']} (client {cid})"
    if info.get("also_matched"):
        header += f"\nNote: also matched {info['also_matched']}"
    if not rows:
        return header + "\nNo rows in dealer locations. Falling back to VDP inventory locations…"
    return header + "\n" + _table_lines(
        ["Location", "Client ID"],
        [[r.get("location_name"), r.get("customer_id")] for r in rows],
    )


@mcp.tool()
def get_vdp_summary(
    dealer: str,
    start_date: str,
    end_date: str = "",
    make: str = "",
    model: str = "",
    location: str = "",
    year: str = "",
    type_: str = "",
    condition: str = "BOTH",
    channel: str = "",
) -> str:
    """Get VDP KPI totals for a dealer over a date range (SmartAnalytics parity).

    Uses the same get_vdp_views_total RPC as the SmartAnalytics VDP report when possible.
    Optional filters (comma-separated): make, model, location, year, type_, condition, channel.
    condition: BOTH | NEW | USED.
    """
    info = _resolve_dealer(dealer)
    cid = info["client_id"]
    start = _parse_date(start_date)
    end = _parse_date(end_date, default=start)
    if end < start:
        start, end = end, start

    inv = _vdp_rpc_filters(
        make=make,
        model=model,
        location=location,
        year=year,
        type_=type_,
        condition=condition,
        channel=channel,
    )
    views: Optional[int] = None
    source = "smart_final_data"
    try:
        total = _rpc(
            "get_vdp_views_total",
            {
                "p_client_id": str(cid),
                "p_from": start.isoformat(),
                "p_to": end.isoformat(),
                **inv,
            },
        )
        if total is not None:
            views = int(total if not isinstance(total, dict) else total.get("total") or total.get("get_vdp_views_total") or 0)
            source = "get_vdp_views_total (SmartAnalytics)"
    except Exception:
        views = None

    # Sessions / users still from smart_final_data (same table SmartAnalytics Step 3 writes)
    filters: list[tuple[str, str, Any]] = [
        ("eq", "client_id", cid),
        ("gte", "report_date", start.isoformat()),
        ("lte", "report_date", end.isoformat()),
    ]
    if inv["p_makes"]:
        filters.append(("ilike", "inv_make", f"%{inv['p_makes'][0]}%"))
    if inv["p_models"]:
        filters.append(("ilike", "inv_model", f"%{inv['p_models'][0]}%"))
    if inv["p_locations"]:
        filters.append(("ilike", "inv_location", f"%{inv['p_locations'][0]}%"))

    agg = _try_aggregate_sum(
        "smart_final_data",
        metrics=["views", "sessions", "total_users", "new_users"],
        filters=filters,
    )
    sessions = users = new_users = 0
    fallback_views = 0
    if agg is not None and agg:
        row = agg[0]

        def pick(r: dict, name: str) -> int:
            if name in r and not isinstance(r[name], dict):
                return int(r[name] or 0)
            for k, v in r.items():
                if name in k.lower():
                    if isinstance(v, dict):
                        return int(v.get("sum") or 0)
                    return int(v or 0)
            return 0

        fallback_views = pick(row, "views")
        sessions = pick(row, "sessions")
        users = pick(row, "total_users")
        new_users = pick(row, "new_users")
    else:
        rows = _fetch_paged(
            "smart_final_data",
            "views, sessions, total_users, new_users",
            filters=filters,
        )
        fallback_views = _sum_field(rows, "views")
        sessions = _sum_field(rows, "sessions")
        users = _sum_field(rows, "total_users")
        new_users = _sum_field(rows, "new_users")

    if views is None:
        views = fallback_views
        source = "smart_final_data"

    filter_note = []
    for label, val in (
        ("make", make),
        ("model", model),
        ("location", location),
        ("year", year),
        ("type", type_),
        ("condition", condition if condition and condition.upper() != "BOTH" else ""),
        ("channel", channel),
    ):
        if val and str(val).strip():
            filter_note.append(f"{label}={val.strip()}")

    lines = [
        f"VDP summary — {info['customer_name']} ({cid})",
        f"Dates: {start.isoformat()} → {end.isoformat()}",
        f"Source: {source}",
        f"Views: {_fmt_int(views)}",
        f"Sessions: {_fmt_int(sessions)}",
        f"Users: {_fmt_int(users)}",
        f"New users: {_fmt_int(new_users)}",
    ]
    if filter_note:
        lines.append("Filters: " + ", ".join(filter_note))
    if info.get("also_matched"):
        lines.append(f"Name match note: {info['also_matched']}")
    return "\n".join(lines)


@mcp.tool()
def get_daily_trend(
    dealer: str,
    start_date: str,
    end_date: str = "",
    make: str = "",
    model: str = "",
    location: str = "",
    year: str = "",
    type_: str = "",
    condition: str = "BOTH",
    channel: str = "",
) -> str:
    """Daily VDP views for charting (SmartAnalytics get_vdp_views_by_date when possible).

    Optional inventory filters match the SmartAnalytics VDP report.
    """
    info = _resolve_dealer(dealer)
    cid = info["client_id"]
    start = _parse_date(start_date)
    end = _parse_date(end_date, default=start)
    if end < start:
        start, end = end, start

    inv = _vdp_rpc_filters(
        make=make,
        model=model,
        location=location,
        year=year,
        type_=type_,
        condition=condition,
        channel=channel,
    )
    daily: dict[str, dict[str, int]] = defaultdict(
        lambda: {"views": 0, "sessions": 0, "users": 0}
    )
    used_rpc = False
    try:
        rows = _rpc(
            "get_vdp_views_by_date",
            {
                "p_client_id": str(cid),
                "p_from": start.isoformat(),
                "p_to": end.isoformat(),
                **inv,
            },
        ) or []
        for r in rows:
            d = str(r.get("report_date") or "")[:10]
            if not d:
                continue
            daily[d]["views"] += int(r.get("views") or 0)
        used_rpc = True
    except Exception:
        used_rpc = False

    filters: list[tuple[str, str, Any]] = [
        ("eq", "client_id", cid),
        ("gte", "report_date", start.isoformat()),
        ("lte", "report_date", end.isoformat()),
    ]
    # Always fill sessions/users from smart_final_data; fill views if RPC failed
    if not used_rpc:
        agg = _try_aggregate_sum(
            "smart_final_data",
            metrics=["views", "sessions", "total_users"],
            filters=filters,
            group_by="report_date",
        )
        if agg is not None:
            for r in agg:
                d = str(r.get("report_date") or "")[:10]
                if not d:
                    continue

                def pick(name: str) -> int:
                    v = r.get(name)
                    if isinstance(v, dict):
                        return int(v.get("sum") or 0)
                    if v is not None:
                        return int(v or 0)
                    for k, val in r.items():
                        if name in k.lower():
                            if isinstance(val, dict):
                                return int(val.get("sum") or 0)
                            return int(val or 0)
                    return 0

                daily[d]["views"] += pick("views")
                daily[d]["sessions"] += pick("sessions")
                daily[d]["users"] += pick("total_users")
        else:
            for r in _fetch_paged(
                "smart_final_data",
                "report_date, views, sessions, total_users",
                filters=filters,
                order="report_date",
            ):
                d = str(r.get("report_date") or "")[:10]
                daily[d]["views"] += int(r.get("views") or 0)
                daily[d]["sessions"] += int(r.get("sessions") or 0)
                daily[d]["users"] += int(r.get("total_users") or 0)
    else:
        # Sessions/users companion metrics from final data (unfiltered grain)
        for r in _fetch_paged(
            "smart_final_data",
            "report_date, sessions, total_users",
            filters=filters,
            order="report_date",
            max_rows=5000,
        ):
            d = str(r.get("report_date") or "")[:10]
            daily[d]["sessions"] += int(r.get("sessions") or 0)
            daily[d]["users"] += int(r.get("total_users") or 0)

    if not daily:
        return f"No VDP daily data for {info['customer_name']} in that range."

    table = [
        [d, _fmt_int(v["views"]), _fmt_int(v["sessions"]), _fmt_int(v["users"])]
        for d, v in sorted(daily.items())
    ]
    src = "get_vdp_views_by_date" if used_rpc else "smart_final_data"
    return (
        f"Daily VDP trend — {info['customer_name']} ({cid}) [{src}]\n"
        f"{start.isoformat()} → {end.isoformat()}\n"
        + _table_lines(["Date", "Views", "Sessions", "Users"], table)
    )


@mcp.tool()
def get_vdp_breakdown(
    dealer: str,
    start_date: str,
    end_date: str = "",
    by: str = "make",
    limit: int = 25,
) -> str:
    """Break down VDP performance by vehicle or location attributes.

    by: one of location, make, model, year, type, condition, custom_type.
    Use for questions like "top makes", "VDP by location", "used vs new".
    """
    dim_map = {
        "location": "inv_location",
        "make": "inv_make",
        "model": "inv_model",
        "year": "inv_year",
        "type": "inv_type",
        "condition": "inv_condition",
        "custom_type": "inv_custom_type",
        "vehicle_condition": "vdp_vehicle_condition",
    }
    key = (by or "make").strip().lower()
    col = dim_map.get(key)
    if not col:
        return (
            "Unknown breakdown. Use by=location|make|model|year|type|condition|custom_type."
        )

    info = _resolve_dealer(dealer)
    cid = info["client_id"]
    start = _parse_date(start_date)
    end = _parse_date(end_date, default=start)
    if end < start:
        start, end = end, start

    filters: list[tuple[str, str, Any]] = [
        ("eq", "client_id", cid),
        ("gte", "report_date", start.isoformat()),
        ("lte", "report_date", end.isoformat()),
    ]

    buckets: dict[str, dict[str, int]] = defaultdict(
        lambda: {"views": 0, "sessions": 0, "users": 0}
    )
    agg = _try_aggregate_sum(
        "smart_final_data",
        metrics=["views", "sessions", "total_users"],
        filters=filters,
        group_by=col,
    )
    if agg is not None:
        for r in agg:
            label = r.get(col) or "(blank)"

            def pick(name: str) -> int:
                v = r.get(name)
                if isinstance(v, dict):
                    return int(v.get("sum") or 0)
                if v is not None and name in r:
                    return int(v or 0)
                for k, val in r.items():
                    if name in k.lower():
                        if isinstance(val, dict):
                            return int(val.get("sum") or 0)
                        return int(val or 0)
                return 0

            buckets[str(label)]["views"] += pick("views")
            buckets[str(label)]["sessions"] += pick("sessions")
            buckets[str(label)]["users"] += pick("total_users")
    else:
        rows = _fetch_paged(
            "smart_final_data",
            f"{col}, views, sessions, total_users",
            filters=filters,
        )
        for r in rows:
            label = r.get(col) or "(blank)"
            buckets[str(label)]["views"] += int(r.get("views") or 0)
            buckets[str(label)]["sessions"] += int(r.get("sessions") or 0)
            buckets[str(label)]["users"] += int(r.get("total_users") or 0)

    ranked = sorted(buckets.items(), key=lambda kv: kv[1]["views"], reverse=True)
    ranked = ranked[: max(1, min(limit, 100))]
    table = [
        [name, _fmt_int(v["views"]), _fmt_int(v["sessions"]), _fmt_int(v["users"])]
        for name, v in ranked
    ]
    return (
        f"VDP by {key} — {info['customer_name']} ({cid})\n"
        f"{start.isoformat()} → {end.isoformat()}\n"
        + _table_lines([key.title(), "Views", "Sessions", "Users"], table)
    )


@mcp.tool()
def get_channel_mix(
    dealer: str,
    start_date: str,
    end_date: str = "",
    vdp_only: bool = True,
    limit: int = 20,
) -> str:
    """Traffic by channel / source / medium (from GA4 page data).

    Use for: "which channels drove traffic", "paid vs organic", "source medium mix".
    vdp_only: True = Vehicle Detail Pages only; False = all pages.
    """
    info = _resolve_dealer(dealer)
    cid = info["client_id"]
    start = _parse_date(start_date)
    end = _parse_date(end_date, default=start)
    if end < start:
        start, end = end, start

    filters: list[tuple[str, str, Any]] = [
        ("eq", "client_id", cid),
        ("gte", "report_date", start.isoformat()),
        ("lte", "report_date", end.isoformat()),
    ]
    if vdp_only:
        filters.append(("eq", "vdp_conditions", True))

    rows = _fetch_paged(
        "smart_ga4_page_data",
        "channel, source, medium, source_medium, views, sessions, total_users",
        filters=filters,
        max_rows=30000,
    )
    buckets: dict[str, dict[str, int]] = defaultdict(
        lambda: {"views": 0, "sessions": 0, "users": 0}
    )
    for r in rows:
        ch = r.get("channel") or r.get("source_medium") or "(unknown)"
        buckets[str(ch)]["views"] += int(r.get("views") or 0)
        buckets[str(ch)]["sessions"] += int(r.get("sessions") or 0)
        buckets[str(ch)]["users"] += int(r.get("total_users") or 0)

    ranked = sorted(buckets.items(), key=lambda kv: kv[1]["views"], reverse=True)[
        : max(1, min(limit, 100))
    ]
    scope = "VDP pages" if vdp_only else "All pages"
    table = [
        [name, _fmt_int(v["views"]), _fmt_int(v["sessions"]), _fmt_int(v["users"])]
        for name, v in ranked
    ]
    return (
        f"Channel mix ({scope}) — {info['customer_name']} ({cid})\n"
        f"{start.isoformat()} → {end.isoformat()}\n"
        + _table_lines(["Channel", "Views", "Sessions", "Users"], table)
    )


@mcp.tool()
def get_top_pages(
    dealer: str,
    start_date: str,
    end_date: str = "",
    vdp_only: bool = True,
    limit: int = 20,
) -> str:
    """Top pages by views (path + title).

    Use for: top VDPs, most viewed URLs, page-title ranking.
    """
    info = _resolve_dealer(dealer)
    cid = info["client_id"]
    start = _parse_date(start_date)
    end = _parse_date(end_date, default=start)
    if end < start:
        start, end = end, start

    filters: list[tuple[str, str, Any]] = [
        ("eq", "client_id", cid),
        ("gte", "report_date", start.isoformat()),
        ("lte", "report_date", end.isoformat()),
    ]
    if vdp_only:
        filters.append(("eq", "vdp_conditions", True))

    rows = _fetch_paged(
        "smart_ga4_page_data",
        "page_path, page_title, views, sessions, total_users",
        filters=filters,
        max_rows=30000,
    )
    buckets: dict[tuple[str, str], dict[str, int]] = defaultdict(
        lambda: {"views": 0, "sessions": 0, "users": 0}
    )
    for r in rows:
        key = (r.get("page_path") or "", r.get("page_title") or "")
        buckets[key]["views"] += int(r.get("views") or 0)
        buckets[key]["sessions"] += int(r.get("sessions") or 0)
        buckets[key]["users"] += int(r.get("total_users") or 0)

    ranked = sorted(buckets.items(), key=lambda kv: kv[1]["views"], reverse=True)[
        : max(1, min(limit, 100))
    ]
    scope = "VDP" if vdp_only else "All pages"
    table = [
        [
            path[:80],
            (title or "")[:60],
            _fmt_int(v["views"]),
            _fmt_int(v["sessions"]),
        ]
        for (path, title), v in ranked
    ]
    return (
        f"Top {scope} pages — {info['customer_name']} ({cid})\n"
        f"{start.isoformat()} → {end.isoformat()}\n"
        + _table_lines(["Path", "Title", "Views", "Sessions"], table)
    )


@mcp.tool()
def search_inventory(
    dealer: str = "",
    make: str = "",
    model: str = "",
    vin: str = "",
    stock: str = "",
    condition: str = "",
    location: str = "",
    year: str = "",
    source: str = "live",
    offset: int = 0,
    limit: int = 100,
) -> str:
    """Search vehicle inventory with pagination (SmartAnalytics Hoot feed parity).

    Prefer source=live (smart_hoot_inventory_live — same live feed SmartAnalytics uses).
    source: live | hoot | scrap | both | all
    Use offset + limit to page through full stock (limit max 500). Response includes
    total_matched and next_offset so you do NOT need to stitch by make.
    """
    source = (source or "live").strip().lower()
    lim = max(1, min(int(limit or 100), 500))
    off = max(0, int(offset or 0))
    dealer_name = ""
    if dealer.strip():
        try:
            dealer_name = _resolve_dealer(dealer)["customer_name"]
        except Exception:
            dealer_name = dealer.strip()

    filter_kwargs = {
        "make": make,
        "model": model,
        "vin": vin,
        "stock_number": stock,
        "condition": condition,
        "location": location,
        "year": year,
    }

    def run(table: str, label: str) -> str:
        total = _count_inventory(table, dealer_name=dealer_name, **filter_kwargs)
        q = _client().table(table).select(
            "vin, make, model, year, trim, price, msrp, condition, location, "
            "stock_number, customer_name, advertiser, url, type_"
        )
        if dealer_name:
            q = q.or_(_inventory_dealer_or_filter(dealer_name))
        for col, val in filter_kwargs.items():
            if val and str(val).strip():
                q = q.ilike(col, f"%{str(val).strip()}%")
        rows = q.range(off, off + lim - 1).execute().data or []
        if not rows:
            return f"{label}: no matches (total_matched={total}, offset={off})."
        table_rows = [
            [
                r.get("year") or "",
                r.get("make") or "",
                r.get("model") or "",
                r.get("trim") or "",
                r.get("condition") or "",
                r.get("price") if r.get("price") is not None else (r.get("msrp") or ""),
                r.get("vin") or "",
                r.get("stock_number") or "",
                r.get("location") or "",
                r.get("customer_name") or r.get("advertiser") or "",
            ]
            for r in rows
        ]
        next_off = off + len(rows)
        more = next_off < total
        footer = (
            f"Showing {off + 1}–{off + len(rows)} of {total}. "
            + (f"Next page: offset={next_off}, limit={lim}" if more else "End of results.")
        )
        return (
            f"{label} — {len(rows)} row(s) this page | total_matched={total}\n"
            + _table_lines(
                [
                    "Year",
                    "Make",
                    "Model",
                    "Trim",
                    "Cond",
                    "Price",
                    "VIN",
                    "Stock",
                    "Location",
                    "Dealer",
                ],
                table_rows,
            )
            + "\n"
            + footer
        )

    blocks: list[str] = []
    if source in ("live", "both", "all"):
        blocks.append(run("smart_hoot_inventory_live", "Live Hoot inventory (SmartAnalytics feed)"))
    if source in ("hoot", "both", "all"):
        blocks.append(run("smart_hoot_inventory", "Hoot inventory (warehouse)"))
    if source in ("scrap", "both", "all"):
        blocks.append(run("smart_scrap_inventory", "Scrap inventory"))
    if not blocks:
        return "source must be live, hoot, scrap, both, or all."
    return "\n\n".join(blocks)


@mcp.tool()
def get_inventory_summary(
    dealer: str,
    by: str = "make",
    source: str = "live",
    limit: int = 50,
) -> str:
    """Inventory mix counts (units) like SmartAnalytics Inventory report sections.

    by: make | model | condition | location | type | year
    source: live (default) | hoot | scrap
    Prefer get_inventory_report for the full SmartAnalytics snapshot report.
    """
    info = _resolve_dealer(dealer)
    name = info["customer_name"]
    dim_map = {
        "make": "make",
        "model": "model",
        "condition": "condition",
        "location": "location",
        "type": "type_",
        "year": "year",
    }
    col = dim_map.get((by or "make").strip().lower())
    if not col:
        return "by must be make|model|condition|location|type|year"

    src = (source or "live").strip().lower()
    table = {
        "live": "smart_hoot_inventory_live",
        "hoot": "smart_hoot_inventory",
        "scrap": "smart_scrap_inventory",
    }.get(src)
    if not table:
        return "source must be live, hoot, or scrap"

    buckets: dict[str, dict[str, Any]] = defaultdict(lambda: {"units": 0, "value": 0.0})
    start = 0
    page = 1000
    while start < 50000:
        q = (
            _client()
            .table(table)
            .select(f"{col}, price, msrp")
            .or_(_inventory_dealer_or_filter(name))
        )
        batch = q.range(start, start + page - 1).execute().data or []
        for r in batch:
            label = str(r.get(col) or "(blank)")
            buckets[label]["units"] += 1
            price = r.get("price") or r.get("msrp") or 0
            try:
                buckets[label]["value"] += float(price or 0)
            except (TypeError, ValueError):
                pass
        if len(batch) < page:
            break
        start += page

    if not buckets:
        return f"No inventory for {name} in {table}."

    ranked = sorted(buckets.items(), key=lambda kv: kv[1]["units"], reverse=True)
    ranked = ranked[: max(1, min(limit, 200))]
    total_units = sum(v["units"] for _, v in buckets.items())
    table_rows = [
        [
            label,
            _fmt_int(v["units"]),
            f"{(v['units'] / total_units * 100):.1f}%" if total_units else "0%",
            f"${v['value']:,.0f}",
        ]
        for label, v in ranked
    ]
    return (
        f"Inventory by {by} — {name} ({info['client_id']}) [{table}]\n"
        f"Total units: {_fmt_int(total_units)}\n"
        + _table_lines(["Label", "Units", "Share", "Value"], table_rows)
    )


@mcp.tool()
def get_inventory_report(
    dealer: str = "",
    report_date: str = "",
    make: str = "",
    model: str = "",
    location: str = "",
    year: str = "",
    type_: str = "",
    condition: str = "BOTH",
    list_limit: int = 100,
) -> str:
    """Full SmartAnalytics Inventory report (same get_inventory_report RPC as the web UI).

    Reads the daily Hoot/scrap snapshot SmartAnalytics uses — units, $ value, and
    condition/location/make/type mix. dealer blank = all dealers.
    report_date: YYYY-MM-DD (default today / latest snapshot).
    """
    cid = None
    dealer_label = "All dealers"
    if dealer.strip():
        info = _resolve_dealer(dealer)
        cid = str(info["client_id"])
        dealer_label = f"{info['customer_name']} ({cid})"

    rd = _parse_date(report_date, default=date.today()) if report_date.strip() else date.today()
    cond = (condition or "BOTH").strip().upper() or "BOTH"
    if cond in ("ALL", "USED+NEW", "NEW+USED"):
        cond = "BOTH"

    def _call_report(day: date) -> Any:
        return _rpc(
            "get_inventory_report",
            {
                "p_client_id": cid,
                "p_report_date": day.isoformat(),
                "p_types": _csv_list(type_),
                "p_makes": _csv_list(make),
                "p_models": _csv_list(model),
                "p_locations": _csv_list(location),
                "p_years": _csv_int_list(year),
                "p_condition": cond,
            },
        )

    def _normalize_raw(payload: Any) -> dict:
        if isinstance(payload, list):
            payload = payload[0] if payload else {}
        return payload if isinstance(payload, dict) else {}

    def _report_units(payload: dict) -> int:
        inv = payload.get("inventoryList") or {}
        if isinstance(inv, dict):
            try:
                return int(inv.get("totalUnits") or 0)
            except (TypeError, ValueError):
                return 0
        meta = payload.get("meta") or {}
        try:
            return int(meta.get("rowCount") or 0)
        except (TypeError, ValueError):
            return 0

    try:
        raw = _normalize_raw(_call_report(rd))
    except Exception as e:
        return f"Inventory report RPC failed: {e}"

    # If today's snapshot is empty / not ready, fall back to latest daily pull_date.
    if (not raw or raw.get("ready") is False or _report_units(raw) == 0) and not report_date.strip():
        try:
            latest = (
                _client()
                .table("smart_hoot_inventory_daily")
                .select("pull_date")
                .order("pull_date", desc=True)
                .limit(1)
                .execute()
                .data
                or []
            )
            if latest and latest[0].get("pull_date"):
                fallback_day = _parse_date(str(latest[0]["pull_date"])[:10])
                if fallback_day != rd:
                    alt = _normalize_raw(_call_report(fallback_day))
                    if alt and (alt.get("ready") is not False) and _report_units(alt) > 0:
                        raw = alt
                        rd = fallback_day
        except Exception:
            pass

    use_live_fallback = (
        dealer.strip()
        and (not raw or raw.get("ready") is False or _report_units(raw) == 0)
    )
    if use_live_fallback:
        # Same live Hoot feed SmartAnalytics syncs from when daily snapshot misses a dealer.
        return (
            get_inventory_summary(dealer=dealer, by="make", source="live", limit=40)
            + "\n\n(Note: SmartAnalytics get_inventory_report had no daily snapshot for this "
            "dealer/date — showing live Hoot feed instead, same source as SmartAnalytics live inventory.)"
        )

    if not raw:
        return f"No inventory report data for {dealer_label} on {rd.isoformat()}."
    if raw.get("ready") is False:
        return (
            f"Inventory report not ready for {dealer_label} on {rd.isoformat()}. "
            f"Try an earlier report_date, or use get_inventory_summary / search_inventory (live)."
        )

    meta = raw.get("meta") or {}
    sections = raw.get("sections") or {}
    inv_list = raw.get("inventoryList") or {}
    list_rows = inv_list.get("rows") if isinstance(inv_list, dict) else []
    if not isinstance(list_rows, list):
        list_rows = []

    parts = [
        f"Inventory report — {dealer_label}",
        f"Requested: {rd.isoformat()} | pull_date: {meta.get('pullDate') or meta.get('requestedDate') or rd.isoformat()}",
        f"Source: {meta.get('inventorySource') or meta.get('source') or 'daily snapshot'} | "
        f"units={_fmt_int(inv_list.get('totalUnits') if isinstance(inv_list, dict) else meta.get('rowCount'))} | "
        f"value=${float((inv_list.get('totalValue') if isinstance(inv_list, dict) else 0) or 0):,.0f}",
    ]

    for section_key in ("condition", "make", "location", "type"):
        sec = sections.get(section_key) or {}
        rows = sec.get("rows") or []
        if not rows:
            continue
        body = [
            [
                r.get("label") or "(blank)",
                _fmt_int(r.get("units")),
                f"${float(r.get('totalValue') or 0):,.0f}",
            ]
            for r in rows[:40]
        ]
        parts.append(
            f"\nBy {section_key} (total units {_fmt_int(sec.get('totalUnits'))})\n"
            + _table_lines(["Label", "Units", "Value"], body)
        )

    lim = max(1, min(int(list_limit or 100), 500))
    if list_rows:
        body = [
            [
                r.get("manufacturer") or "",
                r.get("brandModel") or "",
                r.get("condition") or "",
                _fmt_int(r.get("units")),
                f"${float(r.get('totalValue') or 0):,.0f}",
                f"${float(r.get('averagePrice') or 0):,.0f}",
            ]
            for r in list_rows[:lim]
        ]
        parts.append(
            f"\nInventory list (first {min(lim, len(list_rows))} of {len(list_rows)} groups)\n"
            + _table_lines(
                ["Make", "Model", "Cond", "Units", "Value", "Avg $"],
                body,
            )
        )
    return "\n".join(parts)


@mcp.tool()
def upsert_dealer_location(dealer: str, location_name: str) -> str:
    """Create or keep a rooftop/location for a dealer (CRUD — write).

    Same smart_dealer_locations table SmartAnalytics location filters use.
    """
    info = _resolve_dealer(dealer)
    loc = (location_name or "").strip()
    if not loc:
        return "location_name is required."
    cid = str(info["client_id"])
    existing = (
        _client()
        .table("smart_dealer_locations")
        .select("id, location_name, customer_id")
        .eq("customer_id", cid)
        .ilike("location_name", loc)
        .limit(5)
        .execute()
        .data
        or []
    )
    exact = [r for r in existing if (r.get("location_name") or "").lower() == loc.lower()]
    if exact:
        return f"Location already exists: {exact[0].get('location_name')} (client {cid})"
    _client().table("smart_dealer_locations").insert(
        {"customer_id": cid, "location_name": loc}
    ).execute()
    return f"Added location '{loc}' for {info['customer_name']} ({cid})."


@mcp.tool()
def delete_dealer_location(dealer: str, location_name: str) -> str:
    """Delete a rooftop/location for a dealer (CRUD — delete)."""
    info = _resolve_dealer(dealer)
    loc = (location_name or "").strip()
    if not loc:
        return "location_name is required."
    cid = str(info["client_id"])
    res = (
        _client()
        .table("smart_dealer_locations")
        .delete()
        .eq("customer_id", cid)
        .ilike("location_name", loc)
        .execute()
    )
    deleted = res.data or []
    if not deleted:
        return f"No location matching '{loc}' for {info['customer_name']}."
    return f"Deleted {len(deleted)} location row(s) matching '{loc}' for {info['customer_name']}."


@mcp.tool()
def update_page_rules(
    dealer: str,
    vdp_logic: str = "",
    srp_logic: str = "",
    home_page_logic: str = "",
    others: str = "",
    hoot_link: str = "",
    scrap_link: str = "",
) -> str:
    """Update VDP/SRP/homepage URL rules for a dealer (CRUD — update).

    Only fields you pass (non-empty) are updated on smart_vdp_logic.
    """
    info = _resolve_dealer(dealer)
    name = info["customer_name"]
    cid = str(info["client_id"])
    patch: dict[str, Any] = {}
    if vdp_logic.strip():
        patch["vdp_logic"] = vdp_logic.strip()
    if srp_logic.strip():
        patch["srp_logic"] = srp_logic.strip()
    if home_page_logic.strip():
        patch["home_page_logic"] = home_page_logic.strip()
    if others.strip():
        patch["others"] = others.strip()
    if hoot_link.strip():
        patch["hoot_link"] = hoot_link.strip()
    if scrap_link.strip():
        patch["scrap_link"] = scrap_link.strip()
    if not patch:
        return "Provide at least one field to update (vdp_logic, srp_logic, home_page_logic, others, hoot_link, scrap_link)."

    rows = (
        _client()
        .table("smart_vdp_logic")
        .select("id, dealer_name, dealer_id")
        .or_(f"dealer_id.eq.{cid},dealer_name.ilike.%{name}%")
        .limit(5)
        .execute()
        .data
        or []
    )
    if not rows:
        return f"No smart_vdp_logic row for {name}. Create one in SmartAnalytics admin first."
    ids = [r["id"] for r in rows if r.get("id") is not None]
    _client().table("smart_vdp_logic").update(patch).in_("id", ids).execute()
    return (
        f"Updated page rules for {name} (rows {ids}).\n"
        f"Fields: {', '.join(patch.keys())}"
    )


@mcp.tool()
def upsert_user_access(
    email: str,
    role_key: str = "user",
    all_reports: bool = False,
    all_dealers: bool = False,
) -> str:
    """Create or update a SmartAnalytics user role row (CRUD — write).

    role_key examples: admin, user, viewer (whatever your smart_roles uses).
    """
    em = (email or "").strip().lower()
    if not em or "@" not in em:
        return "Valid email is required."
    role = (role_key or "user").strip() or "user"
    existing = (
        _client()
        .table("smart_user_roles")
        .select("email, role_key")
        .eq("email", em)
        .limit(1)
        .execute()
        .data
        or []
    )
    payload = {
        "email": em,
        "role_key": role,
        "all_reports": bool(all_reports),
        "all_dealers": bool(all_dealers),
        "updated_at": datetime.utcnow().isoformat() + "Z",
    }
    if existing:
        _client().table("smart_user_roles").update(payload).eq("email", em).execute()
        return f"Updated access for {em}: role={role}, all_reports={all_reports}, all_dealers={all_dealers}"
    _client().table("smart_user_roles").insert(payload).execute()
    return f"Created access for {em}: role={role}, all_reports={all_reports}, all_dealers={all_dealers}"


@mcp.tool()
def delete_user_access(email: str) -> str:
    """Remove a user role assignment (CRUD — delete)."""
    em = (email or "").strip().lower()
    if not em:
        return "email is required."
    res = _client().table("smart_user_roles").delete().eq("email", em).execute()
    n = len(res.data or [])
    if not n:
        return f"No smart_user_roles row for {em}."
    return f"Deleted access for {em} ({n} row(s))."


@mcp.tool()
def get_page_rules(dealer: str) -> str:
    """Show VDP / SRP / homepage URL filtration logic for a dealer.

    Use when the user asks how VDPs are detected, URL rules, or CMS patterns.
    """
    info = _resolve_dealer(dealer)
    name = info["customer_name"]
    rows = (
        _client().table("smart_vdp_logic")
        .select(
            "dealer_name, dealer_id, website_url, cms, data_source, "
            "vdp_logic, srp_logic, home_page_logic, others, hoot_link, scrap_link"
        )
        .ilike("dealer_name", f"%{name}%")
        .limit(10)
        .execute()
        .data
        or []
    )
    if not rows and info.get("client_id"):
        rows = (
            _client().table("smart_vdp_logic")
            .select(
                "dealer_name, dealer_id, website_url, cms, data_source, "
                "vdp_logic, srp_logic, home_page_logic, others, hoot_link, scrap_link"
            )
            .eq("dealer_id", info["client_id"])
            .limit(10)
            .execute()
            .data
            or []
        )
    if not rows:
        return f"No VDP logic rows found for {name}."

    parts = [f"Page rules for {name} (client {info['client_id']})"]
    for r in rows:
        parts.append(
            "\n".join(
                [
                    f"Dealer: {r.get('dealer_name')}",
                    f"CMS: {r.get('cms') or '—'}",
                    f"Website: {r.get('website_url') or '—'}",
                    f"Data source: {r.get('data_source') or '—'}",
                    f"VDP logic: {r.get('vdp_logic') or '—'}",
                    f"SRP logic: {r.get('srp_logic') or '—'}",
                    f"Home logic: {r.get('home_page_logic') or '—'}",
                    f"Other: {r.get('others') or '—'}",
                    f"Hoot link: {r.get('hoot_link') or '—'}",
                    f"Scrap link: {r.get('scrap_link') or '—'}",
                ]
            )
        )
    return "\n\n---\n".join(parts)


@mcp.tool()
def get_ga4_sync_status(dealer: str = "") -> str:
    """Check GA4 property sync status for one dealer or list recent configs.

    Use for: is GA4 syncing, last fetch time, sync group / status.
    """
    cols = (
        "client_id, account_name, ga4_property_id, is_active, sync_status, "
        "sync_group, last_fetched_at, master_last_synced, master_sync_status"
    )
    if dealer.strip():
        info = _resolve_dealer(dealer)
        rows = (
            _client().table("smart_ga4_config")
            .select(cols)
            .eq("client_id", info["client_id"])
            .execute()
            .data
            or []
        )
        title = f"GA4 sync — {info['customer_name']} ({info['client_id']})"
    else:
        rows = (
            _client().table("smart_ga4_config")
            .select(cols)
            .eq("is_active", True)
            .order("account_name")
            .limit(40)
            .execute()
            .data
            or []
        )
        title = "GA4 sync (active configs, first 40)"

    if not rows:
        return title + "\nNo config rows found."
    table = [
        [
            r.get("account_name") or "",
            r.get("client_id") or "",
            r.get("ga4_property_id") or "",
            r.get("sync_status") or "",
            r.get("last_fetched_at") or "",
            r.get("master_sync_status") or "",
        ]
        for r in rows
    ]
    return title + "\n" + _table_lines(
        ["Account", "Client ID", "Property", "Sync", "Last fetched", "Master"],
        table,
    )


@mcp.tool()
def list_user_access(email: str = "", limit: int = 30) -> str:
    """List user roles / dealer access assignments (auth helpers, not chart data).

    Use sparingly — for "who has access" style questions.
    email: optional filter.
    """
    q = _client().table("smart_user_roles").select(
        "email, role_key, all_reports, all_dealers, updated_at"
    )
    if email.strip():
        q = q.ilike("email", f"%{email.strip()}%")
    rows = q.order("email").limit(max(1, min(limit, 100))).execute().data or []
    if not rows:
        return "No user role rows matched."
    return _table_lines(
        ["Email", "Role", "All reports", "All dealers", "Updated"],
        [
            [
                r.get("email"),
                r.get("role_key"),
                r.get("all_reports"),
                r.get("all_dealers"),
                r.get("updated_at"),
            ]
            for r in rows
        ],
    )


# ---------------------------------------------------------------------------
# Entrypoint
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    import argparse

    # Cloud Run sets K_SERVICE; Dockerfile / deploy script set MCP_TRANSPORT=http.
    on_cloud = bool(os.environ.get("K_SERVICE"))
    default_http = (
        os.environ.get("MCP_TRANSPORT", "").lower() == "http" or on_cloud
    )
    default_host = os.environ.get("HOST") or (
        "0.0.0.0" if default_http else "127.0.0.1"
    )
    # Cloud Run injects PORT=8080; local HTTP default stays 8001.
    default_port = int(os.environ.get("PORT", "8001" if not on_cloud else "8080"))

    parser = argparse.ArgumentParser(
        description=(
            "VDP MCP connector for Smart Analytics V2 (Supabase). "
            "Ask in plain English — dealer names, VDP views, channels, inventory. "
            "No table names required."
        )
    )
    parser.add_argument(
        "--http",
        action="store_true",
        default=default_http,
        help="Run HTTP MCP via Uvicorn (recommended for Cursor / Cloud Run).",
    )
    parser.add_argument("--host", default=default_host)
    parser.add_argument("--port", type=int, default=default_port)
    args = parser.parse_args()

    print(f"VDP MCP → Supabase {SUPABASE_URL}", file=sys.stderr, flush=True)
    if MCP_PUBLIC_URL:
        print(
            f"OAuth for Claude: enabled (issuer={MCP_PUBLIC_URL})",
            file=sys.stderr,
            flush=True,
        )
    try:
        _client()
        source = "env/Secret Manager" if on_cloud or not os.path.isfile(
            os.path.join(_SCRIPT_DIR, ".env")
        ) else ".env / env"
        print(f"Supabase auth: service_role key loaded ({source})", file=sys.stderr, flush=True)
    except RuntimeError as e:
        sys.exit(f"ERROR: {e}")

    tools = [
        "help_vdp",
        "list_dealers",
        "list_locations",
        "upsert_dealer_location",
        "delete_dealer_location",
        "get_vdp_summary",
        "get_daily_trend",
        "get_vdp_breakdown",
        "get_channel_mix",
        "get_top_pages",
        "search_inventory",
        "get_inventory_summary",
        "get_inventory_report",
        "get_page_rules",
        "update_page_rules",
        "get_ga4_sync_status",
        "list_user_access",
        "upsert_user_access",
        "delete_user_access",
    ]
    print("Tools: " + ", ".join(tools), file=sys.stderr, flush=True)

    try:
        if args.http:
            endpoint = f"http://{args.host}:{args.port}/mcp"
            public = f"{MCP_PUBLIC_URL}/mcp" if MCP_PUBLIC_URL else endpoint
            print(
                "\n"
                "HTTP mode (streamable-http)\n"
                f"  MCP endpoint : {public}\n"
                "  Claude URL   : must end with /mcp (not the bare Cloud Run host)\n"
                "  Example ask  : \"VDP views for Moix RV last 7 days\"\n"
                "  Stop         : Ctrl+C\n",
                file=sys.stderr,
                flush=True,
            )
            from mcp.server.transport_security import TransportSecuritySettings

            # DNS-rebinding protection blocks Cloud Run hostnames; disable like DV360.
            mcp.run(
                transport="streamable-http",
                host=args.host,
                port=args.port,
                transport_security=TransportSecuritySettings(
                    enable_dns_rebinding_protection=False
                ),
            )
        else:
            print(
                "\n"
                "stdio mode — Cursor/Claude spawn this process themselves.\n"
                "For HTTP (local or Cloud Run): python vdp_mcp_server.py --http\n"
                "Or set MCP_TRANSPORT=http (auto on Cloud Run via K_SERVICE).\n",
                file=sys.stderr,
            )
            mcp.run()
    except KeyboardInterrupt:
        print("Stopped.", file=sys.stderr)
