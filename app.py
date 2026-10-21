from __future__ import annotations

import base64
import json
import sqlite3
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

import pandas as pd
import requests
import streamlit as st
import yaml

APP_DIR = Path(__file__).resolve().parent
CONFIG_PATH = APP_DIR / "config.yaml"
DB_PATH = APP_DIR / "data" / "meme_intel.db"
DB_PATH.parent.mkdir(parents=True, exist_ok=True)
USER_AGENT = "SolanaMemeResearchDashboard/0.2 (read-only research tool)"
# Burn / system addresses: tokens sent here are not held by a seller.
NON_CIRCULATING_ADDRESSES = {"11111111111111111111111111111111", "1nc1nerator11111111111111111111111111111111"}


def utc_now() -> datetime:
    return datetime.now(timezone.utc)


def iso_now() -> str:
    return utc_now().isoformat(timespec="seconds")


def parse_time(value: str | None) -> datetime | None:
    if not value:
        return None
    try:
        dt = datetime.fromisoformat(value.replace("Z", "+00:00"))
        return dt if dt.tzinfo else dt.replace(tzinfo=timezone.utc)
    except (ValueError, TypeError):
        return None


def load_config() -> dict[str, Any]:
    with CONFIG_PATH.open("r", encoding="utf-8") as f:
        return yaml.safe_load(f) or {}


CFG = load_config()
PROVIDERS = CFG["providers"]


def db_connect() -> sqlite3.Connection:
    conn = sqlite3.connect(DB_PATH, timeout=15)
    conn.row_factory = sqlite3.Row
    return conn


def init_db() -> None:
    with db_connect() as conn:
        conn.executescript(
            """
            CREATE TABLE IF NOT EXISTS observations (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                observed_at TEXT NOT NULL,
                token_address TEXT NOT NULL,
                symbol TEXT,
                name TEXT,
                pair_address TEXT,
                pair_url TEXT,
                pair_age_minutes REAL,
                price_usd REAL,
                liquidity_usd REAL,
                volume_h1 REAL,
                volume_h24 REAL,
                fdv_usd REAL,
                buys_h1 INTEGER,
                sells_h1 INTEGER,
                holders_count INTEGER,
                top10_pct REAL,
                top_holder_pct REAL,
                cluster_pct REAL,
                score REAL,
                tier TEXT,
                hard_status TEXT,
                features_json TEXT,
                checks_json TEXT,
                raw_rugcheck_json TEXT
            );
            CREATE INDEX IF NOT EXISTS idx_obs_token_time ON observations(token_address, observed_at);
            CREATE TABLE IF NOT EXISTS alerts (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                created_at TEXT NOT NULL,
                token_address TEXT NOT NULL,
                symbol TEXT,
                tier TEXT NOT NULL,
                score REAL NOT NULL,
                price_at_alert REAL,
                pair_url TEXT,
                reason TEXT,
                features_json TEXT,
                price_1h REAL,
                return_1h_pct REAL,
                price_24h REAL,
                return_24h_pct REAL,
                price_7d REAL,
                return_7d_pct REAL
            );
            CREATE INDEX IF NOT EXISTS idx_alert_token_time ON alerts(token_address, created_at);
            CREATE TABLE IF NOT EXISTS holdings (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                token_address TEXT NOT NULL,
                symbol TEXT,
                entry_price REAL NOT NULL,
                quantity REAL,
                created_at TEXT NOT NULL,
                active INTEGER NOT NULL DEFAULT 1
            );
            CREATE TABLE IF NOT EXISTS exit_alerts (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                created_at TEXT NOT NULL,
                holding_id INTEGER NOT NULL,
                token_address TEXT NOT NULL,
                symbol TEXT,
                rule TEXT NOT NULL,
                observed_price REAL,
                entry_price REAL,
                details TEXT,
                UNIQUE(holding_id, rule)
            );
            """
        )


def get_json(url: str, *, params: dict | None = None, timeout: int = 10) -> Any:
    response = requests.get(
        url,
        params=params,
        timeout=timeout,
        headers={"User-Agent": USER_AGENT, "Accept": "application/json"},
    )
    response.raise_for_status()
    return response.json()


def as_list(data: Any) -> list[dict[str, Any]]:
    if isinstance(data, list):
        return [x for x in data if isinstance(x, dict)]
    if isinstance(data, dict):
        for key in ("tokens", "data", "results", "pairs", "items"):
            value = data.get(key)
            if isinstance(value, list):
                return [x for x in value if isinstance(x, dict)]
        if data.get("tokenAddress") or data.get("token_address") or data.get("mint"):
            return [data]
    return []


def discover_candidates(max_count: int) -> list[dict[str, Any]]:
    """A bounded sample from DEX Screener's latest profiles and boost feeds, not all launches."""
    base = PROVIDERS["dexscreener_base_url"].rstrip("/")
    feeds: list[tuple[str, bool]] = [
        (f"{base}/token-profiles/latest/v1", False),
        (f"{base}/token-boosts/latest/v1", True),
    ]
    collected: dict[str, dict[str, Any]] = {}
    for url, boosted in feeds:
        try:
            data = as_list(get_json(url))
        except (requests.RequestException, ValueError) as exc:
            st.warning(f"Discovery feed unavailable ({url.rsplit('/', 1)[-1]}): {exc}")
            continue
        for item in data:
            chain = str(item.get("chainId", item.get("chain_id", ""))).lower()
            addr = str(item.get("tokenAddress", item.get("token_address", ""))).strip()
            if chain != "solana" or not addr:
                continue
            key = addr.lower()
            if key not in collected:
                collected[key] = {
                    "token_address": addr,
                    "profile_url": item.get("url"),
                    "description": item.get("description"),
                    "profile_links": item.get("links") or [],
                    "boosted": boosted,
                }
            else:
                collected[key]["boosted"] = collected[key]["boosted"] or boosted
                if not collected[key].get("profile_links") and item.get("links"):
                    collected[key]["profile_links"] = item.get("links")
    return list(collected.values())[:max_count]


def get_best_pair(token_address: str) -> dict[str, Any] | None:
    base = PROVIDERS["dexscreener_base_url"].rstrip("/")
    url = f"{base}/token-pairs/v1/solana/{token_address}"
    try:
        data = as_list(get_json(url))
    except (requests.RequestException, ValueError):
        return None
    # Only use a pair where this mint is the BASE asset, so priceUsd refers to the token.
    matching = [
        p for p in data
        if str((p.get("baseToken") or {}).get("address", "")).lower() == token_address.lower()
    ]
    if not matching:
        return None
    matching.sort(key=lambda p: float(((p.get("liquidity") or {}).get("usd") or 0)), reverse=True)
    return matching[0]


def solana_mint_authorities(token_address: str) -> dict[str, Any]:
    """Read the first 82 bytes of SPL/Token-2022 Mint layout through public Solana JSON-RPC."""
    endpoint = PROVIDERS["solana_rpc_url"]
    payload = {
        "jsonrpc": "2.0", "id": 1, "method": "getAccountInfo",
        "params": [token_address, {"encoding": "base64", "commitment": "confirmed"}],
    }
    result = requests.post(endpoint, json=payload, timeout=10, headers={"User-Agent": USER_AGENT})
    result.raise_for_status()
    body = result.json()
    value = (((body.get("result") or {}).get("value")) or None)
    if not value:
        return {"mint_authority_revoked": None, "freeze_authority_revoked": None, "rpc_status": "account_not_found"}
    owner_program = value.get("owner")
    allowed_programs = {
        "TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA",
        "TokenzQdBNbLqP5VEhdkAS6EPFLC1PHnBqCXEpPxuEb",
    }
    if owner_program not in allowed_programs:
        return {"mint_authority_revoked": None, "freeze_authority_revoked": None, "rpc_status": "unknown_program"}
    try:
        raw = base64.b64decode(value["data"][0])
    except (KeyError, IndexError, ValueError, TypeError):
        return {"mint_authority_revoked": None, "freeze_authority_revoked": None, "rpc_status": "decode_error"}
    if len(raw) < 82:
        return {"mint_authority_revoked": None, "freeze_authority_revoked": None, "rpc_status": "short_mint_data"}
    # SPL Mint uses COption<Pubkey>: 4-byte little-endian tag + 32-byte key.
    mint_option = int.from_bytes(raw[0:4], "little")
    freeze_option = int.from_bytes(raw[46:50], "little")
    if mint_option not in (0, 1) or freeze_option not in (0, 1):
        return {"mint_authority_revoked": None, "freeze_authority_revoked": None, "rpc_status": "invalid_option_tag"}
    return {
        "mint_authority_revoked": mint_option == 0,
        "freeze_authority_revoked": freeze_option == 0,
        "rpc_status": "ok",
    }


def rugcheck_report(token_address: str) -> dict[str, Any] | None:
    base = PROVIDERS["rugcheck_base_url"].rstrip("/")
    try:
        data = get_json(f"{base}/tokens/{token_address}/report", timeout=12)
        return data if isinstance(data, dict) else None
    except (requests.RequestException, ValueError):
        return None


def first_value(d: dict[str, Any], *keys: str) -> Any:
    for key in keys:
        if key in d and d[key] is not None:
            return d[key]
    return None


def to_float(value: Any) -> float | None:
    try:
        if value is None or value == "":
            return None
        return float(value)
    except (TypeError, ValueError):
        return None


def to_bool(value: Any) -> bool | None:
    if isinstance(value, bool):
        return value
    if value is None:
        return None
    if isinstance(value, (int, float)):
        return bool(value)
    if isinstance(value, str):
        v = value.strip().lower()
        if v in {"true", "yes", "1", "active", "locked", "revoked"}:
            return True
        if v in {"false", "no", "0", "inactive", "unlocked", "renounced"}:
            return False
    return None


def extract_risk_fields(report: dict[str, Any] | None, pair_address: str | None = None) -> dict[str, Any]:
    if not report:
        return {
            "report_available": False, "mint_authority_revoked_report": None,
            "freeze_authority_revoked_report": None, "lp_locked_or_burned_pct": None,
            "top10_pct": None, "top_holder_pct": None, "cluster_pct": None,
            "holder_count": None, "creator_history_clear": None, "rugged": None,
            "risk_level": "unknown", "risk_names": [], "creator": None,
        }
    token = report.get("token") if isinstance(report.get("token"), dict) else {}
    summary = report.get("summary") if isinstance(report.get("summary"), dict) else {}
    graph = report.get("graphInsiderReport") or report.get("graph_insider_report") or {}
    if not isinstance(graph, dict):
        graph = {}

    mint_auth = first_value(report, "mintAuthority", "mint_authority_active")
    if mint_auth is None:
        mint_auth = first_value(token, "mintAuthority", "mint_authority_active")
    freeze_auth = first_value(report, "freezeAuthority", "freeze_authority_active")
    if freeze_auth is None:
        freeze_auth = first_value(token, "freezeAuthority", "freeze_authority_active")
    # In RugCheck reports, a null authority means revoked; an explicit boolean "active" uses the opposite semantics.
    if "mint_authority_active" in report or "mint_authority_active" in token:
        mint_revoked = (to_bool(mint_auth) is False) if mint_auth is not None else None
    else:
        mint_revoked = True if mint_auth is None and ("mintAuthority" in report or "mintAuthority" in token) else (False if mint_auth else None)
    if "freeze_authority_active" in report or "freeze_authority_active" in token:
        freeze_revoked = (to_bool(freeze_auth) is False) if freeze_auth is not None else None
    else:
        freeze_revoked = True if freeze_auth is None and ("freezeAuthority" in report or "freezeAuthority" in token) else (False if freeze_auth else None)

    # RugCheck /report layout (checked against live responses): the LP lock is per market in
    # markets[].lp.lpLockedPct, and pool/vault accounts show up inside topHolders, so they are matched
    # against the market addresses before any holder concentration is computed.
    markets = report.get("markets") if isinstance(report.get("markets"), list) else []
    pool_addrs: set[str] = set(NON_CIRCULATING_ADDRESSES)
    market_lp: list[tuple[float, float | None, bool]] = []  # (size_usd, lpLockedPct, matches_dexscreener_pair)
    for mk in markets:
        if not isinstance(mk, dict):
            continue
        for key in ("pubkey", "liquidityA", "liquidityB", "mintLP"):
            val = mk.get(key)
            if isinstance(val, str) and val:
                pool_addrs.add(val)
        for key in ("liquidityAAccount", "liquidityBAccount"):
            acct = mk.get(key)
            if isinstance(acct, dict) and isinstance(acct.get("owner"), str):
                pool_addrs.add(acct["owner"])
        lp_obj = mk.get("lp") if isinstance(mk.get("lp"), dict) else {}
        size = (to_float(lp_obj.get("quoteUSD")) or 0.0) + (to_float(lp_obj.get("baseUSD")) or 0.0)
        matches = bool(pair_address) and str(mk.get("pubkey", "")).lower() == str(pair_address).lower()
        market_lp.append((size, to_float(lp_obj.get("lpLockedPct")), matches))
    market_lp_pct: float | None = None
    usable = [m for m in market_lp if m[1] is not None]
    if usable:
        pool_pick = [m for m in usable if m[2]] or usable
        market_lp_pct = max(pool_pick, key=lambda m: m[0])[1]

    lp_lock_pct = to_float(first_value(report, "lpLockedPct", "lp_locked_pct"))
    lp_burn_pct = to_float(first_value(report, "lpBurnedPct", "lp_burned_pct"))
    lp_locked = to_bool(first_value(report, "lpLocked", "lp_locked"))
    if lp_lock_pct is not None or lp_burn_pct is not None:
        lp_locked_pct = min(100.0, max(0.0, (lp_lock_pct or 0.0) + (lp_burn_pct or 0.0)))
    elif lp_locked is True:
        lp_locked_pct = 100.0
    elif lp_locked is False:
        lp_locked_pct = 0.0
    else:
        lp_locked_pct = None
    if market_lp_pct is not None:  # the per-market value is what the real report provides
        lp_locked_pct = min(100.0, max(0.0, market_lp_pct))

    # Use an explicit top-10 value where available. Otherwise, compute from holder records only
    # if the API labels each holder's percentage and common pool/LP labels can be excluded.
    top10_pct = to_float(first_value(report, "top10HoldersPct", "top10_holders_pct", "topHoldersPct", "top_holders_pct"))
    top_holder_pct = to_float(first_value(report, "topHolderPct", "top_holder_pct", "largestHolderPct", "largest_holder_pct"))
    holders = report.get("topHolders") or report.get("top_holders") or []
    holder_pcts: list[float] = []
    pool_excluded = 0
    insider_holder_pct = 0.0
    if isinstance(holders, list):
        for holder in holders:
            if not isinstance(holder, dict):
                continue
            label = " ".join(str(holder.get(k, "")) for k in ("label", "name", "type", "accountType")).lower()
            is_pool = (any(x in label for x in ("pool", "liquidity", "lp token", "amm vault")) or bool(holder.get("isLp"))
                       or bool(holder.get("isLiquidityPool")) or str(holder.get("owner", "")) in pool_addrs
                       or str(holder.get("address", "")) in pool_addrs)
            pct = to_float(first_value(holder, "percentage", "pct", "percent"))
            if pct is not None and is_pool:
                pool_excluded += 1
            if pct is not None and not is_pool:
                holder_pcts.append(pct)
                if holder.get("insider") is True:
                    insider_holder_pct += pct
    if top_holder_pct is None and holder_pcts:
        top_holder_pct = max(holder_pcts)
    if top10_pct is None and len(holder_pcts) >= 10:
        top10_pct = sum(sorted(holder_pcts, reverse=True)[:10])

    cluster_pct = to_float(first_value(report, "insider_pct", "insiderPct", "cluster_pct", "clusterPct"))
    if cluster_pct is None:
        cluster_pct = to_float(first_value(graph, "insiderSupplyPct", "insider_supply_pct", "supplyPct", "totalInsiderPct"))
    # Real report: graphInsidersDetected (count), insiderNetworks (list or null) and a per-holder `insider` flag.
    graph_detected = report.get("graphInsidersDetected")
    insider_networks = report.get("insiderNetworks")
    networks_count: int | None = None
    if isinstance(graph_detected, (int, float)) and not isinstance(graph_detected, bool):
        networks_count = int(graph_detected)
    elif isinstance(insider_networks, list):
        networks_count = len(insider_networks)
    cluster_lower_bound = False
    if cluster_pct is None and holder_pcts and networks_count is not None:
        cluster_pct = insider_holder_pct  # insider-flagged holders among the listed top holders
        # Networks were found but their supply share is not provided: this number is only a lower bound.
        cluster_lower_bound = networks_count > 0
    holder_count = first_value(report, "totalHolders", "total_holders", "holderCount", "holder_count")
    try:
        holder_count = int(holder_count) if holder_count is not None else None
    except (ValueError, TypeError):
        holder_count = None

    rugged = to_bool(first_value(report, "rugged", "isRugged", "is_rugged"))
    history_raw = first_value(report, "creatorRugCount", "creator_rug_count", "deployerRugCount", "deployer_rug_count", "creatorRuggedTokens")
    if history_raw is not None:
        try:
            creator_history_clear = int(history_raw) == 0
        except (ValueError, TypeError):
            creator_history_clear = None
    else:
        creator_history_clear = to_bool(first_value(report, "creatorHistoryClear", "creator_history_clear", "deployerHistoryClear"))
    if rugged is True:
        creator_history_clear = False

    risks = report.get("risks") or []
    risk_names = [str(r.get("name", "risk")) for r in risks if isinstance(r, dict)] if isinstance(risks, list) else []
    if creator_history_clear is None and any("creator" in n.lower() and "rug" in n.lower() for n in risk_names):
        creator_history_clear = False  # RugCheck itself lists a creator rug-history risk
    risk_level = str(first_value(report, "riskLevel", "risk_level", "result") or summary.get("riskLevel") or "unknown")
    creator = first_value(report, "creator", "deployer", "creatorAddress", "creator_address")
    return {
        "report_available": True,
        "mint_authority_revoked_report": mint_revoked,
        "freeze_authority_revoked_report": freeze_revoked,
        "lp_locked_or_burned_pct": lp_locked_pct,
        "top10_pct": top10_pct,
        "top_holder_pct": top_holder_pct,
        "cluster_pct": cluster_pct,
        "holder_count": holder_count,
        "creator_history_clear": creator_history_clear,
        "rugged": rugged,
        "risk_level": risk_level,
        "risk_names": risk_names,
        "creator": creator,
        "cluster_lower_bound": cluster_lower_bound,
        "insider_networks": networks_count,
        "pool_accounts_excluded": pool_excluded,
    }


def pair_metrics(pair: dict[str, Any], candidate: dict[str, Any]) -> dict[str, Any]:
    base_token = pair.get("baseToken") or {}
    liq = pair.get("liquidity") or {}
    volume = pair.get("volume") or {}
    txns = pair.get("txns") or {}
    h1 = txns.get("h1") or {}
    info = pair.get("info") or {}
    pair_created = pair.get("pairCreatedAt")
    age_mins: float | None = None
    if pair_created:
        try:
            age_mins = max(0.0, (time.time() * 1000 - float(pair_created)) / 60_000)
        except (ValueError, TypeError):
            pass
    address = candidate["token_address"]
    socials = info.get("socials") or []
    websites = info.get("websites") or []
    profile_links = candidate.get("profile_links") or []
    social_count = len(socials) + sum(1 for x in profile_links if isinstance(x, dict) and "social" in str(x.get("type", "")).lower())
    website_count = len(websites) + sum(1 for x in profile_links if isinstance(x, dict) and "website" in str(x.get("type", "")).lower())
    price = to_float(pair.get("priceUsd"))
    return {
        "token_address": address,
        "symbol": str(base_token.get("symbol") or "?"),
        "name": str(base_token.get("name") or "Unknown"),
        "pair_address": pair.get("pairAddress"),
        "pair_url": pair.get("url") or candidate.get("profile_url"),
        "pair_age_minutes": age_mins,
        "price_usd": price,
        "liquidity_usd": to_float(liq.get("usd")) or 0.0,
        "volume_h1": to_float(volume.get("h1")) or 0.0,
        "volume_h24": to_float(volume.get("h24")) or 0.0,
        "fdv_usd": to_float(pair.get("marketCap")) or to_float(pair.get("fdv")),
        "buys_h1": int(h1.get("buys") or 0),
        "sells_h1": int(h1.get("sells") or 0),
        "social_count": social_count,
        "website_count": website_count,
        "boosted": bool(candidate.get("boosted")),
        "boost_active": to_float((pair.get("boosts") or {}).get("active")) or 0,
        "pair_created_at_ms": pair_created,
    }


def previous_observation(token_address: str, older_than_minutes: float = 0) -> dict[str, Any] | None:
    boundary = (utc_now() - timedelta(minutes=older_than_minutes)).isoformat(timespec="seconds")
    with db_connect() as conn:
        row = conn.execute(
            "SELECT * FROM observations WHERE token_address=? AND observed_at < ? ORDER BY observed_at DESC LIMIT 1",
            (token_address, boundary),
        ).fetchone()
    return dict(row) if row else None


def hard_checks(metrics: dict[str, Any], risk: dict[str, Any], rpc: dict[str, Any]) -> dict[str, dict[str, Any]]:
    filters = CFG["hard_filters"]
    checks: dict[str, dict[str, Any]] = {}

    def put(name: str, status: str, detail: str) -> None:
        checks[name] = {"status": status, "detail": detail}

    liq = metrics.get("liquidity_usd")
    put("minimum liquidity", "REJECT" if liq is not None and liq < filters["min_liquidity_usd"] else "PASS", f"${liq:,.0f} (minimum ${filters['min_liquidity_usd']:,.0f})")

    age = metrics.get("pair_age_minutes")
    lo, hi = filters["pair_age_minutes"]
    if age is None:
        put("pair age", "UNKNOWN", "Pair creation time unavailable; this is pair age, not proven token mint age.")
    elif age < lo or age > hi:
        put("pair age", "REJECT", f"{age:.1f} minutes; allowed {lo}–{hi} minutes")
    else:
        put("pair age", "PASS", f"{age:.1f} minutes (pair age proxy)")

    for label, key, required in [
        ("mint authority revoked", "mint_authority_revoked", filters["require_mint_authority_revoked"]),
        ("freeze authority revoked", "freeze_authority_revoked", filters["require_freeze_authority_revoked"]),
    ]:
        value = rpc.get(key)
        if value is None:
            put(label, "UNKNOWN", f"Authority could not be verified ({rpc.get('rpc_status', 'unknown')}).")
        elif required and not value:
            put(label, "REJECT", "Authority is still active.")
        else:
            put(label, "PASS", "Revoked according to Solana RPC." if value else "Not required by config.")

    lp = risk.get("lp_locked_or_burned_pct")
    min_lp = filters["min_lp_locked_or_burned_pct"]
    if lp is None:
        put("LP locked/burned", "UNKNOWN", "RugCheck did not provide a parseable lock/burn percentage. Check expiry and pool identity manually.")
    elif filters["require_lp_locked_or_burned"] and lp < min_lp:
        put("LP locked/burned", "REJECT", f"{lp:.1f}% verified by provider; minimum {min_lp:.1f}%")
    else:
        put("LP locked/burned", "PASS", f"{lp:.1f}% reported locked/burned; lock expiry still needs review.")

    top10 = risk.get("top10_pct")
    max_top10 = filters["max_top10_holders_pct"]
    if top10 is None:
        put("top-10 concentration", "UNKNOWN", "Provider did not expose a verifiable top-10 percentage excluding pool/vault holdings.")
    elif top10 > max_top10:
        put("top-10 concentration", "REJECT", f"{top10:.2f}% > {max_top10:.2f}%")
    else:
        put("top-10 concentration", "PASS", f"{top10:.2f}% ≤ {max_top10:.2f}% (provider calculation; verify LP exclusions)")

    top1 = risk.get("top_holder_pct")
    max_top1 = filters["max_single_wallet_pct"]
    if top1 is None:
        put("single-holder concentration", "UNKNOWN", "Largest wallet percentage unavailable or pool exclusion unclear.")
    elif top1 > max_top1:
        put("single-holder concentration", "REJECT", f"{top1:.2f}% > {max_top1:.2f}%")
    else:
        put("single-holder concentration", "PASS", f"{top1:.2f}% ≤ {max_top1:.2f}% (provider calculation; verify LP exclusions)")

    cluster = risk.get("cluster_pct")
    max_cluster = filters["max_cluster_pct"]
    cluster_required = bool(filters.get("require_cluster_data", True))
    lower = bool(risk.get("cluster_lower_bound"))
    if cluster is None:
        nets = risk.get("insider_networks")
        why = (f"RugCheck detected {nets} insider network(s) but gave no supply share." if nets
               else "Graph/insider data unavailable; shared-funder relationships may not be covered.")
        put("insider/cluster concentration", "UNKNOWN" if cluster_required else "ADVISORY", why)
    elif cluster > max_cluster:
        put("insider/cluster concentration", "REJECT",
            f"{cluster:.2f}% > {max_cluster:.2f}%" + (" (lower bound from top-20 holders)" if lower else ""))
    elif lower:
        put("insider/cluster concentration", "UNKNOWN" if cluster_required else "ADVISORY",
            f"{cluster:.2f}% held by flagged insiders in the top 20, but insider network(s) were detected; total share cannot be verified.")
    else:
        put("insider/cluster concentration", "PASS",
            f"{cluster:.2f}% ≤ {max_cluster:.2f}% (insider-flagged holders in the top 20; no insider networks detected)")

    if risk.get("rugged") is True:
        put("creator/rug history", "REJECT", "Provider flags this token as rugged.")
    elif risk.get("creator_history_clear") is True:
        put("creator/rug history", "PASS", "Provider exposed a clear creator-history result.")
    elif risk.get("creator_history_clear") is False:
        put("creator/rug history", "REJECT", "Provider indicates previous creator/deployer rug history.")
    else:
        creator_required = bool(filters.get("require_creator_history_clear", True))
        put("creator/rug history", "UNKNOWN" if creator_required else "ADVISORY",
            "RugCheck's report has no deployer rug-history count. Only the rugged flag and risk names were checked and neither flagged the creator; check the creator wallet manually.")

    if risk.get("report_available") is not True:
        put("RugCheck report", "UNKNOWN", "Risk report unavailable; no safety pass is inferred.")
    elif risk.get("rugged") is True:
        put("RugCheck report", "REJECT", "Provider flags current token as rugged.")
    else:
        put("RugCheck report", "PASS", f"Report returned (risk level: {risk.get('risk_level', 'unknown')}). This is not a guarantee of safety.")
    return checks


def previous_holder_growth(token_address: str, current_holders: int | None) -> tuple[float | None, float | None]:
    if current_holders is None:
        return None, None
    prev = previous_observation(token_address, older_than_minutes=8)
    if not prev or prev.get("holders_count") is None:
        return None, None
    prev_time = parse_time(prev.get("observed_at"))
    if not prev_time:
        return None, None
    elapsed = (utc_now() - prev_time).total_seconds() / 60
    if elapsed < 8 or elapsed > 45:
        return None, None
    delta = current_holders - int(prev["holders_count"])
    return delta * 10 / elapsed, elapsed


def score_candidate(metrics: dict[str, Any], risk: dict[str, Any], token_address: str) -> tuple[int, dict[str, Any]]:
    weights = CFG["score_weights"]
    features: dict[str, Any] = {}
    score = 0

    growth, growth_elapsed = previous_holder_growth(token_address, risk.get("holder_count"))
    if growth is None:
        holder_score = 0
    elif growth >= 25:
        holder_score = weights["holder_growth"]
    elif growth >= 10:
        holder_score = round(weights["holder_growth"] * (2 / 3))
    elif growth >= 5:
        holder_score = round(weights["holder_growth"] * (1 / 3))
    else:
        holder_score = 0
    features["holder_growth_per_10m"] = growth
    features["holder_growth_observation_elapsed_minutes"] = growth_elapsed
    features["holder_growth_score"] = holder_score
    score += holder_score

    buys, sells = metrics["buys_h1"], metrics["sells_h1"]
    ratio = buys / max(1, sells)
    if ratio >= 1.5 and buys >= 20:
        organic_score = weights["organic_buying"]
    elif ratio >= 1.2 and buys >= 10:
        organic_score = round(weights["organic_buying"] * 0.75)
    elif ratio > 1 and buys >= 5:
        organic_score = round(weights["organic_buying"] * 0.5)
    elif ratio > 1:
        organic_score = round(weights["organic_buying"] * 0.25)
    else:
        organic_score = 0
    features["buy_sell_txn_ratio_h1"] = round(ratio, 3)
    features["buys_h1"] = buys
    features["sells_h1"] = sells
    features["organic_buying_score_proxy"] = organic_score
    score += organic_score

    vol_h1, vol_h24 = metrics["volume_h1"], metrics["volume_h24"]
    hourly_baseline = vol_h24 / 24 if vol_h24 else 0
    trend_ratio = vol_h1 / hourly_baseline if hourly_baseline > 0 else 0
    cap = metrics.get("fdv_usd")
    liq_ratio = (metrics["liquidity_usd"] / cap) if cap and cap > 0 else None
    if trend_ratio >= 2.0 and (liq_ratio is None or liq_ratio >= 0.05):
        vol_score = weights["volume_trend"]
    elif trend_ratio >= 1.3 and (liq_ratio is None or liq_ratio >= 0.05):
        vol_score = round(weights["volume_trend"] * 0.75)
    elif trend_ratio >= 1.0:
        vol_score = round(weights["volume_trend"] * 0.5)
    elif trend_ratio >= 0.7:
        vol_score = round(weights["volume_trend"] * 0.25)
    else:
        vol_score = 0
    features["h1_volume_vs_24h_hourly_average"] = round(trend_ratio, 3)
    features["liquidity_to_fdv_ratio"] = round(liq_ratio, 4) if liq_ratio is not None else None
    features["volume_trend_score"] = vol_score
    score += vol_score

    top10, top1 = risk.get("top10_pct"), risk.get("top_holder_pct")
    if top10 is not None and top1 is not None:
        if top10 <= 15 and top1 <= 3:
            dist_score = weights["distribution"]
        elif top10 <= 20 and top1 <= 4:
            dist_score = round(weights["distribution"] * 0.7)
        elif top10 <= 25 and top1 <= 5:
            dist_score = round(weights["distribution"] * 0.4)
        else:
            dist_score = 0
    else:
        dist_score = 0
    features["top10_pct"] = top10
    features["top_holder_pct"] = top1
    features["distribution_score"] = dist_score
    score += dist_score

    # Price behaviour is assessed only after multiple local snapshots exist.
    # Reward a measured pullback with a relatively stable latest observation,
    # not a very sharp candle. This is a heuristic, not a prediction.
    current_price = metrics.get("price_usd")
    six_hours_ago = (utc_now() - timedelta(hours=6)).isoformat(timespec="seconds")
    price_rows = []
    with db_connect() as conn:
        price_rows = conn.execute(
            "SELECT price_usd, observed_at FROM observations WHERE token_address=? AND observed_at>=? AND price_usd IS NOT NULL ORDER BY observed_at DESC LIMIT 30",
            (token_address, six_hours_ago),
        ).fetchall()
    historic_prices = [float(r["price_usd"]) for r in price_rows if r["price_usd"] is not None and float(r["price_usd"]) > 0]
    price_pullback_pct = None
    latest_price_change_pct = None
    if current_price and current_price > 0 and historic_prices:
        rolling_high = max(historic_prices + [float(current_price)])
        price_pullback_pct = (1 - float(current_price) / rolling_high) * 100 if rolling_high else None
        previous_price = historic_prices[0]
        latest_price_change_pct = (float(current_price) / previous_price - 1) * 100 if previous_price > 0 else None
    if price_pullback_pct is not None and latest_price_change_pct is not None:
        if 5 <= price_pullback_pct <= 30 and latest_price_change_pct >= -8:
            price_score = weights["price_behavior"]
        elif price_pullback_pct < 5 and abs(latest_price_change_pct) <= 3:
            price_score = round(weights["price_behavior"] * 0.5)
        else:
            price_score = 0
    else:
        price_score = 0
    features["price_pullback_from_6h_local_high_pct"] = round(price_pullback_pct, 3) if price_pullback_pct is not None else None
    features["latest_change_from_previous_snapshot_pct"] = round(latest_price_change_pct, 3) if latest_price_change_pct is not None else None
    features["price_behavior_score"] = price_score
    score += price_score

    socials = int(metrics.get("social_count") or 0)
    websites = int(metrics.get("website_count") or 0)
    social_score = min(weights["social"], socials * 2 + websites * 2)
    # Paid boosting is promotion, not evidence of organic demand; it cannot add points.
    if metrics.get("boosted"):
        social_score = min(social_score, 4)
    features["profile_social_links"] = socials
    features["profile_websites"] = websites
    features["boosted_paid_visibility_flag"] = bool(metrics.get("boosted"))
    features["social_metadata_score"] = social_score
    score += social_score

    features["price_usd"] = metrics.get("price_usd")
    features["liquidity_usd"] = metrics.get("liquidity_usd")
    features["volume_h1"] = vol_h1
    features["volume_h24"] = vol_h24
    features["holder_count"] = risk.get("holder_count")
    features["score_is_proxy_based"] = True
    return min(100, int(score)), features


def evaluate_status(checks: dict[str, dict[str, Any]], score: int) -> tuple[str, str]:
    states = [v["status"] for v in checks.values()]
    if "REJECT" in states:
        return "REJECT", "No alert"
    if "UNKNOWN" in states:
        # The score can be shown for research, but unknown critical safety checks block trade-style alerts.
        return "INCOMPLETE", "Review only — key safety checks unknown"
    alert_cfg = CFG["score_thresholds"]
    if score >= alert_cfg["strong"]:
        return "PASS", "STRONG"
    if score >= alert_cfg["watchlist"]:
        return "PASS", "WATCHLIST"
    return "PASS", "No alert"


def log_observation(metrics: dict[str, Any], risk: dict[str, Any], score: int, tier: str, status: str,
                    features: dict[str, Any], checks: dict[str, Any], raw_report: dict[str, Any] | None) -> None:
    with db_connect() as conn:
        conn.execute(
            """INSERT INTO observations (
                observed_at, token_address, symbol, name, pair_address, pair_url, pair_age_minutes,
                price_usd, liquidity_usd, volume_h1, volume_h24, fdv_usd, buys_h1, sells_h1,
                holders_count, top10_pct, top_holder_pct, cluster_pct, score, tier, hard_status,
                features_json, checks_json, raw_rugcheck_json
            ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
            (
                iso_now(), metrics.get("token_address"), metrics.get("symbol"), metrics.get("name"),
                metrics.get("pair_address"), metrics.get("pair_url"), metrics.get("pair_age_minutes"),
                metrics.get("price_usd"), metrics.get("liquidity_usd"), metrics.get("volume_h1"),
                metrics.get("volume_h24"), metrics.get("fdv_usd"), metrics.get("buys_h1"),
                metrics.get("sells_h1"), risk.get("holder_count"), risk.get("top10_pct"),
                risk.get("top_holder_pct"), risk.get("cluster_pct"), score, tier, status,
                json.dumps(features), json.dumps(checks), json.dumps(raw_report) if (raw_report and status == "PASS") else None,
            ),
        )


def create_alert_if_eligible(metrics: dict[str, Any], risk: dict[str, Any], score: int,
                             tier: str, status: str, features: dict[str, Any],
                             checks: dict[str, Any] | None = None) -> bool:
    if status != "PASS" or tier not in {"WATCHLIST", "STRONG"}:
        return False
    advisories = [n for n, c in (checks or {}).items() if c.get("status") == "ADVISORY"]
    reason = "All required hard checks passed; score threshold reached."
    if advisories:
        reason += " Not verified (advisory only): " + ", ".join(advisories) + "."
    now = utc_now()
    cooldown = int(CFG["alerts"]["cooldown_minutes"])
    daily_cap = int(CFG["alerts"]["daily_cap"])
    with db_connect() as conn:
        last = conn.execute(
            "SELECT created_at FROM alerts WHERE token_address=? ORDER BY created_at DESC LIMIT 1",
            (metrics["token_address"],),
        ).fetchone()
        if last and parse_time(last["created_at"]) and now - parse_time(last["created_at"]) < timedelta(minutes=cooldown):
            return False
        day_start = now.replace(hour=0, minute=0, second=0, microsecond=0).isoformat()
        count = conn.execute("SELECT COUNT(*) FROM alerts WHERE created_at >= ?", (day_start,)).fetchone()[0]
        if count >= daily_cap:
            return False
        conn.execute(
            "INSERT INTO alerts (created_at, token_address, symbol, tier, score, price_at_alert, pair_url, reason, features_json) VALUES (?,?,?,?,?,?,?,?,?)",
            (now.isoformat(timespec="seconds"), metrics["token_address"], metrics["symbol"], tier,
             score, metrics.get("price_usd"), metrics.get("pair_url"),
             reason, json.dumps(features)),
        )
        return True


# A horizon is recorded only if a scan lands inside its window. A scan that first sees an old alert
# 30 hours later must not be stored as that alert's "1 hour" price.
OUTCOME_WINDOWS = (
    ("1h", timedelta(hours=1), timedelta(hours=3)),
    ("24h", timedelta(hours=24), timedelta(hours=30)),
    ("7d", timedelta(days=7), timedelta(days=7, hours=12)),
)


def update_alert_outcomes(token_address: str, current_price: float | None) -> None:
    if current_price is None or current_price <= 0:
        return
    now = utc_now()
    with db_connect() as conn:
        alerts = conn.execute(
            "SELECT * FROM alerts WHERE token_address=? AND (price_1h IS NULL OR price_24h IS NULL OR price_7d IS NULL)",
            (token_address,),
        ).fetchall()
        for alert in alerts:
            created = parse_time(alert["created_at"])
            base = alert["price_at_alert"]
            if not created or not base or base <= 0:
                continue
            elapsed = now - created
            updates: dict[str, Any] = {}
            for label, due, latest in OUTCOME_WINDOWS:
                if alert[f"price_{label}"] is None and due <= elapsed <= latest:
                    updates[f"price_{label}"] = current_price
                    updates[f"return_{label}_pct"] = (current_price / base - 1) * 100
            if updates:
                sql = ", ".join(f"{k}=?" for k in updates)
                conn.execute(f"UPDATE alerts SET {sql} WHERE id=?", (*updates.values(), alert["id"]))


def refresh_alert_outcomes(already_seen: set[str]) -> int:
    """Price-only refresh (one DEX Screener call each) for alerted tokens whose 1h/24h/7d window is open now."""
    now = utc_now()
    with db_connect() as conn:
        rows = conn.execute(
            "SELECT token_address, created_at, price_1h, price_24h, price_7d FROM alerts WHERE created_at >= ?",
            ((now - timedelta(days=8)).isoformat(timespec="seconds"),),
        ).fetchall()
    due_tokens: list[str] = []
    for r in rows:
        created = parse_time(r["created_at"])
        addr = r["token_address"]
        if not created or addr.lower() in already_seen or addr in due_tokens:
            continue
        elapsed = now - created
        if any(r[f"price_{label}"] is None and due <= elapsed <= latest for label, due, latest in OUTCOME_WINDOWS):
            due_tokens.append(addr)
    refreshed = 0
    for addr in due_tokens:
        pair = get_best_pair(addr)
        price = to_float(pair.get("priceUsd")) if pair else None
        if price:
            update_alert_outcomes(addr, price)
            refreshed += 1
    return refreshed


def log_exit_once(holding: sqlite3.Row | dict[str, Any], rule: str, price: float, details: str) -> bool:
    try:
        with db_connect() as conn:
            conn.execute(
                "INSERT INTO exit_alerts (created_at, holding_id, token_address, symbol, rule, observed_price, entry_price, details) VALUES (?,?,?,?,?,?,?,?)",
                (iso_now(), holding["id"], holding["token_address"], holding["symbol"], rule,
                 price, holding["entry_price"], details),
            )
        return True
    except sqlite3.IntegrityError:
        return False


def check_holding_exits(metrics: dict[str, Any], previous: dict[str, Any] | None = None) -> list[dict[str, Any]]:
    price = metrics.get("price_usd")
    if not price or price <= 0:
        return []
    out: list[dict[str, Any]] = []
    with db_connect() as conn:
        holdings = conn.execute("SELECT * FROM holdings WHERE active=1 AND token_address=?", (metrics["token_address"],)).fetchall()
    for h in holdings:
        entry = float(h["entry_price"])
        if entry <= 0:
            continue
        rules = []
        stop_level = entry * (1 - float(CFG["exits"]["stop_loss_pct"]) / 100)
        if price <= stop_level:
            rules.append((f"STOP_LOSS_{CFG['exits']['stop_loss_pct']}PCT", f"Price crossed stop-loss reference ${stop_level:.10g}"))
        for mult in CFG["exits"]["take_profit_multiples"]:
            if price >= entry * float(mult):
                rules.append((f"TAKE_PROFIT_{mult}X", f"Price reached {mult}× entry reference"))
        if previous and previous.get("liquidity_usd"):
            previous_liq = float(previous["liquidity_usd"])
            current_liq = float(metrics.get("liquidity_usd") or 0)
            if previous_liq > 0 and current_liq <= previous_liq * (1 - float(CFG["exits"]["rug_liquidity_drop_pct"]) / 100):
                rules.append((f"LIQUIDITY_DROP_{CFG['exits']['rug_liquidity_drop_pct']}PCT", "Liquidity dropped sharply versus the previous snapshot"))
        for rule, detail in rules:
            if log_exit_once(h, rule, price, detail):
                out.append({"symbol": h["symbol"], "rule": rule, "price": price, "details": detail})
    return out


def scan_once() -> tuple[pd.DataFrame, list[dict[str, Any]], list[dict[str, Any]], str]:
    max_count = int(CFG["alerts"]["max_candidates_per_scan"])
    candidates = discover_candidates(max_count * 2)
    if not candidates:
        return pd.DataFrame(), [], [], "No candidates returned by public discovery feeds."

    # Keep recently alerted/held tokens in the scan loop to improve outcome and exit tracking.
    with db_connect() as conn:
        tracked_alerts = conn.execute(
            "SELECT token_address, symbol, pair_url FROM alerts GROUP BY token_address ORDER BY MAX(created_at) DESC LIMIT ?",
            (int(CFG["alerts"]["max_tracked_tokens"]),),
        ).fetchall()
        tracked_holdings = conn.execute("SELECT DISTINCT token_address, symbol FROM holdings WHERE active=1").fetchall()
    by_address = {c["token_address"].lower(): c for c in candidates}
    for row in list(tracked_alerts) + list(tracked_holdings):
        address = row["token_address"]
        if address.lower() not in by_address:
            by_address[address.lower()] = {"token_address": address, "profile_url": row["pair_url"] if "pair_url" in row.keys() else None,
                                           "profile_links": [], "boosted": False, "tracked": True}
    candidates = list(by_address.values())[: max_count + int(CFG["alerts"]["max_tracked_tokens"])]

    # First obtain DEX market data. Only base-token prices are accepted.
    paired: list[tuple[dict[str, Any], dict[str, Any]]] = []
    for candidate in candidates:
        pair = get_best_pair(candidate["token_address"])
        if pair:
            paired.append((candidate, pair))

    # Fetch risk reports and public RPC authority checks in a small worker pool.
    work: list[tuple[dict[str, Any], dict[str, Any], dict[str, Any] | None, dict[str, Any]]] = []
    def get_risk(pair_metrics_value: dict[str, Any]) -> tuple[dict[str, Any] | None, dict[str, Any]]:
        address = pair_metrics_value["token_address"]
        raw = rugcheck_report(address)
        try:
            rpc = solana_mint_authorities(address)
        except (requests.RequestException, ValueError, KeyError, TypeError):
            rpc = {"mint_authority_revoked": None, "freeze_authority_revoked": None, "rpc_status": "rpc_error"}
        return raw, rpc

    max_to_process = max_count + int(CFG["alerts"]["max_tracked_tokens"])
    paired = paired[:max_to_process]
    with ThreadPoolExecutor(max_workers=4) as pool:
        futures = {}
        for candidate, pair in paired:
            m = pair_metrics(pair, candidate)
            futures[pool.submit(get_risk, m)] = (candidate, m, pair)
        for future in as_completed(futures):
            candidate, m, pair = futures[future]
            try:
                raw, rpc = future.result()
            except Exception:
                raw, rpc = None, {"mint_authority_revoked": None, "freeze_authority_revoked": None, "rpc_status": "unexpected_error"}
            work.append((candidate, m, raw, rpc))

    rows: list[dict[str, Any]] = []
    exits: list[dict[str, Any]] = []
    new_alerts: list[dict[str, Any]] = []
    for candidate, metrics, raw_report, rpc in work:
        previous_snapshot = previous_observation(metrics["token_address"], older_than_minutes=0)
        risk = extract_risk_fields(raw_report, metrics.get("pair_address"))
        checks = hard_checks(metrics, risk, rpc)
        score, features = score_candidate(metrics, risk, metrics["token_address"])
        status, tier = evaluate_status(checks, score)
        log_observation(metrics, risk, score, tier, status, features, checks, raw_report)
        update_alert_outcomes(metrics["token_address"], metrics.get("price_usd"))
        exits.extend(check_holding_exits(metrics, previous_snapshot))
        if create_alert_if_eligible(metrics, risk, score, tier, status, features, checks):
            new_alerts.append({"symbol": metrics["symbol"], "tier": tier, "score": score,
                               "price_usd": metrics.get("price_usd"), "url": metrics.get("pair_url"),
                               "token_address": metrics["token_address"]})
        unknowns = [name for name, check in checks.items() if check["status"] == "UNKNOWN"]
        advisories = [name for name, check in checks.items() if check["status"] == "ADVISORY"]
        rejects = [name for name, check in checks.items() if check["status"] == "REJECT"]
        rows.append({
            "symbol": metrics["symbol"], "name": metrics["name"], "score": score, "tier": tier,
            "hard_status": status, "price_usd": metrics.get("price_usd"),
            "liquidity_usd": metrics.get("liquidity_usd"), "volume_h1": metrics.get("volume_h1"),
            "volume_h24": metrics.get("volume_h24"), "pair_age_min": metrics.get("pair_age_minutes"),
            "holders": risk.get("holder_count"), "top10_pct": risk.get("top10_pct"),
            "top_holder_pct": risk.get("top_holder_pct"), "cluster_pct": risk.get("cluster_pct"),
            "unknown_checks": ", ".join(unknowns), "advisory_checks": ", ".join(advisories), "rejected_checks": ", ".join(rejects),
            "token_address": metrics["token_address"], "url": metrics.get("pair_url"),
            "features": features, "checks": checks, "risk_names": risk.get("risk_names", []),
            "boosted": metrics.get("boosted", False), "risk_level": risk.get("risk_level", "unknown"),
        })
    refreshed = refresh_alert_outcomes({m["token_address"].lower() for _, m, _, _ in work})
    frame = pd.DataFrame(rows)
    if not frame.empty:
        frame.sort_values(["hard_status", "score"], ascending=[True, False], inplace=True)
    return frame, new_alerts, exits, f"Checked {len(rows)} pairs; refreshed outcome prices for {refreshed} older alerted tokens; saved local snapshots to {DB_PATH}."


init_db()
st.set_page_config(page_title="Solana Meme Research Lab", page_icon="🔎", layout="wide")
st.title("Solana Meme Research Lab")
st.caption("Read-only research dashboard • local SQLite • no wallet connection, keys, swaps or automated trading")
st.warning(
    "Research prototype: DEX Screener discovery feeds are a sample, not every launch. Unknown critical checks block Watchlist/Strong alerts. "
    "Provider reports can be incomplete or stale; this is not a guarantee of token safety."
)

with st.sidebar:
    st.subheader("Current starting thresholds")
    st.write(f"Minimum liquidity: **${CFG['hard_filters']['min_liquidity_usd']:,}**")
    st.write(f"Pair age: **{CFG['hard_filters']['pair_age_minutes'][0]}–{CFG['hard_filters']['pair_age_minutes'][1]} min**")
    st.write(f"Top 10: **≤{CFG['hard_filters']['max_top10_holders_pct']}%**")
    st.write(f"Single holder: **≤{CFG['hard_filters']['max_single_wallet_pct']}%**")
    st.write(f"Cluster: **≤{CFG['hard_filters']['max_cluster_pct']}%**")
    st.write(f"LP locked/burned: **≥{CFG['hard_filters']['min_lp_locked_or_burned_pct']}%**")
    st.caption("Edit config.yaml to change these. The values are guesses awaiting logged evidence.")
    scan_btn = st.button("Scan now", type="primary", use_container_width=True)
    st.caption(f"Database: `{DB_PATH}`")

if scan_btn:
    with st.spinner("Reading public market and risk data…"):
        try:
            frame, generated_alerts, generated_exits, message = scan_once()
            st.session_state["last_scan_frame"] = frame
            st.session_state["last_scan_message"] = message
            st.session_state["last_scan_time"] = iso_now()
            st.session_state["last_generated_alerts"] = generated_alerts
            st.session_state["last_generated_exits"] = generated_exits
        except Exception as exc:
            st.error(f"Scan failed: {type(exc).__name__}: {exc}")

if st.session_state.get("last_scan_message"):
    st.caption(f"Last scan: {st.session_state.get('last_scan_time', 'unknown')} UTC — {st.session_state['last_scan_message']}")
    for a in st.session_state.get("last_generated_alerts", []):
        st.success(f"{a['tier']} alert: {a['symbol']} — score {a['score']}/100 — price {a['price_usd']}")
    for e in st.session_state.get("last_generated_exits", []):
        st.error(f"Exit reference: {e['symbol']} — {e['rule']} — {e['details']}")

frame = st.session_state.get("last_scan_frame", pd.DataFrame())
if isinstance(frame, pd.DataFrame) and not frame.empty:
    st.subheader("Latest scan")
    display_cols = ["symbol", "name", "score", "tier", "hard_status", "price_usd", "liquidity_usd", "volume_h1", "volume_h24", "pair_age_min", "holders", "top10_pct", "top_holder_pct", "cluster_pct", "unknown_checks", "advisory_checks", "rejected_checks"]
    st.dataframe(frame[display_cols], use_container_width=True, hide_index=True)
    st.download_button("Download latest scan CSV", frame[display_cols].to_csv(index=False).encode(), "latest_scan.csv", "text/csv")
    with st.expander("Inspect checks and score features"):
        token_pick = st.selectbox("Token", frame["token_address"].tolist(), format_func=lambda a: f"{frame.loc[frame['token_address'] == a, 'symbol'].iloc[0]} — {a[:8]}…")
        record = frame.loc[frame["token_address"] == token_pick].iloc[0]
        st.write("Hard checks")
        st.json(record["checks"])
        st.write("Score components")
        st.json(record["features"])
        st.write("Provider-listed risks")
        st.write(record["risk_names"])
        if record.get("url"):
            st.markdown(f"[Open pair in DEX Screener]({record['url']})")

st.divider()
st.subheader("Alert outcomes — evidence for tuning")
with db_connect() as conn:
    alerts_df = pd.read_sql_query(
        "SELECT created_at, symbol, token_address, tier, score, price_at_alert, return_1h_pct, return_24h_pct, return_7d_pct, pair_url FROM alerts ORDER BY created_at DESC LIMIT 200",
        conn,
    )
    obs_count = conn.execute("SELECT COUNT(*) FROM observations").fetchone()[0]
    alert_count = conn.execute("SELECT COUNT(*) FROM alerts").fetchone()[0]
    holding_count = conn.execute("SELECT COUNT(*) FROM holdings WHERE active=1").fetchone()[0]
col1, col2, col3 = st.columns(3)
col1.metric("Logged snapshots", obs_count)
col2.metric("Valid tier alerts", alert_count)
col3.metric("Active tracked holdings", holding_count)
if not alerts_df.empty:
    st.dataframe(alerts_df, use_container_width=True, hide_index=True)
    st.download_button("Download alert log CSV", alerts_df.to_csv(index=False).encode(), "alert_log.csv", "text/csv")
else:
    st.info("No eligible alerts logged yet. Incomplete safety data intentionally prevents Watchlist/Strong alerts.")

st.divider()
st.subheader("Track a holding for local exit-reference alerts")
st.caption("This only records entry references. It never connects to a wallet or sends a transaction. Tokens must be discovered/tracked by later scans for prices to update.")
with st.form("holding_form", clear_on_submit=True):
    h_address = st.text_input("Solana token mint address")
    h_symbol = st.text_input("Symbol / label (optional)")
    c1, c2 = st.columns(2)
    h_entry = c1.number_input("Entry price in USD", min_value=0.0000000001, format="%.10f")
    h_qty = c2.number_input("Token quantity (optional)", min_value=0.0, format="%.8f")
    add_holding = st.form_submit_button("Add holding reference")
if add_holding:
    if len(h_address.strip()) < 30:
        st.error("That mint address looks too short. Double-check it before saving.")
    elif h_entry <= 0:
        st.error("Entry price must be greater than zero.")
    else:
        with db_connect() as conn:
            conn.execute("INSERT INTO holdings (token_address, symbol, entry_price, quantity, created_at) VALUES (?,?,?,?,?)",
                         (h_address.strip(), h_symbol.strip() or "UNKNOWN", float(h_entry), float(h_qty) or None, iso_now()))
        st.success("Holding reference saved locally. It will be checked when this token is included in a scan.")
with db_connect() as conn:
    holdings_df = pd.read_sql_query("SELECT id, token_address, symbol, entry_price, quantity, created_at FROM holdings WHERE active=1 ORDER BY created_at DESC", conn)
    exits_df = pd.read_sql_query("SELECT created_at, symbol, rule, observed_price, entry_price, details FROM exit_alerts ORDER BY created_at DESC LIMIT 100", conn)
if not holdings_df.empty:
    st.write("Active holding references")
    st.dataframe(holdings_df, use_container_width=True, hide_index=True)
    remove_id = st.number_input("Holding ID to deactivate", min_value=0, step=1, value=0)
    if st.button("Deactivate holding ID") and remove_id > 0:
        with db_connect() as conn:
            conn.execute("UPDATE holdings SET active=0 WHERE id=?", (int(remove_id),))
        st.success("Holding reference deactivated. It was not sold; no transaction was made.")
if not exits_df.empty:
    st.write("Exit-reference log")
    st.dataframe(exits_df, use_container_width=True, hide_index=True)

st.divider()
st.subheader("What this version does not yet prove")
st.markdown(
    "- It does **not** identify every new launch; it samples DEX Screener's latest-profile and boost feeds.\n"
    "- Buy/sell transaction counts are not unique-wallet counts and do not prove organic demand.\n"
    "- Price-pullback scoring is based only on local 6-hour snapshots, so early scans initially receive no points for this feature.\n"
    "- Holder growth scores only after repeated RugCheck snapshots expose a holder count.\n"
    "- Cluster/funder data and deployer history may be unavailable; unknown critical checks suppress alerts.\n"
    "- LP lock percentage may not establish lock expiry or exclude every pool/vault correctly. Review the linked provider report before acting.\n"
    "- Developer-wallet sell detection is not implemented as an authoritative signal.\n"
    "- Take-profit and stop-loss messages are price references, not trade instructions or guaranteed executable prices."
)
