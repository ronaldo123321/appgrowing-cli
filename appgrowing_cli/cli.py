"""Click entrypoint for appgrowing CLI."""

from __future__ import annotations

import csv
import json
import math
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Any

import click

from appgrowing_cli.api_adapter import AppGrowingAPIError, AppGrowingClient
from appgrowing_cli.auth_store import extract_browser_auth, load_auth, save_auth
from appgrowing_cli.schema import validate_payload
from appgrowing_cli.utils import load_json_file, utc_now_iso, write_json_file, write_text_file


def _emit(ctx: click.Context, payload: dict[str, Any]) -> None:
    as_json = bool(ctx.obj.get("json_output", True))
    if as_json:
        click.echo(json.dumps(payload, ensure_ascii=False, indent=2))
        return
    click.echo(payload)


def _validate_if_enabled(
    ctx: click.Context,
    schema_file: str,
    payload: dict[str, Any],
) -> None:
    if not ctx.obj.get("validate", True):
        return
    validate_payload(schema_file, payload)


def _api_client_from_ctx(ctx: click.Context) -> AppGrowingClient:
    return ctx.obj["api_client"]


def _build_snapshot_payload_from_api(
    *,
    keyword: str,
    start: str,
    end: str,
    markets: tuple[str, ...],
    client: AppGrowingClient,
    pages: int = 1,
) -> dict[str, Any]:
    items: list[dict[str, Any]] = []
    try:
        top_items = client.promote_app_list_multi_page(
            keyword=keyword,
            start_date=start,
            end_date=end,
            markets=list(markets),
            pages=pages,
        )
        for idx, row in enumerate(top_items, start=1):
            app_brand = row.get("appBrand") if isinstance(row, dict) else {}
            if not isinstance(app_brand, dict):
                app_brand = {}
            adverts = row.get("adverts") if isinstance(row, dict) else 0
            adverts_int = client.parse_int_maybe(adverts)
            material_cnt = row.get("material_cnt") if isinstance(row, dict) else 0
            material_cnt_int = client.parse_int_maybe(material_cnt)
            app_id = app_brand.get("id") or f"unknown_{idx}"
            app_name = app_brand.get("name") or f"Unknown {idx}"
            items.append(
                {
                    "rank": idx,
                    "app_id": str(app_id),
                    "app_name": str(app_name),
                    "material_count": material_cnt_int,
                    "ad_count": adverts_int,
                }
            )
    except AppGrowingAPIError:
        search_items = client.search_app_multi_page(keyword=keyword, purpose=2, pages=pages)
        for idx, row in enumerate(search_items, start=1):
            app_brand = row.get("appBrand") if isinstance(row, dict) else {}
            if not isinstance(app_brand, dict):
                app_brand = {}
            had_advert = 1 if bool(row.get("hadAdvert")) else 0
            app_id = app_brand.get("id") or app_brand.get("app_id") or f"unknown_{idx}"
            app_name = app_brand.get("name") or f"Unknown {idx}"
            items.append(
                {
                    "rank": idx,
                    "app_id": str(app_id),
                    "app_name": str(app_name),
                    "material_count": had_advert,
                    "ad_count": had_advert,
                }
            )
    return {
        "ok": True,
        "data_source": "api",
        "keyword": keyword,
        "period": {"start": start, "end": end},
        "market": list(markets),
        "items": items,
        "generated_at": utc_now_iso(),
    }


def _appgrowing_app_link(app_id: str, purpose: int = 2) -> str:
    return f"https://appgrowing-global.youcloud.com/appBrand/{app_id}/overview?purpose={purpose}"


def _format_platform_label(platforms: list[str]) -> str:
    cleaned = [p for p in platforms if p]
    return "|".join(cleaned) if cleaned else "unknown"


def _ratio_change(current: int, previous: int) -> float | None:
    if previous <= 0:
        return None
    return round((current - previous) / previous, 6)


def _parse_date(value: str) -> date:
    return datetime.strptime(value, "%Y-%m-%d").date()


def _infer_previous_period(start: str, end: str) -> tuple[str, str]:
    start_dt = _parse_date(start)
    end_dt = _parse_date(end)
    days = (end_dt - start_dt).days + 1
    if days <= 0:
        raise click.ClickException("Invalid period: end must be on or after start.")
    previous_end = start_dt - timedelta(days=1)
    previous_start = previous_end - timedelta(days=days - 1)
    return previous_start.isoformat(), previous_end.isoformat()


def _build_ranking_business_snapshot_for_keyword(
    *,
    client: AppGrowingClient,
    keyword: str,
    this_start: str,
    this_end: str,
    last_start: str,
    last_end: str,
    markets: tuple[str, ...],
    pages: int,
    accurate_search: int = 0,
    order: str = "material_cnt_desc",
) -> dict[str, Any]:
    current_rows = client.promote_app_list_multi_page(
        keyword=keyword,
        start_date=this_start,
        end_date=this_end,
        markets=list(markets),
        pages=pages,
        purpose=2,
        order=order,
        accurate_search=accurate_search,
    )
    previous_rows = client.promote_app_list_multi_page(
        keyword=keyword,
        start_date=last_start,
        end_date=last_end,
        markets=list(markets),
        pages=pages,
        purpose=2,
        order=order,
        accurate_search=accurate_search,
    )
    previous_by_app: dict[str, dict[str, Any]] = {}
    for idx, row in enumerate(previous_rows, start=1):
        if not isinstance(row, dict):
            continue
        app_brand = row.get("appBrand") if isinstance(row.get("appBrand"), dict) else {}
        app_id = str(app_brand.get("id") or f"unknown_prev_{idx}")
        previous_by_app[app_id] = {
            "rank": idx,
            "ad_count": client.parse_int_maybe(row.get("adverts")),
            "material_count": client.parse_int_maybe(row.get("material_cnt")),
        }

    items: list[dict[str, Any]] = []
    for idx, row in enumerate(current_rows, start=1):
        if not isinstance(row, dict):
            continue
        app_brand = row.get("appBrand") if isinstance(row.get("appBrand"), dict) else {}
        app_id = str(app_brand.get("id") or f"unknown_{idx}")
        app_name = str(app_brand.get("name") or f"Unknown {idx}")
        platforms = _publish_platforms_from_app_brand(app_brand)
        ad_count = client.parse_int_maybe(row.get("adverts"))
        material_count = client.parse_int_maybe(row.get("material_cnt"))
        prev = previous_by_app.get(app_id)
        previous_rank = prev.get("rank") if isinstance(prev, dict) else None
        previous_ad_count = prev.get("ad_count", 0) if isinstance(prev, dict) else 0
        previous_material_count = prev.get("material_count", 0) if isinstance(prev, dict) else 0
        rank_change = None
        if isinstance(previous_rank, int):
            rank_change = previous_rank - idx
        items.append(
            {
                "app_id": app_id,
                "product_name": app_name,
                "appgrowing_link": _appgrowing_app_link(app_id, purpose=2),
                "system_platform": _format_platform_label(platforms),
                "current_rank": idx,
                "previous_rank": previous_rank,
                "rank_change": rank_change,
                "ad_count_current": ad_count,
                "ad_count_previous": int(previous_ad_count),
                "ad_count_change_ratio": _ratio_change(ad_count, int(previous_ad_count)),
                "material_count_current": material_count,
                "material_count_previous": int(previous_material_count),
                "material_count_change_ratio": _ratio_change(material_count, int(previous_material_count)),
            }
        )
    return {
        "ok": True,
        "data_source": "api",
        "keyword": keyword,
        "this_period": {"start": this_start, "end": this_end},
        "last_period": {"start": last_start, "end": last_end},
        "market": list(markets),
        "items": items,
        "generated_at": utc_now_iso(),
    }


def _build_channel_distribution_from_promote_rows(
    *,
    rows: list[dict[str, Any]],
    client: AppGrowingClient,
) -> list[dict[str, Any]]:
    counts: dict[str, int] = {}
    for row in rows:
        if not isinstance(row, dict):
            continue
        adverts = client.parse_int_maybe(row.get("adverts"))
        media_list = row.get("media")
        if not isinstance(media_list, list):
            continue
        for media in media_list:
            if not isinstance(media, dict):
                continue
            name = str(media.get("name") or "unknown")
            counts[name] = counts.get(name, 0) + adverts
    total = sum(counts.values())
    result = []
    for name, value in sorted(counts.items(), key=lambda x: x[1], reverse=True):
        ratio = round(value / total, 6) if total > 0 else None
        result.append({"channel": name, "ad_count": value, "ratio": ratio})
    return result


def _build_region_distribution_from_promote_rows(
    *,
    rows: list[dict[str, Any]],
    client: AppGrowingClient,
    top_n: int = 10,
) -> list[dict[str, Any]]:
    counts: dict[str, int] = {}
    for row in rows:
        if not isinstance(row, dict):
            continue
        adverts = client.parse_int_maybe(row.get("adverts"))
        app_brand = row.get("appBrand")
        if not isinstance(app_brand, dict):
            continue
        developer = app_brand.get("developer")
        if not isinstance(developer, dict):
            continue
        area = developer.get("area")
        if not isinstance(area, dict):
            continue
        region_name = str(area.get("name") or area.get("cc") or "unknown")
        counts[region_name] = counts.get(region_name, 0) + adverts
    total = sum(counts.values())
    result = []
    for region_name, value in sorted(counts.items(), key=lambda x: x[1], reverse=True)[:top_n]:
        ratio = round(value / total, 6) if total > 0 else None
        result.append({"region": region_name, "ad_count": value, "ratio": ratio})
    return result


def _build_language_distribution_from_creative(
    *,
    creative_items: list[dict[str, Any]],
    top_n: int = 10,
) -> list[dict[str, Any]]:
    counts: dict[str, int] = {}
    for row in creative_items:
        if not isinstance(row, dict):
            continue
        language = str(row.get("language") or "unknown")
        counts[language] = counts.get(language, 0) + 1
    total = sum(counts.values())
    result = []
    for language, value in sorted(counts.items(), key=lambda x: x[1], reverse=True)[:top_n]:
        ratio = round(value / total, 6) if total > 0 else None
        result.append({"language": language, "count": value, "ratio": ratio})
    return result


def _build_size_top_from_creative(
    *,
    creative_items: list[dict[str, Any]],
    material_type: str,
    top_n: int = 10,
) -> list[dict[str, Any]]:
    counts: dict[str, int] = {}
    for row in creative_items:
        if not isinstance(row, dict):
            continue
        row_material_type = str(row.get("material_type") or "")
        if row_material_type != material_type:
            continue
        size = str(row.get("size") or "unknown")
        counts[size] = counts.get(size, 0) + 1
    total = sum(counts.values())
    result = []
    for size, value in sorted(counts.items(), key=lambda x: x[1], reverse=True)[:top_n]:
        ratio = round(value / total, 6) if total > 0 else None
        result.append({"size": size, "count": value, "ratio": ratio})
    return result


def _publish_platforms_from_app_brand(app_brand: dict[str, Any]) -> list[str]:
    platforms: list[str] = []
    app_id = str(app_brand.get("app_id") or "").strip()
    bundle_id = str(app_brand.get("bundle_id") or "").strip()
    if app_id:
        platforms.append("ios")
    if bundle_id:
        platforms.append("android")
    if not platforms:
        platforms.append("unknown")
    return platforms


def _map_promote_row(
    *,
    row: dict[str, Any],
    rank: int,
    client: AppGrowingClient,
) -> dict[str, Any]:
    app_brand = row.get("appBrand") if isinstance(row.get("appBrand"), dict) else {}
    developer = app_brand.get("developer") if isinstance(app_brand.get("developer"), dict) else {}
    area = developer.get("area") if isinstance(developer.get("area"), dict) else {}
    media_rows = row.get("media") if isinstance(row.get("media"), list) else []
    media_info = []
    for media in media_rows:
        if not isinstance(media, dict):
            continue
        media_info.append(
            {
                "id": str(media.get("id") or ""),
                "name": str(media.get("name") or "unknown"),
            }
        )
    return {
        "rank": rank,
        "app_id": str(app_brand.get("id") or f"unknown_{rank}"),
        "app_name": str(app_brand.get("name") or f"Unknown {rank}"),
        "country": str(area.get("name") or area.get("cc") or "unknown"),
        "publish_platform": _publish_platforms_from_app_brand(app_brand),
        "media_info": media_info,
        "ad_count": client.parse_int_maybe(row.get("adverts")),
        "material_count": client.parse_int_maybe(row.get("material_cnt")),
        "video_fragment_count": client.parse_int_maybe(row.get("duration")),
    }


def _build_promote_ranking_snapshot_from_api(
    *,
    keyword: str,
    start: str,
    end: str,
    markets: tuple[str, ...],
    client: AppGrowingClient,
    pages: int,
    accurate_search: int,
    order: str,
    purpose: int,
) -> dict[str, Any]:
    rows = client.promote_app_list_multi_page(
        keyword=keyword,
        start_date=start,
        end_date=end,
        markets=list(markets),
        pages=pages,
        accurate_search=accurate_search,
        order=order,
        purpose=purpose,
    )
    items = [_map_promote_row(row=row, rank=idx, client=client) for idx, row in enumerate(rows, start=1)]
    return {
        "ok": True,
        "data_source": "api",
        "keyword": keyword,
        "period": {"start": start, "end": end},
        "market": list(markets),
        "accurate_search": accurate_search,
        "order": order,
        "purpose": purpose,
        "items": items,
        "generated_at": utc_now_iso(),
    }


def _build_promote_ranking_compare_from_snapshots(
    *,
    keyword: str,
    this_start: str,
    this_end: str,
    last_start: str,
    last_end: str,
    current_items: list[dict[str, Any]],
    previous_items: list[dict[str, Any]],
) -> dict[str, Any]:
    curr_map = {str(item["app_id"]): item for item in current_items}
    prev_map = {str(item["app_id"]): item for item in previous_items}
    merged: list[dict[str, Any]] = []
    for app_id in sorted(set(curr_map.keys()) | set(prev_map.keys())):
        curr = curr_map.get(app_id)
        prev = prev_map.get(app_id)
        app_name = str((curr or prev or {}).get("app_name", "unknown"))
        country = str((curr or prev or {}).get("country", "unknown"))
        publish_platform = (curr or prev or {}).get("publish_platform", ["unknown"])
        media_info = (curr or prev or {}).get("media_info", [])
        current_rank = curr.get("rank") if curr else None
        previous_rank = prev.get("rank") if prev else None
        rank_change = None
        if isinstance(previous_rank, int) and isinstance(current_rank, int):
            rank_change = previous_rank - current_rank
        material_count_current = int(curr.get("material_count", 0)) if curr else 0
        material_count_previous = int(prev.get("material_count", 0)) if prev else 0
        ad_count_current = int(curr.get("ad_count", 0)) if curr else 0
        ad_count_previous = int(prev.get("ad_count", 0)) if prev else 0
        video_fragment_count_current = int(curr.get("video_fragment_count", 0)) if curr else 0
        video_fragment_count_previous = int(prev.get("video_fragment_count", 0)) if prev else 0
        merged.append(
            {
                "app_id": app_id,
                "app_name": app_name,
                "country": country,
                "publish_platform": publish_platform,
                "media_info": media_info,
                "current_rank": current_rank,
                "previous_rank": previous_rank,
                "rank_change": rank_change,
                "ad_count_current": ad_count_current,
                "ad_count_previous": ad_count_previous,
                "ad_count_change": ad_count_current - ad_count_previous,
                "material_count_current": material_count_current,
                "material_count_previous": material_count_previous,
                "material_count_change": material_count_current - material_count_previous,
                "video_fragment_count_current": video_fragment_count_current,
                "video_fragment_count_previous": video_fragment_count_previous,
                "video_fragment_count_change": video_fragment_count_current - video_fragment_count_previous,
            }
        )
    merged.sort(
        key=lambda x: (
            x["current_rank"] if isinstance(x["current_rank"], int) else 9999,
            x["previous_rank"] if isinstance(x["previous_rank"], int) else 9999,
            x["app_id"],
        )
    )
    return {
        "ok": True,
        "data_source": "api",
        "keyword": keyword,
        "this_period": {"start": this_start, "end": this_end},
        "last_period": {"start": last_start, "end": last_end},
        "items": merged,
        "generated_at": utc_now_iso(),
    }


def _media_names(media_info: list[dict[str, Any]]) -> str:
    names: list[str] = []
    for row in media_info:
        if not isinstance(row, dict):
            continue
        name = str(row.get("name") or "").strip()
        if name:
            names.append(name)
    return "|".join(names)


def _platform_names(publish_platform: list[str]) -> str:
    cleaned = [str(x).strip() for x in publish_platform if str(x).strip()]
    return "|".join(cleaned)


def _write_csv_rows(file_path: Path, fieldnames: list[str], rows: list[dict[str, Any]]) -> None:
    file_path.parent.mkdir(parents=True, exist_ok=True)
    with file_path.open("w", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        for row in rows:
            writer.writerow(row)


def _promote_snapshot_csv_rows(items: list[dict[str, Any]]) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for item in items:
        if not isinstance(item, dict):
            continue
        rows.append(
            {
                "rank": item.get("rank"),
                "app_id": item.get("app_id"),
                "app_name": item.get("app_name"),
                "country": item.get("country"),
                "publish_platform": _platform_names(item.get("publish_platform", [])),
                "media_info": _media_names(item.get("media_info", [])),
                "ad_count": item.get("ad_count"),
                "material_count": item.get("material_count"),
                "video_fragment_count": item.get("video_fragment_count"),
            }
        )
    return rows


def _promote_compare_csv_rows(items: list[dict[str, Any]]) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for item in items:
        if not isinstance(item, dict):
            continue
        rows.append(
            {
                "app_id": item.get("app_id"),
                "app_name": item.get("app_name"),
                "country": item.get("country"),
                "publish_platform": _platform_names(item.get("publish_platform", [])),
                "media_info": _media_names(item.get("media_info", [])),
                "current_rank": item.get("current_rank"),
                "previous_rank": item.get("previous_rank"),
                "rank_change": item.get("rank_change"),
                "ad_count_current": item.get("ad_count_current"),
                "ad_count_previous": item.get("ad_count_previous"),
                "ad_count_change": item.get("ad_count_change"),
                "material_count_current": item.get("material_count_current"),
                "material_count_previous": item.get("material_count_previous"),
                "material_count_change": item.get("material_count_change"),
                "video_fragment_count_current": item.get("video_fragment_count_current"),
                "video_fragment_count_previous": item.get("video_fragment_count_previous"),
                "video_fragment_count_change": item.get("video_fragment_count_change"),
            }
        )
    return rows


def _ranking_business_csv_rows(items: list[dict[str, Any]]) -> list[dict[str, Any]]:
    return _ranking_business_csv_rows_with_context(items=items, keyword="", period_label="每周")


def _period_label_from_range(start: str, end: str, *, previous: bool = False) -> str:
    days = (_parse_date(end) - _parse_date(start)).days + 1
    if days >= 27:
        return "上月" if previous else "每月"
    return "上周" if previous else "每周"


def _signed_int_text(value: int | None) -> str:
    if value is None:
        return "-"
    return f"{value:+d}"


def _signed_percent_text(ratio: float | None) -> str:
    if ratio is None:
        return "-"
    percent = ratio * 100
    text = f"{percent:.2f}".rstrip("0").rstrip(".")
    return f"{text}%"


def _metric_with_change_text(current: Any, ratio: float | None) -> str:
    current_text = "-" if current is None else str(current)
    ratio_text = _signed_percent_text(ratio)
    if ratio_text == "-":
        return current_text
    sign = "+" if not ratio_text.startswith("-") else ""
    return f"{current_text} ({sign}{ratio_text})"


def _ranking_with_change_text(current_rank: Any, rank_change: int | None) -> str:
    current_text = "-" if current_rank is None else str(current_rank)
    change_text = _signed_int_text(rank_change)
    if change_text == "-":
        return current_text
    # Use parentheses to avoid spreadsheet auto-parsing as a date (e.g. "14-5" -> "5月14日").
    return f"{current_text} ({change_text})"


def _ranking_business_csv_rows_with_context(
    *,
    items: list[dict[str, Any]],
    keyword: str,
    period_label: str,
) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for item in items:
        if not isinstance(item, dict):
            continue
        rows.append(
            {
                "赛道关键词": keyword,
                "周期": period_label,
                "产品名称": item.get("product_name"),
                "Appgrowing链接": item.get("appgrowing_link"),
                "系统平台": item.get("system_platform"),
                "排名+变化": _ranking_with_change_text(
                    item.get("current_rank"),
                    item.get("rank_change")
                    if isinstance(item.get("rank_change"), int)
                    else None,
                ),
                "广告数+变化占比": _metric_with_change_text(
                    item.get("ad_count_current"),
                    item.get("ad_count_change_ratio")
                    if isinstance(item.get("ad_count_change_ratio"), (int, float))
                    else None,
                ),
                "素材数+变化占比": _metric_with_change_text(
                    item.get("material_count_current"),
                    item.get("material_count_change_ratio")
                    if isinstance(item.get("material_count_change_ratio"), (int, float))
                    else None,
                ),
            }
        )
    return rows


def _normalize_ratio(value: Any) -> float | None:
    if isinstance(value, (int, float)):
        return round(float(value), 6)
    if isinstance(value, str):
        compact = value.strip().replace("%", "")
        try:
            return round(float(compact), 6)
        except ValueError:
            return None
    return None


def _parse_int_maybe(value: Any) -> int:
    if isinstance(value, int):
        return value
    if isinstance(value, float):
        return int(value)
    if isinstance(value, str):
        compact = value.replace(",", "").strip()
        if compact.isdigit():
            return int(compact)
    return 0


def _build_channel_distribution(media_rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for row in media_rows:
        if not isinstance(row, dict):
            continue
        media = row.get("media") if isinstance(row.get("media"), dict) else {}
        rows.append(
            {
                "channel_id": str(media.get("id") or ""),
                "channel_name": str(media.get("name") or "unknown"),
                "adverts": _parse_int_maybe(row.get("adverts")),
                "material": _parse_int_maybe(row.get("material")),
                "ratio": _normalize_ratio(row.get("percent")),
            }
        )
    rows.sort(key=lambda x: x["material"], reverse=True)
    return rows


def _build_continent_distribution(
    *,
    region_rows: list[dict[str, Any]],
    cc_to_continent: dict[str, str],
) -> list[dict[str, Any]]:
    by_continent: dict[str, dict[str, Any]] = {}
    total_material = 0
    for row in region_rows:
        if not isinstance(row, dict):
            continue
        area = row.get("area") if isinstance(row.get("area"), dict) else {}
        cc = str(area.get("cc") or "").strip().upper()
        continent = cc_to_continent.get(cc, "unknown")
        material = _parse_int_maybe(row.get("material"))
        adverts = _parse_int_maybe(row.get("adverts"))
        total_material += material
        if continent not in by_continent:
            by_continent[continent] = {"continent": continent, "material": 0, "adverts": 0}
        by_continent[continent]["material"] += material
        by_continent[continent]["adverts"] += adverts
    result = []
    for item in by_continent.values():
        ratio = round(item["material"] / total_material, 6) if total_material > 0 else None
        result.append({**item, "ratio": ratio})
    result.sort(key=lambda x: x["material"], reverse=True)
    return result


def _build_language_distribution(language_rows: list[dict[str, Any]], top_n: int) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for row in language_rows:
        if not isinstance(row, dict):
            continue
        language = row.get("language") if isinstance(row.get("language"), dict) else {}
        rows.append(
            {
                "language_code": str(language.get("code") or ""),
                "language_name": str(language.get("name") or "unknown"),
                "adverts": _parse_int_maybe(row.get("adverts")),
                "material": _parse_int_maybe(row.get("material")),
                "ratio": _normalize_ratio(row.get("percent")),
            }
        )
    rows.sort(key=lambda x: x["material"], reverse=True)
    return rows[:top_n]


def _detect_material_kind(material: dict[str, Any]) -> str:
    creative = material.get("creative") if isinstance(material.get("creative"), dict) else {}
    raw_resource = creative.get("resource")
    if isinstance(raw_resource, list):
        resource = raw_resource[0] if raw_resource and isinstance(raw_resource[0], dict) else {}
    elif isinstance(raw_resource, dict):
        resource = raw_resource
    else:
        resource = {}
    fmt = str(resource.get("format") or "").strip().lower()
    duration = _parse_int_maybe(resource.get("duration"))
    material_type = str(material.get("type") or "").strip().lower()
    if duration > 0:
        return "video"
    if fmt in {"mp4", "mov", "webm", "mkv", "avi"}:
        return "video"
    if "video" in material_type:
        return "video"
    return "image"


def _primary_resource(material: dict[str, Any]) -> dict[str, Any]:
    creative = material.get("creative") if isinstance(material.get("creative"), dict) else {}
    raw_resource = creative.get("resource")
    if isinstance(raw_resource, list):
        if raw_resource and isinstance(raw_resource[0], dict):
            return raw_resource[0]
        return {}
    if isinstance(raw_resource, dict):
        return raw_resource
    return {}


def _material_size_key(material: dict[str, Any]) -> str:
    resource = _primary_resource(material)
    width = _parse_int_maybe(resource.get("width"))
    height = _parse_int_maybe(resource.get("height"))
    if width > 0 and height > 0:
        return f"{width}x{height}"
    return "unknown"


def _build_top_material_sizes(rows: list[dict[str, Any]], *, kind: str, top_n: int) -> list[dict[str, Any]]:
    size_counts: dict[str, int] = {}
    for row in rows:
        if not isinstance(row, dict):
            continue
        material = row.get("material") if isinstance(row.get("material"), dict) else {}
        if _detect_material_kind(material) != kind:
            continue
        size = _material_size_key(material)
        size_counts[size] = size_counts.get(size, 0) + 1
    total = sum(size_counts.values())
    result = []
    for size, count in sorted(size_counts.items(), key=lambda x: x[1], reverse=True)[:top_n]:
        ratio = round(count / total, 6) if total > 0 else None
        result.append({"size": size, "count": count, "ratio": ratio})
    return result


def _extract_material_link(material: dict[str, Any], kind: str) -> str | None:
    creative = material.get("creative") if isinstance(material.get("creative"), dict) else {}
    resource = _primary_resource(material)
    path = str(resource.get("path") or "").strip()
    poster = str(resource.get("poster") or "").strip()
    txt_url = str(creative.get("txtUrl") or "").strip()
    if kind == "video":
        return path or txt_url or poster or None
    return path or poster or txt_url or None


def _build_top_material_links(rows: list[dict[str, Any]], *, kind: str, top_n: int) -> list[dict[str, Any]]:
    items: list[dict[str, Any]] = []
    seen: set[str] = set()
    for row in rows:
        if not isinstance(row, dict):
            continue
        material = row.get("material") if isinstance(row.get("material"), dict) else {}
        if _detect_material_kind(material) != kind:
            continue
        material_id = str(material.get("id") or "").strip()
        if not material_id or material_id in seen:
            continue
        link = _extract_material_link(material, kind)
        size = _material_size_key(material)
        resource = _primary_resource(material)
        duration_ms = _parse_int_maybe(resource.get("duration"))
        items.append(
            {
                "material_id": material_id,
                "size": size,
                "duration_ms": duration_ms if duration_ms > 0 else None,
                "link": link,
            }
        )
        seen.add(material_id)
        if len(items) >= top_n:
            break
    return items


def _size_ratio_bucket(material: dict[str, Any]) -> str:
    resource = _primary_resource(material)
    width = _parse_int_maybe(resource.get("width"))
    height = _parse_int_maybe(resource.get("height"))
    if width <= 0 or height <= 0:
        return "unknown"
    ratio = width / height
    if abs(ratio - 1.0) <= 0.05:
        return "1:1"
    if abs(ratio - 0.8) <= 0.05:
        return "4:5"
    if abs(ratio - (9 / 16)) <= 0.05:
        return "9:16"
    return "other"


def _filter_rows_by_kind_and_ratio(rows: list[dict[str, Any]], *, kind: str) -> list[dict[str, Any]]:
    allowed = {"image": {"1:1", "4:5"}, "video": {"9:16"}}
    result: list[dict[str, Any]] = []
    for row in rows:
        if not isinstance(row, dict):
            continue
        material = row.get("material") if isinstance(row.get("material"), dict) else {}
        if _detect_material_kind(material) != kind:
            continue
        ratio_bucket = _size_ratio_bucket(material)
        if ratio_bucket not in allowed[kind]:
            continue
        result.append(row)
    return result


def _slice_top_percent(rows: list[dict[str, Any]], percent: int) -> list[dict[str, Any]]:
    if not rows:
        return []
    count = max(1, math.ceil(len(rows) * percent / 100))
    return rows[:count]


def _slice_bottom_percent(rows: list[dict[str, Any]], percent: int) -> list[dict[str, Any]]:
    if not rows:
        return []
    count = max(1, math.ceil(len(rows) * percent / 100))
    return rows[-count:]


def _summarize_material_bucket(
    rows: list[dict[str, Any]],
    *,
    material_kind: str,
    sample_n: int = 5,
) -> dict[str, Any]:
    filtered = _filter_rows_by_kind_and_ratio(rows, kind=material_kind)
    unique_by_size: dict[str, dict[str, Any]] = {}
    ratio_counter: dict[str, int] = {}
    for row in filtered:
        material = row.get("material") if isinstance(row.get("material"), dict) else {}
        ratio_bucket = _size_ratio_bucket(material)
        size_key = _material_size_key(material)
        ratio_counter[ratio_bucket] = ratio_counter.get(ratio_bucket, 0) + 1
        if size_key not in unique_by_size:
            unique_by_size[size_key] = row

    unique_rows = list(unique_by_size.values())
    all_links = _build_top_material_links(unique_rows, kind=material_kind, top_n=len(unique_rows))
    sample_links = all_links[:sample_n]
    return {
        "material_kind": material_kind,
        "raw_count": len(filtered),
        "unique_size_count": len(unique_rows),
        "ratio_distribution": [
            {"ratio_bucket": k, "count": v}
            for k, v in sorted(ratio_counter.items(), key=lambda x: x[1], reverse=True)
        ],
        "materials": all_links,
        "sample_materials": sample_links,
    }


def _summarize_rule_group(rows: list[dict[str, Any]], *, name: str) -> dict[str, Any]:
    return {
        "group_name": name,
        "total_rows": len(rows),
        "image": _summarize_material_bucket(rows, material_kind="image"),
        "video": _summarize_material_bucket(rows, material_kind="video"),
    }


def _build_creative_rule_groups_for_app(
    *,
    client: AppGrowingClient,
    app_brand_id: str,
    start: str,
    end: str,
    purpose: int,
    material_pages: int,
    top_head_percent: int,
    new_head_percent: int,
    new_trend_percent: int,
) -> dict[str, Any]:
    all_material_payload = client.app_material_list(
        app_brand_id=app_brand_id,
        start_date=start,
        end_date=end,
        purpose=purpose,
        is_new=0,
        order="impression_inc_2y_desc",
        pages=material_pages,
    )
    new_material_payload = client.app_material_list(
        app_brand_id=app_brand_id,
        start_date=start,
        end_date=end,
        purpose=purpose,
        is_new=1,
        order="impression_inc_2y_desc",
        pages=material_pages,
    )

    all_rows = (
        all_material_payload.get("data", [])
        if isinstance(all_material_payload.get("data", []), list)
        else []
    )
    new_rows = (
        new_material_payload.get("data", [])
        if isinstance(new_material_payload.get("data", []), list)
        else []
    )

    head_rows = _slice_top_percent(all_rows, top_head_percent)
    new_head_rows = _slice_top_percent(new_rows, new_head_percent)
    new_trend_rows = _slice_bottom_percent(new_rows, new_trend_percent)

    return {
        "head_landscape": _summarize_rule_group(head_rows, name="头部素材格局"),
        "new_head_creative": _summarize_rule_group(new_head_rows, name="新头部创意"),
        "new_creative_trend": _summarize_rule_group(new_trend_rows, name="新创意趋势"),
        "meta": {
            "all_material_total": _parse_int_maybe(all_material_payload.get("total")),
            "new_material_total": _parse_int_maybe(new_material_payload.get("total")),
            "all_rows_fetched": len(all_rows),
            "new_rows_fetched": len(new_rows),
        },
    }


def _rule_group_text(group: dict[str, Any]) -> str:
    image = group.get("image", {}) if isinstance(group.get("image"), dict) else {}
    video = group.get("video", {}) if isinstance(group.get("video"), dict) else {}
    image_ratios = ", ".join(
        [
            f"{x.get('ratio_bucket')}:{x.get('count')}"
            for x in image.get("ratio_distribution", [])
            if isinstance(x, dict)
        ]
    )
    video_ratios = ", ".join(
        [
            f"{x.get('ratio_bucket')}:{x.get('count')}"
            for x in video.get("ratio_distribution", [])
            if isinstance(x, dict)
        ]
    )
    return (
        f"图片 unique_size={image.get('unique_size_count', 0)} [{image_ratios or '-'}]; "
        f"视频 unique_size={video.get('unique_size_count', 0)} [{video_ratios or '-'}]"
    )


def _group_links_text(group: dict[str, Any], *, kind: str) -> str:
    node = group.get(kind, {}) if isinstance(group.get(kind), dict) else {}
    materials = node.get("materials", []) if isinstance(node.get("materials", []), list) else []
    samples = node.get("sample_materials", []) if isinstance(node.get("sample_materials", []), list) else []
    rows = materials if materials else samples
    parts: list[str] = []
    for sample in rows:
        if not isinstance(sample, dict):
            continue
        material_id = str(sample.get("material_id") or "").strip()
        size = str(sample.get("size") or "").strip()
        link = str(sample.get("link") or "").strip()
        if not link:
            continue
        if material_id and size:
            parts.append(f"{material_id}({size}) {link}")
        elif material_id:
            parts.append(f"{material_id} {link}")
        else:
            parts.append(link)
    return " | ".join(parts)


def _group_links_compact_text(group: dict[str, Any]) -> str:
    image_text = _group_links_text(group, kind="image")
    video_text = _group_links_text(group, kind="video")
    if image_text and video_text:
        return f"图片: {image_text}\n视频: {video_text}"
    if image_text:
        return f"图片: {image_text}"
    if video_text:
        return f"视频: {video_text}"
    return ""


def _build_material_type_distribution_from_material_rows(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    type_counts: dict[str, int] = {"video": 0, "image": 0}
    for row in rows:
        if not isinstance(row, dict):
            continue
        material = row.get("material") if isinstance(row.get("material"), dict) else {}
        kind = _detect_material_kind(material)
        if kind not in type_counts:
            type_counts[kind] = 0
        type_counts[kind] += 1
    total = sum(type_counts.values())
    result: list[dict[str, Any]] = []
    for material_type, count in sorted(type_counts.items(), key=lambda x: x[1], reverse=True):
        ratio = round(count / total, 6) if total > 0 else None
        result.append(
            {
                "material_type": material_type,
                "count": count,
                "ratio": ratio,
            }
        )
    return result


def _plain_percent_text(ratio: float | None) -> str:
    if ratio is None:
        return "-"
    percent = ratio * 100 if abs(ratio) <= 1 else ratio
    text = f"{percent:.2f}".rstrip("0").rstrip(".")
    return f"{text}%"


def _distribution_text(
    items: list[dict[str, Any]],
    *,
    name_key: str,
    top_n: int,
    ratio_is_percent: bool = False,
) -> str:
    parts: list[str] = []
    for item in items[:top_n]:
        if not isinstance(item, dict):
            continue
        name = str(item.get(name_key) or "unknown")
        ratio = item.get("ratio")
        ratio_text = "-"
        if isinstance(ratio, (int, float)):
            if ratio_is_percent:
                text = f"{float(ratio):.2f}".rstrip("0").rstrip(".")
                ratio_text = f"{text}%"
            else:
                ratio_text = _plain_percent_text(float(ratio))
        parts.append(f"{name} {ratio_text}")
    return " | ".join(parts)


def _material_type_distribution_text(items: list[dict[str, Any]]) -> str:
    parts: list[str] = []
    for item in items:
        if not isinstance(item, dict):
            continue
        material_type = str(item.get("material_type") or "unknown")
        count = _parse_int_maybe(item.get("count"))
        ratio = item.get("ratio")
        ratio_text = _plain_percent_text(ratio if isinstance(ratio, (int, float)) else None)
        parts.append(f"{material_type} {count}({ratio_text})")
    return " | ".join(parts)


def _material_links_text(items: list[dict[str, Any]], *, top_n: int) -> str:
    parts: list[str] = []
    for item in items[:top_n]:
        if not isinstance(item, dict):
            continue
        link = str(item.get("link") or "").strip()
        if not link:
            continue
        size = str(item.get("size") or "").strip()
        material_id = str(item.get("material_id") or "").strip()
        if size and material_id:
            parts.append(f"{material_id}({size}) {link}")
        elif material_id:
            parts.append(f"{material_id} {link}")
        else:
            parts.append(link)
    return " | ".join(parts)


def _select_competitors_from_ranking_payloads(
    *,
    ranking_payloads: list[dict[str, Any]],
    top_rank_limit: int,
    min_rank_change_gt: int,
    pick_top_n: int,
) -> list[dict[str, Any]]:
    selected_by_app_id: dict[str, dict[str, Any]] = {}
    for payload in ranking_payloads:
        keyword = str(payload.get("keyword") or "")
        items = payload.get("items", [])
        if not isinstance(items, list):
            continue
        # Rule A: keep Top N by rank directly.
        for item in items:
            if not isinstance(item, dict):
                continue
            current_rank = item.get("current_rank")
            app_id = str(item.get("app_id") or "").strip()
            if not app_id or not isinstance(current_rank, int):
                continue
            if current_rank > pick_top_n:
                continue
            candidate = {**item, "source_keyword": keyword}
            existing = selected_by_app_id.get(app_id)
            if not isinstance(existing, dict):
                selected_by_app_id[app_id] = candidate
                continue
            existing_rank = existing.get("current_rank")
            if isinstance(existing_rank, int) and current_rank < existing_rank:
                selected_by_app_id[app_id] = candidate

        # Rule B: add all strong movers in top rank window.
        for item in items:
            if not isinstance(item, dict):
                continue
            current_rank = item.get("current_rank")
            rank_change = item.get("rank_change")
            app_id = str(item.get("app_id") or "").strip()
            if not app_id:
                continue
            if not isinstance(current_rank, int) or not isinstance(rank_change, int):
                continue
            if current_rank > top_rank_limit or abs(rank_change) <= min_rank_change_gt:
                continue
            candidate = {**item, "source_keyword": keyword}
            existing = selected_by_app_id.get(app_id)
            if not isinstance(existing, dict):
                selected_by_app_id[app_id] = candidate
                continue
            existing_rank = existing.get("current_rank")
            existing_change = existing.get("rank_change")
            if isinstance(existing_rank, int) and current_rank < existing_rank:
                selected_by_app_id[app_id] = candidate
                continue
            if (
                isinstance(existing_rank, int)
                and current_rank == existing_rank
                and isinstance(existing_change, int)
                and abs(rank_change) > abs(existing_change)
            ):
                selected_by_app_id[app_id] = candidate
    selected = list(selected_by_app_id.values())
    selected.sort(
        key=lambda x: (
            int(x.get("current_rank", 10_000)),
            -abs(int(x.get("rank_change", 0))),
        )
    )
    return selected


def _build_compare_from_snapshots(
    *,
    keyword: str,
    this_start: str,
    this_end: str,
    last_start: str,
    last_end: str,
    current_items: list[dict[str, Any]],
    previous_items: list[dict[str, Any]],
) -> dict[str, Any]:
    prev_map = {str(x["app_id"]): x for x in previous_items}
    merged: list[dict[str, Any]] = []
    for curr in current_items:
        app_id = str(curr["app_id"])
        prev = prev_map.get(app_id)
        previous_rank = prev["rank"] if prev else None
        material_prev = int(prev["material_count"]) if prev else 0
        ad_prev = int(prev["ad_count"]) if prev else 0
        merged.append(
            {
                "app_id": app_id,
                "app_name": str(curr["app_name"]),
                "current_rank": int(curr["rank"]),
                "previous_rank": int(previous_rank) if previous_rank is not None else None,
                "rank_change": (int(previous_rank) - int(curr["rank"])) if previous_rank is not None else None,
                "material_count_current": int(curr["material_count"]),
                "material_count_previous": material_prev,
                "material_count_change": int(curr["material_count"]) - material_prev,
                "ad_count_current": int(curr["ad_count"]),
                "ad_count_previous": ad_prev,
                "ad_count_change": int(curr["ad_count"]) - ad_prev,
            }
        )
    merged.sort(key=lambda x: (x["current_rank"] if x["current_rank"] is not None else 9999, x["app_id"]))
    return {
        "ok": True,
        "data_source": "api",
        "keyword": keyword,
        "this_period": {"start": this_start, "end": this_end},
        "last_period": {"start": last_start, "end": last_end},
        "items": merged,
        "generated_at": utc_now_iso(),
    }


def _aggregate_compare_payloads(
    payloads: list[dict[str, Any]],
    *,
    keyword_label: str,
    this_start: str,
    this_end: str,
    last_start: str,
    last_end: str,
) -> dict[str, Any]:
    by_app: dict[str, dict[str, Any]] = {}
    for payload in payloads:
        for item in payload["items"]:
            key = item["app_id"]
            if key not in by_app:
                by_app[key] = {
                    "app_id": item["app_id"],
                    "app_name": item["app_name"],
                    "current_rank": item["current_rank"],
                    "previous_rank": item["previous_rank"],
                    "rank_change": item["rank_change"],
                    "material_count_current": 0,
                    "material_count_previous": 0,
                    "ad_count_current": 0,
                    "ad_count_previous": 0,
                }
            acc = by_app[key]
            if isinstance(item["current_rank"], int):
                acc["current_rank"] = (
                    item["current_rank"]
                    if not isinstance(acc["current_rank"], int)
                    else min(acc["current_rank"], item["current_rank"])
                )
            if isinstance(item["previous_rank"], int):
                acc["previous_rank"] = (
                    item["previous_rank"]
                    if not isinstance(acc["previous_rank"], int)
                    else min(acc["previous_rank"], item["previous_rank"])
                )
            acc["material_count_current"] += int(item["material_count_current"])
            acc["material_count_previous"] += int(item["material_count_previous"])
            acc["ad_count_current"] += int(item["ad_count_current"])
            acc["ad_count_previous"] += int(item["ad_count_previous"])

    merged: list[dict[str, Any]] = []
    for acc in by_app.values():
        previous_rank = acc["previous_rank"]
        current_rank = acc["current_rank"]
        rank_change = None
        if isinstance(previous_rank, int) and isinstance(current_rank, int):
            rank_change = previous_rank - current_rank
        merged.append(
            {
                "app_id": acc["app_id"],
                "app_name": acc["app_name"],
                "current_rank": current_rank,
                "previous_rank": previous_rank,
                "rank_change": rank_change,
                "material_count_current": acc["material_count_current"],
                "material_count_previous": acc["material_count_previous"],
                "material_count_change": acc["material_count_current"] - acc["material_count_previous"],
                "ad_count_current": acc["ad_count_current"],
                "ad_count_previous": acc["ad_count_previous"],
                "ad_count_change": acc["ad_count_current"] - acc["ad_count_previous"],
            }
        )
    merged.sort(
        key=lambda x: (
            x["current_rank"] if isinstance(x["current_rank"], int) else 9999,
            x["app_id"],
        )
    )
    return {
        "ok": True,
        "data_source": "api",
        "keyword": keyword_label,
        "this_period": {"start": this_start, "end": this_end},
        "last_period": {"start": last_start, "end": last_end},
        "items": merged,
        "generated_at": utc_now_iso(),
    }


def _aggregate_creative_payloads(
    *,
    mode: str,
    payloads: list[dict[str, Any]],
    keyword_label: str = "__multi__",
) -> dict[str, Any]:
    all_items: list[dict[str, Any]] = []
    period = {"start": "", "end": ""}
    for payload in payloads:
        if not period["start"]:
            period = payload["period"]
        all_items.extend(payload["items"])
    cluster_counter: dict[str, list[str]] = {}
    for item in all_items:
        key = str(item.get("cover_cluster", "uncategorized"))
        cluster_counter.setdefault(key, []).append(str(item.get("creative_id", "")))
    cluster_summary = [
        {"cluster": key, "count": len(ids), "sample_creative_ids": [x for x in ids[:3] if x]}
        for key, ids in cluster_counter.items()
    ]
    return {
        "ok": True,
        "data_source": "api",
        "mode": mode,
        "keyword": keyword_label,
        "period": period,
        "items": all_items,
        "cluster_summary": cluster_summary,
        "generated_at": utc_now_iso(),
    }


def _safe_change_ratio(current: int, previous: int) -> float | None:
    if previous <= 0:
        return None
    return round((current - previous) / previous, 6)


def _build_table_metrics(
    *,
    trend_compare: dict[str, Any],
    creative_top_head: dict[str, Any],
    channel_distribution: list[dict[str, Any]] | None = None,
    region_distribution_top: list[dict[str, Any]] | None = None,
) -> dict[str, Any]:
    items = trend_compare.get("items", [])
    total_material_current = 0
    total_material_previous = 0
    total_ad_current = 0
    total_ad_previous = 0
    if isinstance(items, list):
        for row in items:
            if not isinstance(row, dict):
                continue
            total_material_current += int(row.get("material_count_current", 0) or 0)
            total_material_previous += int(row.get("material_count_previous", 0) or 0)
            total_ad_current += int(row.get("ad_count_current", 0) or 0)
            total_ad_previous += int(row.get("ad_count_previous", 0) or 0)

    creative_items = creative_top_head.get("items", [])
    type_counter: dict[str, int] = {}
    if isinstance(creative_items, list):
        for row in creative_items:
            if not isinstance(row, dict):
                continue
            material_type = str(row.get("material_type", "unknown"))
            type_counter[material_type] = type_counter.get(material_type, 0) + 1
    total_creatives = sum(type_counter.values())
    material_type_distribution = []
    for material_type, count in sorted(type_counter.items()):
        ratio = round(count / total_creatives, 6) if total_creatives > 0 else None
        material_type_distribution.append(
            {
                "material_type": material_type,
                "count": count,
                "ratio": ratio,
            }
        )
    language_distribution_top = _build_language_distribution_from_creative(creative_items=creative_items, top_n=10)
    top_image_sizes = _build_size_top_from_creative(creative_items=creative_items, material_type="image", top_n=10)
    top_video_sizes = _build_size_top_from_creative(creative_items=creative_items, material_type="video", top_n=10)

    return {
        "ok": True,
        "data_source": "api",
        "generated_at": utc_now_iso(),
        "change_ratio_metrics": {
            "material_count_current_total": total_material_current,
            "material_count_previous_total": total_material_previous,
            "material_count_change_ratio": _safe_change_ratio(total_material_current, total_material_previous),
            "ad_count_current_total": total_ad_current,
            "ad_count_previous_total": total_ad_previous,
            "ad_count_change_ratio": _safe_change_ratio(total_ad_current, total_ad_previous),
        },
        "material_type_distribution": material_type_distribution,
        "channel_distribution": channel_distribution or [],
        "region_distribution_top": region_distribution_top or [],
        "language_distribution_top": language_distribution_top,
        "top_image_sizes": top_image_sizes,
        "top_video_sizes": top_video_sizes,
    }


def _build_requirements_coverage() -> dict[str, Any]:
    return {
        "ok": True,
        "data_source": "api",
        "generated_at": utc_now_iso(),
        "coverage": [
            {
                "requirement_id": "R1",
                "requirement": "趋势双周期排名变化",
                "status": "done",
                "source_artifact": "trend-compare.json",
                "note": "已输出 current_rank/previous_rank/rank_change。",
            },
            {
                "requirement_id": "R2",
                "requirement": "广告数变化占比、素材数变化占比",
                "status": "done",
                "source_artifact": "table-metrics.json",
                "note": "迭代1新增 change_ratio_metrics。",
            },
            {
                "requirement_id": "R3",
                "requirement": "素材类型数量+占比",
                "status": "done",
                "source_artifact": "table-metrics.json",
                "note": "迭代1新增 material_type_distribution。",
            },
            {
                "requirement_id": "R4",
                "requirement": "渠道占比/国家地区TOP/语言TOP/尺寸TOP",
                "status": "done",
                "source_artifact": "table-metrics.json",
                "note": "迭代2已输出 channel/region/language/size Top 字段。",
            },
            {
                "requirement_id": "R5",
                "requirement": "文档评论需求追踪（非广告评论数据）",
                "status": "done",
                "source_artifact": "docs/requirements-matrix.md",
                "note": "矩阵模板已包含 comment_requirement 列。",
            },
        ],
    }


def _build_creative_payload_from_api(
    *,
    client: AppGrowingClient,
    mode: str,
    keyword: str,
    start: str,
    end: str,
    material_id: str,
    markets: tuple[str, ...],
) -> dict[str, Any]:
    rows = client.creative_list(
        material_id=material_id,
        start_date=start,
        end_date=end,
        markets=list(markets),
        mode=mode,
    )
    items: list[dict[str, Any]] = []
    for idx, row in enumerate(rows, start=1):
        media = row.get("media") if isinstance(row, dict) else None
        media_name = media.get("name") if isinstance(media, dict) else ""
        material_type = "video" if "video" in str(media_name).lower() else "image"
        area = row.get("area") if isinstance(row, dict) else None
        area_name = area.get("name") if isinstance(area, dict) else None
        creative = row.get("creative") if isinstance(row, dict) else None
        slogan = creative.get("slogan") if isinstance(creative, dict) else ""
        items.append(
            {
                "creative_id": f"{material_id}_{idx:03d}",
                "app_id": f"material_{material_id}",
                "app_name": str(area_name or "unknown"),
                "material_type": material_type,
                "size": "unknown",
                "is_new": mode != "top_head",
                "impression_total": None,
                "cover_cluster": slogan[:40] if slogan else "uncategorized",
                "language": None,
            }
        )

    cluster_counter: dict[str, list[str]] = {}
    for item in items:
        key = item["cover_cluster"]
        cluster_counter.setdefault(key, []).append(item["creative_id"])
    cluster_summary = [
        {"cluster": key, "count": len(ids), "sample_creative_ids": ids[:3]}
        for key, ids in cluster_counter.items()
    ]
    return {
        "ok": True,
        "data_source": "api",
        "mode": mode,
        "keyword": keyword,
        "period": {"start": start, "end": end},
        "items": items,
        "cluster_summary": cluster_summary,
        "generated_at": utc_now_iso(),
    }


@click.group()
@click.option("--json", "json_output", is_flag=True, default=True, show_default=True)
@click.option("--no-validate", is_flag=True, default=False, help="Skip schema validation.")
@click.option(
    "--source",
    "source_mode",
    type=click.Choice(["api"], case_sensitive=False),
    default="api",
    show_default=True,
    help="Data source mode: real AppGrowing API only.",
)
@click.pass_context
def main(ctx: click.Context, json_output: bool, no_validate: bool, source_mode: str) -> None:
    """AppGrowing monitoring CLI."""
    ctx.ensure_object(dict)
    ctx.obj["json_output"] = json_output
    ctx.obj["validate"] = not no_validate
    ctx.obj["source_mode"] = source_mode.lower()
    auth_data = load_auth()
    ctx.obj["auth"] = auth_data
    ctx.obj["api_client"] = AppGrowingClient(
        endpoint=str(auth_data.get("endpoint", "https://api-appgrowing-global.youcloud.com/graphql")),
        language=str(auth_data.get("language", "zh")),
        cookie=str(auth_data.get("cookie", "")),
    )


@main.group()
def auth() -> None:
    """Authentication commands."""


@auth.command("login")
@click.option(
    "--from-browser",
    "from_browser",
    type=click.Choice(["auto", "chrome", "firefox", "edge", "brave"], case_sensitive=False),
    default="auto",
    show_default=True,
    help="Extract cookies from browser automatically.",
)
@click.option(
    "--domain",
    type=str,
    default=".youcloud.com",
    show_default=True,
    help="Cookie domain used for browser extraction.",
)
@click.pass_context
def auth_login(
    ctx: click.Context,
    from_browser: str,
    domain: str,
) -> None:
    extracted = extract_browser_auth(browser=from_browser, domain=domain)
    effective_cookie = extracted.get("cookie", "").strip()
    if not effective_cookie:
        raise click.ClickException(
            "Failed to extract browser cookies. "
            "Please ensure you are logged in on appgrowing-global.youcloud.com."
        )
    endpoint = extracted.get("endpoint", "https://api-appgrowing-global.youcloud.com/graphql").strip()
    language = extracted.get("language", "zh").strip() or "zh"
    try:
        detected_cookie_count = int(extracted.get("cookie_count", "0"))
    except ValueError:
        detected_cookie_count = None
    source = f"browser:{from_browser}"

    saved_path = save_auth(cookie=effective_cookie, endpoint=endpoint, language=language)
    payload = {
        "ok": True,
        "saved_to": str(saved_path),
        "endpoint": endpoint,
        "language": language,
        "source": source,
        "cookie_count": detected_cookie_count,
        "note": "Auth cookie saved. You can override with APPGROWING_COOKIE env.",
    }
    _emit(ctx, payload)


@auth.command("status")
@click.pass_context
def auth_status(ctx: click.Context) -> None:
    auth_data = ctx.obj["auth"]
    source = auth_data.get("source", "none")
    has_cookie = bool(auth_data.get("cookie"))
    payload: dict[str, Any] = {
        "ok": True,
        "source": source,
        "has_cookie": has_cookie,
        "endpoint": auth_data.get("endpoint"),
        "language": auth_data.get("language"),
    }
    try:
        health = _api_client_from_ctx(ctx).health_check()
        payload["api_health"] = health
    except AppGrowingAPIError as exc:
        payload["ok"] = False
        payload["api_error"] = str(exc)
    _emit(ctx, payload)


@main.group()
def trend() -> None:
    """Trend analysis commands."""


@trend.group("ranking")
def trend_ranking() -> None:
    """Ranking related commands."""


@trend_ranking.command("snapshot")
@click.option("--keyword", "keywords", required=True, multiple=True, type=str)
@click.option("--start", required=True, type=str)
@click.option("--end", required=True, type=str)
@click.option("--market", "markets", multiple=True, type=str)
@click.option("--pages", default=1, show_default=True, type=int, help="Fetch this many pages in API mode.")
@click.option("--accurate-search", default=0, show_default=True, type=click.IntRange(0, 1))
@click.option("--order", default="material_cnt_desc", show_default=True, type=str)
@click.option("--out-file", type=click.Path(), help="Write full snapshot payload to a JSON file.")
@click.option("--csv-file", type=click.Path(), help="Write table-friendly rows to a CSV file.")
@click.pass_context
def trend_ranking_snapshot(
    ctx: click.Context,
    keywords: tuple[str, ...],
    start: str,
    end: str,
    markets: tuple[str, ...],
    pages: int,
    accurate_search: int,
    order: str,
    out_file: str | None,
    csv_file: str | None,
) -> None:
    keyword_list = [k for k in keywords if k.strip()]
    if not keyword_list:
        raise click.ClickException("At least one --keyword is required.")
    if len(keyword_list) > 1:
        raise click.ClickException("trend ranking snapshot currently supports exactly one --keyword.")
    primary_keyword = keyword_list[0]
    last_start, last_end = _infer_previous_period(start, end)
    try:
        payload = _build_ranking_business_snapshot_for_keyword(
            client=_api_client_from_ctx(ctx),
            keyword=primary_keyword,
            this_start=start,
            this_end=end,
            last_start=last_start,
            last_end=last_end,
            markets=markets,
            pages=pages,
            accurate_search=accurate_search,
            order=order,
        )
    except AppGrowingAPIError as exc:
        raise click.ClickException(str(exc)) from exc
    _validate_if_enabled(ctx, "trend-ranking-snapshot.schema.json", payload)
    payload["keywords"] = keyword_list
    if out_file:
        write_json_file(Path(out_file), payload)
    if csv_file:
        period_label = _period_label_from_range(start, end)
        _write_csv_rows(
            Path(csv_file),
            [
                "赛道关键词",
                "周期",
                "产品名称",
                "Appgrowing链接",
                "系统平台",
                "排名+变化",
                "广告数+变化占比",
                "素材数+变化占比",
            ],
            _ranking_business_csv_rows_with_context(
                items=payload.get("items", []),
                keyword=primary_keyword,
                period_label=period_label,
            ),
        )
    _emit(ctx, payload)


@trend_ranking.command("compare")
@click.option("--keyword", "keywords", required=True, multiple=True, type=str)
@click.option("--this-start", required=True, type=str)
@click.option("--this-end", required=True, type=str)
@click.option("--last-start", required=True, type=str)
@click.option("--last-end", required=True, type=str)
@click.option("--pages", default=1, show_default=True, type=int, help="Fetch this many pages in API mode.")
@click.option("--accurate-search", default=0, show_default=True, type=click.IntRange(0, 1))
@click.option("--order", default="material_cnt_desc", show_default=True, type=str)
@click.option("--out-file", type=click.Path(), help="Write full compare payload to a JSON file.")
@click.option("--csv-file", type=click.Path(), help="Write table-friendly compare rows to a CSV file.")
@click.pass_context
def trend_ranking_compare(
    ctx: click.Context,
    keywords: tuple[str, ...],
    this_start: str,
    this_end: str,
    last_start: str,
    last_end: str,
    pages: int,
    accurate_search: int,
    order: str,
    out_file: str | None,
    csv_file: str | None,
) -> None:
    keyword_list = [k for k in keywords if k.strip()]
    if not keyword_list:
        raise click.ClickException("At least one --keyword is required.")
    if len(keyword_list) > 1:
        raise click.ClickException("trend ranking compare currently supports exactly one --keyword.")
    try:
        payload = _build_ranking_business_snapshot_for_keyword(
            client=_api_client_from_ctx(ctx),
            keyword=keyword_list[0],
            this_start=this_start,
            this_end=this_end,
            last_start=last_start,
            last_end=last_end,
            markets=(),
            pages=pages,
            accurate_search=accurate_search,
            order=order,
        )
    except AppGrowingAPIError as exc:
        raise click.ClickException(str(exc)) from exc
    _validate_if_enabled(ctx, "trend-ranking-compare.schema.json", payload)
    payload["keywords"] = keyword_list
    if out_file:
        write_json_file(Path(out_file), payload)
    if csv_file:
        period_label = _period_label_from_range(this_start, this_end)
        _write_csv_rows(
            Path(csv_file),
            [
                "赛道关键词",
                "周期",
                "产品名称",
                "Appgrowing链接",
                "系统平台",
                "排名+变化",
                "广告数+变化占比",
                "素材数+变化占比",
            ],
            _ranking_business_csv_rows_with_context(
                items=payload.get("items", []),
                keyword=keyword_list[0],
                period_label=period_label,
            ),
        )
    _emit(ctx, payload)


@trend.group("promote-ranking")
def trend_promote_ranking() -> None:
    """Promote ranking based on promoteAppList."""


@trend_promote_ranking.command("snapshot")
@click.option("--keyword", required=True, type=str)
@click.option("--start", required=True, type=str)
@click.option("--end", required=True, type=str)
@click.option("--market", "markets", multiple=True, type=str)
@click.option("--pages", default=1, show_default=True, type=int)
@click.option("--accurate-search", default=0, show_default=True, type=click.IntRange(0, 1))
@click.option("--order", default="material_cnt_desc", show_default=True, type=str)
@click.option("--purpose", default=1, show_default=True, type=int)
@click.option("--out-file", type=click.Path(), help="Write full snapshot payload to a JSON file.")
@click.option("--csv-file", type=click.Path(), help="Write table-friendly rows to a CSV file.")
@click.option("--top-n", type=click.IntRange(1), help="Limit CSV export rows to top N.")
@click.pass_context
def trend_promote_ranking_snapshot(
    ctx: click.Context,
    keyword: str,
    start: str,
    end: str,
    markets: tuple[str, ...],
    pages: int,
    accurate_search: int,
    order: str,
    purpose: int,
    out_file: str | None,
    csv_file: str | None,
    top_n: int | None,
) -> None:
    try:
        payload = _build_promote_ranking_snapshot_from_api(
            keyword=keyword,
            start=start,
            end=end,
            markets=markets,
            client=_api_client_from_ctx(ctx),
            pages=pages,
            accurate_search=accurate_search,
            order=order,
            purpose=purpose,
        )
    except AppGrowingAPIError as exc:
        raise click.ClickException(str(exc)) from exc
    _validate_if_enabled(ctx, "promote-ranking-snapshot.schema.json", payload)
    if out_file:
        write_json_file(Path(out_file), payload)
    if csv_file:
        csv_rows = _promote_snapshot_csv_rows(payload.get("items", []))
        if top_n is not None:
            csv_rows = csv_rows[:top_n]
        _write_csv_rows(
            Path(csv_file),
            [
                "rank",
                "app_id",
                "app_name",
                "country",
                "publish_platform",
                "media_info",
                "ad_count",
                "material_count",
                "video_fragment_count",
            ],
            csv_rows,
        )
    _emit(ctx, payload)


@trend_promote_ranking.command("compare")
@click.option("--keyword", required=True, type=str)
@click.option("--this-start", required=True, type=str)
@click.option("--this-end", required=True, type=str)
@click.option("--last-start", required=True, type=str)
@click.option("--last-end", required=True, type=str)
@click.option("--market", "markets", multiple=True, type=str)
@click.option("--pages", default=1, show_default=True, type=int)
@click.option("--accurate-search", default=0, show_default=True, type=click.IntRange(0, 1))
@click.option("--order", default="material_cnt_desc", show_default=True, type=str)
@click.option("--purpose", default=1, show_default=True, type=int)
@click.option("--out-file", type=click.Path(), help="Write full compare payload to a JSON file.")
@click.option("--csv-file", type=click.Path(), help="Write table-friendly compare rows to a CSV file.")
@click.option("--top-n", type=click.IntRange(1), help="Limit CSV export rows to top N.")
@click.pass_context
def trend_promote_ranking_compare(
    ctx: click.Context,
    keyword: str,
    this_start: str,
    this_end: str,
    last_start: str,
    last_end: str,
    markets: tuple[str, ...],
    pages: int,
    accurate_search: int,
    order: str,
    purpose: int,
    out_file: str | None,
    csv_file: str | None,
    top_n: int | None,
) -> None:
    try:
        current = _build_promote_ranking_snapshot_from_api(
            keyword=keyword,
            start=this_start,
            end=this_end,
            markets=markets,
            client=_api_client_from_ctx(ctx),
            pages=pages,
            accurate_search=accurate_search,
            order=order,
            purpose=purpose,
        )
        previous = _build_promote_ranking_snapshot_from_api(
            keyword=keyword,
            start=last_start,
            end=last_end,
            markets=markets,
            client=_api_client_from_ctx(ctx),
            pages=pages,
            accurate_search=accurate_search,
            order=order,
            purpose=purpose,
        )
        payload = _build_promote_ranking_compare_from_snapshots(
            keyword=keyword,
            this_start=this_start,
            this_end=this_end,
            last_start=last_start,
            last_end=last_end,
            current_items=current["items"],
            previous_items=previous["items"],
        )
    except AppGrowingAPIError as exc:
        raise click.ClickException(str(exc)) from exc
    _validate_if_enabled(ctx, "promote-ranking-compare.schema.json", payload)
    if out_file:
        write_json_file(Path(out_file), payload)
    if csv_file:
        csv_rows = _promote_compare_csv_rows(payload.get("items", []))
        if top_n is not None:
            csv_rows = csv_rows[:top_n]
        _write_csv_rows(
            Path(csv_file),
            [
                "app_id",
                "app_name",
                "country",
                "publish_platform",
                "media_info",
                "current_rank",
                "previous_rank",
                "rank_change",
                "ad_count_current",
                "ad_count_previous",
                "ad_count_change",
                "material_count_current",
                "material_count_previous",
                "material_count_change",
                "video_fragment_count_current",
                "video_fragment_count_previous",
                "video_fragment_count_change",
            ],
            csv_rows,
        )
    _emit(ctx, payload)


@trend.command("app-distribution")
@click.option("--app-brand-id", required=True, type=str)
@click.option("--start", required=True, type=str)
@click.option("--end", required=True, type=str)
@click.option("--purpose", default=2, show_default=True, type=int)
@click.option("--market", "markets", multiple=True, type=str)
@click.option("--dimension", default="material", show_default=True, type=str)
@click.option("--top-n", default=10, show_default=True, type=click.IntRange(1))
@click.option("--out-file", type=click.Path(), help="Write payload to JSON file.")
@click.pass_context
def trend_app_distribution(
    ctx: click.Context,
    app_brand_id: str,
    start: str,
    end: str,
    purpose: int,
    markets: tuple[str, ...],
    dimension: str,
    top_n: int,
    out_file: str | None,
) -> None:
    client = _api_client_from_ctx(ctx)
    try:
        media_rows = client.media_launch(
            brand_id=app_brand_id,
            start_date=start,
            end_date=end,
            purpose=purpose,
            markets=list(markets),
            dimension=dimension,
        )
        region_rows = client.region_launch(
            brand_id=app_brand_id,
            start_date=start,
            end_date=end,
            purpose=purpose,
            markets=list(markets),
            dimension=dimension,
        )
        continent_map = client.area_continent_map()
    except AppGrowingAPIError as exc:
        raise click.ClickException(str(exc)) from exc

    language_rows: list[dict[str, Any]] = []
    language_error = None
    try:
        language_rows = client.language_launch(
            brand_id=app_brand_id,
            start_date=start,
            end_date=end,
            purpose=purpose,
            markets=list(markets),
            dimension=dimension,
        )
    except AppGrowingAPIError as exc:
        language_error = str(exc)

    payload: dict[str, Any] = {
        "ok": True,
        "data_source": "api",
        "app_brand_id": app_brand_id,
        "period": {"start": start, "end": end},
        "purpose": purpose,
        "dimension": dimension,
        "market": list(markets),
        "channel_distribution": _build_channel_distribution(media_rows),
        "continent_distribution": _build_continent_distribution(
            region_rows=region_rows,
            cc_to_continent=continent_map,
        ),
        "language_distribution_top": _build_language_distribution(language_rows, top_n=top_n),
        "meta": {
            "channel_rows": len(media_rows),
            "region_rows": len(region_rows),
            "language_rows": len(language_rows),
            "language_error": language_error,
        },
        "generated_at": utc_now_iso(),
    }
    _validate_if_enabled(ctx, "app-distribution.schema.json", payload)
    if out_file:
        write_json_file(Path(out_file), payload)
    _emit(ctx, payload)


@trend.command("app-material-insights")
@click.option("--app-brand-id", required=True, type=str)
@click.option("--this-start", required=True, type=str)
@click.option("--this-end", required=True, type=str)
@click.option("--last-start", required=True, type=str)
@click.option("--last-end", required=True, type=str)
@click.option("--purpose", default=2, show_default=True, type=int)
@click.option("--pages", default=1, show_default=True, type=int)
@click.option("--top-n", default=10, show_default=True, type=click.IntRange(1))
@click.option("--out-file", type=click.Path(), help="Write payload to JSON file.")
@click.pass_context
def trend_app_material_insights(
    ctx: click.Context,
    app_brand_id: str,
    this_start: str,
    this_end: str,
    last_start: str,
    last_end: str,
    purpose: int,
    pages: int,
    top_n: int,
    out_file: str | None,
) -> None:
    client = _api_client_from_ctx(ctx)
    try:
        current = client.app_material_list(
            app_brand_id=app_brand_id,
            start_date=this_start,
            end_date=this_end,
            purpose=purpose,
            pages=pages,
        )
        previous = client.app_material_list(
            app_brand_id=app_brand_id,
            start_date=last_start,
            end_date=last_end,
            purpose=purpose,
            pages=pages,
        )
    except AppGrowingAPIError as exc:
        raise click.ClickException(str(exc)) from exc

    video_rows: list[dict[str, Any]] = []
    image_rows: list[dict[str, Any]] = []
    video_error = None
    image_error = None
    try:
        current_video = client.app_material_list(
            app_brand_id=app_brand_id,
            start_date=this_start,
            end_date=this_end,
            purpose=purpose,
            creative_types=[201, 202, 203],
            pages=pages,
        )
        video_rows = current_video.get("data", []) if isinstance(current_video.get("data", []), list) else []
    except AppGrowingAPIError as exc:
        video_error = str(exc)
    try:
        current_image = client.app_material_list(
            app_brand_id=app_brand_id,
            start_date=this_start,
            end_date=this_end,
            purpose=purpose,
            creative_types=[102, 104, 103],
            pages=pages,
        )
        image_rows = current_image.get("data", []) if isinstance(current_image.get("data", []), list) else []
    except AppGrowingAPIError as exc:
        image_error = str(exc)

    material_count_current = _parse_int_maybe(current.get("total"))
    material_count_previous = _parse_int_maybe(previous.get("total"))
    material_count_change = material_count_current - material_count_previous
    current_rows = current.get("data", [])
    payload: dict[str, Any] = {
        "ok": True,
        "data_source": "api",
        "app_brand_id": app_brand_id,
        "this_period": {"start": this_start, "end": this_end},
        "last_period": {"start": last_start, "end": last_end},
        "purpose": purpose,
        "material_count_current": material_count_current,
        "material_count_previous": material_count_previous,
        "material_count_change": material_count_change,
        "material_count_change_ratio": _safe_change_ratio(material_count_current, material_count_previous),
        "top_video_materials": _build_top_material_links(video_rows, kind="video", top_n=top_n),
        "top_image_materials": _build_top_material_links(image_rows, kind="image", top_n=top_n),
        "meta": {
            "current_rows": len(current_rows) if isinstance(current_rows, list) else 0,
            "previous_rows": len(previous.get("data", [])) if isinstance(previous.get("data", []), list) else 0,
            "video_rows": len(video_rows),
            "image_rows": len(image_rows),
            "video_error": video_error,
            "image_error": image_error,
        },
        "generated_at": utc_now_iso(),
    }
    _validate_if_enabled(ctx, "app-material-insights.schema.json", payload)
    if out_file:
        write_json_file(Path(out_file), payload)
    _emit(ctx, payload)


@trend.command("creative-insights")
@click.option("--keyword", "keywords", required=True, multiple=True, type=str)
@click.option("--start", required=True, type=str)
@click.option("--end", required=True, type=str)
@click.option("--market", "markets", multiple=True, type=str)
@click.option("--purpose", default=2, show_default=True, type=int)
@click.option("--accurate-search", default=0, show_default=True, type=click.IntRange(0, 1))
@click.option("--order", default="material_cnt_desc", show_default=True, type=str)
@click.option("--ranking-pages", default=1, show_default=True, type=int)
@click.option("--top-rank-limit", default=15, show_default=True, type=click.IntRange(1))
@click.option("--min-rank-change", default=3, show_default=True, type=click.IntRange(0))
@click.option("--pick-top-n", default=3, show_default=True, type=click.IntRange(1))
@click.option("--material-pages", default=3, show_default=True, type=int, help="0 means fetch all pages.")
@click.option("--top-head-percent", default=10, show_default=True, type=click.IntRange(1, 100))
@click.option("--new-head-percent", default=20, show_default=True, type=click.IntRange(1, 100))
@click.option("--new-trend-percent", default=20, show_default=True, type=click.IntRange(1, 100))
@click.option("--out-file", type=click.Path(), help="Write payload to JSON file.")
@click.option("--csv-file", type=click.Path(), help="Write summary table to CSV file.")
@click.pass_context
def trend_creative_rule_groups(
    ctx: click.Context,
    keywords: tuple[str, ...],
    start: str,
    end: str,
    markets: tuple[str, ...],
    purpose: int,
    accurate_search: int,
    order: str,
    ranking_pages: int,
    top_rank_limit: int,
    min_rank_change: int,
    pick_top_n: int,
    material_pages: int,
    top_head_percent: int,
    new_head_percent: int,
    new_trend_percent: int,
    out_file: str | None,
    csv_file: str | None,
) -> None:
    if material_pages < 0:
        raise click.ClickException("--material-pages must be >= 0.")
    keyword_list = [k.strip() for k in keywords if k.strip()]
    if not keyword_list:
        raise click.ClickException("At least one --keyword is required.")

    last_start, last_end = _infer_previous_period(start, end)
    client = _api_client_from_ctx(ctx)

    ranking_payloads: list[dict[str, Any]] = []
    for keyword in keyword_list:
        try:
            ranking_payloads.append(
                _build_ranking_business_snapshot_for_keyword(
                    client=client,
                    keyword=keyword,
                    this_start=start,
                    this_end=end,
                    last_start=last_start,
                    last_end=last_end,
                    markets=markets,
                    pages=ranking_pages,
                    accurate_search=accurate_search,
                    order=order,
                )
            )
        except AppGrowingAPIError as exc:
            raise click.ClickException(f"Ranking fetch failed for keyword={keyword}: {exc}") from exc

    selected_competitors = _select_competitors_from_ranking_payloads(
        ranking_payloads=ranking_payloads,
        top_rank_limit=top_rank_limit,
        min_rank_change_gt=min_rank_change,
        pick_top_n=pick_top_n,
    )

    period_label = _period_label_from_range(start, end)
    competitor_results: list[dict[str, Any]] = []
    csv_rows: list[dict[str, Any]] = []
    for competitor in selected_competitors:
        app_brand_id = str(competitor.get("app_id") or "").strip()
        if not app_brand_id:
            continue
        try:
            grouped = _build_creative_rule_groups_for_app(
                client=client,
                app_brand_id=app_brand_id,
                start=start,
                end=end,
                purpose=purpose,
                material_pages=material_pages,
                top_head_percent=top_head_percent,
                new_head_percent=new_head_percent,
                new_trend_percent=new_trend_percent,
            )
        except AppGrowingAPIError as exc:
            raise click.ClickException(f"Material fetch failed for app={app_brand_id}: {exc}") from exc

        app_name = str(competitor.get("product_name") or app_brand_id)
        keyword = str(competitor.get("source_keyword") or keyword_list[0])
        app_payload = {
            "keyword": keyword,
            "period": period_label,
            "app_brand_id": app_brand_id,
            "product_name": app_name,
            "appgrowing_link": competitor.get("appgrowing_link"),
            "rank_context": {
                "current_rank": competitor.get("current_rank"),
                "previous_rank": competitor.get("previous_rank"),
                "rank_change": competitor.get("rank_change"),
            },
            "meta": {
                **grouped.get("meta", {}),
                "note": "Exposure value is not returned by API; ranking uses impression_inc_2y_desc order.",
            },
            "groups": {
                "head_landscape": grouped.get("head_landscape"),
                "new_head_creative": grouped.get("new_head_creative"),
                "new_creative_trend": grouped.get("new_creative_trend"),
            },
        }
        competitor_results.append(app_payload)
        csv_rows.append(
            {
                "赛道关键词": keyword,
                "周期": period_label,
                "产品": app_name,
                "头部素材格局(前10%)": _group_links_compact_text(grouped.get("head_landscape", {})),
                "新头部创意(新素材前20%)": _group_links_compact_text(grouped.get("new_head_creative", {})),
                "新创意趋势(新素材后20%)": _group_links_compact_text(grouped.get("new_creative_trend", {})),
            }
        )

    payload: dict[str, Any] = {
        "ok": True,
        "data_source": "api",
        "keywords": keyword_list,
        "this_period": {"start": start, "end": end, "label": period_label},
        "last_period": {"start": last_start, "end": last_end, "label": _period_label_from_range(start, end, previous=True)},
        "selection_rules": {
            "top_rank_limit": top_rank_limit,
            "min_rank_change_gt": min_rank_change,
            "pick_top_n": pick_top_n,
            "dedup_rule": "app_id, keep lower current_rank when overlapped",
        },
        "rules": {
            "ranking_order": order,
            "material_order": "impression_inc_2y_desc",
            "head_top_percent": top_head_percent,
            "new_head_top_percent": new_head_percent,
            "new_trend_bottom_percent": new_trend_percent,
            "size_mapping": {
                "image": ["1:1", "4:5"],
                "video": ["9:16"],
            },
        },
        "query_options": {
            "market": list(markets),
            "purpose": purpose,
            "accurate_search": accurate_search,
            "ranking_pages": ranking_pages,
            "material_pages": material_pages,
        },
        "meta": {"selected_competitors_count": len(competitor_results)},
        "competitors": competitor_results,
        "generated_at": utc_now_iso(),
    }
    _validate_if_enabled(ctx, "creative-rule-groups.schema.json", payload)
    if out_file:
        write_json_file(Path(out_file), payload)
    if csv_file:
        _write_csv_rows(
            Path(csv_file),
            [
                "赛道关键词",
                "周期",
                "产品",
                "头部素材格局(前10%)",
                "新头部创意(新素材前20%)",
                "新创意趋势(新素材后20%)",
            ],
            csv_rows,
        )
    _emit(ctx, payload)


# Backward-compatible alias for existing scripts.
trend.add_command(trend_creative_rule_groups, "creative-rule-groups")


@trend.command("competitor-table")
@click.option("--keyword", "keywords", required=True, multiple=True, type=str)
@click.option("--start", required=True, type=str)
@click.option("--end", required=True, type=str)
@click.option("--market", "markets", multiple=True, type=str)
@click.option("--purpose", default=2, show_default=True, type=int)
@click.option("--dimension", default="material", show_default=True, type=str)
@click.option("--pages", default=1, show_default=True, type=int)
@click.option("--material-pages", default=1, show_default=True, type=int, help="0 means fetch all pages.")
@click.option("--accurate-search", default=0, show_default=True, type=click.IntRange(0, 1))
@click.option("--order", default="material_cnt_desc", show_default=True, type=str)
@click.option("--top-rank-limit", default=15, show_default=True, type=click.IntRange(1))
@click.option("--min-rank-change", default=3, show_default=True, type=click.IntRange(0))
@click.option("--pick-top-n", default=3, show_default=True, type=click.IntRange(1))
@click.option("--distribution-top-n", default=10, show_default=True, type=click.IntRange(1))
@click.option("--material-top-n", default=10, show_default=True, type=click.IntRange(1))
@click.option("--out-file", type=click.Path(), help="Write payload to JSON file.")
@click.option("--csv-file", type=click.Path(), help="Write flattened table rows to CSV file.")
@click.pass_context
def trend_competitor_table(
    ctx: click.Context,
    keywords: tuple[str, ...],
    start: str,
    end: str,
    markets: tuple[str, ...],
    purpose: int,
    dimension: str,
    pages: int,
    material_pages: int,
    accurate_search: int,
    order: str,
    top_rank_limit: int,
    min_rank_change: int,
    pick_top_n: int,
    distribution_top_n: int,
    material_top_n: int,
    out_file: str | None,
    csv_file: str | None,
) -> None:
    if material_pages < 0:
        raise click.ClickException("--material-pages must be >= 0.")
    keyword_list = [k.strip() for k in keywords if k.strip()]
    if not keyword_list:
        raise click.ClickException("At least one --keyword is required.")

    last_start, last_end = _infer_previous_period(start, end)
    client = _api_client_from_ctx(ctx)

    ranking_payloads: list[dict[str, Any]] = []
    for keyword in keyword_list:
        try:
            ranking_payloads.append(
                _build_ranking_business_snapshot_for_keyword(
                    client=client,
                    keyword=keyword,
                    this_start=start,
                    this_end=end,
                    last_start=last_start,
                    last_end=last_end,
                    markets=markets,
                    pages=pages,
                    accurate_search=accurate_search,
                    order=order,
                )
            )
        except AppGrowingAPIError as exc:
            raise click.ClickException(f"Ranking fetch failed for keyword={keyword}: {exc}") from exc

    selected_competitors = _select_competitors_from_ranking_payloads(
        ranking_payloads=ranking_payloads,
        top_rank_limit=top_rank_limit,
        min_rank_change_gt=min_rank_change,
        pick_top_n=pick_top_n,
    )

    continent_map: dict[str, str] = {}
    continent_map_error = None
    try:
        continent_map = client.area_continent_map()
    except AppGrowingAPIError as exc:
        continent_map_error = str(exc)

    period_label = _period_label_from_range(start, end)
    csv_rows: list[dict[str, Any]] = []
    competitors: list[dict[str, Any]] = []
    for item in selected_competitors:
        app_brand_id = str(item.get("app_id") or "")
        if not app_brand_id:
            continue

        media_rows: list[dict[str, Any]] = []
        region_rows: list[dict[str, Any]] = []
        language_rows: list[dict[str, Any]] = []
        distribution_errors: dict[str, str | None] = {
            "media_error": None,
            "region_error": None,
            "language_error": None,
        }
        try:
            media_rows = client.media_launch(
                brand_id=app_brand_id,
                start_date=start,
                end_date=end,
                purpose=purpose,
                markets=list(markets),
                dimension=dimension,
            )
        except AppGrowingAPIError as exc:
            distribution_errors["media_error"] = str(exc)
        try:
            region_rows = client.region_launch(
                brand_id=app_brand_id,
                start_date=start,
                end_date=end,
                purpose=purpose,
                markets=list(markets),
                dimension=dimension,
            )
        except AppGrowingAPIError as exc:
            distribution_errors["region_error"] = str(exc)
        try:
            language_rows = client.language_launch(
                brand_id=app_brand_id,
                start_date=start,
                end_date=end,
                purpose=purpose,
                markets=list(markets),
                dimension=dimension,
            )
        except AppGrowingAPIError as exc:
            distribution_errors["language_error"] = str(exc)

        channel_distribution = _build_channel_distribution(media_rows)
        continent_distribution = _build_continent_distribution(
            region_rows=region_rows,
            cc_to_continent=continent_map,
        )
        language_distribution_top = _build_language_distribution(language_rows, top_n=distribution_top_n)

        material_error = None
        current_material_payload: dict[str, Any] = {"data": [], "total": 0}
        previous_material_payload: dict[str, Any] = {"data": [], "total": 0}
        try:
            current_material_payload = client.app_material_list(
                app_brand_id=app_brand_id,
                start_date=start,
                end_date=end,
                purpose=purpose,
                pages=material_pages,
            )
            previous_material_payload = client.app_material_list(
                app_brand_id=app_brand_id,
                start_date=last_start,
                end_date=last_end,
                purpose=purpose,
                pages=material_pages,
            )
        except AppGrowingAPIError as exc:
            material_error = str(exc)

        current_material_rows = (
            current_material_payload.get("data", [])
            if isinstance(current_material_payload.get("data", []), list)
            else []
        )
        material_type_distribution = _build_material_type_distribution_from_material_rows(current_material_rows)
        top_video_materials = _build_top_material_links(
            current_material_rows,
            kind="video",
            top_n=material_top_n,
        )
        top_image_materials = _build_top_material_links(
            current_material_rows,
            kind="image",
            top_n=material_top_n,
        )

        competitor = {
            "keyword": item.get("source_keyword"),
            "period": period_label,
            "app_id": app_brand_id,
            "product_name": item.get("product_name"),
            "appgrowing_link": item.get("appgrowing_link"),
            "system_platform": item.get("system_platform"),
            "current_rank": item.get("current_rank"),
            "previous_rank": item.get("previous_rank"),
            "rank_change": item.get("rank_change"),
            "ad_count_current": item.get("ad_count_current"),
            "ad_count_previous": item.get("ad_count_previous"),
            "ad_count_change_ratio": item.get("ad_count_change_ratio"),
            "material_count_current": _parse_int_maybe(current_material_payload.get("total")),
            "material_count_previous": _parse_int_maybe(previous_material_payload.get("total")),
            "material_count_change_ratio": _safe_change_ratio(
                _parse_int_maybe(current_material_payload.get("total")),
                _parse_int_maybe(previous_material_payload.get("total")),
            ),
            "material_type_distribution": material_type_distribution,
            "channel_distribution": channel_distribution[:distribution_top_n],
            "continent_distribution": continent_distribution[:distribution_top_n],
            "language_distribution_top": language_distribution_top,
            "top_video_materials": top_video_materials,
            "top_image_materials": top_image_materials,
            "meta": {
                **distribution_errors,
                "material_error": material_error,
            },
        }
        competitors.append(competitor)
        csv_rows.append(
            {
                "赛道关键词": str(competitor.get("keyword") or ""),
                "周期": period_label,
                "产品名称": competitor.get("product_name"),
                "Appgrowing链接": competitor.get("appgrowing_link"),
                "系统平台": competitor.get("system_platform"),
                "排名+变化": _ranking_with_change_text(
                    competitor.get("current_rank"),
                    competitor.get("rank_change")
                    if isinstance(competitor.get("rank_change"), int)
                    else None,
                ),
                "广告数+变化占比": _metric_with_change_text(
                    competitor.get("ad_count_current"),
                    competitor.get("ad_count_change_ratio")
                    if isinstance(competitor.get("ad_count_change_ratio"), (int, float))
                    else None,
                ),
                "素材数+变化占比": _metric_with_change_text(
                    competitor.get("material_count_current"),
                    competitor.get("material_count_change_ratio")
                    if isinstance(competitor.get("material_count_change_ratio"), (int, float))
                    else None,
                ),
                "素材类型数量+占比": _material_type_distribution_text(
                    competitor.get("material_type_distribution", [])
                    if isinstance(competitor.get("material_type_distribution", []), list)
                    else []
                ),
                "渠道占比": _distribution_text(
                    competitor.get("channel_distribution", [])
                    if isinstance(competitor.get("channel_distribution", []), list)
                    else [],
                    name_key="channel_name",
                    top_n=distribution_top_n,
                    ratio_is_percent=True,
                ),
                "国家地区占比-按大区": _distribution_text(
                    competitor.get("continent_distribution", [])
                    if isinstance(competitor.get("continent_distribution", []), list)
                    else [],
                    name_key="continent",
                    top_n=distribution_top_n,
                ),
                "语言分布占比TOP10": _distribution_text(
                    competitor.get("language_distribution_top", [])
                    if isinstance(competitor.get("language_distribution_top", []), list)
                    else [],
                    name_key="language_name",
                    top_n=distribution_top_n,
                    ratio_is_percent=True,
                ),
                "TOP10视频素材": _material_links_text(
                    competitor.get("top_video_materials", [])
                    if isinstance(competitor.get("top_video_materials", []), list)
                    else [],
                    top_n=material_top_n,
                ),
                "TOP10图片素材": _material_links_text(
                    competitor.get("top_image_materials", [])
                    if isinstance(competitor.get("top_image_materials", []), list)
                    else [],
                    top_n=material_top_n,
                ),
            }
        )

    payload: dict[str, Any] = {
        "ok": True,
        "data_source": "api",
        "keywords": keyword_list,
        "this_period": {"start": start, "end": end, "label": period_label},
        "last_period": {
            "start": last_start,
            "end": last_end,
            "label": _period_label_from_range(start, end, previous=True),
        },
        "selection_rules": {
            "top_rank_limit": top_rank_limit,
            "min_rank_change_gt": min_rank_change,
            "pick_top_n": pick_top_n,
            "dedup_rule": "app_id, keep lower current_rank when overlapped",
        },
        "query_options": {
            "market": list(markets),
            "purpose": purpose,
            "dimension": dimension,
            "pages": pages,
            "material_pages": material_pages,
            "accurate_search": accurate_search,
            "order": order,
            "distribution_top_n": distribution_top_n,
            "material_top_n": material_top_n,
        },
        "meta": {
            "candidates_count": len(selected_competitors),
            "final_count": len(competitors),
            "continent_map_error": continent_map_error,
        },
        "competitors": competitors,
        "generated_at": utc_now_iso(),
    }
    _validate_if_enabled(ctx, "trend-competitor-table.schema.json", payload)
    if out_file:
        write_json_file(Path(out_file), payload)
    if csv_file:
        _write_csv_rows(
            Path(csv_file),
            [
                "赛道关键词",
                "周期",
                "产品名称",
                "Appgrowing链接",
                "系统平台",
                "排名+变化",
                "广告数+变化占比",
                "素材数+变化占比",
                "素材类型数量+占比",
                "渠道占比",
                "国家地区占比-按大区",
                "语言分布占比TOP10",
                "TOP10视频素材",
                "TOP10图片素材",
            ],
            csv_rows,
        )
    _emit(ctx, payload)


@trend.command("pick-competitors")
@click.option("--compare-json", "compare_json_path", required=True, type=click.Path(exists=True))
@click.option("--exclude-app-id", "exclude_app_ids", multiple=True, type=str)
@click.option("--top", default=15, show_default=True, type=int)
@click.option("--min-rank-change", default=3, show_default=True, type=int)
@click.pass_context
def trend_pick_competitors(
    ctx: click.Context,
    compare_json_path: str,
    exclude_app_ids: tuple[str, ...],
    top: int,
    min_rank_change: int,
) -> None:
    compare_data = load_json_file(Path(compare_json_path))
    items = compare_data.get("items", [])
    if not isinstance(items, list):
        raise click.ClickException("Invalid compare json: items must be an array.")

    selected = []
    excluded_set = set(exclude_app_ids)
    for item in items:
        app_id = item.get("app_id")
        current_rank = item.get("current_rank")
        rank_change = item.get("rank_change")
        if app_id in excluded_set:
            continue
        if not isinstance(current_rank, int) or not isinstance(rank_change, int):
            continue
        if current_rank <= top and abs(rank_change) >= min_rank_change:
            selected.append(item)

    payload = {
        "ok": True,
        "rules": {
            "top": top,
            "min_rank_change": min_rank_change,
            "excluded_app_ids": list(exclude_app_ids),
        },
        "selected_competitors": selected,
        "count": len(selected),
        "generated_at": utc_now_iso(),
    }
    _emit(ctx, payload)


if __name__ == "__main__":
    main()
