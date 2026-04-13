"""Tests for creative-insights top material details."""

import json
import pytest

from click.testing import CliRunner

import appgrowing_cli.cli as cli_mod
from appgrowing_cli.api_adapter import AppGrowingClient


class _FakeCreativeClient:
    def __init__(self) -> None:
        self.calls: list[dict[str, object]] = []

    def app_material_list(
        self,
        *,
        app_brand_id: str,
        start_date: str,
        end_date: str,
        purpose: int,
        creative_types: list[int] | None = None,
        material_ratio: list[str] | None = None,
        accurate_search: int = 1,
        is_new: int,
        order: str,
        pages: int,
        detailed: bool = False,
    ) -> dict:
        self.calls.append({"is_new": is_new, "detailed": detailed, "pages": pages})
        del (
            app_brand_id,
            start_date,
            end_date,
            purpose,
            creative_types,
            material_ratio,
            accurate_search,
            order,
            pages,
        )
        image_row = {
            "material": {
                "id": "m_img_1",
                "type": "image",
                "duration": 0,
                "creative": {
                    "type": "image",
                    "txtUrl": "https://example.com/fallback-image",
                    "resource": {
                        "width": 1080,
                        "height": 1080,
                        "format": "jpg",
                        "path": "https://cdn.example.com/image.jpg",
                        "poster": "https://cdn.example.com/poster.jpg",
                        "duration": 0,
                        "id": "r1",
                    },
                },
            }
        }
        if detailed:
            image_row["material"].update(
                {
                    "startDate": "2026-04-03",
                    "endDate": "2026-04-09",
                    "cnt_ad_id": 2,
                    "impression_inc_2y": "5.9M",
                    "area": [
                        {"cc": "US", "name": "United States"},
                        {"cc": "CA", "name": "Canada"},
                        {"cc": "GB", "name": "United Kingdom"},
                        {"cc": "AU", "name": "Australia"},
                        {"cc": "DE", "name": "Germany"},
                        {"cc": "FR", "name": "France"},
                        {"cc": "JP", "name": "Japan"},
                    ],
                    "platform": [
                        {"id": 1, "name": "Facebook"},
                        {"id": 2, "name": "Instagram"},
                    ],
                    "campaign": [
                        {"id": "c1", "name": "Scale campaign"},
                        {"id": "c2", "name": "Canada campaign"},
                    ],
                }
            )
            image_row["material"]["creative"]["slogan"] = "Translate smarter with AI"
        if is_new == 1:
            return {"total": 1, "data": [image_row]}
        return {"total": 1, "data": [image_row]}


def test_build_creative_rule_groups_for_app_adds_top_material_details():
    client = _FakeCreativeClient()
    grouped = cli_mod._build_creative_rule_groups_for_app(
        client=client,
        app_brand_id="brand-1",
        start="2026-04-03",
        end="2026-04-09",
        purpose=2,
        material_pages=1,
        top_head_percent=10,
        new_head_percent=20,
        new_trend_percent=20,
        markets=("US",),
        top_material_details=1,
    )

    details = grouped["head_landscape"]["image"]["material_details"]
    assert len(details) == 1
    assert details[0]["material_id"] == "m_img_1"
    assert details[0]["captions"] == ["Translate smarter with AI"]
    assert details[0]["areas"] == [
        {"cc": "US", "name": "United States"},
        {"cc": "CA", "name": "Canada"},
        {"cc": "GB", "name": "United Kingdom"},
        {"cc": "AU", "name": "Australia"},
        {"cc": "DE", "name": "Germany"},
    ]
    assert details[0]["platforms"] == ["Facebook", "Instagram"]
    assert details[0]["campaigns"] == ["Scale campaign", "Canada campaign"]
    assert details[0]["creative_count"] == 2
    assert details[0]["impression_inc_2y"] == "5.9M"
    assert details[0]["first_seen"] == "2026-04-03"
    assert details[0]["last_seen"] == "2026-04-09"
    assert details[0]["link"] == "https://cdn.example.com/image.jpg"
    assert client.calls == [
        {"is_new": 0, "detailed": True, "pages": 1},
        {"is_new": 1, "detailed": True, "pages": 1},
    ]


def test_build_creative_rule_groups_for_app_limits_area_list_to_top_five():
    client = _FakeCreativeClient()
    grouped = cli_mod._build_creative_rule_groups_for_app(
        client=client,
        app_brand_id="brand-1",
        start="2026-04-03",
        end="2026-04-09",
        purpose=2,
        material_pages=1,
        top_head_percent=10,
        new_head_percent=20,
        new_trend_percent=20,
        markets=("US",),
        top_material_details=1,
    )

    details = grouped["head_landscape"]["image"]["material_details"]
    assert len(details[0]["areas"]) == 5
    assert details[0]["areas"] == [
        {"cc": "US", "name": "United States"},
        {"cc": "CA", "name": "Canada"},
        {"cc": "GB", "name": "United Kingdom"},
        {"cc": "AU", "name": "Australia"},
        {"cc": "DE", "name": "Germany"},
    ]


def test_creative_insights_cli_accepts_top_material_details(monkeypatch):
    runner = CliRunner()

    monkeypatch.setattr(cli_mod, "load_auth", lambda: {"endpoint": "https://example.com/graphql", "cookie": "x"})
    monkeypatch.setattr(
        cli_mod,
        "_build_ranking_business_snapshot_for_keyword",
        lambda **kwargs: {"keyword": kwargs["keyword"], "current_items": [], "previous_items": []},
    )
    monkeypatch.setattr(
        cli_mod,
        "_select_competitors_from_ranking_payloads",
        lambda **kwargs: [
            {
                "app_id": "brand-1",
                "product_name": "Competitor 1",
                "source_keyword": "Translate",
                "current_rank": 1,
                "previous_rank": 3,
                "rank_change": 2,
                "appgrowing_link": "https://appgrowing.example/app/brand-1",
            }
        ],
    )

    captured: dict = {}

    def fake_build_creative_rule_groups_for_app(**kwargs):
        captured["top_material_details"] = kwargs["top_material_details"]
        captured["markets"] = kwargs["markets"]
        return {
            "head_landscape": {
                "group_name": "头部素材格局",
                "total_rows": 1,
                "image": {
                    "material_kind": "image",
                    "raw_count": 1,
                    "unique_size_count": 1,
                    "ratio_distribution": [{"ratio_bucket": "1:1", "count": 1}],
                    "materials": [{"material_id": "m_img_1", "size": "1080x1080", "duration_ms": None, "link": "https://cdn.example.com/image.jpg"}],
                    "sample_materials": [{"material_id": "m_img_1", "size": "1080x1080", "duration_ms": None, "link": "https://cdn.example.com/image.jpg"}],
                    "material_details": [{"material_id": "m_img_1", "captions": ["Translate smarter with AI"], "areas": [{"cc": "US", "name": "United States"}], "impression_inc_2y": "5.9M"}],
                },
                "video": {
                    "material_kind": "video",
                    "raw_count": 0,
                    "unique_size_count": 0,
                    "ratio_distribution": [],
                    "materials": [],
                    "sample_materials": [],
                    "material_details": [],
                },
            },
            "new_head_creative": {
                "group_name": "新头部创意",
                "total_rows": 0,
                "image": {"material_kind": "image", "raw_count": 0, "unique_size_count": 0, "ratio_distribution": [], "materials": [], "sample_materials": [], "material_details": []},
                "video": {"material_kind": "video", "raw_count": 0, "unique_size_count": 0, "ratio_distribution": [], "materials": [], "sample_materials": [], "material_details": []},
            },
            "new_creative_trend": {
                "group_name": "新创意趋势",
                "total_rows": 0,
                "image": {"material_kind": "image", "raw_count": 0, "unique_size_count": 0, "ratio_distribution": [], "materials": [], "sample_materials": [], "material_details": []},
                "video": {"material_kind": "video", "raw_count": 0, "unique_size_count": 0, "ratio_distribution": [], "materials": [], "sample_materials": [], "material_details": []},
            },
            "meta": {"all_material_total": 1, "new_material_total": 0, "all_rows_fetched": 1, "new_rows_fetched": 0},
        }

    monkeypatch.setattr(cli_mod, "_build_creative_rule_groups_for_app", fake_build_creative_rule_groups_for_app)

    result = runner.invoke(
        cli_mod.main,
        [
            "--no-validate",
            "trend",
            "creative-insights",
            "--keyword",
            "Translate",
            "--start",
            "2026-04-03",
            "--end",
            "2026-04-09",
            "--market",
            "US",
            "--top-material-details",
            "5",
        ],
    )

    assert result.exit_code == 0
    payload = json.loads(result.output)
    assert captured["top_material_details"] == 5
    assert captured["markets"] == ("US",)
    details = payload["competitors"][0]["groups"]["head_landscape"]["image"]["material_details"]
    assert details[0]["material_id"] == "m_img_1"


class _BusyDetailClient(_FakeCreativeClient):
    def app_material_list(
        self,
        *,
        app_brand_id: str,
        start_date: str,
        end_date: str,
        purpose: int,
        creative_types: list[int] | None = None,
        material_ratio: list[str] | None = None,
        accurate_search: int = 1,
        is_new: int,
        order: str,
        pages: int,
        detailed: bool = False,
    ) -> dict:
        if detailed:
            raise cli_mod.AppGrowingAPIError("GraphQL error: [00:] The system is busy, please try again later")
        return super().app_material_list(
            app_brand_id=app_brand_id,
            start_date=start_date,
            end_date=end_date,
            purpose=purpose,
            creative_types=creative_types,
            material_ratio=material_ratio,
            accurate_search=accurate_search,
            is_new=is_new,
            order=order,
            pages=pages,
            detailed=detailed,
        )


def test_build_creative_rule_groups_for_app_raises_when_detailed_primary_query_busy():
    with pytest.raises(cli_mod.AppGrowingAPIError):
        cli_mod._build_creative_rule_groups_for_app(
            client=_BusyDetailClient(),
            app_brand_id="brand-1",
            start="2026-04-03",
            end="2026-04-09",
            purpose=2,
            material_pages=1,
            top_head_percent=10,
            new_head_percent=20,
            new_trend_percent=20,
            markets=("US",),
            top_material_details=1,
        )


def test_app_material_list_detailed_query_uses_lean_variables(monkeypatch):
    client = AppGrowingClient(endpoint="https://example.com/graphql", language="zh", cookie="")
    captured: dict[str, object] = {}

    def fake_graphql(*, operation_name: str, query: str, variables: dict[str, object] | None = None) -> dict:
        captured["operation_name"] = operation_name
        captured["query"] = query
        captured["variables"] = variables or {}
        return {"materialList": {"page": 1, "total": 1, "limit": 50, "data": []}}

    monkeypatch.setattr(client, "graphql", fake_graphql)

    client.app_material_list(
        app_brand_id="brand-1",
        start_date="2026-04-03",
        end_date="2026-04-09",
        purpose=2,
        is_new=1,
        order="impression_inc_2y_desc",
        pages=1,
        detailed=True,
    )

    variables = captured["variables"]
    assert captured["operation_name"] == "appMaterialList"
    assert "creativeType" not in variables
    assert "materialRatio" not in variables
    assert "campaign {" in str(captured["query"])
    assert "platform {" in str(captured["query"])
    assert "impression_inc_2y" in str(captured["query"])
