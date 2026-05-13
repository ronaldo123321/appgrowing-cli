"""Tests for independent app search command."""

import json

from click.testing import CliRunner

import appgrowing_cli.cli as cli_mod


class _FakeSearchClient:
    def __init__(self) -> None:
        self.calls: list[dict[str, object]] = []

    def search_app_multi_page(
        self,
        *,
        keyword: str,
        purpose: int,
        pages: int,
        accurate_search: int,
        had_advert: int | None,
    ) -> list[dict]:
        self.calls.append(
            {
                "keyword": keyword,
                "purpose": purpose,
                "pages": pages,
                "accurate_search": accurate_search,
                "had_advert": had_advert,
            }
        )
        return [
            {
                "appBrand": {
                    "id": "brand-1",
                    "name": "Blue Cat",
                    "icon": "https://cdn.example.com/icon.png",
                    "types": [1, 2],
                    "developer": {"id": "dev-1", "name": "Blue Cat Studio"},
                    "bundle_id": "com.example.bluecat",
                    "app_id": "123456789",
                },
                "highlight": "<em>Blue</em> Cat",
                "hadAdvert": True,
            }
        ]


def test_trend_app_search_outputs_brand_ids(monkeypatch):
    runner = CliRunner()
    fake_client = _FakeSearchClient()

    monkeypatch.setattr(cli_mod, "load_auth", lambda: {"endpoint": "https://example.com/graphql", "cookie": "x"})
    monkeypatch.setattr(cli_mod, "_api_client_from_ctx", lambda ctx: fake_client)

    result = runner.invoke(
        cli_mod.main,
        [
            "trend",
            "app-search",
            "--query",
            "Blue Cat",
            "--purpose",
            "2",
            "--pages",
            "2",
            "--accurate-search",
            "1",
        ],
    )

    assert result.exit_code == 0, result.output
    payload = json.loads(result.output)
    assert payload["ok"] is True
    assert payload["queries"] == ["Blue Cat"]
    assert payload["count"] == 1
    assert payload["results"][0]["app_brand_id"] == "brand-1"
    assert payload["results"][0]["app_name"] == "Blue Cat"
    assert payload["results"][0]["appgrowing_link"].endswith("/appBrand/brand-1/overview?purpose=2")
    assert payload["results"][0]["publish_platform"] == ["ios", "android"]
    assert fake_client.calls == [
        {
            "keyword": "Blue Cat",
            "purpose": 2,
            "pages": 2,
            "accurate_search": 1,
            "had_advert": 1,
        }
    ]


def test_trend_app_search_can_include_apps_without_ads(monkeypatch):
    runner = CliRunner()
    fake_client = _FakeSearchClient()

    monkeypatch.setattr(cli_mod, "load_auth", lambda: {"endpoint": "https://example.com/graphql", "cookie": "x"})
    monkeypatch.setattr(cli_mod, "_api_client_from_ctx", lambda ctx: fake_client)

    result = runner.invoke(
        cli_mod.main,
        [
            "trend",
            "app-search",
            "--query",
            "Blue Cat",
            "--include-no-advert",
        ],
    )

    assert result.exit_code == 0, result.output
    assert fake_client.calls[0]["had_advert"] is None
