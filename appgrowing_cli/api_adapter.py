"""Real AppGrowing GraphQL adapter."""

from __future__ import annotations

import json
import os
import ssl
import time
from dataclasses import dataclass
from typing import Any
from urllib.error import HTTPError, URLError
from urllib.request import Request, urlopen


class AppGrowingAPIError(RuntimeError):
    """Raised when AppGrowing API request fails."""


USERINFO_QUERY = """
query userinfo {
  userinfo {
    __typename
  }
}
"""

SEARCH_APP_QUERY = """
query searchApp(
  $purpose: Int
  $keyword: String!
  $accurateSearch: Int
  $hadAdvert: Int
  $page: Int
) {
  searchAppBrand(
    purpose: $purpose
    keyword: $keyword
    accurateSearch: $accurateSearch
    hadAdvert: $hadAdvert
    page: $page
  ) {
    page
    limit
    total
    pageTotal
    data {
      appBrand {
        id
        name
        icon
        types
        developer {
          id
          name
        }
        bundle_id
        app_id
      }
      highlight
      hadAdvert
    }
  }
}
"""

TOP_COMPETE_APP_QUERY = """
query topCompeteApp(
  $page: Int!
  $purpose: Int!
  $order: TopAppBrandListSort!
  $startDate: LocalDate!
  $endDate: LocalDate!
  $area: [String]
  $category: [Int]
) {
  topAppBrandList(
    page: $page
    purpose: $purpose
    order: $order
    startDate: $startDate
    endDate: $endDate
    area: $area
    category: $category
  ) {
    data {
      appBrand {
        id
        name
        icon
        types
        ios_app_url
        gp_app_url
        developer {
          id
          name
          area {
            cc
            name
            icon
          }
        }
      }
      adverts
    }
  }
}
"""

PROMOTE_APP_LIST_QUERY = """
query promoteAppList(
  $page: Int!
  $purpose: Int!
  $order: TopAppBrandListSort!
  $startDate: LocalDate!
  $endDate: LocalDate!
  $area: [String]
  $media: [Int]
  $category: [Int]
  $campaignType: [Int]
  $appStyle: [Int]
  $developerArea: String
  $keyword: String
  $isNew: Int
  $isPre: Int
  $tagIds: [String]
  $isExport: Boolean
  $templateName: String
  $appCashWay: [AppCashWay]
  $accurateSearch: Int
  $gender: [Int]
  $ageRange: String
) {
  topAppBrandList(
    page: $page
    purpose: $purpose
    order: $order
    startDate: $startDate
    endDate: $endDate
    area: $area
    media: $media
    category: $category
    campaignType: $campaignType
    appStyle: $appStyle
    developerArea: $developerArea
    keyword: $keyword
    isNew: $isNew
    isPre: $isPre
    tagIds: $tagIds
    isExport: $isExport
    templateName: $templateName
    appCashWay: $appCashWay
    accurateSearch: $accurateSearch
    gender: $gender
    ageRange: $ageRange
  ) {
    page
    total
    limit
    data {
      appBrand {
        id
        name
        icon
        types
        developer {
          id
          name
          area {
            cc
            name
            icon
          }
        }
        app_id
        bundle_id
        pre_state
      }
      adverts
      material_cnt
      duration
      highlight
      advert_history {
        max_dt
        min_dt
        is_new
      }
      media {
        id
        name
        icon
        description
      }
      media_total
    }
  }
}
"""

CREATIVE_LIST_QUERY = """
query creativeList(
  $materialId: MixID!
  $startDate: LocalDate
  $endDate: LocalDate
  $isAllDate: Int
  $media: [Int]
  $area: [String]
  $campaign: [String]
  $platform: [Int]
  $language: [String]
  $order: CreativeListSort!
  $page: Int
  $topLimit: Int
) {
  creativeList(
    materialId: $materialId
    startDate: $startDate
    endDate: $endDate
    isAllDate: $isAllDate
    media: $media
    area: $area
    campaign: $campaign
    platform: $platform
    language: $language
    order: $order
    page: $page
    topLimit: $topLimit
  ) {
    page
    total
    limit
    data {
      creative {
        slogan
      }
      area {
        cc
        name
        icon
        location
      }
      media {
        id
        name
        icon
        description
      }
      platform {
        id
        name
      }
      campaign {
        id
        name
        icon
        type
      }
      duration
      max_dt
      min_dt
      ad
      link
      homePage {
        type
        name
        url
      }
    }
  }
}
"""

MATERIAL_LIST_QUERY = """
query materialList(
  $purpose: Int!
  $startDate: LocalDate
  $endDate: LocalDate
  $keyword: String
  $area: [String]
  $order: MaterialListSort!
  $page: Int
  $accurateSearch: Int
) {
  materialList(
    purpose: $purpose
    startDate: $startDate
    endDate: $endDate
    keyword: $keyword
    area: $area
    order: $order
    page: $page
    accurateSearch: $accurateSearch
  ) {
    page
    total
    limit
    data {
      material {
        id
        max_dt
        min_dt
        cnt_ad_id
        campaign {
          ... on AppBrand {
            id
            name
            icon
            types
            ios_app_url
            gp_app_url
          }
          ... on App {
            id
            name
            icon
            type
          }
          ... on Website {
            id
            type
            name
            icon
          }
          ... on Playlet {
            id
            type
            name
          }
          ... on Novel {
            id
            type
            name
          }
        }
      }
      highlight
    }
  }
}
"""


FILTER_LIST_QUERY = """
query filterList {
  filterList {
    label
    data {
      ... on Area {
        cc
        name
        location
      }
    }
  }
}
"""


MEDIA_LAUNCH_QUERY = """
query mediaLaunch(
  $brandId: String
  $startDate: LocalDate
  $endDate: LocalDate
  $isAllDate: Int
  $purpose: Int!
  $area: [String]
  $media: [Int]
  $format: [Int]
  $creativeType: [Int]
  $dimension: DimensionType
  $isExport: Boolean
) {
  mediaLaunch(
    brandId: $brandId
    startDate: $startDate
    endDate: $endDate
    isAllDate: $isAllDate
    purpose: $purpose
    area: $area
    media: $media
    format: $format
    creativeType: $creativeType
    dimension: $dimension
    isExport: $isExport
  ) {
    adverts
    material
    percent
    media {
      id
      name
      icon
      description
    }
  }
}
"""


REGION_LAUNCH_QUERY = """
query regionLaunch(
  $brandId: String
  $startDate: LocalDate
  $endDate: LocalDate
  $isAllDate: Int
  $purpose: Int!
  $area: [String]
  $media: [Int]
  $format: [Int]
  $creativeType: [Int]
  $dimension: DimensionType
  $isExport: Boolean
) {
  regionLaunch(
    brandId: $brandId
    startDate: $startDate
    endDate: $endDate
    isAllDate: $isAllDate
    purpose: $purpose
    area: $area
    media: $media
    format: $format
    creativeType: $creativeType
    dimension: $dimension
    isExport: $isExport
  ) {
    adverts
    material
    percent
    area {
      cc
      name
      icon
    }
  }
}
"""


APP_LANGUAGE_QUERY = """
query appLanguage(
  $brandId: String
  $type: Int
  $startDate: LocalDate
  $endDate: LocalDate
  $isAllDate: Int
  $area: [String]
  $media: [Int]
  $format: [Int]
  $creativeType: [Int]
  $dimension: DimensionType
) {
  appLanguage(
    brandId: $brandId
    type: $type
    startDate: $startDate
    endDate: $endDate
    isAllDate: $isAllDate
    area: $area
    media: $media
    format: $format
    creativeType: $creativeType
    dimension: $dimension
  ) {
    adverts
    material
    percent
    language {
      code
      name
    }
  }
}
"""


APP_MATERIAL_LIST_QUERY = """
query appMaterialList(
  $purpose: Int!
  $startDate: LocalDate
  $endDate: LocalDate
  $creativeType: [Int]
  $isNew: Int
  $field: String
  $order: MaterialListSort!
  $page: Int
  $accurateSearch: Int
  $materialRatio: [String]
  $appBrand: String
) {
  materialList(
    purpose: $purpose
    startDate: $startDate
    endDate: $endDate
    creativeType: $creativeType
    isNew: $isNew
    field: $field
    order: $order
    page: $page
    accurateSearch: $accurateSearch
    materialRatio: $materialRatio
    appBrand: $appBrand
  ) {
    page
    total
    limit
    data {
      material {
        id
        type
        duration
        creative {
          type
          txtUrl
          resource {
            width
            height
            format
            path
            poster
            duration
            id
          }
        }
      }
    }
  }
}
"""


APP_MATERIAL_LIST_DETAIL_QUERY = """
query appMaterialList(
  $purpose: Int!
  $startDate: LocalDate
  $endDate: LocalDate
  $isNew: Int
  $field: String
  $order: MaterialListSort!
  $page: Int
  $accurateSearch: Int
  $appBrand: String
) {
  materialList(
    purpose: $purpose
    startDate: $startDate
    endDate: $endDate
    isNew: $isNew
    field: $field
    order: $order
    page: $page
    accurateSearch: $accurateSearch
    appBrand: $appBrand
  ) {
    page
    total
    limit
    data {
      material {
        id
        type
        startDate
        endDate
        duration
        cnt_ad_id
        impression_inc_2y
        area {
          cc
          name
          icon
        }
        creative {
          id
          type
          slogan
          description
          txtUrl
          resource {
            width
            height
            format
            path
            poster
            duration
            id
          }
        }
        platform {
          id
          name
        }
        campaign {
          ... on App {
            id
            name
          }
          ... on AppBrand {
            id
            name
          }
          ... on Website {
            id
            name
          }
          ... on Playlet {
            id
            name
          }
          ... on Novel {
            id
            name
          }
        }
      }
    }
  }
}
"""


@dataclass
class AppGrowingClient:
    """GraphQL client for AppGrowing global endpoint."""

    endpoint: str
    language: str
    cookie: str

    def _headers(self) -> dict[str, str]:
        headers = {
            "content-type": "application/json",
            "accept-language": self.language,
            "origin": "https://appgrowing-global.youcloud.com",
            "referer": "https://appgrowing-global.youcloud.com/",
            "user-agent": "appgrowing-cli/0.1 (+https://appgrowing-global.youcloud.com)",
        }
        if self.cookie:
            headers["cookie"] = self.cookie
        token = os.getenv("APPGROWING_AUTHORIZATION", "").strip()
        if token:
            headers["authorization"] = token
        return headers

    @staticmethod
    def parse_int_maybe(value: Any) -> int:
        if isinstance(value, int):
            return value
        if isinstance(value, float):
            return int(value)
        if isinstance(value, str):
            compact = value.replace(",", "").strip()
            if compact.isdigit():
                return int(compact)
        return 0

    def graphql(
        self,
        *,
        operation_name: str,
        query: str,
        variables: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        payload = {
            "operationName": operation_name,
            "query": query,
            "variables": variables or {},
        }
        headers = self._headers()
        headers["x-operation-name"] = operation_name
        req = Request(
            self.endpoint,
            data=json.dumps(payload).encode("utf-8"),
            headers=headers,
            method="POST",
        )
        try:
            ssl_context = None
            # Default to insecure mode for local convenience unless explicitly disabled.
            skip_verify_raw = os.getenv("APPGROWING_INSECURE_SKIP_VERIFY", "1").strip().lower()
            if skip_verify_raw not in {"0", "false", "no"}:
                ssl_context = ssl._create_unverified_context()
            with urlopen(req, timeout=30, context=ssl_context) as resp:
                raw = resp.read().decode("utf-8", errors="replace")
        except HTTPError as exc:
            body = exc.read().decode("utf-8", errors="replace")
            raise AppGrowingAPIError(f"HTTP {exc.code}: {body}") from exc
        except URLError as exc:
            raise AppGrowingAPIError(f"Network error: {exc}") from exc

        try:
            parsed = json.loads(raw)
        except json.JSONDecodeError as exc:
            raise AppGrowingAPIError(f"Invalid JSON response: {raw[:500]}") from exc

        if "errors" in parsed and parsed["errors"]:
            message = self._errors_to_message(parsed["errors"])
            raise AppGrowingAPIError(message)
        data = parsed.get("data")
        if not isinstance(data, dict):
            raise AppGrowingAPIError("Missing GraphQL data payload.")
        return data

    @staticmethod
    def _errors_to_message(errors: list[dict[str, Any]]) -> str:
        pieces: list[str] = []
        for err in errors[:3]:
            ext = err.get("extensions") if isinstance(err, dict) else {}
            if isinstance(ext, dict):
                code = ext.get("c") or ext.get("code") or "UNKNOWN"
                msg = ext.get("m") or err.get("message") or "Unknown error"
                pieces.append(f"[{code}] {msg}")
            else:
                pieces.append(str(err))
        return "GraphQL error: " + " | ".join(pieces)

    def health_check(self) -> dict[str, Any]:
        data = self.graphql(operation_name="userinfo", query=USERINFO_QUERY, variables={})
        user = data.get("userinfo")
        return {"ok": True, "userinfo_type": user.get("__typename") if isinstance(user, dict) else None}

    def search_app(self, keyword: str, purpose: int = 1, page: int = 1) -> list[dict[str, Any]]:
        data = self.graphql(
            operation_name="searchApp",
            query=SEARCH_APP_QUERY,
            variables={
                "purpose": purpose,
                "keyword": keyword,
                "accurateSearch": 0,
                "hadAdvert": 1,
                "page": page,
            },
        )
        result = data.get("searchAppBrand") or {}
        items = result.get("data") if isinstance(result, dict) else []
        return items if isinstance(items, list) else []

    def search_app_multi_page(self, keyword: str, purpose: int = 1, pages: int = 1) -> list[dict[str, Any]]:
        """Fetch search results across multiple pages."""
        merged: list[dict[str, Any]] = []
        for page in range(1, max(1, pages) + 1):
            items = self.search_app(keyword=keyword, purpose=purpose, page=page)
            if not items:
                break
            merged.extend(items)
        return merged

    def top_compete_app(
        self,
        *,
        keyword: str,
        start_date: str,
        end_date: str,
        markets: list[str] | None,
        page: int = 1,
    ) -> list[dict[str, Any]]:
        _ = keyword
        purpose = int(os.getenv("APPGROWING_PURPOSE", "1"))
        order_candidates = [
            os.getenv("APPGROWING_TOP_ORDER", "").strip(),
            "ADVERTS_DESC",
            "MATERIAL_DESC",
            "ADVERTS",
            "MATERIAL",
        ]
        order_candidates = [x for x in order_candidates if x]
        last_error = None
        for order in order_candidates:
            try:
                data = self.graphql(
                    operation_name="topCompeteApp",
                    query=TOP_COMPETE_APP_QUERY,
                    variables={
                        "page": page,
                        "purpose": purpose,
                        "order": order,
                        "startDate": start_date,
                        "endDate": end_date,
                        "area": markets or None,
                        "category": None,
                    },
                )
                root = data.get("topAppBrandList") or {}
                items = root.get("data") if isinstance(root, dict) else []
                if isinstance(items, list):
                    return items
            except AppGrowingAPIError as exc:
                last_error = exc
                continue
        if last_error:
            raise last_error
        return []

    def top_compete_app_multi_page(
        self,
        *,
        keyword: str,
        start_date: str,
        end_date: str,
        markets: list[str] | None,
        pages: int = 1,
    ) -> list[dict[str, Any]]:
        """Fetch top compete app rows across multiple pages."""
        merged: list[dict[str, Any]] = []
        for page in range(1, max(1, pages) + 1):
            page_items = self.top_compete_app(
                keyword=keyword,
                start_date=start_date,
                end_date=end_date,
                markets=markets,
                page=page,
            )
            if not page_items:
                break
            merged.extend(page_items)
        return merged

    def promote_app_list(
        self,
        *,
        keyword: str,
        start_date: str,
        end_date: str,
        markets: list[str] | None,
        page: int = 1,
        purpose: int | None = None,
        order: str | None = None,
        accurate_search: int | None = None,
    ) -> list[dict[str, Any]]:
        effective_purpose = (
            purpose
            if purpose is not None
            else int(os.getenv("APPGROWING_PROMOTE_PURPOSE", os.getenv("APPGROWING_PURPOSE", "2")))
        )
        effective_order = (
            order
            if order is not None
            else os.getenv("APPGROWING_PROMOTE_ORDER", "material_cnt_desc").strip() or "material_cnt_desc"
        )
        effective_accurate_search = (
            accurate_search
            if accurate_search is not None
            else int(os.getenv("APPGROWING_PROMOTE_ACCURATE_SEARCH", "1"))
        )
        retry_attempts_raw = os.getenv("APPGROWING_PROMOTE_RETRY_ATTEMPTS", "3").strip()
        retry_delay_raw = os.getenv("APPGROWING_PROMOTE_RETRY_DELAY_SECONDS", "1").strip()
        try:
            retry_attempts = max(1, int(retry_attempts_raw))
        except ValueError:
            retry_attempts = 3
        try:
            retry_delay_seconds = max(0.0, float(retry_delay_raw))
        except ValueError:
            retry_delay_seconds = 1.0
        last_error = None
        for attempt in range(retry_attempts):
            try:
                # Keep variables minimal and close to browser requests to reduce 401001/compat issues.
                variables: dict[str, Any] = {
                    "page": page,
                    "purpose": effective_purpose,
                    "order": effective_order,
                    "startDate": start_date,
                    "endDate": end_date,
                    "keyword": keyword,
                }
                if markets:
                    variables["area"] = markets
                if effective_accurate_search is not None:
                    variables["accurateSearch"] = effective_accurate_search
                data = self.graphql(
                    operation_name="promoteAppList",
                    query=PROMOTE_APP_LIST_QUERY,
                    variables=variables,
                )
                root = data.get("topAppBrandList") or {}
                rows = root.get("data") if isinstance(root, dict) else []
                return rows if isinstance(rows, list) else []
            except AppGrowingAPIError as exc:
                last_error = exc
                msg = str(exc).lower()
                is_busy = "system is busy" in msg
                has_next_retry = attempt < (retry_attempts - 1)
                if is_busy and has_next_retry:
                    if retry_delay_seconds > 0:
                        time.sleep(retry_delay_seconds)
                    continue
                raise
        if last_error:
            raise last_error
        return []

    def promote_app_list_multi_page(
        self,
        *,
        keyword: str,
        start_date: str,
        end_date: str,
        markets: list[str] | None,
        pages: int = 1,
        purpose: int | None = None,
        order: str | None = None,
        accurate_search: int | None = None,
    ) -> list[dict[str, Any]]:
        merged: list[dict[str, Any]] = []
        for page in range(1, max(1, pages) + 1):
            rows = self.promote_app_list(
                keyword=keyword,
                start_date=start_date,
                end_date=end_date,
                markets=markets,
                page=page,
                purpose=purpose,
                order=order,
                accurate_search=accurate_search,
            )
            if not rows:
                break
            merged.extend(rows)
        return merged

    def creative_list(
        self,
        *,
        material_id: str,
        start_date: str,
        end_date: str,
        markets: list[str] | None,
        mode: str,
    ) -> list[dict[str, Any]]:
        retry_attempts_raw = os.getenv("APPGROWING_CREATIVE_RETRY_ATTEMPTS", "3").strip()
        retry_delay_raw = os.getenv("APPGROWING_CREATIVE_RETRY_DELAY_SECONDS", "2").strip()
        try:
            retry_attempts = max(1, int(retry_attempts_raw))
        except ValueError:
            retry_attempts = 3
        try:
            retry_delay_seconds = max(0.0, float(retry_delay_raw))
        except ValueError:
            retry_delay_seconds = 2.0

        order_candidates = [
            os.getenv("APPGROWING_CREATIVE_ORDER", "").strip(),
            "MAX_DT_DESC",
            "AD_DESC",
            "LATEST",
        ]
        order_candidates = [x for x in order_candidates if x]
        top_limit = 50 if mode in {"top_head", "new_head"} else 100
        last_error = None
        for order in order_candidates:
            for attempt in range(retry_attempts):
                try:
                    data = self.graphql(
                        operation_name="creativeList",
                        query=CREATIVE_LIST_QUERY,
                        variables={
                            "materialId": material_id,
                            "startDate": start_date,
                            "endDate": end_date,
                            "isAllDate": 0,
                            "media": None,
                            "area": markets or None,
                            "campaign": None,
                            "platform": None,
                            "language": None,
                            "order": order,
                            "page": 1,
                            "topLimit": top_limit,
                        },
                    )
                    root = data.get("creativeList") or {}
                    items = root.get("data") if isinstance(root, dict) else []
                    if isinstance(items, list):
                        return items
                except AppGrowingAPIError as exc:
                    last_error = exc
                    msg = str(exc).lower()
                    is_busy = "system is busy" in msg
                    has_next_retry = attempt < (retry_attempts - 1)
                    if is_busy and has_next_retry:
                        if retry_delay_seconds > 0:
                            time.sleep(retry_delay_seconds)
                        continue
                    break
        if last_error:
            raise last_error
        return []

    def guess_material_id(self, keyword: str) -> str | None:
        """Best-effort guess of material id for creative queries.

        Priority:
        1) APPGROWING_DEFAULT_MATERIAL_ID env
        2) first appBrand.id returned by searchApp
        """
        env_id = os.getenv("APPGROWING_DEFAULT_MATERIAL_ID", "").strip()
        if env_id:
            return env_id
        items = self.search_app(keyword=keyword, purpose=1, page=1)
        for row in items:
            app_brand = row.get("appBrand") if isinstance(row, dict) else None
            if isinstance(app_brand, dict):
                candidate = app_brand.get("id")
                if isinstance(candidate, str) and candidate:
                    return candidate
        return None

    def material_list(
        self,
        *,
        keyword: str,
        start_date: str,
        end_date: str,
        markets: list[str] | None,
        purpose: int = 1,
        pages: int = 1,
    ) -> list[dict[str, Any]]:
        """Fetch materialList rows by keyword/date filters."""
        order_candidates = [
            os.getenv("APPGROWING_MATERIAL_ORDER", "").strip(),
            "MAX_DT_DESC",
            "CNT_AD_ID_DESC",
            "MATERIAL_DESC",
            "LATEST",
        ]
        order_candidates = [x for x in order_candidates if x]
        merged: list[dict[str, Any]] = []
        last_error = None
        for page in range(1, max(1, pages) + 1):
            page_rows: list[dict[str, Any]] = []
            for order in order_candidates:
                try:
                    data = self.graphql(
                        operation_name="materialList",
                        query=MATERIAL_LIST_QUERY,
                        variables={
                            "purpose": purpose,
                            "startDate": start_date,
                            "endDate": end_date,
                            "keyword": keyword,
                            "area": markets or None,
                            "order": order,
                            "page": page,
                            "accurateSearch": 0,
                        },
                    )
                    root = data.get("materialList") or {}
                    rows = root.get("data") if isinstance(root, dict) else []
                    if isinstance(rows, list):
                        page_rows = rows
                        break
                except AppGrowingAPIError as exc:
                    last_error = exc
                    continue
            if not page_rows:
                if page == 1 and last_error is not None:
                    raise last_error
                break
            merged.extend(page_rows)
        return merged

    def discover_material_ids_by_keyword(
        self,
        *,
        keyword: str,
        start_date: str,
        end_date: str,
        markets: list[str] | None,
        pages: int = 1,
        limit: int = 5,
    ) -> list[str]:
        """Discover candidate material ids via real materialList query."""
        rows = self.material_list(
            keyword=keyword,
            start_date=start_date,
            end_date=end_date,
            markets=markets,
            pages=pages,
            purpose=int(os.getenv("APPGROWING_PURPOSE", "1")),
        )
        ids: list[str] = []
        seen: set[str] = set()
        for row in rows:
            material = row.get("material") if isinstance(row, dict) else None
            if not isinstance(material, dict):
                continue
            material_id = material.get("id")
            if isinstance(material_id, str) and material_id and material_id not in seen:
                seen.add(material_id)
                ids.append(material_id)
            if len(ids) >= limit:
                break
        return ids

    @staticmethod
    def _default_creative_types() -> list[int]:
        raw = os.getenv("APPGROWING_CREATIVE_TYPES", "201,202,203,102,104,103,301,100,105").strip()
        result: list[int] = []
        for token in raw.split(","):
            compact = token.strip()
            if compact.isdigit():
                result.append(int(compact))
        return result

    def filter_list(self) -> list[dict[str, Any]]:
        data = self.graphql(operation_name="filterList", query=FILTER_LIST_QUERY, variables={})
        rows = data.get("filterList")
        return rows if isinstance(rows, list) else []

    def area_continent_map(self) -> dict[str, str]:
        mapping: dict[str, str] = {}
        for row in self.filter_list():
            if not isinstance(row, dict):
                continue
            data_rows = row.get("data")
            if not isinstance(data_rows, list):
                continue
            for item in data_rows:
                if not isinstance(item, dict):
                    continue
                cc = str(item.get("cc") or "").strip().upper()
                location = str(item.get("location") or "").strip()
                if cc and location:
                    mapping[cc] = location
        return mapping

    def _launch_rows(
        self,
        *,
        operation_name: str,
        query: str,
        root_field: str,
        brand_id: str,
        start_date: str,
        end_date: str,
        purpose: int,
    include_purpose: bool = True,
        markets: list[str] | None = None,
        dimension: str = "material",
        creative_types: list[int] | None = None,
    ) -> list[dict[str, Any]]:
        retry_attempts_raw = os.getenv("APPGROWING_DISTRIBUTION_RETRY_ATTEMPTS", "3").strip()
        retry_delay_raw = os.getenv("APPGROWING_DISTRIBUTION_RETRY_DELAY_SECONDS", "1").strip()
        try:
            retry_attempts = max(1, int(retry_attempts_raw))
        except ValueError:
            retry_attempts = 3
        try:
            retry_delay_seconds = max(0.0, float(retry_delay_raw))
        except ValueError:
            retry_delay_seconds = 1.0

        variables: dict[str, Any] = {
            "brandId": brand_id,
            "startDate": start_date,
            "endDate": end_date,
            "isAllDate": 0,
            "dimension": dimension,
            "creativeType": creative_types or self._default_creative_types(),
        }
        if include_purpose:
            variables["purpose"] = purpose
        if markets:
            variables["area"] = markets
        last_error = None
        for attempt in range(retry_attempts):
            try:
                data = self.graphql(operation_name=operation_name, query=query, variables=variables)
                rows = data.get(root_field)
                return rows if isinstance(rows, list) else []
            except AppGrowingAPIError as exc:
                last_error = exc
                msg = str(exc).lower()
                is_busy = "system is busy" in msg
                has_next_retry = attempt < (retry_attempts - 1)
                if is_busy and has_next_retry:
                    if retry_delay_seconds > 0:
                        time.sleep(retry_delay_seconds)
                    continue
                raise
        if last_error:
            raise last_error
        return []

    def media_launch(
        self,
        *,
        brand_id: str,
        start_date: str,
        end_date: str,
        purpose: int,
        markets: list[str] | None = None,
        dimension: str = "material",
        creative_types: list[int] | None = None,
    ) -> list[dict[str, Any]]:
        return self._launch_rows(
            operation_name="mediaLaunch",
            query=MEDIA_LAUNCH_QUERY,
            root_field="mediaLaunch",
            brand_id=brand_id,
            start_date=start_date,
            end_date=end_date,
            purpose=purpose,
            include_purpose=True,
            markets=markets,
            dimension=dimension,
            creative_types=creative_types,
        )

    def region_launch(
        self,
        *,
        brand_id: str,
        start_date: str,
        end_date: str,
        purpose: int,
        markets: list[str] | None = None,
        dimension: str = "material",
        creative_types: list[int] | None = None,
    ) -> list[dict[str, Any]]:
        return self._launch_rows(
            operation_name="regionLaunch",
            query=REGION_LAUNCH_QUERY,
            root_field="regionLaunch",
            brand_id=brand_id,
            start_date=start_date,
            end_date=end_date,
            purpose=purpose,
            include_purpose=True,
            markets=markets,
            dimension=dimension,
            creative_types=creative_types,
        )

    def language_launch(
        self,
        *,
        brand_id: str,
        start_date: str,
        end_date: str,
        purpose: int,
        markets: list[str] | None = None,
        dimension: str = "material",
        creative_types: list[int] | None = None,
    ) -> list[dict[str, Any]]:
        return self._launch_rows(
            operation_name="appLanguage",
            query=APP_LANGUAGE_QUERY,
            root_field="appLanguage",
            brand_id=brand_id,
            start_date=start_date,
            end_date=end_date,
            purpose=purpose,
            include_purpose=False,
            markets=markets,
            dimension=dimension,
            creative_types=creative_types,
        )

    def app_material_list(
        self,
        *,
        app_brand_id: str,
        start_date: str,
        end_date: str,
        purpose: int = 2,
        creative_types: list[int] | None = None,
        material_ratio: list[str] | None = None,
        accurate_search: int = 1,
        order: str = "impression_inc_2y_desc",
        is_new: int = 1,
        pages: int = 1,
        detailed: bool = False,
    ) -> dict[str, Any]:
        """Fetch app material list and aggregate total/data across pages.

        Args:
            pages: Max pages to fetch. Use 0 to fetch all available pages.
        """
        merged_data: list[dict[str, Any]] = []
        total = 0
        limit = 0
        last_error = None
        retry_attempts_raw = os.getenv("APPGROWING_MATERIAL_RETRY_ATTEMPTS", "3").strip()
        retry_delay_raw = os.getenv("APPGROWING_MATERIAL_RETRY_DELAY_SECONDS", "1").strip()
        try:
            retry_attempts = max(1, int(retry_attempts_raw))
        except ValueError:
            retry_attempts = 3
        try:
            retry_delay_seconds = max(0.0, float(retry_delay_raw))
        except ValueError:
            retry_delay_seconds = 1.0

        page = 1
        page_cap = pages if pages > 0 else None
        while True:
            if page_cap is not None and page > page_cap:
                break
            page_root: dict[str, Any] | None = None
            variables: dict[str, Any] = {
                "purpose": purpose,
                "startDate": start_date,
                "endDate": end_date,
                "isNew": is_new,
                "field": "all",
                "order": order,
                "page": page,
                "accurateSearch": accurate_search,
                "appBrand": app_brand_id,
            }
            if not detailed:
                variables["creativeType"] = creative_types or self._default_creative_types()
            if material_ratio and not detailed:
                variables["materialRatio"] = material_ratio
            for attempt in range(retry_attempts):
                try:
                    data = self.graphql(
                        operation_name="appMaterialList",
                        query=APP_MATERIAL_LIST_DETAIL_QUERY if detailed else APP_MATERIAL_LIST_QUERY,
                        variables=variables,
                    )
                    root = data.get("materialList")
                    page_root = root if isinstance(root, dict) else {}
                    break
                except AppGrowingAPIError as exc:
                    last_error = exc
                    msg = str(exc).lower()
                    is_busy = "system is busy" in msg
                    has_next_retry = attempt < (retry_attempts - 1)
                    if is_busy and has_next_retry:
                        if retry_delay_seconds > 0:
                            time.sleep(retry_delay_seconds)
                        continue
                    raise
            if page_root is None:
                break
            if page == 1:
                total = self.parse_int_maybe(page_root.get("total"))
                limit = self.parse_int_maybe(page_root.get("limit"))
            rows = page_root.get("data")
            if not isinstance(rows, list) or not rows:
                break
            merged_data.extend(rows)
            if len(rows) < max(1, limit):
                break
            if total > 0 and len(merged_data) >= total:
                break
            page += 1
        if not merged_data and last_error is not None:
            raise last_error
        return {"total": total, "limit": limit, "data": merged_data}
