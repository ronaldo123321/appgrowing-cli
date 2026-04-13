# appgrowing-cli

广告监控 `AppGrowing Global` 的 CLI 工具（面向 agent 和自动化任务）。

## Goal

把AppGrowing竞品监控流程的核心流程落地为可重复执行的命令行工作流：

- 投放趋势：双周期对比、排名变化、竞品筛选、下钻分析
- 创意趋势：头部素材格局、新头部创意、新创意趋势
- 一键聚合：`trend competitor-table` / `trend creative-insights` 输出 JSON/CSV

## Command Surface (Draft)

```bash
# auth
appgrowing auth login
appgrowing auth status

# trend
appgrowing trend ranking snapshot --keyword "ai note taker" --start 2026-03-01 --end 2026-03-31 --accurate-search 0 --order material_cnt_desc
appgrowing trend ranking compare --keyword "ai note taker" --this-start 2026-03-01 --this-end 2026-03-31 --last-start 2026-02-01 --last-end 2026-02-28 --accurate-search 0 --order material_cnt_desc
appgrowing trend promote-ranking snapshot --keyword "ai note taker" --start 2026-03-24 --end 2026-03-31 --accurate-search 0 --order material_cnt_desc
appgrowing trend promote-ranking compare --keyword "ai note taker" --this-start 2026-03-24 --this-end 2026-03-31 --last-start 2026-03-17 --last-end 2026-03-23 --accurate-search 0 --order material_cnt_desc
appgrowing trend app-distribution --app-brand-id "<app_brand_id>" --start 2026-03-10 --end 2026-04-08 --purpose 2 --top-n 10
appgrowing trend app-material-insights --app-brand-id "<app_brand_id>" --this-start 2026-04-03 --this-end 2026-04-09 --last-start 2026-03-27 --last-end 2026-04-02 --purpose 2 --top-n 10
appgrowing trend creative-insights --keyword "translate" --start 2026-04-03 --end 2026-04-09 --top-rank-limit 15 --min-rank-change 3 --pick-top-n 3 --out-file ./out_real/creative-rule-groups.json --csv-file ./out_real/creative-rule-groups.csv
appgrowing trend competitor-table --keyword "translate" --start 2026-04-03 --end 2026-04-09 --top-rank-limit 15 --min-rank-change 3 --pick-top-n 3 --out-file ./out_real/competitor-table.json --csv-file ./out_real/competitor-table.csv
appgrowing trend pick-competitors --compare-json ./out/compare.json --top 15 --min-rank-change 3
```

## Project Artifacts

- CLI 详细规格：`docs/cli-spec.md`
- 需求对齐矩阵（含文档评论需求）：`docs/requirements-matrix.md`
- 请求/结果 schema：`schemas/`

## Notes

- 当前仓库先落地规格与 schema，便于你和团队快速并行开发实现。
- 所有命令建议支持 `--json`，并保证退出码语义明确（0 成功，非 0 失败）。

## Quick Start (Scaffold)

```bash
cd /Users/gaobo/PycharmProjects/appgrowing-cli
pip install -e .

# 查看命令树
appgrowing --help
appgrowing trend --help

# 配置真实登录态（自动从浏览器抽取 .youcloud.com Cookie）
appgrowing auth login
# 指定浏览器抽取
appgrowing auth login --from-browser chrome

# 查看登录态和 API 探测
appgrowing auth status

# 跑聚合创意洞察（只使用真实 API）
appgrowing trend creative-insights \
  --keyword "Translate" \
  --start 2026-04-03 --end 2026-04-09 \
  --top-rank-limit 15 --min-rank-change 3 --pick-top-n 3 \
  --material-pages 0 \
  --out-file ./out_real/creative-insights.json \
  --csv-file ./out_real/creative-insights.csv
```

## Quick Install (Users)

推荐用 `uv tool` 安装（隔离环境，适合 CLI）：

```bash
# install
uv tool install appgrowing-cli

# verify
appgrowing --help

# upgrade
uv tool upgrade appgrowing-cli

# uninstall
uv tool uninstall appgrowing-cli
```

如果你要固定某个版本：

```bash
uv tool install appgrowing-cli==0.1.0
```

首次使用建议先完成登录态配置：

```bash
# 登录（自动从浏览器读取 .youcloud.com Cookie）
appgrowing auth login

# 检查 API 登录态
appgrowing auth status
```


## Promote Ranking Commands

下面这两个命令用于你提到的周榜场景（基于 `promoteAppList`）：

```bash
# 1) 单周期榜单（可分别跑“本周”和“上周”）
appgrowing trend promote-ranking snapshot \
  --keyword "Calorie" \
  --start 2026-03-10 --end 2026-04-08 \
  --purpose 2 \
  --accurate-search 0 \
  --order material_cnt_desc \
  --pages 1 \
  --out-file ./out_real/promote-snapshot.json \
  --csv-file ./out_real/promote-snapshot.csv \
  --top-n 10

# 2) 双周期对比（输出排名/素材数等变化）
appgrowing trend promote-ranking compare \
  --keyword "Calorie" \
  --this-start 2026-03-10 --this-end 2026-04-08 \
  --last-start 2026-02-10 --last-end 2026-03-09 \
  --purpose 2 \
  --accurate-search 0 \
  --order material_cnt_desc \
  --pages 1 \
  --out-file ./out_real/promote-compare.json \
  --csv-file ./out_real/promote-compare.csv \
  --top-n 10
```

字段说明：

- `promote-snapshot.csv`：单周期快照（当前 rank / ad_count / material_count 等）
- `promote-compare.csv`：双周期变化（`current_*` vs `previous_*` + `*_change`）

## Trend Ranking CSV（最终口径）

`trend ranking snapshot` 和 `trend ranking compare` 的 `--csv-file` 统一输出下面 8 列（仅这 8 列）：

- `赛道关键词`
- `周期`
- `产品名称`
- `Appgrowing链接`
- `系统平台`
- `排名+变化`
- `广告数+变化占比`
- `素材数+变化占比`

字段格式示例：

- `排名+变化`：`10 +3`（表示当前排名 10，较上周期上升 3）
- `广告数+变化占比`：`1500 +10%`
- `素材数+变化占比`：`1000 -10%`

周期说明：

- 命令会按查询区间自动输出 `每周` 或 `每月`。
- 若你在外部报表中拆分双周期行，可把上一周期标记为 `上周` / `上月`。

## Competitor Table Command（聚合版）

这个命令会整合此前多个命令的能力，一次性产出你要的竞品表：

1. 按 `关键词 + 周期` 抓取榜单（自动对比上一个等长周期）。
2. 先取 `pick-top-n`（默认 3）个头部竞品（按当前 rank）。
3. 再追加前 `top-rank-limit`（默认 15）里 `|rank_change| > min-rank-change`（默认 3）的竞品。
4. 两组做并集；如果多关键词结果重叠，优先保留 `current_rank` 更靠前的记录。
4. 对每个竞品补齐：
   - 渠道占比（channel）
   - 国家地区占比（按大区）
   - 语言分布占比（TopN）
   - 素材类型数量+占比
   - TopN 视频素材链接 / 图片素材链接

示例：

```bash
appgrowing trend competitor-table \
  --keyword "Translate" \
  --start 2026-04-03 --end 2026-04-09 \
  --top-rank-limit 15 \
  --min-rank-change 3 \
  --pick-top-n 3 \
  --accurate-search 0 \
  --order material_cnt_desc \
  --out-file ./out_real/competitor-table.json \
  --csv-file ./out_real/competitor-table.csv
```

## Creative Insights Command（无 AI 版）

按规则抓素材并分组落盘（不做 AI 分析），并且**先使用与 `competitor-table` 相同的竞品筛选逻辑**：

- 先按 `关键词 + 周期` 拉榜单，自动对比上一个等长周期
- 竞品筛选 = `TopN（pick-top-n）` + `前 top-rank-limit 且 |rank_change| > min-rank-change` 的并集
- 若重复，保留 `current_rank` 更靠前的记录

- 头部素材格局：全量素材按累计曝光排序后的前 `top-head-percent`（默认10%）
- 新头部创意：新素材按累计曝光排序后的前 `new-head-percent`（默认20%）
- 新创意趋势：新素材按累计曝光排序后的后 `new-trend-percent`（默认20%）
- 尺寸映射规则：
  - 图片：仅保留 `1:1`、`4:5`
  - 视频：仅保留 `9:16`
- 对每个分组按“尺寸去重”输出摘要与样例素材链接
- 可选明细增强：传 `--top-material-details N` 后，会基于扩展版 `appMaterialList` 主响应，为每个分组下每种素材类型的前 N 个素材补充 `captions`、`areas`、`platforms`、`campaigns`、`first_seen`、`last_seen`、`impression_inc_2y`

```bash
appgrowing trend creative-insights \
  --keyword "Translate" \
  --start 2026-04-03 --end 2026-04-09 \
  --top-rank-limit 15 \
  --min-rank-change 3 \
  --pick-top-n 3 \
  --accurate-search 0 \
  --order material_cnt_desc \
  --ranking-pages 1 \
  --material-pages 3 \
  --top-material-details 5 \
  --out-file ./out_real/creative-rule-groups.json \
  --csv-file ./out_real/creative-rule-groups.csv
```

兼容说明：

- 旧命令 `creative-rule-groups` 仍可用（向后兼容），建议后续统一改为 `creative-insights`。
- `--material-pages` 默认只抓前几页；传 `--material-pages 0` 可自动翻完全部分页。
- `--top-material-details` 默认关闭；开启后主查询会直接切换到带明细字段的 `appMaterialList`。
- 明细是同一批 `materialList` 返回里的素材属性快照，不再额外补第二轮查询，也不再逐个调用 `creativeList(material_id=...)`。
- 如果开启了 `--top-material-details` 且上游 `appMaterialList` 失败，命令会直接报错，因为这时明细已经是主查询结果的一部分。

本次工程调整：

- 修正了 `--top-material-details` 的实现语义：不再采用“先轻量查询、再补明细查询”的两阶段模式，而是直接把明细字段并入主 `appMaterialList` 查询。
- 收敛了明细版查询的 GraphQL 形状与 variables，去掉了会触发不稳定行为的默认筛选参数组合，使真实环境下可以稳定拿到 `captions`、`areas`、`platforms`、`campaigns`、`impression_inc_2y`、`first_seen`、`last_seen`。
- 明细输出中的 `areas` 现在只保留 Top 5 国家/地区，顺序沿用接口原始返回顺序，避免单条素材挂出过长国家列表。
- 失败语义也同步收紧：开启 `--top-material-details` 后，如果主 `appMaterialList` 明细查询失败，命令直接报错，不再返回“主结果成功但明细为空”的误导性输出。

## App Distribution Command

用于获取某个竞品 App 的：

- 渠道占比（`channel_distribution`）
- 国家地区占比（按大洲聚合，`continent_distribution`）
- 语言分布占比 TopN（`language_distribution_top`）

```bash
appgrowing trend app-distribution \
  --app-brand-id "3J-GwYqX47mmemoSELy7ZQ==" \
  --start 2025-10-12 \
  --end 2026-04-09 \
  --purpose 2 \
  --dimension material \
  --top-n 10 \
  --out-file ./out_real/app-distribution.json
```

输出文件：`out_real/app-distribution.json`

核心输出字段：

- `channel_distribution[]`：渠道维度分布（`channel_name`, `material`, `ratio`）
- `continent_distribution[]`：大洲维度分布（`continent`, `material`, `ratio`）
- `language_distribution_top[]`：语言 TopN（`language_code`, `language_name`, `material`, `ratio`）
- `meta.language_error`：语言接口失败时的错误信息（成功时为 `null`）

## App Material Insights Command

用于获取某个竞品 App 的素材变化和素材链接（基于 `appMaterialList`）：

- 素材数量（本周期 / 上周期）与变化率
- Top10 视频素材链接
- Top10 图片素材链接

```bash
appgrowing trend app-material-insights \
  --app-brand-id "3J-GwYqX47mmemoSELy7ZQ==" \
  --this-start 2026-04-03 \
  --this-end 2026-04-09 \
  --last-start 2026-03-27 \
  --last-end 2026-04-02 \
  --purpose 2 \
  --pages 1 \
  --top-n 10 \
  --out-file ./out_real/app-material-insights.json
```

输出文件：`out_real/app-material-insights.json`

核心输出字段：

- `material_count_current` / `material_count_previous`
- `material_count_change` / `material_count_change_ratio`
- `top_video_materials[]`（`material_id`, `size`, `duration_ms`, `link`）
- `top_image_materials[]`（`material_id`, `size`, `duration_ms`, `link`）

> 说明：
> - 现已固定使用真实 AppGrowing GraphQL API。
> - `auth login` 只支持自动抽取浏览器 Cookie（优先 `uv run --with browser-cookie3` 子进程，失败再本进程兜底）。
> - `trend ranking snapshot/compare` 支持重复传入 `--keyword`（多关键词聚合）和 `--pages`（分页抓取）。
> - `trend ranking snapshot/compare` 支持 `--accurate-search` 与 `--order`，用于对齐网页筛选行为（默认 `0` + `material_cnt_desc`）。
> - `trend ranking snapshot/compare` 的 `--csv-file` 现输出固定 8 列：
>   - `赛道关键词`、`周期`、`产品名称`、`Appgrowing链接`、`系统平台`
>   - `排名+变化`、`广告数+变化占比`、`素材数+变化占比`
> - `周期` 自动按区间判断：周区间输出 `每周`，月区间输出 `每月`（报表侧可映射为 `上周/上月`）。
> - `trend ranking snapshot/compare` 支持一键落盘：
>   - `--out-file`（完整 JSON）
>   - `--csv-file`（业务表 CSV）
> - 新增 `trend promote-ranking snapshot/compare` 两个专用命令（基于 `promoteAppList`）：
>   - 支持 `--accurate-search 0`（无精准匹配）
>   - 支持 `--order material_cnt_desc`（按素材数降序）
>   - 输出字段重点覆盖：`rank/app_name/country/publish_platform/media_info/ad_count/material_count/video_fragment_count`
>   - 支持 `--out-file`（落 JSON）和 `--csv-file`（落表格 CSV）
>   - 支持 `--top-n`（仅限制 CSV 导出前 N 行）
> - 新增 `trend app-distribution`（基于 `mediaLaunch + regionLaunch + filterList + appLanguage`）：
>   - 输出 `channel_distribution`（渠道占比）
>   - 输出 `continent_distribution`（国家地区按大洲聚合占比）
>   - 输出 `language_distribution_top`（语言分布 TopN；默认 10）
> - 趋势查询优先对齐页面 `promoteAppList` 链路（如你抓包所示）。
> - 当前版本API 错误会直接失败并返回错误信息。
> - 若遇到权限或枚举值限制，可通过环境变量覆盖排序枚举：
>   - `APPGROWING_TOP_ORDER`
>   - `APPGROWING_CREATIVE_ORDER`
>   - `APPGROWING_MATERIAL_ORDER`
> - 当前默认关闭 SSL 严格校验（无需手动设置）；如需开启严格校验可设置：
>   - `APPGROWING_INSECURE_SKIP_VERIFY=0`
