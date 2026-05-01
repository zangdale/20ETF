# 20ETF

本项目为 **20 只 ETF** 在固定长区间上的回测与最优配置展示：单页报告 `index.html`（深色主题、图表与可排序表格）。

## 主要内容

- **`index.html`**：组合核心指标、图表（Chart.js）、大类配置、20 只 ETF 明细（含成立日期、回测算术字段等）。若同目录存在 **`etf_metrics.json`**（由 `compute_page_metrics.py` 生成），页面加载时会合并日频指标。数据来源与口径以页面内「风险提示」为准。
- **`scripts/fetch_eastmoney_etf_metadata.py`**：从东方财富·天天基金公开页面拉取各代码的 **场内简称** 与 **成立日期**，用于与报告中的产品信息交叉核对（不重新计算收益、回撤、波动、夏普等指标）。
- **`scripts/compute_page_metrics.py`** 与 **`scripts/etf_universe.json`**：每日拉取东财 ETF 日 K，计算表格与组合摘要并写出 `etf_metrics.json`。

## 本地预览

在项目根目录执行：

```bash
python3 -m http.server 8080
```

浏览器打开 `http://localhost:8080/index.html`。**不要**用 `file://` 直接双击打开，以便脚本与外部 CDN（Chart.js 等）正常加载。

## 核对 ETF 公开档案

默认使用与 `index.html` 一致的 20 个代码：

```bash
python3 scripts/fetch_eastmoney_etf_metadata.py --report-start 2006-04-29 -o etf_meta.json
```

导出到标准输出：

```bash
python3 scripts/fetch_eastmoney_etf_metadata.py --report-start 2006-04-29
```

自定义代码列表（一行一个 6 位代码，`#` 行为注释）：

```bash
python3 scripts/fetch_eastmoney_etf_metadata.py --codes codes.txt
```

## 每日刷新页面收益指标（日频 K 线）

`scripts/compute_page_metrics.py` 从东方财富拉取各 ETF **前复权日 K**，按 `scripts/etf_universe.json` 的权重计算：

- 单只：区间累计收益、最大回撤、年化波动率、夏普（无风险利率 0）  
- 组合：等权与「最优权重」**买入持有**曲线（全体公共交易日）、期末资产、累计收益、复合年化、回撤与回撤时长摘要  

口径为**可交易日线**，与页面内嵌的「理论推算 / 指数拼接」不同；生成 `etf_metrics.json` 后，用 HTTP 打开 `index.html` 会自动合并到表格、指标卡与主图。

```bash
python3 scripts/compute_page_metrics.py --report-start 2006-04-29 -o etf_metrics.json
python3 -m http.server 8080
# 浏览器打开 http://localhost:8080/index.html（勿用 file://，否则无法 fetch JSON）
```

可与 cron 或 GitHub Actions 定时任务每日运行；`etf_metrics.json` 默认已列入 `.gitignore`，部署前如需静态携带请自行复制到站点根目录。

仅需 Python 3 标准库（`fetch_eastmoney_etf_metadata.py` 与 `compute_page_metrics.py`），**需联网**。

## CI 部署（Cloudflare Pages）

仓库含 GitHub Actions：推送时在 `dist/` 中复制根目录全部 `*.html` 并由 Wrangler 部署到 Cloudflare Pages（项目名示例：`20etf`）。

需在仓库 **Secrets** 中配置：`CLOUDFLARE_API_TOKEN`、`CLOUDFLARE_ACCOUNT_ID`。具体绑定以你的工作区为准。

## 免责声明

本仓库中的数值与图表用于研究与展示，不构成投资建议。使用前请阅读 `index.html` 中的风险提示及产品法律文件。
