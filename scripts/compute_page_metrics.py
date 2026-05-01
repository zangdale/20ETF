#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
按东方财富日 K（前复权 fqt=1）拉取 ETF 收盘，计算页面用到的单只指标与组合买入持有指标。

口径（与页面脚注区分见下方说明）：
  - 单只 ETF：窗口起点 = max(报告起点, 成立日, 行情首条)；终点 = --end（默认昨天）。
  - 累计收益率：区间首尾收盘价变动，百分比。
  - 最大回撤：区间内净值峰值→谷底（百分比，输出为负数与页面表格一致）。
  - 年化波动率：日简单收益率样本标准差 × sqrt(252)，百分比。
  - 夏普：日简单收益均值 / 日标准差 × sqrt(252)，无风险利率默认为 0。
  - 组合：各标的日收盘价按「表格权重」买入持有（不对齐日则取全体都有报价的交易日交集）。
  说明：页面原「20 年理论推算」可能含指数拼接；本脚本仅为**可交易净值日频**近似，供每日刷新展示。

用法:
  python3 scripts/compute_page_metrics.py
  python3 scripts/compute_page_metrics.py -o etf_metrics.json
  python3 scripts/compute_page_metrics.py --report-start 2006-04-29 --universe scripts/etf_universe.json

仅标准库；需联网。
"""

from __future__ import annotations

import argparse
import json
import math
import statistics
import sys
import time
import urllib.error
import urllib.request
from dataclasses import asdict, dataclass
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Any, Iterable

USER_AGENT = (
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
    "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
)

EM_REQUEST_HEADERS = {
    "User-Agent": USER_AGENT,
    "Accept": "application/json, text/plain, */*",
    "Accept-Language": "zh-CN,zh;q=0.9,en;q=0.8",
    "Referer": "https://quote.eastmoney.com/",
}

TRADING_DAYS_PER_YEAR = 252


@dataclass
class EtfSeriesStats:
    code: str
    window_start: str
    window_end: str
    trading_days: int
    cum_return_pct: float
    max_drawdown_pct: float
    volatility_ann_pct: float
    sharpe: float


@dataclass
class PortfolioStats:
    name: str
    final_cny: float
    cum_return_pct: float
    cagr_pct: float
    max_drawdown_pct: float
    sharpe: float
    drawdown_months: float | None
    recovery_months: float | None


def _fmt_d(d: date) -> str:
    return d.strftime("%Y%m%d")


def _parse_iso(s: str) -> date:
    y, m, dd = map(int, s.split("-"))
    return date(y, m, dd)


def secid_for_etf(code: str) -> str:
    c = code.strip()
    if c.startswith(
        (
            "159",
            "160",
            "161",
            "162",
            "163",
            "164",
            "165",
            "166",
            "167",
            "168",
            "169",
        )
    ):
        return f"0.{c}"
    return f"1.{c}"


def fetch_json(url: str, timeout: int = 60) -> dict[str, Any]:
    req = urllib.request.Request(url, headers=EM_REQUEST_HEADERS)
    with urllib.request.urlopen(req, timeout=timeout) as resp:
        return json.loads(resp.read().decode("utf-8", errors="ignore"))


def fetch_kline_year_slice(secid: str, d0: date, d1: date, retries: int = 8) -> list[str]:
    """返回 klines 字符串列表。"""
    beg, end = _fmt_d(d0), _fmt_d(d1)
    qs = (
        "https://push2his.eastmoney.com/api/qt/stock/kline/get?"
        "fields1=f1,f2,f3,f4,f5,f6&fields2=f51,f52,f53,f54,f55,f56,f57,f58,f59,f60,f61"
        "&ut=433fd2d0e98eaf36adbe086ea791a2e0&klt=101&fqt=1"
        f"&secid={secid}&beg={beg}&end={end}"
    )
    last_err: Exception | None = None
    for attempt in range(retries):
        try:
            j = fetch_json(qs)
            data = j.get("data") or {}
            kl = data.get("klines")
            if kl is None:
                return []
            return list(kl)
        except (urllib.error.URLError, urllib.error.HTTPError, json.JSONDecodeError, OSError) as e:
            last_err = e
            time.sleep(min(8.0, 0.9 * (2**attempt)))
    raise RuntimeError(f"kline fetch failed {secid} {beg}-{end}: {last_err}")


def iter_year_ranges(d_start: date, d_end: date) -> Iterable[tuple[date, date]]:
    y = d_start.year
    while y <= d_end.year:
        a = date(y, 1, 1)
        b = date(y, 12, 31)
        if a < d_start:
            a = d_start
        if b > d_end:
            b = d_end
        yield a, b
        y += 1


def load_closes_through_years(
    secid: str, d_start: date, d_end: date, inter_sleep: float = 0.35
) -> dict[date, float]:
    """按自然年分片请求并合并为 date->close（复权收盘）。单年失败则跳过该年，避免整只标的被清空。"""
    out: dict[date, float] = {}
    for a, b in iter_year_ranges(d_start, d_end):
        if a > b:
            continue
        try:
            lines = fetch_kline_year_slice(secid, a, b)
        except RuntimeError as e:
            print(f"warn: slice skip {secid} {_fmt_d(a)}-{_fmt_d(b)}: {e}", file=sys.stderr)
            time.sleep(inter_sleep * 3)
            continue
        for line in lines:
            parts = line.split(",")
            if len(parts) < 3:
                continue
            ds = parts[0].strip()
            try:
                dt = datetime.strptime(ds, "%Y-%m-%d").date()
                close = float(parts[2])
            except (TypeError, ValueError):
                continue
            out[dt] = close
        time.sleep(inter_sleep)
    return out


def series_stats_for_window(closes: list[float]) -> tuple[float, float, float, float]:
    """(cum_return_pct, max_drawdown_pct, vol_ann_pct, sharpe)"""
    if len(closes) < 2:
        raise ValueError("need at least 2 prices")
    cum = (closes[-1] / closes[0] - 1.0) * 100.0
    peak = closes[0]
    max_dd = 0.0
    for p in closes:
        if p > peak:
            peak = p
        dd = p / peak - 1.0
        if dd < max_dd:
            max_dd = dd
    max_dd_pct = max_dd * 100.0
    rets = [closes[i] / closes[i - 1] - 1.0 for i in range(1, len(closes))]
    sd = statistics.pstdev(rets)
    mu = statistics.mean(rets)
    vol = sd * math.sqrt(TRADING_DAYS_PER_YEAR) * 100.0
    sharpe = (mu / sd) * math.sqrt(TRADING_DAYS_PER_YEAR) if sd > 1e-12 else 0.0
    return cum, max_dd_pct, vol, sharpe


def equity_curve_buy_hold(
    price_matrix: list[list[float]],
    weights_pct: list[float],
    initial: float,
) -> list[float]:
    """price_matrix[t][i] 为第 t 日第 i 只收盘价；weights_pct 与列顺序一致，和应为 100。"""
    if not price_matrix or not price_matrix[0]:
        return []
    w = [wp / 100.0 for wp in weights_pct]
    p0 = price_matrix[0]
    shares = [(initial * w[i]) / p0[i] for i in range(len(p0))]
    curve: list[float] = []
    for row in price_matrix:
        curve.append(sum(shares[i] * row[i] for i in range(len(row))))
    return curve


def max_drawdown_story(
    values: list[float], dates: list[date]
) -> tuple[float, float | None, float | None]:
    """
    全局最大回撤（谷底相对峰值）百分比；以及该轮回撤持续月数、恢复至前高月数（未恢复则为 None）。
    """
    if len(values) < 2 or len(values) != len(dates):
        return 0.0, None, None
    peak = values[0]
    peak_i = 0
    best_dd = 0.0
    dd_peak_i = 0
    dd_trough_i = 0
    for i in range(1, len(values)):
        if values[i] > peak:
            peak = values[i]
            peak_i = i
        dd = values[i] / peak - 1.0
        if dd < best_dd:
            best_dd = dd
            dd_peak_i = peak_i
            dd_trough_i = i
    max_dd_pct = best_dd * 100.0
    peak_v = values[dd_peak_i]
    dt_peak = dates[dd_peak_i]
    dt_trough = dates[dd_trough_i]
    dur_m = (dt_trough - dt_peak).days / 30.4375
    rec_m: float | None = None
    peak_reach = peak_v
    if dd_trough_i + 1 < len(values):
        for j in range(dd_trough_i + 1, len(values)):
            if values[j] >= peak_reach:
                rec_m = (dates[j] - dt_trough).days / 30.4375
                break
    return max_dd_pct, dur_m, rec_m


def portfolio_metrics_from_curve(
    curve: list[float], dates: list[date], initial: float, name: str
) -> PortfolioStats:
    if len(curve) < 2:
        raise ValueError("portfolio curve too short")
    final_cny = curve[-1]
    cum_pct = (final_cny / initial - 1.0) * 100.0
    years = (dates[-1] - dates[0]).days / 365.25
    cagr_pct = ((final_cny / initial) ** (1.0 / years) - 1.0) * 100.0 if years > 1e-6 else 0.0
    rets = [curve[i] / curve[i - 1] - 1.0 for i in range(1, len(curve))]
    sd = statistics.pstdev(rets)
    mu = statistics.mean(rets)
    sharpe = (mu / sd) * math.sqrt(TRADING_DAYS_PER_YEAR) if sd > 1e-12 else 0.0
    mdd_pct, dm, rm = max_drawdown_story(curve, dates)
    return PortfolioStats(
        name=name,
        final_cny=round(final_cny, 2),
        cum_return_pct=round(cum_pct, 2),
        cagr_pct=round(cagr_pct, 2),
        max_drawdown_pct=round(mdd_pct, 2),
        sharpe=round(sharpe, 2),
        drawdown_months=None if dm is None else round(dm, 1),
        recovery_months=None if rm is None else round(rm, 1),
    )


def load_universe(path: Path) -> list[dict[str, Any]]:
    with open(path, encoding="utf-8") as f:
        return json.load(f)


def main() -> None:
    script_dir = Path(__file__).resolve().parent
    p = argparse.ArgumentParser(description="拉取 ETF 日 K 并计算页面用收益指标")
    p.add_argument(
        "--universe",
        type=Path,
        default=script_dir / "etf_universe.json",
        help="静态名单（含 weight / establish / name 等）",
    )
    p.add_argument(
        "--report-start",
        default="2006-04-29",
        metavar="YYYY-MM-DD",
        help="与页面报告起点一致，用于截取 max(起点, 成立日)",
    )
    p.add_argument(
        "--end",
        default="",
        metavar="YYYY-MM-DD",
        help="样本截止日，默认取昨天自然日",
    )
    p.add_argument(
        "--initial-cny",
        type=float,
        default=200_000.0,
        help="组合初始本金（与页面顶栏一致）",
    )
    p.add_argument(
        "-o",
        "--output",
        type=Path,
        default=Path("etf_metrics.json"),
        help="写出 JSON 路径（默认项目根目录 etf_metrics.json）",
    )
    p.add_argument(
        "--sleep",
        type=float,
        default=0.35,
        help="每标的年片请求之间的休眠（秒）",
    )
    args = p.parse_args()

    report_start = _parse_iso(args.report_start)
    if args.end:
        end_d = _parse_iso(args.end)
    else:
        end_d = date.today() - timedelta(days=1)

    universe = load_universe(args.universe)
    codes = [str(row["code"]).strip() for row in universe]
    establishes = {str(row["code"]).strip(): _parse_iso(str(row["establish"])) for row in universe}
    optimal_w = [float(row["weight"]) for row in universe]

    # --- 各标的行情 ---
    raw_series: dict[str, dict[date, float]] = {}
    for code in codes:
        est = establishes[code]
        d0 = max(report_start, est)
        sec = secid_for_etf(code)
        try:
            raw_series[code] = load_closes_through_years(
                sec, d0, end_d, inter_sleep=args.sleep
            )
        except (RuntimeError, OSError, urllib.error.URLError) as e:
            raw_series[code] = {}
            print(f"warn: {code} kline: {e}", file=sys.stderr)
        time.sleep(min(0.5, args.sleep))

    etf_out: list[dict[str, Any]] = []
    aligned_by_code: dict[str, dict[date, float]] = {}

    for row in universe:
        code = str(row["code"]).strip()
        est = establishes[code]
        series = dict(sorted(raw_series[code].items()))
        if not series:
            etf_out.append(
                {
                    "code": code,
                    "error": "无行情数据",
                    "window_start": None,
                    "window_end": None,
                }
            )
            continue
        d0_eff = max(report_start, est, min(series.keys()))
        d1_eff = max(series.keys())
        subset = {d: v for d, v in series.items() if d >= d0_eff and d <= d1_eff}
        if len(subset) < 2:
            etf_out.append(
                {
                    "code": code,
                    "error": "有效样本不足",
                    "window_start": str(d0_eff),
                    "window_end": str(d1_eff),
                }
            )
            continue
        dates_sorted = sorted(subset.keys())
        closes = [subset[d] for d in dates_sorted]
        cum, mdd, vol, sh = series_stats_for_window(closes)
        st = EtfSeriesStats(
            code=code,
            window_start=str(dates_sorted[0]),
            window_end=str(dates_sorted[-1]),
            trading_days=len(closes),
            cum_return_pct=round(cum, 2),
            max_drawdown_pct=round(mdd, 2),
            volatility_ann_pct=round(vol, 2),
            sharpe=round(sh, 2),
        )
        etf_out.append({**asdict(st)})
        aligned_by_code[code] = subset

    # --- 组合：全体标的均有有效日线的交易日交集 ---
    portfolio_block: dict[str, Any] = {"equal_weight": None, "optimal": None}
    if len(aligned_by_code) != len(codes):
        portfolio_block["note"] = "部分标的无有效行情，未计算组合曲线"
    else:
        common: set[date] | None = None
        for code in codes:
            ds = set(aligned_by_code[code].keys())
            common = ds if common is None else (common & ds)
        if common and len(common) >= 3:
            common_dates = sorted(d for d in common if d >= report_start)
            if len(common_dates) >= 3:
                mat = [[aligned_by_code[c][d] for c in codes] for d in common_dates]
                ew_w = [100.0 / len(codes)] * len(codes)
                init = float(args.initial_cny)
                cur_ew = equity_curve_buy_hold(mat, ew_w, init)
                cur_opt = equity_curve_buy_hold(mat, optimal_w, init)
                portfolio_block["equal_weight"] = asdict(
                    portfolio_metrics_from_curve(
                        cur_ew, common_dates, init, "equal_weight"
                    )
                )
                portfolio_block["optimal"] = asdict(
                    portfolio_metrics_from_curve(
                        cur_opt, common_dates, init, "optimal"
                    )
                )
        else:
            portfolio_block["note"] = "公共交易日过少，未计算组合曲线"

    def _strip_name(d: dict[str, Any] | None) -> None:
        if d and "name" in d:
            del d["name"]

    _strip_name(portfolio_block.get("equal_weight"))
    _strip_name(portfolio_block.get("optimal"))

    # 等权相对最优的回撤改善标签（百分比）
    dd_label: str | None = None
    ew = portfolio_block.get("equal_weight")
    opt = portfolio_block.get("optimal")
    if ew and opt:
        mde = abs(float(ew["max_drawdown_pct"]))
        mdo = abs(float(opt["max_drawdown_pct"]))
        if mde > 1e-6:
            dd_label = f"最优组合（较等权约降低 {round((1 - mdo / mde) * 100, 1)}%）"

    payload: dict[str, Any] = {
        "generated_at": datetime.now().isoformat(timespec="seconds"),
        "source": "Eastmoney push2his kline (fqt=1)",
        "report_start": str(report_start),
        "end_date": str(end_d),
        "initial_cny": float(args.initial_cny),
        "type_label": "日频收盘·前复权",
        "disclaimer": (
            "区间为可交易日线，非页面原「指数拼接 / 理论推算」口径；仅供每日刷新参考。"
        ),
        "max_drawdown_label_optimal": dd_label,
        "etfs": etf_out,
        "portfolio": portfolio_block,
    }

    out_path: Path = args.output
    if not out_path.is_absolute():
        out_path = Path.cwd() / out_path
    out_path.parent.mkdir(parents=True, exist_ok=True)
    with open(out_path, "w", encoding="utf-8") as f:
        json.dump(payload, f, ensure_ascii=False, indent=2)
        f.write("\n")
    print(f"Wrote {out_path}")


if __name__ == "__main__":
    main()
