#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
从东方财富·天天基金拉取 ETF 公开档案字段，用于与 index.html 中「基金简称 / 成立日期」交叉核对。

数据来源（公开网页，非官方 API）：
  - 行情脚本 fS_name:  https://fund.eastmoney.com/pingzhongdata/{code}.js
  - 成立日期:          https://fundf10.eastmoney.com/tsdata_{code}.html 中「成立日期：<span>YYYY-MM-DD</span>」

用法:
  python3 scripts/fetch_eastmoney_etf_metadata.py
  python3 scripts/fetch_eastmoney_etf_metadata.py -o etf_meta.json
  python3 scripts/fetch_eastmoney_etf_metadata.py --codes codes.txt --report-start 2006-04-29

仅标准库依赖；需联网。
"""

from __future__ import annotations

import argparse
import json
import re
import sys
import urllib.error
import urllib.request
from dataclasses import asdict, dataclass
from datetime import date
from typing import Iterable


USER_AGENT = (
    "Mozilla/5.0 (compatible; ETF-20-metadata-fetch/1.0; "
    "+https://github.com/) Python-urllib"
)

DEFAULT_CODES = [
    "510500",
    "159593",
    "513300",
    "520870",
    "159687",
    "159985",
    "518880",
    "512400",
    "159222",
    "159207",
    "563020",
    "159307",
    "515180",
    "159545",
    "513920",
    "159569",
    "520810",
    "511360",
    "511180",
    "511520",
]


@dataclass
class Row:
    code: str
    fS_name: str | None
    establish_date: str | None
    covers_report_start_pure_nav: bool | None  # True 表示成立日不晚于 report_start（全程可有真实净值口径）
    report_start: str | None


def fetch_text(url: str, timeout: int = 25) -> str:
    req = urllib.request.Request(url, headers={"User-Agent": USER_AGENT})
    with urllib.request.urlopen(req, timeout=timeout) as resp:
        return resp.read().decode("utf-8", errors="ignore")


def parse_fS_name(ping_js: str) -> str | None:
    m = re.search(r'var\s+fS_name\s*=\s*"([^"]+)"', ping_js)
    return m.group(1) if m else None


def parse_establish_date(ts_html: str) -> str | None:
    m = re.search(r"成立日期：<span>(\d{4}-\d{2}-\d{2})</span>", ts_html)
    return m.group(1) if m else None


def parse_iso(d: str) -> date | None:
    try:
        y, mo, dy = map(int, d.split("-"))
        return date(y, mo, dy)
    except (TypeError, ValueError):
        return None


def covers_start(establish_iso: str | None, report_start_iso: str) -> bool | None:
    """若任一日期缺失则返回 None。"""
    ea = parse_iso(establish_iso) if establish_iso else None
    rs = parse_iso(report_start_iso)
    if ea is None or rs is None:
        return None
    return ea <= rs


def fetch_row(code: str, report_start_iso: str | None) -> Row:
    fS_name = establish = None
    try:
        js = fetch_text(f"https://fund.eastmoney.com/pingzhongdata/{code}.js")
        fS_name = parse_fS_name(js)
    except urllib.error.HTTPError as e:
        fS_name = f"(pingzhong HTTP {e.code})"
    except urllib.error.URLError as e:
        fS_name = f"(pingzhong error: {e.reason})"

    try:
        html = fetch_text(f"https://fundf10.eastmoney.com/tsdata_{code}.html")
        establish = parse_establish_date(html)
        if establish is None:
            establish = "(tsdata：未解析到成立日期)"
    except urllib.error.HTTPError as e:
        establish = f"(tsdata HTTP {e.code})"
    except urllib.error.URLError as e:
        establish = f"(tsdata error: {e.reason})"

    cov: bool | None = None
    rs = None
    if report_start_iso and isinstance(establish, str) and re.match(
        r"^\d{4}-\d{2}-\d{2}$", establish
    ):
        rs = report_start_iso
        cov = covers_start(establish, report_start_iso)

    return Row(
        code=code,
        fS_name=fS_name if isinstance(fS_name, str) else None,
        establish_date=establish if isinstance(establish, str) else None,
        covers_report_start_pure_nav=cov,
        report_start=rs,
    )


def load_codes(path: str) -> list[str]:
    raw = []
    with open(path, encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line or line.startswith("#"):
                continue
            raw.append(line.split()[0])
    return raw


def run(
    codes: Iterable[str],
    report_start_iso: str | None,
) -> list[Row]:
    return [fetch_row(c.strip(), report_start_iso) for c in codes]


def main() -> None:
    p = argparse.ArgumentParser(description="抓取天天基金 ETF 简称与成立日期")
    p.add_argument(
        "-o",
        "--output",
        help="写入 JSON 文件（UTF-8）；缺省打印到 stdout",
    )
    p.add_argument(
        "--codes",
        help="自定义代码列表文件，一行一个 6 位代码；缺省使用脚本内 DEFAULT_CODES",
    )
    p.add_argument(
        "--report-start",
        metavar="YYYY-MM-DD",
        help="报告回测起点；若给出则推导「成立日≤起点 → 全称真实净值口径」布尔列 "
        "covers_report_start_pure_nav（仅对齐日期，不涉及你们回测拼接规则）",
    )
    args = p.parse_args()

    codes = load_codes(args.codes) if args.codes else DEFAULT_CODES
    rows = run(codes, args.report_start)
    payload = [asdict(r) for r in rows]

    text = json.dumps(payload, ensure_ascii=False, indent=2)
    if args.output:
        with open(args.output, "w", encoding="utf-8") as f:
            f.write(text + "\n")
    else:
        sys.stdout.write(text + "\n")


if __name__ == "__main__":
    main()
