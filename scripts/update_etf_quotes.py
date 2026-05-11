#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
从东方财富拉取 ETF 最新价（现价），回写 ~/Downloads/etf.json 中的「现价」「市值」「仓位」。

口径：
  - 现价：push2 行情接口最新价（f2）。
  - 某编号拉价失败时：该行「现价」「市值」保持 JSON 原样不改动；stderr 打印告警。
  - 仓位：始终按全表各行当前市值合计重算（含未更新行沿用原市值）。

用法:
  python3 scripts/update_etf_quotes.py
  python3 scripts/update_etf_quotes.py -i ~/Downloads/etf.json
  python3 scripts/update_etf_quotes.py --dry-run

仅标准库依赖；需联网。部分失败不中断，仍写文件（除非合计市值无法计算）。
"""

from __future__ import annotations

import argparse
import json
import math
import sys
import time
import urllib.error
import urllib.parse
import urllib.request
from pathlib import Path
from typing import Any


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

BATCH_SIZE = 80
BATCH_SLEEP_SEC = 0.25
FETCH_RETRIES = 6

# 默认与持仓页面导出的 etf.json 放在用户下载目录
DEFAULT_ETF_JSON = Path.home() / "Downloads" / "etf.json"


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


def fetch_json(url: str, timeout: int = 30) -> dict[str, Any]:
    req = urllib.request.Request(url, headers=EM_REQUEST_HEADERS)
    with urllib.request.urlopen(req, timeout=timeout) as resp:
        return json.loads(resp.read().decode("utf-8", errors="ignore"))


def fetch_prices_batch(secids: list[str]) -> dict[str, float]:
    """secids 如 ['1.518680','0.159915']；仅返回接口确有报价的代码。"""
    q = urllib.parse.urlencode(
        {
            "fltt": "2",
            "secids": ",".join(secids),
            "fields": "f12,f2",
            "ut": "fa5fd1943c7b386f172d6893dbfba10b",
        }
    )
    url = f"https://push2.eastmoney.com/api/qt/ulist.np/get?{q}"
    last_err: Exception | None = None
    for attempt in range(FETCH_RETRIES):
        try:
            j = fetch_json(url)
            data = j.get("data") or {}
            diff = data.get("diff") or []
            out: dict[str, float] = {}
            for row in diff:
                code = str(row.get("f12", "")).strip()
                px = row.get("f2")
                if not code or px is None:
                    continue
                out[code] = float(px)
            return out
        except (urllib.error.URLError, urllib.error.HTTPError, json.JSONDecodeError, OSError, ValueError) as e:
            last_err = e
            time.sleep(min(6.0, 0.5 * (2**attempt)))
    raise RuntimeError(f"批量拉价失败 {secids[:3]}…: {last_err}")


def fetch_all_prices(codes: list[str]) -> dict[str, float]:
    """六位基金代码去重后批量请求，返回 code -> 现价。"""
    uniq = sorted({c.strip() for c in codes if c.strip()})
    secids = [secid_for_etf(c) for c in uniq]
    prices: dict[str, float] = {}
    for i in range(0, len(secids), BATCH_SIZE):
        chunk = secids[i : i + BATCH_SIZE]
        part = fetch_prices_batch(chunk)
        prices.update(part)
        if i + BATCH_SIZE < len(secids):
            time.sleep(BATCH_SLEEP_SEC)
    return prices


def round_price(x: float) -> float:
    return round(float(x), 4)


def round_mv(x: float) -> float:
    return round(float(x), 2)


def fmt_position_pct(mv: float, total: float) -> str:
    if total <= 0:
        raise ValueError("总市值为 0，无法计算仓位")
    return f"{round(mv / total * 100.0, 2)}%"


def parse_row_mktcap(r: dict[str, Any], hold: float) -> float | None:
    """从行内读出用于合计的市值；失败时尝试 持仓×现价。"""
    try:
        v = float(r["市值"])
        if math.isfinite(v):
            return round_mv(v)
    except (TypeError, ValueError, KeyError):
        pass
    try:
        px = float(r["现价"])
        h = float(hold)
        if math.isfinite(px) and math.isfinite(h):
            return round_mv(h * px)
    except (TypeError, ValueError, KeyError):
        pass
    return None


def log_price_miss(code: str, rows: list[dict[str, Any]]) -> None:
    hits = [r for r in rows if str(r.get("编号", "")).strip() == code]
    names = [str(r.get("名称", "")).strip() or "(无名称)" for r in hits]
    n = len(hits)
    name_part = f"名称={names[0]}" if n == 1 else f"共 {n} 行: " + " | ".join(names)
    print(f"warn: 编号 {code} 现价获取失败，跳过更新现价/市值；{name_part}", file=sys.stderr)


def main() -> None:
    p = argparse.ArgumentParser(description="刷新 etf.json 现价、市值、仓位")
    p.add_argument(
        "-i",
        "--input",
        type=Path,
        default=DEFAULT_ETF_JSON,
        help="etf.json 路径（默认 ~/Downloads/etf.json）",
    )
    p.add_argument(
        "-o",
        "--output",
        type=Path,
        default=None,
        help="写出路径，默认覆盖 -i",
    )
    p.add_argument(
        "--dry-run",
        action="store_true",
        help="只打印统计，不写文件",
    )
    args = p.parse_args()

    in_path: Path = args.input
    out_path: Path = args.output if args.output else in_path

    with open(in_path, encoding="utf-8") as f:
        rows: list[dict[str, Any]] = json.load(f)

    codes = [str(r.get("编号", "")).strip() for r in rows]
    if not all(codes):
        print("error: 存在空「编号」行", file=sys.stderr)
        sys.exit(2)

    try:
        price_by_code = fetch_all_prices(codes)
    except RuntimeError as e:
        print(f"error: {e}", file=sys.stderr)
        sys.exit(1)

    uniq = sorted(set(codes))
    missing = [c for c in uniq if c not in price_by_code]

    updated_rows = 0
    for r in rows:
        code = str(r["编号"]).strip()
        hold = float(r["持仓"])
        if code in price_by_code:
            px = price_by_code[code]
            r["现价"] = round_price(px)
            r["市值"] = round_mv(hold * px)
            updated_rows += 1

    for c in missing:
        log_price_miss(c, rows)

    mvs: list[float] = []
    for i, r in enumerate(rows, start=1):
        hold = float(r["持仓"])
        mv = parse_row_mktcap(r, hold)
        if mv is None:
            print(
                f"error: 第 {i} 行（编号 {r.get('编号')}）无法从「市值」或「持仓×现价」得到有效市值，无法计算仓位",
                file=sys.stderr,
            )
            sys.exit(1)
        mvs.append(mv)

    total_mv = sum(mvs)
    if total_mv <= 0:
        print("error: 合计市值为 0", file=sys.stderr)
        sys.exit(1)

    for r, mv in zip(rows, mvs, strict=True):
        r["仓位"] = fmt_position_pct(mv, total_mv)

    text = json.dumps(rows, ensure_ascii=False, indent=2) + "\n"
    if args.dry_run:
        print(
            f"dry-run: {len(rows)} 行，更新现价/市值 {updated_rows} 行，"
            f"无行情跳过 {len(rows) - updated_rows} 行，合计市值 {total_mv:.2f}"
        )
        for r in rows[:3]:
            print(f"  {r.get('名称')} {r.get('编号')}: 现价={r['现价']} 市值={r['市值']} 仓位={r['仓位']}")
        return

    out_path.parent.mkdir(parents=True, exist_ok=True)
    with open(out_path, "w", encoding="utf-8") as f:
        f.write(text)
    print(f"Wrote {out_path} （{len(rows)} 行，更新现价/市值 {updated_rows} 行，合计市值 {total_mv:.2f}）")