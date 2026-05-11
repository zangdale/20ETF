#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
从东方财富拉取 ETF 最新价（现价），回写 ~/Downloads/etf.json 中的「现价」「市值」「仓位」。

口径：
  - 现价：push2 行情接口最新价（f2）。
  - 市值：持仓 × 现价（元），保留两位小数。
  - 仓位：该行市值 / 全表市值合计 × 100%，格式与现有数据一致（如 \"0.53%\"）。

用法:
  python3 scripts/update_etf_quotes.py
  python3 scripts/update_etf_quotes.py -i ~/Downloads/etf.json
  python3 scripts/update_etf_quotes.py --dry-run
  python3 scripts/update_etf_quotes.py --strict

仅标准库依赖；需联网。
  默认：接口无报价的代码保留 JSON 内原「现价」，并在 stderr 告警。
  --strict：禁止沿用旧「现价」，缺行情即失败。
  --no-keep-missing：同禁用沿用旧价（与默认相反）。
"""

from __future__ import annotations

import argparse
import json
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
    p.add_argument(
        "--strict",
        action="store_true",
        help="禁止沿用 JSON 内原「现价」；接口缺报价则失败",
    )
    p.add_argument(
        "--no-keep-missing",
        action="store_true",
        help="等同禁用默认的「缺价沿用旧现价」行为（缺报价即失败）",
    )
    args = p.parse_args()
    keep_missing = not args.no_keep_missing

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

    missing_no_fallback: list[str] = []
    kept: list[str] = []
    for c in sorted(set(codes)):
        if c in price_by_code:
            continue
        rows_c = [r for r in rows if str(r.get("编号", "")).strip() == c]
        fallback = None
        for r in rows_c:
            if "现价" in r and r["现价"] is not None:
                try:
                    fallback = float(r["现价"])
                except (TypeError, ValueError):
                    pass
                break
        if fallback is not None and keep_missing and not args.strict:
            price_by_code[c] = fallback
            kept.append(c)
        else:
            missing_no_fallback.append(c)

    if missing_no_fallback:
        print(
            "error: 以下编号无行情且未启用有效旧「现价」: "
            + ", ".join(missing_no_fallback),
            file=sys.stderr,
        )
        sys.exit(1)

    if kept:
        print(
            "warn: 以下编号沿用 JSON 内原「现价」（接口无报价，请核对代码是否有效）: "
            + ", ".join(kept),
            file=sys.stderr,
        )

    mvs: list[float] = []
    for r in rows:
        code = str(r["编号"]).strip()
        hold = float(r["持仓"])
        px = price_by_code[code]
        mv = round_mv(hold * px)
        mvs.append(mv)

    total_mv = sum(mvs)
    if total_mv <= 0:
        print("error: 合计市值为 0", file=sys.stderr)
        sys.exit(1)

    for r, mv in zip(rows, mvs, strict=True):
        code = str(r["编号"]).strip()
        px = price_by_code[code]
        r["现价"] = round_price(px)
        r["市值"] = mv
        r["仓位"] = fmt_position_pct(mv, total_mv)

    text = json.dumps(rows, ensure_ascii=False, indent=2) + "\n"
    if args.dry_run:
        print(f"dry-run: {len(rows)} 行，合计市值 {total_mv:.2f}，已解析现价样例: ")
        for r in rows[:3]:
            print(f"  {r.get('名称')} {r.get('编号')}: 现价={r['现价']} 市值={r['市值']} 仓位={r['仓位']}")
        return

    out_path.parent.mkdir(parents=True, exist_ok=True)
    with open(out_path, "w", encoding="utf-8") as f:
        f.write(text)
    print(f"Wrote {out_path} （{len(rows)} 行，合计市值 {total_mv:.2f}）")


if __name__ == "__main__":
    main()
