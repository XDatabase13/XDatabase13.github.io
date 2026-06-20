#!/usr/bin/env python3
"""
build_hub.py — 数理投資情報部 ハブ集約バッチ

(1) yfinance で市況9指標を取得(終値・前日比%)
(2) 3サイトの data.json を raw.githubusercontent.com 経由で集約
(3) hub_data.json を出力する

他サイト(sbg-nav/crypto-nav/kioxia-sandisk)と同じ作法:
  - 項目ごとに最大5回リトライ(10秒間隔)
  - 全滅時は前回値 stale フォールバック
  - workflow_dispatch 専用(内蔵 schedule は使わない)
"""

import json
import sys
import time
import urllib.request
from datetime import datetime, timezone, timedelta
from pathlib import Path

import yfinance as yf

try:
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")
except AttributeError:
    pass

# ── 定数 ───────────────────────────────────────────────────────────────────────
SCRIPT_DIR    = Path(__file__).parent
HUB_DATA_PATH = SCRIPT_DIR / "hub_data.json"

JST            = timezone(timedelta(hours=9))
MAX_RETRIES    = 5
RETRY_INTERVAL = 10  # 秒

# 市況9指標の定義(表示順を保つため OrderedDict 相当に記述)
MARKET_SPECS = [
    ("nikkei225", {"label": "日経平均",          "ticker": "^N225"}),
    ("topix_etf", {"label": "TOPIX連動",          "ticker": "1306.T",  "note": "ETF(1306.T)の値"}),
    ("growth250", {"label": "グロース250(連動)",   "ticker": "2516.T",  "note": "ETF(2516.T)の値"}),
    ("dow",       {"label": "ダウ",               "ticker": "^DJI"}),
    ("sp500",     {"label": "S&P500",             "ticker": "^GSPC"}),
    ("nasdaq",    {"label": "NASDAQ総合",         "ticker": "^IXIC"}),
    ("sox",       {"label": "SOX",               "ticker": "^SOX"}),
    ("usdjpy",    {"label": "USD/JPY",            "ticker": "USDJPY=X"}),
    ("btc",       {"label": "BTC",               "ticker": "BTC-USD"}),
]

# 3サイト集約 URL
SITE_URLS = {
    "sbg":    "https://raw.githubusercontent.com/XDatabase13/sbg-nav/master/data.json",
    "crypto": "https://raw.githubusercontent.com/XDatabase13/crypto-nav/master/data.json",
    "kioxia": "https://raw.githubusercontent.com/XDatabase13/kioxia-sandisk/master/data.json",
}


# ── ユーティリティ ──────────────────────────────────────────────────────────────
def now_jst() -> datetime:
    return datetime.now(JST)


def to_iso_jst(dt: datetime) -> str:
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(JST).strftime("%Y-%m-%dT%H:%M:%S+09:00")


def load_json(path: Path) -> dict:
    if path.exists():
        try:
            with open(path, "r", encoding="utf-8") as f:
                return json.load(f)
        except Exception:
            return {}
    return {}


def save_json(path: Path, data: dict) -> None:
    with open(path, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)


# ── yfinance 取得（リトライ付き） ───────────────────────────────────────────────
def fetch_close(ticker_str: str) -> tuple[float | None, float | None]:
    """
    直近終値と前日比%を返す。
    Returns: (close: float|None, change_pct: float|None)
    MAX_RETRIES 全滅時は (None, None)。
    """
    for attempt in range(MAX_RETRIES):
        try:
            hist = yf.Ticker(ticker_str).history(period="10d", auto_adjust=False)
            if hist.empty:
                raise ValueError("empty history")
            closes = hist["Close"].dropna()
            if closes.empty:
                raise ValueError("all NaN closes")

            last_val = round(float(closes.iloc[-1]), 4)
            change_pct = None
            if len(closes) >= 2:
                prev_val = float(closes.iloc[-2])
                if prev_val and prev_val != 0:
                    change_pct = round((last_val - prev_val) / abs(prev_val) * 100, 4)

            return last_val, change_pct

        except Exception as e:
            print(f"  [{ticker_str}] 試行{attempt + 1}/{MAX_RETRIES} 失敗: {e}")
            if attempt < MAX_RETRIES - 1:
                time.sleep(RETRY_INTERVAL)

    return None, None


# ── 外部 JSON 取得 ──────────────────────────────────────────────────────────────
def fetch_remote_json(url: str) -> dict | None:
    """URL から JSON を取得。失敗時は None を返す(バッチは落とさない)。"""
    try:
        req = urllib.request.Request(url, headers={"User-Agent": "build-hub/1.0"})
        with urllib.request.urlopen(req, timeout=20) as resp:
            return json.loads(resp.read().decode("utf-8"))
    except Exception as e:
        print(f"  [HTTP] 取得失敗: {e}")
        return None


# ── サイト集約 ─────────────────────────────────────────────────────────────────
def extract_sbg(d: dict) -> dict:
    """SBG data.json からカード表示に必要な最小フィールドを抽出。"""
    nav   = d.get("nav") or {}
    semi  = nav.get("semi") or {}
    sbg_m = (d.get("market") or {}).get("sbg") or {}

    nav_per_share = semi.get("nav_per_share_jpy")
    discount_pct  = semi.get("discount_pct") or nav.get("semi_discount_pct")
    close         = sbg_m.get("close")
    prev_close    = sbg_m.get("prev_close")

    price_change_pct = None
    if close is not None and prev_close and prev_close != 0:
        price_change_pct = round((close - prev_close) / abs(prev_close) * 100, 4)

    return {
        "status":           d.get("status"),
        "generated_at":     d.get("generated_at"),
        "nav_per_share":    nav_per_share,
        "discount_pct":     discount_pct,
        "close":            close,
        "price_change_pct": price_change_pct,
    }


def extract_crypto(d: dict) -> dict:
    """crypto data.json からカード表示に必要な最小フィールドを抽出。"""
    meta      = d.get("_meta") or {}
    companies = d.get("companies") or []
    mstr_c    = next((c for c in companies if c.get("id") == "mstr"), {})
    meta_c    = next((c for c in companies if c.get("id") == "metaplanet"), {})
    mdata     = d.get("market_data") or {}

    return {
        "status":       meta.get("overall_status"),
        "generated_at": meta.get("generated_at"),
        "mstr": {
            "mnav_premium":     ((mstr_c.get("calc") or {}).get("mnav_premium") or {}).get("value"),
            "price_change_pct": (mdata.get("mstr_price_usd") or {}).get("change_pct"),
        },
        "metaplanet": {
            "mnav_premium":     ((meta_c.get("calc") or {}).get("mnav_premium") or {}).get("value"),
            "price_change_pct": (mdata.get("metaplanet_price_jpy") or {}).get("change_pct"),
        },
    }


def extract_kioxia(d: dict) -> dict:
    """kioxia data.json からカード表示に必要な最小フィールドを抽出。"""
    meta      = d.get("_meta") or {}
    companies = d.get("companies") or []
    k_c       = next((c for c in companies if c.get("id") == "kioxia"), {})
    s_c       = next((c for c in companies if c.get("id") == "sndk"), {})
    mdata     = d.get("market_data") or {}

    return {
        "status":       meta.get("overall_status"),
        "generated_at": meta.get("generated_at"),
        "kioxia": {
            "per":              ((k_c.get("calc") or {}).get("per") or {}).get("value"),
            "price_change_pct": (mdata.get("kioxia_price_jpy") or {}).get("change_pct"),
        },
        "sndk": {
            "per":              ((s_c.get("calc") or {}).get("per") or {}).get("value"),
            "price_change_pct": (mdata.get("sndk_price_usd") or {}).get("change_pct"),
        },
    }


SITE_EXTRACTORS = {
    "sbg":    extract_sbg,
    "crypto": extract_crypto,
    "kioxia": extract_kioxia,
}


# ── メイン ─────────────────────────────────────────────────────────────────────
def build_hub() -> None:
    generated_at = now_jst()
    prev_data    = load_json(HUB_DATA_PATH)
    prev_market  = prev_data.get("market", {})
    prev_cards   = prev_data.get("cards", {})
    alerts: list[str] = []

    # ── 市況9指標の取得 ─────────────────────────────────────────────────────
    print(f"▼ 市況9指標 取得  ({to_iso_jst(generated_at)})")
    market_out: dict[str, dict] = {}

    for key, spec in MARKET_SPECS:
        ticker = spec["ticker"]
        close, change_pct = fetch_close(ticker)

        if close is not None:
            status = "ok"
            print(f"  {ticker:12s}  {close:>14.4f}  {(change_pct or 0):+.2f}%")
        else:
            # stale フォールバック: 前回値保持
            prev_entry = prev_market.get(key, {})
            close      = prev_entry.get("close")
            change_pct = prev_entry.get("change_pct")
            status     = "stale" if close is not None else "failed"
            if status == "stale":
                alerts.append(f"[警告] {spec['label']}({ticker}): 取得失敗。前回値 {close} を保持します。")
                print(f"  {ticker:12s}  {close:>14.4f}  [stale ← 前回値]")
            else:
                alerts.append(f"[エラー] {spec['label']}({ticker}): 取得失敗かつ前回値なし。欠損表示になります。")
                print(f"  {ticker:12s}  {'None':>14s}  [failed]")

        entry: dict = {
            "label":      spec["label"],
            "ticker":     ticker,
            "close":      close,
            "change_pct": change_pct,
            "status":     status,
        }
        if "note" in spec:
            entry["note"] = spec["note"]
        market_out[key] = entry

    # ── 3サイト集約 ─────────────────────────────────────────────────────────
    print(f"\n▼ 3サイト集約")
    cards_out: dict[str, dict] = {}

    for site_key, url in SITE_URLS.items():
        print(f"  [{site_key}] {url}")
        remote = fetch_remote_json(url)
        if remote is not None:
            try:
                cards_out[site_key] = SITE_EXTRACTORS[site_key](remote)
                cards_out[site_key]["_fetch_status"] = "ok"
                print(f"  [{site_key}] OK")
            except Exception as e:
                cards_out[site_key] = dict(prev_cards.get(site_key, {}))
                cards_out[site_key]["_fetch_status"] = "stale"
                alerts.append(f"[警告] {site_key}: 集約時エラー({e})。前回値を保持します。")
                print(f"  [{site_key}] parse error → stale")
        else:
            prev = prev_cards.get(site_key, {})
            cards_out[site_key] = dict(prev) if prev else {"_fetch_status": "failed"}
            if prev:
                cards_out[site_key]["_fetch_status"] = "stale"
            alerts.append(f"[警告] {site_key}: data.json 取得失敗。前回値を保持します。")

    # ── overall_status ───────────────────────────────────────────────────────
    any_mkt_issue  = any(v.get("status") != "ok"    for v in market_out.values())
    any_card_issue = any(v.get("_fetch_status") != "ok" for v in cards_out.values())
    overall_status = "partial" if (any_mkt_issue or any_card_issue) else "complete"

    # ── 出力 ────────────────────────────────────────────────────────────────
    output = {
        "_meta": {
            "schema_version": "1.0",
            "description":    "数理投資情報部 ハブ集約データ。市況9指標 + 3サイトカード情報。",
            "generated_at":   to_iso_jst(generated_at),
            "overall_status": overall_status,
            "_status_vocabulary": {
                "overall_status": "complete=全指標正常 / partial=一部staleまたは取得失敗",
                "item_status":    "ok=正常取得 / stale=取得失敗し前回値保持 / failed=前回値なし",
            },
        },
        "market": market_out,
        "cards":  cards_out,
        "alerts": {
            "_comment": "overall_status が partial のとき UI に表示できる。",
            "messages": alerts,
        },
    }

    save_json(HUB_DATA_PATH, output)
    lbl = "OK" if overall_status == "complete" else "WARN"
    print(f"\n[{lbl}] hub_data.json 書き出し完了  overall_status={overall_status}")
    if alerts:
        print("--- alerts ---")
        for a in alerts:
            print(" ", a)


if __name__ == "__main__":
    build_hub()
