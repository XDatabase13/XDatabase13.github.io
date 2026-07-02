#!/usr/bin/env python3
"""
build_hub.py — 数理投資情報部 ハブ集約バッチ

(1) yfinance で市況9指標を取得(終値・前日比%)
(2) 5サイトの data.json を raw.githubusercontent.com 経由で集約
(3) hub_data.json を出力する

他サイト(sbg-nav/crypto-nav/kioxia-sandisk/momentum-corr/ai-manifold)と同じ作法:
  - 項目ごとに最大5回リトライ(10秒間隔)
  - 全滅時は前回値 stale フォールバック
  - workflow_dispatch 専用(内蔵 schedule は使わない)
"""

import json
import re
import sys
import time
import urllib.request
from datetime import datetime, timezone, timedelta, date as _date
from pathlib import Path

import yfinance as yf

try:
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")
except AttributeError:
    pass

# ── 定数 ───────────────────────────────────────────────────────────────────────
SCRIPT_DIR    = Path(__file__).parent
HUB_DATA_PATH = SCRIPT_DIR / "hub_data.json"
INDEX_PATH    = SCRIPT_DIR / "index.html"

JST            = timezone(timedelta(hours=9))
MAX_RETRIES    = 5
RETRY_INTERVAL = 10  # 秒
STALE_DAYS     = 5   # 取得日付が今日からこの日数を超えるとデータが古いとみなす

# 指数優先 → ETF代替フォールバックを行う日本株2指標
JP_INDEX_FALLBACK = [
    ("nikkei225", {
        "label":        "日経平均",
        "index_ticker": "^N225",
        "etf_ticker":   "1321.T",
        "etf_note":     "ETF(1321.T)の値",
    }),
]

# 固定ティッカーで単純取得する指標（TOPIX + グロース250 + 海外6指標）
MARKET_SPECS = [
    ("topix_etf", {"label": "TOPIX（1306）",      "ticker": "1306.T"}),
    ("growth250", {"label": "グロース250（2516）", "ticker": "2516.T"}),
    ("dow",       {"label": "ダウ",              "ticker": "^DJI"}),
    ("sp500",     {"label": "S&P500",            "ticker": "^GSPC"}),
    ("nasdaq",    {"label": "NASDAQ総合",        "ticker": "^IXIC"}),
    ("sox",       {"label": "SOX",              "ticker": "^SOX"}),
    ("usdjpy",    {"label": "USD/JPY",           "ticker": "USDJPY=X"}),
    ("btc",       {"label": "BTC",              "ticker": "BTC-USD"}),
]

# 3サイト集約 URL
SITE_URLS = {
    "sbg":    "https://raw.githubusercontent.com/XDatabase13/sbg-nav/master/data.json",
    "crypto": "https://raw.githubusercontent.com/XDatabase13/crypto-nav/master/data.json",
    "kioxia":    "https://raw.githubusercontent.com/XDatabase13/kioxia-sandisk/master/data.json",
    "momentum":  "https://raw.githubusercontent.com/XDatabase13/momentum-corr/master/data.json",
    "aiManifold": "https://raw.githubusercontent.com/XDatabase13/ai-manifold/master/data.json",
    "aiSupplyDag": "https://raw.githubusercontent.com/XDatabase13/ai-supply-dag/master/data.json",
}


# ── ユーティリティ ──────────────────────────────────────────────────────────────
def now_jst() -> datetime:
    return datetime.now(JST)


def to_iso_jst(dt: datetime) -> str:
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(JST).strftime("%Y-%m-%dT%H:%M:%S+09:00")


def is_stale(generated_at_iso: str | None, stale_days: int = STALE_DAYS) -> bool:
    """generated_at（ISO8601）が現在JST から stale_days 暦日を超えていれば True。
    None またはパース不能のときは安全側（True = stale）を返す。"""
    if not generated_at_iso:
        return True
    try:
        dt = datetime.fromisoformat(generated_at_iso).astimezone(JST)
        return (now_jst() - dt).days > stale_days
    except Exception:
        return True


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
def fetch_close(ticker_str: str) -> tuple[float | None, float | None, str | None]:
    """
    直近終値・前日比%・取得日付を返す。
    Returns: (close, change_pct, date_str)  全滅時は (None, None, None)。
    """
    for attempt in range(MAX_RETRIES):
        try:
            hist = yf.Ticker(ticker_str).history(period="10d", auto_adjust=False)
            if hist.empty:
                raise ValueError("empty history")
            closes = hist["Close"].dropna()
            if closes.empty:
                raise ValueError("all NaN closes")

            last_val   = round(float(closes.iloc[-1]), 4)
            date_str   = closes.index[-1].strftime("%Y-%m-%d")
            change_pct = None
            if len(closes) >= 2:
                prev_val = float(closes.iloc[-2])
                if prev_val and prev_val != 0:
                    change_pct = round((last_val - prev_val) / abs(prev_val) * 100, 4)

            return last_val, change_pct, date_str

        except Exception as e:
            print(f"  [{ticker_str}] 試行{attempt + 1}/{MAX_RETRIES} 失敗: {e}")
            if attempt < MAX_RETRIES - 1:
                time.sleep(RETRY_INTERVAL)

    return None, None, None


# ── 日本株2指標：指数優先 → ETFフォールバック ────────────────────────────────────
def fetch_jp_index_with_fallback(
    key: str,
    spec: dict,
    prev_entry: dict,
    alerts: list,
) -> dict:
    """日本株指数を 指数→ETF の2段階フォールバックで取得する。
    戻り値エントリに "source": "index" | "etf" | "stale" を付与する。
    """
    label        = spec["label"]
    index_ticker = spec["index_ticker"]
    etf_ticker   = spec["etf_ticker"]
    etf_note     = spec["etf_note"]
    prev_date    = prev_entry.get("date")

    def _try(ticker: str) -> tuple:
        """(close, change_pct, date_str, reason) を返す。
        reason: 'ok' | 'regression' | 'stale' | 'failed'"""
        close, change_pct, data_date = fetch_close(ticker)
        if close is None:
            return None, None, None, "failed"
        age = (_date.today() - _date.fromisoformat(data_date)).days if data_date else None
        if prev_date and data_date and data_date < prev_date:
            alerts.append(
                f"[警告] {label}({ticker}): yfinanceが古い日付({data_date}) を返した"
                f" → 日付逆行のためフォールバック"
            )
            print(f"  {ticker:12s}  {close:>14.4f}  [日付逆行 {data_date} < {prev_date}]")
            return None, None, None, "regression"
        if age is not None and age > STALE_DAYS:
            alerts.append(
                f"[警告] {label}({ticker}): データが{age}日前 > STALE_DAYS({STALE_DAYS}) → フォールバック"
            )
            print(f"  {ticker:12s}  {close:>14.4f}  [{age}日前 → フォールバック]")
            return None, None, None, "stale"
        return close, change_pct, data_date, "ok"

    # ── Step 1: 指数取得 ──────────────────────────────────────
    c, pct, d, reason = _try(index_ticker)
    if reason == "ok":
        print(f"  {index_ticker:12s}  {c:>14.4f}  {(pct or 0):+.2f}%  [{d}]  source=index")
        return {
            "label": label, "ticker": index_ticker,
            "close": c, "change_pct": pct, "date": d,
            "status": "ok", "note": "", "source": "index",
        }
    print(f"  {index_ticker:12s}  {reason} → ETF({etf_ticker})へフォールバック")

    # ── Step 2: ETF取得 ───────────────────────────────────────
    c, pct, d, reason = _try(etf_ticker)
    if reason == "ok":
        alerts.append(
            f"[警告] {label}: 指数({index_ticker})取得失敗のためETF({etf_ticker})で代替表示。"
        )
        print(f"  {etf_ticker:12s}  {c:>14.4f}  {(pct or 0):+.2f}%  [{d}]  source=etf")
        return {
            "label": label, "ticker": etf_ticker,
            "close": c, "change_pct": pct, "date": d,
            "status": "ok", "note": etf_note, "source": "etf",
        }
    print(f"  {etf_ticker:12s}  {reason} → stale")

    # ── 両方失敗 → 前回値（stale） ────────────────────────────
    prev_close = prev_entry.get("close")
    if prev_close is not None:
        alerts.append(
            f"[警告] {label}: 指数・ETF両方とも取得失敗。前回値({prev_close})を保持します。"
        )
        print(f"  {'stale':12s}  {prev_close:>14.4f}  [stale]")
        return {
            "label": label, "ticker": prev_entry.get("ticker", etf_ticker),
            "close": prev_close, "change_pct": prev_entry.get("change_pct"),
            "date": prev_entry.get("date"), "status": "stale",
            "note": prev_entry.get("note", ""), "source": "stale",
        }
    alerts.append(
        f"[エラー] {label}: 指数・ETF両方とも取得失敗かつ前回値なし。欠損表示になります。"
    )
    print(f"  {'failed':12s}  None  [failed]")
    return {
        "label": label, "ticker": etf_ticker,
        "close": None, "change_pct": None, "date": None,
        "status": "failed", "note": "", "source": "stale",
    }


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


def extract_momentum(d: dict) -> dict:
    """momentum-corr data.json からカード表示に必要な最小フィールドを抽出。"""
    meta    = d.get("_meta") or {}
    p10     = ((d.get("periods") or {}).get("10") or {})
    stats10 = p10.get("stats") or {}
    high    = p10.get("pair_ranking_high") or []
    low     = p10.get("pair_ranking_low") or []

    return {
        "status":        meta.get("overall_status"),
        "generated_at":  meta.get("generated_at"),
        "mean_corr_10":  stats10.get("mean_corr"),
        "top_high_pair": high[0] if high else None,
        "top_low_pair":  low[0] if low else None,
    }


def extract_ai_manifold(d: dict) -> dict:
    """ai-manifold data.json からカード表示に必要な最小フィールドを抽出。
    54銘柄を集計し 上昇数/下落数・最高上昇/最大下落銘柄を返す
    (index.html のクライアントJS buildAiManifold と同じロジック)。"""
    meta   = d.get("_meta") or {}
    stocks = d.get("stocks") or {}

    up = down = total = 0
    top_code, top_chg = None, None
    bot_code, bot_chg = None, None

    for code, s in stocks.items():
        s = s or {}
        if s.get("status") == "failed":
            continue
        total += 1
        chg = s.get("change_pct")
        if chg is None:
            continue
        if chg > 0:
            up += 1
        elif chg < 0:
            down += 1
        if top_chg is None or chg > top_chg:
            top_chg, top_code = chg, code
        if bot_chg is None or chg < bot_chg:
            bot_chg, bot_code = chg, code

    return {
        "status":       meta.get("overall_status"),
        "generated_at": meta.get("generated_at"),
        "total":        total,
        "up":           up,
        "down":         down,
        "top":          {"code": top_code, "change_pct": top_chg} if top_code else None,
        "bot":          {"code": bot_code, "change_pct": bot_chg} if bot_code else None,
    }


def extract_ai_supply_dag(d: dict) -> dict:
    """ai-supply-dag data.json からカード表示に必要な最小フィールドを抽出。
    summary フィールドを直接参照する。"""
    meta    = d.get("_meta") or {}
    summary = d.get("summary") or {}
    top_g   = summary.get("top_gainer") or {}
    top_l   = summary.get("top_loser") or {}
    return {
        "status":       meta.get("status"),
        "generated_at": meta.get("generated_at"),
        "total":        summary.get("total_companies"),
        "up":           summary.get("up_count"),
        "down":         summary.get("down_count"),
        "top_gainer":   {"name": top_g.get("name"), "ticker": top_g.get("ticker"), "change_pct": top_g.get("change_pct")} if top_g else None,
        "top_loser":    {"name": top_l.get("name"), "ticker": top_l.get("ticker"), "change_pct": top_l.get("change_pct")} if top_l else None,
    }


SITE_EXTRACTORS = {
    "sbg":        extract_sbg,
    "crypto":     extract_crypto,
    "kioxia":     extract_kioxia,
    "momentum":   extract_momentum,
    "aiManifold": extract_ai_manifold,
    "aiSupplyDag": extract_ai_supply_dag,
}


# ── 静的HTML焼き込み(Phase 2c SEO) ────────────────────────────────────────────

def _replace_between(text: str, start: str, end: str, inner: str) -> str:
    """start と end マーカーの間を inner で置換(マーカー自体は残す)。"""
    pattern = re.escape(start) + r".*?" + re.escape(end)
    if not re.search(pattern, text, flags=re.DOTALL):
        print(f"[bake 警告] マーカーが見つかりません: {start}")
        return text
    replacement = start + inner + end
    return re.sub(pattern, lambda _: replacement, text, count=1, flags=re.DOTALL)


def _fmt_pct_html(pct) -> str:
    """前日比%を ▲/▼ span に変換。None なら ''。"""
    if pct is None:
        return ""
    v = float(pct)
    a = abs(v)
    if v > 0:
        return f'<span class="chg pos">▲{a:.2f}%</span>'
    if v < 0:
        return f'<span class="chg neg">▼{a:.2f}%</span>'
    return '<span class="chg neutral">±0.00%</span>'


def _fmt_mkt_val(val) -> str | None:
    """市況値を整形。1000以上→整数, 未満→小数2桁。None なら None。"""
    if val is None:
        return None
    v = float(val)
    return f"{round(v):,}" if v >= 1000 else f"{v:.2f}"


def _fmt_time_jst(iso_str: str) -> str:
    """ISO日時 → JST の YYYY/MM/DD HH:MM JST 形式。"""
    if not iso_str:
        return ""
    try:
        dt = datetime.fromisoformat(iso_str).astimezone(JST)
        return dt.strftime("%Y/%m/%d %H:%M JST")
    except Exception:
        return ""


def _status_cls(s: str | None) -> str:
    return {"complete": "s-ok", "partial": "s-warn", "failed": "s-fail", "stale": "s-stale"}.get(s or "", "")


def _status_lbl(s: str | None) -> str:
    return {"complete": "正常更新", "partial": "一部前回値", "failed": "取得失敗", "stale": "更新停滞"}.get(s or "", "確認中")


def _card_effective_status(card: dict) -> str:
    """カードの表示ステータスを最終決定する。
    優先順位: HTTP失敗/パースエラー(failed) > 鮮度切れ(stale) > overall_status。"""
    if card.get("_fetch_status") == "failed":
        return "failed"
    if is_stale(card.get("generated_at")):
        return "stale"
    return card.get("status") or "failed"


def _build_market_section_html(hub_data: dict) -> str:
    """市況帯セクション全体の静的HTML を生成。"""
    market = hub_data.get("market", {})
    gen_at = (hub_data.get("_meta") or {}).get("generated_at", "")

    JP = ["nikkei225", "topix_etf", "growth250"]
    US = ["dow", "sp500", "nasdaq", "sox"]
    FX = ["usdjpy", "btc"]

    def mk_item(key: str) -> str:
        e = market.get(key, {})
        label  = e.get("label", key)
        source = e.get("source")
        if source == "etf":
            display_label = label + "（ETF連動値）"
        elif source == "stale":
            display_label = label + "（前回値）"
        else:
            display_label = label
        val_str = _fmt_mkt_val(e.get("close"))
        raw_note = e.get("note", "")
        ticker = raw_note.replace("ETF(", "").replace(")の値", "").strip() if raw_note else ""
        note_html = f'<span class="mkt-note">({ticker})</span>' if ticker else ""
        if e.get("status") == "failed" or val_str is None:
            return f'<div class="mkt-item"><span class="mkt-name">{display_label}</span>{note_html}<span class="mkt-fail">—</span></div>'
        pct_html = _fmt_pct_html(e.get("change_pct"))
        return (f'<div class="mkt-item"><span class="mkt-name">{display_label}</span>'
                f'{note_html}<span class="mkt-val">{val_str}</span>{pct_html}</div>')

    def mk_group(lbl: str, keys: list) -> str:
        items = "".join(mk_item(k) for k in keys)
        return (f'<div class="mkt-group"><span class="mkt-grp-lbl">{lbl}</span>'
                f'<div class="mkt-items">{items}</div></div>')

    time_str = ""
    if gen_at:
        try:
            dt = datetime.fromisoformat(gen_at).astimezone(JST)
            time_str = dt.strftime("%m/%d %H:%M JST時点")
        except Exception:
            pass

    # 注意喚起バナー（日経・TOPIXのフォールバック発動時のみ表示）
    alert_msgs = []
    nk_src = market.get("nikkei225", {}).get("source")
    if nk_src == "etf":
        alert_msgs.append("日経平均株価の取得に失敗したため、日経平均連動ETF（1321）の値で代替表示しています")
    elif nk_src == "stale":
        alert_msgs.append("日経平均の最新値を取得できませんでした。前回取得値を表示しています")
    if alert_msgs:
        alert_inner = "<br>".join(alert_msgs)
        alert_html  = f'<div id="mkt-alert" class="mkt-alert">{alert_inner}</div>'
    else:
        alert_html  = '<div id="mkt-alert" class="mkt-alert" hidden></div>'

    groups = "\n        ".join([
        mk_group("🇯🇵 日本株", JP),
        mk_group("🇺🇸 米国株", US),
        mk_group("💱 為替/仮想通貨", FX),
    ])

    return f"""
    {alert_html}
    <section class="mkt-band" id="mkt-band">
      <div class="mkt-hdr">
        <span class="mkt-label">市況</span>
        <span class="mkt-time" id="mkt-time">{time_str}</span>
      </div>
      <div class="mkt-groups" id="mkt-groups">
        {groups}
      </div>
    </section>
    """


def _build_sbg_card_html(sbg: dict) -> str:
    nav   = sbg.get("nav_per_share")
    close = sbg.get("close")
    disc  = sbg.get("discount_pct")
    p_chg = sbg.get("price_change_pct")
    st    = _card_effective_status(sbg)
    gen   = sbg.get("generated_at", "")

    nav_s   = f"¥{round(float(nav)):,}"   if nav   is not None else "—"
    close_s = f"¥{round(float(close)):,}" if close is not None else "—"
    disc_s  = f"{float(disc):.1f}%"       if disc  is not None else "—"
    p_html  = _fmt_pct_html(p_chg)
    comment = ("半透明NAV理論株価に対し割安" if disc is not None and float(disc) >= 0
               else "半透明NAV理論株価に対し割高" if disc is not None else "")
    c_html  = f'<p class="dcard-comment">{comment}</p>' if comment else ""

    return f"""<div class="dcard" onclick="location.href='https://xdbdb.com/sbg-nav/'">
      <div class="dcard-head">
        <span class="tag">Investment</span>
        <span class="status-badge {_status_cls(st)}"><span class="status-dot"></span>{_status_lbl(st)}</span>
      </div>
      <h2 class="dcard-title">SBG 理論株価モニター</h2>
      <p class="dcard-sub">ソフトバンクG · NAV分析</p>
      <p class="dcard-desc">ソフトバンクグループの保有上場株時価を合算した「半透明NAV」から算出した理論株価と、実株価とのディスカウント率を毎日更新。</p>
      <div class="metrics">
        <div class="metric">
          <span class="mlabel">理論株価(半透明NAV)</span>
          <span class="mval">{nav_s}</span>
        </div>
        <div class="metric metric-main">
          <span class="mlabel">実株価</span>
          <span class="mval">{close_s} {p_html}</span>
        </div>
        <div class="metric metric-main">
          <span class="mlabel">ディスカウント率</span>
          <span class="mval mval-hi">{disc_s}</span>
        </div>
      </div>
      {c_html}
      <a href="https://xdbdb.com/sbg-nav/" class="dcard-cta">構成銘柄の情報を詳しく見る →</a>
      <p class="dcard-time">{_fmt_time_jst(gen)} 更新</p>
    </div>"""


def _build_crypto_card_html(crypto: dict) -> str:
    mstr_d = crypto.get("mstr") or {}
    meta_d = crypto.get("metaplanet") or {}
    st     = _card_effective_status(crypto)
    gen    = crypto.get("generated_at", "")

    mm   = mstr_d.get("mnav_premium")
    mp   = meta_d.get("mnav_premium")
    mc   = mstr_d.get("price_change_pct")
    mpc  = meta_d.get("price_change_pct")

    mm_s  = f"{float(mm):.2f}x"  if mm is not None else "—"
    mp_s  = f"{float(mp):.2f}x"  if mp is not None else "—"

    parts = []
    if mm  is not None: parts.append(f'MSTRは{"プレミアム圏" if float(mm)  >= 1 else "ディスカウント圏"}')
    if mp  is not None: parts.append(f'メタプラは{"プレミアム圏" if float(mp) >= 1 else "ディスカウント圏"}')
    c_html = f'<p class="dcard-comment">{"、".join(parts)}</p>' if parts else ""

    return f"""<div class="dcard" onclick="location.href='https://xdbdb.com/crypto-nav/'">
      <div class="dcard-head">
        <span class="tag">Investment</span>
        <span class="status-badge {_status_cls(st)}"><span class="status-dot"></span>{_status_lbl(st)}</span>
      </div>
      <h2 class="dcard-title">暗号資産トレジャリー mNAV</h2>
      <p class="dcard-sub">MSTR · メタプラネット</p>
      <p class="dcard-desc">MicroStrategy(MSTR)とメタプラネットのBitcoin保有量に基づくmNAV(時価総額÷純資産価値)を毎日更新。1倍超でプレミアム、1倍未満でディスカウント。</p>
      <div class="metrics">
        <div class="metric metric-main">
          <span class="mlabel">MSTR mNAV</span>
          <span class="mval mval-hi">{mm_s}</span>
        </div>
        <div class="metric">
          <span class="mlabel">MSTR 株価前日比</span>
          <span class="mval">{_fmt_pct_html(mc)}</span>
        </div>
        <div class="metric metric-main">
          <span class="mlabel">メタプラ mNAV</span>
          <span class="mval mval-hi">{mp_s}</span>
        </div>
        <div class="metric">
          <span class="mlabel">メタプラ 株価前日比</span>
          <span class="mval">{_fmt_pct_html(mpc)}</span>
        </div>
      </div>
      {c_html}
      <a href="https://xdbdb.com/crypto-nav/" class="dcard-cta">比較・計算法を詳しく確認 →</a>
      <p class="dcard-time">{_fmt_time_jst(gen)} 更新</p>
    </div>"""


def _build_kioxia_card_html(kioxia: dict) -> str:
    k_d = kioxia.get("kioxia") or {}
    s_d = kioxia.get("sndk") or {}
    st  = _card_effective_status(kioxia)
    gen = kioxia.get("generated_at", "")

    kp  = k_d.get("per")
    sp  = s_d.get("per")
    kc  = k_d.get("price_change_pct")
    sc  = s_d.get("price_change_pct")

    kp_s = f"{float(kp):.1f}倍" if kp is not None else "—"
    sp_s = f"{float(sp):.1f}倍" if sp is not None else "—"

    comment = ""
    if kp is not None and sp is not None:
        if   float(kp) < float(sp): comment = "キオクシアが割安（PERが低い）"
        elif float(sp) < float(kp): comment = "サンディスクが割安（PERが低い）"
        else:                       comment = "両社のPERは同水準"
    c_html = f'<p class="dcard-comment">{comment}</p>' if comment else ""

    return f"""<div class="dcard" onclick="location.href='https://xdbdb.com/kioxia-sandisk/'">
      <div class="dcard-head">
        <span class="tag">Investment</span>
        <span class="status-badge {_status_cls(st)}"><span class="status-dot"></span>{_status_lbl(st)}</span>
      </div>
      <h2 class="dcard-title">キオクシア vs サンディスク</h2>
      <p class="dcard-sub">NAND予想PER比較</p>
      <p class="dcard-desc">キオクシア(285A.T)とウエスタンデジタル傘下サンディスク(SNDK)の予想PER(株価収益率)を毎日更新。NAND型フラッシュメモリ大手2社の割安性を定量比較。</p>
      <div class="metrics">
        <div class="metric metric-main">
          <span class="mlabel">キオクシア 予想PER</span>
          <span class="mval mval-hi">{kp_s}</span>
        </div>
        <div class="metric">
          <span class="mlabel">キオクシア 株価前日比</span>
          <span class="mval">{_fmt_pct_html(kc)}</span>
        </div>
        <div class="metric metric-main">
          <span class="mlabel">サンディスク 予想PER</span>
          <span class="mval mval-hi">{sp_s}</span>
        </div>
        <div class="metric">
          <span class="mlabel">サンディスク 株価前日比</span>
          <span class="mval">{_fmt_pct_html(sc)}</span>
        </div>
      </div>
      {c_html}
      <a href="https://xdbdb.com/kioxia-sandisk/" class="dcard-cta">PER推移を詳しく見る →</a>
      <p class="dcard-time">{_fmt_time_jst(gen)} 更新</p>
    </div>"""


_MOMENTUM_SHORT = {
    "285A": "キオクシア", "6976": "太陽誘電", "9984": "SBG",
    "6981": "村田",       "8035": "東エレク",  "6920": "レーザテク",
    "4062": "イビデン",   "5801": "古河電工",  "5706": "三井金属",
    "6525": "KOKUSAI",   "6723": "ルネサス",   "4004": "レゾナック",
    "6762": "TDK",        "6752": "パナソニック","3436": "SUMCO",
    "7735": "SCREEN",     "6098": "リクルート", "5803": "フジクラ",
}


def _build_momentum_card_html(mom: dict) -> str:
    st  = _card_effective_status(mom)
    gen = mom.get("generated_at", "")
    m10 = mom.get("mean_corr_10")
    hi  = mom.get("top_high_pair")
    lo  = mom.get("top_low_pair")

    m10_s = f"{float(m10):.2f}" if m10 is not None else "—"

    def _sh(code: str) -> str:
        return _MOMENTUM_SHORT.get(code, code)

    def _pair_str(pair) -> str:
        if not pair:
            return "—"
        a, b  = _sh(pair.get("a", "")), _sh(pair.get("b", ""))
        rho   = pair.get("rho")
        rho_s = f"{float(rho):+.2f}" if rho is not None else "—"
        return f"{a} × {b}　{rho_s}"

    comment = ""
    if m10 is not None:
        v = float(m10)
        if   v >= 0.6: comment = "群れの結束が強い"
        elif v >= 0.4: comment = "群れはほどほどに連動"
        else:          comment = "群れはばらけ気味"
    c_html = f'<p class="dcard-comment">{comment}</p>' if comment else ""

    return f"""<div class="dcard" onclick="location.href='https://xdbdb.com/momentum-corr/'">
      <div class="dcard-head">
        <span class="tag">Investment</span>
        <span class="status-badge {_status_cls(st)}"><span class="status-dot"></span>{_status_lbl(st)}</span>
      </div>
      <h2 class="dcard-title">モメンタム銘柄 相関係数</h2>
      <p class="dcard-sub">AI半導体テーマ18銘柄 · 群れの強さ</p>
      <p class="dcard-desc">AI半導体テーマ銘柄18社の日次リターン相関係数を毎朝更新。銘柄が一斉に動く「群れ」の強さを平均相関で可視化。相場の結束・崩壊の予兆を読む。</p>
      <div class="metrics">
        <div class="metric metric-main">
          <span class="mlabel">平均相関（10日）</span>
          <span class="mval mval-hi">{m10_s}</span>
        </div>
        <div class="metric">
          <span class="mlabel">最も相関</span>
          <span class="mval">{_pair_str(hi)}</span>
        </div>
        <div class="metric">
          <span class="mlabel">最も逆相関</span>
          <span class="mval">{_pair_str(lo)}</span>
        </div>
      </div>
      {c_html}
      <a href="https://xdbdb.com/momentum-corr/" class="dcard-cta">相関ヒートマップを詳しく見る →</a>
      <p class="dcard-time">{_fmt_time_jst(gen)} 更新</p>
    </div>"""


# ミニマップSVG(島の座標は ai-manifold/index.html の ISLANDS と一致 / viewBox 0 0 1600 1000)
_AI_MANIFOLD_MINIMAP = """<svg xmlns="http://www.w3.org/2000/svg" viewBox="150 100 1110 730"
        style="width:100%;display:block;border-radius:8px;background:#0b0e18;margin:6px 0 4px">
        <line x1="300" y1="560" x2="560" y2="470" stroke="#1c2840" stroke-width="3"/>
        <line x1="340" y1="300" x2="560" y2="470" stroke="#1c2840" stroke-width="3"/>
        <line x1="300" y1="560" x2="560" y2="720" stroke="#1c2840" stroke-width="3"/>
        <line x1="560" y1="470" x2="1060" y2="210" stroke="#1c2840" stroke-width="3"/>
        <line x1="560" y1="720" x2="1060" y2="210" stroke="#1c2840" stroke-width="3"/>
        <line x1="1060" y1="620" x2="1060" y2="210" stroke="#1c2840" stroke-width="3"/>
        <line x1="820" y1="760" x2="1060" y2="210" stroke="#1c2840" stroke-width="3"/>
        <line x1="560" y1="720" x2="1180" y2="420" stroke="#1c2840" stroke-width="3"/>
        <line x1="1060" y1="620" x2="1180" y2="420" stroke="#1c2840" stroke-width="3"/>
        <line x1="1060" y1="210" x2="800" y2="300" stroke="#1c2840" stroke-width="3"/>
        <circle cx="340" cy="300" r="62" fill="rgba(112,96,216,.18)" stroke="#7060d8" stroke-width="1.5"/>
        <circle cx="300" cy="560" r="58" fill="rgba(168,95,200,.18)" stroke="#a85fc8" stroke-width="1.5"/>
        <circle cx="560" cy="470" r="56" fill="rgba(80,112,232,.18)" stroke="#5070e8" stroke-width="1.5"/>
        <circle cx="560" cy="720" r="60" fill="rgba(200,95,160,.18)" stroke="#c85fa0" stroke-width="1.5"/>
        <circle cx="820" cy="760" r="48" fill="rgba(200,120,80,.18)" stroke="#c87850" stroke-width="1.5"/>
        <circle cx="1060" cy="620" r="52" fill="rgba(200,160,78,.18)" stroke="#c8a04e" stroke-width="1.5"/>
        <circle cx="1180" cy="420" r="55" fill="rgba(95,184,127,.18)" stroke="#5fb87f" stroke-width="1.5"/>
        <circle cx="1060" cy="210" r="52" fill="rgba(95,200,190,.18)" stroke="#5fc8be" stroke-width="1.5"/>
        <circle cx="800" cy="300" r="70" fill="rgba(111,168,255,.22)" stroke="#6fa8ff" stroke-width="2"/>
        <text x="340" y="300" text-anchor="middle" dy="0.35em" fill="#e8e9ec" font-size="42" font-family="sans-serif" font-weight="600">装置</text>
        <text x="300" y="560" text-anchor="middle" dy="0.35em" fill="#e8e9ec" font-size="42" font-family="sans-serif" font-weight="600">材料</text>
        <text x="560" y="470" text-anchor="middle" dy="0.35em" fill="#e8e9ec" font-size="42" font-family="sans-serif" font-weight="600">半導</text>
        <text x="560" y="720" text-anchor="middle" dy="0.35em" fill="#e8e9ec" font-size="42" font-family="sans-serif" font-weight="600">部品</text>
        <text x="820" y="760" text-anchor="middle" dy="0.35em" fill="#e8e9ec" font-size="42" font-family="sans-serif" font-weight="600">電線</text>
        <text x="1060" y="620" text-anchor="middle" dy="0.35em" fill="#e8e9ec" font-size="42" font-family="sans-serif" font-weight="600">パワ</text>
        <text x="1180" y="420" text-anchor="middle" dy="0.35em" fill="#e8e9ec" font-size="42" font-family="sans-serif" font-weight="600">FA</text>
        <text x="1060" y="210" text-anchor="middle" dy="0.35em" fill="#e8e9ec" font-size="42" font-family="sans-serif" font-weight="600">DC</text>
        <text x="800" y="300" text-anchor="middle" dy="0.35em" fill="#e8e9ec" font-size="50" font-family="sans-serif" font-weight="700">AI</text>
      </svg>"""


def _build_ai_manifold_card_html(ai: dict) -> str:
    st    = _card_effective_status(ai)
    gen   = ai.get("generated_at", "")
    total = ai.get("total") or 0
    up    = ai.get("up") or 0
    down  = ai.get("down") or 0
    top   = ai.get("top")
    bot   = ai.get("bot")

    up_h   = f'<span class="chg pos">▲ {up}銘柄</span>'  if up   > 0 else "—"
    down_h = f'<span class="chg neg">▼ {down}銘柄</span>' if down > 0 else "—"

    def _mover_html(pair, cls: str, mark: str) -> str:
        if not pair or pair.get("change_pct") is None or not pair.get("code"):
            return "—"
        return f'{pair["code"]} <span class="chg {cls}">{mark}{abs(float(pair["change_pct"])):.2f}%</span>'

    top_h      = _mover_html(top, "pos", "▲")
    bot_h      = _mover_html(bot, "neg", "▼")
    total_disp = total if total else 54

    return f"""<div class="dcard" onclick="location.href='https://xdbdb.com/ai-manifold/'">
      <div class="dcard-head">
        <span class="tag">AI / 業界地図</span>
        <span class="status-badge {_status_cls(st)}"><span class="status-dot"></span>{_status_lbl(st)}</span>
      </div>
      <h2 class="dcard-title">AI関連銘柄の多様体</h2>
      <p class="dcard-sub">業界地図 · {total_disp}銘柄 9セクター</p>
      <p class="dcard-desc">AI関連の日本株54銘柄をサプライチェーン上の9つのセクターに分類し、島間の取引関係とともに業界構造を地図化。毎朝更新。</p>
      {_AI_MANIFOLD_MINIMAP}
      <div class="metrics">
        <div class="metric"><span class="mlabel">本日上昇</span><span class="mval">{up_h}</span></div>
        <div class="metric"><span class="mlabel">本日下落</span><span class="mval">{down_h}</span></div>
        <div class="metric"><span class="mlabel">最高上昇</span><span class="mval mval-hi">{top_h}</span></div>
        <div class="metric"><span class="mlabel">最大下落</span><span class="mval">{bot_h}</span></div>
      </div>
      <a href="https://xdbdb.com/ai-manifold/" class="dcard-cta">業界地図を詳しく見る →</a>
      <p class="dcard-time">{_fmt_time_jst(gen)} 更新</p>
    </div>"""


# ミニマップSVG(9島・上流→下流バリューチェーン配置)
_AI_SUPPLY_DAG_MINIMAP = """<svg xmlns="http://www.w3.org/2000/svg" viewBox="200 50 1200 930"
        style="width:100%;display:block;border-radius:8px;background:#0b0e18;margin:6px 0 4px">
        <!-- edges -->
        <line x1="380" y1="100" x2="560" y2="255" stroke="#1c2840" stroke-width="2.5"/>
        <line x1="800" y1="400" x2="560" y2="255" stroke="#1c2840" stroke-width="2.5"/>
        <line x1="1220" y1="100" x2="800" y2="400" stroke="#1c2840" stroke-width="2.5"/>
        <line x1="1040" y1="255" x2="800" y2="400" stroke="#1c2840" stroke-width="2.5"/>
        <line x1="560" y1="255" x2="800" y2="530" stroke="#1c2840" stroke-width="2.5"/>
        <line x1="1040" y1="255" x2="800" y2="530" stroke="#1c2840" stroke-width="2.5"/>
        <line x1="800" y1="530" x2="800" y2="660" stroke="#1c2840" stroke-width="2.5"/>
        <line x1="800" y1="660" x2="800" y2="800" stroke="#1c2840" stroke-width="2.5"/>
        <line x1="800" y1="800" x2="800" y2="930" stroke="#1c2840" stroke-width="2.5"/>
        <line x1="1220" y1="100" x2="1040" y2="255" stroke="#1c2840" stroke-width="2.5"/>
        <!-- islands -->
        <circle cx="380"  cy="100" r="52" fill="rgba(112,96,216,.18)"  stroke="#7060d8" stroke-width="1.5"/>
        <circle cx="1220" cy="100" r="62" fill="rgba(168,95,200,.18)"  stroke="#a85fc8" stroke-width="1.5"/>
        <circle cx="560"  cy="255" r="56" fill="rgba(80,112,232,.18)"  stroke="#5070e8" stroke-width="1.5"/>
        <circle cx="1040" cy="255" r="52" fill="rgba(200,95,160,.18)"  stroke="#c85fa0" stroke-width="1.5"/>
        <circle cx="800"  cy="400" r="50" fill="rgba(200,120,80,.18)"  stroke="#c87850" stroke-width="1.5"/>
        <circle cx="800"  cy="530" r="58" fill="rgba(95,184,127,.18)"  stroke="#5fb87f" stroke-width="1.5"/>
        <circle cx="800"  cy="660" r="56" fill="rgba(95,200,190,.18)"  stroke="#5fc8be" stroke-width="1.5"/>
        <circle cx="800"  cy="800" r="60" fill="rgba(200,160,78,.18)"  stroke="#c8a04e" stroke-width="1.5"/>
        <circle cx="800"  cy="930" r="52" fill="rgba(111,168,255,.22)" stroke="#6fa8ff" stroke-width="2"/>
        <!-- labels -->
        <text x="380"  y="100"  text-anchor="middle" dy="0.35em" fill="#e8e9ec" font-size="36" font-family="sans-serif" font-weight="600">EDA</text>
        <text x="1220" y="100"  text-anchor="middle" dy="0.35em" fill="#e8e9ec" font-size="32" font-family="sans-serif" font-weight="600">装置</text>
        <text x="560"  y="255"  text-anchor="middle" dy="0.35em" fill="#e8e9ec" font-size="32" font-family="sans-serif" font-weight="600">AIチップ</text>
        <text x="1040" y="255"  text-anchor="middle" dy="0.35em" fill="#e8e9ec" font-size="32" font-family="sans-serif" font-weight="600">メモリ</text>
        <text x="800"  y="400"  text-anchor="middle" dy="0.35em" fill="#e8e9ec" font-size="32" font-family="sans-serif" font-weight="600">製造</text>
        <text x="800"  y="530"  text-anchor="middle" dy="0.35em" fill="#e8e9ec" font-size="32" font-family="sans-serif" font-weight="600">DCインフラ</text>
        <text x="800"  y="660"  text-anchor="middle" dy="0.35em" fill="#e8e9ec" font-size="36" font-family="sans-serif" font-weight="600">クラウド</text>
        <text x="800"  y="800"  text-anchor="middle" dy="0.35em" fill="#e8e9ec" font-size="36" font-family="sans-serif" font-weight="600">AIモデル</text>
        <text x="800"  y="930"  text-anchor="middle" dy="0.35em" fill="#e8e9ec" font-size="36" font-family="sans-serif" font-weight="700">実装</text>
      </svg>"""


def _build_ai_supply_dag_card_html(dag: dict) -> str:
    st    = _card_effective_status(dag)
    gen   = dag.get("generated_at", "")
    total = dag.get("total") or 0
    up    = dag.get("up") or 0
    down  = dag.get("down") or 0
    top_g = dag.get("top_gainer")
    top_l = dag.get("top_loser")

    up_h   = f'<span class="chg pos">▲ {up}社</span>'  if up   > 0 else "—"
    down_h = f'<span class="chg neg">▼ {down}社</span>' if down > 0 else "—"

    def _mover_html(pair, cls: str, mark: str) -> str:
        if not pair or pair.get("change_pct") is None:
            return "—"
        name   = pair.get("name") or pair.get("ticker") or "—"
        ticker = pair.get("ticker") or ""
        disp   = ticker if ticker else name
        return f'{disp} <span class="chg {cls}">{mark}{abs(float(pair["change_pct"])):.2f}%</span>'

    top_h      = _mover_html(top_g, "pos", "▲")
    bot_h      = _mover_html(top_l, "neg", "▼")
    total_disp = total if total else 56

    return f"""<div class="dcard" onclick="location.href='https://xdbdb.com/ai-supply-dag/'">
      <div class="dcard-head">
        <span class="tag">AI / サプライチェーン</span>
        <span class="status-badge {_status_cls(st)}"><span class="status-dot"></span>{_status_lbl(st)}</span>
      </div>
      <h2 class="dcard-title">生成AIサプライチェーンの有向グラフ</h2>
      <p class="dcard-sub">グローバル{total_disp}社 9セクター DAG</p>
      <p class="dcard-desc">生成AI三大プレイヤーを中核に、半導体・製造装置・材料・データセンター・フィジカルAIまで、グローバル56社のサプライチェーンを有向グラフで可視化。毎朝更新。</p>
      {_AI_SUPPLY_DAG_MINIMAP}
      <div class="metrics">
        <div class="metric"><span class="mlabel">本日上昇</span><span class="mval">{up_h}</span></div>
        <div class="metric"><span class="mlabel">本日下落</span><span class="mval">{down_h}</span></div>
        <div class="metric"><span class="mlabel">最高上昇</span><span class="mval mval-hi">{top_h}</span></div>
        <div class="metric"><span class="mlabel">最大下落</span><span class="mval">{bot_h}</span></div>
      </div>
      <a href="https://xdbdb.com/ai-supply-dag/" class="dcard-cta">サプライチェーンを詳しく見る →</a>
      <p class="dcard-time">{_fmt_time_jst(gen)} 更新</p>
    </div>"""


def _build_meta_description(hub_data: dict) -> str:
    cards = hub_data.get("cards", {})
    sbg   = cards.get("sbg", {})
    cryp  = cards.get("crypto", {})
    kiox  = cards.get("kioxia", {})
    mom   = cards.get("momentum", {})
    aim   = cards.get("aiManifold", {})

    disc  = sbg.get("discount_pct")
    mm    = (cryp.get("mstr") or {}).get("mnav_premium")
    mp    = (cryp.get("metaplanet") or {}).get("mnav_premium")
    kp    = (kiox.get("kioxia") or {}).get("per")
    m10   = mom.get("mean_corr_10")
    a_tot = aim.get("total")
    a_up  = aim.get("up")

    parts = []
    if disc is not None: parts.append(f"SBGディスカウント率{float(disc):.1f}%")
    if mm   is not None: parts.append(f"MSTR mNAV {float(mm):.2f}x")
    if mp   is not None: parts.append(f"メタプラ mNAV {float(mp):.2f}x")
    if kp   is not None: parts.append(f"キオクシア予想PER {float(kp):.1f}倍")
    if m10  is not None: parts.append(f"AI半導体平均相関（10日）{float(m10):.2f}")
    if a_tot:            parts.append(f"AI関連{int(a_tot)}銘柄の業界地図（本日上昇{int(a_up or 0)}）")
    dag  = cards.get("aiSupplyDag", {})
    d_up = dag.get("up")
    if d_up is not None: parts.append(f"AIサプライチェーン{int(dag.get('total') or 56)}社（本日上昇{int(d_up)}）")

    if parts:
        return " / ".join(parts) + "。理論株価・mNAV・予想PER・相関係数を毎日更新する投資家向けハブ。"
    return "SBG理論株価・暗号資産mNAV・半導体PER・銘柄相関係数を毎朝自動更新。数理・ファンダメンタルズ指標をまとめた投資家向けハブサイト。"


def bake_index_html(hub_data: dict, index_path: Path) -> None:
    """hub_data の値を index.html のプレースホルダーに焼き込む。"""
    if not index_path.exists():
        print(f"[bake 警告] {index_path} が見つかりません。スキップ。")
        return

    content = index_path.read_text(encoding="utf-8")

    # 市況帯セクション全体を置換
    market_html = _build_market_section_html(hub_data)
    content = _replace_between(content, "<!--MARKET_SECTION_START-->", "<!--MARKET_SECTION_END-->", market_html)

    # カード4枚を置換
    cards = hub_data.get("cards", {})
    cards_inner = "\n      ".join([
        _build_sbg_card_html(cards.get("sbg", {})),
        _build_crypto_card_html(cards.get("crypto", {})),
        _build_kioxia_card_html(cards.get("kioxia", {})),
        _build_momentum_card_html(cards.get("momentum", {})),
        _build_ai_manifold_card_html(cards.get("aiManifold", {})),
        _build_ai_supply_dag_card_html(cards.get("aiSupplyDag", {})),
    ])
    content = _replace_between(content, "<!--CARDS_START-->", "<!--CARDS_END-->",
                                "\n      " + cards_inner + "\n      ")

    # meta description を最新値入りで更新
    new_desc = _build_meta_description(hub_data)
    content = re.sub(
        r'<meta name="description" content="[^"]*">',
        f'<meta name="description" content="{new_desc}">',
        content,
        count=1,
    )

    # ヘッダー更新日時プレースホルダーを置換
    gen_at = (hub_data.get("_meta") or {}).get("generated_at", "")
    if gen_at:
        try:
            dt = datetime.fromisoformat(gen_at)
            time_str = dt.strftime('%m/%d %H:%M JST')
            content = content.replace('<!--BKD_UPDATE_TIME-->', '最終更新 ' + time_str)
        except Exception:
            pass

    index_path.write_text(content, encoding="utf-8")
    print(f"[bake] index.html 焼き込み完了")


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

    # 日本株2指標：指数優先 → ETFフォールバック
    for key, spec in JP_INDEX_FALLBACK:
        prev_entry = prev_market.get(key, {})
        market_out[key] = fetch_jp_index_with_fallback(key, spec, prev_entry, alerts)

    # グロース250 + 海外6指標：単一ティッカー取得（既存ロジック）
    for key, spec in MARKET_SPECS:
        ticker     = spec["ticker"]
        prev_entry = prev_market.get(key, {})
        prev_date  = prev_entry.get("date")

        close, change_pct, data_date = fetch_close(ticker)

        if close is not None:
            status = "ok"
            age    = (_date.today() - _date.fromisoformat(data_date)).days if data_date else None

            # ① 前回より古い日付が返ってきた場合は前回値を保持（日付逆行チェック）
            if prev_date and data_date and data_date < prev_date:
                alerts.append(
                    f"[警告] {spec['label']}({ticker}): yfinanceが古い日付({data_date})"
                    f" を返した → 前回値({prev_date})を保持"
                )
                print(f"  {ticker:12s}  {prev_entry['close']:>14.4f}"
                      f"  [日付逆行 {data_date} < {prev_date} → stale]")
                close      = prev_entry["close"]
                change_pct = prev_entry.get("change_pct")
                data_date  = prev_date
                status     = "stale"

            # ② STALE_DAYS 超の古いデータも前回値を保持
            elif age is not None and age > STALE_DAYS:
                if prev_entry.get("close") is not None:
                    alerts.append(
                        f"[警告] {spec['label']}({ticker}): データが{age}日前({data_date})"
                        f" > STALE_DAYS({STALE_DAYS}) → 前回値保持"
                    )
                    print(f"  {ticker:12s}  {prev_entry['close']:>14.4f}  [{age}日前 → stale]")
                    close      = prev_entry["close"]
                    change_pct = prev_entry.get("change_pct")
                    data_date  = prev_entry.get("date")
                    status     = "stale"
                else:
                    alerts.append(f"[警告] {spec['label']}({ticker}): データが{age}日前({data_date})、前回値なし")
                    print(f"  {ticker:12s}  {close:>14.4f}  [{age}日前 → 警告・前回値なし]")

            else:
                print(f"  {ticker:12s}  {close:>14.4f}  {(change_pct or 0):+.2f}%  [{data_date}]")

        else:
            # 取得失敗 → 前回値フォールバック
            close      = prev_entry.get("close")
            change_pct = prev_entry.get("change_pct")
            data_date  = prev_entry.get("date")
            status     = "stale" if close is not None else "failed"
            if status == "stale":
                alerts.append(f"[警告] {spec['label']}({ticker}): 取得失敗。前回値 {close} を保持します。")
                print(f"  {ticker:12s}  {close:>14.4f}  [取得失敗 → stale]")
            else:
                alerts.append(f"[エラー] {spec['label']}({ticker}): 取得失敗かつ前回値なし。欠損表示になります。")
                print(f"  {ticker:12s}  {'None':>14s}  [failed]")

        entry: dict = {
            "label":      spec["label"],
            "ticker":     ticker,
            "close":      close,
            "change_pct": change_pct,
            "date":       data_date,
            "status":     status,
        }
        if "note" in spec:
            entry["note"] = spec["note"]
        market_out[key] = entry

    # ── 5サイト集約 ─────────────────────────────────────────────────────────
    print(f"\n▼ 5サイト集約")
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
                cards_out[site_key]["_fetch_status"] = "failed"
                alerts.append(f"[警告] {site_key}: 集約時エラー({e})。前回値を保持します。")
                print(f"  [{site_key}] parse error → failed")
        else:
            prev = prev_cards.get(site_key, {})
            cards_out[site_key] = dict(prev) if prev else {"_fetch_status": "failed"}
            cards_out[site_key]["_fetch_status"] = "failed"
            alerts.append(f"[警告] {site_key}: data.json 取得失敗。前回値を保持します。")

    # ── overall_status ───────────────────────────────────────────────────────
    any_mkt_issue  = any(v.get("status") != "ok"    for v in market_out.values())
    any_card_issue = any(v.get("_fetch_status") != "ok" for v in cards_out.values())
    overall_status = "partial" if (any_mkt_issue or any_card_issue) else "complete"

    # ── 出力 ────────────────────────────────────────────────────────────────
    output = {
        "_meta": {
            "schema_version": "1.0",
            "description":    "数理投資情報部 ハブ集約データ。市況9指標 + 5サイトカード情報。",
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
    bake_index_html(output, INDEX_PATH)
    lbl = "OK" if overall_status == "complete" else "WARN"
    print(f"\n[{lbl}] hub_data.json 書き出し完了  overall_status={overall_status}")
    if alerts:
        print("--- alerts ---")
        for a in alerts:
            print(" ", a)


if __name__ == "__main__":
    build_hub()
