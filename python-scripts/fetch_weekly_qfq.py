from __future__ import annotations

import sqlite3
from pathlib import Path
from typing import Dict, Iterable, List, Tuple


ROOT_DIR = Path(__file__).resolve().parents[1]
DB_PATH = ROOT_DIR / "stock-data.sqlite"


def detect_exchange(ts_code: str) -> str:
    """Return the human-readable exchange name based on the ts_code suffix."""
    suffix = ts_code.rsplit(".", 1)[-1].upper() if "." in ts_code else ""
    if suffix == "SH":
        return "上交所"
    if suffix == "SZ":
        return "深交所"
    return "未知"


def fetch_weekly_rows(conn: sqlite3.Connection) -> Dict[str, List[Dict[str, object]]]:
    """Fetch grouped weekly_qfq rows, excluding ST stocks and keeping at least 240 records."""
    # trade_date is stored as TEXT in YYYYMMdd format, so lexical ordering matches chronological order.
    query = """
        WITH filtered AS (
            SELECT w.*
            FROM weekly_qfq AS w
            JOIN stock_info AS si ON w.ts_code = si.ts_code
            WHERE INSTR(UPPER(si.name), 'ST') = 0
        ),
        global_max AS (
            SELECT MAX(trade_date) AS max_trade_date
            FROM filtered
        ),
        qualified AS (
            SELECT ts_code
            FROM filtered
            GROUP BY ts_code
            HAVING COUNT(*) >= 240
               AND MAX(trade_date) = (SELECT max_trade_date FROM global_max)
        )
        SELECT
            ts_code,
            trade_date,
            end_date,
            freq,
            open,
            high,
            low,
            close,
            pre_close,
            open_qfq,
            high_qfq,
            low_qfq,
            close_qfq,
            open_hfq,
            high_hfq,
            low_hfq,
            close_hfq,
            vol,
            amount,
            change,
            pct_chg,
            is_suspension_fill
        FROM filtered
        WHERE ts_code IN (SELECT ts_code FROM qualified)
        ORDER BY ts_code ASC, trade_date ASC
    """

    grouped: Dict[str, List[Dict[str, object]]] = {}
    for row in conn.execute(query):
        ts_code = row["ts_code"]
        item = dict(row)
        grouped.setdefault(ts_code, []).append(item)
    return grouped


def fetch_stock_info(conn: sqlite3.Connection, ts_codes: Iterable[str]) -> Dict[str, Dict[str, object]]:
    """Retrieve stock_info rows for the provided ts_codes, excluding ST names."""
    ts_codes = list(ts_codes)
    if not ts_codes:
        return {}

    # Build parameter placeholders for the IN clause.
    placeholders = ",".join("?" for _ in ts_codes)
    query = f"""
        SELECT
            ts_code,
            symbol,
            name,
            area,
            industry,
            cnspell,
            market,
            list_date,
            act_name,
            act_ent_type
        FROM stock_info
        WHERE ts_code IN ({placeholders})
          AND INSTR(UPPER(name), 'ST') = 0
    """

    info_map: Dict[str, Dict[str, object]] = {}
    for row in conn.execute(query, ts_codes):
        item = dict(row)
        item["exchange"] = detect_exchange(row["ts_code"])
        info_map[row["ts_code"]] = item
    return info_map


def build_dataset() -> Dict[str, Dict[str, object]]:
    """Compose the final dictionary keyed by ts_code."""
    if not DB_PATH.exists():
        raise FileNotFoundError(f"SQLite database not found at {DB_PATH}")

    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row

    try:
        weekly_data = fetch_weekly_rows(conn)
        stock_info_map = fetch_stock_info(conn, weekly_data.keys())

        dataset: Dict[str, Dict[str, object]] = {}
        for ts_code, rows in weekly_data.items():
            dataset[ts_code] = {
                "stock_info": stock_info_map.get(ts_code, {"ts_code": ts_code, "exchange": detect_exchange(ts_code)}),
                "weekly_qfq": rows,
            }
        return dataset
    finally:
        conn.close()


def main() -> None:
    data = build_dataset()
    sample_items: List[Tuple[str, Dict[str, object]]] = list(data.items())[:3]
    print("样例数据预览（最多展示3个ts_code）:")
    for ts_code, payload in sample_items:
        info = payload["stock_info"]
        weekly_rows = payload["weekly_qfq"]
        print(f"ts_code: {ts_code}")
        print(f"  名称: {info.get('name', '未知')} 交易所: {info.get('exchange')}")
        print(f"  行业: {info.get('industry', '未知')} 上市日期: {info.get('list_date', '未知')}")
        if weekly_rows:
            first_trade = weekly_rows[0]["trade_date"]
            last_trade = weekly_rows[-1]["trade_date"]
            print(f"  周线记录数: {len(weekly_rows)} 首个交易日: {first_trade} 最新交易日: {last_trade}")
        print("-" * 40)


if __name__ == "__main__":
    main()
