"""
output_utils.py
===============
JSON Lines / JSON / テーブル形式の出力ユーティリティ。
"""

import json


def output_jsonl(rows: list) -> None:
    for row in rows:
        print(json.dumps(row, ensure_ascii=False))


def output_json(rows: list) -> None:
    print(json.dumps(rows, ensure_ascii=False, indent=2))


def output_table(rows: list) -> None:
    if not rows:
        print("(0 件)")
        return
    headers = list(rows[0].keys())
    col_widths = [len(h) for h in headers]
    for row in rows:
        for i, h in enumerate(headers):
            col_widths[i] = max(col_widths[i], len(str(row[h]) if row[h] is not None else ""))
    sep = "+-" + "-+-".join("-" * w for w in col_widths) + "-+"
    header_line = "| " + " | ".join(h.ljust(col_widths[i]) for i, h in enumerate(headers)) + " |"
    print(sep)
    print(header_line)
    print(sep)
    for row in rows:
        line = "| " + " | ".join(
            str(row[h] if row[h] is not None else "").ljust(col_widths[i])
            for i, h in enumerate(headers)
        ) + " |"
        print(line)
    print(sep)
    print(f"({len(rows)} 件)")


def print_rows(rows: list, fmt: str) -> None:
    if fmt == "jsonl":
        output_jsonl(rows)
    elif fmt == "json":
        output_json(rows)
    else:
        output_table(rows)
