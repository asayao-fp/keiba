"""
inspect_raw_layouts.py
======================
raw_jv_records の固定長レコードレイアウトを調査するための読み取り専用 CLI ツール。

使用例:
  # デフォルト候補スライスで JG1 / HR2 を調査する
  python scripts/inspect_raw_layouts.py --db jv_data.db --prefix JG1 HR2

  # スライス位置を明示する (1-始まりバイト位置, 長さ)
  python scripts/inspect_raw_layouts.py --db jv_data.db --prefix SE7 \\
      --date-slice 12,8 --date-slice 4,8

  # サンプル数・表示文字数を変更する
  python scripts/inspect_raw_layouts.py --db jv_data.db --prefix WF7 \\
      --limit 500 --samples 3 --chars 80
"""

import argparse
import re
import sqlite3
import statistics

DEFAULT_DB_PATH = "jv_data.db"
DEFAULT_DATASPEC = "RACE"
DEFAULT_LIMIT = 1000
DEFAULT_SAMPLES = 5
DEFAULT_CHARS = 120

# デフォルトで試みる日付候補スライス (1-始まりバイト位置, 長さ)
# 複数の JV レコード形式でよく見られる位置を網羅する
_DEFAULT_DATE_SLICES: list[tuple[int, int]] = [
    (4, 8),
    (9, 8),
    (11, 8),
    (12, 8),
]

# Intentionally permissive: only checks century and total length.
# Month/day validity is not enforced here because this is a first-pass filter;
# the caller should verify plausibility via min/max range inspection.
_YYYYMMDD_RE = re.compile(r"^(19|20)\d{6}$")


def _extract_date(payload: str, pos: int, length: int) -> str | None:
    """
    payload (文字列) から 1-始まり pos 位置で length 文字を切り出し、
    YYYYMMDD 形式と思われる場合にその文字列を返す。それ以外は None。
    """
    # payload は Python str (cp932 デコード済み)
    # 日付フィールドは ASCII 数字のみで構成されるため、バイト位置と文字位置が一致する。
    # ただし、先行する文字に 2-byte cp932 文字が含まれる場合はバイト位置がずれる可能性がある。
    # その場合は --date-slice の位置を手動で調整してください。
    start = pos - 1
    end = start + length
    if end > len(payload):
        return None
    candidate = payload[start:end]
    return candidate if _YYYYMMDD_RE.match(candidate) else None


def inspect_prefix(
    conn: sqlite3.Connection,
    dataspec: str,
    prefix: str,
    limit: int,
    samples: int,
    chars: int,
    date_slices: list[tuple[int, int]],
) -> None:
    prefix_len = len(prefix)
    rows = conn.execute(
        """
        SELECT payload_text, payload_size
        FROM raw_jv_records
        WHERE dataspec = ?
          AND SUBSTR(payload_text, 1, ?) = ?
        LIMIT ?
        """,
        (dataspec, prefix_len, prefix, limit),
    ).fetchall()

    count = len(rows)
    print(f"\n{'='*60}")
    print(f"[INFO] prefix={prefix!r}  dataspec={dataspec!r}  サンプル数: {count:,} / limit={limit}")

    if count == 0:
        print("[INFO]  該当レコードなし")
        return

    # payload_size 統計
    sizes_stored = [r[1] for r in rows if r[1] is not None]
    if sizes_stored:
        print(
            f"[INFO] payload_size (stored)  "
            f"min={min(sizes_stored)}  "
            f"median={statistics.median(sizes_stored):.0f}  "
            f"max={max(sizes_stored)}"
        )

    # len(payload_text) 統計
    text_lens = [len(r[0]) for r in rows if r[0]]
    if text_lens:
        print(
            f"[INFO] len(payload_text)      "
            f"min={min(text_lens)}  "
            f"median={statistics.median(text_lens):.0f}  "
            f"max={max(text_lens)}"
        )

    # 日付候補スライス
    for pos, length in date_slices:
        dates: list[str] = []
        for payload, _ in rows:
            if not payload:
                continue
            d = _extract_date(payload, pos, length)
            if d:
                dates.append(d)
        hit_rate = len(dates) / count * 100 if count else 0.0
        if dates:
            print(
                f"[INFO] date-slice pos={pos},len={length}  "
                f"hit={len(dates)}/{count} ({hit_rate:.1f}%)  "
                f"min={min(dates)}  max={max(dates)}"
            )
        else:
            print(
                f"[INFO] date-slice pos={pos},len={length}  "
                f"hit=0/{count} (0.0%)  — YYYYMMDD パターンなし"
            )

    # テキスト先頭サンプル表示
    print(f"[INFO] payload_text 先頭 {chars} 文字 サンプル (最大 {samples} 件):")
    for i, (payload, _) in enumerate(rows[:samples]):
        if payload:
            print(f"  [{i+1}] {payload[:chars]!r}")
        else:
            print(f"  [{i+1}] (empty)")


def parse_date_slice(value: str) -> tuple[int, int]:
    """'pos,length' 形式の文字列をパースして (pos, length) タプルを返す。"""
    parts = value.split(",")
    if len(parts) != 2:
        raise argparse.ArgumentTypeError(
            f"--date-slice は 'pos,length' 形式で指定してください (例: 12,8): {value!r}"
        )
    try:
        pos = int(parts[0])
        length = int(parts[1])
    except ValueError:
        raise argparse.ArgumentTypeError(
            f"--date-slice の pos/length は整数である必要があります: {value!r}"
        )
    if pos < 1 or length < 1:
        raise argparse.ArgumentTypeError(
            f"--date-slice の pos/length は 1 以上である必要があります: {value!r}"
        )
    return (pos, length)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "raw_jv_records の固定長レコードレイアウトを調査する読み取り専用ツール。"
            " 指定したプレフィックスのレコードをサンプリングし、"
            " payload サイズ統計・日付候補スライス・テキスト先頭を表示する。"
        )
    )
    parser.add_argument(
        "--db",
        default=DEFAULT_DB_PATH,
        metavar="PATH",
        help=f"SQLite DB ファイルパス (デフォルト: {DEFAULT_DB_PATH})",
    )
    parser.add_argument(
        "--dataspec",
        default=DEFAULT_DATASPEC,
        metavar="SPEC",
        help=f"対象の dataspec (デフォルト: {DEFAULT_DATASPEC})",
    )
    parser.add_argument(
        "--prefix",
        nargs="+",
        required=True,
        metavar="PREFIX",
        help="調査するプレフィックス (例: JG1 HR2 H15 WF7 RA1 SE7)。複数指定可。",
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=DEFAULT_LIMIT,
        metavar="N",
        help=f"各プレフィックスのサンプル上限 (デフォルト: {DEFAULT_LIMIT})",
    )
    parser.add_argument(
        "--samples",
        type=int,
        default=DEFAULT_SAMPLES,
        metavar="N",
        help=f"テキスト先頭を表示するサンプル件数 (デフォルト: {DEFAULT_SAMPLES})",
    )
    parser.add_argument(
        "--chars",
        type=int,
        default=DEFAULT_CHARS,
        metavar="N",
        help=f"テキスト先頭の表示文字数 (デフォルト: {DEFAULT_CHARS})",
    )
    parser.add_argument(
        "--date-slice",
        dest="date_slices",
        type=parse_date_slice,
        action="append",
        metavar="pos,length",
        help=(
            "試みる日付候補スライスを '1始まりバイト位置,バイト長' で指定。"
            " 複数回指定可能。省略時はデフォルト候補 (4,8), (9,8), (11,8), (12,8) を使用。"
        ),
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    date_slices: list[tuple[int, int]] = args.date_slices or _DEFAULT_DATE_SLICES

    print(f"[INFO] DB: {args.db}")
    print(f"[INFO] dataspec: {args.dataspec}")
    print(f"[INFO] prefixes: {args.prefix}")
    print(f"[INFO] date-slices: {date_slices}")

    conn = sqlite3.connect(args.db)
    try:
        for prefix in args.prefix:
            inspect_prefix(
                conn=conn,
                dataspec=args.dataspec,
                prefix=prefix,
                limit=args.limit,
                samples=args.samples,
                chars=args.chars,
                date_slices=date_slices,
            )
    finally:
        conn.close()

    print("\n[INFO] 完了")


if __name__ == "__main__":
    main()
