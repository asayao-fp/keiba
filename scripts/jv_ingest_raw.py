"""
jv_ingest_raw.py
================
JV-Link (COM: JVDTLab.JVLink) から過去データを取得し、SQLite に生レコードとして保存する。

使用環境:
  - Windows
  - Python 32bit
  - pywin32 インストール済み
  - JV-Link COM コンポーネント (JVDTLab.JVLink) 登録済み

実行例:
  python scripts/jv_ingest_raw.py --from-date 20240101 --dataspec RACE
  python scripts/jv_ingest_raw.py --from-date 20240101 --dataspec RACE,TOKU --data-option 1
"""

import argparse
import datetime
import sqlite3
import sys
import time

# pywin32 は Windows 専用。インポートに失敗した場合は分かりやすいメッセージを表示する。
try:
    import win32com.client
except ImportError:
    print(
        "[ERROR] pywin32 が見つかりません。\n"
        "  pip install pywin32\n"
        "  (Python 32bit 環境で実行してください)"
    )
    sys.exit(1)

# デフォルト DB パス
DEFAULT_DB_PATH = "jv_data.db"

# JVOpen DataOption 定数
DATA_OPTION_NORMAL = 1
DATA_OPTION_THIS_WEEK = 2
DATA_OPTION_SETUP = 3

# JVRead / JVGets 戻り値
RC_EOF = 0
RC_FILE_CHANGED = -1

# JVOpen 戻り値
RC_JVOPEN_OK = 0

# JVStatus ポーリング間隔(秒)
JVSTATUS_POLL_INTERVAL = 0.5

# JVRead バッファサイズ(bytes)
BUFF_SIZE = 110000


def init_db(db_path: str) -> sqlite3.Connection:
    """SQLite DB を初期化し、raw_jv_records テーブルを作成する。"""
    conn = sqlite3.connect(db_path)
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS raw_jv_records (
            id           INTEGER PRIMARY KEY AUTOINCREMENT,
            dataspec     TEXT    NOT NULL,
            buffname     TEXT    NOT NULL,
            payload_text TEXT    NOT NULL,
            payload_size INTEGER NOT NULL,
            fetched_at   TEXT    NOT NULL
        )
        """
    )
    conn.commit()
    return conn


def jv_open(jvlink, dataspec: str, from_date: str, data_option: int):
    """
    JVOpen を呼び出す。

    pywin32 の Dispatch (遅延バインディング) では COM の [out] パラメータは IN 引数として渡さず、
    戻り値タプル (rc, read_count, download_count, last_file_timestamp) から取得する。
    型ライブラリキャッシュ環境によっては全引数を要求する場合があるため、
    まず 3 引数 (IN のみ) で呼び出し、失敗した場合はダミーの OUT 引数を付けてフォールバックする。

    Returns:
        (rc, read_count, download_count, last_file_timestamp)
    """
    try:
        result = jvlink.JVOpen(dataspec, from_date, data_option)
    except Exception as e:
        # 型ライブラリキャッシュ環境では全引数 (IN + OUT) を要求する場合がある
        print(f"[DEBUG] JVOpen (3引数) が失敗 ({e})。6引数フォールバックを試みます ...")
        result = jvlink.JVOpen(dataspec, from_date, data_option, 0, 0, "")

    if isinstance(result, (tuple, list)):
        rc = result[0]
        read_count = int(result[1]) if len(result) > 1 else 0
        download_count = int(result[2]) if len(result) > 2 else 0
        last_file_timestamp = result[3] if len(result) > 3 else ""
    else:
        rc = result
        read_count = 0
        download_count = 0
        last_file_timestamp = ""
    return rc, read_count, download_count, last_file_timestamp


def wait_for_download(jvlink, download_count: int):
    """
    ダウンロード完了まで JVStatus をポーリングする。

    Returns:
        True on success, False on error.
    """
    while True:
        status = jvlink.JVStatus()
        if status < 0:
            print(f"[ERROR] JVStatus エラー: {status}")
            return False
        print(f"  ダウンロード中 ... {status}/{download_count}", end="\r")
        if status >= download_count:
            print(f"  ダウンロード完了: {status}/{download_count}    ")
            return True
        time.sleep(JVSTATUS_POLL_INTERVAL)


def read_and_store(jvlink, dataspec: str, conn: sqlite3.Connection) -> int:
    """
    JVRead ループでデータを読み出し、SQLite に保存する。

    Returns:
        保存したレコード数。
    """
    saved = 0
    current_buffname = ""
    fetched_at = datetime.datetime.now().isoformat()

    while True:
        # JVRead(Buff, BuffSize, BuffName) の呼び出し
        # pywin32 では OUT パラメータは戻り値のタプルに含まれる場合がある。
        result = jvlink.JVRead("", BUFF_SIZE, "")
        if isinstance(result, (tuple, list)):
            rc = result[0]
            buff = result[1] if len(result) > 1 else ""
            buffname = result[2] if len(result) > 2 else current_buffname
        else:
            # 旧い pywin32 バインディングでは rc だけ返る場合もある
            rc = result
            buff = ""
            buffname = current_buffname

        if rc == RC_EOF:
            print(f"  [INFO] JVRead: EOF (全レコード読み込み完了)")
            break
        elif rc == RC_FILE_CHANGED:
            # ファイル切り替わり通知
            current_buffname = buffname
            print(f"  [INFO] JVRead: ファイル切り替え -> {buffname}")
            continue
        elif rc < RC_FILE_CHANGED:
            print(f"  [ERROR] JVRead エラー: {rc}")
            break
        else:
            # rc > 0: 正常読み込み (rc はレコードサイズ)
            if isinstance(buff, bytes):
                buff = buff.decode("cp932", errors="replace")
            elif not isinstance(buff, str):
                print(f"  [WARN] JVRead: 予期しない型 {type(buff)} のデータをスキップします")
                continue
            payload_size = rc
            conn.execute(
                "INSERT INTO raw_jv_records (dataspec, buffname, payload_text, payload_size, fetched_at) "
                "VALUES (?, ?, ?, ?, ?)",
                (dataspec, current_buffname, buff, payload_size, fetched_at),
            )
            saved += 1
            if saved % 1000 == 0:
                conn.commit()
                print(f"  ... {saved} レコード保存済み", end="\r")

    conn.commit()
    return saved


def ingest(
    dataspecs: list,
    from_date: str,
    data_option: int,
    db_path: str,
    sid: str,
    allow_no_data: bool = False,
) -> bool:
    """
    メイン処理: JV-Link から取得して SQLite に保存する。

    Returns:
        True if all DataSpecs succeeded (or only -111 with allow_no_data), False on error.
    """
    conn = init_db(db_path)

    jvlink = win32com.client.Dispatch("JVDTLab.JVLink")

    # JVInit
    rc = jvlink.JVInit(sid)
    if rc == -101 and not sid:
        print(f"[WARN] JVInit が -101 で失敗しました (SID が空文字)。SID を 'UNKNOWN' にフォールバックして再試行します ...")
        rc = jvlink.JVInit("UNKNOWN")
    if rc != 0:
        print(f"[ERROR] JVInit エラー: {rc}")
        conn.close()
        return False
    print(f"[INFO] JVInit 成功")

    has_error = False

    for dataspec in dataspecs:
        dataspec = dataspec.strip()
        if not dataspec:
            continue
        print(f"\n[INFO] DataSpec={dataspec} の取得を開始します ...")

        rc, read_count, download_count, last_ts = jv_open(
            jvlink, dataspec, from_date, data_option
        )

        if rc != RC_JVOPEN_OK:
            print(f"[ERROR] JVOpen エラー (DataSpec={dataspec}): {rc}")
            skip_as_no_data = False
            if rc == -1:
                print(
                    f"[HINT]  JVOpen -1 の主な原因: 契約・提供対象外の DataSpec / FromDate 形式不正 / "
                    f"DIFF など差分系は提供範囲外の場合あり"
                )
            elif rc == -111:
                print(
                    f"[HINT]  JVOpen -111 の主な原因: 当該 DataSpec のデータが現時点で提供されていない / "
                    f"サービス対象外 / 無効な DataSpec。"
                    f"オッズ系 (O1 等) はレース当日以外はデータが存在しない場合があります。"
                )
                if allow_no_data:
                    print(f"[WARN]  --allow-no-data が指定されているため、DataSpec={dataspec} をスキップして続行します。")
                    skip_as_no_data = True
            if not skip_as_no_data:
                has_error = True
            jvlink.JVClose()
            continue

        print(
            f"[INFO] JVOpen 成功: ReadCount={read_count}, DownloadCount={download_count}, "
            f"LastFileTimeStamp={last_ts}"
        )

        if download_count > 0:
            ok = wait_for_download(jvlink, download_count)
            if not ok:
                jvlink.JVClose()
                continue

        saved = read_and_store(jvlink, dataspec, conn)
        print(f"[INFO] DataSpec={dataspec}: {saved} レコードを保存しました")

        rc_close = jvlink.JVClose()
        if rc_close != 0:
            print(f"[WARN] JVClose エラー: {rc_close}")
        else:
            print(f"[INFO] JVClose 成功")

    conn.close()
    print(f"\n[INFO] 完了。DB: {db_path}")
    return not has_error


def parse_args():
    parser = argparse.ArgumentParser(
        description="JV-Link から過去データを取得して SQLite に保存する"
    )
    parser.add_argument(
        "--from-date",
        required=True,
        metavar="YYYYMMDD[000000]",
        help="データ提供開始日時 (例: 20240101 または 20240101000000)",
    )
    parser.add_argument(
        "--dataspec",
        required=True,
        metavar="DATASPEC[,DATASPEC...]",
        help="DataSpec をカンマ区切りで指定 (例: RACE または RACE,TOKU)",
    )
    parser.add_argument(
        "--data-option",
        type=int,
        default=DATA_OPTION_NORMAL,
        choices=[DATA_OPTION_NORMAL, DATA_OPTION_THIS_WEEK, DATA_OPTION_SETUP],
        help="DataOption: 1=通常データ, 2=今週データ, 3=セットアップデータ (デフォルト: 1)",
    )
    parser.add_argument(
        "--db",
        default=DEFAULT_DB_PATH,
        metavar="PATH",
        help=f"SQLite DB ファイルパス (デフォルト: {DEFAULT_DB_PATH})",
    )
    parser.add_argument(
        "--sid",
        default="UNKNOWN",
        metavar="SID",
        help="JVInit に渡すサービスID (デフォルト: UNKNOWN。空文字で -101 が返る環境では UNKNOWN が必要)",
    )
    parser.add_argument(
        "--allow-no-data",
        action="store_true",
        default=False,
        help="JVOpen が -111 (データなし) を返した場合をエラーとして扱わず警告のみ表示して続行する。"
             "定期実行でオッズ等のデータが存在しない時間帯にも終了コード 0 で完了させたい場合に使用する。",
    )
    return parser.parse_args()


def main():
    args = parse_args()

    from_date = args.from_date
    # 14桁に正規化 (yyyymmdd -> yyyymmdd000000)
    if len(from_date) == 8:
        from_date = from_date + "000000"
    elif len(from_date) != 14:
        print(f"[ERROR] --from-date は YYYYMMDD または YYYYMMDD000000 形式で指定してください: {args.from_date}")
        sys.exit(1)
    # 日付の妥当性チェック
    try:
        datetime.datetime.strptime(from_date[:8], "%Y%m%d")
    except ValueError:
        print(f"[ERROR] --from-date に無効な日付が含まれています: {args.from_date}")
        sys.exit(1)

    dataspecs = [s.strip() for s in args.dataspec.split(",") if s.strip()]
    if not dataspecs:
        print("[ERROR] --dataspec に有効な値を指定してください")
        sys.exit(1)

    print(f"[INFO] 取得設定:")
    print(f"  DataSpec    : {', '.join(dataspecs)}")
    print(f"  FromDate    : {from_date}")
    print(f"  DataOption  : {args.data_option}")
    print(f"  DB          : {args.db}")

    ok = ingest(
        dataspecs=dataspecs,
        from_date=from_date,
        data_option=args.data_option,
        db_path=args.db,
        sid=args.sid,
        allow_no_data=args.allow_no_data,
    )
    if not ok:
        sys.exit(1)


if __name__ == "__main__":
    main()
