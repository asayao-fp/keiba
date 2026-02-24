CREATE TABLE IF NOT EXISTS raw_jv_records (
    id           INTEGER PRIMARY KEY AUTOINCREMENT,
    dataspec     TEXT    NOT NULL,
    buffname     TEXT    NOT NULL,
    payload_text TEXT    NOT NULL,
    payload_size INTEGER NOT NULL,
    fetched_at   TEXT    NOT NULL
);
