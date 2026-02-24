CREATE TABLE IF NOT EXISTS raw_jv_records (
    id           INTEGER PRIMARY KEY AUTOINCREMENT,
    dataspec     TEXT    NOT NULL,
    buffname     TEXT    NOT NULL,
    payload_text TEXT    NOT NULL,
    payload_size INTEGER NOT NULL,
    fetched_at   TEXT    NOT NULL
);

CREATE TABLE IF NOT EXISTS races (
    race_key        TEXT    PRIMARY KEY,
    yyyymmdd        TEXT    NOT NULL,
    course_code     TEXT    NOT NULL,
    kai             TEXT    NOT NULL,
    day             TEXT    NOT NULL,
    race_no         TEXT    NOT NULL,
    grade_code      TEXT    NOT NULL,
    race_name_short TEXT    NOT NULL,
    created_at      TEXT    NOT NULL
);

CREATE TABLE IF NOT EXISTS entries (
    entry_key            TEXT    PRIMARY KEY,
    race_key             TEXT    NOT NULL REFERENCES races(race_key),
    horse_no             TEXT    NOT NULL,
    horse_id             TEXT    NOT NULL,
    finish_pos           INTEGER,
    is_place             INTEGER,
    jockey_code          TEXT,
    trainer_code         TEXT,
    body_weight          INTEGER,
    handicap_weight_x10  INTEGER,
    UNIQUE (race_key, horse_no)
);
