PRAGMA journal_mode = WAL;

DROP TABLE IF EXISTS call_forwarding;
DROP TABLE IF EXISTS special_facility;
DROP TABLE IF EXISTS access_info;
DROP TABLE IF EXISTS subscriber;

CREATE TABLE subscriber
(
    s_id         INTEGER NOT NULL PRIMARY KEY,
    sub_nbr      TEXT    NOT NULL UNIQUE,
    bit_1        INTEGER,
    bit_2        INTEGER,
    bit_3        INTEGER,
    bit_4        INTEGER,
    bit_5        INTEGER,
    bit_6        INTEGER,
    bit_7        INTEGER,
    bit_8        INTEGER,
    bit_9        INTEGER,
    bit_10       INTEGER,
    hex_1        INTEGER,
    hex_2        INTEGER,
    hex_3        INTEGER,
    hex_4        INTEGER,
    hex_5        INTEGER,
    hex_6        INTEGER,
    hex_7        INTEGER,
    hex_8        INTEGER,
    hex_9        INTEGER,
    hex_10       INTEGER,
    byte2_1      INTEGER,
    byte2_2      INTEGER,
    byte2_3      INTEGER,
    byte2_4      INTEGER,
    byte2_5      INTEGER,
    byte2_6      INTEGER,
    byte2_7      INTEGER,
    byte2_8      INTEGER,
    byte2_9      INTEGER,
    byte2_10     INTEGER,
    msc_location INTEGER,
    vlr_location INTEGER
);

CREATE TABLE access_info
(
    s_id    INTEGER NOT NULL,
    ai_type INTEGER NOT NULL,
    data1   INTEGER,
    data2   INTEGER,
    data3   TEXT,
    data4   TEXT,
    PRIMARY KEY (s_id, ai_type),
    FOREIGN KEY (s_id) REFERENCES subscriber (s_id)
);

CREATE TABLE special_facility
(
    s_id        INTEGER NOT NULL,
    sf_type     INTEGER NOT NULL,
    is_active   INTEGER,
    error_cntrl INTEGER,
    data_a      INTEGER,
    data_b      TEXT,
    PRIMARY KEY (s_id, sf_type),
    FOREIGN KEY (s_id) REFERENCES subscriber (s_id)
);

CREATE TABLE call_forwarding
(
    s_id       INTEGER NOT NULL,
    sf_type    INTEGER NOT NULL,
    start_time INTEGER NOT NULL,
    end_time   INTEGER,
    numberx    TEXT,
    PRIMARY KEY (s_id, sf_type, start_time),
    FOREIGN KEY (s_id, sf_type)
        REFERENCES special_facility (s_id, sf_type)
);
