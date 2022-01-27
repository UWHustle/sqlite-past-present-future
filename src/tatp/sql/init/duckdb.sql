PRAGMA memory_limit = '8GB';

DROP TABLE IF EXISTS call_forwarding;
DROP TABLE IF EXISTS special_facility;
DROP TABLE IF EXISTS access_info;
DROP TABLE IF EXISTS subscriber;

CREATE TABLE subscriber
(
    s_id         UINTEGER NOT NULL PRIMARY KEY,
    sub_nbr      VARCHAR  NOT NULL UNIQUE,
    bit_1        BOOLEAN,
    bit_2        BOOLEAN,
    bit_3        BOOLEAN,
    bit_4        BOOLEAN,
    bit_5        BOOLEAN,
    bit_6        BOOLEAN,
    bit_7        BOOLEAN,
    bit_8        BOOLEAN,
    bit_9        BOOLEAN,
    bit_10       BOOLEAN,
    hex_1        UTINYINT,
    hex_2        UTINYINT,
    hex_3        UTINYINT,
    hex_4        UTINYINT,
    hex_5        UTINYINT,
    hex_6        UTINYINT,
    hex_7        UTINYINT,
    hex_8        UTINYINT,
    hex_9        UTINYINT,
    hex_10       UTINYINT,
    byte2_1      UTINYINT,
    byte2_2      UTINYINT,
    byte2_3      UTINYINT,
    byte2_4      UTINYINT,
    byte2_5      UTINYINT,
    byte2_6      UTINYINT,
    byte2_7      UTINYINT,
    byte2_8      UTINYINT,
    byte2_9      UTINYINT,
    byte2_10     UTINYINT,
    msc_location UINTEGER,
    vlr_location UINTEGER
);

CREATE TABLE access_info
(
    s_id    UINTEGER NOT NULL,
    ai_type UTINYINT NOT NULL,
    data1   UTINYINT,
    data2   UTINYINT,
    data3   VARCHAR,
    data4   VARCHAR,
    PRIMARY KEY (s_id, ai_type)
);

CREATE TABLE special_facility
(
    s_id        UINTEGER NOT NULL,
    sf_type     UTINYINT NOT NULL,
    is_active   BOOLEAN,
    error_cntrl UTINYINT,
    data_a      UTINYINT,
    data_b      TEXT,
    PRIMARY KEY (s_id, sf_type)
);

CREATE TABLE call_forwarding
(
    s_id       UINTEGER NOT NULL,
    sf_type    UTINYINT NOT NULL,
    start_time UTINYINT NOT NULL,
    end_time   UTINYINT,
    numberx    TEXT,
    PRIMARY KEY (s_id, sf_type, start_time)
);
