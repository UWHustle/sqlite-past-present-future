DROP TABLE IF EXISTS lineorder;
DROP TABLE IF EXISTS part;
DROP TABLE IF EXISTS supplier;
DROP TABLE IF EXISTS customer;
DROP TABLE IF EXISTS date;

CREATE TABLE part
(
    p_partkey   INTEGER,
    p_name      TEXT,
    p_mfgr      TEXT,
    p_category  TEXT,
    p_brand1    TEXT,
    p_color     TEXT,
    p_type      TEXT,
    p_size      INTEGER,
    p_container TEXT,
    PRIMARY KEY (p_partkey)
);

CREATE TABLE supplier
(
    s_suppkey INTEGER,
    s_name    TEXT,
    s_address TEXT,
    s_city    TEXT,
    s_nation  TEXT,
    s_region  TEXT,
    s_phone   TEXT,
    PRIMARY KEY (s_suppkey)
);

CREATE TABLE customer
(
    c_custkey    INTEGER,
    c_name       TEXT,
    c_address    TEXT,
    c_city       TEXT,
    c_nation     TEXT,
    c_region     TEXT,
    c_phone      TEXT,
    c_mktsegment TEXT,
    PRIMARY KEY (c_custkey)
);

CREATE TABLE date
(
    d_datekey          INTEGER,
    d_date             TEXT,
    d_dayofweek        TEXT,
    d_month            TEXT,
    d_year             INTEGER,
    d_yearmonthnum     INTEGER,
    d_yearmonth        TEXT,
    d_daynuminweek     INTEGER,
    d_daynuminmonth    INTEGER,
    d_daynuminyear     INTEGER,
    d_monthnuminyear   INTEGER,
    d_weeknuminyear    INTEGER,
    d_sellingseason    TEXT,
    d_lastdayinweekfl  INTEGER,
    d_lastdayinmonthfl INTEGER,
    d_holidayfl        INTEGER,
    d_weekdayfl        INTEGER,
    PRIMARY KEY (d_datekey)
);

CREATE TABLE lineorder
(
    lo_orderkey      INTEGER,
    lo_linenumber    INTEGER,
    lo_custkey       INTEGER,
    lo_partkey       INTEGER,
    lo_suppkey       INTEGER,
    lo_orderdate     INTEGER,
    lo_orderpriority TEXT,
    lo_shippriority  TEXT,
    lo_quantity      INTEGER,
    lo_extendedprice INTEGER,
    lo_ordtotalprice INTEGER,
    lo_discount      INTEGER,
    lo_revenue       INTEGER,
    lo_supplycost    INTEGER,
    lo_tax           INTEGER,
    lo_commitdate    INTEGER,
    lo_shipmode      TEXT,
    PRIMARY KEY (lo_orderkey, lo_linenumber),
    FOREIGN KEY (lo_custkey) REFERENCES customer (c_custkey),
    FOREIGN KEY (lo_partkey) REFERENCES part (p_partkey),
    FOREIGN KEY (lo_suppkey) REFERENCES supplier (s_suppkey),
    FOREIGN KEY (lo_orderdate) REFERENCES date (d_datekey)
);

.import part.tbl part
.import supplier.tbl supplier
.import customer.tbl customer
.import date.tbl date
.import lineorder.tbl lineorder
