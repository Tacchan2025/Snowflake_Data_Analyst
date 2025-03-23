use role sysadmin;
use warehouse tacchan_wh;
use database tacchan_db;
use schema public;
-------------------------------------------------------------------------------------------------

-------------------------------------------------------------------------------------------------
-- row access policy

-------------------------------------------------------------------------------------------------
-- Querying Metadata for Staged Files

create or replace file format myformat
  type = 'csv'
  field_delimiter = '|'
;

SELECT
  METADATA$FILENAME,
  METADATA$FILE_ROW_NUMBER,
  METADATA$FILE_CONTENT_KEY,
  METADATA$FILE_LAST_MODIFIED,
  METADATA$START_SCAN_TIME, 
  t.$1,
  t.$2
FROM @mystage1 (file_format => myformat) t
;

create or replace file format my_json_format
  type = 'json';

create or replace stage mystage2
  file_format = my_json_format;

SELECT
  metadata$filename,
  metadata$file_row_number,
  parse_json($1)
 FROM @mystage1/tmp/data1.json (file_format => my_json_format);

CREATE OR REPLACE temp TABLE table1 (
  filename varchar,
  file_row_number int,
  file_content_key varchar,
  file_last_modified timestamp_ntz,
  start_scan_time timestamp_ltz,
  col1 varchar,
  col2 varchar
);

COPY INTO table1(filename, file_row_number, file_content_key, file_last_modified, start_scan_time, col1, col2)
  FROM (SELECT METADATA$FILENAME, METADATA$FILE_ROW_NUMBER, METADATA$FILE_CONTENT_KEY, METADATA$FILE_LAST_MODIFIED, METADATA$START_SCAN_TIME, t.$1, t.$2 FROM @mystage1/tmp/data1.csv (file_format => myformat) t);

SELECT * FROM table1;









-------------------------------------------------------------------------------------------------
-- timestamp

SELECT TO_TIMESTAMP(31000000),
       TO_TIMESTAMP(PARSE_JSON(31000000)),
       PARSE_JSON(31000000)::TIMESTAMP_NTZ,
       TO_TIMESTAMP(PARSE_JSON(31000000)::INT),
       PARSE_JSON(31000000)::INT::TIMESTAMP_NTZ

;

alter session set timezone = 'Asia/Tokyo';

select current_timestamp();

set crtime = current_timestamp();

select to_timestamp_tz($crtime);

select to_timestamp_ntz($crtime);

SELECT TO_TIMESTAMP_TZ('04/05/2024 01:02:03', 'mm/dd/yyyy hh24:mi:ss');


ALTER SESSION SET TIMESTAMP_OUTPUT_FORMAT = 'YYYY-MM-DD HH24:MI:SS.FF9 TZH:TZM';

SELECT TO_TIMESTAMP_NTZ(40 * 365.25 * 86400);


CREATE OR REPLACE temp TABLE demo1 (
  description VARCHAR,
  value VARCHAR -- string rather than bigint
);

INSERT INTO demo1 (description, value) VALUES
  ('Seconds',      '31536000'),
  ('Milliseconds', '31536000000'),
  ('Microseconds', '31536000000000'),
  ('Nanoseconds',  '31536000000000000');


select
  description,
  value,
  to_timestamp(value),
  to_date(value)
from demo1
order by value;



-------------------------------------------------------------------------------------------------
-- Visualizing Worksheet Data

select
  count(o_orderdate) as orderdates,
  o_orderdate as date
from orders
where
   o_orderdate = :daterange
group by
  :datebucket(o_orderdate), o_orderdate
order by
  o_orderdate
limit 10
;

-------------------------------------------------------------------------------------------------
-- explain

drop table z1;
drop table z2;
drop table z3;

CREATE temp TABLE Z1 (ID INTEGER);
CREATE temp TABLE Z2 (ID INTEGER);
CREATE temp TABLE Z3 (ID INTEGER);

insert into z1 values (1), (2), (3);
insert into z2 values (1), (2), (4);



EXPLAIN USING TABULAR 
SELECT Z1.ID, Z2.ID 
    FROM Z1, Z2
    WHERE Z2.ID = Z1.ID;

explain using text
    select
        z1.id,
        z2.id
    from z1, z2
    where z2.id = z1.id
;

EXPLAIN USING JSON SELECT Z1.ID, Z2.ID 
    FROM Z1, Z2
    WHERE Z2.ID = Z1.ID;


-------------------------------------------------------------------------------------------------
-- SPLIT_TO_TABLE

select table1.value
    from table(split_to_table('a,b,c', ',')) table1
    order by table1.value;


create or replace temp table splittable (v varchar);
insert into splittable (v) values ('a.b.c'), ('d'), ('');

select * from splittable;

select * from splittable, lateral split_to_table(splittable.v, '.')
order by seq, index;

-- select * from split_to_table(splittable.v, '.');

drop table authors_books_test;
CREATE OR REPLACE temp TABLE authors_books_test (author VARCHAR, titles VARCHAR);
INSERT INTO authors_books_test (author, titles) VALUES
  ('Nathaniel Hawthorne', 'The Scarlet Letter , The House of the Seven Gables,The Blithedale Romance'),
  ('Herman Melville', 'Moby Dick,The Confidence-Man');
SELECT * FROM authors_books_test;

select author, trim(value) as title
    from authors_books_test, lateral split_to_table(titles, ',')
    order by author;

SELECT author, TRIM(value) AS title
    FROM authors_books_test, LATERAL SPLIT_TO_TABLE(titles, ',')
  ORDER BY author;

-------------------------------------------------------------------------------------------------
-- Using Persisted Query Results　クエリ結果キャッシュ

show tables;

SELECT  *
    FROM table(RESULT_SCAN(LAST_QUERY_ID()))
    -- WHERE "rows" = 0
    ;

SHOW TABLES;

SELECT
  *
FROM
  TABLE (RESULT_SCAN (LAST_QUERY_ID ()));



-------------------------------------------------------------------------------------------------
-- pivot

CREATE OR REPLACE temp TABLE quarterly_sales(
  empid INT, 
  amount INT, 
  quarter TEXT)
  AS SELECT * FROM VALUES
    (1, 10000, '2023_Q1'),
    (1, 400, '2023_Q1'),
    (2, 4500, '2023_Q1'),
    (2, 35000, '2023_Q1'),
    (1, 5000, '2023_Q2'),
    (1, 3000, '2023_Q2'),
    (2, 200, '2023_Q2'),
    (2, 90500, '2023_Q2'),
    (1, 6000, '2023_Q3'),
    (1, 5000, '2023_Q3'),
    (2, 2500, '2023_Q3'),
    (2, 9500, '2023_Q3'),
    (1, 8000, '2023_Q4'),
    (1, 10000, '2023_Q4'),
    (2, 800, '2023_Q4'),
    (2, 4500, '2023_Q4');


select * from quarterly_sales;

select *
    from quarterly_sales
        pivot(sum(amount) for quarter in (any order by quarter))
    order by empid
;


CREATE OR REPLACE temp TABLE ad_campaign_types_by_quarter(
  quarter VARCHAR,
  television BOOLEAN,
  radio BOOLEAN,
  print BOOLEAN)
  AS SELECT * FROM VALUES
    ('2023_Q1', TRUE, FALSE, FALSE),
    ('2023_Q2', FALSE, TRUE, TRUE),
    ('2023_Q3', FALSE, TRUE, FALSE),
    ('2023_Q4', TRUE, FALSE, TRUE);

select * from ad_campaign_types_by_quarter;


select *
from quarterly_sales
  pivot(sum(amount) for quarter in (
    select distinct quarter
    from ad_campaign_types_by_quarter
    where television = true
    order by quarter
)
)
order by empid
;


select *
    from quarterly_sales
        pivot(sum(amount) for quarter in ('2023_Q1', '2023_Q2', '2023_Q3')
        )
order by empid
;












-------------------------------------------------------------------------------------------------
-- object agg
-- use role accountadmin;
drop table objectagg_example;

CREATE OR REPLACE temp TABLE objectagg_example(g NUMBER, k VARCHAR(30), v VARIANT);
INSERT INTO objectagg_example SELECT 0, 'name', 'Joe'::VARIANT;
INSERT INTO objectagg_example SELECT 0, 'age', 21::VARIANT;
INSERT INTO objectagg_example SELECT 1, 'name', 'Sue'::VARIANT;
INSERT INTO objectagg_example SELECT 1, 'zip', 94401::VARIANT;

SELECT * FROM objectagg_example;

select object_agg(k, v) from objectagg_example group by g;

select seq, key, value
    from (select object_agg(k, v) o from objectagg_example group by g),
        lateral flatten(input => o);

-------------------------------------------------------------------------------------------------
-- Numeric type
drop table t1;

create temp table t1 (c1 number(2, 1));
insert into t1 values (9);
insert into t1 values (0.1);

select * from t1;

-------------------------------------------------------------------------------------------------
-- sp
SELECT * FROM information_schema.packages WHERE package_name = 'snowflake-snowpark-python' ORDER BY version DESC;

select * from information_schema.packages where language = 'python';

CREATE OR REPLACE TABLE employees(id NUMBER, name VARCHAR, role VARCHAR);
INSERT INTO employees (id, name, role) VALUES (1, 'Alice', 'op'), (2, 'Bob', 'dev'), (3, 'Cindy', 'dev');


create or replace procedure filterByRole(tableName varchar, role varchar)
returns table(id number, name varchar, role varchar)
language python
runtime_version = '3.9'
packages = ('snowflake-snowpark-python')
handler = 'filter_by_role'
as
$$
from snowflake.snowpark.functions import col

def filter_by_role(session, table_name, role):
    df = session.table(table_name)
    return df.filter(col("role") == role)
$$;

CALL filterByRole('employees', 'dev');