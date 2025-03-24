use role sysadmin;
use warehouse tacchan_wh;
use database tacchan_db;
use schema public;
-------------------------------------------------------------------------------------------------

-------------------------------------------------------------------------------------------------
-- row access policy

show roles;

use role useradmin;
drop role mapping_role;
drop role sales_analyst_role;
drop role schema_owner_role;

create role schema_owner_role;
grant role schema_owner_role to role sysadmin;
grant role schema_owner_role to user tacchan;

use role sysadmin;
grant usage on database tacchan_db to role schema_owner_role;
grant create schema on database tacchan_db to role schema_owner_role;



-- use role accouadmin;
-- drop schema security;

use role sysadmin;
grant usage on warehouse tacchan_wh to role schema_owner_role;

use role schema_owner_role;
use warehouse tacchan_wh;
create schema security;
use schema security;


CREATE or replace table sales (
  customer   varchar,
  product    varchar,
  spend      decimal(20, 2),
  sale_date  date,
  region     varchar
);



INSERT INTO sales (customer, product, spend, sale_date, region)
VALUES 
  ('Alice',   'Laptop',    1200.00, '2024-01-15', 'North'),
  ('Bob',     'Phone',      800.50, '2024-02-20', 'East'),
  ('Charlie', 'Tablet',     450.99, '2024-03-05', 'South'),
  ('Diana',   'Monitor',    300.00, '2024-01-22', 'West'),
  ('Eve',     'Laptop',    1350.75, '2024-02-10', 'North'),
  ('Frank',   'Phone',      699.99, '2024-03-12', 'East'),
  ('Grace',   'Tablet',     499.95, '2024-02-28', 'South'),
  ('Heidi',   'Monitor',    280.00, '2024-01-30', 'West'),
  ('Ivan',    'Laptop',    1100.00, '2024-03-01', 'North'),
  ('Judy',    'Phone',      899.90, '2024-02-05', 'East');


CREATE TABLE security.salesmanagerregions (
  sales_manager varchar,
  region        varchar
);

INSERT INTO security.salesmanagerregions (sales_manager, region)
VALUES
  ('John Smith',    'North'),
  ('Emily Davis',   'East'),
  ('Michael Brown', 'South'),
  ('Sarah Wilson',  'West');

use role useradmin;
create role mapping_role;
create role sales_manager_role;
create role sales_analytics_role;

use role securityadmin;
grant usage on database tacchan_db to role mapping_role;
grant usage on schema tacchan_db.security to role mapping_role;

use role schema_owner_role;
grant select on table salesmanagerregions to role mapping_role;

use role useradmin;
grant role mapping_role to user tacchan;

use role sysadmin;
grant usage on warehouse tacchan_wh to role mapping_role;

use role mapping_role;
use warehouse tacchan_wh;
use schema tacchan_db.security;

select * from salesmanagerregions;

use role useradmin;
create role sales_executive_role;


use role schema_owner_role;

create or replace row access policy security.sales_policy as
(sales_region varchar) returns boolean ->
  'sales_executive_role' = current_role()
    or exists (
      select 1 from salesmanagerregions
        where sales_manager = current_role()
        and region = sales_region
    )
;

-- use role securityadmin;
USE ROLE SECURITYADMIN;
ß
ALTER TABLE sales ADD ROW ACCESS POLICY tacchan_db.security.sales_policy ON (region);



------------------------
-- grant
use role securityadmin;
grant ownership on row access policy tacchan_db.security.sales_policy to mapping_role;

-- grant ownership on row access policy tacchan_db.security.sales_policy to role schema_owner_role;

grant apply on row access policy tacchan_db.security.sales_policy to role sales_analyst_role;

use role useradmin;
grant role mapping_role to role schema_owner_role;


-- grant data usage, select
use role sysadmin;
grant usage on database tacchan_db to role mapping_role;
grant usage on schema security to role mapping_role;

use role schema_owner_role;
-- grant select on table sales to role mapping_role;
-- grant all on table sales to role mapping_role;

grant all on table sales to 


---------------
-- role organization
use role useradmin;
drop role sales_analytics_role;

create role sales_analyst_role;



-------------------------------------------------------------------------------------------------
-- regr_

CREATE OR REPLACE temp TABLE aggr(k INT, v DECIMAL(10,2), v2 DECIMAL(10, 2));
INSERT INTO aggr VALUES(1, 10, null);
INSERT INTO aggr VALUES(2, 10, 11), (2, 20, 22), (2, 25, null), (2, 30, 35);

SELECT k, REGR_INTERCEPT(v, v2), regr_slope(v, v2)
FROM aggr GROUP BY k;


-------------------------------------------------------------------------------------------------
-- regexp_like

CREATE OR REPLACE temp TABLE cities(city varchar(20));
INSERT INTO cities VALUES
  ('Sacramento'),
  ('San Francisco'),
  ('San Jose'),
  (null);

select * from cities
where regexp_like(city, 'san.*', 'i')
;


-------------------------------------------------------------------------------------------------
-- Querying Semi-structured Data
-- https://docs.snowflake.com/ja/user-guide/querying-semistructured

CREATE OR REPLACE TABLE car_sales
( 
  src variant
)
AS
SELECT PARSE_JSON(column1) AS src
FROM VALUES
('{ 
    "date" : "2017-04-28", 
    "dealership" : "Valley View Auto Sales",
    "salesperson" : {
      "id": "55",
      "name": "Frank Beasley"
    },
    "customer" : [
      {"name": "Joyce Ridgely", "phone": "16504378889", "address": "San Francisco, CA"}
    ],
    "vehicle" : [
      {"make": "Honda", "model": "Civic", "year": "2017", "price": "20275", "extras":["ext warranty", "paint protection"]}
    ]
}'),
('{ 
    "date" : "2017-04-28", 
    "dealership" : "Tindel Toyota",
    "salesperson" : {
      "id": "274",
      "name": "Greg Northrup"
    },
    "customer" : [
      {"name": "Bradley Greenbloom", "phone": "12127593751", "address": "New York, NY"}
    ],
    "vehicle" : [
      {"make": "Toyota", "model": "Camry", "year": "2017", "price": "23500", "extras":["ext warranty", "rust proofing", "fabric protection"]}  
    ]
}') v;

select * from car_sales;

create temp table t1 as
select src:dealership, src:dealership::varchar
  from car_sales
  order by 1;

DESCRIBE RESULT last_query_id();

select src:salesperson.name
  from car_sales
  order by 1
;

select src['salesperson']['name']
  from car_sales;

select src:customer[0].name, src:vehicle[0]
from car_sales;

CREATE TABLE pets (v variant);

INSERT INTO pets SELECT PARSE_JSON ('{"species":"dog", "name":"Fido", "is_dog":"true"} ');
INSERT INTO pets SELECT PARSE_JSON ('{"species":"cat", "name":"Bubby", "is_dog":"false"}');
INSERT INTO pets SELECT PARSE_JSON ('{"species":"cat", "name":"dog terror", "is_dog":"false"}');


select * from pets;

select
  a.v,
  b.key,
  b.value
from pets a, 
      lateral  flatten(input => a.v) b
where b.value like '%dog%'
;

SELECT a.v, b.key, b.value FROM pets a,LATERAL FLATTEN(input => a.v) b
WHERE b.value LIKE '%dog%';

select * from table(flatten(pets))
;

SELECT REGEXP_REPLACE(f.path, '\\[[0-9]+\\]', '[]') AS "Path",
  TYPEOF(f.value) AS "Type",
  COUNT(*) AS "Count"
FROM pets a,
LATERAL FLATTEN(a.v, RECURSIVE=>true) f
GROUP BY 1, 2 ORDER BY 1, 2;

SELECT *
FROM pets a,
LATERAL FLATTEN(a.v) f
-- GROUP BY 1, 2 ORDER BY 1, 2
;

desc table pets;


select
  t.v,
  f.seq,
  f.key,
  f.path,
  regexp_count(f.path, '\\.|\\[') + 1 as level,
  typeof(f.value) as "Type",
  f.index,
  f.value as clv,
  f.this as alv
from pets t
  lateral flatten(t.v, recursive=>true) f
;



SELECT
  t.v,
  f.seq,
  f.key,
  f.path,
  REGEXP_COUNT(f.path,'\\.|\\[') +1 AS Level,
  TYPEOF(f.value) AS "Type",
  f.index,
  f.value AS "Current Level Value",
  f.this AS "Above Level Value"
FROM pets t,
LATERAL FLATTEN(t.v, recursive=>true) f;


select
  vm.value:make::string as mk,
  vm.value:model::string as md,
  ve.value::string as ex
from car_sales,
  lateral flatten(input => src:vehicle) vm,
  lateral flatten(input => vm.value:extras) ve
order by 1, 2, 3
;


CREATE OR replace TABLE colors (v variant);

INSERT INTO
   colors
   SELECT
      parse_json(column1) AS v
   FROM
   VALUES
     ('[{r:255,g:12,b:0},{r:0,g:255,b:0},{r:0,g:0,b:255}]'),
     ('[{c:0,m:1,y:1,k:0},{c:1,m:0,y:1,k:0},{c:1,m:1,y:0,k:0}]')
    v;

select * from colors;
desc table colors;

select *,
  get(v, array_size(v) + 1),
  get(v, array_size(v) - 1)
from colors;

select get_path(src, 'vehicle[0]:make') from car_sales;

desc file format my_json_format;

list @mystage1;

select 
  'The First Employee Record is ' ||
  s.$1:root[0].employees[0].firstName ||
  ' ' || s.$1:root[0].employees[0].lastName

from @mystage1/tmp/contacts.json
(file_format => 'my_json_format') as s
;

desc table car_sales;
desc stage @mystage1;

show tables like '%customer%';


CREATE OR REPLACE TABLE jcustomers AS
SELECT
   $1 AS id,
   parse_json($2) AS info
FROM
   VALUES
      (12712555, '{"name": {"first": "John", "last":"Smith"}}'),
      (98127771, '{"name": {"first": "Jane", "last":"Doe"}}');

select * from jcustomers;

SELECT
   id,
   info:name.first AS first_name,
   info:name.last AS last_name
FROM
   jcustomers;
  
DESCRIBE RESULT last_query_id();

-------------------------------------------------------------------------------------------------
-- study test guide
SELECT n,
  ROW_NUMBER() OVER (ORDER BY n) as row_number,
  RANK() OVER (ORDER BY n) as rank,
  DENSE_RANK() OVER (ORDER BY n) as dense_rank,
  NTILE(2) OVER (ORDER BY n) as ntile
FROM (VALUES (34), (14), (34), (55))
AS Numbers(n);

select src:



/* 
D, D, B, (C,E), D

d, b, b, ce, d

半構造化　→ Fundamental 演習
ランク系
daterange
*/

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