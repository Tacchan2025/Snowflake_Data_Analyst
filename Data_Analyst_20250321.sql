use role sysadmin;
use warehouse tacchan_wh;
use database tacchan_db;
use schema public;
--------------------------------------------------------------------------

--------------------------------------------------------------------------
-- unpivot

CREATE OR REPLACE TABLE monthly_sales(
  empid INT,
  dept TEXT,
  jan INT,
  feb INT,
  mar INT,
  apr INT
);

INSERT INTO monthly_sales VALUES
  (1, 'electronics', 100, 200, 300, 100),
  (2, 'clothes', 100, 300, 150, 200),
  (3, 'cars', 200, 400, 100, 50),
  (4, 'appliances', 100, NULL, 100, 50);

SELECT * FROM monthly_sales;

SELECT *
  FROM monthly_sales
    UNPIVOT (sales FOR month IN (jan, feb, mar, apr))
  ORDER BY empid;





















SELECT * FROM (SELECT * FROM orders sample (1));

--------------------------------------------------------------------------
-- datediff
select DATEDIFF('month', '2024-11-28', '2024-12-05');

--------------------------------------------------------------------------
-- objectagg
CREATE OR REPLACE TABLE objectagg_example(g NUMBER, k VARCHAR(30), v VARIANT);
INSERT INTO objectagg_example SELECT 0, 'name', 'Joe'::VARIANT;
INSERT INTO objectagg_example SELECT 0, 'age', 21::VARIANT;
INSERT INTO objectagg_example SELECT 1, 'name', 'Sue'::VARIANT;
INSERT INTO objectagg_example SELECT 1, 'zip', 94401::VARIANT;

SELECT * FROM objectagg_example;

SELECT g, OBJECT_AGG(k, v) FROM objectagg_example GROUP BY g;


--------------------------------------------------------------------------
-- semi-strucutured data


SET my_variable = 10;
SELECT {'key1': $my_variable+1, 'key2': $my_variable+2};

CREATE OR REPLACE TABLE demo_ca_provinces (province VARCHAR, capital VARCHAR);
INSERT INTO demo_ca_provinces (province, capital) VALUES
  ('Ontario', 'Toronto'),
  ('British Columbia', 'Victoria');

SELECT province, capital
  FROM demo_ca_provinces
  ORDER BY province;

INSERT INTO my_object_table (my_object)
  SELECT {*} FROM demo_ca_provinces;

SELECT * FROM my_object_table;
CREATE OR REPLACE TABLE my_object_table (my_object OBJECT);

INSERT INTO my_object_table (my_object)
  SELECT { 'PROVINCE': 'Alberta'::VARIANT , 'CAPITAL': 'Edmonton'::VARIANT };

INSERT INTO my_object_table (my_object)
  SELECT OBJECT_CONSTRUCT('PROVINCE', 'Manitoba'::VARIANT , 'CAPITAL', 'Winnipeg'::VARIANT );

SELECT * FROM my_object_table;

CREATE OR REPLACE TABLE object_example (object_column OBJECT);
INSERT INTO object_example (object_column)
  SELECT OBJECT_CONSTRUCT('thirteen', 13, 'zero', 0);
SELECT * FROM object_example;

SELECT OBJECT_CONSTRUCT(
  'name', 'Jones'::VARIANT,
  'age',  42::VARIANT);

--------------------------------------------------------------------------
-- avg
CREATE OR REPLACE TABLE avg_example(int_col int, d decimal(10,5), s1 varchar(10), s2 varchar(10));
INSERT INTO avg_example VALUES
    (1, 1.1, '1.1','one'), 
    (1, 10, '10','ten'),
    (2, 2.4, '2.4','two'), 
    (2, NULL, NULL, 'NULL'),
    (3, NULL, NULL, 'NULL'),
    (NULL, 9.9, '9.9','nine');

SELECT AVG(int_col), AVG(d), avg(distinct d), avg(s1), 
-- avg(s2)
    FROM avg_example;

--------------------------------------------------------------------------
-- flatten

SELECT * FROM TABLE(FLATTEN(input => parse_json('{"a":1, "b":[77,88], "c": {"d":"X"}}'))) f;

SELECT * FROM TABLE(FLATTEN(input => parse_json('{"a":1, "b":[77,88], "c": {"d":"X"}}'),
                            recursive => true )) f;

 create or replace table persons as
    select column1 as id, parse_json(column2) as c
 from values
   (12712555,
   '{ name:  { first: "John", last: "Smith"},
     contact: [
     { business:[
       { type: "phone", content:"555-1234" },
       { type: "email", content:"j.smith@company.com" } ] } ] }'),
   (98127771,
   '{ name:  { first: "Jane", last: "Doe"},
     contact: [
     { business:[
       { type: "phone", content:"555-1236" },
       { type: "email", content:"j.doe@company.com" } ] } ] }') v;

 SELECT id as "ID",
   f.value AS "Contact",
   f1.value:type AS "Type",
   f1.value:content AS "Details"
 FROM persons p,
   lateral flatten(input => p.c, path => 'contact') f,
   lateral flatten(input => f.value:business) f1;

SELECT * FROM TABLE(FLATTEN(input => parse_json('{"a":1, "b":[77,88]}'), outer => true)) f;



--------------------------------------------------------------------------
-- mv
CREATE TABLE inventory (product_ID INTEGER, wholesale_price FLOAT,
  description VARCHAR);
    
CREATE OR REPLACE MATERIALIZED VIEW mv1 AS
  SELECT product_ID, wholesale_price FROM inventory;

INSERT INTO inventory (product_ID, wholesale_price, description) VALUES 
    (1, 1.00, 'cog');

SELECT product_ID, wholesale_price FROM mv1;

CREATE or replace table sales (product_ID INTEGER, quantity INTEGER, price FLOAT);

INSERT INTO sales (product_ID, quantity, price) VALUES 
   (1,  1, 1.99);

CREATE or replace VIEW profits AS
  SELECT m.product_ID, SUM(IFNULL(s.quantity, 0)) AS quantity,
      SUM(IFNULL(quantity * (s.price - m.wholesale_price), 0)) AS profit
    FROM mv1 AS m LEFT OUTER JOIN sales AS s ON s.product_ID = m.product_ID
    GROUP BY m.product_ID;

select * from profits;

ALTER MATERIALIZED VIEW mv1 SUSPEND;
    
INSERT INTO inventory (product_ID, wholesale_price, description) VALUES 
    (2, 2.00, 'sprocket');

INSERT INTO sales (product_ID, quantity, price) VALUES 
   (2, 10, 2.99),
   (2,  1, 2.99);

SELECT * FROM profits ORDER BY product_ID;

ALTER MATERIALIZED VIEW mv1 RESUME;

CREATE TABLE pipeline_segments (
    segment_ID BIGINT,
    material VARCHAR, -- e.g. copper, cast iron, PVC.
    installation_year DATE,  -- older pipes are more likely to be corroded.
    rated_pressure FLOAT  -- maximum recommended pressure at installation time.
    );
    
INSERT INTO pipeline_segments 
    (segment_ID, material, installation_year, rated_pressure)
  VALUES
    (1, 'PVC', '1994-01-01'::DATE, 60),
    (2, 'cast iron', '1950-01-01'::DATE, 120)
    ;

CREATE TABLE pipeline_pressures (
    segment_ID BIGINT,
    pressure_psi FLOAT,  -- pressure in Pounds per Square Inch
    measurement_timestamp TIMESTAMP
    );
INSERT INTO pipeline_pressures 
   (segment_ID, pressure_psi, measurement_timestamp) 
  VALUES
    (2, 10, '2018-09-01 00:01:00'),
    (2, 95, '2018-09-01 00:02:00')
    ;

CREATE MATERIALIZED VIEW vulnerable_pipes 
  (segment_ID, installation_year, rated_pressure) 
  AS
    SELECT segment_ID, installation_year, rated_pressure
        FROM pipeline_segments 
        WHERE material = 'cast iron' AND installation_year < '1980'::DATE;

ALTER MATERIALIZED VIEW vulnerable_pipes CLUSTER BY (installation_year);

CREATE VIEW high_risk AS
    SELECT seg.segment_ID, installation_year, measurement_timestamp::DATE AS measurement_date, 
         DATEDIFF('YEAR', installation_year::DATE, measurement_timestamp::DATE) AS age, 
         rated_pressure - age AS safe_pressure, pressure_psi AS actual_pressure
       FROM vulnerable_pipes AS seg INNER JOIN pipeline_pressures AS psi 
           ON psi.segment_ID = seg.segment_ID
       WHERE pressure_psi > safe_pressure
       ;

select * from high_risk;

--------------------------------------------------------------------------
-- count

CREATE TABLE basic_example (i_col INTEGER, j_col INTEGER);
INSERT INTO basic_example VALUES
    (11,101), (11,102), (11,NULL), (12,101), (NULL,101), (NULL,102);

select * from basic_example
order by i_col;

SELECT COUNT(*) AS "All",
       COUNT(* ILIKE 'i_c%') AS "ILIKE",
       COUNT(* EXCLUDE i_col) AS "EXCLUDE",
       COUNT(i_col) AS "i_col", 
       COUNT(DISTINCT i_col) AS "DISTINCT i_col", 
       COUNT(j_col) AS "j_col", 
       COUNT(DISTINCT j_col) AS "DISTINCT j_col"
  FROM basic_example;

SELECT i_col, COUNT(*), COUNT(j_col)
    FROM basic_example
    GROUP BY i_col
    ORDER BY i_col;

CREATE OR REPLACE TABLE count_example_with_variant_column (
  i_col INTEGER, 
  j_col INTEGER, 
  v VARIANT);



INSERT INTO count_example_with_variant_column (i_col, j_col, v) 
  VALUES (NULL, 10, NULL);
INSERT INTO count_example_with_variant_column (i_col, j_col, v) 
  SELECT 1, 11, PARSE_JSON('{"Title": null}');
INSERT INTO count_example_with_variant_column (i_col, j_col, v) 
  SELECT 2, 12, PARSE_JSON('{"Title": "O"}');
INSERT INTO count_example_with_variant_column (i_col, j_col, v) 
  SELECT 3, 12, PARSE_JSON('{"Title": "I"}');

SELECT i_col, j_col, v, v:Title
    FROM count_example_with_variant_column
    ORDER BY i_col;

SELECT COUNT(v:Title)
    FROM count_example_with_variant_column;

-------------------------------------------------------------------------------------------------
-- join
CREATE or replace TABLE PROJECTS (
    PROJECT_ID INTEGER,
    PROJECT_NAME STRING
);

CREATE or replace TABLE EMPLOYEES (
    EMPLOYEE_ID INTEGER,
    EMPLOYEE_NAME STRING,
    PROJECT_ID INTEGER
);

INSERT INTO PROJECTS (PROJECT_ID, PROJECT_NAME) VALUES
(1000, 'COVID-19 Vaccine'),
(1001, 'Malaria Vaccine'),
(1002, 'NewProject');

INSERT INTO EMPLOYEES (EMPLOYEE_ID, EMPLOYEE_NAME, PROJECT_ID) VALUES
(10000001, 'Terry Smith', 1000),
(10000002, 'Maria Inverness', 1000),
(10000003, 'Pat Wang', 1001),
(10000004, 'NewEmployee', NULL);


SELECT p.project_ID, project_name, employee_ID, employee_name, e.project_ID
    FROM projects AS p JOIN employees AS e
        ON e.project_ID = p.project_ID
    ORDER BY p.project_ID, e.employee_ID;

SELECT *
    FROM projects NATURAL JOIN employees
    ORDER BY employee_ID;

SELECT *
    FROM projects AS p INNER JOIN employees AS e
        ON e.project_ID = p.project_ID
    ORDER BY p.project_ID, e.employee_ID;

explain
SELECT *
    FROM projects, employees
    ORDER BY employee_ID;

SELECT p.project_name, e.employee_name
    FROM projects AS p FULL OUTER JOIN employees AS e
        ON e.project_ID = p.project_ID
    ORDER BY p.project_name, e.employee_name;

SELECT p.project_name, e.employee_name
    FROM projects AS p CROSS JOIN employees AS e
    ORDER BY p.project_ID, e.employee_ID;

-------------------------------------------------------------------------------------------------
-- sha2
SELECT sha2('Snowflake', 224);

SELECT sha2(null, 256);

SELECT sha2(1, 256);


create or replace table t1 (c1 varchar, c2 number);
insert into t1 values ('a', 1), ('b', 2), ('c', 3);

select c1, sha2(c1, 256) from t1;
select c2, sha2(c2, 256) from t1;

-------------------------------------------------------------------------------------------------
-- load conversion

create or replace table mytable (
  col1 number autoincrement start 1 increment 1,
  col2 varchar,
  col3 varchar
  );

select $1, $2 from @mystage1/tmp/myfile.csv;

copy into mytable (col2, col3)
from (
  select $1, $2
  from @mystage1/tmp/myfile.csv t
)
;

select * from mytable;

-- json
create or replace stage mystage_json
  file_format = (type = 'json');

select $1 from @mystage_json/sales.json;

list @mystage_json;

 CREATE OR REPLACE TABLE home_sales (
   CITY VARCHAR,
   POSTAL_CODE VARCHAR,
   SQ_FT NUMBER,
   SALE_DATE DATE,
   PRICE NUMBER
 );

 COPY INTO home_sales(city, postal_code, sq_ft, sale_date, price)
 FROM (select
 $1:location.city::varchar,
 $1:location.zip::varchar,
 $1:dimensions.sq_ft::number,
 $1:sale_date::date,
 $1:price::number
 FROM @mystage_json/sales.json t);

select * from home_sales;


create or replace table flattened_source
(seq string, key string, path string, index string, value variant, element variant)
as
  select
    seq::string
  , key::string
  , path::string
  , index::string
  , value::variant
  , this::variant
  from @mystage_json/sales.json
    , table(flatten(input => parse_json($1)));

select * from flattened_source;

list @mystage_json;

-- @mystage_json/ipaddress.json

select $1 from @mystage_json/ipaddress.json;

create or replace table splitjson (
  col1 array,
  col2 array
  );

copy into splitjson
  from (
    select split($1:ip_address.router1, '.'), split($1:ip_address.router2, '.')
    from @mystage_json/ipaddress.json t)
;

select * from splitjson;


select
 $1:location.city::varchar,
 $1:location.zip::varchar,
 $1:dimensions.sq_ft::number,
 $1:sale_date::date,
 $1:price::number
 FROM @mystage_json/sales.json t;

-------------------------------------------------------------------------------------------------
-- timezone
SELECT TO_TIMESTAMP_TZ('04/05/2024 01:02:03', 'mm/dd/yyyy hh24:mi:ss');

SELECT TO_TIMESTAMP('2024/12/12 01:02:03');


-------------------------------------------------------------------------------------------------
-- parse json

CREATE OR REPLACE TABLE vartab (n NUMBER(2), v VARIANT);

INSERT INTO vartab
  SELECT column1 AS n, PARSE_JSON(column2) AS v
    FROM VALUES (1, 'null'), 
                (2, null), 
                (3, 'true'),
                (4, '-17'), 
                (5, '123.12'), 
                (6, '1.912e2'),
                (7, '"Om ara pa ca na dhih"  '), 
                (8, '[-1, 12, 289, 2188, false,]'), 
                (9, '{ "x" : "abc", "y" : false, "z": 10} ') 
       AS vals;

SELECT n, v, TYPEOF(v)
  FROM vartab
  ORDER BY n;

desc table vartab;


SELECT TO_JSON(PARSE_JSON('{"b":1,"a":2}')),
       TO_JSON(PARSE_JSON('{"b":1,"a":2}')) = '{"b":1,"a":2}',
       TO_JSON(PARSE_JSON('{"b":1,"a":2}')) = '{"a":2,"b":1}';


select to_json(parse_json('{"b":1,"a":2}'));

CREATE OR REPLACE TABLE jdemo3 (
  variant1 VARIANT,
  variant2 VARIANT);

INSERT INTO jdemo3 (variant1, variant2)
  SELECT
    PARSE_JSON('{"PI":3.14}'),
    TO_VARIANT('{"PI":3.14}');

SELECT variant1,
       TYPEOF(variant1),
       variant2,
       TYPEOF(variant2),
       variant1 = variant2
  FROM jdemo3;

-------------------------------------------------------------------------------------------------
-- using template

show stages;
show file formats;

list @mystage1;

-- error
create or replace my_new_table
using template (
  select array_agg(object_construct(*))
  from table(infer_schema(
    location => '@mystage1/tmp/contacts.json',
    file_format => 'my_json_format'
  ))
)
;

CREATE TABLE mytable
  USING TEMPLATE (
    SELECT ARRAY_AGG(OBJECT_CONSTRUCT(*))
      FROM TABLE(
        INFER_SCHEMA(
          LOCATION=>'@mystage1/tmp/contacts.json',
          FILE_FORMAT=>'my_json_format'
        )
      ));

select * from mytable;
desc table mytable;


-------------------------------------------------------------------------------------------------
-- copy history

select *
from table(information_schema.copy_history(TABLE_NAME=>'ORDERS', START_TIME=> DATEADD(hours, -1, CURRENT_TIMESTAMP())));

show tables;

-------------------------------------------------------------------------------------------------
-- Account Usage
use role accountadmin;
use schema snowflake.account_usage;

select user_name,
       count(*) as failed_logins,
       avg(seconds_between_login_attempts) as average_seconds_between_login_attempts
from (
      select user_name,
             timediff(seconds, event_timestamp, lead(event_timestamp)
                 over(partition by user_name order by event_timestamp)) as seconds_between_login_attempts
      from login_history
      where event_timestamp > date_trunc(month, current_date)
      and is_success = 'NO'
     )
group by 1
order by 3;

select warehouse_name,
  sum(credits_used) as total_credits_used
from warehouse_metering_history
where start_time >= dateadd(month, -1, current_date)
group by 1
order by 2 desc;

select user_name,
       sum(execution_time) as average_execution_time
from query_history
where start_time >= date_trunc(month, current_date)
group by 1
order by 2 desc;



-------------------------------------------------------------------------------------------------
-- access history
show tables;

select * from orders sample(10);

use role accountadmin;
select * from snowflake.account_usage.access_history
order by query_start_time desc
limit 10;

select 
  user_name,
  query_id,
  query_start_time,
  direct_objects_accessed,
  base_objects_accessed
from snowflake.account_usage.access_history,

where
  query_start_time >= dateadd(day, -7, current_date)
  -- and
  -- base_objects_accessed alike '%orders%'
order by 3 desc;

SELECT distinct user_name
FROM access_history
     , lateral flatten(base_objects_accessed) f1
WHERE f1.value:"objectId"::int=<fill_in_object_id>
AND f1.value:"objectDomain"::string='Table'
AND query_start_time >= dateadd('day', -30, current_timestamp())
;

desc table orders;

show tables;

SHOW OBJECTS IN SCHEMA tacchan_db.public;

SELECT *
FROM TABLE(RESULT_SCAN(LAST_QUERY_ID()))
WHERE "name" = 'orders';

use role accountadmin;
use schema snowflake.account_usage;

select * from tables
where table_name = 'ORDERS'
  and deleted is null
;

-- 7172

SELECT distinct user_name
FROM access_history
     , lateral flatten(base_objects_accessed) f1
WHERE f1.value:"objectId"::int=7172
AND f1.value:"objectDomain"::string='Table'
AND query_start_time >= dateadd('day', -30, current_timestamp())
;

SELECT query_id
       , query_start_time
FROM access_history
     , lateral flatten(base_objects_accessed) f1
WHERE f1.value:"objectId"::int=7172
AND f1.value:"objectDomain"::string='Table'
AND query_start_time >= dateadd('day', -30, current_timestamp())
;


select 
  distinct f4.value as column_name
from access_history,
  lateral flatten(base_objects_accessed) f1,
  lateral flatten(f1.value) f2,
  lateral flatten(f2.value) f3,
  lateral flatten(f3.value) f4
where f1.value:"objectId"::int=7172
  and f1.value:"objectDomain"::string='Table'
  and f4.key = 'columnName'
;

-- stage
use role sysadmin;
use schema tacchan_db.public;

list @mystage1;

select $1, $2 from @mystage1/tmp/data1.csv (file_format => myformat);

create table table1 (col1 varchar, col2 varchar);

copy into table1
from (select $1, $2 from @mystage1/tmp/data1.csv (file_format => myformat));

-- あとで書き込みを見る

copy into @%orders/orders1.csv
  from (select * from orders sample(1));

-- 機密性の高い
-- drop table t1;
create schema test_schema;
use schema test_schema;
create or replace table T1(content variant);
insert into T1(content) select parse_json('{"name": "A", "id":1}');

-- drop table t6;
create table t6 like t1;
insert into t6 select * from t1 sample(10);

-- s6
create stage s1;
copy into @s1 from t1;

create table t2 as
  select content:"name" as name,
         content:"id" as id
  from t1;

create stage s2;
copy into @s2 from t1;

create or replace table T3(customer_info variant);
copy into T3 from @s1;

-- T1 -> T4
create or replace table T4(name string, id string, address string);
insert into T4(name, id) select content:"name", content:"id" from T1;

-- T6 -> T7
create table T7 as select * from T6;

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