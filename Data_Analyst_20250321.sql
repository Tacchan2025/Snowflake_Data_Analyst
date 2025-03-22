use role sysadmin;
use warehouse tacchan_wh;
use database tacchan_db;
use schema public;
-------------------------------------------------------------------------------------------------

-------------------------------------------------------------------------------------------------
-- row access policy




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