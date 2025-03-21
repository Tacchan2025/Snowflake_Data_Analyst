use role sysadmin;
use warehouse tacchan_wh;
use database tacchan_db;
use schema public;
-------------------------------------------------------------------------------------------------


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