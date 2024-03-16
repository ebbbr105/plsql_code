-- compares number of rows in oracle and greenplum same tables after migration
create or replace function testing.row_cnt ()
RETURNS text
LANGUAGE plpgsql
VOLATILE
AS $$

DECLARE
    e record;
    g record;

    ex_tbl_name TEXT;
    ex_cnt_rec numeric;

    gp_tbl_name TEXT;
    gp_cnt_rec numeric;

    l_sql_text text;

begin

    truncate testing.temp_cnt;

    FOR e IN (
        SELECT table_schema, table_name 
        FROM information_schema.tables
        where 1 = 1
        and table_schema <> 'pg_catalog' 
        and table_schema <> 'gp_toolkit' 
        and table_schema <> 'information_schema'
        and table_name not like '%$%' 
        and table_name not like '%prt%' 
        and table_schema like 'schema%' 
        and table_name like '%table_name%'
        order by table_name
        OFFSET 3

    LOOP
        execute 'SELECT ''' || e.table_name || ''', COUNT (*) FROM ' || quote_ident(e.table_schema) || '.' || quote_ident(e.table_name) into strict ex_tbl_name, ex_cnt_rec;
        l_sql_text := 'INSERT INTO testing.temp_cnt (ora_tbl_name, ora_r_cnt) VALUES (''' || ex_tbl_name || ''', ' || ex_cnt_rec || ')';
        execute l_sql_text;
    END LOOP;

    begin
    EXCEPTION 
    WHEN others THEN
    RAISE NOTICE 'SQLSTATE: % - Код ошибки', SQLSTATE;
    END;

    return 'Успешная загрузка';
END;
$$
EXECUTE ON ANY;