SELECT concat("OPTIMIZE TABLE ", table_schema, ".", table_name, ";") FROM
information_schema.tables WHERE DATA_FREE > 0 INTO OUTFILE '/tmp/optimize.sql';
SOURCE /tmp/optimize.sql;
