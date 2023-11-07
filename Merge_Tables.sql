-- Basic Example
MERGE merge_example.table_data T -- as Target_Table
USING merge_example.table_changes S -- as Source_Table
ON T.id = S.id
WHEN MATCHED THEN
  UPDATE SET T.value = S.value
WHEN NOT MATCHED THEN
  INSERT (id, value) VALUES(id, value)
;

-- Complicate Example
MERGE `project.dataset.table` T -- as Target_Table
USING (
    WITH t1_example AS (
        SELECT *
        FROM(
            SELECT *
            FROM subqueries
        ) AS tb1
    )
    , t2_example AS (
        SELECT *
        FROM
        (
            SELECT *
            FROM subquery_a
        ) AS a
        LEFT JOIN
        (
            SELECT *
            FROM subquery_b
        ) AS b ON a.id= b.id
        GROUP BY x,y,z
    )
    , t3_example AS (
        SELECT *
        FROM t3
        LEFT JOIN join_example AS je
            ON t3.id = je.id
    )
    SELECT *
    FROM t1
    FULL JOIN (
        SELECT * 
        FROM (
            SELECT *
            FROM t2
        ) AS t3
        ON t1.event_id = t3.event_id
) S -- as Source_Table
ON T.id1 = Source_Table.id1 AND T.id2 = S.id2 AND T.id3 = S.id3
WHEN NOT MATCHED BY SOURCE AND T.condition1 = some_condition_parameter AND T.utc_date = some_date_parameter THEN
    DELETE
WHEN MATCHED THEN
UPDATE SET T.variable1 = S.variable1
    , T.variable2 = S.variable2
    , T.variable3 = S.variable3
    ...
    ...
    ...
    , T.variableN = S.variableN
WHEN NOT MATCHED THEN
INSERT(variable1, variable2, variable3, ... , variableN)
VALUES(value1, value2, value3, ... , valueN)
;
