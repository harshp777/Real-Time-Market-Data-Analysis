

SELECT COUNT(*)
FROM RAW_FOOD_DATA





create or replace table row_count (
  index_no number autoincrement start 1 increment 1,
  no_of_rows bigint,
  date varchar(100),
  no_of_rows_added bigint,
  no_of_rows_in_transformed_table bigint
  );


---------------------------------------


call DATA_TRANSFORMATION();


EXPORT INTO STORED PROCEDURE

--   Create cron job task
--- Create the Task which calls the procedure

create or replace task transform_schema 
  warehouse = COMPUTE_WH 
  schedule = 'USING CRON 0 12 * * * UTC' --schedule daily at 12 PM UTC 
as 
  call DATA_TRANSFORMATION();

--start the task 
alter task transform_schema resume;


-------------Lets start from begining

SELECT COUNT(*) FROM RAW_FOOD_DATA;

SELECT * FROM ROW_COUNT;

  