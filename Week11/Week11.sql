-- FROSTY FRIDAY WEEK 11

-- Set the database and schema
use database general_database;
use schema public;

CREATE OR REPLACE FILE FORMAT frosty_format TYPE = 'csv' skip_header = 1;

-- Create the stage that points at the data.
create or replace stage week_11_frosty_stage
    url = 's3://frostyfridaychallenges/challenge_11/'
    file_format = (TYPE = csv);
    list @week_11_frosty_stage;
-- Create the table as a CTAS statement.
create or replace table week11 as
select m.$1 as milking_datetime,
        m.$2 as cow_number,
        m.$3 as fat_percentage,
        m.$4 as farm_code,
        m.$5 as centrifuge_start_time,
        m.$6 as centrifuge_end_time,
        m.$7 as centrifuge_kwph,
        m.$8 as centrifuge_electricity_used,
        m.$9 as centrifuge_processing_time,
        m.$10 as task_used
from @week_11_frosty_stage (file_format => frosty_format, pattern => '.*milk_data.*[.]csv') m;
select * from week11;

-- TASK 1: Remove all the centrifuge dates and centrifuge kwph and replace them with NULLs WHERE fat = 3. 
-- Add note to task_used.
create or replace task whole_milk_updates
    schedule = '1400 minutes'
as
    update week11 set centrifuge_start_time = NULL, centrifuge_end_time = NULL, centrifuge_kwph = NULL, task_used = 'whole_milk' where fat_percentage = 3;


-- TASK 2: Calculate centrifuge processing time (difference between start and end time) WHERE fat != 3. 
-- Add note to task_used.
create or replace task skim_milk_updates
    after whole_milk_updates
as
    update week11 set centrifuge_processing_time = DATEDIFF(second, centrifuge_start_time::timestamp , centrifuge_end_time::timestamp), task_used = 'skim_milk' where fat_percentage <> 3;

-- Manually execute the task.
--alter task skim_milk_updates resume;
--alter task whole_milk_updates suspend;
select SYSTEM$TASK_DEPENDENTS_ENABLE('general_database.public.whole_milk_updates');
execute task whole_milk_updates;

-- Check that the data looks as it should.
select * from week11;

-- Check that the numbers are correct.
select task_used, count(*) as row_count from week11 group by task_used;
