
use demo_db.public;

create or replace table start_date as (
    select current_date()
);