
use demo_db.public;

---test
create or replace table start_date (date date) as (
    select current_date()
);