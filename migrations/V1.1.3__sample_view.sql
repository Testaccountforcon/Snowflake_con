use demo_db.public;

create view sample_view as (
    current_date() as col1
);