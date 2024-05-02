use demo_db.public;

create or replace view date_transform (date date) as (
    select date_trunc('month', date) as month_of_date 
    from current_date
);