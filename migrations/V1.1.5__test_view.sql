use demo_db;
create or replace view test_view(
     select *
     from demo_db.demo.hello_world
);
