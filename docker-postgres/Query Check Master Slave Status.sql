-- CREATE USER reading_user WITH PASSWORD 'reading_pass';
-- GRANT CONNECT ON DATABASE my_database TO reading_user;
-- connect my_database
-- GRANT SELECT ON ALL TABLES IN SCHEMA public TO reading_user;
-- GRANT SELECT ON ALL SEQUENCES IN SCHEMA public TO reading_user;
-- GRANT USAGE ON SCHEMA public TO reading_user;
-- ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT SELECT ON TABLES TO reading_user;

-- Check Replication Status on Master
SELECT * FROM pg_stat_replication;
-- Check Replication Status on Slave
select * from pg_stat_wal_receiver;
SELECT * FROM pg_replication_slots;