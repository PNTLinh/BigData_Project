-- Tạo Keyspace
CREATE KEYSPACE IF NOT EXISTS mta_data 
WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1};
USE mta_data;

-- Tạo Table
CREATE TABLE IF NOT EXISTS mta_data.train_updates (
    trip_id text,
    route_id text,
    stop_id text,
    arrival_timestamp bigint,
    processed_at timestamp,
    PRIMARY KEY ((route_id), stop_id, trip_id)
);

CREATE TABLE IF NOT EXISTS daily_route_stats (
    arrival_date date,
    route_id text,
    total_trips bigint,
    avg_arrival_ts double,
    PRIMARY KEY (arrival_date, route_id)
);