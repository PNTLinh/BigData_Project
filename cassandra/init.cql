CREATE KEYSPACE IF NOT EXISTS taxi_streaming
WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1};

USE taxi_streaming;

CREATE TABLE IF NOT EXISTS zone_performance_30m (
    window_start TIMESTAMP,
    window_end TIMESTAMP,
    pickup_zone TEXT,
    total_trips BIGINT,
    total_revenue DOUBLE,
    avg_fare_per_mile DOUBLE,
    avg_speed_mph DOUBLE,
    avg_duration_sec DOUBLE,
    PRIMARY KEY ((pickup_zone), window_start)
) WITH CLUSTERING ORDER BY (window_start DESC);

CREATE TABLE IF NOT EXISTS zone_performance_5m (
    window_start TIMESTAMP,
    window_end TIMESTAMP,
    pickup_zone TEXT,
    total_trips BIGINT,
    total_revenue DOUBLE,
    avg_fare_per_mile DOUBLE,
    avg_speed_mph DOUBLE,
    avg_duration_sec DOUBLE,
    PRIMARY KEY ((pickup_zone), window_start)
) WITH CLUSTERING ORDER BY (window_start DESC);

CREATE TABLE IF NOT EXISTS zone_performance_1h (
    window_start TIMESTAMP,
    window_end TIMESTAMP,
    pickup_zone TEXT,
    total_trips BIGINT,
    total_revenue DOUBLE,
    avg_fare_per_mile DOUBLE,
    avg_speed_mph DOUBLE,
    avg_duration_sec DOUBLE,
    PRIMARY KEY ((pickup_zone), window_start)
) WITH CLUSTERING ORDER BY (window_start DESC);

CREATE TABLE IF NOT EXISTS zone_performance_1d (
    window_start TIMESTAMP,
    window_end TIMESTAMP,
    pickup_zone TEXT,
    total_trips BIGINT,
    total_revenue DOUBLE,
    avg_fare_per_mile DOUBLE,
    avg_speed_mph DOUBLE,
    avg_duration_sec DOUBLE,
    PRIMARY KEY ((pickup_zone), window_start)
) WITH CLUSTERING ORDER BY (window_start DESC);

CREATE TABLE IF NOT EXISTS global_kpis_30m (
    shard_id TEXT, 
    window_start TIMESTAMP,
    window_end TIMESTAMP,
    total_trips BIGINT,
    total_revenue DOUBLE,
    avg_speed_mph DOUBLE,
    avg_tip_ratio DOUBLE,
    PRIMARY KEY ((shard_id), window_start)
) WITH CLUSTERING ORDER BY (window_start DESC);

CREATE TABLE IF NOT EXISTS global_kpis_5m (
    shard_id TEXT, 
    window_start TIMESTAMP,
    window_end TIMESTAMP,
    total_trips BIGINT,
    total_revenue DOUBLE,
    avg_speed_mph DOUBLE,
    avg_tip_ratio DOUBLE,
    PRIMARY KEY ((shard_id), window_start)
) WITH CLUSTERING ORDER BY (window_start DESC);

CREATE TABLE IF NOT EXISTS global_kpis_1h (
    shard_id TEXT, 
    window_start TIMESTAMP,
    window_end TIMESTAMP,
    total_trips BIGINT,
    total_revenue DOUBLE,
    avg_speed_mph DOUBLE,
    avg_tip_ratio DOUBLE,
    PRIMARY KEY ((shard_id), window_start)
) WITH CLUSTERING ORDER BY (window_start DESC);

CREATE TABLE IF NOT EXISTS global_kpis_1d (
    shard_id TEXT, 
    window_start TIMESTAMP,
    window_end TIMESTAMP,
    total_trips BIGINT,
    total_revenue DOUBLE,
    avg_speed_mph DOUBLE,
    avg_tip_ratio DOUBLE,
    PRIMARY KEY ((shard_id), window_start)
) WITH CLUSTERING ORDER BY (window_start DESC);

CREATE TABLE IF NOT EXISTS peak_analysis_30m (
    window_start TIMESTAMP,
    peak_category TEXT,
    total_trips BIGINT,
    PRIMARY KEY ((peak_category), window_start)
) WITH CLUSTERING ORDER BY (window_start DESC);

CREATE TABLE IF NOT EXISTS peak_analysis_15m (
    window_start TIMESTAMP,
    peak_category TEXT,
    total_trips BIGINT,
    PRIMARY KEY ((peak_category), window_start)
) WITH CLUSTERING ORDER BY (window_start DESC);

CREATE TABLE IF NOT EXISTS peak_analysis_1d (
    window_start TIMESTAMP,
    peak_category TEXT,
    total_trips BIGINT,
    PRIMARY KEY ((peak_category), window_start)
) WITH CLUSTERING ORDER BY (window_start DESC);

CREATE TABLE IF NOT EXISTS payment_analysis_30m (
    window_start TIMESTAMP,
    payment_type TEXT,
    total_trips BIGINT,
    avg_tip_ratio DOUBLE,
    PRIMARY KEY ((payment_type), window_start)
) WITH CLUSTERING ORDER BY (window_start DESC);

CREATE TABLE IF NOT EXISTS payment_analysis_15m (
    window_start TIMESTAMP,
    payment_type TEXT,
    total_trips BIGINT,
    avg_tip_ratio DOUBLE,
    PRIMARY KEY ((payment_type), window_start)
) WITH CLUSTERING ORDER BY (window_start DESC);

CREATE TABLE IF NOT EXISTS payment_analysis_1d (
    window_start TIMESTAMP,
    payment_type TEXT,
    total_trips BIGINT,
    avg_tip_ratio DOUBLE,
    PRIMARY KEY ((payment_type), window_start)
) WITH CLUSTERING ORDER BY (window_start DESC);