CREATE DATABASE flight_analytics;
\c flight_analytics;

-- Cleaned & validated data
CREATE TABLE IF NOT EXISTS flight_prices_clean (
    airline TEXT,
    source TEXT,
    destination TEXT,
    departure_datetime TIMESTAMP,
    arrival_datetime TIMESTAMP,
    duration_hrs NUMERIC,
    stopovers INT,
    class TEXT,
    booking_source TEXT,
    base_fare_bdt NUMERIC,
    tax_surcharge_bdt NUMERIC,
    total_fare_bdt NUMERIC,
    seasonality TEXT,
    days_before_departure INT
);

-- KPI 1: Average fare by airline
CREATE TABLE IF NOT EXISTS kpi_avg_fare_by_airline (
    airline TEXT,
    avg_total_fare_bdt NUMERIC
);

-- KPI 2: Booking count by airline
CREATE TABLE IF NOT EXISTS kpi_booking_count_by_airline (
    airline TEXT,
    booking_count INT
);

-- KPI 3: Most popular routes
CREATE TABLE IF NOT EXISTS kpi_popular_routes (
    route TEXT,
    booking_count INT
);

-- KPI 4: Seasonal fare comparison
CREATE TABLE IF NOT EXISTS kpi_seasonal_fares (
    seasonality TEXT,
    avg_total_fare_bdt NUMERIC
);
