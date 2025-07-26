-- SQL script to create tables for the Mars Data Warehouse

-- Drop tables if they exist to allow for clean re-runs
DROP TABLE IF EXISTS Fact_Mars_Observations CASCADE;
DROP TABLE IF EXISTS Dim_Rover CASCADE;
DROP TABLE IF EXISTS Dim_Camera CASCADE;
DROP TABLE IF EXISTS Dim_Time CASCADE;

-- Dim_Rover Table
CREATE TABLE Dim_Rover (
    rover_id INT PRIMARY KEY,
    rover_name VARCHAR(50) NOT NULL,
    landing_date DATE,
    launch_date DATE,
    status VARCHAR(20)
);

-- Dim_Camera Table
CREATE TABLE Dim_Camera (
    camera_id INT PRIMARY KEY,
    camera_name VARCHAR(500) NOT NULL,
    full_name VARCHAR(500)
);

-- Dim_Time Table
CREATE TABLE Dim_Time (
    date_key INT PRIMARY KEY, -- YYYYMMDD
    earth_date DATE NOT NULL UNIQUE,
    sol INT,
    year INT,
    month INT,
    day INT,
    day_of_week VARCHAR(10),
    season VARCHAR(50)
);

-- Fact_Mars_Observations Table (without foreign keys)
CREATE TABLE Fact_Mars_Observations (
    observation_id SERIAL PRIMARY KEY,
    sol INT,
    earth_date DATE,
    rover_id INT,
    camera_id INT,
    photo_count INT,
    avg_temp_celsius DECIMAL(5, 2),
    min_temp_celsius DECIMAL(5, 2),
    max_temp_celsius DECIMAL(5, 2),
    avg_pressure_pa DECIMAL(10, 2),
    avg_wind_speed_mps DECIMAL(5, 2)
);

-- Indexing for performance (Foreign key indexes are not needed if FKs are removed)
CREATE INDEX idx_fact_sol ON Fact_Mars_Observations (sol);
CREATE INDEX idx_fact_earth_date ON Fact_Mars_Observations (earth_date);
CREATE INDEX idx_dim_time_earth_date ON Dim_Time (earth_date);

