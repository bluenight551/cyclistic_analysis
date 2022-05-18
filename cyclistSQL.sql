-- on windows, open SQL Shell (psql). we can use pgadmin, 
-- but the psql shell is faster.  

-- list all available databases 
\l 


-- connect to "Trackpath" database
\c Trackpath


-- create tables
CREATE TABLE IF NOT EXISTS trips2020_q1(
	ride_id VARCHAR(25) UNIQUE,
	rideable_type VARCHAR(25),
	started_at TIMESTAMP,
	ended_at TIMESTAMP,
	start_station_name VARCHAR(50),
	start_station_id VARCHAR(25),
	end_station_name VARCHAR(50),
	end_station_id VARCHAR(25),
	start_lat NUMERIC,
	start_lng NUMERIC,
	end_lat NUMERIC,
	end_lng NUMERIC, 
	member_casual VARCHAR(25));

CREATE TABLE IF NOT EXISTS trips2019_q4(
	ride_id VARCHAR(25) UNIQUE,
	started_at TIMESTAMP,
	ended_at TIMESTAMP,
	rideable_type VARCHAR(25),
	tripduration INTEGER,
	start_station_id VARCHAR(25),
	start_station_name VARCHAR(50),
	end_station_id VARCHAR(25),
	end_station_name VARCHAR(50),
	member_casual VARCHAR(25),
	gender VARCHAR(15),
	birthyear INTEGER);

CREATE TABLE IF NOT EXISTS trips2019_q3(
	ride_id VARCHAR(25) UNIQUE,
	started_at TIMESTAMP,
	ended_at TIMESTAMP,
	rideable_type VARCHAR(25),
	tripduration INTEGER,
	start_station_id VARCHAR(25),
	start_station_name VARCHAR(50),
	end_station_id VARCHAR(25),
	end_station_name VARCHAR(50),
	member_casual VARCHAR(25),
	gender VARCHAR(15),
	birthyear INTEGER);

CREATE TABLE IF NOT EXISTS trips2019_q2(
	ride_id VARCHAR(25) UNIQUE,
	started_at TIMESTAMP,
	ended_at TIMESTAMP,
	rideable_type VARCHAR(25),
	tripduration INTEGER,
	start_station_id VARCHAR(25),
	start_station_name VARCHAR(50),
	end_station_id VARCHAR(25),
	end_station_name VARCHAR(50),
	member_casual VARCHAR(25),
	gender VARCHAR(15),
	birthyear INTEGER);


-- copy data from csv files to psql tables
COPY trips2019_q2
FROM '...\Divvy_Trips_2019_Q2'
DELIMITER ','
CSV HEADER;

COPY trips2019_q3
FROM '...\Divvy_Trips_2019_Q2'
DELIMITER ','
CSV HEADER;

COPY trips2019_q4
FROM '...\Divvy_Trips_2019_Q2'
DELIMITER ','
CSV HEADER;

COPY trips2020_q1
FROM '...\Divvy_Trips_2019_Q2'
DELIMITER ','
CSV HEADER;


-- check all data were copied without issue
SELECT * FROM trips2019_q2 LIMIT 5;
SELECT * FROM trips2019_q3 LIMIT 5;
SELECT * FROM trips2019_q4 LIMIT 5;
SELECT * FROM trips2020_q1 LIMIT 5;


-- check all any nulls
SELECT * FROM trips2020_q1
WHERE ride_id IS NULL
AND started_at IS NULL
AND ended_at IS NULL
AND start_station_id IS NULL
AND start_station_name IS NULL
AND end_station_id IS NULL
AND end_station_name IS NULL
AND member_casual IS NULL;

SELECT * FROM trips2019_q4
WHERE ride_id IS NULL
AND started_at IS NULL
AND ended_at IS NULL
AND start_station_id IS NULL
AND start_station_name IS NULL
AND end_station_id IS NULL
AND end_station_name IS NULL
AND member_casual IS NULL;

SELECT * FROM trips2019_q3
WHERE ride_id IS NULL
AND started_at IS NULL
AND ended_at IS NULL
AND start_station_id IS NULL
AND start_station_name IS NULL
AND end_station_id IS NULL
AND end_station_name IS NULL
AND member_casual IS NULL;

SELECT * FROM trips2019_q2
WHERE ride_id IS NULL
AND started_at IS NULL
AND ended_at IS NULL
AND start_station_id IS NULL
AND start_station_name IS NULL
AND end_station_id IS NULL
AND end_station_name IS NULL
AND member_casual IS NULL;


-- just to keep the original data remain untouched
-- create a temporary table
CREATE TEMPORARY TABLE temp_all_trips(
	ride_id VARCHAR(25) UNIQUE,
	started_at TIMESTAMP,
	ended_at TIMESTAMP,
	start_station_id VARCHAR(25),
	start_station_name VARCHAR(50),
	end_station_id VARCHAR(25),
	end_station_name VARCHAR(50),
	member_casual VARCHAR(25));


-- create a query, insert the data to the temporary table
-- union all four tables with 'WITH' clause
WITH all_trips AS (
	SELECT ride_id, started_at, ended_at, start_station_id,
	start_station_name, end_station_id,	end_station_name, member_casual
	FROM trips2020_q1
	UNION
	SELECT ride_id, started_at, ended_at, start_station_id,
	start_station_name, end_station_id, end_station_name, member_casual
	FROM trips2019_q4
	UNION
	SELECT ride_id, started_at, ended_at, start_station_id,
	start_station_name, end_station_id,	end_station_name, member_casual
	FROM trips2019_q3
	UNION
	SELECT ride_id, started_at, ended_at, start_station_id,
	start_station_name,	end_station_id, end_station_name, member_casual
	FROM trips2019_q2)

INSERT INTO temp_all_trips
SELECT * FROM all_trips;


-- manipulate the data only in temporary table
-- counting numbers of rows that have start station name is 'HQ QR' 
-- and less than zero second
SELECT COUNT(*) FROM temp_all_trips
WHERE start_station_id = 'HQ QR'
OR EXTRACT(EPOCH FROM ended_at - started_at) < 0;


-- replace 'Subscriber' and 'Customer' with 'memeber' and 'casual' for data consistency
UPDATE temp_all_trips
SET member_casual = REPLACE(REPLACE(member_casual, 'Subscriber', 'member'), 'Customer', 'casual');


-- percentage of member and casual
WITH temp_version AS(
	SELECT * FROM temp_all_trips
	WHERE start_station_id <> 'HQ QR'
	OR EXTRACT(EPOCH FROM ended_at - started_at) > 0)

SELECT member_casual,
COUNT(*) * 100.0 / SUM(COUNT(*)) OVER() AS percentage
FROM temp_version
GROUP BY member_casual;


-- top five renting places
WITH temp_version AS(
	SELECT *, 
	EXTRACT(EPOCH FROM ended_at - started_at) AS ride_length,
	TO_CHAR(started_at, 'Day') AS day_of_week
	FROM temp_all_trips
	WHERE start_station_id <> 'HQ QR'
	OR EXTRACT(EPOCH FROM ended_at - started_at) > 0)

SELECT start_station_name,
COUNT(ride_length) AS total_rides
FROM temp_version
GROUP BY start_station_name
ORDER BY total_rides DESC
LIMIT 5;


-- top five returning places
WITH temp_version AS(
	SELECT *, 
	EXTRACT(EPOCH FROM ended_at - started_at) AS ride_length,
	TO_CHAR(started_at, 'Day') AS day_of_week
	FROM temp_all_trips
	WHERE start_station_id <> 'HQ QR'
	OR EXTRACT(EPOCH FROM ended_at - started_at) > 0)

SELECT end_station_name,
COUNT(ride_length) AS total_rides
FROM temp_version
GROUP BY end_station_name
ORDER BY total_rides DESC
LIMIT 5;


-- total numbers of rides by days of week
WITH temp_version AS(
	SELECT *, 
	EXTRACT(EPOCH FROM ended_at - started_at) AS ride_length,
	TO_CHAR(started_at, 'Day') AS day_of_week
	FROM temp_all_trips
	WHERE start_station_id <> 'HQ QR'
	OR EXTRACT(EPOCH FROM ended_at - started_at) > 0)

SELECT day_of_week, COUNT(ride_length)
FROM temp_version
GROUP BY day_of_week, TO_CHAR(started_at, 'd')
ORDER BY TO_CHAR(started_at, 'd');


-- numbers of rides by month of the year
WITH temp_version AS(
	SELECT *, 
	EXTRACT(EPOCH FROM ended_at - started_at) AS ride_length,
	TO_CHAR(started_at, 'Day') AS day_of_week
	FROM temp_all_trips
	WHERE start_station_id <> 'HQ QR'
	OR EXTRACT(EPOCH FROM ended_at - started_at) > 0)

SELECT CASE 
	WHEN DATE_PART('month', started_at) = 1 THEN 'Jan'
	WHEN DATE_PART('month', started_at) = 2 THEN 'Feb'
	WHEN DATE_PART('month', started_at) = 3 THEN 'Mar'
	WHEN DATE_PART('month', started_at) = 4 THEN 'Apr'
	WHEN DATE_PART('month', started_at) = 5 THEN 'May'
	WHEN DATE_PART('month', started_at) = 6 THEN 'Jun'
	WHEN DATE_PART('month', started_at) = 7 THEN 'Jul'
	WHEN DATE_PART('month', started_at) = 8 THEN 'Aug'
	WHEN DATE_PART('month', started_at) = 9 THEN 'Sep'
	WHEN DATE_PART('month', started_at) = 10 THEN 'Oct'
	WHEN DATE_PART('month', started_at) = 11 THEN 'Nov'
	WHEN DATE_PART('month', started_at) = 12 THEN 'Dec'
END AS month, COUNT(ride_length)
FROM temp_version
GROUP BY DATE_PART('month', started_at)
ORDER BY DATE_PART('month', started_at);


-- top 5 renting hours of the day
WITH temp_version AS(
	SELECT *, 
	EXTRACT(EPOCH FROM ended_at - started_at) AS ride_length,
	TO_CHAR(started_at, 'Day') AS day_of_week
	FROM temp_all_trips
	WHERE start_station_id <> 'HQ QR'
	OR EXTRACT(EPOCH FROM ended_at - started_at) > 0)

SELECT CASE 
	WHEN EXTRACT(HOUR FROM started_at) = 0 THEN '12 AM'
	WHEN EXTRACT(HOUR FROM started_at) = 1 THEN '1 AM'
	WHEN EXTRACT(HOUR FROM started_at) = 2 THEN '2 AM'
	WHEN EXTRACT(HOUR FROM started_at) = 3 THEN '3 AM'
	WHEN EXTRACT(HOUR FROM started_at) = 4 THEN '4 AM'
	WHEN EXTRACT(HOUR FROM started_at) = 5 THEN '5 AM'
	WHEN EXTRACT(HOUR FROM started_at) = 6 THEN '6 AM'
	WHEN EXTRACT(HOUR FROM started_at) = 7 THEN '7 AM'
	WHEN EXTRACT(HOUR FROM started_at) = 8 THEN '8 AM'
	WHEN EXTRACT(HOUR FROM started_at) = 9 THEN '9 AM'
	WHEN EXTRACT(HOUR FROM started_at) = 10 THEN '10 AM'
	WHEN EXTRACT(HOUR FROM started_at) = 11 THEN '11 AM'
	WHEN EXTRACT(HOUR FROM started_at) = 12 THEN '12 PM'
	WHEN EXTRACT(HOUR FROM started_at) = 13 THEN '1 PM'
	WHEN EXTRACT(HOUR FROM started_at) = 14 THEN '2 PM'
	WHEN EXTRACT(HOUR FROM started_at) = 15 THEN '3 PM'
	WHEN EXTRACT(HOUR FROM started_at) = 16 THEN '4 PM'
	WHEN EXTRACT(HOUR FROM started_at) = 17 THEN '5 PM'
	WHEN EXTRACT(HOUR FROM started_at) = 18 THEN '6 PM'
	WHEN EXTRACT(HOUR FROM started_at) = 19 THEN '7 PM'
	WHEN EXTRACT(HOUR FROM started_at) = 20 THEN '8 PM'
	WHEN EXTRACT(HOUR FROM started_at) = 21 THEN '9 PM'
	WHEN EXTRACT(HOUR FROM started_at) = 22 THEN '10 PM'
	WHEN EXTRACT(HOUR FROM started_at) = 23 THEN '11 PM'
END AS hour, COUNT(ride_length) AS rides
FROM temp_version
GROUP BY EXTRACT(HOUR FROM started_at)
ORDER BY rides DESC
LIMIT 5;


-- average trip duration by subscription type
WITH temp_version AS(
	SELECT *, 
	EXTRACT(EPOCH FROM ended_at - started_at)/60 AS ride_length,
	TO_CHAR(started_at, 'Day') AS day_of_week
	FROM temp_all_trips
	WHERE start_station_id <> 'HQ QR'
	OR EXTRACT(EPOCH FROM ended_at - started_at) > 0)

SELECT member_casual, ROUND(CAST(AVG(ride_length) AS NUMERIC), 2) AS average_minutes
FROM temp_version
GROUP BY member_casual;


-- average tripduration of each subscription type by day of the week
-- member type
WITH temp_version AS(
	SELECT *, 
	EXTRACT(EPOCH FROM ended_at - started_at)/60 AS ride_length,
	TO_CHAR(started_at, 'Day') AS day_of_week
	FROM temp_all_trips
	WHERE start_station_id <> 'HQ QR'
	OR EXTRACT(EPOCH FROM ended_at - started_at) > 0)

SELECT day_of_week AS day, ROUND(AVG(ride_length)::NUMERIC, 2) AS average_minutes
FROM temp_version
WHERE member_casual = 'member'
GROUP BY  day_of_week, TO_CHAR(started_at, 'd')
ORDER BY TO_CHAR(started_at, 'd');

-- casual type
WITH temp_version AS(
	SELECT *, 
	EXTRACT(EPOCH FROM ended_at - started_at)/60 AS ride_length,
	TO_CHAR(started_at, 'Day') AS day_of_week
	FROM temp_all_trips
	WHERE start_station_id <> 'HQ QR'
	OR EXTRACT(EPOCH FROM ended_at - started_at) > 0)

SELECT day_of_week AS day, ROUND(AVG(ride_length)::NUMERIC, 2) AS average_minutes
FROM temp_version
WHERE member_casual = 'casual'
GROUP BY  day_of_week, TO_CHAR(started_at, 'd')
ORDER BY TO_CHAR(started_at, 'd');


-- average tripduration of each subscription type by month of the year
-- member type
WITH temp_version AS(
	SELECT *, 
	EXTRACT(EPOCH FROM ended_at - started_at)/60 AS ride_length,
	TO_CHAR(started_at, 'Day') AS day_of_week
	FROM temp_all_trips
	WHERE start_station_id <> 'HQ QR'
	OR EXTRACT(EPOCH FROM ended_at - started_at) > 0)

SELECT CASE
	WHEN DATE_PART('month', started_at) = 1 THEN 'Jan'
	WHEN DATE_PART('month', started_at) = 2 THEN 'Feb'
	WHEN DATE_PART('month', started_at) = 3 THEN 'Mar'
	WHEN DATE_PART('month', started_at) = 4 THEN 'Apr'
	WHEN DATE_PART('month', started_at) = 5 THEN 'May'
	WHEN DATE_PART('month', started_at) = 6 THEN 'Jun'
	WHEN DATE_PART('month', started_at) = 7 THEN 'Jul'
	WHEN DATE_PART('month', started_at) = 8 THEN 'Aug'
	WHEN DATE_PART('month', started_at) = 9 THEN 'Sep'
	WHEN DATE_PART('month', started_at) = 10 THEN 'Oct'
	WHEN DATE_PART('month', started_at) = 11 THEN 'Nov'
	WHEN DATE_PART('month', started_at) = 12 THEN 'Dec'
END AS month, ROUND(AVG(ride_length)::NUMERIC, 2) AS average_minutes
FROM temp_version
WHERE member_casual = 'member'
GROUP BY DATE_PART('month', started_at)
ORDER BY DATE_PART('month', started_at);

-- casual type
WITH temp_version AS(
	SELECT *, 
	EXTRACT(EPOCH FROM ended_at - started_at)/60 AS ride_length,
	TO_CHAR(started_at, 'Day') AS day_of_week
	FROM temp_all_trips
	WHERE start_station_id <> 'HQ QR'
	OR EXTRACT(EPOCH FROM ended_at - started_at) > 0)

SELECT CASE
	WHEN DATE_PART('month', started_at) = 1 THEN 'Jan'
	WHEN DATE_PART('month', started_at) = 2 THEN 'Feb'
	WHEN DATE_PART('month', started_at) = 3 THEN 'Mar'
	WHEN DATE_PART('month', started_at) = 4 THEN 'Apr'
	WHEN DATE_PART('month', started_at) = 5 THEN 'May'
	WHEN DATE_PART('month', started_at) = 6 THEN 'Jun'
	WHEN DATE_PART('month', started_at) = 7 THEN 'Jul'
	WHEN DATE_PART('month', started_at) = 8 THEN 'Aug'
	WHEN DATE_PART('month', started_at) = 9 THEN 'Sep'
	WHEN DATE_PART('month', started_at) = 10 THEN 'Oct'
	WHEN DATE_PART('month', started_at) = 11 THEN 'Nov'
	WHEN DATE_PART('month', started_at) = 12 THEN 'Dec'
END AS month, ROUND(AV	G(ride_length)::NUMERIC, 2) AS average_minutes
FROM temp_version
WHERE member_casual = 'casual'
GROUP BY DATE_PART('month', started_at)
ORDER BY DATE_PART('month', started_at);


-- average tripduration of each subscription type by hour of the day
-- member type
WITH temp_version AS(
	SELECT *, 
	EXTRACT(EPOCH FROM ended_at - started_at)/60 AS ride_length,
	TO_CHAR(started_at, 'Day') AS day_of_week
	FROM temp_all_trips
	WHERE start_station_id <> 'HQ QR'
	OR EXTRACT(EPOCH FROM ended_at - started_at) > 0)

SELECT CASE 
	WHEN EXTRACT(HOUR FROM started_at) = 0 THEN '12 AM'
	WHEN EXTRACT(HOUR FROM started_at) = 1 THEN '1 AM'
	WHEN EXTRACT(HOUR FROM started_at) = 2 THEN '2 AM'
	WHEN EXTRACT(HOUR FROM started_at) = 3 THEN '3 AM'
	WHEN EXTRACT(HOUR FROM started_at) = 4 THEN '4 AM'
	WHEN EXTRACT(HOUR FROM started_at) = 5 THEN '5 AM'
	WHEN EXTRACT(HOUR FROM started_at) = 6 THEN '6 AM'
	WHEN EXTRACT(HOUR FROM started_at) = 7 THEN '7 AM'
	WHEN EXTRACT(HOUR FROM started_at) = 8 THEN '8 AM'
	WHEN EXTRACT(HOUR FROM started_at) = 9 THEN '9 AM'
	WHEN EXTRACT(HOUR FROM started_at) = 10 THEN '10 AM'
	WHEN EXTRACT(HOUR FROM started_at) = 11 THEN '11 AM'
	WHEN EXTRACT(HOUR FROM started_at) = 12 THEN '12 PM'
	WHEN EXTRACT(HOUR FROM started_at) = 13 THEN '1 PM'
	WHEN EXTRACT(HOUR FROM started_at) = 14 THEN '2 PM'
	WHEN EXTRACT(HOUR FROM started_at) = 15 THEN '3 PM'
	WHEN EXTRACT(HOUR FROM started_at) = 16 THEN '4 PM'
	WHEN EXTRACT(HOUR FROM started_at) = 17 THEN '5 PM'
	WHEN EXTRACT(HOUR FROM started_at) = 18 THEN '6 PM'
	WHEN EXTRACT(HOUR FROM started_at) = 19 THEN '7 PM'
	WHEN EXTRACT(HOUR FROM started_at) = 20 THEN '8 PM'
	WHEN EXTRACT(HOUR FROM started_at) = 21 THEN '9 PM'
	WHEN EXTRACT(HOUR FROM started_at) = 22 THEN '10 PM'
	WHEN EXTRACT(HOUR FROM started_at) = 23 THEN '11 PM'
END AS hour, ROUND(AVG(ride_length)::NUMERIC, 2) AS average_minutes
FROM temp_version
WHERE member_casual = 'member'
GROUP BY EXTRACT(HOUR FROM started_at)
ORDER BY EXTRACT(HOUR FROM started_at);

-- casual type
WITH temp_version AS(
	SELECT *, 
	EXTRACT(EPOCH FROM ended_at - started_at)/60 AS ride_length,
	TO_CHAR(started_at, 'Day') AS day_of_week
	FROM temp_all_trips
	WHERE start_station_id <> 'HQ QR'
	OR EXTRACT(EPOCH FROM ended_at - started_at) > 0)

SELECT CASE 
	WHEN EXTRACT(HOUR FROM started_at) = 0 THEN '12 AM'
	WHEN EXTRACT(HOUR FROM started_at) = 1 THEN '1 AM'
	WHEN EXTRACT(HOUR FROM started_at) = 2 THEN '2 AM'
	WHEN EXTRACT(HOUR FROM started_at) = 3 THEN '3 AM'
	WHEN EXTRACT(HOUR FROM started_at) = 4 THEN '4 AM'
	WHEN EXTRACT(HOUR FROM started_at) = 5 THEN '5 AM'
	WHEN EXTRACT(HOUR FROM started_at) = 6 THEN '6 AM'
	WHEN EXTRACT(HOUR FROM started_at) = 7 THEN '7 AM'
	WHEN EXTRACT(HOUR FROM started_at) = 8 THEN '8 AM'
	WHEN EXTRACT(HOUR FROM started_at) = 9 THEN '9 AM'
	WHEN EXTRACT(HOUR FROM started_at) = 10 THEN '10 AM'
	WHEN EXTRACT(HOUR FROM started_at) = 11 THEN '11 AM'
	WHEN EXTRACT(HOUR FROM started_at) = 12 THEN '12 PM'
	WHEN EXTRACT(HOUR FROM started_at) = 13 THEN '1 PM'
	WHEN EXTRACT(HOUR FROM started_at) = 14 THEN '2 PM'
	WHEN EXTRACT(	HOUR FROM started_at) = 15 THEN '3 PM'
	WHEN EXTRACT(HOUR FROM started_at) = 16 THEN '4 PM'
	WHEN EXTRACT(HOUR FROM started_at) = 17 THEN '5 PM'
	WHEN EXTRACT(HOUR FROM started_at) = 18 THEN '6 PM'
	WHEN EXTRACT(HOUR FROM started_at) = 19 THEN '7 PM'
	WHEN EXTRACT(HOUR FROM started_at) = 20 THEN '8 PM'
	WHEN EXTRACT(HOUR FROM started_at) = 21 THEN '9 PM'
	WHEN EXTRACT(HOUR FROM started_at) = 22 THEN '10 PM'
	WHEN EXTRACT(HOUR FROM started_at) = 23 THEN '11 PM'
END AS hour, ROUND(AVG(ride_length)::NUMERIC, 2) AS average_minutes
FROM temp_version
WHERE member_casual = 'casual'
GROUP BY EXTRACT(HOUR FROM started_at)
ORDER BY EXTRACT(HOUR FROM started_at), average_minutes;
