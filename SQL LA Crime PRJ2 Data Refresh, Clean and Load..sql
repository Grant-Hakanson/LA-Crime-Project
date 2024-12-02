-- la_crime data set update --
/* 
   The la crime data set updates bimonthly. Below will be code that is used to take the updated data set 
   and place it into a temp table where it will be cleansd of the old data leaving only the new data. 
   Then it will be transformed in order to be added to the existing fact table and reference tables.
   There will be repeated code with slight alterations from la crime PRJ 1 plus unique code relevant 
   to the data updating proccess.
*/

-- Creating a temp table, loading the new data, filtering out old dates and removing duplicates. --

CREATE TEMP TABLE la_crimes_temp (
division_records_num varchar(10),
date_rptd timestamptz,
date_occ timestamptz,
time_occ time,
geo_area_num varchar(5),
area_name varchar(20),
rpt_dist_no varchar(5),
part_1_2 integer,
crime_code varchar(5),
crime_code_description text,
modus_operandi_code varchar(50),
victim_age smallint,
victim_sex varchar(1),
victim_descent varchar(1),   
premises_code integer,
premises_description text,
weapon_used_code varchar(3),
weapon_description varchar(50),
case_status text,
status_description text,
crime_code_1 varchar(5),
crime_code_2 varchar(5),
crime_code_3 varchar(5),
crime_code_4 varchar(5),
crime_location varchar(50),
cross_street varchar(50),
latitude double precision,
longtitude double precision
);


-- Import la_crimes_temp data into table.
COPY la_crimes_temp
FROM 'C:\Users\13179\Desktop\SQL Projects\LA Crime Project\Crime Data 11.27.2024.csv' 
WITH (FORMAT CSV, HEADER); 


-- Clean the Data
---Remove Duplicates
BEGIN TRANSACTION;

-- Create a backup of the original table, just in case (optional but recommended)
CREATE TEMP TABLE la_crimes_temp_backup AS 
SELECT * FROM la_crimes_temp;

-- Remove all existing data from the original table
TRUNCATE TABLE la_crimes_temp;

-- Insert distinct rows back into the original table
INSERT INTO la_crimes_temp
SELECT DISTINCT * FROM la_crimes_temp_backup;

--Drop the backup table if everything is successful
DROP TABLE la_crimes_temp_backup;


COMMIT;

-- Filtering data.
BEGIN TRANSACTION;

-- creating filtered  data to load into la_crimes_temp.
CREATE TEMP TABLE filtered_data AS
	Select * FROM
	(SELECT * FROM la_crimes_temp
	EXCEPT
	SELECT * FROM la_crimes)
	ORDER BY date_occ;
	
-- Dropping all rows to make room for filtered data.
TRUNCATE TABLE la_crimes_temp;

-- Load in the filtered data.
INSERT INTO la_crimes_temp
SELECT DISTINCT * FROM filtered_data;

-- Filtered table is now waste. Drop this table.
DROP TABLE filtered_data;

COMMIT;

--- Clean Address fields of abnormal spaces.
DO $$
DECLARE
    i INT;
BEGIN
    FOR i IN 1..6 LOOP
        -- Update operation
        UPDATE la_crimes_temp
        SET crime_location = TRIM(REPLACE(crime_location, '  ', ' ')),
            cross_street = TRIM(REPLACE(cross_street, '  ', ' '));

        -- Select operation
        PERFORM crime_location, cross_street
        FROM la_crimes_temp
        WHERE cross_street IS NOT NULL;
    END LOOP;
END $$;

-- Create Reference tables

-- Crimes
CREATE TEMP TABLE crimes_temp AS
SELECT DISTINCT 
    crime_code, 
    crime_code_description 
FROM la_crimes_temp
WHERE crime_code_description IS NOT NULL;

-- Weapons
CREATE TEMP TABLE crime_weapons_temp AS
SELECT DISTINCT 
    weapon_used_code,
    weapon_description
FROM la_crimes_temp
WHERE weapon_used_code IS NOT NULL AND weapon_description IS NOT NULL;


-- Case Details
CREATE TEMP TABLE case_details_temp AS
SELECT DISTINCT 
    case_status,
    status_description
FROM la_crimes_temp
WHERE case_status IS NOT NULL AND status_description IS NOT NULL;

-- Premises
CREATE TEMP TABLE premises_temp AS
SELECT DISTINCT 
    premises_code,
    premises_description
FROM la_crimes_temp
WHERE premises_code IS NOT NULL AND premises_description IS NOT NULL;

-- Area 
CREATE TEMP TABLE crime_geos_temp AS
SELECT DISTINCT 
    geo_area_num,
    area_name
FROM la_crimes_temp
WHERE geo_area_num IS NOT NULL AND area_name IS NOT NULL;

-- Victim Descent
CREATE TEMP TABLE crime_victim_descents_temp AS
SELECT DISTINCT 
    victim_descent
FROM la_crimes_temp
WHERE victim_descent IS NOT NULL;

-- All Crime Codes
CREATE TEMP TABLE crime_codes_temp AS
SELECT DISTINCT
    division_records_num,
    date_rptd::date AS date_rptd,
    crime_code_1, 
    crime_code_2, 
    crime_code_3, 
    crime_code_4
FROM la_crimes_temp;

--- Adding columns

--- Crime case count 
ALTER TABLE crime_codes_temp
ADD COLUMN crime_case_density INTEGER,
ADD COLUMN crime_tally INTEGER DEFAULT 1;


UPDATE crime_codes_temp
SET crime_case_density = 
  CASE 
    WHEN crime_code_1 IS NOT NULL AND crime_code_2 IS NULL AND crime_code_3 IS NULL AND crime_code_4 IS NULL THEN 1
    WHEN crime_code_1 IS NOT NULL AND crime_code_2 IS NOT NULL AND crime_code_3 IS NULL AND crime_code_4 IS NULL THEN 2
    WHEN crime_code_1 IS NOT NULL AND crime_code_2 IS NOT NULL AND crime_code_3 IS NOT NULL AND crime_code_4 IS NULL THEN 3
    WHEN crime_code_1 IS NOT NULL AND crime_code_2 IS NOT NULL AND crime_code_3 IS NOT NULL AND crime_code_4 IS NOT NULL THEN 4
    ELSE NULL
  END
  ;


--- Adding Columns
ALTER TABLE crime_weapons_temp
ADD COLUMN weapon_type  varchar(10);


UPDATE crime_weapons_temp
SET weapon_type =
CASE 
	WHEN weapon_used_code = '307'   THEN 'MISC'
	WHEN weapon_used_code = '514'   THEN 'Bludgen'
	WHEN weapon_used_code IN('513','502') THEN 'GUN'
    WHEN weapon_used_code LIKE '1%' THEN 'GUN'
	WHEN weapon_used_code LIKE '2%' THEN 'Sharp Obj.'
	WHEN weapon_used_code LIKE '3%' THEN 'Bludgen'
	WHEN weapon_used_code LIKE '4%' THEN 'Body'
	WHEN weapon_used_code LIKE '5%' THEN 'MISC'
    ELSE weapon_type 
END;


---- FOR DESCENDENT FIELD TO CREATE DESCENDENT REFERENCE TABLE---


/*	
Conversion found in Data Dictionary link
Code: A - Other Asian B - Black C - Chinese D - Cambodian
F - Filipino G - Guamanian H - Hispanic/Latin/Mexican 
I - American Indian/Alaskan Native J - Japanese K - Korean
L - Laotian O - Other P - Pacific Islander S - Samoan 
U - Hawaiian V - Vietnamese W - White X - Unknown Z - Asian Indian
*/


ALTER TABLE crime_victim_descents_temp
ADD COLUMN victim_descent_full VARCHAR(50);

UPDATE crime_victim_descents_temp
SET victim_descent_full =
CASE
    WHEN victim_descent = 'A' THEN 'Other Asian'
    WHEN victim_descent = 'B' THEN 'Black'
    WHEN victim_descent = 'C' THEN 'Chinese'
    WHEN victim_descent = 'D' THEN 'Cambodian'
    WHEN victim_descent = 'F' THEN 'Filipino'
    WHEN victim_descent = 'G' THEN 'Guamanian'
    WHEN victim_descent = 'H' THEN 'Hispanic/Latin/Mexican'
    WHEN victim_descent = 'I' THEN 'American Indian/Alaskan Native'
    WHEN victim_descent = 'J' THEN 'Japanese'
    WHEN victim_descent = 'K' THEN 'Korean'
    WHEN victim_descent = 'L' THEN 'Laotian'
    WHEN victim_descent = 'O' THEN 'Other'
    WHEN victim_descent = 'P' THEN 'Pacific Islander'
    WHEN victim_descent = 'S' THEN 'Samoan'
    WHEN victim_descent = 'U' THEN 'Hawaiian'
    WHEN victim_descent = 'V' THEN 'Vietnamese'
    WHEN victim_descent = 'W' THEN 'White'
    WHEN victim_descent = 'X' THEN 'Unknown'
    WHEN victim_descent = 'Z' THEN 'Asian Indian'
END;

  -- Accomodate data set date range by creating the date table from 2019 to the present.
CREATE TEMP TABLE date_table_temp AS
SELECT generate_series('2019-01-01'::date, CURRENT_DATE, '1 day')::date AS date;

ALTER TABLE date_table_temp
ADD COLUMN month varchar(10),
ADD COLUMN month_year varchar(25),
ADD COLUMN year varchar(4),
ADD COLUMN day varchar(10),
ADD COLUMN holiday_season varchar(15),
ADD COLUMN season varchar(6);

UPDATE date_table_temp
SET month = TRIM(TO_CHAR(date, 'Month')),
    year = TRIM(TO_CHAR(date, 'YYYY')),
    month_year = TRIM(TO_CHAR(date, 'Month YYYY')),
    day = TRIM(TO_CHAR(date, 'Day')),
    holiday_season = CASE
        WHEN month = 'December' THEN 'Christmas'
        WHEN month = 'November' THEN 'Thanksgiving'
        WHEN month = 'October' THEN 'Halloween'
        WHEN month = 'July' THEN '4th of July'
        WHEN month = 'February' THEN 'Valentines'
        WHEN month = 'March' THEN 'St. Patricks'
        ELSE NULL
    END,
    season = CASE
        WHEN month IN ('December', 'January', 'February') THEN 'Winter'
        WHEN month IN ('March', 'April', 'May') THEN 'Spring'
        WHEN month IN ('June', 'July', 'August') THEN 'Summer'
        WHEN month IN ('September', 'October', 'November') THEN 'Fall'
        ELSE NULL
    END;



---- Calculated Fields

-- Crime Counts Rolling

ALTER TABLE crime_codes_temp
ADD COLUMN rptd_month_rolling_crime_cnt integer,
ADD COLUMN rptd_month_yr_rolling_crime_cnt integer,
ADD COLUMN rptd_yr_rolling_crime_cnt integer,
ADD COLUMN rptd_dy_rolling_crime_cnt integer,
ADD COLUMN rptd_holiday_szn_rolling_crime_cnt integer,
ADD COLUMN rptd_szn_rolling_crime_cnt integer;

WITH rolling_crime_counts AS (
    SELECT
       c.division_records_num, 
        SUM(c.crime_tally) OVER(PARTITION BY month ORDER BY d.date ASC) AS rptd_month_rolling_crime_cnt,
        SUM(c.crime_tally) OVER(PARTITION BY month_year ORDER BY d.date ASC) AS rptd_month_yr_rolling_crime_cnt,
        SUM(c.crime_tally) OVER(PARTITION BY year ORDER BY d.date ASC) AS rptd_yr_rolling_crime_cnt,
        SUM(c.crime_tally) OVER(PARTITION BY day ORDER BY d.date ASC) AS rptd_dy_rolling_crime_cnt,
        SUM(c.crime_tally) OVER(PARTITION BY season ORDER BY d.date ASC) AS rptd_holiday_szn_rolling_crime_cnt,
        SUM(c.crime_tally) OVER(PARTITION BY holiday_season ORDER BY d.date ASC) AS rptd_szn_rolling_crime_cnt
    FROM crime_codes_temp c
	LEFT JOIN date_table_temp d
	ON c.date_rptd = d.date
)
UPDATE crime_codes_temp c
SET
    rptd_month_rolling_crime_cnt = r.rptd_month_rolling_crime_cnt,
    rptd_month_yr_rolling_crime_cnt = r.rptd_month_yr_rolling_crime_cnt,
    rptd_yr_rolling_crime_cnt = r.rptd_yr_rolling_crime_cnt,
    rptd_dy_rolling_crime_cnt = r.rptd_dy_rolling_crime_cnt,
    rptd_holiday_szn_rolling_crime_cnt = r.rptd_holiday_szn_rolling_crime_cnt,
    rptd_szn_rolling_crime_cnt = r.rptd_szn_rolling_crime_cnt
FROM rolling_crime_counts r
WHERE c.division_records_num = r.division_records_num; 

-- Crime Density

ALTER TABLE crime_codes_temp
ADD COLUMN rptd_month_rolling_cd integer,
ADD COLUMN rptd_month_yr_rolling_cd integer,
ADD COLUMN rptd_yr_rolling_cd integer,
ADD COLUMN rptd_dy_rolling_cd integer,
ADD COLUMN rptd_holiday_szn_rolling_cd integer,
ADD COLUMN rptd_szn_rolling_cd integer;

WITH crime_codez AS (
    SELECT
       c.division_records_num, 
        SUM(c.crime_case_density) OVER(PARTITION BY month ORDER BY d.date ASC) AS rptd_month_rolling_cd,
        SUM(c.crime_case_density) OVER(PARTITION BY month_year ORDER BY d.date ASC) AS rptd_month_yr_rolling_cd,
        SUM(c.crime_case_density) OVER(PARTITION BY year ORDER BY d.date ASC) AS rptd_yr_rolling_cd,
        SUM(c.crime_case_density) OVER(PARTITION BY day ORDER BY d.date ASC) AS rptd_dy_rolling_cd,
        SUM(c.crime_case_density) OVER(PARTITION BY season ORDER BY d.date ASC) AS rptd_holiday_szn_rolling_cd,
        SUM(c.crime_case_density) OVER(PARTITION BY holiday_season ORDER BY d.date ASC) AS rptd_szn_rolling_cd
      FROM crime_codes_temp c
	LEFT JOIN date_table_temp d
	ON c.date_rptd = d.date
)
UPDATE crime_codes_temp 
SET
    rptd_month_rolling_cd = r.rptd_month_rolling_cd,
    rptd_month_yr_rolling_cd = r.rptd_month_yr_rolling_cd,
    rptd_yr_rolling_cd = r.rptd_yr_rolling_cd,
    rptd_dy_rolling_cd = r.rptd_dy_rolling_cd,
    rptd_holiday_szn_rolling_cd = r.rptd_holiday_szn_rolling_cd,
    rptd_szn_rolling_cd = r.rptd_szn_rolling_cd
FROM crime_codez r
WHERE crime_codes_temp.division_records_num = r.division_records_num;



--- Creating loads of temp tables into actual tables


INSERT INTO crimes
SELECT * FROM crimes_temp
ON CONFLICT (crime_code) DO NOTHING;

-- Load data into the `crime_weapons` table
INSERT INTO crime_weapons
SELECT * FROM crime_weapons_temp
ON CONFLICT (weapon_used_code) DO NOTHING;

-- Load data into the `case_details` table
INSERT INTO case_details
SELECT * FROM case_details_temp
ON CONFLICT (case_status) DO NOTHING;

-- Load data into the `premises` table
INSERT INTO premises
SELECT * FROM premises_temp
ON CONFLICT (premises_code) DO NOTHING;

-- Load data into the `crime_geos` table
INSERT INTO crime_geos
SELECT * FROM crime_geos_temp
ON CONFLICT (geo_area_num) DO NOTHING;

-- Load data into the `crime_victim_descents` table
INSERT INTO crime_victim_descents
SELECT * FROM crime_victim_descents_temp
ON CONFLICT (victim_descent) DO NOTHING;

-- Load data into the `crime_codes` table
INSERT INTO crime_codes
SELECT * FROM crime_codes_temp
ON CONFLICT (division_records_num) DO NOTHING;

-- Load data into the `date_table` table
INSERT INTO date_table
SELECT dt.*
FROM date_table_temp dt
LEFT JOIN date_table d ON dt.date = d.date
WHERE d.date IS NULL;

-- Load data into the `la_crimes` table
INSERT INTO la_crimes
SELECT * FROM la_crimes_temp
ON CONFLICT (division_records_num) DO NOTHING;




