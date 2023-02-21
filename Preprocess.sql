-- Active: 1675711761378@@127.0.0.1@3306@starbucks_customer_data

CREATE SCHEMA starbucks_customer_data;

-- portfolio_proc: after data cleanning

CREATE TABLE PORTFOLIO_PROC AS
SELECT
    reward,
    difficulty,
    duration,
    TRIM(UPPER(offer_type)) AS offer_type,
    id AS offer_id,
    CASE
        WHEN JSON_CONTAINS(channels, '["web"]') THEN 1
        ELSE 0
    END AS channel_web,
    CASE
        WHEN JSON_CONTAINS(channels, '["email"]') THEN 1
        ELSE 0
    END AS channel_email,
    CASE
        WHEN JSON_CONTAINS(channels, '["social"]') THEN 1
        ELSE 0
    END AS channel_social,
    CASE
        WHEN JSON_CONTAINS(channels, '["mobile"]') THEN 1
        ELSE 0
    END AS channel_mobile
FROM portfolio;

-- profile: convert DATETIME to DATE

ALTER TABLE profile MODIFY COLUMN became_member_on DATE;

-- profile_proc: after data cleaning

CREATE TABLE PROFILE_PROC AS
SELECT
    MyUnknownColumn,
    CASE
        WHEN GENDER IN ('F', 'M', 'O') THEN TRIM(UPPER(GENDER))
        ELSE 'U'
    END AS gender,
    age,
    id AS custommer_id,
    became_member_on,
    YEAR(became_member_on) AS became_member_year,
    CAST(NULLIF(income, '') AS FLOAT) AS income_null
FROM profile;

-- transcript_proc: after data cleaning

CREATE TABLE TRANSCRIPT_PROC AS
SELECT
    `Unnamed: 0` AS MyUnknownColumn,
    person,
    event,
    COALESCE(
        REPLACE (value -> '$."offer id"', '"', ''),
        REPLACE (value -> '$."offer_id"', '"', '')
    ) AS offer_id,
    value -> '$."amount"' AS amount,
    value -> '$."reward"' AS reward,
    time
FROM transcript;

-- convert time (hours since become member) of transaction to DATETIME

CREATE VIEW TRANSCRIPT_PROC_VIEW AS 
	SELECT
	    trans.*,
	    DATE_ADD(
	        prof.became_member_on,
	        INTERVAL trans.time HOUR
	    ) AS time_date
	FROM transcript_proc AS trans
	    LEFT JOIN profile_proc AS prof ON trans.person = prof.custommer_id
; 