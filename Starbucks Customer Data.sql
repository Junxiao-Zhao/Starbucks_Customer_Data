CREATE SCHEMA starbucks_customer_data;

SELECT
    *,
    UPPER(offer_type) AS offer_type_new
FROM portfolio
WHERE
    JSON_CONTAINS(channels, '["email", "web"]');

CREATE VIEW PORTFOLIO_PROC AS 
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
	FROM PORTFOLIO
; 

SELECT * FROM portfolio_proc;

DROP TABLE portfolio;

ALTER TABLE profile MODIFY COLUMN became_member_on DATE;

DROP TABLE profile;