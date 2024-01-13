WITH download AS (
SELECT app_download_key, 0 AS funnel_step, 0 AS ride_count
FROM metrocar
GROUP BY app_download_key
),
 sign_up AS (
SELECT DISTINCT met.app_download_key, 1 AS funnel_step, 0 AS ride_count
FROM download d
INNER JOIN metrocar met ON d.app_download_key = met.app_download_key
WHERE user_id IS NOT NULL
),
 ride_req AS (
SELECT DISTINCT met.app_download_key, 2 AS funnel_step, COUNT(met.ride_id) AS ride_count
FROM sign_up s
INNER JOIN metrocar met ON s.app_download_key = met.app_download_key
WHERE ride_id IS NOT NULL
GROUP BY DISTINCT met.app_download_key, funnel_step
),
 ride_accept AS (
SELECT DISTINCT met.app_download_key, 3 AS funnel_step, COUNT(met.accept_ts) AS ride_count
FROM ride_req rr
INNER JOIN metrocar met ON rr.app_download_key = met.app_download_key
WHERE ride_id IS NOT NULL AND met.accept_ts IS NOT NULL
GROUP BY DISTINCT met.app_download_key, funnel_step
),
ride_complete AS (
SELECT DISTINCT met.app_download_key, 4 AS funnel_step, COUNT(met.dropoff_location) AS ride_count
FROM ride_accept ra
INNER JOIN metrocar met ON ra.app_download_key = met.app_download_key
WHERE transaction_id IS NOT NULL
GROUP BY DISTINCT met.app_download_key, funnel_step
),
 transac AS (
SELECT DISTINCT met.app_download_key, 5 AS funnel_step, COUNT(met.transaction_id) AS ride_count
FROM ride_complete rc
INNER JOIN metrocar met ON rc.app_download_key = met.app_download_key
WHERE transaction_id IS NOT NULL AND charge_status = 'Approved'
GROUP BY DISTINCT met.app_download_key, funnel_step
),
 review AS (
SELECT DISTINCT met.app_download_key, 6 AS funnel_step, COUNT(met.review_id) AS ride_count
FROM transac tra
INNER JOIN metrocar met ON tra.app_download_key = met.app_download_key
WHERE review_id IS NOT NULL
GROUP BY DISTINCT met.app_download_key, funnel_step
),

Steps AS (SELECT funnel_step, 'Download' AS Funnel_Name, COUNT(*) AS user_count, SUM(ride_count) AS rides FROM download GROUP BY funnel_step, Funnel_Name 
UNION
SELECT funnel_step, 'Sign Up' AS Funnel_Name, COUNT(*) AS user_count, SUM(ride_count) AS rides FROM sign_up GROUP BY funnel_step, Funnel_Name
UNION
SELECT funnel_step, 'Ride Request' AS Funnel_Name, COUNT(*) AS user_count, SUM(ride_count) AS rides FROM ride_req GROUP BY funnel_step, Funnel_Name
UNION
SELECT funnel_step, 'Ride Acccepted' AS Funnel_Name, COUNT(*) AS user_count, SUM(ride_count) AS rides FROM ride_accept GROUP BY funnel_step, Funnel_Name
UNION
SELECT funnel_step, 'Ride Completed' AS Funnel_Name, COUNT(*) AS user_count, SUM(ride_count) AS rides FROM ride_complete GROUP BY funnel_step, Funnel_Name     
UNION
SELECT funnel_step, 'Payment' AS Funnel_Name, COUNT(*) AS user_count, SUM(ride_count) AS rides FROM transac GROUP BY funnel_step, Funnel_Name
UNION
SELECT funnel_step, 'Review' AS Funnel_Name, COUNT(*) AS user_count, SUM(ride_count) AS rides FROM review GROUP BY funnel_step, Funnel_Name
ORDER BY funnel_step)

SELECT funnel_step,
	Funnel_Name, 
	user_count,
    	lag(user_count, 1) over (),
       	round((1.0 - user_count::numeric/lag(user_count, 1) over ()),3) as drop_off,
       	rides
from steps









