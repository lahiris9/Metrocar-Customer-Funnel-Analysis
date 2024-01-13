WITH download AS (
SELECT app_download_key, platform, age_range, DATE(download_ts) AS download_date, 0 AS funnel_step, 0 AS ride_count
FROM metrocar
GROUP BY app_download_key, platform, age_range, DATE(download_ts)
),
 sign_up AS (
SELECT DISTINCT met.app_download_key, met.platform AS platform, met.age_range AS age_range, DATE(met.download_ts) AS download_date, 1 AS funnel_step, 0 AS ride_count
FROM download d
INNER JOIN metrocar met ON d.app_download_key = met.app_download_key
WHERE user_id IS NOT NULL
GROUP BY DISTINCT met.app_download_key, met.platform, met.age_range, DATE(met.download_ts)
),
 ride_req AS (
SELECT DISTINCT met.app_download_key, met.platform AS platform, met.age_range AS age_range, DATE(met.download_ts) AS download_date, 2 AS funnel_step, COUNT(met.ride_id) AS ride_count
FROM sign_up s
INNER JOIN metrocar met ON s.app_download_key = met.app_download_key
WHERE ride_id IS NOT NULL
GROUP BY DISTINCT met.app_download_key, met.platform, met.age_range, DATE(met.download_ts), funnel_step
),
 ride_accept AS (
SELECT DISTINCT met.app_download_key, met.platform AS platform, met.age_range AS age_range, DATE(met.download_ts) AS download_date, 3 AS funnel_step, COUNT(met.accept_ts) AS ride_count
FROM sign_up s
INNER JOIN metrocar met ON s.app_download_key = met.app_download_key
WHERE ride_id IS NOT NULL AND met.accept_ts IS NOT NULL
GROUP BY DISTINCT met.app_download_key, met.platform, met.age_range, DATE(met.download_ts), funnel_step
),
ride_complete AS (
SELECT DISTINCT met.app_download_key, met.platform AS platform, met.age_range AS age_range, DATE(met.download_ts) AS download_date, 4 AS funnel_step, COUNT(met.dropoff_location) AS ride_count
FROM sign_up s
INNER JOIN metrocar met ON s.app_download_key = met.app_download_key
WHERE transaction_id IS NOT NULL
GROUP BY DISTINCT met.app_download_key, met.platform, met.age_range, DATE(met.download_ts), funnel_step
),
 transac AS (
SELECT DISTINCT met.app_download_key, met.platform AS platform, met.age_range AS age_range, DATE(met.download_ts) AS download_date, 5 AS funnel_step, COUNT(met.transaction_id) AS ride_count
FROM ride_req r
INNER JOIN metrocar met ON r.app_download_key = met.app_download_key
WHERE transaction_id IS NOT NULL AND charge_status = 'Approved'
GROUP BY DISTINCT met.app_download_key, met.platform, met.age_range, DATE(met.download_ts), funnel_step
),
 review AS (
SELECT DISTINCT met.app_download_key, met.platform AS platform, met.age_range AS age_range, DATE(met.download_ts) AS download_date, 6 AS funnel_step, COUNT(met.review_id) AS ride_count
FROM transac tra
INNER JOIN metrocar met ON tra.app_download_key = met.app_download_key
WHERE review_id IS NOT NULL
GROUP BY DISTINCT met.app_download_key, met.platform, met.age_range, DATE(met.download_ts), funnel_step
),

Steps AS (
SELECT funnel_step, 'Download' AS Funnel_Name, platform, age_range, download_date, COUNT(*) AS user_count, SUM(ride_count) AS rides FROM download GROUP BY funnel_step, Funnel_Name, platform, age_range, download_date
UNION
SELECT funnel_step, 'Sign Up' AS Funnel_Name, platform, age_range, download_date, COUNT(*) AS user_count, SUM(ride_count) AS rides FROM sign_up GROUP BY funnel_step, Funnel_Name, platform, age_range, download_date
UNION
SELECT funnel_step, 'Ride Request' AS Funnel_Name, platform, age_range, download_date, COUNT(*) AS user_count, SUM(ride_count) AS rides FROM ride_req GROUP BY funnel_step, Funnel_Name, platform, age_range, download_date
UNION
SELECT funnel_step, 'Ride Acccepted' AS Funnel_Name, platform, age_range, download_date, COUNT(*) AS user_count, SUM(ride_count) AS rides FROM ride_accept GROUP BY funnel_step, Funnel_Name, platform, age_range, download_date
UNION
SELECT funnel_step, 'Ride Completed' AS Funnel_Name, platform, age_range, download_date, COUNT(*) AS user_count, SUM(ride_count) AS rides FROM ride_complete GROUP BY funnel_step, Funnel_Name, platform, age_range, download_date   
UNION
SELECT funnel_step, 'Payment' AS Funnel_Name, platform, age_range, download_date, COUNT(*) AS user_count, SUM(ride_count) AS rides FROM transac GROUP BY funnel_step, Funnel_Name, platform, age_range, download_date
UNION
SELECT funnel_step, 'Review' AS Funnel_Name, platform, age_range, download_date, COUNT(*) AS user_count, SUM(ride_count) AS rides FROM review GROUP BY funnel_step, Funnel_Name, platform, age_range, download_date
ORDER BY funnel_step
)

SELECT funnel_step,
               Funnel_Name,
               platform,
               age_range,
               download_date,
               user_count,
               rides
FROM steps