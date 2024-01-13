SELECT platform, EXTRACT(HOUR FROM request_ts) AS hourly_ride_request, COUNT(ride_id) AS ride_count
FROM metrocar
WHERE request_ts IS NOT NULL
GROUP BY platform, EXTRACT(HOUR FROM request_ts)
ORDER BY ride_count DESC
