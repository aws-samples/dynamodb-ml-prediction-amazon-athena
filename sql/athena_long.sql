-- 
-- Query demand of dockless vehicles for the next hour
--
-- Define function to represent the SageMaker model and endpoint for the prediction
USING EXTERNAL FUNCTION predict_demand( location_id BIGINT, hr BIGINT , dow BIGINT, 
                                        n_pickup_1 BIGINT, n_pickup_2 BIGINT, n_pickup_3 BIGINT, n_pickup_4 BIGINT,
                                        n_dropoff_1 BIGINT, n_dropoff_2 BIGINT, n_dropoff_3 BIGINT, n_dropoff_4 BIGINT
                                      )
                        RETURNS DOUBLE SAGEMAKER '${SageMakerEndpoint}'
-- define current time
WITH current_ts AS (
  -- --------------------------------
  -- Set the "current time" here ! --
  -- --------------------------------
  SELECT TIMESTAMP '2019-09-07 15:00' AS ts
  -- if this were a live data feed we would just use the current time
  -- SELECT NOW() AS ts
),
-- define the time window of 5 hours for which we need to query data
-- we use the epoch time (aka UNIX time) format because operations on floating point number are faster than timestamps
now AS (
  SELECT ts
      , to_unixtime(ts) AS t_epoch
      , to_unixtime(ts - interval '5' hour) AS t_epoch_5
  FROM current_ts
),
-- query raw trip data from DynamoDB, we only need the past five hours (i.e. 5 hour time window ending with the time we set as current time)
trips_raw AS (
  SELECT *
      , from_unixtime(start_epoch) AS t_start
      , from_unixtime(end_epoch) AS t_end
  FROM "lambda:${AthenaDynamoDBConnectorFunction}".default."${DynamoDBTable}" dls
  JOIN now ON TRUE
  WHERE start_epoch BETWEEN t_epoch_5 AND t_epoch
     OR end_epoch BETWEEN t_epoch_5 AND t_epoch
),
-- prepare individual trip records
--    1. assign 1-hour bins to start and end times, we use these to aggregate over hours
--    2. determine the respective neighborhoods of start and end geo-location 
trips AS (
  SELECT tr.*
      , nb1.nh_code AS start_nbid
      , nb2.nh_code AS end_nbid
      , floor( ( tr.start_epoch - now.t_epoch_5 )/3600 ) AS t_hour_start
      , floor( ( tr.end_epoch - now.t_epoch_5 )/3600 ) AS t_hour_end
  FROM trips_raw tr
  JOIN "AwsDataCatalog".default."loisville_ky_neighborhoods" nb1
      ON ST_Within(ST_POINT(CAST(tr.startlongitude AS DOUBLE), CAST(tr.startlatitude AS DOUBLE)), ST_GeometryFromText(nb1.shape))  
  JOIN "AwsDataCatalog".default."loisville_ky_neighborhoods" nb2
      ON ST_Within(ST_POINT(CAST(tr.endlongitude AS DOUBLE), CAST(tr.endlatitude AS DOUBLE)), ST_GeometryFromText(nb2.shape))
  JOIN now ON TRUE
),
-- aggregating trips over start time and start neighborhood
start_count AS (
  SELECT start_nbid AS nbid, COUNT(start_nbid) AS n_total_start
      , SUM(CASE WHEN t_hour_start=1 THEN 1 ELSE 0 END) AS n1_start
      , SUM(CASE WHEN t_hour_start=2 THEN 1 ELSE 0 END) AS n2_start
      , SUM(CASE WHEN t_hour_start=3 THEN 1 ELSE 0 END) AS n3_start
      , SUM(CASE WHEN t_hour_start=4 THEN 1 ELSE 0 END) AS n4_start
  FROM trips
  WHERE start_nbid BETWEEN 1 AND 98
  GROUP BY start_nbid
),
-- aggregating trips over end time and end neighborhood
end_count AS (
  SELECT end_nbid AS nbid, COUNT(end_nbid) AS n_total_end
      , SUM(CASE WHEN t_hour_end=1 THEN 1 ELSE 0 END) AS n1_end
      , SUM(CASE WHEN t_hour_end=2 THEN 1 ELSE 0 END) AS n2_end
      , SUM(CASE WHEN t_hour_end=3 THEN 1 ELSE 0 END) AS n3_end
      , SUM(CASE WHEN t_hour_end=4 THEN 1 ELSE 0 END) AS n4_end
  FROM trips
  WHERE end_nbid BETWEEN 1 AND 98
  GROUP BY end_nbid
),
-- call the predictive model to get the demand forecast for the next hour
predictions AS (
  SELECT sc.nbid
      , predict_demand(
          CAST(sc.nbid AS BIGINT),
          hour(now.ts), day_of_week(now.ts),
          sc.n1_start, sc.n2_start, sc.n3_start, sc.n4_start,
          ec.n1_end, ec.n2_end, ec.n3_end, ec.n4_end
        ) AS n_demand        
  FROM start_count sc
  JOIN end_count ec
    ON sc.nbid=ec.nbid
  JOIN now ON TRUE
)
-- finally join the predicted values with the neighborhoods' meta data
SELECT nh.nh_code AS nbid, nh.nh_name AS neighborhood, nh.cog_longitude AS longitude, nh.cog_latitude AS latitude
 , ST_POINT(nh.cog_longitude, nh.cog_latitude) AS geo_location
 , COALESCE( round(predictions.n_demand), 0 ) AS demand
FROM "AwsDataCatalog".default."loisville_ky_neighborhoods" nh
LEFT JOIN predictions
  ON nh.nh_code=predictions.nbid
