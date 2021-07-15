-- 
-- Query demand of dockless vehicles for the next hour
--
-- Define function to represent the SageMaker model and endpoint for the prediction


-- In Athena you can access SageMaker endpoints for ML inference as external function with the keyword SAGEMAKER
-- and the name of the endpoint. THis is the definition of the inference function predict_demand() that returns
-- the predicted number of trips for the next hour based on the counts of the past four hours, the neighborhood,
-- and day of the week and hour of day. The endpoint with the pre-trained ML model was launched during installation.
-- You can find the Python code for training the ML model in the Notebook
-- “Demand Prediction for Dockless Vehicles using Amazon SageMaker and Amazon Athena”.
USING EXTERNAL FUNCTION predict_demand( location_id BIGINT, hr BIGINT , dow BIGINT, 
                                        n_pickup_1 BIGINT, n_pickup_2 BIGINT, n_pickup_3 BIGINT, n_pickup_4 BIGINT,
                                        n_dropoff_1 BIGINT, n_dropoff_2 BIGINT, n_dropoff_3 BIGINT, n_dropoff_4 BIGINT
                                      )
                        RETURNS DOUBLE SAGEMAKER '${SageMakerEndpoint}'

-- First “trips_raw” uses the Lambda function to pull data from the DynamoDB table. It only returns
-- records that are less than five hours older than the target time.
-- Note: With a live data feed we would just use NOW() to get the current time. This example
--       uses archived data and a fixed timestamp. You need to change the TIMESTAMP expression
--       in multiple places for different predictions.
-- We use epoch time, aka. UNIX time, because DynamoDB does not support a native time-stamp format.
-- This is faster than using strings and convert them to timestamps.
WITH trips_raw AS (
  SELECT *
      , from_unixtime(start_epoch) AS t_start
      , from_unixtime(end_epoch) AS t_end
  FROM "lambda:${AthenaDynamoDBConnectorFunction}"."${Database}"."${DynamoDBTable}" dls
  WHERE start_epoch BETWEEN to_unixtime(TIMESTAMP '2019-09-07 15:00' - interval '5' hour) AND to_unixtime(TIMESTAMP '2019-09-07 15:00')
    OR end_epoch BETWEEN to_unixtime(TIMESTAMP '2019-09-07 15:00' - interval '5' hour) AND to_unixtime(TIMESTAMP '2019-09-07 15:00')
),

-- The next CTE “trips” prepares the selected data from “trips_raw” for aggregation over
-- time and geography by associating 1-hour bins and neighborhoods to the trip records.
-- The trip has a start time and location and end time and location. The sub-query generates
-- the respective fields “t_hour_start”, “start_nbid” and “t_hour_end”, “end_nbid”.
-- We use geospatial functions (https://docs.aws.amazon.com/athena/latest/ug/geospatial-functions-list-v2.html)
-- in Athena to map the longitude-latitude coordinates of the start and end locations to their respective
-- neighborhoods. The geospatial function ST_WITHIN(), that determines if the given points lays
-- within the boundaries of the polygon, is used to join the neighborhood table the trip data.
-- It has to be joined twice, for the start and the end location. 
trips AS (
  SELECT tr.*
      , nb1.nh_code AS start_nbid
      , nb2.nh_code AS end_nbid
      , floor( ( tr.start_epoch - to_unixtime(TIMESTAMP '2019-09-07 15:00' - interval '5' hour) )/3600 ) AS t_hour_start
      , floor( ( tr.end_epoch - to_unixtime(TIMESTAMP '2019-09-07 15:00' - interval '5' hour) )/3600 ) AS t_hour_end
  FROM trips_raw tr
  JOIN "AwsDataCatalog"."${Database}"."loisville_ky_neighborhoods" nb1
      ON ST_Within(ST_POINT(CAST(tr.startlongitude AS DOUBLE), CAST(tr.startlatitude AS DOUBLE)), ST_GeometryFromText(nb1.shape))  
  JOIN "AwsDataCatalog"."${Database}"."loisville_ky_neighborhoods" nb2
      ON ST_Within(ST_POINT(CAST(tr.endlongitude AS DOUBLE), CAST(tr.endlatitude AS DOUBLE)), ST_GeometryFromText(nb2.shape))
),

-- The CTEs “start_count” and “end_count” perform the aggregation over hours and neighborhoods.
-- Both sub-queries operate on the same way. Technically, we aggregate over neighborhood, “*_nbid”,
-- and hour “t_hour_*”. However, the query uses GROUP BY only on the neighborhood.
-- The construct SUM(CASE WHEN ...) is used to mimic result of a pivot table in order to get the aggregates
-- for different grouping in separate columns.
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

-- The final CTE “predictions” uses the counts per preceding hours and neighborhoods
-- to build the feature vector for the ML model. In Athena you can access SageMaker
-- endpoints for ML inference as external function with the keyword SAGEMAKER and the
-- name of the endpoint. The top of the SQL statement shows the definition of
-- the inference function predict_demand() that returns the predicted number of trips
-- for the next hour based on the counts of the past four hours, the neighborhood,
-- and day of the week and hour of day. The endpoint with the pre-trained ML model was
-- launched during installation. You can find the Python code for training the ML model
-- in the Notebook “Demand Prediction for Dockless Vehicles using Amazon SageMaker and Amazon Athena”.
predictions AS (
  SELECT sc.nbid
      , predict_demand(
          CAST(sc.nbid AS BIGINT),
          hour(TIMESTAMP '2019-09-07 15:00'), day_of_week(TIMESTAMP '2019-09-07 15:00'),
          sc.n1_start, sc.n2_start, sc.n3_start, sc.n4_start,
          ec.n1_end, ec.n2_end, ec.n3_end, ec.n4_end
        ) AS n_demand        
  FROM start_count sc
  JOIN end_count ec
    ON sc.nbid=ec.nbid
)
-- Predicted values with the neighborhoods' meta data
SELECT nh.nh_code AS nbid, nh.nh_name AS neighborhood, nh.cog_longitude AS longitude, nh.cog_latitude AS latitude
 , ST_POINT(nh.cog_longitude, nh.cog_latitude) AS geo_location
 , COALESCE( round(predictions.n_demand), 0 ) AS demand
FROM "AwsDataCatalog"."${Database}"."loisville_ky_neighborhoods" nh
LEFT JOIN predictions
  ON nh.nh_code=predictions.nbid
