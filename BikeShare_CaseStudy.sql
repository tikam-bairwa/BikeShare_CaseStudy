WITH

      -- combining monthly datasets into a single yearly dataset

yearly_data AS
(
  SELECT *
  FROM `bike-share-case-study-355021.gda_casestudy.tripdata_202106`
  UNION ALL 
  SELECT *
  FROM `bike-share-case-study-355021.gda_casestudy.tripdata_202107`
  UNION ALL 
  SELECT *
  FROM `bike-share-case-study-355021.gda_casestudy.tripdata_202108`
  UNION ALL 
  SELECT *
  FROM `bike-share-case-study-355021.gda_casestudy.tripdata_202109`
  UNION ALL 
  SELECT *
  FROM `bike-share-case-study-355021.gda_casestudy.tripdata_202110`
  UNION ALL 
  SELECT *
  FROM `bike-share-case-study-355021.gda_casestudy.tripdata_202111`
  UNION ALL 
  SELECT *
  FROM `bike-share-case-study-355021.gda_casestudy.tripdata_202112`
  UNION ALL 
  SELECT *
  FROM `bike-share-case-study-355021.gda_casestudy.tripdata_202201`
  UNION ALL 
  SELECT *
  FROM `bike-share-case-study-355021.gda_casestudy.tripdata_202202`
  UNION ALL 
  SELECT *
  FROM `bike-share-case-study-355021.gda_casestudy.tripdata_202203`
  UNION ALL 
  SELECT *
  FROM `bike-share-case-study-355021.gda_casestudy.tripdata_202204`
  UNION ALL 
  SELECT *
  FROM `bike-share-case-study-355021.gda_casestudy.tripdata_202205`
  ),
   
     -- remove empty cells/rows from the dataset

null_cleaned_data AS
(
	SELECT *
	FROM yearly_data
  WHERE start_station_name NOT LIKE "%NULL%"
        AND end_station_name NOT LIKE "%NULL%"
        AND start_lat IS NOT NULL
        AND start_lng IS NOT NULL
        AND end_lat IS NOT NULL
        AND end_lng IS NOT NULL
),

     -- ride_id validation --> remove ride_id with more than 16 characters

clean_ride_id AS 
(
  SELECT *
  FROM null_cleaned_data
  WHERE LENGTH(ride_id)= 16
),

      ---- TRIM station names to ensure no extra space, and to replace (Temp) in station name

clean_station_name AS 
 (
   SELECT ride_id, start_station_id, end_station_id,
       TRIM ( REPLACE (start_station_name,	'(TEMP)','')) AS clean_start_station_name,
       TRIM ( REPLACE (end_station_name,	'(TEMP)','')) AS clean_end_station_name
   FROM clean_ride_id
 ),

      /* Calculate ride_length in minutes and assigning respective day of the week and  */

aggregated_data AS 
(
  SELECT *,
    DATE_DIFF(ended_at, started_at, MINUTE ) as ride_length,
      CASE
      WHEN day_of_week= 1  THEN 'Sunday'
      WHEN day_of_week = 2 THEN 'Monday'
      WHEN day_of_week = 3 THEN 'Tuesday'
      WHEN day_of_week = 4 THEN 'Wednesday'
      WHEN day_of_week = 5 THEN 'Thursday'
      WHEN day_of_week = 6 THEN 'Friday'
      ELSE 'Saturday'
      END AS weekday
  FROM clean_ride_id
),

     --remove rides with ride_length less than one minute

clean_aggregated_data AS 
(
  SELECT *
  FROM aggregated_data
  WHERE ride_length>=1
),

    -- JOIN cleaned station columns to the main dataset ON ride_id  

final_table AS 
(
  SELECT cad.ride_id, cad.rideable_type, cad.started_at, cad.ended_at, CAST(cad.started_at AS DATE) AS ride_day,
         csn.clean_start_station_name AS start_station,  csn.clean_end_station_name AS end_station,
         cad.ride_length, cad.weekday, cad.member_casual AS member_type,
         start_lat, start_lng, end_lat, end_lng
  FROM clean_aggregated_data AS cad
  JOIN clean_station_name AS csn
   ON cad.ride_id = csn.ride_id
),

                                 -- DATA ANALYSIS AND VISUALIZATION --

            -- Find out total numbers of member or casual or all riders DEPARTING from respective stations 

casual_depart_station AS 
(
  SELECT start_station, COUNT(member_type) AS casual
  FROM final_table
  WHERE member_type = "casual"
  GROUP BY start_station
  ORDER BY casual
),

member_depart_station AS
(
  SELECT start_station, COUNT(member_type) AS member
  FROM final_table
  WHERE member_type = "member"
  GROUP BY start_station
  ORDER BY member DESC
),

depart_station AS
(
	SELECT cds.start_station, cds.casual, mds.member
	FROM casual_depart_station AS cds
	  FULL JOIN member_depart_station AS mds
	  ON cds.start_station = mds.start_station
),
                  --GROUP departing station name with distinct latitude and longitude

depart_station_latlng AS
(
   SELECT start_station, ROUND(AVG(start_lat),4) AS dep_lat,  ROUND(AVG(start_lng),4) AS dep_lng
   FROM final_table
   GROUP BY start_station
),

              --Join location coordinate data with ridership count and 
              -- Export to excel & import to tableau for geo-visualisation

depart_location_viz AS
(
  SELECT dsl.start_station, dsl.dep_lat, dsl.dep_lng, ds.casual, ds.member
  FROM depart_station_latlng  dsl
  JOIN depart_station  ds
  ON dsl.start_station = ds.start_station
),

          -- Find out total numbers of member or casual or all riders ARRIVING for respective stations 

 casual_arrive_station AS 
(
  SELECT end_station, COUNT(member_type) AS casual
  FROM final_table
  WHERE member_type = "casual"
  GROUP BY end_station
  ORDER BY casual
),

member_arrive_station AS
(
  SELECT end_station, COUNT(member_type) AS member
  FROM final_table
  WHERE member_type = "member"
  GROUP BY end_station
  ORDER BY member DESC
),

arrive_station AS
(
	SELECT cas.end_station, cas.casual, mas.member
	FROM casual_arrive_station AS cas
	  FULL JOIN member_arrive_station AS mas
	  ON cas.end_station = mas.end_station
),
                  --GROUP arriving station name with distinct latitude and longitude

arrive_station_latlng AS
(
   SELECT end_station, ROUND(AVG(end_lat),4) AS arv_lat,  ROUND(AVG(end_lng),4) AS arv_lng
   FROM final_table
   GROUP BY end_station
),

              --Join location coordinate data with ridership count and 
              -- Export to excel & import to tableau for geo-visualisation

arrive_location_viz AS
(
  SELECT asl.end_station, asl.arv_lat, asl.arv_lng, ars.casual, ars.member
  FROM arrive_station_latlng  asl
  JOIN arrive_station ars
  ON asl.end_station = ars.end_station
),

        -- Find out total numbers of trips taken by member or casual or all riders by day of year(date)

datewise_casual_trips AS
(
	SELECT count(member_type) AS casual_rides, ride_day
	FROM final_table
	WHERE member_type = 'casual'
	GROUP BY ride_day
),

datewise_member_trips AS
(
	SELECT count(member_type) AS member_rides, ride_day
	FROM final_table
	WHERE member_type = 'member'
	GROUP BY ride_day
),

datewise_trips AS
(
	SELECT dmt.ride_day, dct.casual_rides, dmt.member_rides,
  (dct.casual_rides+dmt.member_rides) AS total_rides
	FROM datewise_casual_trips dct
	JOIN datewise_member_trips dmt
	 ON dct.ride_day = dmt.ride_day
   ORDER BY ride_day
),

      -- Find out total numbers of trips taken by member or casual or all riders by the day of the week

daywise_casual_trips AS
(
	SELECT count(member_type) AS casual_rides, weekday
	FROM final_table
	WHERE member_type = 'casual'
	GROUP BY weekday
),

daywise_member_trips AS
(
	SELECT count(member_type) AS member_rides, weekday
	FROM final_table
	WHERE member_type = 'member'
	GROUP BY weekday
),

daywise_trips AS
(
	SELECT dwmt.weekday, dwct.casual_rides, dwmt.member_rides,
  (dwct.casual_rides + dwmt.member_rides) AS total_rides
	FROM daywise_casual_trips dwct
	JOIN daywise_member_trips dwmt
	 ON dwct.weekday = dwmt.weekday
),

          --To find the AVERAGE ride time for Casual & Member Riders 

data_totalmin_casual AS
(
	SELECT AVG(ride_length) AS AVG_ride_casual
	FROM final_table
	WHERE member_type = 'casual'
),


data_totalmin_member AS
(
	SELECT AVG(ride_length) AS AVG_ride_member
	FROM final_table
	WHERE member_type = 'member'
),

       -- To find the Overall Rider Count for Casual & Member riders

total_casual_rides AS
(
	SELECT count(member_type) AS ridership_casual
	FROM final_table
	WHERE member_type = 'casual'
),


total_member_rides AS
(
	SELECT count(member_type) AS ridership_member
	FROM final_table
	WHERE member_type = 'member'
)

SELECT *
FROM total_casual_rides       
