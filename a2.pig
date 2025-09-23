
-- Task 1: Clean the Trips
trips_input = LOAD '/Input/Trips.txt'
    USING PigStorage('\t')
    AS (trip_id:int, taxi_id:int, company_id:int,
        lat:double, lon:double,
        dist:double, fare:double);

taxis_data = LOAD '/Input/Taxis.txt'
    USING PigStorage('\t')
    AS (taxi_id:int, plate:chararray,
        year:int, rating:double);

company_data = LOAD '/Input/Companies.txt'
    USING PigStorage('\t')
    AS (company_id:int, name:chararray);

trips_clean = FILTER trips_input BY
    (trip_id IS NOT NULL AND
     taxi_id IS NOT NULL AND
     company_id IS NOT NULL AND
     lat IS NOT NULL AND
     lon IS NOT NULL AND
     dist IS NOT NULL AND
     fare IS NOT NULL) AND
    (dist > 0 AND dist <= 20 AND fare >= 5);

STORE trips_clean INTO '/Output/clean_trips' USING PigStorage('\t');


-- Task 2: Join & Enrich
tj = JOIN trips_clean BY taxi_id, taxis_data BY taxi_id;

full_join = JOIN tj BY trips_clean::company_id, company_data BY company_id;

trips_extended = FOREACH full_join GENERATE
    trips_clean::taxi_id,
    trips_clean::company_id,
    company_data::name,
    taxis_data::rating,
    trips_clean::dist,
    trips_clean::fare,
    trips_clean::lat,
    trips_clean::lon;

STORE trips_extended INTO '/Output/enriched_trips' USING PigStorage('\t');

-- Task 3: Aggregation
com_group = GROUP trips_extended BY (company_id, name);

com_data = FOREACH com_group {
    trip_count = COUNT(trips_extended);
    total_dist = SUM(trips_extended.dist);
    avg_dist   = AVG(trips_extended.dist);
    avg_fare   = AVG(trips_extended.fare);

    total_distance_km = (double)ROUND(total_dist * 100.0) / 100.0;
    avg_distance_km   = (double)ROUND(avg_dist * 100.0) / 100.0;
    avg_fare_2dp      = (double)ROUND(avg_fare * 100.0) / 100.0;

    GENERATE
        (int)group.company_id      AS company_id,
        (chararray)group.name      AS company_name,
        (long)trip_count           AS trip_count,
        (double)total_distance_km  AS total_distance_km,
        (double)avg_distance_km    AS avg_distance_km,
        (double)avg_fare_2dp       AS avg_fare;
};

company_stats = ORDER com_data BY trip_count ASC, company_name ASC;

STORE company_stats INTO '/Output/company_stats' USING PigStorage('\t');