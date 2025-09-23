
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
