## Preparation

# conda enviroment with postgres 15 installed
```conda create -n pg15_env postgresql=15 -c conda-forge```
```conda activate pg15_env```

# Initialize the database cluster (first time only):
```initdb -D ~/pg15data```
#  Start the PostgreSQL server:
```pg_ctl -D ~/pg15data start```

# create database:
```createdb files_challenge2```

# execute script to insert data into tables
```psql -d files_challenge2 -f challenge_test_data.sql```

# connect to the db
```psql -d files_challenge2```


# Check content of tables
```
SELECT * FROM file_processing_events LIMIT 1;
SELECT * FROM file_configurations LIMIT 1;
SELECT * FROM file_sources LIMIT 1;
SELECT * FROM priority_thresholds LIMIT 1;
```

## Excercise 1 
```
SELECT fpe.event_id,
fpe.file_name,
fpe.event_time,
COALESCE(fpe.metadata->>'file_type', 'UNKNOWN') as file_type,
COALESCE(fs.source_description, 'unknown') AS source_description,
COALESCE(fc.config_value, 'no_config') AS config_value
FROM file_processing_events fpe
LEFT JOIN file_sources fs
ON fpe.file_name = fs.file_name
LEFT JOIN file_configurations fc 
ON fc.file_type = COALESCE(fpe.metadata->>'file_type', 'UNKNOWN')
ORDER BY fpe.event_time;
```

## Excercise 2
```
WITH flat_tags AS (
  SELECT 
    DATE(event_time) AS event_date,
    unnest(tags) AS tag
  FROM file_processing_events
  WHERE tags IS NOT NULL
),
ranked_events AS (
SELECT 
  event_id,
  DATE(event_time) AS event_date,  
  metadata->'weights' AS weights,
  RANK() OVER (
    PARTITION BY DATE(event_time)
    ORDER BY processing_time_ms DESC
  ) AS rnk
FROM file_processing_events
WHERE metadata->>'source' = 'HTSGET'
),
summary_event AS (
    SELECT     
        fpe.event_id,
        fpe.event_date,
        SUM(CAST(weights_elem AS numeric)) AS sum_weight,
        AVG(CAST(weights_elem AS numeric)) AS avg_weight
    FROM ranked_events fpe,
    LATERAL jsonb_array_elements_text(fpe.weights) AS weights_elem
    WHERE rnk = 1  
    GROUP BY fpe.event_id, fpe.event_date
),
summary_event_day AS (
    SELECT
        event_date,
        MAX(sum_weight) AS sum_weight,
        MAX(avg_weight) AS avg_weight
    FROM summary_event 
    GROUP BY event_date
)
SELECT 
  DATE(fpe.event_time) AS event_date,
  COUNT(*) AS num_entries_per_day,
  SUM(bytes_processed) AS total_bytes_processed,
  AVG(processing_time_ms) AS avg_processing_time_ms,
  SUM(SUM(bytes_processed)) OVER (ORDER BY DATE(event_time) ) AS total_bytes_cumulative,
  STRING_AGG(DISTINCT(file_name), ', ') AS files,
  COALESCE(ARRAY_AGG(DISTINCT ft.tag), '{}') AS tags,
  MAX(sum_weight) AS sum_weight,
  MAX(avg_weight) AS avg_weight

FROM file_processing_events fpe
LEFT JOIN flat_tags ft ON DATE(fpe.event_time) = ft.event_date
LEFT JOIN summary_event_day se ON DATE(fpe.event_time) = se.event_date
GROUP BY DATE(fpe.event_time)
ORDER BY event_date;
```


## Questions
# Did you need to do any assumptions because of any ambiguity or any edge cases?
***Edge case:*** We can have more that one cases(event_id) on one same day that all have same processing_time_ms and HTSGET source label. To avoid that scenario, I first computed the sum(weights) for all event_ids and then i selected as the sum(weights) of the day, the maximum among those events. This happens in the summary_event_day table that gets the max among event_ids of the same day as provided by the summary_event table.

# Have you encountered any errors during the development? Can you describe which ones and what did you do to solve them?
Syntax errors as always.
Logical errors as for instance in the last query, I had to do again MAX(sum_weight) because after the join with flat_tags, i was getting multiple rows per day and thus mutliple "copies" of the same weight coming from the summary_event_day. So I had to take again the max (or min since it is just the same value repeated among lines)

# Would you create any indexes on the table? Which ones and why?
I would create an INDEX on the day: DATE(event_time) , since i have been using that variable a lot to GROUP BY and ORDER
```CREATE INDEX idx_file_processing_events_event_date ON file_processing_events (DATE(event_time)) ; ```


# If we were going to create a graph on a website with the event_date, average_processing_time and total_bytes_cumulative, would you modify something? HINT: Think about how many results you got
We have data for multiple years. That means that if we plot those, the plto will be too dense because we have one record per day.
So we can:
- Split the resutls in intervals of X days, and show only those intervals in the plot
- Create a Materialized View of the query in order to be faster to make the same query again and again while i select different intervals
 ```CREATE MATERIALIZED VIEW daily_summary AS <the sql query above>``` Then I can take fast the intervals like: 
    - 30 days
    ```SELECT * FROM daily_summary
    WHERE event_date >= CURRENT_DATE - INTERVAL '30 days'; ```
