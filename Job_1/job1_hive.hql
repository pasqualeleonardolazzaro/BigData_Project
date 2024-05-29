-- Create Hive table if not exists
CREATE TABLE IF NOT EXISTS sd1 (
    Unnamed_0 STRING,
    ticker STRING,
    name STRING,
    sector STRING,
    industry STRING,
    open STRING,
    close STRING,
    low STRING,
    high STRING,
    volume STRING,
    `date` DATE)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ',';

-- Load data into Hive table
LOAD DATA LOCAL INPATH 'out_50.csv' INTO TABLE sd1;

-- Query per generare le statistiche per ciascuna azione
CREATE TEMPORARY TABLE temp_table AS
SELECT ticker, name, MAX(`date`) as max_date, MIN(`date`) as min_date, year(`date`) as year, min(low) as min_low,max(high) as max_high, avg(volume) as avg_volume
FROM sd1
group by ticker, name, year(`date`)
order by ticker;

CREATE TEMPORARY TABLE Last AS
SELECT temp_table.ticker, temp_table.name, temp_table.year, sd1.close as last, temp_table.max_high, temp_table.min_low, temp_table.avg_volume, temp_table.min_date
FROM temp_table
JOIN sd1 WHERE (temp_table.ticker = sd1.ticker and sd1.`date` = temp_table.max_date);

CREATE TEMPORARY TABLE First AS
SELECT Last.ticker, Last.name, Last.year, sd1.close as first, Last.max_high, Last.min_low, Last.avg_volume, Last.last
FROM Last
JOIN sd1 WHERE (Last.ticker = sd1.ticker and sd1.`date` = Last.min_date);

CREATE TEMPORARY TABLE Result AS
SELECT ticker, name, year, min_low, avg_volume, max_high, (last - first)/first*100 as price_percentage
FROM First;

CREATE TEMPORARY TABLE FinalResult AS
SELECT ticker,
       name,
       COLLECT_LIST(
           STRUCT(
               year,
               STRUCT(
                   price_percentage,
                   min_low,
                   max_high,
                   avg_volume
               )
           )
       ) AS yearly_stats
FROM Result
GROUP BY ticker, name
ORDER BY ticker;

DESCRIBE  FinalResult;

INSERT OVERWRITE LOCAL DIRECTORY '/home/paleo/BigData/2Proj/Dataset/Job1_hive'
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
SELECT *
FROM FinalResult;


DROP table sd1;
