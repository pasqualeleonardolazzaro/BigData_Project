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
LOAD DATA LOCAL INPATH '/home/paleo/BigData/2Proj/Dataset/out.csv' INTO TABLE sd1;


-- Calcolo della variazione percentuale per ciascuna azione per anno e industria
CREATE TEMPORARY TABLE IndustryYearlyChange AS
SELECT
    industry,
    sector,
    ticker,
    YEAR(`date`) AS year,
    (LAST_VALUE(close) OVER (PARTITION BY ticker, YEAR(`date`) ORDER BY `date` ASC ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) -
     FIRST_VALUE(close) OVER (PARTITION BY ticker, YEAR(`date`) ORDER BY `date` ASC ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING)) /
     FIRST_VALUE(close) OVER (PARTITION BY ticker, YEAR(`date`) ORDER BY `date` ASC ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) * 100 AS percentage_change
FROM
    sd1;

-- Calcolo dell'azione con il maggiore incremento percentuale per industria per anno
CREATE TEMPORARY TABLE MaxIncreaseAction AS
SELECT
    industry,
    sector,
    year,
    ticker,
    percentage_change,
    RANK() OVER (PARTITION BY industry, year ORDER BY percentage_change DESC) AS rank_increase
FROM
    IndustryYearlyChange;

-- Calcolo dell'azione con il maggiore volume di transazioni per industria per anno
CREATE TEMPORARY TABLE MaxVolumeAction AS
SELECT
    industry,
    sector,
    ticker,
    YEAR(`date`) AS year,
    SUM(volume) AS total_volume,
    RANK() OVER (PARTITION BY industry, YEAR(`date`) ORDER BY SUM(volume) DESC) AS rank_volume
FROM
    sd1
GROUP BY
    industry,
    sector,
    ticker,
    YEAR(`date`);

-- Combinazione delle informazioni in un unico report
CREATE TEMPORARY TABLE Final AS
SELECT
    a.industry,
    a.sector,
    a.year,
    AVG(a.percentage_change) AS avg_percentage_change,
    b.ticker AS max_increase_ticker,
    b.percentage_change AS max_increase_percentage,
    c.ticker AS max_volume_ticker,
    c.total_volume AS max_volume
FROM
    IndustryYearlyChange a
JOIN
    MaxIncreaseAction b ON a.industry = b.industry AND a.year = b.year AND b.rank_increase = 1
JOIN
    MaxVolumeAction c ON a.industry = c.industry AND a.year = c.year AND c.rank_volume = 1
GROUP BY
    a.industry,
    a.sector,
    a.year,
    b.ticker,
    b.percentage_change,
    c.ticker,
    c.total_volume
ORDER BY
    a.sector,
    avg_percentage_change DESC;


INSERT OVERWRITE LOCAL DIRECTORY '/home/paleo/BigData/2Proj/Dataset/Job2_hive'
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
SELECT *
FROM Final;

DROP table sd1;
