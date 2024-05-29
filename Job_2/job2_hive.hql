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
CREATE TEMPORARY TABLE IndustryYearlyChange0 AS
SELECT
    industry,
    sector,
    ticker,
    YEAR(`date`) AS year,
    LAST_VALUE(close) OVER (PARTITION BY ticker, YEAR(`date`) ORDER BY `date` ASC ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) as last_close,
    FIRST_VALUE(close) OVER (PARTITION BY ticker, YEAR(`date`) ORDER BY `date` ASC ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) as first_close,
    ((LAST_VALUE(close) OVER (PARTITION BY ticker, YEAR(`date`) ORDER BY `date` ASC ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) -
     FIRST_VALUE(close) OVER (PARTITION BY ticker, YEAR(`date`) ORDER BY `date` ASC ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING)) /
     FIRST_VALUE(close) OVER (PARTITION BY ticker, YEAR(`date`) ORDER BY `date` ASC ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING)) * 100 AS percentage_change
FROM
    sd1;

CREATE TEMPORARY TABLE IndustryYearlyChange AS
SELECT
    industry,
    year,
    ((SUM(last_close)-SUM(first_close))/SUM(first_close))*100 as percentage_industry
FROM
    IndustryYearlyChange0
GROUP BY industry, year;
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
    IndustryYearlyChange0;

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
    b.industry,
    b.sector,
    b.year,
    d.percentage_industry as percentage_industry ,
    b.ticker AS max_increase_ticker,
    b.percentage_change AS max_increase_percentage,
    c.ticker AS max_volume_ticker,
    c.total_volume AS max_volume

FROM
    MaxIncreaseAction b
JOIN
    MaxVolumeAction c ON b.industry = c.industry AND b.year = c.year AND c.rank_volume = 1
JOIN
    IndustryYearlyChange d ON b.industry = d.industry AND b.year = d.year
WHERE  b.rank_increase = 1
GROUP BY
    b.industry,
    b.sector,
    b.year,
    b.ticker,
    d.percentage_industry,
    b.percentage_change,
    c.ticker,
    c.total_volume
ORDER BY
    b.sector,
    d.percentage_industry DESC;


INSERT OVERWRITE LOCAL DIRECTORY '/home/paleo/BigData/2Proj/Dataset/Job2_hive'
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
SELECT *
FROM Final;

DROP table sd1;
