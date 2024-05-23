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


-- Calcolo delle variazioni percentuali per anno >= 2000
CREATE TEMPORARY TABLE AnnualPercentChange AS
SELECT
    ticker,
    name,
    YEAR(`date`) AS year,
    (LAST_VALUE(close) OVER w - FIRST_VALUE(close) OVER w) / FIRST_VALUE(close) OVER w * 100 AS percent_change
FROM
    sd1
WHERE
    YEAR(`date`) >= 2000
WINDOW w AS (PARTITION BY ticker, YEAR(`date`) ORDER BY `date` ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING);

-- ThreeYearTrends
CREATE TEMPORARY TABLE ThreeYearTrends AS
SELECT DISTINCT
    a.ticker,
    a.name,
    CONCAT_WS(',', CAST(a.year AS STRING), CAST((a.year + 1) AS STRING), CAST((a.year + 2) AS STRING)) AS year_sequence,
    CONCAT_WS(':', CAST(a.year AS STRING), CAST(ROUND(a.percent_change, 2) AS STRING)) AS first_year_trend,
    CONCAT_WS(':', CAST((a.year + 1) AS STRING), CAST(ROUND(b.percent_change, 2) AS STRING)) AS second_year_trend,
    CONCAT_WS(':', CAST((a.year + 2) AS STRING), CAST(ROUND(c.percent_change, 2) AS STRING)) AS third_year_trend
FROM
    AnnualPercentChange a
JOIN
    AnnualPercentChange b ON a.ticker = b.ticker AND b.year = a.year + 1
JOIN
    AnnualPercentChange c ON a.ticker = c.ticker AND c.year = a.year + 2;



--final result

SELECT
    COLLECT_SET(name) AS companies,
    CONCAT_WS(', ', first_year_trend, second_year_trend, third_year_trend) AS trends
FROM
    ThreeYearTrends
GROUP BY
    trends
LIMIT 20;



-- INSERT OVERWRITE LOCAL DIRECTORY '/home/paleo/BigData/2Proj/Dataset/Job2_hive'
-- ROW FORMAT DELIMITED
-- FIELDS TERMINATED BY ','
-- SELECT *
-- FROM Final;

DROP table sd1;
