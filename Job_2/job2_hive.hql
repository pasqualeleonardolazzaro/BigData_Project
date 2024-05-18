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

-- Query per generare le statistiche per ciascuna azione
-- CREATE TEMPORARY TABLE temp_table AS
-- SELECT ticker, name, MAX(`date`) as max_date, MIN(`date`) as min_date, year(`date`) as year, SUM(volume) as tot_volume
-- FROM sd1
-- group by ticker, name, year(`date`)
-- order by ticker;
--
-- CREATE TEMPORARY TABLE Last AS
-- SELECT sd1.industry, temp_table.ticker, temp_table.name, temp_table.year, sd1.close as last, temp_table.tot_volume, temp_table.min_date
-- FROM temp_table
-- JOIN sd1 WHERE (temp_table.ticker = sd1.ticker and sd1.`date` = temp_table.max_date);
--
-- CREATE TEMPORARY TABLE First AS
-- SELECT Last.industry, Last.ticker, Last.name, Last.year, sd1.close as first, Last.tot_volume, Last.last
-- FROM Last
-- JOIN sd1 WHERE (Last.ticker = sd1.ticker and sd1.`date` = Last.min_date);
--
-- CREATE TEMPORARY TABLE Result AS
-- SELECT industry, year, MAX(tot_volume) as max_totVolume, (SUM(last) - SUM(first))/SUM(first)*100 as industryQuote_variation, MAX((last - first)/first*100)  as max_price_percentage
-- FROM First
-- GROUP BY industry, year;
--
-- CREATE TEMPORARY TABLE PricePercentage AS
-- SELECT sd1.sector,First.ticker, First.name, First.year, First.industry, (First.last - First.first)/First.first*100 as price_percentage
-- FROM First
-- Join sd1 WHERE  (First.ticker = sd1.ticker and First.name = sd1.name);
--
-- CREATE TEMPORARY TABLE stock_max_volume AS
-- SELECT Result.industry, Result.year, Result.max_totVolume, Result.industryQuote_variation, Result.max_price_percentage, Last.ticker as max_volume_company
-- FROM Result
-- JOIN Last
-- WHERE (Result.industry = Last.industry and Result.year=Last.year and Result.max_totVolume = Last.tot_volume);
--
-- CREATE TEMPORARY TABLE Final AS
-- SELECT PricePercentage.sector, stock_max_volume.industry, stock_max_volume.year, stock_max_volume.max_totVolume, stock_max_volume.industryQuote_variation, stock_max_volume.max_price_percentage, stock_max_volume.max_volume_company, PricePercentage.ticker as max_price_percentage_company
-- FROM stock_max_volume
-- JOIN PricePercentage
-- WHERE (stock_max_volume.industry = PricePercentage.industry and stock_max_volume.year=PricePercentage.year and stock_max_volume.max_price_percentage= PricePercentage.price_percentage)
-- ORDER BY sector ASC, industryQuote_variation DESC;
--
-- DESCRIBE  Final;

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
