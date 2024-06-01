DROP table sd1;

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


-- Caricamento dei dati nella tabella (assumendo che i dati siano in un file chiamato stock_data.csv)
LOAD DATA LOCAL INPATH '/home/paleo/BigData/2Proj/Dataset/out_05.csv' INTO TABLE sd1;

-- Filtraggio e preparazione dei dati dal 2000 in poi
CREATE TEMPORARY TABLE filtered_stock_data AS
SELECT
    ticker,
    YEAR(`date`) AS year,
    `date`,
    close,
    name
FROM
    sd1
WHERE
    YEAR(`date`) >= 2000;

-- Calcolo della variazione percentuale per ogni ticker e anno
CREATE TEMPORARY TABLE percentage_change AS
SELECT
    ticker,
    year,
    ((LAST_VALUE(close) OVER (PARTITION BY ticker, YEAR(`date`) ORDER BY `date` ASC ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) -
     FIRST_VALUE(close) OVER (PARTITION BY ticker, YEAR(`date`) ORDER BY `date` ASC ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING)) /
     FIRST_VALUE(close) OVER (PARTITION BY ticker, YEAR(`date`) ORDER BY `date` ASC ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING)) * 100 AS percentage_change,
    name
FROM
    filtered_stock_data;

-- Raggruppamento dei ticker con gli stessi trend di 3 anni consecutivi
CREATE TEMPORARY TABLE pattern_groups AS
SELECT
    p1.ticker AS ticker1,
    p2.ticker AS ticker2,
    p3.ticker AS ticker3,
    p1.name AS name,
    p1.year AS year1,
    p2.year AS year2,
    p3.year AS year3,
    p1.percentage_change AS change1,
    p2.percentage_change AS change2,
    p3.percentage_change AS change3
FROM
    percentage_change p1
JOIN
    percentage_change p2 ON p1.ticker = p2.ticker AND p2.year = p1.year + 1
JOIN
    percentage_change p3 ON p2.ticker = p3.ticker AND p3.year = p2.year + 1
WHERE
    p1.percentage_change IS NOT NULL
    AND p2.percentage_change IS NOT NULL
    AND p3.percentage_change IS NOT NULL;


CREATE TEMPORARY TABLE final_trends AS
SELECT
    collect_set(name) AS companies,
    concat_ws(', ', collect_list(concat(year1, ':', cast(change1 as string))),
                      collect_list(concat(year2, ':', cast(change2 as string))),
                      collect_list(concat(year3, ':', cast(change3 as string)))) AS trends
FROM
    pattern_groups
GROUP BY
    change1, change2, change3, year1, year2, year3;


CREATE TEMPORARY TABLE Final AS
SELECT
    companies,
    trends
FROM
    final_trends
WHERE
    size(companies) > 1;


INSERT OVERWRITE LOCAL DIRECTORY '/home'
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
SELECT *
FROM percentage_change;

DROP table sd1;