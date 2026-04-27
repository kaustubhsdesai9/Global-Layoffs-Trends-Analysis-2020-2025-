## Data Cleaning and Exploratory Data Analysis Project
## World Layoffs 

SELECT* 
FROM World_layoffs.layoffs;

##1. Remove Duplicates 
##2. Standardise the Data 
##3. Null Values or Blank Values 
##4. Remove Any Columns


## Creating a staging table to aboid any manipulations of raw data 
CREATE TABLE layoffs_staging
LIKE layoffs
; 

INSERT  layoffs_staging 
SELECT* 
FROM layoffs
; 

## 1. Remove Duplicates 

SELECT *,
ROW_NUMBER() OVER
(PARTITION BY company,location,total_laid_off,`date`,percentage_laid_off,industry,`source`,stage,funds_raised,country,date_added)
AS ROW_NUM   
FROM World_layoffs.layoffs_staging
; ##If ROW_NUM is 2 its a duplicate 

WITH duplicate_cte AS 
(
SELECT *,
ROW_NUMBER() OVER
(PARTITION BY company,location,total_laid_off,`date`,percentage_laid_off,industry,`source`,stage,funds_raised,country,date_added)
AS ROW_NUM   
FROM World_layoffs.layoffs_staging
)
SELECT*
FROM duplicate_cte 
WHERE ROW_NUM > 1
;  ## Filtering for the duplicates using CTE 

SELECT* 
FROM layoffs_staging
WHERE company = 'Cars24' 
; ## Confirming the results of above qury 


CREATE TABLE `layoffs_staging2`(
  `company` text,
  `location` text,
  `total_laid_off` text,
  `date` text,
  `percentage_laid_off` text,
  `industry` text,
  `source` text,
  `stage` text,
  `funds_raised` int DEFAULT NULL,
  `country` text,
  `date_added` text,
  `ROW_NUM` INT 
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci
; ## Created a layoffs _staging2 table to manupulate dupliate data 

SELECT* 
FROM World_layoffs.layoffs_staging2 
; 

INSERT INTO layoffs_staging2
SELECT *,
ROW_NUMBER() OVER
(PARTITION BY company,location,total_laid_off,`date`,percentage_laid_off,industry,`source`,stage,funds_raised,country,date_added)
AS ROW_NUM   
FROM World_layoffs.layoffs_staging
; ## Added all the data from layoffs_staging table plus the ROW_NUM coloumn which is to be filtered next 

SELECT* 
FROM World_layoffs.layoffs_staging2 
WHERE ROW_NUM > 1
; ## Filtered Duplicates in layoffs_staging2 table 

DELETE 
FROM World_layoffs.layoffs_staging2 
WHERE ROW_NUM > 1
; ##Deleted duplicates from layoffs_staging2 table 

SELECT* 
FROM World_layoffs.layoffs_staging2;

## Stage 1 - removing duplicates complete 

## 2. Standardise the Data 

SELECT company, (TRIM(company)) 
FROM World_layoffs.layoffs_staging2
;

UPDATE World_layoffs.layoffs_staging2
SET company = TRIM(company)
;

SELECT DISTINCT industry
FROM World_layoffs.layoffs_staging2
ORDER BY 1
; ## Just to check 
 
SELECT DISTINCT location
FROM World_layoffs.layoffs_staging2
ORDER BY 1
; ## Just to check 

SELECT DISTINCT country 
FROM World_layoffs.layoffs_staging2
ORDER BY 1
; ## Just to check 

UPDATE World_layoffs.layoffs_staging2
SET `date` = STR_TO_DATE(`date`, '%m/%d/%Y')
WHERE `date` LIKE '%/%/%'
; ## Converting date coloumn from text to int 

ALTER TABLE World_layoffs.layoffs_staging2
MODIFY COLUMN `date` DATE
; ## Modifying the coloumn and table 

SELECT*
FROM World_layoffs.layoffs_staging2
WHERE industry IS NULL
OR industry = ''
; ## Found another duplicate company named appsmith - it wasnt filtered earlier as industry coloumn had no data on these rows


## Addition to stage 1 - removing the duplicates 
SELECT*
FROM World_layoffs.layoffs_staging2
WHERE company = 'appsmith'
;## making sure if there is more data on Company 'Appsmith' - only 2 lines both same 

DELETE 
FROM World_layoffs.layoffs_staging2 
WHERE company = 'appsmith'
; ##Deleted all layoff data from company 'Applesmith' as data couldnt be varified as accurate 

## 3. Null Values or Blank Values 
## No NULLS in relevent coloumns 

## 4. Remove Any Columns
## No Coloumns to be removed at this stage


## Exploratory Data Analysis 

SELECT* 
FROM World_layoffs.layoffs_staging2 
;

SELECT MAX(total_laid_off), MAX(percentage_laid_off) 
FROM World_layoffs.layoffs_staging2
; ## finding MAX 

SELECT* 
FROM World_layoffs.layoffs_staging2 
WHERE percentage_laid_off = 1
ORDER BY total_laid_off DESC
; ## companies which went bust 

SELECT* 
FROM World_layoffs.layoffs_staging2 
WHERE percentage_laid_off = 1
ORDER BY funds_raised DESC
; ## layoffs based on funds raised by companies 

SELECT MIN(`date`), MAX(`date`) 
FROM World_layoffs.layoffs_staging2
;## finding start and end date of the records in the dataset 


SELECT company, SUM(total_laid_off)
FROM World_layoffs.layoffs_staging2 
GROUP BY company 
ORDER BY 2 DESC
; ## Layoffs based on company

SELECT industry, SUM(total_laid_off)
FROM World_layoffs.layoffs_staging2 
GROUP BY industry 
ORDER BY 2 DESC
; ## layoffs based on industry 

SELECT country, SUM(total_laid_off)
FROM World_layoffs.layoffs_staging2 
GROUP BY country 
ORDER BY 2 DESC
;## layoffs based on country 

SELECT YEAR (`date`), SUM(total_laid_off)
FROM World_layoffs.layoffs_staging2 
GROUP BY YEAR (`date`)
ORDER BY 1 DESC
;## layoffs based on year 

SELECT stage, SUM(total_laid_off)
FROM World_layoffs.layoffs_staging2 
GROUP BY stage 
ORDER BY 2 DESC
; ## layoffs based on fundraising stage of the company 

SELECT substring(`date`,1,7) as `MONTH`, SUM(total_laid_off)
FROM World_layoffs.layoffs_staging2 
WHERE substring(`date`,1,7) IS NOT NULL
GROUP BY `MONTH`
ORDER BY 1 ASC 
; ## layoffs based on month of the year 

WITH rolling_total AS 
(
SELECT substring(`date`,1,7) as `MONTH`, SUM(total_laid_off) as total_fired 
FROM World_layoffs.layoffs_staging2 
WHERE substring(`date`,1,7) IS NOT NULL
GROUP BY `MONTH`
ORDER BY 1 ASC 
)
SELECT `MONTH`, total_fired, SUM(total_fired) OVER (ORDER BY `MONTH`) AS rolling_total 
FROM rolling_total 
; ##rolling total of layoffs 


SELECT company, YEAR (`date`),  SUM(total_laid_off)
FROM World_layoffs.layoffs_staging2 
GROUP BY company, YEAR (`date`)
ORDER BY company ASC
; ## layoffs per company per year 


WITH company_year (company, years, total_laid_off) AS 
(
SELECT company, YEAR (`date`),  SUM(total_laid_off)
FROM World_layoffs.layoffs_staging2 
GROUP BY company, YEAR (`date`)
),
Company_Year_Rank AS 
(
SELECT*, DENSE_RANK () OVER(PARTITION BY years ORDER BY total_laid_off DESC) AS ranking
FROM company_year
WHERE years IS NOT NULL
ORDER BY ranking ASC
)
SELECT *
FROM Company_Year_Rank 
WHERE ranking  <= 5
;## 2 CTEs used to find out top 5 companies (in terms of employee lay offs) every year. 


## Prep For Data Viz 

SELECT
    SUM(total_laid_off) AS total_layoffs,
    COUNT(DISTINCT company) AS companies_affected,
    COUNT(DISTINCT country) AS countries_affected,
    MIN(`date`) AS start_date,
    MAX(`date`) AS end_date
FROM layoffs_staging2
; ## KPIs. Metrics = total Layoffs, number of afftected companies, number of countries and date range. 

SELECT
    DATE_FORMAT(`date`, '%Y-%m') AS month,
    SUM(total_laid_off) AS total_laid_off
FROM layoffs_staging2
GROUP BY month
ORDER BY month
; ## Time based Trends

WITH monthly AS (
    SELECT
        DATE_FORMAT(`date`, '%Y-%m') AS month,
        SUM(total_laid_off) AS total_laid_off
    FROM layoffs_staging2
    GROUP BY month
)
SELECT
    month,
    total_laid_off,
    SUM(total_laid_off) OVER (ORDER BY month) AS cumulative_layoffs
FROM monthly
;## Rolling (Cumulative) Layoffs

SELECT
    company,
    SUM(total_laid_off) AS total_laid_off
FROM layoffs_staging2
GROUP BY company
ORDER BY total_laid_off DESC
LIMIT 10
; ## Top 10 Companies by Total Layoffs

SELECT
    industry,
    SUM(total_laid_off) AS total_laid_off
FROM layoffs_staging2
GROUP BY industry
ORDER BY total_laid_off DESC
;## Layoffs by Industry

SELECT
    country,
    SUM(total_laid_off) AS total_laid_off
FROM layoffs_staging2
GROUP BY country
; ##Layoffs by Country (Map)

SELECT
    stage,
    SUM(total_laid_off) AS total_laid_off
FROM layoffs_staging2
GROUP BY stage
ORDER BY total_laid_off DESC
;## Layoffs by Company Stage

SELECT
    company,
    SUM(total_laid_off) AS total_laid_off,
    MAX(funds_raised) AS funds_raised
FROM layoffs_staging2
WHERE funds_raised IS NOT NULL
GROUP BY company
; ## Funds Raised vs Layoffs

SELECT
    company,
    industry,
    country,
    total_laid_off,
    funds_raised
FROM layoffs_staging2
WHERE percentage_laid_off = 1
ORDER BY total_laid_off DESC
;## Companies That Shut Down (100% Layoffs)

WITH company_year AS (
    SELECT
        company,
        YEAR(`date`) AS year,
        SUM(total_laid_off) AS total_laid_off
    FROM layoffs_staging2
    GROUP BY company, year
),
ranked AS (
    SELECT *,
           DENSE_RANK() OVER (PARTITION BY year ORDER BY total_laid_off DESC) AS rank_
    FROM company_year
)
SELECT *
FROM ranked
WHERE rank_ <= 5
; ## Top 5 Companies by Layoffs Each Year

CREATE VIEW v_monthly_layoffs AS
SELECT		
    DATE_FORMAT(`date`, '%Y-%m') AS month,
    SUM(total_laid_off) AS total_laid_off
FROM layoffs_staging2
GROUP BY month
;
