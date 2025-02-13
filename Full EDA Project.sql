-- Exloratory Data Analysis --
-- EDA(world_layoffs)

SELECT * 
FROM world_layoffs.layoffs_staging2;
-- Cleaned data 


SELECT MAX(total_laid_off), MAX(percentage_laid_off)
FROM layoffs_staging2
;

SELECT * 
FROM layoffs_staging2
WHERE percentage_laid_off = 1
ORDER BY funds_raised_millions DESC
;

-- Laid off every employees(100%)


SELECT company, SUM(total_laid_off)
FROM layoffs_staging2
GROUP BY company
ORDER BY 2 DESC
;

-- 'GROUP BY' gives distinct company

SELECT MIN(`date`), MAX(`date`)
FROM layoffs_staging2
;

-- Lay offs started during the start of COVID-19 pandemic, for almost 3 years(null are excluded)

SELECT industry, SUM(total_laid_off)
FROM layoffs_staging2
GROUP BY industry
ORDER BY 2 DESC
;

-- 'GROUP BY' gives distinct industry

SELECT stage, SUM(total_laid_off)
FROM layoffs_staging2
GROUP BY stage
ORDER BY 2 DESC
;

-- 'GROUP BY' gives distinct stage


SELECT country, SUM(total_laid_off)
FROM layoffs_staging2
GROUP BY country
ORDER BY 2 DESC
;

-- 'GROUP BY' gives distinct country

SELECT YEAR(`date`), SUM(total_laid_off)
FROM layoffs_staging2
GROUP BY YEAR(`date`)
ORDER BY 1 DESC
;

-- YEAR(DATE) retreives only the year
-- 'GROUP BY' gives distinct year and the total_laid each year


SELECT MONTH(`date`) `month`, SUM(total_laid_off)
FROM layoffs_staging2
GROUP BY `month`
ORDER BY 1 
;

-- The above is inefficient as distinct month includes every year of it.
-- Month 1 include sum(laid_off) in 2020,2021,2022,.

SELECT SUBSTRING(`date`,1,7) `month`, SUM(total_laid_off) as total_laid_month
FROM layoffs_staging2
WHERE SUBSTRING(`date`,1,7) IS NOT NULL
GROUP BY `month`
ORDER BY 1
;

-- This looks much clear, the individual laid_off each month from 2020 to 2023
-- Where clause should not contain aggregated function

WITH ROLLING_CTE AS
(SELECT SUBSTRING(`date`,1,7) `month`, SUM(total_laid_off) as total_laid_month
FROM layoffs_staging2
WHERE SUBSTRING(`date`,1,7) IS NOT NULL
GROUP BY `month`
ORDER BY 1
)
SELECT `month`, total_laid_month,
SUM(total_laid_month) OVER(ORDER BY `month`) rolling_total_month
FROM ROLLING_CTE
;

-- With CTE, we can produce a progression of rolling total of total_laid_off each month of these three years
-- OVER() does not need partition by as already grouped with the distinct `month`


-- '383159' employees were laid off during the time period from Mar 2020 and Mar 2023.


SELECT company, YEAR(`date`), SUM(total_laid_off)
FROM layoffs_staging2
GROUP BY company, YEAR(`date`)
ORDER BY 3 DESC
;

-- Lay_offs, each year by each companies

WITH Ranking_CTE(company, `year`, total_laid) AS
(
SELECT company, YEAR(`date`), SUM(total_laid_off)
FROM layoffs_staging2
GROUP BY company, YEAR(`date`)
)
SELECT *,
DENSE_RANK() OVER(PARTITION BY `year` ORDER BY total_laid DESC) rank_company_laid 
FROM Ranking_CTE
WHERE `year` IS NOT NULL
;

-- Assigning rank based on total_laid_off by a company in a single year
-- Group every disitinct year with total lay offs by the particular company and order the 'rank' based on the num of layoffs in that particular year series
-- PARTITION BY() groups every distinct years for every company already grouped in the CTE
-- order by() assign rank in descending order of total_laid_off in each year

WITH Ranking_CTE(company, `year`, total_laid) AS
(
SELECT company, YEAR(`date`), SUM(total_laid_off)
FROM layoffs_staging2
GROUP BY company, YEAR(`date`)
)
SELECT *,
DENSE_RANK() OVER(PARTITION BY `year` ORDER BY total_laid DESC) rank_company_laid 
FROM Ranking_CTE
WHERE `year` IS NOT NULL
ORDER BY rank_company_laid
;

-- Ascending order of the rank

WITH Ranking_CTE(company, `year`, total_laid) AS
(
SELECT company, YEAR(`date`), SUM(total_laid_off)
FROM layoffs_staging2
GROUP BY company, YEAR(`date`)
),
Ranking_Company_CTE AS
(SELECT *,
DENSE_RANK() OVER(PARTITION BY `year` ORDER BY total_laid DESC) rank_company_laid 
FROM Ranking_CTE
WHERE `year` IS NOT NULL
)
SELECT *
FROM Ranking_Company_CTE 
WHERE rank_company_laid <=5
;

-- Top 5 companies with most lay oofs in each year
-- Multiple CTEs as mutilple analysis, Sum and top 5 ranked companies
-- We hit off the first CTE to make the 2nd CTE 
