-- Data Cleaning in world_layoffs --

SELECT * FROM world_layoffs.layoffs;

-- 0. Create a new table copying all raw data from the imported table, in case if we commit error, the raw data can be useful in the ways..
-- 1. Remove Duplicates
-- 2. Standardize the data (spell check, issues with values)
-- 3. Null values or Blank values
-- 4. Remove unnecessory columns


-- 0.Create new table with same raw data --

CREATE TABLE layoffs_staging
LIKE layoffs;

-- LIKE during creating a table is used for getting the exact same columns as the given table

SELECT * FROM layoffs_staging;

INSERT INTO layoffs_staging
SELECT *
FROM layoffs;

-- We can use select statement for copying data during inserting values to the new table


-- 1. Removing Duplicates --

SELECT *,
ROW_NUMBER() OVER(PARTITION BY company, industry, total_laid_off, percentage_laid_off, 'date') row_num
FROM layoffs_staging; 

-- duplicate data are represented by increasing row_num

WITH cte_duplicates AS
(
SELECT *,
ROW_NUMBER() OVER(PARTITION BY company, industry, total_laid_off, percentage_laid_off, 'date') row_num
FROM layoffs_staging
)
SELECT * FROM cte_duplicates 
WHERE row_num > 1;

-- cte for subquery alternative to select only the duplicates with row_num column

select *
from layoffs_staging
where company = 'oda'
;

-- checking the duplicates for confirmation is a great practice.
-- 'Oda' data are not duplicate and are supplementary.
-- lets partition by every column

SELECT *,
ROW_NUMBER() OVER(PARTITION BY company, location, industry, total_laid_off, percentage_laid_off, 
`date`, stage, country, funds_raised_millions) as row_num
FROM layoffs_staging; 

-- new distinct values in the data are rowed again from the start, 1 1 1 - represent needed data

WITH cte_duplicates AS
(
SELECT *,
ROW_NUMBER() OVER
(PARTITION BY company, location, industry, total_laid_off, percentage_laid_off, 
`date`, stage, country, funds_raised_millions) as row_num
FROM layoffs_staging
)
SELECT * FROM cte_duplicates 
WHERE row_num > 1;

select *
from layoffs_staging
where company = 'yahoo'
;

-- duplicates are founfd using row_number() over(partition by)
-- verified, confirmed

WITH cte_duplicates AS
(
SELECT *,
ROW_NUMBER() OVER
(PARTITION BY company, location, industry, total_laid_off, percentage_laid_off, 
`date`, stage, country, funds_raised_millions) as row_num
FROM layoffs_staging
)
DELETE 
FROM cte_duplicates 
WHERE row_num > 1;

-- mysql does not allow make updates(delete) in CTEs

-- lets create a new table with the same data in layoffs_staging and filter with row_num
-- copy to clipboard, create statement 

CREATE TABLE `layoffs_staging2` (
  `company` text,
  `location` text,
  `industry` text,
  `total_laid_off` int DEFAULT NULL,
  `percentage_laid_off` text,
  `date` text,
  `stage` text,
  `country` text,
  `funds_raised_millions` int DEFAULT NULL,
  `row_num` int DEFAULT NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

SELECT * FROM layoffs_staging2;

INSERT INTO layoffs_staging2
SELECT *,
ROW_NUMBER() OVER
(PARTITION BY company, location, industry, total_laid_off, percentage_laid_off, 
`date`, stage, country, funds_raised_millions) as row_num
FROM layoffs_staging;

-- we added a new column row_num with old query with row_number() into this table

DELETE
FROM layoffs_staging2
WHERE row_num > 1
;

SELECT *
FROM layoffs_staging2
WHERE row_num > 1
;

-- no duplicates


-- 2.Standardizing  Data (spell check, remove unwanted blank spaces)



SELECT *
FROM layoffs_staging2;

SELECT DISTINCT(company)
FROM  layoffs_staging2;

SELECT company, TRIM(company)
FROM layoffs_staging2;

-- Trimming the blank space

UPDATE layoffs_staging2
SET company = TRIM(company);

-- Updating the trimmed company column to staging2

SELECT DISTINCT industry
FROM layoffs_staging2
ORDER BY 1;

-- This helps to identify the same industry with different names(basic knowledge as per the raw data)

UPDATE layoffs_staging2
SET industry = 'Crypto'
WHERE industry LIKE 'Crypto%'
;

-- Updated the 'Crypto' industry as it had diff names

-- Location column looks fine as of now

SELECT DISTINCT country
FROM layoffs_staging2
ORDER BY 1;

-- United states with a '.' at the end

SELECT DISTINCT country, TRIM(TRAILING '.' FROM country)
FROM layoffs_staging2
ORDER BY 1;

-- (TRAILING '' FROM col_name) is used to trim the specified that is at the end

UPDATE layoffs_staging2
SET country = 'United States'
WHERE country like 'United States%';

-- AS full stop is removed, it has a blank space at the end, straight updating it with orginal



