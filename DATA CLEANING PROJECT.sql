                                      -- SQL PROJECT -> DATA  CLEANING 


select * from layoffs;

-- first thing we want to do is create a staging table. This is the one we will work in and clean the data. We want a table with the raw data in case something happens

create table layoffs_staging 
	like layoffs ;

select * from layoffs_staging;

insert layoffs_staging 
	select * from layoffs;

-- now when we are data cleaning we usually follow a few steps
-- 1. check for duplicates and remove any
-- 2. standardize data and fix errors
-- 3. Look at null values and see what 
-- 4. remove any columns and rows that are not necessary - few ways

-- First Step -> " REMOVING DUPLICATES " 

# First let's check for duplicates

-- We need to really look at every single row to be accurate

-- those will our real duplicates 

select * ,
	row_number() over(
		partition by company ,location, industry , total_laid_off, percentage_laid_off , 'date', stage, country,funds_raised_millions) as row_num 
from layoffs_staging;

-- create CTEs to filter the column (row_num)

WITH duplicate_cte as 
( 
select * ,
	row_number() over(
		partition by company ,location, industry , total_laid_off, percentage_laid_off , date, stage, country,funds_raised_millions) as row_num 
from layoffs_staging
)

select * from duplicate_cte 
	where row_num > 1;

select * from layoffs_staging 
	where company = 'cazoo' ; 

-- one solution, which I think is a good one. Is to create a new column and add those row numbers in. Then delete where row numbers are over 2, then delete that column
-- so let's do it!!


create table layoffs_staging2 
	like layoffs ;

alter table layoffs_staging2
	add column row_num INT ;

insert into layoffs_staging2 
select * ,
	row_number() over(
		partition by company ,location, industry , total_laid_off, percentage_laid_off , date, stage, country,funds_raised_millions) as row_num 
from layoffs_staging  ;

-- now that we have this we can delete rows were row_num is greater than 2

SELECT * FROM layoffs_staging2 ;

DELETE  FROM layoffs_staging2 
	WHERE ROW_NUM > 1  ;

-- STEP 2 => STANDARDIZING DATA -> Finding issues in data and fixiing it 

-- Removing Whitespaces from the company column  
select company , trim(company) 
	from  layoffs_staging2;

update layoffs_staging2 
	set company = trim(company) ;


-- if we look at industry it looks like we have some null and empty rows, let's take a look at these
SELECT DISTINCT industry
	FROM layoffs_staging2
		ORDER BY industry;

SELECT *
	FROM layoffs_staging2
		WHERE industry IS NULL 
			OR industry = ''
ORDER BY industry;

-- let's take a look at these
SELECT *
	FROM layoffs_staging2
		WHERE company LIKE 'Bally%';
-- nothing wrong here
SELECT *
	FROM layoffs_staging2
		WHERE company LIKE 'airbnb%';

-- it looks like airbnb is a travel, but this one just isn't populated.
-- I'm sure it's the same for the others. What we can do is
-- write a query that if there is another row with the same company name, it will update it to the non-null industry values
-- makes it easy so if there were thousands we wouldn't have to manually check them all

-- we should set the blanks to nulls since those are typically easier to work with
UPDATE layoffs_staging2
	SET industry = NULL
		WHERE industry = '';

-- now if we check those are all null

SELECT *
	FROM layoffs_staging2
		WHERE industry IS NULL 
			OR industry = ''
ORDER BY industry;

-- now we need to populate those nulls if possible

select * 
	from layoffs_staging2 t1 
		join layoffs_staging2 t2 
			ON t1.company = t2.company 
		where t1.industry is null 
and t2.industry is not null ;

UPDATE layoffs_staging2 t1
	JOIN layoffs_staging2 t2
		ON t1.company = t2.company
		SET t1.industry = t2.industry
	WHERE t1.industry IS NULL
AND t2.industry IS NOT NULL;

-- and if we check it looks like Bally's was the only one without a populated row to populate this null values
SELECT *
	FROM world_layoffs.layoffs_staging2
		WHERE industry IS NULL 
	OR industry = ''
ORDER BY industry;

-- ---------------------------------------------------

-- I also noticed the Crypto has multiple different variations. We need to standardize that - let's say all to Crypto
SELECT DISTINCT industry
	FROM layoffs_staging2
ORDER BY industry;


update layoffs_staging2 
	set industry = 'Crypto'
where industry like 'Crypto%' ; 

select * 
	from layoffs_staging2 
where industry like 'Crypto';

-- everything looks good except apparently we have some "United States" and some "United States." with a period at the end. In the country column

#Let's standardize this.


select distinct country 
	from layoffs_staging2
order by 1 ; 

select * from layoffs_staging2
	where country 
like 'United States%' ;

update layoffs_staging2 
	set country = 'United States'
where country 
like 'United States%' ;

-- now if we run this again it is fixed
SELECT DISTINCT country
	FROM layoffs_staging2
ORDER BY country ;

-- 'DATE' column stadardize -> changing the column datatypr from text to date 

select `date` ,
str_to_date(`date`,'%m/%d/%Y')
from layoffs_staging2;

update layoffs_staging2 
set `date` = str_to_date(`date`,'%m/%d/%Y') ;


-- now we can convert the data type properly

Alter table layoffs_staging2 
	modify column `date` date ;

-- Step 3 -> Look at Null Values

-- the null values in total_laid_off, percentage_laid_off, and funds_raised_millions all look normal. I don't think I want to change that
-- so there isn't anything I want to change with the null values


-- Step 4. -> Remove any columns and rows we need to

-- if total_laid_off and Percentage_laid_off both have null values then we will remove these column from the table 

select *
	from layoffs_staging2 
		where total_laid_off is null 
and percentage_laid_off is null ;

Delete 
	from layoffs_staging2 
		where total_laid_off is null 
and percentage_laid_off is null ;

-- Now we delete the 'row_num' column because we dont need it anymore 
 
 alter table layoffs_staging2 
	drop column row_num ;
 
 select * from layoffs_staging2





