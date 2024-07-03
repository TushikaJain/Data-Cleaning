use world_layoffs;
select * from layoffs;
create table lf
like layoffs;
select * from lf;
insert lf
select *
from layoffs;
#removing duplicates
with duplicate_cte as
(
select *,
row_number() over(
partition by company, location, industry, total_laid_off, percentage_laid_off, date, stage, country, funds_raised_millions) as row_num
from lf
)
select *
from duplicate_cte
where row_num>1;
select * from lf
where company='spotify';
select * from lf
where company='cazoo';
with duplicate_cte as
(
select *,
row_number() over(
partition by company, location, industry, total_laid_off, percentage_laid_off, date, stage, country, funds_raised_millions) as row_num
from lf
)
delete 
from duplicate_cte
where row_num>1;
set SQL_SAFE_UPDATES = 0;
CREATE TABLE `lf2` (
  `company` text,
  `location` text,
  `industry` text,
  `total_laid_off` int DEFAULT NULL,
  `percentage_laid_off` text,
  `date` text,
  `stage` text,
  `country` text,
  `funds_raised_millions` int DEFAULT NULL,
  `row_num` int
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;
select * from lf2;
insert into lf2 
select *,
row_number() over(
partition by company, location, industry, total_laid_off, percentage_laid_off, date, stage, country, funds_raised_millions) as row_num
from lf;
delete from lf2
where row_num>1;
select * from lf2;
#STANDARDISING DATA
select distinct(trim(company))
from lf2;
update lf2
set company=trim(company);
select distinct industry
from lf2
order by 1;
select * from lf2 
where industry like 'crypto%';
update lf2
set industry = 'Crypto'
where industry like 'Crypto%';
select distinct country from lf2
order by 1;
select *
from lf2 
where country like 'united states%';
update lf2
set country = 'United States'
where country like 'country';
select *
from lf2 ;
select distinct country
from lf ;
select date,
str_to_date(date, '%m/%d/%Y')
from lf2;
update lf2
set `date` = str_to_date(`date`, '%m/%d/%Y');
alter table lf2
modify column `date` date;
select *
from lf2
where total_laid_off is null
and percentage_laid_off is null;
select *
from lf2
where industry is null
or industry = '';
select * 
from lf2
where company = "Airbnb";
update lf2 
set industry = NULL
where industry = '';
select *
from lf2 t1
join lf2 t2
	on t1.company = t2.company
where (t1.industry is null or t1.industry = '')
and t2.industry is not null;
 select t1.industry, t2.industry
from lf2 t1
join lf2 t2
	on t1.company = t2.company
where (t1.industry is null or t1.industry = '')
and t2.industry is not null;   
update lf2 t1
join lf2 t2
	on t1.company = t2.company
set t1.industry = t2.industry
where t1.industry is null 
and t2.industry is not null;
select * 
from lf2
where company = "Airbnb";
select * 
from lf2
where company like "Bally%";
select *
from lf2
where total_laid_off is null
and percentage_laid_off is null;
delete
from lf2
where total_laid_off is null
and percentage_laid_off is null;
alter table lf2
drop column row_num;
select * from lf2;