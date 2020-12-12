create table photo_dates (
    source_file varchar(255) not null primary key
    ,date_time_original varchar(25)
    ,create_date varchar(25)
    ,modify_date varchar(25)
    --,best_date datetime
) AS SELECT * FROM CSVREAD('src/main/resources/static/all_dates_by_file.csv');

ALTER TABLE PHOTO_DATES ADD COLUMN google_date varchar(25);
ALTER TABLE PHOTO_DATES ADD COLUMN best_date varchar(25);
DELETE FROM PHOTO_DATES WHERE SOURCE_FILE LIKE '%.json';
UPDATE PHOTO_DATES SET google_date=SUBSTRING(source_file, 47, 10) WHERE SOURCE_FILE LIKE 'D:/google-takeout%';
UPDATE PHOTO_DATES SET best_date=COALESCE (CASE
		WHEN length(date_time_original) > 0 AND date_time_original > '2000' THEN date_time_original
		WHEN length(create_date) > 0 AND create_date > '2000' THEN create_date
		WHEN length(modify_date) > 0 AND modify_date > '2000' THEN modify_date
		WHEN length(google_date) > 0 AND google_date > '2000' THEN google_date
	END);


--# sort -k2 duplicates-all-v2.2.csv |uniq|sort > uniq.csv
create table duplicates(
    long_hash varchar(255) not null
    ,source_file varchar(255) not null primary key
    ,base_name varchar(100)
    ,num int
) AS SELECT * FROM CSVREAD('src/main/resources/static/test1.csv');

create index idx_dup_file on duplicates(source_file);
create index idx_dup_hash on duplicates(long_hash);

ALTER TABLE DUPLICATES ADD COLUMN score int;
ALTER TABLE DUPLICATES ADD COLUMN best_date varchar(25);
UPDATE DUPLICATES SET SOURCE_FILE = REPLACE(SOURCE_FILE,'\','/');

UPDATE DUPLICATES
SET  DUPLICATES.BEST_DATE=(SELECT PHOTO_DATES.BEST_DATE
                            FROM PHOTO_DATES
                            WHERE DUPLICATES.SOURCE_FILE = PHOTO_DATES.SOURCE_FILE);
--WHERE LENGTH(DUPLICATES.BEST_DATE)=0;

UPDATE DUPLICATES SET score=COALESCE (CASE
		WHEN SOURCE_FILE LIKE 'D:%Media%' AND INSTR(SOURCE_FILE, BASE_NAME) > 0 THEN 1
		WHEN INSTR(SOURCE_FILE, BEST_DATE) > 0 AND INSTR(SOURCE_FILE, BASE_NAME) > 0 THEN 2
		WHEN INSTR(SOURCE_FILE, BEST_DATE) = 0 THEN 5
		WHEN INSTR(SOURCE_FILE, BASE_NAME) > 0 THEN 7
		WHEN SOURCE_FILE LIKE '%duplicate%' THEN 3
		WHEN SOURCE_FILE LIKE '%(1)%' THEN 3
		WHEN SOURCE_FILE LIKE '%edited%' THEN 3
	END);

SELECT left(LONG_HASH, INSTR(LONG_HASH,'_')+5) hash,SOURCE_FILE,row_num,BEST_DATE,BASE_NAME --select count(*)
FROM (
    SELECT
    ROW_NUMBER() over(PARTITION BY LONG_HASH,BEST_DATE,BASE_NAME ORDER BY score) row_num
    ,rank() over(PARTITION BY LONG_HASH,BEST_DATE,BASE_NAME ORDER BY score) rnk
    ,*
    FROM DUPLICATES d
) x
ORDER BY LONG_HASH,row_num;



