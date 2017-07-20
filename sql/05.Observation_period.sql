/**************************************
 --encoding : UTF-8
 --Author: 이성원
 --Date: 2017.01.24
 
 @NHISDatabaseSchema : DB containing NHIS National Sample cohort DB
 @NHIS_JK: JK table in NHIS NSC
 @NHIS_20T: 20 table in NHIS NSC
 @NHIS_30T: 30 table in NHIS NSC
 @NHIS_40T: 40 table in NHIS NSC
 @NHIS_60T: 60 table in NHIS NSC
 @NHIS_GJ: GJ table in NHIS NSC
 --Description: Observation_period 테이블 생성
 --Generating Table: OBSERVATION_PERIOD
***************************************/

/**************************************
 1. 테이블 생성
***************************************/ 
CREATE TABLE @ResultDatabaseSchema.OBSERVATION_PERIOD ( 
     observation_period_id				INTEGER	 IDENTITY(1,1)	NOT NULL , 
     person_id							INTEGER		NOT NULL , 
     observation_period_start_date		DATE		NOT NULL , 
     observation_period_end_date		DATE		NOT NULL ,
	 period_type_concept_id				INTEGER		NOT NULL
    ) 
/**************************************
 2. 데이터 입력
    1) 관측시작일: 자격년도.01.01이 디폴트. 출생년도가 그 이전이면 출생년도.01.01
	2) 관측종료일: 자격년도.12.31이 디폴트. 사망년월이 그 이후면 사망년.월.마지막날
***************************************/ 

insert into @ResultDatabaseSchema.OBSERVATION_PERIOD
	(person_id, observation_period_start_date, observation_period_end_date, period_type_concept_id)
select 
	a.person_id as person_id, 
	case when a.min_stnd_y >= b.year_of_birth then convert(date, convert(varchar, a.min_stnd_y) + '0101', 112) 
		else convert(date, convert(varchar, b.year_of_birth) + '0101', 112) 
	end as observation_period_start_date, --관측시작일
	case when convert(date, a.max_stnd_y + '1231', 112) > c.death_date then c.death_date
		else convert(date, a.max_stnd_y + '1231', 112)
	end as observation_period_end_date, --관측종료일
	44814725 as period_type_concept_id
from (select person_id, min(stnd_y) as min_stnd_y, max(stnd_y) as max_stnd_y 
	from @NHISDatabaseSchema.@NHIS_JK
	group by person_id) a,
	@ResultDatabaseSchema.person b left join @ResultDatabaseSchema.death c
	on b.person_id=c.person_id
where a.person_id=b.person_id


SELECT TOP 100 PERSON_ID, MIN(STND_Y),MAX(STND_Y), CAST(MAX(STND_Y) AS INT)- CAST(MIN(STND_Y) AS INT) +1, COUNT(DISTINCT STND_Y) FROM [nhid].[dbo].[ID02_13_JK] 
GROUP BY PERSON_ID

SELECT TOP 100 * FROM @NHISDatabaseSchema.@NHIS_JK


insert into @ResultDatabaseSchema.OBSERVATION_PERIOD
	(person_id, observation_period_start_date, observation_period_end_date, period_type_concept_id)

select 
	a.person_id as person_id, 
	case when a.stnd_y >= b.year_of_birth then convert(date, convert(varchar, a.stnd_y) + '0101', 112) 
		else convert(date, convert(varchar, b.year_of_birth) + '0101', 112) 
	end as observation_period_start_date, --관측시작일
	case when convert(date, a.stnd_y + '1231', 112) > c.death_date then c.death_date
		else convert(date, a.stnd_y + '1231', 112)
	end as observation_period_end_date, --관측종료일
	44814725 as period_type_concept_id
into #person_temp
from @NHISDatabaseSchema.@NHIS_JK a,
	@ResultDatabaseSchema.person b left join @ResultDatabaseSchema.death c
	on b.person_id=c.person_id
where a.person_id=b.person_id


SELECT person_id, MIN(observation_period_start_date), MAX(observation_period_end_date), 44814725 AS period_type_concept_id
	--, ROW_NUMBER ()
FROM #person_temp
GROUP BY person_id
	--WHERE 
