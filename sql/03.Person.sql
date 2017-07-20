/**************************************
 --encoding : UTF-8
 --Author: 이성원
 --Date: 2017.01.20
 
 @NHISDatabaseSchema : DB containing NHIS National Sample cohort DB
 @NHIS_JK: JK table in NHIS NSC
 @NHIS_20T: 20 table in NHIS NSC
 @NHIS_30T: 30 table in NHIS NSC
 @NHIS_40T: 40 table in NHIS NSC
 @NHIS_60T: 60 table in NHIS NSC
 @NHIS_GJ: GJ table in NHIS NSC
 --Description: Person 테이블 생성
			   1) 표본코호트DB에는 person이 년도별로 중복 입력되어 있음. 사람들의 소득수준 변화지역이동, 설립구분의 변화등이 추적 가능함
			      하지만, CDM에서는 1개의 person이 들어가야 하므로, 최근 person 데이터를 변환함
			   2) 출생년도를 5년 간격 연령대 데이터를 이용하여 추정, 입력
 --Generating Table: PERSON
***************************************/

/**************************************
 1. 테이블 생성
***************************************/  
CREATE TABLE @ResultDatabaseSchema.PERSON (
     person_id						INTEGER		PRIMARY key , 
     gender_concept_id				INTEGER		NOT NULL , 
     year_of_birth					INTEGER		NOT NULL , 
     month_of_birth					INTEGER		NULL, 
     day_of_birth					INTEGER		NULL, 
	 time_of_birth					VARCHAR(50)	NULL,
     race_concept_id				INTEGER		NOT NULL, 
     ethnicity_concept_id			INTEGER		NOT NULL, 
     location_id					integer		NULL, 
     provider_id					INTEGER		NULL, 
     care_site_id					INTEGER		NULL, 
     person_source_value			VARCHAR(50) NULL, 
     gender_source_value			VARCHAR(50) NULL,
	 gender_source_concept_id		INTEGER		NULL, 
     race_source_value				VARCHAR(50) NULL, 
	 race_source_concept_id			INTEGER		NULL, 
     ethnicity_source_value			VARCHAR(50) NULL,
	 ethnicity_source_concept_id	INTEGER		NULL
);


/**************************************
 2. 데이터 입력
	: 5년 간격의 연령대를 이용해 출생년도를 추정해야 함.
	  총 8개의 추정 포인트에 맞춰 8개의 쿼리를 따로 실행
***************************************/  

/**
	1) 1개 이상 구간 + 5개 풀 구간 있음
*/
INSERT INTO @ResultDatabaseSchema.PERSON
	(person_id, gender_concept_id, year_of_birth, month_of_birth, day_of_birth,
	time_of_birth, race_concept_id, ethnicity_concept_id, location_id, provider_id,
	care_site_id, person_source_value, gender_source_value, gender_source_concept_id, race_source_value,
	race_source_concept_id, ethnicity_source_value, ethnicity_source_concept_id)
select 
	m.person_id as person_id,
	case when o.sex=1 then 8507
		 when o.sex=2 then 8532 end as gender_concept_id,
	m.stnd_y - ((m.age_group-1) * 5) as year_of_birth,
	null as month_of_birth,
	null as day_of_birth,
	null as time_of_birth,
	38003585 as race_concept_id, --인종
	38003564 as ethnicity_concept_id, --민족성
	o.sgg as location_id,
	null as provider_id,
	null as care_site_id,
	m.person_id as person_source_value,
	o.sex as gender_source_value,
	null as gender_source_concept_id,
	null as race_source_value,
	null as race_source_concept_id,
	null as ethnicity_source_value,
	null as ethnicity_source_concept_id
from @NHISDatabaseSchema.@NHIS_JK m, --출생년도 추정에 사용되는 person 데이터
	(select x.person_id, min(x.stnd_y) as stnd_y
	from @NHISDatabaseSchema.@NHIS_JK x, (
	select person_id, max(age_group) as age_group
	from (
		select distinct person_id, age_group
		from @NHISDatabaseSchema.@NHIS_JK
		where person_id in (
			select distinct person_id
			from (
				select person_id, age_group, count(age_group) as age_group_cnt, min(year) as min_year, max(year) as max_year
				from @NHISDatabaseSchema.@NHIS_JK
				group by person_id, age_group
			) a
			group by person_id
			having count(person_id)>1
		)
		group by person_id, age_group
		having count(age_group) = 5
	) b
	group by person_id) y
	where x.person_id=y.person_id
	and x.age_group=y.age_group
	group by x.person_id, y.person_id, x.age_group, y.age_group) n, --추정포인트 조건에 맞는 person 목록 추출
	(select w.person_id, w.stnd_y, q.sex, q.sgg
	from @NHISDatabaseSchema.@NHIS_JK q, (
		select person_id, max(stnd_y) as stnd_y
		from @NHISDatabaseSchema.@NHIS_JK
		group by person_id) w
	where q.person_id=w.person_id
	and q.stnd_y=w.stnd_y) o --최신 지역 데이터를 가져오기 위해 조인
where m.person_id=n.PERSON_ID
and m.stnd_y=n.stnd_y
and m.person_id=o.person_id

/**
	2) 1개 이상 구간 + 5개 풀 구간 없음 + 0구간 포함
		: 자격 테이블 전체에 0구간이 2개 이상인 사람이 12명 있음. 이에 0구간 중 min(stnd_y)를 기준으로 출생년도를 정함
*/
INSERT INTO @ResultDatabaseSchema.PERSON
	(person_id, gender_concept_id, year_of_birth, month_of_birth, day_of_birth,
	time_of_birth, race_concept_id, ethnicity_concept_id, location_id, provider_id,
	care_site_id, person_source_value, gender_source_value, gender_source_concept_id, race_source_value,
	race_source_concept_id, ethnicity_source_value, ethnicity_source_concept_id)
select 
	m.person_id as person_id,
	case when o.sex=1 then 8507
		 when o.sex=2 then 8532 end as gender_concept_id,
	m.stnd_y as year_of_birth,
	null as month_of_birth,
	null as day_of_birth,
	null as time_of_birth,
	38003585 as race_concept_id, --인종
	38003564 as ethnicity_concept_id, --민족성
	o.sgg as location_id,
	null as provider_id,
	null as care_site_id,
	m.person_id as person_source_value,
	o.sex as gender_source_value,
	null as gender_source_concept_id,
	null as race_source_value,
	null as race_source_concept_id,
	null as ethnicity_source_value,
	null as ethnicity_source_concept_id
from @NHISDatabaseSchema.@NHIS_JK m, --출생년도 추정에 사용되는 person 데이터
	(select x.person_id, min(x.stnd_y) as stnd_y
	from @NHISDatabaseSchema.@NHIS_JK x, (
		select distinct person_id
		from @NHISDatabaseSchema.@NHIS_JK
		where age_group=0
		and person_id in (
		select person_id
		from (
		select person_id, age_group, count(age_group) as age_group_cnt
		from @NHISDatabaseSchema.@NHIS_JK
		where person_id in (
			select distinct person_id
			from (
				select distinct person_id
				from (
					select person_id, age_group, count(age_group) as age_group_cnt, min(year) as min_year, max(year) as max_year
					from @NHISDatabaseSchema.@NHIS_JK
					group by person_id, age_group
				) a
				group by person_id
				having count(person_id)>1
			) b
			where b.person_id not in (
				select person_id 
				from @NHISDatabaseSchema.@NHIS_JK
				where person_id =b.person_id
				group by person_id, age_group
				having count(age_group) = 5
			) 
		)
		group by person_id, age_group
		) x
		group by x.person_id
		having max(x.age_group_cnt) < 5
		) ) y
	where x.person_id=y.person_id
	and x.age_group=0
	group by x.person_id) n, --추정포인트 조건에 맞는 person 목록 추출
	(select w.person_id, w.stnd_y, q.sex, q.sgg
	from @NHISDatabaseSchema.@NHIS_JK q, (
		select person_id, max(stnd_y) as stnd_y
		from @NHISDatabaseSchema.@NHIS_JK
		group by person_id) w
	where q.person_id=w.person_id
	and q.stnd_y=w.stnd_y) o --최신 지역 데이터를 가져오기 위해 조인
where m.person_id=n.person_id
and m.stnd_y=n.stnd_y
and m.person_id=o.person_id


/**
	3-1) 1개 이상 구간 + 5개 풀 구간 없음 + 0구간 비포함 + 구간 변경 시점에 년도가 연속
	: 총 76,594 건
*/
-- 연속 구간 데이터
INSERT INTO @ResultDatabaseSchema.PERSON
	(person_id, gender_concept_id, year_of_birth, month_of_birth, day_of_birth,
	time_of_birth, race_concept_id, ethnicity_concept_id, location_id, provider_id,
	care_site_id, person_source_value, gender_source_value, gender_source_concept_id, race_source_value,
	race_source_concept_id, ethnicity_source_value, ethnicity_source_concept_id)
select 
	d1.person_id as person_id,
	case when d3.sex=1 then 8507
		 when d3.sex=2 then 8532 end as gender_concept_id,
	d1.stnd_y - ((d1.age_group-1) * 5) as year_of_birth,
	null as month_of_birth,
	null as day_of_birth,
	null as time_of_birth,
	38003585 as race_concept_id, --인종
	38003564 as ethnicity_concept_id, --민족성
	d3.sgg as location_id,
	null as provider_id,
	null as care_site_id,
	d1.person_id as person_source_value,
	d3.sex as gender_source_value,
	null as gender_source_concept_id,
	null as race_source_value,
	null as race_source_concept_id,
	null as ethnicity_source_value,
	null as ethnicity_source_concept_id
from @NHISDatabaseSchema.@NHIS_JK d1, --출생년도 추정에 사용되는 person 데이터
(select x.person_id, min(y.min_stnd_y) as stnd_y
from 

(
select distinct m.person_id, m.age_group, min(m.stnd_y) as min_stnd_y, max(m.stnd_y) as max_stnd_y
from @NHISDatabaseSchema.@NHIS_JK m, 
(select distinct person_id, min_age_group
from (
	select person_id, min(age_group) as min_age_group
	from (
	select person_id, age_group, count(age_group) as age_group_cnt
	from @NHISDatabaseSchema.@NHIS_JK
	where person_id in (
		select distinct person_id
		from (
			select distinct person_id
			from (
				select person_id, age_group, count(age_group) as age_group_cnt, min(year) as min_year, max(year) as max_year
				from @NHISDatabaseSchema.@NHIS_JK
				group by person_id, age_group
			) a
			group by person_id
			having count(person_id)>1
		) b
		where b.person_id not in (
			select person_id 
			from @NHISDatabaseSchema.@NHIS_JK
			where person_id =b.person_id
			group by person_id, age_group
			having count(age_group) = 5
		) 
	)
	group by person_id, age_group
	) x
	group by x.person_id
	having max(x.age_group_cnt) < 5
) y
where y.person_id not in (
select distinct person_id
from @NHISDatabaseSchema.@NHIS_JK
where person_id=y.person_id
and age_group=0)) n
where m.person_id=n.person_id
group by m.person_id, m.age_group
) x,

(
select distinct m.person_id, m.age_group, min(m.stnd_y) as min_stnd_y, max(m.stnd_y) as max_stnd_y
from @NHISDatabaseSchema.@NHIS_JK m, 
(select distinct person_id, min_age_group
from (
	select person_id, min(age_group) as min_age_group
	from (
	select person_id, age_group, count(age_group) as age_group_cnt
	from @NHISDatabaseSchema.@NHIS_JK
	where person_id in (
		select distinct person_id
		from (
			select distinct person_id
			from (
				select person_id, age_group, count(age_group) as age_group_cnt, min(year) as min_year, max(year) as max_year
				from @NHISDatabaseSchema.@NHIS_JK
				group by person_id, age_group
			) a
			group by person_id
			having count(person_id)>1
		) b
		where b.person_id not in (
			select person_id 
			from @NHISDatabaseSchema.@NHIS_JK
			where person_id =b.person_id
			group by person_id, age_group
			having count(age_group) = 5
		) 
	)
	group by person_id, age_group
	) x
	group by x.person_id
	having max(x.age_group_cnt) < 5
) y
where y.person_id not in (
select distinct person_id
from @NHISDatabaseSchema.@NHIS_JK
where person_id=y.person_id
and age_group=0)) n
where m.person_id=n.person_id
group by m.person_id, m.age_group
) y

where x.person_id=y.person_id
and x.age_group + 1=y.age_group
and x.max_stnd_y + 1=y.min_stnd_y

group by x.person_id) d2, --추정포인트 조건에 맞는 person 목록 추출
	(select w.person_id, w.stnd_y, q.sex, q.sgg
	from @NHISDatabaseSchema.@NHIS_JK q, (
		select person_id, max(stnd_y) as stnd_y
		from @NHISDatabaseSchema.@NHIS_JK
		group by person_id) w
	where q.person_id=w.person_id
	and q.stnd_y=w.stnd_y) d3 --최신 지역 데이터를 가져오기 위해 조인
where d1.person_id=d2.person_id
and d1.stnd_y=d2.stnd_y
and d1.person_id=d3.person_id


/**
	3-2) 1개 이상 구간 + 5개 풀 구간 없음 + 0구간 비포함 + 구간 변경 시점에 년도가 비연속
	: 새 구간 시작년도에 구간대가 시작된 것으로 추정함
	: 총 2,862 건
*/
-- 연속 구간 데이터
INSERT INTO @ResultDatabaseSchema.PERSON
	(person_id, gender_concept_id, year_of_birth, month_of_birth, day_of_birth,
	time_of_birth, race_concept_id, ethnicity_concept_id, location_id, provider_id,
	care_site_id, person_source_value, gender_source_value, gender_source_concept_id, race_source_value,
	race_source_concept_id, ethnicity_source_value, ethnicity_source_concept_id)
select 
	d1.person_id as person_id,
	case when d3.sex=1 then 8507
		 when d3.sex=2 then 8532 end as gender_concept_id,
	d1.stnd_y - ((d1.age_group-1) * 5) as year_of_birth,
	null as month_of_birth,
	null as day_of_birth,
	null as time_of_birth,
	38003585 as race_concept_id, --인종
	38003564 as ethnicity_concept_id, --민족성
	d3.sgg as location_id,
	null as provider_id,
	null as care_site_id,
	d1.person_id as person_source_value,
	d3.sex as gender_source_value,
	null as gender_source_concept_id,
	null as race_source_value,
	null as race_source_concept_id,
	null as ethnicity_source_value,
	null as ethnicity_source_concept_id
from @NHISDatabaseSchema.@NHIS_JK d1, --출생년도 추정에 사용되는 person 데이터
	(
	select s1.person_id, s1.age_group, min(s1.stnd_y) as stnd_y
	from @NHISDatabaseSchema.@NHIS_JK s1,
	(
	select distinct person_id, max_age_group, min_age_group
	from (
	select distinct person_id, max_age_group, min_age_group
	from (
		select person_id, max(age_group) as max_age_group, min(age_group) as min_age_group
		from (
		select person_id, age_group, count(age_group) as age_group_cnt
		from @NHISDatabaseSchema.@NHIS_JK
		where person_id in (
			select distinct person_id
			from (
				select distinct person_id
				from (
					select person_id, age_group, count(age_group) as age_group_cnt, min(year) as min_year, max(year) as max_year
					from @NHISDatabaseSchema.@NHIS_JK
					group by person_id, age_group
				) a
				group by person_id
				having count(person_id)>1
			) b
			where b.person_id not in (
				select person_id 
				from @NHISDatabaseSchema.@NHIS_JK
				where person_id =b.person_id
				group by person_id, age_group
				having count(age_group) = 5
			) 
		)
		group by person_id, age_group
		) x
		group by x.person_id
		having max(x.age_group_cnt) < 5
	) y
	where y.person_id not in (
	select distinct person_id
	from @NHISDatabaseSchema.@NHIS_JK
	where person_id=y.person_id
	and age_group=0)) x
	where person_id not in (


	-- 
	select distinct x.person_id
	from 

	(
	select distinct m.person_id, m.age_group, min(m.stnd_y) as min_stnd_y, max(m.stnd_y) as max_stnd_y
	from @NHISDatabaseSchema.@NHIS_JK m, 
	(select distinct person_id, min_age_group
	from (
		select person_id, min(age_group) as min_age_group
		from (
		select person_id, age_group, count(age_group) as age_group_cnt
		from @NHISDatabaseSchema.@NHIS_JK
		where person_id in (
			select distinct person_id
			from (
				select distinct person_id
				from (
					select person_id, age_group, count(age_group) as age_group_cnt, min(year) as min_year, max(year) as max_year
					from @NHISDatabaseSchema.@NHIS_JK
					group by person_id, age_group
				) a
				group by person_id
				having count(person_id)>1
			) b
			where b.person_id not in (
				select person_id 
				from @NHISDatabaseSchema.@NHIS_JK
				where person_id =b.person_id
				group by person_id, age_group
				having count(age_group) = 5
			) 
		)
		group by person_id, age_group
		) x
		group by x.person_id
		having max(x.age_group_cnt) < 5
	) y
	where y.person_id not in (
	select distinct person_id
	from @NHISDatabaseSchema.@NHIS_JK
	where person_id=y.person_id
	and age_group=0)) n
	where m.person_id=n.person_id
	group by m.person_id, m.age_group
	) x,

	(
	select distinct m.person_id, m.age_group, min(m.stnd_y) as min_stnd_y, max(m.stnd_y) as max_stnd_y
	from @NHISDatabaseSchema.@NHIS_JK m, 
	(select distinct person_id, min_age_group
	from (
		select person_id, min(age_group) as min_age_group
		from (
		select person_id, age_group, count(age_group) as age_group_cnt
		from @NHISDatabaseSchema.@NHIS_JK
		where person_id in (
			select distinct person_id
			from (
				select distinct person_id
				from (
					select person_id, age_group, count(age_group) as age_group_cnt, min(year) as min_year, max(year) as max_year
					from @NHISDatabaseSchema.@NHIS_JK
					group by person_id, age_group
				) a
				group by person_id
				having count(person_id)>1
			) b
			where b.person_id not in (
				select person_id 
				from @NHISDatabaseSchema.@NHIS_JK
				where person_id =b.person_id
				group by person_id, age_group
				having count(age_group) = 5
			) 
		)
		group by person_id, age_group
		) x
		group by x.person_id
		having max(x.age_group_cnt) < 5
	) y
	where y.person_id not in (
	select distinct person_id
	from @NHISDatabaseSchema.@NHIS_JK
	where person_id=y.person_id
	and age_group=0)) n
	where m.person_id=n.person_id
	group by m.person_id, m.age_group
	) y

	where x.person_id=y.person_id
	and x.age_group + 1=y.age_group
	and x.max_stnd_y + 1=y.min_stnd_y
	)
	) s2
	where s1.person_id=s2.person_id
	and s1.age_group=s2.min_age_group
	group by s1.person_id, s1.age_group
	) d2, --추정포인트 조건에 맞는 person 목록 추출

	(select w.person_id, w.stnd_y, q.sex, q.sgg
	from @NHISDatabaseSchema.@NHIS_JK q, (
		select person_id, max(stnd_y) as stnd_y
		from @NHISDatabaseSchema.@NHIS_JK
		group by person_id) w
	where q.person_id=w.person_id
	and q.stnd_y=w.stnd_y) d3 --최신 지역 데이터를 가져오기 위해 조인

where d1.person_id=d2.person_id
and d1.stnd_y=d2.stnd_y
and d1.person_id=d3.person_id



/**
	4) 1개 이상 구간 + 5개 풀 구간 없음 + 맥스 구간 데이터 건수가 5개보다 많음
		: 맥스 구간이 최고령 구간대가 아닌 데이터가 236건
		: 동일하게 맥스 구간의 min(stnd_y)를 기준으로 출생년도 추정
*/
INSERT INTO @ResultDatabaseSchema.PERSON
	(person_id, gender_concept_id, year_of_birth, month_of_birth, day_of_birth,
	time_of_birth, race_concept_id, ethnicity_concept_id, location_id, provider_id,
	care_site_id, person_source_value, gender_source_value, gender_source_concept_id, race_source_value,
	race_source_concept_id, ethnicity_source_value, ethnicity_source_concept_id)
select 
	m.person_id as person_id,
	case when o.sex=1 then 8507
		 when o.sex=2 then 8532 end as gender_concept_id,
	m.stnd_y - ((m.age_group-1) * 5) as year_of_birth,
	null as month_of_birth,
	null as day_of_birth,
	null as time_of_birth,
	38003585 as race_concept_id, --인종
	38003564 as ethnicity_concept_id, --민족성
	o.sgg as location_id,
	null as provider_id,
	null as care_site_id,
	m.person_id as person_source_value,
	o.sex as gender_source_value,
	null as gender_source_concept_id,
	null as race_source_value,
	null as race_source_concept_id,
	null as ethnicity_source_value,
	null as ethnicity_source_concept_id
from @NHISDatabaseSchema.@NHIS_JK m, --출생년도 추정에 사용되는 person 데이터
	(select x.person_id, min(stnd_y) as stnd_y
	from @NHISDatabaseSchema.@NHIS_JK x, (
		select distinct person_id, age_group
		from (
		select person_id, age_group, count(age_group) as age_group_cnt
		from @NHISDatabaseSchema.@NHIS_JK
		where person_id in (
			select distinct person_id
			from (
				select distinct person_id
				from (
					select person_id, age_group, count(age_group) as age_group_cnt, min(year) as min_year, max(year) as max_year
					from @NHISDatabaseSchema.@NHIS_JK
					group by person_id, age_group
				) a
				group by person_id
				having count(person_id)>1
			) b
			where b.person_id not in (
				select person_id 
				from @NHISDatabaseSchema.@NHIS_JK
				where person_id =b.person_id
				group by person_id, age_group
				having count(age_group) = 5
			) 
		)
		group by person_id, age_group
		) x
		group by x.person_id, age_group
		having max(x.age_group_cnt) > 5
	) y
	where x.PERSON_ID=y.PERSON_ID
	and x.age_group=y.age_group
	group by x.person_id, x.age_group
	) n, --추정포인트 조건에 맞는 person 목록 추출
	(select w.person_id, w.stnd_y, q.sex, q.sgg
	from @NHISDatabaseSchema.@NHIS_JK q, (
		select person_id, max(stnd_y) as stnd_y
		from @NHISDatabaseSchema.@NHIS_JK
		group by person_id) w
	where q.person_id=w.person_id
	and q.stnd_y=w.stnd_y) o --최신 지역 데이터를 가져오기 위해 조인
where m.person_id=n.person_id
and m.stnd_y=n.stnd_y
and m.person_id=o.person_id


/**
	5) 1개 구간 + 5개 풀 구간임
	: 2002년에 최고령 구간에 포함되어 5년째 사망한 사람 데이터 있음. 정확한 추정 불가능
*/
INSERT INTO @ResultDatabaseSchema.PERSON
	(person_id, gender_concept_id, year_of_birth, month_of_birth, day_of_birth,
	time_of_birth, race_concept_id, ethnicity_concept_id, location_id, provider_id,
	care_site_id, person_source_value, gender_source_value, gender_source_concept_id, race_source_value,
	race_source_concept_id, ethnicity_source_value, ethnicity_source_concept_id)
select 
	m.person_id as person_id,
	case when o.sex=1 then 8507
		 when o.sex=2 then 8532 end as gender_concept_id,
	m.stnd_y - ((m.age_group-1) * 5) as year_of_birth,
	null as month_of_birth,
	null as day_of_birth,
	null as time_of_birth,
	38003585 as race_concept_id, --인종
	38003564 as ethnicity_concept_id, --민족성
	o.sgg as location_id,
	null as provider_id,
	null as care_site_id,
	m.person_id as person_source_value,
	o.sex as gender_source_value,
	null as gender_source_concept_id,
	null as race_source_value,
	null as race_source_concept_id,
	null as ethnicity_source_value,
	null as ethnicity_source_concept_id
from @NHISDatabaseSchema.@NHIS_JK m, --출생년도 추정에 사용되는 person 데이터
(select person_id, age_group, min(stnd_y) as stnd_y
from @NHISDatabaseSchema.@NHIS_JK
where person_id in (
	select distinct person_id
	from (
		select person_id, age_group, count(age_group) as age_group_cnt, min(year) as min_year, max(year) as max_year
		from @NHISDatabaseSchema.@NHIS_JK
		group by person_id, age_group
	) a
	group by person_id
	having count(person_id)=1
)
group by person_id, age_group
having count(age_group) = 5) n, --추정포인트 조건에 맞는 person 목록 추출
(select w.person_id, w.stnd_y, q.sex, q.sgg
	from @NHISDatabaseSchema.@NHIS_JK q, (
		select person_id, max(stnd_y) as stnd_y
		from @NHISDatabaseSchema.@NHIS_JK
		group by person_id) w
	where q.person_id=w.person_id
	and q.stnd_y=w.stnd_y) o --최신 지역 데이터를 가져오기 위해 조인
where m.person_id=n.person_id
and m.stnd_y=n.stnd_y
and m.person_id=o.person_id


/**
	6) 1개 구간 + 5개 풀 구간 아님 + 0구간 포함
	: 0 구간 데이터가 2개인 데이터 1건 있음
*/
INSERT INTO @ResultDatabaseSchema.PERSON
	(person_id, gender_concept_id, year_of_birth, month_of_birth, day_of_birth,
	time_of_birth, race_concept_id, ethnicity_concept_id, location_id, provider_id,
	care_site_id, person_source_value, gender_source_value, gender_source_concept_id, race_source_value,
	race_source_concept_id, ethnicity_source_value, ethnicity_source_concept_id)
select 
	m.person_id as person_id,
	case when o.sex=1 then 8507
		 when o.sex=2 then 8532 end as gender_concept_id,
	m.stnd_y as year_of_birth,
	null as month_of_birth,
	null as day_of_birth,
	null as time_of_birth,
	38003585 as race_concept_id, --인종
	38003564 as ethnicity_concept_id, --민족성
	o.sgg as location_id,
	null as provider_id,
	null as care_site_id,
	m.person_id as person_source_value,
	o.sex as gender_source_value,
	null as gender_source_concept_id,
	null as race_source_value,
	null as race_source_concept_id,
	null as ethnicity_source_value,
	null as ethnicity_source_concept_id
from @NHISDatabaseSchema.@NHIS_JK m, --출생년도 추정에 사용되는 person 데이터
	(select person_id, min(stnd_y) as stnd_y
	from @NHISDatabaseSchema.@NHIS_JK
	where age_group=0
	and person_id in (
	select person_id
	from (
	select person_id, age_group, count(age_group) as age_group_cnt
	from @NHISDatabaseSchema.@NHIS_JK
	where person_id in (
		select distinct person_id
		from (
			select distinct person_id
			from (
				select person_id, age_group, count(age_group) as age_group_cnt, min(year) as min_year, max(year) as max_year
				from @NHISDatabaseSchema.@NHIS_JK
				group by person_id, age_group
			) a
			group by person_id
			having count(person_id)=1
		) b
		where b.person_id not in (
			select person_id 
			from @NHISDatabaseSchema.@NHIS_JK
			where person_id =b.person_id
			group by person_id, age_group
			having count(age_group) = 5
		) 
	)
	group by person_id, age_group
	) x
	group by x.person_id
	having max(x.age_group_cnt) < 5
	) 
	group by person_id) n, --추정포인트 조건에 맞는 person 목록 추출
	(select w.person_id, w.stnd_y, q.sex, q.sgg
	from @NHISDatabaseSchema.@NHIS_JK q, (
		select person_id, max(stnd_y) as stnd_y
		from @NHISDatabaseSchema.@NHIS_JK
		group by person_id) w
	where q.person_id=w.person_id
	and q.stnd_y=w.stnd_y) o --최신 지역 데이터를 가져오기 위해 조인
where m.person_id=n.person_id
and m.stnd_y=n.stnd_y
and m.person_id=o.person_id


/**
	7) 1개 구간 + 5개 풀 구간 아님 + 0구간 비포함
	: 정확한 추정 불가
	: 구간 시작 년도에 구간대의 최소값을 갖도록 추정함 (예: 2002년에 20~24세 구간이면, 2002년에 22세로 추정)
*/
INSERT INTO @ResultDatabaseSchema.PERSON
	(person_id, gender_concept_id, year_of_birth, month_of_birth, day_of_birth,
	time_of_birth, race_concept_id, ethnicity_concept_id, location_id, provider_id,
	care_site_id, person_source_value, gender_source_value, gender_source_concept_id, race_source_value,
	race_source_concept_id, ethnicity_source_value, ethnicity_source_concept_id)
select 
	m.person_id as person_id,
	case when o.sex=1 then 8507
		 when o.sex=2 then 8532 end as gender_concept_id,
	m.stnd_y - ((m.age_group-1) * 5) as year_of_birth,
	null as month_of_birth,
	null as day_of_birth,
	null as time_of_birth,
	38003585 as race_concept_id, --인종
	38003564 as ethnicity_concept_id, --민족성
	o.sgg as location_id,
	null as provider_id,
	null as care_site_id,
	m.person_id as person_source_value,
	o.sex as gender_source_value,
	null as gender_source_concept_id,
	null as race_source_value,
	null as race_source_concept_id,
	null as ethnicity_source_value,
	null as ethnicity_source_concept_id
from @NHISDatabaseSchema.@NHIS_JK m, --출생년도 추정에 사용되는 person 데이터
	(select x.person_id, x.age_group, min(x.stnd_y) as stnd_y
	from @NHISDatabaseSchema.@NHIS_JK x,
	(select person_id, age_group
	from (
		select person_id, min(age_group) as age_group
		from (
		select person_id, age_group, count(age_group) as age_group_cnt
		from @NHISDatabaseSchema.@NHIS_JK
		where person_id in (												
			select distinct person_id
			from (
				select distinct person_id
				from (
					select person_id, age_group, count(age_group) as age_group_cnt, min(year) as min_year, max(year) as max_year
					from @NHISDatabaseSchema.@NHIS_JK
					group by person_id, age_group
				) a
				group by person_id
				having count(person_id)=1
			) b
			where b.person_id not in (
				select person_id 
				from @NHISDatabaseSchema.@NHIS_JK
				where person_id =b.person_id
				group by person_id, age_group
				having count(age_group) = 5
			) 
		)
		group by person_id, age_group
		) x
		group by x.person_id
		having max(x.age_group_cnt) < 5
	) y					
	where y.person_id not in (
	select distinct person_id
	from @NHISDatabaseSchema.@NHIS_JK
	where person_id=y.person_id
	and age_group=0)) y
	where x.person_id=y.person_id
	and x.age_group=y.age_group
	group by x.person_id, x.age_group) n, --추정포인트 조건에 맞는 person 목록 추출
	(select w.person_id, w.stnd_y, q.sex, q.sgg
	from @NHISDatabaseSchema.@NHIS_JK q, (
		select person_id, max(stnd_y) as stnd_y
		from @NHISDatabaseSchema.@NHIS_JK
		group by person_id) w
	where q.person_id=w.person_id
	and q.stnd_y=w.stnd_y) o --최신 지역 데이터를 가져오기 위해 조인
where m.person_id=n.person_id
and m.stnd_y=n.stnd_y
and m.person_id=o.person_id


/**
	8) 1개 구간 + 5개 풀 구간 아님 + 구간 건수가 5개보다 많음
	: 정확한 추정 불가
	: 구간 시작 년도에 구간대의 중간값을 갖도록 추정함 (예: 2002년에 20~24세 구간이면, 2002년에 22세로 추정)
*/
INSERT INTO @ResultDatabaseSchema.PERSON
	(person_id, gender_concept_id, year_of_birth, month_of_birth, day_of_birth,
	time_of_birth, race_concept_id, ethnicity_concept_id, location_id, provider_id,
	care_site_id, person_source_value, gender_source_value, gender_source_concept_id, race_source_value,
	race_source_concept_id, ethnicity_source_value, ethnicity_source_concept_id)
select 
	m.person_id as person_id,
	case when o.sex=1 then 8507
		 when o.sex=2 then 8532 end as gender_concept_id,
	m.stnd_y - ((m.age_group-1) * 5) as year_of_birth,
	null as month_of_birth,
	null as day_of_birth,
	null as time_of_birth,
	38003585 as race_concept_id, --인종
	38003564 as ethnicity_concept_id, --민족성
	o.sgg as location_id,
	null as provider_id,
	null as care_site_id,
	m.person_id as person_source_value,
	o.sex as gender_source_value,
	null as gender_source_concept_id,
	null as race_source_value,
	null as race_source_concept_id,
	null as ethnicity_source_value,
	null as ethnicity_source_concept_id
from @NHISDatabaseSchema.@NHIS_JK m, --출생년도 추정에 사용되는 person 데이터
	(select m.person_id, min(m.age_group) as age_group, min(m.stnd_y) as stnd_y
	from @NHISDatabaseSchema.@NHIS_JK m,
		(select distinct person_id
		from (
		select person_id, age_group, count(age_group) as age_group_cnt
		from @NHISDatabaseSchema.@NHIS_JK
		where person_id in (
			select distinct person_id
			from (
				select distinct person_id
				from (
					select person_id, age_group, count(age_group) as age_group_cnt, min(year) as min_year, max(year) as max_year
					from @NHISDatabaseSchema.@NHIS_JK
					group by person_id, age_group
				) a
				group by person_id
				having count(person_id)=1
			) b
			where b.person_id not in (
				select person_id 
				from @NHISDatabaseSchema.@NHIS_JK
				where person_id =b.person_id
				group by person_id, age_group
				having count(age_group) = 5
			) 
		)
		group by person_id, age_group
		) x
		group by x.person_id
		having max(x.age_group_cnt) > 5) n
	where m.person_id=n.person_id
	group by m.person_id) n, --추정포인트 조건에 맞는 person 목록 추출
	(select w.person_id, w.stnd_y, q.sex, q.sgg
	from @NHISDatabaseSchema.@NHIS_JK q, (
		select person_id, max(stnd_y) as stnd_y
		from @NHISDatabaseSchema.@NHIS_JK
		group by person_id) w
	where q.person_id=w.person_id
	and q.stnd_y=w.stnd_y) o --최신 지역 데이터를 가져오기 위해 조인
where m.person_id=n.person_id
and m.stnd_y=n.stnd_y
and m.person_id=o.person_id
