/**************************************
 --encoding : UTF-8
 --Author: 이성원
 --Date: 2018.09.10
 
 @NHISNSC_rawdata : DB containing NHIS National Sample cohort DB
 @NHISNSC_database : DB for NHIS-NSC in CDM format
 @NHIS_JK: JK table in NHIS NSC
 @NHIS_20T: 20 table in NHIS NSC
 @NHIS_30T: 30 table in NHIS NSC
 @NHIS_40T: 40 table in NHIS NSC
 @NHIS_60T: 60 table in NHIS NSC
 @NHIS_GJ: GJ table in NHIS NSC
 --Description: Visit_occurrence 테이블 생성
 --Generating Table: VISIT_OCCURRENCE
***************************************/

/**************************************
 1. 테이블 생성
***************************************/ 
/*
CREATE TABLE @NHISNSC_database.VISIT_OCCURRENCE (
	visit_occurrence_id	bigint	primary key,
	person_id			integer	not null,
	visit_concept_id	integer	not null,
	visit_start_date	date	not null,
	visit_start_time	time,
	visit_end_date		date	not null,
	visit_end_time		time,
	visit_type_concept_id	integer	not null,
	provider_id			integer,
	care_site_id		integer,
	visit_source_value	varchar(50),
	visit_source_concept_id	integer
);
*/
/**************************************
 2. 데이터 입력
***************************************/ 
insert into @NHISNSC_database.VISIT_OCCURRENCE (
	visit_occurrence_id, person_id, visit_concept_id, visit_start_date, visit_start_time,
	visit_end_date, visit_end_time, visit_type_concept_id, provider_id, care_site_id,
	visit_source_value, visit_source_concept_id
)
select 
	key_seq as visit_occurrence_id,
	person_id as person_id,
	case when form_cd in ('02', '2', '04', '06', '07', '10', '12') and in_pat_cors_type in ('11', '21', '31') then 9203 --입원 + 응급
		when form_cd in ('02', '2', '04', '06', '07', '10', '12') and in_pat_cors_type not in ('11', '21', '31') then 9201 --입원 + 입원
		when form_cd in ('03', '3', '05', '08', '8', '09', '9', '11', '13', '20', '21', 'ZZ') and in_pat_cors_type in ('11', '21', '31') then 9203 --외래 + 응급
		when form_cd in ('03', '3', '05', '08', '8', '09', '9', '11', '13', '20', '21', 'ZZ') and in_pat_cors_type not in ('11', '21', '31') then 9202 --외래 + 외래
		else 0
	end as visit_concept_id,
	convert(date, recu_fr_dt, 112) as visit_start_date,
	null as visit_start_time,
	case when form_cd in ('02', '2', '04', '06', '07', '10', '12') and VSCN > 0 then DATEADD(DAY, vscn-1, convert(date, recu_fr_dt, 112))
		when form_cd in ('02', '2', '04', '06', '07', '10', '12') and VSCN = 0 then DATEADD(DAY, convert(int, vscn)  ,convert(date, recu_fr_dt, 112)) 
		when form_cd in ('03', '3', '05', '08', '8', '09', '9', '11', '13', '20', '21', 'ZZ') and in_pat_cors_type in ('11', '21', '31') and VSCN > 0 then DATEADD(DAY, vscn-1, convert(date, recu_fr_dt, 112))
		when form_cd in ('03', '3', '05', '08', '8', '09', '9', '11', '13', '20', '21', 'ZZ') and in_pat_cors_type in ('11', '21', '31') and VSCN = 0 then DATEADD(DAY, convert(int, vscn), convert(date, recu_fr_dt, 112))
		else convert(date, recu_fr_dt, 112)
	end as visit_end_date,
	null as visit_end_time,
	44818517 as visit_type_concept_id,
	null as provider_id,
	ykiho_id as care_site_id,
	key_seq as visit_source_value,
	null as visit_source_concept_id
from @NHISNSC_rawdata.@NHIS_20T
;

--건강검진 INSERT
insert into @NHISNSC_database.VISIT_OCCURRENCE (
	visit_occurrence_id, person_id, visit_concept_id, visit_start_date, visit_start_time,
	visit_end_date, visit_end_time, visit_type_concept_id, provider_id, care_site_id,
	visit_source_value, visit_source_concept_id
)
select 
	b.master_seq as visit_concept_id,
	a.person_id as person_id,
	9202 as visit_concept_id,
	cast(CONVERT(VARCHAR, a.hchk_year+'0101', 23)as date) as visit_start_date,
	null as visit_start_time,
	cast(CONVERT(VARCHAR, a.hchk_year+'0101', 23)as date) as visit_end_date,
	null as visit_end_time,
	44818517 as visit_type_concept_id,
	null as provider_id,
	null as care_site_id,
	b.master_seq as visit_source_value,
	null as visit_source_concept_id
from @NHISNSC_rawdata.@NHIS_GJ a JOIN @NHISNSC_database.seq_master b on a.person_id=b.person_id and a.hchk_year=b.hchk_year
;

