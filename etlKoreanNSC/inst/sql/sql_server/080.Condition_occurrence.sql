/**************************************
 --encoding : UTF-8
 --Author: SW Lee
 --Date: 2018.09.11
 
 @NHISNSC_rawdata: DB containing NHIS National Sample cohort DB
 @NHISNSC_database : DB for NHIS-NSC in CDM format
 @Mapping_database : DB for mapping table
 @NHIS_JK: JK table in NHIS NSC
 @NHIS_20T: 20 table in NHIS NSC
 @NHIS_30T: 30 table in NHIS NSC
 @NHIS_40T: 40 table in NHIS NSC
 @NHIS_60T: 60 table in NHIS NSC
 @NHIS_GJ: GJ table in NHIS NSC
 @CONDITION_MAPPINGTABLE : mapping table between KCD and SNOMED-CT
 --Description: Create Condition_occurrence table
 --Generating Table: CONDITION_OCCURRENCE
***************************************/

/**************************************
 1. Create table
***************************************/ 
/*
CREATE TABLE @NHISNSC_database.CONDITION_OCCURRENCE ( 
     condition_occurrence_id		BIGINT			PRIMARY KEY, 
     person_id						INTEGER			NOT NULL , 
     condition_concept_id			INTEGER			NOT NULL , 
     condition_start_date			DATE			NOT NULL , 
     condition_end_date				DATE, 
     condition_type_concept_id		INTEGER			NOT NULL , 
     stop_reason					VARCHAR(20), 
     provider_id					INTEGER, 
     visit_occurrence_id			BIGINT, 
     condition_source_value			VARCHAR(50),
	 condition_source_concept_id	VARCHAR(50)
);
*/

/**************************************
 1-1. Create temp mapping table
***************************************/
select a.source_code, a.target_concept_id, a.domain_id, REPLACE(a.invalid_reason, '', NULL) as invalid_reason
into #mapping_table
from @Mapping_database.source_to_concept_map a join @Mapping_database.CONCEPT b on a.target_concept_id=b.concept_id
where a.invalid_reason is null and b.invalid_reason is null and a.domain_id='condition'
;

select a.source_code, a.target_concept_id, a.domain_id, REPLACE(a.invalid_reason, '', NULL) as invalid_reason
into #mapping_table2
from @Mapping_database.source_to_concept_map a join @Mapping_database.CONCEPT b on a.target_concept_id=b.concept_id
where a.invalid_reason is null and b.invalid_reason is null
;

/**************************************
 2. Insert date
    1) start date : Qualified year + 01.01 as default. If Birth_year is before the qualified year then birth_year + 01.01
	2) end date: Qualified year + 12.31 as default. If the death year is after the qualified year then death_year.month.day
	
	Ref) 20T: 119,362,188
        40T: 299,379,698
	
	-- checklist
	   1) kcdcode full set -> SY Cho : Done
	   2) Check condition_type_concept_id value-> SC You
***************************************/ 
-- Using data only between observation_period & visiti_occurrence 
--((299,311,028), 00:50:39)
INSERT INTO @NHISNSC_database.CONDITION_OCCURRENCE
	(condition_occurrence_id, person_id, condition_concept_id, condition_start_date, condition_end_date,
	condition_type_concept_id, stop_reason, provider_id, visit_occurrence_id, condition_source_value, 
	condition_source_concept_id)
select
	convert(bigint, convert(bigint, m.master_seq) * 10 + convert(bigint, ROW_NUMBER() OVER(partition BY key_seq, seq_no order by target_concept_id desc))) as condition_occurrence_id,
	--ROW_NUMBER() OVER(partition BY key_seq, seq_no order by concept_id desc) AS rank, m.seq_no,
	m.person_id as person_id,
	n.target_concept_id as condition_concept_id,
	convert(date, m.recu_fr_dt, 112) as condition_start_date,
	m.visit_end_date as condition_end_date,
	m.sick_order as condition_type_concept_id,
	null as stop_reason,
	null as provider_id,
	m.key_seq as visit_occurrence_id,
	m.sick_sym as condition_source_value,
	null as condition_source_concept_id
from (
	select
		a.master_seq, a.person_id, a.key_seq, a.seq_no, b.recu_fr_dt,
		case when b.form_cd in ('02', '2', '04', '06', '07', '10', '12') and b.vscn > 0 then DATEADD(DAY, b.vscn-1, convert(date, b.recu_fr_dt , 112)) 
			when b.form_cd in ('02', '2', '04', '06', '07', '10', '12') and b.vscn = 0 then DATEADD(DAY, cast(b.vscn as int), convert(date, b.recu_fr_dt , 112)) 
			when b.form_cd in ('03', '3', '05', '08', '8', '09', '9', '11', '13', '20', '21', 'ZZ') and b.in_pat_cors_type in ('11', '21', '31') and vscn > 0 then DATEADD(DAY, b.vscn-1, convert(date, b.recu_fr_dt, 112)) 
			when b.form_cd in ('03', '3', '05', '08', '8', '09', '9', '11', '13', '20', '21', 'ZZ') and b.in_pat_cors_type in ('11', '21', '31') and vscn = 0 then DATEADD(DAY, cast(b.vscn as int), convert(date, b.recu_fr_dt, 112)) 
			else convert(date, b.recu_fr_dt, 112)
		end as visit_end_date,
		c.sick_sym,
		case when c.SEQ_NO=1 then '44786627'--primary condition
			when c.SEQ_NO=2 then '44786629' --secondary condition
			when c.SEQ_NO=3 then '45756845' --third condition
			when c.SEQ_NO=4 then '45756846'	-- 4th condition
			else '45756847'					-- 5th condition and etc
		end as sick_order,
		case when b.sub_sick=c.sick_sym then 'Y' else 'N' end as sub_sick_yn
	from (select master_seq, person_id, key_seq, seq_no from @NHISNSC_database.SEQ_MASTER where source_table='140') a, 
		@NHISNSC_rawdata.@NHIS_20T b, 
		@NHISNSC_rawdata.@NHIS_40T c,
		@NHISNSC_database.observation_period d --added
	where a.person_id=b.person_id
	and a.key_seq=b.key_seq
	and a.key_seq=c.key_seq
	and a.seq_no=c.seq_no
	and b.person_id=d.person_id --added
	and convert(date, c.recu_fr_dt, 112) between d.observation_period_start_date and d.observation_period_end_date) as m, --added
	#mapping_table as n
where m.sick_sym=n.source_code;


/********************************************
	2-1. Insert data which are unmapped with temp mapping table as concept_id=0
********************************************/
INSERT INTO @NHISNSC_database.CONDITION_OCCURRENCE
	(condition_occurrence_id, person_id, condition_concept_id, condition_start_date, condition_end_date,
	condition_type_concept_id, stop_reason, provider_id, visit_occurrence_id, condition_source_value, 
	condition_source_concept_id)
select
	convert(bigint, convert(bigint, m.master_seq) * 10 + convert(bigint, ROW_NUMBER() OVER(partition BY key_seq, seq_no order by m.sick_sym desc))) as condition_occurrence_id,
	m.person_id as person_id,
	0 as condition_concept_id,
	convert(date, m.recu_fr_dt, 112) as condition_start_date,
	m.visit_end_date as condition_end_date,
	m.sick_order as condition_type_concept_id,
	null as stop_reason,
	null as provider_id,
	m.key_seq as visit_occurrence_id,
	m.sick_sym as condition_source_value,
	null as condition_source_concept_id

from (
	select
		a.master_seq, a.person_id, a.key_seq, a.seq_no, b.recu_fr_dt,
		case when b.form_cd in ('02', '2', '04', '06', '07', '10', '12') and b.vscn > 0 then DATEADD(DAY, b.vscn-1, convert(date, b.recu_fr_dt , 112)) 
			when b.form_cd in ('02', '2', '04', '06', '07', '10', '12') and b.vscn = 0 then DATEADD(DAY, cast(b.vscn as int), convert(date, b.recu_fr_dt , 112)) 
			when b.form_cd in ('03', '3', '05', '08', '8', '09', '9', '11', '13', '20', '21', 'ZZ') and b.in_pat_cors_type in ('11', '21', '31') and vscn > 0 then DATEADD(DAY, b.vscn-1, convert(date, b.recu_fr_dt, 112)) 
			when b.form_cd in ('03', '3', '05', '08', '8', '09', '9', '11', '13', '20', '21', 'ZZ') and b.in_pat_cors_type in ('11', '21', '31') and vscn = 0 then DATEADD(DAY, cast(b.vscn as int), convert(date, b.recu_fr_dt, 112)) 
			else convert(date, b.recu_fr_dt, 112)
		end as visit_end_date,
		c.sick_sym,
		case when c.SEQ_NO=1 then '44786627'--primary condition
			when c.SEQ_NO=2 then '44786629' --secondary condition
			when c.SEQ_NO=3 then '45756845' --third condition
			when c.SEQ_NO=4 then '45756846'	-- 4th condition
			else '45756847'					-- 5th condition and etc
		end as sick_order,
		case when b.sub_sick=c.sick_sym then 'Y' else 'N' end as sub_sick_yn
	from (select master_seq, person_id, key_seq, seq_no from @NHISNSC_database.SEQ_MASTER where source_table='140') a, 
		@NHISNSC_rawdata.@NHIS_20T b, 
		@NHISNSC_rawdata.@NHIS_40T c,
		@NHISNSC_database.observation_period d --added
	where a.person_id=b.person_id
	and a.key_seq=b.key_seq
	and a.key_seq=c.key_seq
	and a.seq_no=c.seq_no
	and b.person_id=d.person_id --added
	and convert(date, c.recu_fr_dt, 112) between d.observation_period_start_date and d.observation_period_end_date) as m --added
where m.sick_sym not in (select source_code from #mapping_table2)
;


drop table #mapping_table;
drop table #mapping_table2;
