/**************************************
 --encoding : UTF-8
 --Author: SW Lee
 --Date: 2018.09.12
 
 @NHISNSC_rawdata : DB containing NHIS National Sample cohort DB
 @NHISNSC_database : DB for NHIS-NSC in CDM format
 @Mapping_database : DB for mapping table
 @NHIS_JK: JK table in NHIS NSC
 @NHIS_20T: 20 table in NHIS NSC
 @@NHIS_30T: 30 table in NHIS NSC
 @NHIS_40T: 40 table in NHIS NSC
 @@NHIS_60T: 60 table in NHIS NSC
 @NHIS_GJ: GJ table in NHIS NSC
 @CONDITION_MAPPINGTABLE : mapping table between KCD and OMOP vocabulary
 @DRUG_MAPPINGTABLE : mapping table between EDI and OMOP vocabulary
 @PROCEDURE_MAPPINGTABLE : mapping table between Korean procedure and OMOP vocabulary
 
 --Description: Create Procedure_occurrence table
			   * ETL should be performed individualy by 30T(diagnosis), 60T(prescription) table
 --Generating Table: PROCEDURE_OCCURRENCE
***************************************/

/**************************************
 1. Identify the row counts of the source tables
***************************************/ 
/*
-- Expected row counts of 30T(allow 1:N)
select count(a.key_seq)
from @NHISNSC_rawdata.@@NHIS_30T a, (select * from @NHISNSC_database.@source_to_concept_map where domain_id='procedure' and invalid_reason is null) b, @NHISNSC_rawdata.@NHIS_20T c
where a.div_cd=b.source_code
and a.key_seq=c.key_seq

-- Ref) Expected row counts of 30T (allow only the distinct vocabularies)
select count(a.key_seq)
from @NHISNSC_rawdata.@@NHIS_30T a, @NHISNSC_rawdata.@NHIS_20T b
where a.key_seq=b.key_seq
and a.div_cd in (select distinct c.source_code
	from (select * from @NHISNSC_database.@source_to_concept_map where domain_id='procedure' and invalid_reason is null) as c)
	
-- Ref) Identify the expected 1:N counts of 30T
select count(a.key_seq), sum(cnt)
from @NHISNSC_rawdata.@@NHIS_30T a, 
	(select source_code, count(source_code)-1 as cnt 
	from (select * from @NHISNSC_database.@source_to_concept_map where domain_id='procedure' and invalid_reason is null) as c
	group by source_code 
	having count(source_code) > 1) b
where a.div_cd=b.source_code

----------------------------------------
-- Expected row counts of 60T(allow 1:N)
select count(a.key_seq)
from @NHISNSC_rawdata.@@NHIS_60T a, (select * from @NHISNSC_database.@source_to_concept_map where domain_id='procedure' and invalid_reason is null) b, @NHISNSC_rawdata.@NHIS_20T c
where a.div_cd=b.source_code
and a.key_seq=c.key_seq

-- Ref) Expected row counts of 60T (allow only the distinct vocabularies)
select count(a.key_seq)
from @NHISNSC_rawdata.@@NHIS_60T a, @NHISNSC_rawdata.@NHIS_20T b
where a.key_seq=b.key_seq
and a.div_cd in (select distinct source_code
	from (select * from @NHISNSC_database.@source_to_concept_map where domain_id='procedure' and invalid_reason is null) as c)

-- Ref) Identify the expected 1:N counts of 60T
select count(a.key_seq), sum(cnt)
from @NHISNSC_rawdata.@@NHIS_60T a, 
	(select source_code, count(source_code)-1 as cnt 
	from (select * from @NHISNSC_database.@source_to_concept_map where domain_id='procedure' and invalid_reason is null) as m
	group by source_code 
	having count(source_code) > 1) b,
	@NHISNSC_rawdata.@NHIS_20T c
where a.div_cd=b.source_code
and a.key_seq=c.key_seq

*/

/**************************************
 2. Create table
***************************************/ 
/*
CREATE TABLE @NHISNSC_database.PROCEDURE_OCCURRENCE ( 
     procedure_occurrence_id		BIGINT			PRIMARY KEY, 
     person_id						INTEGER			NOT NULL, 
     procedure_concept_id			INTEGER			NOT NULL, 
     procedure_date					DATE			NOT NULL, 
     procedure_type_concept_id		INTEGER			NOT NULL,
	 modifier_concept_id			INTEGER			NULL,
	 quantity						INTEGER			NULL, 
     provider_id					INTEGER			NULL, 
     visit_occurrence_id			BIGINT			NULL, 
     procedure_source_value			VARCHAR(50)		NULL,
	 procedure_source_concept_id	INTEGER			NULL,
	 qualifier_source_value			VARCHAR(50)		NULL
    )
;
*/
/**************************************
 2-1. Using temp mapping table
***************************************/ 
IF OBJECT_ID('tempdb..#mapping_table', 'U') IS NOT NULL
	DROP TABLE #mapping_table;
IF OBJECT_ID('tempdb..#temp', 'U') IS NOT NULL
	DROP TABLE #temp;
IF OBJECT_ID('tempdb..#duplicated', 'U') IS NOT NULL
	DROP TABLE #duplicated;
IF OBJECT_ID('tempdb..#pro', 'U') IS NOT NULL
	DROP TABLE #pro;
IF OBJECT_ID('tempdb..#five', 'U') IS NOT NULL
	DROP TABLE #five;

select a.source_code, a.target_concept_id, a.domain_id, REPLACE(a.invalid_reason, '', NULL) as invalid_reason
	into #temp
from @Mapping_database.source_to_concept_map a join @Mapping_database.CONCEPT b on a.target_concept_id=b.concept_id
where a.invalid_reason='' and b.invalid_reason='' and a.domain_id='procedure';

select * into #pro from @Mapping_database.source_to_concept_map where domain_id='procedure';
select * into #five from @Mapping_database.source_to_concept_map where domain_id='device';

select a.*
	into #duplicated
from #pro a, #five b
where a.source_code=b.source_code
	and a.invalid_reason='' and b.invalid_reason='';

select * into #mapping_table from #temp
where source_code not in (select source_code from #duplicated);

drop table #pro, #five, #temp;

/**************************************
 3-1. Insert data using 30T
***************************************/
INSERT INTO @NHISNSC_database.PROCEDURE_OCCURRENCE 
	(procedure_occurrence_id, person_id, procedure_concept_id, procedure_date, procedure_type_concept_id, 
	modifier_concept_id, quantity, provider_id, visit_occurrence_id, procedure_source_value, 
	procedure_source_concept_id)
SELECT
	convert(bigint, convert(bigint, a.master_seq) * 10 + convert(bigint, row_number() over (partition by a.key_seq, a.seq_no order by b.target_concept_id))) as procedure_occurrence_id,
	a.person_id as person_id,
	CASE WHEN b.target_concept_id IS NOT NULL THEN b.target_concept_id ELSE 0 END as procedure_concept_id,
	CONVERT(VARCHAR, a.recu_fr_dt, 112) as procedure_date,
	45756900 as procedure_type_concept_id,
	NULL as modifier_concept_id,
	convert(float, a.dd_mqty_exec_freq) * convert(float, a.mdcn_exec_freq) * convert(float, a.dd_mqty_freq) as quantity,
	NULL as provider_id,
	a.key_seq as visit_occurrence_id,
	a.div_cd as procedure_source_value,
	null as procedure_source_concept_id
FROM (SELECt x.key_seq, x.seq_no, x.recu_fr_dt, x.div_cd, 
			case when x.mdcn_exec_freq is not null and isnumeric(x.mdcn_exec_freq)=1 and cast(x.mdcn_exec_freq as float) > '0' then cast(x.mdcn_exec_freq as float) else 1 end as mdcn_exec_freq,
			case when x.dd_mqty_exec_freq is not null and isnumeric(x.dd_mqty_exec_freq)=1 and cast(x.dd_mqty_exec_freq as float) > '0' then cast(x.dd_mqty_exec_freq as float) else 1 end as dd_mqty_exec_freq,
			case when x.dd_mqty_freq is not null and isnumeric(x.dd_mqty_freq)=1 and cast(x.dd_mqty_freq as float) > '0' then cast(x.dd_mqty_freq as float) else 1 end as dd_mqty_freq,
			y.master_seq, y.person_id
	FROM (select * from @NHISNSC_rawdata.@NHIS_30T where div_type_cd not in ('3','4','5', '7','8')) x, 
		 (select master_seq, key_seq, seq_no, person_id from @NHISNSC_database.SEQ_MASTER where source_table='130') y
	WHERE x.key_seq=y.key_seq
	AND x.seq_no=y.seq_no) a, #mapping_table b
WHERE left(a.div_cd,5)=b.source_code
;

/**************************************
 3-2. Insert data using 60T
***************************************/
INSERT INTO @NHISNSC_database.PROCEDURE_OCCURRENCE 
	(procedure_occurrence_id, person_id, procedure_concept_id, procedure_date, procedure_type_concept_id, 
	modifier_concept_id, quantity, provider_id, visit_occurrence_id, procedure_source_value, 
	procedure_source_concept_id)
SELECT 
	convert(bigint, convert(bigint, a.master_seq) * 10 + convert(bigint, row_number() over (partition by a.key_seq, a.seq_no order by b.target_concept_id))) as procedure_occurrence_id,
	a.person_id as person_id,
	CASE WHEN b.target_concept_id IS NOT NULL THEN b.target_concept_id ELSE 0 END as procedure_concept_id,
	CONVERT(VARCHAR, a.recu_fr_dt, 112) as procedure_date,
	45756900 as procedure_type_concept_id,
	NULL as modifier_concept_id,
	convert(float, a.dd_mqty_freq) * convert(float, a.dd_exec_freq) * convert(float, a.mdcn_exec_freq) as quantity,
	NULL as provider_id,
	a.key_seq as visit_occurrence_id,
	a.div_cd as procedure_source_value,
	null as procedure_source_concept_id
FROM (SELECt x.key_seq, x.seq_no, x.recu_fr_dt, x.div_cd, 
			case when x.mdcn_exec_freq is not null and isnumeric(x.mdcn_exec_freq)=1 and cast(x.mdcn_exec_freq as float) > '0' then cast(x.mdcn_exec_freq as float) else 1 end as mdcn_exec_freq,
			case when x.dd_exec_freq is not null and isnumeric(x.dd_exec_freq)=1 and cast(x.dd_exec_freq as float) > '0' then cast(x.dd_exec_freq as float) else 1 end as dd_exec_freq,
			case when x.dd_mqty_freq is not null and isnumeric(x.dd_mqty_freq)=1 and cast(x.dd_mqty_freq as float) > '0' then cast(x.dd_mqty_freq as float) else 1 end as dd_mqty_freq,
			y.master_seq, y.person_id
	FROM (select * from @NHISNSC_rawdata.@NHIS_60T where div_type_cd not in ('3','4','5', '7','8')) x, 
		 (select master_seq, key_seq, seq_no, person_id from @NHISNSC_database.SEQ_MASTER where source_table='160') y
	WHERE x.key_seq=y.key_seq
	AND x.seq_no=y.seq_no) a, #mapping_table b
WHERE left(a.div_cd,5)=b.source_code
;


/**************************************
 3-5. Insert data using 30T duplicated
***************************************/
INSERT INTO @NHISNSC_database.PROCEDURE_OCCURRENCE 
	(procedure_occurrence_id, person_id, procedure_concept_id, procedure_date, procedure_type_concept_id, 
	modifier_concept_id, quantity, provider_id, visit_occurrence_id, procedure_source_value, 
	procedure_source_concept_id)
SELECT
	convert(bigint, convert(bigint, a.master_seq) * 10 + convert(bigint, row_number() over (partition by a.key_seq, a.seq_no order by b.target_concept_id))) as procedure_occurrence_id,
	a.person_id as person_id,
	CASE WHEN b.target_concept_id IS NOT NULL THEN b.target_concept_id ELSE 0 END as procedure_concept_id,
	CONVERT(VARCHAR, a.recu_fr_dt, 112) as procedure_date,
	45756900 as procedure_type_concept_id,
	NULL as modifier_concept_id,
	convert(float, a.dd_mqty_exec_freq) * convert(float, a.mdcn_exec_freq) * convert(float, a.dd_mqty_freq) as quantity,
	NULL as provider_id,
	a.key_seq as visit_occurrence_id,
	a.div_cd as procedure_source_value,
	null as procedure_source_concept_id
FROM (SELECt x.key_seq, x.seq_no, x.recu_fr_dt, x.div_cd, 
			case when x.mdcn_exec_freq is not null and isnumeric(x.mdcn_exec_freq)=1 and cast(x.mdcn_exec_freq as float) > '0' then cast(x.mdcn_exec_freq as float) else 1 end as mdcn_exec_freq,
			case when x.dd_mqty_exec_freq is not null and isnumeric(x.dd_mqty_exec_freq)=1 and cast(x.dd_mqty_exec_freq as float) > '0' then cast(x.dd_mqty_exec_freq as float) else 1 end as dd_mqty_exec_freq,
			case when x.dd_mqty_freq is not null and isnumeric(x.dd_mqty_freq)=1 and cast(x.dd_mqty_freq as float) > '0' then cast(x.dd_mqty_freq as float) else 1 end as dd_mqty_freq,
			y.master_seq, y.person_id
	FROM (select * from @NHISNSC_rawdata.@NHIS_30T where div_type_cd in ('1','2')) x, 
		 (select master_seq, key_seq, seq_no, person_id from @NHISNSC_database.SEQ_MASTER where source_table='130') y
	WHERE x.key_seq=y.key_seq
	AND x.seq_no=y.seq_no) a, #duplicated b
WHERE left(a.div_cd,5)=b.source_code
;

/**************************************
 3-6. Insert data using 60T duplicated
***************************************/
INSERT INTO @NHISNSC_database.PROCEDURE_OCCURRENCE 
	(procedure_occurrence_id, person_id, procedure_concept_id, procedure_date, procedure_type_concept_id, 
	modifier_concept_id, quantity, provider_id, visit_occurrence_id, procedure_source_value, 
	procedure_source_concept_id)
SELECT 
	convert(bigint, convert(bigint, a.master_seq) * 10 + convert(bigint, row_number() over (partition by a.key_seq, a.seq_no order by b.target_concept_id))) as procedure_occurrence_id,
	a.person_id as person_id,
	CASE WHEN b.target_concept_id IS NOT NULL THEN b.target_concept_id ELSE 0 END as procedure_concept_id,
	CONVERT(VARCHAR, a.recu_fr_dt, 112) as procedure_date,
	45756900 as procedure_type_concept_id,
	NULL as modifier_concept_id,
	convert(float, a.dd_mqty_freq) * convert(float, a.dd_exec_freq) * convert(float, a.mdcn_exec_freq) as quantity,
	NULL as provider_id,
	a.key_seq as visit_occurrence_id,
	a.div_cd as procedure_source_value,
	null as procedure_source_concept_id
FROM (SELECt x.key_seq, x.seq_no, x.recu_fr_dt, x.div_cd, 
			case when x.mdcn_exec_freq is not null and isnumeric(x.mdcn_exec_freq)=1 and cast(x.mdcn_exec_freq as float) > '0' then cast(x.mdcn_exec_freq as float) else 1 end as mdcn_exec_freq,
			case when x.dd_exec_freq is not null and isnumeric(x.dd_exec_freq)=1 and cast(x.dd_exec_freq as float) > '0' then cast(x.dd_exec_freq as float) else 1 end as dd_exec_freq,
			case when x.dd_mqty_freq is not null and isnumeric(x.dd_mqty_freq)=1 and cast(x.dd_mqty_freq as float) > '0' then cast(x.dd_mqty_freq as float) else 1 end as dd_mqty_freq,
			y.master_seq, y.person_id
	FROM (select * from @NHISNSC_rawdata.@NHIS_60T where div_type_cd in ('1', '2')) x, 
		 (select master_seq, key_seq, seq_no, person_id from @NHISNSC_database.SEQ_MASTER where source_table='160') y
	WHERE x.key_seq=y.key_seq
	AND x.seq_no=y.seq_no) a, #duplicated b
WHERE left(a.div_cd,5)=b.source_code
;


/**************************************
 3-3. Insert 30T data which are unmapped with temp mapping table 
***************************************/
INSERT INTO @NHISNSC_database.PROCEDURE_OCCURRENCE 
	(procedure_occurrence_id, person_id, procedure_concept_id, procedure_date, procedure_type_concept_id, 
	modifier_concept_id, quantity, provider_id, visit_occurrence_id, procedure_source_value, 
	procedure_source_concept_id)
SELECT
	convert(bigint, convert(bigint, a.master_seq)*10 + convert(bigint, row_number() over (partition by a.key_seq, a.seq_no order by a.div_cd))) as procedure_occurrence_id,
	a.person_id as person_id,
	'0' as procedure_concept_id,
	CONVERT(VARCHAR, a.recu_fr_dt, 112) as procedure_date,
	45756900 as procedure_type_concept_id,
	NULL as modifier_concept_id,
	convert(float, a.dd_mqty_exec_freq) * convert(float, a.mdcn_exec_freq) * convert(float, a.dd_mqty_freq) as quantity,
	NULL as provider_id,
	a.key_seq as visit_occurrence_id,
	a.div_cd as procedure_source_value,
	null as procedure_source_concept_id
FROM (SELECt x.key_seq, x.seq_no, x.recu_fr_dt, x.div_cd, 
			case when x.mdcn_exec_freq is not null and isnumeric(x.mdcn_exec_freq)=1 and cast(x.mdcn_exec_freq as float) > '0' then cast(x.mdcn_exec_freq as float) else 1 end as mdcn_exec_freq,
			case when x.dd_mqty_exec_freq is not null and isnumeric(x.dd_mqty_exec_freq)=1 and cast(x.dd_mqty_exec_freq as float) > '0' then cast(x.dd_mqty_exec_freq as float) else 1 end as dd_mqty_exec_freq,
			case when x.dd_mqty_freq is not null and isnumeric(x.dd_mqty_freq)=1 and cast(x.dd_mqty_freq as float) > '0' then cast(x.dd_mqty_freq as float) else 1 end as dd_mqty_freq,
			y.master_seq, y.person_id
	FROM (select * from @NHISNSC_rawdata.@NHIS_30T where div_type_cd in ('1', '2')) x, 
		 (select master_seq, key_seq, seq_no, person_id from @NHISNSC_database.SEQ_MASTER where source_table='130') y
	WHERE x.key_seq=y.key_seq
	AND x.seq_no=y.seq_no) a 
WHERE left(a.div_cd,5) not in (select source_code from #duplicated union all select source_code from #mapping_table )
;

/**************************************
 3-4. Insert 60T data which are unmapped with temp mapping table 
***************************************/
INSERT INTO @NHISNSC_database.PROCEDURE_OCCURRENCE 
	(procedure_occurrence_id, person_id, procedure_concept_id, procedure_date, procedure_type_concept_id, 
	modifier_concept_id, quantity, provider_id, visit_occurrence_id, procedure_source_value, 
	procedure_source_concept_id)
SELECT 
	convert(bigint, convert(bigint, a.master_seq)*10 + convert(bigint, row_number() over (partition by a.key_seq, a.seq_no order by a.div_cd))) as procedure_occurrence_id,
	a.person_id as person_id,
	'0' as procedure_concept_id,
	CONVERT(VARCHAR, a.recu_fr_dt, 112) as procedure_date,
	45756900 as procedure_type_concept_id,
	NULL as modifier_concept_id,
	convert(float, a.dd_mqty_freq) * convert(float, a.dd_exec_freq) * convert(float, a.mdcn_exec_freq) as quantity,
	NULL as provider_id,
	a.key_seq as visit_occurrence_id,
	a.div_cd as procedure_source_value,
	null as procedure_source_concept_id
FROM (SELECt x.key_seq, x.seq_no, x.recu_fr_dt, x.div_cd, 
			case when x.mdcn_exec_freq is not null and isnumeric(x.mdcn_exec_freq)=1 and cast(x.mdcn_exec_freq as float) > '0' then cast(x.mdcn_exec_freq as float) else 1 end as mdcn_exec_freq,
			case when x.dd_exec_freq is not null and isnumeric(x.dd_exec_freq)=1 and cast(x.dd_exec_freq as float) > '0' then cast(x.dd_exec_freq as float) else 1 end as dd_exec_freq,
			case when x.dd_mqty_freq is not null and isnumeric(x.dd_mqty_freq)=1 and cast(x.dd_mqty_freq as float) > '0' then cast(x.dd_mqty_freq as float) else 1 end as dd_mqty_freq,
			y.master_seq, y.person_id
	FROM (select * from @NHISNSC_rawdata.@NHIS_60T where div_type_cd in ('1', '2')) x, 
		 (select master_seq, key_seq, seq_no, person_id from @NHISNSC_database.SEQ_MASTER where source_table='160') y
	WHERE x.key_seq=y.key_seq
	AND x.seq_no=y.seq_no) a 
WHERE left(a.div_cd,5) not in (select source_code from #duplicated union all select source_code from #mapping_table)
;

drop table #mapping_table, #duplicated;

-- Delete duplicated keys
delete from @NHISNSC_database.procedure_occurrence
where procedure_occurrence_id in (select drug_exposure_id from @NHISNSC_database.drug_exposure)


