/**************************************
 --encoding : UTF-8
 --Author: SW Lee, JM Park
 --Date: 2018.09.20
 
 @NHISNSC_rawdata : DB containing NHIS National Sample cohort DB
 @NHISNSC_database : DB for NHIS-NSC in CDM format
 @Mapping_database : DB for mapping table
 @NHIS_JK: JK table in NHIS NSC
 @NHIS_20T: 20 table in NHIS NSC
 @NHIS_30T: 30 table in NHIS NSC
 @NHIS_40T: 40 table in NHIS NSC
 @NHIS_60T: 60 table in NHIS NSC
 @NHIS_GJ: GJ table in NHIS NSC
 @CONDITION_MAPPINGTABLE : mapping table between KCD and OMOP vocabulary
 @DRUG_MAPPINGTABLE : mapping table between EDI and OMOP vocabulary
 @PROCEDURE_MAPPINGTABLE : mapping table between Korean procedure and OMOP vocabulary
 @DEVICE_MAPPINGTABLE : mapping table between EDI and OMOP vocabulary
 
 --Description: Create Cost table
 --Generating Table: COST
***************************************/

/**************************************
 1. Create table
***************************************/ 
/*
CREATE TABLE @NHISNSC_database.COST (
	cost_id	bigint	primary key,
	cost_event_id	bigint	not null,
	cost_domain_id	varchar(20)	not null,
	cost_type_concept_id	integer	not null,
	currency_concept_id	integer,
	total_charge	float,
	total_cost	float,
	total_paid	float,
	paid_by_payer	float,
	paid_by_patient	float,
	paid_patient_copay	float,
	paid_patient_coinsurance	float,
	paid_patient_deductiable	float,
	paid_by_primary	float,
	paid_ingredient_cost	float,
	paid_dispensing_fee	float,
	payer_plan_period_id	bigint,
	amount_allowed	float,
	revenue_code_concept_id	integer,
	drg_concept_id	integer,
	revenue_code_source_value	varchar(50),
	drg_source_value	varchar(50)
);
*/
/**************************************
 1-1. Using temp mapping table
***************************************/ 
IF OBJECT_ID('tempdb..#mapping_table', 'U') IS NOT NULL
	DROP TABLE #mapping_table;
select a.source_code, a.target_concept_id, a.domain_id, REPLACE(a.invalid_reason, '', NULL) as invalid_reason 
into #mapping_table
from @Mapping_database.source_to_concept_map a join @Mapping_database.CONCEPT b on a.target_concept_id=b.concept_id
where a.invalid_reason='' and b.invalid_reason='';


/**************************************
 2. Insert data
    1) Visit
	2) Drug
	3) Procedure
	4) Device
***************************************/ 
---------------------------------------------------
-- 1) Visit
---------------------------------------------------

INSERT INTO @NHISNSC_database.COST
	(cost_id, cost_event_id, cost_domain_id, cost_type_concept_id, currency_concept_id,
	total_charge, total_cost, total_paid, paid_by_payer, paid_by_patient,
	paid_patient_copay, paid_patient_coinsurance, paid_patient_deductible, paid_by_primary, paid_ingredient_cost,
	paid_dispensing_fee, payer_plan_period_id, amount_allowed, revenue_code_concept_id, drg_concept_id,
	revenue_code_source_value, drg_source_value)
SELECT 
	a.visit_occurrence_id as cost_id,
	a.visit_occurrence_id as cost_event_id,
	'Visit' as cost_domain_id,
	5031 as cost_type_concept_id,
	44818598 as currency_concept_id,
	b.dmd_tramt as total_charge,
	null as total_cost,
	b.edec_tramt as total_paid,
	b.edec_jbrdn_amt as paid_by_payer,
	b.edec_sbrdn_amt as paid_by_patient,
	null as paid_patient_copay,
	null as paid_patient_coinsurance, 
	null as paid_patient_deductible,
	null as paid_by_primary,
	null as paid_ingredient_cost,
	null as paid_dispensing_fee,
	convert(bigint, convert(varchar, a.person_id) + left(convert(varchar, visit_start_date, 112), 4)) as payer_plan_period_id,
	null as amount_allowed,
	null as revenue_code_concept_id,
	null as drg_concept_id,
	null as revenue_code_source_value,
	b.dmd_drg_no as drg_source_value
from @NHISNSC_database.VISIT_OCCURRENCE a, @NHISNSC_rawdata.@NHIS_20T b
where a.visit_occurrence_id=b.key_seq
and a.person_id=b.person_id;


---------------------------------------------------
-- 2) Drug
---------------------------------------------------
-- Check the duplicated IDs from Drug and Device tables
/*
select * from #mapping_table
where source_code in (
					select drug_source_value from @NHISNSC_database.DRUG_EXPOSURE a, @NHISNSC_database.DEVICE_EXPOSURE b
					where a.drug_exposure_id=b.device_exposure_id and a.person_id=b.person_id
					)
order by source_code
*/

-- Delete duplicated ID keys from Drug_era table
delete from @NHISNSC_database.DRUG_EXPOSURE
where drug_source_value in (select source_code from #mapping_table
							where domain_id='drug' and source_code in (
												select drug_source_value from @NHISNSC_database.DRUG_EXPOSURE a, @NHISNSC_database.DEVICE_EXPOSURE b
												where a.drug_exposure_id=b.device_exposure_id 
													and a.person_id=b.person_id 
											)
								)

-- Delete duplicated ID keys from temp mapping table
delete from #mapping_table where domain_id='drug' and source_code in (
												select drug_source_value from @NHISNSC_database.DRUG_EXPOSURE a, @NHISNSC_database.DEVICE_EXPOSURE b
												where a.drug_exposure_id=b.device_exposure_id 
													and a.person_id=b.person_id )

--Insert data
-- Source data: 30T
INSERT INTO @NHISNSC_database.COST
	(cost_id, cost_event_id, cost_domain_id, cost_type_concept_id, currency_concept_id,
	total_charge, total_cost, total_paid, paid_by_payer, paid_by_patient,
	paid_patient_copay, paid_patient_coinsurance, paid_patient_deductible, paid_by_primary, paid_ingredient_cost,
	paid_dispensing_fee, payer_plan_period_id, amount_allowed, revenue_code_concept_id, drg_concept_id,
	revenue_code_source_value, drg_source_value)
SELECT 
	a.drug_exposure_id as cost_id,
	a.drug_exposure_id as cost_event_id,
	'Drug' as cost_domain_id,
	5031 as cost_type_concept_id,
	44818598 as currency_concept_id,
	null as total_charge,
	b.amt as total_cost,
	null as total_paid,
	null as paid_by_payer,
	null as paid_by_patient,
	null as paid_patient_copay,
	null as paid_patient_coinsurance, 
	null as paid_patient_deductible,
	null as paid_by_primary,
	null as paid_ingredient_cost,
	null as paid_dispensing_fee,
	convert(bigint, convert(varchar, b.person_id) + left(convert(varchar, a.drug_exposure_start_date, 112), 4)) as payer_plan_period_id,
	null as amount_allowed,
	null as revenue_code_concept_id,
	null as drg_concept_id,
	null as revenue_code_source_value,
	null as drg_source_value
from (select person_id, drug_exposure_id, drug_exposure_start_date
	from @NHISNSC_database.DRUG_EXPOSURE
	where drug_type_concept_id=38000180) a, 
	(select m.master_seq, m.key_seq, m.seq_no, m.person_id, n.amt
	from @NHISNSC_database.SEQ_MASTER m, @NHISNSC_rawdata.@NHIS_30T n
	where m.source_table='130'
		and m.key_seq=n.key_seq
		and m.seq_no=n.seq_no) b
where left(a.drug_exposure_id, 10)=b.master_seq
and a.person_id=b.person_id;


-- Sourec data: 60T
INSERT INTO @NHISNSC_database.COST
	(cost_id, cost_event_id, cost_domain_id, cost_type_concept_id, currency_concept_id,
	total_charge, total_cost, total_paid, paid_by_payer, paid_by_patient,
	paid_patient_copay, paid_patient_coinsurance, paid_patient_deductible, paid_by_primary, paid_ingredient_cost,
	paid_dispensing_fee, payer_plan_period_id, amount_allowed, revenue_code_concept_id, drg_concept_id,
	revenue_code_source_value, drg_source_value)
SELECT 
	a.drug_exposure_id as cost_id,
	a.drug_exposure_id as cost_event_id,
	'Drug' as cost_domain_id,
	5031 as cost_type_concept_id,
	44818598 as currency_concept_id,
	null as total_charge,
	b.amt as total_cost,
	null as total_paid,
	null as paid_by_payer,
	null as paid_by_patient,
	null as paid_patient_copay,
	null as paid_patient_coinsurance, 
	null as paid_patient_deductible,
	null as paid_by_primary,
	null as paid_ingredient_cost,
	null as paid_dispensing_fee,
	convert(bigint, convert(varchar, b.person_id) + left(convert(varchar, a.drug_exposure_start_date, 112), 4)) as payer_plan_period_id,
	null as amount_allowed,
	null as revenue_code_concept_id,
	null as drg_concept_id,
	null as revenue_code_source_value,
	null as drg_source_value
from (select person_id, drug_exposure_id, drug_exposure_start_date
	from @NHISNSC_database.DRUG_EXPOSURE
	where drug_type_concept_id=38000177) a, 
	(select m.master_seq, m.key_seq, m.seq_no, m.person_id, n.amt
	from (select master_seq, key_seq, seq_no, person_id from @NHISNSC_database.SEQ_MASTER where source_table='160') m, 
	@NHISNSC_rawdata.@NHIS_60T n
	where m.key_seq=n.key_seq
	and m.seq_no=n.seq_no) b
where b.master_seq=left(a.drug_exposure_id, 10)
and a.person_id=b.person_id;


---------------------------------------------------
-- 3) Procedure
---------------------------------------------------

-- Sourec data: 30T
INSERT INTO @NHISNSC_database.COST
	(cost_id, cost_event_id, cost_domain_id, cost_type_concept_id, currency_concept_id,
	total_charge, total_cost, total_paid, paid_by_payer, paid_by_patient,
	paid_patient_copay, paid_patient_coinsurance, paid_patient_deductible, paid_by_primary, paid_ingredient_cost,
	paid_dispensing_fee, payer_plan_period_id, amount_allowed, revenue_code_concept_id, drg_concept_id,
	revenue_code_source_value, drg_source_value)
SELECT 
	a.procedure_occurrence_id as cost_id,
	a.procedure_occurrence_id as cost_event_id,
	'Procedure' as cost_domain_id,
	5031 as cost_type_concept_id,
	44818598 as currency_concept_id,
	null as total_charge,
	b.amt as total_cost,
	null as total_paid,
	null as paid_by_payer,
	null as paid_by_patient,
	null as paid_patient_copay,
	null as paid_patient_coinsurance, 
	null as paid_patient_deductible,
	null as paid_by_primary,
	null as paid_ingredient_cost,
	null as paid_dispensing_fee,
	convert(bigint, convert(varchar, b.person_id) + left(convert(varchar, a.procedure_date, 112), 4)) as payer_plan_period_id,
	null as amount_allowed,
	null as revenue_code_concept_id,
	null as drg_concept_id,
	null as revenue_code_source_value,
	null as drg_source_value
from @NHISNSC_database.PROCEDURE_OCCURRENCE a, 
	(select m.master_seq, m.key_seq, m.seq_no, m.person_id, n.amt
	from @NHISNSC_database.SEQ_MASTER m, @NHISNSC_rawdata.@NHIS_30T n
	where m.source_table='130'
	and m.key_seq=n.key_seq
	and m.seq_no=n.seq_no) b
where left(a.procedure_occurrence_id, 10)=b.master_seq
and a.person_id=b.person_id;


-- Sourec data: 60T
INSERT INTO @NHISNSC_database.COST
	(cost_id, cost_event_id, cost_domain_id, cost_type_concept_id, currency_concept_id,
	total_charge, total_cost, total_paid, paid_by_payer, paid_by_patient,
	paid_patient_copay, paid_patient_coinsurance, paid_patient_deductible, paid_by_primary, paid_ingredient_cost,
	paid_dispensing_fee, payer_plan_period_id, amount_allowed, revenue_code_concept_id, drg_concept_id,
	revenue_code_source_value, drg_source_value)
SELECT 
	a.procedure_occurrence_id as cost_id,
	a.procedure_occurrence_id as cost_event_id,
	'Procedure' as cost_domain_id,
	5031 as cost_type_concept_id,
	44818598 as currency_concept_id,
	null as total_charge,
	b.amt as total_cost,
	null as total_paid,
	null as paid_by_payer,
	null as paid_by_patient,
	null as paid_patient_copay,
	null as paid_patient_coinsurance, 
	null as paid_patient_deductible,
	null as paid_by_primary,
	null as paid_ingredient_cost,
	null as paid_dispensing_fee,
	convert(bigint, convert(varchar, b.person_id) + left(convert(varchar, a.procedure_date, 112), 4)) as payer_plan_period_id,
	null as amount_allowed,
	null as revenue_code_concept_id,
	null as drg_concept_id,
	null as revenue_code_source_value,
	null as drg_source_value
from @NHISNSC_database.PROCEDURE_OCCURRENCE a, 
	(select m.master_seq, m.key_seq, m.seq_no, m.person_id, n.amt
	from (select master_seq, key_seq, seq_no, person_id from @NHISNSC_database.SEQ_MASTER where source_table='160') m, 
	@NHISNSC_rawdata.@NHIS_60T n
	where m.key_seq=n.key_seq
	and m.seq_no=n.seq_no) b
where left(a.procedure_occurrence_id, 10)=b.master_seq
and a.person_id=b.person_id;


---------------------------------------------------
-- 4) Device
---------------------------------------------------
-- Sourec data: 30T
INSERT INTO @NHISNSC_database.COST
	(cost_id, cost_event_id, cost_domain_id, cost_type_concept_id, currency_concept_id,
	total_charge, total_cost, total_paid, paid_by_payer, paid_by_patient,
	paid_patient_copay, paid_patient_coinsurance, paid_patient_deductible, paid_by_primary, paid_ingredient_cost,
	paid_dispensing_fee, payer_plan_period_id, amount_allowed, revenue_code_concept_id, drg_concept_id,
	revenue_code_source_value, drg_source_value)
SELECT 
	a.device_exposure_id as cost_id,
	a.device_exposure_id as cost_event_id,
	'Device' as cost_domain_id,
	5031 as cost_type_concept_id,
	44818598 as currency_concept_id,
	null as total_charge,
	b.amt as total_cost,
	null as total_paid,
	null as paid_by_payer,
	null as paid_by_patient,
	null as paid_patient_copay,
	null as paid_patient_coinsurance, 
	null as paid_patient_deductible,
	null as paid_by_primary,
	null as paid_ingredient_cost,
	null as paid_dispensing_fee,
	convert(bigint, convert(varchar, b.person_id) + left(convert(varchar, a.device_exposure_start_date, 112), 4)) as payer_plan_period_id,
	null as amount_allowed,
	null as revenue_code_concept_id,
	null as drg_concept_id,
	null as revenue_code_source_value,
	null as drg_source_value
from (select device_exposure_id, person_id, device_exposure_start_date
	from @NHISNSC_database.DEVICE_EXPOSURE 
	where device_source_value not in (select source_code from #mapping_table where domain_id='procedure' )) a, --procedure 와 device 에 둘다 변환된 건수들 제외
	(select m.master_seq, m.key_seq, m.seq_no, m.person_id, n.amt
	from @NHISNSC_database.SEQ_MASTER m, @NHISNSC_rawdata.@NHIS_30T n
	where m.source_table='130'
	and m.key_seq=n.key_seq
	and m.seq_no=n.seq_no) b
where left(a.device_exposure_id, 10)=b.master_seq
and a.person_id=b.person_id;


-- Sourec data: 60T
INSERT INTO @NHISNSC_database.COST
	(cost_id, cost_event_id, cost_domain_id, cost_type_concept_id, currency_concept_id,
	total_charge, total_cost, total_paid, paid_by_payer, paid_by_patient,
	paid_patient_copay, paid_patient_coinsurance, paid_patient_deductible, paid_by_primary, paid_ingredient_cost,
	paid_dispensing_fee, payer_plan_period_id, amount_allowed, revenue_code_concept_id, drg_concept_id,
	revenue_code_source_value, drg_source_value)
SELECT 
	a.device_exposure_id as cost_id,
	a.device_exposure_id as cost_event_id,
	'Device' as cost_domain_id,
	5031 as cost_type_concept_id,
	44818598 as currency_concept_id,
	null as total_charge,
	b.amt as total_cost,
	null as total_paid,
	null as paid_by_payer,
	null as paid_by_patient,
	null as paid_patient_copay,
	null as paid_patient_coinsurance, 
	null as paid_patient_deductible,
	null as paid_by_primary,
	null as paid_ingredient_cost,
	null as paid_dispensing_fee,
	convert(bigint, convert(varchar, b.person_id) + left(convert(varchar, a.device_exposure_start_date, 112), 4)) as payer_plan_period_id,
	null as amount_allowed,
	null as revenue_code_concept_id,
	null as drg_concept_id,
	null as revenue_code_source_value,
	null as drg_source_value
from (select device_exposure_id, person_id, device_exposure_start_date
	from @NHISNSC_database.DEVICE_EXPOSURE 
	where device_source_value not in (select source_code from #mapping_table where domain_id='procedure' )) a,  --procedure 와 device 에 둘다 변환된 건수들 제외
	(select m.master_seq, m.key_seq, m.seq_no, m.person_id, n.amt
	from (select master_seq, key_seq, seq_no, person_id from @NHISNSC_database.SEQ_MASTER where source_table='160') m, 
	@NHISNSC_rawdata.@NHIS_60T n
	where m.key_seq=n.key_seq
	and m.seq_no=n.seq_no) b
where left(a.device_exposure_id, 10)=b.master_seq
and a.person_id=b.person_id;


drop table #mapping_table;