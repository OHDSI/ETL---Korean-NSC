/**************************************
 --encoding : UTF-8
 --Author: JH Cho
 --Date: 2018.09.12
 
 @NHISNSC_rawdata: DB containing NHIS National Sample cohort DB
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
 
 --Description: Create device_exposure table
			   1) device_exposure_end_date should be created same way as end_date of drug_exposure table
			   2) About the quantity, many cases are having abnormal cost(UN_COST, AMT), abnormal usages(DD_MQTY_EXEC_FREQ, MDCN_EXEC_FREQ, DD_MQTY_FREQ) or many cases are not integer(using medifoam by cutting)
					1. If UN_COST and AMT are normal, then AMT/UN_COST 
					2. If UN_COST and AMT are abnormal(0, Null, UN_COST>AMT), usage should be multiplied in case of 30T using DD_MQTY_EXEC_FREQ, MDCN_EXEC_FREQ and DD_MQTY_FREQ 
																												or in case of 60T then using DD_EXEC_FREQ, MDCN_EXEC_FREQ, DD_MQTY_FREQ
					3. If all of UN_COST,AMT and usages are abnormal('0') then define as '1'
 --Generating Table: Device_exposure
***************************************/

/**************************************
 1. Create table
***************************************/ 
/*
CREATE TABLE @NHISNSC_database.DEVICE_EXPOSURE ( 
     device_exposure_id				BIGINT	 		PRIMARY KEY , 
     person_id						INTEGER			NOT NULL , 
     divce_concept_id				INTEGER			NOT NULL , 
     device_exposure_start_date		DATE			NOT NULL , 
     device_exposure_end_date		DATE			NULL , 
     device_type_concept_id			INTEGER			NOT NULL , 
     unique_device_id				VARCHAR(20)		NULL , 
     quantity						float			NULL , 
     provider_id					INTEGER			NULL , 
     visit_occurrence_id			BIGINT			NULL , 
	 device_source_value			VARCHAR(50)		NULL ,
	 device_source_concept_id		integer			NULL 
    );
*/

/**************************************
 1-1. Using temp mapping table
***************************************/ 
IF OBJECT_ID('tempdb..#mapping_table', 'U') IS NOT NULL
	DROP TABLE #mapping_table;
IF OBJECT_ID('tempdb..#temp', 'U') IS NOT NULL
	DROP TABLE #temp;
IF OBJECT_ID('tempdb..#duplicated', 'U') IS NOT NULL
	DROP TABLE #duplicated;
IF OBJECT_ID('tempdb..#device', 'U') IS NOT NULL
	DROP TABLE #device;
IF OBJECT_ID('tempdb..#five', 'U') IS NOT NULL
	DROP TABLE #five;

select a.source_code, a.target_concept_id, a.domain_id, REPLACE(a.invalid_reason, '', NULL) as invalid_reason
	into #temp
from @Mapping_database.source_to_concept_map a join @Mapping_database.CONCEPT b on a.target_concept_id=b.concept_id
where a.invalid_reason is null and b.invalid_reason is null and a.domain_id='device';

select * into #device from @Mapping_database.source_to_concept_map where domain_id='device';
select * into #five from @Mapping_database.source_to_concept_map where domain_id='procedure';

select a.*
	into #duplicated
from #device a, #five b
where a.source_code=b.source_code
	and a.invalid_reason='' and b.invalid_reason='';

select * into #mapping_table from #temp
where source_code not in (select source_code from #duplicated);

drop table #device, #five, #temp;

/**************************************
 2-1. Insert data using 30T
***************************************/  
insert into @NHISNSC_database.DEVICE_EXPOSURE
(device_exposure_id, person_id, device_concept_id, device_exposure_start_date, 
device_exposure_end_date, device_type_concept_id, unique_device_id, quantity, 
provider_id, visit_occurrence_id, device_source_value, device_source_concept_id)
select  convert(bigint, convert(bigint, a.master_seq) *10 + convert(bigint, row_number() over (partition by a.key_seq, a.seq_no order by b.target_concept_id))) as device_exposure_id,
		a.person_id as person_id,
		b.target_concept_id as device_concept_id ,
		CONVERT(VARCHAR, a.recu_fr_dt, 23) as device_source_start_date,
		CONVERT(VARCHAR, DATEADD(DAY, a.mdcn_exec_freq-1, a.recu_fr_dt),23) as device_source_end_date,
		44818705 as device_type_concept_id,
		null as unique_device_id,
		case	when a.AMT is not null and cast(a.AMT as float) > 0 and a.UN_COST is not null and cast(a.UN_COST as float) > 0 and cast(a.AMT as float)>=cast(a.UN_COST as float) then cast(a.AMT as float)/cast(a.UN_COST as float)
				when a.AMT is not null and cast(a.AMT as float) > 0 and a.UN_COST is not null and cast(a.UN_COST as float) > 0 and cast(a.UN_COST as float)>cast(a.AMT as float) then a.DD_MQTY_EXEC_FREQ * a.MDCN_EXEC_FREQ * a.DD_MQTY_FREQ 
				else a.DD_MQTY_EXEC_FREQ * a.MDCN_EXEC_FREQ * a.DD_MQTY_FREQ 
		end as quantity,
		null as provider_id,
		a.key_seq as visit_occurence_id,
		a.div_cd as device_source_value,
		null as device_source_concept_id

FROM 
	(SELECT x.key_seq, x.seq_no, x.recu_fr_dt, x.div_cd, 
			case when x.mdcn_exec_freq is not null and x.mdcn_exec_freq > '0' and isnumeric(x.mdcn_exec_freq)=1 then cast(x.mdcn_exec_freq as float) else 1 end as mdcn_exec_freq,
			case when x.dd_mqty_exec_freq is not null and x.dd_mqty_exec_freq > '0' and isnumeric(x.dd_mqty_exec_freq)=1 then cast(x.dd_mqty_exec_freq as float) else 1 end as dd_mqty_exec_freq,
			case when x.dd_mqty_freq is not null and x.dd_mqty_freq > '0' and isnumeric(x.dd_mqty_freq)=1 then cast(x.dd_mqty_freq as float) else 1 end as dd_mqty_freq,
			cast(x.amt as float) as amt , cast(x.un_cost as float) as un_cost, y.master_seq, y.person_id
	FROM (select * from @NHISNSC_rawdata.@NHIS_30T where div_type_cd not in ('1', '2', '3', '4', '5')) x, @NHISNSC_database.SEQ_MASTER y
	WHERE y.source_table='130'
	AND x.key_seq=y.key_seq
	AND x.seq_no=y.seq_no) a JOIN #mapping_table b 
ON a.div_cd=b.source_code
;

/**************************************
 2-2. Insert data using 60T
***************************************/  
insert into @NHISNSC_database.DEVICE_EXPOSURE
(device_exposure_id, person_id, device_concept_id, device_exposure_start_date, 
device_exposure_end_date, device_type_concept_id, unique_device_id, quantity, 
provider_id, visit_occurrence_id, device_source_value, device_source_concept_id)
select 	convert(bigint, convert(bigint, a.master_seq) *10 + convert(bigint, row_number() over (partition by a.key_seq, a.seq_no order by b.target_concept_id))) as device_exposure_id,
		a.person_id as person_id,
		b.target_concept_id as device_concept_id ,
		CONVERT(VARCHAR, a.recu_fr_dt, 23) as device_source_start_date,
		CONVERT(VARCHAR, DATEADD(DAY, a.mdcn_exec_freq-1, a.recu_fr_dt),23) as device_source_end_date,
		44818705 as device_type_concept_id,
		null as unique_device_id,
case	when a.AMT is not null and cast(a.AMT as float) > 0 and a.UN_COST is not null and cast(a.UN_COST as float) > 0 and cast(a.AMT as float)>=cast(a.UN_COST as float) then cast(a.AMT as float)/cast(a.UN_COST as float)
		when a.AMT is not null and cast(a.AMT as float) > 0 and a.UN_COST is not null and cast(a.UN_COST as float) > 0 and cast(a.UN_COST as float)>cast(a.AMT as float) then a.MDCN_EXEC_FREQ * a.DD_MQTY_FREQ * a.DD_EXEC_FREQ
		else a.MDCN_EXEC_FREQ * a.DD_MQTY_FREQ * a.DD_EXEC_FREQ
		end as quantity,
		null as provider_id,
		a.key_seq as visit_occurence_id,
		a.div_cd as device_source_value,
		null as device_source_concept_id

FROM 
	(SELECT x.key_seq, x.seq_no, x.recu_fr_dt, x.div_cd, 
			case when x.mdcn_exec_freq is not null and x.mdcn_exec_freq > '0' and isnumeric(x.mdcn_exec_freq)=1 then cast(x.mdcn_exec_freq as float) else 1 end as mdcn_exec_freq,
			case when x.dd_mqty_freq is not null and x.dd_mqty_freq > '0' and isnumeric(x.dd_mqty_freq)=1 then cast(x.dd_mqty_freq as float) else 1 end as dd_mqty_freq,
			case when x.dd_exec_freq is not null and x.dd_exec_freq > '0' and isnumeric(x.dd_exec_freq)=1 then cast(x.dd_exec_freq as float) else 1 end as dd_exec_freq,
			cast(x.amt as float) as amt , cast(x.un_cost as float) as un_cost, y.master_seq, y.person_id
	FROM (select * from @NHISNSC_rawdata.@NHIS_60T where div_type_cd not in ('1', '2', '3', '4', '5')) x, @NHISNSC_database.SEQ_MASTER y
	WHERE y.source_table='160'
	AND x.key_seq=y.key_seq
	AND x.seq_no=y.seq_no) a JOIN #mapping_table b 
	on a.div_cd=b.source_code
;

/**************************************
 2-1. Insert data using 30T duplicated
***************************************/  
insert into @NHISNSC_database.DEVICE_EXPOSURE
(device_exposure_id, person_id, device_concept_id, device_exposure_start_date, 
device_exposure_end_date, device_type_concept_id, unique_device_id, quantity, 
provider_id, visit_occurrence_id, device_source_value, device_source_concept_id)
select  convert(bigint, convert(bigint, a.master_seq) *10 + convert(bigint, row_number() over (partition by a.key_seq, a.seq_no order by b.target_concept_id))) as device_exposure_id,
		a.person_id as person_id,
		b.target_concept_id as device_concept_id ,
		CONVERT(VARCHAR, a.recu_fr_dt, 23) as device_source_start_date,
		CONVERT(VARCHAR, DATEADD(DAY, a.mdcn_exec_freq-1, a.recu_fr_dt),23) as device_source_end_date,
		44818705 as device_type_concept_id,
		null as unique_device_id,
		case	when a.AMT is not null and cast(a.AMT as float) > 0 and a.UN_COST is not null and cast(a.UN_COST as float) > 0 and cast(a.AMT as float)>=cast(a.UN_COST as float) then cast(a.AMT as float)/cast(a.UN_COST as float)
				when a.AMT is not null and cast(a.AMT as float) > 0 and a.UN_COST is not null and cast(a.UN_COST as float) > 0 and cast(a.UN_COST as float)>cast(a.AMT as float) then a.DD_MQTY_EXEC_FREQ * a.MDCN_EXEC_FREQ * a.DD_MQTY_FREQ 
				else a.DD_MQTY_EXEC_FREQ * a.MDCN_EXEC_FREQ * a.DD_MQTY_FREQ 
		end as quantity,
		null as provider_id,
		a.key_seq as visit_occurence_id,
		a.div_cd as device_source_value,
		null as device_source_concept_id

FROM 
	(SELECT x.key_seq, x.seq_no, x.recu_fr_dt, x.div_cd, 
			case when x.mdcn_exec_freq is not null and x.mdcn_exec_freq > '0' and isnumeric(x.mdcn_exec_freq)=1 then cast(x.mdcn_exec_freq as float) else 1 end as mdcn_exec_freq,
			case when x.dd_mqty_exec_freq is not null and x.dd_mqty_exec_freq > '0' and isnumeric(x.dd_mqty_exec_freq)=1 then cast(x.dd_mqty_exec_freq as float) else 1 end as dd_mqty_exec_freq,
			case when x.dd_mqty_freq is not null and x.dd_mqty_freq > '0' and isnumeric(x.dd_mqty_freq)=1 then cast(x.dd_mqty_freq as float) else 1 end as dd_mqty_freq,
			cast(x.amt as float) as amt , cast(x.un_cost as float) as un_cost, y.master_seq, y.person_id
	FROM (select * from @NHISNSC_rawdata.@NHIS_30T where div_type_cd in ('7', '8')) x, @NHISNSC_database.SEQ_MASTER y
	WHERE y.source_table='130'
	AND x.key_seq=y.key_seq
	AND x.seq_no=y.seq_no) a JOIN #duplicated b 
ON a.div_cd=b.source_code
;

/**************************************
 2-2. Insert data using 60T duplicated
***************************************/  
insert into @NHISNSC_database.DEVICE_EXPOSURE
(device_exposure_id, person_id, device_concept_id, device_exposure_start_date, 
device_exposure_end_date, device_type_concept_id, unique_device_id, quantity, 
provider_id, visit_occurrence_id, device_source_value, device_source_concept_id)
select 	convert(bigint, convert(bigint, a.master_seq) *10 + convert(bigint, row_number() over (partition by a.key_seq, a.seq_no order by b.target_concept_id))) as device_exposure_id,
		a.person_id as person_id,
		b.target_concept_id as device_concept_id ,
		CONVERT(VARCHAR, a.recu_fr_dt, 23) as device_source_start_date,
		CONVERT(VARCHAR, DATEADD(DAY, a.mdcn_exec_freq-1, a.recu_fr_dt),23) as device_source_end_date,
		44818705 as device_type_concept_id,
		null as unique_device_id,
case	when a.AMT is not null and cast(a.AMT as float) > 0 and a.UN_COST is not null and cast(a.UN_COST as float) > 0 and cast(a.AMT as float)>=cast(a.UN_COST as float) then cast(a.AMT as float)/cast(a.UN_COST as float)
		when a.AMT is not null and cast(a.AMT as float) > 0 and a.UN_COST is not null and cast(a.UN_COST as float) > 0 and cast(a.UN_COST as float)>cast(a.AMT as float) then a.MDCN_EXEC_FREQ * a.DD_MQTY_FREQ * a.DD_EXEC_FREQ
		else a.MDCN_EXEC_FREQ * a.DD_MQTY_FREQ * a.DD_EXEC_FREQ
		end as quantity,
		null as provider_id,
		a.key_seq as visit_occurence_id,
		a.div_cd as device_source_value,
		null as device_source_concept_id

FROM 
	(SELECT x.key_seq, x.seq_no, x.recu_fr_dt, x.div_cd, 
			case when x.mdcn_exec_freq is not null and x.mdcn_exec_freq > '0' and isnumeric(x.mdcn_exec_freq)=1 then cast(x.mdcn_exec_freq as float) else 1 end as mdcn_exec_freq,
			case when x.dd_mqty_freq is not null and x.dd_mqty_freq > '0' and isnumeric(x.dd_mqty_freq)=1 then cast(x.dd_mqty_freq as float) else 1 end as dd_mqty_freq,
			case when x.dd_exec_freq is not null and x.dd_exec_freq > '0' and isnumeric(x.dd_exec_freq)=1 then cast(x.dd_exec_freq as float) else 1 end as dd_exec_freq,
			cast(x.amt as float) as amt , cast(x.un_cost as float) as un_cost, y.master_seq, y.person_id
	FROM (select * from @NHISNSC_rawdata.@NHIS_60T where div_type_cd in ('7', '8')) x, @NHISNSC_database.SEQ_MASTER y
	WHERE y.source_table='160'
	AND x.key_seq=y.key_seq
	AND x.seq_no=y.seq_no) a JOIN #duplicated b 
	on a.div_cd=b.source_code

/**************************************
 2-3. Insert data using 30T which are unmapped with temp mapping table
***************************************/
insert into @NHISNSC_database.DEVICE_EXPOSURE
(device_exposure_id, person_id, device_concept_id, device_exposure_start_date, 
device_exposure_end_date, device_type_concept_id, unique_device_id, quantity, 
provider_id, visit_occurrence_id, device_source_value, device_source_concept_id)
select  
		convert(bigint, convert(bigint, a.master_seq)*10 + convert(bigint, row_number() over (partition by a.key_seq, a.seq_no order by a.div_cd))) as device_exposure_id,
		a.person_id as person_id,
		0 as device_concept_id ,
		CONVERT(VARCHAR, a.recu_fr_dt, 23) as device_source_start_date,
		CONVERT(VARCHAR, DATEADD(DAY, a.mdcn_exec_freq-1, a.recu_fr_dt),23) as device_source_end_date,
		44818705 as device_type_concept_id,
		null as unique_device_id,
case	when a.AMT is not null and cast(a.AMT as float) > 0 and a.UN_COST is not null and cast(a.UN_COST as float) > 0 and cast(a.AMT as float)>=cast(a.UN_COST as float) then cast(a.AMT as float)/cast(a.UN_COST as float)
		when a.AMT is not null and cast(a.AMT as float) > 0 and a.UN_COST is not null and cast(a.UN_COST as float) > 0 and cast(a.UN_COST as float)>cast(a.AMT as float) then a.DD_MQTY_EXEC_FREQ * a.MDCN_EXEC_FREQ * a.DD_MQTY_FREQ 
		else a.DD_MQTY_EXEC_FREQ * a.MDCN_EXEC_FREQ * a.DD_MQTY_FREQ 
		end as quantity,
		null as provider_id,
		a.key_seq as visit_occurence_id,
		a.div_cd as device_source_value,
		null as device_source_concept_id

FROM 
	(SELECT x.key_seq, x.seq_no, x.recu_fr_dt, x.div_cd, 
			case when x.mdcn_exec_freq is not null and x.mdcn_exec_freq > '0' and isnumeric(x.mdcn_exec_freq)=1 then cast(x.mdcn_exec_freq as float) else 1 end as mdcn_exec_freq,
			case when x.dd_mqty_exec_freq is not null and x.dd_mqty_exec_freq > '0' and isnumeric(x.dd_mqty_exec_freq)=1 then cast(x.dd_mqty_exec_freq as float) else 1 end as dd_mqty_exec_freq,
			case when x.dd_mqty_freq is not null and x.dd_mqty_freq > '0' and isnumeric(x.dd_mqty_freq)=1 then cast(x.dd_mqty_freq as float) else 1 end as dd_mqty_freq,
			cast(x.amt as float) as amt , cast(x.un_cost as float) as un_cost, y.master_seq, y.person_id
	FROM (select * from @NHISNSC_rawdata.@NHIS_30T where div_type_cd in ('7', '8')) x, @NHISNSC_database.SEQ_MASTER y
	WHERE y.source_table='130'
	AND x.key_seq=y.key_seq
	AND x.seq_no=y.seq_no) a  
where a.div_cd not in (select source_code from #duplicated union all select source_code from #mapping_table);


/**************************************
 2-4. Insert data using 60T which are unmapped with temp mapping table
***************************************/  
insert into @NHISNSC_database.DEVICE_EXPOSURE
(device_exposure_id, person_id, device_concept_id, device_exposure_start_date, 
device_exposure_end_date, device_type_concept_id, unique_device_id, quantity, 
provider_id, visit_occurrence_id, device_source_value, device_source_concept_id)
select 
		convert(bigint, convert(bigint, a.master_seq)*10 + convert(bigint, row_number() over (partition by a.key_seq, a.seq_no order by a.div_cd))) as device_exposure_id,
		a.person_id as person_id,
		0 as device_concept_id ,
		CONVERT(VARCHAR, a.recu_fr_dt, 23) as device_source_start_date,
		CONVERT(VARCHAR, DATEADD(DAY, a.mdcn_exec_freq-1, a.recu_fr_dt),23) as device_source_end_date,
		44818705 as device_type_concept_id,
		null as unique_device_id,
case	when a.AMT is not null and cast(a.AMT as float) > 0 and a.UN_COST is not null and cast(a.UN_COST as float) > 0 and cast(a.AMT as float)>=cast(a.UN_COST as float) then cast(a.AMT as float)/cast(a.UN_COST as float)
		when a.AMT is not null and cast(a.AMT as float) > 0 and a.UN_COST is not null and cast(a.UN_COST as float) > 0 and cast(a.UN_COST as float)>cast(a.AMT as float) then a.MDCN_EXEC_FREQ * a.DD_MQTY_FREQ * a.DD_EXEC_FREQ
		else a.MDCN_EXEC_FREQ * a.DD_MQTY_FREQ * a.DD_EXEC_FREQ
		end as quantity,
		null as provider_id,
		a.key_seq as visit_occurence_id,
		a.div_cd as device_source_value,
		null as device_source_concept_id
		
FROM 
	(SELECT x.key_seq, x.seq_no, x.recu_fr_dt, x.div_cd, 
			case when x.mdcn_exec_freq is not null and x.mdcn_exec_freq > '0' and isnumeric(x.mdcn_exec_freq)=1 then cast(x.mdcn_exec_freq as float) else 1 end as mdcn_exec_freq,
			case when x.dd_mqty_freq is not null and x.dd_mqty_freq > '0' and isnumeric(x.dd_mqty_freq)=1 then cast(x.dd_mqty_freq as float) else 1 end as dd_mqty_freq,
			case when x.dd_exec_freq is not null and x.dd_exec_freq > '0' and isnumeric(x.dd_exec_freq)=1 then cast(x.dd_exec_freq as float) else 1 end as dd_exec_freq,
			cast(x.amt as float) as amt , cast(x.un_cost as float) as un_cost, y.master_seq, y.person_id
	FROM (select * from @NHISNSC_rawdata.@NHIS_60T where div_type_cd in ('7', '8')) x, @NHISNSC_database.SEQ_MASTER y
	WHERE y.source_table='160'
	AND x.key_seq=y.key_seq
	AND x.seq_no=y.seq_no) a  
where a.div_cd not in (select source_code from #duplicated union all select source_code from #mapping_table)
;

drop table #mapping_table, #duplicated;


/*
-- If quantity is '0' then change into '1'
update @NHISNSC_database.DEVICE_EXPOSURE
set quantity = 1
where quantity = 0
;
*/
/******************* Check the result of before and after changing quantity from '0' to '1' *********************
select * from @ResultDatabaseSchema.device_exposure where quantity=0 -- before -> 6268(Catheterization of vein : 5275) / before -> 0
select * from @ResultDatabaseSchema.device_exposure where quantity=1 -- after -> 4548117 / after -> 4554385
*************************************************************************************/
