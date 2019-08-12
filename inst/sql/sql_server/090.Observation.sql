/**************************************
 --encoding : UTF-8
 --Author: JH Cho
 --Date: 2018.09.20
 
 @NHISNSC_rawdata : DB containing NHIS National Sample cohort DB
 @NHISNSC_database : DB for NHIS-NSC in CDM format
 @NHIS_JK: JK table in NHIS NSC
 @NHIS_20T: 20 table in NHIS NSC
 @NHIS_30T: 30 table in NHIS NSC
 @NHIS_40T: 40 table in NHIS NSC
 @NHIS_60T: 60 table in NHIS NSC
 @NHIS_GJ: GJ table in NHIS NSC
 @GJ_vertical : GJ table from NHIS NSC, which was vertically transformatted
 @CONDITION_MAPPINGTABLE : mapping table between KCD and OMOP vocabulary
 @DRUG_MAPPINGTABLE : mapping table between EDI and OMOP vocabulary
 @PROCEDURE_MAPPINGTABLE : mapping table between Korean procedure and OMOP vocabulary
 @DEVICE_MAPPINGTABLE : mapping table between EDI and OMOP vocabulary
 
 --Description: Create OBSERVATION table
 --Generating Table: OBSERVATION
***************************************/
/**************************************
 1. Create table
***************************************/ 
--drop table @ResultDatabaseSchema.OBSERVATION
--drop table #observation_mapping
--drop table #observation_mapping09

--IF OBJECT_ID(@ResultDatabaseSchema.OBSERVATION', 'U') IS NULL
/*
CREATE TABLE @NHISNSC_database.OBSERVATION
    (
     observation_id						BIGINT						NOT NULL , 
     person_id							INTEGER						NOT NULL ,
     observation_concept_id				INTEGER						NOT NULL ,
     observation_date					DATE						NOT NULL ,
     observation_time					TIME						NULL,  
     observation_type_concept_id		integer		 				NULL,  
	 value_as_number					float		 				NULL,
	 value_as_string					VARCHAR(50) 				NULL,
	 value_as_concept_id				integer		 				NULL,
	 qualifier_concept_id				integer		 				NULL,
	 unit_concept_id					integer						NULL,
	 provider_id						integer						NULL,
	 visit_occurrence_id				bigint						NULL,
	 observation_source_value			VARCHAR(50) 				NULL,
	 observation_source_concept_id		integer						NULL,
	 unit_source_value					VARCHAR(50) 				NULL,
	 qualifier_source_value				VARCHAR(50) 				NULL
	)
;
*/
	
/*    
-- Creating Vertical tables
select hchk_year, person_id, ykiho_gubun_cd, meas_type, meas_value 
into @NHISNSC_rawdata.GJ_VERTICAL
from @NHISNSC_rawdata.@NHIS_GJ
unpivot (meas_value for meas_type in ( -- 47 GJ items
    height, weight, waist, bp_high, bp_lwst,
    blds, tot_chole, triglyceride, hdl_chole, ldl_chole,
    hmg, gly_cd, olig_occu_cd, olig_ph, olig_prote_cd,
    creatinine, sgot_ast, sgpt_alt, gamma_gtp, hchk_pmh_cd1,
    hchk_pmh_cd2, hchk_pmh_cd3, hchk_apop_pmh_yn, hchk_hdise_pmh_yn, hchk_hprts_pmh_yn,
    hchk_diabml_pmh_yn, hchk_hplpdm_pmh_yn, hchk_etcdse_pmh_yn, hchk_phss_pmh_yn, fmly_liver_dise_patien_yn,
    fmly_hprts_patien_yn, fmly_apop_patien_yn, fmly_hdise_patien_yn, fmly_diabml_patien_yn, fmly_cancer_patien_yn,
    smk_stat_type_rsps_cd, smk_term_rsps_cd, cur_smk_term_rsps_cd, cur_dsqty_rsps_cd, past_smk_term_rsps_cd,
    past_dsqty_rsps_cd, dsqty_rsps_cd, drnk_habit_rsps_Cd, tm1_drkqty_rsps_cd, exerci_freq_rsps_cd,
    mov20_wek_freq_id, mov30_wek_freq_id, wlk30_wek_freq_id
)) as unpivortn
;


select STND_Y as hchk_year, person_id, jk_type, jk_value into @NHISNSC_rawdata.JK_VERTICAL
from @NHISNSC_rawdata.@NHIS_JK
unpivot (jk_value for jk_type in ( -- 2 JK items
        CTRB_PT_TYPE_CD, DFAB_GRD_CD
)) as unpivortn
;

*/

-- observation mapping table(temp)
CREATE TABLE #observation_mapping
    (
     meas_type						varchar(50)					NULL , 
     id_value						varchar(50)					NULL ,
     answer							bigint						NULL ,
     observation_concept_id			bigint						NULL ,
	 observation_type_concept_id	bigint						NULL ,
	 observation_unit_concept_id	bigint						NULL ,
	 value_as_concept_id			bigint						NULL ,
	 value_as_number				float						NULL 
	)
;
	
-- insert mapping data
insert into #observation_mapping (meas_type, id_value, answer, observation_concept_id, observation_type_concept_id, observation_unit_concept_id, value_as_concept_id, value_as_number) values ('HCHK_PMH_CD1', '20', 1, 4058267, 44814721, null, null, null);
insert into #observation_mapping (meas_type, id_value, answer, observation_concept_id, observation_type_concept_id, observation_unit_concept_id, value_as_concept_id, value_as_number) values ('HCHK_PMH_CD1', '20', 2, 43021368, 44814721, null, null, null);
insert into #observation_mapping (meas_type, id_value, answer, observation_concept_id, observation_type_concept_id, observation_unit_concept_id, value_as_concept_id, value_as_number) values ('HCHK_PMH_CD1', '20', 3, 4058725, 44814721, null, null, null);
insert into #observation_mapping (meas_type, id_value, answer, observation_concept_id, observation_type_concept_id, observation_unit_concept_id, value_as_concept_id, value_as_number) values ('HCHK_PMH_CD1', '20', 4, 4058286, 44814721, null, null, null);
insert into #observation_mapping (meas_type, id_value, answer, observation_concept_id, observation_type_concept_id, observation_unit_concept_id, value_as_concept_id, value_as_number) values ('HCHK_PMH_CD1', '20', 5, 4077352, 44814721, null, null, null);
insert into #observation_mapping (meas_type, id_value, answer, observation_concept_id, observation_type_concept_id, observation_unit_concept_id, value_as_concept_id, value_as_number) values ('HCHK_PMH_CD1', '20', 6, 4077982, 44814721, null, null, null);
insert into #observation_mapping (meas_type, id_value, answer, observation_concept_id, observation_type_concept_id, observation_unit_concept_id, value_as_concept_id, value_as_number) values ('HCHK_PMH_CD1', '20', 7, 4058709, 44814721, null, null, null);
insert into #observation_mapping (meas_type, id_value, answer, observation_concept_id, observation_type_concept_id, observation_unit_concept_id, value_as_concept_id, value_as_number) values ('HCHK_PMH_CD1', '20', 8, 4144289, 44814721, null, null, null);
insert into #observation_mapping (meas_type, id_value, answer, observation_concept_id, observation_type_concept_id, observation_unit_concept_id, value_as_concept_id, value_as_number) values ('HCHK_PMH_CD1', '20', 9, 4195979, 44814721, null, null, null);
insert into #observation_mapping (meas_type, id_value, answer, observation_concept_id, observation_type_concept_id, observation_unit_concept_id, value_as_concept_id, value_as_number) values ('HCHK_PMH_CD2', '21', 1, 4058267, 44814721, null, null, null);
insert into #observation_mapping (meas_type, id_value, answer, observation_concept_id, observation_type_concept_id, observation_unit_concept_id, value_as_concept_id, value_as_number) values ('HCHK_PMH_CD2', '21', 2, 43021368, 44814721, null, null, null);
insert into #observation_mapping (meas_type, id_value, answer, observation_concept_id, observation_type_concept_id, observation_unit_concept_id, value_as_concept_id, value_as_number) values ('HCHK_PMH_CD2', '21', 3, 4058725, 44814721, null, null, null);
insert into #observation_mapping (meas_type, id_value, answer, observation_concept_id, observation_type_concept_id, observation_unit_concept_id, value_as_concept_id, value_as_number) values ('HCHK_PMH_CD2', '21', 4, 4058286, 44814721, null, null, null);
insert into #observation_mapping (meas_type, id_value, answer, observation_concept_id, observation_type_concept_id, observation_unit_concept_id, value_as_concept_id, value_as_number) values ('HCHK_PMH_CD2', '21', 5, 4077352, 44814721, null, null, null);
insert into #observation_mapping (meas_type, id_value, answer, observation_concept_id, observation_type_concept_id, observation_unit_concept_id, value_as_concept_id, value_as_number) values ('HCHK_PMH_CD2', '21', 6, 4077982, 44814721, null, null, null);
insert into #observation_mapping (meas_type, id_value, answer, observation_concept_id, observation_type_concept_id, observation_unit_concept_id, value_as_concept_id, value_as_number) values ('HCHK_PMH_CD2', '21', 7, 4058709, 44814721, null, null, null);
insert into #observation_mapping (meas_type, id_value, answer, observation_concept_id, observation_type_concept_id, observation_unit_concept_id, value_as_concept_id, value_as_number) values ('HCHK_PMH_CD2', '21', 8, 4144289, 44814721, null, null, null);
insert into #observation_mapping (meas_type, id_value, answer, observation_concept_id, observation_type_concept_id, observation_unit_concept_id, value_as_concept_id, value_as_number) values ('HCHK_PMH_CD2', '21', 9, 4195979, 44814721, null, null, null);
insert into #observation_mapping (meas_type, id_value, answer, observation_concept_id, observation_type_concept_id, observation_unit_concept_id, value_as_concept_id, value_as_number) values ('HCHK_PMH_CD3', '22', 1, 4058267, 44814721, null, null, null);
insert into #observation_mapping (meas_type, id_value, answer, observation_concept_id, observation_type_concept_id, observation_unit_concept_id, value_as_concept_id, value_as_number) values ('HCHK_PMH_CD3', '22', 2, 43021368, 44814721, null, null, null);
insert into #observation_mapping (meas_type, id_value, answer, observation_concept_id, observation_type_concept_id, observation_unit_concept_id, value_as_concept_id, value_as_number) values ('HCHK_PMH_CD3', '22', 3, 4058725, 44814721, null, null, null);
insert into #observation_mapping (meas_type, id_value, answer, observation_concept_id, observation_type_concept_id, observation_unit_concept_id, value_as_concept_id, value_as_number) values ('HCHK_PMH_CD3', '22', 4, 4058286, 44814721, null, null, null);
insert into #observation_mapping (meas_type, id_value, answer, observation_concept_id, observation_type_concept_id, observation_unit_concept_id, value_as_concept_id, value_as_number) values ('HCHK_PMH_CD3', '22', 5, 4077352, 44814721, null, null, null);
insert into #observation_mapping (meas_type, id_value, answer, observation_concept_id, observation_type_concept_id, observation_unit_concept_id, value_as_concept_id, value_as_number) values ('HCHK_PMH_CD3', '22', 6, 4077982, 44814721, null, null, null);
insert into #observation_mapping (meas_type, id_value, answer, observation_concept_id, observation_type_concept_id, observation_unit_concept_id, value_as_concept_id, value_as_number) values ('HCHK_PMH_CD3', '22', 7, 4058709, 44814721, null, null, null);
insert into #observation_mapping (meas_type, id_value, answer, observation_concept_id, observation_type_concept_id, observation_unit_concept_id, value_as_concept_id, value_as_number) values ('HCHK_PMH_CD3', '22', 8, 4144289, 44814721, null, null, null);
insert into #observation_mapping (meas_type, id_value, answer, observation_concept_id, observation_type_concept_id, observation_unit_concept_id, value_as_concept_id, value_as_number) values ('HCHK_PMH_CD3', '22', 9, 4195979, 44814721, null, null, null);
insert into #observation_mapping (meas_type, id_value, answer, observation_concept_id, observation_type_concept_id, observation_unit_concept_id, value_as_concept_id, value_as_number) values ('HCHK_APOP_PMH_YN',		'23',	1,		4077982,		44814721,	null,		null,		null);
insert into #observation_mapping (meas_type, id_value, answer, observation_concept_id, observation_type_concept_id, observation_unit_concept_id, value_as_concept_id, value_as_number) values ('HCHK_HDISE_PMH_YN',		'24',	1,		4077352,		44814721,	null,		null,		null);
insert into #observation_mapping (meas_type, id_value, answer, observation_concept_id, observation_type_concept_id, observation_unit_concept_id, value_as_concept_id, value_as_number) values ('HCHK_HPRTS_PMH_YN',		'25',	1,		4058286,		44814721,	null,		null,		null);
insert into #observation_mapping (meas_type, id_value, answer, observation_concept_id, observation_type_concept_id, observation_unit_concept_id, value_as_concept_id, value_as_number) values ('HCHK_DIABML_PMH_YN',	'26',	1,		4058709,		44814721,	null,		null,		null);
insert into #observation_mapping (meas_type, id_value, answer, observation_concept_id, observation_type_concept_id, observation_unit_concept_id, value_as_concept_id, value_as_number) values ('HCHK_HPLPDM_PMH_YN',	'27',	1,		4058275,		44814721,	null,		null,		null);
insert into #observation_mapping (meas_type, id_value, answer, observation_concept_id, observation_type_concept_id, observation_unit_concept_id, value_as_concept_id, value_as_number) values ('HCHK_ETCDSE_PMH_YN',	'28',	1,		44834226,		44814721,	null,		null,		null);
insert into #observation_mapping (meas_type, id_value, answer, observation_concept_id, observation_type_concept_id, observation_unit_concept_id, value_as_concept_id, value_as_number) values ('HCHK_PHSS_PMH_YN',		'29',	1,		4058267,		44814721,	null,		null,		null);
insert into #observation_mapping (meas_type, id_value, answer, observation_concept_id, observation_type_concept_id, observation_unit_concept_id, value_as_concept_id, value_as_number) values ('FMLY_LIVER_DISE_PATIEN_YN', '30', 1,	4144266,		44814721,	null,		null,		null);
insert into #observation_mapping (meas_type, id_value, answer, observation_concept_id, observation_type_concept_id, observation_unit_concept_id, value_as_concept_id, value_as_number) values ('FMLY_HPRTS_PATIEN_YN',	'31',	0,		4053372,		44814721,	null,		null,		null);
insert into #observation_mapping (meas_type, id_value, answer, observation_concept_id, observation_type_concept_id, observation_unit_concept_id, value_as_concept_id, value_as_number) values ('FMLY_HPRTS_PATIEN_YN',	'31',	1,		4050816,		44814721,	null,		null,		null);
insert into #observation_mapping (meas_type, id_value, answer, observation_concept_id, observation_type_concept_id, observation_unit_concept_id, value_as_concept_id, value_as_number) values ('FMLY_APOP_PATIEN_YN',	'32',	0,		4175587,		44814721,	null,		null,		null);
insert into #observation_mapping (meas_type, id_value, answer, observation_concept_id, observation_type_concept_id, observation_unit_concept_id, value_as_concept_id, value_as_number) values ('FMLY_APOP_PATIEN_YN',	'32',	1,		4169009,		44814721,	null,		null,		null);
insert into #observation_mapping (meas_type, id_value, answer, observation_concept_id, observation_type_concept_id, observation_unit_concept_id, value_as_concept_id, value_as_number) values ('FMLY_HDISE_PATIEN_YN',	'33',	0,		4050792,		44814721,	null,		null,		null);
insert into #observation_mapping (meas_type, id_value, answer, observation_concept_id, observation_type_concept_id, observation_unit_concept_id, value_as_concept_id, value_as_number) values ('FMLY_HDISE_PATIEN_YN',	'33',	1,		4173498,		44814721,	null,		null,		null);
insert into #observation_mapping (meas_type, id_value, answer, observation_concept_id, observation_type_concept_id, observation_unit_concept_id, value_as_concept_id, value_as_number) values ('FMLY_DIABML_PATIEN_YN',	'34',	0,		4051106,		44814721,	null,		null,		null);
insert into #observation_mapping (meas_type, id_value, answer, observation_concept_id, observation_type_concept_id, observation_unit_concept_id, value_as_concept_id, value_as_number) values ('FMLY_DIABML_PATIEN_YN',	'34',	1,		4051114,		44814721,	null,		null,		null);
insert into #observation_mapping (meas_type, id_value, answer, observation_concept_id, observation_type_concept_id, observation_unit_concept_id, value_as_concept_id, value_as_number) values ('FMLY_CANCER_PATIEN_YN',	'35',	0,		4051100,		44814721,	null,		null,		null);
insert into #observation_mapping (meas_type, id_value, answer, observation_concept_id, observation_type_concept_id, observation_unit_concept_id, value_as_concept_id, value_as_number) values ('FMLY_CANCER_PATIEN_YN',	'35',	1,		4171594,		44814721,	null,		null,		null);
insert into #observation_mapping (meas_type, id_value, answer, observation_concept_id, observation_type_concept_id, observation_unit_concept_id, value_as_concept_id, value_as_number) values ('SMK_STAT_TYPE_RSPS_CD',	'36',	1,		4222303,		44814721,	NULL,		NULL,		NULL);
insert into #observation_mapping (meas_type, id_value, answer, observation_concept_id, observation_type_concept_id, observation_unit_concept_id, value_as_concept_id, value_as_number) values ('SMK_STAT_TYPE_RSPS_CD',	'36',	2,		4310250,		44814721,	NULL,		NULL,		NULL);
insert into #observation_mapping (meas_type, id_value, answer, observation_concept_id, observation_type_concept_id, observation_unit_concept_id, value_as_concept_id, value_as_number) values ('SMK_STAT_TYPE_RSPS_CD',	'36',	3,		4276526,		44814721,	NULL,		NULL,		NULL);
insert into #observation_mapping (meas_type, id_value, answer, observation_concept_id, observation_type_concept_id, observation_unit_concept_id, value_as_concept_id, value_as_number) values ('SMK_TERM_RSPS_CD',		'37',	1,		40766364,		44818704,	NULL,		NULL,		2.5) ;
insert into #observation_mapping (meas_type, id_value, answer, observation_concept_id, observation_type_concept_id, observation_unit_concept_id, value_as_concept_id, value_as_number) values ('SMK_TERM_RSPS_CD',		'37',	2,		40766364,		44818704,	NULL,		NULL,		7.5) ;
insert into #observation_mapping (meas_type, id_value, answer, observation_concept_id, observation_type_concept_id, observation_unit_concept_id, value_as_concept_id, value_as_number) values ('SMK_TERM_RSPS_CD',		'37',	3,		40766364,		44818704,	NULL,		NULL,		15) ;
insert into #observation_mapping (meas_type, id_value, answer, observation_concept_id, observation_type_concept_id, observation_unit_concept_id, value_as_concept_id, value_as_number) values ('SMK_TERM_RSPS_CD',		'37',	4,		40766364,		44818704,	NULL,		NULL,		25) ;
insert into #observation_mapping (meas_type, id_value, answer, observation_concept_id, observation_type_concept_id, observation_unit_concept_id, value_as_concept_id, value_as_number) values ('SMK_TERM_RSPS_CD',		'37',	5,		40766364,		44818704,	NULL,		NULL,		30) ;
insert into #observation_mapping (meas_type, id_value, answer, observation_concept_id, observation_type_concept_id, observation_unit_concept_id, value_as_concept_id, value_as_number) values ('CUR_SMK_TERM_RSPS_CD',	'38',	0,		40766364,		44818704,	9448,		NULL,		NULL); 
insert into #observation_mapping (meas_type, id_value, answer, observation_concept_id, observation_type_concept_id, observation_unit_concept_id, value_as_concept_id, value_as_number) values ('CUR_DSQTY_RSPS_CD',		'39',	0,		40766929,		44818704,	45756923,	NULL,		NULL) ;
insert into #observation_mapping (meas_type, id_value, answer, observation_concept_id, observation_type_concept_id, observation_unit_concept_id, value_as_concept_id, value_as_number) values ('PAST_SMK_TERM_RSPS_CD',	'40',	0,		40766364,		44818704,	9448,		NULL,		NULL) ;
insert into #observation_mapping (meas_type, id_value, answer, observation_concept_id, observation_type_concept_id, observation_unit_concept_id, value_as_concept_id, value_as_number) values ('PAST_DSQTY_RSPS_CD',	'41',	0,		40766930,		44818704,	45756923,	NULL,		NULL) ;
insert into #observation_mapping (meas_type, id_value, answer, observation_concept_id, observation_type_concept_id, observation_unit_concept_id, value_as_concept_id, value_as_number) values ('DSQTY_RSPS_CD',			'42',	1,		40766929,		44818704,	45756954,	NULL,		0.25) ;
insert into #observation_mapping (meas_type, id_value, answer, observation_concept_id, observation_type_concept_id, observation_unit_concept_id, value_as_concept_id, value_as_number) values ('DSQTY_RSPS_CD',			'42',	2,		40766929,		44818704,	45756954,	NULL,		0.75) ;
insert into #observation_mapping (meas_type, id_value, answer, observation_concept_id, observation_type_concept_id, observation_unit_concept_id, value_as_concept_id, value_as_number) values ('DSQTY_RSPS_CD',			'42',	3,		40766929,		44818704,	45756954,	NULL,		1.5) ;
insert into #observation_mapping (meas_type, id_value, answer, observation_concept_id, observation_type_concept_id, observation_unit_concept_id, value_as_concept_id, value_as_number) values ('DSQTY_RSPS_CD',			'42',	4,		40766929,		44818704,	45756954,	NULL,		2) ;
insert into #observation_mapping (meas_type, id_value, answer, observation_concept_id, observation_type_concept_id, observation_unit_concept_id, value_as_concept_id, value_as_number) values ('DRNK_HABIT_RSPS_CD',	'43',	1,		40771103,		44818704,	NULL,		45882527,	NULL);
insert into #observation_mapping (meas_type, id_value, answer, observation_concept_id, observation_type_concept_id, observation_unit_concept_id, value_as_concept_id, value_as_number) values ('DRNK_HABIT_RSPS_CD',	'43',	2,		40771103,		44818704,	NULL,		45885249,	NULL);
insert into #observation_mapping (meas_type, id_value, answer, observation_concept_id, observation_type_concept_id, observation_unit_concept_id, value_as_concept_id, value_as_number) values ('DRNK_HABIT_RSPS_CD',	'43',	3,		40771103,		44818704,	NULL,		45881653,	NULL);
insert into #observation_mapping (meas_type, id_value, answer, observation_concept_id, observation_type_concept_id, observation_unit_concept_id, value_as_concept_id, value_as_number) values ('DRNK_HABIT_RSPS_CD',	'43',	4,		40771103,		44818704,	NULL,		45885248,	NULL);
insert into #observation_mapping (meas_type, id_value, answer, observation_concept_id, observation_type_concept_id, observation_unit_concept_id, value_as_concept_id, value_as_number) values ('DRNK_HABIT_RSPS_CD',	'43',	5,		40771103,		44818704,	NULL,		45879676,	NULL);
insert into #observation_mapping (meas_type, id_value, answer, observation_concept_id, observation_type_concept_id, observation_unit_concept_id, value_as_concept_id, value_as_number) values ('TM1_DRKQTY_RSPS_CD',	'44',	1,		3037705,		44818704,	4045131,	NULL,		3.5) ;
insert into #observation_mapping (meas_type, id_value, answer, observation_concept_id, observation_type_concept_id, observation_unit_concept_id, value_as_concept_id, value_as_number) values ('TM1_DRKQTY_RSPS_CD',	'44',	2,		3037705,		44818704,	4045131,	NULL,		7)	;	
insert into #observation_mapping (meas_type, id_value, answer, observation_concept_id, observation_type_concept_id, observation_unit_concept_id, value_as_concept_id, value_as_number) values ('TM1_DRKQTY_RSPS_CD',	'44',	3,		3037705,		44818704,	4045131,	NULL,		10.5);	
insert into #observation_mapping (meas_type, id_value, answer, observation_concept_id, observation_type_concept_id, observation_unit_concept_id, value_as_concept_id, value_as_number) values ('TM1_DRKQTY_RSPS_CD',	'44',	4,		3037705,		44818704,	4045131,	NULL,		14)	;
insert into #observation_mapping (meas_type, id_value, answer, observation_concept_id, observation_type_concept_id, observation_unit_concept_id, value_as_concept_id, value_as_number) values ('EXERCI_FREQ_RSPS_CD',	'45',	1,		4036426,		44818704,	NULL,		45882527,	NULL);
insert into #observation_mapping (meas_type, id_value, answer, observation_concept_id, observation_type_concept_id, observation_unit_concept_id, value_as_concept_id, value_as_number) values ('EXERCI_FREQ_RSPS_CD',	'45',	2,		4036426,		44818704,	NULL,		45881653,	NULL);																						   
insert into #observation_mapping (meas_type, id_value, answer, observation_concept_id, observation_type_concept_id, observation_unit_concept_id, value_as_concept_id, value_as_number) values ('EXERCI_FREQ_RSPS_CD',	'45',	3,		4036426,		44818704,	NULL,		45885248,	NULL);
insert into #observation_mapping (meas_type, id_value, answer, observation_concept_id, observation_type_concept_id, observation_unit_concept_id, value_as_concept_id, value_as_number) values ('EXERCI_FREQ_RSPS_CD',	'45',	4,		4036426,		44818704,	NULL,		45883166,	NULL);
insert into #observation_mapping (meas_type, id_value, answer, observation_concept_id, observation_type_concept_id, observation_unit_concept_id, value_as_concept_id, value_as_number) values ('EXERCI_FREQ_RSPS_CD',	'45',	5,		4036426,		44818704,	NULL,		45879676,	NULL);
insert into #observation_mapping (meas_type, id_value, answer, observation_concept_id, observation_type_concept_id, observation_unit_concept_id, value_as_concept_id, value_as_number) values ('MOV20_WEK_FREQ_ID',		'46',	0,		82020119,		44818704,	NULL,		NULL,		NULL) ;
insert into #observation_mapping (meas_type, id_value, answer, observation_concept_id, observation_type_concept_id, observation_unit_concept_id, value_as_concept_id, value_as_number) values ('MOV30_WEK_FREQ_ID',		'47',	0,		82020120,		44818704,	NULL,		NULL,		NULL) ;
insert into #observation_mapping (meas_type, id_value, answer, observation_concept_id, observation_type_concept_id, observation_unit_concept_id, value_as_concept_id, value_as_number) values ('WLK30_WEK_FREQ_ID',		'48',	0,		82020121,		44818704,	NULL,		NULL,		NULL) ;
insert into #observation_mapping (meas_type, id_value, answer, observation_concept_id, observation_type_concept_id, observation_unit_concept_id, value_as_concept_id, value_as_number) values ('CTRB_PT_TYPE_CD',		'49',	0,		3004572,		44814721,	4155146,	NULL,		NULL);
insert into #observation_mapping (meas_type, id_value, answer, observation_concept_id, observation_type_concept_id, observation_unit_concept_id, value_as_concept_id, value_as_number) values ('CTRB_PT_TYPE_CD',		'49',	1,		3004572,		44814721,	4155146,	NULL,		NULL);
insert into #observation_mapping (meas_type, id_value, answer, observation_concept_id, observation_type_concept_id, observation_unit_concept_id, value_as_concept_id, value_as_number) values ('CTRB_PT_TYPE_CD',		'49',	2,		3004572,		44814721,	4155146,	NULL,		NULL);
insert into #observation_mapping (meas_type, id_value, answer, observation_concept_id, observation_type_concept_id, observation_unit_concept_id, value_as_concept_id, value_as_number) values ('CTRB_PT_TYPE_CD',		'49',	3,		3004572,		44814721,	4155146,	NULL,		NULL);
insert into #observation_mapping (meas_type, id_value, answer, observation_concept_id, observation_type_concept_id, observation_unit_concept_id, value_as_concept_id, value_as_number) values ('CTRB_PT_TYPE_CD',		'49',	4,		3004572,		44814721,	4155146,	NULL,		NULL);
insert into #observation_mapping (meas_type, id_value, answer, observation_concept_id, observation_type_concept_id, observation_unit_concept_id, value_as_concept_id, value_as_number) values ('CTRB_PT_TYPE_CD',		'49',	5,		3004572,		44814721,	4155146,	NULL,		NULL);
insert into #observation_mapping (meas_type, id_value, answer, observation_concept_id, observation_type_concept_id, observation_unit_concept_id, value_as_concept_id, value_as_number) values ('CTRB_PT_TYPE_CD',		'49',	6,		3004572,		44814721,	4155146,	NULL,		NULL);
insert into #observation_mapping (meas_type, id_value, answer, observation_concept_id, observation_type_concept_id, observation_unit_concept_id, value_as_concept_id, value_as_number) values ('CTRB_PT_TYPE_CD',		'49',	7,		3004572,		44814721,	4155146,	NULL,		NULL);
insert into #observation_mapping (meas_type, id_value, answer, observation_concept_id, observation_type_concept_id, observation_unit_concept_id, value_as_concept_id, value_as_number) values ('CTRB_PT_TYPE_CD',		'49',	8,		3004572,		44814721,	4155146,	NULL,		NULL);
insert into #observation_mapping (meas_type, id_value, answer, observation_concept_id, observation_type_concept_id, observation_unit_concept_id, value_as_concept_id, value_as_number) values ('CTRB_PT_TYPE_CD',		'49',	9,		3004572,		44814721,	4155146,	NULL,		NULL);
insert into #observation_mapping (meas_type, id_value, answer, observation_concept_id, observation_type_concept_id, observation_unit_concept_id, value_as_concept_id, value_as_number) values ('CTRB_PT_TYPE_CD',		'49',	10,		3004572,		44814721,	4155146,	NULL,		NULL);

																																	 


/**************************************
 2. Insert categorical data
***************************************/ 
INSERT INTO @NHISNSC_database.OBSERVATION (observation_id, person_id, observation_concept_id, observation_date, observation_datetime, observation_type_concept_id, value_as_number, value_As_string, value_as_concept_id,
										qualifier_concept_id, unit_concept_id, provider_id, visit_occurrence_id, observation_source_value, observation_source_concept_id, unit_source_value, qualifier_source_value)

	select	cast(concat(c.master_seq, b.id_value) as bigint) as observation_id,
			a.person_id as person_id,
			b.observation_concept_id as observation_concept_id,
			cast(CONVERT(VARCHAR, a.hchk_year+'0101', 23)as date) as observation_date,
			oservation_datetime = null,
			b.observation_type_concept_id as observation_type_concept_id,
				CASE WHEN b.answer is not null and b.value_as_number is not null
				then b.value_as_number
				else a.meas_value
				END as value_as_number,
			value_as_string = null,
			b.value_as_concept_id as value_as_concept_id,
			qualifier_source_value = null,
			unit_concept_id = null,
			provider_id = null,
			visit_occurrence_id = c.master_seq,
			a.meas_value as observation_source_value,
			observation_source_concept_id = null,
			unit_source_value = null,
			qualifier_source_Value = null

	from (select hchk_year, person_id, ykiho_gubun_cd, meas_type, 
				--Family history (Starting with FMLY_) existence, recored as 1 or 2 until 2008 and 0 or 1 until 2009
				case	when substring(meas_type, 1, 30) in('FMLY_LIVER_DISE_PATIEN_YN', 'FMLY_HPRTS_PATIEN_YN', 'FMLY_APOP_PATIEN_YN', 'FMLY_HDISE_PATIEN_YN', 'FMLY_DIABML_PATIEN_YN', 'FMLY_CANCER_PATIEN_YN') 
							and substring(hchk_year, 1, 4) in ('2002', '2003', '2004', '2005', '2006', '2007', '2008') then cast(cast(meas_value as int)-1 as varchar(50))
				else meas_value
				end as meas_value 			
			from @NHISNSC_rawdata.GJ_VERTICAL) a
		JOIN #observation_mapping b 
		on isnull(a.meas_type,'') = isnull(b.meas_type,'') 
			and isnull(a.meas_value,'0') = isnull(cast(b.answer as char),'0')
		JOIN @NHISNSC_database.SEQ_MASTER c
		on a.person_id = cast(c.person_id as char)
			and a.hchk_year = c.hchk_year
	where (a.meas_value != '' and substring(a.meas_type, 1, 30) in ('HCHK_PMH_CD1', 'HCHK_PMH_CD2', 'HCHK_PMH_CD3','HCHK_APOP_PMH_YN', 'HCHK_HDISE_PMH_YN', 'HCHK_HPRTS_PMH_YN', 
																	'HCHK_DIABML_PMH_YN', 'HCHK_HPLPDM_PMH_YN', 'HCHK_ETCDSE_PMH_YN', 'HCHK_PHSS_PMH_YN', 'FMLY_LIVER_DISE_PATIEN_YN', 'FMLY_HPRTS_PATIEN_YN', 
																	'FMLY_APOP_PATIEN_YN', 'FMLY_HDISE_PATIEN_YN', 'FMLY_DIABML_PATIEN_YN', 'FMLY_CANCER_PATIEN_YN', 'SMK_STAT_TYPE_RSPS_CD', 'SMK_TERM_RSPS_CD',
																	 'DSQTY_RSPS_CD', 'EXERCI_FREQ_RSPS_CD')
		or(a.meas_value != '' and substring(a.meas_type, 1, 30) in ('DRNK_HABIT_RSPS_CD', 'TM1_DRKQTY_RSPS_CD') and substring(a.hchk_year, 1, 4) in ('2002', '2003', '2004', '2005', '2006', '2007', '2008')))
			and c.source_table like 'GJT'
;



/**************************************
 2. Insert continuous data
***************************************/ 
INSERT INTO @NHISNSC_database.OBSERVATION (observation_id, person_id, observation_concept_id, observation_date, observation_datetime, observation_type_concept_id, value_as_number, value_As_string, value_as_concept_id,
										qualifier_concept_id, unit_concept_id, provider_id, visit_occurrence_id, observation_source_value, observation_source_concept_id, unit_source_value, qualifier_source_value)

	select	cast(concat(c.master_seq, b.id_value) as bigint) as observation_id,
			a.person_id as person_id,
			b.observation_concept_id as observation_concept_id,
			cast(CONVERT(VARCHAR, a.hchk_year+'0101', 23)as date) as observation_date,
			oservation_time = null,
			b.observation_type_concept_id as observation_type_concept_id,
				CASE WHEN b.answer is not null and b.value_as_number is not null
				then b.value_as_number
				else a.meas_value
				END as value_as_number,
			value_as_string = null,
			b.value_as_concept_id as value_as_concept_id,
			qualifier_source_value = null,
			b.observation_unit_concept_id as unit_concept_id ,
			provider_id = null,
			visit_occurrence_id = c.master_seq,
			a.meas_value as observation_source_value,
			observation_source_concept_id = null,
			unit_source_value = null,
			qualifier_source_Value = null

	from (select hchk_year, person_id, ykiho_gubun_cd, meas_type, meas_value
			from @NHISNSC_rawdata.GJ_VERTICAL) a
		JOIN #observation_mapping b 
		on isnull(a.meas_type,'') = isnull(b.meas_type,'') 
			and isnull(a.meas_value,'0') >= isnull(cast(b.answer as char),'0')
		JOIN @NHISNSC_database.SEQ_MASTER c
		on a.person_id = cast(c.person_id as char)
			and a.hchk_year = c.hchk_year
	where (a.meas_value != '' and substring(a.meas_type, 1, 30) in ('CUR_SMK_TERM_RSPS_CD', 'CUR_DSQTY_RSPS_CD', 'PAST_SMK_TERM_RSPS_CD', 'PAST_DSQTY_RSPS_CD', 
																	'MOV20_WEK_FREQ_ID', 'MOV30_WEK_FREQ_ID', 'WLK30_WEK_FREQ_ID'))
			and c.source_table like 'GJT'
;


/**************************************
 2. Insert continuous drinking data changing from 2009
***************************************/ 
--temp mapping table



CREATE TABLE #observation_mapping09
    (
     meas_type						varchar(50)					NULL , 
     id_value						varchar(50)					NULL ,
     answer							bigint						NULL ,
     observation_concept_id			bigint						NULL ,
	 observation_type_concept_id	bigint						NULL ,
	 observation_unit_concept_id	bigint						NULL ,
	 value_as_concept_id			bigint						NULL ,
	 value_as_number				float						NULL 
	)
;


insert into #observation_mapping09 (meas_type, id_value, answer, observation_concept_id, observation_type_concept_id, observation_unit_concept_id, value_as_concept_id, value_as_number) values ('DRNK_HABIT_RSPS_CD',	'43',	1,		40771103,		44818704,	45881908,		NULL,		0);
insert into #observation_mapping09 (meas_type, id_value, answer, observation_concept_id, observation_type_concept_id, observation_unit_concept_id, value_as_concept_id, value_as_number) values ('DRNK_HABIT_RSPS_CD',	'43',	2,		40771103,		44818704,	45881908,		NULL,		1);
insert into #observation_mapping09 (meas_type, id_value, answer, observation_concept_id, observation_type_concept_id, observation_unit_concept_id, value_as_concept_id, value_as_number) values ('DRNK_HABIT_RSPS_CD',	'43',	3,		40771103,		44818704,	45881908,		NULL,		2);
insert into #observation_mapping09 (meas_type, id_value, answer, observation_concept_id, observation_type_concept_id, observation_unit_concept_id, value_as_concept_id, value_as_number) values ('DRNK_HABIT_RSPS_CD',	'43',	4,		40771103,		44818704,	45881908,		NULL,		3);
insert into #observation_mapping09 (meas_type, id_value, answer, observation_concept_id, observation_type_concept_id, observation_unit_concept_id, value_as_concept_id, value_as_number) values ('DRNK_HABIT_RSPS_CD',	'43',	5,		40771103,		44818704,	45881908,		NULL,		4);
insert into #observation_mapping09 (meas_type, id_value, answer, observation_concept_id, observation_type_concept_id, observation_unit_concept_id, value_as_concept_id, value_as_number) values ('DRNK_HABIT_RSPS_CD',	'43',	6,		40771103,		44818704,	45881908,		NULL,		5);
insert into #observation_mapping09 (meas_type, id_value, answer, observation_concept_id, observation_type_concept_id, observation_unit_concept_id, value_as_concept_id, value_as_number) values ('DRNK_HABIT_RSPS_CD',	'43',	7,		40771103,		44818704,	45881908,		NULL,		6);
insert into #observation_mapping09 (meas_type, id_value, answer, observation_concept_id, observation_type_concept_id, observation_unit_concept_id, value_as_concept_id, value_as_number) values ('DRNK_HABIT_RSPS_CD',	'43',	8,		40771103,		44818704,	45881908,		NULL,		7);
insert into #observation_mapping09 (meas_type, id_value, answer, observation_concept_id, observation_type_concept_id, observation_unit_concept_id, value_as_concept_id, value_as_number) values ('TM1_DRKQTY_RSPS_CD',	'44',	0,		3037705,		44818704,	4045131,		NULL,		NULL) ;



INSERT INTO @NHISNSC_database.OBSERVATION (observation_id, person_id, observation_concept_id, observation_date, observation_datetime, observation_type_concept_id, value_as_number, value_As_string, value_as_concept_id,
										qualifier_concept_id, unit_concept_id, provider_id, visit_occurrence_id, observation_source_value, observation_source_concept_id, unit_source_value, qualifier_source_value)

select		cast(concat(c.master_seq, b.id_value) as bigint) as observation_id,
			a.person_id as person_id,
			b.observation_concept_id as observation_concept_id,
			cast(CONVERT(VARCHAR, a.hchk_year+'0101', 23)as date) as observation_date,
			oservation_time = null,
			b.observation_type_concept_id as observation_type_concept_id,
				CASE WHEN b.answer is not null and b.value_as_number is not null
				then b.value_as_number
				else a.meas_value
				END as value_as_number,
			value_as_string = null,
			b.value_as_concept_id as value_as_concept_id,
			qualifier_source_value = null,
			b.observation_unit_concept_id as unit_concept_id ,
			provider_id = null,
			visit_occurrence_id = c.master_seq,
			a.meas_value as observation_source_value,
			observation_source_concept_id = null,
			unit_source_value = null,
			qualifier_source_Value = null

	from (select hchk_year, person_id, ykiho_gubun_cd, meas_type, meas_value
			from @NHISNSC_rawdata.GJ_VERTICAL) a
		JOIN #observation_mapping09 b 
		on isnull(a.meas_type,'') = isnull(b.meas_type,'') 
			and isnull(a.meas_value,'0') >= isnull(cast(b.answer as char),'0')
		JOIN @NHISNSC_database.SEQ_MASTER c
		on a.person_id = cast(c.person_id as char)
			and a.hchk_year = c.hchk_year
	where (a.meas_value != '' and substring(a.meas_type, 1, 30) in ('TM1_DRKQTY_RSPS_CD') and substring(a.hchk_year, 1, 4) in ('2009', '2010', '2011', '2012', '2013'))
			and c.source_table like 'GJT'
;

/**************************************
 2. Insert categorical drinking data changing from 2009
***************************************/ 
INSERT INTO @NHISNSC_database.OBSERVATION (observation_id, person_id, observation_concept_id, observation_date, observation_datetime, observation_type_concept_id, value_as_number, value_As_string, value_as_concept_id,
										qualifier_concept_id, unit_concept_id, provider_id, visit_occurrence_id, observation_source_value, observation_source_concept_id, unit_source_value, qualifier_source_value)

	select	cast(concat(c.master_seq, b.id_value) as bigint) as observation_id,
			a.person_id as person_id,
			b.observation_concept_id as observation_concept_id,
			cast(CONVERT(VARCHAR, a.hchk_year+'0101', 23)as date) as observation_date,
			oservation_time = null,
			b.observation_type_concept_id as observation_type_concept_id,
				CASE WHEN b.answer is not null and b.value_as_number is not null
				then b.value_as_number
				else a.meas_value
				END as value_as_number,
			value_as_string = null,
			b.value_as_concept_id as value_as_concept_id,
			qualifier_source_value = null,
			b.observation_unit_concept_id as unit_concept_id ,
			provider_id = null,
			visit_occurrence_id = c.master_seq,
			a.meas_value as observation_source_value,
			observation_source_concept_id = null,
			unit_source_value = null,
			qualifier_source_Value = null

	from (select hchk_year, person_id, ykiho_gubun_cd, meas_type, meas_value
			from @NHISNSC_rawdata.GJ_VERTICAL) a
		JOIN #observation_mapping09 b 
		on isnull(a.meas_type,'') = isnull(b.meas_type,'') 
			and isnull(a.meas_value,'0') = isnull(cast(b.answer as char),'0')
		JOIN @NHISNSC_database.SEQ_MASTER c
		on a.person_id = cast(c.person_id as char)
			and a.hchk_year = c.hchk_year
	where (a.meas_value != '' and substring(a.meas_type, 1, 30) in ('DRNK_HABIT_RSPS_CD') and substring(a.hchk_year, 1, 4) in ('2009', '2010', '2011', '2012', '2013'))
			and c.source_table like 'GJT'
;

/*************************************
 2. Pivot rows to columns of JK table
 *************************************/
/*
select STND_Y as hchk_year, person_id, jk_type, jk_value into @NHISNSC_rawdata.JK_VERTICAL
from @NHISNSC_rawdata.@NHIS_JK
unpivot (jk_value for jk_type in ( -- 2 JK variable
        CTRB_PT_TYPE_CD, DFAB_GRD_CD
)) as unpivortn
*/
/**************************************
 2. Insert data of income quantiles
***************************************/ 
INSERT INTO @NHISNSC_database.OBSERVATION (observation_id, person_id, observation_concept_id, observation_date, observation_datetime, observation_type_concept_id, value_as_number, value_As_string, value_as_concept_id,
										qualifier_concept_id, unit_concept_id, provider_id, visit_occurrence_id, observation_source_value, observation_source_concept_id, unit_source_value, qualifier_source_value)


select			cast(concat(c.master_seq, b.id_value) as bigint) as observation_id,		
				a.person_id as person_id,
				b.observation_concept_id as observation_concept_id,
				cast(CONVERT(VARCHAR, a.hchk_year+'0101', 23)as date) as observation_date,
				observation_datetime = null,
				b.observation_type_concept_id as observation_type_concept_id,
				CASE WHEN b.answer is not null and b.value_as_number is not null then b.value_as_number
					else a.jk_value
				END as value_as_number,
				value_as_string = null,
				b.value_as_concept_id as value_as_concept_id,
				qualifier_source_value = null,
				b.observation_unit_concept_id as unit_concept_id,
				provider_id = null,
				visit_occurrence_id = null ,
				a.jk_value as observation_source_value,
				observation_source_concept_id = null,
				unit_source_value = null,
				qualifier_source_Value = null
	from (select * from @NHISNSC_rawdata.JK_VERTICAL where jk_type='CTRB_PT_TYPE_CD') a
				JOIN #observation_mapping b 
				on isnull(a.jk_value,'') = isnull(b.answer,'') 
				JOIN @NHISNSC_database.SEQ_MASTER c
				on a.person_id = cast(c.person_id as char)
				and a.hchk_year = c.stnd_y
	where a.jk_value != '' and b.meas_type = 'CTRB_PT_TYPE_CD' 
			and c.source_table='JKT'
;

drop table #observation_mapping;
drop table #observation_mapping09;


/*****************************************************
					Check the table
*****************************************************/
/*
--------------Before pivot, 29
select distinct meas_type, count(meas_type)
from @NHISNSC_rawdata.@GJ_VERTICAL
where meas_value != ''  and substring(meas_type, 1, 30) in ('HCHK_PMH_CD1', 'HCHK_PMH_CD2', 'HCHK_PMH_CD3','HCHK_APOP_PMH_YN', 'HCHK_HDISE_PMH_YN', 'HCHK_HPRTS_PMH_YN', 
																	'HCHK_DIABML_PMH_YN', 'HCHK_HPLPDM_PMH_YN', 'HCHK_ETCDSE_PMH_YN', 'HCHK_PHSS_PMH_YN', 'FMLY_LIVER_DISE_PATIEN_YN', 'FMLY_HPRTS_PATIEN_YN', 
																	'FMLY_APOP_PATIEN_YN', 'FMLY_HDISE_PATIEN_YN', 'FMLY_DIABML_PATIEN_YN', 'FMLY_CANCER_PATIEN_YN', 'SMK_STAT_TYPE_RSPS_CD', 'SMK_TERM_RSPS_CD', 
																	'DSQTY_RSPS_CD', 'DRNK_HABIT_RSPS_CD', 'TM1_DRKQTY_RSPS_CD', 'EXERCI_FREQ_RSPS_CD', 'CUR_SMK_TERM_RSPS_CD', 'CUR_DSQTY_RSPS_CD', 'PAST_SMK_TERM_RSPS_CD', 'PAST_DSQTY_RSPS_CD', 
																	'MOV20_WEK_FREQ_ID', 'MOV30_WEK_FREQ_ID', 'WLK30_WEK_FREQ_ID')
group by meas_type 
order by meas_type 

*/