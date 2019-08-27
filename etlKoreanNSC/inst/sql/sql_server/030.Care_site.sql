/**************************************
 --encoding : UTF-8
 --Author: 이성원
 --Date: 2018.08.22
 
 @NHISNSC_rawdata : DB containing NHIS National Sample cohort DB
 @NHISNSC_database : DB for NHIS-NSC in CDM format
 @NHIS_JK: JK table in NHIS NSC
 @NHIS_20T: 20 table in NHIS NSC
 @NHIS_30T: 30 table in NHIS NSC
 @NHIS_40T: 40 table in NHIS NSC
 @NHIS_60T: 60 table in NHIS NSC
 @NHIS_GJ: GJ table in NHIS NSC
 @NHIS_YK: YK table in NHIS NSC
 
 --Description: Create Care_site table
			   1) In sample cohort DB, medical institution data are inserted as duplicated by years, which makes it possible to track the change of established division, location and etc..
			    In CDM, however, medical institution should be unique, so the latest medical institution data would be converted
			   2) place of service: Considering sepciality of South Korea, make new concepts (Ref) ETL definition document)
 --Generating Table: CARE_SITE
***************************************/

/**************************************
 1. Create table
***************************************/  
/*
Create table @NHISNSC_database.CARE_SITE (
	care_site_id 	integer, --primary key,
	care_site_name	varchar(255),
	place_of_service_concept_id	integer,
	location_id	integer,
	care_site_source_value	varchar(50),
	place_of_service_source_value	varchar(50)
);
*/

/**************************************
 2. Insert data
	: place_of_service_source_value - Medical Institution, type code/established division 
									- If established divisin code is consist with one number then add 0 in front of it.
***************************************/  

IF OBJECT_ID('tempdb..#temp', 'U') IS NOT NULL
	DROP TABLE #temp;

SELECT a.ykiho_id,
	null as care_site_name,
	case when a.ykiho_gubun_cd='10' then 4068130 --(Tertiary care hospital) 
		 when a.ykiho_gubun_cd between '20' and '27' then 4318944 --Hospital
		 when a.ykiho_gubun_cd='28' then 82020103 --Nursing home
		 when a.ykiho_gubun_cd='29' then 4268912 --Psychiatric hospital 
		 when a.ykiho_gubun_cd between '30' and '39' then 82020105 --clinic
		 when a.ykiho_gubun_cd between '40' and '49' then 82020106 --dental hospital
		 when a.ykiho_gubun_cd between '50' and '59' then 82020107 --dental clinic
		 when a.ykiho_gubun_cd between '60' and '69' then 82020108 --midwife center
		 when a.ykiho_gubun_cd='70' then 82020109 --public health center
		 when a.ykiho_gubun_cd between '71' and '72' then 82020110 --public health branch center
		 when a.ykiho_gubun_cd between '73' and '74' then 82020111 --public health clinic 
		 when a.ykiho_gubun_cd between '75' and '76' then 82020112 --mother and child health care center
		 when a.ykiho_gubun_cd='77' then 82020113 --public medical clinic 
		 when a.ykiho_gubun_cd between '80' and '89' then 4131032 --Pharmacy
		 when a.ykiho_gubun_cd='91' then 82020115 --traditional medicine tertiary care hospital
		 when a.ykiho_gubun_cd='92' then 82020116 --traditional medicine hospital
		 when a.ykiho_gubun_cd between '93' and '97' then 82020117 --traditional medicine clinic
		 when a.ykiho_gubun_cd between '98' and '99' then 82020118 --traditional pharmacy
	end as place_of_service_concept_id,
	a.ykiho_sido as location_id,
	a.ykiho_id as care_site_source_value,
	(a.ykiho_gubun_cd + '/' + (case when len(a.org_type) = 1 then '0' + org_type else org_type end)) as place_of_service_source_value
into #temp

FROM @NHISNSC_rawdata.@NHIS_YK a, (select ykiho_id, max(stnd_y) as max_stnd_y
	from @NHISNSC_rawdata.@NHIS_YK c
	group by ykiho_id) b
where a.ykiho_id=b.ykiho_id
and a.stnd_y=b.max_stnd_y
;

INSERT INTO @NHISNSC_database.CARE_SITE
select * from #temp
group by YKIHO_ID, care_site_name, place_of_service_concept_id, location_id, care_site_source_value, place_of_service_source_value
;

drop table #temp;