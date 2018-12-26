/**************************************
 --encoding : UTF-8
 --Author: OHDSI
  
@NHISNSC_rawdata : DB containing NHIS National Sample cohort DB
@NHISNSC_database : DB for NHIS-NSC in CDM format
 
 --Description: OHDSI에서 생성한 dose_era 생성 쿼리
 --Generating Table: DOSE_ERA
***************************************/

/**************************************
 1. dose_era 테이블 생성
***************************************/ 
 CREATE TABLE @NHISNSC_database.DOSE_ERA (
     dose_era_id					INTEGER	 identity(1,1)    NOT NULL , 
     person_id						INTEGER     NOT NULL ,
     drug_concept_id				INTEGER   NOT NULL ,
     unit_concept_id				INTEGER      NOT NULL ,
     dose_value						float  NOT NULL ,
     dose_era_start_date			DATE 		NOT	NULL, 
	 dose_era_end_date				DATE 		NOT	NULL
);


/**************************************
 2. 1단계: 필요 데이터 조회
***************************************/ 

--------------------------------------------#cteDrugTarget, 457250250, 00:03:20
SELECT
	d.drug_exposure_id
	, d.person_id
	, c.concept_id AS ingredient_concept_id
	, d.dose_unit_concept_id AS unit_concept_id
	, d.effective_drug_dose AS dose_value
	, d.drug_exposure_start_date
	, d.days_supply AS days_supply
	, COALESCE(d.drug_exposure_end_date, DATEADD(DAY, d.days_supply, d.drug_exposure_start_date), DATEADD(DAY, 1, drug_exposure_start_date)) AS drug_exposure_end_date
INTO #cteDrugTarget 
FROM @NHISNSC_database.DRUG_EXPOSURE d
	 JOIN @NHISNSC_database.CONCEPT_ANCESTOR ca ON ca.descendant_concept_id = d.drug_concept_id
	 JOIN @NHISNSC_database.CONCEPT c ON ca.ancestor_concept_id = c.concept_id
	 WHERE c.vocabulary_id = 'RxNorm'
	 AND c.concept_class_ID = 'Ingredient';
	
	
--------------------------------------------#cteEndDates, 217802424, 00:13:32
SELECT
	person_id
	, ingredient_concept_id
	, unit_concept_id
	, dose_value
	, DATEADD( DAY, -30, event_date) AS end_date
INTO #cteEndDates FROM
(
	SELECT
		person_id
		, ingredient_concept_id
		, unit_concept_id
		, dose_value
		, event_date
		, event_type
		, MAX(start_ordinal) OVER (PARTITION BY person_id, ingredient_concept_id, unit_concept_id, dose_value ORDER BY event_date, event_type ROWS unbounded preceding) AS start_ordinal
		, ROW_NUMBER() OVER (PARTITION BY person_id, ingredient_concept_id, unit_concept_id, dose_value ORDER BY event_date, event_type) AS overall_ord
	FROM
	(
		SELECT
			person_id
			, ingredient_concept_id
			, unit_concept_id
			, dose_value
			, drug_exposure_start_date AS event_date
			, -1 AS event_type, ROW_NUMBER() OVER(PARTITION BY person_id, ingredient_concept_id, unit_concept_id, dose_value ORDER BY drug_exposure_start_date) AS start_ordinal
		FROM #cteDrugTarget 

		UNION ALL

		SELECT
			person_id
			, ingredient_concept_id
			, unit_concept_id
			, dose_value
			, DATEADD(DAY, 30, drug_exposure_end_date) AS drug_exposure_end_date
			, 1 AS event_type
			, NULL
		FROM #cteDrugTarget
	) RAWDATA
) e
WHERE (2 * e.start_ordinal) - e.overall_ord = 0;

--------------------------------------------#cteDoseEraEnds, 457250250, 00:10:55
SELECT
	dt.person_id
	, dt.ingredient_concept_id as drug_concept_id
	, dt.unit_concept_id 
	, dt.dose_value
	, dt.drug_exposure_start_date
	, MIN(e.end_date) AS dose_era_end_date
into #cteDoseEraEnds FROM #cteDrugTarget dt
JOIN #cteEndDates e
ON dt.person_id = e.person_id AND dt.ingredient_concept_id = e.ingredient_concept_id AND e.end_date >= dt.drug_exposure_start_date
--AND dt.unit_concept_id = e.unit_concept_id AND dt.dose_value = e.dose_value		--unit_concpet_id 와 dose_value 는 양쪽 테이블 모두에서 전부 null 이기에 제외시킴
GROUP BY
	dt.drug_exposure_id
	, dt.person_id
	, dt.ingredient_concept_id
	, dt.unit_concept_id
	, dt.dose_value
	, dt.drug_exposure_start_date;
	
	
/**************************************
 3. 2단계: dose_era에 데이터 입력
***************************************/ 

-- 임시테이블을 만들 당시 이미 drug exposure 에는 unit concept id 와 dose value 가 전부 null로 입력되있는데 현재 테이블에서는 not null로 설정되어있다.

INSERT INTO @NHISNSC_database.dose_era (person_id, drug_concept_id, unit_concept_id, dose_value, dose_era_start_date, dose_era_end_date)
SELECT
	person_id
	, drug_concept_id
	, unit_concept_id
	, dose_value
	, MIN(drug_exposure_start_date) AS dose_era_start_date
	, dose_era_end_date
	from #cteDoseEraEnds
GROUP BY person_id, drug_concept_id, unit_concept_id, dose_value, dose_era_end_date
ORDER BY person_id, drug_concept_id;
