/**************************************
 @Author: 이성원
 @Date: 2017.01.19
 
 @Database: NHIS_NSC (16호 서버)
 @Description: 새로운 concept 등록
 @관련 Table: CONCEPT
***************************************/

/**************************************
 CARE_SITE.place_of_service

  * NHIS_NSC 사용 코드
	1) 10:종합병원
	2) 20~27:일반병원,정신병원
	3) 28:요양병원
	4) 29: 정신요양병원
	5) 30~39: 의원
	6) 40~49: 치과병원
	7) 50~59: 치과의원
	8) 60~69: 조산원
	9) 70: 보건소
	10) 71~72: 보건지소
	11) 73~74: 보건진료소
	12) 75~76: 모자보건센터
	13) 77: 보건의료원
	14) 80~89: 약국
	15) 91: 한방종합병원
	16) 92: 한방병원
	17) 93~97: 한의원
	18) 98~99: 한약방
	
  * 생성 concept_id
	1) 82020101: general hospital (종합병원)
	2) 82020102: hospital (일반병원)
	3) 82020103: care hospital (요양병원)
	4) 82020104: mental care hospital (정신요양병원)
	5) 82020105: clinic (의원)
	6) 82020106: dental hospital (치과 병원)
	7) 82020107: dental clinic (치과 의원)
	8) 82020108: maternity center (조산원)
	9) 82020109: public health center (보건소)
	10) 82020110: branch of public health center (보건지소)
	11) 82020111: health care center (보건진료소)
	12) 82020112: mother and child health center (모자보건센터)
	13) 82020113: health medical center (보건의료원)
	14) 82020114: pharmacy (약국)
	15) 82020115: oriental general hospital (한방종합병원)
	16) 82020116: oriental hospital (한방병원)
	17) 82020117: oriental clinic (한의원)
	18) 82020118: galenic pharmacy (한약방)
***************************************/


insert into concept 
	(concept_id, concept_name, domain_id, vocabulary_id, concept_class_id, 
	standard_concept, concept_code, valid_start_date, valid_end_date, invalid_reason)
values
	('82020101', 'General hospital (NHIS-NSC)', 'Place of Service', 'NHIS-NSC', 'Location', 'K', '10', '20170101', '20991231', null),
	('82020102', 'Hospital (NHIS-NSC)', 'Place of Service', 'NHIS-NSC', 'Location', 'K', '20-27', '20170101', '20991231', null),
	('82020103', 'Care hospital (NHIS-NSC)', 'Place of Service', 'NHIS-NSC', 'Location', 'K', '28', '20170101', '20991231', null),
	('82020104', 'Mental care hospital (NHIS-NSC)', 'Place of Service', 'NHIS-NSC', 'Location', 'K', '29', '20170101', '20991231', null),
	('82020105', 'Clinic (NHIS-NSC)', 'Place of Service', 'NHIS-NSC', 'Location', 'K', '30-39', '20170101', '20991231', null),
	('82020106', 'Dental hospital (NHIS-NSC)', 'Place of Service', 'NHIS-NSC', 'Location', 'K', '40-49', '20170101', '20991231', null),
	('82020107', 'Dental clinic (NHIS-NSC)', 'Place of Service', 'NHIS-NSC', 'Location', 'K', '50-59', '20170101', '20991231', null),
	('82020108', 'Maternity center (NHIS-NSC)', 'Place of Service', 'NHIS-NSC', 'Location', 'K', '60-69', '20170101', '20991231', null),
	('82020109', 'Public health center (NHIS-NSC)', 'Place of Service', 'NHIS-NSC', 'Location', 'K', '70', '20170101', '20991231', null),
	('82020110', 'Branch of public health center (NHIS-NSC)', 'Place of Service', 'NHIS-NSC', 'Location', 'K', '71-72', '20170101', '20991231', null),
	('82020111', 'Health care center (NHIS-NSC)', 'Place of Service', 'NHIS-NSC', 'Location', 'K', '73-34', '20170101', '20991231', null),
	('82020112', 'Mother and child health center (NHIS-NSC)', 'Place of Service', 'NHIS-NSC', 'Location', 'K', '75-76', '20170101', '20991231', null),
	('82020113', 'Health medical center (NHIS-NSC)', 'Place of Service', 'NHIS-NSC', 'Location', 'K', '77', '20170101', '20991231', null),
	('82020114', 'Pharmacy (NHIS-NSC)', 'Place of Service', 'NHIS-NSC', 'Location', 'K', '80-89', '20170101', '20991231', null),
	('82020115', 'Oriental general hospital (NHIS-NSC)', 'Place of Service', 'NHIS-NSC', 'Location', 'K', '91', '20170101', '20991231', null),
	('82020116', 'Oriental hospital (NHIS-NSC)', 'Place of Service', 'NHIS-NSC', 'Location', 'K', '92', '20170101', '20991231', null),
	('82020117', 'Oriental clinic (NHIS-NSC)', 'Place of Service', 'NHIS-NSC', 'Location', 'K', '93-97', '20170101', '20991231', null),
	('82020118', 'Galenic pharmacy (NHIS-NSC)', 'Place of Service', 'NHIS-NSC', 'Location', 'K', '98-99', '20170101', '20991231', null);
