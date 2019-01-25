/*
Data Cleansing
 @NHISNSC_rawdata : DB containing NHIS National Sample cohort DB
 @NHISNSC_database : DB for NHIS-NSC in CDM format
 @NHIS_JK: JK table in NHIS NSC
 @NHIS_20T: 20 table in NHIS NSC
 @NHIS_30T: 30 table in NHIS NSC
 @NHIS_40T: 40 table in NHIS NSC
 @NHIS_60T: 60 table in NHIS NSC
 @NHIS_GJ: GJ table in NHIS NSC
*/

/*
-----Person issue update
-- person¿¡´Â 2002³â Ãâ»ýÀ¸·Î ³ª¿ÀÁö¸¸ JK table¿¡´Â age_groupÀÌ 1·Î ³ª¿À´Â »ç¶÷µé(Àß¸øºÐ·ùµÈ»ç¶÷µé), 241
-- 3¹ø, 7¹ø¿¡ ¼ÓÇØ ÀÖÀ½.
-- À§¿Í µ¿ÀÏÇÑ id µéÀ» person table ¿¡¼­ È®ÀÎ
select * 
from @NHISNSC_database.PERSON 
where person_id in (
					SELECT person_id
					FROM @NHISNSC_rawdata.dbo.NHID_JK
					where person_id in (
								  select person_id from @NHISNSC_database.PERSON
								  where year_of_birth = 2002
									)
							and AGE_GROUP = 1 and STND_Y = 2002
					);


--JK ¿øº» Å×ÀÌºí¿¡´Â 2009³â ½Å»ý¾Æ·Î ³ª¿ÀÁö¸¸ person¿¡´Â ¾È ³ª¿À´Â ÄÉÀÌ½ºµé, person table¿¡´Â 2008³â Ãâ»ýÀ¸·Î ³ª¿È, 2
select * from @NHISNSC_database.PERSON
where person_id in (select person_id from @NHISNSC_rawdata.@NHIS_JK
					where age_group = 0 and STND_Y = 2009 and  PERSON_ID not in (
									select person_id from @NHISNSC_database.PERSON
									where year_of_birth = 2009)
					);



-- 241¸í¿¡ ÇØ´çµÇ´Â µ¥ÀÌÅÍ ¼öÁ¤, Ãâ»ý³âµµ¸¦ 2002³â¿¡¼­ 2001³âÀ¸·Î.
update @NHISNSC_database.PERSON
set year_of_birth='2001' where person_id in (
									select person_id
									from @NHISNSC_database.PERSON 
									where person_id in (
											SELECT person_id
											FROM @NHISNSC_rawdata.dbo.NHID_JK
											where person_id in (
														  select person_id from @NHISNSC_database.PERSON
														  where year_of_birth = 2002
															)
											and AGE_GROUP = 1 and STND_Y = 2002
														)
									);

-- 2008, 2009³â¿¡ Ãâ»ýÇÑ 12°ÇÀÇ °Ç¼öµéÀº ½ÇÁ¦·Î 2008³â¿¡ ÅÂ¾î³­ °ÍÀ» JK ¿¡¼­ È®ÀÎ. person¿¡´Â 2008·Î ºÐ·ùµÇ¾ú±â¿¡ º°µµÀÇ update ÇÏÁö ¾ÊÀ½

----- 2°ÇÀÇ id¿¡ ÇÑÇÏ¿© sex°¡ µÚ¹Ù²ï ¿¬µµ°¡ ÀÖ´Â °ÍÀ» È®ÀÎ
--person¿¡¼­ È®ÀÎ
select * from @NHISNSC_database.PERSON
where person_id in (
					select person_id from @NHISNSC_rawdata.@NHIS_JK
					where sex=1 and STND_Y=2011 and person_id in (
								select PERSON_ID from @NHISNSC_rawdata.@NHIS_JK
								where sex=2 and STND_Y=2012
								)
					);
-- 95292839ÀÇ gender °ªÀÌ ¿©¼º(8532)ÀÌ±â¿¡ ³²¼º(8507)·Î º¯°æ
update @NHISNSC_database.PERSON
set gender_concept_id='8507' 
where person_id = 95292839;

update @NHISNSC_database.PERSON
set gender_source_value=1
where person_id=95292839;

*/