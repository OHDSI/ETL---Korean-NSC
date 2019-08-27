DQevaluation <- function(NHISNSC_rawdata,
                         NHISNSC_database,
                         Mapping_database,
                         NHIS_JK,
                         NHIS_20T,
                         NHIS_30T,
                         NHIS_40T,
                         NHIS_60T,
                         NHIS_GJ,
                         NHIS_YK,
                         
                         connection,
                         outputFolder,
                         
                         drug_exposure = TRUE,
                         procedure_occurrent = TRUE,
                         device_exposure = TRUE,
                         condition_occurrence = TRUE,
                         measurement = TRUE
                         
                         
){
    
    
    ## Drug_exposure
    if(drug_exposure){
        
        ## Mapping Table
        SqlMapping <- c("
                        IF Object_id('tempdb..#mapping_table', 'U') IS NOT NULL 
                        DROP TABLE #mapping_table; 
                        SELECT a.source_code, a.target_concept_id, a.domain_id, Replace(a.invalid_reason, '', NULL) AS invalid_reason 
                        INTO   #mapping_table 
                        FROM   nhis_nsc_new_mapping.dbo.source_to_concept_map a 
                        JOIN nhis_nsc_new_mapping.dbo.concept b 
                        ON a.target_concept_id = b.concept_id 
                        WHERE  a.invalid_reason IS NULL 
                        AND b.invalid_reason IS NULL 
                        AND a.domain_id = 'drug';
                        ")
        DatabaseConnector::executeSql(connection,SqlMapping)
        
        
        ## 30T Mappied
        SqlMappied30T <- c("
                           SELECT multimappied, Count(*) as count -- 104,292,115
                           FROM   (SELECT master_seq, Count(*) AS multimappied -- 104,292,115 
                           FROM   (SELECT master_seq, div_cd 
                           FROM   nhisnsc2013original.dbo.nhid_gy30_t1 x, 
                           (SELECT master_seq, person_id, key_seq, seq_no 
                           FROM   nhis_nsc_v5_3_1.dbo.seq_master 
                           WHERE  source_table = '130') y, 
                           nhisnsc2013original.dbo.nhid_gy20_t1 z 
                           WHERE  x.key_seq = y.key_seq 
                           AND x.seq_no = y.seq_no 
                           AND y.key_seq = z.key_seq 
                           AND y.person_id = z.person_id) a, 
                           #mapping_table b 
                           WHERE  a.div_cd = b.source_code 
                           GROUP  BY master_seq) c 
                           GROUP  BY multimappied 
                           ")
        ConvertedDrugCountByMappied30T <- DatabaseConnector::querySql(connection,SqlMappied30T)
        
        ## 30T Unmappied
        SqlUnMappied30T <- c("
                             SELECT Count(*) -- 4,400,052
                             FROM   (SELECT master_seq, div_cd 
                             FROM   (SELECT * 
                             FROM   nhisnsc2013original.dbo.nhid_gy30_t1 
                             WHERE  div_type_cd IN ( '3', '4', '5' )) x, 
                             (SELECT master_seq, person_id, key_seq, seq_no 
                             FROM   nhis_nsc_v5_3_1.dbo.seq_master 
                             WHERE  source_table = '130') y, 
                             nhisnsc2013original.dbo.nhid_gy20_t1 z 
                             WHERE  x.key_seq = y.key_seq 
                             AND x.seq_no = y.seq_no 
                             AND y.key_seq = z.key_seq 
                             AND y.person_id = z.person_id) a 
                             WHERE  a.div_cd NOT IN (SELECT source_code 
                             FROM   #mapping_table)
                             ")
        ConvertedDrugCountByUnMappied30T <- DatabaseConnector::querySql(connection,SqlUnMappied30T)
        
        ## 30T Raw
        SqlRawToDrugBy30T_1 <- c("
                                 SELECT div_type_cd, multimappied, Count(*) AS count 
                                 FROM   (SELECT key_seq, seq_no, div_type_cd, Count(*) AS multimappied 
                                 FROM   (SELECT * 
                                 FROM   (SELECT * 
                                 FROM   nhisnsc2013original.dbo.nhid_gy30_t1 
                                 WHERE  div_type_cd NOT IN ( '3', '4', '5' )) a 
                                 JOIN #mapping_table b 
                                 ON a.div_cd = b.source_code) c -- 1:1 mapping  
                                 GROUP  BY key_seq, seq_no, div_type_cd) d 
                                 GROUP  BY div_type_cd, multimappied 
                                 ")
        SqlRawToDrugBy30T_2 <- c("
                                 SELECT div_type_cd, 1 as multimappied, Count(*) as count 
                                 FROM   (SELECT * 
                                 FROM   nhisnsc2013original.dbo.nhid_gy30_t1 
                                 WHERE  div_type_cd IN ( '3', '4', '5' )) a
                                 GROUP by div_type_cd
                                 ")
        HowManyContainDrugByMappied30T <- DatabaseConnector::querySql(connection,SqlRawToDrugBy30T_1)
        HowManyContainDrugByMappied30T <- rbind(HowManyContainDrugByMappied30T,DatabaseConnector::querySql(connection,SqlRawToDrugBy30T_2))
        
        
        ## 60T Mappied
        SqlMappied60T <- c("
                           SELECT multimappied, Count(*) -- 384,321,194
                           FROM   (SELECT master_seq, Count(*) AS multimappied -- 
                           FROM   (SELECT master_seq, div_cd 
                           FROM   nhisnsc2013original.dbo.nhid_gy60_t1 x, 
                           (SELECT master_seq, person_id, key_seq, seq_no 
                           FROM   nhis_nsc_v5_3_1.dbo.seq_master 
                           WHERE  source_table = '160') y, 
                           nhisnsc2013original.dbo.nhid_gy20_t1 z 
                           WHERE  x.key_seq = y.key_seq 
                           AND x.seq_no = y.seq_no 
                           AND y.key_seq = z.key_seq 
                           AND y.person_id = z.person_id) a, 
                           #mapping_table b 
                           WHERE  a.div_cd = b.source_code 
                           GROUP  BY master_seq) c 
                           GROUP  BY multimappied
                           ")
        ConvertedDrugCountByMappied60T <- DatabaseConnector::querySql(connection,SqlMappied60T)
        
        ## 60T Unmappied
        SqlUnMappied60T <- c("
                             SELECT Count(*) -- 
                             FROM   (SELECT master_seq, div_cd 
                             FROM   (SELECT * 
                             FROM   nhisnsc2013original.dbo.nhid_gy60_t1 
                             WHERE  div_type_cd IN ( '3', '4', '5' )) x, 
                             (SELECT master_seq, person_id, key_seq, seq_no 
                             FROM   nhis_nsc_v5_3_1.dbo.seq_master 
                             WHERE  source_table = '160') y, 
                             nhisnsc2013original.dbo.nhid_gy20_t1 z 
                             WHERE  x.key_seq = y.key_seq 
                             AND x.seq_no = y.seq_no 
                             AND y.key_seq = z.key_seq 
                             AND y.person_id = z.person_id) a 
                             WHERE  a.div_cd NOT IN (SELECT source_code 
                             FROM   #mapping_table) 
                             ")
        ConvertedDrugCountByUnMappied60T <- DatabaseConnector::querySql(connection,SqlUnMappied60T)
        
        ## 60T Raw
        SqlRawToDrugBy60T_1 <- c("
                                 SELECT div_type_cd, multimappied, Count(*) AS count 
                                 FROM   (SELECT key_seq, seq_no, div_type_cd, Count(*) AS multimappied 
                                 FROM   (SELECT * 
                                 FROM   (SELECT * 
                                 FROM   nhisnsc2013original.dbo.nhid_gy60_t1 
                                 WHERE  div_type_cd NOT IN ( '3', '4', '5' )) a 
                                 JOIN #mapping_table b 
                                 ON a.div_cd = b.source_code) c -- 1:1 mapping  
                                 GROUP  BY key_seq, seq_no, div_type_cd) d 
                                 GROUP  BY div_type_cd, multimappied
                                 ")
        SqlRawToDrugBy60T_2 <- c("
                                 SELECT div_type_cd, 1 as multimappied, Count(*) as COUNT 
                                 FROM   (SELECT * 
                                 FROM   nhisnsc2013original.dbo.nhid_gy60_t1 
                                 WHERE  div_type_cd IN ( '3', '4', '5' )) a
                                 GROUP by div_type_cd
                                 ")
        HowManyContainDrugByMappied60T <- DatabaseConnector::querySql(connection,SqlRawToDrugBy60T_1)
        HowManyContainDrugByMappied60T <- rbind(HowManyContainDrugByMappied60T,DatabaseConnector::querySql(connection,SqlRawToDrugBy60T_2))
        
        # DrugExposureDQ <- list()
        # DrugExposureDQ[[1]] <- c(ConvertedDrugCountByMappied30T, ConvertedDrugCountByUnMappied30T)
        # DrugExposureDQ[[2]] <- c(ConvertedDrugCountByMappied60T, ConvertedDrugCountByUnMappied60T)
        # DrugExposureDQ[[3]] <- c(HowManyContainDrugByMappied30T)
        # DrugExposureDQ[[4]] <- c(HowManyContainDrugByMappied60T)
        # DrugExposureDQ
        
    } ## DrugTable : 20T join으로 인해 4건 차이남, Death기간으로 인해 delete 발생, Cost 적재시 delete 발생
    
    
    
    
    ## Procedure_occurrence
    if(procedure_occurrence){
        
        ## Mapping Table
        SqlMapping <- c("
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
                        from NHIS_NSC_new_mapping.dbo.source_to_concept_map a join NHIS_NSC_new_mapping.dbo.CONCEPT b on a.target_concept_id=b.concept_id
                        where a.invalid_reason is null and b.invalid_reason is null and a.domain_id='procedure';
                        
                        select * into #pro from NHIS_NSC_new_mapping.dbo.source_to_concept_map where domain_id='procedure';
                        select * into #five from NHIS_NSC_new_mapping.dbo.source_to_concept_map where domain_id='device';
                        
                        select a.*
                        into #duplicated
                        from #pro a, #five b
                        where a.source_code=b.source_code
                        and a.invalid_reason is null and b.invalid_reason is null;
                        
                        select * into #mapping_table from #temp
                        where source_code not in (select source_code from #duplicated);
                        
                        drop table #pro, #five, #temp;
                        ")
        DatabaseConnector::executeSql(connection,SqlMapping)
        
        
        ## 30T Mappied
        SqlMappied30T <- c("
                           SELECT Count(*) -- 234,624,188
                           FROM   (SELECT x.div_cd, x.div_type_cd 
                           FROM   (SELECT * 
                           FROM   nhisnsc2013original.dbo.nhid_gy30_t1 
                           WHERE  div_type_cd NOT IN ( '3', '4', '5', '7', '8' )) x, 
                           (SELECT * FROM   nhis_nsc_v5_3_1.dbo.seq_master 
                           WHERE  source_table = '130') y 
                           WHERE  x.key_seq = y.key_seq 
                           AND x.seq_no = y.seq_no) a, 
                           #mapping_table b -- 1:n mappied
                           WHERE  LEFT(a.div_cd, 5) = b.source_code
                           ")
        ConvertedProcCountByMappied30T <- DatabaseConnector::querySql(connection,SqlMappied30T)
        
        ## 30T Dup Mappied
        SqlDupMappied30T <- c("
                              SELECT Count(*) -- 3,448,362 
                              FROM   (SELECT x.div_cd, x.div_type_cd 
                              FROM   (SELECT * 
                              FROM   nhisnsc2013original.dbo.nhid_gy30_t1 
                              WHERE  div_type_cd IN ( '1', '2' )) x, 
                              (SELECT * FROM   nhis_nsc_v5_3_1.dbo.seq_master 
                              WHERE  source_table = '130') y 
                              WHERE  x.key_seq = y.key_seq 
                              AND x.seq_no = y.seq_no) a, 
                              #duplicated b 
                              WHERE  LEFT(a.div_cd, 5) = b.source_code
                              ")
        ConvertedProcCountByDupMappied30T <- DatabaseConnector::querySql(connection,SqlDupMappied30T)
        
        ## 30T UnMappied
        SqlUnMappied30T <- c("
                             SELECT Count(*) -- 214,373,129
                             FROM   (SELECT x.div_cd, x.div_type_cd 
                             FROM   (SELECT * 
                             FROM   nhisnsc2013original.dbo.nhid_gy30_t1 
                             WHERE  div_type_cd IN ( '1', '2' )) x, 
                             (SELECT *
                             FROM   nhis_nsc_v5_3_1.dbo.seq_master 
                             WHERE  source_table = '130') y 
                             WHERE  x.key_seq = y.key_seq 
                             AND x.seq_no = y.seq_no) a 
                             WHERE  LEFT(a.div_cd, 5) NOT IN (SELECT source_code 
                             FROM   #duplicated 
                             UNION ALL 
                             SELECT source_code 
                             FROM   #mapping_table)
                             ")
        ConvertedProcCountByUnMappied30T <- DatabaseConnector::querySql(connection,SqlUnMappied30T)
        
        ## 30T Raw
        SqlRawToProcBy30T_1 <- c("
                                 SELECT div_type_cd, multimappied, Count(*) AS COUNT 
                                 FROM   (SELECT key_seq, seq_no, div_type_cd, Count(*) AS multimappied 
                                 FROM   (SELECT * 
                                 FROM   (SELECT * 
                                 FROM   nhisnsc2013original.dbo.nhid_gy30_t1 
                                 WHERE  div_type_cd NOT IN ( '3', '4', '5', '7', '8' )) a, #mapping_table b 
                                 WHERE  LEFT(a.div_cd, 5) = b.source_code) c 
                                 GROUP  BY key_seq, seq_no, div_type_cd) d 
                                 GROUP  BY div_type_cd, multimappied 
                                 ")
        SqlRawToProcBy30T_2 <- c("
                                 SELECT div_type_cd, multimappied, Count(*) AS COUNT 
                                 FROM   (SELECT key_seq, seq_no, div_type_cd, Count(*) AS multimappied 
                                 FROM   (SELECT * 
                                 FROM   (SELECT * 
                                 FROM   nhisnsc2013original.dbo.nhid_gy30_t1 
                                 WHERE  div_type_cd IN ( '1', '2' )) a, #duplicated b 
                                 WHERE  LEFT(a.div_cd, 5) = b.source_code) c 
                                 GROUP  BY key_seq, seq_no, div_type_cd) d 
                                 GROUP  BY div_type_cd, multimappied 
                                 ")
        SqlRawToProcBy30T_3 <- c("
                                 SELECT div_type_cd, multimappied, Count(*) AS COUNT 
                                 FROM   (SELECT key_seq, seq_no, div_type_cd, Count(*) AS multimappied 
                                 FROM   (SELECT * 
                                 FROM   (SELECT * 
                                 FROM   nhisnsc2013original.dbo.nhid_gy30_t1 
                                 WHERE  div_type_cd IN ( '1', '2' )) a
                                 WHERE  LEFT(a.div_cd, 5) NOT IN (SELECT source_code FROM #duplicated UNION ALL SELECT source_code FROM #mapping_table)) c 
                                 GROUP  BY key_seq, seq_no, div_type_cd) d 
                                 GROUP  BY div_type_cd, multimappied 
                                 ")
        HowManyContainProcByMappied30T <- DatabaseConnector::querySql(connection,SqlRawToProcBy30T_1)
        HowManyContainProcByMappied30T <- rbind(HowManyContainProcByMappied30T,DatabaseConnector::querySql(connection,SqlRawToProcBy30T_2))
        HowManyContainProcByMappied30T <- rbind(HowManyContainProcByMappied30T,DatabaseConnector::querySql(connection,SqlRawToProcBy30T_3))
        
        
        ## 60T Mappied
        SqlMappied60T <- c("
                           SELECT Count(*) -- 8,785
                           FROM   (SELECT x.div_cd, x.div_type_cd 
                           FROM   (SELECT * 
                           FROM   nhisnsc2013original.dbo.nhid_gy60_t1 
                           WHERE  div_type_cd NOT IN ( '3', '4', '5', '7', '8' )) x, 
                           (SELECT * 
                           FROM   nhis_nsc_v5_3_1.dbo.seq_master 
                           WHERE  source_table = '160') y 
                           WHERE  x.key_seq = y.key_seq 
                           AND x.seq_no = y.seq_no) a, 
                           #mapping_table b 
                           WHERE  LEFT(a.div_cd, 5) = b.source_code
                           ")
        ConvertedProcCountByMappied60T <- DatabaseConnector::querySql(connection,SqlMappied60T)
        
        ## 60T Dup Mappied
        SqlDupMappied60T <- c("
                              SELECT Count(*) -- 5 
                              FROM   (SELECT x.div_cd, x.div_type_cd 
                              FROM   (SELECT * 
                              FROM   nhisnsc2013original.dbo.nhid_gy60_t1 
                              WHERE  div_type_cd IN ( '1', '2' )) x, 
                              (SELECT *
                              FROM   nhis_nsc_v5_3_1.dbo.seq_master 
                              WHERE  source_table = '160') y 
                              WHERE  x.key_seq = y.key_seq 
                              AND x.seq_no = y.seq_no) a, 
                              #duplicated b 
                              WHERE  LEFT(a.div_cd, 5) = b.source_code 
                              ")
        ConvertedProcCountByDupMappied60T <- DatabaseConnector::querySql(connection,SqlDupMappied60T)
        
        ## 60T UnMappied
        SqlUnMappied <- c("
                          SELECT Count(*) -- 25,286
                          FROM   (SELECT x.div_cd, x.div_type_cd 
                          FROM   (SELECT * 
                          FROM   nhisnsc2013original.dbo.nhid_gy60_t1 
                          WHERE  div_type_cd IN ( '1', '2' )) x, 
                          (SELECT *
                          FROM   nhis_nsc_v5_3_1.dbo.seq_master 
                          WHERE  source_table = '160') y 
                          WHERE  x.key_seq = y.key_seq 
                          AND x.seq_no = y.seq_no) a 
                          WHERE  LEFT(a.div_cd, 5) NOT IN (SELECT source_code 
                          FROM   #duplicated 
                          UNION ALL 
                          SELECT source_code 
                          FROM   #mapping_table)
                          ")
        ConvertedProcCountByUnMappied60T <- DatabaseConnector::querySql(connection,SqlUnMappied)
        
        ## 60T Raw
        SqlRawToProcBy60T_1 <- c("
                                 SELECT div_type_cd, multimappied, Count(*) AS COUNT 
                                 FROM   (SELECT key_seq, seq_no, div_type_cd, Count(*) AS multimappied 
                                 FROM   (SELECT * 
                                 FROM   (SELECT * 
                                 FROM   nhisnsc2013original.dbo.nhid_gy60_t1 
                                 WHERE  div_type_cd NOT IN ( '3', '4', '5', '7', '8' )) a, #mapping_table b 
                                 WHERE  LEFT(a.div_cd, 5) = b.source_code) c 
                                 GROUP  BY key_seq, seq_no, div_type_cd) d 
                                 GROUP  BY div_type_cd, multimappied 
                                 ")
        SqlRawToProcBy60T_2 <- c("
                                 SELECT div_type_cd, multimappied, Count(*) AS COUNT 
                                 FROM   (SELECT key_seq, seq_no, div_type_cd, Count(*) AS multimappied 
                                 FROM   (SELECT * 
                                 FROM   (SELECT * 
                                 FROM   nhisnsc2013original.dbo.nhid_gy60_t1 
                                 WHERE  div_type_cd IN ( '1', '2' )) a, #duplicated b 
                                 WHERE  LEFT(a.div_cd, 5) = b.source_code) c 
                                 GROUP  BY key_seq, seq_no, div_type_cd) d 
                                 GROUP  BY div_type_cd, multimappied 
                                 ")
        SqlRawToProcBy60T_3 <- c("
                                 SELECT div_type_cd, multimappied, Count(*) AS COUNT 
                                 FROM   (SELECT key_seq, seq_no, div_type_cd, Count(*) AS multimappied 
                                 FROM   (SELECT * 
                                 FROM   (SELECT * 
                                 FROM   nhisnsc2013original.dbo.nhid_gy60_t1 
                                 WHERE  div_type_cd IN ( '1', '2' )) a
                                 WHERE  LEFT(a.div_cd, 5) NOT IN (SELECT source_code FROM #duplicated UNION ALL SELECT source_code FROM #mapping_table)) c 
                                 GROUP  BY key_seq, seq_no, div_type_cd) d 
                                 GROUP  BY div_type_cd, multimappied 
                                 ")
        HowManyContainProcByMappied60T <- DatabaseConnector::querySql(connection,SqlRawToProcBy60T_1)
        HowManyContainProcByMappied60T <- rbind(HowManyContainProcByMappied60T,DatabaseConnector::querySql(connection,SqlRawToProcBy60T_2))
        HowManyContainProcByMappied60T <- rbind(HowManyContainProcByMappied60T,DatabaseConnector::querySql(connection,SqlRawToProcBy60T_3))
        
    }
    
    
    
    
    ## Device_exposure
    if(device_exposure){
        
        ## Mapping Table
        SqlMapping <- c("
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
                        from NHIS_NSC_new_mapping.dbo.source_to_concept_map a join NHIS_NSC_new_mapping.dbo.CONCEPT b on a.target_concept_id=b.concept_id
                        where a.invalid_reason is null and b.invalid_reason is null and a.domain_id='device';
                        
                        select * into #device from NHIS_NSC_new_mapping.dbo.source_to_concept_map where domain_id='device';
                        select * into #five from NHIS_NSC_new_mapping.dbo.source_to_concept_map where domain_id='procedure';
                        
                        select a.*
                        into #duplicated
                        from #device a, #five b
                        where a.source_code=b.source_code
                        and a.invalid_reason is null and b.invalid_reason is null;
                        
                        select * into #mapping_table from #temp
                        where source_code not in (select source_code from #duplicated);
                        
                        drop table #device, #five, #temp;
                        ")
        DatabaseConnector::executeSql(connection,SqlMapping)
        
        
        ## 30T Mappied
        SqlMappied30T <- c("
                           SELECT Count(*) -- 7,886,009
                           FROM   (SELECT x.key_seq, x.div_cd 
                           FROM   (SELECT * 
                           FROM   nhisnsc2013original.dbo.nhid_gy30_t1 
                           WHERE  div_type_cd NOT IN ( '1', '2', '3', '4', '5' )) x, 
                           nhis_nsc_v5_3_1.dbo.seq_master y 
                           WHERE  y.source_table = '130' 
                           AND x.key_seq = y.key_seq 
                           AND x.seq_no = y.seq_no) a 
                           JOIN #mapping_table b 
                           ON a.div_cd = b.source_code; 
                           ")
        ConvertedDeviCountByMappied30T <- DatabaseConnector::querySql(connection,SqlMappied30T)
        
        ## 30T Dup Mappied
        SqlDupMappied30T <- c("
                              SELECT Count(*) --1,016
                              FROM   (SELECT x.key_seq, 
                              x.div_cd 
                              FROM   (SELECT * 
                              FROM   nhisnsc2013original.dbo.nhid_gy30_t1 
                              WHERE  div_type_cd IN ( '7', '8' )) x, 
                              nhis_nsc_v5_3_1.dbo.seq_master y 
                              WHERE  y.source_table = '130' 
                              AND x.key_seq = y.key_seq 
                              AND x.seq_no = y.seq_no) a 
                              JOIN #duplicated b 
                              ON a.div_cd = b.source_code
                              ")
        ConvertedDeviCountByDupMappied30T <- DatabaseConnector::querySql(connection,SqlDupMappied30T)
        
        ## 30T UnMappied
        SqlUnMappied <- c("
                          SELECT Count(*) -- 3,493,993
                          FROM   (SELECT x.key_seq, 
                          x.div_cd 
                          FROM   (SELECT * 
                          FROM   nhisnsc2013original.dbo.nhid_gy30_t1 
                          WHERE  div_type_cd IN ( '7', '8' )) x, 
                          nhis_nsc_v5_3_1.dbo.seq_master y 
                          WHERE  y.source_table = '130' 
                          AND x.key_seq = y.key_seq 
                          AND x.seq_no = y.seq_no) a 
                          WHERE  a.div_cd NOT IN (SELECT source_code 
                          FROM   #duplicated 
                          UNION ALL 
                          SELECT source_code 
                          FROM   #mapping_table); 
                          ")
        ConvertedDeviCountByUnMappied30T <- DatabaseConnector::querySql(connection,SqlUnMappied)
        
        ## 30T Raw
        SqlRawToDeviBy30T_1 <- c("
                                 SELECT div_type_cd, multimappied, Count(*) AS COUNT 
                                 from (select key_seq, seq_no, div_type_cd, count(*) as multimappied
                                 FROM   (SELECT * 
                                 FROM   (SELECT * 
                                 FROM   nhisnsc2013original.dbo.nhid_gy30_t1 
                                 WHERE  div_type_cd NOT IN ( '1', '2', '3', '4', '5', '7', '8' )) x 
                                 JOIN #mapping_table y -- 1:1 mapping 
                                 ON x.div_cd = y.source_code) z 
                                 group by key_seq, seq_no, div_type_cd) a
                                 group by div_type_cd, multimappied
                                 ")
        SqlRawToDeviBy30T_2 <- c("
                                 SELECT div_type_cd, 1 as multimappied, Count(*) AS COUNT
                                 FROM   (SELECT * 
                                 FROM   nhisnsc2013original.dbo.nhid_gy30_t1 
                                 WHERE  div_type_cd IN ( '7', '8' )) x 
                                 GROUP  BY div_type_cd 
                                 ")
        HowManyContainDeviByMappied30T <- DatabaseConnector::querySql(connection,SqlRawToDeviBy30T_1)
        HowManyContainDeviByMappied30T <- rbind(HowManyContainDeviByMappied30T,DatabaseConnector::querySql(connection,SqlRawToDeviBy30T_2))
        
        
        ## 60T Mappied
        SqlMappied60T <- c("
                           SELECT Count(*) -- 2
                           FROM   (SELECT x.key_seq, x.div_cd 
                           FROM   (SELECT * 
                           FROM   nhisnsc2013original.dbo.nhid_gy60_t1 
                           WHERE  div_type_cd NOT IN ( '1', '2', '3', '4', '5' )) x, 
                           nhis_nsc_v5_3_1.dbo.seq_master y 
                           WHERE  y.source_table = '160' 
                           AND x.key_seq = y.key_seq 
                           AND x.seq_no = y.seq_no) a 
                           JOIN #mapping_table b 
                           ON a.div_cd = b.source_code;  
                           ")
        ConvertedDeviCountByMappied60T <- DatabaseConnector::querySql(connection,SqlMappied60T)
        
        ## 60T Dup Mappied
        SqlDupMappied60T <- c("
                              SELECT Count(*) -- 0
                              FROM   (SELECT x.key_seq, 
                              x.div_cd 
                              FROM   (SELECT * 
                              FROM   nhisnsc2013original.dbo.nhid_gy60_t1 
                              WHERE  div_type_cd IN ( '7', '8' )) x, 
                              nhis_nsc_v5_3_1.dbo.seq_master y 
                              WHERE  y.source_table = '160' 
                              AND x.key_seq = y.key_seq 
                              AND x.seq_no = y.seq_no) a 
                              JOIN #duplicated b 
                              ON a.div_cd = b.source_code 
                              ")
        ConvertedDeviCountByDupMappied60T <- DatabaseConnector::querySql(connection,SqlDupMappied60T)
        
        ## 60T UnMappied
        SqlUnMappied <- c("
                          SELECT Count(*) -- 795
                          FROM   (SELECT x.key_seq, 
                          x.div_cd 
                          FROM   (SELECT * 
                          FROM   nhisnsc2013original.dbo.nhid_gy60_t1 
                          WHERE  div_type_cd IN ( '7', '8' )) x, 
                          nhis_nsc_v5_3_1.dbo.seq_master y 
                          WHERE  y.source_table = '160' 
                          AND x.key_seq = y.key_seq 
                          AND x.seq_no = y.seq_no) a 
                          WHERE  a.div_cd NOT IN (SELECT source_code 
                          FROM   #duplicated 
                          UNION ALL 
                          SELECT source_code 
                          FROM   #mapping_table);
                          ")
        ConvertedDeviCountByUnMappied60T <- DatabaseConnector::querySql(connection,SqlUnMappied)
        
        ## 60T Raw
        SqlRawToDeviBy60T_1 <- c("
                                 SELECT div_type_cd, multimappied, Count(*) AS COUNT 
                                 from (select key_seq, seq_no, div_type_cd, count(*) as multimappied
                                 FROM   (SELECT * 
                                 FROM   (SELECT * 
                                 FROM   nhisnsc2013original.dbo.nhid_gy60_t1 
                                 WHERE  div_type_cd NOT IN ( '1', '2', '3', '4', '5', '7', '8' )) x 
                                 JOIN #mapping_table y -- 1:1 mapping 
                                 ON x.div_cd = y.source_code) z 
                                 group by key_seq, seq_no, div_type_cd) a
                                 group by div_type_cd, multimappied
                                 ")
        SqlRawToDeviBy60T_2 <- c("
                                 SELECT div_type_cd, 1 as multimappied, Count(*) AS COUNT
                                 FROM   (SELECT * 
                                 FROM   nhisnsc2013original.dbo.nhid_gy60_t1 
                                 WHERE  div_type_cd IN ( '7', '8' )) x 
                                 GROUP  BY div_type_cd 
                                 ")
        HowManyContainDeviByMappied60T <- DatabaseConnector::querySql(connection,SqlRawToDeviBy60T_1)
        HowManyContainDeviByMappied60T <- rbind(HowManyContainDeviByMappied60T,DatabaseConnector::querySql(connection,SqlRawToDeviBy60T_2))
        
    } ## DeviceTable : 20T join으로 인해 2건 차이남
    
    
    
    
    if(condition_occurrence){
        
        ## Mapping Table
        SqlMapping <- c("
                        IF OBJECT_ID('tempdb..#mapping_table', 'U') IS NOT NULL
                        DROP TABLE #mapping_table;
                        IF OBJECT_ID('tempdb..#mapping_table2', 'U') IS NOT NULL
                        DROP TABLE #mapping_table2;
                        select a.source_code, a.target_concept_id, a.domain_id, REPLACE(a.invalid_reason, '', NULL) as invalid_reason
                        into #mapping_table
                        from NHIS_NSC_new_mapping.dbo.source_to_concept_map a join NHIS_NSC_new_mapping.dbo.CONCEPT b on a.target_concept_id=b.concept_id
                        where a.invalid_reason is null and b.invalid_reason is null and a.domain_id='condition';
                        
                        select a.source_code, a.target_concept_id, a.domain_id, REPLACE(a.invalid_reason, '', NULL) as invalid_reason
                        into #mapping_table2
                        from NHIS_NSC_new_mapping.dbo.source_to_concept_map a join NHIS_NSC_new_mapping.dbo.CONCEPT b on a.target_concept_id=b.concept_id
                        where a.invalid_reason is null and b.invalid_reason is null;
                        ")
        DatabaseConnector::executeSql(connection,SqlMapping)
        
        
        ## 40T Mappied
        SqlMappied40T <- c("
                           select count(*) from (select a.person_id, sick_sym -- 292,249,453
                           from (select * from NHIS_NSC_v5_3_1.dbo.SEQ_MASTER where source_table='140') a,
                           NHISNSC2013Original.dbo.NHID_GY20_T1 b,
                           NHISNSC2013Original.dbo.NHID_GY40_T1 c
                           where a.person_id=b.person_id
                           and a.key_seq=b.key_seq
                           and a.key_seq=c.key_seq
                           and a.seq_no=c.seq_no) as m,
                           #mapping_table as n
                           where m.sick_sym=n.source_code;
                           ")
        ConvertedCondiCountByMappied30T <- DatabaseConnector::querySql(connection,SqlMappied40T)
        
        ## 40T UnMappied
        SqlUnMappied40T <- c("
                             select count(*) from (select a.person_id, sick_sym -- 7,176,297
                             from (select * from NHIS_NSC_v5_3_1.dbo.SEQ_MASTER where source_table='140') a, 
                             NHISNSC2013Original.dbo.NHID_GY20_T1 b, 
                             NHISNSC2013Original.dbo.NHID_GY40_T1 c
                             where a.person_id=b.person_id
                             and a.key_seq=b.key_seq
                             and a.key_seq=c.key_seq
                             and a.seq_no=c.seq_no) as m
                             where m.sick_sym not in (select source_code from #mapping_table2)
                             ")
        ConvertedCondiCountByUnMappied40T <- DatabaseConnector::querySql(connection,SqlUnMappied40T)
        
        ## 40T Raw
        SqlRawToCondiBy40T_1 <- c("
                                  SELECT domain_id, multimappied, Count(*) AS COUNT 
                                  FROM   (SELECT key_seq, seq_no, domain_id, Count(*) AS multimappied 
                                  FROM   (SELECT * 
                                  FROM   nhisnsc2013original.dbo.nhid_gy40_t1 a 
                                  JOIN #mapping_table2 b ON a.sick_sym = b.source_code 
                                  WHERE  b.domain_id = 'condition') c -- 1:n mappied  292,250,891   
                                  GROUP  BY key_seq, seq_no, domain_id) d 
                                  GROUP  BY domain_id, multimappied  
                                  ")
        SqlRawToCondiBy40T_2 <- c("
                                  SELECT 'Unclassified' as domain_id, 1 as multimappied, Count(*) AS COUNT
                                  FROM   nhisnsc2013original.dbo.nhid_gy40_t1 
                                  WHERE  sick_sym NOT IN (SELECT source_code FROM #mapping_table2) 
                                  ")
        HowManyContainCondiByMappied40T <- DatabaseConnector::querySql(connection,SqlRawToCondiBy40T_1)
        HowManyContainCondiByMappied40T <- rbind(HowManyContainCondiByMappied40T,DatabaseConnector::querySql(connection,SqlRawToCondiBy40T_2))
        
    } ## ConditionOccurrenceTable : 20T join으로 인해 3건 차이남, 기간 제거 개수 확인요망
    
    
    
    
    if(measurement){
        
        ## Mapping Table
        SqlMapping <- c("
                        IF OBJECT_ID('tempdb..#measurement_mapping', 'U') IS NOT NULL
                        DROP TABLE #measurement_mapping;
                        CREATE TABLE #measurement_mapping
                        (
                        meas_type					varchar(50)					NULL , 
                        id_value					varchar(50)					NULL ,
                        answer						bigint						NULL ,
                        measurement_concept_id		bigint						NULL ,
                        measurement_type_concept_id	bigint						NULL ,
                        measurement_unit_concept_id	bigint						NULL ,
                        value_as_concept_id			bigint						NULL ,
                        value_as_number				float						NULL 
                        )
                        ;
                        insert into #measurement_mapping (meas_type, id_value, answer, measurement_concept_id, measurement_type_concept_id, measurement_unit_concept_id, value_as_concept_id, value_as_number) values ('HEIGHT',			'01',	0,	3036277,	44818701,	4122378,	NULL,		NULL)
                        insert into #measurement_mapping (meas_type, id_value, answer, measurement_concept_id, measurement_type_concept_id, measurement_unit_concept_id, value_as_concept_id, value_as_number) values ('WEIGHT',			'02',	0,	3025315,	44818701,	4122383,	NULL,		NULL)
                        insert into #measurement_mapping (meas_type, id_value, answer, measurement_concept_id, measurement_type_concept_id, measurement_unit_concept_id, value_as_concept_id, value_as_number) values ('WAIST',				'03',	0,	3016258,	44818701,	4122378,	NULL,		NULL)
                        insert into #measurement_mapping (meas_type, id_value, answer, measurement_concept_id, measurement_type_concept_id, measurement_unit_concept_id, value_as_concept_id, value_as_number) values ('BP_HIGH',			'04',	0,	3028737,	44818701,	4118323,	NULL,		NULL)
                        insert into #measurement_mapping (meas_type, id_value, answer, measurement_concept_id, measurement_type_concept_id, measurement_unit_concept_id, value_as_concept_id, value_as_number) values ('BP_LWST',			'05',	0,	3012888,	44818701,	4118323,	NULL,		NULL)
                        insert into #measurement_mapping (meas_type, id_value, answer, measurement_concept_id, measurement_type_concept_id, measurement_unit_concept_id, value_as_concept_id, value_as_number) values ('BLDS',				'06',	0,	46235168,	44818702,	4121396,	NULL,		NULL)
                        insert into #measurement_mapping (meas_type, id_value, answer, measurement_concept_id, measurement_type_concept_id, measurement_unit_concept_id, value_as_concept_id, value_as_number) values ('TOT_CHOLE',			'07',	0,	3027114,	44818702,	4121396,	NULL,		NULL)
                        insert into #measurement_mapping (meas_type, id_value, answer, measurement_concept_id, measurement_type_concept_id, measurement_unit_concept_id, value_as_concept_id, value_as_number) values ('TRIGLYCERIDE',		'08',	0,	3022038,	44818702,	4121396,	NULL,		NULL)
                        insert into #measurement_mapping (meas_type, id_value, answer, measurement_concept_id, measurement_type_concept_id, measurement_unit_concept_id, value_as_concept_id, value_as_number) values ('HDL_CHOLE',			'09',	0,	3023752,	44818702,	4121396,	NULL,		NULL)
                        insert into #measurement_mapping (meas_type, id_value, answer, measurement_concept_id, measurement_type_concept_id, measurement_unit_concept_id, value_as_concept_id, value_as_number) values ('LDL_CHOLE',			'10',	0,	3028437,	44818702,	4121396,	NULL,		NULL)
                        insert into #measurement_mapping (meas_type, id_value, answer, measurement_concept_id, measurement_type_concept_id, measurement_unit_concept_id, value_as_concept_id, value_as_number) values ('HMG',				'11',	0,	3000963,	44818702,	4121395,	NULL,		NULL)
                        insert into #measurement_mapping (meas_type, id_value, answer, measurement_concept_id, measurement_type_concept_id, measurement_unit_concept_id, value_as_concept_id, value_as_number) values ('GLY_CD',			'12',	1,	3009261,	44818702,	NULL,		9189,		NULL)
                        insert into #measurement_mapping (meas_type, id_value, answer, measurement_concept_id, measurement_type_concept_id, measurement_unit_concept_id, value_as_concept_id, value_as_number) values ('GLY_CD',			'12',	2,	3009261,	44818702,	NULL,		4127785,	NULL)
                        insert into #measurement_mapping (meas_type, id_value, answer, measurement_concept_id, measurement_type_concept_id, measurement_unit_concept_id, value_as_concept_id, value_as_number) values ('GLY_CD',			'12',	3,	3009261,	44818702,	NULL,		4123508,	NULL)
                        insert into #measurement_mapping (meas_type, id_value, answer, measurement_concept_id, measurement_type_concept_id, measurement_unit_concept_id, value_as_concept_id, value_as_number) values ('GLY_CD',			'12',	4,	3009261,	44818702,	NULL,		4126673,	NULL)
                        insert into #measurement_mapping (meas_type, id_value, answer, measurement_concept_id, measurement_type_concept_id, measurement_unit_concept_id, value_as_concept_id, value_as_number) values ('GLY_CD',			'12',	5,	3009261,	44818702,	NULL,		4125547,	NULL)
                        insert into #measurement_mapping (meas_type, id_value, answer, measurement_concept_id, measurement_type_concept_id, measurement_unit_concept_id, value_as_concept_id, value_as_number) values ('GLY_CD',			'12',	6,	3009261,	44818702,	NULL,		4126674,	NULL)
                        insert into #measurement_mapping (meas_type, id_value, answer, measurement_concept_id, measurement_type_concept_id, measurement_unit_concept_id, value_as_concept_id, value_as_number) values ('OLIG_OCCU_CD',		'13',	1,	437038,		44818702,	NULL,		9189,		NULL)
                        insert into #measurement_mapping (meas_type, id_value, answer, measurement_concept_id, measurement_type_concept_id, measurement_unit_concept_id, value_as_concept_id, value_as_number) values ('OLIG_OCCU_CD',		'13',	2,	437038,		44818702,	NULL,		4127785,	NULL)
                        insert into #measurement_mapping (meas_type, id_value, answer, measurement_concept_id, measurement_type_concept_id, measurement_unit_concept_id, value_as_concept_id, value_as_number) values ('OLIG_OCCU_CD',		'13',	3,	437038,		44818702,	NULL,		4123508,	NULL)
                        insert into #measurement_mapping (meas_type, id_value, answer, measurement_concept_id, measurement_type_concept_id, measurement_unit_concept_id, value_as_concept_id, value_as_number) values ('OLIG_OCCU_CD',		'13',	4,	437038,		44818702,	NULL,		4126673,	NULL)
                        insert into #measurement_mapping (meas_type, id_value, answer, measurement_concept_id, measurement_type_concept_id, measurement_unit_concept_id, value_as_concept_id, value_as_number) values ('OLIG_OCCU_CD',		'13',	5,	437038,		44818702,	NULL,		4125547,	NULL)
                        insert into #measurement_mapping (meas_type, id_value, answer, measurement_concept_id, measurement_type_concept_id, measurement_unit_concept_id, value_as_concept_id, value_as_number) values ('OLIG_OCCU_CD',		'13',	6,	437038,		44818702,	NULL,		4126674,	NULL)
                        insert into #measurement_mapping (meas_type, id_value, answer, measurement_concept_id, measurement_type_concept_id, measurement_unit_concept_id, value_as_concept_id, value_as_number) values ('OLIG_PH',			'14',	0,	3015736,	44818702,	8482,		NULL,		NULL)
                        insert into #measurement_mapping (meas_type, id_value, answer, measurement_concept_id, measurement_type_concept_id, measurement_unit_concept_id, value_as_concept_id, value_as_number) values ('OLIG_PROTE_CD',		'15',	1,	3014051,	44818702,	NULL,		9189,		NULL)
                        insert into #measurement_mapping (meas_type, id_value, answer, measurement_concept_id, measurement_type_concept_id, measurement_unit_concept_id, value_as_concept_id, value_as_number) values ('OLIG_PROTE_CD',		'15',	2,	3014051,	44818702,	NULL,		4127785,	NULL)
                        insert into #measurement_mapping (meas_type, id_value, answer, measurement_concept_id, measurement_type_concept_id, measurement_unit_concept_id, value_as_concept_id, value_as_number) values ('OLIG_PROTE_CD',		'15',	3,	3014051,	44818702,	NULL,		4123508,	NULL)
                        insert into #measurement_mapping (meas_type, id_value, answer, measurement_concept_id, measurement_type_concept_id, measurement_unit_concept_id, value_as_concept_id, value_as_number) values ('OLIG_PROTE_CD',		'15',	4,	3014051,	44818702,	NULL,		4126673,	NULL)
                        insert into #measurement_mapping (meas_type, id_value, answer, measurement_concept_id, measurement_type_concept_id, measurement_unit_concept_id, value_as_concept_id, value_as_number) values ('OLIG_PROTE_CD',		'15',	5,	3014051,	44818702,	NULL,		4125547,	NULL)
                        insert into #measurement_mapping (meas_type, id_value, answer, measurement_concept_id, measurement_type_concept_id, measurement_unit_concept_id, value_as_concept_id, value_as_number) values ('OLIG_PROTE_CD',		'15',	6,	3014051,	44818702,	NULL,		4126674,	NULL)
                        insert into #measurement_mapping (meas_type, id_value, answer, measurement_concept_id, measurement_type_concept_id, measurement_unit_concept_id, value_as_concept_id, value_as_number) values ('CREATININE',		'16',	0,	2212294,	44818702,	4121396,	NULL,		NULL)
                        insert into #measurement_mapping (meas_type, id_value, answer, measurement_concept_id, measurement_type_concept_id, measurement_unit_concept_id, value_as_concept_id, value_as_number) values ('SGOT_AST',			'17',	0,	2212597,	44818702,	4118000,	NULL,		NULL)
                        insert into #measurement_mapping (meas_type, id_value, answer, measurement_concept_id, measurement_type_concept_id, measurement_unit_concept_id, value_as_concept_id, value_as_number) values ('SGPT_ALT',			'18',	0,	2212598,	44818702,	4118000,	NULL,		NULL)
                        insert into #measurement_mapping (meas_type, id_value, answer, measurement_concept_id, measurement_type_concept_id, measurement_unit_concept_id, value_as_concept_id, value_as_number) values ('GAMMA_GTP',			'19',	0,	4289475,	44818702,	4118000,	NULL,		NULL)
                        ")
        DatabaseConnector::executeSql(connection,SqlMapping)
        
        
        ## GJ 수치형
        SqlMappiedGJ_num <- c("
                              SELECT Count(*) as COUNT -- 29,145,003
                              FROM   (SELECT a.meas_type, meas_value, hchk_year, person_id 
                              FROM   nhisnsc2013original.dbo.gj_vertical a, -- left join 75,717,081, 원래 75,298,684 -> 1:n mappig
                              #measurement_mapping b 
                              where  Isnull(a.meas_type, '') = Isnull(b.meas_type, '') 
                              AND Isnull(a.meas_value, '0') >= Isnull(Cast(b.answer AS CHAR), '0')) c, --  33,858,848 -> 1:1 mapping
                              nhisnsc2013original.dbo.nhid_gj d 
                              where  c.person_id = Cast(d.person_id AS CHAR) 
                              AND c.hchk_year = d.hchk_year 
                              AND c.meas_value != '' 
                              AND Substring(c.meas_type, 1, 30) IN ( 
                              'HEIGHT', 'WEIGHT', 'WAIST', 'BP_HIGH', 'BP_LWST', 'BLDS', 'TOT_CHOLE', 'TRIGLYCERIDE', 
                              'HDL_CHOLE', 'LDL_CHOLE', 'HMG', 'OLIG_PH', 'CREATININE', 'SGOT_AST', 'SGPT_ALT', 'GAMMA_GTP' ) 
                              ")
        ConvertedMeasuCountByMappiedGJ_num <- DatabaseConnector::querySql(connection,SqlMappiedGJ_num)
        
        ## GJ 코드형
        SqlMappiedGJ_code <- c("
                               SELECT Count(*) as COUNT -- 4,295,448
                               FROM   (SELECT a.meas_type, meas_value, hchk_year, person_id 
                               FROM   nhisnsc2013original.dbo.gj_vertical a, -- left join 75,298,684, 원래 75,298,684 -> 1:1 mappig
                               #measurement_mapping b 
                               where  Isnull(a.meas_type, '') = Isnull(b.meas_type, '') 
                               AND Isnull(a.meas_value, '0') = Isnull(Cast(b.answer AS CHAR), '0')) c, --  -> 1:1 mapping
                               nhisnsc2013original.dbo.nhid_gj d 
                               where   c.person_id = Cast(d.person_id AS CHAR) 
                               AND c.hchk_year = d.hchk_year 
                               AND c.meas_value != '' 
                               AND Substring(c.meas_type, 1, 30) IN ( 'GLY_CD', 'OLIG_OCCU_CD', 'OLIG_PROTE_CD' ) 
                               ")
        ConvertedMeasuCountByMappiedGJ_code <- DatabaseConnector::querySql(connection,SqlMappiedGJ_code)
        
        
    } 
    
    
    
    
}
