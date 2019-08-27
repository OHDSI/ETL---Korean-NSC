
NHISNSC_rawdata <- "nhisnsc2013original.dbo"
NHISNSC_database <- "NHIS_NSC_v5_3_1.dbo"
NHIS_JK <- "NHID_JK"
NHIS_20T <- "NHID_GY20_T1"
NHIS_30T <- "NHID_GY30_T1"
NHIS_40T <- "NHID_GY40_T1"
NHIS_60T <- "NHID_GY60_T1"
NHIS_GJ <- "NHID_GJ"
NHIS_YK <- "NHID_YK"
Mapping_database <- "NHIS_NSC_NEW_MAPPING.dbo"
outputFolder <- "/home/youngjin426"

#usethis::edit_r_environ()
Sys.getenv("myCdmSchema")

connectionDetails <- DatabaseConnector::createConnectionDetails(
    dbms = 'sql server'
    , server = Sys.getenv("myCdmServer")
    , schema = Sys.getenv("myCdmSchema")
    , user = Sys.getenv("userId")
    , password = Sys.getenv("password")
)
DatabaseConnector::connect(connectionDetails = connectionDetails)
connection <- DatabaseConnector::connect(connectionDetails)




executeNHISETL <- function(NHISNSC_rawdata,
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
                           
                           CDM_ddl = TRUE,
                           #import_voca = TRUE,        Importing voca could be unnecessary
                           master_table = TRUE,
                           location = TRUE,
                           care_site = TRUE,
                           person = TRUE,
                           death = TRUE,
                           observation_period = TRUE,
                           visit_occurrence = TRUE,
                           condition_occurrence = TRUE,
                           observation = TRUE,
                           drug_exposure = TRUE,
                           procedure_occurrence = TRUE,
                           device_exposure = TRUE,
                           measurement = TRUE,
                           payer_plan_period = TRUE,
                           cost = TRUE,
                           generateEra = TRUE,
                           dose_era = TRUE,
                           cdm_source = TRUE,
                           indexing = TRUE,
                           constraints = TRUE,
                           data_cleansing = TRUE
) {
    
    ParallelLogger::addDefaultFileLogger(file.path(outputFolder, "log.txt"))
    
    if(CDM_ddl) { ## DDL
        
        SqlFile <- "000.OMOP CDM sql server ddl.sql"
        
        ParallelLogger::logInfo("empty CDM tables are being generated")
        
        NHISNSC_database_use <- strsplit(NHISNSC_database,fixed=TRUE,".")[[1]][1]
        
        sql <- SqlRender::loadRenderTranslateSql(SqlFile,
                                                 packageName = "etlKoreanNSC",
                                                 dbms = connectionDetails$dbms,
                                                 NHISNSC_database = NHISNSC_database_use)
        
        DatabaseConnector::executeSql(connection = connection, sql)
        
        ParallelLogger::logInfo("Generation of CDM tables were completed")
        
    }
    
    
    
    
    if(cdm_source) { ## CDM Source Table
        
        SqlFile <- "320.CDM_source.sql"
        
        ParallelLogger::logInfo(paste("ETL",SqlFile))
        
        sql <- "select max(valid_end_date) from @NHISNSC_database.CONCEPT where standard_concept = 'S' and valid_end_date < CONVERT(date, '@today')"
        sql <- SqlRender::render(sql,NHISNSC_database = NHISNSC_database, today = Sys.Date())
        
        cdm_source_name <- 'The National Health Insurance Service–National Sample Cohort'
        cdm_source_abbreviation <- 'NHIS-NSC'
        cdm_holder <- 'The National Health Insurance Service in South Korea'
        source_description <- 'A representative sample cohort of 1,025,340 participants was randomly selected, comprising 2.2% of the total eligible Korean population in 2002, and followed for 11 years until 2013 unless participants’ eligibility was disqualified due to death or emigration.'
        source_documentation_reference <- 'http://nhiss.nhis.or.kr/bd/ab/bdaba021eng.do'
        cdm_etl_reference <- 'https://github.com/OHDSI/ETL---Korean-NSC'
        cdm_release_date <- Sys.Date()
        cdm_version <- 'v5.3.1'
        vocabulary_version <- DatabaseConnector::querySql(connection,sql)
        
        tb <- data.frame(cdm_source_name, cdm_source_abbreviation, cdm_holder, source_description,
                         source_documentation_reference, cdm_etl_reference,
                         cdm_release_date, cdm_version, vocabulary_version)
        
        colnames(tb) <- c("cdm_source_name", "cdm_source_abbreviation", "cdm_holder", "source_description",
                          "source_documentation_reference", "cdm_etl_reference",
                          "cdm_release_date", "cdm_version", "vocabulary_version")
        
        DatabaseConnector::insertTable(connection = connection,
                                       tableName = "CDM_SOURCE",
                                       data = tb,
                                       dropTableIfExists = FALSE,
                                       createTable = FALSE,
                                       tempTable = FALSE,
                                       useMppBulkLoad = FALSE)
        ParallelLogger::logInfo(paste("ETL",SqlFile, " was completed"))
        
    }
    
    
    
    
    if(master_table) {
        
        SqlFile <- "010.Master_table.sql"
        
        ParallelLogger::logInfo(paste("ETL",SqlFile))
        table <- "SEQ_MASTER"
        startTime <- Sys.time()
        
        sql <- SqlRender::loadRenderTranslateSql(SqlFile,
                                                 packageName = "etlKoreanNSC",
                                                 dbms = connectionDetails$dbms,
                                                 NHISNSC_rawdata = NHISNSC_rawdata,
                                                 NHISNSC_database = NHISNSC_database,
                                                 NHIS_20T = NHIS_20T,
                                                 NHIS_30T = NHIS_30T,
                                                 NHIS_40T = NHIS_40T,
                                                 NHIS_60T = NHIS_60T,
                                                 NHIS_GJ = NHIS_GJ,
                                                 NHIS_JK = NHIS_JK)
        
        DatabaseConnector::executeSql(connection = connection, sql)
        
        elapsedTime = Sys.time() - startTime
        
        sql <- "SELECT COUNT (*) AS count FROM @NHISNSC_database.@table"
        sql <- SqlRender::render(sql,
                                 NHISNSC_database = NHISNSC_database,
                                 table = table)
        sql <- SqlRender::translate(sql,  targetDialect=attr(connection, "dbms"))
        count <- DatabaseConnector::querySql(connection,sql)
        
        ParallelLogger::logInfo(sprintf("%s was converted.\n
                                        elapsed time : %s %s \n
                                        total of %d row was converted", table, elapsedTime, attributes(elapsedTime)$units, count[1,1]))
        
    } ## end
    
    
    
    
    if(location) {
        
        SqlFile <- "020.Location.sql"
        
        ParallelLogger::logInfo(paste("ETL",SqlFile))
        table <- "location"
        startTime <- Sys.time()
        
        sql <- SqlRender::loadRenderTranslateSql(SqlFile,
                                                 packageName = "etlKoreanNSC",
                                                 dbms = connectionDetails$dbms,
                                                 NHISNSC_database = NHISNSC_database)
        
        DatabaseConnector::executeSql(connection = connection, sql)
        
        elapsedTime = Sys.time() - startTime
        
        sql <- "SELECT COUNT (*) AS count FROM @NHISNSC_database.@table"
        sql <- SqlRender::render(sql,
                                 NHISNSC_database = NHISNSC_database,
                                 table = table)
        sql <- SqlRender::translate(sql,  targetDialect=attr(connection, "dbms"))
        count <- DatabaseConnector::querySql(connection,sql)
        
        ParallelLogger::logInfo(sprintf("%s was converted.\n
                                        elapsed time : %s %s \n
                                        total of %d row was converted", table, elapsedTime, attributes(elapsedTime)$units, count[1,1]))
        
    } ## end
    
    
    
    
    if(care_site) {
        
        SqlFile <- "030.Care_site.sql"
        
        ParallelLogger::logInfo(paste("ETL",SqlFile))
        table <- "care_site"
        startTime <- Sys.time()
        
        sql <- SqlRender::loadRenderTranslateSql(SqlFile,
                                                 packageName = "etlKoreanNSC",
                                                 dbms = connectionDetails$dbms,
                                                 NHISNSC_rawdata = NHISNSC_rawdata,
                                                 NHISNSC_database = NHISNSC_database,
                                                 NHIS_YK = NHIS_YK)
        
        DatabaseConnector::executeSql(connection = connection, sql)
        
        elapsedTime = Sys.time() - startTime
        
        sql <- "SELECT COUNT (*) AS count FROM @NHISNSC_database.@table"
        sql <- SqlRender::render(sql,
                                 NHISNSC_database = NHISNSC_database,
                                 table = table)
        sql <- SqlRender::translate(sql,  targetDialect=attr(connection, "dbms"))
        count <- DatabaseConnector::querySql(connection,sql)
        
        ParallelLogger::logInfo(sprintf("%s was converted.\n 
                                        elapsed time : %s %s \n
                                        total of %d row was converted", table, elapsedTime, attributes(elapsedTime)$units, count[1,1]))
        
    } ## end
    
    
    
    
    if(person) {
        
        SqlFile <- "040.Person.sql"
        
        ParallelLogger::logInfo(paste("ETL",SqlFile))
        table <- "person"
        startTime <- Sys.time()
        
        sql <- SqlRender::loadRenderTranslateSql(SqlFile,
                                                 packageName = "etlKoreanNSC",
                                                 dbms = connectionDetails$dbms,
                                                 NHISNSC_rawdata = NHISNSC_rawdata,
                                                 NHISNSC_database = NHISNSC_database,
                                                 NHIS_JK = NHIS_JK)
        
        DatabaseConnector::executeSql(connection = connection, sql)
        
        elapsedTime = Sys.time() - startTime
        
        sql <- "SELECT COUNT (*) AS count FROM @NHISNSC_database.@table"
        sql <- SqlRender::render(sql,
                                 NHISNSC_database = NHISNSC_database,
                                 table = table)
        sql <- SqlRender::translate(sql,  targetDialect=attr(connection, "dbms"))
        count <- DatabaseConnector::querySql(connection,sql)
        
        ParallelLogger::logInfo(sprintf("%s was converted.\n 
                                        elapsed time : %s %s \n
                                        total of %d row was converted", table, elapsedTime, attributes(elapsedTime)$units, count[1,1]))
        
    } ## end 
    
    
    
    
    if(death) {
        
        SqlFile <- "050.Death.sql"
        
        ParallelLogger::logInfo(paste("ETL",SqlFile))
        table <- "death"
        startTime <- Sys.time()
        
        sql <- SqlRender::loadRenderTranslateSql(SqlFile,
                                                 packageName = "etlKoreanNSC",
                                                 dbms = connectionDetails$dbms,
                                                 NHISNSC_rawdata = NHISNSC_rawdata,
                                                 NHISNSC_database = NHISNSC_database,
                                                 Mapping_database = Mapping_database,
                                                 NHIS_JK = NHIS_JK)
        
        DatabaseConnector::executeSql(connection = connection, sql)
        
        elapsedTime = Sys.time() - startTime
        
        sql <- "SELECT COUNT (*) AS count FROM @NHISNSC_database.@table"
        sql <- SqlRender::render(sql,
                                 NHISNSC_database = NHISNSC_database,
                                 table = table)
        sql <- SqlRender::translate(sql,  targetDialect=attr(connection, "dbms"))
        count <- DatabaseConnector::querySql(connection,sql)
        
        ParallelLogger::logInfo(sprintf("%s was converted.\n 
                                        elapsed time : %s %s \n
                                        total of %d row was converted", table, elapsedTime, attributes(elapsedTime)$units, count[1,1]))
        
    } ## end
    
    
    
    
    if(observation_period) {
        
        SqlFile <- "060.Observation_period.sql"
        
        ParallelLogger::logInfo(paste("ETL",SqlFile))
        table <- "observation_period"
        startTime <- Sys.time()
        
        sql <- SqlRender::loadRenderTranslateSql(SqlFile,
                                                 packageName = "etlKoreanNSC",
                                                 dbms = connectionDetails$dbms,
                                                 NHISNSC_rawdata = NHISNSC_rawdata,
                                                 NHISNSC_database = NHISNSC_database,
                                                 NHIS_JK = NHIS_JK)
        
        DatabaseConnector::executeSql(connection = connection, sql)
        
        elapsedTime = Sys.time() - startTime
        
        sql <- "SELECT COUNT (*) AS count FROM @NHISNSC_database.@table"
        sql <- SqlRender::render(sql,
                                 NHISNSC_database = NHISNSC_database,
                                 table = table)
        sql <- SqlRender::translate(sql,  targetDialect=attr(connection, "dbms"))
        count <- DatabaseConnector::querySql(connection,sql)
        
        ParallelLogger::logInfo(sprintf("%s was converted.\n 
                                        elapsed time : %s %s \n
                                        total of %d row was converted", table, elapsedTime, attributes(elapsedTime)$units, count[1,1]))
        
    } ## end
    
    
    
    
    if(visit_occurrence) {
        
        SqlFile <- "070.Visit_occurrence.sql"
        
        ParallelLogger::logInfo(paste("ETL",SqlFile))
        table <- "visit_occurrence"
        startTime <- Sys.time()
        
        sql <- SqlRender::loadRenderTranslateSql(SqlFile,
                                                 packageName = "etlKoreanNSC",
                                                 dbms = connectionDetails$dbms,
                                                 NHISNSC_rawdata = NHISNSC_rawdata,
                                                 NHISNSC_database = NHISNSC_database,
                                                 NHIS_JK = NHIS_JK,
                                                 NHIS_20T = NHIS_20T,
                                                 NHIS_GJ = NHIS_GJ)
        
        DatabaseConnector::executeSql(connection = connection, sql)
        
        elapsedTime = Sys.time() - startTime
        
        sql <- "SELECT COUNT (*) AS count FROM @NHISNSC_database.@table"
        sql <- SqlRender::render(sql,
                                 NHISNSC_database = NHISNSC_database,
                                 table = table)
        sql <- SqlRender::translate(sql,  targetDialect=attr(connection, "dbms"))
        count <- DatabaseConnector::querySql(connection,sql)
        
        ParallelLogger::logInfo(sprintf("%s was converted.\n 
                                        elapsed time : %s %s \n
                                        total of %d row was converted", table, elapsedTime, attributes(elapsedTime)$units, count[1,1]))
        
    } ## end
    
    
    
    
    if(condition_occurrence) {
        
        SqlFile <- "080.Condition_occurrence.sql"
        
        ParallelLogger::logInfo(paste("ETL",SqlFile))
        table <- "condition_occurrence"
        startTime <- Sys.time()
        
        sql <- SqlRender::loadRenderTranslateSql(SqlFile,
                                                 packageName = "etlKoreanNSC",
                                                 dbms = connectionDetails$dbms,
                                                 NHISNSC_rawdata = NHISNSC_rawdata,
                                                 NHISNSC_database = NHISNSC_database,
                                                 Mapping_database = Mapping_database,
                                                 NHIS_JK = NHIS_JK,
                                                 NHIS_20T = NHIS_20T,
                                                 NHIS_40T = NHIS_40T)
        
        DatabaseConnector::executeSql(connection = connection, sql)
        
        elapsedTime = Sys.time() - startTime
        
        sql <- "SELECT COUNT (*) AS count FROM @NHISNSC_database.@table"
        sql <- SqlRender::render(sql,
                                 NHISNSC_database = NHISNSC_database,
                                 table = table)
        sql <- SqlRender::translate(sql,  targetDialect=attr(connection, "dbms"))
        count <- DatabaseConnector::querySql(connection,sql)
        
        ParallelLogger::logInfo(sprintf("%s was converted.\n
                                        elapsed time : %s %s \n
                                        total of %d row was converted", table, elapsedTime, attributes(elapsedTime)$units, count[1,1]))
        
    } ## end
    
    
    
    
    if(observation) {
        
        SqlFile <- "090.Observation.sql"
        
        ParallelLogger::logInfo(paste("ETL",SqlFile))
        table <- "observation"
        startTime <- Sys.time()
        
        sql <- SqlRender::loadRenderTranslateSql(SqlFile,
                                                 packageName = "etlKoreanNSC",
                                                 dbms = connectionDetails$dbms,
                                                 NHISNSC_rawdata = NHISNSC_rawdata,
                                                 NHISNSC_database = NHISNSC_database)
        
        DatabaseConnector::executeSql(connection = connection, sql)
        
        elapsedTime = Sys.time() - startTime
        
        sql <- "SELECT COUNT (*) AS count FROM @NHISNSC_database.@table"
        sql <- SqlRender::render(sql,
                                 NHISNSC_database = NHISNSC_database,
                                 table = table)
        sql <- SqlRender::translate(sql,  targetDialect=attr(connection, "dbms"))
        count <- DatabaseConnector::querySql(connection,sql)
        
        ParallelLogger::logInfo(sprintf("%s was converted.\n
                                        elapsed time : %s %s \n
                                        total of %d row was converted", table, elapsedTime, attributes(elapsedTime)$units, count[1,1]))
        
    } ## end
    
    
    
    
    if(drug_exposure) {
        
        SqlFile <- "100.Drug_exposure.sql"
        
        ParallelLogger::logInfo(paste("ETL",SqlFile))
        table <- "drug_exposure"
        startTime <- Sys.time()
        
        sql <- SqlRender::loadRenderTranslateSql(SqlFile,
                                                 packageName = "etlKoreanNSC",
                                                 dbms = connectionDetails$dbms,
                                                 NHISNSC_rawdata = NHISNSC_rawdata,
                                                 NHISNSC_database = NHISNSC_database,
                                                 Mapping_database = Mapping_database,
                                                 NHIS_20T = NHIS_20T,
                                                 NHIS_30T = NHIS_30T,
                                                 NHIS_60T = NHIS_60T)
        
        DatabaseConnector::executeSql(connection = connection, sql)
        
        elapsedTime = Sys.time() - startTime
        
        sql <- "SELECT COUNT (*) AS count FROM @NHISNSC_database.@table"
        sql <- SqlRender::render(sql,
                                 NHISNSC_database = NHISNSC_database,
                                 table = table)
        sql <- SqlRender::translate(sql,  targetDialect=attr(connection, "dbms"))
        count <- DatabaseConnector::querySql(connection,sql)
        
        ParallelLogger::logInfo(sprintf("%s was converted.\n
                                        elapsed time : %s %s \n
                                        total of %d row was converted", table, elapsedTime, attributes(elapsedTime)$units, count[1,1]))
        
    } ## end
    
    
    
    
    if(procedure_occurrence) {
        
        SqlFile <- "110.Procedure_occurrence.sql"
        
        ParallelLogger::logInfo(paste("ETL",SqlFile))
        table <- "procedure_occurrence"
        startTime <- Sys.time()
        
        sql <- SqlRender::loadRenderTranslateSql(SqlFile,
                                                 packageName = "etlKoreanNSC",
                                                 dbms = connectionDetails$dbms,
                                                 NHISNSC_rawdata = NHISNSC_rawdata,
                                                 NHISNSC_database = NHISNSC_database,
                                                 Mapping_database = Mapping_database,
                                                 NHIS_30T = NHIS_30T,
                                                 NHIS_60T = NHIS_60T)
        
        DatabaseConnector::executeSql(connection = connection, sql)
        
        elapsedTime = Sys.time() - startTime
        
        sql <- "SELECT COUNT (*) AS count FROM @NHISNSC_database.@table"
        sql <- SqlRender::render(sql,
                                 NHISNSC_database = NHISNSC_database,
                                 table = table)
        sql <- SqlRender::translate(sql,  targetDialect=attr(connection, "dbms"))
        count <- DatabaseConnector::querySql(connection,sql)
        
        ParallelLogger::logInfo(sprintf("%s was converted.\n
                                        elapsed time : %s %s \n
                                        total of %d row was converted", table, elapsedTime, attributes(elapsedTime)$units, count[1,1]))
        
    } ## end
    
    
    
    
    if(device_exposure) {
        
        SqlFile <- "120.Device_exposure.sql"
        
        ParallelLogger::logInfo(paste("ETL",SqlFile))
        table <- "device_exposure"
        startTime <- Sys.time()
        
        sql <- SqlRender::loadRenderTranslateSql(SqlFile,
                                                 packageName = "etlKoreanNSC",
                                                 dbms = connectionDetails$dbms,
                                                 NHISNSC_rawdata = NHISNSC_rawdata,
                                                 NHISNSC_database = NHISNSC_database,
                                                 Mapping_database = Mapping_database,
                                                 NHIS_30T = NHIS_30T,
                                                 NHIS_60T = NHIS_60T)
        
        DatabaseConnector::executeSql(connection = connection, sql)
        
        elapsedTime = Sys.time() - startTime
        
        sql <- "SELECT COUNT (*) AS count FROM @NHISNSC_database.@table"
        sql <- SqlRender::render(sql,
                                 NHISNSC_database = NHISNSC_database,
                                 table = table)
        sql <- SqlRender::translate(sql,  targetDialect=attr(connection, "dbms"))
        count <- DatabaseConnector::querySql(connection,sql)
        
        ParallelLogger::logInfo(sprintf("%s was converted.\n
                                        elapsed time : %s %s \n
                                        total of %d row was converted", table, elapsedTime, attributes(elapsedTime)$units, count[1,1]))
        
    } ## end
    
    
    
    
    if(measurement) {
        
        SqlFile <- "130.Measurement.sql"
        
        ParallelLogger::logInfo(paste("ETL",SqlFile))
        table <- "measurement"
        startTime <- Sys.time()
        
        sql <- SqlRender::loadRenderTranslateSql(SqlFile,
                                                 packageName = "etlKoreanNSC",
                                                 dbms = connectionDetails$dbms,
                                                 NHISNSC_rawdata = NHISNSC_rawdata,
                                                 NHISNSC_database = NHISNSC_database)
        
        DatabaseConnector::executeSql(connection = connection, sql)
        
        elapsedTime = Sys.time() - startTime
        
        sql <- "SELECT COUNT (*) AS count FROM @NHISNSC_database.@table"
        sql <- SqlRender::render(sql,
                                 NHISNSC_database = NHISNSC_database,
                                 table = table)
        sql <- SqlRender::translate(sql,  targetDialect=attr(connection, "dbms"))
        count <- DatabaseConnector::querySql(connection,sql)
        
        ParallelLogger::logInfo(sprintf("%s was converted.\n
                                        elapsed time : %s %s \n
                                        total of %d row was converted", table, elapsedTime, attributes(elapsedTime)$units, count[1,1]))
        
    } ## end
    
    
    
    
    if(payer_plan_period) {
        
        SqlFile <- "140.Payer_plan_period.sql"
        
        ParallelLogger::logInfo(paste("ETL",SqlFile))
        table <- "payer_plan_period"
        startTime <- Sys.time()
        
        sql <- SqlRender::loadRenderTranslateSql(SqlFile,
                                                 packageName = "etlKoreanNSC",
                                                 dbms = connectionDetails$dbms,
                                                 NHISNSC_rawdata = NHISNSC_rawdata,
                                                 NHISNSC_database = NHISNSC_database,
                                                 NHIS_JK = NHIS_JK)
        
        DatabaseConnector::executeSql(connection = connection, sql)
        
        elapsedTime = Sys.time() - startTime
        
        sql <- "SELECT COUNT (*) AS count FROM @NHISNSC_database.@table"
        sql <- SqlRender::render(sql,
                                 NHISNSC_database = NHISNSC_database,
                                 table = table)
        sql <- SqlRender::translate(sql,  targetDialect=attr(connection, "dbms"))
        count <- DatabaseConnector::querySql(connection,sql)
        
        ParallelLogger::logInfo(sprintf("%s was converted.\n
                                        elapsed time : %s %s \n
                                        total of %d row was converted", table, elapsedTime, attributes(elapsedTime)$units, count[1,1]))
        
    } ## end
    
    
    
    
    if(cost) {
        
        SqlFile <- "150.Cost.sql"
        
        ParallelLogger::logInfo(paste("ETL",SqlFile))
        table <- "cost"
        startTime <- Sys.time()
        
        sql <- SqlRender::loadRenderTranslateSql(SqlFile,
                                                 packageName = "etlKoreanNSC",
                                                 dbms = connectionDetails$dbms,
                                                 NHISNSC_rawdata = NHISNSC_rawdata,
                                                 NHISNSC_database = NHISNSC_database,
                                                 Mapping_database = Mapping_database,
                                                 NHIS_JK = NHIS_JK,
                                                 NHIS_20T = NHIS_20T,
                                                 NHIS_30T = NHIS_30T,
                                                 NHIS_40T = NHIS_40T,
                                                 NHIS_60T = NHIS_60T,
                                                 NHIS_GJ = NHIS_GJ)
        
        DatabaseConnector::executeSql(connection = connection, sql)
        
        elapsedTime = Sys.time() - startTime
        
        sql <- "SELECT COUNT (*) AS count FROM @NHISNSC_database.@table"
        sql <- SqlRender::render(sql,
                                 NHISNSC_database = NHISNSC_database,
                                 table = table)
        sql <- SqlRender::translate(sql,  targetDialect=attr(connection, "dbms"))
        count <- DatabaseConnector::querySql(connection,sql)
        
        ParallelLogger::logInfo(sprintf("%s was converted.\n
                                        elapsed time : %s %s \n
                                        total of %d row was converted", table, elapsedTime, attributes(elapsedTime)$units, count[1,1]))
        
    } ## end
    
    
    
    
    if(generateEra) {
        
        SqlFile <- "300.GenerateEra.sql"
        
        ParallelLogger::logInfo(paste("ETL",SqlFile))
        table <- "condition_era"
        startTime <- Sys.time()
        
        sql <- SqlRender::loadRenderTranslateSql(SqlFile,
                                                 packageName = "etlKoreanNSC",
                                                 dbms = connectionDetails$dbms,
                                                 Mapping_database = Mapping_database,
                                                 NHISNSC_database = NHISNSC_database)
        
        DatabaseConnector::executeSql(connection = connection, sql)
        
        elapsedTime = Sys.time() - startTime
        
        sql <- "SELECT COUNT (*) AS count FROM @NHISNSC_database.@table"
        sql <- SqlRender::render(sql,
                                 NHISNSC_database = NHISNSC_database,
                                 table = table)
        sql <- SqlRender::translate(sql,  targetDialect=attr(connection, "dbms"))
        count <- DatabaseConnector::querySql(connection,sql)
        
        ParallelLogger::logInfo(sprintf("%s was converted.\n
                                        elapsed time : %s %s \n
                                        total of %d row was converted", table, elapsedTime, attributes(elapsedTime)$units, count[1,1]))
        
    } ## end
    
    
    
    
    if(dose_era) {
        
        SqlFile <- "310.Dose_era.sql"
        
        ParallelLogger::logInfo(paste("ETL",SqlFile))
        table <- "dose_era"
        startTime <- Sys.time()
        
        sql <- SqlRender::loadRenderTranslateSql(SqlFile,
                                                 packageName = "etlKoreanNSC",
                                                 dbms = connectionDetails$dbms,
                                                 NHISNSC_rawdata = NHISNSC_rawdata,
                                                 NHISNSC_database = NHISNSC_database,
                                                 Mapping_database = Mapping_database)
        
        DatabaseConnector::executeSql(connection = connection, sql)
        
        elapsedTime = Sys.time() - startTime
        
        sql <- "SELECT COUNT (*) AS count FROM @NHISNSC_database.@table"
        sql <- SqlRender::render(sql,
                                 NHISNSC_database = NHISNSC_database,
                                 table = table)
        sql <- SqlRender::translate(sql,  targetDialect=attr(connection, "dbms"))
        count <- DatabaseConnector::querySql(connection,sql)
        
        ParallelLogger::logInfo(sprintf("%s was converted.\n
                                        elapsed time : %s %s \n
                                        total of %d row was converted", table, elapsedTime, attributes(elapsedTime)$units, count[1,1]))
        
    } ## end
    
    
    
    
    if(indexing) {
        
        SqlFile <- "400.Indexing.sql"
        
        ParallelLogger::logInfo(paste("ETL",SqlFile))
        ##table <- ""
        startTime <- Sys.time()
        
        sql <- SqlRender::loadRenderTranslateSql(SqlFile,
                                                 packageName = "etlKoreanNSC",
                                                 dbms = connectionDetails$dbms,
                                                 NHISNSC_database = NHISNSC_database,
                                                 Mapping_database = Mapping_database)
        
        DatabaseConnector::executeSql(connection = connection, sql)
        
        elapsedTime = Sys.time() - startTime
        
        # sql <- "SELECT COUNT (*) AS count FROM @NHISNSC_database.@table"
        # sql <- SqlRender::render(sql,
        #                          NHISNSC_database = NHISNSC_database,
        #                          table = table)
        # sql <- SqlRender::translate(sql,  targetDialect=attr(connection, "dbms"))
        # count <- DatabaseConnector::querySql(connection,sql)
        
        ParallelLogger::logInfo(sprintf(##"%s was converted.\n
            "elapsed time : %s %s \n",
            ##total of %d row was converted", table, 
            elapsedTime))
        ##, attributes(elapsedTime)$units, count[1,1]))
        
    } ## end
    
    
    
    
    if(constraints) {
        
        SqlFile <- "500.Constraints.sql"
        
        ParallelLogger::logInfo(paste("ETL",SqlFile))
        ##table <- ""
        startTime <- Sys.time()
        
        sql <- SqlRender::loadRenderTranslateSql(SqlFile,
                                                 packageName = "etlKoreanNSC",
                                                 dbms = connectionDetails$dbms,
                                                 NHISNSC_database = NHISNSC_database,
                                                 Mapping_database = Mapping_database)
        
        DatabaseConnector::executeSql(connection = connection, sql)
        
        elapsedTime = Sys.time() - startTime
        
        # sql <- "SELECT COUNT (*) AS count FROM @NHISNSC_database.@table"
        # sql <- SqlRender::render(sql,
        #                          NHISNSC_database = NHISNSC_database,
        #                          table = table)
        # sql <- SqlRender::translate(sql,  targetDialect=attr(connection, "dbms"))
        # count <- DatabaseConnector::querySql(connection,sql)
        
        ParallelLogger::logInfo(sprintf(##"%s was converted.\n
            "elapsed time : %s %s \n",
            ##total of %d row was converted", table, 
            elapsedTime))
        ##, attributes(elapsedTime)$units, count[1,1]))
        
    } ## end
    
    
    
    
    if(data_cleansing) {
        
        SqlFile <- "900.data_cleansing.sql"
        
        ParallelLogger::logInfo(paste("ETL",SqlFile))
        ##table <- ""
        startTime <- Sys.time()
        
        sql <- SqlRender::loadRenderTranslateSql(SqlFile,
                                                 packageName = "etlKoreanNSC",
                                                 dbms = connectionDetails$dbms,
                                                 NHISNSC_rawdata = NHISNSC_rawdata,
                                                 NHISNSC_database = NHISNSC_database,
                                                 NHIS_JK = NHIS_JK,
                                                 NHIS_20T = NHIS_20T,
                                                 NHIS_30T = NHIS_30T,
                                                 NHIS_40T = NHIS_40T,
                                                 NHIS_60T = NHIS_60T,
                                                 NHIS_GJ = NHIS_GJ)
        
        DatabaseConnector::executeSql(connection = connection, sql)
        
        elapsedTime = Sys.time() - startTime
        
        # sql <- "SELECT COUNT (*) AS count FROM @NHISNSC_database.@table"
        # sql <- SqlRender::render(sql,
        #                          NHISNSC_database = NHISNSC_database,
        #                          table = table)
        # sql <- SqlRender::translate(sql,  targetDialect=attr(connection, "dbms"))
        # count <- DatabaseConnector::querySql(connection,sql)
        
        ParallelLogger::logInfo(sprintf(##"%s was converted.\n
            "elapsed time : %s %s \n",
            ##total of %d row was converted", table, 
            elapsedTime))
        ##, attributes(elapsedTime)$units, count[1,1]))
        
    } ## end
}

executeNHISETL(NHISNSC_rawdata,
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
               outputFolder)

