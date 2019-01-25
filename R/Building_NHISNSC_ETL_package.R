
Run_ETL <- function(CDM_ddl = TRUE,
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
                indexing = TRUE,
                constraints = TRUE
                ){
                        if (CDM_ddl == TRUE){
                                sql <- SqlRender::readSql(paste0(sqlFolder, "/000.OMOP CDM sql server ddl.sql"))
                                sql <- SqlRender::renderSql(sql, NHISNSC_database)$sql
                                sql <- SqlRender::translateSql(sql, targetDialect=conn$dbms)$sql

                                DatabaseConnector::executeSql(connection = conn, sql)
                        }

                        if (master_table == TRUE){
                                sql <- SqlRender::readSql(paste0(sqlFolder,"/010.Master_table.sql"))
                                sql <- SqlRender::renderSql(sql
                                                        , NHISNSC_database
                                                        , NHISNSC_rawdata
                                                        , NHIS_20T
                                                        , NHIS_30T
                                                        , NHIS_40T
                                                        , NHIS_60T
                                                        , NHIS_JK
                                                        , NHIS_GJ)$sql
                                sql <- SqlRender::translateSql(sql, targetDialect=conn$dbms)$sql

                                DatabaseConnector::executeSql(connection = conn, sql)
                        }

                        if (location == TRUE){
                                sql <- SqlRender::readSql(paste0(sqlFolder,"/020.Location.sql"))
                                sql <- SqlRender::renderSql(sql, NHISNSC_database)$sql
                                sql <- SqlRender::translateSql(sql, targetDialect=conn$dbms)$sql

                                DatabaseConnector::executeSql(connection = conn, sql)
                        }

                        if (care_site == TRUE){
                                sql <- SqlRender::readSql(paste0(sqlFolder,"/030.Care_site.sql"))
                                sql <- SqlRender::renderSql(sql
                                                            , NHISNSC_database
                                                            , NHISNSC_rawdata
                                                            , NHIS_YK)$sql
                                sql <- SqlRender::translateSql(sql, targetDialect=conn$dbms)$sql

                                DatabaseConnector::executeSql(connection = conn, sql)
                        }

                        if (person == TRUE){
                                sql <- SqlRender::readSql(paste0(sqlFolder,"/040.Person.sql"))
                                sql <- SqlRender::renderSql(sql
                                                            , NHISNSC_database
                                                            , NHISNSC_rawdata
                                                            , NHIS_JK)$sql
                                sql <- SqlRender::translateSql(sql, targetDialect=conn$dbms)$sql

                                DatabaseConnector::executeSql(connection = conn, sql)
                        }

                        if (death == TRUE){
                                sql <- SqlRender::readSql(paste0(sqlFolder,"/050.Death.sql"))
                                sql <- SqlRender::renderSql(sql
                                                            , NHISNSC_database
                                                            , NHISNSC_rawdata
                                                            , NHIS_JK)$sql
                                sql <- SqlRender::translateSql(sql, targetDialect=conn$dbms)$sql

                                DatabaseConnector::executeSql(connection = conn, sql)
                        }
                        if (observation_period == TRUE){
                                sql <- SqlRender::readSql(paste0(sqlFolder,"/060.Observation_period.sql"))
                                sql <- SqlRender::renderSql(sql
                                                            , NHISNSC_database
                                                            , NHISNSC_rawdata
                                                            , NHIS_JK)$sql
                                sql <- SqlRender::translateSql(sql, targetDialect=conn$dbms)$sql

                                DatabaseConnector::executeSql(connection = conn, sql)
                        }

                        if (visit_occurrence == TRUE){
                                sql <- SqlRender::readSql(paste0(sqlFolder,"/070.Visit_occurrence.sql"))
                                sql <- SqlRender::renderSql(sql
                                                            , NHISNSC_database
                                                            , NHISNSC_rawdata
                                                            , NHIS_20T
                                                            , NHIS_GJ)$sql
                                sql <- SqlRender::translateSql(sql, targetDialect=conn$dbms)$sql

                                DatabaseConnector::executeSql(connection = conn, sql)
                        }

                        if (condition_occurrence == TRUE){
                                sql <- SqlRender::readSql(paste0(sqlFolder,"/080.Condition_occurrence.sql"))
                                sql <- SqlRender::renderSql(sql
                                                            , NHISNSC_database
                                                            , NHISNSC_rawdata
                                                            , NHIS_20T
                                                            , NHIS_40T)$sql
                                sql <- SqlRender::translateSql(sql, targetDialect=conn$dbms)$sql

                                DatabaseConnector::executeSql(connection = conn, sql)
                        }

                        if (observation == TRUE){
                                sql <- SqlRender::readSql(paste0(sqlFolder,"/090.Observation.sql"))
                                sql <- SqlRender::renderSql(sql
                                                            , NHISNSC_database
                                                            , NHISNSC_rawdata
                                                            , NHIS_JK
                                                            , NHIS_GJ)$sql
                                sql <- SqlRender::translateSql(sql, targetDialect=conn$dbms)$sql

                                DatabaseConnector::executeSql(connection = conn, sql)
                        }

                        if (drug_exposure == TRUE){
                                sql <- SqlRender::readSql(paste0(sqlFolder,"/100.Drug_exposure.sql"))
                                sql <- SqlRender::renderSql(sql
                                                            , NHISNSC_database
                                                            , NHISNSC_rawdata
                                                            , NHIS_20T
                                                            , NHIS_30T
                                                            , NHIS_60T)$sql
                                sql <- SqlRender::translateSql(sql, targetDialect=conn$dbms)$sql

                                DatabaseConnector::executeSql(connection = conn, sql)
                        }

                        if (procedure_occurrence == TRUE){
                                sql <- SqlRender::readSql(paste0(sqlFolder,"/110.Procedure_occurrence.sql"))
                                sql <- SqlRender::renderSql(sql
                                                            , NHISNSC_database
                                                            , NHISNSC_rawdata
                                                            , NHIS_30T
                                                            , NHIS_60T)$sql
                                sql <- SqlRender::translateSql(sql, targetDialect=conn$dbms)$sql

                                DatabaseConnector::executeSql(connection = conn, sql)
                        }

                        if (device_exposure == TRUE){
                                sql <- SqlRender::readSql(paste0(sqlFolder,"/120.Device_exposure.sql"))
                                sql <- SqlRender::renderSql(sql
                                                            , NHISNSC_database
                                                            , NHISNSC_rawdata
                                                            , NHIS_30T
                                                            , NHIS_60T)$sql
                                sql <- SqlRender::translateSql(sql, targetDialect=conn$dbms)$sql

                                DatabaseConnector::executeSql(connection = conn, sql)
                        }

                        if (measurement == TRUE){
                                sql <- SqlRender::readSql(paste0(sqlFolder,"/130.Measurement.sql"))
                                sql <- SqlRender::renderSql(sql
                                                            , NHISNSC_database
                                                            , NHISNSC_rawdata)$sql
                                sql <- SqlRender::translateSql(sql, targetDialect=conn$dbms)$sql

                                DatabaseConnector::executeSql(connection = conn, sql)
                        }

                        if (payer_plan_period == TRUE){
                                sql <- SqlRender::readSql(paste0(sqlFolder,"/140.Payer_plan_period.sql"))
                                sql <- SqlRender::renderSql(sql
                                                            , NHISNSC_database
                                                            , NHISNSC_rawdata
                                                            , NHIS_JK)$sql
                                sql <- SqlRender::translateSql(sql, targetDialect=conn$dbms)$sql

                                DatabaseConnector::executeSql(connection = conn, sql)
                        }

                        if (cost == TRUE){
                                sql <- SqlRender::readSql(paste0(sqlFolder,"/150.Cost.sql"))
                                sql <- SqlRender::renderSql(sql
                                                            , NHISNSC_database
                                                            , NHISNSC_rawdata
                                                            , NHIS_20T
                                                            , NHIS_30T
                                                            , NHIS_60T)$sql
                                sql <- SqlRender::translateSql(sql, targetDialect=conn$dbms)$sql

                                DatabaseConnector::executeSql(connection = conn, sql)
                        }

                        if (generateEra == TRUE){
                                sql <- SqlRender::readSql(paste0(sqlFolder,"/300.GenerateEra.sql"))
                                sql <- SqlRender::renderSql(sql, NHISNSC_database)$sql
                                sql <- SqlRender::translateSql(sql, targetDialect=conn$dbms)$sql

                                DatabaseConnector::executeSql(connection = conn, sql)
                        }

                        if (dose_era == TRUE){
                                sql <- SqlRender::readSql(paste0(sqlFolder,"/310.Dose_era.sql"))
                                sql <- SqlRender::renderSql(sql, NHISNSC_database)$sql
                                sql <- SqlRender::translateSql(sql, targetDialect=conn$dbms)$sql

                                DatabaseConnector::executeSql(connection = conn, sql)
                        }

                        if (indexing == TRUE){
                                sql <- SqlRender::readSql(paste0(sqlFolder,"/001.Indexing.sql"))
                                sql <- SqlRender::renderSql(sql, NHISNSC_database)$sql
                                sql <- SqlRender::translateSql(sql, targetDialect=conn$dbms)$sql

                                DatabaseConnector::executeSql(connection = conn, sql)
                        }

                        if (indexing == TRUE){
                                sql <- SqlRender::readSql(paste0(sqlFolder,"/500.Constraints.sql"))
                                sql <- SqlRender::renderSql(sql, NHISNSC_database)$sql
                                sql <- SqlRender::translateSql(sql, targetDialect=conn$dbms)$sql

                                DatabaseConnector::executeSql(connection = conn, sql)
                        }
                }
