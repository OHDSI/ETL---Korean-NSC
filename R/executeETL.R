# Copyright 2019 Observational Health Data Sciences and Informatics
#
# This file is part of etlKoreanNSC
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http:\\\\www.apache.org\\licenses\\LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

#' executeETL Function
#' 
#' This function allows to execute ETL process.
#' @param CDM_ddl, import_voca, master_table, location, care_site, person, death, observation_period, visit_occurrence, condition_occurrence, observation, drug_exposure, procedure_occurrence, device_exposure, measurement, payer_plan_period, cost, generateEra, dose_era, CDM_source, indexing, constraints
#' @export
#' @example executeETL() 
executeETL <- function(CDM_ddl = TRUE,
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
                ){
                        if (CDM_ddl == TRUE){
                                sql <- SqlRender::readSql(paste0(sqlFolder, "\\000.OMOP CDM sql server ddl.sql"))
                                sql <- SqlRender::renderSql(sql, NHISNSC_database = NHISNSC_database)$sql
                                sql <- SqlRender::translateSql(sql, targetDialect=attr(connection, "dbms"))$sql

                                DatabaseConnector::executeSql(connection = connection, sql)
                        }

                        # if (import_voca == TRUE){
                        #         sql <- SqlRender::readSql(paste0(sqlFolder,"\\001.Import_voca.sql"))
                        #         sql <- SqlRender::renderSql(sql
                        #                                 , Mapping_database
                        #                                 , vocaFolder)$sql
                        #         sql <- SqlRender::translateSql(sql, targetDialect=attr(connection, "dbms"))$sql
                        # 
                        #         DatabaseConnector::executeSql(connection = connection, sql)
                        # }

                        if (master_table == TRUE){
                                sql <- SqlRender::readSql(paste0(sqlFolder,"\\010.Master_table.sql"))
                                sql <- SqlRender::renderSql(sql
                                                        , NHISNSC_database = paste0(NHISNSC_database, ".dbo")
                                                        , NHISNSC_rawdata = paste0(NHISNSC_rawdata, ".dbo")
                                                        , NHIS_20T = NHIS_20T
                                                        , NHIS_30T = NHIS_30T
                                                        , NHIS_40T = NHIS_40T
                                                        , NHIS_60T = NHIS_60T
                                                        , NHIS_JK = NHIS_JK
                                                        , NHIS_GJ = NHIS_GJ)$sql
                                sql <- SqlRender::translateSql(sql, targetDialect=attr(connection, "dbms"))$sql

                                DatabaseConnector::executeSql(connection = connection, sql)
                        }

                        if (location == TRUE){
                                sql <- SqlRender::readSql(paste0(sqlFolder,"\\020.Location.sql"))
                                sql <- SqlRender::renderSql(sql
                                                            , NHISNSC_database = paste0(NHISNSC_database, ".dbo"))$sql
                                sql <- SqlRender::translateSql(sql, targetDialect=attr(connection, "dbms"))$sql

                                DatabaseConnector::executeSql(connection = connection, sql)
                        }

                        if (care_site == TRUE){
                                sql <- SqlRender::readSql(paste0(sqlFolder,"\\030.Care_site.sql"))
                                sql <- SqlRender::renderSql(sql
                                                            , NHISNSC_database = paste0(NHISNSC_database, ".dbo")
                                                            , NHISNSC_rawdata = paste0(NHISNSC_rawdata, ".dbo")
                                                            , NHIS_YK = NHIS_YK)$sql
                                sql <- SqlRender::translateSql(sql, targetDialect=attr(connection, "dbms"))$sql

                                DatabaseConnector::executeSql(connection = connection, sql)
                        }

                        if (person == TRUE){
                                sql <- SqlRender::readSql(paste0(sqlFolder,"\\040.Person.sql"))
                                sql <- SqlRender::renderSql(sql
                                                            , NHISNSC_database = paste0(NHISNSC_database, ".dbo")
                                                            , NHISNSC_rawdata = paste0(NHISNSC_rawdata, ".dbo")
                                                            , NHIS_JK = NHIS_JK)$sql
                                sql <- SqlRender::translateSql(sql, targetDialect=attr(connection, "dbms"))$sql

                                DatabaseConnector::executeSql(connection = connection, sql)
                        }

                        if (death == TRUE){
                                sql <- SqlRender::readSql(paste0(sqlFolder,"\\050.Death.sql"))
                                sql <- SqlRender::renderSql(sql
                                                            , NHISNSC_database = paste0(NHISNSC_database, ".dbo")
                                                            , NHISNSC_rawdata = paste0(NHISNSC_rawdata, ".dbo")
                                                            , Mapping_database = paste0(Mapping_database, ".dbo")
                                                            , NHIS_JK = NHIS_JK)$sql
                                sql <- SqlRender::translateSql(sql, targetDialect=attr(connection, "dbms"))$sql

                                DatabaseConnector::executeSql(connection = connection, sql)
                        }
                        if (observation_period == TRUE){
                                sql <- SqlRender::readSql(paste0(sqlFolder,"\\060.Observation_period.sql"))
                                sql <- SqlRender::renderSql(sql
                                                            , NHISNSC_database = paste0(NHISNSC_database, ".dbo")
                                                            , NHISNSC_rawdata = paste0(NHISNSC_rawdata, ".dbo")
                                                            , NHIS_JK = NHIS_JK)$sql
                                sql <- SqlRender::translateSql(sql, targetDialect=attr(connection, "dbms"))$sql

                                DatabaseConnector::executeSql(connection = connection, sql)
                        }

                        if (visit_occurrence == TRUE){
                                sql <- SqlRender::readSql(paste0(sqlFolder,"\\070.Visit_occurrence.sql"))
                                sql <- SqlRender::renderSql(sql
                                                            , NHISNSC_database = paste0(NHISNSC_database, ".dbo")
                                                            , NHISNSC_rawdata = paste0(NHISNSC_rawdata, ".dbo")
                                                            , NHIS_20T = NHIS_20T
                                                            , NHIS_GJ = NHIS_GJ)$sql
                                sql <- SqlRender::translateSql(sql, targetDialect=attr(connection, "dbms"))$sql

                                DatabaseConnector::executeSql(connection = connection, sql)
                        }

                        if (condition_occurrence == TRUE){
                                sql <- SqlRender::readSql(paste0(sqlFolder,"\\080.Condition_occurrence.sql"))
                                sql <- SqlRender::renderSql(sql
                                                            , NHISNSC_database = paste0(NHISNSC_database, ".dbo")
                                                            , NHISNSC_rawdata = paste0(NHISNSC_rawdata, ".dbo")
                                                            , Mapping_database = paste0(Mapping_database, ".dbo")
                                                            , NHIS_20T = NHIS_20T
                                                            , NHIS_40T = NHIS_40T)$sql
                                sql <- SqlRender::translateSql(sql, targetDialect=attr(connection, "dbms"))$sql

                                DatabaseConnector::executeSql(connection = connection, sql)
                        }

                        if (observation == TRUE){
                                sql <- SqlRender::readSql(paste0(sqlFolder,"\\090.Observation.sql"))
                                sql <- SqlRender::renderSql(sql
                                                            , NHISNSC_database = paste0(NHISNSC_database, ".dbo")
                                                            , NHISNSC_rawdata = paste0(NHISNSC_rawdata, ".dbo")
                                                            , NHIS_JK = NHIS_JK
                                                            , NHIS_GJ = NHIS_GJ)$sql 
                                sql <- SqlRender::translateSql(sql, targetDialect=attr(connection, "dbms"))$sql

                                DatabaseConnector::executeSql(connection = connection, sql)
                        }

                        if (drug_exposure == TRUE){
                                sql <- SqlRender::readSql(paste0(sqlFolder,"\\100.Drug_exposure.sql"))
                                sql <- SqlRender::renderSql(sql
                                                            , NHISNSC_database = paste0(NHISNSC_database, ".dbo")
                                                            , NHISNSC_rawdata = paste0(NHISNSC_rawdata, ".dbo")
                                                            , Mapping_database = paste0(Mapping_database, ".dbo")
                                                            , NHIS_20T = NHIS_20T
                                                            , NHIS_30T = NHIS_30T
                                                            , NHIS_60T = NHIS_60T)$sql
                                sql <- SqlRender::translateSql(sql, targetDialect=attr(connection, "dbms"))$sql

                                DatabaseConnector::executeSql(connection = connection, sql)
                        }

                        if (procedure_occurrence == TRUE){
                                sql <- SqlRender::readSql(paste0(sqlFolder,"\\110.Procedure_occurrence.sql"))
                                sql <- SqlRender::renderSql(sql
                                                            , NHISNSC_database = paste0(NHISNSC_database, ".dbo")
                                                            , NHISNSC_rawdata = paste0(NHISNSC_rawdata, ".dbo")
                                                            , Mapping_database = paste0(Mapping_database, ".dbo")
                                                            , NHIS_30T = NHIS_30T
                                                            , NHIS_60T = NHIS_60T)$sql
                                sql <- SqlRender::translateSql(sql, targetDialect=attr(connection, "dbms"))$sql

                                DatabaseConnector::executeSql(connection = connection, sql)
                        }

                        if (device_exposure == TRUE){
                                sql <- SqlRender::readSql(paste0(sqlFolder,"\\120.Device_exposure.sql"))
                                sql <- SqlRender::renderSql(sql
                                                            , NHISNSC_database = paste0(NHISNSC_database, ".dbo")
                                                            , NHISNSC_rawdata = paste0(NHISNSC_rawdata, ".dbo")
                                                            , Mapping_database = paste0(Mapping_database, ".dbo")
                                                            , NHIS_30T = NHIS_30T
                                                            , NHIS_60T = NHIS_60T)$sql
                                sql <- SqlRender::translateSql(sql, targetDialect=attr(connection, "dbms"))$sql

                                DatabaseConnector::executeSql(connection = connection, sql)
                        }

                        if (measurement == TRUE){
                                sql <- SqlRender::readSql(paste0(sqlFolder,"\\130.Measurement.sql"))
                                sql <- SqlRender::renderSql(sql
                                                            , NHISNSC_database = paste0(NHISNSC_database, ".dbo")
                                                            , NHISNSC_rawdata = paste0(NHISNSC_rawdata, ".dbo"))$sql
                                sql <- SqlRender::translateSql(sql, targetDialect=attr(connection, "dbms"))$sql

                                DatabaseConnector::executeSql(connection = connection, sql)
                        }

                        if (payer_plan_period == TRUE){
                                sql <- SqlRender::readSql(paste0(sqlFolder,"\\140.Payer_plan_period.sql"))
                                sql <- SqlRender::renderSql(sql
                                                            , NHISNSC_database = paste0(NHISNSC_database, ".dbo")
                                                            , NHISNSC_rawdata = paste0(NHISNSC_rawdata, ".dbo")
                                                            , NHIS_JK = NHIS_JK)$sql
                                sql <- SqlRender::translateSql(sql, targetDialect=attr(connection, "dbms"))$sql

                                DatabaseConnector::executeSql(connection = connection, sql)
                        }

                        if (cost == TRUE){
                                sql <- SqlRender::readSql(paste0(sqlFolder,"\\150.Cost.sql"))
                                sql <- SqlRender::renderSql(sql
                                                            , NHISNSC_database = paste0(NHISNSC_database, ".dbo")
                                                            , NHISNSC_rawdata = paste0(NHISNSC_rawdata, ".dbo")
                                                            , Mapping_database = paste0(Mapping_database, ".dbo")
                                                            , NHIS_20T = NHIS_20T
                                                            , NHIS_30T = NHIS_30T
                                                            , NHIS_60T = NHIS_60T)$sql
                                sql <- SqlRender::translateSql(sql, targetDialect=attr(connection, "dbms"))$sql

                                DatabaseConnector::executeSql(connection = connection, sql)
                        }

                        if (generateEra == TRUE){
                                sql <- SqlRender::readSql(paste0(sqlFolder,"\\300.GenerateEra.sql"))
                                sql <- SqlRender::renderSql(sql
                                                            , NHISNSC_database = paste0(NHISNSC_database, ".dbo")
                                                            , Mapping_database = paste0(Mapping_database, ".dbo")
                                                            )$sql
                                sql <- SqlRender::translateSql(sql, targetDialect=attr(connection, "dbms"))$sql

                                DatabaseConnector::executeSql(connection = connection, sql)
                        }

                        if (dose_era == TRUE){
                                sql <- SqlRender::readSql(paste0(sqlFolder,"\\310.Dose_era.sql"))
                                sql <- SqlRender::renderSql(sql
                                                            , NHISNSC_database = paste0(NHISNSC_database, ".dbo")
                                                            , Mapping_database = paste0(Mapping_database, ".dbo")
                                                            )$sql
                                sql <- SqlRender::translateSql(sql, targetDialect=attr(connection, "dbms"))$sql

                                DatabaseConnector::executeSql(connection = connection, sql)
                        }

                        if (cdm_source == TRUE){
                                sql <- SqlRender::readSql(paste0(sqlFolder,"\\320.CDM_source.sql"))
                                sql <- SqlRender::renderSql(sql
                                                            , NHISNSC_database = paste0(NHISNSC_database, ".dbo"))$sql
                                sql <- SqlRender::translateSql(sql, targetDialect=attr(connection, "dbms"))$sql

                                DatabaseConnector::executeSql(connection = connection, sql)
                        }

                        if (indexing == TRUE){
                                sql <- SqlRender::readSql(paste0(sqlFolder,"\\400.Indexing.sql"))
                                sql <- SqlRender::renderSql(sql
                                                            , NHISNSC_database = NHISNSC_database
                                                            , Mapping_database = Mapping_database)$sql
                                sql <- SqlRender::translateSql(sql, targetDialect=attr(connection, "dbms"))$sql

                                DatabaseConnector::executeSql(connection = connection, sql)
                        }

                        if (constraints == TRUE){
                                sql <- SqlRender::readSql(paste0(sqlFolder,"\\500.Constraints.sql"))
                                sql <- SqlRender::renderSql(sql
                                                            , NHISNSC_database = NHISNSC_database
                                                            , Mapping_database = Mapping_database)$sql
                                sql <- SqlRender::translateSql(sql, targetDialect=attr(connection, "dbms"))$sql

                                DatabaseConnector::executeSql(connection = connection, sql)
                        }
                    
                        if (data_cleansing == TRUE){
                            sql <- SqlRender::readSql(paste0(sqlFolder,"\\900.data_cleansing.sql"))
                            sql <- SqlRender::renderSql(sql
                                                        , NHISNSC_database = paste0(NHISNSC_database, ".dbo")
                                                        , NHISNSC_rawdata = paste0(NHISNSC_rawdata, ".dbo"))$sql
                            sql <- SqlRender::translateSql(sql, targetDialect=attr(connection, "dbms"))$sql
                            
                            DatabaseConnector::executeSql(connection = connection, sql)
                        }
    
                DatabaseConnector::disconnect(connaction)
                        
                }
