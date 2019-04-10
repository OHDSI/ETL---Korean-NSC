# Copyright 2019 Observational Health Data Sciences and Informatics
#
# This file is part of etlKoreanNSC
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

#' setNames Function
#'
#' This function allows to set the names of variables which will be used in executeETL function. The user will be demanded to enter the names of tables and DB schemas.
#' @export
#' @example setNames()
setNames <- function(){
        tryCatch(
                NHISNSC_database,
                error = function(e) {
                  NHISNSC_database <<- readline("Enter name of CDM database schema : ")
                })

        tryCatch(
                NHISNSC_rawdata,
                error = function(e) {
                        NHISNSC_rawdata <<- readline("Enter name of raw database schema : ")
                })
        
        tryCatch(
                Mapping_database,
                error = function(e) {
                        Mapping_database <<- readline("Enter name of Mapping database schema : ")
                })
        
        tryCatch(
                NHIS_20T,
                error = function(e) {
                        NHIS_20T <<- readline("Enter name of NHIS_20T table : ")
                })
        
        tryCatch(
                NHIS_30T,
                error = function(e) {
                        NHIS_30T <<- readline("Enter name of NHIS_30T table : ")
                })
        
        tryCatch(
                NHIS_40T,
                error = function(e) {
                        NHIS_40T <<- readline("Enter name of NHIS_40T table : ")
                })
        
        tryCatch(
                NHIS_60T,
                error = function(e) {
                        NHIS_60T <<- readline("Enter name of NHIS_60T table : ")
                })
        
        tryCatch(
                NHIS_JK,
                error = function(e) {
                        NHIS_JK <<- readline("Enter name of NHIS_JK table : ")
                })
        
        tryCatch(
                NHIS_GJ,
                error = function(e) {
                        NHIS_GJ <<- readline("Enter name of NHIS_GJ table : ")
                })
        
        tryCatch(
                NHIS_YK,
                error = function(e) {
                        NHIS_YK <<- readline("Enter name of NHIS_YK table : ")
                })
}