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

#' connectDB Function
#'
#' This function allows the user to enter the detailed information for connection with Database. If the connection is successed then "Connection success!" will be showd on Console, otherwise "Connection failed" will be showed. 
#' @export
#' @example connectDB()
connectDB <- function(){
        tryCatch(
                dbms,
                error = function(e){
                        dbms <<- readline("Enter your dbms : ")
                })
        
        tryCatch(
                server,
                error = function(e){
                        server <<- readline("Enter your server : ")
                })
        
        tryCatch(
                user,
                error = function(e){
                        user <<- readline("Enter your user id : ")
                })        
                
        tryCatch(
                password,
                error = function(e){
                        password <<- readline("Enter your password : ")
                })                
        
        connectionDetails <<- DatabaseConnector::createConnectionDetails(
                                                        dbms = dbms
                                                        , server = server
                                                        , schema = NHISNSC_rawdata
                                                        , user = user
                                                        , password = password
                                                        )
                
        tryCatch({
                connection <<- DatabaseConnector::connect(connectionDetails)
                DatabaseConnector::dbIsValid(connection)
                                message("Connection success!")},
                error = function(e){
                        stop("Connection failed")
                })
}
