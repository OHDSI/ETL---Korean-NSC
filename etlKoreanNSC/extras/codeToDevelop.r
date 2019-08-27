####Make a package skeleton####
#Make a package in the existing local git directory.
#package.skeleton(name = "etlKoreanNSC", encoding = "UTF-8",path = file.path(Sys.getenv("gitFolder"),"ABMI"), force=TRUE)
#devtools::create_description()
#usethis::use_description()
roxygen2::roxygenise()
