# ------------------------------------------------
# Read_epc.R
# ------------------------------------------------
# Code provided as is and can be used or modified freely. 
# ------------------------------------------------
# Author: BIN CHI
# Urban Big Data Centre at University of Glasgow
# Bin.Chi@glasgow.ac.uk
# Date: 1/4/2022

#set the working directory as "D:/OS_Data/EPC"
setwd("D:/OS_Data/EPC")

#create the csv list for 
x1 <- list.files(path = ".", pattern = NULL, all.files = FALSE,
                 full.names = FALSE, recursive = FALSE)

readlist <- paste("D:/OS_Data/EPC",x1,"certificates.csv",sep="/")
#read in all EPC data as one combined dataset
library(data.table)
epcdata = data.table::rbindlist(lapply(readlist, data.table::fread, showProgress = FALSE))

#Convert field name to lowercase
setnames(epcdata, tolower(names(epcdata)))

#save the EPC data in PostGIS(os_ubdc database)
library("RPostgreSQL")
require(RPostgreSQL)
drv=dbDriver("PostgreSQL")
db <- "os_ubdc"
host_db <- "localhost"
db_port <- "5432"
db_user <- "postgres"
#Update your password for the PostGIS database
db_password <- "654321"
con <- dbConnect(RPostgres::Postgres(), dbname = db, host=host_db, port=db_port, user=db_user, password=db_password)
dbWriteTable(con, "epcdata",value =epcdata, append = TRUE, row.names = FALSE)

