# ------------------------------------------------
# Read_OSadd.R
# ------------------------------------------------
# Code provided as is and can be used or modified freely. 
# ------------------------------------------------
# Author: BIN CHI
# Urban Big Data Centre at University of Glasgow
# Bin.Chi@glasgow.ac.uk
# Date: 1/4/2022


#read in OS AddressBase Plus
#update your OS Addressbase Plus file path in below code
osadd<-fread("D:/OS_Data/e90_ab_plus_csv_gb/AB_Plus_Data.csv", encoding = 'UTF-8')
Sys.time()
# "2022-02-04 21:35:29 GMT"

#change the field name to lower case and remove the "_" in the field name
setnames(osadd, tolower(names(osadd)))
colnames(osadd) <- gsub("_", "", colnames(osadd))

##save OS Addressbase Plus in the PostGIS database(os_ubdc)
library("RPostgreSQL")
require(RPostgreSQL)
drv=dbDriver("PostgreSQL")
db <- "os_ubdc"
host_db <- "localhost"
db_port <- "5432"
db_user <- "postgres"
#update your password for the PostGIS database
db_password <- "654321"
con <- dbConnect(RPostgres::Postgres(), dbname = db, host=host_db, port=db_port, user=db_user, password=db_password)
dbWriteTable(con, "osadd",value =osadd, append = TRUE, row.names = FALSE)