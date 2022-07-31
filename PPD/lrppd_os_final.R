 and # ------------------------------------------------
# lrppd_os_final.R
# ------------------------------------------------
# Code provided as is and can be used or modified freely. 
# ------------------------------------------------
# Author: BIN CHI
# Urban Big Data Centre at University of Glasgow
# Bin.Chi@glasgow.ac.uk
# Date: 1/4/2022

 #################################### Section 1: Load package#################################### 
library("qdap")
library(data.table)
library("RPostgreSQL")
library("sqldf")
library("dplyr")
library(tidyverse)
library(stringr)
library(DBI)
library("dplyr")
 #################################### Section 2: Read in OS AddressBase Plus and Land Registry Price Paid Data(PPD#################################### 
#read in Land Registry PPD
drv=dbDriver("PostgreSQL")
db <- "os_ubdc"
host_db <- "localhost"
db_port <- "5432"
db_user <- "postgres"
#update your password for the PostGIS database(osubdc)
db_password <- "654321"
con <- dbConnect(RPostgres::Postgres(), dbname = db, host=host_db, port=db_port, user=db_user, password=db_password)

tran <- dbGetQuery(con,"select * from pricepaid") 
#read in OS AddressBase Plus
add <- dbGetQuery(con,"select * from  addressgb") 
#################################### Section 3: OS AddressBase data and Land Registry PPD pre-processing#################################### 

######### part 1: Land Registry PPD pre-processing #########
#remove the whitespace from start and end of postcode field in Land Registry PPD 
tran$postcode  <- str_trim(tran$postcode)
#converts Land Registry PPD from dataframe to data tables
setDT(tran)
#create the postset field based on postcode field 
tran[nchar(postcode)>=6,postset :=substring(postcode,0,nchar(postcode)-2)]
#remove the whitespace from start and end of postset field in Land Registry PPD 
tran$postset  <- str_trim(tran$postset)

######### part 2:OS AddressBase data pre-processing #########
#format the address fields in OS AddressBase data for data linkage process
add$paostartnumber<-as.character(add$paostartnumber)
add$paoendnumber<-as.character(add$paoendnumber)
add$saostartnumber<-as.character(add$saostartnumber)
add$saoendnumber<-as.character(add$saoendnumber)
add$buildingnumber<-as.character(add$buildingnumber)

add[is.na(add$buildingnumber),"buildingnumber"] <- ""
add[is.na(add$paostartnumber),"paostartnumber"] <- ""
add[is.na(add$paoendnumber),"paoendnumber"] <- ""
add[is.na(add$saostartnumber),"saostartnumber"] <- ""
add[is.na(add$saoendnumber),"saoendnumber"] <- ""

add[is.na(add)] <- ""
#remove the whitespace from start and end of postcode field(postcodelocator) in OS AddressBase data
add$postcodelocator  <- str_trim(add$postcodelocator)
#converts OS AddressBase data from dataframe to data.tables
setDT(add)
#create the postset field based on postcode field 
add[nchar(postcodelocator)>=6,postset :=substring(postcodelocator,0,nchar(postcodelocator)-2)]
#remove the whitespace from start and end of postset field in OS AddressBase data
add$postset  <- str_trim(add$postset)


#remove the OS AddressBase data which postset does not exist in Land Registry PPD
dim(add)
#36110437       30
add<-add[add$postset %in% tran$postset, ]
dim(add)
#32743067       30
#read in the lookup table for linkage
link1<-fread("lookup_1.csv")
######### part 3: create the funtions used for data linkage #########
#function: remove the same transaciton from one data to another dataset
matchleft <- function(x,y){
  next0 <- x[!(x$transactionid %in% y$transactionid),]
  return(next0)
  
}
#function: indentify the same transaction data
tranneed<- function(x,y){
  next0 <- x[(x$transactionid %in% y$transactionid),]
  return(next0)
  
}

#function: get the one to one linkage results
uniqueresult <- function(x){
  
  dt <- as.data.table(x)
  
  esummary<-dt[,.(count=.N),by=transactionid]
  
  idd1 <- esummary[esummary$count==1,]
  
  result1 <- x[x$transactionid %in% idd1$transactionid,]
  
  return(result1)
}
#function: get the one to many linkage results
doubleresult <-  function(x){
  
  dt <- as.data.table(x)
  
  esummary<-dt[,.(count=.N),by=transactionid]
  
  idd2 <- esummary[esummary$count!=1,]
  
  need1 <- x[x$transactionid %in% idd2$transactionid,]
  
  return(need1)
}

#################################### section 4:data linkage#################################### 
###########################stage 1###########################
#################PAON is NULL and SAON is not NULL#################
trannull <- tran[tran$paon=="",]
trannull<- trannull[trannull$saon !="",]
#provide OS AddressBase data for this stage 1 linkage
addnull<-add[add$postset %in% trannull$postset, ]
setDT(trannull)
setDT(addnull)

############method 1 #SAON is equal to pp ############m
#create the full address for address matching      
trannull$addressf <-paste(trannull$postcode,trannull$saon,sep=",")
addnull$addressf <- paste(addnull$postcodelocator,addnull$pp,sep=",")

#match the two datasets by the common field
null1<- inner_join(trannull,addnull,by="addressf")

#keep transactionid and uprn field in the linked data
needlist1<-c("transactionid","uprn")
null1<-null1[,..needlist1]
#create a new field to record the linkage method
null1$method<-"method1"
#extract the one to one linkage result
null1u<-uniqueresult(null1)

############method 2 SAON is equal to paostartnumber1############
#remove the linked transactions in the previous link method 
trannull<-matchleft(trannull,null1)

add3 <- merge(addnull,link1,by="saotext")
add3$bb1<- paste(add3$paostartnumber,add3$string,sep="")

trannull$addressf <-paste(trannull$postcode,trannull$saon,sep=",")

add3$addressf <- paste(add3$postcodelocator,add3$bb1,sep=",")

null2<- inner_join(trannull,add3,by="addressf")

null2<-null2[,..needlist1]

null2$method<-"method2"

null2u<-uniqueresult(null2)

rm(add3)
############method 3 SAON is equal to ss############
trannull<-matchleft(trannull,null2)

trannull$addressf <-paste(trannull$postcode,trannull$saon,sep=",")
addnull$addressf <- paste(addnull$postcodelocator,addnull$ss,sep=",")

null3<- inner_join(trannull,addnull,by="addressf")

null3<-null3[,..needlist1]

null3$method<-"method3"

null3u<-uniqueresult(null3)

############method 4 SAON is equal to buildingname############
trannull<-matchleft(trannull,null3)

trannull$addressf <-paste(trannull$postcode,trannull$saon,sep=",")
addnull$addressf <- paste(addnull$postcodelocator,addnull$buildingname,sep=",")
#y$addressfinal <- gsub(" ", "", y$addressfinal)
trannull$addressf <-gsub(" ", "",trannull$addressf )
addnull$addressf <- gsub(" ", "",addnull$addressf)

null4<- inner_join(trannull,addnull,by="addressf")

null4<-null4[,..needlist1]

null4$method<-"method4"

null4u<-uniqueresult(null4)
############method 5 SAON is equal to paotext############
trannull<-matchleft(trannull,null4)
dim(trannull)
#17
trannull$addressf <-paste(trannull$postcode,trannull$saon,sep=",")
addnull$addressf <- paste(addnull$postcodelocator,addnull$paotext,sep=",")

null5<- inner_join(trannull,addnull,by="addressf")

null5<-null5[,..needlist1]

null5$method<-"method5"

null5u<-uniqueresult(null5)
############method 6  saonstreet is buildingname############
trannull<-matchleft(trannull,null5)

trannull$pastr <- paste(trannull$saon,trannull$street,sep=" ")
trannull$addressf <-paste(trannull$postcode,trannull$pastr,sep=",")
addnull$addressf <- paste(addnull$postcodelocator,addnull$buildingname,sep=",")
trannull$addressf <-gsub(" ", "",trannull$addressf )
addnull$addressf <- gsub(" ", "",addnull$addressf)

null6<- inner_join(trannull,addnull,by="addressf")

null6<-null6[,..needlist1]

null6$method<-"method6"

null6u<-uniqueresult(null6)
############method 7 saon(remove "The" string)is equal to buildingname############
trannull<-matchleft(trannull,null6)
dim(trannull)
trannull$saon_1<-gsub("THE ","",trannull$saon)
trannull$addressf <-paste(trannull$postcode,trannull$saon_1,sep=",")

addnull$addressf <- paste(addnull$postcodelocator,addnull$buildingname,sep=",")
trannull$addressf <-gsub(" ", "",trannull$addressf )
addnull$addressf <- gsub(" ", "",addnull$addressf)

null7<- inner_join(trannull,addnull,by="addressf")

null7<-null7[,..needlist1]

null7$method<-"method7"

null7u<-uniqueresult(null7)

############sum up the linked result in stage 1############

#combine allone to one linkage data in stage 1
lu1 = list(null1u,null2u,null3u,null4u,null5u,null6u,null7u)
stage1<- rbindlist(lu1 )

#save the stage 1 linked result in PostGIS database
dbWriteTable(con, "stage1",value =stage1, append = TRUE, row.names = FALSE)

#remove the linked results in this stage
rm(null1u,null2u,null3u,null4u,null5u,null6u,null7u)
rm(lu1)
rm(null1,null2,null3,null4,null5,null6,null7)
rm(trannull,addnull)
###########################stage 2###########################
#remove the sucessful linked transaction from Stage 1 in Land Registry PPD data
tran<-matchleft(tran,stage1)

############method 8 PAON is equal to buildingname or buildingnumber or bb############
#PAON is equal to buildingname
tran$addressf <-paste(tran$postcode,tran$paon,sep=",")
add$addressf <- paste(add$postcodelocator,add$buildingname,sep=",")

taba1 <- inner_join(tran,add,by="addressf")
taba1<-taba1[,..needlist1]

#PAON is equal to buildingnumber
add$addressf <- paste(add$postcodelocator,add$buildingnumber,sep=",")

taba2 <- inner_join(tran,add,by="addressf")
taba2<-taba2[,..needlist1]

#PAON is equal to bb
add$addressf <- paste(add$postcodelocator,add$bb,sep=",")
taba3 <- inner_join(tran,add,by="addressf")
taba3<-taba3[,..needlist1]

#combine the linked results
x1<-rbindlist(list(taba1,taba2,taba3) )
setDT(x1)

#keep the one to one linked result
x1<-unique(x1)
rm(taba1,taba2,taba3)
x<- uniqueresult(x1)

x<-x[,..needlist1]
x$method<-"method8"
#extract the one to many linked result for the following linkage process in Stage 2
need2 <- doubleresult(x1)
rm(x1)
#extract the transaction record which show one ot many linkage relationship in the method 8
tran1 <- tran[tran$transactionid %in% need2$transactionid,]
#extract the records with SAON does not have NULL vaule
tran12<- tran1[tran1$saon !="",]
#extract the records with SAON is NULL vaule. this will be used at the end of the stage 2 linkage
tran11<- tran1[tran1$saon=="",]
#extract the OS AddressBase data for the following linkage process in Stage 2
add1<- add[add$postcodelocator %in% tran1$postcode, ]
############method 9 PAON is NULL and saotext is equal to buildingname############

tran121<-tran12[tran12$paon=="",]

tran121$add1ressf <-paste(tran121$postcode,tran121$saon,sep=",")
tran121$add1ressf <-paste(tran121$add1ressf,tran121$street,sep=" ")

add1$add1ressf <- paste(add1$postcodelocator,add1$buildingname,sep=",")

y01 <- inner_join(tran121,add1,by="add1ressf")
needlist11<-c("transactionid","uprn")

y01<-y01[,..needlist11]
f0_1<-uniqueresult(y01)

tran121<-matchleft(tran121,f0_1)

f0_1$method<-"method9"
############method 10 PAON is not NULL and is equal to pp,SAON is equal to saotext############
tran12<-tran12[tran12$paon!="",]

tran12$add1ressf <-paste(tran12$postcode,tran12$paon,sep=",")
tran12$add1ressf <-paste(tran12$add1ressf,tran12$saon,sep=",")


add1$add1ressf <- paste(add1$postcodelocator,add1$pp,sep=",")
add1$add1ressf <- paste(add1$add1ressf,add1$saotext,sep=",")

y1 <- inner_join(tran12,add1,by="add1ressf")
need2_test<-need2_2[need2_2$transactionid %in% y1$transactionid,]

y1<-y1[,..needlist11]
f1<-uniqueresult(y1)

f1$method<-"method10"
############method 11 PAON PAON is not NULL and is equal to buildingname,SAON is equal to saotext############
tran12<- matchleft(tran12,f1)


tran12$add1ressf <-paste(tran12$postcode,tran12$paon,sep=",")
tran12$add1ressf <-paste(tran12$add1ressf,tran12$saon,sep=",")

add1$add1ressf <- paste(add1$postcodelocator,add1$buildingname,sep=",")
add1$add1ressf <- paste(add1$add1ressf,add1$saotext,sep=",")

y2 <- inner_join(tran12,add1,by="add1ressf")

y2 <-y2[,..needlist11]
f2<-uniqueresult(y2)

f2$method<-"method11"
############method 12 PAON is not NULL and is equal to buildingnumber,SAON is equal to saotext############

tran12<- matchleft(tran12,f2)

tran12$add1ressf <-paste(tran12$postcode,tran12$paon,sep=",")
tran12$add1ressf <-paste(tran12$add1ressf,tran12$saon,sep=",")

add1$add1ressf <- paste(add1$postcodelocator,add1$buildingnumber,sep=",")
add1$add1ressf <- paste(add1$add1ressf,add1$saotext,sep=",")

y3 <- inner_join(tran12,add1,by="add1ressf")

y3 <-y3[,..needlist11]
f3<-uniqueresult(y3)

f3$method<-"method12"
############method 13 PAON is not NULL and is not NULL and is equal to bb,SAON is equal to saotext ############
tran12<- matchleft(tran12,f3)

tran12$add1ressf <-paste(tran12$postcode,tran12$paon,sep=",")
tran12$add1ressf <-paste(tran12$add1ressf,tran12$saon,sep=",")

add1$add1ressf <- paste(add1$postcodelocator,add1$bb,sep=",")
add1$add1ressf <- paste(add1$add1ressf,add1$saotext,sep=",")

y4 <- inner_join(tran12,add1,by="add1ressf")
y4 <-y4[,..needlist11]

f4<-uniqueresult(y4)

f4$method<-"method13"
#################method 14 PAON is not NULL and is equal to buildingname,SAON is equal to subbuildingname#################
tran12<- matchleft(tran12,f4)

tran12$add1ressf <-paste(tran12$postcode,tran12$paon,sep=",")
tran12$add1ressf <-paste(tran12$add1ressf,tran12$saon,sep=",")

add1$add1ressf <- paste(add1$postcodelocator,add1$buildingname,sep=",")
add1$add1ressf <- paste(add1$add1ressf,add1$subbuildingname,sep=",")


y5 <- inner_join(tran12,add1,by="add1ressf")
y5<-y5[,..needlist11]

f5<-uniqueresult(y5)

f5$method<-"method14"
############method 15 PAON is not NULL and is equal to buildingnumber,SAON is equal to subbuildingname############
tran12<- matchleft(tran12,f5)

tran12$add1ressf <-paste(tran12$postcode,tran12$paon,sep=",")
tran12$add1ressf <-paste(tran12$add1ressf,tran12$saon,sep=",")

add1$add1ressf <- paste(add1$postcodelocator,add1$buildingnumber,sep=",")
add1$add1ressf <- paste(add1$add1ressf,add1$subbuildingname,sep=",")

y6 <- inner_join(tran12,add1,by="add1ressf")
y6<-y6[,..needlist11]

f6<-uniqueresult(y6)

f6$method<-"method15"
############method 16 PAON is not NULL and is equal to bb,SAON is equal to subbuildingname############
tran12<- matchleft(tran12,f6)

tran12$add1ressf <-paste(tran12$postcode,tran12$paon,sep=",")
tran12$add1ressf <-paste(tran12$add1ressf,tran12$saon,sep=",")

add1$add1ressf <- paste(add1$postcodelocator,add1$bb,sep=",")
add1$add1ressf <- paste(add1$add1ressf,add1$subbuildingname,sep=",")

y7 <- inner_join(tran12,add1,by="add1ressf")
y7<-y7[,..needlist11]

f7<-uniqueresult(y7)

f7$method<-"method16"
############method 17  PAON is not NULL and is equal to paotext,SAON is equal to ss ############
tran12<- matchleft(tran12,f7)

tran12$add1ressf <-paste(tran12$postcode,tran12$paon,sep=",")
tran12$add1ressf <-paste(tran12$add1ressf,tran12$saon,sep=",")

add1$add1ressf <- paste(add1$postcodelocator,add1$paotext,sep=",")
add1$add1ressf <- paste(add1$add1ressf,add1$ss,sep=",")

y8 <- inner_join(tran12,add1,by="add1ressf")
length(unique(y8$transactionid))

y8<-y8[,..needlist11]

f8<-uniqueresult(y8)

f8$method<-"method17"
############method 18 PAON is not NULL and is equal to bb ,SAON is equal to ss############
tran12<- matchleft(tran12,f8)


tran12$add1ressf <-paste(tran12$postcode,tran12$paon,sep=",")
tran12$add1ressf <-paste(tran12$add1ressf,tran12$saon,sep=",")

add1$add1ressf <- paste(add1$postcodelocator,add1$bb,sep=",")
add1$add1ressf <- paste(add1$add1ressf,add1$ss,sep=",")

y9 <- inner_join(tran12,add1,by="add1ressf")
y9<-y9[,..needlist11]
f9<-uniqueresult(y9)

f9$method<-"method18"
############method 19 PAON is not NULL and is equal to buildingname and FLATSAON is equal to subbuildingname############
tran12<- matchleft(tran12,f9)

tran12$flats <- paste("FLAT ",tran12$saon,sep="")

tran12$add1ressf <-paste(tran12$postcode,tran12$paon,sep=",")
tran12$add1ressf <-paste(tran12$add1ressf,tran12$flats,sep=",")

add1$add1ressf <- paste(add1$postcodelocator,add1$buildingname,sep=",")
add1$add1ressf <- paste(add1$add1ressf,add1$subbuildingname,sep="," )

y10 <- inner_join(tran12,add1,by="add1ressf")
y10<-y10[,..needlist11]
f10<-uniqueresult(y10)

f10$method<-"method19"
############method 20 PAON is not NULL and is equal to paotext, FLATSAON is equal to saotext############
tran12<- matchleft(tran12,f10)


tran12$flats <- paste("FLAT ",tran12$saon,sep="")
tran12$add1ressf <-paste(tran12$postcode,tran12$paon,sep=",")
tran12$add1ressf <-paste(tran12$add1ressf,tran12$flats,sep=",")

add1$add1ressf <- paste(add1$postcodelocator,add1$paotext,sep=",")
add1$add1ressf <- paste(add1$add1ressf,add1$saotext,sep=",")

y11 <- inner_join(tran12,add1,by="add1ressf")
y11<-y11[,..needlist11]
f11<-uniqueresult(y11)

f11$method<-"method20"
############method 21 PAON is not NULL and is equal to buildingname,FLATSAON is equal to subbuildingnamenew############
tran12<- matchleft(tran12,f11)

add1$subbuildingnamenew <- gsub("UNIT","FLAT",add1$subbuildingname)
add1$subbuildingnamenew <- gsub("APARTMENT","FLAT",add1$subbuildingnamenew)

tran12$flats <- paste("FLAT ",tran12$saon,sep="")
tran12$add1ressf <-paste(tran12$postcode,tran12$paon,sep=",")
tran12$add1ressf <-paste(tran12$add1ressf,tran12$flats,sep=",")

add1$add1ressf <- paste(add1$postcodelocator,add1$buildingname,sep=",")
add1$add1ressf <- paste(add1$add1ressf,add1$subbuildingnamenew,sep=",")

y12 <- inner_join(tran12,add1,by="add1ressf")
y12<-y12[,..needlist11]
f12<-uniqueresult(y12)

f12$method<-"method21"
############method 22 PAON is not NULL and is equal to buildingname,SAON is equal to fss############
tran12<- matchleft(tran12,f12)

add1$fss<-paste("FLAT ",add1$ss,sep="")

tran12$add1ressf <-paste(tran12$postcode,tran12$paon,sep=",")
tran12$add1ressf <-paste(tran12$add1ressf,tran12$saon,sep=",")

add1$add1ressf <- paste(add1$postcodelocator,add1$buildingname,sep=",")
add1$add1ressf <- paste(add1$add1ressf,add1$fss,sep=",")

y13 <- inner_join(tran12,add1,by="add1ressf")
y13<-y13[,..needlist11]
f13<-uniqueresult(y13)

f13$method<-"method22"
############method 23 PAON is not NULL and is equal to paotext and SAON is equal to fss############
tran12<- matchleft(tran12,f13)

add1$fss<-paste("FLAT ",add1$ss,sep="")

tran12$add1ressf <-paste(tran12$postcode,tran12$paon,sep=",")
tran12$add1ressf <-paste(tran12$add1ressf,tran12$saon,sep=",")

add1$add1ressf <- paste(add1$postcodelocator,add1$paotext,sep=",")
add1$add1ressf <- paste(add1$add1ressf,add1$fss,sep=",")

y14 <- inner_join(tran12,add1,by="add1ressf")
y14<-y14[,..needlist11]

f14<-uniqueresult(y14)

f14$method<-"method23"
############method 24 PAON is not NULL and  is equal to bb,SAON is equal to fss############
tran12<- matchleft(tran12,f14)

add1$fss<-paste("FLAT ",add1$ss,sep="")

tran12$add1ressf <-paste(tran12$postcode,tran12$paon,sep=",")
tran12$add1ressf <-paste(tran12$add1ressf,tran12$saon,sep=",")

add1$add1ressf <- paste(add1$postcodelocator,add1$bb,sep=",")
add1$add1ressf <- paste(add1$add1ressf,add1$fss,sep=",")

tran12$add1ressf <-gsub(" ", "",tran12$add1ressf )
add1$add1ressf <- gsub(" ", "",add1$add1ressf)

y15 <- inner_join(tran12,add1,by="add1ressf")
y15<-y15[,..needlist11]

f15<-uniqueresult(y15)

f15$method<-"method24"
############method 25 PAON is not NULL and SAONPAON is equal to buildingname############
tran12<- matchleft(tran12,f15)

tran12$saonpaon <- paste(tran12$saon,tran12$paon,sep=" ")
tran12$add1ressf <-paste(tran12$postcode,tran12$saonpaon,sep=",")

add1$add1ressf <- paste(add1$postcodelocator,add1$buildingname,sep=",")

y16 <- inner_join(tran12,add1,by="add1ressf")
y16<-y16[,..needlist11]
f16<-uniqueresult(y16)

f16$method<-"method25"
############method 26 PAON is not NULL and is equal to paotext,SAON is equal to saotext############
tran12<- matchleft(tran12,f16)

tran12$add1ressf <-paste(tran12$postcode,tran12$paon,sep=",")
tran12$addressf <-paste(tran12$add1ressf,tran12$saon,sep=",")

add1$add1ressf <- paste(add1$postcodelocator,add1$paotext,sep=",")
add1$addressf <- paste(add1$add1ressf,add1$saotext,sep=",")

y17 <- inner_join(tran12,add1,by="add1ressf")
y17<-y17[,..needlist11]

f17<-uniqueresult(y17)

f17$method<-"method26"
############method 27  PAON is not NULL and isequal to buildingname,SAON is equal to ss############
tran12<- matchleft(tran12,f17)

tran12$add1ressf <-paste(tran12$postcode,tran12$paon,sep=",")
tran12$add1ressf <-paste(tran12$add1ressf,tran12$saon,sep=",")

add1$add1ressf <- paste(add1$postcodelocator,add1$buildingname,sep=",")
add1$add1ressf <- paste(add1$add1ressf,add1$ss,sep=",")

y18 <- inner_join(tran12,add1,by="add1ressf")
y18<-y18[,..needlist11]

f18<-uniqueresult(y18)

y18_left<-y18[!(y18$transactionid %in% f18$transactionid),]

f18$method<-"method27"
############method 28 PAON is not NULL and SAONPAON is equal to subbname############
tran12<- matchleft(tran12,f18)

tran12$saonpaon <- paste(tran12$saon,tran12$paon,sep=" ")
tran12$add1ressf <-paste(tran12$postcode,tran12$saonpaon,sep=",")

add1$add1ressf <- paste(add1$postcodelocator,add1$subbuildingname,sep=",")
add1$add1ressf <- paste(add1$add1ressf,add1$buildingname,sep=" ")

y19 <- inner_join(tran12,add1,by="add1ressf")
y19<-y19[,..needlist11]
f19<-uniqueresult(y19)

f19$method<-"method28"
############method 29  PAON is not NULL and  is equal to bb,FLATSAON is equal to saotext############
tran12<- matchleft(tran12,f19)

tran12$flats <- paste("FLAT ",tran12$saon,sep="")

tran12$add1ressf <-paste(tran12$postcode,tran12$paon,sep=",")
tran12$add1ressf <-paste(tran12$add1ressf,tran12$flats,sep=",")

add1$add1ressf <- paste(add1$postcodelocator,add1$bb,sep=",")
add1$add1ressf <- paste(add1$add1ressf,add1$saotext,sep=",")

y20 <- inner_join(tran12,add1,by="add1ressf")
y20<-y20[,..needlist11]
f20<-uniqueresult(y20)

f20$method<-"method29"
############method 30 PAON is not NULL and is equal to paotext,SAON is equal to pp############
tran12<- matchleft(tran12,f20)

tran12$add1ressf <-paste(tran12$postcode,tran12$paon,sep=",")
tran12$add1ressf <-paste(tran12$add1ressf,tran12$saon,sep=",")

add1$add1ressf <- paste(add1$postcodelocator,add1$paotext,sep=",")
add1$add1ressf <- paste(add1$add1ressf,add1$pp,sep=",")
y21 <- inner_join(tran12,add1,by="add1ressf")
y21<-y21[,..needlist11]

f21<-uniqueresult(y21)

f21$method<-"method30"
############method 31 PAON is not NULL and is equal to buildingname,SAON is equal to pp11############
tran12<- matchleft(tran12,f21)

tran12$add1ressf <-paste(tran12$postcode,tran12$paon,sep=",")
tran12$add1ressf <-paste(tran12$add1ressf,tran12$saon,sep=",")

add1$add1ressf <- paste(add1$postcodelocator,add1$buildingname,sep=",")
add1$pp<-str_trim(add1$pp)
add1$add1ressf <- paste(add1$add1ressf,add1$pp,sep=",")

y22 <- inner_join(tran12,add1,by="add1ressf")
y22<-y22[,..needlist11]
f22<-uniqueresult(y22)

f22$method<-"method31"
############method 32 PAON is not NULL and is equal to buildingname,SAON is equal to subbuildingnamenew1############
tran12<- matchleft(tran12,f22)

#add1$subbuildingnamenew <- gsub("UNIT","FLAT",add1$subbuildingname)
add1$subbuildingnamenew1 <- gsub("FLAT","APARTMENT",add1$subbuildingname)

tran12$add1ressf <-paste(tran12$postcode,tran12$paon,sep=",")
tran12$add1ressf <-paste(tran12$add1ressf,tran12$saon,sep=",")

add1$add1ressf <- paste(add1$postcodelocator,add1$buildingname,sep=",")

add1$add1ressf <- paste(add1$add1ressf,add1$subbuildingnamenew1,sep=",")

y23 <- inner_join(tran12,add1,by="add1ressf")

y23<-y23[,..needlist11]
f23<-uniqueresult(y23)

f23$method<-"method32"
############method 33 PAON is not NULL and is  is equal to buildingname,SAON is equal to newp ############
tran12<- matchleft(tran12,f23)

add3 <- merge(add1,link1,by="saotext")

add3$newp<-paste(add3$pp,add3$string,sep="")
add3$newp<-str_trim(add3$newp)
add3$newp<-gsub(" ", "",add3$newp)
tran12$add1ressf <-paste(tran12$postcode,tran12$paon,sep=",")
tran12$add1ressf <-paste(tran12$add1ressf,tran12$saon,sep=",")

add3$add1ressf <- paste(add3$postcodelocator,add3$buildingname,sep=",")

add3$add1ressf <- paste(add3$add1ressf,add3$newp,sep=",")


y24 <- inner_join(tran12,add3,by="add1ressf")
rm(add3)
y24<-y24[,..needlist11]
f24<-uniqueresult(y24)

f24$method<-"method33"
############method 34 PAON is not NULL and  is equal to buildingname,SAON is equal to fnewp############
tran12<- matchleft(tran12,f24)

add3 <- merge(add1,link1,by="saotext")
add3$newp<-paste(add3$pp,add3$string,sep="")
add3$newp<-str_trim(add3$newp)
add3$newp<-gsub(" ", "",add3$newp)
add3$fnewp<-paste("FLAT ",add3$newp,sep="")

tran12$add1ressf <-paste(tran12$postcode,tran12$paon,sep=",")
tran12$add1ressf <-paste(tran12$add1ressf,tran12$saon,sep=",")

add3$add1ressf <- paste(add3$postcodelocator,add3$buildingname,sep=",")

add3$add1ressf <- paste(add3$add1ressf,add3$fnewp,sep=",")

y25 <- inner_join(tran12,add3,by="add1ressf")
y25<-y25[,..needlist11]
f25<-uniqueresult(y25)

f25$method<-"method34"
rm(add3)

############method 35 PAON is not NULL and is is equal to buildingname and SAON is equal to subbuildingname (remove blank space for both side) ############
tran12<- matchleft(tran12,f26)

tran12$add1ressf <-paste(tran12$postcode,tran12$paon,sep=",")
tran12$add1ressf <-paste(tran12$add1ressf,tran12$saon,sep=",")

add1$add1ressf <- paste(add1$postcodelocator,add1$buildingname,sep=",")

add1$add1ressf <- paste(add1$add1ressf,add1$subbuildingname,sep=",")

tran12$add1ressf <-gsub(" ", "",tran12$add1ressf )
add1$add1ressf <- gsub(" ", "",add1$add1ressf)

y26 <- inner_join(tran12,add1,by="add1ressf")
y26<-y26[,..needlist11]
f26<-uniqueresult(y26)

f26$method<-"method35"
############method 36 PAON is not NULL and is equal to buildingname and SAON is equal to saotext5 (remove blank space for both side)############
tran12<- matchleft(tran12,f26)

add1$saotext5<-gsub("FLAT","BOX ROOM",add1$saotext)

tran12$add1ressf <-paste(tran12$postcode,tran12$paon,sep=",")
tran12$add1ressf <-paste(tran12$add1ressf,tran12$saon,sep=",")

add1$add1ressf <- paste(add1$postcodelocator,add1$buildingname,sep=",")
add1$add1ressf <- paste(add1$add1ressf,add1$saotext5,sep=",")

tran12$addressf <-gsub(" ", "",tran12$addressf )
add1$addressf <- gsub(" ", "",add1$addressf)
y27 <- inner_join(tran12,add1,by="add1ressf")

y27<-y27[,..needlist11]
f27<-uniqueresult(y27)

f27$method<-"method36"
############method 37 PAON is not NULL and is equal to bb,SAON is equal to apss (remove blank space for both side) ############
tran12<- matchleft(tran12,f27)

add1$apss<-paste("APARTMENT ",add1$saotext,sep="")

tran12$add1ressf <-paste(tran12$postcode,tran12$paon,sep=",")
tran12$add1ressf <-paste(tran12$add1ressf,tran12$saon,sep=",")

add1$add1ressf <- paste(add1$postcodelocator,add1$bb,sep=",")
add1$add1ressf <- paste(add1$add1ressf,add1$apss,sep=",")

tran12$add1ressf <-gsub(" ", "",tran12$add1ressf )
add1$add1ressf <- gsub(" ", "",add1$add1ressf)

y28 <- inner_join(tran12,add1,by="add1ressf")
y28<-y28[,..needlist11]
f28<-uniqueresult(y28)

f28$method<-"method37"
############method 38 PAON is not NULL and is equal to buildingname,SAON6 is equal to saotext (remove blank space for both side) ############
tran12<- matchleft(tran12,f28)

tran12$saon_6<-gsub("STORE FLAT","FLAT",tran12$saon)

tran12$add1ressf <-paste(tran12$postcode,tran12$paon,sep=",")
tran12$add1ressf <-paste(tran12$add1ressf,tran12$saon_6,sep=",")

add1$add1ressf <- paste(add1$postcodelocator,add1$buildingname,sep=",")
add1$add1ressf <- paste(add1$add1ressf,add1$saotext,sep=",")

tran12$add1ressf <-gsub(" ", "",tran12$add1ressf )
add1$add1ressf <- gsub(" ", "",add1$add1ressf)

y29 <- inner_join(tran12,add1,by="add1ressf")
y29<-y29[,..needlist11]
f29<-uniqueresult(y29)

f29$method<-"method38"
############method 39 PAON is not NULL and is  equal to buildingname,SAON is equal to ssend (remove blank space for both side) ############
tran12<- matchleft(tran12,f29)

tran12$add1ressf <-paste(tran12$postcode,tran12$paon,sep=",")
tran12$add1ressf <-paste(tran12$add1ressf,tran12$saon,sep=",")

add1$add1ressf <- paste(add1$postcodelocator,add1$buildingname,sep=",")
add1$add1ressf <- paste(add1$add1ressf,add1$saoendnumber,sep=",")
add1$add1ressf <- paste(add1$add1ressf,add1$saoendsuffix,sep="")
tran12$add1ressf <-gsub(" ", "",tran12$add1ressf )
add1$add1ressf <- gsub(" ", "",add1$add1ressf)


y30 <- inner_join(tran12,add1,by="add1ressf")
y30<-y30[,..needlist11]
f30<-uniqueresult(y30)

f30$method<-"method39"

tran12<- matchleft(tran12,f30)

#(PAON is equal to buildingname  or  buildingnumber or  bb) and SAON is NULL
############method 40 PAON is equal to pp ############
tran11$add1ressf <-paste(tran11$postcode,tran11$paon,sep=",")

add1$add1ressf <- paste(add1$postcodelocator,add1$pp,sep=",")

y200 <- inner_join(tran11,add1,by="add1ressf")

y200<-y200[,..needlist11]
f200<-uniqueresult(y200)

f200$method<-"method40"

############method 41 PAON is equal to paotext############
tran11<- matchleft(tran11,f200)
dim(tran11)
#38158    19
tran11$add1ressf <-paste(tran11$postcode,tran11$paon,sep=",")

add1$add1ressf <- paste(add1$postcodelocator,add1$paotext,sep=",")

y201 <- inner_join(tran11,add1,by="add1ressf")

y201<-y201[,..needlist11]
f201<-uniqueresult(y201)

f201$method<-"method41"

tran11<- matchleft(tran11,f201)
#keep the usefully field in OS AddressBase data
addlist1<-c("uprn","parentuprn","postcodelocator","class","postcode","buildingname","buildingnumber","subbuildingname","paostartnumber","paostartsuffix","paoendnumber","paoendsuffix","paotext","saostartnumber","saostartsuffix","saoendnumber","saoendsuffix","saotext","streetdescription","locality","dependentlocality","townname","administrativearea","posttown","bb","ss","pp","postset")
add1<-add1[,..addlist1]
#################sum up the linked data in stage 2##################
#combine allone to one linkage data in stage 2
ly = list(f0_1,f1,f2,f3,f4,f5,f6,f7,f8,f9,f10,f11,f12,f13,f14,f15,f16,f17,f18,f19,f20,f21,f22,f23,f24,f25,f26,f27,f28,f29,f30,f200,f201,x)
stage2<-rbindlist(ly ,use.names=TRUE,fill=T)

#save the stage 1  result in PostGIS database
dbWriteTable(con, "stage2",value =stage2, append = TRUE, row.names = FALSE)
dbWriteTable(con, "x",value =x, append = TRUE, row.names = FALSE)
dbWriteTable(con, "stage2_tran11_left",value =tran11, append = TRUE, row.names = FALSE)
dbWriteTable(con, "stage2_tran12_left",value =tran12, append = TRUE, row.names = FALSE)

#remove the linked results in this stage
rm(need2)

rm(x)
rm(f0_1,f1,f2,f3,f4,f5,f6,f7,f8,f9,f10,f11,f12,f13,f14,f15,f16,f17,f18,f19,f20,f21,f22,f23,f24,f25,f26,f27,f28,f29,f30,f200,f201)
rm(y1_left,y2_left,y3_left,y4_left,y5_left,y6_left,y7_left,y8_left,y9_left,y10_left,y11_left,y12_left,y13_left,y14_left,y15_left,y16_left,y17_left,y18_left,y19_left,y20_left,y21_left,y22_left,y23_left,y24_left,y25_left,y26_left,y27_left,y28_left,y29_left,y30_left,y200_left,y201_left)
rm(y01,y1,y2,y3,y4,y5,y6,y7,y8,y9,y10,y11,y12,y13,y14,y15,y16,y17,y18,y19,y20,y21,y22,y23,y24,y25,y26,y27,y28,y29,y30,y200,y201)
rm(add1)
rm(tran11,tran12,tran121)

###########################stage 3###########################
############method 42 PAON is equal to paostartnumber############
tran1 <- tran[tran$paon!="",]
tran88 <- matchleft(tran1,stage2)

add88<- add[add$postcodelocator %in% tran88$postcode, ]

add88$pp<-str_trim(add88$pp)

add11<- add88[!is.na(add88$pp),]
rm(add88)

tran88$addressf <-paste(tran88$postcode,tran88$paon,sep=",")
add11$addressf <- paste(add11$postcodelocator,add11$paostartnumber,sep=",")


z <- inner_join(tran88,add11,by="addressf")
#get the one to one linkage result in this method
finalz <- uniqueresult(z)
finalz$method<-"method42"

needlistf<-c("transactionid","uprn","method")
finalz<-finalz[,..needlistf]

needz <- doubleresult(z)
############method 43 PAON is equal to paostartnumber and SAON is equal to flatpao1############
#extract the transacion records for the following data linkage process in stage 3
tranpro22 <- tranneed(tran88,needz)
tranpro22$add11ressf <-paste(tranpro22$postcode,tranpro22$paon,sep=",")
tranpro22$add11ressf <-paste(tranpro22$add11ressf,tranpro22$saon,sep=",")

add11$flatp <- paste("FLAT ", add11$paostartsuffix,sep="")
add11$add11ressf <- paste(add11$postcodelocator,add11$paostartnumber,sep=",")
add11$add11ressf <- paste(add11$add11ressf,add11$flatp ,sep=",")

zz1<- inner_join(tranpro22,add11,by="add11ressf")
needlist12<-c("transactionid","uprn")
zz1<-zz1[,..needlist12]
ff1<-uniqueresult(zz1)

ff1$method<-"method43"
############method 44 PAON is equal to pp and SAON is equal to saotext############
tranpro221 <- matchleft(tranpro22,ff1)

tranpro221$add11ressf <-paste(tranpro221$postcode,tranpro221$paon,sep=",")
tranpro221$add11ressf <-paste(tranpro221$add11ressf,tranpro221$saon,sep=",")

add11$add11ressf <- paste(add11$postcodelocator,add11$pp,sep=",")
add11$add11ressf <- paste(add11$add11ressf,add11$saotext,sep=",")

zz2<- inner_join(tranpro221,add11,by="add11ressf")

zz2<-zz2[,..needlist12]

ff2<-uniqueresult(zz2)

ff2$method<-"method44"
############method 45 PAON is equal to pp and FLATSAON is equal to saotext############
tranpro221 <- matchleft(tranpro221,ff2)

tranpro221$flats <- paste("FLAT ",tranpro221$saon,sep="")

tranpro221$addressf <-paste(tranpro221$postcode,tranpro221$paon,sep=",")
tranpro221$add11ressf <-paste(tranpro221$addressf,tranpro221$flats,sep=",")

add11$addressf <- paste(add11$postcodelocator,add11$pp,sep=",")
add11$add11ressf <- paste(add11$addressf,add11$saotext,sep=",")
zz3<- inner_join(tranpro221,add11,by="add11ressf")

zz3<-zz3[,..needlist12]

ff3<-uniqueresult(zz3)

ff3$method<-"method45"
############method 46 PAON is equal to pp and STREET is equal to streetdescription############
tranpro221 <- matchleft(tranpro221,ff3)

tranpro221$add11ressf <-paste(tranpro221$postcode,tranpro221$paon,sep=",")
tranpro221$add11ressf <-paste(tranpro221$add11ressf,tranpro221$street,sep=",")

add11$add11ressf <- paste(add11$postcodelocator,add11$pp,sep=",")
add11$add11ressf <- paste(add11$add11ressf,add11$streetdescription,sep=",")

zz4<- inner_join(tranpro221,add11,by="add11ressf")

zz4<-zz4[,..needlist12]

ff4<-uniqueresult(zz4)

ff4$method<-"method46"
############method 47 PAON is equal to pp and SAON2 is equal to subbuildingname############
tranpro221 <- matchleft(tranpro221,ff4)

tranpro221$add11ressf <-paste(tranpro221$postcode,tranpro221$paon,sep=",")
tranpro221$saontwo<-word(tranpro221$saon,1,2)
tranpro221$add11ressf <-paste(tranpro221$add11ressf,tranpro221$saontwo,sep=",")

add11$add11ressf <- paste(add11$postcodelocator,add11$pp,sep=",")
add11$add11ressf <- paste(add11$add11ressf,add11$subbuildingname,sep=",")

zz5<- inner_join(tranpro221,add11,by="add11ressf")

zz5<-zz5[,..needlist12]

ff5<-uniqueresult(zz5)

ff5$method<-"method47"
############method 48 PAON is equal to pp and SAON2 is equal to saotext############
tranpro221 <- matchleft(tranpro221,ff5)

tranpro221$add11ressf <-paste(tranpro221$postcode,tranpro221$paon,sep=",")
tranpro221$saontwo<-word(tranpro221$saon,1,2)
tranpro221$add11ressf <-paste(tranpro221$add11ressf,tranpro221$saontwo,sep=",")

add11$add11ressf <- paste(add11$postcodelocator,add11$pp,sep=",")
add11$add11ressf <- paste(add11$add11ressf,add11$saotext,sep=",")

zz6<- inner_join(tranpro221,add11,by="add11ressf")
zz6<-zz6[,..needlist12]

ff6<-uniqueresult(zz6)

ff6$method<-"method48"
############method 49 PAON is equal to pp and SAON2 is equal to saotext3############
tranpro221 <- matchleft(tranpro221,ff6)

tranpro221$add11ressf <-paste(tranpro221$postcode,tranpro221$paon,sep=",")
tranpro221$saontwo<-word(tranpro221$saon,1,2)
tranpro221$add11ressf <-paste(tranpro221$add11ressf,tranpro221$saontwo,sep=",")

add11$add11ressf <- paste(add11$postcodelocator,add11$pp,sep=",")
add12<-add11[str_count(add11$saotext,"\\W+")>=2,]
add12$saotext<-gsub("[/]","",add12$saotext)
add12$saotexttwo <-word(add12$saotext,1,2)
add12$add11ressf <- paste(add12$add11ressf,add12$saotexttwo,sep=",")

zz7<- inner_join(tranpro221,add12,by="add11ressf")
zz7<-zz7[,..needlist12]

ff7<-uniqueresult(zz7)

ff7$method<-"method49"
############method 50 PAON is equal to pp and SAON is equal to saotext9############
#GARDEN FLOOR FLAT to GARDEN FLAT
tranpro221 <- matchleft(tranpro221,ff7)

  
tranpro221$add11ressf <-paste(tranpro221$postcode,tranpro221$paon,sep=",")
#tranpro221$saontwo<-word(tranpro221$saon,1,2)
tranpro221$add11ressf <-paste(tranpro221$add11ressf,tranpro221$saon,sep=",")

add11$add11ressf <- paste(add11$postcodelocator,add11$pp,sep=",")
add12<-add11

add12$saotextc <-gsub("GARDEN FLAT","GARDEN FLOOR FLAT",add12$saotext)
add12$add11ressf <- paste(add12$add11ressf,add12$saotextc,sep=",")

zz8<- inner_join(tranpro221,add12,by="add11ressf")
zz8<-zz8[,..needlist12]

ff8<-uniqueresult(zz8)

ff8$method<-"method50"
rm(add12)
############method 51 PAON is equal to pp and SAON is equal to saotext4############
#FLAT FIRST FLOOR to  FIRST FLOOR FLAT
tranpro221 <- matchleft(tranpro221,ff8)

tranpro221$add11ressf <-paste(tranpro221$postcode,tranpro221$paon,sep=",")
#tranpro221$saontwo<-word(tranpro221$saon,1,2)
tranpro221$add11ressf <-paste(tranpro221$add11ressf,tranpro221$saon,sep=",")

add11$add11ressf <- paste(add11$postcodelocator,add11$pp,sep=",")
add12<-add11

add12$saotextc <-gsub("FLAT FIRST FLOOR","FIRST FLOOR FLAT",add12$saotext)
add12$add11ressf <- paste(add12$add11ressf,add12$saotextc,sep=",")
zz9<- inner_join(tranpro221,add12,by="add11ressf")
zz9<-zz9[,..needlist12]

ff9<-uniqueresult(zz9)

ff9$method<-"method51"
rm(add12)
############method 52 For SAON contains "SECOND FLOOR FLAT" and saotext contains "SECOND FLOOR",PAON is equal to pp and SAON is equal to saotext############
#SAON contains "SECOND FLOOR FLAT" and saotext contains "SECOND FLOOR"
tranpro221 <- matchleft(tranpro221,ff9)

tranpro221$add11ressf <-paste(tranpro221$postcode,tranpro221$paon,sep=",")
tranpro221_1<-tranpro221[grepl("SECOND FLOOR FLAT",tranpro221$saon)]

add11$add11ressf <- paste(add11$postcodelocator,add11$pp,sep=",")
add12<-add11[grepl("SECOND FLOOR",add11$saotext)]

zz10<- inner_join(tranpro221_1,add12,by="add11ressf")

zz10<-zz10[,..needlist12]

ff10<-uniqueresult(zz10)

ff10$method<-"method52"
rm(add12)
rm(tranpro221_1)
############method 53 For SAON contains "FIRST AND SECOND FLOOR" and saotext contains "1ST AND 2ND FLOOR", PAON is equal to pp and SAON is equal to saotext############
#FIRST FLOOR FLAT
#FLAT 1ST FLOOR

#FIRST AND SECOND FLOOR
tranpro221 <- matchleft(tranpro221,ff10)

tranpro221$add11ressf <-paste(tranpro221$postcode,tranpro221$paon,sep=",")

tranpro221_1<-tranpro221[grepl("FIRST AND SECOND FLOOR",tranpro221$saon)]

add11$add11ressf <- paste(add11$postcodelocator,add11$pp,sep=",")
add12<-add11[grepl("1ST AND 2ND FLOOR",add11$saotext)]

zz11<- inner_join(tranpro221_1,add12,by="add11ressf")
rm(tranpro221_1)
zz11<-zz11[,..needlist12]

ff11<-uniqueresult(zz11)

ff11$method<-"method53"
############method 54 PAON is equal to pp and SAON is equal to paotext_ss (remove all the blank space for both side)############
#FIRST FLOOR FLAT
#FLAT 1ST FLOOR

tranpro221 <- matchleft(tranpro221,ff11)

tranpro221$add11ressf <-paste(tranpro221$postcode,tranpro221$paon,sep=",")
#tranpro221$saontwo<-word(tranpro221$saon,1,2)
tranpro221_1<-tranpro221[tranpro221$saon!="",]
tranpro221_1$add11ressf <-paste(tranpro221_1$add11ressf,tranpro221_1$saon,sep=",")
tranpro221_1$add11ressf <-gsub(" ","",tranpro221_1$add11ressf )

add11$add11ressf <- paste(add11$postcodelocator,add11$pp,sep=",")
add12<-add11

add12$paotext_ss <-paste(add12$paotext,add12$ss,sep=" ")
add12$add11ressf <- paste(add12$add11ressf,add12$paotext_ss,sep=",")
add12$add11ressf <-gsub(" ","",add12$add11ressf )
zz12<- inner_join(tranpro221_1,add12,by="add11ressf")
rm(tranpro221_1)

zz12<-zz12[,..needlist12]
ff12<-uniqueresult(zz12)

ff12$method<-"method54"
############method 55 PAON is equal to pp and SAON is equal to saotext6############
tranpro221 <- matchleft(tranpro221,ff12)

needz<-needz[!grep("FLAT", needz$saon),]

tranpro221$add11ressf <-paste(tranpro221$postcode,tranpro221$paon,sep=",")
#tranpro221$saontwo<-word(tranpro221$saon,1,2)
tranpro221$add11ressf <-paste(tranpro221$add11ressf,tranpro221$saon,sep=",")

add11$add11ressf <- paste(add11$postcodelocator,add11$pp,sep=",")
add12<-add11

add12$saotext6 <-gsub("FLAT","APARTMENT",add12$saotext)
add12$add11ressf <- paste(add12$add11ressf,add12$saotext6,sep=",")

zz13<- inner_join(tranpro221,add12,by="add11ressf")
zz13<-zz13[,..needlist12]

ff13<-uniqueresult(zz13)
ff13$method<-"method55"
############method 56 PAON is equal to pp and SAON is equal to ss############
tranpro221 <- matchleft(tranpro221,ff13)

tranpro221$add11ressf <-paste(tranpro221$postcode,tranpro221$paon,sep=",")
tranpro221_1<-tranpro221[tranpro221$saon!="",]
tranpro221_1$add11ressf <-paste(tranpro221_1$add11ressf,tranpro221_1$saon,sep=",")

add11$add11ressf <- paste(add11$postcodelocator,add11$pp,sep=",")
add12<-add11
add12$add11ressf <- paste(add12$add11ressf,add12$ss,sep=",")

zz14<- inner_join(tranpro221_1,add12,by="add11ressf")
zz14<-zz14[,..needlist12]

ff14<-uniqueresult(zz14)
ff14$method<-"method56"
rm(tranpro221_1)

############method 57 PAON is equal to pp and SAON3 is equal to saotext8############
tranpro221 <- matchleft(tranpro221,ff14)

tranpro221$add11ressf <-paste(tranpro221$postcode,tranpro221$paon,sep=", ")
#tranpro221$saontwo<-word(tranpro221$saon,1,2)
tranpro221_1<-tranpro221[grepl("\\d$",tranpro221$saon),]
tranpro221_1$saon3<-word(tranpro221_1$saon,-1)
tranpro221_1$add11ressf <-paste(tranpro221_1$add11ressf,tranpro221_1$saon3,sep=", ")

add11$add11ressf <- paste(add11$postcodelocator,add11$pp,sep=", ")
add12<-add11[grepl("\\d$",add12$saotext),]

add12$saotext8 <-word(add12$saotext,-1)
add12$add11ressf <- paste(add12$add11ressf,add12$saotext8,sep=", ")

zz15<- inner_join(tranpro221_1,add12,by="add11ressf")
zz15<-zz15[,..needlist12]

ff15<-uniqueresult(zz15)

ff15$method<-"method57"

tranpro221 <- matchleft(tranpro221,ff15)
############sum up the linked result in stage stage 3############
ly = list(ff1,ff2,ff3,ff4,ff5,ff6,ff7,ff8,ff9,ff10,ff11,ff12,ff13,ff14,ff15,finalz)
stage3<- rbindlist(ly ,use.names=TRUE,fill=T)

dbWriteTable(con, "stage3",value =stage3, append = TRUE, row.names = FALSE)
dbWriteTable(con, "finalz",value =finalz, append = TRUE, row.names = FALSE)
dbWriteTable(con, "tran_stage3_left",value =tranpro221, append = TRUE, row.names = FALSE)

rm(ff1,ff2,ff3,ff4,ff5,ff6,ff7,ff8,ff9,ff10,ff11,ff12,ff13,ff14,ff15,finalz)
rm(zz1,zz2,zz3,zz4,zz5,zz6,zz7,zz8,zz9,zz10,zz11,zz12,zz13,zz14,zz15)
rm(add11)
rm(tranpro22)
rm(z)
rm(add12)
rm(needz)
rm(ly)

###########################stage 4###########################
tran77 <- matchleft(tran88,stage3)

add77 <-add[add$postcodelocator %in% tran77$postcode,]
############method 58 PAON is equal to pp############
tran77$addressf <-paste(tran77$postcode,tran77$paon,sep=",")
add77$addressf <- paste(add77$postcodelocator,add77$pp,sep=",")

b<- inner_join(tran77,add77,by="addressf")

finalb<-uniqueresult(b)
finalb<-finalb[,..needlist12]
finalb$method<-"method58"

needb <- doubleresult(b)

#needb1<-needb[needb$saon!="",]
add77$pp<-str_trim(add77$pp)
############method 59 PAON is equal to pp and SAON is equal to saotext############
tranpro22 <- tranneed(tran77,needb)

tranpro22$add11ressf <-paste(tranpro22$postcode,tranpro22$paon,sep=",")
tranpro22$add11ressf <-paste(tranpro22$add11ressf,tranpro22$saon,sep=",")
tranpro22_1<-tranpro22[tranpro22$saon!="",]

add77$add11ressf <- paste(add77$postcodelocator,add77$pp,sep=",")
add77$add11ressf <- paste(add77$add11ressf,add77$saotext,sep=",")

bb1<- inner_join(tranpro22_1,add77,by="add11ressf")

bb1<-bb1[,..needlist12]
fb1<-uniqueresult(bb1)

fb1$method<-"method59"
############method 60 PAON is equal to pp and SAON is equal to subbuildingname############
tranpro221 <- matchleft(tranpro22,fb1)

tranpro221 <-tranpro221[tranpro221$saon!="",]

tranpro221$add11ressf <-paste(tranpro221$postcode,tranpro221$paon,sep=",")
tranpro221$add11ressf <-paste(tranpro221$add11ressf,tranpro221$saon,sep=",")

add77$add11ressf <- paste(add77$postcodelocator,add77$pp,sep=",")
add77$add11ressf <- paste(add77$add11ressf,add77$subbuildingname ,sep=",")

bb2<- inner_join(tranpro221,add77,by="add11ressf")

bb2<-bb2[,..needlist12]
fb2<-uniqueresult(bb2)

fb2$method<-"method60"
############method 61 PAON is equal to pp and SAON is equal to flatss############
tranpro221 <- matchleft(tranpro221,fb2)

tranpro221$add11ressf <-paste(tranpro221$postcode,tranpro221$paon,sep=",")
tranpro221$add11ressf <-paste(tranpro221$add11ressf,tranpro221$saon,sep=",")

add77$add11ressf <- paste(add77$postcodelocator,add77$pp,sep=",")
add77$flatss <- paste("FLAT",add77$ss,sep=" ")
add77$add11ressf <- paste(add77$add11ressf,add77$flatss ,sep=",")

bb3<- inner_join(tranpro221,add77,by="add11ressf")

bb3<-bb3[,..needlist12]
fb3<-uniqueresult(bb3)

fb3$method<-"method61"
############method 62 PAON is equal to pp and SAON is equal to sspaotext############
tranpro221 <- matchleft(tranpro221,fb3)

tranpro221$add11ressf <-paste(tranpro221$postcode,tranpro221$paon,sep=",")

tranpro221$add11ressf <-paste(tranpro221$add11ressf,tranpro221$saon,sep=",")

add77$add11ressf <- paste(add77$postcodelocator,add77$pp,sep=",")
add77$ss<-str_trim(add77$ss)
add77$sspaotext <- paste(add77$ss,add77$paotext,sep=" ")
add77$add11ressf <- paste(add77$add11ressf,add77$sspaotext ,sep=",")

bb4<- inner_join(tranpro221,add77,by="add11ressf")

bb4<-bb4[,..needlist12]
fb4<-uniqueresult(bb4)

fb4$method<-"method62"
############method 63 PAON is equal to pp and FLATSAON is equal to saotext############
tranpro221 <- matchleft(tranpro221,fb4)

tranpro221$add11ressf <-paste(tranpro221$postcode,tranpro221$paon,sep=",")
tranpro221$flatsaon <-paste("FLAT",tranpro221$saon,sep=" ")
tranpro221$add11ressf <-paste(tranpro221$add11ressf,tranpro221$flatsaon,sep=",")

add77$add11ressf <- paste(add77$postcodelocator,add77$pp,sep=",")
add77$add11ressf <- paste(add77$add11ressf,add77$saotext ,sep=",")

bb5<- inner_join(tranpro221,add77,by="add11ressf")

bb5<-bb5[,..needlist12]
fb5<-uniqueresult(bb5)

fb5$method<-"method63"
############method 64 PAON is equal to pp and FLATSAON  is equal to subbuildingname############
tranpro221 <- matchleft(tranpro221,fb5)

tranpro221$add11ressf <-paste(tranpro221$postcode,tranpro221$paon,sep=",")
tranpro221$flatsaon <-paste("FLAT",tranpro221$saon,sep=" ")
tranpro221$add11ressf <-paste(tranpro221$add11ressf,tranpro221$flatsaon,sep=",")

add77$add11ressf <- paste(add77$postcodelocator,add77$pp,sep=",")
add77$add11ressf <- paste(add77$add11ressf,add77$subbuildingname ,sep=",")
bb6<- inner_join(tranpro221,add77,by="add11ressf")

bb6<-bb6[,..needlist12]
fb6<-uniqueresult(bb6)

fb6$method<-"method64"
############method 65 PAON is equal to pp and SAON is equal to ss############
tranpro221 <- matchleft(tranpro221,fb6)

tranpro221$add11ressf <-paste(tranpro221$postcode,tranpro221$paon,sep=",")
tranpro221$add11ressf <-paste(tranpro221$add11ressf,tranpro221$saon,sep=",")

add77$add11ressf <- paste(add77$postcodelocator,add77$pp,sep=",")
add77$ss<-str_trim(add77$ss)

add77$add11ressf <- paste(add77$add11ressf,add77$ss ,sep=",")
bb7<- inner_join(tranpro221,add77,by="add11ressf")

bb7<-bb7[,..needlist12]
fb7<-uniqueresult(bb7)

fb7$method<-"method65"
############method 66 PAON is equal to pp and SAON is equal to saotext5############
tranpro221 <- matchleft(tranpro221,fb7)

tranpro221$add11ressf <-paste(tranpro221$postcode,tranpro221$paon,sep=",")
tranpro221$add11ressf <-paste(tranpro221$add11ressf,tranpro221$saon,sep=",")

add77$add11ressf <- paste(add77$postcodelocator,add77$pp,sep=",")
add78<-add77
add78$saotext5 <-gsub("FLAT","UNIT",add78$saotext)
add78$add11ressf <- paste(add78$add11ressf,add78$saotext5,sep=",")

bb8<- inner_join(tranpro221,add78,by="add11ressf")

bb8<-bb8[,..needlist12]
fb8<-uniqueresult(bb8)

fb8$method<-"method66"
############method 67 PAON is equal to pp and SAON is equal to saotext6############
tranpro221 <- matchleft(tranpro221,fb8)

tranpro221$add11ressf <-paste(tranpro221$postcode,tranpro221$paon,sep=",")
tranpro221$add11ressf <-paste(tranpro221$add11ressf,tranpro221$saon,sep=",")

add77$add11ressf <- paste(add77$postcodelocator,add77$pp,sep=",")

add78<-add77
add78$saotext6 <-gsub("FLAT","APARTMENT",add78$saotext)
add78$add11ressf <- paste(add78$add11ressf,add78$saotext6,sep=",")

bb9<- inner_join(tranpro221,add78,by="add11ressf")

bb9<-bb9[,..needlist12]
fb9<-uniqueresult(bb9)

fb9$method<-"method67"
############method 68 PAON is equal to pp and SAON is equal to paoendnumber############
tranpro221 <- matchleft(tranpro221,fb9)

tranpro221$add11ressf <-paste(tranpro221$postcode,tranpro221$paon,sep=",")
tranpro221$add11ressf <-paste(tranpro221$add11ressf,tranpro221$saon,sep=",")


add77$add11ressf <- paste(add77$postcodelocator,add77$pp,sep=",")
add77$add11ressf <- paste(add77$add11ressf,add77$paoendnumber,sep=",")

bb10<- inner_join(tranpro221,add77,by="add11ressf")

bb10<-bb10[,..needlist12]
fb10<-uniqueresult(bb10)

fb10$method<-"method68"
############method 69 PAON is equal to pp and SAON3 is equal to subbuildingname############
tranpro221 <- matchleft(tranpro221,fb10)

tranpro221$add11ressf <-paste(tranpro221$postcode,tranpro221$paon,sep=",")
tranpro221_1<-tranpro221[!grepl("FLAT$",tranpro221$saon),]
tranpro221_1$saon3 <-word(tranpro221_1$saon,-1)
tranpro221_1$add11ressf <-paste(tranpro221_1$add11ressf,tranpro221_1$saon3,sep=",")

add77$add11ressf <- paste(add77$postcodelocator,add77$pp,sep=",")
add77$add11ressf <- paste(add77$add11ressf,add77$subbuildingname ,sep=",")

bb11<- inner_join(tranpro221_1,add77,by="add11ressf")

bb11<-bb11[,..needlist12]
fb11<-uniqueresult(bb11)

fb11$method<-"method69"
############method 70 PAON is equal to pp and SAON is equal to subbuildingnamepaotext############
tranpro221 <- matchleft(tranpro221,fb11)

tranpro221$add11ressf <-paste(tranpro221$postcode,tranpro221$paon,sep=",")
tranpro221$add11ressf <-paste(tranpro221$add11ressf,tranpro221$saon,sep=",")


add77$add11ressf <- paste(add77$postcodelocator,add77$pp,sep=",")
add77$subp <- paste(add77$subbuildingname,add77$paotext ,sep=" ")
add77$add11ressf <- paste(add77$add11ressf,add77$subp ,sep=",")

bb12<- inner_join(tranpro221,add77,by="add11ressf")

bb12<-bb12[,..needlist12]
fb12<-uniqueresult(bb12)

fb12$method<-"method70"
############method 71 PAON is equal to pp and SAON3 is equal to subbuildingname3############
tranpro221 <- matchleft(tranpro221,fb12)


tranpro221$add11ressf <-paste(tranpro221$postcode,tranpro221$paon,sep=",")
tranpro221$saon3 <- word(tranpro221$saon,-1)
tranpro221$add11ressf <-paste(tranpro221$add11ressf,tranpro221$saon3,sep=",")


add77$add11ressf <- paste(add77$postcodelocator,add77$pp,sep=",")
add77$subl <- word(add77$subbuildingname,-1)
add78<-add77[!grepl("FLAT$",add77$subbuildingname),]
add78$add11ressf <- paste(add78$add11ressf,add78$subl ,sep=",")

bb13<- inner_join(tranpro221,add78,by="add11ressf")

bb13<-bb13[,..needlist12]
fb13<-uniqueresult(bb13)

fb13$method<-"method71"
############sum up the linked result in stage 4############
tranpro221 <- matchleft(tranpro221,fb13)

ly = list(fb1,fb2,fb3,fb4,fb5,fb6,fb7,fb8,fb9,fb10,fb11,fb12,fb13,finalb)
stage4<- rbindlist(ly ,use.names=TRUE,fill=T)

#save data in PostGIS database
dbWriteTable(con, "stage4",value =stage4, append = TRUE, row.names = FALSE)
dbWriteTable(con, "finalb",value =finalb, append = TRUE, row.names = FALSE)
dbWriteTable(con, "tran_stage4_left",value =tranpro221, append = TRUE, row.names = FALSE)

rm(b)
rm(finalb)
rm(fb1,fb2,fb3,fb4,fb5,fb6,fb7,fb8,fb9,fb10,fb11,fb12,fb13)
rm(bb1,bb2,bb3,bb4,bb5,bb6,bb7,bb8,bb9,bb10,bb11,bb12,bb13)
rm(ly)
rm(needb)
rm(add77,add78)
rm(tranpro22,tranpro221,tranpro22_1,tranpro221_1)
rm(tran88)
###########################stage 5 ###########################
tran66 <- matchleft(tran77,stage4)

add66 <-add[add$postcodelocator %in% tran66$postcode,]
############method 90 PAON is equal to psao############
tran66$addressf <-paste(tran66$postcode,tran66$paon,sep=",")
add66 <- merge(add,link1,by="saotext")

add66$bb1<- paste(add66$paostartnumber,add66$string,sep="")
add66$addressf <- paste(add66$postcodelocator,add66$bb1,sep=",")

e<- inner_join(tran66,add66,by="addressf")

#keep the one to one linkage result
stage5 <- uniqueresult(e)
stage5$method<-"method90"

needlistf<-c("transactionid","uprn","method")
stage5<-stage5[,..needlistf]

neede <- doubleresult(e)
tranpro22 <- tranneed(tran66,neede)

dbWriteTable(con, "stage5",value =stage5, append = TRUE, row.names = FALSE)
dbWriteTable(con, "tran_stage5_left",value =tranpro22, append = TRUE, row.names = FALSE)
rm(neede,e,add66,tranpro22)
###########################stage 6###########################
tran55 <- matchleft(tran66,stage5)

add<-add[add$postset %in% tran55$postset,]

############method 73 PAON is equal to pp2 ############
tran55$addressf <-paste(tran55$postcode,tran55$paon,sep=",")

add$pp2 <-paste(add$paotext,add$pp,sep=", ")
add$addressf <- paste(add$postcodelocator,add$pp2,sep=",")

r1<- inner_join(tran55,add,by="addressf")

needlist1<-c("transactionid","uprn")
r1<-r1[,..needlist1]

finalr<-uniqueresult(r1)

needr <- doubleresult(r1)

finalr$method<-"method73"
rm(r1)
############method 74 PAON is equal to pp2 and SAON is equal to saotext############
tranr <- tranneed(tran55,needr)

tranr$addressf <-paste(tranr$postcode,tranr$paon,sep=",")
tranr$addressf <-paste(tranr$addressf,tranr$saon,sep=",")
tranr_1<-tranr[tranr$saon!="",]
add$pp2 <-paste(add$paotext,add$pp,sep=", ") 

add$addressf <- paste(add$postcodelocator,add$pp2,sep=",")
add$addressf <- paste(add$addressf,add$saotext,sep=",")

rr1<- inner_join(tranr_1,add,by="addressf")

rr1<-rr1[,..needlist1]

fr1<-uniqueresult(rr1)

fr1$method<-"method74"
############method 75 PAON is equal to pp2 and SAON is equal to ss############
tranr<- matchleft(tranr,fr1)
  
tranr$addressf <-paste(tranr$postcode,tranr$paon,sep=",")
tranr$addressf <-paste(tranr$addressf,tranr$saon,sep=",")
tranr_1<-tranr[tranr$saon!="",]

add$pp2 <-paste(add$paotext,add$pp,sep=", ") 
add$addressf <- paste(add$postcodelocator,add$pp2,sep=",")
add$addressf <- paste(add$addressf,add$ss,sep=",")

rr2<- inner_join(tranr_1,add,by="addressf")

rr2<-rr2[,..needlist1]
fr2<-uniqueresult(rr2)

fr2$method<-"method75"
############method 76 PAON is equal to pp2 and SAON is equal to flatss############
tranr<- matchleft(tranr,fr2)

tranr$addressf <-paste(tranr$postcode,tranr$paon,sep=",")
tranr$addressf <-paste(tranr$addressf,tranr$saon,sep=",")
tranr_1<-tranr[tranr$saon!="",]

add$pp2 <-paste(add$paotext,add$pp,sep=", ") 
add$flatss <- paste("FLAT ",add$ss,sep="")
add$addressf <- paste(add$postcodelocator,add$pp2,sep=",")
add$addressf <- paste(add$addressf,add$flatss,sep=",")

rr3<- inner_join(tranr_1,add,by="addressf")

rr3<-rr3[,..needlist1]
fr3<-uniqueresult(rr3)

fr3$method<-"method76"
############method 77 PAON is equal to pp2 and FLATSAON is equal to saotext############
tranr<- matchleft(tranr,fr3)

tranr$fsaon<-paste("FLAT ",tranr$saon,sep="")
tranr$addressf <-paste(tranr$postcode,tranr$paon,sep=",")
tranr$addressf <-paste(tranr$addressf,tranr$fsaon,sep=",")
tranr_1<-tranr[tranr$saon!="",]

add$pp2 <-paste(add$paotext,add$pp,sep=", ") 
add$addressf <- paste(add$postcodelocator,add$pp2,sep=",")
add$addressf <- paste(add$addressf,add$saotext,sep=",")

rr4<- inner_join(tranr_1,add,by="addressf")

rr4<-rr4[,..needlist1]
fr4<-uniqueresult(rr4)

fr4$method<-"method77"
############method 78 PAON is equal to pp2 and SAON is equal to unitss############
tranr<- matchleft(tranr,fr4)

tranr$addressf <-paste(tranr$postcode,tranr$paon,sep=",")
tranr$addressf <-paste(tranr$addressf,tranr$saon,sep=",")
tranr_1<-tranr[tranr$saon!="",]

add$pp2 <-paste(add$paotext,add$pp,sep=", ") 
add$unitss <-paste("UNIT ",add$ss,sep="")
add$addressf <- paste(add$postcodelocator,add$pp2,sep=",")
add$addressf <- paste(add$addressf,add$unitss,sep=",")

rr5<- inner_join(tranr_1,add,by="addressf")

rr5<-rr5[,..needlist1]
fr5<-uniqueresult(rr5)

fr5$method<-"method78"
############method 79 PAON is equal to pp2 and SAON is equal to subbuildingname############
tranr<- matchleft(tranr,fr5)

tranr$addressf <-paste(tranr$postcode,tranr$paon,sep=",")
tranr$addressf <-paste(tranr$addressf,tranr$saon,sep=",")
tranr_1<-tranr[tranr$saon!="",]

add$pp2 <-paste(add$paotext,add$pp,sep=", ")  
add$addressf <- paste(add$postcodelocator,add$pp2,sep=",")
add$addressf <- paste(add$addressf,add$subbuildingname,sep=",")

rr6<- inner_join(tranr_1,add,by="addressf")


rr6<-rr6[,..needlist1]
fr6<-uniqueresult(rr6)

fr6$method<-"method79"
############method 80 PAON1 is equal to buildingname and SAON is equal to subbuildingname############
tranr<- matchleft(tranr,fr6)

tranr$paon1 <- gsub(",","",tranr$paon)
tranr$addressf <-paste(tranr$postcode,tranr$paon1,sep=",")
tranr$addressf <-paste(tranr$addressf,tranr$saon,sep=",")
tranr_1<-tranr[tranr$saon!="",]

add$addressf <- paste(add$postcodelocator,add$buildingname,sep=",")
add$addressf <- paste(add$addressf,add$subbuildingname,sep=",")

rr7<- inner_join(tranr_1,add,by="addressf")

rr7<-rr7[,..needlist1]
fr7<-uniqueresult(rr7)

fr7$method<-"method80"
############method 81 Flat transactions: PAON is equal to pp2 and SAON is equal to ss1############
tranr<- matchleft(tranr,fr7)

data4 <- tranr[tranr$propertytype=="F",]

data4$addressf <-paste(data4$postcode,data4$paon,sep=",")
data4$addressf <-paste(data4$addressf,data4$saon,sep=",")
data4<-data4[data4$saon!="",]

add$pp2 <-paste(add$paotext,add$pp,sep=", ") 
add$ss1 <- paste(add$saostartsuffix,add$saostartnumber,sep="")

add$addressf <- paste(add$postcodelocator,add$pp2,sep=",")
add$addressf <- paste(add$addressf,add$ss1,sep=",")

rr8<- inner_join(data4,add,by="addressf")

rr8<-rr8[,..needlist1]
fr8<-uniqueresult(rr8)

fr8$method<-"method81"
rm(data4)
add<-add[,..addlist1]
############method 82 PAON is equal to pp2 and SAON4 is equal to saotext############
tranr<- matchleft(tranr,fr8)

tranr$addressf <-paste(tranr$postcode,tranr$paon,sep=",")
tranr_1<-tranr
tranr_1$saon4<-gsub("[.]","",tranr_1$saon)
tranr_1$addressf <-paste(tranr_1$addressf,tranr_1$saon4,sep=",")
tranr_1<-tranr_1[tranr_1$saon!="",]

add$pp2 <-paste(add$paotext,add$pp,sep=", ") 
add$addressf <- paste(add$postcodelocator,add$pp2,sep=",")
add$addressf <- paste(add$addressf,add$saotext,sep=",")


rr9<- inner_join(tranr_1,add,by="addressf")

rr9<-rr9[,..needlist1]
fr9<-uniqueresult(rr9)

fr9$method<-"method82"

rm(tranr_1)
############method 83 PAON is equal to pp2 and SAON5 is equal to saotext############
tranr<- matchleft(tranr,fr9)

tranr$addressf <-paste(tranr$postcode,tranr$paon,sep=",")
tranr_1<-tranr
tranr_1$saon5<-word(tranr_1$saon,-2,-1)
tranr_1$addressf <-paste(tranr_1$addressf,tranr_1$saon5,sep=",")
tranr_1<-tranr_1[tranr_1$saon!="",]

add$pp2 <-paste(add$paotext,add$pp,sep=", ") 
add$addressf <- paste(add$postcodelocator,add$pp2,sep=",")
add$addressf <- paste(add$addressf,add$saotext,sep=",")

rr10<- inner_join(tranr_1,add,by="addressf")

rr10<-rr10[,..needlist1]
fr10<-uniqueresult(rr10)

fr10$method<-"method83"
############method 84 PAON is equal to pp2 and SAON is equal to saotext6############
tranr<- matchleft(tranr,fr10)

tranr$addressf <-paste(tranr$postcode,tranr$paon,sep=",")
tranr$addressf <-paste(tranr$addressf,tranr$saon,sep=",")
tranr_1<-tranr[tranr$saon!="",]
add$pp2 <-paste(add$paotext,add$pp,sep=", ") 

add$addressf <- paste(add$postcodelocator,add$pp2,sep=",")
add$saotext6<- gsub("FLAT","APARTMENT",add$saotext)
add$addressf <- paste(add$addressf,add$saotext6,sep=",")

rr11<- inner_join(tranr_1,add,by="addressf")
rr11<-rr11[,..needlist1]
fr11<-uniqueresult(rr11)

fr11$method<-"method84"
############method 85 PAON is equal to pp2 and SAON is equal to saotext5############
tranr<- matchleft(tranr,fr11)

needr<-matchleft(needr,fr10)
needr<-matchleft(needr,fr11)

tranr$addressf <-paste(tranr$postcode,tranr$paon,sep=",")
tranr$addressf <-paste(tranr$addressf,tranr$saon,sep=",")
tranr_1<-tranr[tranr$saon!="",]

add$pp2 <-paste(add$paotext,add$pp,sep=", ") 
add$addressf <- paste(add$postcodelocator,add$pp2,sep=",")
add$saotext5<- gsub("FLAT","UNIT",add$saotext)
add$addressf <- paste(add$addressf,add$saotext5,sep=",")

rr12<- inner_join(tranr_1,add,by="addressf")

rr12<-rr12[,..needlist1]
fr12<-uniqueresult(rr12)

fr12$method<-"method85"

tranr<- matchleft(tranr,fr12)
add<-add[,..addlist1]
############sum up the linked data in stage 6############
ly = list(fr1,fr2,fr3,fr4,fr5,fr6,fr7,fr8,fr9,fr10,fr11,fr12,finalr)
stage6<- rbindlist(ly ,use.names=TRUE,fill=T)

dbWriteTable(con, "stage6",value =stage6, append = TRUE, row.names = FALSE)
dbWriteTable(con, "finalr",value =finalr, append = TRUE, row.names = FALSE)
dbWriteTable(con, "tran_stage6_left",value =tranr, append = TRUE, row.names = FALSE)

rm(rr1,rr2,rr3,rr4,rr5,rr6,rr7,rr8,rr9,rr10,rr11,rr12)
rm(fr1,fr2,fr3,fr4,fr5,fr6,fr7,fr8,fr9,fr10,fr11,fr12)
rm(ly)
rm(tranr,tranr_1,finalr)
rm(needr)
rm(tran66,tran77)
###########################stage 7###########################
tran44 <- matchleft(tran55,stage6)

add<-add[add$postset %in% tran44$postset,]
############method 86 PAON is equal to paotext or PAON is equal to sp############
#part 1 linkage: PAON is equal to paotext
tran44$addressf <-paste(tran44$postcode,tran44$paon,sep=",")
add$addressf <- paste(add$postcodelocator,add$paotext,sep=",")

m1<- inner_join(tran44,add,by="addressf")
m1<-m1[,..needlist1]

finalm1<-uniqueresult(m1)
tran44_1<-matchleft(tran44,finalm1)

rm(finalm1)
#part 2 linkage: PAON is equal to sp
tran44_1$addressf <-paste(tran44_1$postcode,tran44_1$paon,sep=",")

add$sspao <- paste(add$ss,add$paotext,sep=" ")
add$addressf <- paste(add$postcodelocator,add$sspao,sep=",")

m2<- inner_join(tran44_1,add,by="addressf")
m2<-m2[,..needlist1]

#combine the part 1 and part 2 linked datasets
m<-rbindlist(list(m1,m2))

rm(tran44_1)
#extract one to one linkage result
finalm <- uniqueresult(m)
#extract one to many linkage result
needm <- doubleresult(m)
finalm<-finalm[,..needlist1]
finalm$method<-"method86"

rm(m1,m2)
############method 87 PAON is equal to paotext and SAON is equal to ss############
tranm <- tranneed(tran44,needm)

tranm$addressf <-paste(tranm$postcode,tranm$paon,sep=",")
tranm$addressf <-paste(tranm$addressf,tranm$saon,sep=",")
tranm_1<-tranm[tranm$saon!="",]
add$addressf <- paste(add$postcodelocator,add$paotext,sep=",")
add$addressf <- paste(add$addressf,add$ss,sep=",")

mm1<- inner_join(tranm_1,add,by="addressf")

mm1<-mm1[,..needlist1]
fm1<-uniqueresult(mm1)

fm1$method<-"method87"
############method 88 PAON is equal to paotext and FLATSAON is equal to saotext############
tranm <- matchleft(tranm,fm1)

tranm$flats <- paste("FLAT ",tranm$saon,sep="")

tranm$addressf <-paste(tranm$postcode,tranm$paon,sep=",")
tranm$addressf <-paste(tranm$addressf,tranm$flats,sep=",")
tranm_1<-tranm[tranm$saon!="",]
add$addressf <- paste(add$postcodelocator,add$paotext,sep=",")
add$addressf <- paste(add$addressf,add$saotext,sep=",")


mm2<- inner_join(tranm_1,add,by="addressf")

mm2<-mm2[,..needlist1]
fm2<-uniqueresult(mm2)

fm2$method<-"method88"
############method 89 PAON is equal to paotext and SAON is equal to flatss############
tranm <- matchleft(tranm,fm2)

tranm$addressf <-paste(tranm$postcode,tranm$paon,sep=",")
tranm$addressf <-paste(tranm$addressf,tranm$saon,sep=",")
tranm_1<-tranm[tranm$saon!="",]

add$flatss <- paste("FLAT ",add$ss,sep="")
add$addressf <- paste(add$postcodelocator,add$paotext,sep=",")
add$addressf <- paste(add$addressf,add$flatss,sep=",")

mm3<- inner_join(tranm_1,add,by="addressf")

mm3<-mm3[,..needlist1]
fm3<-uniqueresult(mm3)

fm3$method<-"method89"
############method 90 PAON is equal to paotext and SAON is equal to saotext############
tranm <- matchleft(tranm,fm3)

tranm$addressf <-paste(tranm$postcode,tranm$paon,sep=",")
tranm$addressf <-paste(tranm$addressf,tranm$saon,sep=",")
tranm_1<-tranm[tranm$saon!="",]

add$addressf <- paste(add$postcodelocator,add$paotext,sep=",")
add$addressf <- paste(add$addressf,add$saotext,sep=",")

mm4<- inner_join(tranm_1,add,by="addressf")
mm4<-mm4[,..needlist1]
fm4<-uniqueresult(mm4)

fm4$method<-"method90"
############method 91 PAON is equal to paotext and SAON is equal to pp############
tranm <- matchleft(tranm,fm4)

tranm$addressf <-paste(tranm$postcode,tranm$paon,sep=",")
tranm$addressf <-paste(tranm$addressf,tranm$saon,sep=",")
tranm_1<-tranm[tranm$saon!="",]

add$addressf <- paste(add$postcodelocator,add$paotext,sep=",")
add$addressf <- paste(add$addressf,add$pp, sep=",")

mm5<- inner_join(tranm_1,add,by="addressf")

mm5<-mm5[,..needlist1]
fm5<-uniqueresult(mm5)

fm5$method<-"method91"
############method 92 PAON is equal to paotext and SAON is equal to subss############
tranm <- matchleft(tranm,fm5)

tranm$addressf <-paste(tranm$postcode,tranm$paon,sep=",")
tranm$addressf <-paste(tranm$addressf,tranm$saon,sep=",")
tranm_1<-tranm[tranm$saon!="",]

add$subss <- paste(add$subbuildingname,add$ss,sep=" ")
add$addressf <- paste(add$postcodelocator,add$paotext,sep=",")
add$addressf <- paste(add$addressf,add$subss , sep=",")

mm6<- inner_join(tranm_1,add,by="addressf")

mm6<-mm6[,..needlist1]
fm6<-uniqueresult(mm6)

fm6$method<-"method92"
############method 93 PAON is equal to paotext and SAONPAON is equal to saobui############
tranm <- matchleft(tranm,fm6)

tranm$sapa <- paste(tranm$saon,tranm$paon,sep=" ")
tranm$addressf <-paste(tranm$postcode,tranm$paon,sep=",")
tranm$addressf <-paste(tranm$addressf,tranm$sapa,sep=",")
tranm_1<-tranm[tranm$saon!="",]

add$saobui <- paste(add$saotext,add$buildingname,sep=" ")
add$addressf <- paste(add$postcodelocator,add$paotext,sep=",")
add$addressf <- paste(add$addressf,add$saobui , sep=",")

mm7<- inner_join(tranm_1,add,by="addressf")

mm7<-mm7[,..needlist1]
fm7<-uniqueresult(mm7)

fm7$method<-"method93"
############method 94 PAON is equal to paotext and SAON3 is equal to ss############
tranm <- matchleft(tranm,fm7)

tranm$addressf <-paste(tranm$postcode,tranm$paon,sep=",")
tranm_1<-tranm[tranm$saon!="",]
tranm_1<-tranm_1[grepl("\\d$",tranm_1$saon),]
tranm_1$saon3<-word(tranm_1$saon,-1)
tranm_1$addressf <-paste(tranm_1$addressf,tranm_1$saon3,sep=",")

add$addressf <- paste(add$postcodelocator,add$paotext,sep=",")
add$addressf <- paste(add$addressf,add$ss, sep=",")

mm8<- inner_join(tranm_1,add,by="addressf")

mm8<-mm8[,..needlist1]
fm8<-uniqueresult(mm8)

fm8$method<-"method94"
############method 95 PAON is equal to paotext and UNITSAON is equal to saotext############
tranm <- matchleft(tranm,fm8)

tranm$unitsaon <- paste("UNIT ",tranm$saon,sep="")
tranm$addressf <-paste(tranm$postcode,tranm$paon,sep=",")
tranm$addressf <-paste(tranm$addressf,tranm$unitsaon,sep=",")
tranm_1<-tranm[tranm$saon!="",]

add$addressf <- paste(add$postcodelocator,add$paotext,sep=",")
add$addressf <- paste(add$addressf,add$saotext,sep=",")

mm9<- inner_join(tranm_1,add,by="addressf")

mm9<-mm9[,..needlist1]
fm9<-uniqueresult(mm9)

fm9$method<-"method95"
############method 96 PAON is equal to paotext and SAON2 is equal to saotext3############
tranm <- matchleft(tranm,fm9)

tranm$addressf <-paste(tranm$postcode,tranm$paon,sep=",")
tranm_1<-tranm[tranm$saon!="",]

tranm_1<-tranm_1[str_count(tranm_1$saon,"\\W+")>=2,]
tranm_1$saon2<-word(tranm_1$saon,1,2)
tranm_1$addressf <-paste(tranm_1$addressf,tranm_1$saon2,sep=",")


add$addressf <- paste(add$postcodelocator,add$paotext,sep=",")
add_1<-add[str_count(add$saotext,"\\W+")>=2,]
add_1$saotext3 <- word(add_1$saotext,1,2)
add_1$addressf <- paste(add_1$addressf,add_1$saotext3, sep=",")

mm10<- inner_join(tranm_1,add_1,by="addressf")

rm(add_1)
rm(tranm_1)

mm10<-mm10[,..needlist1]
fm10<-uniqueresult(mm10)

fm10$method<-"method96"
############method 97 PAON is equal to paotext and SAON4 is equal to ss############
tranm <- matchleft(tranm,fm10)

tranm$addressf <-paste(tranm$postcode,tranm$paon,sep=",")
tranm_1<-tranm[tranm$saon!="",]
tranm_1$saon4<-gsub("[.]","",tranm_1$saon)
tranm_1$addressf <-paste(tranm_1$addressf,tranm_1$saon4,sep=",")

add$addressf <- paste(add$postcodelocator,add$paotext,sep=",")
add$addressf <- paste(add$addressf,add$ss,sep=",")

mm11<- inner_join(tranm_1,add,by="addressf")

mm11<-mm11[,..needlist1]
fm11<-uniqueresult(mm11)

fm11$method<-"method97"
############method 98 PAON is equal to paotext and SAON is equal to saotext7############
tranm <- matchleft(tranm,fm11)

tranm$addressf <-paste(tranm$postcode,tranm$paon,sep=",")
tranm$addressf <-paste(tranm$addressf,tranm$saon,sep=",")
tranm_1<-tranm[tranm$saon!="",]

add$addressf <- paste(add$postcodelocator,add$paotext,sep=",")
add$saotext7<-gsub("APARTMENT","FLAT",add$saotext)
add$addressf <- paste(add$addressf,add$saotext7,sep=",")

mm12<- inner_join(tranm_1,add,by="addressf")

mm12<-mm12[,..needlist1]
fm12<-uniqueresult(mm12)

fm12$method<-"method98"
############method 99 PAON is equal to paotext and apsaon is equal to saotext############
tranm <- matchleft(tranm,fm12)

tranm$apsaon <- paste("APARTMENT ",tranm$saon,sep="")
tranm$addressf <-paste(tranm$postcode,tranm$paon,sep=",")
tranm$addressf <-paste(tranm$addressf,tranm$apsaon,sep=",")
tranm_1<-tranm[tranm$saon!="",]

add$addressf <- paste(add$postcodelocator,add$paotext,sep=",")
add$addressf <- paste(add$addressf,add$saotext,sep=",")

mm13<- inner_join(tranm_1,add,by="addressf")

mm13<-mm13[,..needlist1]
fm13<-uniqueresult(mm13)

fm13$method<-"method99"
############method 100 PAON is equal to paotext and SAON is equal to ss1############
tranm <- matchleft(tranm,fm13)

tranm$addressf <-paste(tranm$postcode,tranm$paon,sep=", ")
tranm$addressf <-paste(tranm$addressf,tranm$saon,sep=", ")
tranm_1<-tranm[tranm$saon!="",]

add$addressf <- paste(add$postcodelocator,add$paotext,sep=", ")
add$ss1<-paste(add$saostartnumber,add$saoendnumber,sep="-")
add$addressf <- paste(add$addressf,add$ss1,sep=", ")

mm14<- inner_join(tranm_1,add,by="addressf")

mm14<-mm14[,..needlist1]
fm14<-uniqueresult(mm14)

fm14$method<-"method100"

add<-add[,..addlist1]
rm(tranm_1)
tranm <- matchleft(tranm,fm14)
############sum up the linked data in stage 7############
ly = list(fm1,fm2,fm3,fm4,fm5,fm6,fm7,fm8,fm9,fm10,fm11,fm12,fm13,fm14,finalm)
stage7<- rbindlist(ly ,use.names=TRUE,fill=T)

dbWriteTable(con, "stage7",value =stage7, append = TRUE, row.names = FALSE)
dbWriteTable(con, "finalm",value =finalm, append = TRUE, row.names = FALSE)
dbWriteTable(con, "tran_stage7_left",value =tranm, append = TRUE, row.names = FALSE)
rm(mm1,mm2,mm3,mm4,mm5,mm6,mm7,mm8,mm9,mm10,mm11,mm12,mm13,mm14)
rm(fm1,fm2,fm3,fm4,fm5,fm6,fm7,fm8,fm9,fm10,fm11,fm12,fm13,fm14)
rm(ly)
rm(finalm)
rm(needm,tranm)
rm(m)
rm(tran55)
#################stage 8 #################
#remove the linked record from stage 7 in PPD
tran33 <- matchleft(tran44,stage7)

############method 101 PAON1 is equal to buildingname or then PAON1 is equal to pp4############
#part 1: PAON1 is equal to buildingname
tran33$paon1 <- gsub(" - ","-",tran33$paon )
tran33$paon1 <- gsub(", "," ",tran33$paon1 )

tran33$addressf <-paste(tran33$postcode,tran33$paon1,sep=",")
add$addressf <- paste(add$postcodelocator,add$buildingname,sep=",")

ll1<- inner_join(tran33,add,by="addressf")

ll1<-ll1[,..needlist1]

f1<-uniqueresult(ll1)

#part 1: PAON1 is equal to pp4
tran33_1 <- matchleft(tran33,f1)

tran33_1$paon1 <- gsub(" - ","-",tran33_1$paon )
tran33_1$paon1 <- gsub(", "," ",tran33_1$paon1 )

tran33_1$addressf <-paste(tran33_1$postcode,tran33_1$paon1,sep=",")

add$pp4 <-paste(add$paostartnumber,add$paoendnumber,sep="-") 

add$addressf <- paste(add$postcodelocator,add$pp4,sep=",")
ll2<- inner_join(tran33_1,add,by="addressf")

ll2<-ll2[,..needlist1]

f2<-uniqueresult(ll2)

#remove data
rm(tran33_1)
#combine the part 1 and part 2 result
ll<-rbindlist(list(ll1,ll2) )
ll<-unique(ll)

finalll <- uniqueresult(ll)
finalll$method<-"method101"

needl <- doubleresult(ll)

rm(tran44)
rm(ll1,ll2)
############method 102 PAON1 is equal to buildingname and SAON is equal to subbuildingname############
tranl<- tranneed(tran33,needl)

tranl$paon1 <- gsub(" - ","-",tranl$paon )
tranl$paon1 <- gsub(", "," ",tranl$paon1 )

tranl$addressf <-paste(tranl$postcode,tranl$paon1,sep=",")
tranl$addressf <-paste(tranl$addressf,tranl$saon,sep=",")
tranl_1<-tranl[tranl$saon!="",]

add$addressf <- paste(add$postcodelocator,add$buildingname,sep=",")
add$addressf <- paste(add$addressf,add$subbuildingname,sep=",")

l1<- inner_join(tranl_1,add,by="addressf")
l1<-l1[,..needlist1]

fl1<-uniqueresult(l1)

fl1$method<-"method102"

############method 103 PAON1 is equal to buildingname and SAON is equal to saotext############
tranl <- matchleft(tranl,fl1)

tranl$addressf <-paste(tranl$postcode,tranl$paon1,sep=",")
tranl$addressf <-paste(tranl$addressf,tranl$saon,sep=",")
tranl_1<-tranl[tranl$saon!="",]

add$addressf <- paste(add$postcodelocator,add$buildingname,sep=",")
add$addressf <- paste(add$addressf,add$saotext,sep=",")


l2<- inner_join(tranl_1,add,by="addressf")
l2<-l2[,..needlist1]


fl2<-uniqueresult(l2)

fl2$method<-"method103"
rm(tranl_1)
############method 104 PAON2 is equal to pp4 and SAON is NULL############
tranl <- matchleft(tranl,fl2)

tranl$paon2 <- gsub(" - ","-",tranl$paon )
tranl$addressf <-paste(tranl$postcode,tranl$paon2,sep=",")
temp1<- tranl[tranl$saon=="",]

add$pp4 <-paste(add$paostartnumber,add$paoendnumber,sep="-")
add$addressf <- paste(add$postcodelocator,add$pp4,sep=",")

l3<- inner_join(temp1,add,by="addressf")
l3<-l3[,..needlist1]

fl3<-uniqueresult(l3)

fl3$method<-"method104"
rm(temp1)
############method 105 PAON1 is equal to ppp and SAON is equal to ss############
tranl <- matchleft(tranl,fl3)

tranl$paon1 <- gsub(" - ","-",tranl$paon )
tranl$paon1 <- gsub(", "," ",tranl$paon1 )
tranl$addressf <-paste(tranl$postcode,tranl$paon1,sep=",")
tranl$addressf <-paste(tranl$addressf,tranl$saon,sep=",")
tranl_1<-tranl[tranl$saon!="",]

add$pp1 <-paste(add$paostartnumber,add$paoendnumber,sep="-") 
add$ppp <-paste(add$paotext,add$pp1,sep=" ") 

add$addressf <- paste(add$postcodelocator,add$ppp,sep=",")
add$addressf <- paste(add$addressf,add$ss,sep=",")

l4<- inner_join(tranl_1,add,by="addressf")
l4<-l4[,..needlist1]

fl4<-uniqueresult(l4)

fl4$method<-"method105"
rm(tranl_1)
############method 106 PAON1 is equal to ppp and SAON is equal to flatss############
tranl <- matchleft(tranl,fl4)

tranl$paon1 <- gsub(" - ","-",tranl$paon )
tranl$paon1 <- gsub(", "," ",tranl$paon1 )
tranl$addressf <-paste(tranl$postcode,tranl$paon1,sep=",")
tranl$addressf <-paste(tranl$addressf,tranl$saon,sep=",")
tranl_1<-tranl[tranl$saon!="",]

add$pp1 <-paste(add$paostartnumber,add$paoendnumber,sep="-") 
add$ppp <-paste(add$paotext,add$pp1,sep=" ") 

add$addressf <- paste(add$postcodelocator,add$ppp,sep=",")
add$fss <- paste("FLAT ",add$ss,sep="")
add$addressf <- paste(add$addressf,add$fss,sep=",")

l5<- inner_join(tranl_1,add,by="addressf")
l5<-l5[,..needlist1]

fl5<-uniqueresult(l5)

fl5$method<-"method106"
rm(tranl_1)
############method 107 PAON1 is equal to ppp and SAON is equal to saotext############
tranl <- matchleft(tranl,fl5)

tranl$paon1 <- gsub(" - ","-",tranl$paon )
tranl$paon1 <- gsub(", "," ",tranl$paon1 )
tranl$addressf <-paste(tranl$postcode,tranl$paon1,sep=",")
tranl$addressf <-paste(tranl$addressf,tranl$saon,sep=",")
tranl_1<-tranl[tranl$saon!="",]

add$pp1 <-paste(add$paostartnumber,add$paoendnumber,sep="-") 
add$ppp <-paste(add$paotext,add$pp1,sep=" ") 
add$addressf <- paste(add$postcodelocator,add$ppp,sep=",")
add$addressf <- paste(add$addressf,add$saotext,sep=",")


l6<- inner_join(tranl_1,add,by="addressf")
l6<-l6[,..needlist1]

fl6<-uniqueresult(l6)

fl6$method<-"method107"
rm(tranl_1)
############method 108 PAON1 is equal to buildingname and FLATSAON is equal to subbuildingname############
tranl <- matchleft(tranl,fl6)

tranl$fs <- paste("FLAT ",tranl$saon,sep="") 

tranl$paon1 <- gsub(" - ","-",tranl$paon )
tranl$paon1 <- gsub(", "," ",tranl$paon1 )
tranl$addressf <-paste(tranl$postcode,tranl$paon1,sep=",")
tranl$addressf <-paste(tranl$addressf,tranl$fs ,sep=",")
tranl_1<-tranl[tranl$saon!="",]

add$addressf <- paste(add$postcodelocator,add$buildingname,sep=",")
add$addressf <- paste(add$addressf,add$subbuildingname,sep=",")

l7<- inner_join(tranl_1,add,by="addressf")
l7<-l7[,..needlist1]

fl7<-uniqueresult(l7)

fl7$method<-"method108"
rm(tranl_1)
############method 109 PAON1 is equal to buildingname and SAON is equal to flatsub############
tranl <- matchleft(tranl,fl7)

tranl$paon1 <- gsub(" - ","-",tranl$paon )
tranl$paon1 <- gsub(", "," ",tranl$paon1 )
tranl$addressf <-paste(tranl$postcode,tranl$paon1,sep=",")
tranl$addressf <-paste(tranl$addressf,tranl$saon ,sep=",")
tranl_1<-tranl[tranl$saon!="",]

add$addressf <- paste(add$postcodelocator,add$buildingname,sep=",")
add$fsub <- paste("FLAT ",add$subbuildingname,sep="")
add$addressf <- paste(add$addressf,add$fsub,sep=",")

l8<- inner_join(tranl_1,add,by="addressf")
l8<-l8[,..needlist1]

fl8<-uniqueresult(l8)

fl8$method<-"method109"
rm(tranl_1)
############method 110 PAON1 is equal to buildingname and SAON is equal to ss############
tranl <- matchleft(tranl,fl8)

tranl$paon1 <- gsub(" - ","-",tranl$paon )
tranl$paon1 <- gsub(", "," ",tranl$paon1 )
tranl$addressf <-paste(tranl$postcode,tranl$paon1,sep=",")
tranl$addressf <-paste(tranl$addressf,tranl$saon ,sep=",")
tranl_1<-tranl[tranl$saon!="",]

add$addressf <- paste(add$postcodelocator,add$buildingname,sep=",")
add$addressf <- paste(add$addressf,add$ss,sep=",")


l9<- inner_join(tranl_1,add,by="addressf")
l9<-l9[,..needlist1]

fl9<-uniqueresult(l9)

fl9$method<-"method110"
rm(tranl_1)
############method 111 For SAON is not null: PAON2 is equal to pp4 and SAON is equal to saotext ############
tranl <- matchleft(tranl,fl9)

tranl$paon2 <- gsub(" - ","-",tranl$paon )
tranl$addressf <-paste(tranl$postcode,tranl$paon2,sep=",")
tranl$addressf <-paste(tranl$addressf,tranl$saon ,sep=",")
tranl_1<-tranl[tranl$saon!="",]

add$pp4 <-paste(add$paostartnumber,add$paoendnumber,sep="-") 
add$addressf <- paste(add$postcodelocator,add$pp4,sep=",")
add$addressf <- paste(add$addressf,add$saotext,sep=",")
l10<- inner_join(tranl,add,by="addressf")

l10<- inner_join(tranl_1,add,by="addressf")
l10<-l10[,..needlist1]

fl10<-uniqueresult(l10)

fl10$method<-"method111"
rm(tranl_1)
############method 112 PAON2 is equal to pp4 and FLATSAON is equal to saotext############
tranl <- matchleft(tranl,fl10)

tranl$paon2 <- gsub(" - ","-",tranl$paon )
tranl$addressf <-paste(tranl$postcode,tranl$paon2,sep=",")
tranl$fs <- paste("FLAT ",tranl$saon,sep="")
tranl$addressf <-paste(tranl$addressf,tranl$fs ,sep=",")
tranl_1<-tranl[tranl$saon!="",]

add$pp4 <-paste(add$paostartnumber,add$paoendnumber,sep="-") 
add$addressf <- paste(add$postcodelocator,add$pp4,sep=",")
add$addressf <- paste(add$addressf,add$saotext,sep=",")

l11<- inner_join(tranl_1,add,by="addressf")
l11<-l11[,..needlist1]

fl11<-uniqueresult(l11)

fl11$method<-"method112"
rm(tranl_1)
############method 113 PAON2 is equal to pp4 and SAON is equal to subbuildingname############
tranl <- matchleft(tranl,fl11)

tranl$paon2 <- gsub(" - ","-",tranl$paon )
tranl$addressf <-paste(tranl$postcode,tranl$paon2,sep=", ")
tranl$addressf <-paste(tranl$addressf,tranl$saon ,sep=", ")

add$pp1 <-paste(add$paostartnumber,add$paoendnumber,sep="-") 
add$addressf <- paste(add$postcodelocator,add$pp1,sep=", ")
add$addressf <- paste(add$addressf,add$subbuildingname,sep=", ")
tranl_1<-tranl[tranl$saon!="",]

l12<- inner_join(tranl_1,add,by="addressf")
l12<-l12[,..needlist1]

fl12<-uniqueresult(l12)

fl12$method<-"method113"
rm(tranl_1)
############method 114 PAON2 is equal to pp4 and SAON is equal to ssp############
tranl <- matchleft(tranl,fl12)

tranl$paon2 <- gsub(" - ","-",tranl$paon )
tranl$addressf <-paste(tranl$postcode,tranl$paon2,sep=",")
tranl$addressf <-paste(tranl$addressf,tranl$saon ,sep=",")
tranl_1<-tranl[tranl$saon!="",]

add$pp4 <-paste(add$paostartnumber,add$paoendnumber,sep="-") 
add$addressf <- paste(add$postcodelocator,add$pp4,sep=",")
add$sp <-  paste(add$saostartnumber,add$paotext,sep=" ")
add$ssp <- paste(add$saotext,add$sp,sep=", ")
add$addressf <- paste(add$addressf,add$ssp,sep=",")

l13<- inner_join(tranl_1,add,by="addressf")

l13<-l13[,..needlist1]
fl13<-uniqueresult(l13)

fl13$method<-"method114"
rm(tranl_1)
############method 115 PAON1 is equal to buildingname and SAON3 is equal to saotext8############
tranl <- matchleft(tranl,fl13)

tranl$paon1 <- gsub(" - ","-",tranl$paon )
tranl$paon1 <- gsub(", "," ",tranl$paon1 )

tranl$addressf <-paste(tranl$postcode,tranl$paon1,sep=",")

tranl_1<-tranl[grepl("\\d$",tranl$saon),]
tranl_1<-tranl_1[str_count(tranl_1$saon,"\\W+")>=2,]
tranl_1$saon3<-word(tranl_1$saon,-1)
tranl_1$addressf <-paste(tranl_1$addressf,tranl_1$saon3,sep=",")
#tranl_1<-tranl[tranl$saon!="",]

add$addressf <- paste(add$postcodelocator,add$buildingname,sep=",")
add$saotext8 <- word(add$saotext,-1)
add$addressf <- paste(add$addressf,add$saotext8,sep=",")

l14<- inner_join(tranl_1,add,by="addressf")
l14<-l14[,..needlist1]

fl14<-uniqueresult(l14)

fl14$method<-"method115"
############method 116 PAON1 is equal to buildingname and SAON8 is equal to ss############
tranl <- matchleft(tranl,fl14)

tranl$paon1 <- gsub(" - ","-",tranl$paon )
tranl$paon1 <- gsub(", "," ",tranl$paon1 )

tranl$addressf <-paste(tranl$postcode,tranl$paon1,sep=",")

tranl_1<-tranl[grepl("^\\d",tranl$saon),]
tranl_1<-tranl_1[str_count(tranl_1$saon,"\\W+")>=1,]
tranl_1$saonf<-word(tranl_1$saon,1)
tranl_1$addressf <-paste(tranl_1$addressf,tranl_1$saonf,sep=",")
#tranl_1<-tranl[tranl$saon!="",]

add$addressf <- paste(add$postcodelocator,add$buildingname,sep=",")
add$addressf <- paste(add$addressf,add$ss,sep=",")


l15<- inner_join(tranl_1,add,by="addressf")
l15<-l15[,..needlist1]

fl15<-uniqueresult(l15)

fl15$method<-"method116"
############method 117 PAON is equal to buildingname and SAON3 is equal to saotext8############
tranl <- matchleft(tranl,fl15)

tranl$addressf <-paste(tranl$postcode,tranl$paon,sep=",")

tranl_1<-tranl[grepl("\\d$",tranl$saon),]

tranl_1$saon3<-word(tranl_1$saon,-1)
tranl_1$addressf <-paste(tranl_1$addressf,tranl_1$saon3,sep=",")
#tranl_1<-tranl[tranl$saon!="",]

add$addressf <- paste(add$postcodelocator,add$buildingname,sep=",")
add$saotext8 <- word(add$saotext,-1)
add$addressf <- paste(add$addressf,add$saotext8,sep=",")

l16<- inner_join(tranl_1,add,by="addressf")
l16<-l16[,..needlist1]

fl16<-uniqueresult(l16)

fl16$method<-"method117"
############method 118 PAON is equal to buildingname and SAON2 is equal to saotext3############
tranl <- matchleft(tranl,fl16)

tranl$addressf <-paste(tranl$postcode,tranl$paon,sep=",")
tranl_1<-tranl[str_count(tranl$saon,"\\W+")>=2,]
tranl_1$saontwo<-word(tranl_1$saon,1,2)
tranl_1$addressf <-paste(tranl_1$addressf,tranl_1$saontwo,sep=",")

add$addressf <- paste(add$postcodelocator,add$buildingname,sep=",")
add$saotext3 <- word(add$saotext,1,2)
add$addressf <- paste(add$addressf,add$saotext3,sep=",")

l17<- inner_join(tranl_1,add,by="addressf")
l17<-l17[,..needlist1]

fl17<-uniqueresult(l17)

fl17$method<-"method118"
rm(tranl_1)
############method 119 PAON2 is equal to buildingname and SAON3 is equal to saotext8############
tranl <- matchleft(tranl,fl17)

tranl$paon2 <- gsub(" - ","-",tranl$paon )
tranl$addressf <-paste(tranl$postcode,tranl$paon2,sep=", ")

tranl_1<-tranl[tranl$saon!="",]
tranl_1<-tranl_1[grepl("\\d$",tranl_1$saon),]

tranl_1$saonl<-word(tranl_1$saon,-1)
tranl_1$addressf <-paste(tranl_1$addressf,tranl_1$saonl,sep=", ")


add$addressf <- paste(add$postcodelocator,add$buildingname,sep=", ")
add$saotextc <- word(add$saotext,-1)
add$addressf <- paste(add$addressf,add$saotextc,sep=", ")

l18<- inner_join(tranl_1,add,by="addressf")
l18<-l18[,..needlist1]

fl18<-uniqueresult(l18)

fl18$method<-"method119"

tranl <- matchleft(tranl,fl18)
add<-add[,..addlist1]
rm(tranl_1)
############sum up the linked data in stage 8############
ly = list(fl1,fl2,fl3,fl4,fl5,fl6,fl7,fl8,fl9,fl10,fl11,fl12,fl13,fl14,fl15,fl16,fl17,fl18,finalll)
stage8<- rbindlist(ly ,use.names=TRUE,fill=T)

dbWriteTable(con, "stage8",value =stage8, append = TRUE, row.names = FALSE)
dbWriteTable(con, "finalll",value =finalll, append = TRUE, row.names = FALSE)
dbWriteTable(con, "tran_stage8_left",value =tranl, append = TRUE, row.names = FALSE)

rm(finalll,fl)
rm(l1,l2,l3,l4,l5,l6,l7,l8,l9,l10,l11,l12,l13,l14,l15,l16,l17,l18,ll)
rm(fl1,fl2,fl3,fl4,fl5,fl6,fl7,fl8,fl9,fl10,fl11,fl12,fl13,fl14,fl15,fl16,fl17,fl18)
rm(tranl_1,needl)
###########################stage 9###########################
tran22 <- matchleft(tran33,stage8)

add<-add[add$postset %in% tran22$postset,]
############method 120 STREET is equal to paotext and PAON is equal to ss############
#extract the transaciton which can be linked when STREET is equal to paotext in OS AddressBase data within the same postcode
tran22$addressf <-paste(tran22$postcode,tran22$street,sep=",")
add$addressf <- paste(add$postcodelocator,add$paotext,sep=",")

w <- inner_join(tran22,add,by="addressf")

w <-w[w$street!="",]
w<-w[,..needlist1]
tranw <-tranneed(tran22,w) 

#conduct the linkage method 120
tranw$addressf <-paste(tranw$postcode,tranw$paon,sep=",")
tranw$addressf <-paste(tranw$addressf,tranw$street,sep=",")

add$addressf <- paste(add$postcodelocator,add$ss,sep=",")
add$addressf <- paste(add$addressf,add$paotext,sep=",")

w1<- inner_join(tranw,add,by="addressf")
w1<-w1[,..needlist1]

fw1<-uniqueresult(w1)

fw1$method<-"method120"
############method 121 PAONSTREET is equal to buildingname############
tranw <-matchleft(tranw,fw1) 

tranw$pastr <- paste(tranw$paon,tranw$street,sep=" ")
tranw$addressf <-paste(tranw$postcode,tranw$pastr,sep=",")

add$addressf <- paste(add$postcodelocator,add$buildingname,sep=",")


w2<- inner_join(tranw,add,by="addressf")

w2<-w2[,..needlist1]

fw2<-uniqueresult(w2)

fw2$method<-"method121"
############method 122 PAONSTREET is equal to paotext, then keep the linked result which saon is same as ss ############
tranw <-matchleft(tranw,fw2) 

tranw$pastr <- paste(tranw$paon,tranw$street,sep=" ")
tranw$addressf <-paste(tranw$postcode,tranw$pastr,sep=",")
 
add$addressf <- paste(add$postcodelocator,add$paotext,sep=",")

w3<- inner_join(tranw,add,by="addressf")


fw3<-uniqueresult(w3)

fw3<-fw3[fw3$saon==fw3$ss,]
fw3<-fw3[,..needlist1]
fw3$method<-"method122"
############method 123 PAONSTREET is equal to paotext and SAON is equal to ss############
tranw <-matchleft(tranw,fw3) 

tranw$pastr <- paste(tranw$paon,tranw$street,sep=" ")
tranw$addressf <-paste(tranw$postcode,tranw$pastr,sep=",")
tranw$addressf <-paste(tranw$addressf,tranw$saon,sep=",")

add$addressf <- paste(add$postcodelocator,add$paotext,sep=",")
add$addressf <- paste(add$addressf,add$ss,sep=",")

w4<- inner_join(tranw,add,by="addressf")
w4<-w4[,..needlist1]

fw4<-uniqueresult(w4)

fw4$method<-"method123"

############method 124 PAON is equal to ss and SAON is equal to saotext############
tranw <-matchleft(tranw,fw4) 

tranw$addressf <-paste(tranw$postcode,tranw$paon,sep=",")
tranw$addressf <-paste(tranw$addressf,tranw$street,sep=",")
tranw$addressf <-paste(tranw$addressf,tranw$saon,sep=",")

add$addressf <- paste(add$postcodelocator,add$ss,sep=",")
add$addressf <- paste(add$addressf,add$paotext,sep=",")
add$addressf <- paste(add$addressf,add$saotext,sep=",")

w5<- inner_join(tranw,add,by="addressf")
w5<-w5[,..needlist1]

fw5<-uniqueresult(w5)

fw5$method<-"method124"
############method 125 FLATPAON is equal to subbuildingname############
tranw <-matchleft(tranw,fw5) 

tranw$flatpaon <- paste("FLAT ",tranw$paon,sep="") 
tranw$addressf <-paste(tranw$postcode,tranw$flatpaon,sep=",")
tranw$addressf <-paste(tranw$addressf,tranw$street,sep=",")

add$addressf <- paste(add$postcodelocator,add$subbuildingname,sep=",")
add$addressf <- paste(add$addressf,add$paotext,sep=",")

w6<- inner_join(tranw,add,by="addressf")
w6<-w6[,..needlist1]

fw6<-uniqueresult(w6)

fw6$method<-"method125"
############method 126 UNITPAON is equal to saotext############
tranw <-matchleft(tranw,fw6) 

tranw$unpaon <- paste("UNIT ",tranw$paon,sep="") 
tranw$addressf <-paste(tranw$postcode,tranw$unpaon,sep=",")
tranw$addressf <-paste(tranw$addressf,tranw$street,sep=",")

add$addressf <- paste(add$postcodelocator,add$saotext,sep=",")
add$addressf <- paste(add$addressf,add$paotext,sep=",")



w7<- inner_join(tranw,add,by="addressf")
w7<-w7[,..needlist1]

fw7<-uniqueresult(w7)

fw7$method<-"method126"
############method 127 PAON is equal to saotext############
tranw <-matchleft(tranw,fw7) 

tranw$addressf <-paste(tranw$postcode,tranw$street,sep=",")
tranw$addressf <-paste(tranw$addressf,tranw$paon ,sep=",")

add$addressf <- paste(add$postcodelocator,add$paotext,sep=",")
add$addressf <- paste(add$addressf,add$saotext,sep=",")


w8<- inner_join(tranw,add,by="addressf")
w8<-w8[,..needlist1]

fw8<-uniqueresult(w8)

fw8$method<-"method127"
############method 128 PAON3 is equal to ss############
tranw <-matchleft(tranw,fw8) 

tranw$addressf <-paste(tranw$postcode,tranw$street,sep=",")

tranw$paon3 <-word(tranw$paon,-1)
tranw$addressf <-paste(tranw$addressf,tranw$paon3,sep=",")

add$addressf <- paste(add$postcodelocator,add$paotext,sep=",")
add$addressf <- paste(add$addressf,add$ss,sep=",")

w9<- inner_join(tranw,add,by="addressf")
w9<-w9[,..needlist1]

fw9<-uniqueresult(w9)

fw9$method<-"method128"
############method 129 SAONPOAN is equal to saotext############
tranw <-matchleft(tranw,fw9) 

tranw$addressf <-paste(tranw$postcode,tranw$street,sep=",")

tranw$saonpaon <-paste(tranw$saon,tranw$paon ,sep=" ")
tranw$addressf <-paste(tranw$addressf,tranw$saonpaon ,sep=",")

add$addressf <- paste(add$postcodelocator,add$paotext,sep=",")
add$addressf <- paste(add$addressf,add$saotext,sep=",")

w10<- inner_join(tranw,add,by="addressf")
w10<-w10[,..needlist1]

fw10<-uniqueresult(w10)

fw10$method<-"method129"
add<-add[,..addlist1]
############method 130 SAONPOAN is equal to buildingname############
tranw <-matchleft(tranw,fw10) 

tranw$addressf <-paste(tranw$postcode,tranw$street,sep=",")

tranw$saonpaon <-paste(tranw$saon,tranw$paon ,sep=" ")
tranw$addressf <-paste(tranw$addressf,tranw$saonpaon ,sep=",")

add$addressf <- paste(add$postcodelocator,add$paotext,sep=",")
add$addressf <- paste(add$addressf,add$buildingname,sep=",")

w11<- inner_join(tranw,add,by="addressf")
w11<-w11[,..needlist1]

fw11<-uniqueresult(w11)

fw11$method<-"method130"
############method 131 PAONSTREET is equal to buildingname1############
tranw <-matchleft(tranw,fw11) 

tranw$addressf <-paste(tranw$postcode,tranw$paon,sep=",")
tranw$addressf <-paste(tranw$addressf,tranw$street ,sep=" ")

add$buildingname1 <-gsub("[.]","",add$buildingname)
add$addressf <- paste(add$postcodelocator,add$buildingname1,sep=",")

w12<- inner_join(tranw,add,by="addressf")
w12<-w12[,..needlist1]

fw12<-uniqueresult(w12)
fw12$method<-"method131"
############method 132 POANSAON is equal to saotext############
tranw <-matchleft(tranw,fw12) 

tranw$addressf <-paste(tranw$postcode,tranw$street,sep=",")

tranw$paonsaon <-paste(tranw$paon,tranw$saon ,sep=" ")
tranw$addressf <-paste(tranw$addressf,tranw$paonsaon ,sep=",")

add$addressf <- paste(add$postcodelocator,add$paotext,sep=",")

add$addressf <- paste(add$addressf,add$saotext,sep=",")

w13<- inner_join(tranw,add,by="addressf")
w13<-w13[,..needlist1]

fw13<-uniqueresult(w13)
fw13$method<-"method132"
############method 133 PAON4 is equal to saotext############
tranw <-matchleft(tranw,fw13) 

tranw$addressf <-paste(tranw$postcode,tranw$street,sep=",")
tranw$paon1 <- gsub(",","",tranw$paon )
tranw$addressf <-paste(tranw$addressf,tranw$paon1 ,sep=",")

add$addressf <- paste(add$postcodelocator,add$paotext,sep=",")
add$addressf <- paste(add$addressf,add$saotext,sep=",")

w14<- inner_join(tranw,add,by="addressf")
w14<-w14[,..needlist1]

fw14<-uniqueresult(w14)

fw14$method<-"method133"
############method 134 POAN1 is equal to ss2############
tranw <-matchleft(tranw,fw14) 

tranw$addressf <-paste(tranw$postcode,tranw$street,sep=",")
tranw$paon1 <- gsub(" - ","-",tranw$paon )
tranw$addressf <-paste(tranw$addressf,tranw$paon1 ,sep=",")

add$addressf <- paste(add$postcodelocator,add$paotext,sep=",")
add$ss2 <-paste(add$saostartnumber,add$saoendnumber,sep="-")
add$addressf <- paste(add$addressf,add$ss2,sep=",")

w15<- inner_join(tranw,add,by="addressf")
w15<-w15[,..needlist1]

fw15<-uniqueresult(w15)

fw15$method<-"method134"
############method 135 STREET is equal to paotext and POAN is equal to saotext3############
tranw <-matchleft(tranw,fw15) 

tranw$addressf <-paste(tranw$postcode,tranw$street,sep=",")
tranw$addressf <-paste(tranw$addressf,tranw$paon,sep=",")

add$addressf <- paste(add$postcodelocator,add$paotext,sep=",")
add$saotext3 <-word(add$saotext,1,2)
add$addressf <- paste(add$addressf,add$saotext3,sep=",")


w16<- inner_join(tranw,add,by="addressf")
w16<-w16[,..needlist1]

fw16<-uniqueresult(w16)

fw16$method<-"method135"
############method 136 STREET is equal to paotext and PAON3 is equal to saotext8############
tranw <-matchleft(tranw,fw16) 

tranw$addressf <-paste(tranw$postcode,tranw$street,sep=",")
tranw_1<-tranw[grepl("\\d$",tranw$paon),]
tranw_1<-tranw_1[!grepl("-",tranw_1$paon),]
tranw_1$paon3 <-word(tranw_1$paon,-1)
tranw_1$addressf <-paste(tranw_1$addressf,tranw_1$paon3,sep=",")

add$addressf <- paste(add$postcodelocator,add$paotext,sep=",")
add$saotext8 <-word(add$saotext,-1)
add$addressf <- paste(add$addressf,add$saotext8,sep=",")

w17<- inner_join(tranw_1,add,by="addressf")
fw17<-w17[!grepl("\\d",w17$saon),]

fw17<-uniqueresult(fw17)
fw17<-fw17[,..needlist1]

fw17$method<-"method136"
rm(tranw_1)

tranw <-matchleft(tranw,fw17) 
add<-add[,..addlist1]
############sum up the linked data in stage 9############
ly = list(fw1,fw2,fw3,fw4,fw5,fw6,fw7,fw8,fw9,fw10,fw11,fw12,fw13,fw14,fw15,fw16,fw17)
stage9<- rbindlist(ly ,use.names=TRUE,fill=T)

dim(stage9)
#142916     31
length(unique(stage9$transactionid))
# 142916     30
length(unique(stage9$method))
#17

dbWriteTable(con, "stage9",value =stage9, append = TRUE, row.names = FALSE)
rm()
rm(fw1,fw2,fw3,fw4,fw5,fw6,fw7,fw8,fw9,fw10,fw11,fw12,fw13,fw14,fw15,fw16,fw17)
rm(w1,w2,w3,w4,w5,w6,w7,w8,w9,w10,w11,w12,w13,w14,w15,w16,w17)

###########################stage 10 ###########################
tran11 <- matchleft(tran22,stage9)

############method 137 PAON is equal to saopp############
tran11$addressf <-paste(tran11$postcode,tran11$paon,sep=",")

add$saopp <- paste(add$saotext,add$pp,sep=", ")
add$addressf <- paste(add$postcodelocator,add$saopp,sep=",")

a <- inner_join(tran11,add,by="addressf")
a<-a[,..needlist1]

finala <- uniqueresult(a)

needa <- doubleresult(a)

finala$method<-"method137"
############method 138 PAON is equal to saopp and SAON is equal to flatss############
trana<-tranneed(tran11,needa)

trana$addressf <-paste(trana$postcode,trana$paon,sep=",")
trana$addressf <-paste(trana$addressf,trana$saon,sep=",")

add$saopp <- paste(add$saotext,add$pp,sep=", ")
add$flatss <- paste("FLAT ",add$ss,sep="")
add$addressf <- paste(add$postcodelocator,add$saopp,sep=",")
add$addressf <- paste(add$addressf,add$flatss,sep=",")

a1 <- inner_join(trana,add,by="addressf")
a1 <-a1 [,..needlist1]

fa1 <-uniqueresult(a1 )

fa1$method<-"method138"
############method 139 PAON is equal to saopp and SAON is equal to ss############
trana <- matchleft(trana,fa1)

trana$addressf <-paste(trana$postcode,trana$paon,sep=",")
trana$addressf <-paste(trana$addressf,trana$saon,sep=",")
add$saopp <- paste(add$saotext,add$pp,sep=", ")

add$addressf <- paste(add$postcodelocator,add$saopp,sep=",")
add$addressf <- paste(add$addressf,add$ss,sep=",")

a2 <- inner_join(trana,add,by="addressf")
a2 <-a2 [,..needlist1]

fa2 <-uniqueresult(a2)

fa2$method<-"method139"
trana <- matchleft(trana,fa2)
add<-add[,..addlist1]
############sum up the linked data in stage 10############
stage10<-rbindlist(list(fa1,fa2,finala) ,use.names=TRUE,fill=T)

dbWriteTable(con, "stage10",value =stage10, append = TRUE, row.names = FALSE)
rm(fa1,fa2,finala)
###########################stage 11###########################
tran0 <- matchleft(tran11,stage10)
rm(tran22,tran11)

############method 140 SAONPAON is equal to buildingname############
tran0$saonpaon <- paste(tran0$saon,tran0$paon,sep=" ")
tran0$addressf <-paste(tran0$postcode,tran0$saonpaon,sep=",")

add$addressf <- paste(add$postcodelocator,add$buildingname,sep=",")

g <- inner_join(tran0,add,by="addressf")

g<-g[,..needlist1]

stage11 <- uniqueresult(g)
stage11$method<-"method140"

needg <- doubleresult(g)
trang<-tranneed(tran0,needg)

dim(stage11)
# 51692 
length(unique(stage11$transactionid))
# 51692 
length(unique(stage11$method))
#1
#save the result in PostGIS
dbWriteTable(con, "stage11",value =stage11, append = TRUE, row.names = FALSE)
dbWriteTable(con, "stage11_tranleft",value =trang, append = TRUE, row.names = FALSE)
###########################stage 12###########################
tran100 <- matchleft(tran0,stage11)

add<-add[,..addlist1]
add<-add[add$postset %in% tran100$postset,]
############method 141 PAON is equal to ss and SAON is NULL############
tran100$addressf <-paste(tran100$postcode,tran100$paon,sep=",")

add$addressf <- paste(add$postcodelocator,add$ss,sep=",")

k <- inner_join(tran100,add,by="addressf")
k<- k[k$saon=="",]

k<-k[,..needlist1]

finalk <- uniqueresult(k)

needk <- doubleresult(k)

trank<-tranneed(tran100,needk)

finalk$method<-"method141"
############method 142 PAONSTREET is equal to buildingname############
trank<-matchleft(trank,finalk) 

trank$pastr <- paste(trank$paon,trank$street,sep=" ")
trank$addressf <-paste(trank$postcode,trank$pastr,sep=",")

add$addressf <- paste(add$postcodelocator,add$buildingname,sep=",")

k1<- inner_join(trank,add,by="addressf")
k1<- k1[k1$saon=='',]

k1<-k1[,..needlist1]

finalk1 <- uniqueresult(k1)

finalk1$method<-"method142"

trank<-matchleft(trank,finalk1) 
############sum up the linked data in stage 12############
stage12<-rbindlist(list(finalk,finalk1) ,use.names=TRUE,fill=T)

dbWriteTable(con, "stage12",value =stage12, append = TRUE, row.names = FALSE)
dbWriteTable(con, "stage12_tranleft",value =trank, append = TRUE, row.names = FALSE)

############sum up the linked data in all stages############
datalist<-list(stage1,stage2,stage3,stage4,stage5,stage6,stage7,stage8,stage9,stage10,stage11,stage12)
ppd_link<-rbindlist(datalist ,use.names=TRUE,fill=T)
 
needlistf<-c("transactionid","uprn","method")
ppd_link<-ppd_link[,..needlistf]
dim(ppd_link)
#25950983        3
dbWriteTable(con, "ppd_link",value =ppd_link, append = TRUE, row.names = FALSE)
tranleft<-matchleft(tran,ppd_link)
dbWriteTable(con, "tran_left",value =tranleft, append = TRUE, row.names = FALSE)
rm(list= ls()[! (ls() %in% c('ppd_link'))]) 
###########################end of data linkage###########################
#################################################################################################


