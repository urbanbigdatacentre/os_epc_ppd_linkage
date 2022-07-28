# ------------------------------------------------
# lrppd_os_final.R
# ------------------------------------------------
# Code provided as is and can be used or modified freely. 
# ------------------------------------------------
# Author: BIN CHI
# Urban Big Data Centre at University of Glasgow
# Bin.Chi@glasgow.ac.uk
# Date: 1/4/2022


#################### Section 1: Load package ##################
library("qdap")
library(data.table)
library("RPostgreSQL")
library("sqldf")
library("dplyr")
library(tidyverse)
library(stringr)
library(DBI)
library("dplyr")
#############  Section 2: Read in OS AddressBase Plus and Land Registry Price Paid Data(PPD) #############  
#read in OS AddressBase Plus
add<-fread("D:/OS_Data/e90_ab_plus_csv_gb/AB_Plus_Data.csv", encoding = 'UTF-8')
#read in Land Registry PPD
drv=dbDriver("PostgreSQL")
db <- "os_ubdc"
host_db <- "localhost"
db_port <- "5432"
db_user <- "postgres"
#Update your password for the PostGIS database(osubdc)
db_password <- "654321"
con <- dbConnect(RPostgres::Postgres(), dbname = db, host=host_db, port=db_port, user=db_user, password=db_password)

tran <- dbGetQuery(con,"select * from pricepaid") 
######### OS AddressBase data and Land Registry PPD pre-processing #########

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

#convert the field name to lowercase and then remove the "_" in the field name in OS AddressBase data 
setnames(add, tolower(names(add)))
colnames(add) <- gsub("_", "", colnames(add))

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



needlist1<-c("transactionid","uprn","postcode.x","postcodelocator","postset.x", "postset.y","propertytype","paon","saon","street","locality.x","addressf","saostartnumber","saostartsuffix","saoendnumber","saoendsuffix","saotext","paostartnumber","paostartsuffix","paoendnumber","paoendsuffix","paotext","streetdescription","buildingname","buildingnumber","subbuildingname","bb","ss","pp")
needlist11<-c("transactionid","uprn","postcode.x","postcodelocator","postset.x", "postset.y","propertytype","paon","saon","street","locality.x","add1ressf","saostartnumber","saostartsuffix","saoendnumber","saoendsuffix","saotext","paostartnumber","paostartsuffix","paoendnumber","paoendsuffix","paotext","streetdescription","buildingname","buildingnumber","subbuildingname","ostopotoid","bb","ss","pp")
needlist12<-c("transactionid","uprn","postcode.x","postcodelocator","postset.x", "postset.y","propertytype","paon","saon","street","locality.x","add11ressf","saostartnumber","saostartsuffix","saoendnumber","saoendsuffix","saotext","paostartnumber","paostartsuffix","paoendnumber","paoendsuffix","paotext","streetdescription","buildingname","buildingnumber","subbuildingname","ostopotoid","bb","ss","pp")
needlist13<-c("transactionid","uprn","postcode.x","postcodelocator","postset.x", "postset.y","propertytype","paon","saon","street","locality.x","add11ressf","saostartnumber","saostartsuffix","saoendnumber","saoendsuffix","saotext","paostartnumber","paostartsuffix","paoendnumber","paoendsuffix","paotext","streetdescription","buildingname","buildingnumber","subbuildingname","ostopotoid","bb","ss","pp","method")
head(add)
addlist1<-c("uprn","parentuprn","postcodelocator","class","postcode","buildingname","buildingnumber","subbuildingname","paostartnumber","paostartsuffix","paoendnumber","paoendsuffix","paotext","saostartnumber","saostartsuffix","saoendnumber","saoendsuffix","saotext","streetdescription","locality","dependentlocality","townname","administrativearea","posttown","bb","ss","pp","postset")




################# Stage 1 PAON is NULL and SAON is not NULL  #################
trannull <- tran[tran$paon=="",]
trannull<- trannull[trannull$saon !="",]
#provide OS AddressBase data for this stage 1 linkage
addnull<-add[add$postset %in% trannull$postset, ]

############method 1 #SAON is equal to pp ############m
setDT(trannull)
setDT(addnull)
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
dim(stage1)

#save the stage 1 linked result in PostGIS database
dbWriteTable(con, "stage1",value =stage1, append = TRUE, row.names = FALSE)

#remove the linked results in this stage
rm(null1u,null2u,null3u,null4u,null5u,null6u,null7u)
rm(lu1)
rm(null1,null2,null3,null4,null5,null6,null7)


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
dim(x)
x<-x[,..needlist1]
x$method<-"method8"
#extract the one to many linked result for the following linkage process in Stage 2
need2 <- doubleresult(x1)
dim(need2)
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
#################sum up the linked result in stage 2##################
#combine allone to one linkage data in stage 2
ly = list(f0_1,f1,f2,f3,f4,f5,f6,f7,f8,f9,f10,f11,f12,f13,f14,f15,f16,f17,f18,f19,f20,f21,f22,f23,f24,f25,f26,f27,f28,f29,f30,f200,f201,x)
stage2<-rbindlist(ly ,use.names=TRUE,fill=T)

#save the stage 1  result in PostGIS database
dbWriteTable(con, "stage2",value =stage2, append = TRUE, row.names = FALSE)
dbWriteTable(con, "y",value =y, append = TRUE, row.names = FALSE)
dbWriteTable(con, "x",value =x, append = TRUE, row.names = FALSE)
dbWriteTable(con, "stage2_tran11_left",value =tran11, append = TRUE, row.names = FALSE)
dbWriteTable(con, "stage2_tran12_left",value =tran12, append = TRUE, row.names = FALSE)

#remove the linked results in this stage
rm(need2)
rm(need2_test)
rm(x,y)
rm(f0_1,f1,f2,f3,f4,f5,f6,f7,f8,f9,f10,f11,f12,f13,f14,f15,f16,f17,f18,f19,f20,f21,f22,f23,f24,f25,f26,f27,f28,f29,f30,f200,f201)
rm(y1_left,y2_left,y3_left,y4_left,y5_left,y6_left,y7_left,y8_left,y9_left,y10_left,y11_left,y12_left,y13_left,y14_left,y15_left,y16_left,y17_left,y18_left,y19_left,y20_left,y21_left,y22_left,y23_left,y24_left,y25_left,y26_left,y27_left,y28_left,y29_left,y30_left,y200_left,y201_left)
rm(y01,y1,y2,y3,y4,y5,y6,y7,y8,y9,y10,y11,y12,y13,y14,y15,y16,y17,y18,y19,y20,y21,y22,y23,y24,y25,y26,y27,y28,y29,y30,y200,y201)
rm(add1)
rm(x1)
rm(need2_2,tran11,tran12,tran121)
################# stage 3 PAON is equal to paostartnumber #################

tran1 <- tran[tran$paon!="",]
tran88 <- matchleft(tran1,stage2)

add88<- add[add$postcodelocator %in% tran88$postcode,  ]

add88$pp<-str_trim(add88$pp)

add11<- add88[!is.na(add88$pp),]
rm(add88)

tran88$addressf <-paste(tran88$postcode,tran88$paon,sep=",")
add11$addressf <- paste(add11$postcodelocator,add11$paostartnumber,sep=",")


z <- inner_join(tran88,add11,by="addressf")

finalz <- uniqueresult(z)

needz <- doubleresult(z)
#extract the transacion records for the following data linkage process in stage 3
tranpro22 <- tranneed(tran88,needz)

#################method 42##################

tranpro22$add11ressf <-paste(tranpro22$postcode,tranpro22$paon,sep=", ")
tranpro22$add11ressf <-paste(tranpro22$add11ressf,tranpro22$saon,sep=", ")

add11$flatp <- paste("FLAT ", add11$paostartsuffix,sep="")
add11$add11ressf <- paste(add11$postcodelocator,add11$paostartnumber,sep=", ")
add11$add11ressf <- paste(add11$add11ressf,add11$flatp ,sep=", ")

zz1<- inner_join(tranpro22,add11,by="add11ressf")

zz1<-zz1[,..needlist12]
ff1<-uniqueresult(zz1)

ff1$method<-"method42"
#################method 43##################
tranpro221 <- matchleft(tranpro22,ff1)

tranpro221$add11ressf <-paste(tranpro221$postcode,tranpro221$paon,sep=", ")
tranpro221$add11ressf <-paste(tranpro221$add11ressf,tranpro221$saon,sep=", ")

add11$add11ressf <- paste(add11$postcodelocator,add11$pp,sep=", ")
add11$add11ressf <- paste(add11$add11ressf,add11$saotext,sep=", ")

zz2<- inner_join(tranpro221,add11,by="add11ressf")

zz2<-zz2[,..needlist12]

ff2<-uniqueresult(zz2)

ff2$method<-"method43"

#################method 44##################
tranpro221 <- matchleft(tranpro221,ff2)

tranpro221$flats <- paste("FLAT ",tranpro221$saon,sep="")

tranpro221$addressf <-paste(tranpro221$postcode,tranpro221$paon,sep=", ")
tranpro221$add11ressf <-paste(tranpro221$addressf,tranpro221$flats,sep=", ")

add11$addressf <- paste(add11$postcodelocator,add11$pp,sep=", ")
add11$add11ressf <- paste(add11$addressf,add11$saotext,sep=", ")
zz3<- inner_join(tranpro221,add11,by="add11ressf")

zz3<-zz3[,..needlist12]

ff3<-uniqueresult(zz3)

ff3$method<-"method44"
#################method 45##################
tranpro221 <- matchleft(tranpro221,ff3)

tranpro221$add11ressf <-paste(tranpro221$postcode,tranpro221$paon,sep=", ")
tranpro221$add11ressf <-paste(tranpro221$add11ressf,tranpro221$street,sep=", ")

add11$add11ressf <- paste(add11$postcodelocator,add11$pp,sep=", ")
add11$add11ressf <- paste(add11$add11ressf,add11$streetdescription,sep=", ")

zz4<- inner_join(tranpro221,add11,by="add11ressf")

zz4<-zz4[,..needlist12]

ff4<-uniqueresult(zz4)

ff4$method<-"method45"
#################method 54 #################
tranpro221 <- matchleft(tranpro221,ff4)

tranpro221$add11ressf <-paste(tranpro221$postcode,tranpro221$paon,sep=", ")
tranpro221$saontwo<-word(tranpro221$saon,1,2)
tranpro221$add11ressf <-paste(tranpro221$add11ressf,tranpro221$saontwo,sep=", ")

add11$add11ressf <- paste(add11$postcodelocator,add11$pp,sep=", ")
add11$add11ressf <- paste(add11$add11ressf,add11$subbuildingname,sep=", ")

zz5<- inner_join(tranpro221,add11,by="add11ressf")

zz5<-zz5[,..needlist12]

ff5<-uniqueresult(zz5)

ff5$method<-"method54"
#################method 55 #################
tranpro221 <- matchleft(tranpro221,ff5)

tranpro221$add11ressf <-paste(tranpro221$postcode,tranpro221$paon,sep=", ")
tranpro221$saontwo<-word(tranpro221$saon,1,2)
tranpro221$add11ressf <-paste(tranpro221$add11ressf,tranpro221$saontwo,sep=", ")

add11$add11ressf <- paste(add11$postcodelocator,add11$pp,sep=", ")
add11$add11ressf <- paste(add11$add11ressf,add11$saotext,sep=", ")

zz6<- inner_join(tranpro221,add11,by="add11ressf")
zz6<-zz6[,..needlist12]

ff6<-uniqueresult(zz6)

ff6$method<-"method55"
#################method 56 #################
tranpro221 <- matchleft(tranpro221,ff6)

tranpro221$add11ressf <-paste(tranpro221$postcode,tranpro221$paon,sep=", ")
tranpro221$saontwo<-word(tranpro221$saon,1,2)
tranpro221$add11ressf <-paste(tranpro221$add11ressf,tranpro221$saontwo,sep=", ")

add11$add11ressf <- paste(add11$postcodelocator,add11$pp,sep=", ")
add12<-add11[str_count(add11$saotext,"\\W+")>=2,]
add12$saotext<-gsub("[/]","",add12$saotext)
add12$saotexttwo <-word(add12$saotext,1,2)
add12$add11ressf <- paste(add12$add11ressf,add12$saotexttwo,sep=", ")

zz7<- inner_join(tranpro221,add12,by="add11ressf")
zz7<-zz7[,..needlist12]

ff7<-uniqueresult(zz7)

ff7$method<-"method56"
#################method 57 #################
#GARDEN FLOOR FLAT to GARDEN FLAT
tranpro221 <- matchleft(tranpro221,ff7)

  
tranpro221$add11ressf <-paste(tranpro221$postcode,tranpro221$paon,sep=", ")
#tranpro221$saontwo<-word(tranpro221$saon,1,2)
tranpro221$add11ressf <-paste(tranpro221$add11ressf,tranpro221$saon,sep=", ")

add11$add11ressf <- paste(add11$postcodelocator,add11$pp,sep=", ")
add12<-add11

add12$saotextc <-gsub("GARDEN FLAT","GARDEN FLOOR FLAT",add12$saotext)
add12$add11ressf <- paste(add12$add11ressf,add12$saotextc,sep=", ")

zz8<- inner_join(tranpro221,add12,by="add11ressf")
zz8<-zz8[,..needlist12]

ff8<-uniqueresult(zz8)

ff8$method<-"method57"
#################method 58 #################
#FLAT FIRST FLOOR to  FIRST FLOOR FLAT
tranpro221 <- matchleft(tranpro221,ff8)
dim(tranpro221)
# 302583     21
needz<-matchleft(needz,ff8)


tranpro221$add11ressf <-paste(tranpro221$postcode,tranpro221$paon,sep=", ")
#tranpro221$saontwo<-word(tranpro221$saon,1,2)
tranpro221$add11ressf <-paste(tranpro221$add11ressf,tranpro221$saon,sep=", ")

add11$add11ressf <- paste(add11$postcodelocator,add11$pp,sep=", ")
add12<-add11

add12$saotextc <-gsub("FLAT FIRST FLOOR","FIRST FLOOR FLAT",add12$saotext)
add12$add11ressf <- paste(add12$add11ressf,add12$saotextc,sep=", ")
zz9<- inner_join(tranpro221,add12,by="add11ressf")
zz9<-zz9[,..needlist12]

ff9<-uniqueresult(zz9)
dim(ff9)
#9993

dim(tran11)
#90911    19

zz9_left<-zz9[!(zz9$transactionid %in% ff9$transactionid),]


ff9$method<-"method58"
#################method 59 #################

tranpro221 <- matchleft(tranpro221,ff9)
dim(tranpro221)
#300683     21
needz<-matchleft(needz,ff9)

#SECOND FLOOR FLAT in saontext , then saotext has SECOND FLOOR



tranpro221$add11ressf <-paste(tranpro221$postcode,tranpro221$paon,sep=", ")
#tranpro221$saontwo<-word(tranpro221$saon,1,2)
tranpro221_1<-tranpro221[grepl("SECOND FLOOR FLAT",tranpro221$saon)]
#tranpro221$add11ressf <-paste(tranpro221$add11ressf,tranpro221$saon,sep=", ")

add11$add11ressf <- paste(add11$postcodelocator,add11$pp,sep=", ")
add12<-add11[grepl("SECOND FLOOR",add11$saotext)]
# 
# add12$saotextc <-gsub("FLAT FIRST FLOOR","FIRST FLOOR FLAT",add12$saotext)
# add12$add11ressf <- paste(add12$add11ressf,add12$saotextc,sep=", ")


zz10<- inner_join(tranpro221_1,add12,by="add11ressf")
rm(tranpro221_1)
zz10<-zz10[,..needlist12]

ff10<-uniqueresult(zz10)
dim(ff10)
#1010103

dim(tran11)
#1001011    110

zz10_left<-zz10[!(zz10$transactionid %in% ff10$transactionid),]


ff10$method<-"method59"
#################method 60 #################
#FIRST FLOOR FLAT
#FLAT 1ST FLOOR

#FIRST AND SECOND FLOOR
tranpro221 <- matchleft(tranpro221,ff10)
dim(tranpro221)
#299279     21
needz<-matchleft(needz,ff10)





tranpro221$add11ressf <-paste(tranpro221$postcode,tranpro221$paon,sep=", ")
#tranpro221$saontwo<-word(tranpro221$saon,1,2)
tranpro221_1<-tranpro221[grepl("FIRST AND SECOND FLOOR",tranpro221$saon)]
#tranpro221$add11ressf <-paste(tranpro221$add11ressf,tranpro221$saon,sep=", ")

add11$add11ressf <- paste(add11$postcodelocator,add11$pp,sep=", ")
add12<-add11[grepl("1ST AND 2ND FLOOR",add11$saotext)]


zz11<- inner_join(tranpro221_1,add12,by="add11ressf")
rm(tranpro221_1)
zz11<-zz11[,..needlist12]

ff11<-uniqueresult(zz11)
dim(ff11)
#

dim(tran11)
#1101111    111

zz11_left<-zz11[!(zz11$transactionid %in% ff11$transactionid),]


ff11$method<-"method60"

#################method 61 #################
#FIRST FLOOR FLAT
#FLAT 1ST FLOOR

tranpro221 <- matchleft(tranpro221,ff11)
dim(tranpro221)
#299122     2
needz<-matchleft(needz,ff11)


tranpro221$add11ressf <-paste(tranpro221$postcode,tranpro221$paon,sep=", ")
#tranpro221$saontwo<-word(tranpro221$saon,1,2)
tranpro221_1<-tranpro221[tranpro221$saon!="",]
tranpro221_1$add11ressf <-paste(tranpro221_1$add11ressf,tranpro221_1$saon,sep=", ")
tranpro221_1$add11ressf <-gsub(" ","",tranpro221_1$add11ressf )


add11$add11ressf <- paste(add11$postcodelocator,add11$pp,sep=", ")
add12<-add11

add12$paotext_ss <-paste(add12$paotext,add12$ss,sep=" ")
add12$add11ressf <- paste(add12$add11ressf,add12$paotext_ss,sep=", ")
add12$add11ressf <-gsub(" ","",add12$add11ressf )
zz12<- inner_join(tranpro221_1,add12,by="add11ressf")
rm(tranpro221_1)
#

ff12<-uniqueresult(zz12)
dim(ff12)
# 6099

dim(tran12)
#1201212    121

zz12_left<-zz12[!(zz12$transactionid %in% ff12$transactionid),]


ff12$method<-"method61"



#################method 62 #################

tranpro221 <- matchleft(tranpro221,ff12)
dim(tranpro221)
#296918     21
needz<-matchleft(needz,ff12)
#
needz<-needz[!grep("FLAT", needz$saon),]
# needz<-needz[!grep("FLOOR", needz$saon),]
# tranpro221$add11ressf <-paste(tranpro221$postcode,tranpro221$paon,sep=", ")
# #tranpro221$saontwo<-word(tranpro221$saon,1,2)
# tranpro221$add11ressf <-paste(tranpro221$add11ressf,tranpro221$saon,sep=", ")
# 
# add11$add11ressf <- paste(add11$postcodelocator,add11$pp,sep=", ")
# add12<-add11
# 
# add12$paotext_ss <-paste(add12$paotext,add12$ss,sep=" ")
# add12$add11ressf <- paste(add12$add11ressf,add12$paotext_ss,sep=", ")
tranpro221$add11ressf <-paste(tranpro221$postcode,tranpro221$paon,sep=", ")
#tranpro221$saontwo<-word(tranpro221$saon,1,2)
tranpro221$add11ressf <-paste(tranpro221$add11ressf,tranpro221$saon,sep=", ")

add11$add11ressf <- paste(add11$postcodelocator,add11$pp,sep=", ")
add12<-add11

add12$saotextc <-gsub("FLAT","APARTMENT",add12$saotext)
add12$add11ressf <- paste(add12$add11ressf,add12$saotextc,sep=", ")






zz13<- inner_join(tranpro221,add12,by="add11ressf")
zz13<-zz13[,..needlist12]

ff13<-uniqueresult(zz13)
dim(ff13)
#139133

dim(tran11)
#1301311    113

zz13_left<-zz13[!(zz13$transactionid %in% ff13$transactionid),]


ff13$method<-"method62"


#################method 63 #################
tranpro221 <- matchleft(tranpro221,ff13)
dim(tranpro221)
# 297008     22
needz<-matchleft(needz,ff13)


tranpro221$add11ressf <-paste(tranpro221$postcode,tranpro221$paon,sep=", ")
tranpro221_1<-tranpro221[tranpro221$saon!="",]
tranpro221_1$add11ressf <-paste(tranpro221_1$add11ressf,tranpro221_1$saon,sep=", ")

add11$add11ressf <- paste(add11$postcodelocator,add11$pp,sep=", ")
add12<-add11

#add12$saotextc <-gsub("FLAT","APARTMENT",add12$saotext)
add12$add11ressf <- paste(add12$add11ressf,add12$ss,sep=", ")

zz14<- inner_join(tranpro221_1,add12,by="add11ressf")
zz14<-zz14[,..needlist12]

ff14<-uniqueresult(zz14)
dim(ff14)
#
rm(tranpro221_1)
dim(tran11)
#1401411    114

zz14_left<-zz14[!(zz14$transactionid %in% ff14$transactionid),]


ff14$method<-"method63"


#################method 64 #################
tranpro221 <- matchleft(tranpro221,ff14)
dim(tranpro221)
#  296918     21

needz<-matchleft(needz,ff14)
#needz1<-needz[grepl("\\d$",needz$saon),]


tranpro221$add11ressf <-paste(tranpro221$postcode,tranpro221$paon,sep=", ")
#tranpro221$saontwo<-word(tranpro221$saon,1,2)
tranpro221_1<-tranpro221[grepl("\\d$",tranpro221$saon),]
tranpro221_1$saonl<-word(tranpro221_1$saon,-1)
tranpro221_1$add11ressf <-paste(tranpro221_1$add11ressf,tranpro221_1$saonl,sep=", ")

add11$add11ressf <- paste(add11$postcodelocator,add11$pp,sep=", ")
add12<-add11[grepl("\\d$",add12$saotext),]

add12$saotextl <-word(add12$saotext,-1)
add12$add11ressf <- paste(add12$add11ressf,add12$saotextl,sep=", ")

zz15<- inner_join(tranpro221_1,add12,by="add11ressf")
zz15<-zz15[,..needlist12]

ff15<-uniqueresult(zz15)
dim(ff15)
#6099   30



zz15_left<-zz15[!(zz15$transactionid %in% ff15$transactionid),]


ff15$method<-"method64"
#################method 65 #################
tranpro221 <- matchleft(tranpro221,ff15)
dim(tranpro221)
#290819     21
needz<-matchleft(needz,ff15)
needz1<-needz[grepl("\\d",needz$saon),]

#################summ up stage 3 ff1 to ff15  #################
#CHALET 25 to FLAT 25


ly = list(ff1,ff2,ff3,ff4,ff5,ff6,ff7,ff8,ff9,ff10,ff11,ff12,ff13,ff14,ff15)
ff<- rbindlist(ly ,use.names=TRUE,fill=T)
dim(ff)
#191189     56
dim(ff1)[1]+dim(ff2)[1]+dim(ff3)[1]+dim(ff4)[1]+dim(ff5)[1]+dim(ff6)[1]+dim(ff7)[1]+dim(ff8)[1]+dim(ff9)[1]+
  dim(ff10)[1]+ dim(ff11)[1]+dim(ff12)[1]+dim(ff13)[1]+dim(ff14)[1]+dim(ff15)[1]
# 191189
finalz$method<-"method49"
head(finalz)
stage3 <- rbindlist(list(ff,finalz) ,use.names=TRUE,fill=T)
length(unique(stage3$transactionid))
#338426
length(unique(stage3$method))
#16
length(unique(ff$method))
#15

dbWriteTable(con, "stage3",value =stage3, append = TRUE, row.names = FALSE)

dbWriteTable(con, "finalz",value =finalz, append = TRUE, row.names = FALSE)
dbWriteTable(con, "ff",value =ff, append = TRUE, row.names = FALSE)

dbWriteTable(con, "tran_stage3_left",value =tranpro221, append = TRUE, row.names = FALSE)
rm(finalz)
rm(ff1,ff2,ff3,ff4,ff5,ff6,ff7,ff8,ff9,ff10,ff11,ff12,ff13,ff14,ff15)
rm(ly)
rm(ff)
rm(zz1,zz2,zz3,zz4,zz5,zz6,zz7,zz8,zz9,zz10,zz11,zz12,zz13,zz14,zz15)
rm(add11)
rm(tranpro22)
rm(z)
rm(add12)
rm(needz)
rm(zz1_left,zz2_left,zz3_left,zz4_left,zz5_left,zz6_left,zz7_left,zz8_left,zz9_left,zz10_left,zz11_left,zz12_left,zz13_left,zz14_left,zz15_left)
#################stage 4 #################

tran77 <- matchleft(tran88,stage3)
dim(tran77)
#2036996      18
dim(tran77)[1]/dim(tran)[1]

add77 <-add[add$postcodelocator %in% tran77$postcode,]

tran77$addressf <-paste(tran77$postcode,tran77$paon,sep=", ")
add77$addressf <- paste(add77$postcodelocator,add77$pp,sep=", ")

b<- inner_join(tran77,add77,by="addressf")

finalb<-uniqueresult(b)
dim(finalb)
# 50512
#################method 70 #################
needb <- doubleresult(b)
dim(needb)
#1264864      49
needb1<-needb[needb$saon!="",]
add77$pp<-str_trim(add77$pp)

tranpro22 <- tranneed(tran77,needb)
dim(tranpro22)

needb$method<-"method70"
#278179     18
#################method 71 #################
dim(tranpro22)

tranpro22$add11ressf <-paste(tranpro22$postcode,tranpro22$paon,sep=", ")
tranpro22$add11ressf <-paste(tranpro22$add11ressf,tranpro22$saon,sep=", ")
tranpro22_1<-tranpro22[tranpro22$saon!="",]

add77$add11ressf <- paste(add77$postcodelocator,add77$pp,sep=", ")
add77$add11ressf <- paste(add77$add11ressf,add77$saotext ,sep=", ")

bb1<- inner_join(tranpro22_1,add77,by="add11ressf")
head(bb1)

#bb1$method<-"method50"
bb1<-bb1[,..needlist12]
fb1<-uniqueresult(bb1)
dim(fb1)
#3538
bb1_left<-bb1[!(bb1$transactionid %in% fb1$transactionid),]
fb1$method<-"method71"


#################method 72 #################
tranpro221 <- matchleft(tranpro22,fb1)
dim(tranpro221)
# 274641     19

needb<-matchleft(needb,fb1)
needb1<-needb[needb$saon!="",]
needb1<-needb1[needb1$paostartsuffix!="",]


tranpro221 <-tranpro221[tranpro221$saon!="",]


tranpro221$add11ressf <-paste(tranpro221$postcode,tranpro221$paon,sep=", ")
tranpro221$add11ressf <-paste(tranpro221$add11ressf,tranpro221$saon,sep=", ")

add77$add11ressf <- paste(add77$postcodelocator,add77$pp,sep=", ")
add77$add11ressf <- paste(add77$add11ressf,add77$subbuildingname ,sep=", ")

bb2<- inner_join(tranpro221,add77,by="add11ressf")
head(bb2)

#bb2$method<-"method50"
bb2<-bb2[,..needlist12]
fb2<-uniqueresult(bb2)
dim(fb2)

bb2_left<-bb2[!(bb2$transactionid %in% fb2$transactionid),]
fb2$method<-"method72"


#################method 73 #################

tranpro221 <- matchleft(tranpro221,fb2)
dim(tranpro221)
# 89941    19
needb<-matchleft(needb,fb2)


needb<-needb[needb$saon!="",]
needb<-needb[needb$paostartsuffix!="",]


tranpro221$add11ressf <-paste(tranpro221$postcode,tranpro221$paon,sep=", ")
#tranpro221$add11ressf <-paste(tranpro221$add11ressf,tranpro221$saon,sep=", ")
tranpro221$add11ressf <-paste(tranpro221$add11ressf,tranpro221$saon,sep=", ")

add77$add11ressf <- paste(add77$postcodelocator,add77$pp,sep=", ")

add77$flatss <- paste("FLAT",add77$ss,sep=" ")

add77$add11ressf <- paste(add77$add11ressf,add77$flatss ,sep=", ")


bb3<- inner_join(tranpro221,add77,by="add11ressf")
head(bb3)

#bb3$method<-"method50"
bb3<-bb3[,..needlist12]
fb3<-uniqueresult(bb3)
dim(fb3)

bb3_left<-bb3[!(bb3$transactionid %in% fb3$transactionid),]
fb3$method<-"method73"
#################method 74 #################
tranpro221 <- matchleft(tranpro221,fb3)
dim(tranpro221)
# 88668    19
needb<-matchleft(needb,fb3)



tranpro221$add11ressf <-paste(tranpro221$postcode,tranpro221$paon,sep=", ")
#tranpro221$add11ressf <-paste(tranpro221$add11ressf,tranpro221$saon,sep=", ")
tranpro221$add11ressf <-paste(tranpro221$add11ressf,tranpro221$saon,sep=", ")

add77$add11ressf <- paste(add77$postcodelocator,add77$pp,sep=", ")
add77$ss<-str_trim(add77$ss)
add77$sspaotext <- paste(add77$ss,add77$paotext,sep=" ")
add77$add11ressf <- paste(add77$add11ressf,add77$sspaotext ,sep=", ")


bb4<- inner_join(tranpro221,add77,by="add11ressf")
head(bb4)

#bb4$method<-"method50"
bb4<-bb4[,..needlist12]
fb4<-uniqueresult(bb4)
dim(fb4)

bb4_left<-bb4[!(bb4$transactionid %in% fb4$transactionid),]
fb4$method<-"method74"
#################method 75 #################
tranpro221 <- matchleft(tranpro221,fb4)
dim(tranpro221)
# 88341    19
needb<-matchleft(needb,fb4)
needb<-needb[!grepl("FLAT",needb$saon),]
needb<-needb[!grepl("FLOOR",needb$saon),]

tranpro221$add11ressf <-paste(tranpro221$postcode,tranpro221$paon,sep=", ")
tranpro221$flatsaon <-paste("FLAT",tranpro221$saon,sep=" ")
tranpro221$add11ressf <-paste(tranpro221$add11ressf,tranpro221$flatsaon,sep=", ")

add77$add11ressf <- paste(add77$postcodelocator,add77$pp,sep=", ")
#add77$ss<-str_trim(add77$ss)
#add77$sspaotext <- paste(add77$ss,add77$paotext,sep=" ")
add77$add11ressf <- paste(add77$add11ressf,add77$saotext ,sep=", ")



bb5<- inner_join(tranpro221,add77,by="add11ressf")
head(bb5)

#bb5$method<-"method50"
bb5<-bb5[,..needlist12]
fb5<-uniqueresult(bb5)
dim(fb5)
#46

fb5$method<-"method75"
#################method 76 #################
tranpro221 <- matchleft(tranpro221,fb5)
dim(tranpro221)
#88295    20
needb<-matchleft(needb,fb5)



tranpro221$add11ressf <-paste(tranpro221$postcode,tranpro221$paon,sep=", ")
tranpro221$flatsaon <-paste("FLAT",tranpro221$saon,sep=" ")
tranpro221$add11ressf <-paste(tranpro221$add11ressf,tranpro221$flatsaon,sep=", ")

add77$add11ressf <- paste(add77$postcodelocator,add77$pp,sep=", ")
#add77$ss<-str_trim(add77$ss)
#add77$sspaotext <- paste(add77$ss,add77$paotext,sep=" ")
add77$add11ressf <- paste(add77$add11ressf,add77$subbuildingname ,sep=", ")
bb6<- inner_join(tranpro221,add77,by="add11ressf")
head(bb6)

#bb6$method<-"method60"
bb6<-bb6[,..needlist12]
fb6<-uniqueresult(bb6)
dim(fb6)

bb6_left<-bb6[!(bb6$transactionid %in% fb6$transactionid),]
fb6$method<-"method76"
#################method 77 #################
tranpro221 <- matchleft(tranpro221,fb6)
dim(tranpro221)
#88273    20
needb<-matchleft(needb,fb6)



tranpro221$add11ressf <-paste(tranpro221$postcode,tranpro221$paon,sep=", ")
#tranpro221$flatsaon <-paste("FLAT",tranpro221$saon,sep=" ")
tranpro221$add11ressf <-paste(tranpro221$add11ressf,tranpro221$saon,sep=", ")

add77$add11ressf <- paste(add77$postcodelocator,add77$pp,sep=", ")
add77$ss<-str_trim(add77$ss)
#add77$sspaotext <- paste(add77$ss,add77$paotext,sep=" ")
add77$add11ressf <- paste(add77$add11ressf,add77$ss ,sep=", ")
bb7<- inner_join(tranpro221,add77,by="add11ressf")
head(bb7)

#bb7$method<-"method70"
bb7<-bb7[,..needlist12]
fb7<-uniqueresult(bb7)
dim(fb7)

bb7_left<-bb7[!(bb7$transactionid %in% fb7$transactionid),]
fb7$method<-"method77"

#################method 78 #################
tranpro221 <- matchleft(tranpro221,fb7)
dim(tranpro221)
# 88271    20
needb<-matchleft(needb,fb7)



tranpro221$add11ressf <-paste(tranpro221$postcode,tranpro221$paon,sep=", ")
#tranpro221$flatsaon <-paste("FLAT",tranpro221$saon,sep=" ")
tranpro221$add11ressf <-paste(tranpro221$add11ressf,tranpro221$saon,sep=", ")

add77$add11ressf <- paste(add77$postcodelocator,add77$pp,sep=", ")
#add78$ss<-str_trim(add78$ss)
#add78$sspaotext <- paste(add78$ss,add78$paotext,sep=" ")
add78<-add77
#add78$saotextc <-gsub("FLAT","APARTMENT",add78$saotext)
add78$saotextc <-gsub("FLAT","UNIT",add78$saotext)


add78$add11ressf <- paste(add78$add11ressf,add78$saotextc ,sep=", ")
bb8<- inner_join(tranpro221,add78,by="add11ressf")
head(bb8)

#bb8$method<-"method80"
bb8<-bb8[,..needlist12]
fb8<-uniqueresult(bb8)
dim(fb8)

bb8_left<-bb8[!(bb8$transactionid %in% fb8$transactionid),]
fb8$method<-"method78"


#################method 79 #################
tranpro221 <- matchleft(tranpro221,fb8)
dim(tranpro221)
#88257    20
needb<-matchleft(needb,fb8)
tranpro221$add11ressf <-paste(tranpro221$postcode,tranpro221$paon,sep=", ")
#tranpro221$flatsaon <-paste("FLAT",tranpro221$saon,sep=" ")
tranpro221$add11ressf <-paste(tranpro221$add11ressf,tranpro221$saon,sep=", ")

add77$add11ressf <- paste(add77$postcodelocator,add77$pp,sep=", ")
#add78$ss<-str_trim(add78$ss)
#add78$sspaotext <- paste(add78$ss,add78$paotext,sep=" ")
add78<-add77
add78$saotextc <-gsub("FLAT","APARTMENT",add78$saotext)
#add78$saotextc <-gsub("FLAT","UNIT",add78$saotext)

add78$add11ressf <- paste(add78$add11ressf,add78$saotextc ,sep=", ")

bb9<- inner_join(tranpro221,add78,by="add11ressf")
head(bb9)

#bb9$method<-"method90"
bb9<-bb9[,..needlist12]
fb9<-uniqueresult(bb9)
dim(fb9)

#bb9_left<-bb9[!(bb9$transactionid %in% fb9$transactionid),]
fb9$method<-"method79"

#################method 80 #################
tranpro221 <- matchleft(tranpro221,fb9)
dim(tranpro221)
#88201 
needb<-matchleft(needb,fb9)

tranpro221$add11ressf <-paste(tranpro221$postcode,tranpro221$paon,sep=", ")
#tranpro221$flatsaon <-paste("FLAT",tranpro221$saon,sep=" ")
tranpro221$add11ressf <-paste(tranpro221$add11ressf,tranpro221$saon,sep=", ")


add77$add11ressf <- paste(add77$postcodelocator,add77$pp,sep=", ")
#add77$ss<-str_trim(add77$ss)
#add77$sspaotext <- paste(add77$ss,add77$paotext,sep=" ")
add77$add11ressf <- paste(add77$add11ressf,add77$paoendnumber ,sep=", ")

bb10<- inner_join(tranpro221,add77,by="add11ressf")
head(bb10)

#bb10$method<-"method100"
bb10<-bb10[,..needlist12]
fb10<-uniqueresult(bb10)
dim(fb10)

#bb10_left<-bb10[!(bb10$transactionid %in% fb10$transactionid),]
fb10$method<-"method80"
#################method 81 #################
tranpro221 <- matchleft(tranpro221,fb10)
dim(tranpro221)
#88195    20
needb<-matchleft(needb,fb10)

tranpro221$add11ressf <-paste(tranpro221$postcode,tranpro221$paon,sep=", ")
tranpro221_1<-tranpro221[!grepl("FLAT$",tranpro221$saon),]
tranpro221_1$saonl <-word(tranpro221_1$saon,-1)
tranpro221_1$add11ressf <-paste(tranpro221_1$add11ressf,tranpro221_1$saonl,sep=", ")


add77$add11ressf <- paste(add77$postcodelocator,add77$pp,sep=", ")
#add77$ss<-str_trim(add77$ss)
#add77$sspaotext <- paste(add77$ss,add77$paotext,sep=" ")
add77$add11ressf <- paste(add77$add11ressf,add77$subbuildingname ,sep=", ")

bb11<- inner_join(tranpro221_1,add77,by="add11ressf")
head(bb11)

#bb11$method<-"method110"
bb11<-bb11[,..needlist12]
fb11<-uniqueresult(bb11)
dim(fb11)

#bb11_left<-bb11[!(bb11$transactionid %in% fb11$transactionid),]
fb11$method<-"method81"
#################method 82 #################
tranpro221 <- matchleft(tranpro221,fb11)
dim(tranpro221)
# 88092    21
needb<-matchleft(needb,fb11)

tranpro221$add11ressf <-paste(tranpro221$postcode,tranpro221$paon,sep=", ")
#tranpro221$saonl <- word(tranpro221$saon,-1)
tranpro221$add11ressf <-paste(tranpro221$add11ressf,tranpro221$saon,sep=", ")


add77$add11ressf <- paste(add77$postcodelocator,add77$pp,sep=", ")
#add77$ss<-str_trim(add77$ss)
add77$subp <- paste(add77$subbuildingname,add77$paotext ,sep=" ")
add77$add11ressf <- paste(add77$add11ressf,add77$subp ,sep=", ")

bb12<- inner_join(tranpro221,add77,by="add11ressf")
head(bb12)

#bb12$method<-"method120"
bb12<-bb12[,..needlist12]
fb12<-uniqueresult(bb12)
dim(fb12)

bb12_left<-bb12[!(bb12$transactionid %in% fb12$transactionid),]
fb12$method<-"method82"
#################method 83 #################
tranpro221 <- matchleft(tranpro221,fb12)
dim(tranpro221)
# 88069    21
needb<-matchleft(needb,fb12)

tranpro221$add11ressf <-paste(tranpro221$postcode,tranpro221$paon,sep=", ")
tranpro221$saonl <- word(tranpro221$saon,-1)
tranpro221$add11ressf <-paste(tranpro221$add11ressf,tranpro221$saonl,sep=", ")


add77$add11ressf <- paste(add77$postcodelocator,add77$pp,sep=", ")
#add77$ss<-str_trim(add77$ss)
add77$subl <- word(add77$subbuildingname,-1)
add78<-add77[!grepl("FLAT$",add77$subbuildingname),]
add78$add11ressf <- paste(add78$add11ressf,add78$subl ,sep=", ")

bb13<- inner_join(tranpro221,add78,by="add11ressf")
head(bb13)

#bb13$method<-"method130"
bb13<-bb13[,..needlist12]
fb13<-uniqueresult(bb13)
dim(fb13)

#bb13_left<-bb13[!(bb13$transactionid %in% fb13$transactionid),]
fb13$method<-"method83"

#################stage 4 sum up #################
tranpro221 <- matchleft(tranpro221,fb13)
dim(tranpro221)
# 87803    21
needb<-matchleft(needb,fb13)


ly = list(fb1,fb2,fb3,fb4,fb5,fb6,fb7,fb8,fb9,fb10,fb11,fb12,fb13)
fb<- rbindlist(ly ,use.names=TRUE,fill=T)
dim(fb)
#5726
dim(fb1)[1]+dim(fb2)[1]+dim(fb3)[1]+dim(fb4)[1]+dim(fb5)[1]+dim(fb6)[1]+dim(fb7)[1]+dim(fb8)[1]+dim(fb9)[1]+
  dim(fb10)[1]+ dim(fb11)[1]+dim(fb12)[1]+dim(fb13)[1]
#5726

length(unique(fb$transactionid))
length(unique(fb$method))
#13
head(finalb)
finalb$method<-"method70"
stage4<-rbindlist(list(finalb,fb) ,use.names=TRUE,fill=T)
dim(stage4)
#56238
length(unique(stage4$transactionid))
#56238
length(unique(stage4$method))
#14
#sort(unique(y$method))

dbWriteTable(con, "stage4",value =stage4, append = TRUE, row.names = FALSE)

dbWriteTable(con, "finalb",value =finalb, append = TRUE, row.names = FALSE)
dbWriteTable(con, "fb",value =fb, append = TRUE, row.names = FALSE)

dbWriteTable(con, "tran_stage4_left",value =tranpro221, append = TRUE, row.names = FALSE)
rm(b)
rm(finalb)
rm(fb1,fb2,fb3,fb4,fb5,fb6,fb7,fb8,fb9,fb10,fb11,fb12,fb13)
rm(ly)
#rm(bb1,bb2,b3,bb4,bb5,bb6,bb7,bb8,bb9,bb10,bb12,bb13)
rm(needb)
rm(add77,add78)
rm(tranpro22,tranpro221)

#################stage 5 #################
tran66 <- matchleft(tran77,stage4)
dim(tran66)
#1980758      18
dim(tran66)[1]/dim(tran)[1]
rm(tran88)
add66 <-add[add$postcodelocator %in% tran66$postcode,]

head(link1)
tran66$addressf <-paste(tran66$postcode,tran66$paon,sep=", ")
add66 <- merge(add,link1,by="saotext")


add66$bb1<- paste(add66$paostartnumber,add66$string,sep="")

add66$addressf <- paste(add66$postcodelocator,add66$bb1,sep=", ")


e<- inner_join(tran66,add66,by="addressf")
dim(e)


finale <- uniqueresult(e)
dim(finale)
# 28724    52
neede <- doubleresult(e)
dim(neede)
#20
tranpro22 <- tranneed(tran66,neede)
dim(tranpro22)

finale$method<-"method90"
stage5<-finale
#28724
length(unique(stage5$transactionid))
#ique(stage5$transactionid))

length(unique(stage5$method))

dbWriteTable(con, "stage5",value =stage5, append = TRUE, row.names = FALSE)

dbWriteTable(con, "tran_stage5_left",value =tranpro22, append = TRUE, row.names = FALSE)
rm(neede)
rm(e)
rm(add66)
rm(finale)
#################stage 6 #################
tran55 <- matchleft(tran66,stage5)
dim(tran55)
#1997111      18
dim(tran55)[1]/dim(tran)[1]




tran55$addressf <-paste(tran55$postcode,tran55$paon,sep=", ")

dim(add)
add<-add[add$postset %in% tran55$postset,]
dim(add)
# 6004301  




#add$pp1 <-paste(add$paotext,add$paostartnumber,sep=", ") 
add$pp2 <-paste(add$paotext,add$pp,sep=", ")

add$addressf <- paste(add$postcodelocator,add$pp2,sep=", ")

r1<- inner_join(tran55,add,by="addressf")
r1<-r1[,..needlist1]
#r1<-r1[,c("transactionid","postcode","propertytype","paon","saon","street","addressf","postcodelocator","saostartnumber","saostartsuffix","saoendnumber","saoendsuffix","saotext","paostartnumber","paostartsuffix","paoendnumber","paoendsuffix","paotext","streetdescription","buildingname","buildingnumber","subbuildingname","ostopotoid","bb","ss","pp")]


dim(r1)


f1 <-uniqueresult(r1)
dim(f1)
#52853    30
#tran55<-matchleft(tran55,f1)
dim(tran55)


finalr <- f1


needr <- doubleresult(r1)


tranr <- tranneed(tran55,needr)
dim(tranr)
#68637    18

rm(f1)


finalr$method<-"method94"
#################method 95 #################


tranr$addressf <-paste(tranr$postcode,tranr$paon,sep=", ")
tranr$addressf <-paste(tranr$addressf,tranr$saon,sep=", ")
tranr_1<-tranr[tranr$saon!="",]
add$pp2 <-paste(add$paotext,add$pp,sep=", ") 

add$addressf <- paste(add$postcodelocator,add$pp2,sep=", ")
add$addressf <- paste(add$addressf,add$saotext,sep=", ")


rr1<- inner_join(tranr_1,add,by="addressf")
head(rr1)
needlist1<-c("transactionid","uprn","postcode.x","postcodelocator","postset.x", "postset.y","propertytype","paon","saon","street","locality.x","addressf","saostartnumber","saostartsuffix","saoendnumber","saoendsuffix","saotext","paostartnumber","paostartsuffix","paoendnumber","paoendsuffix","paotext","streetdescription","buildingname","buildingnumber","subbuildingname","bb","ss","pp")
#rr1$method<-"method50"
rr1<-rr1[,..needlist1]

fr1<-uniqueresult(rr1)
dim(fr1)

fr1$method<-"method95"
#################method 96 #################
tranr<- matchleft(tranr,fr1)
dim(tranr)
#39579  
needr<- matchleft(needr,fr1)
dim(needr)

tranr$addressf <-paste(tranr$postcode,tranr$paon,sep=", ")
tranr$addressf <-paste(tranr$addressf,tranr$saon,sep=", ")
tranr_1<-tranr[tranr$saon!="",]
add$pp2 <-paste(add$paotext,add$pp,sep=", ") 

add$addressf <- paste(add$postcodelocator,add$pp2,sep=", ")
add$addressf <- paste(add$addressf,add$ss,sep=", ")


rr2<- inner_join(tranr_1,add,by="addressf")
head(rr2)

rr2<-rr2[,..needlist1]
fr2<-uniqueresult(rr2)
dim(fr2)


fr2$method<-"method96"

#################method 97 #################
tranr<- matchleft(tranr,fr2)
dim(tranr)
#29300    18
tranr$addressf <-paste(tranr$postcode,tranr$paon,sep=", ")
tranr$addressf <-paste(tranr$addressf,tranr$saon,sep=", ")
tranr_1<-tranr[tranr$saon!="",]
add$pp2 <-paste(add$paotext,add$pp,sep=", ") 
add$flatss <- paste("FLAT ",add$ss,sep="")
add$addressf <- paste(add$postcodelocator,add$pp2,sep=", ")
add$addressf <- paste(add$addressf,add$flatss,sep=", ")



rr3<- inner_join(tranr_1,add,by="addressf")
head(rr3)

rr3<-rr3[,..needlist1]
fr3<-uniqueresult(rr3)
dim(fr3)
fr3$method<-"method97"






#################method 98 #################
tranr<- matchleft(tranr,fr3)
dim(tranr)
#18149    18
tranr$fsaon<-paste("FLAT ",tranr$saon,sep="")
tranr$addressf <-paste(tranr$postcode,tranr$paon,sep=", ")
tranr$addressf <-paste(tranr$addressf,tranr$fsaon,sep=", ")
tranr_1<-tranr[tranr$saon!="",]
add$pp2 <-paste(add$paotext,add$pp,sep=", ") 

add$addressf <- paste(add$postcodelocator,add$pp2,sep=", ")
add$addressf <- paste(add$addressf,add$saotext,sep=", ")



rr4<- inner_join(tranr_1,add,by="addressf")
head(rr4)

rr4<-rr4[,..needlist1]
fr4<-uniqueresult(rr4)
dim(fr4)
# 2950

fr4$method<-"method98"

#################method 99 #################
tranr<- matchleft(tranr,fr4)
dim(tranr)
#15199    19
tranr$addressf <-paste(tranr$postcode,tranr$paon,sep=", ")
tranr$addressf <-paste(tranr$addressf,tranr$saon,sep=", ")
tranr_1<-tranr[tranr$saon!="",]
add$pp2 <-paste(add$paotext,add$pp,sep=", ") 


add$unss <-paste("UNIT ",add$ss,sep="")
add$addressf <- paste(add$postcodelocator,add$pp2,sep=", ")
add$addressf <- paste(add$addressf,add$unss,sep=", ")

rr5<- inner_join(tranr_1,add,by="addressf")
head(rr5)

rr5<-rr5[,..needlist1]
fr5<-uniqueresult(rr5)
dim(fr5)

fr5$method<-"method99"



#################method 100 #################
tranr<- matchleft(tranr,fr5)
dim(tranr)
# 15136    19


tranr$addressf <-paste(tranr$postcode,tranr$paon,sep=", ")
tranr$addressf <-paste(tranr$addressf,tranr$saon,sep=", ")
tranr_1<-tranr[tranr$saon!="",]
add$pp2 <-paste(add$paotext,add$pp,sep=", ") 


#add$unss <-paste("UNIT ",add$ss,sep="")
add$addressf <- paste(add$postcodelocator,add$pp2,sep=", ")
add$addressf <- paste(add$addressf,add$subbuildingname,sep=", ")

rr6<- inner_join(tranr_1,add,by="addressf")
head(rr6)

rr6<-rr6[,..needlist1]
fr6<-uniqueresult(rr6)
dim(fr6)
fr6$method<-"method100"


#################method 101 #################
tranr<- matchleft(tranr,fr6)
dim(tranr)
#13718    19

tranr$paon1 <- gsub(",","",tranr$paon)
tranr$addressf <-paste(tranr$postcode,tranr$paon1,sep=", ")
tranr$addressf <-paste(tranr$addressf,tranr$saon,sep=", ")
tranr_1<-tranr[tranr$saon!="",]

add$addressf <- paste(add$postcodelocator,add$buildingname,sep=", ")
add$addressf <- paste(add$addressf,add$subbuildingname,sep=", ")



rr7<- inner_join(tranr_1,add,by="addressf")
head(rr7)

rr7<-rr7[,..needlist1]
fr7<-uniqueresult(rr7)
dim(fr7)
#66

fr7$method<-"method101"

#################method 102 #################
tranr<- matchleft(tranr,fr7)
dim(tranr)
#13652    20




data4 <- tranr[tranr$propertytype=="F",]
dim(data4)
# 733

data4$addressf <-paste(data4$postcode,data4$paon,sep=", ")
data4$addressf <-paste(data4$addressf,data4$saon,sep=", ")
data4<-data4[data4$saon!="",]

add$pp2 <-paste(add$paotext,add$pp,sep=", ")
add$ss1 <- paste(add$saostartsuffix,add$saostartnumber,sep="")

add$addressf <- paste(add$postcodelocator,add$pp2,sep=", ")
add$addressf <- paste(add$addressf,add$ss1,sep=", ")


rr8<- inner_join(data4,add,by="addressf")
head(rr8)

rr8<-rr8[,..needlist1]
fr8<-uniqueresult(rr8)
dim(fr8)

fr8$method<-"method102"
rm(data4)
add<-add[,..addlist1]
#################method 103 #################
tranr<- matchleft(tranr,fr8)
dim(tranr)
# 13642    20
# needr<-matchleft(needr,fr1)
# needr<-matchleft(needr,fr2)
# needr<-matchleft(needr,fr3)
# needr<-matchleft(needr,fr4)
# needr<-matchleft(needr,fr5)
# needr<-matchleft(needr,fr6)
# needr<-matchleft(needr,fr7)
# needr<-matchleft(needr,fr8)




tranr$addressf <-paste(tranr$postcode,tranr$paon,sep=", ")
tranr_1<-tranr
tranr_1$saonc<-gsub("[.]","",tranr_1$saon)
tranr_1$addressf <-paste(tranr_1$addressf,tranr_1$saonc,sep=", ")
tranr_1<-tranr_1[tranr_1$saon!="",]


add$pp2 <-paste(add$paotext,add$pp,sep=", ") 

add$addressf <- paste(add$postcodelocator,add$pp2,sep=", ")
add$addressf <- paste(add$addressf,add$saotext,sep=", ")




rr9<- inner_join(tranr_1,add,by="addressf")
dim(rr9)

rr9<-rr9[,..needlist1]
fr9<-uniqueresult(rr9)
dim(fr9)


fr9$method<-"method103"

#################method 104 #################
tranr<- matchleft(tranr,fr9)
dim(tranr)
rm(tranr_1)
#13437    20

needr<-matchleft(needr,fr9)

tranr$addressf <-paste(tranr$postcode,tranr$paon,sep=", ")
tranr_1<-tranr
tranr_1$saonc<-word(tranr_1$saon,-2,-1)
tranr_1$addressf <-paste(tranr_1$addressf,tranr_1$saonc,sep=", ")
tranr_1<-tranr_1[tranr_1$saon!="",]


add$pp2 <-paste(add$paotext,add$pp,sep=", ") 

add$addressf <- paste(add$postcodelocator,add$pp2,sep=", ")
add$addressf <- paste(add$addressf,add$saotext,sep=", ")


rr10<- inner_join(tranr_1,add,by="addressf")
head(rr10)

rr10<-rr10[,..needlist1]
fr10<-uniqueresult(rr10)
dim(fr10)
fr10$method<-"method104"

#################method 105 #################
tranr<- matchleft(tranr,fr10)
dim(tranr)

#13262    20


#add1$subbuildingnamenew <- gsub("APARTMENT","FLAT",add1$subbuildingnamenew)


tranr$addressf <-paste(tranr$postcode,tranr$paon,sep=", ")
tranr$addressf <-paste(tranr$addressf,tranr$saon,sep=", ")
tranr_1<-tranr[tranr$saon!="",]
add$pp2 <-paste(add$paotext,add$pp,sep=", ") 


#add$unss <-paste("UNIT ",add$ss,sep="")
add$addressf <- paste(add$postcodelocator,add$pp2,sep=", ")
add$saotextc<- gsub("FLAT","APARTMENT",add$saotext)
add$addressf <- paste(add$addressf,add$saotextc,sep=", ")



rr11<- inner_join(tranr_1,add,by="addressf")
head(rr11)

rr11<-rr11[,..needlist1]
fr11<-uniqueresult(rr11)
dim(fr11)
fr11$method<-"method105"

#################method 106 #################
tranr<- matchleft(tranr,fr11)
dim(tranr)
#11898    20

needr<-matchleft(needr,fr10)
needr<-matchleft(needr,fr11)

tranr$addressf <-paste(tranr$postcode,tranr$paon,sep=", ")
tranr$addressf <-paste(tranr$addressf,tranr$saon,sep=", ")
tranr_1<-tranr[tranr$saon!="",]
add$pp2 <-paste(add$paotext,add$pp,sep=", ") 


#add$unss <-paste("UNIT ",add$ss,sep="")
add$addressf <- paste(add$postcodelocator,add$pp2,sep=", ")
add$saotextc<- gsub("FLAT","UNIT",add$saotext)
add$addressf <- paste(add$addressf,add$saotextc,sep=", ")

rr12<- inner_join(tranr_1,add,by="addressf")
head(rr12)

rr12<-rr12[,..needlist1]
fr12<-uniqueresult(rr12)
dim(fr12)
fr12$method<-"method106"

#################method 107 #################

tranr<- matchleft(tranr,fr12)
dim(tranr)
#11434    20
rm(needb1)
#################stage 6 sum up #################

ly = list(fr1,fr2,fr3,fr4,fr5,fr6,fr7,fr8,fr9,fr10,fr11,fr12)
fr<- rbindlist(ly ,use.names=TRUE,fill=T)
dim(fr)
#57203
dim(fr1)[1]+dim(fr2)[1]+dim(fr3)[1]+dim(fr4)[1]+dim(fr5)[1]+dim(fr6)[1]+dim(fr7)[1]+dim(fr8)[1]+dim(fr9)[1]+
  dim(fr10)[1]+ dim(fr11)[1]+dim(fr12)[1]
#57203

length(unique(fr$transactionid))
length(unique(fr$method))
#12
#head(finalb)
#finalb$method<-"method70"
stage6<-rbindlist(list(finalr,fr) ,use.names=TRUE,fill=T)
dim(stage6)
#110056 
length(unique(stage6$transactionid))
#110056 
length(unique(stage6$method))
#12
dim(add)
add<-add[,..addlist1]
dim(add)
dbWriteTable(con, "stage6",value =stage6, append = TRUE, row.names = FALSE)

dbWriteTable(con, "finalr",value =finalr, append = TRUE, row.names = FALSE)
dbWriteTable(con, "fr",value =fr, append = TRUE, row.names = FALSE)

dbWriteTable(con, "tran_stage6_left",value =tranr, append = TRUE, row.names = FALSE)
rm(rr1,rr2,rr3,rr4,rr5,rr6,rr7,rr8,rr9,rr10,rr11,rr12)

rm(fr1,fr2,fr3,fr4,fr5,fr6,fr7,fr8,fr9,fr10,fr11,fr12)
rm(ly)
rm(tranr,finalr,fr)
rm(needr)
#################stage 7 #################
tran44 <- matchleft(tran55,stage6)
dim(tran44)
#1841978      18
rm(tran55,tran66,tran77,tran88)
dim(add)
#2703985       38
add<-add[add$postset %in% tran44$postset,]
dim(add)
#
rm(tranpro22,tranpro22_1,tranpro221_1)
tran44$addressf <-paste(tran44$postcode,tran44$paon,sep=", ")
add$addressf <- paste(add$postcodelocator,add$paotext,sep=", ")

m1<- inner_join(tran44,add,by="addressf")
m1<-m1[,..needlist1]
finalm1<-uniqueresult(m1)

tran44_1<-matchleft(tran44,finalm1)
dim(tran44_1)
rm(finalm1)
tran44_1$addressf <-paste(tran44_1$postcode,tran44_1$paon,sep=", ")
add$sspao <- paste(add$ss,add$paotext,sep=" ")
add$addressf <- paste(add$postcodelocator,add$sspao,sep=", ")

m2<- inner_join(tran44_1,add,by="addressf")
m2<-m2[,..needlist1]
m<-rbindlist(list(m1,m2) )
rm(tran44_1)
finalm <- uniqueresult(m)
dim(finalm)
# 101928     30
needm <- doubleresult(m)
dim(needm)
#13856371       30
finalm$method<-"method110"
tranm <- tranneed(tran44,needm)
rm(m1,m2)
#################method 111 #################

tranm$addressf <-paste(tranm$postcode,tranm$paon,sep=", ")
tranm$addressf <-paste(tranm$addressf,tranm$saon,sep=", ")
tranm_1<-tranm[tranm$saon!="",]
add$addressf <- paste(add$postcodelocator,add$paotext,sep=", ")
add$addressf <- paste(add$addressf,add$ss,sep=", ")

mm1<- inner_join(tranm_1,add,by="addressf")



head(mm1)

mm1<-mm1[,..needlist1]
fm1<-uniqueresult(mm1)
dim(fm1)
fm1$method<-"method111"


#################method 112 #################

tranm <- matchleft(tranm,fm1)
dim(tranm)
#119845     18
tranm$flats <- paste("FLAT ",tranm$saon,sep="")

tranm$addressf <-paste(tranm$postcode,tranm$paon,sep=", ")
tranm$addressf <-paste(tranm$addressf,tranm$flats,sep=", ")
add$addressf <- paste(add$postcodelocator,add$paotext,sep=", ")
add$addressf <- paste(add$addressf,add$saotext,sep=", ")



tranm_1<-tranm[tranm$saon!="",]
mm2<- inner_join(tranm_1,add,by="addressf")



head(mm2)

mm2<-mm2[,..needlist1]
fm2<-uniqueresult(mm2)

fm2$method<-"method112"

#################method 113 #################
tranm <- matchleft(tranm,fm2)
dim(tranm)
#90616    19
tranm$addressf <-paste(tranm$postcode,tranm$paon,sep=", ")
tranm$addressf <-paste(tranm$addressf,tranm$saon,sep=", ")

add$flatsao <- paste("FLAT ",add$ss,sep="")
add$addressf <- paste(add$postcodelocator,add$paotext,sep=", ")
add$addressf <- paste(add$addressf,add$flatsao,sep=", ")


tranm_1<-tranm[tranm$saon!="",]
mm3<- inner_join(tranm_1,add,by="addressf")





mm3<-mm3[,..needlist1]
fm3<-uniqueresult(mm3)
dim(fm3)
fm3$method<-"method113"

#################method 114 #################
tranm <- matchleft(tranm,fm3)
dim(tranm)
#66322    19
tranm$addressf <-paste(tranm$postcode,tranm$paon,sep=", ")
tranm$addressf <-paste(tranm$addressf,tranm$saon,sep=", ")

add$addressf <- paste(add$postcodelocator,add$paotext,sep=", ")
add$addressf <- paste(add$addressf,add$saotext,sep=", ")

tranm_1<-tranm[tranm$saon!="",]
mm4<- inner_join(tranm_1,add,by="addressf")



head(mm4)

mm4<-mm4[,..needlist1]
fm4<-uniqueresult(mm4)
dim(fm4)
fm4$method<-"method114"

#################method 115 #################
tranm <- matchleft(tranm,fm4)
dim(tranm)
#47355    19
tranm$addressf <-paste(tranm$postcode,tranm$paon,sep=", ")
tranm$addressf <-paste(tranm$addressf,tranm$saon,sep=", ")


add$addressf <- paste(add$postcodelocator,add$paotext,sep=", ")
add$addressf <- paste(add$addressf,add$pp, sep=", ")

tranm_1<-tranm[tranm$saon!="",]
mm5<- inner_join(tranm_1,add,by="addressf")



head(mm5)

mm5<-mm5[,..needlist1]
fm5<-uniqueresult(mm5)
dim(fm5)
fm5$method<-"method115"

#################method 116 #################
tranm <- matchleft(tranm,fm5)
dim(tranm)

tranm$addressf <-paste(tranm$postcode,tranm$paon,sep=", ")
tranm$addressf <-paste(tranm$addressf,tranm$saon,sep=", ")

add$suss <- paste(add$subbuildingname,add$ss,sep=" ")
add$addressf <- paste(add$postcodelocator,add$paotext,sep=", ")
add$addressf <- paste(add$addressf,add$suss , sep=", ")
tranm_1<-tranm[tranm$saon!="",]
mm6<- inner_join(tranm_1,add,by="addressf")



head(mm6)

mm6<-mm6[,..needlist1]
fm6<-uniqueresult(mm6)
dim(fm6)
fm6$method<-"method116"

#################method 117 #################
tranm <- matchleft(tranm,fm6)
dim(tranm)

tranm$sapa <- paste(tranm$saon,tranm$paon,sep=" ")
tranm$addressf <-paste(tranm$postcode,tranm$paon,sep=", ")
tranm$addressf <-paste(tranm$addressf,tranm$sapa,sep=", ")


add$saobuil <- paste(add$saotext,add$buildingname,sep=" ")
add$addressf <- paste(add$postcodelocator,add$paotext,sep=", ")
add$addressf <- paste(add$addressf,add$saobuil , sep=", ")

tranm_1<-tranm[tranm$saon!="",]
mm7<- inner_join(tranm_1,add,by="addressf")



head(mm7)

mm7<-mm7[,..needlist1]
fm7<-uniqueresult(mm7)
dim(fm7)
fm7$method<-"method117"

#################method 118 #################
tranm <- matchleft(tranm,fm7)
dim(tranm)
# 45194    20

needm<-matchleft(needm,fm1)
needm<-matchleft(needm,fm2)
needm<-matchleft(needm,fm3)
needm<-matchleft(needm,fm4)
needm<-matchleft(needm,fm5)
needm<-matchleft(needm,fm6)
needm<-matchleft(needm,fm7)

needm<-needm[needm$saon!="",]


tranm$addressf <-paste(tranm$postcode,tranm$paon,sep=", ")
tranm_1<-tranm[tranm$saon!="",]
tranm_1<-tranm_1[grepl("\\d$",tranm_1$saon),]
tranm_1$saonlast<-word(tranm_1$saon,-1)
tranm_1$addressf <-paste(tranm_1$addressf,tranm_1$saonlast,sep=", ")



add$addressf <- paste(add$postcodelocator,add$paotext,sep=", ")
add$addressf <- paste(add$addressf,add$ss , sep=", ")


# 
# tranm_1<-tranm[tranm$saon!="",]
# tranm_1<-tranm_1[grepl("\\d$",tranm_1$saon),]

mm8<- inner_join(tranm_1,add,by="addressf")



head(mm8)

mm8<-mm8[,..needlist1]
fm8<-uniqueresult(mm8)
dim(fm8)

fm8$method<-"method118"


#################method 119 #################
tranm <- matchleft(tranm,fm8)
dim(tranm)
#38782    20
needm<-matchleft(needm,fm8)


tranm$units <- paste("UNIT ",tranm$saon,sep="")

tranm$addressf <-paste(tranm$postcode,tranm$paon,sep=", ")
tranm$addressf <-paste(tranm$addressf,tranm$units,sep=", ")
add$addressf <- paste(add$postcodelocator,add$paotext,sep=", ")
add$addressf <- paste(add$addressf,add$saotext,sep=", ")



tranm_1<-tranm[tranm$saon!="",]
mm9<- inner_join(tranm_1,add,by="addressf")



head(mm9)

mm9<-mm9[,..needlist1]
fm9<-uniqueresult(mm9)
dim(fm9)
fm9$method<-"method119"
#################method 120 #################
tranm <- matchleft(tranm,fm9)
dim(tranm)
#38570    21
needm<-matchleft(needm,fm9)


tranm$addressf <-paste(tranm$postcode,tranm$paon,sep=", ")
tranm_1<-tranm[tranm$saon!="",]
#tranm_1<-tranm_1[grepl("\\d$",tranm_1$saon),]
tranm_1<-tranm_1[str_count(tranm_1$saon,"\\W+")>=2,]
tranm_1$saontwo<-word(tranm_1$saon,1,2)
tranm_1$addressf <-paste(tranm_1$addressf,tranm_1$saontwo,sep=", ")



add$addressf <- paste(add$postcodelocator,add$paotext,sep=", ")
add_1<-add[str_count(add$saotext,"\\W+")>=2,]
add_1$saotexttwo <- word(add_1$saotext,1,2)
add_1$addressf <- paste(add_1$addressf,add_1$saotexttwo , sep=", ")


#tranm_1<-tranm[tranm$saon!="",]
mm10<- inner_join(tranm_1,add_1,by="addressf")

rm(add_1)
rm(tranm_1)
head(mm10)

mm10<-mm10[,..needlist1]
fm10<-uniqueresult(mm10)
dim(fm10)
fm10$method<-"method120"


#################method 121 #################
tranm <- matchleft(tranm,fm10)
dim(tranm)
#38434    21
needm<-matchleft(needm,fm10)


tranm$addressf <-paste(tranm$postcode,tranm$paon,sep=", ")
tranm_1<-tranm[tranm$saon!="",]

tranm_1$saonc<-gsub("[.]","",tranm_1$saon)
tranm_1$addressf <-paste(tranm_1$addressf,tranm_1$saonc,sep=", ")

add$addressf <- paste(add$postcodelocator,add$paotext,sep=", ")
add$addressf <- paste(add$addressf,add$ss,sep=", ")



#tranm_1<-tranm[tranm$saon!="",]
mm11<- inner_join(tranm_1,add,by="addressf")


mm11<-mm11[,..needlist1]
fm11<-uniqueresult(mm11)
dim(fm11)
fm11$method<-"method121"

#################method 122 #################
tranm <- matchleft(tranm,fm11)
dim(tranm)
# 38422    21
needm<-matchleft(needm,fm11)


tranm$addressf <-paste(tranm$postcode,tranm$paon,sep=", ")
tranm$addressf <-paste(tranm$addressf,tranm$saon,sep=", ")

add$addressf <- paste(add$postcodelocator,add$paotext,sep=", ")
add$saotextc<-gsub("APARTMENT","FLAT",add$saotext)

add$addressf <- paste(add$addressf,add$saotextc,sep=", ")

tranm_1<-tranm[tranm$saon!="",]
mm12<- inner_join(tranm_1,add,by="addressf")





mm12<-mm12[,..needlist1]
fm12<-uniqueresult(mm12)
dim(mm12)
fm12$method<-"method122"
#rm(add1)
#################method 123 #################
tranm <- matchleft(tranm,fm12)
dim(tranm)
#37990    21
needm<-matchleft(needm,fm12)

tranm$aps <- paste("APARTMENT ",tranm$saon,sep="")

tranm$addressf <-paste(tranm$postcode,tranm$paon,sep=", ")
tranm$addressf <-paste(tranm$addressf,tranm$aps,sep=", ")
add$addressf <- paste(add$postcodelocator,add$paotext,sep=", ")
add$addressf <- paste(add$addressf,add$saotext,sep=", ")


tranm_1<-tranm[tranm$saon!="",]
mm13<- inner_join(tranm_1,add,by="addressf")



head(mm13)

mm13<-mm13[,..needlist1]
fm13<-uniqueresult(mm13)
fm13$method<-"method123"
#################method 124 #################
tranm <- matchleft(tranm,fm13)
dim(tranm)
#35009    22
rm(mm1,mm2,mm3,mm4,mm5,mm6,mm7,mm8,mm9,mm10,mm11,mm12,mm13)
needm<-matchleft(needm,fm13)

tranm$addressf <-paste(tranm$postcode,tranm$paon,sep=", ")
tranm$addressf <-paste(tranm$addressf,tranm$saon,sep=", ")
add$addressf <- paste(add$postcodelocator,add$paotext,sep=", ")
add$ss1<-paste(add$saostartnumber,add$saoendnumber,sep="-")
add$addressf <- paste(add$addressf,add$ss1,sep=", ")


tranm_1<-tranm[tranm$saon!="",]
mm14<- inner_join(tranm_1,add,by="addressf")
rm(tranm_1)


head(mm14)

mm14<-mm14[,..needlist1]
fm14<-uniqueresult(mm14)

fm14$method<-"method124"

#################method 125 #################
tranm <- matchleft(tranm,fm14)
dim(tranm)
#34809    22
needm<-matchleft(needm,fm13)
needm<-matchleft(needm,fm14)

dim(add)
add<-add[,..addlist1]
dim(add)
##stage 7 sum up###########################

ly = list(fm1,fm2,fm3,fm4,fm5,fm6,fm7,fm8,fm9,fm10,fm11,fm12,fm13,fm14)
fm<- rbindlist(ly ,use.names=TRUE,fill=T)
dim(fm)
#534614
dim(fm1)[1]+dim(fm2)[1]+dim(fm3)[1]+dim(fm4)[1]+dim(fm5)[1]+dim(fm6)[1]+dim(fm7)[1]+dim(fm8)[1]+dim(fm9)[1]+
  dim(fm10)[1]+ dim(fm11)[1]+dim(fm12)[1]+ dim(fm13)[1]+dim(fm14)[1]
#534614

length(unique(fm$transactionid))
length(unique(fm$method))
#12

stage7<-rbindlist(list(finalm,fm) ,use.names=TRUE,fill=T)
dim(stage7)
#636542
length(unique(stage7$transactionid))
#636542
length(unique(stage7$method))
#15


dbWriteTable(con, "stage7",value =stage7, append = TRUE, row.names = FALSE)

dbWriteTable(con, "finalm",value =finalm, append = TRUE, row.names = FALSE)
dbWriteTable(con, "fm",value =fm, append = TRUE, row.names = FALSE)

dbWriteTable(con, "tran_stage7_left",value =tranm, append = TRUE, row.names = FALSE)


rm(fm1,fm2,fm3,fm4,fm5,fm6,fm7,fm8,fm9,fm10,fm11,fm12,fm13,fm14)
rm(ly)
rm(finalm,fm)
rm(needm)
rm(m,mm14)
#################stage 8 #################
tran33 <- matchleft(tran44,stage7)
dim(tran33)
#1250513 

dim(tran33)[1]/dim(tran)[1]

tran33$paon1 <- gsub(" - ","-",tran33$paon )
tran33$paon1 <- gsub(", "," ",tran33$paon1 )

tran33$addressf <-paste(tran33$postcode,tran33$paon1,sep=", ")
add$addressf <- paste(add$postcodelocator,add$buildingname,sep=", ")

ll1<- inner_join(tran33,add,by="addressf")

ll1<-ll1[,..needlist1]

f1<-uniqueresult(ll1)
dim(f1)
#50126
######PAON1 is equal to pp4
tran33_1 <- matchleft(tran33,f1)
dim(tran33_1)

tran33_1$paon1 <- gsub(" - ","-",tran33_1$paon )
tran33_1$paon1 <- gsub(", "," ",tran33_1$paon1 )

tran33_1$addressf <-paste(tran33_1$postcode,tran33_1$paon1,sep=", ")

add$pp4 <-paste(add$paostartnumber,add$paoendnumber,sep="-") 

add$addressf <- paste(add$postcodelocator,add$pp4,sep=", ")
ll2<- inner_join(tran33_1,add,by="addressf")
ll2<-ll2[,..needlist1]

f2<-uniqueresult(ll2)
head(f2[f2$transactionid %in% f1$transactionid,])
head(f1[f1$transactionid %in% f2$transactionid,])
# ff<-rbindlist(list(f1,f2) )
# ff<-unique(ff)
# dim(ff)
# dim(f1)[1]+dim(f2)[1]
# ff[!(ff$transactionid %in% finalll$transactionid),]
# rm(ff)

rm(tran33_1)
ll<-rbindlist(list(ll1,ll2) )
ll<-unique(ll)

finalll <- uniqueresult(ll)
dim(finalll)

# 27594    30
needl <- doubleresult(ll)
dim(needl)
finalll$method<-"method139"
tranl<- tranneed(tran33,needl)


rm(tran44)
rm(ll1,ll2)
#################method 140 #################
tranl$paon1 <- gsub(" - ","-",tranl$paon )
tranl$paon1 <- gsub(", "," ",tranl$paon1 )

tranl$addressf <-paste(tranl$postcode,tranl$paon1,sep=", ")
tranl$addressf <-paste(tranl$addressf,tranl$saon,sep=", ")
tranl_1<-tranl[tranl$saon!="",]

add$addressf <- paste(add$postcodelocator,add$buildingname,sep=", ")
add$addressf <- paste(add$addressf,add$subbuildingname,sep=", ")

l1<- inner_join(tranl_1,add,by="addressf")
l1<-l1[,..needlist1]

fl1<-uniqueresult(l1)
dim(fl1)
fl1$method<-"method140"

#################method 141 #################
tranl <- matchleft(tranl,fl1)
dim(tranl)
#35639    19
tranl$addressf <-paste(tranl$postcode,tranl$paon1,sep=", ")
tranl$addressf <-paste(tranl$addressf,tranl$saon,sep=", ")
tranl_1<-tranl[tranl$saon!="",]

add$addressf <- paste(add$postcodelocator,add$buildingname,sep=", ")
add$addressf <- paste(add$addressf,add$saotext,sep=", ")


l2<- inner_join(tranl_1,add,by="addressf")
l2<-l2[,..needlist1]
rm(tranl_1)
fl2<-uniqueresult(l2)
dim(fl2)

fl2$method<-"method141"


#################method 142 #################
tranl <- matchleft(tranl,fl2)
dim(tranl)
#35205    19
tranl$paon2 <- gsub(" - ","-",tranl$paon )
tranl$addressf <-paste(tranl$postcode,tranl$paon2,sep=", ")
temp1<- tranl[tranl$saon=="",]

add$pp4 <-paste(add$paostartnumber,add$paoendnumber,sep="-")
add$addressf <- paste(add$postcodelocator,add$pp4,sep=", ")

l3<- inner_join(temp1,add,by="addressf")
l3<-l3[,..needlist1]
rm(temp1)
fl3<-uniqueresult(l3)
dim(fl3)
fl3$method<-"method142"

#################method 143 #################
tranl <- matchleft(tranl,fl3)
dim(tranl)


tranl$paon1 <- gsub(" - ","-",tranl$paon )
tranl$paon1 <- gsub(", "," ",tranl$paon1 )
tranl$addressf <-paste(tranl$postcode,tranl$paon1,sep=", ")
tranl$addressf <-paste(tranl$addressf,tranl$saon,sep=", ")


add$pp1 <-paste(add$paostartnumber,add$paoendnumber,sep="-") 
add$ppp <-paste(add$paotext,add$pp1,sep=" ") 

add$addressf <- paste(add$postcodelocator,add$ppp,sep=", ")
add$addressf <- paste(add$addressf,add$ss,sep=", ")

tranl_1<-tranl[tranl$saon!="",]
l4<- inner_join(tranl_1,add,by="addressf")
l4<-l4[,..needlist1]
rm(tranl_1)
fl4<-uniqueresult(l4)
dim(fl4)


fl4$method<-"method143"

#################method 144 #################
tranl <- matchleft(tranl,fl4)
dim(tranl)

tranl$paon1 <- gsub(" - ","-",tranl$paon )
tranl$paon1 <- gsub(", "," ",tranl$paon1 )
tranl$addressf <-paste(tranl$postcode,tranl$paon1,sep=", ")
tranl$addressf <-paste(tranl$addressf,tranl$saon,sep=", ")


add$pp1 <-paste(add$paostartnumber,add$paoendnumber,sep="-") 
add$ppp <-paste(add$paotext,add$pp1,sep=" ") 

add$addressf <- paste(add$postcodelocator,add$ppp,sep=", ")
add$fss <- paste("FLAT ",add$ss,sep="")
add$addressf <- paste(add$addressf,add$fss,sep=", ")

tranl_1<-tranl[tranl$saon!="",]

l5<- inner_join(tranl_1,add,by="addressf")
l5<-l5[,..needlist1]
rm(tranl_1)
fl5<-uniqueresult(l5)
dim(fl5)
fl5$method<-"method144"


#################method 145 #################
tranl <- matchleft(tranl,fl5)
dim(tranl)
#33856    20


tranl$paon1 <- gsub(" - ","-",tranl$paon )
tranl$paon1 <- gsub(", "," ",tranl$paon1 )
tranl$addressf <-paste(tranl$postcode,tranl$paon1,sep=", ")
tranl$addressf <-paste(tranl$addressf,tranl$saon,sep=", ")

add$pp1 <-paste(add$paostartnumber,add$paoendnumber,sep="-") 
add$ppp <-paste(add$paotext,add$pp1,sep=" ") 

add$addressf <- paste(add$postcodelocator,add$ppp,sep=", ")
add$addressf <- paste(add$addressf,add$saotext,sep=", ")


tranl_1<-tranl[tranl$saon!="",]
l6<- inner_join(tranl_1,add,by="addressf")
l6<-l6[,..needlist1]
rm(tranl_1)
fl6<-uniqueresult(l6)
dim(fl6)
fl6$method<-"method145"




#################method 146 #################
tranl <- matchleft(tranl,fl6)
dim(tranl)


tranl$fs <- paste("FLAT ",tranl$saon,sep="") 

tranl$paon1 <- gsub(" - ","-",tranl$paon )
tranl$paon1 <- gsub(", "," ",tranl$paon1 )
tranl$addressf <-paste(tranl$postcode,tranl$paon1,sep=", ")
tranl$addressf <-paste(tranl$addressf,tranl$fs ,sep=", ")

add$addressf <- paste(add$postcodelocator,add$buildingname,sep=", ")
add$addressf <- paste(add$addressf,add$subbuildingname,sep=", ")
tranl_1<-tranl[tranl$saon!="",]

l7<- inner_join(tranl_1,add,by="addressf")
l7<-l7[,..needlist1]
rm(tranl_1)
fl7<-uniqueresult(l7)
dim(fl7)
fl7$method<-"method146"

#################method 147 #################
tranl <- matchleft(tranl,fl7)
dim(tranl)
# 32751 
tranl$paon1 <- gsub(" - ","-",tranl$paon )
tranl$paon1 <- gsub(", "," ",tranl$paon1 )
tranl$addressf <-paste(tranl$postcode,tranl$paon1,sep=", ")
tranl$addressf <-paste(tranl$addressf,tranl$saon ,sep=", ")


add$addressf <- paste(add$postcodelocator,add$buildingname,sep=", ")
add$fsub <- paste("FLAT ",add$subbuildingname,sep="")
add$addressf <- paste(add$addressf,add$fsub,sep=", ")
tranl_1<-tranl[tranl$saon!="",]
l8<- inner_join(tranl_1,add,by="addressf")
l8<-l8[,..needlist1]
rm(tranl_1)
fl8<-uniqueresult(l8)
dim(fl8)
fl8$method<-"method147"


#################method 148 #################
tranl <- matchleft(tranl,fl8)
dim(tranl)

tranl$paon1 <- gsub(" - ","-",tranl$paon )
tranl$paon1 <- gsub(", "," ",tranl$paon1 )
tranl$addressf <-paste(tranl$postcode,tranl$paon1,sep=", ")
tranl$addressf <-paste(tranl$addressf,tranl$saon ,sep=", ")

add$addressf <- paste(add$postcodelocator,add$buildingname,sep=", ")
add$addressf <- paste(add$addressf,add$ss,sep=", ")


tranl_1<-tranl[tranl$saon!="",]
l9<- inner_join(tranl_1,add,by="addressf")
l9<-l9[,..needlist1]
rm(tranl_1)
fl9<-uniqueresult(l9)
dim(fl9)

fl9$method<-"method148"

#################method 149 #################

tranl <- matchleft(tranl,fl9)
dim(tranl)
# 32656    21

tranl$paon2 <- gsub(" - ","-",tranl$paon )
tranl$addressf <-paste(tranl$postcode,tranl$paon2,sep=", ")
tranl$addressf <-paste(tranl$addressf,tranl$saon ,sep=", ")

add$pp1 <-paste(add$paostartnumber,add$paoendnumber,sep="-") 
add$addressf <- paste(add$postcodelocator,add$pp1,sep=", ")
add$addressf <- paste(add$addressf,add$saotext,sep=", ")
l10<- inner_join(tranl,add,by="addressf")

tranl_1<-tranl[tranl$saon!="",]
l10<- inner_join(tranl_1,add,by="addressf")
l10<-l10[,..needlist1]
rm(tranl_1)
fl10<-uniqueresult(l10)
dim(fl10)

fl10$method<-"method149"


#################method 150 #################

tranl <- matchleft(tranl,fl10)
dim(tranl)


tranl$paon2 <- gsub(" - ","-",tranl$paon )
tranl$addressf <-paste(tranl$postcode,tranl$paon2,sep=", ")
tranl$fs <- paste("FLAT ",tranl$saon,sep="")
tranl$addressf <-paste(tranl$addressf,tranl$fs ,sep=", ")

add$pp1 <-paste(add$paostartnumber,add$paoendnumber,sep="-") 
add$addressf <- paste(add$postcodelocator,add$pp1,sep=", ")
add$addressf <- paste(add$addressf,add$saotext,sep=", ")


tranl_1<-tranl[tranl$saon!="",]

l11<- inner_join(tranl_1,add,by="addressf")
l11<-l11[,..needlist1]
rm(tranl_1)
fl11<-uniqueresult(l11)
dim(fl11)
fl11$method<-"method150"

#################method 151 #################

tranl <- matchleft(tranl,fl11)
dim(tranl)


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
dim(fl12)
rm(tranl_1)
fl12$method<-"method151"


#################method 152 #################

tranl <- matchleft(tranl,fl12)
dim(tranl)
# 24540    21


tranl$paon2 <- gsub(" - ","-",tranl$paon )
tranl$addressf <-paste(tranl$postcode,tranl$paon2,sep=", ")
tranl$addressf <-paste(tranl$addressf,tranl$saon ,sep=", ")

add$pp1 <-paste(add$paostartnumber,add$paoendnumber,sep="-") 
add$addressf <- paste(add$postcodelocator,add$pp1,sep=", ")
add$sp <-  paste(add$saostartnumber,add$paotext,sep=" ")
add$ssp <- paste(add$saotext,add$sp,sep=", ")
add$addressf <- paste(add$addressf,add$ssp,sep=", ")
tranl_1<-tranl[tranl$saon!="",]
l13<- inner_join(tranl_1,add,by="addressf")
l13<-l13[,..needlist1]
rm(tranl_1)
fl13<-uniqueresult(l13)
dim(fl13)

fl13$method<-"method152"

#################method 153 #################

tranl <- matchleft(tranl,fl13)
dim(tranl)
#24533    21
dim(needl)
needl<-matchleft(needl,fl1)
needl<-matchleft(needl,fl2)
needl<-matchleft(needl,fl3)
needl<-matchleft(needl,fl4)
needl<-matchleft(needl,fl5)
needl<-matchleft(needl,fl6)
needl<-matchleft(needl,fl7)

needl<-matchleft(needl,fl8)
needl<-matchleft(needl,fl9)
needl<-matchleft(needl,fl10)
needl<-matchleft(needl,fl11)
needl<-matchleft(needl,fl12)
needl<-matchleft(needl,fl13)
dim(needl)


tranl$paon1 <- gsub(" - ","-",tranl$paon )
tranl$paon1 <- gsub(", "," ",tranl$paon1 )

tranl$addressf <-paste(tranl$postcode,tranl$paon1,sep=", ")

tranl_1<-tranl[grepl("\\d$",tranl$saon),]
tranl_1<-tranl_1[str_count(tranl_1$saon,"\\W+")>=2,]
tranl_1$saonl<-word(tranl_1$saon,-1)
tranl_1$addressf <-paste(tranl_1$addressf,tranl_1$saonl,sep=", ")
#tranl_1<-tranl[tranl$saon!="",]

add$addressf <- paste(add$postcodelocator,add$buildingname,sep=", ")
add$saotextc <- word(add$saotext,-1)
add$addressf <- paste(add$addressf,add$saotextc,sep=", ")

# 


#tranl_1<-tranl[tranl$saon!="",]

l14<- inner_join(tranl_1,add,by="addressf")
l14<-l14[,..needlist1]

fl14<-uniqueresult(l14)
dim(fl14)

fl14$method<-"method153"

#################method 154 #################

tranl <- matchleft(tranl,fl14)
dim(tranl)

tranl$paon1 <- gsub(" - ","-",tranl$paon )
tranl$paon1 <- gsub(", "," ",tranl$paon1 )

tranl$addressf <-paste(tranl$postcode,tranl$paon1,sep=", ")

tranl_1<-tranl[grepl("^\\d",tranl$saon),]
tranl_1<-tranl_1[str_count(tranl_1$saon,"\\W+")>=1,]
tranl_1$saonf<-word(tranl_1$saon,1)
tranl_1$addressf <-paste(tranl_1$addressf,tranl_1$saonf,sep=", ")
#tranl_1<-tranl[tranl$saon!="",]

add$addressf <- paste(add$postcodelocator,add$buildingname,sep=", ")
#add$saotextf <- word(add$saotext,1)
add$addressf <- paste(add$addressf,add$ss,sep=", ")


l15<- inner_join(tranl_1,add,by="addressf")
l15<-l15[,..needlist1]

fl15<-uniqueresult(l15)
dim(fl15)


fl15$method<-"method154"


#################method 155 #################
tranl <- matchleft(tranl,fl15)
dim(tranl)
#24061    21


needl<-matchleft(needl,fl14)
needl<-matchleft(needl,fl15)
needl<-needl[needl$saon!="",]
dim(needl)
# tranl$paon2 <- gsub(" - ","-",tranl$paon )
# tranl$addressf <-paste(tranl$postcode,tranl$paon2,sep=", ")

tranl$addressf <-paste(tranl$postcode,tranl$paon,sep=", ")

tranl_1<-tranl[grepl("\\d$",tranl$saon),]
#tranl_1<-tranl_1[str_count(tranl_1$saon,"\\W+")>=2,]
tranl_1$saonl<-word(tranl_1$saon,-1)
tranl_1$addressf <-paste(tranl_1$addressf,tranl_1$saonl,sep=", ")
#tranl_1<-tranl[tranl$saon!="",]

add$addressf <- paste(add$postcodelocator,add$buildingname,sep=", ")
add$saotextc <- word(add$saotext,-1)
add$addressf <- paste(add$addressf,add$saotextc,sep=", ")



l16<- inner_join(tranl_1,add,by="addressf")
l16<-l16[,..needlist1]

fl16<-uniqueresult(l16)
dim(fl16)




fl16$method<-"method155"


#################method 156 #################
tranl <- matchleft(tranl,fl16)
dim(tranl)
needl<-matchleft(needl,fl16)

tranl$addressf <-paste(tranl$postcode,tranl$paon,sep=", ")

#tranl_1<-tranl[grepl("\\d$",tranl$saon),]
tranl_1<-tranl[str_count(tranl$saon,"\\W+")>=2,]
tranl_1$saontwo<-word(tranl_1$saon,1,2)
tranl_1$addressf <-paste(tranl_1$addressf,tranl_1$saontwo,sep=", ")
#tranl_1<-tranl[tranl$saon!="",]




add$addressf <- paste(add$postcodelocator,add$buildingname,sep=", ")
add$saotexttwo <- word(add$saotext,1,2)
add$addressf <- paste(add$addressf,add$saotexttwo,sep=", ")



l17<- inner_join(tranl_1,add,by="addressf")
l17<-l17[,..needlist1]
rm(tranl_1)
fl17<-uniqueresult(l17)
dim(fl17)
fl17$method<-"method156"
head(add)
class(add)
dim(add)
#add[, c("fsub","pp4","ssp","sp","suss","saobuil","unss","flatsao"):=NULL]
#add[, c("pp1","pp2","flatss","ss1","saotextc","sspao","saotexttwo","ppp"):=NULL]

#################method 157 #################

tranl <- matchleft(tranl,fl17)
dim(tranl)
#23539    21
needl<-matchleft(needl,fl17)

# 
tranl$paon2 <- gsub(" - ","-",tranl$paon )
tranl$addressf <-paste(tranl$postcode,tranl$paon2,sep=", ")

#tranl$addressf <-paste(tranl$postcode,tranl$paon,sep=", ")
tranl_1<-tranl[tranl$saon!="",]
tranl_1<-tranl_1[grepl("\\d$",tranl_1$saon),]

tranl_1$saonl<-word(tranl_1$saon,-1)
tranl_1$addressf <-paste(tranl_1$addressf,tranl_1$saonl,sep=", ")
#tranl_1<-tranl[tranl$saon!="",]

add$addressf <- paste(add$postcodelocator,add$buildingname,sep=", ")
add$saotextc <- word(add$saotext,-1)
add$addressf <- paste(add$addressf,add$saotextc,sep=", ")





l18<- inner_join(tranl_1,add,by="addressf")
l18<-l18[,..needlist1]

fl18<-uniqueresult(l18)
dim(fl18)
fl18$method<-"method157"
dim(add)
add[, c("addressf","saotextc"):=NULL]
dim(add)
#################method 158 #################
tranl <- matchleft(tranl,fl18)
dim(tranl)
# 23458    21
needl<-matchleft(needl,fl18)


add<-add[,..addlist1]
###stage 8 sum up ########

ly = list(fl1,fl2,fl3,fl4,fl5,fl6,fl7,fl8,fl9,fl10,fl11,fl12,fl13,fl14,fl15,fl16,fl17,fl18)
fl<- rbindlist(ly ,use.names=TRUE,fill=T)
dim(fl)
#82654
dim(fl1)[1]+dim(fl2)[1]+dim(fl3)[1]+dim(fl4)[1]+dim(fl5)[1]+dim(fl6)[1]+dim(fl7)[1]+dim(fl8)[1]+dim(fl9)[1]+
  dim(fl10)[1]+ dim(fl11)[1]+dim(fl12)[1]+ dim(fl13)[1]+dim(fl14)[1]+dim(fl15)[1]+dim(fl16)[1]+dim(fl17)[1]+dim(fl18)[1]
#82654


length(unique(fl$transactionid))
length(unique(fl$method))
#18
stage8<-rbindlist(list(finalll,fl) ,use.names=TRUE,fill=T)
dim(stage8)
#110248
length(unique(stage8$transactionid))
#110248
length(unique(stage8$method))
#19
dbWriteTable(con, "stage8",value =stage8, append = TRUE, row.names = FALSE)

dbWriteTable(con, "finalll",value =finalll, append = TRUE, row.names = FALSE)
dbWriteTable(con, "fl",value =fl, append = TRUE, row.names = FALSE)

dbWriteTable(con, "tran_stage8_left",value =tranl, append = TRUE, row.names = FALSE)
rm(finalll,fl)
rm(l1,l2,l3,l4,l5,l6,l7,l8,l9,l10,l11,l12,l13,l14,l15,l16,l17,l18)
rm(fl1,fl2,fl3,fl4,fl5,fl6,fl7,fl8,fl9,fl10,fl11,fl12,fl13,fl14,fl15,fl16,fl17,fl18)
rm(tranl_1)
#################stage 9 #################
tran22 <- matchleft(tran33,stage8)
dim(tran22)
#1095188      19
dim(tran22)[1]/dim(tran)[1]

dim(add)
#32702603       32
add<-add[add$postset %in% tran22$postset,]

dim(add)



tran22$addressf <-paste(tran22$postcode,tran22$street,sep=", ")
add$addressf <- paste(add$postcodelocator,add$paotext,sep=", ")

w <- inner_join(tran22,add,by="addressf")

w<-w[,..needlist1]
w <-w[w$street!="",]
# finalw <- uniqueresult(w)
# dim(finalw)
# # 4932   30
# needw <- doubleresult(w)
# dim(needw)
#2476026      30
tranw <-tranneed(tran22,w) 
dim(tranw)
# 153431     19
#rm(w)
#################method 231 #################
# tranw$flatpaon <- paste("FLAT ",tranw$paon,sep="") 
# tranw$unpaon <- paste("UNIT ",tranw$paon,sep="") 
# tranw$pastr <- paste(tranw$paon,tranw$street,sep=" ")


tranw$addressf <-paste(tranw$postcode,tranw$paon,sep=", ")
tranw$addressf <-paste(tranw$addressf,tranw$street ,sep=", ")

add$addressf <- paste(add$postcodelocator,add$ss,sep=", ")
add$addressf <- paste(add$addressf,add$paotext,sep=", ")


w1<- inner_join(tranw,add,by="addressf")
w1<-w1[,..needlist1]

fw1<-uniqueresult(w1)
dim(fw1)
# 127138 

fw1$method<-"method231"


#################method 232 #################
tranw <-matchleft(tranw,fw1) 
dim(tranw)
#42592    22



tranw$pastr <- paste(tranw$paon,tranw$street,sep=" ")
tranw$addressf <-paste(tranw$postcode,tranw$pastr,sep=", ")

add$addressf <- paste(add$postcodelocator,add$buildingname,sep=", ")


w2<- inner_join(tranw,add,by="addressf")
#w2<- inner_join(tranw_1,add,by="addressf")
w2<-w2[,..needlist1]
#rm(tranw_1)
fw2<-uniqueresult(w2)
dim(fw2)
#2727
fw2$method<-"method232"

#################method 233 #################
tranw <-matchleft(tranw,fw2) 
dim(tranw)
# 23566    20
w <-matchleft(w,fw2) 
w <-matchleft(w,fw1) 

 tranw$pastr <- paste(tranw$paon,tranw$street,sep=" ")
 tranw$addressf <-paste(tranw$postcode,tranw$pastr,sep=", ")
 
 add$addressf <- paste(add$postcodelocator,add$paotext,sep=", ")
w3<- inner_join(tranw,add,by="addressf")
w3<-w3[,..needlist1]

fw3<-uniqueresult(w3)
dim(fw3)
#47 30
fw3<-fw3[fw3$saon==fw3$ss,]

fw3$method<-"method233"

#################method 234 #################
tranw <-matchleft(tranw,fw3) 
dim(tranw)
#23520    20


tranw$pastr <- paste(tranw$paon,tranw$street,sep=" ")
tranw$addressf <-paste(tranw$postcode,tranw$pastr,sep=", ")
tranw$addressf <-paste(tranw$addressf,tranw$saon,sep=", ")


add$addressf <- paste(add$postcodelocator,add$paotext,sep=", ")
add$addressf <- paste(add$addressf,add$ss,sep=", ")


w4<- inner_join(tranw,add,by="addressf")
w4<-w4[,..needlist1]

fw4<-uniqueresult(w4)
dim(fw4)

fw4$method<-"method234"


#################method 235 #################
tranw <-matchleft(tranw,fw4) 
dim(tranw)
#39623    22
w <-matchleft(w,fw3) 
w <-matchleft(w,fw4) 


 tranw$addressf <-paste(tranw$postcode,tranw$paon,sep=", ")
 tranw$addressf <-paste(tranw$addressf,tranw$street ,sep=", ")
 tranw$addressf <-paste(tranw$addressf,tranw$saon ,sep=", ")
 #tranw_1<-tranw[tranw$saon!="",]
 add$addressf <- paste(add$postcodelocator,add$ss,sep=", ")
 add$addressf <- paste(add$addressf,add$paotext,sep=", ")
 add$addressf <- paste(add$addressf,add$saotext,sep=", ")


w5<- inner_join(tranw,add,by="addressf")
w5<-w5[,..needlist1]

fw5<-uniqueresult(w5)
dim(fw5)
# 638
fw5$method<-"method235"


#################method 236 #################
tranw <-matchleft(tranw,fw5) 
dim(tranw)
#22687    20
#tranw<-tranw[tranw$street!="",]

tranw$flatpaon <- paste("FLAT ",tranw$paon,sep="") 
tranw$addressf <-paste(tranw$postcode,tranw$flatpaon,sep=", ")
tranw$addressf <-paste(tranw$addressf,tranw$street ,sep=", ")

add$addressf <- paste(add$postcodelocator,add$subbuildingname,sep=", ")
add$addressf <- paste(add$addressf,add$paotext,sep=", ")



w6<- inner_join(tranw,add,by="addressf")
w6<-w6[,..needlist1]

fw6<-uniqueresult(w6)
dim(fw6)

fw6$method<-"method236"

#################method 237 #################
tranw <-matchleft(tranw,fw6) 
dim(tranw)
#20751    21

w <-matchleft(w,fw5) 
w <-matchleft(w,fw6) 




tranw$unpaon <- paste("UNIT ",tranw$paon,sep="") 
tranw$addressf <-paste(tranw$postcode,tranw$unpaon,sep=", ")
tranw$addressf <-paste(tranw$addressf,tranw$street ,sep=", ")

add$addressf <- paste(add$postcodelocator,add$saotext,sep=", ")
add$addressf <- paste(add$addressf,add$paotext,sep=", ")



w7<- inner_join(tranw,add,by="addressf")
w7<-w7[,..needlist1]

fw7<-uniqueresult(w7)
dim(fw7)

fw7$method<-"method237"
###################here############################

#################method 238 #################
tranw <-matchleft(tranw,fw7) 
dim(tranw)
#20602    22
w <-matchleft(w,fw7) 

tranw$addressf <-paste(tranw$postcode,tranw$street,sep=", ")

#tranw$paonl <-word(tranw$paon,-1)
tranw$addressf <-paste(tranw$addressf,tranw$paon ,sep=", ")

add$addressf <- paste(add$postcodelocator,add$paotext,sep=", ")
add$addressf <- paste(add$addressf,add$saotext,sep=", ")


w8<- inner_join(tranw,add,by="addressf")
w8<-w8[,..needlist1]

fw8<-uniqueresult(w8)
dim(fw8)

fw8$method<-"method238"

#################method 239 #################
tranw <-matchleft(tranw,fw8) 
dim(tranw)
w <-matchleft(w,fw8)

tranw$addressf <-paste(tranw$postcode,tranw$street,sep=", ")

tranw$paonl <-word(tranw$paon,-1)
tranw$addressf <-paste(tranw$addressf,tranw$paonl ,sep=", ")

add$addressf <- paste(add$postcodelocator,add$paotext,sep=", ")
add$addressf <- paste(add$addressf,add$ss,sep=", ")

w9<- inner_join(tranw,add,by="addressf")
w9<-w9[,..needlist1]

fw9<-uniqueresult(w9)
dim(fw9)

fw9$method<-"method239"

#################method 240 #################
tranw <-matchleft(tranw,fw9) 
dim(tranw)
w <-matchleft(w,fw9)

tranw$addressf <-paste(tranw$postcode,tranw$street,sep=", ")

tranw$saonpaon <-paste(tranw$saon,tranw$paon ,sep=" ")
tranw$addressf <-paste(tranw$addressf,tranw$saonpaon ,sep=", ")

add$addressf <- paste(add$postcodelocator,add$paotext,sep=", ")
#add$saotextl <-word(add$saotext,-1)
add$addressf <- paste(add$addressf,add$saotext,sep=", ")

w10<- inner_join(tranw,add,by="addressf")
w10<-w10[,..needlist1]

fw10<-uniqueresult(w10)
dim(fw10)
fw10$method<-"method240"
head(add)
class(add)
dim(add)
add[, c("saotextl","addressf","fss","add11ressf"):=NULL]

#################method 241 #################
tranw <-matchleft(tranw,fw10) 
dim(tranw)
#12268    24
w <-matchleft(w,fw10)
tranw$addressf <-paste(tranw$postcode,tranw$street,sep=", ")

tranw$saonpaon <-paste(tranw$saon,tranw$paon ,sep=" ")
tranw$addressf <-paste(tranw$addressf,tranw$saonpaon ,sep=", ")

add$addressf <- paste(add$postcodelocator,add$paotext,sep=", ")
#add$saotextl <-word(add$saotext,-1)
add$addressf <- paste(add$addressf,add$buildingname,sep=", ")

w11<- inner_join(tranw,add,by="addressf")
w11<-w11[,..needlist1]

fw11<-uniqueresult(w11)
dim(fw11)
fw11$method<-"method241"


#################method 242 #################
tranw <-matchleft(tranw,fw11) 
dim(tranw)
# 11520    24
w <-matchleft(w,fw11)


tranw$addressf <-paste(tranw$postcode,tranw$paon,sep=", ")

#tranw$saonpaon <-paste(tranw$saon,tranw$paon ,sep=" ")
tranw$addressf <-paste(tranw$addressf,tranw$street ,sep=" ")

#add$addressf <- paste(add$postcodelocator,add$paotext,sep=", ")
add$buildingnamec <-gsub("[.]","",add$buildingname)
add$addressf <- paste(add$postcodelocator,add$buildingnamec,sep=", ")


w12<- inner_join(tranw,add,by="addressf")
w12<-w12[,..needlist1]

fw12<-uniqueresult(w12)
dim(fw12)
fw12$method<-"method242"

#################method 243 #################

tranw <-matchleft(tranw,fw12) 
dim(tranw)
#11445    24
w <-matchleft(w,fw12)

tranw$addressf <-paste(tranw$postcode,tranw$street,sep=", ")

tranw$paonsaon <-paste(tranw$paon,tranw$saon ,sep=" ")
tranw$addressf <-paste(tranw$addressf,tranw$paonsaon ,sep=", ")

add$addressf <- paste(add$postcodelocator,add$paotext,sep=", ")
#add$saotextl <-word(add$saotext,-1)
add$addressf <- paste(add$addressf,add$saotext,sep=", ")



w13<- inner_join(tranw,add,by="addressf")
w13<-w13[,..needlist1]

fw13<-uniqueresult(w13)
dim(fw13)
fw13$method<-"method243"

#################method 244 #################

tranw <-matchleft(tranw,fw13) 
dim(tranw)
#11443    25
w <-matchleft(w,fw13)


tranw$addressf <-paste(tranw$postcode,tranw$street,sep=", ")

#tranw$paonsaon <-paste(tranw$paon,tranw$saon ,sep=" ")
#tranw$paon1 <- gsub(" - ","-",tranw$paon )
tranw$paon1 <- gsub(",","",tranw$paon )
tranw$addressf <-paste(tranw$addressf,tranw$paon1 ,sep=", ")

add$addressf <- paste(add$postcodelocator,add$paotext,sep=", ")
#add$ss1 <-paste(add$saostartnumber,add$saoendnumber,sep="-")
add$addressf <- paste(add$addressf,add$saotext,sep=", ")


w14<- inner_join(tranw,add,by="addressf")
w14<-w14[,..needlist1]

fw14<-uniqueresult(w14)
dim(fw14)
fw14$method<-"method244"


#################method 245 #################
tranw <-matchleft(tranw,fw14) 
dim(tranw)
#11409    25
tranw$addressf <-paste(tranw$postcode,tranw$street,sep=", ")

#tranw$paonsaon <-paste(tranw$paon,tranw$saon ,sep=" ")
tranw$paon1 <- gsub(" - ","-",tranw$paon )
#tranw$paon1 <- gsub(",","",tranw$paon )
tranw$addressf <-paste(tranw$addressf,tranw$paon1 ,sep=", ")

add$addressf <- paste(add$postcodelocator,add$paotext,sep=", ")
add$ss1 <-paste(add$saostartnumber,add$saoendnumber,sep="-")
add$addressf <- paste(add$addressf,add$ss1,sep=", ")



w15<- inner_join(tranw,add,by="addressf")
w15<-w15[,..needlist1]

fw15<-uniqueresult(w15)
dim(fw15)
fw15$method<-"method245"


#################method 246 #################
tranw <-matchleft(tranw,fw15) 
dim(tranw)
#11219    25
w <-matchleft(w,fw14)
w <-matchleft(w,fw15)


tranw$addressf <-paste(tranw$postcode,tranw$street,sep=", ")

#tranw$paonsaon <-paste(tranw$paon,tranw$saon ,sep=" ")
#tran33$paon1 <- gsub(" - ","-",tran33$paon )
tranw$addressf <-paste(tranw$addressf,tranw$paon ,sep=", ")

add$addressf <- paste(add$postcodelocator,add$paotext,sep=", ")
add$saotext2 <-word(add$saotext,1,2)
add$addressf <- paste(add$addressf,add$saotext2,sep=", ")


w16<- inner_join(tranw,add,by="addressf")
w16<-w16[,..needlist1]

fw16<-uniqueresult(w16)
dim(fw16)
fw16$method<-"method246"

#################method 247 #################

tranw <-matchleft(tranw,fw16) 
dim(tranw)
#11144    25
w <-matchleft(w,fw16)
tranw$addressf <-paste(tranw$postcode,tranw$street,sep=", ")
tranw_1<-tranw[grepl("\\d$",tranw$paon),]
tranw_1<-tranw_1[!grepl("-",tranw_1$paon),]
tranw_1$paonl <-word(tranw_1$paon,-1)
tranw_1$addressf <-paste(tranw_1$addressf,tranw_1$paonl ,sep=", ")

add$addressf <- paste(add$postcodelocator,add$paotext,sep=", ")
add$saotextl <-word(add$saotext,-1)
add$addressf <- paste(add$addressf,add$saotextl,sep=", ")


w17<- inner_join(tranw_1,add,by="addressf")
w17<-w17[,..needlist1]
rm(tranw_1)
fw17<-uniqueresult(w17)
dim(fw17)

fw17<-fw17[!grepl("\\d",fw17$saon),]
fw17$method<-"method247"




tranw <-matchleft(tranw,fw17) 
dim(tranw)
w <-matchleft(w,fw17)
head(add)
add[, c("buildingnamec","addressf"," ss1","saotext2","saotextl"):=NULL]
#################stage  9 sum up#################

ly = list(fw1,fw2,fw3,fw4,fw5,fw6,fw7,fw8,fw9,fw10,fw11,fw12,fw13,fw14,fw15,fw16,fw17)
fw<- rbindlist(ly ,use.names=TRUE,fill=T)
dim(fw)
# 142916
dim(fw1)[1]+dim(fw2)[1]+dim(fw3)[1]+dim(fw4)[1]+dim(fw5)[1]+dim(fw6)[1]+dim(fw7)[1]+dim(fw8)[1]+dim(fw9)[1]+
  dim(fw10)[1]+ dim(fw11)[1]+dim(fw12)[1]+ dim(fw13)[1]+dim(fw14)[1]+dim(fw15)[1]+dim(fw16)[1]+dim(fw17)[1]
# 142916
length(unique(fw$transactionid))
length(unique(fw$method))
#18
stage9<-fw 
dim(stage9)
#142916     31
length(unique(stage9$transactionid))
# 142916     30
length(unique(stage9$method))
#17
sort(unique(stage9$method))
dbWriteTable(con, "stage9",value =stage9, append = TRUE, row.names = FALSE)

rm(fw1,fw2,fw3,fw4,fw5,fw6,fw7,fw8,fw9,fw10,fw11,fw12,fw13,fw14,fw15,fw16,fw17)
rm(w1,w2,w3,w4,w5,w6,w7,w8,w9,w10,w11,w12,w13,w14,w15,w16,w17)

#################stage 10 #################
tran11 <- matchleft(tran22,stage9)
dim(tran11)
#997349     19
dim(tran11)[1]/dim(tran)[1]




tran11$addressf <-paste(tran11$postcode,tran11$paon,sep=", ")
add$sapa <- paste(add$saotext,add$pp,sep=", ")
add$addressf <- paste(add$postcodelocator,add$sapa,sep=", ")

a <- inner_join(tran11,add,by="addressf")


a<-a[,..needlist1]

finala <- uniqueresult(a)
dim(finala)
#3259   30
needa <- doubleresult(a)

trana<-tranneed(tran11,needa)
finala$method<-"method254"
#################method 255 #################


trana$addressf <-paste(trana$postcode,trana$paon,sep=", ")
trana$addressf <-paste(trana$addressf,trana$saon,sep=", ")
add$sapa <- paste(add$saotext,add$pp,sep=", ")
add$fss <- paste("FLAT ",add$ss,sep="")
add$addressf <- paste(add$postcodelocator,add$sapa,sep=", ")
add$addressf <- paste(add$addressf,add$fss,sep=", ")

a1 <- inner_join(trana,add,by="addressf")
a1 <-a1 [,..needlist1]

fa1 <-uniqueresult(a1 )
dim(fa1)


fa1$method<-"method255"

#################method 256 #################
trana <- matchleft(trana,fa1)
dim(trana)
# 28 19
needa <- matchleft(needa,fa1)
dim(needa)

trana$addressf <-paste(trana$postcode,trana$paon,sep=", ")
trana$addressf <-paste(trana$addressf,trana$saon,sep=", ")
add$sapa <- paste(add$saotext,add$pp,sep=", ")

add$addressf <- paste(add$postcodelocator,add$sapa,sep=", ")
add$addressf <- paste(add$addressf,add$ss,sep=", ")

a2 <- inner_join(trana,add,by="addressf")
a2 <-a2 [,..needlist1]

fa2 <-uniqueresult(a2 )
dim(fa2)


fa2$method<-"method256"

#################method 257 #################

trana <- matchleft(trana,fa2)
dim(trana)
# 28 19
needa <- matchleft(needa,fa2)
dim(needa)


#################stage 10 sum up#################
stage10<-rbindlist(list(fa1,fa2,finala) ,use.names=TRUE,fill=T)

dim(stage10)
#3498
length(unique(stage10$transactionid))
#3498
length(unique(stage10$method))
#3

dbWriteTable(con, "stage10",value =stage10, append = TRUE, row.names = FALSE)
rm(fa1,fa2,finala)
#################stage 11 #################
tran0 <- matchleft(tran11,stage10)
dim(tran0)
#993851     19
rm(tran22,tran11)
dim(tran0)[1]/dim(tran)[1]
#0.03696986

tran0$sapa <- paste(tran0$saon,tran0$paon,sep=" ")
tran0$addressf <-paste(tran0$postcode,tran0$sapa,sep=", ")

add$addressf <- paste(add$postcodelocator,add$buildingname,sep=", ")

g <- inner_join(tran0,add,by="addressf")


g<-g[,..needlist1]

stage11 <- uniqueresult(g)
dim(stage11)
#51692    30
needg <- doubleresult(g)

trang<-tranneed(tran0,needg)
stage11$method<-"method260"


#stage11<-finalg

dim(stage11)
# 51692 
length(unique(stage11$transactionid))
# 51692 
length(unique(stage11$method))
#1

dbWriteTable(con, "stage11",value =stage11, append = TRUE, row.names = FALSE)


#################stage 12 #################
tran100 <- matchleft(tran0,stage11)
dim(tran100)
# 897082     20
dim(add)
add<-add[,..addlist1]
dim(add)
add<-add[add$postset %in% tran100$postset,]


tran100$addressf <-paste(tran100$postcode,tran100$paon,sep=", ")

add$addressf <- paste(add$postcodelocator,add$ss,sep=", ")

k <- inner_join(tran100,add,by="addressf")

k<-k[,..needlist1]
k<- k[k$saon=="",]
finalk <- uniqueresult(k)
dim(finalk)
#12094    30
needk <- doubleresult(k)

trank<-tranneed(tran100,needk)
dim(trank)
finalk$method<-"method270"
#################method 271 ################
trank<-matchleft(trank,finalk) 
dim(trank)
#2296   20
trank$pastr <- paste(trank$paon,trank$street,sep=" ")
trank$addressf <-paste(trank$postcode,trank$pastr,sep=", ")

add$addressf <- paste(add$postcodelocator,add$buildingname,sep=", ")


k1<- inner_join(trank,add,by="addressf")

k1<-k1[,..needlist1]
k1<- k1[k1$saon=='',]
finalk1 <- uniqueresult(k1)
dim(finalk1)
#12094    30

finalk1$method<-"method271"

#################method 272 ################
trank<-matchleft(trank,finalk1) 
dim(trank)
# 1504   21
needk<-matchleft(needk,finalk1) 
dim(needk)



#################stage12 sum up ################

stage12<-rbindlist(list(finalk,finalk1) ,use.names=TRUE,fill=T)

dim(stage12)
#12886
length(unique(stage12$transactionid))
#12886
length(unique(stage12$method))
#2

dbWriteTable(con, "stage12",value =stage12, append = TRUE, row.names = FALSE)
rm(finalk,finalk1)

tranleft<-matchleft(tran100,stage12)
dim(tranleft)
#884196     20
dbWriteTable(con, "tranleft",value =tranleft, append = TRUE, row.names = FALSE)
###Total sum up######
datlist<-list(stage1,stage2,stage3,stage4,stage5,stage6,stage7,stage8,stage9,stage10,stage11,stage12)


ppd_link<-rbindlist(datlist ,use.names=TRUE,fill=T)

dim(ppd_link)
# 25950983
length(unique(ppd_link$transactionid))
# 25950983 



length(unique(ppd_link$method))
#141
needlistf<-c("transactionid","uprn","postcode.x","postcodelocator","postset.x", "postset.y","propertytype","paon","saon","street","locality.x","saostartnumber","saostartsuffix","saoendnumber","saoendsuffix","saotext","paostartnumber","paostartsuffix","paoendnumber","paoendsuffix","paotext","streetdescription","buildingname","buildingnumber","subbuildingname","bb","ss","pp","method")
ppd_link<-ppd_link[,..needlistf]
dim(ppd_link)
#25950982       30
length(unique(ppd_link$transactionid))
# 25950982
dbWriteTable(con, "ppd_link",value =ppd_link, append = TRUE, row.names = FALSE)
dim(tran)[1]

rm(stage1,stage2,stage3,stage4,stage5,stage6,stage7,stage8,stage9,stage10,stage11,stage12)
rm(list= ls()[! (ls() %in% c('ppd_link'))]) 
#################stage 13 #################
tranfail<-tran[tran$transactionid %in% ppd_link,]

tranfail_saon<-tranfail[tranfail$saon=="",]





tranfail$addressf <-paste(tranfail$paon,tranfail$street,sep=",")

add$addressf <- paste(add$paotext,add$streetdescription,sep=",")

o <- inner_join(trana,add,by="addressf")
o  <-o  [,..needlist1]

fo1 <-uniqueresult(o )
dim(fa1)


#fa1$method<-"method255"





#################################################################################################

Sys.time()
#"2022-03-23 10:21:27 GMT"
library(DBI)
library(RPostgreSQL)
#DBI::dbDriver('PostgreSQL')
require(RPostgreSQL)
drv=dbDriver("PostgreSQL")
db <- "postgis23"
host_db <- "localhost"
db_port <- "5432"
db_user <- "postgres"
db_password <- "232323"
con <- dbConnect(RPostgres::Postgres(), dbname = db, host=host_db, port=db_port, user=db_user, password=db_password)


add <- dbGetQuery(con,"select * from  addressgb") 
Sys.time()
#"2022-03-23 10:26:31 GMT"
head(add)
str(add)
#osadd<-add
######### format the OS addressBase data #########

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


##clean the postcode variable before conduct the linkage
#epc$postcode  <- str_trim(epc$postcode)
add$postcodelocator  <- str_trim(add$postcodelocator)


#############  Section 2: Read in Land Registry PPD  #############  
Sys.time()

db <- "postgis23"
host_db <- "localhost"
db_port <- "5432"
db_user <- "postgres"
db_password <- "232323"
###creates a connection to the postgres database
### note that "con" will be used later in each connection to the database
con <- dbConnect(RPostgres::Postgres(), dbname = db, host=host_db, port=db_port, user=db_user, password=db_password)

##read the land registy data at tran, read the addressbase plus data(addressengland) as add
#addresseng is the address data clean up the paretnes who has children in postgis
Sys.time()
#"2022-01-16 20:20:05 GMT"
tran <- dbGetQuery(con,"select * from pricepaid") 
Sys.time()

tranleft<-matchleft(tran,ppd_link)
dim(tranleft)
#932186     17




# 882916
dim(allkeep)[1]/dim(tran)[1]
dim(tranleft)
#884196     20
dim(tran)
#26883169  
tranl<-tran[!(tran$transactionid %in% allkeep$transactionid),]
dim(tranl)
#884196
tranlf<-tranl[tranl$transactionid %in% tranleft$transactionid,]
dim(tranlf)

tranlf1<-tranl[!(tranl$transactionid %in% tranleft$transactionid),]
dim(tranlf1)
dim(tranl)
dim(tranleft)
dim(tran)[1]-length(unique(tran$transactionid))
dim(allkeep)[1]-length(unique(allkeep$transactionid))
dim(tran)[1]-dim(allkeep)[1]
dim(tran)[1]
dim(allkeep)[1]

#0.967097
head(allkeep)
head(tran)
rm(list= ls()[! (ls() %in% c('tran','allkeep',"tranleft"))]) 
head(allkeep)



nspl<-fread("D:/os/NSPL_NOV_2021_UK.csv")
head(nspl)
head(epcdata)


head(tran)
dim(tran)
# 26833898       18
nspl<-nspl[,c("pcds","lsoa11","msoa11","laua","ctry","ttwa","imd")]



tranl<-tran[,c("transactionid","dateoftransfer","postcode","propertytype")]

#unique(nspl$ctry)
#E92000001 = England;
# W92000004 = Wales;
# S92000003 = Scotland;
# N92000002 = Northern Ireland;
# L93000001 = Channel Islands;
# M83000003 = Isle of Man

colnames(nspl)[1]<-"postcode"

epcdata1<-merge(tranl,nspl,by="postcode")
dim(epcdata)[1]-dim(epcdata1)[1]
#11769

dim(link_all)
#21051379       35

epc_2<-epcdata1[epcdata1$transactionid %in% allkeep$transactionid,]
head(allkeep)
matcht<-allkeep[, .(count=.N), by=method]
head(epcdata1)
head(epc_2)
setDT(epcdata1)
setDT(epc_2)
matchre<-epcdata1[, .(count=.N), by=laua]
matchl<-epc_2[, .(countl=.N), by=laua]

###create data for mosaic plot and treemaps
library(ggplot2) 
library(ggmosaic)


mtcars <- mtcars %>% mutate(
  gear = factor(gear),  # Converts the gear variable to a factor
  cyl = factor(cyl)  # Converts the cyl variable to a factor
)

mtcars_tab <- mtcars %>% count(cyl, gear)


head(matchl)
head(matchre)
match_re1<-merge(matchre,matchl,by="laua")
match_re1<-match_re1[match_re1$laua!="",]
head(match_re1)
match_re1$pro1<-(match_re1$countl/match_re1$count)

fwrite(match_re1,"D:/os/match_reppd.csv")
fwrite(matcht,"D:/os/matcht.csv")
summary(match_re1$pro)

ll<-fread("D:/os/ll.csv")
allkeep1<-merge(allkeep,ll,by="method")
dim(allkeep1)
#25950982       31
dim(allkeep)
head(allkeep1)
matcht<-allkeep1[, .(count=.N), by=stage]
matcht$pro<-matcht$count/dim(tran)[1]
###################check the ###############################
tran0<-tran
tranl<-tran[!(tran$transactionid %in% allkeep$transactionid),]
dim(tranl)
#
tranl1<-tranl[tranl$postset %in% add$postset, ]
dim(tranl1)
##match rate for each method#
head(ppd_link)
dim(ppd_link)[1]/26883169
tran <- dbGetQuery(con,"select * from pricepaid") 
dim(tran)
#26883169
class(ppd_link)
sta_matchrate_ppd<-ppd_link[,.(count=.N),by="method"]
sta_matchrate_ppd$pro<-sta_matchrate_ppd$count/26883169
head(sta_matchrate_ppd)
sta_matchrate_ppd<-sta_matchrate_ppd[order(-pro)]
sta_matchrate_ppd[, no := .I]
top20<-sta_matchrate_ppd[no<=20,]

length(unique(ppd_link$method))
rm(top20)
#################Check the match rate###########################

###conduct in another workstation

fwrite(top20,"D:/plot/PPDtop20.csv")
db <- "os"
host_db <- "localhost"
db_port <- "5432"
db_user <- "postgres"
db_password <- "232323"
###creates a connection to the postgres database
### note that "con" will be used later in each connection to the database
con <- dbConnect(RPostgres::Postgres(), dbname = db, host=host_db, port=db_port, user=db_user, password=db_password)
dbWriteTable(con, "ppd_link",value =ppd_link, append = TRUE, row.names = FALSE)

#################################

##match rate for each year.

head(ppd_link)
head(tran)
p_year1<-tran[ ,c("transactionid","propertytype","dateoftransfer")]
p_year<-tran[tran$transactionid %in% ppd_link$transactionid ,c("transactionid","propertytype","dateoftransfer")]
str(p_year)
p_year1$year<-format(as.Date(p_year1$dateoftransfer, format="%d/%m/%Y"),"%Y")

p_year$year<-format(as.Date(p_year$dateoftransfer, format="%d/%m/%Y"),"%Y")
p_year$year<-format(as.Date(p_year$dateoftransfer, format="%Y-%m-%d"),"%Y")
head(p_year)
sort(unique(p_year$year))
 
sta_matchratey_ppd1<-p_year1[,.(counto=.N),by="year"]

sta_matchratey_ppd<-p_year[,.(count=.N),by="year"]
sta_matchratey_ppd$pro<-sta_matchratey_ppd$count

sta_matchratey_ppd_all<-merge(sta_matchratey_ppd, sta_matchratey_ppd1,by="year")
sta_matchratey_ppd_all$pro<-sta_matchratey_ppd_all$count/sta_matchratey_ppd_all$counto
fwrite(sta_matchratey_ppd_all,"D:/plot/sta_matchratey_ppd.csv")

sta_matchratey_ppd1t<-p_year1[,.(counto=.N),by="propertytype"]

sta_matchratey_ppdt<-p_year[,.(count=.N),by="propertytype"]
sta_matchratey_ppd_allt<-merge(sta_matchratey_ppdt, sta_matchratey_ppd1t,by="propertytype")
sta_matchratey_ppd_allt$pro<-sta_matchratey_ppd_allt$count/sta_matchratey_ppd_allt$counto
fwrite(sta_matchratey_ppd_allt,"D:/plot/sta_matchratey_ppdt.csv")


dim(tran)
#26883169       16
head(tran)
tran0<-tran[tran$propertytype=="O",]
tranf<-tran[tran$propertytype=="F",]
rm(tranf)
tranf<-tranf[!(tranf$transactionid %in% ppd_link$transactionid),]
tran0l<-tran0[!(tran0$transactionid %in% ppd_link$transactionid),]
dim(tran0l)
#133438     17
dim(tran0)
#376891     17
dim(tran0[grep("GARAGE",tran0$saon)])
# 3484 
tran0l<-tran0l[!grep("GARAGE",tran0l$saon),]
tran0l<-tran0l[!grep("GARAGE",tran0l$paon),]
dim(tran0l)
#126458     17
tran0l<-tran0l[!grep("PARKING SPACE",tran0l$saon),]
tran0l<-tran0l[!grep("PARKING SPACE",tran0l$paon),]
dim(tran0l)
#122977     17
#
#BEACH HUT 


tran0l<-tran0l[!grep("BEACH HUT",tran0l$saon),]
tran0l<-tran0l[!grep("BEACH HUT",tran0l$paon),]
dim(tran0l)
#122760     17

tran0l<-tran0l[!grep("PARK",tran0l$saon),]
tran0l<-tran0l[!grep("PARK",tran0l$paon),]
dim(tran0l)
#118428     17
tran0l<-tran0l[!grep("LODGE",tran0l$saon),]
tran0l<-tran0l[!grep("LODGE",tran0l$paon),]
dim(tran0l)
#117395     17

#BARN
tran0l<-tran0l[!grep("BARN",tran0l$saon),]
tran0l<-tran0l[!grep("BARN",tran0l$paon),]
dim(tran0l)
#116149     17


#BARN
tran0l<-tran0l[!grep("BARN",tran0l$saon),]
tran0l<-tran0l[!grep("BARN",tran0l$paon),]
dim(tran0l)
#117395     17

#######fail part#####


tran_un<-tran[!(tran$postcode %in% add$postcodelocator), ]
dim(tran_un)
# 87807    16
dim(tran_un)[1]/dim(tran)[1]
#0.003266244

dim(ppd_link)[1]/dim(tran)[1]
#0.9653245


###########

dim(tran0l[tran0l$postcode=="",])
#25845    17
25845/376891
#0.0685742
