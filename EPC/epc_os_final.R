# ------------------------------------------------
# epc_os_final.R
# ------------------------------------------------
# Code provided as is and can be used or modified freely. 
# ------------------------------------------------
# Author: BIN CHI
# Urban Big Data Centre at University of Glasgow
# Bin.Chi@glasgow.ac.uk
# Date: 1/4/2022

#################################### Section 1: load package#################################### 
library("qdap")
library(data.table)
library("RPostgreSQL")
library("sqldf")
library("dplyr")
library(tidyverse)
library(stringr)
library(DBI)


drv=dbDriver("PostgreSQL")
db <- "os_ubdc"
host_db <- "localhost"
db_port <- "5432"
db_user <- "postgres"
#Update your password for the PostGIS database(osubdc)
db_password <- "654321"
con <- dbConnect(RPostgres::Postgres(), dbname = db, host=host_db, port=db_port, user=db_user, password=db_password)

#################################### Section 2: read in Domesitic EPC and OS AddressBase Plus#################################### 
#read in EPC from PostGIS os_ubdc database
db <- "os_ubdc"
host_db <- "localhost"
db_port <- "5432"
db_user <- "postgres"
#Update your password for the PostGIS database
db_password <- "654321"
con <- dbConnect(RPostgres::Postgres(), dbname = db, host=host_db, port=db_port, user=db_user, password=db_password)
add <- dbGetQuery(con,"select * from  addressgb") 
#read in epc by selecting part of variables
epc <- dbGetQuery(con,"select lmk_key,address1,address2,address3,address,postcode,building_reference_number,property_type,transaction_type,built_form,inspection_date,lodgement_date,total_floor_area,number_habitable_rooms,current_energy_rating,potential_energy_rating,current_energy_efficiency,potential_energy_efficiency,floor_level,flat_top_storey,flat_storey_count,floor_height,construction_age_band,co2_emissions_current,co2_emiss_curr_per_floor_area,lodgement_datetime from epcdata") 

####################################Section 3: OS AddressBase data and Domestic EPCs pre-processing#################################### 
#capitalise address1, address2, address3 and address four fields into four new fields (add1,add2,add3 and add)
epc[,  `:=`(add1 = toupper(address1),
            add2 = toupper(address2),
            add3 = toupper(address3),
            add = toupper(address))]
#Remove the address1, address2 and address3 fields
epc[, address1:=NULL]
epc[, address2:=NULL]
epc[, address3:=NULL]

#Remove whitespace from the left and right sides of the address string and postcode string
epc$add1 <- str_squish(epc$add1)
epc$add2 <- str_squish(epc$add2)
epc$add3 <- str_squish(epc$add3)
epc$add  <- str_squish(epc$add)

epc$add1 <- str_trim(epc$add1)
epc$add2 <- str_trim(epc$add2)
epc$add3 <- str_trim(epc$add3)
epc$add  <- str_trim(epc$add)

#Remove multiple commas and trailing commasin add field
epc$add<-gsub("^,*|(?<=,),|,*$", "", epc$add, perl=T)


######### format the OS addressBase data #########
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
#clean the postcode variable before conduct the linkage
epc$postcode  <- str_trim(epc$postcode)
add$postcodelocator  <- str_trim(add$postcodelocator)
#Covert add and epc to a data table format
setDT(add)
setDT(epc)
#Prepare the field name list for the linkage

needlist1<-c("lmk_key","postcode.y","property_type","uprn","add1","add2","add3","add","postcode.x","postcodelocator","buildingname","buildingnumber","subbuildingname","paostartnumber","paostartsuffix","paoendnumber","paoendsuffix","paotext","saostartnumber","saostartsuffix","saoendnumber","saoendsuffix","saotext","streetdescription","locality","dependentlocality","townname","class","lodgement_date","inspection_date","lodgement_datetime")
needlist2<-c("method","lmk_key","uprn","property_type","add1","add2","add3","add","postcode.x","postcode.y","postcodelocator","buildingname","buildingnumber","subbuildingname","paostartnumber","paostartsuffix","paoendnumber","paoendsuffix","paotext","saostartnumber","saostartsuffix","saoendnumber","saoendsuffix","saotext","streetdescription","dependentlocality","townname","class","lodgement_date","inspection_date","lodgement_datetime")
needlist3<-c("lmk_key","postcode.y","property_type","uprn","add1","add2","add3","add","postcode.x","postcodelocator","buildingname","buildingnumber","subbuildingname","paostartnumber","paostartsuffix","paoendnumber","paoendsuffix","paotext","saostartnumber","saostartsuffix","saoendnumber","saoendsuffix","saotext","streetdescription","dependentlocality","locality","townname","class","lodgement_date","inspection_date","lodgement_datetime")

#Create function used in the linkage
matchleft <- function(x,y){
  next0 <- x[!(x$lmk_key %in% y$lmk_key),]
  
  return(next0)
}
keepneed <- function(x,y){
  next0 <- x[(x$lmk_key %in% y$lmk_key),]
  
  return(next0)

}
uniqueresult <- function(x){
  dt <- as.data.table(x)
  esummary<-dt[,.(count=.N),by=lmk_key]
  idd1 <- esummary[esummary$count==1,]
  result1 <- x[x$lmk_key %in% idd1$lmk_key,]

  return(result1)
}
doubleresult <-  function(x){
  dt <- as.data.table(x)
  esummary<-dt[,.(count=.N),by=lmk_key]
  idd2 <- esummary[esummary$count!=1,]
  need1 <- x[x$lmk_key %in% idd2$lmk_key,]
  
  return(need1)
}
#################################### section 4:data linkage#################################### 
####################method 1 ####################
#create the funciton for matching rule 1
function1<- function(x,y){
  x<-x[x$saotext=="",]
  x<-x[x$subbuildingname=="",]
  x<-x[x$paotext=="",]
  x<-x[x$saostartnumber=="",]
  x<-x[x$saostartsuffix=="",]
  x<-x[x$saoendnumber=="",]
  x<-x[x$saoendsuffix=="",]
  x<-x[x$paostartsuffix=="",]
  x<-x[x$paoendnumber=="",]
  x<-x[x$paoendsuffix=="",]
  x<-x[x$buildingname=="",]
  x<-x[x$subbuildingname=="",]
  #combine buildingnumber and streetdescription with a comma into bnstreet field
  x$bnstreet <-    paste(x$buildingnumber,x$streetdescription,sep=",")
  #remove the blank space in bnstreet
  x$bnstreet <- gsub(" ", "", x$bnstreet)
  x$addressf <-paste(x$postcodelocator,x$bnstreet,sep=",")
  
  #remove the blank space in add
  y$addressfinal <- trimws(y$add)
  y$addressfinal <- gsub(" ", "", y$addressfinal)
  y$addressf <- paste(y$postcode,y$addressfinal,sep=",")
  taba1 <- inner_join(x,y,by="addressf")
 
  return(taba1)
}
#run the matching rule 1 function
link1<-function1(add,epc)

#keep part of the variables in linked dataset
needlist1<-c("lmk_key","postcode.y","property_type","uprn","add1","add2","add3","add","postcode.x","postcodelocator","buildingname","buildingnumber","subbuildingname","paostartnumber","paostartsuffix","paoendnumber","paoendsuffix","paotext","saostartnumber","saostartsuffix","saoendnumber","saoendsuffix","saotext","streetdescription","locality","dependentlocality","townname","class","lodgement_date","inspection_date","lodgement_datetime")
link1<-link1[,..needlist1]
#Get the one to one linkage result
link1u<- uniqueresult(link1)
#Get the one to many linkage result
link1d <- doubleresult(link1)
#remove the linked records from the original EPC dataset
epc <- matchleft(epc,link1)
#remove the linked result to save memory
rm(link1)
####################method 2####################
function2<- function(x,y){
  x<-x[x$saotext=="",]
  x<-x[x$subbuildingname=="",]
  x$bnstreet <-    paste(x$buildingnumber,x$streetdescription,sep=",")
  x$bnstreet <- gsub(" ", "", x$bnstreet)
  x$addressf <-paste(x$postcodelocator,x$bnstreet,sep=",")
  
  y$addressfinal <- trimws(y$add)
  y$addressfinal <- gsub(" ", "", y$addressfinal)
  y$addressf <- paste(y$postcode,y$addressfinal,sep=",")
  taba1 <- inner_join(x,y,by="addressf")

  return(taba1)
}

link2<-function2(add,epc)
link2<-link2[,..needlist1]

link2u<- uniqueresult(link2)
link2d <- doubleresult(link2)

epc <- matchleft(epc,link2)

rm(link2)
####################method 3####################
function3<- function(x,y){
  x<-x[x$paotext=="",]
  x<-x[x$saostartnumber=="",]
  x<-x[x$saostartsuffix=="",]
  x<-x[x$saoendnumber=="",]
  x<-x[x$saoendsuffix=="",]
  x<-x[x$paostartsuffix=="",]
  x<-x[x$paoendnumber=="",]
  x<-x[x$paoendsuffix=="",]
  x<-x[x$buildingname=="",]
  x<-x[x$subbuildingname=="",]
  
  x$bnstreet <- paste(x$buildingnumber,x$streetdescription,sep=",")
  x$bnstreet <- paste(x$bnstreet,x$townname,sep=",")
  x$bnstreet <- gsub(" ", "", x$bnstreet)
  x$addressf <-paste(x$postcodelocator,x$bnstreet,sep=",")
  
  y$addressfinal <- trimws(y$add)
  y$addressfinal <- gsub(" ", "", y$addressfinal)
  y$addressf <- paste(y$postcode,y$addressfinal,sep=",")
  
  taba1 <- inner_join(x,y,by="addressf")
 
  return(taba1)
}

link3<-function3(add,epc)
link3<-link3[,..needlist1]

link3u<- uniqueresult(link3)
link3d <- doubleresult(link3)

epc <- matchleft(epc,link3)

rm(link3)
####################method 4####################
function4<- function(x,y){
  x$bnstreet <-    paste(x$buildingnumber,x$streetdescription,sep=",")
  x$bnstreet <-    paste(x$bnstreet,x$townname,sep=",")
  x$bnstreet <- gsub(" ", "", x$bnstreet)
  x$addressf <-paste(x$postcodelocator,x$bnstreet,sep=",")
  
  y$addressfinal <- trimws(y$add)
  y$addressfinal <- gsub(" ", "", y$addressfinal)
  y$addressf <- paste(y$postcode,y$addressfinal,sep=",")
  
  taba1 <- inner_join(x,y,by="addressf")
 
  return(taba1)
}

link4<-function4(add,epc)
link4<-link4[,..needlist1]

link4u<- uniqueresult(link4)
link4d <- doubleresult(link4)

epc <- matchleft(epc,link4)

rm(link4)
####################method 5####################
function5<- function(x,y){
  x$bnstreet <-    paste(x$buildingnumber,x$streetdescription,sep=",")

  x$bnstreet <- gsub(" ", "", x$bnstreet)
  x$addressf <-paste(x$postcodelocator,x$bnstreet,sep=",")
  
  y$addressfinal <- trimws(y$add)
  y$addressfinal <- gsub(" ", "", y$addressfinal)
  y$addressf <- paste(y$postcode,y$addressfinal,sep=",")
  
  taba1 <- inner_join(x,y,by="addressf")
 
  return(taba1)
}

link5<-function5(add,epc)
link5<-link5[,..needlist1]

link5u<- uniqueresult(link5)
link5d <- doubleresult(link5)

epc <- matchleft(epc,link5)

rm(link5)
####################method 6####################
function6<- function(x,y){
  x$bnstreet <-    paste(x$paostartnumber,x$paostartsuffix,sep="")
  x$bnstreet <- paste(x$bnstreet,x$streetdescription,sep=",")
  x$bnstreet <- gsub(" ", "", x$bnstreet)
  x$addressf <-paste(x$postcodelocator,x$bnstreet,sep=",")
  
  y$addressfinal <- trimws(y$add)
  y$addressfinal <- gsub(" ", "", y$addressfinal)
  y$addressf <- paste(y$postcode,y$addressfinal,sep=",")
  
  taba1 <- inner_join(x,y,by="addressf")
 
  return(taba1)
}

link6<-function6(add,epc)
link6<-link6[,..needlist1]

link6u<- uniqueresult(link6)
link6d <- doubleresult(link6)

epc <- matchleft(epc,link6)

rm(link6)
####################method 7####################
function7<- function(x,y){
  
  x$bnstreet <-    paste(x$buildingnumber,x$streetdescription,sep=",")
  x$bnstreet <-    paste(x$bnstreet,x$dependentlocality,sep=",")
  x$bnstreet <- gsub(" ", "", x$bnstreet)
  x$addressf <-paste(x$postcodelocator,x$bnstreet,sep=",")
  
  y$addressfinal <- trimws(y$add)
  y$addressfinal <- gsub(" ", "", y$addressfinal)
  y$addressf <- paste(y$postcode,y$addressfinal,sep=",")
  
  taba1 <- inner_join(x,y,by="addressf")

  return(taba1)
}
link7<-function7(add,epc)
link7<-link7[,..needlist1]

link7u<- uniqueresult(link7)
link7d <- doubleresult(link7)

epc <- matchleft(epc,link7)

rm(link7)
####################method 8####################
function8<- function(x,y){
  x$bnstreet <-    paste(x$buildingname,x$streetdescription,sep=",")
  x$bnstreet <-    paste(x$bnstreet,x$dependentlocality,sep=",")
  x$bnstreet <- gsub(" ", "", x$bnstreet)
  x$addressf <-paste(x$postcodelocator,x$bnstreet,sep=",")
  
  y$addressfinal <- trimws(y$add)
  y$addressfinal <- gsub(" ", "", y$addressfinal)
  y$addressf <- paste(y$postcode,y$addressfinal,sep=",")
  
  taba1 <- inner_join(x,y,by="addressf")
 
  return(taba1)
}
link8<-function8(add,epc)
link8<-link8[,..needlist1]

link8u<- uniqueresult(link8)
link8d <- doubleresult(link8)

epc <- matchleft(epc,link8)

rm(link8)
####################method 9####################
function9<- function(x,y){
  x$bnstreet <-    paste(x$subbuildingname,x$buildingname,sep=" ")
  x$bnstreet <-    paste(x$bnstreet,x$streetdescription,sep=",")
  x$bnstreet <- gsub(" ", "", x$bnstreet)
  x$addressf <-paste(x$postcodelocator,x$bnstreet,sep=",")
  
  y$addressfinal <- trimws(y$add)
  y$addressfinal <- gsub(" ", "", y$addressfinal)
  y$addressf <- paste(y$postcode,y$addressfinal,sep=",")
  
  taba1 <- inner_join(x,y,by="addressf")

  return(taba1)
}
link9<-function9(add,epc)
link9<-link9[,..needlist1]

link9u<- uniqueresult(link9)
link9d <- doubleresult(link9)

epc <- matchleft(epc,link9)

rm(link9)
####################method 10####################
function10<- function(x,y){
  x$bnstreet <-    paste(x$subbuildingname,x$buildingname,sep=" ")
  x$bnstreet <-    paste(x$bnstreet,x$buildingnumber,sep=",")
  x$bnstreet <-    paste(x$bnstreet,x$streetdescription,sep=",")
  x$bnstreet <- gsub(" ", "", x$bnstreet)
  x$addressf <-paste(x$postcodelocator,x$bnstreet,sep=",")
  
  y$addressfinal <- trimws(y$add)
  y$addressfinal <- gsub(" ", "", y$addressfinal)
  y$addressf <- paste(y$postcode,y$addressfinal,sep=",")
  
  taba1 <- inner_join(x,y,by="addressf")
 
  return(taba1)
}

link10<-function10(add,epc)
link10<-link10[,..needlist1]

link10u<- uniqueresult(link10)
link10d <- doubleresult(link10)

epc <- matchleft(epc,link10)

rm(link10)
####################method 11####################
function11<- function(x,y){
  #x<-x[is.na(x$buildingnumber),]
  x<-x[x$buildingnumber=="",]
  x$bnstreet <-    paste(x$buildingname,x$streetdescription,sep=",")
  x$bnstreet <- gsub("['] ", "", x$bnstreet)
  x$bnstreet <- gsub("[.]", "", x$bnstreet)
  x$bnstreet <- gsub(" ", "", x$bnstreet)
  x$addressf <-paste(x$postcodelocator,x$bnstreet,sep=",")
  
  y$addressfinal <- trimws(y$add)
  y$addressfinal <- gsub("[']", "", y$addressfinal)
  y$addressfinal <- gsub("[.]", "", y$addressfinal)
  y$addressfinal <- gsub(" ", "", y$addressfinal)
  y$addressf <- paste(y$postcode,y$addressfinal,sep=",")
  
  taba1 <- inner_join(x,y,by="addressf")
  return(taba1)
}

link11<-function11(add,epc)
link11<-link11[,..needlist1]

link11u<- uniqueresult(link11)
link11d <- doubleresult(link11)

epc <- matchleft(epc,link11)
rm(link11)
####################section sum up(method 1 to method 11)####################
#create a method vairable to record the linkage method ID
link1u$method<-"link1u"
link2u$method<-"link2u"
link3u$method<-"link3u"
link4u$method<-"link4u"
link5u$method<-"link5u"
link6u$method<-"link6u"
link7u$method<-"link7u"
link8u$method<-"link8u"
link9u$method<-"link9u"
link10u$method<-"link10u"
link11u$method<-"link11u"

link1d$method<-"link1d"
link2d$method<-"link2d"
link3d$method<-"link3d"
link4d$method<-"link4d"
link5d$method<-"link5d"
link6d$method<-"link6d"
link7d$method<-"link7d"
link8d$method<-"link8d"
link9d$method<-"link9d"
link10d$method<-"link10d"
link11d$method<-"link11d"
#combine all the one to one linkage results
l1_11u = list(link1u,link2u,link3u,link4u,link5u,link6u,link7u,link8u,link9u,link10u,link11u)
link1_11u<- rbindlist(l1_11u)
#combine all one to many linkage results
l1_11d = list(link1d,link2d,link3d,link4d,link5d,link6d,link7d,link8d,link9d,link10d,link11d)
link1_11d<- rbindlist(l1_11d)

#save the linked data in PostGIS
dbWriteTable(con, "link1_11dnew",value =link1_11d, append = TRUE, row.names = FALSE)
dbWriteTable(con, "link1_11unew",value =link1_11u, append = TRUE, row.names = FALSE)

#delete temporal result in the above linkage methods
rm(link1u,link2u,link3u,link4u,link5u,link6u,link7u,link8u,link9u,link10u,link11u)
rm(link1d,link2d,link3d,link4d,link5d,link6d,link7d,link8d,link9d,link10d,link11d)
rm(l1_11u,l1_11d)
#delete linkage functions in the above linkage methods
rm(function1,function2,function3,function4,function5,function6,function7,function8,function9,function10,function11)

####################method 12####################
function12<- function(x,y){
  x$bnstreet <-    paste(x$buildingname,x$dependentlocality,sep=",")
  
  x$bnstreet <- gsub(" ", "", x$bnstreet)
  x$addressf <-paste(x$postcodelocator,x$bnstreet,sep=",")
  
  y$addressfinal <- trimws(y$add)
  #y$addressfinal <- gsub("/", "", y$addressfinal)
  y$addressfinal <- gsub(" ", "", y$addressfinal)
  y$addressf <- paste(y$postcode,y$addressfinal,sep=",")
  
  taba1 <- inner_join(x,y,by="addressf")

  return(taba1)
}
link12<-function12(add,epc)
link12<-link12[,..needlist1]

link12u<- uniqueresult(link12)
link12d <- doubleresult(link12)

epc <- matchleft(epc,link12)
####################method 13####################
function13<- function(x,y){
  x$bnstreet <-    paste(x$subbuildingname,x$buildingnumber,sep=",")
  x$bnstreet <-    paste(x$bnstreet,x$streetdescription,sep=",")
  x$bnstreet <- gsub(" ", "", x$bnstreet)
  x$addressf <-paste(x$postcodelocator,x$bnstreet,sep=",")
  
  y$addressfinal <- trimws(y$add)
  y$addressfinal <- gsub(" ", "", y$addressfinal)
  y$addressf <- paste(y$postcode,y$addressfinal,sep=",")
  
  taba1 <- inner_join(x,y,by="addressf")
 
  return(taba1)
  
}


link13<-function13(add,epc)
link13<-link13[,..needlist1]

link13u<- uniqueresult(link13)
link13d <- doubleresult(link13)

epc <- matchleft(epc,link13)
####################method 14####################
function14<- function(x,y){
  x$bnstreet <-    paste(x$subbuildingname,x$buildingname,sep=",")
  x$bnstreet <-    paste(x$bnstreet,x$buildingnumber,sep=",")
  x$bnstreet <-    paste(x$bnstreet,x$streetdescription,sep=",")
  x$bnstreet <- paste(x$bnstreet,x$dependentlocality,sep=",")
  x$bnstreet <- gsub(" ", "", x$bnstreet)
  x$addressf <-paste(x$postcodelocator,x$bnstreet,sep=",")
  
  y$addressfinal <- trimws(y$add)
  y$addressfinal <- gsub(" ", "", y$addressfinal)
  y$addressf <- paste(y$postcode,y$addressfinal,sep=",")
  
  taba1 <- inner_join(x,y,by="addressf")
 
  return(taba1)
  
}
link14<-function14(add,epc)
link14<-link14[,..needlist1]


link14u<- uniqueresult(link14)
link14d <- doubleresult(link14)

epc <- matchleft(epc,link14)
####################method 15####################
function15<- function(x,y){
  
  x$bnstreet <-    paste(x$buildingnumber,x$streetdescription,sep=",")
  x$bnstreet <- gsub("[']", "", x$bnstreet)
  x$bnstreet <- gsub("[.]", "", x$bnstreet)
  x$bnstreet <- gsub(" ", "", x$bnstreet)
  x$addressf <-paste(x$postcodelocator,x$bnstreet,sep=",")
  
  y$addressfinal <- trimws(y$add)
  y$addressfinal <- gsub("[']", "", y$addressfinal)
  y$addressfinal <- gsub("[.]", "", y$addressfinal)
  y$addressfinal <- gsub(" ", "", y$addressfinal)
  y$addressf <- paste(y$postcode,y$addressfinal,sep=",")
  
  taba1 <- inner_join(x,y,by="addressf")
 
  return(taba1)
  
}
link15<-function15(add,epc)
link15<-link15[,..needlist1]

link15u<- uniqueresult(link15)
link15d <- doubleresult(link15)

epc <- matchleft(epc,link15)
####################method 16####################
function16<- function(x,y){
  
  x$bnstreet <-    paste(x$saotext,x$paotext,sep=",")
  x$bnstreet <-    paste(x$bnstreet,x$streetdescription,sep=",")
  x$bnstreet <- gsub(" ", "", x$bnstreet)
  x$addressf <-paste(x$postcodelocator,x$bnstreet,sep=",")
  
  y$addressfinal <- trimws(y$add)
  y$addressfinal <- gsub(" ", "", y$addressfinal)
  y$addressf <- paste(y$postcode,y$addressfinal,sep=",")
  
  taba1 <- inner_join(x,y,by="addressf")
  return(taba1)
  
}

link16<-function16(add,epc)
link16<-link16[,..needlist1]


link16u<- uniqueresult(link16)
link16d <- doubleresult(link16)

epc <- matchleft(epc,link16)
####################method 17####################
function17<- function(x,y){
  
  x$bnstreet <-    paste(x$saotext,x$buildingname,sep=",")
  x$bnstreet <-    paste(x$bnstreet,x$streetdescription,sep=",")
  x$bnstreet <-    paste(x$bnstreet,x$dependentlocality ,sep=",")
  x$bnstreet <- gsub(" ", "", x$bnstreet)
  x$addressf <-paste(x$postcodelocator,x$bnstreet,sep=",")
  
  y$addressfinal <- trimws(y$add)
  y$addressfinal <- gsub(" ", "", y$addressfinal)
  y$addressf <- paste(y$postcode,y$addressfinal,sep=",")
  
  taba1 <- inner_join(x,y,by="addressf")
  return(taba1)
  
}
link17<-function17(add,epc)
link17<-link17[,..needlist1]

link17u<- uniqueresult(link17)
link17d <- doubleresult(link17)

epc <- matchleft(epc,link17)
####################method 18####################
function18<- function(x,y){
  #x<-x[is.na(x$paostartsuffix),]
  x<-x[x$paostartsuffix=="",]
  x$bnstreet <-    paste(x$saotext,x$paostartnumber,sep=",")
  x$bnstreet <-    paste(x$bnstreet,x$streetdescription,sep=",")
  x$bnstreet <-    paste(x$bnstreet,x$dependentlocality ,sep=",")
  x$bnstreet <- gsub(" ", "", x$bnstreet)
  x$addressf <-paste(x$postcodelocator,x$bnstreet,sep=",")
  
  y$addressfinal <- trimws(y$add)
  y$addressfinal <- gsub(" ", "", y$addressfinal)
  y$addressf <- paste(y$postcode,y$addressfinal,sep=",")
  
  taba1 <- inner_join(x,y,by="addressf")
  return(taba1)
  
}

link18<-function18(add,epc)
link18<-link18[,..needlist1]

link18u<- uniqueresult(link18)
link18d <- doubleresult(link18)

epc <- matchleft(epc,link18)
####################method 19####################
function19<- function(x,y){
  # x<-x[is.na(x$buildingnumber),]
  # x<-x[is.na(x$buildingname),]
  # x<-x[is.na(x$subbuildingname),]
  x<-x[x$buildingnumber=="",]
  x<-x[x$buildingname=="",]
  x<-x[x$subbuildingname=="",]
  x$bnstreet <-    paste(x$paotext,x$streetdescription ,sep=",")
  x$bnstreet <- gsub(" ", "", x$bnstreet)
  x$addressf <-paste(x$postcodelocator,x$bnstreet,sep=",")
  
  y$addressfinal <- paste(y$add)
  y$addressfinal <- gsub(" ", "", y$addressfinal)
  y$addressf <- paste(y$postcode,y$addressfinal,sep=",")
  
  taba1 <- inner_join(x,y,by="addressf")
  return(taba1)
}

link19<-function19(add,epc)
link19<-link19[,..needlist1]

link19u<- uniqueresult(link19)
link19d <- doubleresult(link19)

epc <- matchleft(epc,link19)
####################method 20####################
function20<- function(x,y){
  
  x$bnstreet <-    paste(x$paotext,x$locality,sep=",")
  x$bnstreet <-    paste(x$bnstreet,x$townname,sep=",")
  x$bnstreet <- gsub(" ", "", x$bnstreet)
  x$addressf <-paste(x$postcodelocator,x$bnstreet,sep=",")
  
  y$addressfinal <- paste(y$add)
  y$addressfinal <- gsub(" ", "", y$addressfinal)
  y$addressf <- paste(y$postcode,y$addressfinal,sep=",")
  
  taba1 <- inner_join(x,y,by="addressf")
  
  return(taba1)
}
link20<-function20(add,epc)
link20<-link20[,..needlist1]


link20u<- uniqueresult(link20)
link20d <- doubleresult(link20)

epc <- matchleft(epc,link20)
####################method 21####################
function21<- function(x,y){
  x$bnstreet <-    paste(x$subbuildingname,x$buildingnumber,sep=",")
  x$bnstreet <-    paste(x$bnstreet,x$streetdescription,sep=" ")
  x$bnstreet <- paste(x$bnstreet,x$dependentlocality,sep=",")
  x$bnstreet <- gsub(" ", "", x$bnstreet)
  x$addressf <-paste(x$postcodelocator,x$bnstreet,sep=",")
  
  y$addressfinal <- trimws(y$add)
  y$addressfinal <- gsub(" ", "", y$addressfinal)
  y$addressf <- paste(y$postcode,y$addressfinal,sep=",")
  
  taba1 <- inner_join(x,y,by="addressf")
  return(taba1)
  
}
link21<-function21(add,epc)
link21<-link21[,..needlist1]

link21u<- uniqueresult(link21)
link21d <- doubleresult(link21)

epc <- matchleft(epc,link21)
####################method 22####################
function22<- function(x,y){
  x$bnstreet <-    paste(x$subbuildingname,x$buildingnumber,sep=",")
  x$bnstreet <-    paste(x$bnstreet,x$streetdescription,sep=",")
  x$bnstreet <- paste(x$bnstreet,x$dependentlocality,sep=",")
  x$bnstreet <- gsub(" ", "", x$bnstreet)
  x$addressf <-paste(x$postcodelocator,x$bnstreet,sep=",")
  
  y$addressfinal <- trimws(y$add)
  y$addressfinal <- gsub(" ", "", y$addressfinal)
  y$addressf <- paste(y$postcode,y$addressfinal,sep=",")
  
  taba1 <- inner_join(x,y,by="addressf")
  return(taba1)
  
}

link22<-function22(add,epc)
link22<-link22[,..needlist1]

link22u<- uniqueresult(link22)
link22d <- doubleresult(link22)

epc <- matchleft(epc,link22)
####################method 23####################
function23<- function(x,y){
  
  
  x$bnstreet <-    paste(x$paostartnumber,x$paoendnumber,sep="-")
  x$bnstreet <- paste(x$bnstreet,x$streetdescription,sep=",")
  x$bnstreet <- gsub(" ", "", x$bnstreet)
  x$addressf <-paste(x$postcodelocator,x$bnstreet,sep=",")
  
  y$addressfinal <- trimws(y$add)
  y$addressfinal <- gsub(" ", "", y$addressfinal)
  y$addressf <- paste(y$postcode,y$addressfinal,sep=",")
  
  taba1 <- inner_join(x,y,by="addressf")
  return(taba1)
  
}
link23<-function23(add,epc)
link23<-link23[,..needlist1]

link23u<- uniqueresult(link23)
link23d <- doubleresult(link23)

epc <- matchleft(epc,link23)
####################method 24####################
function24<- function(x,y){
  
  x$bnstreet <-    paste(x$subbuildingname,x$buildingname,sep=" ")
  x$bnstreet <-    paste(x$bnstreet,x$buildingnumber,sep=",")
  x$bnstreet <-    paste(x$bnstreet,x$streetdescription,sep=",")
  x$bnstreet <- paste(x$bnstreet,x$dependentlocality,sep=",")
  x$bnstreet <- gsub(" ", "", x$bnstreet)
  x$addressf <-paste(x$postcodelocator,x$bnstreet,sep=",")
  
  y$addressfinal <- trimws(y$add)
  y$addressfinal <- gsub(" ", "", y$addressfinal)
  y$addressf <- paste(y$postcode,y$addressfinal,sep=",")
  
  taba1 <- inner_join(x,y,by="addressf")
  return(taba1)
  
}
link24<-function24(add,epc)
link24<-link24[,..needlist1]

link24u<- uniqueresult(link24)
link24d <- doubleresult(link24)

epc <- matchleft(epc,link24)
####################method 25####################
function25<- function(x,y){
  
  x$bnstreet <-    paste(x$subbuildingname,x$buildingname,sep=",")
  x$bnstreet <-    paste(x$bnstreet,x$streetdescription,sep=",")
  
  x$bnstreet <- gsub(" ", "", x$bnstreet)
  x$addressf <-paste(x$postcodelocator,x$bnstreet,sep=",")
  
  y$addressfinal <- trimws(y$add)
  
  y$addressfinal <- gsub(" ", "", y$addressfinal)
  y$addressf <- paste(y$postcode,y$addressfinal,sep=",")
  
  taba1 <- inner_join(x,y,by="addressf")
  return(taba1)
  
}
link25<-function25(add,epc)
link25<-link25[,..needlist1]

link25u<- uniqueresult(link25)
link25d <- doubleresult(link25)

epc <- matchleft(epc,link25)
####################method 26####################
function26<- function(x,y){
  x$bnstreet <-    paste(x$subbuildingname,x$buildingname,sep=",")
  x$bnstreet <-    paste(x$bnstreet,x$streetdescription,sep=" ")
  
  x$bnstreet <- gsub(" ", "", x$bnstreet)
  x$addressf <-paste(x$postcodelocator,x$bnstreet,sep=",")
  
  y$addressfinal <- trimws(y$add)
  
  y$addressfinal <- gsub(" ", "", y$addressfinal)
  y$addressf <- paste(y$postcode,y$addressfinal,sep=",")
  
  taba1 <- inner_join(x,y,by="addressf")
  return(taba1)
  
}
link26<-function26(add,epc)
link26<-link26[,..needlist1]

link26u<- uniqueresult(link26)
link26d <- doubleresult(link26)

epc <- matchleft(epc,link26)
####################sum up section 2####################
link12u$method<-"link12u"
link13u$method<-"link13u"
link14u$method<-"link14u"
link15u$method<-"link15u"
link16u$method<-"link16u"
link17u$method<-"link17u"
link18u$method<-"link18u"
link19u$method<-"link19u"
link20u$method<-"link20u"
link21u$method<-"link21u"
link22u$method<-"link22u"
link23u$method<-"link23u"
link24u$method<-"link24u"
link25u$method<-"link25u"
link26u$method<-"link26u"

link12d$method<-"link12d"
link13d$method<-"link13d"
link14d$method<-"link14d"
link15d$method<-"link15d"
link16d$method<-"link16d"
link17d$method<-"link17d"
link18d$method<-"link18d"
link19d$method<-"link19d"
link20d$method<-"link20d"
link21d$method<-"link21d"
link22d$method<-"link22d"
link23d$method<-"link23d"
link24d$method<-"link24d"
link25d$method<-"link25d"
link26d$method<-"link26d"


l12_26u = list(link12u,link13u,link14u,link15u,link16u,link17u,link18u,link19u,link20u,link21u,link22u,link23u,link24u,link25u,link26u)
link12_26u<- rbindlist(l12_26u, use.names=TRUE, fill=TRUE)

l12_26d = list(link12d,link13d,link14d,link15d,link16d,link17d,link18d,link19d,link20d,link21d,link22d,link23d,link24d,link25d,link26d)
link12_26d<- rbindlist(l12_26d, use.names=TRUE, fill=TRUE)

dbWriteTable(con, "link12_26unew",value =link12_26u, append = TRUE, row.names = FALSE)
dbWriteTable(con, "link12_26dnew",value =link12_26d, append = TRUE, row.names = FALSE)


rm(link12,link13,link14,link15,link16,link17,link18,link19,link20,link21,link22,link23,link24,link25,link26)
rm(link12u,link13u,link14u,link15u,link16u,link17u,link18u,link19u,link20u,link21u,link22u,link23u,link24u,link25u,link26u)
rm(link12d,link13d,link14d,link15d,link16d,link17d,link18d,link19d,link20d,link21d,link22d,link23d,link24d,link25d,link26d)
rm(l12_26u,l12_26d)
rm(function12,function13,function14,function15,function16,function17,function18,function19,function20,function21,function22,function23,function24,function25,function26)


####################method 27####################
function27<- function(x,y){
  x$bnstreet <-    paste(x$subbuildingname,x$buildingnumber,sep=" ")
  x$bnstreet <-    paste(x$bnstreet,x$streetdescription,sep=" ")
  x$addressf <-paste(x$postcodelocator,x$bnstreet,sep=",")
  
  y$addressfinal <- trimws(y$add)
  y$addressf <- paste(y$postcode,y$addressfinal,sep=",")

  taba1 <- inner_join(x,y,by="addressf")
  return(taba1)
}
link27<-function27(add,epc)

link27<-link27[,..needlist1]

link27u<- uniqueresult(link27)
link27d <- doubleresult(link27)

epc <- matchleft(epc,link27)
####################method 28####################
function28<- function(x,y){
  
  x$bnstreet <-    paste(x$saotext,x$buildingnumber,sep=" ")
  x$bnstreet <-    paste(x$bnstreet,x$streetdescription,sep=" ")
  x$addressf <-paste(x$postcodelocator,x$bnstreet,sep=",")
  
  y$addressfinal <- trimws(y$add)
  y$addressf <- paste(y$postcode,y$addressfinal,sep=",")
  
  taba1 <- inner_join(x,y,by="addressf")
  return(taba1)
  
}
link28<-function28(add,epc)
link28<-link28[,..needlist1]

link28u<- uniqueresult(link28)
link28d <- doubleresult(link28)

epc <- matchleft(epc,link28)
####################method 29####################
function29<- function(x,y){
  
  x$bnstreet <-    paste(x$saotext,x$pp,sep=" ")
  x$bnstreet <-    paste(x$bnstreet,x$streetdescription,sep=" ")
  x$addressf <-paste(x$postcodelocator,x$bnstreet,sep=",")
  
  y$addressfinal <- trimws(y$add)
  y$addressf <- paste(y$postcode,y$addressfinal,sep=",")
  
  taba1 <- inner_join(x,y,by="addressf")
  return(taba1)
}
link29<-function29(add,epc)
link29<-link29[,..needlist1]

link29u<- uniqueresult(link29)
link29d <- doubleresult(link29)

epc <- matchleft(epc,link29)
####################method 30####################
function30<- function(x,y){
  x$bnstreet <-    x$buildingname
  x$addressf <-paste(x$postcodelocator,x$bnstreet,sep=",")
  
  y$addressfinal <- trimws(y$add)
  y$addressf <- paste(y$postcode,y$addressfinal,sep=",")
  
  taba1 <- inner_join(x,y,by="addressf")
  return(taba1)
}
link30<-function30(add,epc)
link30<-link30[,..needlist1]

link30u<- uniqueresult(link30)
link30d <- doubleresult(link30)

epc <- matchleft(epc,link30)
####################method 31####################
function31<- function(x,y){
  x$ss <- trimws(x$ss)
  x$paotext <- trimws(x$paotext)
  x$bnstreet <-    paste(x$ss,x$paotext,sep=" ")
  x$addressf <-paste(x$postcodelocator,x$bnstreet,sep=",")
  
  y$addressfinal <- trimws(y$add)
  y$addressf <- paste(y$postcode,y$addressfinal,sep=",")
  
  taba1 <- inner_join(x,y,by="addressf")
  return(taba1)
  
}
link31<-function31(add,epc)
link31<-link31[,..needlist1]

link31u<- uniqueresult(link31)
link31d <- doubleresult(link31)

epc <- matchleft(epc,link31)
####################method 32####################
function32<- function(x,y){
  x$pp <- trimws(x$pp)
  x$bnstreet <-    paste(x$pp,x$streetdescription,sep=" ")
  x$addressf <-paste(x$postcodelocator,x$bnstreet,sep=",")
  
  y$addressfinal <- trimws(y$add)
  y$addressf <- paste(y$postcode,y$addressfinal,sep=",")
  
  taba1 <- inner_join(x,y,by="addressf")
  return(taba1)
}
link32<-function32(add,epc)
link32<-link32[,..needlist1]

link32u<- uniqueresult(link32)
link32d <- doubleresult(link32)

epc <- matchleft(epc,link32)
####################method 33####################
function33<- function(x,y){
  x$bnstreet <-    paste(x$subbuildingname,x$buildingnumber,sep=",")
  x$bnstreet <- gsub(" ", "", x$bnstreet)
  x$addressf <-paste(x$postcodelocator,x$bnstreet,sep=",")
  
  y$addressfinal <- trimws(y$add)
  y$addressfinal <- gsub(" ", "", y$addressfinal)
  y$addressf <- paste(y$postcode,y$addressfinal,sep=",")
  
  taba1 <- inner_join(x,y,by="addressf")
  return(taba1)

}
link33<-function33(add,epc)
link33<-link33[,..needlist1]

link33u<- uniqueresult(link33)
link33d <- doubleresult(link33)

epc <- matchleft(epc,link33)
####################method 34####################
function34<- function(x,y){
  x$bnstreet <-    paste(x$buildingnumber,x$paotext,sep=",")
  x$bnstreet <- gsub(" ", "", x$bnstreet)
  x$addressf <-paste(x$postcodelocator,x$bnstreet,sep=",")
  
  y$addressfinal <- trimws(y$add)
  y$addressfinal <- gsub(" ", "", y$addressfinal)
  y$addressf <- paste(y$postcode,y$addressfinal,sep=",")
  
  taba1 <- inner_join(x,y,by="addressf")
  return(taba1)
}
link34<-function34(add,epc)
link34<-link34[,..needlist1]

link34u<- uniqueresult(link34)
link34d <- doubleresult(link34)

epc<- matchleft(epc,link34)
####################method 35####################
function35<- function(x,y){
  x$bnstreet <-    paste(x$ss,x$paotext,sep=",")
  x$bnstreet <- gsub(" ", "", x$bnstreet)
  x$addressf <-paste(x$postcodelocator,x$bnstreet,sep=",")
  
  y$addressfinal <- trimws(y$add)
  y$addressfinal <- gsub(" ", "", y$addressfinal)
  y$addressf <- paste(y$postcode,y$addressfinal,sep=",")
  
  taba1 <- inner_join(x,y,by="addressf")
  return(taba1)
  
}
link35<-function35(add,epc)
link35<-link35[,..needlist1]

link35u<- uniqueresult(link35)
link35d <- doubleresult(link35)

epc <- matchleft(epc,link35)
####################method 36####################
function36<- function(x,y){
  x$bnstreet <-    x$buildingname
  x$addressf <-paste(x$postcodelocator,x$bnstreet,sep=",")
  
  y$addressfinal <- trimws(y$add)
  y$addressfinal <- gsub(",", "", y$addressfinal)
  y$addressf <- paste(y$postcode,y$addressfinal,sep=",")
  
  taba1 <- inner_join(x,y,by="addressf")
  return(taba1)
}

link36<-function36(add,epc)
link36<-link36[,..needlist1]

link36u<- uniqueresult(link36)
link36d <- doubleresult(link36)

epc <- matchleft(epc,link36)
####################method 37####################
function37<- function(x,y){
  x$bnstreet <-    x$buildingname
  y$addressfinal <- gsub("[.]", "", y$addressfinal)
  y$addressfinal <- gsub("[-]", "", y$addressfinal)
  y$addressfinal <- gsub("[']", "", y$addressfinal)
  y$addressfinal <- gsub("[/]", "", y$addressfinal)
  y$addressfinal <- gsub(",", "", y$addressfinal)
  #y$addressfinal <- gsub(" ", "", y$addressfinal)
  x$addressf <-paste(x$postcodelocator,x$bnstreet,sep=",")
  
  y$addressfinal <- trimws(y$add)
  y$addressfinal <- gsub("[.]", "", y$addressfinal)
  y$addressfinal <- gsub("[-]", "", y$addressfinal)
  y$addressfinal <- gsub("[']", "", y$addressfinal)
  y$addressfinal <- gsub("[/]", "", y$addressfinal)
  y$addressfinal <- gsub(",", "", y$addressfinal)
  #y$addressfinal <- gsub(" ", "", y$addressfinal)
  y$addressf <- paste(y$postcode,y$addressfinal,sep=",")
  
  taba1 <- inner_join(x,y,by="addressf")
  return(taba1)
}

link37<-function37(add,epc)
link37<-link37[,..needlist1]

link37u<- uniqueresult(link37)
link37d <- doubleresult(link37)

epc <- matchleft(epc,link37)
####################method 38####################
function38<- function(x,y){
  x$bnstreet <-    paste(x$ss,x$paotext,sep=",")
  y$addressfinal <- gsub("[.]", "", y$addressfinal)
  y$addressfinal <- gsub("[-]", "", y$addressfinal)
  y$addressfinal <- gsub("[']", "", y$addressfinal)
  y$addressfinal <- gsub("[/]", "", y$addressfinal)
  y$addressfinal <- gsub(",", "", y$addressfinal)
  x$bnstreet <- gsub(" ", "", x$bnstreet)
  x$addressf <-paste(x$postcodelocator,x$bnstreet,sep=",")
  
  y$addressfinal <- trimws(y$add)
  y$addressfinal <- gsub("[.]", "", y$addressfinal)
  y$addressfinal <- gsub("[-]", "", y$addressfinal)
  y$addressfinal <- gsub("[']", "", y$addressfinal)
  y$addressfinal <- gsub("[/]", "", y$addressfinal)
  #y$addressfinal <- gsub(",", "", y$addressfinal)
  y$addressfinal <- gsub(" ", "", y$addressfinal)
  y$addressf <- paste(y$postcode,y$addressfinal,sep=",")
  
  taba1 <- inner_join(x,y,by="addressf")
  return(taba1)
  
}
link38<-function38(add,epc)
link38<-link38[,..needlist1]

link38u<- uniqueresult(link38)
link38d <- doubleresult(link38)

epc <- matchleft(epc,link38)
####################method 39####################
function39<- function(x,y){
  x$bnstreet <-    paste(x$saotext,x$paotext,sep=" ")
  x$bnstreet <-    paste(x$bnstreet,x$paostartnumber,sep=",")
  x$bnstreet <-    paste(x$bnstreet,x$paoendnumber,sep="-")
  x$bnstreet <-    paste(x$bnstreet,x$streetdescription,sep=" ")
  x$bnstreet <- gsub(" ", "", x$bnstreet)
  x$addressf <-paste(x$postcodelocator,x$bnstreet,sep=",")
  
  y$addressfinal <- trimws(y$add) 
  y$addressfinal <- gsub(" ", "", y$addressfinal)
  y$addressf <- paste(y$postcode,y$addressfinal,sep=",")
  
  taba1 <- inner_join(x,y,by="addressf")
  return(taba1)
}
link39<-function39(add,epc)
link39<-link39[,..needlist1]

link39u<- uniqueresult(link39)
link39d <- doubleresult(link39)

epc <- matchleft(epc,link39)
####################method 40####################
function40<- function(x,y){
  
  x$bnstreet <-    paste(x$buildingnumber,x$paotext,sep=",")
  #y$addressfinal <- gsub("[.]", "", y$addressfinal)

  x$bnstreet <- gsub(" ", "", x$bnstreet)
  x$addressf <-paste(x$postcodelocator,x$bnstreet,sep=",")
  
  y$addressfinal <- trimws(y$add)
  y$addressfinal <- gsub("[.]", "", y$addressfinal)
  y$addressfinal <- gsub("[']", "", y$addressfinal)
  y$addressfinal <- gsub(" ", "", y$addressfinal)
  y$addressf <- paste(y$postcode,y$addressfinal,sep=",")
  
  taba1 <- inner_join(x,y,by="addressf")
  return(taba1)

}
link40<-function40(add,epc)
link40<-link40[,..needlist1]

link40u<- uniqueresult(link40)
link40d <- doubleresult(link40)

epc <- matchleft(epc,link40)
####################method 41####################
function41<- function(x,y){
  x$bnstreet <-    paste(x$ss,x$paotext,sep=" ")
  #x$bnstreet <- gsub(",", "", x$bnstreet)
  x$addressf <-paste(x$postcodelocator,x$bnstreet,sep=",")
  
  y$addressfinal <- trimws(y$add)
  y$addressfinal <- gsub(",", "", y$addressfinal)
  y$addressf <- paste(y$postcode,y$addressfinal,sep=",")
  
  taba1 <- inner_join(x,y,by="addressf")
  return(taba1)
}
link41<-function41(add,epc)
link41<-link41[,..needlist1]


link41u<- uniqueresult(link41)
link41d <- doubleresult(link41)

epc <- matchleft(epc,link41)
####################method 42####################
function42<- function(x,y){
  
  x$bnstreet <-    paste(x$subbuildingname,x$paotext,sep=" ")
  x$bnstreet <- gsub(" ", "", x$bnstreet)
  x$addressf <-paste(x$postcodelocator,x$bnstreet,sep=",")
  
  y$addressfinal <- trimws(y$add)
  y$addressfinal <- gsub(" ", "", y$addressfinal)
  y$addressf <- paste(y$postcode,y$addressfinal,sep=",")
  
  taba1 <- inner_join(x,y,by="addressf")
  return(taba1)

}
link42<-function42(add,epc)

link42<-link42[,..needlist1]

link42u<- uniqueresult(link42)
link42d <- doubleresult(link42)

epc <- matchleft(epc,link42)
####################method 43####################
function43<- function(x,y){
  x$bnstreet <-    paste(x$saotext,x$paotext,sep=" ")
  x$bnstreet <- gsub(" ", "", x$bnstreet)
  x$addressf <-paste(x$postcodelocator,x$bnstreet,sep=",")
  
  y$addressfinal <- trimws(y$add)
  y$addressfinal <- gsub(" ", "", y$addressfinal)
  y$addressf <- paste(y$postcode,y$addressfinal,sep=",")
  
  taba1 <- inner_join(x,y,by="addressf")
  return(taba1)
  
}
link43<-function43(add,epc)

link43<-link43[,..needlist1]

link43u<- uniqueresult(link43)
link43d <- doubleresult(link43)

epc <- matchleft(epc,link43)
####################method 44####################
function44<- function(x,y){
  x<-x[x$buildingname!="",]
  
  x$bnstreet <- beg2char(x$buildingname, " ")
  x$bnstreet <-    paste(x$bnstreet,x$streetdescription,sep=" ")
  x$bnstreet <- gsub(" ", "", x$bnstreet)
  x$addressf <-paste(x$postcodelocator,x$bnstreet,sep=",")
  
  y$addressfinal <- trimws(y$add)
  
  y$addressfinal <- gsub(" ", "", y$addressfinal)
  y$addressf <- paste(y$postcode,y$addressfinal,sep=",")
  
  taba1 <- inner_join(x,y,by="addressf")
  return(taba1)
}
link44<-function44(add,epc)

link44<-link44[,..needlist1]

link44u<- uniqueresult(link44)
link44d <- doubleresult(link44)

epc<- matchleft(epc,link44)
####################method 45####################
function45<- function(x,y){
  x$bnstreet <-    paste(x$pp,x$streetdescription,sep=" ")
  x$bnstreet <- gsub(" ", "", x$bnstreet)
  x$addressf <-paste(x$postcodelocator,x$bnstreet,sep=",")
  
  y$addressfinal <- trimws(y$add)
  
  y$addressfinal <- gsub(" ", "", y$addressfinal)
  y$addressf <- paste(y$postcode,y$addressfinal,sep=",")
  
  taba1 <- inner_join(x,y,by="addressf")
  return(taba1)
}

link45<-function45(add,epc)
link45<-link45[,..needlist1]
link45u<- uniqueresult(link45)
link45d <- doubleresult(link45)

epc <- matchleft(epc,link45)
####################method 46####################
function46<- function(x,y){
  x$bnstreet <-    paste(x$ss,x$streetdescription,sep=" ")
  x$bnstreet <- gsub(" ", "", x$bnstreet)
  x$addressf <-paste(x$postcodelocator,x$bnstreet,sep=",")
  
  y$addressfinal <- trimws(y$add)
  
  y$addressfinal <- gsub(" ", "", y$addressfinal)
  y$addressf <- paste(y$postcode,y$addressfinal,sep=",")
  
  taba1 <- inner_join(x,y,by="addressf")
  return(taba1)
}
link46<-function46(add,epc)
link46<-link46[,..needlist1]

link46u<- uniqueresult(link46)
link46d <- doubleresult(link46)

epc <- matchleft(epc,link46)
####################method 47####################
function47<- function(x,y){
  x$bnstreet <-    paste(x$buildingnumber,x$streetdescription,sep=" ")
  
  #$bnstreet <- gsub("[']", "", x$bnstreet)
  x$bnstreet <- gsub(" ", "", x$bnstreet)
  x$addressf <-paste(x$postcodelocator,x$bnstreet,sep=",")
  
  y$addressfinal <- trimws(y$add)
  
  y$addressfinal <- gsub(" ", "", y$addressfinal)
  y$addressf <- paste(y$postcode,y$addressfinal,sep=",")
  
  taba1 <- inner_join(x,y,by="addressf")
  return(taba1)
}
link47<-function47(add,epc)
link47<-link47[,..needlist1]

link47u<- uniqueresult(link47)
link47d <- doubleresult(link47)

epc <- matchleft(epc,link47)
#################### method 48 ##################
function48<- function(x,y){
  x$bnstreet <-    paste(x$subbuildingname,x$buildingname,sep=",")
  x$bnstreet <-    paste(x$bnstreet,x$buildingnumber,sep=" ")
  x$bnstreet <-    paste(x$bnstreet,x$streetdescription,sep=" ")
  #$bnstreet <- gsub("[']", "", x$bnstreet)
  x$bnstreet <- gsub(" ", "", x$bnstreet)
  x$addressf <-paste(x$postcodelocator,x$bnstreet,sep=",")
  
  y$addressfinal <- trimws(y$add)
  y$addressfinal <- gsub(" ", "", y$addressfinal)
  y$addressf <- paste(y$postcode,y$addressfinal,sep=",")
  
  taba1 <- inner_join(x,y,by="addressf")
  return(taba1)
}
link48<-function48(add,epc)

link48<-link48[,..needlist1]

link48u<- uniqueresult(link48)
link48d <- doubleresult(link48)

epc <- matchleft(epc,link48)
#################### method 49 ##################
function49<- function(x,y){
  x$bnstreet <-    paste(x$saotext,x$buildingname,sep=",")
  x$bnstreet <-    paste(x$bnstreet,x$buildingnumber,sep=" ")
  x$bnstreet <-    paste(x$bnstreet,x$streetdescription,sep=" ")
  #$bnstreet <- gsub("[']", "", x$bnstreet)
  x$bnstreet <- gsub(" ", "", x$bnstreet)
  x$addressf <-paste(x$postcodelocator,x$bnstreet,sep=",")
  
  y$addressfinal <- trimws(y$add)
  y$addressfinal <- gsub(" ", "", y$addressfinal)
  y$addressf <- paste(y$postcode,y$addressfinal,sep=",")
  
  taba1 <- inner_join(x,y,by="addressf")
  return(taba1)
}

link49<-function49(add,epc)
link49<-link49[,..needlist1]


link49u<- uniqueresult(link49)
link49d <- doubleresult(link49)
epc<-matchleft(epc,link49)

#################### method 50 ##################

function50<- function(x,y){
  
  x$bnstreet <-    paste(x$subbuildingname,x$buildingname,sep=",")
  x$bnstreet <-    paste(x$bnstreet,x$pp,sep=" ")
  x$bnstreet <-    paste(x$bnstreet,x$streetdescription,sep=" ")
  #$bnstreet <- gsub("[']", "", x$bnstreet)
  x$bnstreet <- gsub(" ", "", x$bnstreet)
  x$addressf <-paste(x$postcodelocator,x$bnstreet,sep=",")
  
  y$addressfinal <- trimws(y$add)
  y$addressfinal <- gsub(" ", "", y$addressfinal)
  y$addressf <- paste(y$postcode,y$addressfinal,sep=",")
  
  taba1 <- inner_join(x,y,by="addressf")
  return(taba1)
  
  
  
}


link50<-function50(add,epc)


link50<-link50[,..needlist1]


link50u<- uniqueresult(link50)
dim(link50u)


link50d <- doubleresult(link50)
dim(link50d)

dim(epc)

epc <- matchleft(epc,link50)
dim(epc)
#3748892      27




#################### method 51 SY16 1Q##################

function51<- function(x,y){
  
  
  x$bnstreet <-    x$buildingnumber
  #x$bnstreet <-    paste(x$bnstreet,x$streetdescription,sep=" ")
  x$bnstreet <- gsub(" ", "", x$bnstreet)
  x$addressf <-paste(x$postcodelocator,x$bnstreet,sep=",")
  
  y<- y[grepl("^SY16 1Q",y$postcode),]
  y$addressfinal <- beg2char(y$add, ",")
  
  y$addressfinal <- gsub(" ", "", y$addressfinal)
  y$addressf <- paste(y$postcode,y$addressfinal,sep=",")
  
  taba1 <- inner_join(x,y,by="addressf")
  return(taba1)
  
}


link51<-function51(add,epc)


link51<-link51[,..needlist1]


link51u<- uniqueresult(link51)
dim(link51u)


link51d <- doubleresult(link51)
dim(link51d)

dim(epc)
#
epc <- matchleft(epc,link51)
dim(epc)
#3747899      27


#################### method 52 ##################


function52<- function(x,y){
  
  
  x$bnstreet <-    paste(x$subbuildingname,x$paotext,sep=",")
  x$bnstreet <-    paste(x$bnstreet,x$buildingnumber,sep=" ")
  x$bnstreet <-    paste(x$bnstreet,x$streetdescription,sep=" ")
  x$bnstreet <- gsub("[']", "", x$bnstreet)
  x$bnstreet <- gsub(" ", "", x$bnstreet)
  x$addressf <-paste(x$postcodelocator,x$bnstreet,sep=",")
  
  y$addressfinal <- trimws(y$add)
  y$addressfinal <- gsub("[']", "", y$addressfinal)
  y$addressfinal <- gsub(" ", "", y$addressfinal)
  y$addressf <- paste(y$postcode,y$addressfinal,sep=",")
  
  taba1 <- inner_join(x,y,by="addressf")
  return(taba1)
  
  
}

link52<-function52(add,epc)

link52<-link52[,..needlist1]


link52u<- uniqueresult(link52)
dim(link52u)


link52d <- doubleresult(link52)
dim(link52d)


dim(epc)
#
epc <- matchleft(epc,link52)
dim(epc)
#3746997      27



#################### method 53 ##################

function53<- function(x,y){
  
  x$bnstreet <-    paste(x$saotext,x$paotext,sep=",")
  x$bnstreet <-    paste(x$bnstreet,x$buildingnumber,sep=" ")
  x$bnstreet <-    paste(x$bnstreet,x$streetdescription,sep=" ")
  x$bnstreet <- gsub("[']", "", x$bnstreet)
  x$bnstreet <- gsub(" ", "", x$bnstreet)
  x$addressf <-paste(x$postcodelocator,x$bnstreet,sep=",")
  
  y$addressfinal <- trimws(y$add)
  y$addressfinal <- gsub("[']", "", y$addressfinal)
  y$addressfinal <- gsub(" ", "", y$addressfinal)
  y$addressf <- paste(y$postcode,y$addressfinal,sep=",")
  
  taba1 <- inner_join(x,y,by="addressf")
  return(taba1)
  
}



link53<-function53(add,epc)
dim(link53)
# 
link53<-link53[,..needlist1]

link53u<- uniqueresult(link53)
dim(link53u)
#

link53d <- doubleresult(link53)
dim(link53d)


dim(epc)
#
epc <- matchleft(epc,link53)
dim(epc)
#3746829      27


#################### method 54  ##################

function54<- function(x,y){
  
  x$bnstreet <-    paste(x$saotext,x$paotext,sep=",")
  x$bnstreet <-    paste(x$bnstreet,x$pp,sep=" ")
  x$bnstreet <-    paste(x$bnstreet,x$streetdescription,sep=" ")
  x$bnstreet <- gsub("[']", "", x$bnstreet)
  x$bnstreet <- gsub("[.]", "", x$bnstreet)
  x$bnstreet <- gsub(" ", "", x$bnstreet)
  x$addressf <-paste(x$postcodelocator,x$bnstreet,sep=",")
  
  y$addressfinal <- trimws(y$add)
  y$addressfinal <- gsub("[']", "", y$addressfinal)
  y$addressfinal <- gsub("[.]", "", y$addressfinal)
  y$addressfinal <- gsub(" ", "", y$addressfinal)
  y$addressf <- paste(y$postcode,y$addressfinal,sep=",")
  
  taba1 <- inner_join(x,y,by="addressf")
  return(taba1)
  
}


link54<-function54(add,epc)

link54<-link54[,..needlist1]

link54u<- uniqueresult(link54)
dim(link54u)


link54d <- doubleresult(link54)


dim(epc)
# 
epc<- matchleft(epc,link54)
dim(epc)
# 3476377      27
#################### method 55 ##################


function55<- function(x,y){
  
  x$bnstreet <-    paste(x$subbuildingname,x$paotext,sep=",")
  x$bnstreet <-    paste(x$bnstreet,x$buildingnumber,sep=" ")
  x$bnstreet <-    paste(x$bnstreet,x$streetdescription,sep=" ")
  #$bnstreet <- gsub("[']", "", x$bnstreet)
  x$bnstreet <- gsub("[']", "", x$bnstreet)
  x$bnstreet <- gsub("[.]", "", x$bnstreet)
  x$bnstreet <- gsub(" ", "", x$bnstreet)
  x$addressf <-paste(x$postcodelocator,x$bnstreet,sep=",")
  
  y$addressfinal <- trimws(y$add)
  y$addressfinal <- gsub("[']", "", y$addressfinal)
  y$addressfinal <- gsub("[.]", "", y$addressfinal)
  y$addressfinal <- gsub(" ", "", y$addressfinal)
  y$addressf <- paste(y$postcode,y$addressfinal,sep=",")
  
  taba1 <- inner_join(x,y,by="addressf")
  return(taba1)
  
}



link55<-function55(add,epc)


link55<-link55[,..needlist1]


link55u<- uniqueresult(link55)
dim(link55u)


link55d <- doubleresult(link55)
dim(link55d)


dim(epc)
# 
epc <- matchleft(epc,link55)
dim(epc)
#3476256      27

#
#################### method 56 ##################

function56<- function(x,y){
  
  
  x$bnstreet <-    paste(x$subbuildingname,x$paotext,sep=",")
  x$bnstreet <-    paste(x$bnstreet,x$pp,sep=" ")
  x$bnstreet <-    paste(x$bnstreet,x$streetdescription,sep=" ")
  #$bnstreet <- gsub("[']", "", x$bnstreet)
  x$bnstreet <- gsub("[']", "", x$bnstreet)
  x$bnstreet <- gsub("[.]", "", x$bnstreet)
  x$bnstreet <- gsub(" ", "", x$bnstreet)
  x$addressf <-paste(x$postcodelocator,x$bnstreet,sep=",")
  
  y$addressfinal <- trimws(y$add)
  y$addressfinal <- gsub("[']", "", y$addressfinal)
  y$addressfinal <- gsub("[.]", "", y$addressfinal)
  y$addressfinal <- gsub(" ", "", y$addressfinal)
  y$addressf <- paste(y$postcode,y$addressfinal,sep=",")
  
  taba1 <- inner_join(x,y,by="addressf")
  return(taba1)
  
  
}


link56<-function56(add,epc)
dim(link56)
# 

link56<-link56[,..needlist1]

link56u<- uniqueresult(link56)
dim(link56u)
# 

link56d <- doubleresult(link56)
dim(link56d)


epc <- matchleft(epc,link56)
dim(epc)
# 3476163      27

#################### method 57 ##################



function57<- function(x,y){
  
  
  x$bnstreet <-    paste(x$buildingnumber,x$paotext,sep=",")
  x$bnstreet <- gsub("[.]", "", x$bnstreet)
  x$bnstreet <- gsub("[']", "", x$bnstreet)
  x$bnstreet <- gsub(" ", "", x$bnstreet)
  x$addressf <-paste(x$postcodelocator,x$bnstreet,sep=",")
  
  y$addressfinal <- trimws(y$add)
  y$addressfinal <- gsub("[']", "", y$addressfinal)
  y$addressfinal <- gsub("[.]", "", y$addressfinal)
  y$addressfinal <- gsub(" ", "", y$addressfinal)
  y$addressf <- paste(y$postcode,y$addressfinal,sep=",")
  
  taba1 <- inner_join(x,y,by="addressf")
  return(taba1)
  
}



link57<-function57(add,epc)
dim(link57)


link57<-link57[,..needlist1]

link57u<- uniqueresult(link57)
dim(link57u)


link57d <- doubleresult(link57)
dim(link57d)


dim(epc)
# 
epc <- matchleft(epc,link57)
dim(epc)
#3475961      27
#
#################### method 58 ##################

function58<- function(x,y){
  x$bnstreet <-    paste(x$saotext,x$pp,sep=",")
  x$bnstreet <-    paste(x$bnstreet,x$buildingnumber,sep=" ")
  x$bnstreet <-    paste(x$bnstreet,x$streetdescription,sep=" ")
  x$bnstreet <- gsub("[']", "", x$bnstreet)
  x$bnstreet <- gsub(" ", "", x$bnstreet)
  x$addressf <-paste(x$postcodelocator,x$bnstreet,sep=",")
  
  y$addressfinal <- trimws(y$add)
  y$addressfinal <- gsub("[']", "", y$addressfinal)
  y$addressfinal <- gsub(" ", "", y$addressfinal)
  y$addressf <- paste(y$postcode,y$addressfinal,sep=",")
  
  taba1 <- inner_join(x,y,by="addressf")
  return(taba1)
  
}


link58<-function58(add,epc)
dim(link58)


link58<-link58[,..needlist1]



link58u<- uniqueresult(link58)
dim(link58u)


link58d <- doubleresult(link58)
dim(link58d)


dim(epc)
# 
epc <- matchleft(epc,link58)
dim(epc)
#3468027      27
#################### method 59 ##################
function59<- function(x,y){
  
  
  
  x$bnstreet <-    paste(x$subbuildingname,x$buildingname,sep=" ")
  x$addressf <-paste(x$postcodelocator,x$bnstreet,sep=",")
  
  y$addressfinal <- trimws(y$add)
  y$addressf <- paste(y$postcode,y$addressfinal,sep=",")
  
  taba1 <- inner_join(x,y,by="addressf")
  return(taba1) 
  
}



#
link59<-function59(add,epc)

link59<-link59[,..needlist1]


link59u<- uniqueresult(link59)
dim(link59u)


link59d <- doubleresult(link59)
dim(link59d)

dim(epc)
#2649854      27
epc <- matchleft(epc,link59)
dim(epc)
#2620314      27

#



#################### method 60 ##################

function60<- function(x,y){
  
  x$bnstreet <-    paste(x$paotext,x$buildingname,sep=" ")
  x$addressf <-paste(x$postcodelocator,x$bnstreet,sep=",")
  
  y$addressfinal <- trimws(y$add)
  y$addressf <- paste(y$postcode,y$addressfinal,sep=",")
  
  taba1 <- inner_join(x,y,by="addressf")
  return(taba1) 
  
}


link60<-function60(add,epc)


link60<-link60[,..needlist1]


link60u<- uniqueresult(link60)
dim(link60u)


link60d <- doubleresult(link60)
dim(link60d)

dim(epc)
#
epc <- matchleft(epc,link60)
dim(epc)
# 3466228      27




#################### method 61 ##################

function61<- function(x,y){
  
  
  x$bnstreet <-    paste(x$saotext,x$buildingname,sep=" ")
  x$addressf <-paste(x$postcodelocator,x$bnstreet,sep=",")
  
  y$addressfinal <- trimws(y$add)
  y$addressf <- paste(y$postcode,y$addressfinal,sep=",")
  
  taba1 <- inner_join(x,y,by="addressf")
  return(taba1) 
  
}


link61<-function61(add,epc)


link61<-link61[,..needlist1]


link61u<- uniqueresult(link61)
dim(link61u)


link61d <- doubleresult(link61)
dim(link61d)

dim(epc)
#
epc <- matchleft(epc,link61)
dim(epc)
#3466224      27

#################### method 62 ##################
function62<- function(x,y){
  x$bnstreet <-    paste(x$subbuildingname,x$buildingname,sep=",")
  x$bnstreet <-    paste(x$bnstreet,x$buildingnumber,sep=" ")
  x$bnstreet <-    paste(x$bnstreet,x$streetdescription,sep=",")
  x$bnstreet <- gsub(" ", "", x$bnstreet)
  x$addressf <-paste(x$postcodelocator,x$bnstreet,sep=",")
  
  y$addressfinal <- trimws(y$add)
  y$addressfinal <- gsub(" ", "", y$addressfinal)
  y$addressf <- paste(y$postcode,y$addressfinal,sep=",")
  
  taba1 <- inner_join(x,y,by="addressf")
  
  return(taba1)
  
}

link62<-function62(add,epc)

link62<-link62[,..needlist1]


link62u<- uniqueresult(link62)
dim(link62u)


link62d <- doubleresult(link62)
dim(link62d)


dim(epc)
#
epc <- matchleft(epc,link62)
dim(epc)
# 3465977      27



#################### method 63 ##################

function63<- function(x,y){
  
  x$bnstreet <-    paste(x$saotext,x$buildingname,sep=",")
  x$bnstreet <-    paste(x$bnstreet,x$buildingnumber,sep=" ")
  x$bnstreet <-    paste(x$bnstreet,x$streetdescription,sep=",")
  x$bnstreet <- gsub(" ", "", x$bnstreet)
  x$addressf <-paste(x$postcodelocator,x$bnstreet,sep=",")
  
  y$addressfinal <- trimws(y$add)
  y$addressfinal <- gsub(" ", "", y$addressfinal)
  y$addressf <- paste(y$postcode,y$addressfinal,sep=",")
  
  taba1 <- inner_join(x,y,by="addressf")
  
  return(taba1)
  
}



link63<-function63(add,epc)
dim(link63)
# 
link63<-link63[,..needlist1]

link63u<- uniqueresult(link63)
dim(link63u)
#

link63d <- doubleresult(link63)
dim(link63d)


dim(epc)
#3109964      27
epc <- matchleft(epc,link63)
dim(epc)
# 3016421      27


#################### method 64  ##################

function64<- function(x,y){
  
  x$bnstreet <-    paste(x$subbuildingname,x$paotext,sep=",")
  x$bnstreet <-    paste(x$bnstreet,x$buildingnumber,sep=" ")
  x$bnstreet <-    paste(x$bnstreet,x$streetdescription,sep=",")
  x$bnstreet <- gsub(" ", "", x$bnstreet)
  x$addressf <-paste(x$postcodelocator,x$bnstreet,sep=",")
  
  y$addressfinal <- trimws(y$add)
  y$addressfinal <- gsub(" ", "", y$addressfinal)
  y$addressf <- paste(y$postcode,y$addressfinal,sep=",")
  
  taba1 <- inner_join(x,y,by="addressf")
  
  return(taba1)
  
  
}


link64<-function64(add,epc)



link64<-link64[,..needlist1]


link64u<- uniqueresult(link64)
dim(link64u)


link64d <- doubleresult(link64)


dim(epc)
# 3016421      27
epc<- matchleft(epc,link64)
dim(epc)
# 2764250      27
#################### method 65 ##################


function65<- function(x,y){
  
  x$bnstreet <-    paste(x$subbuildingname,x$paotext,sep=",")
  x$bnstreet <-    paste(x$bnstreet,x$pp,sep=" ")
  x$bnstreet <-    paste(x$bnstreet,x$streetdescription,sep=",")
  x$bnstreet <- gsub(" ", "", x$bnstreet)
  x$addressf <-paste(x$postcodelocator,x$bnstreet,sep=",")
  
  y$addressfinal <- trimws(y$add)
  y$addressfinal <- gsub(" ", "", y$addressfinal)
  y$addressf <- paste(y$postcode,y$addressfinal,sep=",")
  
  taba1 <- inner_join(x,y,by="addressf")
  
  return(taba1) 
  
}



link65<-function65(add,epc)


link65<-link65[,..needlist1]


link65u<- uniqueresult(link65)
dim(link65u)


link65d <- doubleresult(link65)
dim(link65d)


dim(epc)
# 
epc <- matchleft(epc,link65)
dim(epc)
# 3465594      27

#
#################### method 66 ##################

function66<- function(x,y){
  
  x$bnstreet <-    paste(x$saotext,x$paotext,sep=",")
  x$bnstreet <-    paste(x$bnstreet,x$pp,sep=" ")
  x$bnstreet <-    paste(x$bnstreet,x$streetdescription,sep=",")
  x$bnstreet <- gsub(" ", "", x$bnstreet)
  x$addressf <-paste(x$postcodelocator,x$bnstreet,sep=",")
  
  y$addressfinal <- trimws(y$add)
  y$addressfinal <- gsub(" ", "", y$addressfinal)
  y$addressf <- paste(y$postcode,y$addressfinal,sep=",")
  
  taba1 <- inner_join(x,y,by="addressf")
  
  return(taba1) 
  
}




link66<-function66(add,epc)
dim(link66)
# 

link66<-link66[,..needlist1]



link66u<- uniqueresult(link66)
dim(link66u)
# 

link66d <- doubleresult(link66)
dim(link66d)

dim(epc)
# 
epc <- matchleft(epc,link66)
dim(epc)
# 3449262      27

#################### method 67 ##################

function67<- function(x,y){
  
  
  x$bnstreet <-    paste(x$buildingnumber,x$paotext,sep=" ")
  #x$bnstreet <-    paste(x$bnstreet,x$pp,sep=" ")
  #x$bnstreet <-    paste(x$bnstreet,x$streetdescription,sep=",")
  x$bnstreet <- gsub(" ", "", x$bnstreet)
  x$addressf <-paste(x$postcodelocator,x$bnstreet,sep=",")
  
  y$addressfinal <- trimws(y$add)
  y$addressfinal <- gsub(" ", "", y$addressfinal)
  y$addressf <- paste(y$postcode,y$addressfinal,sep=",")
  
  taba1 <- inner_join(x,y,by="addressf")
  
  return(taba1)
  
}

link67<-function67(add,epc)
dim(link67)

link67<-link67[,..needlist1]



link67u<- uniqueresult(link67)
dim(link67u)


link67d <- doubleresult(link67)
dim(link67d)


dim(epc)
# 
epc <- matchleft(epc,link67)
dim(epc)
#
#
#################### method 68 ##################

function68<- function(x,y){
  
  x$bnstreet <-    paste(x$buildingname,x$paotext,sep=" ")
  #x$bnstreet <-    paste(x$bnstreet,x$pp,sep=" ")
  #x$bnstreet <-    paste(x$bnstreet,x$streetdescription,sep=",")
  x$bnstreet <- gsub(" ", "", x$bnstreet)
  x$addressf <-paste(x$postcodelocator,x$bnstreet,sep=",")
  
  y$addressfinal <- trimws(y$add)
  y$addressfinal <- gsub(" ", "", y$addressfinal)
  y$addressf <- paste(y$postcode,y$addressfinal,sep=",")
  
  taba1 <- inner_join(x,y,by="addressf")
  
  return(taba1)
}


link68<-function68(add,epc)
dim(link68)
link68<-link68[,..needlist1]

link68u<- uniqueresult(link68)
dim(link68u)

link68d <- doubleresult(link68)
dim(link68d)


dim(epc)
# 
epc <- matchleft(epc,link68)
dim(epc)
#3448887      27
#################### method 69 ##################
function69<- function(x,y){
  x<-x[x$subbuildingname=="",]
  x$bnstreet <-    paste(x$ss,x$streetdescription,sep=",")
  
  x$bnstreet <- gsub(" ", "", x$bnstreet)
  x$addressf <-paste(x$postcodelocator,x$bnstreet,sep=",")
  
  y$addressfinal <- trimws(y$add)
  y$addressfinal <- gsub(" ", "", y$addressfinal)
  y$addressf <- paste(y$postcode,y$addressfinal,sep=",")
  
  taba1 <- inner_join(x,y,by="addressf")
  
  return(taba1)
  
  
}


link69<-function69(add,epc)

link69<-link69[,..needlist1]


link69u<- uniqueresult(link69)
dim(link69u)


link69d <- doubleresult(link69)
dim(link69d)

dim(epc)
#
epc <- matchleft(epc,link69)
dim(epc)
#  3433640      27

######sum up section 3#########



link27u$method<-"link27u"
link28u$method<-"link28u"
link29u$method<-"link29u"
link30u$method<-"link30u"
link31u$method<-"link31u"
link32u$method<-"link32u"
link33u$method<-"link33u"
link34u$method<-"link34u"
link35u$method<-"link35u"
link36u$method<-"link36u"
link37u$method<-"link37u"
link38u$method<-"link38u"
link39u$method<-"link39u"
link40u$method<-"link40u"

link41u$method<-"link41u"
link42u$method<-"link42u"
link43u$method<-"link43u"
link44u$method<-"link44u"
link45u$method<-"link45u"
link46u$method<-"link46u"
link47u$method<-"link47u"
link48u$method<-"link48u"
link49u$method<-"link49u"

link50u$method<-"link50u"
link51u$method<-"link51u"
link52u$method<-"link52u"
link53u$method<-"link53u"
link54u$method<-"link54u"
link55u$method<-"link55u"
link56u$method<-"link56u"
link57u$method<-"link57u"
link58u$method<-"link58u"
link59u$method<-"link59u"
link60u$method<-"link60u"
link61u$method<-"link61u"
link62u$method<-"link62u"
link63u$method<-"link63u"
link64u$method<-"link64u"
link65u$method<-"link65u"
link66u$method<-"link66u"
link67u$method<-"link67u"
link68u$method<-"link68u"
link69u$method<-"link69u"



link27d$method<-"link27d"
link28d$method<-"link28d"
link29d$method<-"link29d"
link30d$method<-"link30d"
link31d$method<-"link31d"
link32d$method<-"link32d"
link33d$method<-"link33d"
link34d$method<-"link34d"
link35d$method<-"link35d"
link36d$method<-"link36d"
link37d$method<-"link37d"
link38d$method<-"link38d"
link39d$method<-"link39d"
link40d$method<-"link40d"
link41d$method<-"link41d"
link42d$method<-"link42d"
link43d$method<-"link43d"
link44d$method<-"link44d"
link45d$method<-"link45d"
link46d$method<-"link46d"
link47d$method<-"link47d"
link48d$method<-"link48d"
link49d$method<-"link49d"
link50d$method<-"link50d"
link51d$method<-"link51d"
link52d$method<-"link52d"
link53d$method<-"link53d"
link54d$method<-"link54d"
link55d$method<-"link55d"
link56d$method<-"link56d"
link57d$method<-"link57d"
link58d$method<-"link58d"
link59d$method<-"link59d"
link60d$method<-"link60d"
link61d$method<-"link61d"
link62d$method<-"link62d"
link63d$method<-"link63d"
link64d$method<-"link64d"
link65d$method<-"link65d"
link66d$method<-"link66d"
link67d$method<-"link67d"
link68d$method<-"link68d"
link69d$method<-"link69d"

l27_69u = list(link27u,link28u,link29u,link30u,link31u,link32u,link33u,link34u,link35u,link36u,link37u,link38u,link39u,link40u,link41u,link42u,link43u,link44u,link45u,link46u,link47u,link48u,link49u,link50u,link51u,link52u,link53u,link54u,link55u,link56u,link57u,link58u,link59u,link60u,link61u,link62u,link63u,link64u,link65u,link66u,link67u,link68u,link69u)
link27_69u<- rbindlist(l27_69u, use.names=TRUE, fill=TRUE)
dim(link27_69u)
#1007507
length(unique(link27_69u$lmk_key))

length(unique(link27_69u$method))

dim(link27u)[1]+dim(link28u)[1]+dim(link29u)[1]+dim(link30u)[1]+dim(link31u)[1]+dim(link32u)[1]+dim(link33u)[1]+dim(link34u)[1]+dim(link35u)[1]+dim(link36u)[1]+dim(link37u)[1]+dim(link38u)[1]+dim(link39u)[1]+dim(link40u)[1]+dim(link41u)[1]+dim(link42u)[1]+dim(link43u)[1]+dim(link44u)[1]+dim(link45u)[1]+dim(link46u)[1]+dim(link47u)[1]+dim(link48u)[1]+dim(link49u)[1]+dim(link50u)[1]+dim(link51u)[1]+dim(link52u)[1]+dim(link53u)[1]+dim(link54u)[1]+dim(link55u)[1]+dim(link56u)[1]+dim(link57u)[1]+dim(link58u)[1]+dim(link59u)[1]+dim(link60u)[1]+dim(link61u)[1]+dim(link62u)[1]+dim(link63u)[1]+dim(link64u)[1]+dim(link65u)[1]+dim(link66u)[1]+dim(link67u)[1]+dim(link68u)[1]+dim(link69u)[1]
#1007507
l27_69d = list(link27d,link28d,link29d,link30d,link31d,link32d,link33d,link34d,link35d,link36d,link37d,link38d,link39d,link40d,link41d,link42d,link43d,link44d,link45d,link46d,link47d,link48d,link49d,link50d,link51d,link52d,link53d,link54d,link55d,link56d,link57d,link58d,link59d,link60d,link61d,link62d,link63d,link64d,link65d,link66d,link67d,link68d,link69d)
link27_69d<- rbindlist(l27_69d, use.names=TRUE, fill=TRUE)
dim(link27_69d)
#23388 
dim(link27d)[1]+dim(link28d)[1]+dim(link29d)[1]+dim(link30d)[1]+dim(link31d)[1]+dim(link32d)[1]+dim(link33d)[1]+dim(link34d)[1]+dim(link35d)[1]+dim(link36d)[1]+dim(link37d)[1]+dim(link38d)[1]+dim(link39d)[1]+dim(link40d)[1]+dim(link41d)[1]+dim(link42d)[1]+dim(link43d)[1]+dim(link44d)[1]+dim(link45d)[1]+dim(link46d)[1]+dim(link47d)[1]+dim(link48d)[1]+dim(link49d)[1]+dim(link50d)[1]+dim(link51d)[1]+dim(link52d)[1]+dim(link53d)[1]+dim(link54d)[1]+dim(link55d)[1]+dim(link56d)[1]+dim(link57d)[1]+dim(link58d)[1]+dim(link59d)[1]+dim(link60d)[1]+dim(link61d)[1]+dim(link62d)[1]+dim(link63d)[1]+dim(link64d)[1]+dim(link65d)[1]+dim(link66d)[1]+dim(link67d)[1]+dim(link68d)[1]+dim(link69d)[1]
#23388

dbWriteTable(con, "link27_69dnew",value =link27_69d, append = TRUE, row.names = FALSE)
dbWriteTable(con, "link27_69unew",value =link27_69u, append = TRUE, row.names = FALSE)

rm(link27,link28,link29,link30,link31,link32,link33,link34,link35,link36,link37,link38,link39,link40,link41,link42,link43,link44,link45,link46,link47,link48,link49,link50,link51,link52,link53,link54,link55,link56,link57,link58,link59,link60,link61,link62,link63,link64,link65,link66,link67,link68,link69)

rm(link27u,link28u,link29u,link30u,link31u,link32u,link33u,link34u,link35u,link36u,link37u,link38u,link39u,link40u,link41u,link42u,link43u,link44u,link45u,link46u,link47u,link48u,link49u,link50u,link51u,link52u,link53u,link54u,link55u,link56u,link57u,link58u,link59u,link60u,link61u,link62u,link63u,link64u,link65u,link66u,link67u,link68u,link69u)

rm(link27d,link28d,link29d,link30d,link31d,link32d,link33d,link34d,link35d,link36d,link37d,link38d,link39d,link40d,link41d,link42d,link43d,link44d,link45d,link46d,link47d,link48d,link49d,link50d,link51d,link52d,link53d,link54d,link55d,link56d,link57d,link58d,link59d,link60d,link61d,link62d,link63d,link64d,link65d,link66d,link67d,link68d,link69d)

rm(l27_69u,l27_69d)

rm(function27,function28,function29,function30,function31,function32,function33,function34,function35,function36,function37,function38,function39,function40,function41,function42,function43,function44,function45,function46,function47,function48,function49,function50,function51,function52,function53,function54,function55,function56,function57,function58,function59,function60,function61,function62,function63,function64,function65,function66,function67,function68,function69)
Sys.time()
#################### method 70 ##################

function70<- function(x,y){

  x$bnstreet <-    paste(x$subbuildingname,x$buildingname,sep=",")
  x$bnstreet <-    paste(x$bnstreet,x$buildingnumber,sep=",")
  x$bnstreet <-    paste(x$bnstreet,x$streetdescription,sep=",")
  x$bnstreet <- gsub("[.]", "", x$bnstreet)
  x$bnstreet <- gsub("[']", "", x$bnstreet)
  x$bnstreet <- gsub(" ", "", x$bnstreet)
  x$addressf <-paste(x$postcodelocator,x$bnstreet,sep=",")
  
  y$addressfinal <- trimws(y$add)

  y$addressfinal <- gsub("[.]", "", y$addressfinal)
  y$addressfinal <- gsub("[']", "", y$addressfinal)
  y$addressfinal <- gsub(" ", "", y$addressfinal)
  y$addressf <- paste(y$postcode,y$addressfinal,sep=",")
  
  taba1 <- inner_join(x,y,by="addressf")
  return(taba1)
  
}


link70<-function70(add,epc)


link70<-link70[,..needlist1]


link70u<- uniqueresult(link70)
dim(link70u)


link70d <- doubleresult(link70)
dim(link70d)

dim(epc)
#
epc <- matchleft(epc,link70)
dim(epc)
# 3435246      27




#################### method 71 ##################

function71<- function(x,y){
  # x<-x[x$subbuildingname!="",]
  
  x$bnstreet <-    paste(x$saotext,x$paotext,sep=" ")
  x$bnstreet <-    paste(x$bnstreet,x$buildingname ,sep=",")
  
  x$bnstreet <-    paste(x$bnstreet,x$streetdescription,sep=",")
  x$bnstreet <- gsub("[']", "", x$bnstreet)
  x$bnstreet <- gsub(" ", "", x$bnstreet)
  x$addressf <-paste(x$postcodelocator,x$bnstreet,sep=",")
  
  
  y$addressfinal <- y$add
  
  y$addressfinal <- gsub(" ", "", y$addressfinal)
  y$addressf <- paste(y$postcode,y$addressfinal,sep=",")
  
  taba1 <- inner_join(x,y,by="addressf")
  return(taba1)
  
  
}


link71<-function71(add,epc)


link71<-link71[,..needlist1]


link71u<- uniqueresult(link71)
dim(link71u)


link71d <- doubleresult(link71)
dim(link71d)

dim(epc)
#3540864      27
epc <- matchleft(epc,link71)
dim(epc)
#3481491      27


#################### method 72 ##################


function72<- function(x,y){
  
  x$bnstreet <-    paste(x$subbuildingname,x$paotext,sep=" ")
  x$bnstreet <-    paste(x$bnstreet,x$buildingname ,sep=",")
  #x$bnstreet <-    paste(x$bnstreet,x$paoendnumber,sep="-")
  x$bnstreet <-    paste(x$bnstreet,x$streetdescription,sep=",")
  x$bnstreet <- gsub("[']", "", x$bnstreet)
  x$bnstreet <- gsub(" ", "", x$bnstreet)
  x$addressf <-paste(x$postcodelocator,x$bnstreet,sep=",")
  
  
  y$addressfinal <- y$add
  
  y$addressfinal <- gsub(" ", "", y$addressfinal)
  y$addressf <- paste(y$postcode,y$addressfinal,sep=",")
  
  taba1 <- inner_join(x,y,by="addressf")
  return(taba1)
  
}

link72<-function72(add,epc)

link72<-link72[,..needlist1]


link72u<- uniqueresult(link72)
dim(link72u)


link72d <- doubleresult(link72)
dim(link72d)


dim(epc)
#
epc <- matchleft(epc,link72)
dim(epc)
# 3434711      27



#################### method 73 ##################

function73<- function(x,y){
  x$bnstreet <-    paste(x$buildingname,x$paotext,sep=",")
  x$bnstreet <-    paste(x$bnstreet,x$streetdescription,sep=",")
  x$bnstreet <- gsub(" ", "", x$bnstreet)
  x$addressf <-paste(x$postcodelocator,x$bnstreet,sep=",")
  
  y$addressfinal <- trimws(y$add)
  y$addressfinal <- gsub(" ", "", y$addressfinal)
  y$addressf <- paste(y$postcode,y$addressfinal,sep=",")
  
  taba1 <- inner_join(x,y,by="addressf")
  return(taba1)
  
  
  
}



link73<-function73(add,epc)
dim(link73)
# 
link73<-link73[,..needlist1]

link73u<- uniqueresult(link73)
dim(link73u)
#

link73d <- doubleresult(link73)
dim(link73d)


dim(epc)
#
epc <- matchleft(epc,link73)
dim(epc)
# 


#################### method 74  ##################

function74<- function(x,y){
  
  x$bnstreet <-    paste(x$ss,x$paotext,sep=",")
  x$bnstreet <-    paste(x$bnstreet,x$streetdescription,sep=",")
  x$bnstreet <- gsub("[.]", "", x$bnstreet)
  x$bnstreet <- gsub("[']", "", x$bnstreet)
  x$bnstreet <- gsub(" ", "", x$bnstreet)
  x$addressf <-paste(x$postcodelocator,x$bnstreet,sep=",")
  
  y$addressfinal <- trimws(y$add)
  y$addressfinal <- gsub("[']", "", y$addressfinal)
  y$addressfinal <- gsub("[.]", "", y$addressfinal)
  y$addressfinal <- gsub(" ", "", y$addressfinal)
  y$addressf <- paste(y$postcode,y$addressfinal,sep=",")
  
  taba1 <- inner_join(x,y,by="addressf")
  return(taba1)
  
}


link74<-function74(add,epc)
dim(link74)


link74<-link74[,..needlist1]


link74u<- uniqueresult(link74)
dim(link74u)


link74d <- doubleresult(link74)


dim(epc)
#
epc<- matchleft(epc,link74)
dim(epc)
# 3285365      27
#################### method 75 ##################


function75<- function(x,y){
  x<-x[paostartsuffix=="",]
  x$bnstreet <-    paste(x$buildingnumber,x$streetdescription,sep=" ")
  x$bnstreet <-    paste(x$bnstreet,x$dependentlocality,sep=",")
  x$bnstreet <- gsub("[.]", "", x$bnstreet)
  x$bnstreet <- gsub("[']", "", x$bnstreet)
  x$bnstreet <- gsub(" ", "", x$bnstreet)
  x$addressf <-paste(x$postcodelocator,x$bnstreet,sep=",")
  
  y$addressfinal <- trimws(y$add)
  y$addressfinal <- gsub("[']", "", y$addressfinal)
  y$addressfinal <- gsub("[.]", "", y$addressfinal)
  y$addressfinal <- gsub(" ", "", y$addressfinal)
  y$addressf <- paste(y$postcode,y$addressfinal,sep=",")
  
  taba1 <- inner_join(x,y,by="addressf")
  return(taba1)
  
}



link75<-function75(add,epc)
dim(link75)

link75<-link75[,..needlist1]


link75u<- uniqueresult(link75)
dim(link75u)


link75d <- doubleresult(link75)
dim(link75d)


dim(epc)
# 
epc <- matchleft(epc,link75)
dim(epc)
#2978707      27

#################### method 76 ##################

function76<- function(x,y){
  
  
  x$bnstreet <-    paste(x$buildingnumber,x$paotext,sep=",")
  x$bnstreet <-    paste(x$bnstreet,x$streetdescription,sep=",")
  x$bnstreet <- gsub("[.]", "", x$bnstreet)
  x$bnstreet <- gsub("[']", "", x$bnstreet)
  x$bnstreet <- gsub(" ", "", x$bnstreet)
  x$addressf <-paste(x$postcodelocator,x$bnstreet,sep=",")
  
  y$addressfinal <- trimws(y$add)
  y$addressfinal <- gsub("[']", "", y$addressfinal)
  y$addressfinal <- gsub("[.]", "", y$addressfinal)
  y$addressfinal <- gsub(" ", "", y$addressfinal)
  y$addressf <- paste(y$postcode,y$addressfinal,sep=",")
  
  taba1 <- inner_join(x,y,by="addressf")
  return(taba1)
}

link76<-function76(add,epc)
dim(link76)
# 
length(unique(link76$lmk_key))

link76<-link76[,..needlist1]

link76u<- uniqueresult(link76)
dim(link76u)
# 

link76d <- doubleresult(link76)
dim(link76d)

dim(epc)
# 
epc <- matchleft(epc,link76)
dim(epc)
# 2969576      27

#################### method 77 ##################

function77<- function(x,y){
  
  x$bnstreet <-    paste(x$saotext,x$paotext,sep=",")
  x$bnstreet <-    paste(x$bnstreet,x$streetdescription,sep=",")
  x$bnstreet <- gsub("[.]", "", x$bnstreet)
  x$bnstreet <- gsub("[']", "", x$bnstreet)
  x$bnstreet <- gsub(" ", "", x$bnstreet)
  x$addressf <-paste(x$postcodelocator,x$bnstreet,sep=",")
  
  y$addressfinal <- trimws(y$add)
  y$addressfinal <- gsub("[']", "", y$addressfinal)
  y$addressfinal <- gsub("[.]", "", y$addressfinal)
  y$addressfinal <- gsub(" ", "", y$addressfinal)
  y$addressf <- paste(y$postcode,y$addressfinal,sep=",")
  
  taba1 <- inner_join(x,y,by="addressf")
  return(taba1)
  
  
}



link77<-function77(add,epc)
dim(link77)


link77<-link77[,..needlist1]



link77u<- uniqueresult(link77)
dim(link77u)


link77d <- doubleresult(link77)
dim(link77d)


dim(epc)
#
epc <- matchleft(epc,link77)
dim(epc)
#2968444      27
#
#################### method 78 ##################

function78<- function(x,y){
  
  x$bnstreet <-    paste(x$buildingnumber,x$streetdescription,sep=" ")
  x$bnstreet <-    paste(x$bnstreet,x$townname,sep=",")
  x$bnstreet <- gsub(" ", "", x$bnstreet)
  x$addressf <-paste(x$postcodelocator,x$bnstreet,sep=",")
  
  y$addressfinal <- trimws(y$add)
  y$addressfinal <- gsub(" ", "", y$addressfinal)
  y$addressf <- paste(y$postcode,y$addressfinal,sep=",")
  
  taba1 <- inner_join(x,y,by="addressf")
  return(taba1)
}


link78<-function78(add,epc)
dim(link78)


link78<-link78[,..needlist1]



link78u<- uniqueresult(link78)
dim(link78u)


link78d <- doubleresult(link78)
dim(link78d)


dim(epc)
# 
epc <- matchleft(epc,link78)
dim(epc)
#2598852      27
#################### method 79 ##################
function79<- function(x,y){
  
  x$bnstreet <-    paste(x$pp,x$streetdescription,sep=" ")
  x$bnstreet <-    paste(x$bnstreet,x$townname,sep=",")
  x$bnstreet <- gsub(" ", "", x$bnstreet)
  x$addressf <-paste(x$postcodelocator,x$bnstreet,sep=",")
  
  y$addressfinal <- trimws(y$add)
  y$addressfinal <- gsub(" ", "", y$addressfinal)
  y$addressf <- paste(y$postcode,y$addressfinal,sep=",")
  
  taba1 <- inner_join(x,y,by="addressf")
  return(taba1)
  
  
}



#
link79<-function79(add,epc)

link79<-link79[,..needlist1]


link79u<- uniqueresult(link79)
dim(link79u)


link79d <- doubleresult(link79)
dim(link79d)

dim(epc)
#
epc <- matchleft(epc,link79)
dim(epc)
#2554314      27




#################### method 80 ##################

function80<- function(x,y){
  x$bnstreet <-    paste(x$saotext,x$pp,sep=",")
  x$bnstreet <-    paste(x$bnstreet,x$buildingnumber,sep=" ")
  x$bnstreet <-    paste(x$bnstreet,x$streetdescription,sep=" ")
  x$bnstreet <- gsub("[.]", "", x$bnstreet)
  x$bnstreet <- gsub("[']", "", x$bnstreet)
  #$bnstreet <- gsub("[']", "", x$bnstreet)
  x$bnstreet <- gsub(" ", "", x$bnstreet)
  x$addressf <-paste(x$postcodelocator,x$bnstreet,sep=",")
  
  y$addressfinal <- trimws(y$add)
  y$addressfinal <- gsub("[']", "", y$addressfinal)
  y$addressfinal <- gsub("[.]", "", y$addressfinal)
  y$addressfinal <- gsub(" ", "", y$addressfinal)
  y$addressf <- paste(y$postcode,y$addressfinal,sep=",")
  
  taba1 <- inner_join(x,y,by="addressf")
  return(taba1)
  
  
  
}


link80<-function80(add,epc)


link80<-link80[,..needlist1]


link80u<- uniqueresult(link80)
dim(link80u)


link80d <- doubleresult(link80)
dim(link80d)

dim(epc)
#
epc <- matchleft(epc,link80)
dim(epc)
#2553897      27




#################### method 81 ##################

function81<- function(x,y){
  x$bnstreet <-    paste(x$buildingnumber,x$streetdescription,sep=",")
  x$bnstreet <-    paste(x$bnstreet,x$dependentlocality,sep=",")
  x$bnstreet <- gsub("[.]", "", x$bnstreet)
  x$bnstreet <- gsub("[']", "", x$bnstreet)
  x$bnstreet <- gsub(" ", "", x$bnstreet)
  x$addressf <-paste(x$postcodelocator,x$bnstreet,sep=",")
  
  y$addressfinal <- trimws(y$add)
  y$addressfinal <- gsub("[']", "", y$addressfinal)
  y$addressfinal <- gsub("[.]", "", y$addressfinal)
  y$addressfinal <- gsub(" ", "", y$addressfinal)
  y$addressf <- paste(y$postcode,y$addressfinal,sep=",")
  
  taba1 <- inner_join(x,y,by="addressf")
  return(taba1)
}


link81<-function81(add,epc)


link81<-link81[,..needlist1]
dim(link81)

link81u<- uniqueresult(link81)
dim(link81u)


link81d <- doubleresult(link81)
dim(link81d)

dim(epc)
#
epc <- matchleft(epc,link81)
dim(epc)
#2491768      27


#################### method 82 ##################


function82<- function(x,y){
  
  
  x$bnstreet <-    paste(x$pp,x$streetdescription,sep=",")
  x$bnstreet <-    paste(x$bnstreet,x$dependentlocality,sep=",")
  x$bnstreet <- gsub("[.]", "", x$bnstreet)
  x$bnstreet <- gsub("[']", "", x$bnstreet)
  x$bnstreet <- gsub(" ", "", x$bnstreet)
  x$addressf <-paste(x$postcodelocator,x$bnstreet,sep=",")
  
  y$addressfinal <- trimws(y$add)
  y$addressfinal <- gsub("[']", "", y$addressfinal)
  y$addressfinal <- gsub("[.]", "", y$addressfinal)
  y$addressfinal <- gsub(" ", "", y$addressfinal)
  y$addressf <- paste(y$postcode,y$addressfinal,sep=",")
  
  taba1 <- inner_join(x,y,by="addressf")
  return(taba1)
  
  
  
  
}

link82<-function82(add,epc)

link82<-link82[,..needlist1]


link82u<- uniqueresult(link82)
dim(link82u)


link82d <- doubleresult(link82)
dim(link82d)


dim(epc)

epc <- matchleft(epc,link82)
dim(epc)
#  2488299      27



#################### method 83 ##################

function83<- function(x,y){
  x<-x[x$buildingname=="",]
  x<-x[x$buildingnumber=="",]
  x<-x[x$subbuildingname=="",]
  x<-x[x$paostartsuffix=="",]
  x$bnstreet <-    paste(x$saotext,x$paostartnumber,sep=",")
  x$bnstreet <-    paste(x$bnstreet,x$streetdescription,sep=",")
  
  x$bnstreet <- gsub(" ", "", x$bnstreet)
  x$addressf <-paste(x$postcodelocator,x$bnstreet,sep=",")
  
  y$addressfinal <- trimws(y$add)
  y$addressfinal <- gsub(" ", "", y$addressfinal)
  y$addressf <- paste(y$postcode,y$addressfinal,sep=",")
  
  taba1 <- inner_join(x,y,by="addressf")
  return(taba1)
  
  
}



link83<-function83(add,epc)
dim(link83)
# 
link83<-link83[,..needlist1]

link83u<- uniqueresult(link83)
dim(link83u)
#

link83d <- doubleresult(link83)
dim(link83d)


dim(epc)
#
epc <- matchleft(epc,link83)
dim(epc)
#2488082      27


#################### method 84  ##################

function84<- function(x,y){
  
  x<-x[x$buildingnumber=="",]
  x<-x[x$paostartsuffix=="",]
  x<-x[x$saotext=="",]
  x<-x[x$paoendnumber=="",]
  x<-x[x$saostartsuffix=="",]
  x$bnstreet <-    paste(x$paostartnumber,x$streetdescription,sep=",")
  x$bnstreet <- gsub("[.]", "", x$bnstreet)
  x$bnstreet <- gsub("[']", "", x$bnstreet)
  x$bnstreet <- gsub(" ", "", x$bnstreet)
  x$addressf <-paste(x$postcodelocator,x$bnstreet,sep=",")
  
  y$addressfinal <- trimws(y$add)
  y$addressfinal <- gsub("[']", "", y$addressfinal)
  y$addressfinal <- gsub("[.]", "", y$addressfinal)
  y$addressfinal <- gsub(" ", "", y$addressfinal)
  y$addressf <- paste(y$postcode,y$addressfinal,sep=",")
  
  taba1 <- inner_join(x,y,by="addressf")
  return(taba1)
  
}


link84<-function84(add,epc)



link84<-link84[,..needlist1]


link84u<- uniqueresult(link84)
dim(link84u)


link84d <- doubleresult(link84)


dim(epc)
# 2488082      27
epc<- matchleft(epc,link84)
dim(epc)
# 2487824      27
#################### method 85 ##################


function85<- function(x,y){
  
  x$bnstreet <-    paste(x$subbuildingname,x$buildingname,sep=" ")
  x$bnstreet <-    paste(x$bnstreet,x$streetdescription,sep=",")
  x$bnstreet <- paste(x$bnstreet,x$dependentlocality,sep=",")
  x$bnstreet <- gsub("[.]", "", x$bnstreet)
  x$bnstreet <- gsub("[']", "", x$bnstreet)
  x$bnstreet <- gsub(" ", "", x$bnstreet)
  x$addressf <-paste(x$postcodelocator,x$bnstreet,sep=",")
  
  
  y$addressfinal <- trimws(y$add)
  y$addressfinal <- gsub("[']", "", y$addressfinal)
  y$addressfinal <- gsub("[.]", "", y$addressfinal)
  y$addressfinal <- gsub(" ", "", y$addressfinal)
  y$addressf <- paste(y$postcode,y$addressfinal,sep=",")
  
  taba1 <- inner_join(x,y,by="addressf")
  return(taba1)
  
}



link85<-function85(add,epc)

length(unique(link85$lmk_key))
link85<-link85[,..needlist1]


link85u<- uniqueresult(link85)
dim(link85u)


link85d <- doubleresult(link85)
dim(link85d)


dim(epc)
# 
epc <- matchleft(epc,link85)
dim(epc)


#2383438      27
#################### method 86 ##################

function86<- function(x,y){
  
  x$bnstreet <-    paste(x$saotext,x$buildingname,sep=" ")
  x$bnstreet <-    paste(x$bnstreet,x$streetdescription,sep=",")
  x$bnstreet <- paste(x$bnstreet,x$dependentlocality,sep=",")
  x$bnstreet <- gsub("[.]", "", x$bnstreet)
  x$bnstreet <- gsub("[']", "", x$bnstreet)
  x$bnstreet <- gsub(" ", "", x$bnstreet)
  x$addressf <-paste(x$postcodelocator,x$bnstreet,sep=",")
  
  
  y$addressfinal <- trimws(y$add)
  y$addressfinal <- gsub("[']", "", y$addressfinal)
  y$addressfinal <- gsub("[.]", "", y$addressfinal)
  y$addressfinal <- gsub(" ", "", y$addressfinal)
  y$addressf <- paste(y$postcode,y$addressfinal,sep=",")
  
  taba1 <- inner_join(x,y,by="addressf")
  return(taba1)
}




link86<-function86(add,epc)
dim(link86)
# 

link86<-link86[,..needlist1]



link86u<- uniqueresult(link86)
dim(link86u)
# 

link86d <- doubleresult(link86)
dim(link86d)

dim(epc)
# 
epc <- matchleft(epc,link86)
dim(epc)
# 2383274      27

#################### method 87 ##################



function87<- function(x,y){
  
  x$bnstreet <-    paste(x$subbuildingname,x$paotext,sep=" ")
  x$bnstreet <-    paste(x$bnstreet,x$streetdescription,sep=",")
  x$bnstreet <- paste(x$bnstreet,x$dependentlocality,sep=",")
  x$bnstreet <- gsub("[.]", "", x$bnstreet)
  x$bnstreet <- gsub("[']", "", x$bnstreet)
  x$bnstreet <- gsub(" ", "", x$bnstreet)
  x$addressf <-paste(x$postcodelocator,x$bnstreet,sep=",")
  
  
  y$addressfinal <- trimws(y$add)
  y$addressfinal <- gsub("[']", "", y$addressfinal)
  y$addressfinal <- gsub("[.]", "", y$addressfinal)
  y$addressfinal <- gsub(" ", "", y$addressfinal)
  y$addressf <- paste(y$postcode,y$addressfinal,sep=",")
  
  taba1 <- inner_join(x,y,by="addressf")
  return(taba1)
  
  
}



link87<-function87(add,epc)
dim(link87)


link87<-link87[,..needlist1]



link87u<- uniqueresult(link87)
dim(link87u)


link87d <- doubleresult(link87)
dim(link87d)


dim(epc)
#
epc <- matchleft(epc,link87)
dim(epc)
#2372924      27
#
#################### method 88 ##################

function88<- function(x,y){
  x$bnstreet <-    paste(x$subbuildingname,x$buildingname,sep=" ")
  x$bnstreet <- gsub("[.]", "", x$bnstreet)
  x$bnstreet <- gsub("[']", "", x$bnstreet)
  x$bnstreet <- gsub(" ", "", x$bnstreet)
  x$addressf <-paste(x$postcodelocator,x$bnstreet,sep=",")
  
  y$addressfinal <- trimws(y$add)
  y$addressfinal <- gsub("[']", "", y$addressfinal)
  y$addressfinal <- gsub("[.]", "", y$addressfinal)
  y$addressfinal <- gsub(" ", "", y$addressfinal)
  y$addressf <- paste(y$postcode,y$addressfinal,sep=",")
  
  taba1 <- inner_join(x,y,by="addressf")
  return(taba1)
  
}






link88<-function88(add,epc)
dim(link88)


link88<-link88[,..needlist1]



link88u<- uniqueresult(link88)
dim(link88u)


link88d <- doubleresult(link88)
dim(link88d)


dim(epc)
# 
epc <- matchleft(epc,link88)
dim(epc)
#2372874      27
#################### method 89 only one  ##################
function89<- function(x,y){
 
  
  x<-x[x$buildingnumber=="",]
  x$bnstreet <-    paste(x$saotext,x$pp,sep=",")
  #x$bnstreet <-    paste(x$bnstreet,x$paostartsuffix,sep="")
  x$bnstreet <-    paste(x$bnstreet,x$streetdescription,sep=",")
  x$bnstreet <- gsub("/", "", x$bnstreet)
  x$bnstreet <- gsub("[.]", "", x$bnstreet)
  x$bnstreet <- gsub("[']", "", x$bnstreet)
  x$bnstreet <- gsub(" ", "", x$bnstreet)
  x$addressf <-paste(x$postcodelocator,x$bnstreet,sep=",")
  
  
  y$addressfinal <- y$add
  y$addressfinal <- gsub("[.]", "", y$addressfinal)
  y$addressfinal <- gsub("[']", "", y$addressfinal)
  y$addressfinal <- gsub("/", "", y$addressfinal)
  y$addressfinal <- gsub(" ", "", y$addressfinal)
  y$addressf <- paste(y$postcode,y$addressfinal,sep=",")
  
  taba1 <- inner_join(x,y,by="addressf")
  return(taba1)
  
}



#
link89<-function89(add,epc)

link89<-link89[,..needlist1]


link89u<- uniqueresult(link89)
dim(link89u)


link89d <- doubleresult(link89)
dim(link89d)

dim(epc)
#
epc <- matchleft(epc,link89)
dim(epc)
#  2369962      27





#################### method 90 ##################

function90<- function(x,y){
  
  x$bnstreet <-    paste(x$saotext,x$pp,sep=",")
  x$bnstreet <-    paste(x$bnstreet,x$streetdescription,sep=" ")
  x$bnstreet <- gsub("/", "", x$bnstreet)
  x$bnstreet <- gsub("[.]", "", x$bnstreet)
  x$bnstreet <- gsub("[']", "", x$bnstreet)
  x$bnstreet <- gsub(" ", "", x$bnstreet)
  x$addressf <-paste(x$postcodelocator,x$bnstreet,sep=",")
  
  
  y$addressfinal <- y$add
  y$addressfinal <- gsub("[.]", "", y$addressfinal)
  y$addressfinal <- gsub("[']", "", y$addressfinal)
  y$addressfinal <- gsub("/", "", y$addressfinal)
  y$addressfinal <- gsub(" ", "", y$addressfinal)
  y$addressf <- paste(y$postcode,y$addressfinal,sep=",")
  
  taba1 <- inner_join(x,y,by="addressf")
  return(taba1)
  
  
}


link90<-function90(add,epc)
length(unique(link90$lmk_key))

link90<-link90[,..needlist1]


link90u<- uniqueresult(link90)
dim(link90u)


link90d <- doubleresult(link90)
dim(link90d)

dim(epc)
#
epc <- matchleft(epc,link90)
dim(epc)
# 2365561      27




#################### method 91 ##################

function91<- function(x,y){
  
  x$bnstreet <-    paste(x$saotext,x$buildingname,sep=",")
  x$bnstreet <-    paste(x$bnstreet,x$buildingnumber,sep=",")
  x$bnstreet <-    paste(x$bnstreet,x$streetdescription,sep=",")
  x$bnstreet <- gsub("[.]", "", x$bnstreet)
  x$bnstreet <- gsub("[']", "", x$bnstreet)
  x$bnstreet <- gsub(" ", "", x$bnstreet)
  x$addressf <-paste(x$postcodelocator,x$bnstreet,sep=",")
  
  y$addressfinal <- trimws(y$add)
  #y$addressfinal <- gsub("[']", "", y$addressfinal)
  y$addressfinal <- gsub("[.]", "", y$addressfinal)
  y$addressfinal <- gsub("[']", "", y$addressfinal)
  y$addressfinal <- gsub(" ", "", y$addressfinal)
  y$addressf <- paste(y$postcode,y$addressfinal,sep=",")
  
  taba1 <- inner_join(x,y,by="addressf")
  return(taba1)
  
  
}


link91<-function91(add,epc)


link91<-link91[,..needlist1]


link91u<- uniqueresult(link91)
dim(link91u)


link91d <- doubleresult(link91)
dim(link91d)

dim(epc)
#
epc <- matchleft(epc,link91)
dim(epc)
#2363181      27


#################### method 92 ##################


function92<- function(x,y){
  
  
  x$bnstreet <-    paste(x$subbuildingname,x$buildingname,sep=",")
  x$bnstreet <-    paste(x$bnstreet,x$pp,sep=",")
  x$bnstreet <-    paste(x$bnstreet,x$streetdescription,sep=",")
  x$bnstreet <- gsub("[.]", "", x$bnstreet)
  x$bnstreet <- gsub("[']", "", x$bnstreet)
  x$bnstreet <- gsub(" ", "", x$bnstreet)
  x$addressf <-paste(x$postcodelocator,x$bnstreet,sep=",")
  
  y$addressfinal <- trimws(y$add)
  #y$addressfinal <- gsub("[']", "", y$addressfinal)
  y$addressfinal <- gsub("[.]", "", y$addressfinal)
  y$addressfinal <- gsub("[']", "", y$addressfinal)
  y$addressfinal <- gsub(" ", "", y$addressfinal)
  y$addressf <- paste(y$postcode,y$addressfinal,sep=",")
  
  taba1 <- inner_join(x,y,by="addressf")
  return(taba1)
  
  
  
  
}

link92<-function92(add,epc)

link92<-link92[,..needlist1]


link92u<- uniqueresult(link92)
dim(link92u)


link92d <- doubleresult(link92)
dim(link92d)


dim(epc)
#2363181      27
epc <- matchleft(epc,link92)
dim(epc)
#  2363174      27


#################### method 93 ##################

function93<- function(x,y){
  
  x$bnstreet <-    paste(x$subbuildingname,x$buildingname,sep=",")
  x$bnstreet <-    paste(x$bnstreet,x$buildingnumber,sep=",")
  x$bnstreet <-    paste(x$bnstreet,x$streetdescription,sep=" ")
  x$bnstreet <- gsub(" ", "", x$bnstreet)
  x$addressf <-paste(x$postcodelocator,x$bnstreet,sep=",")
  
  y$addressfinal <- trimws(y$add)
  
  y$addressfinal <- gsub("[-]", "", y$addressfinal)
  y$addressfinal <- gsub(" ", "", y$addressfinal)
  y$addressf <- paste(y$postcode,y$addressfinal,sep=",")
  
  taba1 <- inner_join(x,y,by="addressf")
  return(taba1)
  
  
}



link93<-function93(add,epc)
dim(link93)
# 
link93<-link93[,..needlist1]

link93u<- uniqueresult(link93)
dim(link93u)
#

link93d <- doubleresult(link93)
dim(link93d)


dim(epc)
#
epc <- matchleft(epc,link93)
dim(epc)
# 2306691      27


#################### method 94  ##################

function94<- function(x,y){
  
  x$bnstreet <-    paste(x$subbuildingname,x$buildingname,sep=",")
  x$bnstreet <-    paste(x$bnstreet,x$pp,sep=",")
  x$bnstreet <-    paste(x$bnstreet,x$streetdescription,sep=" ")
  x$bnstreet <- gsub("[.]", "", x$bnstreet)
  x$bnstreet <- gsub("[']", "", x$bnstreet)
  x$bnstreet <- gsub(" ", "", x$bnstreet)
  x$addressf <-paste(x$postcodelocator,x$bnstreet,sep=",")
  
  y$addressfinal <- trimws(y$add)
  y$addressfinal <- gsub("[.]", "", y$addressfinal)
  y$addressfinal <- gsub("[']", "", y$addressfinal)
  y$addressfinal <- gsub("[-]", "", y$addressfinal)
  y$addressfinal <- gsub(" ", "", y$addressfinal)
  y$addressf <- paste(y$postcode,y$addressfinal,sep=",")
  
  taba1 <- inner_join(x,y,by="addressf")
  return(taba1)
  
}


link94<-function94(add,epc)



link94<-link94[,..needlist1]


link94u<- uniqueresult(link94)
dim(link94u)


link94d <- doubleresult(link94)


dim(epc)
# 
epc<- matchleft(epc,link94)
dim(epc)
# 2303985      27
#################### method 95 ##################


function95<- function(x,y){
  
  
  x$bnstreet <-    paste(x$saotext,x$buildingname,sep=",")
  x$bnstreet <-    paste(x$bnstreet,x$pp,sep=",")
  x$bnstreet <-    paste(x$bnstreet,x$streetdescription,sep=" ")
  x$bnstreet <- gsub("[.]", "", x$bnstreet)
  x$bnstreet <- gsub("[']", "", x$bnstreet)
  x$bnstreet <- gsub(" ", "", x$bnstreet)
  x$addressf <-paste(x$postcodelocator,x$bnstreet,sep=",")
  
  
  y$addressfinal <- y$add
  y$addressfinal <- gsub("[.]", "", y$addressfinal)
  y$addressfinal <- gsub("[']", "", y$addressfinal)
  y$addressfinal <- gsub("[-]", "", y$addressfinal)
  y$addressfinal <- gsub(" ", "", y$addressfinal)
  y$addressf <- paste(y$postcode,y$addressfinal,sep=",")
  
  taba1 <- inner_join(x,y,by="addressf")
  return(taba1)
  
}



link95<-function95(add,epc)


link95<-link95[,..needlist1]


link95u<- uniqueresult(link95)
dim(link95u)


link95d <- doubleresult(link95)
dim(link95d)


dim(epc)
# 
epc <- matchleft(epc,link95)
dim(epc)
#2300965      27

#
#################### method 96 ##################

function96<- function(x,y){
 
  x$bnstreet <-    paste(x$pp,x$townname,sep=",")
  
  x$bnstreet <- gsub(" ", "", x$bnstreet)
  x$addressf <-paste(x$postcodelocator,x$bnstreet,sep=",")
  
  
  
  y$addressfinal <- y$add
  y$addressfinal <- gsub("[.]", "", y$addressfinal)
  y$addressfinal <- gsub(" ", "", y$addressfinal)
  y$addressf <- paste(y$postcode,y$addressfinal,sep=",")
  
  taba1 <- inner_join(x,y,by="addressf")
  return(taba1)
  
}




link96<-function96(add,epc)
dim(link96)
# 

link96<-link96[,..needlist1]



link96u<- uniqueresult(link96)
dim(link96u)
# 

link96d <- doubleresult(link96)
dim(link96d)


# 
epc <- matchleft(epc,link96)
dim(epc)
# 2300004      27

#################### method 97 ##################



function97<- function(x,y){
  
  x$bnstreet <-    paste(x$pp,x$dependentlocality,sep=",")
  #x$bnstreet <- gsub("/", "", x$bnstreet)
  x$bnstreet <- gsub("[.]", "", x$bnstreet)
  x$bnstreet <- gsub("[']", "", x$bnstreet)
  x$bnstreet <- gsub(" ", "", x$bnstreet)
  x$addressf <-paste(x$postcodelocator,x$bnstreet,sep=",")
  
  y$addressfinal <- paste(y$add)
   #y$bnstreet <- gsub("/", "",  y$bnstreet)
  y$bnstreet <- gsub("[.]", "",  y$bnstreet)
  y$bnstreet <- gsub("[']", "",  y$bnstreet)
  y$addressfinal <- gsub(" ", "", y$addressfinal)
  y$addressf <- paste(y$postcode,y$addressfinal,sep=",")
  
  taba1 <- inner_join(x,y,by="addressf")
  return(taba1)
  
  
  
}



link97<-function97(add,epc)
dim(link97)


link97<-link97[,..needlist1]



link97u<- uniqueresult(link97)
dim(link97u)


link97d <- doubleresult(link97)
dim(link97d)


dim(epc)
# 
epc <- matchleft(epc,link97)
dim(epc)

#2286593      27
#################### method 98 ##################

function98<- function(x,y){
  #x<-x[x$paostartsuffix=="",]
  x$bnstreet <-    paste(x$pp,x$locality,sep=",")
  #x$bnstreet <- gsub("/", "", x$bnstreet)
  x$bnstreet <- gsub("[.]", "", x$bnstreet)
  x$bnstreet <- gsub("[']", "", x$bnstreet)
  x$bnstreet <- gsub(" ", "", x$bnstreet)
  x$addressf <-paste(x$postcodelocator,x$bnstreet,sep=",")
  
  y$addressfinal <- paste(y$add)
  # y$bnstreet <- gsub("/", "",  y$bnstreet)
  y$bnstreet <- gsub("[.]", "",  y$bnstreet)
  y$bnstreet <- gsub("[']", "",  y$bnstreet)
  y$addressfinal <- gsub(" ", "", y$addressfinal)
  y$addressf <- paste(y$postcode,y$addressfinal,sep=",")
  
  taba1 <- inner_join(x,y,by="addressf")
  return(taba1)
}






link98<-function98(add,epc)
dim(link98)


link98<-link98[,..needlist1]

length(unique(link98$lmk_key))

link98u<- uniqueresult(link98)
dim(link98u)


link98d <- doubleresult(link98)
dim(link98d)


dim(epc)
# 
epc <- matchleft(epc,link98)
dim(epc)

#################### method 99 ##################
function99<- function(x,y){
  
  
  x$bnstreet <-    paste(x$saotext,x$buildingnumber ,sep=",")
  #x$bnstreet <-    paste(x$bnstreet,x$paostartsuffix ,sep=" ")
  x$bnstreet <-    paste(x$bnstreet,x$streetdescription ,sep=",")
  x$bnstreet <- gsub("[.]", "", x$bnstreet)
  x$bnstreet <- gsub("[']", "", x$bnstreet)
  x$bnstreet <- gsub(" ", "", x$bnstreet)
  x$addressf <-paste(x$postcodelocator,x$bnstreet,sep=",")
  
  y$addressfinal <- paste(y$add)
  y$addressfinal <- gsub("[.]", "", y$addressfinal)
  y$addressfinal <- gsub("[']", "", y$addressfinal)
  y$addressfinal <- gsub(" ", "", y$addressfinal)
  y$addressf <- paste(y$postcode,y$addressfinal,sep=",")
  
  taba1 <- inner_join(x,y,by="addressf")
  return(taba1)
  
  
  
}



#
link99<-function99(add,epc)
bbbbbbb
link99<-link99[,..needlist1]


link99u<- uniqueresult(link99)
dim(link99u)


link99d <- doubleresult(link99)
dim(link99d)

dim(epc)
#
epc <- matchleft(epc,link99)
dim(epc)

#2274221      27

Sys.time()

########summ up##############
link70u$method<-"link70u"
link71u$method<-"link71u"
link72u$method<-"link72u"
link73u$method<-"link73u"
link74u$method<-"link74u"
link75u$method<-"link75u"
link76u$method<-"link76u"
link77u$method<-"link77u"
link78u$method<-"link78u"
link79u$method<-"link79u"
link80u$method<-"link80u"
link81u$method<-"link81u"
link82u$method<-"link82u"
link83u$method<-"link83u"
link84u$method<-"link84u"
link85u$method<-"link85u"
link86u$method<-"link86u"
link87u$method<-"link87u"
link88u$method<-"link88u"
link89u$method<-"link89u"

link90u$method<-"link90u"
link91u$method<-"link91u"
link92u$method<-"link92u"
link93u$method<-"link93u"
link94u$method<-"link94u"
link95u$method<-"link95u"
link96u$method<-"link96u"
link97u$method<-"link97u"
link98u$method<-"link98u"
link99u$method<-"link99u"

link70d$method<-"link70d"
link71d$method<-"link71d"
link72d$method<-"link72d"
link73d$method<-"link73d"
link74d$method<-"link74d"
link75d$method<-"link75d"
link76d$method<-"link76d"
link77d$method<-"link77d"
link78d$method<-"link78d"
link79d$method<-"link79d"
link80d$method<-"link80d"
link81d$method<-"link81d"
link82d$method<-"link82d"
link83d$method<-"link83d"
link84d$method<-"link84d"
link85d$method<-"link85d"
link86d$method<-"link86d"
link87d$method<-"link87d"
link88d$method<-"link88d"
link89d$method<-"link89d"
link90d$method<-"link90d"
link91d$method<-"link91d"
link92d$method<-"link92d"
link93d$method<-"link93d"
link94d$method<-"link94d"
link95d$method<-"link95d"
link96d$method<-"link96d"
link97d$method<-"link97d"
link98d$method<-"link98d"
link99d$method<-"link99d"


Sys.time()
#"2022-03-10 23:14:39 GMT"

l70_99u = list(link70u,link71u,link72u,link73u,link74u,link75u,link76u,link77u,link78u,link79u,link80u,link81u,link82u,link83u,link84u,link85u,link86u,link87u,link88u,link89u,link90u,link91u,link92u,link93u,link94u,link95u,link96u,link97u,link98u,link99u)
link70_99u<- rbindlist(l70_99u)
dim(link70_99u)
#1156088
dim(link70u)[1]+dim(link71u)[1]+dim(link72u)[1]+dim(link73u)[1]+dim(link74u)[1]+dim(link75u)[1]+dim(link76u)[1]+dim(link77u)[1]+dim(link78u)[1]+dim(link79u)[1]+dim(link80u)[1]+dim(link81u)[1]+dim(link82u)[1]+dim(link83u)[1]+dim(link84u)[1]+dim(link85u)[1]+dim(link86u)[1]+dim(link87u)[1]+dim(link88u)[1]+dim(link89u)[1]+
  dim(link90u)[1]+dim(link91u)[1]+dim(link92u)[1]+dim(link93u)[1]+dim(link94u)[1]+dim(link95u)[1]+dim(link96u)[1]+dim(link97u)[1]+dim(link98u)[1]+dim(link99u)[1]
#1156088
length(unique(link70_99u$method))
length(unique(link70_99u$lmk_key))
#1156088
c1<-doubleresult(link70_99u)
l70_99d = list(link70d,link71d,link72d,link73d,link74d,link75d,link76d,link77d,link78d,link79d,link80d,link81d,link82d,link83d,link84d,link85d,link86d,link87d,link88d,link89d,link90d,link91d,link92d,link93d,link94d,link95d,link96d,link97d,link98d,link99d)
link70_99d<- rbindlist(l70_99d, use.names=TRUE, fill=TRUE)
dim(link70_99d)[1]
#11111
dim(link70d)[1]+dim(link71d)[1]+dim(link72d)[1]+dim(link73d)[1]+dim(link74d)[1]+dim(link75d)[1]+dim(link76d)[1]+dim(link77d)[1]+dim(link78d)[1]+dim(link79d)[1]+dim(link80d)[1]+dim(link81d)[1]+dim(link82d)[1]+dim(link83d)[1]+dim(link84d)[1]+dim(link85d)[1]+dim(link86d)[1]+dim(link87d)[1]+dim(link88d)[1]+dim(link89d)[1]+
  dim(link90d)[1]+dim(link91d)[1]+dim(link92d)[1]+dim(link93d)[1]+dim(link94d)[1]+dim(link95d)[1]+dim(link96d)[1]+dim(link97d)[1]+dim(link98d)[1]+dim(link99d)[1]
#11111

rm(link70d,link71d,link72d,link73d,link74d,link75d,link76d,link77d,link78d,link79d,link80d,link81d,link82d,link83d,link84d,link85d,link86d,link87d,link88d,link89d,link90d,link91d,link92d,link93d,link94d,link95d,link96d,link97d,link98d,link99d)
rm(link70u,link71u,link72u,link73u,link74u,link75u,link76u,link77u,link78u,link79u,link80u,link81u,link82u,link83u,link84u,link85u,link86u,link87u,link88u,link89u,link90u,link91u,link92u,link93u,link94u,link95u,link96u,link97u,link98u,link99u)
rm(link70,link71,link72,link73,link74,link75,link76,link77,link78,link79,link80,link81,link82,link83,link84,link85,link86,link87,link88,link89,link90,link91,link92,link93,link94,link95,link96,link97,link98,link99)
rm(l70_99u,l70_99d)
rm(function70,function71,function72,function73,function74,function75,function76,function77,function78,function79,function80,function81,function82,function83,function84,function85,function86,function87,function88,function89,function90,function91,function92,function93,function94,function95,function96,function97,function98,function99)

dbWriteTable(con, "link70_99unew",value =link70_99u, append = TRUE, row.names = FALSE)
dbWriteTable(con, "link70_99dnew",value =link70_99d, append = TRUE, row.names = FALSE)

Sys.time()
#################### method 100 ##################

function100<- function(x,y){
  
  
  x$bnstreet <-    paste(x$saotext,x$pp ,sep=",")
  #x$bnstreet <-    paste(x$bnstreet,x$paostartsuffix ,sep=" ")
  x$bnstreet <-    paste(x$bnstreet,x$streetdescription ,sep=",")
  x$bnstreet <- gsub("/", "", x$bnstreet)
  x$bnstreet <- gsub("[.]", "", x$bnstreet)
  x$bnstreet <- gsub("[']", "", x$bnstreet)
  x$bnstreet <- gsub(" ", "", x$bnstreet)
  x$addressf <-paste(x$postcodelocator,x$bnstreet,sep=",")
  
  y$addressfinal <- paste(y$add)
  y$addressfinal <- gsub("/", "", y$addressfinal)
  y$addressfinal <- gsub("[.]", "", y$addressfinal)
  y$addressfinal <- gsub("[']", "", y$addressfinal)
  y$addressfinal <- gsub(" ", "", y$addressfinal)
  y$addressf <- paste(y$postcode,y$addressfinal,sep=",")
  
  taba1 <- inner_join(x,y,by="addressf")
  return(taba1)
  
  
}


link100<-function100(add,epc)

length(unique(link100$lmk_key))
link100<-link100[,..needlist1]


link100u<- uniqueresult(link100)
dim(link100u)


link100d <- doubleresult(link100)
dim(link100d)

dim(epc)
#
epc <- matchleft(epc,link100)
dim(epc)
#2279730      27

#################### method 101 ##################

function101<- function(x,y){
  
  x$bnstreet <-    paste(x$subbuildingname,x$paostartnumber ,sep=",")
  x$bnstreet <-    paste(x$bnstreet,x$paostartsuffix ,sep=" ")
  x$bnstreet <-    paste(x$bnstreet,x$streetdescription ,sep=",")
  x$bnstreet <- gsub("/", "", x$bnstreet)
  x$bnstreet <- gsub("[.]", "", x$bnstreet)
  x$bnstreet <- gsub("[']", "", x$bnstreet)
  x$bnstreet <- gsub(" ", "", x$bnstreet)
  x$addressf <-paste(x$postcodelocator,x$bnstreet,sep=",")
  
  y$addressfinal <- paste(y$add)
  y$addressfinal <- gsub("/", "", y$addressfinal)
  y$addressfinal <- gsub("[.]", "", y$addressfinal)
  y$addressfinal <- gsub("[']", "", y$addressfinal)
  y$addressfinal <- gsub(" ", "", y$addressfinal)
  y$addressf <- paste(y$postcode,y$addressfinal,sep=",")
  
  taba1 <- inner_join(x,y,by="addressf")
  return(taba1)
  
  
}


link101<-function101(add,epc)


link101<-link101[,..needlist1]


link101u<- uniqueresult(link101)
dim(link101u)


link101d <- doubleresult(link101)
dim(link101d)

dim(epc)
#
epc <- matchleft(epc,link101)
dim(epc)
#2278669      27


#################### method 102 ##################


function102<- function(x,y){
  
  
  x$bnstreet <-    paste(x$subbuildingname,x$buildingname ,sep=",")
  
  x$bnstreet <-    paste(x$bnstreet,x$streetdescription ,sep=",")
  x$bnstreet <- gsub("[.]", "", x$bnstreet)
  x$bnstreet <- gsub("[']", "", x$bnstreet)
  x$bnstreet <- gsub(" ", "", x$bnstreet)
  x$addressf <-paste(x$postcodelocator,x$bnstreet,sep=",")
  
  y$addressfinal <- paste(y$add)
  y$addressfinal <- gsub("[.]", "", y$addressfinal)
  y$addressfinal <- gsub("[']", "", y$addressfinal)
  y$addressfinal <- gsub(" ", "", y$addressfinal)
  y$addressf <- paste(y$postcode,y$addressfinal,sep=",")
  
  taba1 <- inner_join(x,y,by="addressf")
  return(taba1) 
  
  
  
}

link102<-function102(add,epc)

link102<-link102[,..needlist1]


link102u<- uniqueresult(link102)
dim(link102u)


link102d <- doubleresult(link102)
dim(link102d)


dim(epc)
#
epc <- matchleft(epc,link102)
dim(epc)
# 2277648      27



#################### method 103 ##################

function103<- function(x,y){
  
  x$bnstreet <-    paste(x$buildingnumber,x$paotext,sep=",")
  x$bnstreet <-    paste(x$bnstreet,x$locality,sep=",")
  x$bnstreet <- gsub("[.]", "", x$bnstreet)
  x$bnstreet <- gsub("[']", "", x$bnstreet)
  x$bnstreet <- gsub(" ", "", x$bnstreet)
  x$addressf <-paste(x$postcodelocator,x$bnstreet,sep=",")
  
  y$addressfinal <- y$add
  y$addressfinal <- gsub("[.]", "", y$addressfinal)
  y$addressfinal <- gsub("[']", "", y$addressfinal)
  #y$addressfinal <- gsub("[/]", "", y$addressfinal)
  #y$addressfinal <- gsub("[.]", "", y$addressfinal)
  y$addressfinal <- gsub(" ", "", y$addressfinal)
  y$addressf <- paste(y$postcode,y$addressfinal,sep=",")
  
  taba1 <- inner_join(x,y,by="addressf")
  return(taba1)
  
}



link103<-function103(add,epc)
dim(link103)
# 
link103<-link103[,..needlist1]

link103u<- uniqueresult(link103)
dim(link103u)
#

link103d <- doubleresult(link103)
dim(link103d)


dim(epc)
#
epc <- matchleft(epc,link103)
dim(epc)
# 2277634      27


#################### method 104  ##################

function104<- function(x,y){
  
  x$bnstreet <-    paste(x$buildingname,x$streetdescription,sep=" ")
  x$bnstreet <-    paste(x$bnstreet,x$locality,sep=",")
  x$bnstreet <- gsub("[.]", "", x$bnstreet)
  x$bnstreet <- gsub("[']", "", x$bnstreet)
  x$bnstreet <- gsub(" ", "", x$bnstreet)
  x$addressf <-paste(x$postcodelocator,x$bnstreet,sep=",")
  
  y$addressfinal <- trimws(y$add)
  y$addressfinal <- gsub("[.]", "", y$addressfinal)
  y$addressfinal <- gsub("[']", "", y$addressfinal)
  y$addressfinal <- gsub(" ", "", y$addressfinal)
  y$addressf <- paste(y$postcode,y$addressfinal,sep=",")
  
  taba1 <- inner_join(x,y,by="addressf")
  return(taba1)
  
}


link104<-function104(add,epc)



link104<-link104[,..needlist1]


link104u<- uniqueresult(link104)
dim(link104u)


link104d <- doubleresult(link104)
#head(link104[grepl("[']", link104$add),])
#head(link104[grepl("[.]", link104$add),])

dim(epc)
# 
epc<- matchleft(epc,link104)
dim(epc)
#  2272664      27
#################### method 105 ##################


function105<- function(x,y){
  x$bnstreet <-    paste(x$subbuildingname,x$buildingnumber,sep=",")
  x$bnstreet <-    paste(x$bnstreet,x$streetdescription,sep=",")
  x$bnstreet <- gsub("[.]", "", x$bnstreet)
  x$bnstreet <- gsub("[']", "", x$bnstreet)
  x$bnstreet <- gsub(" ", "", x$bnstreet)
  x$addressf <-paste(x$postcodelocator,x$bnstreet,sep=",")
  
  y$addressfinal <- trimws(y$add)
  y$addressfinal <- gsub("/", "", y$addressfinal)
  y$addressfinal <- gsub("[.]", "", y$addressfinal)
  y$addressfinal <- gsub("[']", "", y$addressfinal)
  y$addressfinal <- gsub(" ", "", y$addressfinal)
  y$addressf <- paste(y$postcode,y$addressfinal,sep=",")
  
  taba1 <- inner_join(x,y,by="addressf")
  return(taba1)
  
  
}



link105<-function105(add,epc)


link105<-link105[,..needlist1]


link105u<- uniqueresult(link105)
dim(link105u)


link105d <- doubleresult(link105)
dim(link105d)


dim(epc)
#
epc <- matchleft(epc,link105)
dim(epc)
#2270304      27

#
#################### method 106 ##################

function106<- function(x,y){

  x$bnstreet <-    paste(x$buildingnumber,x$paotext,sep=",")
  x$bnstreet <-    paste(x$bnstreet,x$locality,sep=" ")
  x$bnstreet <- gsub("/", "", x$bnstreet)
  x$bnstreet <- gsub("[.]", "", x$bnstreet)
  x$bnstreet <- gsub("[']", "", x$bnstreet)
  x$bnstreet <- gsub(" ", "", x$bnstreet)
  x$addressf <-paste(x$postcodelocator,x$bnstreet,sep=",")
  
  
  
  y$addressfinal <- y$add
  y$addressfinal <- gsub("/", "", y$addressfinal)
  y$addressfinal <- gsub("[']", "", y$addressfinal)
  y$addressfinal <- gsub("[.]", "", y$addressfinal)
  y$addressfinal <- gsub(" ", "", y$addressfinal)
  y$addressf <- paste(y$postcode,y$addressfinal,sep=",")
  
  taba1 <- inner_join(x,y,by="addressf")
  return(taba1)
  
}




link106<-function106(add,epc)
dim(link106)
# 

link106<-link106[,..needlist1]



link106u<- uniqueresult(link106)
dim(link106u)
# 

link106d <- doubleresult(link106)
dim(link106d)


epc <- matchleft(epc,link106)
dim(epc)
# 2270251      27

#################### method 107 ##################



function107<- function(x,y){
  
  
  #x<-x[x$paostartsuffix=="",]
  x<-x[x$saotext=="",]
  x$bnstreet <-    paste(x$pp,x$streetdescription,sep=",")
  x$bnstreet <- gsub("[']", "", x$bnstreet)
  x$bnstreet <- gsub("[.]", "", x$bnstreet)
  x$bnstreet <- gsub(" ", "", x$bnstreet)
  x$addressf <-paste(x$postcodelocator,x$bnstreet,sep=",")
  
  
  
  y$addressfinal <- y$add
  y$addressfinal <- gsub("[.]", "", y$addressfinal)
  #y$addressfinal <- gsub("[']", "", y$addressfinal)
  y$addressfinal <- gsub(" ", "", y$addressfinal)
  y$addressf <- paste(y$postcode,y$addressfinal,sep=",")
  
  taba1 <- inner_join(x,y,by="addressf")
  return(taba1)
  
}

link107<-function107(add,epc)
dim(link107)


link107<-link107[,..needlist1]

link107u<- uniqueresult(link107)
dim(link107u)


link107d <- doubleresult(link107)
dim(link107d)


dim(epc)
# 
epc <- matchleft(epc,link107)
dim(epc)
#2270190      27
#
#################### method 108 ##################

function108<- function(x,y){
  #x<-x[x$paostartsuffix=="",]
  
  x$bnstreet <-    paste(x$pp,x$streetdescription,sep=" ")
  x$bnstreet <-    paste(x$bnstreet,x$townname,sep=",")
  x$bnstreet <- gsub("[.]", "", x$bnstreet)
  x$bnstreet <- gsub("[']", "", x$bnstreet)
  x$bnstreet <- gsub(" ", "", x$bnstreet)
  x$addressf <-paste(x$postcodelocator,x$bnstreet,sep=",")
  
  
  
  y$addressfinal <- y$add
  y$addressfinal <- gsub("[.]", "", y$addressfinal)
  y$addressfinal <- gsub("[']", "", y$addressfinal)
  y$addressfinal <- gsub("[/]", "", y$addressfinal)
  y$addressfinal <- gsub(" ", "", y$addressfinal)
  y$addressf <- paste(y$postcode,y$addressfinal,sep=",")
  
  taba1 <- inner_join(x,y,by="addressf")
  return(taba1)
  
}



link108<-function108(add,epc)
dim(link108)


link108<-link108[,..needlist1]



link108u<- uniqueresult(link108)
dim(link108u)


link108d <- doubleresult(link108)
dim(link108d)


dim(epc)
# 
epc <- matchleft(epc,link108)
dim(epc)
#2269509      27
#################### method 109 ##################
function109<- function(x,y){
  x$bnstreet <-    paste(x$buildingnumber,x$paotext,sep=",")
  x$bnstreet <-    paste(x$bnstreet,x$dependentlocality,sep=",")
  x$bnstreet <- gsub("[.]", "", x$bnstreet)
  x$bnstreet <- gsub("[']", "", x$bnstreet)
  x$bnstreet <- gsub(" ", "", x$bnstreet)
  x$addressf <-paste(x$postcodelocator,x$bnstreet,sep=",")
  
  y$addressfinal <- y$add
  y$addressfinal <- gsub("[.]", "", y$addressfinal)
  y$addressfinal <- gsub("[']", "", y$addressfinal)
  #y$addressfinal <- gsub("[/]", "", y$addressfinal)
  #y$addressfinal <- gsub("[.]", "", y$addressfinal)
  y$addressfinal <- gsub(" ", "", y$addressfinal)
  y$addressf <- paste(y$postcode,y$addressfinal,sep=",")
  
  taba1 <- inner_join(x,y,by="addressf")
  return(taba1)
  
}


link109<-function109(add,epc)

link109<-link109[,..needlist1]


link109u<- uniqueresult(link109)
dim(link109u)


link109d <- doubleresult(link109)
dim(link109d)

dim(epc)
#2649854      27
epc <- matchleft(epc,link109)
dim(epc)
#





#################### method 110 ##################

function110<- function(x,y){
  x$bnstreet <-    paste(x$ss,x$paotext,sep=",")
  x$bnstreet <-    paste(x$bnstreet,x$dependentlocality,sep=",")
  x$bnstreet <- gsub("[.]", "", x$bnstreet)
  x$bnstreet <- gsub("[']", "", x$bnstreet)
  x$bnstreet <- gsub(" ", "", x$bnstreet)
  x$addressf <-paste(x$postcodelocator,x$bnstreet,sep=",")
  
  y$addressfinal <- y$add
  
  y$addressfinal <- gsub("[.]", "", y$addressfinal)
  #y$addressfinal <- gsub("[/]", "", y$addressfinal)
  y$addressfinal <- gsub("[']", "", y$addressfinal)
  y$addressfinal <- gsub(" ", "", y$addressfinal)
  y$addressf <- paste(y$postcode,y$addressfinal,sep=",")
  
  taba1 <- inner_join(x,y,by="addressf")
  return(taba1)
  
  
  
}


link110<-function110(add,epc)


link110<-link110[,..needlist1]


link110u<- uniqueresult(link110)
dim(link110u)


link110d <- doubleresult(link110)
dim(link110d)

dim(epc)
#
epc <- matchleft(epc,link110)
dim(epc)
#2208691      27

#################### method 111 ##################

function111<- function(x,y){
  
 
  
  x$bnstreet <-    paste(x$subbuildingname,x$buildingname,sep=",")

  x$bnstreet <-    paste(x$bnstreet,x$paotext,sep=" ")
  x$bnstreet <-    paste(x$bnstreet,x$buildingnumber,sep=",")
  x$bnstreet <-    paste(x$bnstreet,x$streetdescription,sep=" ")
  x$bnstreet <- gsub("[']", "", x$bnstreet)
  x$bnstreet <- gsub("[.]", "", x$bnstreet)
  x$bnstreet <- gsub("/", "", x$bnstreet)
  x$bnstreet <- gsub(" ", "", x$bnstreet)
  x$addressf <-paste(x$postcodelocator,x$bnstreet,sep=", ")


  y$addressfinal <-y$add
  y$addressfinal <- gsub("/", "", y$addressfinal)
  y$addressfinal <- gsub("[.]", "", y$addressfinal)
  y$addressfinal <- gsub("[']", "", y$addressfinal)
  y$addressfinal <- gsub(" ", "", y$addressfinal)
  y$addressf <- paste(y$postcode,y$addressfinal,sep=", ")

  taba1 <- inner_join(x,y,by="addressf")
  return(taba1)
  
}


link111<-function111(add,epc)


link111<-link111[,..needlist1]


link111u<- uniqueresult(link111)
dim(link111u)


link111d <- doubleresult(link111)
dim(link111d)

dim(epc)
#
epc <- matchleft(epc,link111)
dim(epc)
#2208307      27


#################### method 112 ##################

function112<- function(x,y){
  
  
  #x<-x[x$saostartsuffix=="",]
  x$bnstreet <-    paste(x$ss,x$paotext,sep=" ")
  x$bnstreet <-    paste(x$bnstreet,x$dependentlocality,sep=",")
  x$bnstreet <- gsub("[.]", "", x$bnstreet)
  x$bnstreet <- gsub("[']", "", x$bnstreet)
  x$bnstreet <- gsub(" ", "", x$bnstreet)
  x$addressf <-paste(x$postcodelocator,x$bnstreet,sep=",")
  
  y$addressfinal <- y$add
  y$addressfinal <- gsub("[']", "", y$addressfinal)
  y$addressfinal <- gsub("[.]", "", y$addressfinal)
  y$addressfinal <- gsub(" ", "", y$addressfinal)
  y$addressf <- paste(y$postcode,y$addressfinal,sep=",")
  
  taba1 <- inner_join(x,y,by="addressf")
  return(taba1)
  
  
}

link112<-function112(add,epc)
length(unique(link112$lmk_key))
link112<-link112[,..needlist1]


link112u<- uniqueresult(link112)
dim(link112u)


link112d <- doubleresult(link112)
dim(link112d)


dim(epc)
#
epc <- matchleft(epc,link112)
dim(epc)
# 2199076      27



#################### method 113 ##################

function113<- function(x,y){
  x<-x[x$saostartsuffix=="",]
  x$bnstreet <-    paste(x$buildingnumber,x$paotext,sep=" ")
  x$bnstreet <-    paste(x$bnstreet,x$dependentlocality,sep=",")
  x$bnstreet <- gsub("[.]", "", x$bnstreet)
  x$bnstreet <- gsub("[']", "", x$bnstreet)
  x$bnstreet <- gsub(" ", "", x$bnstreet)
  x$addressf <-paste(x$postcodelocator,x$bnstreet,sep=",")
  
  y$addressfinal <- y$add
  y$addressfinal <- gsub("[.]", "", y$addressfinal)
  y$addressfinal <- gsub("[']", "", y$addressfinal)
  y$addressfinal <- gsub(" ", "", y$addressfinal)
  y$addressf <- paste(y$postcode,y$addressfinal,sep=",")
  
  taba1 <- inner_join(x,y,by="addressf")
  return(taba1)
  
  
}

link113<-function113(add,epc)
dim(link113)
# 
link113<-link113[,..needlist1]

link113u<- uniqueresult(link113)
dim(link113u)
#

link113d <- doubleresult(link113)
dim(link113d)


dim(epc)
#
epc <- matchleft(epc,link113)
dim(epc)
# 2196395      27


#################### method 114  ##################

function114<- function(x,y){
  #x<-x[x$saostartsuffix=="",]
  x$bnstreet <-    paste(x$buildingnumber,x$buildingname,sep=" ")
  x$bnstreet <-    paste(x$bnstreet,x$dependentlocality,sep=",")
  x$bnstreet <- gsub("[.]", "", x$bnstreet)
  x$bnstreet <- gsub("[']", "", x$bnstreet)
  x$bnstreet <- gsub(" ", "", x$bnstreet)
  x$addressf <-paste(x$postcodelocator,x$bnstreet,sep=",")
  
  y$addressfinal <- y$add
  y$addressfinal <- gsub("[.]", "", y$addressfinal)
  y$addressfinal <- gsub("[']", "", y$addressfinal)
  y$addressfinal <- gsub(" ", "", y$addressfinal)
  y$addressf <- paste(y$postcode,y$addressfinal,sep=",")
  
  taba1 <- inner_join(x,y,by="addressf")
  return(taba1)
  
  
}


link114<-function114(add,epc)

link114<-link114[,..needlist1]


link114u<- uniqueresult(link114)
dim(link114u)


link114d <- doubleresult(link114)


dim(epc)
# 
epc<- matchleft(epc,link114)
dim(epc)
#  2195596      27
#################### method 115 ##################


function115<- function(x,y){
  
  #x<-x[x$saostartsuffix=="",]
  x$bnstreet <-    paste(x$ss,x$paotext,sep=" ")
  x$bnstreet <-    paste(x$bnstreet,x$streetdescription,sep=",")
  x$bnstreet <- gsub("[.]", "", x$bnstreet)
  x$bnstreet <- gsub("[']", "", x$bnstreet)
  x$bnstreet <- gsub(" ", "", x$bnstreet)
  x$addressf <-paste(x$postcodelocator,x$bnstreet,sep=",")
  
  
  
  y$addressfinal <- y$add
  y$addressfinal <- gsub("[.]", "", y$addressfinal)
  
  y$addressfinal <- gsub("[']", "", y$addressfinal)
  y$addressfinal <- gsub(" ", "", y$addressfinal)
  y$addressf <- paste(y$postcode,y$addressfinal,sep=",")
  
  taba1 <- inner_join(x,y,by="addressf")
  return(taba1)
  
}



link115<-function115(add,epc)


link115<-link115[,..needlist1]
length(unique(link115$lmk_key))

link115u<- uniqueresult(link115)
dim(link115u)


link115d <- doubleresult(link115)
dim(link115d)


dim(epc)
# 
epc <- matchleft(epc,link115)
dim(epc)
#2171198      27

#
#################### method 116 ##################

function116<- function(x,y){
  
  #x<-x[x$saostartsuffix=="",]
  x$bnstreet <-    paste(x$ss,x$buildingname,sep=" ")
  x$bnstreet <-    paste(x$bnstreet,x$streetdescription,sep=",")
  x$bnstreet <- gsub("[.]", "", x$bnstreet)
  x$bnstreet <- gsub("[']", "", x$bnstreet)
  x$bnstreet <- gsub(" ", "", x$bnstreet)
  x$addressf <-paste(x$postcodelocator,x$bnstreet,sep=",")
  
  
  
  y$addressfinal <- y$add
  y$addressfinal <- gsub("[.]", "", y$addressfinal)
  #y$addressfinal <- gsub("[.]", "", y$addressfinal)
  y$addressfinal <- gsub("[']", "", y$addressfinal)
  y$addressfinal <- gsub(" ", "", y$addressfinal)
  y$addressf <- paste(y$postcode,y$addressfinal,sep=",")
  
  taba1 <- inner_join(x,y,by="addressf")
  return(taba1)
  
}

link116<-function116(add,epc)
dim(link116)
# 

link116<-link116[,..needlist1]



link116u<- uniqueresult(link116)
dim(link116u)
# 

link116d <- doubleresult(link116)
dim(link116d)

dim(epc)
# 
epc <- matchleft(epc,link116)
dim(epc)
# 2169243      27

#################### method 117 ##################



function117<- function(x,y){
  
  x<-x[x$paostartsuffix=="",]
  
  x$bnstreet <-    paste(x$saotext ,x$paostartnumber,sep=",")
  x$bnstreet <-    paste(x$bnstreet ,x$paoendnumber,sep="-")
  x$bnstreet <-    paste(x$bnstreet,x$streetdescription,sep=" ")
  x$bnstreet <- gsub(" ", "", x$bnstreet)
  x$addressf <-paste(x$postcodelocator,x$bnstreet,sep=",")
  
  
  y$addressfinal <-y$add
  
  y$addressfinal <- gsub("[.]", "", y$addressfinal)
  y$addressfinal <- gsub("[']", "", y$addressfinal)
  y$addressfinal <- gsub(" ", "", y$addressfinal)
  y$addressf <- paste(y$postcode,y$addressfinal,sep=",")
  
  taba1 <- inner_join(x,y,by="addressf")
  return(taba1)
  
}



link117<-function117(add,epc)
dim(link117)


link117<-link117[,..needlist1]



link117u<- uniqueresult(link117)
dim(link117u)


link117d <- doubleresult(link117)
dim(link117d)


dim(epc)
# 
epc <- matchleft(epc,link117)
dim(epc)
#2167406      27
#
#################### method 118 ##################

function118<- function(x,y){
  #x<-x[x$paostartsuffix=="",]
  
  x$bnstreet <-    paste(x$subbuildingname ,x$paostartnumber,sep=",")
  x$bnstreet <-    paste(x$bnstreet ,x$paoendnumber,sep="-")
  x$bnstreet <-    paste(x$bnstreet,x$streetdescription,sep=" ")
  x$bnstreet <- gsub(" ", "", x$bnstreet)
  x$addressf <-paste(x$postcodelocator,x$bnstreet,sep=",")
  
  
  y$addressfinal <-y$add
  
  y$addressfinal <- gsub("[.]", "", y$addressfinal)
  y$addressfinal <- gsub("[']", "", y$addressfinal)
  y$addressfinal <- gsub(" ", "", y$addressfinal)
  y$addressf <- paste(y$postcode,y$addressfinal,sep=",")
  
  taba1 <- inner_join(x,y,by="addressf")
  return(taba1)
  
}


link118<-function118(add,epc)
dim(link118)


link118<-link118[,..needlist1]



link118u<- uniqueresult(link118)
dim(link118u)


link118d <- doubleresult(link118)
dim(link118d)


dim(epc)
# 
epc <- matchleft(epc,link118)
dim(epc)
#2167336      27
#################### method 119 ##################
function119<- function(x,y){
  x$bnstreet <-    paste(x$buildingname,x$streetdescription,sep=",")
  #x$bnstreet <- gsub("[']", "", x$bnstreet)
  x$bnstreet <- gsub("[.]", "", x$bnstreet)
  x$bnstreet <- gsub("[']", "", x$bnstreet)
  x$bnstreet <- gsub(" ", "", x$bnstreet)
  x$addressf <-paste(x$postcodelocator,x$bnstreet,sep=",")
  
  
  y$addressfinal <- y$add
  y$addressfinal <- gsub("[.]", "", y$addressfinal)
  y$addressfinal <- gsub("[']", "", y$addressfinal)
  y$addressfinal <- gsub(" ", "", y$addressfinal)
  y$addressf <- paste(y$postcode,y$addressfinal,sep=",")
  
  taba1 <- inner_join(x,y,by="addressf")
  return(taba1) 
  
  
  
}


#
link119<-function119(add,epc)

link119<-link119[,..needlist1]

length(unique(link119$lmk_key))

link119u<- uniqueresult(link119)
dim(link119u)


link119d <- doubleresult(link119)
dim(link119d)

dim(epc)
#
epc <- matchleft(epc,link119)
dim(epc)
#2166770      27

#################### method 120 ##################

function120<- function(x,y){
  
  x<-x[x$paostartsuffix=="",]
  
  x$bnstreet <-    paste(x$subbuildingname,x$buildingnumber,sep=",")
  x$bnstreet <-    paste(x$bnstreet,x$streetdescription,sep=" ")
  
  x$bnstreet <- gsub("[.]", "", x$bnstreet)
  x$bnstreet <- gsub("[']", "", x$bnstreet)
  
  x$bnstreet <- gsub(" ", "", x$bnstreet)
  x$addressf <-paste(x$postcodelocator,x$bnstreet,sep=",")
  
  y$addressfinal <- y$add
  
  y$addressfinal <- gsub("[.]", "", y$addressfinal)
  y$addressfinal <- gsub("[']", "", y$addressfinal)
  y$addressfinal <- gsub("[/]", "", y$addressfinal)
  y$addressfinal <- gsub(" ", "", y$addressfinal)
  y$addressf <- paste(y$postcode,y$addressfinal,sep=",")
  
  taba1 <- inner_join(x,y,by="addressf")
  return(taba1)
  
}

link120<-function120(add,epc)

length(unique(link120$lmk_key))
link120<-link120[,..needlist1]


link120u<- uniqueresult(link120)
dim(link120u)


link120d <- doubleresult(link120)
dim(link120d)

dim(epc)
#
epc <- matchleft(epc,link120)
dim(epc)
# 2165883      27

#################### method 121 ##################

function121<- function(x,y){
  #x<-x[x$paostartsuffix=="",]
  
  x$bnstreet <-    paste(x$subbuildingname,x$pp,sep=",")
  x$bnstreet <-    paste(x$bnstreet,x$streetdescription,sep=" ")
  
  x$bnstreet <- gsub("[.]", "", x$bnstreet)
  x$bnstreet <- gsub("[']", "", x$bnstreet)
  
  x$bnstreet <- gsub(" ", "", x$bnstreet)
  x$addressf <-paste(x$postcodelocator,x$bnstreet,sep=",")
  
  y$addressfinal <- y$add
  
  y$addressfinal <- gsub("[.]", "", y$addressfinal)
  y$addressfinal <- gsub("[']", "", y$addressfinal)
  y$addressfinal <- gsub("[/]", "", y$addressfinal)
  y$addressfinal <- gsub(" ", "", y$addressfinal)
  y$addressf <- paste(y$postcode,y$addressfinal,sep=",")
  
  taba1 <- inner_join(x,y,by="addressf")
  return(taba1)
  
  
}


link121<-function121(add,epc)

link121<-link121[,..needlist1]


link121u<- uniqueresult(link121)
dim(link121u)


link121d <- doubleresult(link121)
dim(link121d)

dim(epc)
#
epc <- matchleft(epc,link121)
dim(epc)
#2165841      27


#################### method 122 ##################


function122<- function(x,y){
  
  x$bnstreet <-    paste(x$saotext,x$paotext,sep=" ")
  x$bnstreet <-    paste(x$bnstreet,x$paostartnumber,sep=",")
  x$bnstreet <-    paste(x$bnstreet,x$paoendnumber,sep="-")
  x$bnstreet <-    paste(x$bnstreet,x$streetdescription,sep=",")
  #x$bnstreet <- gsub("[']", "", x$bnstreet)
  
  x$bnstreet <- gsub("[.]", "", x$bnstreet)
  x$bnstreet <- gsub("[']", "", x$bnstreet)
  x$bnstreet <- gsub(" ", "", x$bnstreet)
  x$addressf <-paste(x$postcodelocator,x$bnstreet,sep=",")
  
  
  y$addressfinal <- y$add
  y$addressfinal <- gsub("[.]", "", y$addressfinal)
  y$addressfinal <- gsub("[']", "", y$addressfinal)
  y$addressfinal <- gsub(" ", "", y$addressfinal)
  y$addressf <- paste(y$postcode,y$addressfinal,sep=",")
  
  taba1 <- inner_join(x,y,by="addressf")
  return(taba1)
  
}

link122<-function122(add,epc)

link122<-link122[,..needlist1]

length(unique(link122$lmk_key))
link122u<- uniqueresult(link122)
dim(link122u)


link122d <- doubleresult(link122)
dim(link122d)


dim(epc)
#
epc <- matchleft(epc,link122)
dim(epc)
# 2277648      27



#################### method 123 ##################

function123<- function(x,y){
  x$bnstreet <-    paste(x$subbuildingname,x$paotext,sep=" ")
  x$bnstreet <-    paste(x$bnstreet,x$paostartnumber,sep=",")
  x$bnstreet <-    paste(x$bnstreet,x$paoendnumber,sep="-")
  x$bnstreet <-    paste(x$bnstreet,x$streetdescription,sep=",")
  #x$bnstreet <- gsub("[']", "", x$bnstreet)
  
  x$bnstreet <- gsub("[.]", "", x$bnstreet)
  x$bnstreet <- gsub("[']", "", x$bnstreet)
  x$bnstreet <- gsub(" ", "", x$bnstreet)
  x$addressf <-paste(x$postcodelocator,x$bnstreet,sep=",")
  
  
  y$addressfinal <- y$add
  y$addressfinal <- gsub("[.]", "", y$addressfinal)
  y$addressfinal <- gsub("[']", "", y$addressfinal)
  y$addressfinal <- gsub(" ", "", y$addressfinal)
  y$addressf <- paste(y$postcode,y$addressfinal,sep=",")
  
  taba1 <- inner_join(x,y,by="addressf")
  return(taba1)
  
  
}



link123<-function123(add,epc)
dim(link123)
# 
link123<-link123[,..needlist1]

link123u<- uniqueresult(link123)
dim(link123u)
#

link123d <- doubleresult(link123)
dim(link123d)


dim(epc)
#
epc <- matchleft(epc,link123)
dim(epc)
# 2124577      27


#################### method 124  ##################

function124<- function(x,y){
  
  
  #x<-x[x$paostartsuffix=="",]
  
  #x$bnstreet <-    paste(x$saostartnumber,x$saostartsuffix ,sep=" ")
  x$bnstreet <-    paste(x$ss,x$paotext,sep=" ")
  x$bnstreet <-    paste(x$bnstreet,x$pp,sep=",")
  x$bnstreet <-    paste(x$bnstreet,x$streetdescription,sep=" ")
  x$bnstreet <- gsub("[.]", "", x$bnstreet)
  
  x$bnstreet <- gsub("[']", "", x$bnstreet)
  x$bnstreet <- gsub(" ", "", x$bnstreet)
  x$addressf <-paste(x$postcodelocator,x$bnstreet,sep=",")
  
  
  y$addressfinal <- y$add
  y$addressfinal <- gsub("[/]", "", y$addressfinal)
  y$addressfinal <- gsub("[.]", "", y$addressfinal)
  y$addressfinal <- gsub("[']", "", y$addressfinal)
  y$addressfinal <- gsub(" ", "", y$addressfinal)
  y$addressf <- paste(y$postcode,y$addressfinal,sep=",")
  
  taba1 <- inner_join(x,y,by="addressf")
  return(taba1)
}


link124<-function124(add,epc)



link124<-link124[,..needlist1]


link124u<- uniqueresult(link124)
dim(link124u)


link124d <- doubleresult(link124)
#head(link124[grepl("[']", link124$add),])
#head(link124[grepl("[.]", link124$add),])
dim(epc)
#
epc <- matchleft(epc,link124)
dim(epc)
#  2108517      27



#################### method 125 ##################


function125<- function(x,y){
  
  x$bnstreet <-    paste(x$saotext,x$paotext,sep=" ")
  x$bnstreet <-    paste(x$bnstreet,x$streetdescription,sep=",")
  x$bnstreet <- gsub("[.]", "", x$bnstreet)
  
  x$bnstreet <- gsub("[']", "", x$bnstreet)
  x$bnstreet <- gsub(" ", "", x$bnstreet)
  x$addressf <-paste(x$postcodelocator,x$bnstreet,sep=",")
  
  y$addressfinal <- trimws(y$add)
  y$addressfinal <- gsub("[.]", "", y$addressfinal)
  y$addressfinal <- gsub("[']", "", y$addressfinal)
  y$addressfinal <- gsub("[/]", "", y$addressfinal)
  
  y$addressfinal <- gsub(" ", "", y$addressfinal)
  y$addressf <- paste(y$postcode,y$addressfinal,sep=",")
  
  taba1 <- inner_join(x,y,by="addressf")
  return(taba1)
}



link125<-function125(add,epc)

length(unique(link125$lmk_key))

link125<-link125[,..needlist1]


link125u<- uniqueresult(link125)
dim(link125u)


link125d <- doubleresult(link125)
dim(link125d)


dim(epc)
#
epc <- matchleft(epc,link125)
dim(epc)
#2093597      27

#
#################### method 126 ##################

function126<- function(x,y){
  
  x$bnstreet <-    paste(x$subbuildingname,x$paotext,sep=" ")
  x$bnstreet <-    paste(x$bnstreet,x$streetdescription,sep=",")
  x$bnstreet <- gsub("[.]", "", x$bnstreet)
  #x$bnstreet <- gsub("[']", "", x$bnstreet)
  x$bnstreet <- gsub("[']", "", x$bnstreet)
  x$bnstreet <- gsub(" ", "", x$bnstreet)
  x$addressf <-paste(x$postcodelocator,x$bnstreet,sep=",")
  
  y$addressfinal <- trimws(y$add)
  y$addressfinal <- gsub("[.]", "", y$addressfinal)
  y$addressfinal <- gsub("[']", "", y$addressfinal)
  y$addressfinal <- gsub("[/]", "", y$addressfinal)
  # y$addressfinal <- gsub("[-]", "", y$addressfinal)
  y$addressfinal <- gsub(" ", "", y$addressfinal)
  y$addressf <- paste(y$postcode,y$addressfinal,sep=",")
  
  taba1 <- inner_join(x,y,by="addressf")
  return(taba1)
}




link126<-function126(add,epc)
dim(link126)
# 

link126<-link126[,..needlist1]



link126u<- uniqueresult(link126)
dim(link126u)
# 

link126d <- doubleresult(link126)
dim(link126d)


epc <- matchleft(epc,link126)
dim(epc)
# 2081667      27

#################### method 127 ##################
function127<- function(x,y){
  
  
  x$bnstreet <-    paste(x$subbuildingname,x$buildingname,sep=" ")
  x$bnstreet <-    paste(x$bnstreet,x$streetdescription,sep=",")
  x$bnstreet <- gsub("[.]", "", x$bnstreet)
  #x$bnstreet <- gsub("[']", "", x$bnstreet)
  x$bnstreet <- gsub("[']", "", x$bnstreet)
  x$bnstreet <- gsub(" ", "", x$bnstreet)
  x$addressf <-paste(x$postcodelocator,x$bnstreet,sep=",")
  
  y$addressfinal <- trimws(y$add)
  y$addressfinal <- gsub("[.]", "", y$addressfinal)
  y$addressfinal <- gsub("[']", "", y$addressfinal)
  y$addressfinal <- gsub("[/]", "", y$addressfinal)
  # y$addressfinal <- gsub("[-]", "", y$addressfinal)
  y$addressfinal <- gsub(" ", "", y$addressfinal)
  y$addressf <- paste(y$postcode,y$addressfinal,sep=",")
  
  taba1 <- inner_join(x,y,by="addressf")
  return(taba1)
  
}

link127<-function127(add,epc)
dim(link127)


link127<-link127[,..needlist1]

link127u<- uniqueresult(link127)
dim(link127u)


link127d <- doubleresult(link127)
dim(link127d)


dim(epc)
# 
epc <- matchleft(epc,link127)
dim(epc)
#2080763      27
#
#################### method 128 ##################

function128<- function(x,y){
  
  
  x$bnstreet <-    paste(x$saotext,x$buildingname,sep=" ")
  x$bnstreet <-    paste(x$bnstreet,x$streetdescription,sep=",")
  x$bnstreet <- gsub("[.]", "", x$bnstreet)
  #x$bnstreet <- gsub("[']", "", x$bnstreet)
  x$bnstreet <- gsub("[']", "", x$bnstreet)
  x$bnstreet <- gsub(" ", "", x$bnstreet)
  x$addressf <-paste(x$postcodelocator,x$bnstreet,sep=",")
  
  y$addressfinal <- trimws(y$add)
  y$addressfinal <- gsub("[.]", "", y$addressfinal)
  y$addressfinal <- gsub("[']", "", y$addressfinal)
  y$addressfinal <- gsub("[/]", "", y$addressfinal)
  # y$addressfinal <- gsub("[-]", "", y$addressfinal)
  y$addressfinal <- gsub(" ", "", y$addressfinal)
  y$addressf <- paste(y$postcode,y$addressfinal,sep=",")
  
  taba1 <- inner_join(x,y,by="addressf")
  return(taba1)
  
}



link128<-function128(add,epc)
dim(link128)


link128<-link128[,..needlist1]



link128u<- uniqueresult(link128)
dim(link128u)


link128d <- doubleresult(link128)
dim(link128d)


dim(epc)
# 
epc <- matchleft(epc,link128)
dim(epc)
#2080763      27
#################### method 129 ##################
function129<- function(x,y){
  
  #<-x[x$saostartsuffix=="",]
  x$bnstreet <-    paste(x$saotext,x$ss,sep=",")
  x$bnstreet <-    paste(x$bnstreet,x$paotext,sep=" ")
  x$bnstreet <-    paste(x$bnstreet,x$streetdescription,sep=",")
  x$bnstreet <- gsub("[.]", "", x$bnstreet)
  #x$bnstreet <- gsub("[']", "", x$bnstreet)
  x$bnstreet <- gsub("[']", "", x$bnstreet)
  x$bnstreet <- gsub(" ", "", x$bnstreet)
  x$addressf <-paste(x$postcodelocator,x$bnstreet,sep=",")
  
  y$addressfinal <- trimws(y$add)
  #y$addressfinal <- gsub("[.]", "", y$addressfinal)
  y$addressfinal <- gsub("[.]", "", y$addressfinal)
  y$addressfinal <- gsub("[']", "", y$addressfinal)
  y$addressfinal <- gsub("[/]", "", y$addressfinal)
  y$addressfinal <- gsub(" ", "", y$addressfinal)
  y$addressf <- paste(y$postcode,y$addressfinal,sep=",")
  
  taba1 <- inner_join(x,y,by="addressf")
  return(taba1)
  
  
}


link129<-function129(add,epc)

link129<-link129[,..needlist1]


link129u<- uniqueresult(link129)
dim(link129u)


link129d <- doubleresult(link129)
dim(link129d)

dim(epc)
#
epc <- matchleft(epc,link129)
dim(epc)
#2078727      27



#################### method 130 ##################

function130<- function(x,y){
  
  #x<-x[x$saostartsuffix=="",]
  x$bnstreet <-    paste(x$saotext,x$ss,sep=",")
  x$bnstreet <-    paste(x$bnstreet,x$buildingnumber,sep=" ")
  x$bnstreet <-    paste(x$bnstreet,x$streetdescription,sep=",")
  x$bnstreet <- gsub("[.]", "", x$bnstreet)
  #x$bnstreet <- gsub("[']", "", x$bnstreet)
  x$bnstreet <- gsub("[']", "", x$bnstreet)
  x$bnstreet <- gsub(" ", "", x$bnstreet)
  x$addressf <-paste(x$postcodelocator,x$bnstreet,sep=",")
  
  y$addressfinal <- trimws(y$add)
  #y$addressfinal <- gsub("[.]", "", y$addressfinal)
  y$addressfinal <- gsub("[.]", "", y$addressfinal)
  y$addressfinal <- gsub("[']", "", y$addressfinal)
  y$addressfinal <- gsub("[/]", "", y$addressfinal)
  y$addressfinal <- gsub(" ", "", y$addressfinal)
  y$addressf <- paste(y$postcode,y$addressfinal,sep=",")
  
  taba1 <- inner_join(x,y,by="addressf")
  return(taba1)
  
}


link130<-function130(add,epc)

length(unique(link130$lmk_key))
link130<-link130[,..needlist1]


link130u<- uniqueresult(link130)
dim(link130u)


link130d <- doubleresult(link130)
dim(link130d)

dim(epc)
#
epc <- matchleft(epc,link130)
dim(epc)
#2078680      27

#################### method 131 ##################

function131<- function(x,y){
  
  #x<-x[x$saostartsuffix=="",]
  x$bnstreet <-    paste(x$saotext,x$ss,sep=" ")
  x$bnstreet <-    paste(x$bnstreet,x$paotext,sep=",")
  x$bnstreet <-    paste(x$bnstreet,x$streetdescription,sep=",")
  x$bnstreet <- gsub("[.]", "", x$bnstreet)
  x$bnstreet <- gsub("[']", "", x$bnstreet)
  x$bnstreet <- gsub(" ", "", x$bnstreet)
  x$addressf <-paste(x$postcodelocator,x$bnstreet,sep=",")
  
  y$addressfinal <- trimws(y$add)
  y$addressfinal <- gsub("[.]", "", y$addressfinal)
  y$addressfinal <- gsub("[']", "", y$addressfinal)
  y$addressfinal <- gsub(" ", "", y$addressfinal)
  y$addressf <- paste(y$postcode,y$addressfinal,sep=",")
  
  taba1 <- inner_join(x,y,by="addressf")
  return(taba1)
  
}


link131<-function131(add,epc)


link131<-link131[,..needlist1]


link131u<- uniqueresult(link131)
dim(link131u)


link131d <- doubleresult(link131)
dim(link131d)

dim(epc)
#
epc <- matchleft(epc,link131)
dim(epc)
#2078856      27


#################### method 132 ##################


function132<- function(x,y){
  
  
  #x<-x[x$saostartsuffix=="",]
  x$bnstreet <-    paste(x$paotext,x$ss,sep=" ")
  x$bnstreet <-    paste(x$bnstreet,x$streetdescription,sep=",")
  x$bnstreet <- gsub(" ", "", x$bnstreet)
  x$addressf <-paste(x$postcodelocator,x$bnstreet,sep=",")
  
  y$addressfinal <- trimws(y$add)
  y$addressfinal <- gsub(" ", "", y$addressfinal)
  y$addressf <- paste(y$postcode,y$addressfinal,sep=",")
  
  taba1 <- inner_join(x,y,by="addressf")
  return(taba1)
  
}

link132<-function132(add,epc)

link132<-link132[,..needlist1]


link132u<- uniqueresult(link132)
dim(link132u)


link132d <- doubleresult(link132)
dim(link132d)


dim(epc)
#
epc <- matchleft(epc,link132)
dim(epc)
# 2078751      27



#################### method 133 ##################

function133<- function(x,y){
  
  x<-x[x$subbuildingname=="",]
  x$bnstreet <-    paste(x$buildingnumber,x$paotext,sep=" ")
  x$bnstreet <- gsub("[.]", "", x$bnstreet)
  x$bnstreet <- gsub("[']", "", x$bnstreet)
  #x$bnstreet <- gsub(" ", "", x$bnstreet)
  x$addressf <-paste(x$postcodelocator,x$bnstreet,sep=",")
  
  
  y$addressfinal <- trimws(y$add)
  #y$addressfinal <- gsub("/", "", y$addressfinal)
  y$addressfinal <- gsub("[.]", "", y$addressfinal)
  y$addressfinal <- gsub("[']", "", y$addressfinal)
  #y$addressfinal <- gsub(" ", "", y$addressfinal)
  y$addressf <- paste(y$postcode,y$addressfinal,sep=",")
  
  taba1 <- inner_join(x,y,by="addressf")
  return(taba1)
  
}



link133<-function133(add,epc)
dim(link133)
# 
link133<-link133[,..needlist1]

link133u<- uniqueresult(link133)
dim(link133u)
#

link133d <- doubleresult(link133)
dim(link133d)


dim(epc)
#
epc <- matchleft(epc,link133)
dim(epc)
# 2070723      27
#######################section sum up ############################
Sys.time()
link100u$method<-"link100u"
link101u$method<-"link101u"
link102u$method<-"link102u"
link103u$method<-"link103u"
link104u$method<-"link104u"
link105u$method<-"link105u"
link106u$method<-"link106u"
link107u$method<-"link107u"
link108u$method<-"link108u"
link109u$method<-"link109u"
link110u$method<-"link110u"
link111u$method<-"link111u"
link112u$method<-"link112u"
link113u$method<-"link113u"
link114u$method<-"link114u"
link115u$method<-"link115u"
link116u$method<-"link116u"
link117u$method<-"link117u"
link118u$method<-"link118u"
link119u$method<-"link119u"
link120u$method<-"link120u"
link121u$method<-"link121u"
link122u$method<-"link122u"
link123u$method<-"link123u"
link124u$method<-"link124u"
link125u$method<-"link125u"
link126u$method<-"link126u"
link127u$method<-"link127u"
link128u$method<-"link128u"
link129u$method<-"link129u"
link130u$method<-"link130u"

link131u$method<-"link131u"
link132u$method<-"link132u"
link133u$method<-"link133u"

link100d$method<-"link100d"
link101d$method<-"link101d"
link102d$method<-"link102d"
link103d$method<-"link103d"
link104d$method<-"link104d"
link105d$method<-"link105d"
link106d$method<-"link106d"
link107d$method<-"link107d"
link108d$method<-"link108d"
link109d$method<-"link109d"
link110d$method<-"link110d"
link111d$method<-"link111d"
link112d$method<-"link112d"
link113d$method<-"link113d"
link114d$method<-"link114d"
link115d$method<-"link115d"
link116d$method<-"link116d"
link117d$method<-"link117d"
link118d$method<-"link118d"
link119d$method<-"link119d"
link120d$method<-"link120d"
link121d$method<-"link121d"
link122d$method<-"link122d"
link123d$method<-"link123d"
link124d$method<-"link124d"
link125d$method<-"link125d"
link126d$method<-"link126d"
link127d$method<-"link127d"
link128d$method<-"link128d"
link129d$method<-"link129d"
link130d$method<-"link130d"
link131d$method<-"link131d"

link132d$method<-"link132d"
link133d$method<-"link133d"


l100_133u = list(link100u,link101u,link102u,link103u,link104u,link105u,link106u,link107u,link108u,link109u,link110u,link111u,link112u,link113u,link114u,link115u,link116u,link117u,link118u,link119u,link120u,link121u,link122u,link123u,link124u,link125u,link126u,link127u,link128u,link129u,link130u,link131u,link132u,link133u)
link100_133u<- rbindlist(l100_133u)
dim(link100_133u)
#   202229
dim(link100u)[1]+dim(link101u)[1]+dim(link102u)[1]+dim(link103u)[1]+dim(link104u)[1]+dim(link105u)[1]+dim(link106u)[1]+dim(link107u)[1]+dim(link108u)[1]+dim(link109u)[1]+dim(link110u)[1]+dim(link111u)[1]+dim(link112u)[1]+dim(link113u)[1]+dim(link114u)[1]+dim(link115u)[1]+dim(link116u)[1]+dim(link117u)[1]+dim(link118u)[1]+dim(link119u)[1]+dim(link120u)[1]+dim(link121u)[1]+dim(link122u)[1]+dim(link123u)[1]+dim(link124u)[1]+dim(link125u)[1]+dim(link126u)[1]+dim(link127u)[1]+dim(link128u)[1]+dim(link129u)[1]+dim(link130u)[1]+dim(link131u)[1]+dim(link132u)[1]+dim(link133u)[1]

# 202229
length(unique(link100_133u$lmk_key))
#202229


length(unique(link100_133u$method))
#34
l100_133d = list(link100d,link101d,link102d,link103d,link104d,link105d,link106d,link107d,link108d,link109d,link110d,link111d,link112d,link113d,link114d,link115d,link116d,link117d,link118d,link119d,link120d,link121d,link122d,link123d,link124d,link125d,link126d,link127d,link128d,link129d,link130d,link131d,link132d,link133d)
link100_133d<- rbindlist(l100_133d, use.names=TRUE, fill=TRUE)
dim(link100_133d)[1]
# 5613
dim(link100d)[1]+dim(link101d)[1]+dim(link102d)[1]+dim(link103d)[1]+dim(link104d)[1]+dim(link105d)[1]+dim(link106d)[1]+dim(link107d)[1]+dim(link108d)[1]+dim(link109d)[1]+dim(link110d)[1]+dim(link111d)[1]+dim(link112d)[1]+dim(link113d)[1]+dim(link114d)[1]+dim(link115d)[1]+dim(link116d)[1]+dim(link117d)[1]+dim(link118d)[1]+dim(link119d)[1]+dim(link120d)[1]+dim(link121d)[1]+dim(link122d)[1]+dim(link123d)[1]+dim(link124d)[1]+dim(link125d)[1]+dim(link126d)[1]+dim(link127d)[1]+dim(link128d)[1]+dim(link129d)[1]+dim(link130d)[1]+dim(link131d)[1]+dim(link132d)[1]+dim(link133d)[1]
# 5613



dbWriteTable(con, "link100_133dnew",value =link100_133d, append = TRUE, row.names = FALSE)
dbWriteTable(con, "link100_133unew",value =link100_133u, append = TRUE, row.names = FALSE)

rm(link100u,link101u,link102u,link103u,link104u,link105u,link106u,link107u,link108u,link109u,link110u,link111u,link112u,link113u,link114u,link115u,link116u,link117u,link118u,link119u,link120u,link121u,link122u,link123u,link124u,link125u,link126u,link127u,link128u,link129u,link130u,link131u,link132u,link133u)
rm(link100d,link101d,link102d,link103d,link104d,link105d,link106d,link107d,link108d,link109d,link110d,link111d,link112d,link113d,link114d,link115d,link116d,link117d,link118d,link119d,link120d,link121d,link122d,link123d,link124d,link125d,link126d,link127d,link128d,link129d,link130d,link131d,link132d,link133d)
rm(link100,link101,link102,link103,link104,link105,link106,link107,link108,link109,link110,link111,link112,link113,link114,link115,link116,link117,link118,link119,link120,link121,link122,link123,link124,link125,link126,link127,link128,link129,link130,link131,link132,link133)
rm(l100_133u,l100_133d)
rm(function100,function101,function102,function103,function104,function105,function106,function107,function108,function109,function110,function111,function112,function113,function114,function115,function116,function117,function118,function119,function120,function121,function122,function123,function124,function125,function126,function127,function128,function129,function130,function131,function132,function133)



#################### method 134  ##################

function134<- function(x,y){
  
  x$bnstreet <-    paste(x$subbuildingname,x$buildingnumber,sep=",")
  x$bnstreet <-    paste(x$bnstreet,x$streetdescription,sep=" ")
  
  x$bnstreet <- gsub("/", "", x$bnstreet)
  x$bnstreet <- gsub("[.]", "", x$bnstreet)
  x$bnstreet <- gsub("[']", "", x$bnstreet)
  
  x$bnstreet <- gsub(" ", "", x$bnstreet)
  x$addressf <-paste(x$postcodelocator,x$bnstreet,sep=",")
  
  
  y$addressfinal <-  y$add
  y$addressfinal <- gsub("[/]", "", y$addressfinal)
  y$addressfinal <- gsub("[.]", "", y$addressfinal)
  y$addressfinal <- gsub("[']", "", y$addressfinal)
  
  y$addressfinal <- gsub(" ", "", y$addressfinal)
  y$addressf <- paste(y$postcode,y$addressfinal,sep=",")
  
  taba1 <- inner_join(x,y,by="addressf")
  return(taba1) 
}


link134<-function134(add,epc)



link134<-link134[,..needlist1]

length(unique(link134$lmk_key))
link134u<- uniqueresult(link134)
dim(link134u)


link134d <- doubleresult(link134)
#head(link134[grepl("[']", link134$add),])
#head(link134[grepl("[.]", link134$add),])

dim(epc)
# 
epc<- matchleft(epc,link134)
dim(epc)
# 2067795      27
#################### method 135 ##################


function135<- function(x,y){
  
    
 # x<-x[x$subbuildingname=="",]
  
  x$bnstreet <-    paste(x$saotext,x$paotext,sep=",")
  x$bnstreet <-    paste(x$bnstreet,x$pp ,sep=",")
 
  x$bnstreet <-    paste(x$bnstreet,x$streetdescription ,sep=" ")
  x$bnstreet <- gsub("[']", "", x$bnstreet)
  x$bnstreet <- gsub("[.]", "", x$bnstreet)
  x$bnstreet <- gsub(" ", "", x$bnstreet)
  x$addressf <-paste(x$postcodelocator,x$bnstreet,sep=",")
  
  
  y$addressfinal <- paste(y$add1,y$add2,sep=",")
  y$addressfinal <- paste(y$addressfinal,y$add3,sep=",")
  y$addressfinal <- gsub("/", "", y$addressfinal)
  y$addressfinal <- gsub("[.]", "", y$addressfinal)
  y$addressfinal <- gsub("[']", "", y$addressfinal)
  y$addressfinal <- gsub(" ", "", y$addressfinal)
  y$addressf <- paste(y$postcode,y$addressfinal,sep=",")
  
  taba1 <- inner_join(x,y,by="addressf")
  return(taba1)
  
}



link135<-function135(add,epc)


link135<-link135[,..needlist1]

length(unique(link135$lmk_key))
link135u<- uniqueresult(link135)
dim(link135u)


link135d <- doubleresult(link135)
dim(link135d)


dim(epc)
#
epc <- matchleft(epc,link135)
dim(epc)
#2062816      27

#
#################### method 136 ##################

function136<- function(x,y){
 x$bnstreet <-    paste(x$subbuildingname,x$buildingname,sep=",")
  x$bnstreet <-    paste(x$bnstreet,x$streetdescription,sep=",")
  x$bnstreet <- gsub("/", "", x$bnstreet)
  x$bnstreet <- gsub("[']", "", x$bnstreet)
  x$bnstreet <- gsub("[.]", "", x$bnstreet)
  x$bnstreet <- gsub(" ", "", x$bnstreet)
  x$addressf <-paste(x$postcodelocator,x$bnstreet,sep=",")
  
  
  
  y$addressfinal <- paste(y$add1,y$add2,sep=",")
  y$addressfinal <- gsub("/", "", y$addressfinal)
  y$addressfinal <- gsub("[.]", "", y$addressfinal)
  y$addressfinal <- gsub("[']", "", y$addressfinal)
  y$addressfinal <- gsub(" ", "", y$addressfinal)
  y$addressf <- paste(y$postcode,y$addressfinal,sep=",")
  
  taba1 <- inner_join(x,y,by="addressf")
  return(taba1) 
  
}




link136<-function136(add,epc)
dim(link136)
# 

link136<-link136[,..needlist1]



link136u<- uniqueresult(link136)
dim(link136u)
# 

link136d <- doubleresult(link136)
dim(link136d)


epc <- matchleft(epc,link136)
dim(epc)
#  2062805      27

#################### method 137 ##################



function137<- function(x,y){
  
  x$bnstreet <-    paste(x$saotext,x$buildingname,sep=",")
  x$bnstreet <-    paste(x$bnstreet,x$streetdescription,sep=",")
  x$bnstreet <- gsub("/", "", x$bnstreet)
  x$bnstreet <- gsub("[']", "", x$bnstreet)
  x$bnstreet <- gsub("[.]", "", x$bnstreet)
  x$bnstreet <- gsub(" ", "", x$bnstreet)
  x$addressf <-paste(x$postcodelocator,x$bnstreet,sep=",")
  
  
  
  y$addressfinal <- paste(y$add1,y$add2,sep=",")
  y$addressfinal <- gsub("/", "", y$addressfinal)
  y$addressfinal <- gsub("[.]", "", y$addressfinal)
  y$addressfinal <- gsub("[']", "", y$addressfinal)
  y$addressfinal <- gsub(" ", "", y$addressfinal)
  y$addressf <- paste(y$postcode,y$addressfinal,sep=",")
  
  taba1 <- inner_join(x,y,by="addressf")
  return(taba1)
  
}

link137<-function137(add,epc)
dim(link137)


link137<-link137[,..needlist1]

link137u<- uniqueresult(link137)
dim(link137u)


link137d <- doubleresult(link137)
dim(link137d)


dim(epc)
# 
epc <- matchleft(epc,link137)
dim(epc)
#2060092      27
#
#################### method 138 ##################

function138<- function(x,y){
  x$bnstreet <-    paste(x$saotext,x$pp,sep=",")
  x$bnstreet <-    paste(x$bnstreet,x$streetdescription,sep=",")
  x$bnstreet <- gsub("/", "", x$bnstreet)
  x$bnstreet <- gsub("[']", "", x$bnstreet)
  x$bnstreet <- gsub("[.]", "", x$bnstreet)
  x$bnstreet <- gsub(" ", "", x$bnstreet)
  x$addressf <-paste(x$postcodelocator,x$bnstreet,sep=",")
  
  
  
  y$addressfinal <- paste(y$add1,y$add2,sep=",")
  y$addressfinal <- gsub("/", "", y$addressfinal)
  y$addressfinal <- gsub("[.]", "", y$addressfinal)
  y$addressfinal <- gsub("[']", "", y$addressfinal)
  y$addressfinal <- gsub(" ", "", y$addressfinal)
  y$addressf <- paste(y$postcode,y$addressfinal,sep=",")
  
  taba1 <- inner_join(x,y,by="addressf")
  return(taba1)
}



link138<-function138(add,epc)
dim(link138)


link138<-link138[,..needlist1]



link138u<- uniqueresult(link138)
dim(link138u)


link138d <- doubleresult(link138)
dim(link138d)


dim(epc)
# 
epc <- matchleft(epc,link138)
dim(epc)
# 2058711      27

#fwrite(epc,"D:/BINCHI/epc_read/epc_2302.csv",row.names= F)
#######################stop##########################
#dim(epc)
#2058711      27
###Removing multiple commas and trailing commas using gsub in add for the rest

# epc$add<-gsub("^,*|(?<=,),|,*$", "", x, perl=T)
# 
# 
# epc2<-epc[grepl(",," ,epc$add),]
# 
# x <- c("a,b,,c","a,b ","a b", ",a,b,,c", ",,,a,,,b,c,,,")


#################### method 139 ##################
function139<- function(x,y){
  x$bnstreet <-    paste(x$subbuildingname,x$buildingname,sep=",")
  x$bnstreet <-    paste(x$bnstreet,x$ss,sep=",")
  x$bnstreet <-    paste(x$bnstreet,x$paotext,sep=" ")
  x$bnstreet <-    paste(x$bnstreet,x$streetdescription,sep=",")
  x$bnstreet <- gsub("/", "", x$bnstreet)
  x$bnstreet <- gsub("[']", "", x$bnstreet)
  x$bnstreet <- gsub("[.]", "", x$bnstreet)
  x$bnstreet <- gsub(" ", "", x$bnstreet)
  x$addressf <-paste(x$postcodelocator,x$bnstreet,sep=",")
  
  
  
  y$addressfinal <- y$add
  y$addressfinal <- gsub("/", "", y$addressfinal)
  y$addressfinal <- gsub("[.]", "", y$addressfinal)
  y$addressfinal <- gsub("[']", "", y$addressfinal)
  y$addressfinal <- gsub(" ", "", y$addressfinal)
  y$addressf <- paste(y$postcode,y$addressfinal,sep=",")
  
  taba1 <- inner_join(x,y,by="addressf")
  return(taba1)
}


link139<-function139(add,epc)

link139<-link139[,..needlist1]


link139u<- uniqueresult(link139)
dim(link139u)


link139d <- doubleresult(link139)
dim(link139d)


#link139d<-link139d[!grepl(",,", link139d$add),]

dim(epc)
#
epc <- matchleft(epc,link139)
dim(epc)
#2058548      27


#################### method 140 one result but interesting##################

function140<- function(x,y){
  
 x$bnstreet <-    paste(x$subbuildingname,x$buildingname,sep=",")
  x$bnstreet <-    paste(x$bnstreet,x$streetdescription,sep=",")
  x$bnstreet <- gsub("/", "", x$bnstreet)
  x$bnstreet <- gsub("[']", "", x$bnstreet)
  x$bnstreet <- gsub("[.]", "", x$bnstreet)
  x$bnstreet <- gsub(" ", "", x$bnstreet)
  x$addressf <-paste(x$postcodelocator,x$bnstreet,sep=",")
  
  
  
  y$addressfinal <- paste(y$add1,y$add3,sep=",")
  y$addressfinal <- gsub("/", "", y$addressfinal)
  y$addressfinal <- gsub("[.]", "", y$addressfinal)
  y$addressfinal <- gsub("[']", "", y$addressfinal)
  y$addressfinal <- gsub(" ", "", y$addressfinal)
  y$addressf <- paste(y$postcode,y$addressfinal,sep=",")
  
  taba1 <- inner_join(x,y,by="addressf")
  return(taba1)
  
}


link140<-function140(add,epc)

length(unique(link140$lmk_key))
link140<-link140[,..needlist1]


link140u<- uniqueresult(link140)
dim(link140u)


link140d <- doubleresult(link140)
dim(link140d)

dim(epc)
#
epc <- matchleft(epc,link140)
dim(epc)
#2279730      27

#################### method 141 remove the control ##################

function141<- function(x,y){
  
  #x<-x[x$paostartsuffix=="",]
  x<-x[x$saotext=="",]
  x$bnstreet <-    paste(x$pp,x$streetdescription,sep=",")
  x$bnstreet <- gsub("/", "", x$bnstreet)
  x$bnstreet <- gsub("[']", "", x$bnstreet)
  x$bnstreet <- gsub("[.]", "", x$bnstreet)
  x$bnstreet <- gsub(" ", "", x$bnstreet)
  x$addressf <-paste(x$postcodelocator,x$bnstreet,sep=",")
  
  
  
  y$addressfinal <- y$add
  y$addressfinal <- gsub("/", "", y$addressfinal)
  y$addressfinal <- gsub("[.]", "", y$addressfinal)
  y$addressfinal <- gsub("[']", "", y$addressfinal)
  y$addressfinal <- gsub(" ", "", y$addressfinal)
  y$addressf <- paste(y$postcode,y$addressfinal,sep=",")
  
  taba1 <- inner_join(x,y,by="addressf")
  return(taba1)
  
}


link141<-function141(add,epc)


link141<-link141[,..needlist1]
length(unique(link141$lmk_key))

link141u<- uniqueresult(link141)
dim(link141u)


link141d <- doubleresult(link141)
dim(link141d)

dim(epc)
#
epc <- matchleft(epc,link141)
dim(epc)
#1928207      27


#################### method 142  ##################


function142<- function(x,y){
  
  x$bnstreet <-    paste(x$buildingnumber,x$streetdescription,sep=",")
  x$bnstreet <- gsub("[']", "", x$bnstreet)
  x$bnstreet <- gsub("[.]", "", x$bnstreet)
  x$bnstreet <- gsub(" ", "", x$bnstreet)
  x$addressf <-paste(x$postcodelocator,x$bnstreet,sep=",")
  
  y$addressfinal <- trimws(y$add1)
  #y$addressfinal <- gsub("/", "", y$addressfinal)
  y$addressfinal <- gsub("[.]", "", y$addressfinal)
  y$addressfinal <- gsub("[']", "", y$addressfinal)
  y$addressfinal <- gsub(" ", "", y$addressfinal)
  y$addressf <- paste(y$postcode,y$addressfinal,sep=",")
  
  taba1 <- inner_join(x,y,by="addressf")
  return(taba1)
  
  
}

link142<-function142(add,epc)

link142<-link142[,..needlist1]
length(unique(link142$lmk_key))

link142u<- uniqueresult(link142)
dim(link142u)


link142d <- doubleresult(link142)
dim(link142d)


dim(epc)
#
epc <- matchleft(epc,link142)
dim(epc)
# 2277648      27



#################### method 143 ,  (old 43 ) ##################


function143<- function(x,y){
  x$bnstreet <-   x$buildingname
  x$bnstreet <- gsub("[']", "", x$bnstreet)
  x$bnstreet <- gsub("[.]", "", x$bnstreet)
  x$bnstreet <- gsub(" ", "", x$bnstreet)
  x$addressf <-paste(x$postcodelocator,x$bnstreet,sep=",")
  
  y$addressfinal <- trimws(y$add1)
  y$addressfinal <- gsub("[.]", "", y$addressfinal)
  y$addressfinal <- gsub("[']", "", y$addressfinal)
  y$addressfinal <- gsub(" ", "", y$addressfinal)
  y$addressf <- paste(y$postcode,y$addressfinal,sep=",")
  
  taba1 <- inner_join(x,y,by="addressf")
  return(taba1)
 
}



link143<-function143(add,epc)
dim(link143)
# 
link143<-link143[,..needlist1]
length(unique(link143$lmk_key))


link143u<- uniqueresult(link143)
dim(link143u)
#

link143d <- doubleresult(link143)
dim(link143d)


dim(epc)
#
epc <- matchleft(epc,link143)
dim(epc)
# 1771287      27


#################### method 144  check##################

function144<- function(x,y){
  x$bnstreet <-   x$buildingname
  x$bnstreet <- gsub("[-]", "", x$bnstreet)
  x$bnstreet <- gsub(" ", "", x$bnstreet)
  x$addressf <-paste(x$postcodelocator,x$bnstreet,sep=",")
  
  y$addressfinal <- trimws(y$add1)
  y$addressfinal <- gsub("[-]", "", y$addressfinal)
  y$addressfinal <- gsub(" ", "", y$addressfinal)
  y$addressf <- paste(y$postcode,y$addressfinal,sep=",")
  
  taba1 <- inner_join(x,y,by="addressf")
  return(taba1)
 
}


link144<-function144(add,epc)

length(unique(link144$lmk_key))

link144<-link144[,..needlist1]


link144u<- uniqueresult(link144)
dim(link144u)


link144d <- doubleresult(link144)


dim(epc)
# 
epc<- matchleft(epc,link144)
dim(epc)
#  1758663      27
#################### method 145 ##################


function145<- function(x,y){
  
  x$bnstreet <-    paste(x$buildingname,x$buildingnumber,sep=" ")
  x$bnstreet <-    paste(x$bnstreet,x$streetdescription,sep=" ")
  x$bnstreet <- gsub("[']", "", x$bnstreet)
  x$bnstreet <- gsub("[.]", "", x$bnstreet)
  x$bnstreet <- gsub("/", "", x$bnstreet)
  x$bnstreet <- gsub(" ", "", x$bnstreet)
  x$addressf <-paste(x$postcodelocator,x$bnstreet,sep=",")
  
  y$addressfinal <- paste(y$add1,y$add2,sep=" ")
  y$addressfinal <- gsub("/", "", y$addressfinal)
  y$addressfinal <- gsub("[.]", "", y$addressfinal)
  y$addressfinal <- gsub(",", "", y$addressfinal)
  y$addressfinal <- gsub(" ", "", y$addressfinal)
  y$addressf <- paste(y$postcode,y$addressfinal,sep=",")
  
  taba1 <- inner_join(x,y,by="addressf")
  return(taba1)
  
}



link145<-function145(add,epc)


link145<-link145[,..needlist1]

length(unique(link145$lmk_key))
link145u<- uniqueresult(link145)
dim(link145u)


link145d <- doubleresult(link145)
dim(link145d)


dim(epc)
#
epc <- matchleft(epc,link145)
dim(epc)
#1754646      27

#########################################################################################
#################### method 146 (62)change##################

function146<- function(x,y){
  x$bnstreet <-    paste(x$buildingname,x$pp,sep=" ")
  x$bnstreet <-    paste(x$bnstreet,x$streetdescription,sep=" ")
  x$bnstreet <- gsub("[']", "", x$bnstreet)
  x$bnstreet <- gsub("[.]", "", x$bnstreet)
  x$bnstreet <- gsub("/", "", x$bnstreet)
  x$bnstreet <- gsub(" ", "", x$bnstreet)
  x$addressf <-paste(x$postcodelocator,x$bnstreet,sep=",")
  
  y$addressfinal <- paste(y$add1,y$add2,sep=" ")
  y$addressfinal <- gsub("/", "", y$addressfinal)
  y$addressfinal <- gsub("[.]", "", y$addressfinal)
  y$addressfinal <- gsub(",", "", y$addressfinal)
  y$addressfinal <- gsub(" ", "", y$addressfinal)
  y$addressf <- paste(y$postcode,y$addressfinal,sep=",")
  
  taba1 <- inner_join(x,y,by="addressf")
  return(taba1)
  
}




link146<-function146(add,epc)
dim(link146)
# 

link146<-link146[,..needlist1]



link146u<- uniqueresult(link146)
dim(link146u)
# 

link146d <- doubleresult(link146)
dim(link146d)


epc <- matchleft(epc,link146)
dim(epc)
#1730893      27

#################### method 147 ##################

function147<- function(x,y){
  
  #x<-x[x$buildingnumber=="",]
  x$bnstreet <-    paste(x$saotext,x$buildingnumber,sep=",")
  x$bnstreet <-    paste(x$bnstreet,x$pp,sep=" ")
  x$bnstreet <-    paste(x$bnstreet,x$streetdescription,sep=" ")
  x$bnstreet <- gsub("[']", "", x$bnstreet)
  x$bnstreet <- gsub("[.]", "", x$bnstreet)
  x$bnstreet <- gsub("/", "", x$bnstreet)
  x$bnstreet <- gsub(" ", "", x$bnstreet)
  x$addressf <-paste(x$postcodelocator,x$bnstreet,sep=",")
  
  
  y$addressfinal <- paste(y$add1,y$add2,sep=",")
  y$addressfinal <- gsub("/", "", y$addressfinal)
  y$addressfinal <- gsub("[.]", "", y$addressfinal)
  y$addressfinal <- gsub("[']", "", y$addressfinal)
  y$addressfinal <- gsub(" ", "", y$addressfinal)
  y$addressf <- paste(y$postcode,y$addressfinal,sep=",")
  
  taba1 <- inner_join(x,y,by="addressf")
  return(taba1)
  
}

link147<-function147(add,epc)
dim(link147)


link147<-link147[,..needlist1]

link147u<- uniqueresult(link147)
dim(link147u)


link147d <- doubleresult(link147)
dim(link147d)


dim(epc)
# 
epc <- matchleft(epc,link147)
dim(epc)
#1727409      27
#
#################### method 148 ##################

function148<- function(x,y){
  #x<-x[x$buildingnumber=="",]
  x$bnstreet <-    paste(x$saotext,x$buildingname,sep=",")
  
  x$bnstreet <-    paste(x$bnstreet,x$streetdescription,sep=" ")
  x$bnstreet <- gsub("[']", "", x$bnstreet)
  x$bnstreet <- gsub("[.]", "", x$bnstreet)
  x$bnstreet <- gsub("/", "", x$bnstreet)
  x$bnstreet <- gsub(" ", "", x$bnstreet)
  x$addressf <-paste(x$postcodelocator,x$bnstreet,sep=",")
  
  
  y$addressfinal <- paste(y$add1,y$add2,sep=",")
  y$addressfinal <- gsub("/", "", y$addressfinal)
  y$addressfinal <- gsub("[.]", "", y$addressfinal)
  y$addressfinal <- gsub("[']", "", y$addressfinal)
  y$addressfinal <- gsub(" ", "", y$addressfinal)
  y$addressf <- paste(y$postcode,y$addressfinal,sep=",")
  
  taba1 <- inner_join(x,y,by="addressf")
  return(taba1)
}



link148<-function148(add,epc)
dim(link148)


link148<-link148[,..needlist1]



link148u<- uniqueresult(link148)
dim(link148u)


link148d <- doubleresult(link148)
dim(link148d)


dim(epc)
# 
epc <- matchleft(epc,link148)
dim(epc)
#2269509      27
#################### method 149  the d has an interesting example##################
function149<- function(x,y){
  x<-x[x$buildingnumber=="",]
  x$bnstreet <-    paste(x$subbuildingname,x$pp,sep=",")
  
  x$bnstreet <-    paste(x$bnstreet,x$streetdescription,sep=" ")
  x$bnstreet <- gsub("[']", "", x$bnstreet)
  x$bnstreet <- gsub("[.]", "", x$bnstreet)
  x$bnstreet <- gsub("/", "", x$bnstreet)
  x$bnstreet <- gsub(" ", "", x$bnstreet)
  x$addressf <-paste(x$postcodelocator,x$bnstreet,sep=",")
  
  
  y$addressfinal <- paste(y$add1,y$add2,sep=",")
  y$addressfinal <- gsub("/", "", y$addressfinal)
  y$addressfinal <- gsub("[.]", "", y$addressfinal)
  y$addressfinal <- gsub("[']", "", y$addressfinal)
  y$addressfinal <- gsub(" ", "", y$addressfinal)
  y$addressf <- paste(y$postcode,y$addressfinal,sep=",")
  
  taba1 <- inner_join(x,y,by="addressf")
  return(taba1)
  
}


link149<-function149(add,epc)

link149<-link149[,..needlist1]


link149u<- uniqueresult(link149)
dim(link149u)


link149d <- doubleresult(link149)
dim(link149d)

dim(epc)
#2649854      27
epc <- matchleft(epc,link149)
dim(epc)
#1684345      27



#################### method 150 check##################

function150<- function(x,y){
  
  #x<-x[x$buildingnumber=="",]
  x$bnstreet <-    paste(x$saotext,x$pp,sep=",")
  #x$bnstreet <-    paste(x$bnstreet,x$paostartsuffix,sep=" ")
  x$bnstreet <-    paste(x$bnstreet,x$streetdescription,sep=" ")
  x$bnstreet <- gsub("[']", "", x$bnstreet)
  x$bnstreet <- gsub("[.]", "", x$bnstreet)
  x$bnstreet <- gsub("/", "", x$bnstreet)
  x$bnstreet <- gsub(" ", "", x$bnstreet)
  x$addressf <-paste(x$postcodelocator,x$bnstreet,sep=",")
  
  
  y$addressfinal <- paste(y$add1,y$add2,sep=",")
  y$addressfinal <- gsub("/", "", y$addressfinal)
  y$addressfinal <- gsub("[.]", "", y$addressfinal)
  y$addressfinal <- gsub("[']", "", y$addressfinal)
  y$addressfinal <- gsub(" ", "", y$addressfinal)
  y$addressf <- paste(y$postcode,y$addressfinal,sep=",")
  
  taba1 <- inner_join(x,y,by="addressf")
  return(taba1)
  
}


link150<-function150(add,epc)

length(unique(link150$lmk_key))
link150<-link150[,..needlist1]


link150u<- uniqueresult(link150)
dim(link150u)


link150d <- doubleresult(link150)
dim(link150d)

dim(epc)
#
epc <- matchleft(epc,link150)
dim(epc)
#2279730      27

#################### method 151 ##################

function151<- function(x,y){
  x$bnstreet <-    paste(x$subbuildingname,x$buildingname,sep=",")
  x$bnstreet <-    paste(x$bnstreet,x$paotext,sep=" ")
  x$bnstreet <- gsub("[']", "", x$bnstreet)
  x$bnstreet <- gsub("[.]", "", x$bnstreet)
  x$bnstreet <- gsub("/", "", x$bnstreet)
  x$bnstreet <- gsub(" ", "", x$bnstreet)
  x$addressf <-paste(x$postcodelocator,x$bnstreet,sep=", ")
  
  
  y$addressfinal <- paste(y$add1,y$add2,sep=",")
  y$addressfinal <- gsub("/", "", y$addressfinal)
  y$addressfinal <- gsub("[.]", "", y$addressfinal)
  y$addressfinal <- gsub("[']", "", y$addressfinal)
  y$addressfinal <- gsub(" ", "", y$addressfinal)
  y$addressf <- paste(y$postcode,y$addressfinal,sep=", ")
  
  taba1 <- inner_join(x,y,by="addressf")
  return(taba1)
}

link151<-function151(add,epc)


link151<-link151[,..needlist1]


link151u<- uniqueresult(link151)
dim(link151u)


link151d <- doubleresult(link151)
dim(link151d)

dim(epc)
#
epc <- matchleft(epc,link151)
dim(epc)
#1681309      27


#################### method 152 ##################


function152<- function(x,y){
  x$bnstreet <-    paste(x$saotext,x$paostartnumber,sep=",")
  x$bnstreet <-    paste(x$bnstreet,x$paoendnumber,sep="-")
  
  x$bnstreet <-    paste(x$bnstreet,x$paotext,sep=",")
  x$bnstreet <- gsub("[']", "", x$bnstreet)
  x$bnstreet <- gsub("[.]", "", x$bnstreet)
  x$bnstreet <- gsub("/", "", x$bnstreet)
  x$bnstreet <- gsub(" ", "", x$bnstreet)
  x$addressf <-paste(x$postcodelocator,x$bnstreet,sep=",")
  
  
  y$addressfinal <- paste(y$add1,y$add2,sep=",")
  y$addressfinal <- gsub("/", "", y$addressfinal)
  y$addressfinal <- gsub("[.]", "", y$addressfinal)
  y$addressfinal <- gsub("[']", "", y$addressfinal)
  y$addressfinal <- gsub(" ", "", y$addressfinal)
  y$addressf <- paste(y$postcode,y$addressfinal,sep=",")
  
  taba1 <- inner_join(x,y,by="addressf")
  return(taba1)
} 



link152<-function152(add,epc)

link152<-link152[,..needlist1]


link152u<- uniqueresult(link152)
dim(link152u)


link152d <- doubleresult(link152)
dim(link152d)


dim(epc)
#
epc <- matchleft(epc,link152)
dim(epc)
# 1681300      27



#################### method 153 ##################

function153<- function(x,y){
  
  x$bnstreet <-    paste(x$subbuildingname,x$buildingnumber,sep=",")
  x$bnstreet <-    paste(x$bnstreet,x$streetdescription,sep=" ")
  x$bnstreet <- gsub("[']", "", x$bnstreet)
  x$bnstreet <- gsub("[.]", "", x$bnstreet)
  x$bnstreet <- gsub("/", "", x$bnstreet)
  x$bnstreet <- gsub(" ", "", x$bnstreet)
  x$addressf <-paste(x$postcodelocator,x$bnstreet,sep=",")
  
  
  y$addressfinal <- paste(y$add1,y$add3,sep=",")
  y$addressfinal <- gsub("/", "", y$addressfinal)
  y$addressfinal <- gsub("[.]", "", y$addressfinal)
  y$addressfinal <- gsub("[']", "", y$addressfinal)
  y$addressfinal <- gsub(" ", "", y$addressfinal)
  y$addressf <- paste(y$postcode,y$addressfinal,sep=",")
  
  taba1 <- inner_join(x,y,by="addressf")
  return(taba1)
}





link153<-function153(add,epc)
dim(link153)
# 
link153<-link153[,..needlist1]

link153u<- uniqueresult(link153)
dim(link153u)
#

link153d <- doubleresult(link153)
dim(link153d)


dim(epc)
#
epc <- matchleft(epc,link153)
dim(epc)
# 1672898      27


#################### method 154  ##################

function154<- function(x,y){
  x$bnstreet <-    paste(x$saotext,x$paotext,sep=",")
  x$bnstreet <-    paste(x$bnstreet,x$paostartnumber,sep=",")
  x$bnstreet <-    paste(x$bnstreet,x$paoendnumber,sep="-")
  x$bnstreet <-    paste(x$bnstreet,x$streetdescription,sep=" ")
  x$bnstreet <- gsub("[']", "", x$bnstreet)
  x$bnstreet <- gsub("[.]", "", x$bnstreet)
  x$bnstreet <- gsub("/", "", x$bnstreet)
  x$bnstreet <- gsub(" ", "", x$bnstreet)
  x$addressf <-paste(x$postcodelocator,x$bnstreet,sep=",")
  
  
  y$addressfinal <- paste(y$add1,y$add2,sep=",")
  y$addressfinal <- gsub("/", "", y$addressfinal)
  y$addressfinal <- gsub("[.]", "", y$addressfinal)
  y$addressfinal <- gsub("[']", "", y$addressfinal)
  y$addressfinal <- gsub(" ", "", y$addressfinal)
  y$addressf <- paste(y$postcode,y$addressfinal,sep=",")
  
  taba1 <- inner_join(x,y,by="addressf")
  return(taba1)
  
  
}


link154<-function154(add,epc)

length(unique(link154$lmk_key))


link154<-link154[,..needlist1]


link154u<- uniqueresult(link154)
dim(link154u)


link154d <- doubleresult(link154)

dim(epc)
# 
epc<- matchleft(epc,link154)
dim(epc)
#  1672219      27
#C1<-link155d
#################### method 155 ##################


function155<- function(x,y){
  
  x<-x[x$subbuildingname=="",]
  x$bnstreet <-    paste(x$saotext,x$paotext,sep=",")
  x$bnstreet <-    paste(x$bnstreet,x$ss,sep=",")
  x$bnstreet <-    paste(x$bnstreet,x$streetdescription,sep=" ")
  x$bnstreet <- gsub("[']", "", x$bnstreet)
  x$bnstreet <- gsub("[.]", "", x$bnstreet)
  x$bnstreet <- gsub("/", "", x$bnstreet)
  x$bnstreet <- gsub(" ", "", x$bnstreet)
  x$addressf <-paste(x$postcodelocator,x$bnstreet,sep=",")
  
  
  y$addressfinal <- y$add
  y$addressfinal <- gsub("/", "", y$addressfinal)
  y$addressfinal <- gsub("[.]", "", y$addressfinal)
  y$addressfinal <- gsub("[']", "", y$addressfinal)
  y$addressfinal <- gsub(" ", "", y$addressfinal)
  y$addressf <- paste(y$postcode,y$addressfinal,sep=",")
  
  taba1 <- inner_join(x,y,by="addressf")
  return(taba1)
  
  
}



link155<-function155(add,epc)
length(unique(link155$lmk_key))

link155<-link155[,..needlist1]


link155u<- uniqueresult(link155)
dim(link155u)


link155d <- doubleresult(link155)
dim(link155d)


dim(epc)
#
epc <- matchleft(epc,link155)
dim(epc)
#21672198      27

#
#################### method 156 ##################

function156<- function(x,y){
  
  
  x$bnstreet <-    paste(x$saotext,x$paotext,sep=",")
  x$bnstreet <-    paste(x$bnstreet,x$ss,sep=",")
  x$bnstreet <-    paste(x$bnstreet,x$streetdescription,sep=" ")
  x$bnstreet <- gsub("[']", "", x$bnstreet)
  x$bnstreet <- gsub("[.]", "", x$bnstreet)
  x$bnstreet <- gsub("/", "", x$bnstreet)
  x$bnstreet <- gsub(" ", "", x$bnstreet)
  x$addressf <-paste(x$postcodelocator,x$bnstreet,sep=",")
  
  
  y$addressfinal <- y$add
  y$addressfinal <- gsub("/", "", y$addressfinal)
  y$addressfinal <- gsub("[.]", "", y$addressfinal)
  y$addressfinal <- gsub("[']", "", y$addressfinal)
  y$addressfinal <- gsub(" ", "", y$addressfinal)
  y$addressf <- paste(y$postcode,y$addressfinal,sep=",")
  
  taba1 <- inner_join(x,y,by="addressf")
  return(taba1)
  
  
  
}




link156<-function156(add,epc)
dim(link156)
# 

link156<-link156[,..needlist1]



link156u<- uniqueresult(link156)
dim(link156u)
# 

link156d <- doubleresult(link156)
dim(link156d)


epc <- matchleft(epc,link156)
dim(epc)
# 1672191      27

#################### method 157 ##################



function157<- function(x,y){
  
  x$bnstreet <-    paste(x$saotext,x$paotext,sep=",")
  x$bnstreet <-    paste(x$bnstreet,x$paostartnumber,sep=",")
  x$bnstreet <-    paste(x$bnstreet,x$paoendnumber,sep="-")
  
 
  x$bnstreet <-    paste(x$bnstreet,x$streetdescription,sep=" ")
  x$bnstreet <- gsub("[']", "", x$bnstreet)
  x$bnstreet <- gsub("[.]", "", x$bnstreet)
  #x$bnstreet <- gsub("/", "", x$bnstreet)
  x$bnstreet <- gsub(" ", "", x$bnstreet)
  x$addressf <-paste(x$postcodelocator,x$bnstreet,sep=",")
  
  
  y$addressfinal <- y$add
  #y$addressfinal <- gsub("/", "", y$addressfinal)
  y$addressfinal <- gsub("[.]", "", y$addressfinal)
  y$addressfinal <- gsub("[']", "", y$addressfinal)
  y$addressfinal <- gsub(" ", "", y$addressfinal)
  y$addressf <- paste(y$postcode,y$addressfinal,sep=",")
  
  taba1 <- inner_join(x,y,by="addressf")
  return(taba1)
  
  
}

link157<-function157(add,epc)
dim(link157)
length(unique(link157$lmk_key))

link157<-link157[,..needlist1]

link157u<- uniqueresult(link157)
dim(link157u)


link157d <- doubleresult(link157)
dim(link157d)


dim(epc)
# 
epc <- matchleft(epc,link157)
dim(epc)
#1664413      27
#
#################### method 158 / must after 157##################

function158<- function(x,y){
  
  x$bnstreet <-    paste(x$saotext,x$paotext,sep=",")
  x$bnstreet <-    paste(x$bnstreet,x$paostartnumber,sep=",")
  x$bnstreet <-    paste(x$bnstreet,x$paoendnumber,sep="-")
  
 
  x$bnstreet <-    paste(x$bnstreet,x$streetdescription,sep=" ")
  x$bnstreet <- gsub("[']", "", x$bnstreet)
  x$bnstreet <- gsub("[.]", "", x$bnstreet)
  x$bnstreet <- gsub("/", "", x$bnstreet)
  x$bnstreet <- gsub(" ", "", x$bnstreet)
  x$addressf <-paste(x$postcodelocator,x$bnstreet,sep=",")
  
  
  y$addressfinal <- y$add
  y$addressfinal <- gsub("/", "", y$addressfinal)
  y$addressfinal <- gsub("[.]", "", y$addressfinal)
  y$addressfinal <- gsub("[']", "", y$addressfinal)
  y$addressfinal <- gsub(" ", "", y$addressfinal)
  y$addressf <- paste(y$postcode,y$addressfinal,sep=",")
  
  taba1 <- inner_join(x,y,by="addressf")
  return(taba1)
}



link158<-function158(add,epc)
dim(link158)


link158<-link158[,..needlist1]



link158u<- uniqueresult(link158)
dim(link158u)


link158d <- doubleresult(link158)
dim(link158d)


dim(epc)
# 
epc <- matchleft(epc,link158)
dim(epc)
#1664412      27
#################### method 159 ##################
function159<- function(x,y){
  #x<-x[x$subbuildingname!="",]
  x$bnstreet <-    paste(x$saotext,x$paotext,sep=",")
  x$bnstreet <-    paste(x$bnstreet,x$pp,sep=",")
  x$bnstreet <-    paste(x$bnstreet,x$streetdescription,sep=" ")
  
  
  x$bnstreet <- gsub("[']", "", x$bnstreet)
  x$bnstreet <- gsub("[.]", "", x$bnstreet)
  x$bnstreet <- gsub("/", "", x$bnstreet)
  x$bnstreet <- gsub(" ", "", x$bnstreet)
  x$addressf <-paste(x$postcodelocator,x$bnstreet,sep=",")
  
  
  y$addressfinal <- y$add
  y$addressfinal <- gsub("/", "", y$addressfinal)
  y$addressfinal <- gsub("[.]", "", y$addressfinal)
  y$addressfinal <- gsub("[']", "", y$addressfinal)
  y$addressfinal <- gsub(" ", "", y$addressfinal)
  y$addressf <- paste(y$postcode,y$addressfinal,sep=",")
  
  taba1 <- inner_join(x,y,by="addressf")
  return(taba1)
  
  
}


link159<-function159(add,epc)

link159<-link159[,..needlist1]


link159u<- uniqueresult(link159)
dim(link159u)


link159d <- doubleresult(link159)
dim(link159d)

dim(epc)
#
epc <- matchleft(epc,link159)
dim(epc)
#1660701      27




#################### method 160 ##################

function160<- function(x,y){
  
  #x<-x[x$subbuildingname!="",]
  x$bnstreet <-    paste(x$subbuildingname,x$paotext,sep=",")
  x$bnstreet <-    paste(x$bnstreet,x$pp,sep=",")
  x$bnstreet <-    paste(x$bnstreet,x$streetdescription,sep=" ")
  
  
  x$bnstreet <- gsub("[']", "", x$bnstreet)
  x$bnstreet <- gsub("[.]", "", x$bnstreet)
  x$bnstreet <- gsub("/", "", x$bnstreet)
  x$bnstreet <- gsub(" ", "", x$bnstreet)
  x$addressf <-paste(x$postcodelocator,x$bnstreet,sep=",")
  
  
  y$addressfinal <- y$add
  y$addressfinal <- gsub("/", "", y$addressfinal)
  y$addressfinal <- gsub("[.]", "", y$addressfinal)
  y$addressfinal <- gsub("[']", "", y$addressfinal)
  y$addressfinal <- gsub(" ", "", y$addressfinal)
  y$addressf <- paste(y$postcode,y$addressfinal,sep=",")
  
  taba1 <- inner_join(x,y,by="addressf")
  return(taba1)
  
  
  
  
}


link160<-function160(add,epc)

length(unique(link160$lmk_key))
link160<-link160[,..needlist1]


link160u<- uniqueresult(link160)
dim(link160u)


link160d <- doubleresult(link160)
dim(link160d)

dim(epc)
#
epc <- matchleft(epc,link160)
dim(epc)
#1660540      27

#################### method 161 ##################

function161<- function(x,y){
  
  x<-x[x$subbuildingname=="",]
  x$bnstreet <-    paste(x$saotext,x$paotext,sep=",")
  x$bnstreet <- gsub("[']", "", x$bnstreet)
  x$bnstreet <- gsub("[.]", "", x$bnstreet)
  x$bnstreet <- gsub("/", "", x$bnstreet)
  x$bnstreet <- gsub(" ", "", x$bnstreet)
  x$addressf <-paste(x$postcodelocator,x$bnstreet,sep=",")
  
  
  y$addressfinal <- paste(y$add1,y$add2,sep=",")
  y$addressfinal <- gsub("/", "", y$addressfinal)
  y$addressfinal <- gsub("[.]", "", y$addressfinal)
  y$addressfinal <- gsub("[']", "", y$addressfinal)
  y$addressfinal <- gsub(" ", "", y$addressfinal)
  y$addressf <- paste(y$postcode,y$addressfinal,sep=",")
  
  taba1 <- inner_join(x,y,by="addressf")
  return(taba1)
  
}


link161<-function161(add,epc)


link161<-link161[,..needlist1]


link161u<- uniqueresult(link161)
dim(link161u)


link161d <- doubleresult(link161)
dim(link161d)

dim(epc)
#
epc <- matchleft(epc,link161)
dim(epc)
# 1653957      27


#################### method 162 ##################


function162<- function(x,y){
  
  x$bnstreet <-    paste(x$saotext,x$paotext,sep=",")
  x$bnstreet <- gsub("[']", "", x$bnstreet)
  x$bnstreet <- gsub("[.]", "", x$bnstreet)
  x$bnstreet <- gsub("/", "", x$bnstreet)
  x$bnstreet <- gsub(" ", "", x$bnstreet)
  x$addressf <-paste(x$postcodelocator,x$bnstreet,sep=",")
  
  
  y$addressfinal <- paste(y$add1,y$add2,sep=",")
  y$addressfinal <- gsub("/", "", y$addressfinal)
  y$addressfinal <- gsub("[.]", "", y$addressfinal)
  y$addressfinal <- gsub("[']", "", y$addressfinal)
  y$addressfinal <- gsub(" ", "", y$addressfinal)
  y$addressf <- paste(y$postcode,y$addressfinal,sep=",")
  
  taba1 <- inner_join(x,y,by="addressf")
  return(taba1)
  
  
}

link162<-function162(add,epc)

link162<-link162[,..needlist1]


link162u<- uniqueresult(link162)
dim(link162u)


link162d <- doubleresult(link162)
dim(link162d)


dim(epc)
#
epc <- matchleft(epc,link162)
dim(epc)
#  1643859      27



#################### method 163 ##################

function163<- function(x,y){
  x$bnstreet <-    paste(x$subbuildingname,x$buildingnumber ,sep=",")
  x$bnstreet <-    paste(x$bnstreet,x$paotext ,sep=" ")
  
  x$bnstreet <- gsub("/", "", x$bnstreet)
  x$bnstreet <- gsub("[.]", "", x$bnstreet)
  x$bnstreet <- gsub("[']", "", x$bnstreet)
  x$bnstreet <- gsub(" ", "", x$bnstreet)
  x$addressf <-paste(x$postcodelocator,x$bnstreet,sep=",")
  
  
  y$addressfinal <- paste( y$add1, y$add2,sep=",")
  y$addressfinal <- gsub("[/]", "", y$addressfinal)
  y$addressfinal <- gsub("[.]", "", y$addressfinal)
  y$addressfinal <- gsub("[']", "", y$addressfinal)
  y$addressfinal <- gsub(" ", "", y$addressfinal)
  y$addressf <- paste(y$postcode,y$addressfinal,sep=",")
  
  taba1 <- inner_join(x,y,by="addressf")
  return(taba1)
  #x<-x[x$paostartsuffix=="",]
  # 
  # x$bnstreet <-    paste(x$saotext,x$pp,sep=",")
  # x$bnstreet <-    paste(x$bnstreet,x$streetdescription,sep=" ")
  # x$bnstreet <- gsub("[']", "", x$bnstreet)
  # x$bnstreet <- gsub("[.]", "", x$bnstreet)
  # x$bnstreet <- gsub("/", "", x$bnstreet)
  # x$bnstreet <- gsub(" ", "", x$bnstreet)
  # x$addressf <-paste(x$postcodelocator,x$bnstreet,sep=",")
  # 
  # 
  # y$addressfinal <- paste(y$add1,y$add2,sep=",")
  # y$addressfinal <- gsub("/", "", y$addressfinal)
  # y$addressfinal <- gsub("[.]", "", y$addressfinal)
  # y$addressfinal <- gsub("[']", "", y$addressfinal)
  # y$addressfinal <- gsub(" ", "", y$addressfinal)
  # y$addressf <- paste(y$postcode,y$addressfinal,sep=",")
  # 
  # taba1 <- inner_join(x,y,by="addressf")
  # return(taba1)
  
 
  
}

link163<-function163(add,epc)
dim(link163)
# 
link163<-link163[,..needlist1]

link163u<- uniqueresult(link163)
dim(link163u)
#

link163d <- doubleresult(link163)
dim(link163d)


dim(epc)
#
epc <- matchleft(epc,link163)
dim(epc)
# 1643837      27


#################### method 164  ##################

function164<- function(x,y){
  
  x$bnstreet <-    paste(x$saotext,x$paotext,sep=",")
  x$bnstreet <-    paste(x$bnstreet,x$streetdescription,sep=",")
  x$bnstreet <- gsub("[']", "", x$bnstreet)
  x$bnstreet <- gsub("[.]", "", x$bnstreet)
  x$bnstreet <- gsub("/", "", x$bnstreet)
  x$bnstreet <- gsub(" ", "", x$bnstreet)
  x$addressf <-paste(x$postcodelocator,x$bnstreet,sep=",")
  
  
  y$addressfinal <- paste(y$add1,y$add2,sep=",")
  y$addressfinal <- gsub("/", "", y$addressfinal)
  y$addressfinal <- gsub("[.]", "", y$addressfinal)
  y$addressfinal <- gsub("[']", "", y$addressfinal)
  
  y$addressfinal <- gsub(" ", "", y$addressfinal)
  y$addressf <- paste(y$postcode,y$addressfinal,sep=",")
  
  taba1 <- inner_join(x,y,by="addressf")
  return(taba1)  
  
  
}


link164<-function164(add,epc)



link164<-link164[,..needlist1]


link164u<- uniqueresult(link164)
dim(link164u)


link164d <- doubleresult(link164)


dim(epc)
# 
epc<- matchleft(epc,link164)
dim(epc)
# 1641825      27
#################### method 165 ##################


function165<- function(x,y){
  
  x$bnstreet <-    paste(x$saotext,x$paotext,sep=" ")
  x$bnstreet <-    paste(x$bnstreet,x$paostartnumber,sep=",")
  x$bnstreet <-    paste(x$bnstreet,x$paoendnumber,sep="-")
  x$bnstreet <-    paste(x$bnstreet,x$streetdescription,sep=",")
  x$bnstreet <- gsub("[']", "", x$bnstreet)
  x$bnstreet <- gsub("[.]", "", x$bnstreet)
  x$bnstreet <- gsub("/", "", x$bnstreet)
  x$bnstreet <- gsub(" ", "", x$bnstreet)
  x$addressf <-paste(x$postcodelocator,x$bnstreet,sep=",")
  
  
  y$addressfinal <- paste(y$add1,y$add2,sep=",")
  y$addressfinal <- gsub("/", "", y$addressfinal)
  y$addressfinal <- gsub("[.]", "", y$addressfinal)
  y$addressfinal <- gsub("[']", "", y$addressfinal)
  
  y$addressfinal <- gsub(" ", "", y$addressfinal)
  y$addressf <- paste(y$postcode,y$addressfinal,sep=",")
  
  taba1 <- inner_join(x,y,by="addressf")
  return(taba1)
  
}



link165<-function165(add,epc)


link165<-link165[,..needlist1]


link165u<- uniqueresult(link165)
dim(link165u)


link165d <- doubleresult(link165)
dim(link165d)


dim(epc)
#
epc <- matchleft(epc,link165)
dim(epc)
#1639850      27
#
#################### method 166 ##################

function166<- function(x,y){
  
  x$bnstreet <-    paste(x$subbuildingname,x$paotext,sep=" ")
  x$bnstreet <-    paste(x$bnstreet,x$paostartnumber,sep=",")
  x$bnstreet <-    paste(x$bnstreet,x$paoendnumber,sep="-")
  x$bnstreet <-    paste(x$bnstreet,x$streetdescription,sep=",")
  x$bnstreet <- gsub("[']", "", x$bnstreet)
  x$bnstreet <- gsub("[.]", "", x$bnstreet)
  x$bnstreet <- gsub("/", "", x$bnstreet)
  x$bnstreet <- gsub(" ", "", x$bnstreet)
  x$addressf <-paste(x$postcodelocator,x$bnstreet,sep=",")
  
  
  y$addressfinal <- paste(y$add1,y$add2,sep=",")
  y$addressfinal <- gsub("/", "", y$addressfinal)
  y$addressfinal <- gsub("[.]", "", y$addressfinal)
  y$addressfinal <- gsub("[']", "", y$addressfinal)
  
  y$addressfinal <- gsub(" ", "", y$addressfinal)
  y$addressf <- paste(y$postcode,y$addressfinal,sep=",")
  
  taba1 <- inner_join(x,y,by="addressf")
  return(taba1)
}




link166<-function166(add,epc)
dim(link166)
# 

link166<-link166[,..needlist1]



link166u<- uniqueresult(link166)
dim(link166u)
# 

link166d <- doubleresult(link166)
dim(link166d)


epc <- matchleft(epc,link166)
dim(epc)
# 1631732      27

#######summary###############




link134u$method<-"link134u"
link135u$method<-"link135u"
link136u$method<-"link136u"
link137u$method<-"link137u"
link138u$method<-"link138u"
link139u$method<-"link139u"
link140u$method<-"link140u"
link141u$method<-"link141u"
link142u$method<-"link142u"
link143u$method<-"link143u"
link144u$method<-"link144u"
link145u$method<-"link145u"
link146u$method<-"link146u"
link147u$method<-"link147u"
link148u$method<-"link148u"
link149u$method<-"link149u"
link150u$method<-"link150u"
link151u$method<-"link151u"
link152u$method<-"link152u"
link153u$method<-"link153u"
link154u$method<-"link154u"
link155u$method<-"link155u"
link156u$method<-"link156u"
link157u$method<-"link157u"
link158u$method<-"link158u"
link159u$method<-"link159u"
link160u$method<-"link160u"
link161u$method<-"link161u"
link162u$method<-"link162u"
link163u$method<-"link163u"
link164u$method<-"link164u"
link165u$method<-"link165u"
link166u$method<-"link166u"


link134d$method<-"link134d"
link135d$method<-"link135d"
link136d$method<-"link136d"
link137d$method<-"link137d"
link138d$method<-"link138d"
link139d$method<-"link139d"
link140d$method<-"link140d"
link141d$method<-"link141d"
link142d$method<-"link142d"
link143d$method<-"link143d"
link144d$method<-"link144d"
link145d$method<-"link145d"
link146d$method<-"link146d"
link147d$method<-"link147d"
link148d$method<-"link148d"
link149d$method<-"link149d"
link150d$method<-"link150d"
link151d$method<-"link151d"
link152d$method<-"link152d"
link153d$method<-"link153d"
link154d$method<-"link154d"
link155d$method<-"link155d"
link156d$method<-"link156d"
link157d$method<-"link157d"
link158d$method<-"link158d"
link159d$method<-"link159d"

link160d$method<-"link160d"
link161d$method<-"link161d"
link162d$method<-"link162d"
link163d$method<-"link163d"
link164d$method<-"link164d"
link165d$method<-"link165d"
link166d$method<-"link166d"


l134_166l = list(link134u,link135u,link136u,link137u,link138u,link139u,link140u,link141u,link142u,link143u,link144u,link145u,link146u,link147u,link148u,link149u,link150u,link151u,link152u,link153u,link154u,link155u,link156u,link157u,link158u,link159u,link160u,link161u,link162u,link163u,link164u,link165u,link166u)
link134_166u<- rbindlist(l134_166l)
dim(link134_166u)
#  435624     32
length(unique(link134_166u$lmk_key))
#435624     32
length(unique(link134_166u$method))
#33


l134_166dl = list(link134d,link135d,link136d,link137d,link138d,link139d,link140d,link141d,link142d,link143d,link144d,link145d,link146d,link147d,link148d,link149d,link150d,link151d,link152d,link153d,link154d,link155d,link156d,link157d,link158d,link159d,link160d,link161d,link162d,link163d,link164d,link165d,link166d)
link134_166d<- rbindlist(l134_166dl, use.names=TRUE, fill=TRUE)

dim(link134_166d)
# 27235

dim(link134d)[1]+dim(link135d)[1]+dim(link136d)[1]+dim(link137d)[1]+dim(link138d)[1]+dim(link139d)[1]+dim(link140d)[1]+dim(link141d)[1]+dim(link142d)[1]+dim(link143d)[1]+dim(link144d)[1]+dim(link145d)[1]+dim(link146d)[1]+dim(link147d)[1]+dim(link148d)[1]+dim(link149d)[1]+dim(link150d)[1]+dim(link151d)[1]+dim(link152d)[1]+dim(link153d)[1]+dim(link154d)[1]+dim(link155d)[1]+
  dim(link156d)[1]+dim(link157d)[1]+dim(link158d)[1]+dim(link159d)[1]+dim(link160d)[1]+dim(link161d)[1]+dim(link162d)[1]+dim(link163d)[1]+dim(link164d)[1]+dim(link165d)[1]+dim(link166d)[1]
#  27235
dim(link134u)[1]+dim(link135u)[1]+dim(link136u)[1]+dim(link137u)[1]+dim(link138u)[1]+dim(link139u)[1]+dim(link140u)[1]+dim(link141u)[1]+dim(link142u)[1]+dim(link143u)[1]+dim(link144u)[1]+dim(link145u)[1]+dim(link146u)[1]+dim(link147u)[1]+dim(link148u)[1]+dim(link149u)[1]+dim(link150u)[1]+dim(link151u)[1]+dim(link152u)[1]+dim(link153u)[1]+dim(link154u)[1]+dim(link155u)[1]+
  dim(link156u)[1]+dim(link157u)[1]+dim(link158u)[1]+dim(link159u)[1]+dim(link160u)[1]+dim(link161u)[1]+dim(link162u)[1]+dim(link163u)[1]+dim(link164u)[1]+dim(link165u)[1]+dim(link166u)[1]
#   435624
Sys.time()


dbWriteTable(con, "link134_166dnew",value =link134_166d, append = TRUE, row.names = FALSE)
dbWriteTable(con, "link134_166unew",value =link134_166u, append = TRUE, row.names = FALSE)


rm(link134u,link135u,link136u,link137u,link138u,link139u,link140u,link141u,link142u,link143u,link144u,link145u,link146u,link147u,link148u,link149u,link150u,link151u,link152u,link153u,link154u,link155u,link156u,link157u,link158u,link159u,link160u,link161u,link162u,link163u,link164u,link165u,link166u)
rm(link134d,link135d,link136d,link137d,link138d,link139d,link140d,link141d,link142d,link143d,link144d,link145d,link146d,link147d,link148d,link149d,link150d,link151d,link152d,link153d,link154d,link155d,link156d,link157d,link158d,link159d,link160d,link161d,link162d,link163d,link164d,link165d,link166d)
rm(link134,link135,link136,link137,link138,link139,link140,link141,link142,link143,link144,link145,link146,link147,link148,link149,link150,link151,link152,link153,link154,link155,link156,link157,link158,link159,link160,link161,link162,link163,link164,link165,link166)
rm(function134,function135,function136,function137,function138,function139,function140,function141,function142,function143,function144,function145,function146,function147,function148,function149,function150,function151,function152,function153,function154,function155,function156,function157,function158,function159,function160,function161,function162,function163,function164,function165,function166)

rm(l134_166l,l134_166dl)

# epc <- matchleft(epc,link134_166d)
# epc <- matchleft(epc,link134_166u)
# dim(epc)
# dim(epc)
# 
# epc <- matchleft(epc,link134_166d)
# epc <- matchleft(epc,link134_166u)
# dim(epc)
# #1638966

#################### method 167 ##################



function167<- function(x,y){
  x <- x[!grepl("^\\d",x$buildingname),]
  x$bnstreet <-    paste(x$subbuildingname,x$buildingname,sep=" ")
  x$bnstreet <-    paste(x$bnstreet,x$streetdescription,sep=",")
  x$bnstreet <- gsub("[.]", "", x$bnstreet)
  x$bnstreet <- gsub(",", "", x$bnstreet)
  x$bnstreet <- gsub(" ", "", x$bnstreet)
  x$addressf <-paste(x$postcodelocator,x$bnstreet,sep=",")
  
  y$addressfinal <-  paste(y$add1,y$add2,sep=",")
  y$addressfinal <- gsub(",", "", y$addressfinal)
  
  y$addressfinal <- gsub(" ", "", y$addressfinal)
  y$addressf <- paste(y$postcode,y$addressfinal,sep=",")
  
  taba1 <- inner_join(x,y,by="addressf")
  return(taba1)
  
  
}

link167<-function167(add,epc)
dim(link167)


link167<-link167[,..needlist1]

link167u<- uniqueresult(link167)
dim(link167u)


link167d <- doubleresult(link167)
dim(link167d)


dim(epc)
# 
epc <- matchleft(epc,link167)
dim(epc)
#1602950      27
#
#################### method 168 ##################

function168<- function(x,y){
  x <- x[!grepl("^\\d",x$buildingname),]
  x$bnstreet <-    paste(x$subbuildingname,x$buildingname,sep=" ")
  x$bnstreet <-    paste(x$bnstreet,x$streetdescription,sep=",")
  x$bnstreet <- gsub("/", "", x$bnstreet)
  x$bnstreet <- gsub("[.]", "", x$bnstreet)
  x$bnstreet <- gsub("[']", "", x$bnstreet)
  x$bnstreet <- gsub(",", "", x$bnstreet)
  x$bnstreet <- gsub(" ", "", x$bnstreet)
  x$addressf <-paste(x$postcodelocator,x$bnstreet,sep=",")
  
  y$addressfinal <-  paste(y$add1,y$add2,sep=",")
  y$addressfinal <- gsub(",", "", y$addressfinal)
  y$addressfinal <- gsub("/", "", y$addressfinal)
  y$addressfinal <- gsub("[.]", "", y$addressfinal)
  y$addressfinal <- gsub("[']", "", y$addressfinal)
  y$addressfinal <- gsub(" ", "", y$addressfinal)
  y$addressf <- paste(y$postcode,y$addressfinal,sep=",")
  
  taba1 <- inner_join(x,y,by="addressf")
  return(taba1)
}



link168<-function168(add,epc)
dim(link168)
length(unique(link168$lmk_key))

link168<-link168[,..needlist1]



link168u<- uniqueresult(link168)
dim(link168u)


link168d <- doubleresult(link168)
dim(link168d)


dim(epc)
# 
epc <- matchleft(epc,link168)
dim(epc)
#1591269      27
#################### method 169 (old 51)##################
function169<- function(x,y){
  x$bnstreet <-    x$paotext
  x$bnstreet <- gsub("/", "", x$bnstreet)
  x$bnstreet <- gsub("[.]", "", x$bnstreet)
  x$bnstreet <- gsub("[']", "", x$bnstreet)
  x$bnstreet <- gsub(" ", "", x$bnstreet)
  x$addressf <-paste(x$postcodelocator,x$bnstreet,sep=",")
  
  y$addressfinal <- trimws(y$add1)
  y$addressfinal <- gsub(",", "", y$addressfinal)
  
  y$addressfinal <- gsub("[']", "", y$addressfinal)
  y$addressfinal <- gsub("[.]", "", y$addressfinal)
  y$addressfinal <- gsub("/", "", y$addressfinal)
  y$addressfinal <- gsub(" ", "", y$addressfinal)
  y$addressf <- paste(y$postcode,y$addressfinal,sep=",")
  
  taba1 <- inner_join(x,y,by="addressf")
  return(taba1)
}


link169<-function169(add,epc)
length(unique(link169$lmk_key))
link169<-link169[,..needlist1]


link169u<- uniqueresult(link169)
dim(link169u)


link169d <- doubleresult(link169)
dim(link169d)

dim(epc)
#
epc <- matchleft(epc,link169)
dim(epc)
#1526096      27



#################### method 170 ##################

function170<- function(x,y){
  x$bnstreet <-    paste(x$subbuildingname,x$buildingname,sep=",")
  x$bnstreet <-    paste(x$bnstreet,x$streetdescription,sep=",")
  x$bnstreet <- gsub("/", "", x$bnstreet)
  x$bnstreet <- gsub("[.]", "", x$bnstreet)
  x$bnstreet <- gsub("[']", "", x$bnstreet)
  x$bnstreet <- gsub(" ", "", x$bnstreet)
  x$addressf <-paste(x$postcodelocator,x$bnstreet,sep=",")
  
  y$addressfinal <- y$add1
  #y$addressfinal <- gsub("[.]", "", y$addressfinal)
  #y$addressfinal <- gsub("/", "", y$addressfinal)
  y$addressfinal <- gsub("[.]", "", y$addressfinal)
  y$addressfinal <- gsub("[']", "", y$addressfinal)
  y$addressfinal <- gsub("/", "", y$addressfinal)
  y$addressfinal <- gsub(" ", "", y$addressfinal)
  y$addressf <- paste(y$postcode,y$addressfinal,sep=",")
  
  taba1 <- inner_join(x,y,by="addressf")
  return(taba1)
  
  
}


link170<-function170(add,epc)

length(unique(link170$lmk_key))
link170<-link170[,..needlist1]


link170u<- uniqueresult(link170)
dim(link170u)


link170d <- doubleresult(link170)
dim(link170d)

epc <- matchleft(epc,link170)
dim(epc)
#1526096      27



#################### method 171 ##################

function171<- function(x,y){
  #x<-x[x$paostartsuffix=="",]
  x$bnstreet <-    paste(x$saotext ,x$pp,sep=",")
  #x$bnstreet <-    paste(x$bnstreet,x$streetdescription,sep=" ")
  x$bnstreet <- gsub(" ", "", x$bnstreet)
  x$addressf <-paste(x$postcodelocator,x$bnstreet,sep=", ")
  
  y<- y[grepl("-",y$add2),]
  y$addressfinal <- beg2char(y$add2, "-")
  
  y$addressfinal <- paste(y$add1,y$addressfinal,sep=",")
  
  y$addressfinal <- gsub("[.]", "", y$addressfinal)
  y$addressfinal <- gsub("[']", "", y$addressfinal)
  y$addressfinal <- gsub(" ", "", y$addressfinal)
  y$addressf <- paste(y$postcode,y$addressfinal,sep=", ")
  
  taba1 <- inner_join(x,y,by="addressf")
  return(taba1)
  
  
}


link171<-function171(add,epc)


link171<-link171[,..needlist1]


link171u<- uniqueresult(link171)
dim(link171u)
# 7250   31


link171d <- doubleresult(link171)
dim(link171d)
# 18 31

dim(epc)
# 1521554      27
#
epc <- matchleft(epc,link171)
dim(epc)
# 1514295      27
#1518889      27


#################### method 172 ##################


function172<- function(x,y){
  
  #x<-x[x$paostartsuffix=="",]
  x$bnstreet <-    paste(x$saotext ,x$pp,sep=",")
  #x$bnstreet <-    paste(x$bnstreet,x$streetdescription,sep=" ")
  x$bnstreet <- gsub("/", "", x$bnstreet)
  x$bnstreet <- gsub("[.]", "", x$bnstreet)
  x$bnstreet <- gsub("[']", "", x$bnstreet)
  x$bnstreet <- gsub(" ", "", x$bnstreet)
  x$addressf <-paste(x$postcodelocator,x$bnstreet,sep=", ")
  
  y<- y[grepl("-",y$add2),]
  y$addressfinal <- beg2char(y$add2, "-")
  
  y$addressfinal <- paste(y$add1,y$addressfinal,sep=",")
  y$addressfinal <- gsub("/", "", y$addressfinal)
  y$addressfinal <- gsub("[.]", "", y$addressfinal)
  y$addressfinal <- gsub("[']", "", y$addressfinal)
  y$addressfinal <- gsub(" ", "", y$addressfinal)
  y$addressf <- paste(y$postcode,y$addressfinal,sep=", ")
  
  taba1 <- inner_join(x,y,by="addressf")
  return(taba1)
  
  
}

link172<-function172(add,epc)

link172<-link172[,..needlist1]


link172u<- uniqueresult(link172)
dim(link172u)
#  6 31


link172d <- doubleresult(link172)
dim(link172d)
#  0 31


dim(epc)
# 1514295      27
#
epc <- matchleft(epc,link172)
dim(epc)
# 1514289      27
# 1518787      27



#################### method 173 (old 75)##################

function173<- function(x,y){
  
  x<-x[x$paostartsuffix=="",]
  x$bnstreet <-    paste(x$buildingnumber,x$streetdescription,sep=" ")
  x$bnstreet <- gsub("/", "", x$bnstreet)
  x$bnstreet <- gsub("[.]", "", x$bnstreet)
  x$bnstreet <- gsub("[']", "", x$bnstreet)
  x$bnstreet <- gsub(" ", "", x$bnstreet)
  x$addressf <-paste(x$postcodelocator,x$bnstreet,sep=",")
  
  y<-y[y$add3=="",]
  y$addressfinal <- y$add1
  y$addressfinal <- gsub("/", "", y$addressfinal)
  y$addressfinal <- gsub("[.]", "", y$addressfinal)
  y$addressfinal <- gsub("[']", "", y$addressfinal)
  y$addressfinal <- gsub(" ", "", y$addressfinal)
  y$addressf <- paste(y$postcode,y$addressfinal,sep=",")
  
  taba1 <- inner_join(x,y,by="addressf")
  return(taba1)
  
}



link173<-function173(add,epc)
dim(link173)
# 48999    59
# 
link173<-link173[,..needlist1]

link173u<- uniqueresult(link173)
dim(link173u)
# 32406    31
#

link173d <- doubleresult(link173)
dim(link173d)
# 16593    31


dim(epc)
# 1514289      27
#
epc <- matchleft(epc,link173)
dim(epc)
# 1481620      27
# 1486118      27


#################### method 174  add2 not start with number##################

function174<- function(x,y){
  
  x<-x[x$subbuildingname=="",]
  x$bnstreet <-    paste(x$buildingnumber,x$streetdescription,sep=",")
  x$bnstreet <-    paste(x$bnstreet,x$locality,sep=",")
  #x$bnstreet <- gsub("/", "", x$bnstreet)
  x$bnstreet <- gsub("[.]", "", x$bnstreet)
  x$bnstreet <- gsub("[']", "", x$bnstreet)
  x$bnstreet <- gsub(" ", "", x$bnstreet)
  x$addressf <-paste(x$postcodelocator,x$bnstreet,sep=", ")
  
  
  
  y$addressfinal <- beg2char(y$add1, " ")
  y <- y[!grepl("^\\d",y$add2),]
  y$addressfinal <- paste( y$addressfinal,y$add2,sep=" ")
  y$addressfinal <- paste( y$addressfinal,y$add3,sep=",")
  #y$addressfinal <- gsub("/", "", y$addressfinal)
  y$addressfinal <- gsub("[.]", "", y$addressfinal)
  y$addressfinal <- gsub("[']", "", y$addressfinal)
  #y$addressfinal <- gsub("[.]", "", y$addressfinal)
  y$addressfinal <- gsub(" ", "", y$addressfinal)
  y$addressf <- paste(y$postcode,y$addressfinal,sep=", ")
  
  taba1 <- inner_join(x,y,by="addressf")
  return(taba1)
  
}


link174<-function174(add,epc)



link174<-link174[,..needlist1]


link174u<- uniqueresult(link174)
dim(link174u)
# 3096   31


link174d <- doubleresult(link174)


dim(epc)
# 1481620      27
# 
epc<- matchleft(epc,link174)
dim(epc)
# 1478523      27
# 1483021      27
#################### method 175 add2 not start with number##################


function175<- function(x,y){
  
  x<-x[x$subbuildingname=="",]
  x$bnstreet <-    paste(x$ss,x$streetdescription,sep=",")
  x$bnstreet <-    paste(x$bnstreet,x$locality,sep=",")
  
  x$bnstreet <- gsub("[.]", "", x$bnstreet)
  x$bnstreet <- gsub("[']", "", x$bnstreet)
  x$bnstreet <- gsub(" ", "", x$bnstreet)
  x$addressf <-paste(x$postcodelocator,x$bnstreet,sep=", ")
  
  
  
  y$addressfinal <- beg2char(y$add1, " ")
  y <- y[!grepl("^\\d",y$add2),]
  y$addressfinal <- paste( y$addressfinal,y$add2,sep=" ")
  y$addressfinal <- paste( y$addressfinal,y$add3,sep=",")
  
  y$addressfinal <- gsub("[.]", "", y$addressfinal)
  y$addressfinal <- gsub("[']", "", y$addressfinal)
  
  y$addressfinal <- gsub(" ", "", y$addressfinal)
  y$addressf <- paste(y$postcode,y$addressfinal,sep=", ")
  
  taba1 <- inner_join(x,y,by="addressf")
  return(taba1)
  
}



link175<-function175(add,epc)


link175<-link175[,..needlist1]


link175u<- uniqueresult(link175)
dim(link175u)
# 310  31


link175d <- doubleresult(link175)
dim(link175d)
#  2 31


dim(epc)
# 1478523      27
#
epc <- matchleft(epc,link175)
dim(epc)
# 1478212      27
#

#1482710      27
#################### method 176 ##################

function176<- function(x,y){
  
  x$bnstreet <-    paste(x$ss,x$paotext,sep=" ")
  x$bnstreet <- gsub("/", "", x$bnstreet)
  x$bnstreet <- gsub("[.]", "", x$bnstreet)
  x$bnstreet <- gsub("[']", "", x$bnstreet)
  x$bnstreet <- gsub(" ", "", x$bnstreet)
  x$addressf <-paste(x$postcodelocator,x$bnstreet,sep=", ")
  
  
  y$addressfinal <- y$add1
  y$addressfinal <- gsub("/", "", y$addressfinal)
  y$addressfinal <- gsub("[.]", "", y$addressfinal)
  y$addressfinal <- gsub("[']", "", y$addressfinal)
  y$addressfinal <- gsub(" ", "", y$addressfinal)
  y$addressf <- paste(y$postcode,y$addressfinal,sep=", ")
  
  taba1 <- inner_join(x,y,by="addressf")
  return(taba1)
  
}




link176<-function176(add,epc)
dim(link176)
# 63804    59
# 

link176<-link176[,..needlist1]



link176u<- uniqueresult(link176)
dim(link176u)
# 63386    31
# 

link176d <- doubleresult(link176)
dim(link176d)
# 418  31


epc <- matchleft(epc,link176)
dim(epc)
# 1414661      27
# 1418863      27

#################### method 177 ##################



function177<- function(x,y){
  
  
  x$bnstreet <-    paste(x$subbuildingname,x$buildingname ,sep=",")
  x$bnstreet <- gsub("/", "", x$bnstreet)
  x$bnstreet <- gsub("[.]", "", x$bnstreet)
  x$bnstreet <- gsub("[']", "", x$bnstreet)
  x$bnstreet <- gsub(" ", "", x$bnstreet)
  x$addressf <-paste(x$postcodelocator,x$bnstreet,sep=",")
  
  
  y$addressfinal <- beg2char(y$add2, ",")
  
  y$addressfinal <- paste( y$add1, y$addressfinal,sep=",")
  y$addressfinal <- gsub("[/]", "", y$addressfinal)
  y$addressfinal <- gsub("[.]", "", y$addressfinal)
  y$addressfinal <- gsub("[']", "", y$addressfinal)
  y$addressfinal <- gsub(" ", "", y$addressfinal)
  y$addressf <- paste(y$postcode,y$addressfinal,sep=",")
  
  taba1 <- inner_join(x,y,by="addressf")
  return(taba1)
  
  
}

link177<-function177(add,epc)
dim(link177)
# 2022   59


link177<-link177[,..needlist1]

link177u<- uniqueresult(link177)
dim(link177u)
# 2022   31


link177d <- doubleresult(link177)
dim(link177d)
#  0 31


dim(epc)
# 1414661      27
# 
epc <- matchleft(epc,link177)
dim(epc)
# 1412639      27
#################### method 178 ##################

function178<- function(x,y){
  x$bnstreet <-    paste(x$buildingnumber,x$streetdescription,sep=" ")
  x$bnstreet <- gsub("-", "", x$bnstreet)
  
  x$bnstreet <- gsub("[']", "", x$bnstreet)
  x$bnstreet <- gsub("[.]", "", x$bnstreet)
  #x$bnstreet <- gsub(" ", "", x$bnstreet)
  x$addressf <-paste(x$postcodelocator,x$bnstreet,sep=",")
  
  
  y<-y[!grepl("\\d+-\\d",y$add),]
  y$addressfinal <- y$add
  
  
  y$addressfinal <- gsub("-", "", y$addressfinal)
  y$addressfinal <- gsub(",", "", y$addressfinal)
  y$addressfinal <- gsub("[.]", "", y$addressfinal)
  y$addressfinal <- gsub("[']", "", y$addressfinal)
  #y$addressfinal <- gsub(" ", "", y$addressfinal)
  y$addressf <- paste(y$postcode,y$addressfinal,sep=",")
  
  taba1 <- inner_join(x,y,by="addressf")
  return(taba1)
  
}

link178<-function178(add,epc)
dim(link178)



link178<-link178[,..needlist1]



link178u<- uniqueresult(link178)
dim(link178u)


link178d <- doubleresult(link178)
dim(link178d)


dim(epc)
# 
epc <- matchleft(epc,link178)
dim(epc)
#2269509      27
#################### method 179 ##################
function179<- function(x,y){
  x$bnstreet <-    paste(x$subbuildingname,x$pp ,sep=",")
  x$bnstreet <-    paste(x$bnstreet,x$paotext ,sep=" ")
  
  x$bnstreet <- gsub("/", "", x$bnstreet)
  x$bnstreet <- gsub("[.]", "", x$bnstreet)
  x$bnstreet <- gsub("[']", "", x$bnstreet)
  x$bnstreet <- gsub(" ", "", x$bnstreet)
  x$addressf <-paste(x$postcodelocator,x$bnstreet,sep=",")
  
  
  y$addressfinal <- paste( y$add1, y$add2,sep=",")
  y$addressfinal <- gsub("[/]", "", y$addressfinal)
  y$addressfinal <- gsub("[.]", "", y$addressfinal)
  y$addressfinal <- gsub("[']", "", y$addressfinal)
  y$addressfinal <- gsub(" ", "", y$addressfinal)
  y$addressf <- paste(y$postcode,y$addressfinal,sep=",")
  
  taba1 <- inner_join(x,y,by="addressf")
  return(taba1)
}


link179<-function179(add,epc)

link179<-link179[,..needlist1]


link179u<- uniqueresult(link179)
dim(link179u)


link179d <- doubleresult(link179)
dim(link179d)

dim(epc)
#2649854      27
epc <- matchleft(epc,link179)
dim(epc)
#1414557      27



#################### method 180 ##################

function180<- function(x,y){
  
  x <- x[!grepl("^\\d",x$buildingnumber),]
  #buildingname is not a number
  x$bnstreet <-    paste(x$saotext,x$paotext,sep=" ")
  
  x$bnstreet <-    paste(x$bnstreet,x$streetdescription,sep=",")
  x$bnstreet <- gsub("/", "", x$bnstreet)
  x$bnstreet <- gsub("[.]", "", x$bnstreet)
  x$bnstreet <- gsub("[']", "", x$bnstreet)
  x$bnstreet <- gsub(" ", "", x$bnstreet)
  x$addressf <-paste(x$postcodelocator,x$bnstreet,sep=",")
  
  
  y$addressfinal <- paste(y$add1,y$add3,sep=",")
  y$addressfinal <- gsub("[/]", "", y$addressfinal)
  y$addressfinal <- gsub("[.]", "", y$addressfinal)
  y$addressfinal <- gsub("[']", "", y$addressfinal)
  y$addressfinal <- gsub(" ", "", y$addressfinal)
  y$addressf <- paste(y$postcode,y$addressfinal,sep=",")
  
  taba1 <- inner_join(x,y,by="addressf")
  return(taba1)
  
}


link180<-function180(add,epc)


link180<-link180[,..needlist1]
length(unique(link180$lmk_key))

link180u<- uniqueresult(link180)
dim(link180u)


link180d <- doubleresult(link180)
dim(link180d)

dim(epc)
#
epc <- matchleft(epc,link180)
dim(epc)
# 1412584      27

#################### method 181 ##################

function181<- function(x,y){
  
  x$bnstreet <-    paste(x$subbuildingname,x$buildingnumber,sep=",")
  x$streetdescription <- gsub("-", "", x$streetdescription)
  
  
  x$bnstreet <-    paste(x$bnstreet,x$streetdescription,sep=",")
  
  
  x$bnstreet <- gsub("/", "", x$bnstreet)
  x$bnstreet <- gsub("[.]", "", x$bnstreet)
  x$bnstreet <- gsub("[']", "", x$bnstreet)
  x$bnstreet <- gsub(" ", "", x$bnstreet)
  x$addressf <-paste(x$postcodelocator,x$bnstreet,sep=",")
  
  
  y$addressfinal <- y$add
  y$addressfinal <- gsub("[/]", "", y$addressfinal)
  y$addressfinal <- gsub("[.]", "", y$addressfinal)
  y$addressfinal <- gsub("[']", "", y$addressfinal)
  y$addressfinal <- gsub(" ", "", y$addressfinal)
  y$addressf <- paste(y$postcode,y$addressfinal,sep=",")
  
  taba1 <- inner_join(x,y,by="addressf")
  return(taba1)
  
  
}


link181<-function181(add,epc)


link181<-link181[,..needlist1]


link181u<- uniqueresult(link181)
dim(link181u)


link181d <- doubleresult(link181)
dim(link181d)

dim(epc)
#
epc <- matchleft(epc,link181)
dim(epc)
#1412572      27


#################### method 182 ##################


function182<- function(x,y){
  
  
  # x<-x[x$paostartsuffix=="",]
  
  x$bnstreet <-    paste(x$subbuildingname,x$buildingname,sep=" ")
  x$bnstreet <-    paste(x$bnstreet,x$buildingnumber,sep=",")
  x$bnstreet <-    paste(x$bnstreet,x$streetdescription,sep=" ")
  x$bnstreet <- gsub("/", "", x$bnstreet)
  x$bnstreet <- gsub("[.]", "", x$bnstreet)
  x$bnstreet <- gsub("[']", "", x$bnstreet)
  #x$bnstreet <- gsub("[']", "", x$bnstreet)
  x$bnstreet <- gsub(" ", "", x$bnstreet)
  x$addressf <-paste(x$postcodelocator,x$bnstreet,sep=",")
  
  
  y$addressfinal <- paste(y$add1,y$add2,sep="")
  
  y$addressfinal <- paste(y$addressfinal,y$add3,sep=",")
  y$addressfinal <- gsub("[/]", "", y$addressfinal)
  y$addressfinal <- gsub("[.]", "", y$addressfinal)
  y$addressfinal <- gsub("[']", "", y$addressfinal)
  
  y$addressfinal <- gsub(" ", "", y$addressfinal)
  y$addressf <- paste(y$postcode,y$addressfinal,sep=",")
  
  taba1 <- inner_join(x,y,by="addressf")
  return(taba1)
  
}

link182<-function182(add,epc)

link182<-link182[,..needlist1]

length(unique(link182$lmk_key))
link182u<- uniqueresult(link182)
dim(link182u)


link182d <- doubleresult(link182)
dim(link182d)


dim(epc)
#
epc <- matchleft(epc,link182)
dim(epc)
# 1412445      27



#################### method 183 ##################

function183<- function(x,y){
  x$bnstreet <-    paste(x$subbuildingname,x$buildingnumber,sep=",")
  x$bnstreet <-    paste(x$bnstreet,x$paotext,sep=",")
  
  x$bnstreet <- gsub("/", "", x$bnstreet)
  x$bnstreet <- gsub("[.]", "", x$bnstreet)
  x$bnstreet <- gsub("[']", "", x$bnstreet)
  #x$bnstreet <- gsub("[']", "", x$bnstreet)
  x$bnstreet <- gsub(" ", "", x$bnstreet)
  x$addressf <-paste(x$postcodelocator,x$bnstreet,sep=",")
  
  
  y$addressfinal <- paste(y$add1,y$add2,sep=",")
  
  
  y$addressfinal <- gsub("[/]", "", y$addressfinal)
  y$addressfinal <- gsub("[.]", "", y$addressfinal)
  y$addressfinal <- gsub("[']", "", y$addressfinal)
  
  y$addressfinal <- gsub(" ", "", y$addressfinal)
  y$addressf <- paste(y$postcode,y$addressfinal,sep=",")
  
  taba1 <- inner_join(x,y,by="addressf")
  return(taba1)
  
  
}



link183<-function183(add,epc)
dim(link183)
# 
link183<-link183[,..needlist1]

link183u<- uniqueresult(link183)
dim(link183u)
#

link183d <- doubleresult(link183)
dim(link183d)


dim(epc)
#
epc <- matchleft(epc,link183)
dim(epc)
#1410418      27


#################### method 184  ##################

function184<- function(x,y){
  
  x$bnstreet <-    paste(x$saotext,x$buildingnumber,sep=",")
  x$bnstreet <-    paste(x$bnstreet,x$paotext,sep=",")
  
  x$bnstreet <- gsub("/", "", x$bnstreet)
  x$bnstreet <- gsub("[.]", "", x$bnstreet)
  x$bnstreet <- gsub("[']", "", x$bnstreet)
  
  x$bnstreet <- gsub(" ", "", x$bnstreet)
  x$addressf <-paste(x$postcodelocator,x$bnstreet,sep=",")
  
  
  y$addressfinal <- paste(y$add1,y$add2,sep=",")
  
  
  y$addressfinal <- gsub("[/]", "", y$addressfinal)
  y$addressfinal <- gsub("[.]", "", y$addressfinal)
  y$addressfinal <- gsub("[']", "", y$addressfinal)
  
  y$addressfinal <- gsub(" ", "", y$addressfinal)
  y$addressf <- paste(y$postcode,y$addressfinal,sep=",")
  
  taba1 <- inner_join(x,y,by="addressf")
  return(taba1)
  
}


link184<-function184(add,epc)



link184<-link184[,..needlist1]


link184u<- uniqueresult(link184)
dim(link184u)


link184d <- doubleresult(link184)


dim(epc)
# 
epc<- matchleft(epc,link184)
dim(epc)
#   1409886      27
#################### method 185 ##################


function185<- function(x,y){
  
  x$bnstreet <-    paste(x$subbuildingname,x$ss,sep=",")
  x$bnstreet <-    paste(x$bnstreet,x$paotext,sep=",")
  
  x$bnstreet <- gsub("/", "", x$bnstreet)
  x$bnstreet <- gsub("[.]", "", x$bnstreet)
  x$bnstreet <- gsub("[']", "", x$bnstreet)
  
  x$bnstreet <- gsub(" ", "", x$bnstreet)
  x$addressf <-paste(x$postcodelocator,x$bnstreet,sep=",")
  
  
  y$addressfinal <- paste(y$add1,y$add2,sep=",")
  
  
  y$addressfinal <- gsub("[/]", "", y$addressfinal)
  y$addressfinal <- gsub("[.]", "", y$addressfinal)
  y$addressfinal <- gsub("[']", "", y$addressfinal)
  
  y$addressfinal <- gsub(" ", "", y$addressfinal)
  y$addressf <- paste(y$postcode,y$addressfinal,sep=",")
  
  taba1 <- inner_join(x,y,by="addressf")
  return(taba1)
  
}



link185<-function185(add,epc)


link185<-link185[,..needlist1]


link185u<- uniqueresult(link185)
dim(link185u)


link185d <- doubleresult(link185)
dim(link185d)


dim(epc)
#
epc <- matchleft(epc,link185)
dim(epc)
#1409877      27

#
#################### method 186 ##################

function186<- function(x,y){
  x$bnstreet <-    paste(x$saotext,x$ss,sep=",")
  x$bnstreet <-    paste(x$bnstreet,x$paotext,sep=",")
  
  x$bnstreet <- gsub("/", "", x$bnstreet)
  x$bnstreet <- gsub("[.]", "", x$bnstreet)
  x$bnstreet <- gsub("[']", "", x$bnstreet)
  #x$bnstreet <- gsub("[']", "", x$bnstreet)
  x$bnstreet <- gsub(" ", "", x$bnstreet)
  x$addressf <-paste(x$postcodelocator,x$bnstreet,sep=",")
  
  
  y$addressfinal <- paste(y$add1,y$add2,sep=",")
  
  
  y$addressfinal <- gsub("[/]", "", y$addressfinal)
  y$addressfinal <- gsub("[.]", "", y$addressfinal)
  y$addressfinal <- gsub("[']", "", y$addressfinal)
  
  y$addressfinal <- gsub(" ", "", y$addressfinal)
  y$addressf <- paste(y$postcode,y$addressfinal,sep=",")
  
  taba1 <- inner_join(x,y,by="addressf")
  return(taba1)
  
  
}




link186<-function186(add,epc)
dim(link186)
# 

link186<-link186[,..needlist1]



link186u<- uniqueresult(link186)
dim(link186u)
# 

link186d <- doubleresult(link186)
dim(link186d)


epc <- matchleft(epc,link186)
dim(epc)
# 1409783      27

#################### method 187 ##################



function187<- function(x,y){
  
  
  
  x$bnstreet <-    paste(x$saotext,x$ss,sep=",")
  x$bnstreet <-    paste(x$bnstreet,x$paotext,sep=" ")
  x$bnstreet <-    paste(x$bnstreet,x$pp,sep=",")
  x$bnstreet <-    paste(x$bnstreet,x$streetdescription,sep=" ")
  x$bnstreet <- gsub("/", "", x$bnstreet)
  x$bnstreet <- gsub("[.]", "", x$bnstreet)
  
  x$bnstreet <- gsub("[']", "", x$bnstreet)
  x$bnstreet <- gsub(" ", "", x$bnstreet)
  x$addressf <-paste(x$postcodelocator,x$bnstreet,sep=",")
  
  
  y$addressfinal <-y$add
  y$addressfinal <- gsub("[/]", "", y$addressfinal)
  y$addressfinal <- gsub("[.]", "", y$addressfinal)
  y$addressfinal <- gsub("[']", "", y$addressfinal)
  
  y$addressfinal <- gsub(" ", "", y$addressfinal)
  y$addressf <- paste(y$postcode,y$addressfinal,sep=",")
  
  taba1 <- inner_join(x,y,by="addressf")
  return(taba1) 
  
  
}

link187<-function187(add,epc)
dim(link187)


link187<-link187[,..needlist1]

link187u<- uniqueresult(link187)
dim(link187u)


link187d <- doubleresult(link187)
dim(link187d)


dim(epc)
# 
epc <- matchleft(epc,link187)
dim(epc)
#1409689      27
#
#################### method 188 ##################

function188<- function(x,y){
  
  #x<-x[x$paostartsuffix=="",]
  
  x$bnstreet <-    paste(x$saotext,x$pp,sep=",")
  x$bnstreet <-    paste(x$bnstreet,x$paotext,sep=" ")

  x$bnstreet <- gsub("/", "", x$bnstreet)
  x$bnstreet <- gsub("[.]", "", x$bnstreet)
  x$bnstreet <- gsub("[']", "", x$bnstreet)
  
  x$bnstreet <- gsub(" ", "", x$bnstreet)
  x$addressf <-paste(x$postcodelocator,x$bnstreet,sep=",")
  
  
  y$addressfinal <-  paste(y$add1,y$add2,sep=",")
  y$addressfinal <- gsub("[/]", "", y$addressfinal)
  y$addressfinal <- gsub("[.]", "", y$addressfinal)
  y$addressfinal <- gsub("[']", "", y$addressfinal)
  
  y$addressfinal <- gsub(" ", "", y$addressfinal)
  y$addressf <- paste(y$postcode,y$addressfinal,sep=",")
  
  taba1 <- inner_join(x,y,by="addressf")
  return(taba1) 
  
  
  
}



link188<-function188(add,epc)
dim(link188)


link188<-link188[,..needlist1]



link188u<- uniqueresult(link188)
dim(link188u)


link188d <- doubleresult(link188)
dim(link188d)


dim(epc)
# 
epc <- matchleft(epc,link188)
dim(epc)
# 1409527      27
#################### method 189 ##################
function189<- function(x,y){
  
  x$bnstreet <-    paste(x$saotext,x$pp,sep=" ")
  x$bnstreet <-    paste(x$bnstreet,x$paotext,sep=" ")

  x$bnstreet <- gsub("/", "", x$bnstreet)
  x$bnstreet <- gsub("[.]", "", x$bnstreet)
  x$bnstreet <- gsub("[']", "", x$bnstreet)
  
  #x$bnstreet <- gsub(" ", "", x$bnstreet)
  x$addressf <-paste(x$postcodelocator,x$bnstreet,sep=",")
  
  
  y$addressfinal <-  paste(y$add1,y$add2,sep="")
  y$addressfinal <- gsub("[/]", "", y$addressfinal)
  y$addressfinal <- gsub("[.]", "", y$addressfinal)
  y$addressfinal <- gsub("[']", "", y$addressfinal)
  
  #y$addressfinal <- gsub(" ", "", y$addressfinal)
  y$addressf <- paste(y$postcode,y$addressfinal,sep=",")
  
  taba1 <- inner_join(x,y,by="addressf")
  return(taba1) 
}


link189<-function189(add,epc)

link189<-link189[,..needlist1]


link189u<- uniqueresult(link189)
dim(link189u)


link189d <- doubleresult(link189)
dim(link189d)

dim(epc)
#2
epc <- matchleft(epc,link189)
dim(epc)
# 1409450      27

#################### method 190 ##################

function190<- function(x,y){
  
  x$bnstreet <-    paste(x$buildingnumber,x$dependentlocality,sep=",")
  #x$bnstreet <- gsub("/", "", x$bnstreet)
  x$bnstreet <- gsub("[.]", "", x$bnstreet)
  x$bnstreet <- gsub("[']", "", x$bnstreet)
  x$bnstreet <- gsub(" ", "", x$bnstreet)
  x$addressf <-paste(x$postcodelocator,x$bnstreet,sep=",")
  
  y$addressfinal <- paste(y$add)
  # y$bnstreet <- gsub("/", "",  y$bnstreet)
  y$bnstreet <- gsub("[.]", "",  y$bnstreet)
  y$bnstreet <- gsub("[']", "",  y$bnstreet)
  y$addressfinal <- gsub(" ", "", y$addressfinal)
  y$addressf <- paste(y$postcode,y$addressfinal,sep=",")
  
  taba1 <- inner_join(x,y,by="addressf")
  return(taba1)

}




link190<-function190(add,epc)

length(unique(link190$lmk_key))
link190<-link190[,..needlist1]


link190u<- uniqueresult(link190)
dim(link190u)


link190d <- doubleresult(link190)
dim(link190d)

dim(epc)
#
epc <- matchleft(epc,link190)
dim(epc)
#1409447      27

#################### method 191 ##################

function191<- function(x,y){
  
  x$bnstreet <-    paste(x$subbuildingname,x$buildingnumber,sep=",")
  x$bnstreet <-    paste(x$bnstreet,x$streetdescription,sep=" ")

  x$bnstreet <- gsub("/", "", x$bnstreet)
  x$bnstreet <- gsub("[.]", "", x$bnstreet)
  x$bnstreet <- gsub("[']", "", x$bnstreet)
  
  x$bnstreet <- gsub(" ", "", x$bnstreet)
  x$addressf <-paste(x$postcodelocator,x$bnstreet,sep=",")
  
  
  y$addressfinal <-    paste(y$add1,y$add2,sep=",")
  y$addressfinal <- gsub("[/]", "", y$addressfinal)
  y$addressfinal <- gsub("[.]", "", y$addressfinal)
  y$addressfinal <- gsub("[']", "", y$addressfinal)
  
  y$addressfinal <- gsub(" ", "", y$addressfinal)
  y$addressf <- paste(y$postcode,y$addressfinal,sep=",")
  
  taba1 <- inner_join(x,y,by="addressf")
  return(taba1) 
  
}


link191<-function191(add,epc)


link191<-link191[,..needlist1]


link191u<- uniqueresult(link191)
dim(link191u)


link191d <- doubleresult(link191)
dim(link191d)

dim(epc)
#
epc <- matchleft(epc,link191)
dim(epc)
#1409134      27


#################### method 192 ##################


function192<- function(x,y){
  
  
  
  x$bnstreet <-    paste(x$saotext,x$saostartnumber,sep=",")
  x$bnstreet <-    paste(x$bnstreet,x$saoendnumber,sep="-")
  x$bnstreet <-    paste(x$bnstreet,x$paotext,sep=" ")
  x$bnstreet <-    paste(x$bnstreet,x$streetdescription,sep=",")
  x$bnstreet <- gsub("/", "", x$bnstreet)
  x$bnstreet <- gsub("[.]", "", x$bnstreet)
  x$bnstreet <- gsub("[']", "", x$bnstreet)
  #x$bnstreet <- gsub("[']", "", x$bnstreet)
  x$bnstreet <- gsub(" ", "", x$bnstreet)
  x$addressf <-paste(x$postcodelocator,x$bnstreet,sep=",")
  
  
  y$addressfinal <- y$add
  y$addressfinal <- gsub("[/]", "", y$addressfinal)
  y$addressfinal <- gsub("[.]", "", y$addressfinal)
  y$addressfinal <- gsub("[']", "", y$addressfinal)
  
  y$addressfinal <- gsub(" ", "", y$addressfinal)
  y$addressf <- paste(y$postcode,y$addressfinal,sep=",")
  
  taba1 <- inner_join(x,y,by="addressf")
  return(taba1) 
  
  
}

link192<-function192(add,epc)

link192<-link192[,..needlist1]


link192u<- uniqueresult(link192)
dim(link192u)


link192d <- doubleresult(link192)
dim(link192d)


dim(epc)
#
epc <- matchleft(epc,link192)
dim(epc)
# 1409079      27



#################### method 193 ##################

function193<- function(x,y){
  
  
  
  x$bnstreet <-    paste(x$saotext,x$saostartnumber,sep=",")
  x$bnstreet <-    paste(x$bnstreet,x$saoendnumber,sep="-")
  x$bnstreet <-    paste(x$bnstreet,x$paotext,sep=" ")
  x$bnstreet <-    paste(x$bnstreet,x$streetdescription,sep=",")
  x$bnstreet <- gsub("/", "", x$bnstreet)
  x$bnstreet <- gsub("[.]", "", x$bnstreet)
  x$bnstreet <- gsub("[']", "", x$bnstreet)
  
  x$bnstreet <- gsub(" ", "", x$bnstreet)
  x$addressf <-paste(x$postcodelocator,x$bnstreet,sep=",")
  
  
  y$addressfinal <-  paste(y$add1,y$add2,sep=",")
  y$addressfinal <- gsub("[/]", "", y$addressfinal)
  y$addressfinal <- gsub("[.]", "", y$addressfinal)
  y$addressfinal <- gsub("[']", "", y$addressfinal)
  
  y$addressfinal <- gsub(" ", "", y$addressfinal)
  y$addressf <- paste(y$postcode,y$addressfinal,sep=",")
  
  taba1 <- inner_join(x,y,by="addressf")
  return(taba1)
  
  
}



link193<-function193(add,epc)
dim(link193)
# 
link193<-link193[,..needlist1]

link193u<- uniqueresult(link193)
dim(link193u)
#

link193d <- doubleresult(link193)
dim(link193d)


dim(epc)
#
epc <- matchleft(epc,link193)
dim(epc)
# 1409075      27


#################### method 194  ##################

function194<- function(x,y){
  
  x$bnstreet <-    paste(x$saotext,x$saostartnumber,sep=",")
  x$bnstreet <-    paste(x$bnstreet,x$saoendnumber,sep="-")
  x$bnstreet <-    paste(x$bnstreet,x$paotext,sep=" ")
  #x$bnstreet <-    paste(x$bnstreet,x$streetdescription,sep=",")
  x$bnstreet <- gsub("/", "", x$bnstreet)
  x$bnstreet <- gsub("[.]", "", x$bnstreet)
  x$bnstreet <- gsub("[']", "", x$bnstreet)

  x$bnstreet <- gsub(" ", "", x$bnstreet)
  x$addressf <-paste(x$postcodelocator,x$bnstreet,sep=",")
  
  
  y$addressfinal <-  paste(y$add1,y$add2,sep=",")
  y$addressfinal <- gsub("[/]", "", y$addressfinal)
  y$addressfinal <- gsub("[.]", "", y$addressfinal)
  y$addressfinal <- gsub("[']", "", y$addressfinal)
  
  y$addressfinal <- gsub(" ", "", y$addressfinal)
  y$addressf <- paste(y$postcode,y$addressfinal,sep=",")
  
  taba1 <- inner_join(x,y,by="addressf")
  return(taba1)
  
  
}


link194<-function194(add,epc)



link194<-link194[,..needlist1]


link194u<- uniqueresult(link194)
dim(link194u)


link194d <- doubleresult(link194)
head(link194[grepl("[']", link194$add),])
head(link194[grepl("[.]", link194$add),])

dim(epc)
# 
epc<- matchleft(epc,link194)
dim(epc)
#   1409055      27
#################### method 195 ##################


function195<- function(x,y){
  
  x$bnstreet <-    paste(x$saotext,x$buildingname,sep=",")
  
  x$bnstreet <-    paste(x$bnstreet,x$paotext,sep=" ")
 
  x$bnstreet <- gsub("/", "", x$bnstreet)
  x$bnstreet <- gsub("[.]", "", x$bnstreet)
  x$bnstreet <- gsub("[']", "", x$bnstreet)

  x$bnstreet <- gsub(" ", "", x$bnstreet)
  x$addressf <-paste(x$postcodelocator,x$bnstreet,sep=",")
  
  
  y$addressfinal <-  paste(y$add1,y$add2,sep=",")
  y$addressfinal <- gsub("[/]", "", y$addressfinal)
  y$addressfinal <- gsub("[.]", "", y$addressfinal)
  y$addressfinal <- gsub("[']", "", y$addressfinal)
  
  y$addressfinal <- gsub(" ", "", y$addressfinal)
  y$addressf <- paste(y$postcode,y$addressfinal,sep=",")
  
  taba1 <- inner_join(x,y,by="addressf")
  return(taba1)
  
}



link195<-function195(add,epc)


link195<-link195[,..needlist1]


link195u<- uniqueresult(link195)
dim(link195u)


link195d <- doubleresult(link195)
dim(link195d)


dim(epc)
#
epc <- matchleft(epc,link195)
dim(epc)
# 1409032      27

#
#################### method 196 ##################

function196<- function(x,y){
  
  
  x$bnstreet <-    paste(x$subbuildingname,x$buildingnumber,sep=",")
  x$bnstreet <-    paste(x$bnstreet,x$streetdescription,sep=" ")
  x$bnstreet <- gsub(" ", "", x$bnstreet)
  x$addressf <-paste(x$postcodelocator,x$bnstreet,sep=",")
  
  
  y<-y[y$property_type=="Flat",]
  
  #y<-y[y$property_type=="Flat"|y$property_type=="Maisonette",]
  y$addressfinal <- beg2char(y$add1, " ")
  y$addressfinal <-  paste("FLAT ",y$addressfinal,sep=" ")
  y$addressfinal <- paste( y$addressfinal,y$add2,sep=",")
  y$addressfinal <- gsub("[.]", "", y$addressfinal)
  #y$addressfinal <- gsub("[']", "", y$addressfinal)
  y$addressfinal <- gsub(" ", "", y$addressfinal)
  y$addressf <- paste(y$postcode,y$addressfinal,sep=",")
  
  taba1 <- inner_join(x,y,by="addressf")
  return(taba1)
  
}




link196<-function196(add,epc)
dim(link196)
# 
length(unique(link196$lmk_key))
link196<-link196[,..needlist1]



link196u<- uniqueresult(link196)
dim(link196u)
# 

link196d <- doubleresult(link196)
dim(link196d)


epc <- matchleft(epc,link196)
dim(epc)
# 1393625      27

#################### method 197 ##################



function197<- function(x,y){
  
  
  x$bnstreet <-    paste(x$subbuildingname,x$buildingnumber,sep=",")
  x$bnstreet <-    paste(x$bnstreet,x$streetdescription,sep=" ")
  x$bnstreet <- gsub(" ", "", x$bnstreet)
  x$addressf <-paste(x$postcodelocator,x$bnstreet,sep=",")
  
  
  #y<-y[y$property_type=="Flat",]
  
  y<-y[y$property_type=="Maisonette",]
  y$addressfinal <- beg2char(y$add1, " ")
  y$addressfinal <-  paste("FLAT ",y$addressfinal,sep=" ")
  y$addressfinal <- paste( y$addressfinal,y$add2,sep=", ")
  y$addressfinal <- gsub("[.]", "", y$addressfinal)
  #y$addressfinal <- gsub("[']", "", y$addressfinal)
  y$addressfinal <- gsub(" ", "", y$addressfinal)
  y$addressf <- paste(y$postcode,y$addressfinal,sep=",")
  
  taba1 <- inner_join(x,y,by="addressf")
  return(taba1)
  
}

link197<-function197(add,epc)
dim(link197)


link197<-link197[,..needlist1]

link197u<- uniqueresult(link197)
dim(link197u)


link197d <- doubleresult(link197)
dim(link197d)


dim(epc)
# 
epc <- matchleft(epc,link197)
dim(epc)
#393275      27
#
#################### method 198 ##################

function198<- function(x,y){
  
  x$bnstreet <-    paste("FLAT ",x$saostartnumber,sep="")
  x$bnstreet <-    paste(x$bnstreet,x$saostartsuffix ,sep="")
  #x$bnstreet <- str_replace_all(x$bnstreet, "NA", "")
  x$bnstreet <-    paste(x$bnstreet,x$ paostartnumber,sep=",")
  x$bnstreet <-    paste(x$bnstreet,x$ paoendnumber,sep="-")
  x$bnstreet <-    paste(x$bnstreet,x$ streetdescription,sep=" ")
  x$bnstreet <- gsub(" ", "", x$bnstreet)
  x$addressf <-paste(x$postcodelocator,x$bnstreet,sep=",")
  
  y<-y[y$property_type=="Flat"|y$property_type=="Maisonette",]
  y$addressfinal <-  paste(y$add1,y$add2,sep=",")
  
  y$addressfinal <- gsub(" ", "", y$addressfinal)
  y$addressf <- paste(y$postcode,y$addressfinal,sep=",")
  
  taba1 <- inner_join(x,y,by="addressf")
  return(taba1)
}



link198<-function198(add,epc)
dim(link198)


link198<-link198[,..needlist1]



link198u<- uniqueresult(link198)
dim(link198u)


link198d <- doubleresult(link198)
dim(link198d)


dim(epc)
# 
epc <- matchleft(epc,link198)
dim(epc)
#1392695      27
#################### method 199 ##################
function199<- function(x,y){
  x$bnstreet <-    paste("FLAT ",x$saostartnumber,sep="")
  x$bnstreet <-    paste(x$bnstreet,x$saostartsuffix ,sep="")
  #x$bnstreet <- str_replace_all(x$bnstreet, "NA", "")
  x$bnstreet <-    paste(x$bnstreet,x$ paostartnumber,sep=",")
  x$bnstreet <-    paste(x$bnstreet,x$ paoendnumber,sep="-")
  x$bnstreet <-    paste(x$bnstreet,x$ streetdescription,sep=" ")
  #x$bnstreet <- gsub("/", "", x$bnstreet)
  x$bnstreet <- gsub("[.]", "", x$bnstreet)
  x$bnstreet <- gsub("[']", "", x$bnstreet)
  x$bnstreet <- gsub(" ", "", x$bnstreet)
  x$addressf <-paste(x$postcodelocator,x$bnstreet,sep=",")
  
  y<-y[y$property_type=="Flat"|y$property_type=="Maisonette",]
  y$addressfinal <-  paste(y$add1,y$add2,sep=",")
  #y$addressfinal <- gsub("[/]", "", y$addressfinal)
  y$addressfinal <- gsub("[.]", "", y$addressfinal)
  y$addressfinal <- gsub("[']", "", y$addressfinal)
  y$addressfinal <- gsub(" ", "", y$addressfinal)
  y$addressf <- paste(y$postcode,y$addressfinal,sep=",")
  
  taba1 <- inner_join(x,y,by="addressf")
  return(taba1)
}


link199<-function199(add,epc)

link199<-link199[,..needlist1]


link199u<- uniqueresult(link199)
dim(link199u)


link199d <- doubleresult(link199)
dim(link199d)

dim(epc)
#2649854      27
epc <- matchleft(epc,link199)
dim(epc)
#1392630      27



#################### method 200 ##################

function200<- function(x,y){
  
  x$bnstreet <-  gsub("FLAT", "APARTMENT", x$subbuildingname) 
  x$bnstreet <-    paste(x$bnstreet,x$  buildingname,sep=" ")
  x$bnstreet <-    paste(x$bnstreet,x$ streetdescription,sep=",")

  x$bnstreet <- gsub("[.]", "", x$bnstreet)
  x$bnstreet <- gsub("[']", "", x$bnstreet)
  x$bnstreet <- gsub(" ", "", x$bnstreet)
  x$addressf <-paste(x$postcodelocator,x$bnstreet,sep=",")
  
  y<-y[y$property_type=="Flat"|y$property_type=="Maisonette",]
  y$addressfinal <-  paste(y$add1,y$add2,sep=" ")
  y$addressfinal <-  paste(y$addressfinal,y$add3,sep=",")

  y$addressfinal <- gsub("[.]", "", y$addressfinal)
  y$addressfinal <- gsub("[']", "", y$addressfinal)
  y$addressfinal <- gsub(" ", "", y$addressfinal)
  y$addressf <- paste(y$postcode,y$addressfinal,sep=",")
  
  taba1 <- inner_join(x,y,by="addressf")
  return(taba1)
  
}


link200<-function200(add,epc)

length(unique(link200$lmk_key))
link200<-link200[,..needlist1]


link200u<- uniqueresult(link200)
dim(link200u)


link200d <- doubleresult(link200)
dim(link200d)

dim(epc)
#
epc <- matchleft(epc,link200)
dim(epc)
#1391575      27

#################### method 201 ##################

function201<- function(x,y){
  
  x$bnstreet <- paste( "APARTMENT", x$saotext) 
  x$bnstreet <-    paste(x$bnstreet,x$ streetdescription,sep=",")

  x$bnstreet <- gsub("[.]", "", x$bnstreet)
  x$bnstreet <- gsub("[']", "", x$bnstreet)
  x$bnstreet <- gsub(" ", "", x$bnstreet)
  x$addressf <-paste(x$postcodelocator,x$bnstreet,sep=",")
  
  y<-y[y$property_type=="Flat"|y$property_type=="Maisonette",]
  y$addressfinal <-  paste(y$add1,y$add2,sep=" ")
  y$addressfinal <-  paste(y$addressfinal,y$add3,sep=",")
 
  y$addressfinal <- gsub("[.]", "", y$addressfinal)
  y$addressfinal <- gsub("[']", "", y$addressfinal)
  y$addressfinal <- gsub(" ", "", y$addressfinal)
  y$addressf <- paste(y$postcode,y$addressfinal,sep=",")
  
  taba1 <- inner_join(x,y,by="addressf")
  return(taba1)
  
}


link201<-function201(add,epc)


link201<-link201[,..needlist1]


link201u<- uniqueresult(link201)
dim(link201u)


link201d <- doubleresult(link201)
dim(link201d)

dim(epc)
#
epc <- matchleft(epc,link201)
dim(epc)
# 1391564      27


#################### method 202 ##################


function202<- function(x,y){
  
  x$bnstreet <-   beg2char(x$saotext, " ")
  x$bnstreet <-    paste("APARTMENT ",x$bnstreet,sep="")
  
  x$bnstreet <-    paste(x$bnstreet,x$streetdescription,sep=",")
  #x$bnstreet <- gsub("/", "", x$bnstreet)
  x$bnstreet <- gsub("[.]", "", x$bnstreet)
  x$bnstreet <- gsub("[']", "", x$bnstreet)
  x$bnstreet <- gsub(" ", "", x$bnstreet)
  x$addressf <-paste(x$postcodelocator,x$bnstreet,sep=",")
  
  y<-y[y$property_type=="Flat"|y$property_type=="Maisonette",]
  y$addressfinal <-  paste(y$add1,y$add3,sep=",")
  #y$addressfinal <- gsub("[/]", "", y$addressfinal)
  y$addressfinal <- gsub("[.]", "", y$addressfinal)
  y$addressfinal <- gsub("[']", "", y$addressfinal)
  y$addressfinal <- gsub(" ", "", y$addressfinal)
  y$addressf <- paste(y$postcode,y$addressfinal,sep=",")
  
  taba1 <- inner_join(x,y,by="addressf")
  return(taba1)
  
  
}

link202<-function202(add,epc)

link202<-link202[,..needlist1]


link202u<- uniqueresult(link202)
dim(link202u)


link202d <- doubleresult(link202)
dim(link202d)


dim(epc)
#
epc <- matchleft(epc,link202)
dim(epc)
# 1391438      27



#################### method 203 ##################

function203<- function(x,y){
  x<-x[x$paostartsuffix=="",]
  x$bnstreet <-    paste(x$subbuildingname,x$paostartnumber,sep=",")
  x$bnstreet <-    paste(x$bnstreet,x$streetdescription,sep=" ")
  x$bnstreet <- gsub("/", "", x$bnstreet)
  x$bnstreet <- gsub("[.]", "", x$bnstreet)
  x$bnstreet <- gsub("[']", "", x$bnstreet)
  x$bnstreet <- gsub(" ", "", x$bnstreet)
  x$addressf <-paste(x$postcodelocator,x$bnstreet,sep=",")
  
  y<-y[y$property_type=="Flat",]
  y$addressfinal <- gsub("APARTMENT", "FLAT", y$add1)
  y$addressfinal <-  paste(y$addressfinal,y$add3,sep=",")
  y$addressfinal <- gsub(" ", "", y$addressfinal)
  y$addressfinal <- gsub("[/]", "", y$addressfinal)
  y$addressfinal <- gsub("[.]", "", y$addressfinal)
  y$addressfinal <- gsub("[']", "", y$addressfinal)
  y$addressf <- paste(y$postcode,y$addressfinal,sep=",")
  
  taba1 <- inner_join(x,y,by="addressf")
  return(taba1)
  
  
  
}



link203<-function203(add,epc)
dim(link203)
# 
link203<-link203[,..needlist1]

link203u<- uniqueresult(link203)
dim(link203u)
#

link203d <- doubleresult(link203)
dim(link203d)


dim(epc)
#
epc <- matchleft(epc,link203)
dim(epc)
# 1389037      27


#################### method 204  ##################

function204<- function(x,y){
  
  x$bnstreet <-    paste(x$subbuildingname,x$buildingname ,sep=" ")
  x$bnstreet <- gsub("[']", "", x$bnstreet)
  x$bnstreet <- gsub(" ", "", x$bnstreet)
  x$addressf <-paste(x$postcodelocator,x$bnstreet,sep=",")
  
  
  y$addressfinal <- beg2char(y$add2, ",")
  
  y$addressfinal <- paste( y$add1, y$addressfinal,sep=" ")
  y$addressfinal <- gsub("[/]", "", y$addressfinal)
  y$addressfinal <- gsub(" ", "", y$addressfinal)
  y$addressf <- paste(y$postcode,y$addressfinal,sep=",")
  
  taba1 <- inner_join(x,y,by="addressf")
  return(taba1)
  
  
}


link204<-function204(add,epc)



link204<-link204[,..needlist1]


link204u<- uniqueresult(link204)
dim(link204u)


link204d <- doubleresult(link204)


dim(epc)
# 
epc<- matchleft(epc,link204)
dim(epc)
#   1388383      27
#################### method 205 ##################


function205<- function(x,y){
  
  x <- x[grepl("FLAT\\s[A-Z]$",x$saotext),]
  x<-x[x$paostartsuffix=="",]
  x$add1ff <- gsub("FLAT ", "", x$saotext)
  x$bnstreet <-    paste(x$paostartnumber,x$add1ff,sep="")
  x$bnstreet <-    paste(x$bnstreet,x$streetdescription,sep=" ")
  #x$bnstreet <- gsub("[']", "", x$bnstreet)
  #x$bnstreet <- gsub("/", "", x$bnstreet)
  x$bnstreet <- gsub("[.]", "", x$bnstreet)
  x$bnstreet <- gsub("[']", "", x$bnstreet)
  x$bnstreet <- gsub(" ", "", x$bnstreet)
  x$addressf <-paste(x$postcodelocator,x$bnstreet,sep=",")
  
  
  y$addressfinal <- y$add
  #y$addressfinal <- gsub("[/]", "", y$addressfinal)
  y$addressfinal <- gsub("[.]", "", y$addressfinal)
  y$addressfinal <- gsub("[']", "", y$addressfinal)
  
  y$addressfinal <- gsub(" ", "", y$addressfinal)
  y$addressf <- paste(y$postcode,y$addressfinal,sep=",")
  
  taba1 <- inner_join(x,y,by="addressf")
  return(taba1)
  
}



link205<-function205(add,epc)


link205<-link205[,..needlist1]
length(unique(link205$lmk_key))

link205u<- uniqueresult(link205)
dim(link205u)


link205d <- doubleresult(link205)
dim(link205d)


dim(epc)
#
epc <- matchleft(epc,link205)
dim(epc)
# 1352809      27

#
#################### method 206 ##################

function206<- function(x,y){
  
  x$bnstreet <-    paste(x$saotext,x$pp,sep=", ")
  
  x$bnstreet <-    paste(x$bnstreet,x$streetdescription,sep=" ")
  x$addressf <-paste(x$postcodelocator,x$bnstreet,sep=",")
  
  y<-y[y$property_type=="Flat"|y$property_type=="Maisonette",]
  y$addressfinal <-  paste("ROOM ",y$add1,sep="")
  y$addressfinal <-  paste(y$addressfinal,y$add3,sep=", ")
  y$addressf <- paste(y$postcode,y$addressfinal,sep=",")
  
  taba1 <- inner_join(x,y,by="addressf")
  return(taba1)
}




link206<-function206(add,epc)
dim(link206)
# 

link206<-link206[,..needlist1]



link206u<- uniqueresult(link206)
dim(link206u)
# 

link206d <- doubleresult(link206)
dim(link206d)


epc <- matchleft(epc,link206)
dim(epc)
#  1352799      27

#################### method 207 ##################



function207<- function(x,y){
  
  
  x$bnstreet <-    paste(x$subbuildingname,x$buildingname,sep=" ")
  x$bnstreet <-    paste(x$bnstreet,x$streetdescription,sep=",")
  x$bnstreet <- gsub("/", "", x$bnstreet)
  x$bnstreet <- gsub("[.]", "", x$bnstreet)
  x$bnstreet <- gsub("[']", "", x$bnstreet)
  x$bnstreet <- gsub(" ", "", x$bnstreet)
  x$addressf <-paste(x$postcodelocator,x$bnstreet,sep=",")
  
  y<-y[y$property_type=="Flat"|y$property_type=="Maisonette",]
  y$addressfinal <-  paste("APARTMENT ",y$add,sep="")
  y$addressfinal <- gsub("[/]", "", y$addressfinal)
  y$addressfinal <- gsub("[.]", "", y$addressfinal)
  y$addressfinal <- gsub("[']", "", y$addressfinal)
  y$addressfinal <- gsub(" ", "", y$addressfinal)
  y$addressf <- paste(y$postcode,y$addressfinal,sep=",")
  
  taba1 <- inner_join(x,y,by="addressf")
  return(taba1)
  
  
  
  
  
}

link207<-function207(add,epc)
dim(link207)


link207<-link207[,..needlist1]

link207u<- uniqueresult(link207)
dim(link207u)


link207d <- doubleresult(link207)
dim(link207d)


dim(epc)
# 
epc <- matchleft(epc,link207)
dim(epc)
#1351685      27
#
#################### method 208 ##################

function208<- function(x,y){
  
  x$bnstreet <-    paste(x$subbuildingname,x$buildingname,sep=" ")
  x$bnstreet <-    paste(x$bnstreet,x$streetdescription,sep=",")
  x$bnstreet <- gsub("[.]", "", x$bnstreet)
  #x$bnstreet <- gsub("[']", "", x$bnstreet)
  x$bnstreet <- gsub(" ", "", x$bnstreet)
  x$addressf <-paste(x$postcodelocator,x$bnstreet,sep=",")
  
  y<-y[y$property_type=="Flat"|y$property_type=="Maisonette",]
  y$addressfinal <-  paste("APARTMENT ",y$add1,sep="")
  y$addressfinal <-  paste(y$addressfinal,y$add3,sep=",")
  y$addressfinal <- gsub("[.]", "", y$addressfinal)
  #y$addressfinal <- gsub("[']", "", y$addressfinal)
  y$addressfinal <- gsub(" ", "", y$addressfinal)
  y$addressf <- paste(y$postcode,y$addressfinal,sep=",")
  
  taba1 <- inner_join(x,y,by="addressf")
  return(taba1) 
}



link208<-function208(add,epc)
dim(link208)


link208<-link208[,..needlist1]



link208u<- uniqueresult(link208)
dim(link208u)


link208d <- doubleresult(link208)
dim(link208d)


dim(epc)
# 
epc <- matchleft(epc,link208)
dim(epc)
#1351518      27
#################### method 209 ##################
function209<- function(x,y){
  
  x$bnstreet <-    paste(x$saotext,x$paotext,sep=",")
  x$bnstreet <-    paste(x$bnstreet,x$paostartnumber ,sep=",")
  x$bnstreet <-    paste(x$bnstreet,x$paostartsuffix ,sep="")
  x$bnstreet <- str_replace_all(x$bnstreet, "NA", "")
  x$bnstreet <-    paste(x$bnstreet,x$streetdescription,sep=" ")
 # x$bnstreet <- gsub("[']", "", x$bnstreet)
  x$bnstreet <- gsub("[']", "", x$bnstreet)
  x$bnstreet <- gsub(" ", "", x$bnstreet)
  x$addressf <-paste(x$postcodelocator,x$bnstreet,sep=",")
  
  y<-y[y$property_type=="Flat"| y$property_type=="Maisonette",]
  y$addressfinal <- paste("APARTMENT ",y$add,sep=" ")
  #y$addressfinal <- gsub("[.]", "", y$addressfinal)
  # y$addressfinal <- gsub("[']", "", y$addressfinal)
  y$addressfinal <- gsub("[.]", "", y$addressfinal)
  y$addressfinal <- gsub("[']", "", y$addressfinal)
  y$addressfinal <- gsub(" ", "", y$addressfinal)
  y$addressf <- paste(y$postcode,y$addressfinal,sep=",")
  
  taba1 <- inner_join(x,y,by="addressf")
  return(taba1)
}


link209<-function209(add,epc)

link209<-link209[,..needlist1]


link209u<- uniqueresult(link209)
dim(link209u)


link209d <- doubleresult(link209)
dim(link209d)

dim(epc)
#
epc <- matchleft(epc,link209)
dim(epc)
# 1350901      27



#################### method 210 ##################

function210<- function(x,y){
  
  
  x$bnstreet <-    paste(x$subbuildingname,x$buildingname,sep=",")
  x$bnstreet <-    paste(x$bnstreet,x$streetdescription,sep=",")
  #x$bnstreet <- gsub("/", "", x$bnstreet)
  x$bnstreet <- gsub("[']", "", x$bnstreet)
  x$bnstreet <- gsub("[.]", "", x$bnstreet)
  x$bnstreet <- gsub(" ", "", x$bnstreet)
  x$addressf <-paste(x$postcodelocator,x$bnstreet,sep=",")
  
  y<-y[y$property_type=="Flat"| y$property_type=="Maisonette",]
  y$addressfinal <-  gsub("FLAT", "APARTMENT", y$add) 
  y$addressfinal <- gsub("[.]", "", y$addressfinal)
  y$addressfinal <- gsub("[']", "", y$addressfinal)
  #y$addressfinal <- gsub("[/]", "", y$addressfinal)
  #y$addressfinal <- gsub("[.]", "", y$addressfinal)
  y$addressfinal <- gsub(" ", "", y$addressfinal)
  y$addressf <- paste(y$postcode,y$addressfinal,sep=",")
  
  taba1 <- inner_join(x,y,by="addressf")
  return(taba1)
  
}


link210<-function210(add,epc)

length(unique(link210$lmk_key))
link210<-link210[,..needlist1]


link210u<- uniqueresult(link210)
dim(link210u)


link210d <- doubleresult(link210)
dim(link210d)

dim(epc)
#
epc <- matchleft(epc,link210)
dim(epc)
#1350116      27

#################### method 211 ##################

function211<- function(x,y){
  
  x$bnstreet <-    paste(x$saotext,x$paotext,sep="")
  
  x$bnstreet <-    paste(x$bnstreet,x$streetdescription,sep=",")
  x$bnstreet <- gsub("[']", "", x$bnstreet)
  x$bnstreet <- gsub("[.]", "", x$bnstreet)
  x$bnstreet <- gsub(" ", "", x$bnstreet)
  x$addressf <-paste(x$postcodelocator,x$bnstreet,sep=",")
  
  y<-y[y$property_type=="Flat"| y$property_type=="Maisonette",]
  y$addressfinal <- paste("FLAT ",y$add) 
  y$addressfinal <- gsub("[.]", "", y$addressfinal)
  y$addressfinal <- gsub("[']", "", y$addressfinal)
  y$addressfinal <- gsub(" ", "", y$addressfinal)
  y$addressf <- paste(y$postcode,y$addressfinal,sep=",")
  
  taba1 <- inner_join(x,y,by="addressf")
  return(taba1)
  
  
}


link211<-function211(add,epc)


link211<-link211[,..needlist1]


link211u<- uniqueresult(link211)
dim(link211u)


link211d <- doubleresult(link211)
dim(link211d)

dim(epc)
#
epc <- matchleft(epc,link211)
dim(epc)
#2278669      27


#################### method 212 ##################


function212<- function(x,y){
  
  #change
  x$bnstreet <-    paste(x$saotext,x$pp,sep=",")
  x$bnstreet <-    paste(x$bnstreet,x$streetdescription,sep=",")
  # x$bnstreet <- gsub("/", "", x$bnstreet)
  # x$bnstreet <- gsub("[']", "", x$bnstreet)
  # x$bnstreet <- gsub("[']", "", x$bnstreet)
  x$bnstreet <- gsub(" ", "", x$bnstreet)
  x$addressf <-paste(x$postcodelocator,x$bnstreet,sep=",")
  
  
  y<-y[y$property_type=="Flat"| y$property_type=="Maisonette",]
  y$addressfinal <-  gsub("FLAT", "APARTMENT", y$add) 
  # y$addressfinal <- gsub("/", "", y$addressfinal)
  # y$addressfinal <- gsub("[.]", "", y$addressfinal)
  # y$addressfinal <- gsub("[']", "", y$addressfinal)
  y$addressfinal <- gsub(" ", "", y$addressfinal)
  y$addressf <- paste(y$postcode,y$addressfinal,sep=",")
  
  taba1 <- inner_join(x,y,by="addressf")
  return(taba1)
  
}

link212<-function212(add,epc)

link212<-link212[,..needlist1]


link212u<- uniqueresult(link212)
dim(link212u)


link212d <- doubleresult(link212)
dim(link212d)


dim(epc)
#
epc <- matchleft(epc,link212)
dim(epc)
# 1346438      27



#################### method 213 change##################

function213<- function(x,y){
  
  x$bnstreet <-    paste(x$saotext,x$pp,sep=",")
  #x$bnstreet <-    paste(x$bnstreet,x$paostartsuffix,sep="")
  #x$bnstreet <- str_replace_all(x$bnstreet, "NA", "")
  x$bnstreet <-    paste(x$bnstreet,x$streetdescription,sep=" ")
  x$bnstreet <- gsub(" ", "", x$bnstreet)
  x$addressf <-paste(x$postcodelocator,x$bnstreet,sep=",")
  
  y<-y[y$property_type=="Flat"| y$property_type=="Maisonette",]
  setDF(y)
  y[y$postcode=="N4 2DE","add"] <- gsub("F0", "F", y[y$postcode=="N4 2DE","add"])
  y[y$postcode=="N4 2DE","add"] <- gsub("G0", "G", y[y$postcode=="N4 2DE","add"])
  y[y$postcode=="N4 2DE","add"] <- gsub("IVY HOUSE, 279", " 279", y[y$postcode=="N4 2DE","add"])
  
  y$addressfinal <- paste("ROOM ",y$add,sep="")
  y$addressfinal <- gsub(" ", "", y$addressfinal)
  y$addressf <- paste(y$postcode,y$addressfinal,sep=",")
  
  taba1 <- inner_join(x,y,by="addressf")
  return(taba1)
  
}



link213<-function213(add,epc)
dim(link213)
# 
link213<-link213[,..needlist1]

link213u<- uniqueresult(link213)
dim(link213u)
#

link213d <- doubleresult(link213)
dim(link213d)


dim(epc)
#
epc <- matchleft(epc,link213)
dim(epc)
# 21346257      27


#################### method 214  ##################

function214<- function(x,y){
  
  #x<-x[x$paostartsuffix=="",]
  x$bnstreet <-    paste(x$saotext,x$pp,sep=",")
  x$bnstreet <-    paste(x$bnstreet,x$streetdescription,sep=" ")
  x$bnstreet <- gsub("/", "", x$bnstreet)
  x$bnstreet <- gsub("[.]", "", x$bnstreet)
  x$bnstreet <- gsub("[']", "", x$bnstreet)
  x$bnstreet <- gsub(" ", "", x$bnstreet)
  x$addressf <-paste(x$postcodelocator,x$bnstreet,sep=",")
  
  
  y<-y[y$property_type=="Flat"| y$property_type=="Maisonette",]
  y$addressfinal <- beg2char(y$add1, " ")
  y$addressfinal <-  paste("APARTMENT ",y$addressfinal,sep=" ")
  y$addressfinal <- paste( y$addressfinal,y$add2,sep=",")
  
  y$addressfinal <- gsub("[/]", "", y$addressfinal)
  y$addressfinal <- gsub("[.]", "", y$addressfinal)
  y$addressfinal <- gsub("[']", "", y$addressfinal)
  
  y$addressfinal <- gsub(" ", "", y$addressfinal)
  y$addressf <- paste(y$postcode,y$addressfinal,sep=",")
  
  taba1 <- inner_join(x,y,by="addressf")
  return(taba1)
  
}


link214<-function214(add,epc)

length(unique(link214$lmk_key))

link214<-link214[,..needlist1]


link214u<- uniqueresult(link214)
dim(link214u)


link214d <- doubleresult(link214)


dim(epc)
# 
epc<- matchleft(epc,link214)
dim(epc)
#  2272664      27
#################### method 215 ##################


function215<- function(x,y){
  
  
  x$bnstreet <-    paste(x$saotext,x$paotext,sep=" ")
  
  x$bnstreet <-    paste(x$bnstreet,x$streetdescription,sep=",")
  #x$bnstreet <- gsub("/", "", x$bnstreet)
  x$bnstreet <- gsub("[.]", "", x$bnstreet)
  x$bnstreet <- gsub("[']", "", x$bnstreet)
  x$bnstreet <- gsub(" ", "", x$bnstreet)
  x$addressf <-paste(x$postcodelocator,x$bnstreet,sep=",")
  
  y<-y[y$property_type=="Flat"| y$property_type=="Maisonette",]
  y$addressfinal <- paste("APARTMENT ",y$add1,sep="")
  y$addressfinal <- paste(y$addressfinal,y$add3,sep=",")
  # y$addressfinal <- gsub("[/]", "", y$addressfinal)
  y$addressfinal <- gsub("[.]", "", y$addressfinal)
  y$addressfinal <- gsub("[']", "", y$addressfinal)
  
  y$addressfinal <- gsub(" ", "", y$addressfinal)
  y$addressf <- paste(y$postcode,y$addressfinal,sep=",")
  
  taba1 <- inner_join(x,y,by="addressf")
  return(taba1)
  
}



link215<-function215(add,epc)


link215<-link215[,..needlist1]


link215u<- uniqueresult(link215)
dim(link215u)


link215d <- doubleresult(link215)
dim(link215d)


dim(epc)
#
epc <- matchleft(epc,link215)
dim(epc)
# 1340225      27

#
#################### method 216 ##################

function216<- function(x,y){
  
  x$bnstreet <-    paste(x$subbuildingname,x$buildingnumber,sep=",")
  
  x$bnstreet <-    paste(x$bnstreet,x$streetdescription,sep=" ")
  x$bnstreet <- gsub("[.]", "", x$bnstreet)
  x$bnstreet <- gsub("[']", "", x$bnstreet)
  #x$bnstreet <- gsub(" ", "", x$bnstreet)
  x$bnstreet <- gsub(" ", "", x$bnstreet)
  x$addressf <-paste(x$postcodelocator,x$bnstreet,sep=",")
  
  
  y<-y[y$property_type=="Flat",]
  y$addressfinal <-  gsub( "APARTMENT","FLAT", y$add) 
  y$addressfinal <- gsub("[/]", "", y$addressfinal)
  y$addressfinal <- gsub("[.]", "", y$addressfinal)
  y$addressfinal <- gsub("[']", "", y$addressfinal)
  y$addressfinal <- gsub(" ", "", y$addressfinal)
  y$addressf <- paste(y$postcode,y$addressfinal,sep=",")
  
  taba1 <- inner_join(x,y,by="addressf")
  return(taba1)
}




link216<-function216(add,epc)
dim(link216)
# 

link216<-link216[,..needlist1]



link216u<- uniqueresult(link216)
dim(link216u)
# 

link216d <- doubleresult(link216)
dim(link216d)


epc <- matchleft(epc,link216)
dim(epc)
#  1338876      27

#################### method 217 ##################



function217<- function(x,y){
  
  
  x$bnstreet <-    paste("FLAT ",x$buildingnumber,sep="")
  x$bnstreet <-    paste(x$bnstreet,x$streetdescription,sep=",")
  x$bnstreet <- gsub("[.]", "", x$bnstreet)
  x$bnstreet <- gsub("[']", "", x$bnstreet)
  x$bnstreet <- gsub("/", "", x$bnstreet)
  #x$bnstreet <- gsub("[']", "", x$bnstreet)
  x$bnstreet <- gsub(" ", "", x$bnstreet)
  x$addressf <-paste(x$postcodelocator,x$bnstreet,sep=",")
  
  y<-y[y$property_type=="Flat"|y$property_type=="Maisonette",]
  y$addressfinal <-  trimws(y$add)
  
  y$addressfinal <- gsub("[/]", "", y$addressfinal)
  y$addressfinal <- gsub("[.]", "", y$addressfinal)
  y$addressfinal <- gsub("[']", "", y$addressfinal)
  
  y$addressfinal <- gsub(" ", "", y$addressfinal)
  y$addressf <- paste(y$postcode,y$addressfinal,sep=",")
  
  taba1 <- inner_join(x,y,by="addressf")
  return(taba1)
  
  
}

link217<-function217(add,epc)
dim(link217)


link217<-link217[,..needlist1]

link217u<- uniqueresult(link217)
dim(link217u)


link217d <- doubleresult(link217)
dim(link217d)


dim(epc)
# 
epc <- matchleft(epc,link217)
dim(epc)
#1338117      27

#################### method 218  ##################

function218<- function(x,y){
  x<-x[x$subbuildingname!="",]
  
  x$bnstreet <-    paste(x$subbuildingname,x$streetdescription,sep=",")
  x$bnstreet <- gsub("[.]", "", x$bnstreet)
  x$bnstreet <- gsub("[']", "", x$bnstreet)
  x$bnstreet <- gsub("/", "", x$bnstreet)
  x$bnstreet <- gsub(" ", "", x$bnstreet)
  x$addressf <-paste(x$postcodelocator,x$bnstreet,sep=",")
  
  y<-y[y$property_type=="Flat"|y$property_type=="Maisonette",]
  y$addressfinal <-  paste("FLAT ",y$add,sep=" ")
  
  y$addressfinal <- gsub("[/]", "", y$addressfinal)
  y$addressfinal <- gsub("[.]", "", y$addressfinal)
  y$addressfinal <- gsub("[']", "", y$addressfinal)
  y$addressfinal <- gsub(" ", "", y$addressfinal)
  y$addressf <- paste(y$postcode,y$addressfinal,sep=",")
  
  taba1 <- inner_join(x,y,by="addressf")
  return(taba1)
  
}



link218<-function218(add,epc)
dim(link218)
length(unique(link218$lmk_key))
link218<-link218[,..needlist1]



link218u<- uniqueresult(link218)
dim(link218u)


link218d <- doubleresult(link218)
dim(link218d)


dim(epc)
# 
epc <- matchleft(epc,link218)
dim(epc)
#2269509      27
#################### method 219 ##################
function219<- function(x,y){
  x<-x[x$saotext!="",]
  
  x$bnstreet <-    paste(x$saotext,x$streetdescription,sep=",")
  x$bnstreet <- gsub("[.]", "", x$bnstreet)
  x$bnstreet <- gsub("[']", "", x$bnstreet)
  x$bnstreet <- gsub("/", "", x$bnstreet)
  x$bnstreet <- gsub(" ", "", x$bnstreet)
  x$addressf <-paste(x$postcodelocator,x$bnstreet,sep=",")
  
  y<-y[y$property_type=="Flat"|y$property_type=="Maisonette",]
  y$addressfinal <-  paste("FLAT ",y$add,sep=" ")
  
  y$addressfinal <- gsub("[/]", "", y$addressfinal)
  y$addressfinal <- gsub("[.]", "", y$addressfinal)
  y$addressfinal <- gsub("[']", "", y$addressfinal)
  y$addressfinal <- gsub(" ", "", y$addressfinal)
  y$addressf <- paste(y$postcode,y$addressfinal,sep=",")
  
  taba1 <- inner_join(x,y,by="addressf")
  return(taba1)
  
}


link219<-function219(add,epc)

link219<-link219[,..needlist1]


link219u<- uniqueresult(link219)
dim(link219u)


link219d <- doubleresult(link219)
dim(link219d)

dim(epc)
#2
epc <- matchleft(epc,link219)
dim(epc)
#1337221      27


#################### method 220 ##################

function220<- function(x,y){
  
  x$bnstreet <-    paste(x$subbuildingname,x$buildingnumber,sep=",")
  x$bnstreet <-    paste(x$bnstreet,x$streetdescription,sep=" ")
  x$bnstreet <- gsub("[.]", "", x$bnstreet)
  x$bnstreet <- gsub("[']", "", x$bnstreet)
  x$bnstreet <- gsub("/", "", x$bnstreet)
  x$bnstreet <- gsub(" ", "", x$bnstreet)
  x$addressf <-paste(x$postcodelocator,x$bnstreet,sep=",")
  
  y<-y[y$property_type=="Flat"|y$property_type=="Maisonette",]
  y$addressfinal <-  paste("APARTMENT ",y$add,sep="")
  y$addressfinal <- gsub("[/]", "", y$addressfinal)
  y$addressfinal <- gsub("[.]", "", y$addressfinal)
  y$addressfinal <- gsub("[']", "", y$addressfinal)
  y$addressfinal <- gsub(" ", "", y$addressfinal)
  y$addressf <- paste(y$postcode,y$addressfinal,sep=",")
  
  taba1 <- inner_join(x,y,by="addressf")
  return(taba1)
  
  
}


link220<-function220(add,epc)

length(unique(link220$lmk_key))
link220<-link220[,..needlist1]


link220u<- uniqueresult(link220)
dim(link220u)


link220d <- doubleresult(link220)
dim(link220d)

dim(epc)
#
epc <- matchleft(epc,link220)
dim(epc)
#1336186      27
#################### method 221 ##################

function221<- function(x,y){
  x<-x[x$subbuildingname!="",]
  x$bnstreet <-    paste(x$subbuildingname,x$streetdescription,sep=",")
  
  x$bnstreet <- gsub(" ", "", x$bnstreet)
  x$addressf <-paste(x$postcodelocator,x$bnstreet,sep=",")
  
  y$addressfinal <- trimws(y$add)
  y$addressfinal <- gsub(" ", "", y$addressfinal)
  y$addressf <- paste(y$postcode,y$addressfinal,sep=",")
  
  taba1 <- inner_join(x,y,by="addressf")
  
  return(taba1)
  
}


link221<-function221(add,epc)


link221<-link221[,..needlist1]


link221u<- uniqueresult(link221)
dim(link221u)


link221d <- doubleresult(link221)
dim(link221d)

dim(epc)
#
epc <- matchleft(epc,link221)
dim(epc)
# 1336186      27


#################### method 222 ##################


function222<- function(x,y){
  
  #x<-x[x$saostartsuffix=="",]
  x$bnstreet <-    paste(x$ss,x$paotext,sep=",")
  x$bnstreet <-    paste(x$bnstreet,x$paostartnumber,sep=",")
  x$bnstreet <-    paste(x$bnstreet,x$paoendnumber,sep="-")
  x$bnstreet <-    paste(x$bnstreet,x$streetdescription,sep=" ")
  x$bnstreet <- gsub("/", "", x$bnstreet)
  x$bnstreet <- gsub("[.]", "", x$bnstreet)
  x$bnstreet <- gsub("[']", "", x$bnstreet)
  x$bnstreet <- gsub(" ", "", x$bnstreet)
  x$addressf <-paste(x$postcodelocator,x$bnstreet,sep=",")
  
  
  y<-y[y$property_type=="Flat",]
  y$addressfinal <-  gsub( "FLAT ","", y$add) 
  y$addressfinal <- gsub("[/]", "", y$addressfinal)
  y$addressfinal <- gsub("[.]", "", y$addressfinal)
  y$addressfinal <- gsub("[']", "", y$addressfinal)
  y$addressfinal <- gsub(" ", "", y$addressfinal)
  y$addressf <- paste(y$postcode,y$addressfinal,sep=",")
  
  taba1 <- inner_join(x,y,by="addressf")
  return(taba1)
  
  
}

link222<-function222(add,epc)

link222<-link222[,..needlist1]

length(unique(link222$lmk_key))

link222u<- uniqueresult(link222)
dim(link222u)


link222d <- doubleresult(link222)
dim(link222d)


dim(epc)
#
epc <- matchleft(epc,link222)
dim(epc)
#1334985      27



#################### method 223 ##################

function223<- function(x,y){
  x$bnstreet <-    paste(x$saotext,x$paotext,sep=",")
  
  x$bnstreet <- gsub("/", "", x$bnstreet)
  x$bnstreet <- gsub("[.]", "", x$bnstreet)
  x$bnstreet <- gsub("[']", "", x$bnstreet)
  x$bnstreet <- gsub(" ", "", x$bnstreet)
  x$addressf <-paste(x$postcodelocator,x$bnstreet,sep=",")
  
  y<-y[y$property_type=="Flat",]
  setDF(y)
  y[y$postcode=="NN5 4RD","add"] <- gsub(", ST. CRISPIN RETIREMENT VILLAGE, ST. CRISPIN DRIVE", ",ST CRISPIN RETIREMENT VILLAGE", y[y$postcode=="NN5 4RD","add"])
  y[y$postcode=="NN5 4RA","add"] <- gsub(", ST. CRISPIN RETIREMENT VILLAGE, ST. CRISPIN DRIVE", ",ST CRISPIN RETIREMENT VILLAGE", y[y$postcode=="NN5 4RA","add"])
  y$addressfinal <- paste("APARTMENT ",y$add,sep="")
  #y$addressfinal <- gsub("[.]", "", y$addressfinal)
  y$addressfinal <- gsub("[/]", "", y$addressfinal)
  y$addressfinal <- gsub("[.]", "", y$addressfinal)
  y$addressfinal <- gsub("[']", "", y$addressfinal)
  y$addressfinal <- gsub(" ", "", y$addressfinal)
  y$addressf <- paste(y$postcode,y$addressfinal,sep=",")
  
  taba1 <- inner_join(x,y,by="addressf")
  return(taba1)
  
  
}



link223<-function223(add,epc)
dim(link223)
# 
link223<-link223[,..needlist1]

link223u<- uniqueresult(link223)
dim(link223u)
#

link223d <- doubleresult(link223)
dim(link223d)


dim(epc)
#
epc <- matchleft(epc,link223)
dim(epc)
#  1334758      27


#################### method 224  ##################

function224<- function(x,y){
  #x<-x[x$paostartsuffix=="",]
  x$bnstreet <-    paste(x$pp,x$streetdescription,sep=",")
  x$bnstreet <- gsub("/", "", x$bnstreet)
  x$bnstreet <- gsub("[.]", "", x$bnstreet)
  x$bnstreet <- gsub("[']", "", x$bnstreet)
  x$bnstreet <- gsub(" ", "", x$bnstreet)
  x$addressf <-paste(x$postcodelocator,x$bnstreet,sep=",")
  
  y<-y[y$property_type=="Flat" | y$property_type=="Maisonette",]
  y$addressfinal <-  gsub( "FLAT ","", y$add) 
  
  y$addressfinal <- gsub("[/]", "", y$addressfinal)
  y$addressfinal <- gsub("[.]", "", y$addressfinal)
  y$addressfinal <- gsub("[']", "", y$addressfinal)
  y$addressfinal <- gsub(" ", "", y$addressfinal)
  y$addressf <- paste(y$postcode,y$addressfinal,sep=",")
  
  taba1 <- inner_join(x,y,by="addressf")
  return(taba1)
  
  
}


link224<-function224(add,epc)



link224<-link224[,..needlist1]


link224u<- uniqueresult(link224)
dim(link224u)


link224d <- doubleresult(link224)

dim(epc)
# 
epc<- matchleft(epc,link224)
dim(epc)
# 1334826      27
#################### method 225 ##################


function225<- function(x,y){
  
  # x<-x[x$paostartsuffix=="",]
  # x<-x[x$saoendnumber=="",]
  x$bnstreet <-    paste("APARTMENT ",x$ss,sep=" ")
  x$bnstreet <-    paste(x$bnstreet,x$paotext,sep=",")
  x$bnstreet <-    paste(x$bnstreet,x$pp,sep=",")
  x$bnstreet <-    paste(x$bnstreet,x$streetdescription,sep=" ")
  #x$bnstreet <- gsub("/", "", x$bnstreet)
  x$bnstreet <- gsub("[.]", "", x$bnstreet)
  x$bnstreet <- gsub("[']", "", x$bnstreet)
  x$bnstreet <- gsub(" ", "", x$bnstreet)
  x$addressf <-paste(x$postcodelocator,x$bnstreet,sep=",")
  
  
  y$addressfinal <- trimws(y$add)
  # y$addressfinal <- gsub("[/]", "", y$addressfinal)
  y$addressfinal <- gsub("[.]", "", y$addressfinal)
  y$addressfinal <- gsub("[']", "", y$addressfinal)
  #y$addressfinal <- gsub("[']", "", y$addressfinal)
  y$addressfinal <- gsub(" ", "", y$addressfinal)
  y$addressf <- paste(y$postcode,y$addressfinal,sep=",")
  
  taba1 <- inner_join(x,y,by="addressf")
  return(taba1)
  
}



link225<-function225(add,epc)


link225<-link225[,..needlist1]


link225u<- uniqueresult(link225)
dim(link225u)


link225d <- doubleresult(link225)
dim(link225d)


dim(epc)
#
epc <- matchleft(epc,link225)
dim(epc)
#2270304      27

#wait
#################### method 226 ##################

function226<- function(x,y){
  x <- x[!grepl("^\\d",x$buildingname),]
  x$bnstreet <-    paste(x$subbuildingname,x$buildingname,sep=" ")
  x$bnstreet <-    paste(x$bnstreet,x$streetdescription,sep=" ")
  x$bnstreet <- gsub(",", "", x$bnstreet)
  x$bnstreet <- gsub(" ", "", x$bnstreet)
  x$addressf <-paste(x$postcodelocator,x$bnstreet,sep=",")
  
  y<-y[y$property_type=="Flat"|y$property_type=="Maisonette",]
  y$addressfinal <-  paste("FLAT ",y$add,sep="")
  
  y$addressfinal <- gsub(",", "", y$addressfinal)
  y$addressfinal <- gsub(" ", "", y$addressfinal)
  y$addressf <- paste(y$postcode,y$addressfinal,sep=",")
  
  taba1 <- inner_join(x,y,by="addressf")
  return(taba1)
  
}




link226<-function226(add,epc)
dim(link226)
# 

link226<-link226[,..needlist1]



link226u<- uniqueresult(link226)
dim(link226u)
# 

link226d <- doubleresult(link226)
dim(link226d)


epc <- matchleft(epc,link226)
dim(epc)
# 1329330      27

#################### method 227 ##################

function227<- function(x,y){
  
  
  x$bnstreet <- paste("FLAT ",x$subbuildingname,sep=" ")
  x$bnstreet <- paste(x$bnstreet,x$paotext,sep=",")
  x$bnstreet <-  paste(x$bnstreet,x$buildingnumber,sep=",")
  x$bnstreet <- paste(x$bnstreet,x$streetdescription,sep=" ")
  x$bnstreet <- gsub(",", "", x$bnstreet)
  #x$bnstreet <- gsub("[']", "", x$bnstreet)
  x$bnstreet <- gsub(" ", "", x$bnstreet)
  x$addressf <-paste(x$postcodelocator,x$bnstreet,sep=",")
  
  y<-y[y$property_type=="Flat"|y$property_type=="Maisonette",]
  #add1 is not end with number 
  
  y <- y[!grepl("\\d$",y$add1),]
  y$addressfinal <-  trimws(y$add)
  
  y$addressfinal <- gsub(",", "", y$addressfinal)
  #y$addressfinal <- gsub("[']", "", y$addressfinal)
  y$addressfinal <- gsub(" ", "", y$addressfinal)
  y$addressf <- paste(y$postcode,y$addressfinal,sep=",")
  
  taba1 <- inner_join(x,y,by="addressf")
  return(taba1)
  
}

link227<-function227(add,epc)
dim(link227)


link227<-link227[,..needlist1]

link227u<- uniqueresult(link227)
dim(link227u)


link227d <- doubleresult(link227)
dim(link227d)


dim(epc)
# 
epc <- matchleft(epc,link227)
dim(epc)
# 1326466      27
#
#################### method 228 ##################

function228<- function(x,y){
  
  x1<-x[postcodelocator=="M3 6FZ",]
  setDT(x1)
  
  x1$bnstreet <- gsub("APARTMENT ", "", x1$saotext)
  x1$bnstreet <-    paste(x1$buildingname, x1$bnstreet,sep="")
  
  x1$bnstreet1<-   word(x1$bnstreet , 1  , -2)
  x1$bnstreet2<-    word(x1$bnstreet,-1)
  
  x1$bnstreet3 <-    paste(x1$bnstreet2,x1$bnstreet1,sep=" ")
  x1$addressf <-paste(x1$postcodelocator,x1$bnstreet3,sep=",")
  
  y<-y[y$property_type=="Flat"|y$property_type=="Maisonette",]
  y$addressfinal <- beg2char(y$add, ",")  
  #y$addressfinal <- gsub(" ", "", y$addressfinal)
  y$addressf <- paste(y$postcode,y$addressfinal,sep=",")
  taba1 <- inner_join(x1,y,by="addressf")
  return(taba1)
}



link228<-function228(add,epc)
dim(link228)


link228<-link228[,..needlist1]



link228u<- uniqueresult(link228)
dim(link228u)


link228d <- doubleresult(link228)
dim(link228d)


dim(epc)
# 
epc <- matchleft(epc,link228)
dim(epc)
#1326390      27
#################### method 229 ##################
function229<- function(x,y){
  x$bnstreet <-    paste("FLAT ",x$ss,sep="")
  x$bnstreet <-    paste(x$bnstreet,x$pp,sep=" ")
  x$bnstreet <-    paste(x$bnstreet,x$streetdescription,sep=" ")
  #x$bnstreet <- gsub(" ", "", x$bnstreet)
  x$addressf <-paste(x$postcodelocator,x$bnstreet,sep=",")
  
  y<-y[y$property_type=="Flat"|y$property_type=="Maisonette",]
  y$addressfinal <-  y$add
  
  #y$addressfinal <- gsub(" ", "", y$addressfinal)
  y$addressf <- paste(y$postcode,y$addressfinal,sep=",")
  
  taba1 <- inner_join(x,y,by="addressf")
  return(taba1)
}


link229<-function229(add,epc)

link229<-link229[,..needlist1]


link229u<- uniqueresult(link229)
dim(link229u)


link229d <- doubleresult(link229)
dim(link229d)

dim(epc)
#2649854      27
epc <- matchleft(epc,link229)
dim(epc)
#1326297      27



#################### method 230 ##################

function230<- function(x,y){
  
  x<-x[x$subbuildingname!="",]
  x$bnstreet <- gsub("FLAT", "APT", x$subbuildingname)
  
  x$bnstreet <-    paste(x$bnstreet,x$buildingname,sep=" ")
  x$bnstreet <-    paste(x$bnstreet,x$buildingnumber,sep=" ")
  x$bnstreet <-    paste(x$bnstreet,x$streetdescription,sep=" ")
  #x$bnstreet <- gsub(",", "", x$bnstreet)
  #x$bnstreet <- gsub("[']", "", x$bnstreet)
  #xbnstreet <- gsub(" ", "", x$bnstreet)
  x$addressf <-paste(x$postcodelocator,x$bnstreet,sep=",")
  
  y<-y[y$property_type=="Flat"|y$property_type=="Maisonette",]
  y$addressfinal <-  trimws(y$add)
  
  #y$addressfinal <- gsub(",", "", y$addressfinal)
  #y$addressfinal <- gsub("[']", "", y$addressfinal)
  #y$addressfinal <- gsub(" ", "", y$addressfinal)
  y$addressf <- paste(y$postcode,y$addressfinal,sep=",")
  
  taba1 <- inner_join(x,y,by="addressf")
  return(taba1)
  
}


link230<-function230(add,epc)

length(unique(link230$lmk_key))
link230<-link230[,..needlist1]


link230u<- uniqueresult(link230)
dim(link230u)


link230d <- doubleresult(link230)
dim(link230d)

dim(epc)
#
epc <- matchleft(epc,link230)
dim(epc)
#1326156      27

#################### method 231 ##################

function231<- function(x,y){
  #x$bnstreet <- x$subbuildingname
  x$bnstreet <-    paste(x$subbuildingname,x$buildingname,sep=" ")
  x$bnstreet <-    paste(x$bnstreet,x$buildingnumber,sep=" ")
  x$bnstreet <-    paste(x$bnstreet,x$streetdescription,sep=" ")
  x$bnstreet <- gsub("[']", "", x$bnstreet)
  x$bnstreet <- gsub("[.]", "", x$bnstreet)
  x$addressf <-paste(x$postcodelocator,x$bnstreet,sep=",")
  
  y<-y[y$property_type=="Flat"|y$property_type=="Maisonette",]
  y$addressfinal <-  trimws(y$add)
  y$addressfinal <- gsub("[']", "", y$addressfinal)
  y$addressfinal <- gsub("[.]", "", y$addressfinal)
  y$addressfinal <- gsub(",", "", y$addressfinal)
  y$addressf <- paste(y$postcode,y$addressfinal,sep=",")
  
  taba1 <- inner_join(x,y,by="addressf")
  return(taba1)
}





link231<-function231(add,epc)
length(unique(link231$lmk_key))

link231<-link231[,..needlist1]


link231u<- uniqueresult(link231)
dim(link231u)


link231d <- doubleresult(link231)
dim(link231d)

dim(epc)
#
epc <- matchleft(epc,link231)
dim(epc)
#1309411      27

################################check###################################################
#################### method 232 only one##################


function232<- function(x,y){
  
  x<-x[x$buildingname!="",]
  x$bnstreet <-    paste(x$buildingname,x$streetdescription,sep=" ")
  #x$bnstreet <- gsub(",", "", x$bnstreet)
  #x$bnstreet <- gsub("[']", "", x$bnstreet)
  #xbnstreet <- gsub(" ", "", x$bnstreet)
  x$bnstreet<-  trimws(x$bnstreet) 
  x$addressf <-paste(x$postcodelocator,x$bnstreet,sep=",")
  
  #y<-y[y$property_type=="Flat"|y$property_type=="Maisonette",]
  y$addressfinal <-  trimws(y$add)
  
  #y$addressfinal <- gsub(",", "", y$addressfinal)
  #y$addressfinal <- gsub("[']", "", y$addressfinal)
  y$addressfinal <-  trimws(y$addressfinal) 
  
  y$addressf <- paste(y$postcode,y$addressfinal,sep=",")
  
  taba1 <- inner_join(x,y,by="addressf")
  return(taba1)
  
  
}

link232<-function232(add,epc)

link232<-link232[,..needlist1]


link232u<- uniqueresult(link232)
dim(link232u)


link232d <- doubleresult(link232)
dim(link232d)


dim(epc)
#
epc <- matchleft(epc,link232)
dim(epc)
#  1309410      27



#################### method 233 ##################

function233<- function(x,y){
  
  
  x$bnstreet <-    paste(x$buildingnumber,x$locality,sep=",")
  #x$bnstreet <- gsub("/", "", x$bnstreet)
  x$bnstreet <- gsub("[.]", "", x$bnstreet)
  x$bnstreet <- gsub("[']", "", x$bnstreet)
  x$bnstreet <- gsub(" ", "", x$bnstreet)
  x$addressf <-paste(x$postcodelocator,x$bnstreet,sep=",")
  
  y$addressfinal <- paste(y$add)
  # y$bnstreet <- gsub("/", "",  y$bnstreet)
  y$bnstreet <- gsub("[.]", "",  y$bnstreet)
  y$bnstreet <- gsub("[']", "",  y$bnstreet)
  y$addressfinal <- gsub(" ", "", y$addressfinal)
  y$addressf <- paste(y$postcode,y$addressfinal,sep=",")
  
  taba1 <- inner_join(x,y,by="addressf")
  return(taba1) 
  
}



link233<-function233(add,epc)
dim(link233)
# 
link233<-link233[,..needlist1]
length(unique(link233$lmk_key))


link233u<- uniqueresult(link233)
dim(link233u)
#

link233d <- doubleresult(link233)
dim(link233d)


dim(epc)
#
epc <- matchleft(epc,link233)
dim(epc)
#1308032      27


#################### method 234  ##################

function234<- function(x,y){
  
  # x$bnstreet <-    paste(x$pp,x$locality,sep=",")
  # #x$bnstreet <- gsub("/", "", x$bnstreet)
  # x$bnstreet <- gsub("[.]", "", x$bnstreet)
  # x$bnstreet <- gsub("[']", "", x$bnstreet)
  # x$bnstreet <- gsub(" ", "", x$bnstreet)
  # x$addressf <-paste(x$postcodelocator,x$bnstreet,sep=",")
  # 
  # y$addressfinal <- paste(y$add)
  # # y$bnstreet <- gsub("/", "",  y$bnstreet)
  # y$bnstreet <- gsub("[.]", "",  y$bnstreet)
  # y$bnstreet <- gsub("[']", "",  y$bnstreet)
  # y$addressfinal <- gsub(" ", "", y$addressfinal)
  # y$addressf <- paste(y$postcode,y$addressfinal,sep=",")
  # 
  # taba1 <- inner_join(x,y,by="addressf")
  # return(taba1) 
  x$bnstreet <- paste(x$paotext,x$townname,sep=",")
  
  x$bnstreet <- gsub(",", "", x$bnstreet)
  x$bnstreet <- gsub("[.]", "", x$bnstreet)
  x$bnstreet <- gsub("[/]", "", x$bnstreet)
  x$bnstreet <- gsub("[']", "", x$bnstreet)
  x$bnstreet <- gsub(" ", "", x$bnstreet)
  x$addressf <-paste(x$postcodelocator,x$bnstreet,sep=",")
  
 
  y$addressfinal <-  trimws(y$add)
  y$addressfinal <- gsub("[.]", "", y$addressfinal)
  y$addressfinal <- gsub("/", "", y$addressfinal)
  y$addressfinal <- gsub(",", "", y$addressfinal)
  y$addressfinal <- gsub("[']", "", y$addressfinal)
  y$addressfinal <- gsub(" ", "", y$addressfinal)
  y$addressf <- paste(y$postcode,y$addressfinal,sep=",")
  
  taba1 <- inner_join(x,y,by="addressf")
  return(taba1)
}


link234<-function234(add,epc)



link234<-link234[,..needlist1]


link234u<- uniqueresult(link234)
dim(link234u)


link234d <- doubleresult(link234)


dim(epc)
# 
epc<- matchleft(epc,link234)
dim(epc)
#  2272664      27
#################### method 235 ##################


function235<- function(x,y){
  
  x$bnstreet <-    paste(x$buildingname,x$locality,sep=",")
  x$bnstreet <- gsub("/", "", x$bnstreet)
  x$bnstreet <- gsub("[.]", "", x$bnstreet)
  x$bnstreet <- gsub("[']", "", x$bnstreet)
  x$bnstreet <- gsub(" ", "", x$bnstreet)
  x$addressf <-paste(x$postcodelocator,x$bnstreet,sep=",")
  
  y$addressfinal <- paste(y$add)
  y$bnstreet <- gsub("/", "",  y$bnstreet)
  y$bnstreet <- gsub("[.]", "",  y$bnstreet)
  y$bnstreet <- gsub("[']", "",  y$bnstreet)
  y$addressfinal <- gsub(" ", "", y$addressfinal)
  y$addressf <- paste(y$postcode,y$addressfinal,sep=",")
  
  taba1 <- inner_join(x,y,by="addressf")
  return(taba1)
  
}



link235<-function235(add,epc)


link235<-link235[,..needlist1]


link235u<- uniqueresult(link235)
dim(link235u)


link235d <- doubleresult(link235)
dim(link235d)


dim(epc)
#
epc <- matchleft(epc,link235)
dim(epc)
#1307912      27

#
#################### method 236 ##################

function236<- function(x,y){
  
  x$bnstreet <-    paste(x$saotext,x$buildingname,sep=" ")

  x$bnstreet <-    paste(x$bnstreet,x$streetdescription,sep=" ")
  x$bnstreet <-    paste(x$bnstreet,x$dependentlocality,sep=",")
  x$bnstreet <- gsub("/", "", x$bnstreet)
  x$bnstreet <- gsub("[.]", "", x$bnstreet)
  x$bnstreet <- gsub("[']", "", x$bnstreet)
  x$bnstreet <- gsub(" ", "", x$bnstreet)
  x$addressf <-paste(x$postcodelocator,x$bnstreet,sep=",")
  
  y$addressfinal <- paste(y$add1,y$add2,sep=",")
  y$addressfinal <- gsub("[/]", "", y$addressfinal)
  y$addressfinal <- gsub("[.]", "", y$addressfinal)
  y$addressfinal <- gsub("[']", "", y$addressfinal)
  y$addressfinal <- gsub(" ", "", y$addressfinal)
  y$addressf <- paste(y$postcode,y$addressfinal,sep=",")
  
  taba1 <- inner_join(x,y,by="addressf")
  
  return(taba1)
}




link236<-function236(add,epc)
dim(link236)
# 

link236<-link236[,..needlist1]



link236u<- uniqueresult(link236)
dim(link236u)
# 

link236d <- doubleresult(link236)
dim(link236d)


epc <- matchleft(epc,link236)
dim(epc)
# 1307910      27

#################### method 237 check##################



function237<- function(x,y){
  
  x$bnstreet <-    paste(x$saotext,x$buildingnumber,sep=" ")
  
  x$bnstreet <-    paste(x$bnstreet,x$streetdescription,sep=" ")
  x$bnstreet <-    paste(x$bnstreet,x$locality,sep=",")
  x$bnstreet <- gsub("/", "", x$bnstreet)
  x$bnstreet <- gsub("[.]", "", x$bnstreet)
  x$bnstreet <- gsub("[']", "", x$bnstreet)
  x$bnstreet <- gsub(" ", "", x$bnstreet)
  x$addressf <-paste(x$postcodelocator,x$bnstreet,sep=",")
  
  y$addressfinal <- paste(y$add1,y$add2,sep=",")
  y$addressfinal <- gsub("[/]", "", y$addressfinal)
  y$addressfinal <- gsub("[.]", "", y$addressfinal)
  y$addressfinal <- gsub("[']", "", y$addressfinal)
  y$addressfinal <- gsub(" ", "", y$addressfinal)
  y$addressf <- paste(y$postcode,y$addressfinal,sep=",")
  
  taba1 <- inner_join(x,y,by="addressf")
  
  return(taba1)
  
}

link237<-function237(add,epc)
dim(link237)


link237<-link237[,..needlist1]

link237u<- uniqueresult(link237)
dim(link237u)


link237d <- doubleresult(link237)
dim(link237d)


dim(epc)
# 
epc <- matchleft(epc,link237)
dim(epc)
#1307906      27
#
#################### method 238 ##################

function238<- function(x,y){
  
  x$bnstreet <- paste(x$buildingnumber,x$streetdescription,sep=",")
  x$bnstreet <- gsub("-", "", x$bnstreet)
  #x$bnstreet <- gsub("", "", x$bnstreet)
  x$bnstreet <- gsub(" ", "", x$bnstreet)
  x$addressf <-paste(x$postcodelocator,x$bnstreet,sep=",")
  
  y<-y[!grepl("\\d+-\\d",y$add),]
  y$addressfinal <- trimws(y$add)
  y$addressfinal <- gsub("-", " ", y$addressfinal)
  
  y$addressfinal <- gsub(" ", "", y$addressfinal)
  
  y$addressf <- paste(y$postcode,y$addressfinal,sep=",")
  
  taba1 <- inner_join(x,y,by="addressf")
  return(taba1)
}



link238<-function238(add,epc)
dim(link238)


link238<-link238[,..needlist1]



link238u<- uniqueresult(link238)
dim(link238u)


link238d <- doubleresult(link238)
dim(link238d)


dim(epc)
# 
epc <- matchleft(epc,link238)
dim(epc)
#2269509      27
#################### method 239 ##################
function239<- function(x,y){

  x$bnstreet <-    paste(x$saotext,x$pp,sep=" ")
  
  x$bnstreet <-    paste(x$bnstreet,x$streetdescription,sep=" ")
  x$bnstreet <-    paste(x$bnstreet,x$locality,sep=",")
  x$bnstreet <- gsub("/", "", x$bnstreet)
  x$bnstreet <- gsub("[.]", "", x$bnstreet)
  x$bnstreet <- gsub("[']", "", x$bnstreet)
  x$bnstreet <- gsub(" ", "", x$bnstreet)
  x$addressf <-paste(x$postcodelocator,x$bnstreet,sep=",")
  
  y$addressfinal <- paste(y$add1,y$add2,sep=",")
  y$addressfinal <- gsub("[/]", "", y$addressfinal)
  y$addressfinal <- gsub("[.]", "", y$addressfinal)
  y$addressfinal <- gsub("[']", "", y$addressfinal)
  y$addressfinal <- gsub(" ", "", y$addressfinal)
  y$addressf <- paste(y$postcode,y$addressfinal,sep=",")
  
  taba1 <- inner_join(x,y,by="addressf")
  
  return(taba1)
}


link239<-function239(add,epc)

link239<-link239[,..needlist1]
length(unique(link239$lmk_key))

link239u<- uniqueresult(link239)
dim(link239u)


link239d <- doubleresult(link239)
dim(link239d)

dim(epc)
#2649854      27
epc <- matchleft(epc,link239)
dim(epc)
#1301593      27


#################### method 240 ##################

function240<- function(x,y){
  
  x$bnstreet <-    paste(x$pp,x$streetdescription,sep=" ")
  x$bnstreet <- gsub("-", "", x$bnstreet)
  
  x$bnstreet <- gsub("[']", "", x$bnstreet)
  x$bnstreet <- gsub("[.]", "", x$bnstreet)
  #x$bnstreet <- gsub(" ", "", x$bnstreet)
  x$addressf <-paste(x$postcodelocator,x$bnstreet,sep=",")
  
  
  y<-y[!grepl("\\d+-\\d",y$add),]
  y$addressfinal <- y$add
  
  
  y$addressfinal <- gsub("-", "", y$addressfinal)
  y$addressfinal <- gsub(",", "", y$addressfinal)
  y$addressfinal <- gsub("[.]", "", y$addressfinal)
  y$addressfinal <- gsub("[']", "", y$addressfinal)
  #y$addressfinal <- gsub(" ", "", y$addressfinal)
  y$addressf <- paste(y$postcode,y$addressfinal,sep=",")
  
  taba1 <- inner_join(x,y,by="addressf")
  return(taba1)
  
  
}


link240<-function240(add,epc)

length(unique(link240$lmk_key))
link240<-link240[,..needlist1]


link240u<- uniqueresult(link240)
dim(link240u)


link240d <- doubleresult(link240)
dim(link240d)

dim(epc)
#
epc <- matchleft(epc,link240)
dim(epc)
# 1300961      27

#################### method 241 ##################

function241<- function(x,y){
  
  x<-x[paostartsuffix=="",]
  x$bnstreet <-    x$paotext
  
  x$bnstreet <- gsub(" ", "", x$bnstreet)
  x$addressf <-paste(x$postcodelocator,x$bnstreet,sep=",")
  
  
  y<-y[y$property_type=="Flat"|y$property_type=="Maisonette",]
  y$addressfinal <-  paste("FLAT ",y$add,sep="")
  y$addressfinal <- gsub(",", "", y$addressfinal)
  
  y$addressfinal <- gsub(" ", "", y$addressfinal)
  y$addressf <- paste(y$postcode,y$addressfinal,sep=",")
  
  taba1 <- inner_join(x,y,by="addressf")
  return(taba1) 
  
}


link241<-function241(add,epc)


link241<-link241[,..needlist1]


link241u<- uniqueresult(link241)
dim(link241u)


link241d <- doubleresult(link241)
dim(link241d)

dim(epc)
#
epc <- matchleft(epc,link241)
dim(epc)
# 1300898      27


#################### method 242 ##################


function242<- function(x,y){
  
  
  x$bnstreet <- paste(x$subbuildingname,x$paotext ,sep=" ")
  # x$bnstreet <- gsub("/", "", x$bnstreet)
  # x$bnstreet <- gsub("[.]", "", x$bnstreet)
  # x$bnstreet <- gsub("[']", "", x$bnstreet)
  x$bnstreet <- gsub(" ", "", x$bnstreet)
  x$addressf <-paste(x$postcodelocator,x$bnstreet,sep=",")
  
  
  y<-y[y$property_type=="Flat"|y$property_type=="Maisonette",]
  y$addressfinal <-  paste("FLAT ",y$add,sep="")
  #y$addressfinal <- gsub(",", "", y$addressfinal)
  # y$addressfinal <- gsub("[/]", "", y$addressfinal)
  # y$addressfinal <- gsub("[.]", "", y$addressfinal)
  # y$addressfinal <- gsub("[']", "", y$addressfinal)
  y$addressfinal <- gsub(" ", "", y$addressfinal)
  y$addressf <- paste(y$postcode,y$addressfinal,sep=",")
  
  taba1 <- inner_join(x,y,by="addressf")
  return(taba1) 
  
}

link242<-function242(add,epc)

link242<-link242[,..needlist1]


link242u<- uniqueresult(link242)
dim(link242u)


link242d <- doubleresult(link242)
dim(link242d)


dim(epc)
#
epc <- matchleft(epc,link242)
dim(epc)
#  1300765      27



#################### method 243 ##################

function243<- function(x,y){
  
  
  x$bnstreet <-    paste("FLAT ",x$subbuildingname,sep=" ")
  x$bnstreet <-    paste(x$bnstreet,x$paotext,sep=",")
  x$bnstreet <-    paste(x$bnstreet,x$buildingnumber,sep=",")
  x$bnstreet <-    paste(x$bnstreet,x$streetdescription,sep=" ")
  #x$bnstreet <- gsub(",", "", x$bnstreet)
  #x$bnstreet <- gsub("[']", "", x$bnstreet)
  x$bnstreet <- gsub(" ", "", x$bnstreet)
  x$addressf <-paste(x$postcodelocator,x$bnstreet,sep=",")
  
  y<-y[y$property_type=="Flat"|y$property_type=="Maisonette",]
  y$addressfinal <-  trimws(y$add)
  
  #y$addressfinal <- gsub(",", "", y$addressfinal)
  #y$addressfinal <- gsub("[']", "", y$addressfinal)
  y$addressfinal <- gsub(" ", "", y$addressfinal)
  y$addressf <- paste(y$postcode,y$addressfinal,sep=",")
  
  taba1 <- inner_join(x,y,by="addressf")
  return(taba1)
  
}



link243<-function243(add,epc)
dim(link243)
# 
link243<-link243[,..needlist1]

link243u<- uniqueresult(link243)
dim(link243u)
#

link243d <- doubleresult(link243)
dim(link243d)


dim(epc)
#
epc <- matchleft(epc,link243)
dim(epc)
# 1299001      27


#################### method 244  ##################

function244<- function(x,y){
  
  
  x$saotext <- gsub("FLAT", "APARTMENT", x$saotext)
  
  x$bnstreet <-    paste(x$saotext,x$paotext ,sep=",")
  x$bnstreet <-    paste(x$bnstreet,x$pp,sep=",")
  x$bnstreet <-    paste(x$bnstreet,x$streetdescription,sep=" ")
  x$bnstreet <- gsub("/", "", x$bnstreet)
  x$bnstreet <- gsub("[.]", "", x$bnstreet)
  x$bnstreet <- gsub("[']", "", x$bnstreet)
  x$bnstreet <- gsub(" ", "", x$bnstreet)
  x$addressf <-paste(x$postcodelocator,x$bnstreet,sep=",")
  
  y<-y[y$property_type=="Flat"|y$property_type=="Maisonette",]
  y$addressfinal <-  trimws(y$add)
  y$addressfinal <- gsub("[/]", "", y$addressfinal)
  y$addressfinal <- gsub("[.]", "", y$addressfinal)
  y$addressfinal <- gsub("[']", "", y$addressfinal)
  y$addressfinal <- gsub(" ", "", y$addressfinal)
  y$addressf <- paste(y$postcode,y$addressfinal,sep=",")
  
  taba1 <- inner_join(x,y,by="addressf")
  return(taba1)
  
}


link244<-function244(add,epc)



link244<-link244[,..needlist1]


link244u<- uniqueresult(link244)
dim(link244u)


link244d <- doubleresult(link244)


dim(epc)
# 
epc<- matchleft(epc,link244)
dim(epc)
#  1298516      27
#################### method 245 ##################


function245<- function(x,y){
  
  
  x$saotext <- gsub("FLAT", "APARTMENT", x$saotext)
  
  x$bnstreet <-    paste(x$saotext,x$paotext ,sep=" ")
  x$bnstreet <-    paste(x$bnstreet,x$pp,sep=",")
  x$bnstreet <-    paste(x$bnstreet,x$streetdescription,sep=" ")
  
  x$bnstreet <- gsub(" ", "", x$bnstreet)
  x$addressf <-paste(x$postcodelocator,x$bnstreet,sep=",")
  
  y<-y[y$property_type=="Flat"|y$property_type=="Maisonette",]
  y$addressfinal <-  trimws(y$add)
  y$addressfinal <- gsub(" ", "", y$addressfinal)
  y$addressf <- paste(y$postcode,y$addressfinal,sep=",")
  
  taba1 <- inner_join(x,y,by="addressf")
  return(taba1)
  
}



link245<-function245(add,epc)


link245<-link245[,..needlist1]


link245u<- uniqueresult(link245)
dim(link245u)


link245d <- doubleresult(link245)
dim(link245d)


dim(epc)
#
epc <- matchleft(epc,link245)
dim(epc)
#1297028      27

#
#################### method 246 ##################

function246<- function(x,y){
  #x$bnstreet <- paste("FLAT ",x$saotext,sep="")
  x$bnstreet <- paste(x$saotext,x$paotext,sep=" ")
  x$bnstreet <- gsub(" ", "", x$bnstreet)
  x$addressf <-paste(x$postcodelocator,x$bnstreet,sep=",")
  
  y<-y[y$property_type=="Flat"|y$property_type=="Maisonette",]
  y$addressfinal <- trimws(y$add1)
  y$addressfinal <- paste("FLAT ",y$addressfinal,sep="")
  #y$addressfinal <- gsub(",", "", y$addressfinal)
  y$addressfinal <- gsub(" ", "", y$addressfinal)
  y$addressf <- paste(y$postcode,y$addressfinal,sep=",")
  
  taba1 <- inner_join(x,y,by="addressf")
  
  return(taba1)
  
}




link246<-function246(add,epc)
dim(link246)
# 

link246<-link246[,..needlist1]



link246u<- uniqueresult(link246)
dim(link246u)
# 

link246d <- doubleresult(link246)
dim(link246d)


epc <- matchleft(epc,link246)
dim(epc)
# 1290285      27

#################### method 247 ##################



function247<- function(x,y){
  
  x<-x[x$saotext!="",]
  
  x$bnstreet <- paste("FLAT ",x$saotext,sep="")
  
  x$bnstreet <- gsub(" ", "", x$bnstreet)
  x$addressf <-paste(x$postcodelocator,x$bnstreet,sep=",")
  
  y<-y[y$property_type=="Flat"|y$property_type=="Maisonette",]
  y$addressfinal <- trimws(y$add)
  y$addressfinal <- word(y$addressfinal,1,2)
  y$addressfinal <- gsub(",", "", y$addressfinal)
  y$addressfinal <- gsub(" ", "", y$addressfinal)
  y$addressf <- paste(y$postcode,y$addressfinal,sep=",")
  
  taba1 <- inner_join(x,y,by="addressf")
  
  return(taba1)
  
}

link247<-function247(add,epc)
dim(link247)


link247<-link247[,..needlist1]

link247u<- uniqueresult(link247)
dim(link247u)


link247d <- doubleresult(link247)
dim(link247d)


dim(epc)
# 
epc <- matchleft(epc,link247)
dim(epc)
#1289526      27
#################### method 248 ##################

function248<- function(x,y){
  x<-x[x$paoendnumber=="",]
  
  x$bnstreet <-    paste(x$saotext,x$pp,sep=",")
  #x$bnstreet <-    paste(x$bnstreet,x$paoendnumber ,sep="-")
  x$bnstreet <-    paste(x$bnstreet,x$streetdescription ,sep=" ")
  x$bnstreet <- gsub("FLAT ", "", x$bnstreet)
  x$bnstreet <- gsub("/", "", x$bnstreet)
  x$bnstreet <- gsub("[.]", "", x$bnstreet)
  x$bnstreet <- gsub("[']", "", x$bnstreet)
  x$bnstreet <- gsub(" ", "", x$bnstreet)
  x$addressf <-paste(x$postcodelocator,x$bnstreet,sep=",")
  
  y<-y[y$property_type=="Flat"|y$property_type=="Maisonette",]
  y$addressfinal <- trimws(y$add)
  y$addressfinal <- gsub("[/]", "", y$addressfinal)
  y$addressfinal <- gsub("[.]", "", y$addressfinal)
  y$addressfinal <- gsub("[']", "", y$addressfinal)
  y$addressfinal <- gsub(" ", "", y$addressfinal)
  y$addressf <- paste(y$postcode,y$addressfinal,sep=",")
  
  taba1 <- inner_join(x,y,by="addressf")
  
  return(taba1)
}



link248<-function248(add,epc)
dim(link248)


link248<-link248[,..needlist1]



link248u<- uniqueresult(link248)
dim(link248u)


link248d <- doubleresult(link248)
dim(link248d)


dim(epc)
# 
epc <- matchleft(epc,link248)
dim(epc)
# 1289350      27
#################### method 249 ##################
function249<- function(x,y){
  x$bnstreet <-    paste(x$saotext,x$pp,sep=" ")
  #x$bnstreet <-    paste(x$bnstreet,x$paostartsuffix,sep="")
  x$bnstreet <-    paste(x$bnstreet,x$streetdescription,sep=" ")
  x$bnstreet <-    paste(x$bnstreet,x$dependentlocality,sep=",")
  x$bnstreet <- gsub("/", "", x$bnstreet)
  x$bnstreet <- gsub("[.]", "", x$bnstreet)
  x$bnstreet <- gsub("[']", "", x$bnstreet)
  x$bnstreet <- gsub(" ", "", x$bnstreet)
  x$addressf <-paste(x$postcodelocator,x$bnstreet,sep=",")
  
  y$addressfinal <- paste(y$add1,y$add2,sep=",")
  y$addressfinal <- gsub("[/]", "", y$addressfinal)
  y$addressfinal <- gsub("[.]", "", y$addressfinal)
  y$addressfinal <- gsub("[']", "", y$addressfinal)
  y$addressfinal <- gsub(" ", "", y$addressfinal)
  y$addressf <- paste(y$postcode,y$addressfinal,sep=",")
  
  taba1 <- inner_join(x,y,by="addressf")
  
  return(taba1)
}


link249<-function249(add,epc)

link249<-link249[,..needlist1]


link249u<- uniqueresult(link249)
dim(link249u)


link249d <- doubleresult(link249)
dim(link249d)

dim(epc)
#
epc <- matchleft(epc,link249)
dim(epc)
#  1227060      27
epc1<-epc
##########section sum up############



link167u$method<-"link167u"
link168u$method<-"link168u"
link169u$method<-"link169u"
link170u$method<-"link170u"
link171u$method<-"link171u"
link172u$method<-"link172u"
link173u$method<-"link173u"
link174u$method<-"link174u"
link175u$method<-"link175u"
link176u$method<-"link176u"
link177u$method<-"link177u"
link178u$method<-"link178u"
link179u$method<-"link179u"
link180u$method<-"link180u"
link181u$method<-"link181u"
link182u$method<-"link182u"
link183u$method<-"link183u"
link184u$method<-"link184u"
link185u$method<-"link185u"
link186u$method<-"link186u"
link187u$method<-"link187u"
link188u$method<-"link188u"
link189u$method<-"link189u"
link190u$method<-"link190u"
link191u$method<-"link191u"
link192u$method<-"link192u"
link193u$method<-"link193u"
link194u$method<-"link194u"
link195u$method<-"link195u"
link196u$method<-"link196u"
link197u$method<-"link197u"
link198u$method<-"link198u"
link199u$method<-"link199u"
link200u$method<-"link200u"
link201u$method<-"link201u"
link202u$method<-"link202u"
link203u$method<-"link203u"
link204u$method<-"link204u"
link205u$method<-"link205u"
link206u$method<-"link206u"
link207u$method<-"link207u"
link208u$method<-"link208u"
link209u$method<-"link209u"
link210u$method<-"link210u"
link211u$method<-"link211u"
link212u$method<-"link212u"
link213u$method<-"link213u"
link214u$method<-"link214u"
link215u$method<-"link215u"
link216u$method<-"link216u"
link217u$method<-"link217u"
link218u$method<-"link218u"
link219u$method<-"link219u"
link220u$method<-"link220u"
link221u$method<-"link221u"
link222u$method<-"link222u"
link223u$method<-"link223u"
link224u$method<-"link224u"
link225u$method<-"link225u"
link226u$method<-"link226u"
link227u$method<-"link227u"
link228u$method<-"link228u"
link229u$method<-"link229u"
link230u$method<-"link230u"
link231u$method<-"link231u"
link232u$method<-"link232u"
link233u$method<-"link233u"
link234u$method<-"link234u"
link235u$method<-"link235u"
link236u$method<-"link236u"
link237u$method<-"link237u"
link238u$method<-"link238u"
link239u$method<-"link239u"
link240u$method<-"link240u"
link241u$method<-"link241u"
link242u$method<-"link242u"
link243u$method<-"link243u"
link244u$method<-"link244u"
link245u$method<-"link245u"
link246u$method<-"link246u"
link247u$method<-"link247u"
link248u$method<-"link248u"
link249u$method<-"link249u"




link167d$method<-"link167d"
link168d$method<-"link168d"
link169d$method<-"link169d"
link170d$method<-"link170d"
link171d$method<-"link171d"
link172d$method<-"link172d"
link173d$method<-"link173d"
link174d$method<-"link174d"
link175d$method<-"link175d"
link176d$method<-"link176d"
link177d$method<-"link177d"
link178d$method<-"link178d"
link179d$method<-"link179d"
link180d$method<-"link180d"
link181d$method<-"link181d"
link182d$method<-"link182d"
link183d$method<-"link183d"
link184d$method<-"link184d"
link185d$method<-"link185d"
link186d$method<-"link186d"
link187d$method<-"link187d"
link188d$method<-"link188d"
link189d$method<-"link189d"
link190d$method<-"link190d"
link191d$method<-"link191d"

link192d$method<-"link192d"
link193d$method<-"link193d"
link194d$method<-"link194d"
link195d$method<-"link195d"
link196d$method<-"link196d"
link197d$method<-"link197d"
link198d$method<-"link198d"
link199d$method<-"link199d"
link200d$method<-"link200d"
link201d$method<-"link201d"
link202d$method<-"link202d"
link203d$method<-"link203d"
link204d$method<-"link204d"
link205d$method<-"link205d"
link206d$method<-"link206d"
link207d$method<-"link207d"
link208d$method<-"link208d"
link209d$method<-"link209d"
link210d$method<-"link210d"
link211d$method<-"link211d"
link212d$method<-"link212d"
link213d$method<-"link213d"
link214d$method<-"link214d"
link215d$method<-"link215d"
link216d$method<-"link216d"
link217d$method<-"link217d"
link218d$method<-"link218d"
link219d$method<-"link219d"

link220d$method<-"link220d"
link221d$method<-"link221d"
link222d$method<-"link222d"
link223d$method<-"link223d"
link224d$method<-"link224d"
link225d$method<-"link225d"
link226d$method<-"link226d"
link227d$method<-"link227d"
link228d$method<-"link228d"
link229d$method<-"link229d"
link230d$method<-"link230d"
link231d$method<-"link231d"
link232d$method<-"link232d"
link233d$method<-"link233d"
link234d$method<-"link234d"
link235d$method<-"link235d"
link236d$method<-"link236d"
link237d$method<-"link237d"
link238d$method<-"link238d"
link239d$method<-"link239d"
link240d$method<-"link240d"
link241d$method<-"link241d"
link242d$method<-"link242d"
link243d$method<-"link243d"
link244d$method<-"link244d"
link245d$method<-"link245d"
link246d$method<-"link246d"
link247d$method<-"link247d"
link248d$method<-"link248d"
link249d$method<-"link249d"



dim(link167u)[1]+dim(link168u)[1]+dim(link169u)[1]+dim(link170u)[1]+dim(link171u)[1]+dim(link172u)[1]+dim(link173u)[1]+dim(link174u)[1]+dim(link175u)[1]+dim(link176u)[1]+dim(link177u)[1]+dim(link178u)[1]+dim(link179u)[1]+
  dim(link180u)[1]+dim(link181u)[1]+dim(link182u)[1]+dim(link183u)[1]+dim(link184u)[1]+dim(link185u)[1]+dim(link186u)[1]+dim(link187u)[1]+dim(link188u)[1]+dim(link189u)[1]+
  dim(link190u)[1]+dim(link191u)[1]+dim(link192u)[1]+dim(link193u)[1]+dim(link194u)[1]+dim(link195u)[1]+dim(link196u)[1]+dim(link197u)[1]+dim(link198u)[1]+dim(link199u)[1]+
  dim(link200u)[1]+dim(link201u)[1]+dim(link202u)[1]+dim(link203u)[1]+dim(link204u)[1]+dim(link205u)[1]+dim(link206u)[1]+dim(link207u)[1]+dim(link208u)[1]+dim(link209u)[1]+
  dim(link210u)[1]+dim(link211u)[1]+dim(link212u)[1]+dim(link213u)[1]+dim(link214u)[1]+dim(link215u)[1]+dim(link216u)[1]+dim(link217u)[1]+dim(link218u)[1]+dim(link219u)[1]+
  dim(link220u)[1]+dim(link221u)[1]+dim(link222u)[1]+dim(link223u)[1]+dim(link224u)[1]+dim(link225u)[1]+dim(link226u)[1]+dim(link227u)[1]+dim(link228u)[1]+dim(link229u)[1]+
  dim(link230u)[1]+dim(link231u)[1]+dim(link232u)[1]+dim(link233u)[1]+dim(link234u)[1]+dim(link235u)[1]+dim(link236u)[1]+dim(link237u)[1]+dim(link238u)[1]+dim(link239u)[1]+
  dim(link240u)[1]+dim(link241u)[1]+dim(link242u)[1]+dim(link243u)[1]+dim(link244u)[1]+dim(link245u)[1]+dim(link246u)[1]+dim(link247u)[1]+dim(link248u)[1]+dim(link249u)[1]
# 468305

dim(link167d)[1]+dim(link168d)[1]+dim(link169d)[1]+dim(link170d)[1]+dim(link171d)[1]+dim(link172d)[1]+dim(link173d)[1]+dim(link174d)[1]+dim(link175d)[1]+dim(link176d)[1]+dim(link177d)[1]+dim(link178d)[1]+dim(link179d)[1]+
  dim(link180d)[1]+dim(link181d)[1]+dim(link182d)[1]+dim(link183d)[1]+dim(link184d)[1]+dim(link185d)[1]+dim(link186d)[1]+dim(link187d)[1]+dim(link188d)[1]+dim(link189d)[1]+
  dim(link190d)[1]+dim(link191d)[1]+dim(link192d)[1]+dim(link193d)[1]+dim(link194d)[1]+dim(link195d)[1]+dim(link196d)[1]+dim(link197d)[1]+dim(link198d)[1]+dim(link199d)[1]+
  dim(link200d)[1]+dim(link201d)[1]+dim(link202d)[1]+dim(link203d)[1]+dim(link204d)[1]+dim(link205d)[1]+dim(link206d)[1]+dim(link207d)[1]+dim(link208d)[1]+dim(link209d)[1]+
  dim(link210d)[1]+dim(link211d)[1]+dim(link212d)[1]+dim(link213d)[1]+dim(link214d)[1]+dim(link215d)[1]+dim(link216d)[1]+dim(link217d)[1]+dim(link218d)[1]+dim(link219d)[1]+
  dim(link220d)[1]+dim(link221d)[1]+dim(link222d)[1]+dim(link223d)[1]+dim(link224d)[1]+dim(link225d)[1]+dim(link226d)[1]+dim(link227d)[1]+dim(link228d)[1]+dim(link229d)[1]+
  dim(link230d)[1]+dim(link231d)[1]+dim(link232d)[1]+dim(link233d)[1]+dim(link234d)[1]+dim(link235d)[1]+dim(link236d)[1]+dim(link237d)[1]+dim(link238d)[1]+dim(link239d)[1]+
  dim(link240d)[1]+dim(link241d)[1]+dim(link242d)[1]+dim(link243d)[1]+dim(link244d)[1]+dim(link245d)[1]+dim(link246d)[1]+dim(link247d)[1]+dim(link248d)[1]+dim(link249d)[1]
#82546
l167_249u = list(link167u,link168u,link169u,link170u,link171u,link172u,link173u,link174u,link175u,link176u,link177u,link178u,link179u,
                 link180u,link181u,link182u,link183u,link184u,link185u,link186u,link187u,link188u,link189u,
                 link190u,link191u,link192u,link193u,link194u,link195u,link196u,link197u,link198u,link199u,
                 link200u,link201u,link202u,link203u,link204u,link205u,link206u,link207u,link208u,link209u,
                 link210u,link211u,link212u,link213u,link214u,link215u,link216u,link217u,link218u,link219u,
                 link220u,link221u,link222u,link223u,link224u,link225u,link226u,link227u,link228u,link229u,
                 link230u,link231u,link232u,link233u,link234u,link235u,link236u,link237u,link238u,link239u,
                 link240u,link241u,link242u,link243u,link244u,link245u,link246u,link247u,link248u,link249u)



link167_249u<- rbindlist(l167_249u)
dim(link167_249u)
#  468305

length(unique(link167_249u$method))
length(unique(link167_249u$lmk_key))
#468305
l167_249d = list(link167d,link168d,link169d,link170d,link171d,link172d,link173d,link174d,link175d,link176d,link177d,link178d,link179d,
                 link180d,link181d,link182d,link183d,link184d,link185d,link186d,link187d,link188d,link189d,
                 link190d,link191d,link192d,link193d,link194d,link195d,link196d,link197d,link198d,link199d,
                 link200d,link201d,link202d,link203d,link204d,link205d,link206d,link207d,link208d,link209d,
                 link210d,link211d,link212d,link213d,link214d,link215d,link216d,link217d,link218d,link219d,
                 link220d,link221d,link222d,link223d,link224d,link225d,link226d,link227d,link228d,link229d,
                 link230d,link231d,link232d,link233d,link234d,link235d,link236d,link237d,link238d,link239d,
                 link240d,link241d,link242d,link243d,link244d,link245d,link246d,link247d,link248d,link249d)

link167_249d<- rbindlist(l167_249d)
dim(link167_249d)
# 82546    32
dbWriteTable(con, "link167_249dnew",value =link167_249d, append =  TRUE, row.names = FALSE)
dbWriteTable(con, "link167_249unew",value =link167_249u, append =  TRUE, row.names = FALSE)
rm(link167,link168,link169,link170,link171,link172,link173,link174,link175,link176,link177,link178,link179,
   link180,link181,link182,link183,link184,link185,link186,link187,link188,link189,
   link190,link191,link192,link193,link194,link195,link196,link197,link198,link199,
   link200,link201,link202,link203,link204,link205,link206,link207,link208,link209,
   link210,link211,link212,link213,link214,link215,link216,link217,link218,link219,
   link220,link221,link222,link223,link224,link225,link226,link227,link228,link229,
   link230,link231,link232,link233,link234,link235,link236,link237,link238,link239,
   link240,link241,link242,link243,link244,link245,link246,link247,link248,link249)


rm(l167_249d,l167_249u)
rm(link167d,link168d,link169d,link170d,link171d,link172d,link173d,link174d,link175d,link176d,link177d,link178d,link179d,
   link180d,link181d,link182d,link183d,link184d,link185d,link186d,link187d,link188d,link189d,
   link190d,link191d,link192d,link193d,link194d,link195d,link196d,link197d,link198d,link199d,
   link200d,link201d,link202d,link203d,link204d,link205d,link206d,link207d,link208d,link209d,
   link210d,link211d,link212d,link213d,link214d,link215d,link216d,link217d,link218d,link219d,
   link220d,link221d,link222d,link223d,link224d,link225d,link226d,link227d,link228d,link229d,
   link230d,link231d,link232d,link233d,link234d,link235d,link236d,link237d,link238d,link239d,
   link240d,link241d,link242d,link243d,link244d,link245d,link246d,link247d,link248d,link249d)

rm(link167u,link168u,link169u,link170u,link171u,link172u,link173u,link174u,link175u,link176u,link177u,link178u,link179u,
   link180u,link181u,link182u,link183u,link184u,link185u,link186u,link187u,link188u,link189u,
   link190u,link191u,link192u,link193u,link194u,link195u,link196u,link197u,link198u,link199u,
   link200u,link201u,link202u,link203u,link204u,link205u,link206u,link207u,link208u,link209u,
   link210u,link211u,link212u,link213u,link214u,link215u,link216u,link217u,link218u,link219u,
   link220u,link221u,link222u,link223u,link224u,link225u,link226u,link227u,link228u,link229u,
   link230u,link231u,link232u,link233u,link234u,link235u,link236u,link237u,link238u,link239u,
   link240u,link241u,link242u,link243u,link244u,link245u,link246u,link247u,link248u,link249u)

rm(function167,function168,function169,function170,function171,function172,function173,function174,function175,function176,function177,function178,function179,
   function180,function181,function182,function183,function184,function185,function186,function187,function188,function189,
   function190,function191,function192,function193,function194,function195,function196,function197,function198,function199,
   function200,function201,function202,function203,function204,function205,function206,function207,function208,function209,
   function210,function211,function212,function213,function214,function215,function216,function217,function218,function219,
   function220,function221,function222,function223,function224,function225,function226,function227,function228,function229,
   function230,function231,function232,function233,function234,function235,function236,function237,function238,function239,
   function240,function241,function242,function243,function244,function245,function246,function247,function248,function249)


dim(epc)

dim(epc)

#  1152672      27

#################### method 250 ##################

function250<- function(x,y){
 
  
  x$bnstreet <-    paste(x$saotext,x$paotext,sep=",")
  
  x$bnstreet <- gsub("/", "", x$bnstreet)
  x$bnstreet <- gsub("[.]", "", x$bnstreet)
  x$bnstreet <- gsub("[']", "", x$bnstreet)
  x$bnstreet <- gsub(" ", "", x$bnstreet)
  x$addressf <-paste(x$postcodelocator,x$bnstreet,sep=",")
  
  
  
  y$addressfinal <- trimws(y$add1) 
  y$addressfinal <- gsub(",", "", y$addressfinal)
  #y$addressfinal <-  paste("APARTMENT ",y$addressfinal,sep="")
  y$addressfinal <- paste(y$addressfinal,y$add2,sep=",")
  y$addressfinal <- gsub("[/]", "", y$addressfinal)
  y$addressfinal <- gsub("[.]", "", y$addressfinal)
  y$addressfinal <- gsub("[']", "", y$addressfinal)
  y$addressfinal <- gsub(" ", "", y$addressfinal)
  y$addressf <- paste(y$postcode,y$addressfinal,sep=",")
  
  taba1 <- inner_join(x,y,by="addressf")
  return(taba1) 
  
  
}


link250<-function250(add,epc)

link250<-link250[,..needlist1]


link250u<- uniqueresult(link250)

link250d <- doubleresult(link250)



epc <- matchleft(epc,link250)


#################### method 251 ##################


function251<- function(x,y){
  x$bnstreet <-    paste(x$subbuildingname,x$paotext,sep=" ")
  x$bnstreet <-    paste(x$bnstreet,x$buildingnumber,sep=",")
  x$bnstreet <-    paste(x$bnstreet,x$streetdescription,sep=",")
  x$bnstreet <- gsub("/", "", x$bnstreet)
  x$bnstreet <- gsub("[.]", "", x$bnstreet)
  x$bnstreet <- gsub("[']", "", x$bnstreet)
  x$bnstreet <- gsub(" ", "", x$bnstreet)
  x$addressf <-paste(x$postcodelocator,x$bnstreet,sep=",")
  
  
  y$addressfinal <-  y$add
  y$addressfinal <- gsub("[/]", "", y$addressfinal)
  y$addressfinal <- gsub("[.]", "", y$addressfinal)
  y$addressfinal <- gsub("[']", "", y$addressfinal)
  y$addressfinal <- gsub(" ", "", y$addressfinal)
  y$addressf <- paste(y$postcode,y$addressfinal,sep=",")
  
  taba1 <- inner_join(x,y,by="addressf")
  return(taba1) 
  

}


link251<-function251(add,epc)
link251<-link251[,..needlist1]

link251u<- uniqueresult(link251)

link251d <- doubleresult(link251)

epc <- matchleft(epc,link251)

#################### method 252 ##################


function252<- function(x,y){
  
  
  
  x$bnstreet <-    paste(x$saotext,x$buildingname,sep=" ")
  x$bnstreet <-    paste(x$bnstreet,x$streetdescription,sep=" ")
  x$bnstreet <-    paste(x$bnstreet,x$locality,sep=",")
  x$bnstreet <- gsub("/", "", x$bnstreet)
  x$bnstreet <- gsub("[.]", "", x$bnstreet)
  x$bnstreet <- gsub("[']", "", x$bnstreet)
  x$bnstreet <- gsub(" ", "", x$bnstreet)
  x$addressf <-paste(x$postcodelocator,x$bnstreet,sep=",")
  
  y$addressfinal <- paste(y$add1,y$add2,sep=",")
  y$addressfinal <- gsub("[/]", "", y$addressfinal)
  y$addressfinal <- gsub("[.]", "", y$addressfinal)
  y$addressfinal <- gsub("[']", "", y$addressfinal)
  y$addressfinal <- gsub(" ", "", y$addressfinal)
  y$addressf <- paste(y$postcode,y$addressfinal,sep=",")
  
  taba1 <- inner_join(x,y,by="addressf")
  
  return(taba1)
  
}

link252<-function252(add,epc)

link252<-link252[,..needlist1]


link252u<- uniqueresult(link252)

link252d <- doubleresult(link252)

epc <- matchleft(epc,link252)
#################### method 253 ##################

function253<- function(x,y){
  

  
  x$bnstreet <-    paste(x$saotext,x$pp,sep=",")
  
  
  x$bnstreet <-    paste(x$bnstreet,x$streetdescription,sep=" ")
  x$bnstreet <- gsub("/", "", x$bnstreet)
  x$bnstreet <- gsub("[.]", "", x$bnstreet)
  x$bnstreet <- gsub("[']", "", x$bnstreet)
  x$bnstreet <- gsub(" ", "", x$bnstreet)
  x$addressf <-paste(x$postcodelocator,x$bnstreet,sep=",")
  
  y[postcode=="SW8 2PX",add3:=gsub("COURLAND ROAD", "COURLAND GROVE",add3)]
  #y<-y[y$property_type=="Flat"|y$property_type=="Maisonette",]
  y$addressfinal <- paste(y$add1,y$add3,sep=",")
  
  y$addressfinal <- gsub("[/]", "", y$addressfinal)
  y$addressfinal <- gsub("[.]", "", y$addressfinal)
  y$addressfinal <- gsub("[']", "", y$addressfinal)
  y$addressfinal <- gsub(" ", "", y$addressfinal)
  y$addressf <- paste(y$postcode,y$addressfinal,sep=",")
  
  taba1 <- inner_join(x,y,by="addressf")
  
  return(taba1) 
  
}



link253<-function253(add,epc)
dim(link253)
# 
link253<-link253[,..needlist1]

link253u<- uniqueresult(link253)
dim(link253u)
#

link253d <- doubleresult(link253)
dim(link253d)


dim(epc)
#
epc <- matchleft(epc,link253)
dim(epc)
#  1153703      27


#################### method 254  ##################

function254<- function(x,y){
  
  
  x$bnstreet <-    paste(x$pp,x$streetdescription,sep=" ")
  x$bnstreet <- gsub("/", "", x$bnstreet)
  x$bnstreet <- gsub("[.]", "", x$bnstreet)
  x$bnstreet <- gsub("[']", "", x$bnstreet)
  x$bnstreet <- gsub(" ", "", x$bnstreet)
  x$addressf <-paste(x$postcodelocator,x$bnstreet,sep=",")
  
  y$addressfinal <- trimws(y$add1)
  y$addressfinal <- gsub("[/]", "", y$addressfinal)
  y$addressfinal <- gsub("[.]", "", y$addressfinal)
  y$addressfinal <- gsub("[']", "", y$addressfinal)
  y$addressfinal <- gsub(" ", "", y$addressfinal)
  y$addressf <- paste(y$postcode,y$addressfinal,sep=",")
  
  taba1 <- inner_join(x,y,by="addressf")
  
  return(taba1)
  
}


link254<-function254(add,epc)



link254<-link254[,..needlist1]


link254u<- uniqueresult(link254)
dim(link254u)


link254d <- doubleresult(link254)


dim(epc)
# 
epc<- matchleft(epc,link254)
dim(epc)
#  1137567      27
#################### method 255 ##################


function255<- function(x,y){
  
  x$bnstreet <-    paste(x$buildingname,x$streetdescription,sep=" ")
  x$bnstreet <- gsub("/", "", x$bnstreet)
  x$bnstreet <- gsub("[.]", "", x$bnstreet)
  x$bnstreet <- gsub("[']", "", x$bnstreet)
  x$bnstreet <- gsub(" ", "", x$bnstreet)
  x$addressf <-paste(x$postcodelocator,x$bnstreet,sep=",")
  
  y$addressfinal <- trimws(y$add1)
  y$addressfinal <- gsub("[/]", "", y$addressfinal)
  y$addressfinal <- gsub("[.]", "", y$addressfinal)
  y$addressfinal <- gsub("[']", "", y$addressfinal)
  y$addressfinal <- gsub(" ", "", y$addressfinal)
  y$addressf <- paste(y$postcode,y$addressfinal,sep=",")
  
  taba1 <- inner_join(x,y,by="addressf")
  
  return(taba1)
  
}



link255<-function255(add,epc)


link255<-link255[,..needlist1]


link255u<- uniqueresult(link255)
dim(link255u)


link255d <- doubleresult(link255)
dim(link255d)


dim(epc)
#
epc <- matchleft(epc,link255)
dim(epc)
#1136849      27

#
#################### method 256 ##################

function256<- function(x,y){
  
  
  x$bnstreet <-    paste(x$buildingnumber,x$streetdescription,sep=" ")
  x$bnstreet <- gsub("/", "", x$bnstreet)
  x$bnstreet <- gsub("[.]", "", x$bnstreet)
  x$bnstreet <- gsub("[']", "", x$bnstreet)
  x$bnstreet <- gsub(" ", "", x$bnstreet)
  x$addressf <-paste(x$postcodelocator,x$bnstreet,sep=",")
  
  y$addressfinal <- trimws(y$add1)
  y$addressfinal <- gsub("[/]", "", y$addressfinal)
  y$addressfinal <- gsub("[.]", "", y$addressfinal)
  y$addressfinal <- gsub("[']", "", y$addressfinal)
  y$addressfinal <- gsub(" ", "", y$addressfinal)
  y$addressf <- paste(y$postcode,y$addressfinal,sep=",")
  
  taba1 <- inner_join(x,y,by="addressf")
  
  return(taba1)
}




link256<-function256(add,epc)
dim(link256)
# 

link256<-link256[,..needlist1]



link256u<- uniqueresult(link256)
dim(link256u)
# 

link256d <- doubleresult(link256)
dim(link256d)


epc <- matchleft(epc,link256)
dim(epc)
# 1136790      27

#################### method 257 ##################



function257<- function(x,y){
  
  x$bnstreet <-    paste(x$saotext,x$pp,sep=" ")
  
  x$bnstreet <-    paste(x$bnstreet,x$streetdescription,sep=" ")
  x$bnstreet <- gsub("/", "", x$bnstreet)
  x$bnstreet <- gsub("[.]", "", x$bnstreet)
  x$bnstreet <- gsub("[']", "", x$bnstreet)
  x$bnstreet <- gsub(" ", "", x$bnstreet)
  x$addressf <-paste(x$postcodelocator,x$bnstreet,sep=",")
  
  y$addressfinal <- trimws(y$add1)
  y$addressfinal <- gsub("[/]", "", y$addressfinal)
  y$addressfinal <- gsub("[.]", "", y$addressfinal)
  y$addressfinal <- gsub("[']", "", y$addressfinal)
  y$addressfinal <- gsub(" ", "", y$addressfinal)
  y$addressf <- paste(y$postcode,y$addressfinal,sep=",")
  
  taba1 <- inner_join(x,y,by="addressf")
  
  return(taba1)
  
}

link257<-function257(add,epc)
dim(link257)


link257<-link257[,..needlist1]

link257u<- uniqueresult(link257)
dim(link257u)


link257d <- doubleresult(link257)
dim(link257d)


dim(epc)
# 
epc <- matchleft(epc,link257)
dim(epc)
#1136362      27
#
#################### method 258 ##################

function258<- function(x,y){
  x$bnstreet <- paste(x$subbuildingname,x$paotext ,sep=" ")
  x$bnstreet <- paste(x$bnstreet,x$buildingnumber ,sep=" ")
  x$bnstreet <- paste(x$bnstreet,x$streetdescription ,sep=",")

  x$bnstreet<- trimws(x$bnstreet)
  x$bnstreet <- gsub("/", "", x$bnstreet)
  x$bnstreet <- gsub("[.]", "", x$bnstreet)
  x$bnstreet <- gsub("[']", "", x$bnstreet)
  x$bnstreet <- gsub(" ", "", x$bnstreet)
  x$addressf <-paste(x$postcodelocator,x$bnstreet,sep=",")
  
  
  y<-y[y$property_type=="Flat"|y$property_type=="Maisonette",]
  y$addressfinal <-  trimws(y$add)
 
  y$addressfinal <- gsub("[/]", "", y$addressfinal)
  y$addressfinal <- gsub("[.]", "", y$addressfinal)
  y$addressfinal <- gsub("[']", "", y$addressfinal)
  
  y$addressfinal <- gsub(" ", "", y$addressfinal)
  y$addressf <- paste(y$postcode,y$addressfinal,sep=",")
  
  taba1 <- inner_join(x,y,by="addressf")
  return(taba1) 
  
}



link258<-function258(add,epc)
dim(link258)


link258<-link258[,..needlist1]



link258u<- uniqueresult(link258)
dim(link258u)


link258d <- doubleresult(link258)
dim(link258d)


dim(epc)
# 
epc <- matchleft(epc,link258)
dim(epc)
# 1136258      27
#################### method 259 ##################
epc2<-epc[epc$lmk_key %in% epc$lmk_key,]
add2<-add
setDT(add2)
setDT(epc2)
function259<- function(z,k){
  
  x<-z
  setDT(x)
  x[postcodelocator=="L6 6AR",streetdescription:=gsub("CAMBRIA STREET NORTH","CAMBRIA STREET", streetdescription)]
  x[postcodelocator=="L6 6AR",streetdescription:=gsub("CAMBRIA STREET SOUTH","CAMBRIA STREET", streetdescription)]
  x[postcodelocator=="L6 6AP",streetdescription:=gsub("CAMBRIA STREET NORTH","CAMBRIA STREET", streetdescription)]
  x[postcodelocator=="L6 6AP",streetdescription:=gsub("CAMBRIA STREET SOUTH","CAMBRIA STREET", streetdescription)]
  x[postcodelocator=="L6 6AG",streetdescription:=gsub("TUDOR STREET SOUTH","TUDOR STREET", streetdescription)]
  x[postcodelocator=="L6 6AG",streetdescription:=gsub("TUDOR STREET NORTH","TUDOR STREET", streetdescription)]
  x[postcodelocator=="L6 6AQ",streetdescription:=gsub("TUDOR STREET SOUTH","TUDOR STREET", streetdescription)]
  x[postcodelocator=="L6 6AQ",streetdescription:=gsub("TUDOR STREET NORTH","TUDOR STREET", streetdescription)]
  
  x[postcodelocator=="DN15 6BE",streetdescription:=gsub("GROSVENOR STREET SOUTH","GROSVENOR STREET", streetdescription)]
  
  
  x$bnstreet <- paste(x$buildingnumber,x$streetdescription,sep=",")
  x$bnstreet <- gsub("[.]", "", x$bnstreet)
  x$bnstreet <- gsub("[']", "", x$bnstreet)
  x$bnstreet <- gsub(" ", "", x$bnstreet)
  x$addressf <-paste(x$postcodelocator,x$bnstreet,sep=",")
  
  
  y<-k
  setDT(y)
  y[postcode=="LD6 5EP",add:=gsub("MAES BRENNIN", "MAES BRENIN",add)]
  y[postcode=="TN34 2JP",add:=gsub("WATERMENS CLOSE", "WATERMANS CLOSE",add)]
  
  
  y[postcode=="LN2 5BG",add:=gsub("ST. HUGH STREET", "ST HUGHS STREET",add)]
  y[postcode=="LN6 8BD",add:=gsub("PAVILION GARDENS", "PAVILLION GARDENS",add)]
  
  
  
  #y[postcode=="DN15 6BE",add:=gsub("GROSVENOR STREET", "SAINT LUKES ROAD",add)]
  y[postcode=="SR4 6PF",add:=gsub("ST. LUKES ROAD", "SAINT LUKES ROAD",add)]
  y[postcode=="NG11 9JF",add:=gsub("WALCOTT GREEN", "WALCOT GREEN",add)]
  y[postcode=="L28 1NT",add:=gsub("BLACKTHORN CRESCENT", "BLACKTHORNE CRESCENT",add)]
  y[postcode=="TA20 2DU",add:=gsub("HALCOMBE ESTATE", "HALCOMBE",add)]
  
  y[postcode=="CF47 9RW",add:=gsub("HAWTHORNE AVENUE", "HAWTHORN AVENUE",add)]
  
  y[postcode=="SR2 8QB",add:=gsub("ST. LEONARD STREET", "SAINT LEONARD STREET",add)]
  
  
  y[postcode=="SA12 6QB",add:=gsub("THE POPLARS", "POPLARS",add)]
  y[postcode=="CV4 9NR",add:=gsub("BUSHBERRY AVENUE", "BUSHBERY AVENUE",add)]
  y[postcode=="SE1 2PY",add:=gsub("BUTLERS & COLONIAL WHARF", "BUTLERS AND COLONIAL WHARF",add)]
  y[postcode=="SE1 2PX",add:=gsub("BUTLERS & COLONIAL WHARF", "BUTLERS AND COLONIAL WHARF",add)]
  
  y[postcode=="LL59 5DS",add:=gsub("TYDDYN MOSTYN ESTATE", "TYDDYN MOSTYN",add)]
  y[postcode=="NR3 3RU",add:=gsub("KEY & CASTLE YARD", "KEY AND CASTLE YARD",add)]
  
  y[postcode=="CM7 3RA",add:=gsub("BARTRAM AVENUE","BARTRAM AVENUE NORTH", add)]
  y[postcode=="CM7 3RB",add:=gsub("BARTRAM AVENUE","BARTRAM AVENUE SOUTH", add)]
  y[postcode=="BB2 3AF",add:=gsub("ST. MARYS WHARFE","ST MARYS WHARF", add)]
  y[postcode=="RM6 4XE",add:=gsub("BLACKSMITHS CLOSE","BLACKSMITH CLOSE", add)]
  
  y[postcode=="N4 4BH",add:=gsub("HOLLY PARK ESTATE", "HOLLY PARK",add)]
  y[postcode=="N4 4BD",add:=gsub("HOLLY PARK ESTATE", "HOLLY PARK",add)]
  y[postcode=="N4 4BN",add:=gsub("HOLLY PARK ESTATE", "HOLLY PARK",add)]
  y[postcode=="N4 4BJ",add:=gsub("HOLLY PARK ESTATE", "HOLLY PARK",add)]
  y[postcode=="N4 4BW",add:=gsub("HOLLY PARK ESTATE", "HOLLY PARK",add)]
  y[postcode=="N4 4BE",add:=gsub("HOLLY PARK ESTATE", "HOLLY PARK",add)]
  y[postcode=="N4 4BL",add:=gsub("HOLLY PARK ESTATE", "HOLLY PARK",add)]
  
  
  
  y[postcode=="WR5 3FG",add:=gsub("CHUB CLOSE","CHUBB CLOSE", add)]
  y[postcode=="WF8 2BX",add:=gsub("MOVERLEY FLATS","MOVERLEY FLATTS", add)]
  
  y[postcode=="WF8 2PF",add:=gsub("ST. THOMAS'S TERRACE","ST THOMAS'S TERRACE FERRYBRIDGE ROAD", add)]
  y[postcode=="WR5 3AJ",add:=gsub("RANSOME AVENUE","RANSOM AVENUE", add)]
  y[postcode=="WR5 3AW",add:=gsub("RANSOME AVENUE","RANSOM AVENUE", add)]
  y[postcode=="WR5 3AL",add:=gsub("RANSOME AVENUE","RANSOM AVENUE", add)]
  #y[postcode=="L6 6AR",add:=gsub("CAMBRIA STREET","TREGELLES ROAD", add)]
  
  y[postcode=="SA10 7HT",add:=gsub("TREGELLIS ROAD","TREGELLES ROAD", add)]
  y[postcode=="SA10 7EE",add:=gsub("TREHARNE COURT","TRAHERNE COURT", add)]
  y[postcode=="SA10 7DA",add:=gsub("ADAMS WALK","ADAM WALK", add)]
  y[postcode=="SA10 7JG",add:=gsub("RYAN CLOSE","RYANS CLOSE", add)]
  
  y[postcode=="SR1 3QN",add:=gsub("GILLHURST GRANGE","GILHURST GRANGE", add)]
  
  
  
  y[postcode=="CF63 4LT",add:=gsub("ST. MARYS AVENUE","ST MARY'S AVENUE", add)]
  
  y[postcode=="LL65 2NN",add:=gsub("TRESEIFION ESTATE","TRESEIFION", add)]
  
  
  y[postcode=="B13 9EY",add:=gsub("LENCHS CLOSE", "LENCH CLOSE",add)]
  y[postcode=="OL8 2QG",add:=gsub("REINS LEE AVENUE", "REINS LEA AVENUE",add)]
  y[postcode=="SY16 1QD",add:=gsub("LON MAESYCOED", "L?N MAESYCOED",add)]
  y[postcode=="SY16 1QQ",add:=gsub("LON MAESYCOED", "L?N MAESYCOED",add)]
  y[postcode=="LL65 2YN",add:=gsub("TRESEIFION ESTATE", "TRESEIFION",add)]
  y[postcode=="ME10 2DT",add:=gsub("CORTLANDS MEWS", "CORTLAND MEWS",add)]
  y[postcode=="PL6 6LT",add:=gsub("BLACKHALL GARDENS", "BLACKALL GARDENS",add)]
  
  y[postcode=="SR6 0PB",add:=gsub("ST. ANDREWS TERRACE", "SAINT ANDREWS TERRACE",add)]
  y[postcode=="SR6 0AG",add:=gsub("ST. PETERS VIEW", "SAINT PETERS VIEW",add)]
  
  
  y[postcode=="ME10 2BT",add:=gsub("MEADS AVENUE", "THE MEADS AVENUE",add)]
  y[postcode=="LL65 2HL",add:=gsub("HARBOUR VIEW ESTATE", "HARBOUR VIEW",add)]
  # y[postcode=="M6 7RT",add:=gsub("IRLAM SQUARE", "IRLAMS SQUARE",add)]
  #y[postcode=="SY16 1QJ",add:=gsub("Lon Pantyllyn", "L?N PANTYLLYN",add)]
  y[postcode=="B49 5RA",add:=gsub("COLBROOK CLOSE", "COLEBROOK CLOSE",add)]
  
  y[postcode=="NG11 8QA",add:=gsub("EDDLESTONE DRIVE", "EDDLESTON DRIVE",add)]
  y[postcode=="NG11 8SU",add:=gsub("HAWKESLEY GARDENS", "HAWKSLEY GARDENS",add)]
  
  y[postcode=="SR8 4AP",add:=gsub("MCGUINNESS AVENUE", "MCGUINESS AVENUE",add)]
  
  y[postcode=="SR8 4EJ",add:=gsub("RODGERS CLOSE", "ROGERS CLOSE",add)]
  
  y[postcode=="PL6 6NY",add:=gsub("BILLINGS CLOSE", "BILLING CLOSE",add)]
  
  y[postcode=="OL6 6JJ",add:=gsub("ALBERMARLE TERRACE", "ALBEMARLE TERRACE",add)]
  
  y[postcode=="SR4 0HA",add:=gsub("ST. LUKES ROAD", "SAINT LUKES ROAD",add)]
  y[postcode=="SR4 0AL",add:=gsub("ST. LUKES ROAD", "SAINT LUKES ROAD",add)]
  
  y[postcode=="SR4 0HA",add:=gsub("ST. LUKES ROAD", "SAINT LUKES ROAD",add)]
  y[postcode=="SR4 0AQ",add:=gsub("ST. LUKES ROAD", "SAINT LUKES ROAD",add)]
  
  y[postcode=="WV10 9EH",add:=gsub("PURCELL ROAD", "PURCEL ROAD",add)]
  y[postcode=="WV10 9EQ",add:=gsub("PURCELL ROAD", "PURCEL ROAD",add)]
  
  y[postcode=="DA8 2BF",add:=gsub("SHERMANSBURY CLOSE", "SHERMANBURY CLOSE",add)]
  y[postcode=="DA8 2PN",add:=gsub("COLOMBUS SQUARE", "COLUMBUS SQUARE",add)]
  y[postcode=="SA47 0PS",add:=gsub("BRO LLETHI", "BRON LLETHI",add)]
  y[postcode=="SA47 0RFc2",add:=gsub("BRO LLETHI", "BRON LLETHI",add)]
  
  y[postcode=="OL11 1TZ",add:=gsub("IAN FRAZER COURT", "IAN FRASER COURT",add)]
  y[postcode=="WF1 1NJ",add:=gsub("NEW WELLS","NEW WELLS THORNHILL STREET",add)]
  y[postcode=="LL61 5QG",add:=gsub("TYN CAEAU ESTATE", "TYN CAEAU",add)]
  y[postcode=="LL61 5JR",add:=gsub("STAD TY CROES", "LON TY CROES",add)]
  
  y[postcode=="LL61 5TJ",add:=gsub("MAES Y WAEN", "STAD MAES Y WAEN",add)]
  y[postcode=="LL61 5PX",add:=gsub("BRYN BRAS ESTATE", "BRYN BRAS",add)]
  
  
  y[postcode=="BR5 2LG",add:=gsub("MILLFIELD COTTAGES", "MILLFIELDS COTTAGES",add)]
  y[postcode=="SA1 2EW",add:=gsub("GOLWG Y GARREG", "GOLWG Y GARREG WEN",add)]
  y[postcode=="SR8 4HY",add:=gsub("STAYPLTON DRIVE", "STAPYLTON DRIVE",add)]
  y[postcode=="SR4 0AN",add:=gsub("ST. LUKES ROAD", "SAINT LUKES ROAD",add)]
  y[postcode=="LL65 2NB",add:=gsub("TRESEIFION ESTATE", "TRESEIFION",add)]
  y$addressfinal <- trimws(y$add)
  y$addressfinal <- gsub("[']", "", y$addressfinal)
  y$addressfinal <- gsub("[.]", "", y$addressfinal)
  y$addressfinal <- gsub(" ", "", y$addressfinal)
  
  y$addressf <- paste(y$postcode,y$addressfinal,sep=",")
  
  taba1 <- inner_join(x,y,by="addressf")
  return(taba1)
  
}




link259<-function259(add2,epc2)

link259<-link259[,..needlist1]


link259u<- uniqueresult(link259)
dim(link259u)


link259d <- doubleresult(link259)
dim(link259d)
rm(add2,epc2)
dim(epc)
#2649854      27
epc <- matchleft(epc,link259)
dim(epc)
#1131986      27


#################### method 260 ##################

function260<- function(x,y){
  x$bnstreet <-    paste(x$ss,x$streetdescription,sep=",")
  
  x$bnstreet <- gsub(" ", "", x$bnstreet)
  x$addressf <-paste(x$postcodelocator,x$bnstreet,sep=",")
  
  y$addressfinal <- trimws(y$add)
  y$addressfinal <- gsub(" ", "", y$addressfinal)
  y$addressf <- paste(y$postcode,y$addressfinal,sep=",")
  
  taba1 <- inner_join(x,y,by="addressf")
  
  return(taba1)
  
  
}


link260<-function260(add,epc)

length(unique(link260$lmk_key))
link260<-link260[,..needlist1]


link260u<- uniqueresult(link260)
dim(link260u)


link260d <- doubleresult(link260)
dim(link260d)

dim(epc)
#
epc <- matchleft(epc,link260)
dim(epc)
# 1131901      27

#################### method 261 ##################

function261<- function(x,y){
  
  x$bnstreet <- paste(x$buildingnumber,x$paotext,sep=",")
  x$bnstreet<-  trimws(x$bnstreet) 
  x$bnstreet <- gsub(" ", "", x$bnstreet)
  
  x$addressf <-paste(x$postcodelocator,x$bnstreet,sep=",")
  
  
  y[postcode=="LN11 0YS",add:=gsub("NORTHOLME COURT","NORTH HOLME COURT", add)]
  y$addressfinal <- y$add
  
  #y$addressfinal <- gsub(",", "", y$addressfinal)
  
  y$addressfinal <-  trimws(y$addressfinal) 
  y$addressfinal <- gsub(" ", "", y$addressfinal)
  y$addressf <- paste(y$postcode,y$addressfinal,sep=",")
  
  taba1 <- inner_join(x,y,by="addressf")
  return(taba1)
  
}


link261<-function261(add,epc)


link261<-link261[,..needlist1]


link261u<- uniqueresult(link261)
dim(link261u)


link261d <- doubleresult(link261)
dim(link261d)

dim(epc)
#
epc <- matchleft(epc,link261)
dim(epc)
#1131620      27


#################### method 262 super interesting##################


function262<- function(x,y){
  
  x$bnstreet <- paste(x$buildingnumber,x$streetdescription,sep=",")
  x$bnstreet <- gsub("-", "", x$bnstreet)
  #x$bnstreet <- gsub("", "", x$bnstreet)
  x$bnstreet <- gsub(" ", "", x$bnstreet)
  x$addressf <-paste(x$postcodelocator,x$bnstreet,sep=",")
  
  
  y$addressfinal <- trimws(y$add)
  y$addressfinal <- gsub("-", " ", y$addressfinal)
  
  y$addressfinal <- gsub(" ", "", y$addressfinal)
  
  y$addressf <- paste(y$postcode,y$addressfinal,sep=",")
  
  taba1 <- inner_join(x,y,by="addressf")
  return(taba1)
  
  
  
}

link262<-function262(add,epc)

link262<-link262[,..needlist1]


link262u<- uniqueresult(link262)
dim(link262u)


link262d <- doubleresult(link262)
dim(link262d)


dim(epc)
#
epc <- matchleft(epc,link262)
dim(epc)
# 1131585      27



#################### method 263 ##################

function263<- function(x,y){
  x$bnstreet <-    paste(x$buildingnumber,x$streetdescription,sep=" ")
  x$bnstreet <- gsub(" ", "", x$bnstreet)
  x$addressf <-paste(x$postcodelocator,x$bnstreet,sep=",")
  setDF(y)
  y[y$postcode=="CH6 5GF","add1"] <- gsub( "FFORD MADOG", "FFORDD MADOG",y[y$postcode=="CH6 5GF","add1"])
  y[y$postcode=="LN2 4NY","add1"] <- gsub( "POPLAR GROVE", "POPLARS GROVE",y[y$postcode=="LN2 4NY","add1"])
  y$addressfinal <- trimws(y$add1)
  y$addressfinal <- gsub(",", "", y$addressfinal)
  y$addressfinal <- gsub(" ", "", y$addressfinal)
  y$addressf <- paste(y$postcode,y$addressfinal,sep=",")
  
  taba1 <- inner_join(x,y,by="addressf")
  
  return(taba1)
  
  
}



link263<-function263(add,epc)
dim(link263)
# 
link263<-link263[,..needlist1]

link263u<- uniqueresult(link263)
dim(link263u)
#

link263d <- doubleresult(link263)
dim(link263d)


dim(epc)
#
epc <- matchleft(epc,link263)
dim(epc)
# 2277634      27


#################### method 264  ##################

function264<- function(x,y){
  
  x$bnstreet <-    paste(x$pp,x$streetdescription,sep=" ")
  x$bnstreet <- gsub(" ", "", x$bnstreet)
  x$addressf <-paste(x$postcodelocator,x$bnstreet,sep=",")
  setDF(y)
  y[y$postcode=="CH6 5GF","add1"] <- gsub( "FFORD MADOG", "FFORDD MADOG",y[y$postcode=="CH6 5GF","add1"])
  y[y$postcode=="LN2 4NY","add1"] <- gsub( "POPLAR GROVE", "POPLARS GROVE",y[y$postcode=="LN2 4NY","add1"])
  y$addressfinal <- trimws(y$add1)
  y$addressfinal <- gsub(",", "", y$addressfinal)
  y$addressfinal <- gsub(" ", "", y$addressfinal)
  y$addressf <- paste(y$postcode,y$addressfinal,sep=",")
  
  taba1 <- inner_join(x,y,by="addressf")

  return(taba1)
  
}


link264<-function264(add,epc)



link264<-link264[,..needlist1]


link264u<- uniqueresult(link264)
dim(link264u)


link264d <- doubleresult(link264)


dim(epc)
# 
epc<- matchleft(epc,link264)
dim(epc)
#  2272664      27
#################### method 265 ##################


function265<- function(x,y){
  
  x$bnstreet <-    paste(x$buildingname,x$streetdescription,sep=" ")
  x$bnstreet <- gsub(" ", "", x$bnstreet)
  x$addressf <-paste(x$postcodelocator,x$bnstreet,sep=",")
  
  y$addressfinal <- trimws(y$add1)
  y$addressfinal <- gsub(",", "", y$addressfinal)
  y$addressfinal <- gsub(" ", "", y$addressfinal)
  y$addressf <- paste(y$postcode,y$addressfinal,sep=",")
  
  taba1 <- inner_join(x,y,by="addressf")
  
  return(taba1)
  
}



link265<-function265(add,epc)


link265<-link265[,..needlist1]


link265u<- uniqueresult(link265)
dim(link265u)


link265d <- doubleresult(link265)
dim(link265d)


dim(epc)
#
epc <- matchleft(epc,link265)
dim(epc)
#1104252      27

#
#################### method 266 ##################

function266<- function(x,y){
  x<-x[x$subbuildingname!="",]
  
  x$bnstreet <-    paste(x$subbuildingname,x$buildingnumber,sep=" ")
  x$bnstreet <-    paste(x$bnstreet,x$streetdescription,sep=" ")
  x$bnstreet <- gsub("/", "", x$bnstreet)
  x$bnstreet <- gsub("[.]", "", x$bnstreet)
  x$bnstreet <- gsub("[']", "", x$bnstreet)
  #x$bnstreet <- gsub(" ", "", x$bnstreet)
  x$addressf <-paste(x$postcodelocator,x$bnstreet,sep=",")
  
  y<-y[y$property_type=="Flat"|y$property_type=="Maisonette",]
  y$addressfinal <- gsub("APARTMENT", "FLAT", y$add)
  y$addressfinal <- gsub("[/]", "", y$addressfinal)
  y$addressfinal <- gsub("[.]", "", y$addressfinal)
  y$addressfinal <- gsub("[']", "", y$addressfinal)
  
  #y$addressfinal <- gsub(" ", "", y$addressfinal)
  y$addressf <- paste(y$postcode,y$addressfinal,sep=",")
  
  taba1 <- inner_join(x,y,by="addressf")
  return(taba1)
  
  
}




link266<-function266(add,epc)
dim(link266)
# 

link266<-link266[,..needlist1]



link266u<- uniqueresult(link266)
dim(link266u)
# 

link266d <- doubleresult(link266)
dim(link266d)


epc <- matchleft(epc,link266)
dim(epc)
#  1104151      27

#################### method 267 ##################



function267<- function(x,y){
  
  x$bnstreet <-    paste(x$subbuildingname,x$paotext,sep=" ")
  x$bnstreet <-    paste(x$bnstreet,x$streetdescription,sep=",")
  x$bnstreet <- gsub(" ", "", x$bnstreet)
  x$addressf <-paste(x$postcodelocator,x$bnstreet,sep=",")
  
  y<-y[y$property_type=="Flat"|y$property_type=="Maisonette",]
  y$addressfinal <-  paste("APARTMENT ",y$add,sep="")
  
  y$addressfinal <- gsub(" ", "", y$addressfinal)
  y$addressf <- paste(y$postcode,y$addressfinal,sep=",")
  
  taba1 <- inner_join(x,y,by="addressf")
  return(taba1)
}



link267<-function267(add,epc)
dim(link267)


link267<-link267[,..needlist1]

link267u<- uniqueresult(link267)
dim(link267u)


link267d <- doubleresult(link267)
dim(link267d)


dim(epc)
# 
epc <- matchleft(epc,link267)
dim(epc)
#21104129      27
#
#################### method 268 ##################

function268<- function(x,y){
  
  x$bnstreet <-    paste(x$subbuildingname,x$buildingname,sep=" ")
  x$bnstreet <-    paste(x$bnstreet,x$ buildingnumber,sep=" ")
  x$bnstreet <-    paste(x$bnstreet,x$streetdescription,sep=" ")
  
  x$addressf <-paste(x$postcodelocator,x$bnstreet,sep=",")
  
  y<-y[y$property_type=="Flat"|y$property_type=="Maisonette",]
  y$addressfinal <-  paste("APARTMENT ",y$add,sep="")
  
  
  y$addressf <- paste(y$postcode,y$addressfinal,sep=",")
  
  taba1 <- inner_join(x,y,by="addressf")
  return(taba1)
}




link268<-function268(add,epc)
dim(link268)


link268<-link268[,..needlist1]



link268u<- uniqueresult(link268)
dim(link268u)


link268d <- doubleresult(link268)
dim(link268d)


dim(epc)
# 
epc <- matchleft(epc,link268)
dim(epc)
# 1104118      27
#################### method 269 ##################
function269<- function(x,y){
  x$bnstreet <-    paste(x$subbuildingname,x$buildingname,sep=" ")
  x$bnstreet <-    paste(x$bnstreet,x$buildingnumber,sep=",")
  x$bnstreet <-    paste(x$bnstreet,x$streetdescription,sep=" ")
  x$bnstreet <- gsub("/", "", x$bnstreet)
  x$bnstreet <- gsub("[.]", "", x$bnstreet)
  x$bnstreet <- gsub("[']", "", x$bnstreet)
  x$bnstreet <- gsub(" ", "", x$bnstreet)
  x$addressf <-paste(x$postcodelocator,x$bnstreet,sep=",")
  
  y<-y[y$property_type=="Flat"|y$property_type=="Maisonette",]
  y$addressfinal <-  paste("APARTMENT ",y$add,sep="")
  y$addressfinal <- gsub("[/]", "", y$addressfinal)
  y$addressfinal <- gsub("[.]", "", y$addressfinal)
  y$addressfinal <- gsub("[']", "", y$addressfinal)
  y$addressfinal <- gsub(" ", "", y$addressfinal)
  y$addressf <- paste(y$postcode,y$addressfinal,sep=",")
  
  taba1 <- inner_join(x,y,by="addressf")
  return(taba1)
}


link269<-function269(add,epc)

link269<-link269[,..needlist1]


link269u<- uniqueresult(link269)
dim(link269u)


link269d <- doubleresult(link269)
dim(link269d)

dim(epc)
#2649854      27
epc <- matchleft(epc,link269)
dim(epc)
#1103953      27



#################### method 270 only works for "SE1 6SH"##################

function270<- function(x,k){
  #x<-x[x$saostartsuffix=="",]
  x$bnstreet <-    paste(x$ss,x$paotext,sep=" ")
  x$bnstreet <- gsub(" ", "", x$bnstreet)
  x$addressf <-paste(x$postcodelocator,x$bnstreet,sep=",")
  
  
  y<-k
  setDT(y)
  y$addressfinal <- trimws(y$add1)
  y[postcode=="SE1 6SH",addressfinal:=gsub("PORCHESTER HOUSE", "PORTCHESTER HOUSE",addressfinal)]
  #y[y$postcode=="SE1 6SH","addressfinal"] <- gsub("PORCHESTER HOUSE", "PORTCHESTER HOUSE",y[y$postcode=="SE1 6SH","addressfinal"])
  y$addressfinal <- gsub(" ", "", y$addressfinal)
  y$addressf <- paste(y$postcode,y$addressfinal,sep=",")
  
  taba1 <- inner_join(x,y,by="addressf")
  
  return(taba1)  
  
  
}


link270<-function270(add,epc)

length(unique(link270$postcode.y))
link270<-link270[,..needlist1]


link270u<- uniqueresult(link270)
dim(link270u)


link270d <- doubleresult(link270)
dim(link270d)

dim(epc)
#
epc <- matchleft(epc,link270)
dim(epc)
#1103672      27

#################### method 271 ##################

function271<- function(x,y){
  
  x<-x[x$paotext!="",]
  x$bnstreet <-    paste(x$saotext,x$paotext,sep=" ")
  x$bnstreet <- gsub("/", "", x$bnstreet)
  x$bnstreet <- gsub("[.]", "", x$bnstreet)
  x$bnstreet <- gsub("[']", "", x$bnstreet)
  x$bnstreet <- gsub(" ", "", x$bnstreet)
  x$addressf <-paste(x$postcodelocator,x$bnstreet,sep=",")
  
  
  
  y$addressfinal <- trimws(y$add1)
  #
  y$addressfinal <- gsub(",", "", y$addressfinal)
  y$addressfinal <- gsub("[/]", "", y$addressfinal)
  y$addressfinal <- gsub("[.]", "", y$addressfinal)
  y$addressfinal <- gsub("[']", "", y$addressfinal)
  y$addressfinal <- gsub(" ", "", y$addressfinal)
  y$addressf <- paste(y$postcode,y$addressfinal,sep=",")
  
  taba1 <- inner_join(x,y,by="addressf")
  
  return(taba1)
  
}


link271<-function271(add,epc)


link271<-link271[,..needlist1]


link271u<- uniqueresult(link271)
dim(link271u)


link271d <- doubleresult(link271)
dim(link271d)

dim(epc)
#
epc <- matchleft(epc,link271)
dim(epc)
#1070505      27


#################### method 272 manually check##################


function272<- function(x,y){
  
  x<-x[x$paotext!="",]
  x$bnstreet <-    paste(x$subbuildingname,x$paotext,sep=" ")
  x$bnstreet <- gsub("/", "", x$bnstreet)
  x$bnstreet <- gsub("[.]", "", x$bnstreet)
  x$bnstreet <- gsub("[']", "", x$bnstreet)
  x$bnstreet <- gsub(" ", "", x$bnstreet)
  x$addressf <-paste(x$postcodelocator,x$bnstreet,sep=",")
  
  
  
  y$addressfinal <- trimws(y$add1)
  
  y$addressfinal <- gsub(",", "", y$addressfinal)
  y$addressfinal <- gsub("[/]", "", y$addressfinal)
  y$addressfinal <- gsub("[.]", "", y$addressfinal)
  y$addressfinal <- gsub("[']", "", y$addressfinal)
  y$addressfinal <- gsub(" ", "", y$addressfinal)
  y$addressf <- paste(y$postcode,y$addressfinal,sep=",")
  
  taba1 <- inner_join(x,y,by="addressf")
  
  return(taba1)
  
  
  
}

link272<-function272(add,epc)

link272<-link272[,..needlist1]


link272u<- uniqueresult(link272)
dim(link272u)


link272d <- doubleresult(link272)
dim(link272d)


dim(epc)
#
epc <- matchleft(epc,link272)
dim(epc)
# 1050152      27



#################### method 273 ##################

function273<- function(x,y){
  
  x<-x[x$buildingname!="",]
  x$bnstreet <-    paste(x$saotext,x$buildingname,sep=" ")
  x$bnstreet <- gsub("/", "", x$bnstreet)
  x$bnstreet <- gsub("[.]", "", x$bnstreet)
  x$bnstreet <- gsub("[']", "", x$bnstreet)
  x$bnstreet <- gsub(" ", "", x$bnstreet)
  x$addressf <-paste(x$postcodelocator,x$bnstreet,sep=",")
  
  
  
  y$addressfinal <- trimws(y$add1)
  #
  y$addressfinal <- gsub(",", "", y$addressfinal)
  y$addressfinal <- gsub("[/]", "", y$addressfinal)
  y$addressfinal <- gsub("[.]", "", y$addressfinal)
  y$addressfinal <- gsub("[']", "", y$addressfinal)
  y$addressfinal <- gsub(" ", "", y$addressfinal)
  y$addressf <- paste(y$postcode,y$addressfinal,sep=",")
  
  taba1 <- inner_join(x,y,by="addressf")
  
  return(taba1)
  
  
}



link273<-function273(add,epc)
dim(link273)
# 
link273<-link273[,..needlist1]

link273u<- uniqueresult(link273)
dim(link273u)
#

link273d <- doubleresult(link273)
dim(link273d)


dim(epc)
#
epc <- matchleft(epc,link273)
dim(epc)
#1043009      27


#################### method 274  ##################

function274<- function(x,y){
  
  
  x<-x[x$buildingname!="",]
  x$bnstreet <-    paste(x$subbuildingname,x$buildingname,sep=" ")
  x$bnstreet <- gsub("/", "", x$bnstreet)
  x$bnstreet <- gsub("[.]", "", x$bnstreet)
  x$bnstreet <- gsub("[']", "", x$bnstreet)
  x$bnstreet <- gsub(" ", "", x$bnstreet)
  x$addressf <-paste(x$postcodelocator,x$bnstreet,sep=",")
  
  
  
  y$addressfinal <- trimws(y$add1)
  #
  y$addressfinal <- gsub(",", "", y$addressfinal)
  y$addressfinal <- gsub("[/]", "", y$addressfinal)
  y$addressfinal <- gsub("[.]", "", y$addressfinal)
  y$addressfinal <- gsub("[']", "", y$addressfinal)
  y$addressfinal <- gsub(" ", "", y$addressfinal)
  y$addressf <- paste(y$postcode,y$addressfinal,sep=",")
  
  taba1 <- inner_join(x,y,by="addressf")
  
  return(taba1)
  
}


link274<-function274(add,epc)



link274<-link274[,..needlist1]


link274u<- uniqueresult(link274)
dim(link274u)


link274d <- doubleresult(link274)


dim(epc)
# 
epc<- matchleft(epc,link274)
dim(epc)
# 1015413      27
#################### method 275 ##################


function275<- function(x,y){
  
  x$bnstreet <-    paste(x$buildingnumber,x$paotext,sep=",")
  
  x$bnstreet <-    paste(x$bnstreet,x$streetdescription,sep=",")
  x$bnstreet <- gsub("[.]", "", x$bnstreet)
  x$bnstreet <- gsub("-", "", x$bnstreet)
  x$bnstreet <- gsub(" ", "", x$bnstreet)
  x$addressf <-paste(x$postcodelocator,x$bnstreet,sep=",")
  
  
  y$addressfinal <-  y$add
  y$addressfinal <- gsub("-", "", y$addressfinal)
  y$addressfinal <- gsub("[.]", "", y$addressfinal)
  y$addressfinal <- gsub(" ", "", y$addressfinal)
  y$addressf <- paste(y$postcode,y$addressfinal,sep=",")
  
  taba1 <- inner_join(x,y,by="addressf")
  return(taba1)
  
}



link275<-function275(add,epc)


link275<-link275[,..needlist1]


link275u<- uniqueresult(link275)
dim(link275u)


link275d <- doubleresult(link275)
dim(link275d)


dim(epc)
#
epc <- matchleft(epc,link275)
dim(epc)
# 1015252      27

#
#################### method 276  only for PL7 1LJ##################

function276<- function(x,k){
  x$bnstreet <-    paste(x$saotext,x$paotext,sep=",")
  x$bnstreet <-    paste(x$bnstreet,x$streetdescription,sep=",")
  x$bnstreet <- gsub("/", "", x$bnstreet)
  x$bnstreet <- gsub("[.]", "", x$bnstreet)
  x$bnstreet <- gsub("[']", "", x$bnstreet)
  x$bnstreet <- gsub(" ", "", x$bnstreet)
  x$addressf <-paste(x$postcodelocator,x$bnstreet,sep=",")
  
  
  y<-k
  setDT(y)
  y[postcode=="PL7 1LJ",add:=gsub("POCKLINGTON RISE", "THE RISE",add)]
  #y[y$postcode=="PL7 1LJ","add"] <- gsub("POCKLINGTON RISE", "THE RISE", y[y$postcode=="PL7 1LJ","add"])
  y$addressfinal <- trimws(y$add)
  
  y$addressfinal <- gsub("[/]", "", y$addressfinal)
  y$addressfinal <- gsub("[.]", "", y$addressfinal)
  y$addressfinal <- gsub("[']", "", y$addressfinal)
  y$addressfinal <- gsub(" ", "", y$addressfinal)
  y$addressf <- paste(y$postcode,y$addressfinal,sep=",")
  
  taba1 <- inner_join(x,y,by="addressf")
  return(taba1) 
}




link276<-function276(add,epc)
dim(link276)
# 

link276<-link276[,..needlist1]



link276u<- uniqueresult(link276)
dim(link276u)
# 

link276d <- doubleresult(link276)
dim(link276d)


epc <- matchleft(epc,link276)
dim(epc)
# 1015236      27

#################### method 277 only for PL7 1LJ##################



function277<- function(x,k){
  
  x$bnstreet <-    paste(x$saotext,x$ paotext,sep=" ")
  x$bnstreet <-    paste(x$bnstreet,x$streetdescription,sep=",")
  x$bnstreet <- gsub(" ", "", x$bnstreet)
  x$addressf <-paste(x$postcodelocator,x$bnstreet,sep=",")
  
  
  y<-k
  setDT(y)
  y[postcode=="PL7 1LJ",add:=gsub("POCKLINGTON RISE", "THE RISE",add)]
  #y[y$postcode=="PL7 1LJ","add"] <- gsub("POCKLINGTON RISE", "THE RISE", y[y$postcode=="PL7 1LJ","add"])
  y$addressfinal <- trimws(y$add)
  
  y$addressfinal <- gsub(" ", "", y$addressfinal)
  y$addressf <- paste(y$postcode,y$addressfinal,sep=",")
  
  taba1 <- inner_join(x,y,by="addressf")
  return(taba1)
  
}

link277<-function277(add,epc)
dim(link277)


link277<-link277[,..needlist1]

link277u<- uniqueresult(link277)
dim(link277u)


link277d <- doubleresult(link277)
dim(link277d)


dim(epc)
# 
epc <- matchleft(epc,link277)
dim(epc)
#1015088      27
#
#################### method 278 ##################

function278<- function(x,y){
  # x$bnstreet <- paste(x$pp,x$streetdescription,sep=",")
  # 
  # x$bnstreet <- gsub(" ", "", x$bnstreet)
  # x$addressf <-paste(x$postcodelocator,x$bnstreet,sep=",")
  # 
  # 
  # y$addressfinal <- trimws(y$add)
  # y[postcode=="LL65 2YN",add:=gsub("TRESEIFION ESTATE", "TRESEIFION",add)]
  # #y$addressfinal <- gsub("[.]", "", y$addressfinal)
  # y$addressfinal <- gsub(" ", "", y$addressfinal)
  # 
  # y$addressf <- paste(y$postcode,y$addressfinal,sep=",")
  # 
  # taba1 <- inner_join(x,y,by="addressf")
  # return(taba1)
  x$bnstreet <-    paste(x$saotext,x$pp,sep=",")
  
  
  x$bnstreet <-    paste(x$bnstreet,x$streetdescription,sep=" ")
  x$bnstreet <- gsub("/", "", x$bnstreet)
  x$bnstreet <- gsub("[.]", "", x$bnstreet)
  x$bnstreet <- gsub("[']", "", x$bnstreet)
  x$bnstreet <- gsub(" ", "", x$bnstreet)
  x$addressf <-paste(x$postcodelocator,x$bnstreet,sep=",")
  

  y<-y[y$property_type=="Flat"|y$property_type=="Maisonette",]
  y$addressfinal <- paste("APARTMENT ",y$add,sep="")
  
  y$addressfinal <- gsub("[/]", "", y$addressfinal)
  y$addressfinal <- gsub("[.]", "", y$addressfinal)
  y$addressfinal <- gsub("[']", "", y$addressfinal)
  y$addressfinal <- gsub(" ", "", y$addressfinal)
  y$addressf <- paste(y$postcode,y$addressfinal,sep=",")
  
  taba1 <- inner_join(x,y,by="addressf")
  
  return(taba1)
}



link278<-function278(add,epc)
dim(link278)


link278<-link278[,..needlist1]



link278u<- uniqueresult(link278)
dim(link278u)


link278d <- doubleresult(link278)
dim(link278d)


dim(epc)
# 
epc <- matchleft(epc,link278)
dim(epc)
# 1015023      27
#################### method 279 ##################
function279<- function(x,y){
  # x$bnstreet <-    paste(x$subbuildingname,x$buildingname,sep=" ")
  # x$bnstreet <-    paste(x$bnstreet,x$buildingnumber,sep=",")
  # x$bnstreet <-    paste(x$bnstreet,x$streetdescription,sep=" ")
  # x$bnstreet <- gsub("/", "", x$bnstreet)
  # x$bnstreet <- gsub("[.]", "", x$bnstreet)
  # x$bnstreet <- gsub("[']", "", x$bnstreet)
  # x$bnstreet <- gsub(" ", "", x$bnstreet)
  # x$addressf <-paste(x$postcodelocator,x$bnstreet,sep=",")
  # 
  # y<-y[y$property_type=="Flat"|y$property_type=="Maisonette",]
  # y$addressfinal <-  paste("APARTMENT ",y$add,sep="")
  # y$addressfinal <- gsub("[/]", "", y$addressfinal)
  # y$addressfinal <- gsub("[.]", "", y$addressfinal)
  # y$addressfinal <- gsub("[']", "", y$addressfinal)
  # y$addressfinal <- gsub(" ", "", y$addressfinal)
  # y$addressf <- paste(y$postcode,y$addressfinal,sep=",")
  # 
  # taba1 <- inner_join(x,y,by="addressf")
  # return(taba1)
  
  x$bnstreet <-    paste(x$subbuildingname,x$buildingname,sep=",")
  x$bnstreet <-    paste(x$bnstreet,x$streetdescription,sep=",")
  x$bnstreet <- gsub("/", "", x$bnstreet)
  x$bnstreet <- gsub("[.]", "", x$bnstreet)
  x$bnstreet <- gsub("[']", "", x$bnstreet)
  x$bnstreet <- gsub(" ", "", x$bnstreet)
  x$addressf <-paste(x$postcodelocator,x$bnstreet,sep=",")
  
  y$addressfinal <- trimws(y$add)
  y$addressfinal <- gsub("APT", "FLAT", y$addressfinal)
  y$addressfinal <- gsub("[/]", "", y$addressfinal)
  y$addressfinal <- gsub("[.]", "", y$addressfinal)
  y$addressfinal <- gsub("[']", "", y$addressfinal)
  y$addressfinal <- gsub(" ", "", y$addressfinal)
  y$addressf <- paste(y$postcode,y$addressfinal,sep=",")
  
  taba1 <- inner_join(x,y,by="addressf")
  
  return(taba1)
  
  
}


link279<-function279(add,epc)

link279<-link279[,..needlist1]


link279u<- uniqueresult(link279)
dim(link279u)


link279d <- doubleresult(link279)
dim(link279d)

dim(epc)
#
epc <- matchleft(epc,link279)
dim(epc)
#1015023      27


#################### method 280 ##################

function280<- function(x,y){
  x$bnstreet <-    paste(x$subbuildingname,x$buildingname,sep=",")
  x$bnstreet <-    paste(x$bnstreet,x$buildingnumber,sep=" ")
  x$bnstreet <-    paste(x$bnstreet,x$streetdescription,sep=" ")
  #x$bnstreet <- gsub("['] ", "", x$bnstreet)
  x$bnstreet <- gsub("/", "", x$bnstreet)
  x$bnstreet <- gsub("[.]", "", x$bnstreet)
  x$bnstreet <- gsub("[']", "", x$bnstreet)
  x$bnstreet <- gsub(" ", "", x$bnstreet)
  x$addressf <-paste(x$postcodelocator,x$bnstreet,sep=",")
  
  
  y$addressfinal <- trimws(y$add)
  #y$addressfinal <- gsub("[']", "", y$addressfinal)
  y$addressfinal <- gsub("[/]", "", y$addressfinal)
  y$addressfinal <- gsub("[.]", "", y$addressfinal)
  y$addressfinal <- gsub("[']", "", y$addressfinal)
  
  y$addressfinal <- gsub(" ", "", y$addressfinal)
  y$addressf <- paste(y$postcode,y$addressfinal,sep=",")
  
  taba1 <- inner_join(x,y,by="addressf")
  return(taba1)
  
  
}


link280<-function280(add,epc)

length(unique(link280$lmk_key))
link280<-link280[,..needlist1]


link280u<- uniqueresult(link280)
dim(link280u)


link280d <- doubleresult(link280)
dim(link280d)

dim(epc)
#
epc <- matchleft(epc,link280)
dim(epc)
#1015013      27

#################### method 281 old 141 , manually check##################

function281<- function(x,y){
  
  x<-x[x$paotext!="",]
  
  x$bnstreet <- x$saotext
  
  x$bnstreet <- gsub(" ", "", x$bnstreet)
  x$addressf <-paste(x$postcodelocator,x$bnstreet,sep=",")
  
  y<-y[y$property_type=="Flat"|y$property_type=="Maisonette",]
  y$addressfinal <- trimws(y$add1)
  y$addressfinal <- word(y$addressfinal,1,2)
  y$addressfinal <- gsub(",", "", y$addressfinal)
  y$addressfinal <- gsub(" ", "", y$addressfinal)
  y$addressf <- paste(y$postcode,y$addressfinal,sep=",")
  
  taba1 <- inner_join(x,y,by="addressf")
  
  return(taba1)
  
  
}


link281<-function281(add,epc)


link281<-link281[,..needlist1]


link281u<- uniqueresult(link281)
dim(link281u)


link281d <- doubleresult(link281)
dim(link281d)

dim(epc)
#
epc <- matchleft(epc,link281)
dim(epc)
#980468     27


#################### method 282 ##################


function282<- function(x,y){
  
  x$bnstreet <-    paste(x$ss,x$streetdescription,sep=",")
  x$bnstreet <- gsub("/", "", x$bnstreet)
  x$bnstreet <- gsub("[.]", "", x$bnstreet)
  x$bnstreet <- gsub("[']", "", x$bnstreet)
  
  x$bnstreet <- gsub(" ", "", x$bnstreet)
  x$addressf <-paste(x$postcodelocator,x$bnstreet,sep=",")
  
  
  y$addressfinal <- trimws(y$add)
  y$addressfinal <- gsub("[/]", "", y$addressfinal)
  y$addressfinal <- gsub("[.]", "", y$addressfinal)
  y$addressfinal <- gsub("[']", "", y$addressfinal)
  
  #y$addressfinal <- gsub("[.]", "", y$addressfinal)
  y$addressfinal <- gsub(" ", "", y$addressfinal)
  y$addressf <- paste(y$postcode,y$addressfinal,sep=",")
  
  taba1 <- inner_join(x,y,by="addressf")
  return(taba1)
  
  
}

link282<-function282(add,epc)

link282<-link282[,..needlist1]
length(unique(link282$lmk_key))

link282u<- uniqueresult(link282)
dim(link282u)


link282d <- doubleresult(link282)
dim(link282d)


dim(epc)
#
epc <- matchleft(epc,link282)
dim(epc)
# 980374     27



#################### method 283 ##################

function283<- function(x,y){
  
  #flat a to a 
  x<-x[x$paostartsuffix=="",]
  #x <- x[grepl("FLAT",x$saotext),]
  x1<-x
  setDT(x1)
  
  
  x1<-x1[grepl("FLAT A",saotext),new:= "A"]
  x1<-x1[grepl("FLAT B",saotext),new:= "B"]
  x1<-x1[grepl("FLAT C",saotext),new:= "C"]
  x1<-x1[grepl("FLAT D",saotext),new:= "D"]
  x1<-x1[grepl("FLAT E",saotext),new:= "E"]
  x1<-x1[grepl("FLAT F",saotext),new:= "F"]
  x1<-x1[grepl("FLAT H",saotext),new:= "H"]
  x1<-x1[grepl("FLAT G",saotext),new:= "G"]
  
  x1<-x1[grepl("FLAT I",saotext),new:= "I"]
  x1<-x1[grepl("FLAT J",saotext),new:= "J"]
  x1<-x1[grepl("FLAT K",saotext),new:= "K"]
  x1<-x1[grepl("FLAT G",saotext),new:= "G"]
  
  x1$bnstreet <-    paste(x1$paostartnumber,x1$saotext,sep="")
  x1$bnstreet <-    paste(x1$bnstreet,x1$streetdescription,sep=",")
  
  x1$bnstreet <- gsub(" ", "", x1$bnstreet)
  x1$addressf <-paste(x1$postcodelocator,x1$bnstreet,sep=",")
  
  
  y$addressfinal <-  trimws(y$add)
  
  y$addressfinal <- gsub(" ", "", y$addressfinal)
  y$addressf <- paste(y$postcode,y$addressfinal,sep=",")
  
  taba1 <- inner_join(x1,y,by="addressf")
  return(taba1)
  
  
}



link283<-function283(add,epc)
dim(link283)
#

length(unique(link283$lmk_key))

link283<-link283[,..needlist1]

link283u<- uniqueresult(link283)
dim(link283u)
#

link283d <- doubleresult(link283)
dim(link283d)


dim(epc)
#
epc <- matchleft(epc,link283)
dim(epc)
#  980349     27


#################### method 284  ##################

function284<- function(x,y){
  x<-x[x$paostartsuffix=="",]
  x <- x[grepl("FLAT",x$saotext),]
  x1<-x
  setDT(x1)
  x1<-x1[!grepl("FLAT [A-Z]{2}",saotext),]
  x1<-x1[!grepl("FLAT [A-Z]+[0-9]",saotext),]
  
  x1<-x1[grepl("FLAT A",saotext),new:= "A"]
  x1<-x1[grepl("FLAT B",saotext),new:= "B"]
  x1<-x1[grepl("FLAT C",saotext),new:= "C"]
  x1<-x1[grepl("FLAT D",saotext),new:= "D"]
  x1<-x1[grepl("FLAT E",saotext),new:= "E"]
  x1<-x1[grepl("FLAT F",saotext),new:= "F"]
  x1<-x1[grepl("FLAT H",saotext),new:= "H"]
  x1<-x1[grepl("FLAT G",saotext),new:= "G"]
  
  x1<-x1[grepl("FLAT I",saotext),new:= "I"]
  x1<-x1[grepl("FLAT J",saotext),new:= "J"]
  x1<-x1[grepl("FLAT K",saotext),new:= "K"]
  x1<-x1[grepl("FLAT l",saotext),new:= "l"]
  
  x1$bnstreet <-    paste(x1$paostartnumber,x1$new,sep="")
  x1$bnstreet <-    paste(x1$bnstreet,x1$streetdescription,sep=" ")
  
  #x1$bnstreet <- gsub(" ", "", x1$bnstreet)
  x1$addressf <-paste(x1$postcodelocator,x1$bnstreet,sep=",")
  
  y$addressfinal <-  trimws(y$add)
  
  #y$addressfinal <- gsub(" ", "", y$addressfinal)
  y$addressf <- paste(y$postcode,y$addressfinal,sep=",")
  
  taba1 <- inner_join(x1,y,by="addressf")
  return(taba1)
  
  
}


link284<-function284(add,epc)

length(unique(link284$lmk_key))


link284<-link284[,..needlist1]


link284u<- uniqueresult(link284)
dim(link284u)


link284d <- doubleresult(link284)

dim(epc)
# 
epc<- matchleft(epc,link284)
dim(epc)
#  975790     27
#################### method 285 ##################


function285<- function(x,y){
  
  #<-x[x$paostartsuffix=="",]
  x1<-x
  
  x1$bnstreet <-    paste(x1$pp,x1$saotext,sep="")
  x1$bnstreet <-    paste(x1$bnstreet,x1$streetdescription,sep=" ")
  x$bnstreet <- gsub("/", "", x$bnstreet)
  x$bnstreet <- gsub("[.]", "", x$bnstreet)
  x$bnstreet <- gsub("[']", "", x$bnstreet)
  #x1$bnstreet <- gsub(" ", "", x1$bnstreet)
  x1$addressf <-paste(x1$postcodelocator,x1$bnstreet,sep=",")
  
  y$addressfinal <-  trimws(y$add)
  y$addressfinal <- gsub("[/]", "", y$addressfinal)
  y$addressfinal <- gsub("[.]", "", y$addressfinal)
  y$addressfinal <- gsub("[']", "", y$addressfinal)
  
  #y$addressfinal <- gsub(" ", "", y$addressfinal)
  y$addressf <- paste(y$postcode,y$addressfinal,sep=",")
  
  taba1 <- inner_join(x1,y,by="addressf")
  return(taba1)
  
}



link285<-function285(add,epc)


link285<-link285[,..needlist1]


link285u<- uniqueresult(link285)
dim(link285u)


link285d <- doubleresult(link285)
dim(link285d)


dim(epc)
#
epc <- matchleft(epc,link285)
dim(epc)
#975681     27

#
#################### method 286 ##################

function286<- function(x,y){
  x<-x[x$paotext!="",]
  #x$bnstreet <- paste("FLAT ",x$saotext,sep="")
  x$bnstreet <- x$saotext
  x$bnstreet <- gsub("STUDIO", "FLAT", x$bnstreet)
  x$bnstreet <- gsub("/", "", x$bnstreet)
  x$bnstreet <- gsub("[.]", "", x$bnstreet)
  x$bnstreet <- gsub("[']", "", x$bnstreet)
  x$bnstreet <- gsub(" ", "", x$bnstreet)
  x$addressf <-paste(x$postcodelocator,x$bnstreet,sep=",")
  
  y<-y[y$property_type=="Flat"|y$property_type=="Maisonette",]
  y$addressfinal <- trimws(y$add1)
  y$addressfinal <- word(y$addressfinal,1,2)
  y$addressfinal <- gsub("[/]", "", y$addressfinal)
  y$addressfinal <- gsub("[.]", "", y$addressfinal)
  y$addressfinal <- gsub("[']", "", y$addressfinal)
  y$addressfinal <- gsub(",", "", y$addressfinal)
  y$addressfinal <- gsub(" ", "", y$addressfinal)
  y$addressf <- paste(y$postcode,y$addressfinal,sep=",")
  
  taba1 <- inner_join(x,y,by="addressf")
  
  return(taba1)
  
}




link286<-function286(add,epc)
dim(link286)
# 

link286<-link286[,..needlist1]



link286u<- uniqueresult(link286)
dim(link286u)
# 

link286d <- doubleresult(link286)
dim(link286d)


epc <- matchleft(epc,link286)
dim(epc)
# 974416     27

#################### method 287 ##################



function287<- function(x,y){
  
  x<-x[x$paotext!="",]
  #x$bnstreet <- paste("FLAT ",x$saotext,sep="")
  x$bnstreet <- paste(x$saotext,x$paotext,sep=" ")
  x$bnstreet <- gsub("APARTMENT", "0", x$bnstreet)
  x$bnstreet <- gsub("/", "", x$bnstreet)
  x$bnstreet <- gsub("[.]", "", x$bnstreet)
  x$bnstreet <- gsub("[']", "", x$bnstreet)
  x$bnstreet <- gsub(" ", "", x$bnstreet)
  x$addressf <-paste(x$postcodelocator,x$bnstreet,sep=",")
  
  y<-y[y$property_type=="Flat"|y$property_type=="Maisonette",]
  y$addressfinal <- trimws(y$add1)
  y$addressfinal <- gsub("[/]", "", y$addressfinal)
  y$addressfinal <- gsub("[.]", "", y$addressfinal)
  y$addressfinal <- gsub("[']", "", y$addressfinal)
  y$addressfinal <- gsub(",", "", y$addressfinal)
  y$addressfinal <- gsub(" ", "", y$addressfinal)
  y$addressf <- paste(y$postcode,y$addressfinal,sep=",")
  
  taba1 <- inner_join(x,y,by="addressf")
  
  return(taba1)
  
}

link287<-function287(add,epc)
dim(link287)


link287<-link287[,..needlist1]

link287u<- uniqueresult(link287)
dim(link287u)


link287d <- doubleresult(link287)
dim(link287d)


dim(epc)
# 
epc <- matchleft(epc,link287)
dim(epc)
#974332     27
#
#################### method 288 ##################

function288<- function(x,y){
  
  x$bnstreet <- x$pp
  
  x$bnstreet <- gsub(" ", "", x$bnstreet)
  x$addressf <-paste(x$postcodelocator,x$bnstreet,sep=",")
  
  #y<-y[y$property_type=="Flat"|y$property_type=="Maisonette",]
  y$addressfinal <- trimws(y$add1)
  y$addressfinal <- word(y$addressfinal,1)
  y$addressfinal <- gsub(",", "", y$addressfinal)
  y$addressfinal <- gsub(" ", "", y$addressfinal)
  y$addressf <- paste(y$postcode,y$addressfinal,sep=",")
  
  taba1 <- inner_join(x,y,by="addressf")
  
  return(taba1) 
}



link288<-function288(add,epc)
dim(link288)


link288<-link288[,..needlist1]



link288u<- uniqueresult(link288)
dim(link288u)


link288d <- doubleresult(link288)
dim(link288d)


dim(epc)
# 
epc <- matchleft(epc,link288)
dim(epc)
#863642     27
#################### method 289 ##################
function289<- function(x,y){
  x<-x[x$subbuildingname!="",]
  x<- x[grepl("^\\d+",x$subbuildingname),]
  
  
  x$bnstreet <- x$subbuildingname
  
  x$bnstreet <- gsub(" ", "", x$bnstreet)
  x$addressf <-paste(x$postcodelocator,x$bnstreet,sep=",")
  
  
  y$addressfinal <- trimws(y$add1)
  y$addressfinal <- word(y$addressfinal,1)
  y$addressfinal <- gsub(",", "", y$addressfinal)
  y$addressfinal <- gsub(" ", "", y$addressfinal)
  y$addressf <- paste(y$postcode,y$addressfinal,sep=",")
  
  taba1 <- inner_join(x,y,by="addressf")
  
  return(taba1)
}


link289<-function289(add,epc)

link289<-link289[,..needlist1]


link289u<- uniqueresult(link289)
dim(link289u)


link289d <- doubleresult(link289)
dim(link289d)

dim(epc)
#2849854      28
epc <- matchleft(epc,link289)
dim(epc)
#862193     27


#################### method 290 ##################

function290<- function(x,y){
  
  x<-x[x$buildingnumber!="",]
  x<- x[grepl("^\\d+",x$buildingnumber),]
  
  x$bnstreet <- x$buildingnumber 
  
  x$bnstreet <- gsub(" ", "", x$bnstreet)
  x$addressf <-paste(x$postcodelocator,x$bnstreet,sep=",")
  
  #y<-y[y$property_type=="Flat"|y$property_type=="Maisonette",]
  y$addressfinal <- trimws(y$add1)
  y$addressfinal <- word(y$addressfinal,1)
  y$addressfinal <- gsub(",", "", y$addressfinal)
  y$addressfinal <- gsub(" ", "", y$addressfinal)
  y$addressf <- paste(y$postcode,y$addressfinal,sep=",")
  
  taba1 <- inner_join(x,y,by="addressf")
  
  return(taba1)
  
  
  
}


link290<-function290(add,epc)

length(unique(link290$lmk_key))
link290<-link290[,..needlist1]


link290u<- uniqueresult(link290)
dim(link290u)


link290d <- doubleresult(link290)
dim(link290d)

dim(epc)
#
epc <- matchleft(epc,link290)
dim(epc)
#847384     27

#################### method 291 ##################

function291<- function(x,y){
  
  
  x$bnstreet <- paste(x$saostartnumber,x$saostartsuffix, sep=" ")
  x<- x[grepl("^\\d+",x$bnstreet),]
  #x$bnstreet <- gsub("STUDIO", "FLAT", x$bnstreet)
  x$bnstreet <- gsub("/", "", x$bnstreet)
  x$bnstreet <- gsub("[.]", "", x$bnstreet)
  x$bnstreet <- gsub("[']", "", x$bnstreet)
  x$bnstreet <- gsub(" ", "", x$bnstreet)
  x$addressf <-paste(x$postcodelocator,x$bnstreet,sep=",")
  
  #y<-y[y$property_type=="Flat"|y$property_type=="Maisonette",]
  y$addressfinal <- trimws(y$add1)
  y$addressfinal <- word(y$addressfinal,1)
  y$addressfinal <- gsub("[/]", "", y$addressfinal)
  y$addressfinal <- gsub("[.]", "", y$addressfinal)
  y$addressfinal <- gsub("[']", "", y$addressfinal)
  y$addressfinal <- gsub(",", "", y$addressfinal)
  y$addressfinal <- gsub(" ", "", y$addressfinal)
  y$addressf <- paste(y$postcode,y$addressfinal,sep=",")
  
  taba1 <- inner_join(x,y,by="addressf")
  
  return(taba1)
  
  
}


link291<-function291(add,epc)


link291<-link291[,..needlist1]


link291u<- uniqueresult(link291)
dim(link291u)


link291d <- doubleresult(link291)
dim(link291d)

dim(epc)
#
epc <- matchleft(epc,link291)
dim(epc)
#828053     27


#################### method 292 ##################


function292<- function(x,y){
  
  
  x<-x[x$subbuildingname!="",]
  
  x$bnstreet <- paste("APARTMENT ",x$subbuildingname,sep="")
  x$bnstreet <- gsub("/", "", x$bnstreet)
  x$bnstreet <- gsub("[.]", "", x$bnstreet)
  x$bnstreet <- gsub("[']", "", x$bnstreet)
  x$bnstreet <- gsub(" ", "", x$bnstreet)
  x$addressf <-paste(x$postcodelocator,x$bnstreet,sep=",")
  
  y<-y[y$property_type=="Flat"|y$property_type=="Maisonette",]
  y$addressfinal <- trimws(y$add1)
  y$addressfinal <- word(y$addressfinal,1,2)
  y$addressfinal <- gsub("[/]", "", y$addressfinal)
  y$addressfinal <- gsub("[.]", "", y$addressfinal)
  y$addressfinal <- gsub("[']", "", y$addressfinal)
  y$addressfinal <- gsub(",", "", y$addressfinal)
  y$addressfinal <- gsub(" ", "", y$addressfinal)
  y$addressf <- paste(y$postcode,y$addressfinal,sep=",")
  
  taba1 <- inner_join(x,y,by="addressf")
  
  return(taba1)
  
}

link292<-function292(add,epc)

link292<-link292[,..needlist1]


link292u<- uniqueresult(link292)
dim(link292u)


link292d <- doubleresult(link292)
dim(link292d)


dim(epc)
#
epc <- matchleft(epc,link292)
dim(epc)
# 826591     27



#################### method 293 ##################

function293<- function(x,y){
  #x<-x[x$saostartnumber!="",]
  
  x$bnstreet <- paste("APARTMENT ",x$ss,sep="")
  #x$bnstreet <- paste(x$bnstreet ,x$saostartsuffix,sep="")
  
  x$bnstreet <- gsub(" ", "", x$bnstreet)
  x$addressf <-paste(x$postcodelocator,x$bnstreet,sep=",")
  
  y<-y[y$property_type=="Flat"|y$property_type=="Maisonette",]
  y$addressfinal <- trimws(y$add1)
  y$addressfinal <- word(y$addressfinal,1,2)
  y$addressfinal <- gsub(",", "", y$addressfinal)
  y$addressfinal <- gsub(" ", "", y$addressfinal)
  y$addressf <- paste(y$postcode,y$addressfinal,sep=",")
  
  taba1 <- inner_join(x,y,by="addressf")
  
  return(taba1)
  
  
}


link293<-function293(add,epc)
dim(link293)
# 
link293<-link293[,..needlist1]

link293u<- uniqueresult(link293)
dim(link293u)
#

link293d <- doubleresult(link293)
dim(link293d)


dim(epc)
#
epc <- matchleft(epc,link293)
dim(epc)
#  823975     27


#################### method 294  ##################

function294<- function(x,y){
  
  x<-x[x$saotext!="",]
  
  x$bnstreet <- x$saotext
  
  x$bnstreet <- gsub("CHALET ", "", x$bnstreet)
  x$bnstreet <- gsub(" ", "", x$bnstreet)
  x$addressf <-paste(x$postcodelocator,x$bnstreet,sep=",")
  
  y<-y[y$property_type=="Flat"|y$property_type=="Maisonette",]
  y$addressfinal <- trimws(y$add1)
  y$addressfinal <- word(y$addressfinal,1)
  y$addressfinal <- gsub(",", "", y$addressfinal)
  y$addressfinal <- gsub(" ", "", y$addressfinal)
  y$addressf <- paste(y$postcode,y$addressfinal,sep=",")
  
  taba1 <- inner_join(x,y,by="addressf")
  
  taba1<- taba1[!grepl("FLAT",taba1$saotext),]
  taba1<- taba1[!grepl("BASEMENT",taba1$saotext),]
  taba1<- taba1[!grepl("MAISONETTE",taba1$saotext),]
  taba1<- taba1[!grepl("ROOM",taba1$saotext),]
  #taba1<- taba1[!grepl("UNIT",taba1$saotext),]
  taba1<- taba1[!grepl("APARTMENT",taba1$saotext),]
  taba1<- taba1[!grepl("GROUND",taba1$saotext),]
  #taba1<- taba1[!grepl("UNIT",taba1$saotext),]
  return(taba1)
  
}


link294<-function294(add,epc)



link294<-link294[,..needlist1]


link294u<- uniqueresult(link294)
dim(link294u)


link294d <- doubleresult(link294)

dim(epc)
# 
epc<- matchleft(epc,link294)
dim(epc)
#  2292964      29
#################### method 295 ##################


function295<- function(x,y){
  
  x<-x[x$saostartnumber!="",]
  #x<- x[!grepl("^\\d+",x$subbuildingname),]
  
  #x$bnstreet <- paste("FLAT ",x$saotext,sep="")
  x$bnstreet <- paste("FLAT ",x$ss,sep="")
  
  x$bnstreet <- paste(x$bnstreet ,x$paotext,sep="")
  x$bnstreet <- gsub(" ", "", x$bnstreet)
  x$addressf <-paste(x$postcodelocator,x$bnstreet,sep=",")
  
  y<-y[y$property_type=="Flat"|y$property_type=="Maisonette",]
  
  y$addressfinal <- paste(y$add1,y$add2,sep="")
  #y$addressfinal <- gsub(",", "", y$addressfinal)
  y$addressfinal <- gsub("[']", "", y$addressfinal)
  y$addressfinal <- gsub("[.]", "", y$addressfinal)
  y$addressfinal <- gsub(" ", "", y$addressfinal)
  y$addressf <- paste(y$postcode,y$addressfinal,sep=",")
  
  taba1 <- inner_join(x,y,by="addressf")
  
  return(taba1)
  
}



link295<-function295(add,epc)


link295<-link295[,..needlist1]


link295u<- uniqueresult(link295)
dim(link295u)


link295d <- doubleresult(link295)
dim(link295d)


dim(epc)
#
epc <- matchleft(epc,link295)
dim(epc)
#810518     27
#
#################### method 296 ##################

function296<- function(x,y){
  x<-x[x$saostartnumber!="",]
  
  x$bnstreet <- paste("FLAT ",x$ss,sep="")
  #x$bnstreet <- paste(x$bnstreet ,x$saostartsuffix,sep="")
  x$bnstreet <- paste(x$bnstreet ,x$paotext,sep="")
  x$bnstreet <- gsub(" ", "", x$bnstreet)
  x$addressf <-paste(x$postcodelocator,x$bnstreet,sep=",")
  
  y<-y[y$property_type=="Flat"|y$property_type=="Maisonette",]
  #y$addressfinal <- trimws(y$add1)
  y$addressfinal <- paste(y$add1,y$add2,sep="")
  y$addressfinal <- gsub(",", "", y$addressfinal)
  y$addressfinal <- gsub("[']", "", y$addressfinal)
  y$addressfinal <- gsub("[.]", "", y$addressfinal)
  y$addressfinal <- gsub(" ", "", y$addressfinal)
  y$addressf <- paste(y$postcode,y$addressfinal,sep=",")
  
  taba1 <- inner_join(x,y,by="addressf")
  
  
  
  return(taba1)
  
  
}




link296<-function296(add,epc)
dim(link296)
# 

link296<-link296[,..needlist1]



link296u<- uniqueresult(link296)
dim(link296u)
# 

link296d <- doubleresult(link296)
dim(link296d)


epc <- matchleft(epc,link296)
dim(epc)
#810448     27
######################change ####################
#################### method 297 ##################



function297<- function(x,y){
  
  x<-x[x$saostartnumber!="",]
  
  x$bnstreet <- paste("FLAT ",x$ss,sep=" ")
  
  x$bnstreet <- paste(x$bnstreet ,x$pp,sep=" ")
  
  x$bnstreet <- paste(x$bnstreet ,x$streetdescription,sep=" ")
  x$bnstreet <- gsub(" ", "", x$bnstreet)
  x$addressf <-paste(x$postcodelocator,x$bnstreet,sep=",")
  
  y<-y[y$property_type=="Flat"|y$property_type=="Maisonette",]
  
  y$addressfinal <- paste(y$add1,y$add2,sep=" ")
  #y$addressfinal <- gsub(",", "", y$addressfinal)
  y$addressfinal <- gsub("[']", "", y$addressfinal)
  y$addressfinal <- gsub("[.]", "", y$addressfinal)
  y$addressfinal <- gsub(" ", "", y$addressfinal)
  y$addressf <- paste(y$postcode,y$addressfinal,sep=",")
  
  taba1 <- inner_join(x,y,by="addressf")
  
  
  
  return(taba1)
  
  
}

link297<-function297(add,epc)
dim(link297)


link297<-link297[,..needlist1]

link297u<- uniqueresult(link297)
dim(link297u)


link297d <- doubleresult(link297)
dim(link297d)


dim(epc)
# 
epc <- matchleft(epc,link297)
dim(epc)
#807621     27
#
#################### method 298 ##################

function298<- function(x,y){
  
  x<-x[x$saostartnumber!="",]
  #x<- x[!grepl("^\\d+",x$subbuildingname),]
  
  x$bnstreet <- paste("FLAT ",x$ss,sep="")
  
  x$bnstreet <- paste(x$bnstreet ,x$paostartnumber,sep="")
  x$bnstreet <- paste(x$bnstreet ,x$paoendnumber,sep="-")
  x$bnstreet <- paste(x$bnstreet ,x$streetdescription,sep=" ")
  x$bnstreet <- gsub("/", "", x$bnstreet)
  x$bnstreet <- gsub("[.]", "", x$bnstreet)
  x$bnstreet <- gsub("[']", "", x$bnstreet)
  x$bnstreet <- gsub(" ", "", x$bnstreet)
  x$addressf <-paste(x$postcodelocator,x$bnstreet,sep=",")
  
  y<-y[y$property_type=="Flat"|y$property_type=="Maisonette",]
  
  y$addressfinal <- paste(y$add1,y$add2,sep=" ")
  
  y$addressfinal <- gsub("[/]", "", y$addressfinal)
  y$addressfinal <- gsub("[']", "", y$addressfinal)
  y$addressfinal <- gsub("[.]", "", y$addressfinal)
  y$addressfinal <- gsub(",", "", y$addressfinal)
  y$addressfinal <- gsub(" ", "", y$addressfinal)
  y$addressf <- paste(y$postcode,y$addressfinal,sep=",")
  
  taba1 <- inner_join(x,y,by="addressf")
  
  
  
  return(taba1)
}



link298<-function298(add,epc)
dim(link298)


link298<-link298[,..needlist1]



link298u<- uniqueresult(link298)
dim(link298u)


link298d <- doubleresult(link298)
dim(link298d)


dim(epc)
# 
epc <- matchleft(epc,link298)
dim(epc)
#2299509      29
#################### method 299 ##################
function299<- function(x,y){
  x<-x[x$saostartnumber!="",]
  
  x<-x[x$paostartnumber!="",]
  #x$bnstreet <- paste("FLAT ",x$saotext,sep="")
  x$bnstreet <- paste("FLAT ",x$ss,sep=" ")
  x$bnstreet <- paste(x$bnstreet ,x$pp,sep=" ")
  
  x$bnstreet <- paste(x$bnstreet ,x$streetdescription,sep=" ")
  x$bnstreet <- gsub("/", "", x$bnstreet)
  x$bnstreet <- gsub("[.]", "", x$bnstreet)
  x$bnstreet <- gsub("[']", "", x$bnstreet)
  x$bnstreet <- gsub(" ", "", x$bnstreet)
  x$addressf <-paste(x$postcodelocator,x$bnstreet,sep=",")
  
  y<-y[y$property_type=="Flat"|y$property_type=="Maisonette",]
  #y$addressfinal <- trimws(y$add1)
  y$addressfinal <- paste(y$add1,y$add3,sep=",")
  y$addressfinal <- gsub("[/]", "", y$addressfinal)
  y$addressfinal <- gsub("[']", "", y$addressfinal)
  y$addressfinal <- gsub("[.]", "", y$addressfinal)
  y$addressfinal <- gsub(",", "", y$addressfinal)
  y$addressfinal <- gsub(" ", "", y$addressfinal)
  y$addressf <- paste(y$postcode,y$addressfinal,sep=",")
  
  taba1 <- inner_join(x,y,by="addressf")
  
  
}


link299<-function299(add,epc)

link299<-link299[,..needlist1]


link299u<- uniqueresult(link299)
dim(link299u)


link299d <- doubleresult(link299)
dim(link299d)

dim(epc)
#
epc <- matchleft(epc,link299)
dim(epc)
#806982     27
#################### sum up############



link250u$method<-"link250u"
link251u$method<-"link251u"
link252u$method<-"link252u"
link253u$method<-"link253u"
link254u$method<-"link254u"
link255u$method<-"link255u"
link256u$method<-"link256u"
link257u$method<-"link257u"
link258u$method<-"link258u"
link259u$method<-"link259u"
link260u$method<-"link260u"
link261u$method<-"link261u"
link262u$method<-"link262u"
link263u$method<-"link263u"
link264u$method<-"link264u"
link265u$method<-"link265u"
link266u$method<-"link266u"
link267u$method<-"link267u"
link268u$method<-"link268u"
link269u$method<-"link269u"
link270u$method<-"link270u"
link271u$method<-"link271u"
link272u$method<-"link272u"
link273u$method<-"link273u"
link274u$method<-"link274u"
link275u$method<-"link275u"
link276u$method<-"link276u"
link277u$method<-"link277u"
link278u$method<-"link278u"
link279u$method<-"link279u"
link280u$method<-"link280u"
link281u$method<-"link281u"
link282u$method<-"link282u"
link283u$method<-"link283u"
link284u$method<-"link284u"
link285u$method<-"link285u"
link286u$method<-"link286u"
link287u$method<-"link287u"
link288u$method<-"link288u"
link289u$method<-"link289u"
link290u$method<-"link290u"
link291u$method<-"link291u"
link292u$method<-"link292u"
link293u$method<-"link293u"
link294u$method<-"link294u"
link295u$method<-"link295u"
link296u$method<-"link296u"
link297u$method<-"link297u"
link298u$method<-"link298u"
link299u$method<-"link299u"




link250d$method<-"link250d"
link251d$method<-"link251d"
link252d$method<-"link252d"
link253d$method<-"link253d"
link254d$method<-"link254d"
link255d$method<-"link255d"
link256d$method<-"link256d"
link257d$method<-"link257d"
link258d$method<-"link258d"
link259d$method<-"link259d"
link260d$method<-"link260d"
link261d$method<-"link261d"
link262d$method<-"link262d"
link263d$method<-"link263d"
link264d$method<-"link264d"
link265d$method<-"link265d"
link266d$method<-"link266d"
link267d$method<-"link267d"
link268d$method<-"link268d"
link269d$method<-"link269d"
link270d$method<-"link270d"
link271d$method<-"link271d"
link272d$method<-"link272d"
link273d$method<-"link273d"
link274d$method<-"link274d"
link275d$method<-"link275d"
link276d$method<-"link276d"
link277d$method<-"link277d"
link278d$method<-"link278d"
link279d$method<-"link279d"
link280d$method<-"link280d"
link281d$method<-"link281d"
link282d$method<-"link282d"
link283d$method<-"link283d"
link284d$method<-"link284d"
link285d$method<-"link285d"
link286d$method<-"link286d"
link287d$method<-"link287d"
link288d$method<-"link288d"
link289d$method<-"link289d"
link290d$method<-"link290d"
link291d$method<-"link291d"
link292d$method<-"link292d"
link293d$method<-"link293d"
link294d$method<-"link294d"
link295d$method<-"link295d"
link296d$method<-"link296d"
link297d$method<-"link297d"
link298d$method<-"link298d"
link299d$method<-"link299d"

dim(link250u)[1]+dim(link251u)[1]+dim(link252u)[1]+dim(link253u)[1]+dim(link254u)[1]+dim(link255u)[1]+dim(link256u)[1]+dim(link257u)[1]+dim(link258u)[1]+dim(link259u)[1]+
  dim(link260u)[1]+dim(link261u)[1]+dim(link262u)[1]+dim(link263u)[1]+dim(link264u)[1]+dim(link265u)[1]+dim(link266u)[1]+dim(link267u)[1]+dim(link268u)[1]+dim(link269u)[1]+
  dim(link270u)[1]+dim(link271u)[1]+dim(link272u)[1]+dim(link273u)[1]+dim(link274u)[1]+dim(link275u)[1]+dim(link276u)[1]+dim(link277u)[1]+dim(link278u)[1]+dim(link279u)[1]+
  dim(link280u)[1]+dim(link281u)[1]+dim(link282u)[1]+dim(link283u)[1]+dim(link284u)[1]+dim(link285u)[1]+dim(link286u)[1]+dim(link287u)[1]+dim(link288u)[1]+dim(link289u)[1]+
  dim(link290u)[1]+dim(link291u)[1]+dim(link292u)[1]+dim(link293u)[1]+dim(link294u)[1]+dim(link295u)[1]+dim(link296u)[1]+dim(link297u)[1]+dim(link298u)[1]+dim(link299u)[1]

# 336433


l250_299u = list(link250u,link251u,link252u,link253u,link254u,link255u,link256u,link257u,link258u,link259u,
                 link260u,link261u,link262u,link263u,link264u,link265u,link266u,link267u,link268u,link269u,
                 link270u,link271u,link272u,link273u,link274u,link275u,link276u,link277u,link278u,link279u,
                 link280u,link281u,link282u,link283u,link284u,link285u,link286u,link287u,link288u,link289u,
                 link290u,link291u,link292u,link293u,link294u,link295u,link296u,link297u,link298u,link299u)

#
link250_299u<- rbindlist(l250_299u)
dim(link250_299u)
# 336433
#334859
length(unique(link250_299u$lmk_key))
# 336433
length(unique(link250_299u$method))
# 50
l250_299d = list(link250d,link251d,link252d,link253d,link254d,link255d,link256d,link257d,link258d,link259d,
                 link260d,link261d,link262d,link263d,link264d,link265d,link266d,link267d,link268d,link269d,
                 link270d,link271d,link272d,link273d,link274d,link275d,link276d,link277d,link278d,link279d,
                 link280d,link281d,link282d,link283d,link284d,link285d,link286d,link287d,link288d,link289d,
                 link290d,link291d,link292d,link293d,link294d,link295d,link296d,link297d,link298d,link299d)

link250_299d<- rbindlist(l250_299d)
dim(link250_299d)
#  53956 
#52182

dim(link250d)[1]+dim(link251d)[1]+dim(link252d)[1]+dim(link253d)[1]+dim(link254d)[1]+dim(link255d)[1]+dim(link256d)[1]+dim(link257d)[1]+dim(link258d)[1]+dim(link259d)[1]+
  dim(link260d)[1]+dim(link261d)[1]+dim(link262d)[1]+dim(link263d)[1]+dim(link264d)[1]+dim(link265d)[1]+dim(link266d)[1]+dim(link267d)[1]+dim(link268d)[1]+dim(link269d)[1]+
  dim(link270d)[1]+dim(link271d)[1]+dim(link272d)[1]+dim(link273d)[1]+dim(link274d)[1]+dim(link275d)[1]+dim(link276d)[1]+dim(link277d)[1]+dim(link278d)[1]+dim(link279d)[1]+
  dim(link280d)[1]+dim(link281d)[1]+dim(link282d)[1]+dim(link283d)[1]+dim(link284d)[1]+dim(link285d)[1]+dim(link286d)[1]+dim(link287d)[1]+dim(link288d)[1]+dim(link289d)[1]+
  dim(link290d)[1]+dim(link291d)[1]+dim(link292d)[1]+dim(link293d)[1]+dim(link294d)[1]+dim(link295d)[1]+dim(link296d)[1]+dim(link297d)[1]+dim(link298d)[1]+dim(link299d)[1]


dbWriteTable(con, "link250_299dnew1",value =link250_299d, append =  TRUE, row.names = FALSE)
dbWriteTable(con, "link250_299unew1",value =link250_299u, append =  TRUE, row.names = FALSE)


rm(l250_299d,l250_299u)
rm(link250d,link251d,link252d,link253d,link254d,link255d,link256d,link257d,link258d,link259d,
   link260d,link261d,link262d,link263d,link264d,link265d,link266d,link267d,link268d,link269d,
   link270d,link271d,link272d,link273d,link274d,link275d,link276d,link277d,link278d,link279d,
   link280d,link281d,link282d,link283d,link284d,link285d,link286d,link287d,link288d,link289d,
   link290d,link291d,link292d,link293d,link294d,link295d,link296d,link297d,link298d,link299d)

rm(link250u,link251u,link252u,link253u,link254u,link255u,link256u,link257u,link258u,link259u,
   link260u,link261u,link262u,link263u,link264u,link265u,link266u,link267u,link268u,link269u,
   link270u,link271u,link272u,link273u,link274u,link275u,link276u,link277u,link278u,link279u,
   link280u,link281u,link282u,link283u,link284u,link285u,link286u,link287u,link288u,link289u,
   link290u,link291u,link292u,link293u,link294u,link295u,link296u,link297u,link298u,link299u)


rm(link250,link251,link252,link253,link254,link255,link256,link257,link258,link259,
   link260,link261,link262,link263,link264,link265,link266,link267,link268,link269,
   link270,link271,link272,link273,link274,link275,link276,link277,link278,link279,
   link280,link281,link282,link283,link284,link285,link286,link287,link288,link289,
   link290,link291,link292,link293,link294,link295,link296,link297,link298,link299)

rm(function250,function251,function252,function253,function254,function255,function256,function257,function258,function259,
   function260,function261,function262,function263,function264,function265,function266,function267,function268,function269,
   function270,function271,function272,function273,function274,function275,function276,function277,function278,function279,
   function280,function281,function282,function283,function284,function285,function286,function287,function288,function289,
   function290,function291,function292,function293,function294,function295,function296,function297,function298,function299)


epc1<-matchleft(epc1,link250_299d)
epc1<-matchleft(epc1,link250_299u)

dim(epc1)
# 800820     27
#802761     27
################part sum up#################


ldouble1 = list(link1_11d,link12_26d,link27_69d,link70_99d,link100_133d,link134_166d,link167_249d,link250_299d)
lunique1 = list(link1_11u,link12_26u,link27_69u,link70_99u,link100_133u,link134_166u,link167_249u,link250_299u)
linku1<-rbindlist(lunique1,use.names=TRUE, fill=TRUE)
dim(linku1)
#20944575
#20943001
linkd1<-rbindlist(ldouble1,use.names=TRUE, fill=TRUE)

dim(linkd1)
#453988 
# 452214     32
dim(link1_11u)[1]+dim(link12_26u)[1]+dim(link27_69u)[1]+dim(link70_99u)[1]+dim(link100_133u)[1]+dim(link134_166u)[1]+dim(link167_249u)[1]+dim(link250_299u)[1]
#20944575
dim(link1_11d)[1]+dim(link12_26d)[1]+dim(link27_69d)[1]+dim(link70_99d)[1]+dim(link100_133d)[1]+dim(link134_166d)[1]+dim(link167_249d)[1]+dim(link250_299d)[1]
#453988 

length(unique(linku1$lmk_key))
# 20944575
#20943001
length(unique(linku1$method))
#299
dbWriteTable(con, "linku1new",value =linku1, append =  TRUE, row.names = FALSE)
dbWriteTable(con, "linkd1new",value =linkd1, append =  TRUE, row.names = FALSE)

rm(ldouble1,lunique1)
rm(link1_11d,link12_26d,link27_69d,link70_99d,link100_133d,link134_166d,link167_249d,link250_299d)
rm(link1_11u,link12_26u,link27_69u,link70_99u,link100_133u,link134_166u,link167_249u,link250_299u)
rm(add)

#################### prepare the next stage ##################
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
###creates a connection to the postgres database
### note that "con" will be used later in each connection to the database
con <- dbConnect(RPostgres::Postgres(), dbname = db, host=host_db, port=db_port, user=db_user, password=db_password)

add <- dbGetQuery(con,"select * from  addressgb") 

head(add)
str(add)

######### format the OS addressBase data as before#########

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
add$postcodelocator  <- str_trim(add$postcodelocator)
setDT(add)
class(add)
class(epc)


head(add)
head(epc)
str(add)
str(epc)
##########################################################

#needlist11<-c("postset.y","postset.x","lmk_key","postcode.y","property_type","uprn","add1","add2","add3","add","postcode.x","postcodelocator","buildingname","buildingnumber","subbuildingname","paostartnumber","paostartsuffix","paoendnumber","paoendsuffix","paotext","saostartnumber","saostartsuffix","saoendnumber","saoendsuffix","saotext","streetdescription","dependentlocality","townname","class","lodgement_date","inspection_date","lodgement_datetime")
needlist11<-c("postset.y","postset.x","lmk_key","postcode.y","property_type","uprn","add1","add2","add3","add","postcode.x","postcodelocator","buildingname","buildingnumber","subbuildingname","paostartnumber","paostartsuffix","paoendnumber","paoendsuffix","paotext","saostartnumber","saostartsuffix","saoendnumber","saoendsuffix","saotext","streetdescription","locality","dependentlocality","townname","class","lodgement_date","inspection_date","lodgement_datetime")
epc[nchar(postcode)>=6,postset :=substring(postcode,0,nchar(postcode)-2)]
add[nchar(postcodelocator)>=6,postset :=substring(postcodelocator,0,nchar(postcodelocator)-2)]
add$postset  <- str_trim(add$postset)
epc$postset  <- str_trim(epc$postset)

dim(add)
#36110437       30
add<-add[(add$postset  %in% epc$postset), ]


dim(add)
# 32664070       30
#

##reorder here
#################### method 300##################

function300<- function(x,y){
  
  x$bnstreet <-    paste(x$saotext,x$paotext,sep=",")
  x$bnstreet <-    paste(x$bnstreet,x$pp,sep=", ")
  
  x$bnstreet <-    paste(x$bnstreet,x$streetdescription,sep=" ")
  x$bnstreet <- gsub("/", "", x$bnstreet)
  x$bnstreet <- gsub("[.]", "", x$bnstreet)
  x$bnstreet <- gsub("[']", "", x$bnstreet)
  x$bnstreet <- gsub(" ", "", x$bnstreet)
  x$addressf <-paste(x$postcodelocator,x$bnstreet,sep=",")
  
  y<-y[y$property_type=="Flat"|y$property_type=="Maisonette",]
  y$addressfinal <-  paste("FLAT ",y$add,sep="")
  #y$addressfinal <-  paste(y$add1,y$add3,sep=", ")
  y$addressfinal <- gsub("[/]", "", y$addressfinal)
  y$addressfinal <- gsub("[.]", "", y$addressfinal)
  y$addressfinal <- gsub("[']", "", y$addressfinal)
  y$addressfinal <- gsub(" ", "", y$addressfinal)
  y$addressf <- paste(y$postcode,y$addressfinal,sep=",")
  
  taba1 <- inner_join(x,y,by="addressf")
  return(taba1)
  
}


link300<-function300(add,epc)

length(unique(link300$lmk_key))
link300<-link300[,..needlist1]


link300u<- uniqueresult(link300)
dim(link300u)


link300d <- doubleresult(link300)
dim(link300d)

dim(epc)
#
epc <- matchleft(epc,link300)
dim(epc)



#################### method 301 ##################
function301<- function(x,y){

  
  x$bnstreet <-    paste("STUDIO",x$saotext,sep=" ")
  x$bnstreet <-    paste(x$bnstreet,x$pp,sep=", ")
  
  x$bnstreet <-    paste(x$bnstreet,x$streetdescription,sep=" ")
  x$bnstreet <- gsub("/", "", x$bnstreet)
  x$bnstreet <- gsub("[.]", "", x$bnstreet)
  x$bnstreet <- gsub("[']", "", x$bnstreet)
  x$bnstreet <- gsub(" ", "", x$bnstreet)
  x$addressf <-paste(x$postcodelocator,x$bnstreet,sep=",")
  
  y<-y[y$property_type=="Flat"|y$property_type=="Maisonette",]
 
  y$addressfinal <-  paste(y$add1,y$add3,sep=",")
  y$addressfinal <- gsub("[/]", "", y$addressfinal)
  y$addressfinal <- gsub("[.]", "", y$addressfinal)
  y$addressfinal <- gsub("[']", "", y$addressfinal)
  y$addressfinal <- gsub(" ", "", y$addressfinal)
  y$addressf <- paste(y$postcode,y$addressfinal,sep=",")
  
  taba1 <- inner_join(x,y,by="addressf")
  return(taba1)
  
  
}


link301<-function301(add,epc)


link301<-link301[,..needlist1]


link301u<- uniqueresult(link301)
dim(link301u)


link301d <- doubleresult(link301)
dim(link301d)

dim(epc)
#
epc <- matchleft(epc,link301)
dim(epc)
#2308669      30


#################### method 302 ##################


function302<- function(x,y){
  
  x$bnstreet <-    paste(x$saotext,x$paotext,sep=" ")
  x$bnstreet <-    paste(x$bnstreet,x$paostartnumber,sep=",")
  x$bnstreet <-    paste(x$bnstreet,x$paoendnumber,sep="-")
  x$bnstreet <-    paste(x$bnstreet,x$streetdescription,sep=" ")
  x$bnstreet <- gsub("/", "", x$bnstreet)
  x$bnstreet <- gsub("[.]", "", x$bnstreet)
  x$bnstreet <- gsub("[']", "", x$bnstreet)
  x$bnstreet <- gsub(" ", "", x$bnstreet)
  x$addressf <-paste(x$postcodelocator,x$bnstreet,sep=",")
  
  
  y<-y[y$property_type=="Flat"|y$property_type=="Maisonette",]
  #y<-y[grepl("\\d+-\\d",y$add1),]
  
  
  y$addressfinal <- trimws(y$add1) 
  y$addressfinal <- gsub("-", "", y$addressfinal)
  y$addressfinal <-  paste("APARTMENT ",y$addressfinal,sep="")
  y$addressfinal <- paste(y$addressfinal,y$add2,sep=",")
  y$addressfinal <- gsub("[/]", "", y$addressfinal)
  y$addressfinal <- gsub("[.]", "", y$addressfinal)
  y$addressfinal <- gsub("[']", "", y$addressfinal)
  y$addressfinal <- gsub(" ", "", y$addressfinal)
  y$addressf <- paste(y$postcode,y$addressfinal,sep=",")
  
  taba1 <- inner_join(x,y,by="addressf")
  return(taba1) 
  
  
  
}

link302<-function302(add,epc)

link302<-link302[,..needlist1]


link302u<- uniqueresult(link302)
dim(link302u)


link302d <- doubleresult(link302)
dim(link302d)


dim(epc)
#
epc <- matchleft(epc,link302)
dim(epc)
#  805408     27



#################### method 303 ##################

function303<- function(x,y){
  
  # x$bnstreet <-    paste(x$saotext,x$pp,sep=",")
  # 
  # x$bnstreet <-    paste(x$bnstreet,x$streetdescription,sep=" ")
  # x$bnstreet <- gsub("/", "", x$bnstreet)
  # x$bnstreet <- gsub("[.]", "", x$bnstreet)
  # x$bnstreet <- gsub("[']", "", x$bnstreet)
  # x$bnstreet <- gsub(" ", "", x$bnstreet)
  # x$addressf <-paste(x$postcodelocator,x$bnstreet,sep=",")
  # 
  # 
  # y<-y[y$property_type=="Flat"|y$property_type=="Maisonette",]
  # y$addressfinal <- y$add1
  # y$addressfinal <-  paste("APARTMENT ",y$addressfinal,sep=" ")
  # y$addressfinal <-  paste( y$addressfinal,y$add3,sep=",")
  # y$addressfinal <- gsub("[/]", "", y$addressfinal)
  # y$addressfinal <- gsub("[.]", "", y$addressfinal)
  # y$addressfinal <- gsub("[']", "", y$addressfinal)
  # 
  # y$addressfinal <- gsub(" ", "", y$addressfinal)
  # y$addressf <- paste(y$postcode,y$addressfinal,sep=",")
  # 
  # taba1 <- inner_join(x,y,by="addressf")
  # return(taba1)
  
  x$bnstreet <-    paste("APARTMENT ",x$ss,sep=" ")
  x$bnstreet <-    paste(x$bnstreet,x$paotext,sep=",")
  x$bnstreet <-    paste(x$bnstreet,x$streetdescription,sep=",")
  x$bnstreet <-    paste(x$bnstreet,x$townname,sep=",")
  x$bnstreet <- gsub("[.]", "", x$bnstreet)
  x$bnstreet <- gsub("[/]", "", x$bnstreet)
  x$bnstreet <- gsub("[']", "", x$bnstreet)
  x$bnstreet <- gsub(" ", "", x$bnstreet)
  x$addressf <-paste(x$postcodelocator,x$bnstreet,sep=",")
  
  
  
  y<-y[y$property_type=="Flat"|y$property_type=="Maisonette",]
  y$addressfinal <- y$add
  
 
  y$addressfinal <- gsub("[.]", "", y$addressfinal)
  y$addressfinal <- gsub("[/]", "", y$addressfinal)
  y$addressfinal <- gsub("[']", "", y$addressfinal)
   y$addressfinal <- gsub(" ", "", y$addressfinal)
  y$addressf <- paste(y$postcode,y$addressfinal,sep=",")
  
  taba1 <- inner_join(x,y,by="addressf")
  return(taba1)
  
}



link303<-function303(add,epc)
dim(link303)
# 
link303<-link303[,..needlist1]

link303u<- uniqueresult(link303)
dim(link303u)
#

link303d <- doubleresult(link303)
dim(link303d)


dim(epc)
#
epc <- matchleft(epc,link303)
dim(epc)
# 2 805203     27


#################### method 304  ##################
# epc1 <- matchleft(epc1,link300)
# epc1 <- matchleft(epc1,link301)
# epc1 <- matchleft(epc1,link302)
# epc1 <- matchleft(epc1,link303)

# epc<-epc1
# class(epc)



#26418147       30

function304<- function(x,y){

  # x$bnstreet <-    paste(x$saotext,x$pp,sep=",")
  # x$bnstreet <-    paste(x$bnstreet,x$streetdescription,sep=" ")
  # #x$bnstreet <-    paste(x$bnstreet,x$streetdescription,sep=" ")
  # x$bnstreet <- gsub("[.]", "", x$bnstreet)
  # x$bnstreet <- gsub("[/]", "", x$bnstreet)
  # x$bnstreet <- gsub("[']", "", x$bnstreet)
  # x$bnstreet <- gsub(" ", "", x$bnstreet)
  # x$addressf <-paste(x$postcodelocator,x$bnstreet,sep=",")
  # 
  # 
  # # y<-y[y$property_type=="Flat",]
  # 
  # y<-y[y$property_type=="Flat"|y$property_type=="Maisonette",]
  # y$addressfinal <- paste(y$add1,y$add3,sep=",")
  # 
  # y$addressfinal <- gsub("FLAT", "APARTMENT", y$addressfinal)
  # y$addressfinal <- gsub("[.]", "", y$addressfinal)
  # y$addressfinal <- gsub("[/]", "", y$addressfinal)
  # y$addressfinal <- gsub("[']", "", y$addressfinal)
  # 
  # y$addressfinal <- gsub(" ", "", y$addressfinal)
  # y$addressf <- paste(y$postcode,y$addressfinal,sep=",")
  # 
  # taba1 <- inner_join(x,y,by="addressf")
  # return(taba1)
  
  x$bnstreet <-    paste(x$saotext,x$buildingname,sep=",")
  x$bnstreet <-    paste(x$bnstreet,x$buildingnumber,sep=",")
  x$bnstreet <-    paste(x$bnstreet,x$streetdescription,sep=" ")
  x$bnstreet <- gsub("[.]", "", x$bnstreet)
  x$bnstreet <- gsub("[/]", "", x$bnstreet)
  x$bnstreet <- gsub("[']", "", x$bnstreet)
  x$bnstreet <- gsub(" ", "", x$bnstreet)
  x$addressf <-paste(x$postcodelocator,x$bnstreet,sep=",")
  
  
  #y<-y[y$property_type=="Flat",]
  
  #y<-y[y$property_type=="Flat"|y$property_type=="Maisonette",]
  y$addressfinal <- y$add1
  y$addressfinal <- gsub("[-]", "", y$addressfinal)
  y$addressfinal <- gsub("FLAT 01", "FLAT 1", y$addressfinal)
  y$addressfinal <- gsub("FLAT 02", "FLAT 2", y$addressfinal)
  y$addressfinal <- gsub("FLAT 03", "FLAT 3", y$addressfinal)
  y$addressfinal <- gsub("FLAT 04", "FLAT 4", y$addressfinal)
  y$addressfinal <- gsub("FLAT 05", "FLAT 5", y$addressfinal)
  y$addressfinal <- gsub("FLAT 06", "FLAT 6", y$addressfinal)
  y$addressfinal <- gsub("FLAT 07", "FLAT 7", y$addressfinal)
  y$addressfinal <- gsub("FLAT 08", "FLAT 8", y$addressfinal)
  y$addressfinal <- gsub("FLAT 09", "FLAT 9", y$addressfinal)
  
  y$addressfinal <- paste( y$addressfinal ,  y$add2, sep=",")
  y$addressfinal <- paste( y$addressfinal ,  y$add3, sep=",")
  y$addressfinal <- gsub(" ", "", y$addressfinal)
  y$addressfinal <- gsub("[.]", "", y$addressfinal)
  y$addressfinal <- gsub("[/]", "", y$addressfinal)
  y$addressfinal <- gsub("[']", "", y$addressfinal)
  y$addressf <- paste(y$postcode,y$addressfinal,sep=",")
  
  taba1 <- inner_join(x,y,by="addressf")
  return(taba1)
  
  
  
  

  
  
}


link304<-function304(add,epc)



link304<-link304[,..needlist1]


link304u<- uniqueresult(link304)
dim(link304u)


link304d <- doubleresult(link304)
# head(link304[grepl("[']", link304$add),])
# head(link304[grepl("[.]", link304$add),])

dim(epc)
# 
epc<- matchleft(epc,link304)
dim(epc)
#  2303064      30


#################### method 305 ##################
function305<- function(x,y){
  
  x$bnstreet <-    paste("FLAT ",x$buildingname,sep=" ")
  
  x$bnstreet <-    paste(x$bnstreet,x$streetdescription,sep=" ")
  #x$bnstreet <-    paste(x$bnstreet,x$streetdescription,sep=" ")
  x$bnstreet <- gsub(",", "", x$bnstreet)
  x$bnstreet <- gsub("/", "", x$bnstreet)
  x$bnstreet <- gsub("[.]", "", x$bnstreet)
  x$bnstreet <- gsub("[']", "", x$bnstreet)
  x$bnstreet <- gsub(" ", "", x$bnstreet)
  x$addressf <-paste(x$postcodelocator,x$bnstreet,sep=",")
  
  
  y<-y[y$property_type=="Flat"|y$property_type=="Maisonette",]
  
  
  y$addressfinal <- y$add
  y$addressfinal <- gsub(",", "", y$addressfinal)
  y$addressfinal <- gsub("[/]", "", y$addressfinal)
  y$addressfinal <- gsub("[.]", "", y$addressfinal)
  y$addressfinal <- gsub("[']", "", y$addressfinal)
  y$addressfinal <- gsub(" ", "", y$addressfinal)
  y$addressf <- paste(y$postcode,y$addressfinal,sep=",")
  
  taba1 <- inner_join(x,y,by="addressf")
  
  return(taba1) 
 
  
}
link305<-function305(add,epc)


link305<-link305[,..needlist1]


link305u<- uniqueresult(link305)
dim(link305u)


link305d <- doubleresult(link305)
dim(link305d)


dim(epc)
#
epc <- matchleft(epc,link305)
dim(epc)
#2300304      30

#
#################### method 306 ##################

function306<- function(x,y){
  
  
    
  x$bnstreet <-    paste("STUDIO ",x$subbuildingname,sep=" ")
  x$bnstreet <-    paste(x$bnstreet,x$paotext,sep=" ")
  x$bnstreet <-    paste(x$bnstreet,x$buildingnumber,sep=" ")
  x$bnstreet <-    paste(x$bnstreet,x$streetdescription,sep=" ")
  x$bnstreet <- gsub(",", "", x$bnstreet)
  x$bnstreet <- gsub("/", "", x$bnstreet)
  x$bnstreet <- gsub("[.]", "", x$bnstreet)
  x$bnstreet <- gsub("[']", "", x$bnstreet)
  x$bnstreet <- gsub(" ", "", x$bnstreet)
  x$addressf <-paste(x$postcodelocator,x$bnstreet,sep=",")
  
  y<-y[y$property_type=="Flat"|y$property_type=="Maisonette",]
  y$addressfinal <-  y$add
  
  y$addressfinal <- gsub("-", "", y$addressfinal)
  y$addressfinal <- gsub("[/]", "", y$addressfinal)
  y$addressfinal <- gsub("[.]", "", y$addressfinal)
  y$addressfinal <- gsub("[']", "", y$addressfinal)
  y$addressfinal <- gsub(" ", "", y$addressfinal)
  y$addressf <- paste(y$postcode,y$addressfinal,sep=",")
  
  taba1 <- inner_join(x,y,by="addressf")
  return(taba1)
  
  
  
  
}




link306<-function306(add,epc)
dim(link306)
# 

link306<-link306[,..needlist1]



link306u<- uniqueresult(link306)
dim(link306u)
# 

link306d <- doubleresult(link306)
dim(link306d)


epc <- matchleft(epc,link306)
dim(epc)
# 2300301      30

#################### method 307 ##################



function307<- function(x,y){
  
   
  x$bnstreet <-    paste(x$subbuildingname,x$paotext,sep=" ")
  x$bnstreet <-    paste(x$bnstreet,x$buildingnumber,sep=",")
  x$bnstreet <-    paste(x$bnstreet,x$streetdescription,sep=",")
  #x$bnstreet <- gsub(",", "", x$bnstreet)
  x$bnstreet <- gsub("/", "", x$bnstreet)
  x$bnstreet <- gsub("[.]", "", x$bnstreet)
  x$bnstreet <- gsub("[']", "", x$bnstreet)
  x$bnstreet <- gsub(" ", "", x$bnstreet)
  x$addressf <-paste(x$postcodelocator,x$bnstreet,sep=",")
  
  y<-y[y$property_type=="Flat"|y$property_type=="Maisonette",]
  y$addressfinal <-  y$add
  
  y$addressfinal <- gsub("-", "", y$addressfinal)
  y$addressfinal <- gsub("[/]", "", y$addressfinal)
  y$addressfinal <- gsub("[.]", "", y$addressfinal)
  y$addressfinal <- gsub("[']", "", y$addressfinal)
  y$addressfinal <- gsub(" ", "", y$addressfinal)
  y$addressf <- paste(y$postcode,y$addressfinal,sep=",")
  
  taba1 <- inner_join(x,y,by="addressf")
  return(taba1) 

  
  
  
}

link307<-function307(add,epc)
dim(link307)


link307<-link307[,..needlist1]

link307u<- uniqueresult(link307)
dim(link307u)


link307d <- doubleresult(link307)
dim(link307d)


dim(epc)
# 
epc <- matchleft(epc,link307)
dim(epc)
# 800893     28
#
#################### method 308 ##################

function308<- function(x,y){
  x$bnstreet <-    paste(x$subbuildingname,x$buildingname,sep=",")
  x$bnstreet <-    paste(x$bnstreet,x$streetdescription,sep=",")
  x$bnstreet <- gsub("/", "", x$bnstreet)
  x$bnstreet <- gsub("[']", "", x$bnstreet)
  x$bnstreet <- gsub("[.]", "", x$bnstreet)
  x$bnstreet <- gsub(" ", "", x$bnstreet)
  x$addressf <-paste(x$postcodelocator,x$bnstreet,sep=",")
  
  y<-y[y$property_type=="Flat"| y$property_type=="Maisonette",]
  y$addressfinal <-  gsub("STUDIO", "FLAT", y$add) 
  y$addressfinal <- gsub("[.]", "", y$addressfinal)
  y$addressfinal <- gsub("[']", "", y$addressfinal)
  y$addressfinal <- gsub("/", "", y$addressfinal)
  y$addressfinal <- gsub(" ", "", y$addressfinal)
  y$addressf <- paste(y$postcode,y$addressfinal,sep=",")
  
  taba1 <- inner_join(x,y,by="addressf")
  return(taba1)
}



link308<-function308(add,epc)
dim(link308)


link308<-link308[,..needlist1]



link308u<- uniqueresult(link308)
dim(link308u)


link308d <- doubleresult(link308)
dim(link308d)


dim(epc)
# 
epc <- matchleft(epc,link308)
dim(epc)
#2309509      30
#################### method 309 ##################
function309<- function(x,y){
x$bnstreet <-    paste(x$subbuildingname,x$buildingname,sep=",")
  x$bnstreet <-    paste(x$bnstreet,x$streetdescription,sep=" ")
  x$bnstreet <- gsub("/", "", x$bnstreet)
  x$bnstreet <- gsub("[']", "", x$bnstreet)
  x$bnstreet <- gsub("[.]", "", x$bnstreet)
  x$bnstreet <- gsub(" ", "", x$bnstreet)
  x$addressf <-paste(x$postcodelocator,x$bnstreet,sep=",")
  
  y<-y[y$property_type=="Flat"| y$property_type=="Maisonette",]
  y$addressfinal <-  gsub("STUDIO", "FLAT", y$add) 
  y$addressfinal <- gsub("[.]", "", y$addressfinal)
  y$addressfinal <- gsub("[']", "", y$addressfinal)
  y$addressfinal <- gsub("/", "", y$addressfinal)
  y$addressfinal <- gsub(" ", "", y$addressfinal)
  y$addressf <- paste(y$postcode,y$addressfinal,sep=",")
  
  taba1 <- inner_join(x,y,by="addressf")
  return(taba1)
}


link309<-function309(add,epc)

link309<-link309[,..needlist1]


link309u<- uniqueresult(link309)
dim(link309u)


link309d <- doubleresult(link309)
dim(link309d)

dim(epc)
#3049854      30
epc <- matchleft(epc,link309)
dim(epc)
#


#################### method 310 ##################


function310<- function(x,y){
 
   x$bnstreet <-    paste(x$subbuildingname,x$buildingnumber,sep=",")
  x$bnstreet <-    paste(x$bnstreet,x$streetdescription,sep=" ")
  x$bnstreet <- gsub("/", "", x$bnstreet)
  x$bnstreet <- gsub("[']", "", x$bnstreet)
  x$bnstreet <- gsub("[.]", "", x$bnstreet)
  x$bnstreet <- gsub(" ", "", x$bnstreet)
  x$addressf <-paste(x$postcodelocator,x$bnstreet,sep=",")
  
  y<-y[y$property_type=="Flat"| y$property_type=="Maisonette",]
  y$addressfinal <-  gsub("STUDIO", "FLAT", y$add) 
  y$addressfinal <- gsub("[.]", "", y$addressfinal)
  y$addressfinal <- gsub("[']", "", y$addressfinal)
  y$addressfinal <- gsub("/", "", y$addressfinal)
  y$addressfinal <- gsub(" ", "", y$addressfinal)
  y$addressf <- paste(y$postcode,y$addressfinal,sep=",")
  
  taba1 <- inner_join(x,y,by="addressf")
  return(taba1)
}


link310<-function310(add,epc)

length(unique(link310$lmk_key))
link310<-link310[,..needlist1]


link310u<- uniqueresult(link310)
dim(link310u)


link310d <- doubleresult(link310)
dim(link310d)

dim(epc)
#
epc <- matchleft(epc,link310)
dim(epc)
# 795290     28


#################### method 311  ##################

function311<- function(x,y){
  
  x$bnstreet <-    paste(x$subbuildingname,x$pp,sep=",")
  x$bnstreet <-    paste(x$bnstreet,x$streetdescription,sep=" ")
  x$bnstreet <- gsub("/", "", x$bnstreet)
  x$bnstreet <- gsub("[']", "", x$bnstreet)
  x$bnstreet <- gsub("[.]", "", x$bnstreet)
  x$bnstreet <- gsub(" ", "", x$bnstreet)
  x$addressf <-paste(x$postcodelocator,x$bnstreet,sep=",")
  
  y<-y[y$property_type=="Flat"| y$property_type=="Maisonette",]
  y$addressfinal <-  gsub("STUDIO", "FLAT", y$add) 
  y$addressfinal <- gsub("[.]", "", y$addressfinal)
  y$addressfinal <- gsub("[']", "", y$addressfinal)
  y$addressfinal <- gsub("/", "", y$addressfinal)
  y$addressfinal <- gsub(" ", "", y$addressfinal)
  y$addressf <- paste(y$postcode,y$addressfinal,sep=",")
  
  taba1 <- inner_join(x,y,by="addressf")
  return(taba1)
}


link311<-function311(add,epc)


link311<-link311[,..needlist1]


link311u<- uniqueresult(link311)
dim(link311u)


link311d <- doubleresult(link311)
dim(link311d)

dim(epc)
#
epc <- matchleft(epc,link311)
dim(epc)
#2318669      31





######################################


#################### method 312 ##################


function312<- function(x,y){
 
  #   x$bnstreet <-    paste("FLAT ",x$buildingname,sep=" ")
  # x$bnstreet <- gsub(",", "", x$bnstreet)
  # x$bnstreet <- gsub("/", "", x$bnstreet)
  # x$bnstreet <- gsub("[.]", "", x$bnstreet)
  # x$bnstreet <- gsub("[']", "", x$bnstreet)
  # x$bnstreet <- gsub(" ", "", x$bnstreet)
  # x$addressf <-paste(x$postcodelocator,x$bnstreet,sep=",")
  # 
  # y<-y[y$property_type=="Flat"|y$property_type=="Maisonette",]
  # y$addressfinal <-  paste(y$add1,y$add2,sep=" ")
  # #y$addressfinal <- gsub(",", "", y$addressfinal)
  # y$addressfinal <- gsub("[/]", "", y$addressfinal)
  # y$addressfinal <- gsub("[.]", "", y$addressfinal)
  # y$addressfinal <- gsub("[']", "", y$addressfinal)
  # y$addressfinal <- gsub(" ", "", y$addressfinal)
  # y$addressf <- paste(y$postcode,y$addressfinal,sep=",")
  # 
  # taba1 <- inner_join(x,y,by="addressf")
  # return(taba1)


  x$bnstreet <- paste(x$saotext,x$paotext,sep=",")
  x$bnstreet <- paste(x$bnstreet,x$pp,sep=",")
  x$bnstreet <- paste(x$bnstreet,x$streetdescription,sep=" ")
  #x$bnstreet <- paste(x$subbuildingname,x$streetdescription,sep=" ")
  x$bnstreet <- gsub(",", "", x$bnstreet)
  x$bnstreet <- gsub("/", "", x$bnstreet)
  x$bnstreet <- gsub("[.]", "", x$bnstreet)
  x$bnstreet <- gsub("[']", "", x$bnstreet)
  x$bnstreet <- gsub(" ", "", x$bnstreet)
  x$addressf <-paste(x$postcodelocator,x$bnstreet,sep=",")

  y<-y[y$property_type=="Flat"|y$property_type=="Maisonette",]
  y$addressfinal <- y$add
  y$addressfinal <- gsub("FLAT", "APARTMENT", y$addressfinal)
  #y$addressfinal <- gsub("VILLAGE CENTRE", "THE VILLAGE CENTRE", y$addressfinal)
  y$addressfinal <- gsub("[/]", "", y$addressfinal)
  y$addressfinal <- gsub("[.]", "", y$addressfinal)
  y$addressfinal <- gsub("[']", "", y$addressfinal)
  y$addressfinal <- gsub(",", "", y$addressfinal)
  y$addressfinal <- gsub(" ", "", y$addressfinal)
  y$addressf <- paste(y$postcode,y$addressfinal,sep=",")

  taba1 <- inner_join(x,y,by="addressf")

  return(taba1)
  

}

link312<-function312(add,epc)





link312<-link312[,..needlist1]


link312u<- uniqueresult(link312)
dim(link312u)


link312d <- doubleresult(link312)
dim(link312d)


dim(epc)
#
epc <- matchleft(epc,link312)
dim(epc)
# 2317648      31



#################### method 313 check##################

function313<- function(x,y){
 

 # x$bnstreet <-    paste("FLAT ",x$buildingname,sep=" ")
 #  x$bnstreet <-    paste(x$bnstreet,x$streetdescription,sep=",")
 #  x$bnstreet <- gsub(",", "", x$bnstreet)
 #  x$bnstreet <- gsub("/", "", x$bnstreet)
 #  x$bnstreet <- gsub("[.]", "", x$bnstreet)
 #  x$bnstreet <- gsub("[']", "", x$bnstreet)
 #  x$bnstreet <- gsub(" ", "", x$bnstreet)
 #  x$addressf <-paste(x$postcodelocator,x$bnstreet,sep=",")
 #  
 #  y<-y[y$property_type=="Flat"|y$property_type=="Maisonette",]
 #  y$addressfinal <-  y$add
 #  y$addressfinal <- gsub(",", "", y$addressfinal)
 #  y$addressfinal <- gsub("[/]", "", y$addressfinal)
 #  y$addressfinal <- gsub("[.]", "", y$addressfinal)
 #  y$addressfinal <- gsub("[']", "", y$addressfinal)
 #  y$addressfinal <- gsub(" ", "", y$addressfinal)
 #  y$addressf <- paste(y$postcode,y$addressfinal,sep=",")
 #  
 #  taba1 <- inner_join(x,y,by="addressf")
 #  return(taba1)
  
  x$bnstreet <- paste(x$subbuildingname,x$buildingnumber,sep=",")
 
  x$bnstreet <- paste(x$bnstreet,x$streetdescription,sep=",")
 
  x$bnstreet <- gsub(",", "", x$bnstreet)
  x$bnstreet <- gsub("/", "", x$bnstreet)
  x$bnstreet <- gsub("[.]", "", x$bnstreet)
  x$bnstreet <- gsub("[']", "", x$bnstreet)
  x$bnstreet <- gsub(" ", "", x$bnstreet)
  x$addressf <-paste(x$postcodelocator,x$bnstreet,sep=",")
  
  #y<-y[y$property_type=="Flat"|y$property_type=="Maisonette",]
  #y$addressfinal <- word(y$add1,2)
  
  y$addressfinal <-  paste(y$add1,y$add2,sep=",")
  y$addressfinal <- gsub("FLAT", "APARTMENT", y$addressfinal)
  
  y$addressfinal <- gsub("[/]", "", y$addressfinal)
  y$addressfinal <- gsub("[.]", "", y$addressfinal)
  y$addressfinal <- gsub("[']", "", y$addressfinal)
  y$addressfinal <- gsub(",", "", y$addressfinal)
  y$addressfinal <- gsub(" ", "", y$addressfinal)
  y$addressf <- paste(y$postcode,y$addressfinal,sep=",")
  
  taba1 <- inner_join(x,y,by="addressf")
  
  return(taba1)
}



link313<-function313(add,epc)
dim(link313)
# 
link313<-link313[,..needlist1]

link313u<- uniqueresult(link313)
dim(link313u)
#

link313d <- doubleresult(link313)
dim(link313d)


dim(epc)
#
epc <- matchleft(epc,link313)
dim(epc)
#  796054     28


#################### method 314  ##################

function314<- function(x,y){
  
  x$bnstreet <-    paste(x$subbuildingname,x$paotext,sep=" ")
  x$bnstreet <-    paste(x$bnstreet,x$buildingnumber,sep=",")
  x$bnstreet <-    paste(x$bnstreet,x$streetdescription,sep=",")
  
  x$bnstreet <- gsub(",", "", x$bnstreet)
  x$bnstreet <- gsub("/", "", x$bnstreet)
  x$bnstreet <- gsub("[.]", "", x$bnstreet)
  x$bnstreet <- gsub("[']", "", x$bnstreet)
  x$bnstreet <- gsub(" ", "", x$bnstreet)
  x$addressf <-paste(x$postcodelocator,x$bnstreet,sep=",")
  
  
  y$addressfinal <-  y$add
  y$addressfinal <- gsub(",", "", y$addressfinal)
  y$addressfinal <- gsub("[/]", "", y$addressfinal)
  y$addressfinal <- gsub("[.]", "", y$addressfinal)
  y$addressfinal <- gsub("[']", "", y$addressfinal)
  y$addressfinal <- gsub(" ", "", y$addressfinal)
  y$addressf <- paste(y$postcode,y$addressfinal,sep=",")
  
  taba1 <- inner_join(x,y,by="addressf")
  return(taba1)
 
  
  
}


link314<-function314(add,epc)



link314<-link314[,..needlist1]


link314u<- uniqueresult(link314)
dim(link314u)


link314d <- doubleresult(link314)


dim(epc)
# 
epc<- matchleft(epc,link314)
dim(epc)
#   795956     28
#################### method 315 ##################


function315<- function(x,y){
   x$bnstreet <-    paste(x$subbuildingname,x$paotext,sep=" ")
  x$bnstreet <-    paste(x$bnstreet,x$buildingnumber,sep=",")
  x$bnstreet <-    paste(x$bnstreet,x$streetdescription,sep=",")
  #x$bnstreet <-    paste(x$bnstreet,x$streetdescription,sep=",")
  #x$bnstreet <-    paste(x$bnstreet,x$streetdescription,sep=",")
  #x$bnstreet <- gsub(",", "", x$bnstreet)
  x$bnstreet <- gsub("/", "", x$bnstreet)
  x$bnstreet <- gsub("[.]", "", x$bnstreet)
  x$bnstreet <- gsub("[']", "", x$bnstreet)
  x$bnstreet <- gsub(" ", "", x$bnstreet)
  x$addressf <-paste(x$postcodelocator,x$bnstreet,sep=",")
  
  y[postcode=="B3 1PW",add:=gsub("CASPER HOUSE", "CASPAR HOUSE",add)]
  y$addressfinal <-  y$add
  #y$addressfinal <- gsub(",", "", y$addressfinal)
  y$addressfinal <- gsub("[/]", "", y$addressfinal)
  y$addressfinal <- gsub("[.]", "", y$addressfinal)
  y$addressfinal <- gsub("[']", "", y$addressfinal)
  y$addressfinal <- gsub(" ", "", y$addressfinal)
  y$addressf <- paste(y$postcode,y$addressfinal,sep=",")
  
  taba1 <- inner_join(x,y,by="addressf")
  return(taba1) 
 
  
}



link315<-function315(add,epc)


link315<-link315[,..needlist1]


link315u<- uniqueresult(link315)
dim(link315u)


link315d <- doubleresult(link315)
dim(link315d)


dim(epc)
#
epc <- matchleft(epc,link315)
dim(epc)
# 795594     28

#
#################### method 316 ##################

function316<- function(x,y){
  
  x$bnstreet <- paste(x$saotext,x$paotext,sep=" ")
  x$bnstreet <- paste(x$bnstreet,x$streetdescription,sep=" ")
  x$bnstreet <- paste(x$bnstreet,x$townname,sep=" ")
  #x$bnstreet <- paste(x$subbuildingname,x$streetdescription,sep=" ")
  x$bnstreet <- gsub(",", "", x$bnstreet)
  x$bnstreet <- gsub("/", "", x$bnstreet)
  x$bnstreet <- gsub("[.]", "", x$bnstreet)
  x$bnstreet <- gsub("[']", "", x$bnstreet)
  x$bnstreet <- gsub(" ", "", x$bnstreet)
  x$addressf <-paste(x$postcodelocator,x$bnstreet,sep=",")
  
  y<-y[y$property_type=="Flat"|y$property_type=="Maisonette",]
  y$addressfinal <- paste("APARTMENT ",y$add,sep="")
  
  #y$addressfinal <- gsub("VILLAGE CENTRE", "THE VILLAGE CENTRE", y$addressfinal)
  y$addressfinal <- gsub("[/]", "", y$addressfinal)
  y$addressfinal <- gsub("[.]", "", y$addressfinal)
  y$addressfinal <- gsub("[']", "", y$addressfinal)
  y$addressfinal <- gsub(",", "", y$addressfinal)
  y$addressfinal <- gsub(" ", "", y$addressfinal)
  y$addressf <- paste(y$postcode,y$addressfinal,sep=",")
  
  taba1 <- inner_join(x,y,by="addressf")
  
  return(taba1)
  
}




link316<-function316(add,epc)
dim(link316)
# 

link316<-link316[,..needlist1]



link316u<- uniqueresult(link316)
dim(link316u)
# 

link316d <- doubleresult(link316)
dim(link316d)


epc <- matchleft(epc,link316)
dim(epc)
#  792453     28

#################### method 317 ##################



function317<- function(x,y){
  

  # x$bnstreet <-    paste(x$saotext,x$pp,sep=",")
  # x$bnstreet <-    paste(x$bnstreet,x$streetdescription,sep=" ")
  # 
  # x$bnstreet <- gsub("[.]", "", x$bnstreet)
  #  x$bnstreet <- gsub("[/]", "", x$bnstreet)
  #  x$bnstreet <- gsub("[']", "", x$bnstreet)
  #  x$bnstreet <- gsub(" ", "", x$bnstreet)
  #  x$addressf <-paste(x$postcodelocator,x$bnstreet,sep=",")
  # 
  #  y<-y[y$property_type=="Flat"|y$property_type=="Maisonette",]
  #  y$addressfinal <- paste(y$add1,y$add3,sep=",")
  # 
  #  y$addressfinal <- gsub("FLAT", "STUDIO", y$addressfinal)
  # y$addressfinal <- gsub("[.]", "", y$addressfinal)
  # y$addressfinal <- gsub("[/]", "", y$addressfinal)
  # y$addressfinal <- gsub("[']", "", y$addressfinal)
  #
  # y$addressfinal <- gsub(" ", "", y$addressfinal)
  # y$addressf <- paste(y$postcode,y$addressfinal,sep=",")
  #
  # taba1 <- inner_join(x,y,by="addressf")
  # return(taba1)
  
   # x$bnstreet <-    paste(x$pp,x$streetdescription,sep=" ")
   # x$bnstreet <- gsub("[.]", "", x$bnstreet)
   # x$bnstreet <- gsub("[/]", "", x$bnstreet)
   # x$bnstreet <- gsub("[']", "", x$bnstreet)
   # x$bnstreet <- gsub(" ", "", x$bnstreet)
   # x$addressf <-paste(x$postcodelocator,x$bnstreet,sep=",")
   # 
   # #y<-y[y$property_type=="Flat",]
   # 
   # #y<-y[y$property_type=="Flat"|y$property_type=="Maisonette",]
   # y<-y[add1=="FLAT",]
   # y$addressfinal <- y$add2
   # 
   # y$addressfinal <- gsub(" ", "", y$addressfinal)
   # y$addressfinal <- gsub("[.]", "", y$addressfinal)
   # y$addressfinal <- gsub("[/]", "", y$addressfinal)
   # y$addressfinal <- gsub("[']", "", y$addressfinal)
   # y$addressf <- paste(y$postcode,y$addressfinal,sep=",")
   # 
   # taba1 <- inner_join(x,y,by="addressf")
   # return(taba1)
  x$bnstreet <-    paste(x$saotext,x$pp,sep=",")
  
  
  x$bnstreet <-    paste(x$bnstreet,x$streetdescription,sep=" ")
  x$bnstreet <- gsub("/", "", x$bnstreet)
  x$bnstreet <- gsub("[.]", "", x$bnstreet)
  x$bnstreet <- gsub("[']", "", x$bnstreet)
  x$bnstreet <- gsub(" ", "", x$bnstreet)
  x$addressf <-paste(x$postcodelocator,x$bnstreet,sep=",")
  
  y<-y[y$property_type=="Flat"|y$property_type=="Maisonette",]
  y$addressfinal <- y$add
  y$addressfinal <- gsub("FLAT", "APARTMENT", y$addressfinal)
  y$addressfinal <- gsub("[/]", "", y$addressfinal)
  y$addressfinal <- gsub("[.]", "", y$addressfinal)
  y$addressfinal <- gsub("[']", "", y$addressfinal)
  y$addressfinal <- gsub(" ", "", y$addressfinal)
  y$addressf <- paste(y$postcode,y$addressfinal,sep=",")
  
  taba1 <- inner_join(x,y,by="addressf")
  
  return(taba1) 
  
  
}

link317<-function317(add,epc)
dim(link317)


link317<-link317[,..needlist1]

link317u<- uniqueresult(link317)
dim(link317u)


link317d <- doubleresult(link317)
dim(link317d)


dim(epc)
# 
epc <- matchleft(epc,link317)
dim(epc)
#2310190      31
#
#################### method 318 (check manually, put it later)##################

function318<- function(x,y){
  # x$bnstreet <-    paste(x$saotext,x$streetdescription,sep=",")
  # 
  # x$bnstreet <- gsub("/", "", x$bnstreet)
  # x$bnstreet <- gsub("[.]", "", x$bnstreet)
  # x$bnstreet <- gsub("[']", "", x$bnstreet)
  # x$bnstreet <- gsub(" ", "", x$bnstreet)
  # x$addressf <-paste(x$postcodelocator,x$bnstreet,sep=",")
  # 
  # 
  # y<-y[grepl("^\\d",y$add3) ,]
  # y[,addressfinal1 :=word(add3,2,-1)]
  # #y$addressfinal1 <- word(y$add3,2,nchar(y$add3))
  # 
  # y$addressfinal <- paste(y$add1,y$addressfinal1,sep=",")
  # y$addressfinal <- gsub("STUDIO", "APARTMENT", y$addressfinal)
  # y$addressfinal <- gsub("[/]", "", y$addressfinal)
  # y$addressfinal <- gsub("[.]", "", y$addressfinal)
  # y$addressfinal <- gsub("[']", "", y$addressfinal)
  # y$addressfinal <- gsub(" ", "", y$addressfinal)
  # y$addressf <- paste(y$postcode,y$addressfinal,sep=",")
  # 
  # taba1 <- inner_join(x,y,by="addressf")
  # 
  # return(taba1)
  
  x$bnstreet <-    paste("FLAT ",x$buildingname,sep=" ")
  x$bnstreet <- gsub(",", "", x$bnstreet)
  x$bnstreet <- gsub("/", "", x$bnstreet)
  x$bnstreet <- gsub("[.]", "", x$bnstreet)
  x$bnstreet <- gsub("[']", "", x$bnstreet)
  x$bnstreet <- gsub(" ", "", x$bnstreet)
  x$addressf <-paste(x$postcodelocator,x$bnstreet,sep=",")
  
  y<-y[y$property_type=="Flat"|y$property_type=="Maisonette",]
  y$addressfinal <-  paste(y$add1,y$add2,sep=" ")
  #y$addressfinal <- gsub(",", "", y$addressfinal)
  y$addressfinal <- gsub("[/]", "", y$addressfinal)
  y$addressfinal <- gsub("[.]", "", y$addressfinal)
  y$addressfinal <- gsub("[']", "", y$addressfinal)
  y$addressfinal <- gsub(" ", "", y$addressfinal)
  y$addressf <- paste(y$postcode,y$addressfinal,sep=",")
  
  taba1 <- inner_join(x,y,by="addressf")
  return(taba1)
}



link318<-function318(add,epc)
dim(link318)


link318<-link318[,..needlist1]



link318u<- uniqueresult(link318)
dim(link318u)


link318d <- doubleresult(link318)
dim(link318d)


dim(epc)
# 
epc <- matchleft(epc,link318)
dim(epc)
#791602     28
#################### method 319 ##################
function319<- function(x,y){
  
  x$bnstreet <-    paste(x$saotext,x$paotext,sep=" ")
  x$bnstreet <- gsub(",", "", x$bnstreet)
  x$bnstreet <- gsub("/", "", x$bnstreet)
  x$bnstreet <- gsub("[.]", "", x$bnstreet)
  x$bnstreet <- gsub("[']", "", x$bnstreet)
  x$bnstreet <- gsub(" ", "", x$bnstreet)
  x$addressf <-paste(x$postcodelocator,x$bnstreet,sep=",")
  
  y<-y[y$property_type=="Flat"|y$property_type=="Maisonette",]
  y$addressfinal <-  paste("APARTMENT ",y$add1,sep="")
  y$addressfinal <- gsub(",", "", y$addressfinal)
  y$addressfinal <- gsub("[/]", "", y$addressfinal)
  y$addressfinal <- gsub("[.]", "", y$addressfinal)
  y$addressfinal <- gsub("[']", "", y$addressfinal)
  y$addressfinal <- gsub(" ", "", y$addressfinal)
  y$addressf <- paste(y$postcode,y$addressfinal,sep=",")
  
  taba1 <- inner_join(x,y,by="addressf")
  return(taba1)
  
  
}


link319<-function319(add,epc)

link319<-link319[,..needlist1]


link319u<- uniqueresult(link319)
dim(link319u)


link319d <- doubleresult(link319)
dim(link319d)

dim(epc)
#
epc <- matchleft(epc,link319)
dim(epc)
# 790825     28
#################### method 320 ##################

function320<- function(x,y){
  
  # x$bnstreet <-    paste(x$saotext,x$paostartnumber,sep=",")
  # 
  # x$bnstreet <-    paste(x$bnstreet,x$paoendnumber,sep="-")
  # x$bnstreet <-    paste(x$bnstreet,x$streetdescription,sep=" ")
  # x$bnstreet <- gsub("/", "", x$bnstreet)
  # x$bnstreet <- gsub("[.]", "", x$bnstreet)
  # x$bnstreet <- gsub("[']", "", x$bnstreet)
  # x$bnstreet <- gsub(" ", "", x$bnstreet)
  # x$addressf <-paste(x$postcodelocator,x$bnstreet,sep=",")
  # 
  # 
  # y$addressfinal <- y$add
  # y$addressfinal <- gsub("APARTMENT", "ROOM", y$addressfinal)
  # y$addressfinal <- gsub("[/]", "", y$addressfinal)
  # y$addressfinal <- gsub("[.]", "", y$addressfinal)
  # y$addressfinal <- gsub("[']", "", y$addressfinal)
  # y$addressfinal <- gsub(" ", "", y$addressfinal)
  # y$addressf <- paste(y$postcode,y$addressfinal,sep=",")
  # 
  # taba1 <- inner_join(x,y,by="addressf")
  # 
  # return(taba1) 
  
  x$bnstreet <-    paste(x$saotext,x$pp,sep=",")
  
  x$bnstreet <-    paste(x$bnstreet,x$streetdescription,sep=" ")
  x$bnstreet <- gsub("/", "", x$bnstreet)
  x$bnstreet <- gsub("[.]", "", x$bnstreet)
  x$bnstreet <- gsub("[']", "", x$bnstreet)
  x$bnstreet <- gsub(" ", "", x$bnstreet)
  x$addressf <-paste(x$postcodelocator,x$bnstreet,sep=",")
  
  
  y<-y[y$property_type=="Flat"|y$property_type=="Maisonette",]
  y$addressfinal <- y$add1
  y$addressfinal <-  paste("APARTMENT ",y$addressfinal,sep=" ")
  y$addressfinal <-  paste( y$addressfinal,y$add3,sep=",")
  y$addressfinal <- gsub("[/]", "", y$addressfinal)
  y$addressfinal <- gsub("[.]", "", y$addressfinal)
  y$addressfinal <- gsub("[']", "", y$addressfinal)
  
  y$addressfinal <- gsub(" ", "", y$addressfinal)
  y$addressf <- paste(y$postcode,y$addressfinal,sep=",")
  
  taba1 <- inner_join(x,y,by="addressf")
  return(taba1)

}


link320<-function320(add,epc)

length(unique(link320$lmk_key))
link320<-link320[,..needlist1]


link320u<- uniqueresult(link320)
dim(link320u)


link320d <- doubleresult(link320)
dim(link320d)

dim(epc)
#
epc <- matchleft(epc,link320)
dim(epc)
#2329732      32

#################### method 321 ##################

function321<- function(x,y){
  
  x<-x[x$subbuildingname!="",]
  
  x$bnstreet <- paste(x$subbuildingname,x$streetdescription,sep=" ")
  x$bnstreet <- gsub("/", "", x$bnstreet)
  x$bnstreet <- gsub("[.]", "", x$bnstreet)
  x$bnstreet <- gsub("[']", "", x$bnstreet)
  x$bnstreet <- gsub(" ", "", x$bnstreet)
  x$addressf <-paste(x$postcodelocator,x$bnstreet,sep=",")
  
  y<-y[y$property_type=="Flat"|y$property_type=="Maisonette",]
  y$addressfinal <- paste(y$add1,y$add3,sep=" ")
  y$addressfinal <- gsub("APARTMENT", "FLAT", y$addressfinal)

  y$addressfinal <- gsub("[/]", "", y$addressfinal)
  y$addressfinal <- gsub("[.]", "", y$addressfinal)
  y$addressfinal <- gsub("[']", "", y$addressfinal)
  y$addressfinal <- gsub(",", "", y$addressfinal)
  y$addressfinal <- gsub(" ", "", y$addressfinal)
  y$addressf <- paste(y$postcode,y$addressfinal,sep=",")
  
  taba1 <- inner_join(x,y,by="addressf")
  
  return(taba1)
 
}


link321<-function321(add,epc)


link321<-link321[,..needlist1]


link321u<- uniqueresult(link321)
dim(link321u)


link321d <- doubleresult(link321)
dim(link321d)

dim(epc)
#
epc <- matchleft(epc,link321)
dim(epc)
#2328669      32


#################### method 322  SW8 2PX ##################


function322<- function(x,y){
  
  # 
  # x$bnstreet <-    paste(x$saotext,x$pp,sep=",")
  # 
  # 
  # x$bnstreet <-    paste(x$bnstreet,x$streetdescription,sep=" ")
  # x$bnstreet <- gsub("/", "", x$bnstreet)
  # x$bnstreet <- gsub("[.]", "", x$bnstreet)
  # x$bnstreet <- gsub("[']", "", x$bnstreet)
  # x$bnstreet <- gsub(" ", "", x$bnstreet)
  # x$addressf <-paste(x$postcodelocator,x$bnstreet,sep=",")
  # 
  # y[postcode=="SW8 2PX",add3:=gsub("COURLAND ROAD", "COURLAND GROVE",add3)]
  # #y<-y[y$property_type=="Flat"|y$property_type=="Maisonette",]
  # y$addressfinal <- paste(y$add1,y$add3,sep=",")
  # 
  # #y$addressfinal <- gsub("FLAT", "APARTMENT", y$addressfinal)
  # y$addressfinal <- gsub("[/]", "", y$addressfinal)
  # y$addressfinal <- gsub("[.]", "", y$addressfinal)
  # y$addressfinal <- gsub("[']", "", y$addressfinal)
  # y$addressfinal <- gsub(" ", "", y$addressfinal)
  # y$addressf <- paste(y$postcode,y$addressfinal,sep=",")
  # 
  # taba1 <- inner_join(x,y,by="addressf")
  # 
  # return(taba1) 
  x$bnstreet <-    paste(x$saotext,x$pp,sep=",")
  x$bnstreet <-    paste(x$bnstreet,x$streetdescription,sep=" ")
  
  x$bnstreet <- gsub("[.]", "", x$bnstreet)
  x$bnstreet <- gsub("[/]", "", x$bnstreet)
  x$bnstreet <- gsub("[']", "", x$bnstreet)
  x$bnstreet <- gsub(" ", "", x$bnstreet)
  x$addressf <-paste(x$postcodelocator,x$bnstreet,sep=",")
  
  

  
  y<-y[y$property_type=="Flat"|y$property_type=="Maisonette",]
  y$addressfinal <- paste(y$add1,y$add3,sep=",")
  
  y$addressfinal <- gsub("FLAT", "APARTMENT", y$addressfinal)
  y$addressfinal <- gsub("[.]", "", y$addressfinal)
  y$addressfinal <- gsub("[/]", "", y$addressfinal)
  y$addressfinal <- gsub("[']", "", y$addressfinal)
  
  y$addressfinal <- gsub(" ", "", y$addressfinal)
  y$addressf <- paste(y$postcode,y$addressfinal,sep=",")
  
  taba1 <- inner_join(x,y,by="addressf")
  return(taba1)
}

link322<-function322(add,epc)


# link322_1<-link322[ grepl("^\\d",link322$ss) ,]
# 
# link322_2<-link322[grepl("\\d$",link322$subbuildingname) ,]
# 
# 
# link322_2<-link322_2[link322_2$lmk_key %in% link322_1$lmk_key,]
# link322<-link322[!(link322$lmk_key %in% link322_2$lmk_key),]
# 
# rm(link322_1,link322_2)

link322<-link322[,..needlist1]


link322u<- uniqueresult(link322)
dim(link322u)


link322d <- doubleresult(link322)
dim(link322d)


dim(epc)
#
epc <- matchleft(epc,link322)
dim(epc)
#789495     28



#################### method 323 manually ##################


epc1 <- matchleft(epc1,link300)
epc1 <- matchleft(epc1,link301)
epc1 <- matchleft(epc1,link302)
epc1 <- matchleft(epc1,link303)
epc1 <- matchleft(epc1,link304)
epc1 <- matchleft(epc1,link305)
epc1 <- matchleft(epc1,link306)
epc1 <- matchleft(epc1,link307)
epc1 <- matchleft(epc1,link308)
epc1 <- matchleft(epc1,link309)

epc1 <- matchleft(epc1,link310)
epc1 <- matchleft(epc1,link311)
epc1 <- matchleft(epc1,link312)
epc1 <- matchleft(epc1,link313)
epc1 <- matchleft(epc1,link314)
epc1 <- matchleft(epc1,link315)
epc1 <- matchleft(epc1,link316)
epc1 <- matchleft(epc1,link317)
epc1 <- matchleft(epc1,link318)
epc1 <- matchleft(epc1,link319)
epc1 <- matchleft(epc1,link320)

epc1 <- matchleft(epc1,link321)
epc1 <- matchleft(epc1,link322)


dim(epc1)
function323<- function(x,y){
  x$bnstreet <-    paste(x$pp,x$streetdescription,sep=",")
  x$bnstreet <- gsub("/", "", x$bnstreet)
  x$bnstreet <- gsub("[.]", "", x$bnstreet)
  x$bnstreet <- gsub("[']", "", x$bnstreet)
  x$bnstreet <- gsub(" ", "", x$bnstreet)
  x$addressf <-paste(x$postset,x$bnstreet,sep=",")
  
  y$addressfinal <- trimws(y$add)
  y$addressfinal <- gsub("[/]", "", y$addressfinal)
  y$addressfinal <- gsub("[.]", "", y$addressfinal)
  y$addressfinal <- gsub("[']", "", y$addressfinal)
  y$addressfinal <- gsub(" ", "", y$addressfinal)
  y$addressf <- paste(y$postset,y$addressfinal,sep=",")
  
  taba1 <- inner_join(x,y,by="addressf")
  

  
}



link323<-function323(add,epc)
dim(link323)
# 
link323<-link323[,..needlist11]

link323u<- uniqueresult(link323)
dim(link323u)
#

link323d <- doubleresult(link323)
dim(link323d)


dim(epc)
#
epc <- matchleft(epc,link323)
dim(epc)
#  791517     28


#################### method 324  ##################

function324<- function(x,y){
   x$bnstreet <-    paste(x$buildingnumber,x$streetdescription,sep=",")
  x$bnstreet <-    paste(x$bnstreet,x$townname,sep=",")
  x$bnstreet <- gsub("/", "", x$bnstreet)
  x$bnstreet <- gsub("[.]", "", x$bnstreet)
  x$bnstreet <- gsub("[']", "", x$bnstreet)
  x$bnstreet <- gsub(" ", "", x$bnstreet)
  x$addressf <-paste(x$postset,x$bnstreet,sep=",")
  
  y$addressfinal <- trimws(y$add)
  y$addressfinal <- gsub("[/]", "", y$addressfinal)
  y$addressfinal <- gsub("[.]", "", y$addressfinal)
  y$addressfinal <- gsub("[']", "", y$addressfinal)
  y$addressfinal <- gsub(" ", "", y$addressfinal)
  y$addressf <- paste(y$postset,y$addressfinal,sep=",")
  
  taba1 <- inner_join(x,y,by="addressf")
  
  return(taba1)
  


}


link324<-function324(add,epc)



link324<-link324[,..needlist11]


link324u<- uniqueresult(link324)
dim(link324u)


link324d <- doubleresult(link324)



dim(epc)
# 
epc<- matchleft(epc,link324)
dim(epc)
# 790108     28
#################### method 325 ##################


function325<- function(x,y){
    x$bnstreet <-    paste(x$pp,x$streetdescription,sep=",")
  x$bnstreet <-    paste(x$bnstreet,x$townname,sep=",")
  x$bnstreet <- gsub("/", "", x$bnstreet)
  x$bnstreet <- gsub("[.]", "", x$bnstreet)
  x$bnstreet <- gsub("[']", "", x$bnstreet)
  x$bnstreet <- gsub(" ", "", x$bnstreet)
  x$addressf <-paste(x$postset,x$bnstreet,sep=",")
  
  y$addressfinal <- trimws(y$add)
  y$addressfinal <- gsub("[/]", "", y$addressfinal)
  y$addressfinal <- gsub("[.]", "", y$addressfinal)
  y$addressfinal <- gsub("[']", "", y$addressfinal)
  y$addressfinal <- gsub(" ", "", y$addressfinal)
  y$addressf <- paste(y$postset,y$addressfinal,sep=",")
  
  taba1 <- inner_join(x,y,by="addressf")
  
  return(taba1)

}



link325<-function325(add,epc)


link325<-link325[,..needlist11]


link325u<- uniqueresult(link325)
dim(link325u)


link325d <- doubleresult(link325)
dim(link325d)


dim(epc)
#
epc <- matchleft(epc,link325)
dim(epc)
#2320324      32

#
#################### method 326 ##################

function326<- function(x,y){

x$bnstreet <-    paste(x$buildingname,x$streetdescription,sep=",")
  x$bnstreet <- gsub("/", "", x$bnstreet)
  x$bnstreet <- gsub("[.]", "", x$bnstreet)
  x$bnstreet <- gsub("[']", "", x$bnstreet)
  x$bnstreet <- gsub(" ", "", x$bnstreet)
  x$addressf <-paste(x$postset,x$bnstreet,sep=",")
  
  y$addressfinal <- trimws(y$add)
  y$addressfinal <- gsub("[/]", "", y$addressfinal)
  y$addressfinal <- gsub("[.]", "", y$addressfinal)
  y$addressfinal <- gsub("[']", "", y$addressfinal)
  y$addressfinal <- gsub(" ", "", y$addressfinal)
  y$addressf <- paste(y$postset,y$addressfinal,sep=",")
  
  taba1 <- inner_join(x,y,by="addressf")
  
  return(taba1)
}




link326<-function326(add,epc)
dim(link326)
# 

link326<-link326[,..needlist11]



link326u<- uniqueresult(link326)
dim(link326u)
# 

link326d <- doubleresult(link326)
dim(link326d)


epc <- matchleft(epc,link326)
dim(epc)
# 790409     28

#################### method 327 ##################



function327<- function(x,y){
  
 x$bnstreet <-    paste(x$buildingnumber,x$streetdescription,sep=",")
  x$bnstreet <-    paste(x$bnstreet,x$dependentlocality,sep=",")
  x$bnstreet <- gsub("/", "", x$bnstreet)
  x$bnstreet <- gsub("[.]", "", x$bnstreet)
  x$bnstreet <- gsub("[']", "", x$bnstreet)
  x$bnstreet <- gsub(" ", "", x$bnstreet)
  x$addressf <-paste(x$postset,x$bnstreet,sep=",")
  
  
  
  y$addressfinal <- trimws(y$add)
  y$addressfinal <- gsub("[/]", "", y$addressfinal)
  y$addressfinal <- gsub("[.]", "", y$addressfinal)
  y$addressfinal <- gsub("[']", "", y$addressfinal)
  y$addressfinal <- gsub(" ", "", y$addressfinal)
  y$addressf <- paste(y$postset,y$addressfinal,sep=",")
  
  taba1 <- inner_join(x,y,by="addressf")
  
  
  return(taba1)
  
}

link327<-function327(add,epc)
dim(link327)


link327<-link327[,..needlist11]

link327u<- uniqueresult(link327)
dim(link327u)


link327d <- doubleresult(link327)
dim(link327d)


dim(epc)
# 
epc <- matchleft(epc,link327)
dim(epc)
#790000     28
#
#################### method 328 ##################

function328<- function(x,y){
 x$bnstreet <-    paste(x$pp,x$streetdescription,sep=",")
  x$bnstreet <-    paste(x$bnstreet,x$dependentlocality,sep=",")
  x$bnstreet <- gsub("/", "", x$bnstreet)
  x$bnstreet <- gsub("[.]", "", x$bnstreet)
  x$bnstreet <- gsub("[']", "", x$bnstreet)
  x$bnstreet <- gsub(" ", "", x$bnstreet)
  x$addressf <-paste(x$postset,x$bnstreet,sep=",")
  
  y$addressfinal <- trimws(y$add)
  y$addressfinal <- gsub("[/]", "", y$addressfinal)
  y$addressfinal <- gsub("[.]", "", y$addressfinal)
  y$addressfinal <- gsub("[']", "", y$addressfinal)
  y$addressfinal <- gsub(" ", "", y$addressfinal)
  y$addressf <- paste(y$postset,y$addressfinal,sep=",")
  
  taba1 <- inner_join(x,y,by="addressf")
  
  
  return(taba1) 
  

}



link328<-function328(add,epc)
dim(link328)


link328<-link328[,..needlist11]



link328u<- uniqueresult(link328)
dim(link328u)


link328d <- doubleresult(link328)
dim(link328d)


dim(epc)
# 
epc <- matchleft(epc,link328)
dim(epc)
#2329509      32
#################### method 329 ##################
function329<- function(x,y){
 
   x$bnstreet <-    paste(x$buildingnumber,x$streetdescription,sep=",")
  x$bnstreet <-    paste(x$bnstreet,x$locality,sep=",")
  x$bnstreet <- gsub("/", "", x$bnstreet)
  x$bnstreet <- gsub("[.]", "", x$bnstreet)
  x$bnstreet <- gsub("[']", "", x$bnstreet)
  x$bnstreet <- gsub(" ", "", x$bnstreet)
  x$addressf <-paste(x$postset,x$bnstreet,sep=",")
  
  
  
  y$addressfinal <- trimws(y$add)
  y$addressfinal <- gsub("[/]", "", y$addressfinal)
  y$addressfinal <- gsub("[.]", "", y$addressfinal)
  y$addressfinal <- gsub("[']", "", y$addressfinal)
  y$addressfinal <- gsub(" ", "", y$addressfinal)
  y$addressf <- paste(y$postset,y$addressfinal,sep=",")
  
  taba1 <- inner_join(x,y,by="addressf")
  
  
  return(taba1) 
  
}


link329<-function329(add,epc)

link329<-link329[,..needlist11]


link329u<- uniqueresult(link329)
dim(link329u)


link329d <- doubleresult(link329)
dim(link329d)

dim(epc)
#3249854      32
epc <- matchleft(epc,link329)
dim(epc)
#



#################### method 330 ##################

function330<- function(x,y){
  x$bnstreet <-    paste(x$pp,x$streetdescription,sep=",")
  x$bnstreet <-    paste(x$bnstreet,x$locality,sep=",")
  x$bnstreet <- gsub("/", "", x$bnstreet)
  x$bnstreet <- gsub("[.]", "", x$bnstreet)
  x$bnstreet <- gsub("[']", "", x$bnstreet)
  x$bnstreet <- gsub(" ", "", x$bnstreet)
  x$addressf <-paste(x$postset,x$bnstreet,sep=",")
  
  y$addressfinal <- trimws(y$add)
  y$addressfinal <- gsub("[/]", "", y$addressfinal)
  y$addressfinal <- gsub("[.]", "", y$addressfinal)
  y$addressfinal <- gsub("[']", "", y$addressfinal)
  y$addressfinal <- gsub(" ", "", y$addressfinal)
  y$addressf <- paste(y$postset,y$addressfinal,sep=",")
  
  taba1 <- inner_join(x,y,by="addressf")
  
  
  return(taba1)
  
}


link330<-function330(add,epc)

length(unique(link330$lmk_key))
link330<-link330[,..needlist11]


link330u<- uniqueresult(link330)
dim(link330u)


link330d <- doubleresult(link330)
dim(link330d)

dim(epc)
#
epc <- matchleft(epc,link330)
dim(epc)
#2339733      33


(dim(epcdata)[1]-dim(epc)[1])/dim(epcdata)[1]
length(unique(linku1$lmk_key))

#################### method 331 manually##################

function331<- function(x,y){

  x$bnstreet <-    paste(x$buildingname,x$streetdescription,sep=",")
  x$bnstreet <-    paste(x$bnstreet,x$dependentlocality,sep=",")
  x$bnstreet <- gsub("/", "", x$bnstreet)
  x$bnstreet <- gsub("[.]", "", x$bnstreet)
  x$bnstreet <- gsub("[']", "", x$bnstreet)
  x$bnstreet <- gsub(" ", "", x$bnstreet)
  x$addressf <-paste(x$postset,x$bnstreet,sep=",")
  
  y$addressfinal <- trimws(y$add)
  y$addressfinal <- gsub("[/]", "", y$addressfinal)
  y$addressfinal <- gsub("[.]", "", y$addressfinal)
  y$addressfinal <- gsub("[']", "", y$addressfinal)
  y$addressfinal <- gsub(" ", "", y$addressfinal)
  y$addressf <- paste(y$postset,y$addressfinal,sep=",")
  
  taba1 <- inner_join(x,y,by="addressf")
  
  return(taba1)
}


link331<-function331(add,epc)


link331<-link331[,..needlist11]


link331u<- uniqueresult(link331)
dim(link331u)


link331d <- doubleresult(link331)
dim(link331d)

dim(epc)
#
epc <- matchleft(epc,link331)
dim(epc)
#2338669      33


#################### method 332 ##################


function332<- function(x,y){
  x$bnstreet <-    paste(x$buildingname,x$streetdescription,sep=",")
  x$bnstreet <-    paste(x$bnstreet,x$locality,sep=",")
  x$bnstreet <- gsub("/", "", x$bnstreet)
  x$bnstreet <- gsub("[.]", "", x$bnstreet)
  x$bnstreet <- gsub("[']", "", x$bnstreet)
  x$bnstreet <- gsub(" ", "", x$bnstreet)
  x$addressf <-paste(x$postset,x$bnstreet,sep=",")
  
  y$addressfinal <- trimws(y$add)
  y$addressfinal <- gsub("[/]", "", y$addressfinal)
  y$addressfinal <- gsub("[.]", "", y$addressfinal)
  y$addressfinal <- gsub("[']", "", y$addressfinal)
  y$addressfinal <- gsub(" ", "", y$addressfinal)
  y$addressf <- paste(y$postset,y$addressfinal,sep=",")
  
  taba1 <- inner_join(x,y,by="addressf")
  
  return(taba1)
  
  
}

link332<-function332(add,epc)

link332<-link332[,..needlist11]


link332u<- uniqueresult(link332)
dim(link332u)


link332d <- doubleresult(link332)
dim(link332d)


dim(epc)
#
epc <- matchleft(epc,link332)
dim(epc)
# 2337648      33



#################### method 333  ##################

function333<- function(x,y){
  

  x$bnstreet <-    paste(x$paotext,x$streetdescription,sep=",")
  x$bnstreet <-    paste(x$bnstreet,x$dependentlocality,sep=",")
  x$bnstreet <- gsub("/", "", x$bnstreet)
  x$bnstreet <- gsub("[.]", "", x$bnstreet)
  x$bnstreet <- gsub("[']", "", x$bnstreet)
  x$bnstreet <- gsub(" ", "", x$bnstreet)
  x$addressf <-paste(x$postset,x$bnstreet,sep=",")
  
  y$addressfinal <- trimws(y$add)
  y$addressfinal <- gsub("[/]", "", y$addressfinal)
  y$addressfinal <- gsub("[.]", "", y$addressfinal)
  y$addressfinal <- gsub("[']", "", y$addressfinal)
  y$addressfinal <- gsub(" ", "", y$addressfinal)
  y$addressf <- paste(y$postset,y$addressfinal,sep=",")
  
  taba1 <- inner_join(x,y,by="addressf")
  
  return(taba1) 
  
}



link333<-function333(add,epc)
dim(link333)
# 
link333<-link333[,..needlist11]

link333u<- uniqueresult(link333)
dim(link333u)
#

link333d <- doubleresult(link333)
dim(link333d)


dim(epc)
#
epc <- matchleft(epc,link333)
dim(epc)
# 2337634      33


#################### method 334  ##################

function334<- function(x,y){
  
 x$bnstreet <-    paste(x$paotext,x$streetdescription,sep=",")
  x$bnstreet <-    paste(x$bnstreet,x$locality,sep=",")
  x$bnstreet <- gsub("/", "", x$bnstreet)
  x$bnstreet <- gsub("[.]", "", x$bnstreet)
  x$bnstreet <- gsub("[']", "", x$bnstreet)
  x$bnstreet <- gsub(" ", "", x$bnstreet)
  x$addressf <-paste(x$postset,x$bnstreet,sep=",")
  
  y$addressfinal <- trimws(y$add)
  y$addressfinal <- gsub("[/]", "", y$addressfinal)
  y$addressfinal <- gsub("[.]", "", y$addressfinal)
  y$addressfinal <- gsub("[']", "", y$addressfinal)
  y$addressfinal <- gsub(" ", "", y$addressfinal)
  y$addressf <- paste(y$postset,y$addressfinal,sep=",")
  
  taba1 <- inner_join(x,y,by="addressf")
  
  return(taba1) 
  
}


link334<-function334(add,epc)



link334<-link334[,..needlist11]


link334u<- uniqueresult(link334)
dim(link334u)


link334d <- doubleresult(link334)
# head(link334[grepl("[']", link334$add),])
# head(link334[grepl("[.]", link334$add),])

dim(epc)
# 
epc<- matchleft(epc,link334)
dim(epc)
#  2333364      33
#################### method 335 ##################


function335<- function(x,y){
  
  
  x$bnstreet <-    paste(x$subbuildingname,x$buildingname,sep=",")
  #x$bnstreet <-    paste(x$bnstreet,x$locality,sep=",")
  x$bnstreet <- gsub("/", "", x$bnstreet)
  x$bnstreet <- gsub("[.]", "", x$bnstreet)
  x$bnstreet <- gsub("[']", "", x$bnstreet)
  x$bnstreet <- gsub(" ", "", x$bnstreet)
  x$addressf <-paste(x$postset,x$bnstreet,sep=",")
  
  y$addressfinal <- trimws(y$add)
  y$addressfinal <- gsub("[/]", "", y$addressfinal)
  y$addressfinal <- gsub("[.]", "", y$addressfinal)
  y$addressfinal <- gsub("[']", "", y$addressfinal)
  y$addressfinal <- gsub(" ", "", y$addressfinal)
  y$addressf <- paste(y$postset,y$addressfinal,sep=",")
  
  taba1 <- inner_join(x,y,by="addressf")
  
}



link335<-function335(add,epc)


link335<-link335[,..needlist11]


link335u<- uniqueresult(link335)
dim(link335u)


link335d <- doubleresult(link335)
dim(link335d)


dim(epc)
#
epc <- matchleft(epc,link335)
dim(epc)
#2330334      33

#
#################### method 336 ##################

function336<- function(x,y){
  
   x$bnstreet <-    paste(x$buildingname,x$dependentlocality,sep=",")
  x$bnstreet <- gsub("/", "", x$bnstreet)
  x$bnstreet <- gsub("[.]", "", x$bnstreet)
  x$bnstreet <- gsub("[']", "", x$bnstreet)
  x$bnstreet <- gsub(" ", "", x$bnstreet)
  x$addressf <-paste(x$postset,x$bnstreet,sep=",")
  
  y$addressfinal <- trimws(y$add)
  y$addressfinal <- gsub("[/]", "", y$addressfinal)
  y$addressfinal <- gsub("[.]", "", y$addressfinal)
  y$addressfinal <- gsub("[']", "", y$addressfinal)
  y$addressfinal <- gsub(" ", "", y$addressfinal)
  y$addressf <- paste(y$postset,y$addressfinal,sep=",")
  
  taba1 <- inner_join(x,y,by="addressf")
  
  return(taba1)
}




link336<-function336(add,epc)
dim(link336)
# 

link336<-link336[,..needlist11]



link336u<- uniqueresult(link336)
dim(link336u)
# 

link336d <- doubleresult(link336)
dim(link336d)


epc <- matchleft(epc,link336)
dim(epc)
# 2330331      33

#################### method 337 ##################



function337<- function(x,y){
    x$bnstreet <-    paste(x$buildingname,x$locality,sep=",")
  x$bnstreet <- gsub("/", "", x$bnstreet)
  x$bnstreet <- gsub("[.]", "", x$bnstreet)
  x$bnstreet <- gsub("[']", "", x$bnstreet)
  x$bnstreet <- gsub(" ", "", x$bnstreet)
  x$addressf <-paste(x$postset,x$bnstreet,sep=",")
  
  y$addressfinal <- trimws(y$add)
  y$addressfinal <- gsub("[/]", "", y$addressfinal)
  y$addressfinal <- gsub("[.]", "", y$addressfinal)
  y$addressfinal <- gsub("[']", "", y$addressfinal)
  y$addressfinal <- gsub(" ", "", y$addressfinal)
  y$addressf <- paste(y$postset,y$addressfinal,sep=",")
  
  taba1 <- inner_join(x,y,by="addressf")
  
  return(taba1)
  
  
}

link337<-function337(add,epc)
dim(link337)


link337<-link337[,..needlist11]

link337u<- uniqueresult(link337)
dim(link337u)


link337d <- doubleresult(link337)
dim(link337d)


dim(epc)
# 
epc <- matchleft(epc,link337)
dim(epc)
#2330190      33
#
#################### method 338 ##################

function338<- function(x,y){
  x$bnstreet <-    paste(x$subbuildingname,x$buildingname,sep=" ")
  x$bnstreet <-    paste(x$bnstreet,x$streetdescription,sep=",")
  x$bnstreet <- gsub("/", "", x$bnstreet)
  x$bnstreet <- gsub("[.]", "", x$bnstreet)
  x$bnstreet <- gsub("[']", "", x$bnstreet)
  x$bnstreet <- gsub(" ", "", x$bnstreet)
  x$addressf <-paste(x$postset,x$bnstreet,sep=",")
  
  y$addressfinal <- trimws(y$add)
  y$addressfinal <- gsub("[/]", "", y$addressfinal)
  y$addressfinal <- gsub("[.]", "", y$addressfinal)
  y$addressfinal <- gsub("[']", "", y$addressfinal)
  y$addressfinal <- gsub(" ", "", y$addressfinal)
  y$addressf <- paste(y$postset,y$addressfinal,sep=",")
  
  taba1 <- inner_join(x,y,by="addressf")
  
  return(taba1)
}



link338<-function338(add,epc)
dim(link338)


link338<-link338[,..needlist11]



link338u<- uniqueresult(link338)
dim(link338u)


link338d <- doubleresult(link338)
dim(link338d)


dim(epc)
# 
epc <- matchleft(epc,link338)
dim(epc)
#2339509      33
#################### method 339 ##################
function339<- function(x,y){
  
  x$bnstreet <-    paste(x$saotext,x$buildingname,sep=" ")
  x$bnstreet <-    paste(x$bnstreet,x$streetdescription,sep=",")
  x$bnstreet <- gsub("/", "", x$bnstreet)
  x$bnstreet <- gsub("[.]", "", x$bnstreet)
  x$bnstreet <- gsub("[']", "", x$bnstreet)
  x$bnstreet <- gsub(" ", "", x$bnstreet)
  x$addressf <-paste(x$postset,x$bnstreet,sep=",")
  
  y$addressfinal <- trimws(y$add)
  y$addressfinal <- gsub("[/]", "", y$addressfinal)
  y$addressfinal <- gsub("[.]", "", y$addressfinal)
  y$addressfinal <- gsub("[']", "", y$addressfinal)
  y$addressfinal <- gsub(" ", "", y$addressfinal)
  y$addressf <- paste(y$postset,y$addressfinal,sep=",")
  
  taba1 <- inner_join(x,y,by="addressf")
  
  return(taba1)
}


link339<-function339(add,epc)

link339<-link339[,..needlist11]


link339u<- uniqueresult(link339)
dim(link339u)


link339d <- doubleresult(link339)
dim(link339d)

dim(epc)
#3349854      33
epc <- matchleft(epc,link339)
dim(epc)
#


#################### method 340 ##################
function340<- function(x,y){
   x$bnstreet <-    paste(x$subbuildingname,x$ss,sep=" ")
  x$bnstreet <-    paste(x$bnstreet,x$paotext,sep=",")
  x$bnstreet <-    paste(x$bnstreet,x$streetdescription,sep=",")
  x$bnstreet <- gsub("/", "", x$bnstreet)
  x$bnstreet <- gsub("[.]", "", x$bnstreet)
  x$bnstreet <- gsub("[']", "", x$bnstreet)
  x$bnstreet <- gsub(" ", "", x$bnstreet)
  x$addressf <-paste(x$postset,x$bnstreet,sep=",")
  
  y$addressfinal <- trimws(y$add)
  y$addressfinal <- gsub("[/]", "", y$addressfinal)
  y$addressfinal <- gsub("[.]", "", y$addressfinal)
  y$addressfinal <- gsub("[']", "", y$addressfinal)
  y$addressfinal <- gsub(" ", "", y$addressfinal)
  y$addressf <- paste(y$postset,y$addressfinal,sep=",")
  
  taba1 <- inner_join(x,y,by="addressf")
  
  return(taba1)
 
  
}

link340<-function340(add,epc)
length(unique(link340$lmk_key))
link340_1<-link340[ grepl("^\\d",link340$ss) ,]

link340_2<-link340[grepl("\\d$",link340$subbuildingname) ,]


link340_2<-link340_2[link340_2$lmk_key %in% link340_1$lmk_key,]
link340<-link340[!(link340$lmk_key %in% link340_2$lmk_key),]

rm(link340_1,link340_2)




length(unique(link340$lmk_key))

link340<-link340[,..needlist11]

link340u<- uniqueresult(link340)

dim(link340u)


link340d <- doubleresult(link340)
dim(link340d)

dim(epc)
#
epc <- matchleft(epc,link340)
dim(epc)
#777396     28

#################### method 341 manually##################

function341<- function(x,y){
  
  x<-x[x$buildingnumber=="",]
  x$bnstreet <-    paste(x$paotext,x$streetdescription,sep=",")
  x$bnstreet <- gsub("['] ", "", x$bnstreet)
  x$bnstreet <- gsub("[.]", "", x$bnstreet)
  x$bnstreet <- gsub(" ", "", x$bnstreet)
  x$addressf <-paste(x$postset,x$bnstreet,sep=",")
  
  y$addressfinal <- trimws(y$add)
  y$addressfinal <- gsub("[']", "", y$addressfinal)
  y$addressfinal <- gsub("[.]", "", y$addressfinal)
  y$addressfinal <- gsub(" ", "", y$addressfinal)
  y$addressf <- paste(y$postset,y$addressfinal,sep=",")
  
  taba1 <- inner_join(x,y,by="addressf")
  return(taba1)
  
  
  
}


link341<-function341(add,epc)


link341<-link341[,..needlist11]


link341u<- uniqueresult(link341)
dim(link341u)


link341d <- doubleresult(link341)
dim(link341d)

dim(epc)
#
epc <- matchleft(epc,link341)
dim(epc)
#2348669      34


#################### method 342 ##################


function342<- function(x,y){
  

    
  x$bnstreet <-    paste(x$subbuildingname,x$buildingnumber,sep=",")
  x$bnstreet <-    paste(x$bnstreet,x$streetdescription,sep=",")
  x$bnstreet <- gsub("/", "", x$bnstreet)
  x$bnstreet <- gsub("[.]", "", x$bnstreet)
  x$bnstreet <- gsub("[']", "", x$bnstreet)
  x$bnstreet <- gsub(" ", "", x$bnstreet)
  x$addressf <-paste(x$postset,x$bnstreet,sep=",")
  
  y$addressfinal <- trimws(y$add)
  y$addressfinal <- gsub("[/]", "", y$addressfinal)
  y$addressfinal <- gsub("[.]", "", y$addressfinal)
  y$addressfinal <- gsub("[']", "", y$addressfinal)
  y$addressfinal <- gsub(" ", "", y$addressfinal)
  y$addressf <- paste(y$postset,y$addressfinal,sep=",")
  
  taba1 <- inner_join(x,y,by="addressf")
  
  return(taba1)
}

link342<-function342(add,epc)

link342<-link342[,..needlist11]


link342u<- uniqueresult(link342)
dim(link342u)


link342d <- doubleresult(link342)
dim(link342d)


dim(epc)
#
epc <- matchleft(epc,link342)
dim(epc)
# 2347648      34



#################### method 343 ##################

function343<- function(x,y){
   
  x$bnstreet <-    paste(x$saotext,x$buildingnumber,sep=",")
  x$bnstreet <-    paste(x$bnstreet,x$streetdescription,sep=",")
  x$bnstreet <- gsub("/", "", x$bnstreet)
  x$bnstreet <- gsub("[.]", "", x$bnstreet)
  x$bnstreet <- gsub("[']", "", x$bnstreet)
  x$bnstreet <- gsub(" ", "", x$bnstreet)
  x$addressf <-paste(x$postset,x$bnstreet,sep=",")
  
  y$addressfinal <- trimws(y$add)
  y$addressfinal <- gsub("[/]", "", y$addressfinal)
  y$addressfinal <- gsub("[.]", "", y$addressfinal)
  y$addressfinal <- gsub("[']", "", y$addressfinal)
  y$addressfinal <- gsub(" ", "", y$addressfinal)
  y$addressf <- paste(y$postset,y$addressfinal,sep=",")
  
  taba1 <- inner_join(x,y,by="addressf")
  
  return(taba1)
  
}



link343<-function343(add,epc)
dim(link343)
# 
link343<-link343[,..needlist11]

link343u<- uniqueresult(link343)
dim(link343u)
#

link343d <- doubleresult(link343)
dim(link343d)


dim(epc)
#
epc <- matchleft(epc,link343)
dim(epc)
# 2347634      34


#################### method 344  ##################

function344<- function(x,y){
  
  x$bnstreet <-    paste(x$subbuildingname,x$buildingnumber,sep=",")
  x$bnstreet <-    paste(x$bnstreet,x$streetdescription,sep=",")
  x$bnstreet <- paste(x$bnstreet,x$dependentlocality,sep=",")
  x$bnstreet <- gsub("/", "", x$bnstreet)
  x$bnstreet <- gsub("[.]", "", x$bnstreet)
  x$bnstreet <- gsub("[']", "", x$bnstreet)
  x$bnstreet <- gsub(" ", "", x$bnstreet)
  x$addressf <-paste(x$postset,x$bnstreet,sep=",")
  
  y$addressfinal <- trimws(y$add)
  y$addressfinal <- gsub("[/]", "", y$addressfinal)
  y$addressfinal <- gsub("[.]", "", y$addressfinal)
  y$addressfinal <- gsub("[']", "", y$addressfinal)
  y$addressfinal <- gsub(" ", "", y$addressfinal)
  y$addressf <- paste(y$postset,y$addressfinal,sep=",")
  
  taba1 <- inner_join(x,y,by="addressf")
  return(taba1)
  
}


link344<-function344(add,epc)



link344<-link344[,..needlist11]


link344u<- uniqueresult(link344)
dim(link344u)


link344d <- doubleresult(link344)

dim(epc)
# 
epc<- matchleft(epc,link344)
dim(epc)
#   777220     28
#################### method 345 ##################


function345<- function(x,y){
  
 x$bnstreet <-    paste(x$saotext,x$buildingnumber,sep=",")
  x$bnstreet <-    paste(x$bnstreet,x$streetdescription,sep=",")
  x$bnstreet <- paste(x$bnstreet,x$dependentlocality,sep=",")
  x$bnstreet <- gsub(" ", "", x$bnstreet)
  x$bnstreet <- gsub("/", "", x$bnstreet)
  x$bnstreet <- gsub("[.]", "", x$bnstreet)
  x$bnstreet <- gsub("[']", "", x$bnstreet)
  x$addressf <-paste(x$postset,x$bnstreet,sep=",")
  
  y$addressfinal <- trimws(y$add)
  y$addressfinal <- gsub("[/]", "", y$addressfinal)
  y$addressfinal <- gsub("[.]", "", y$addressfinal)
  y$addressfinal <- gsub("[']", "", y$addressfinal)
  y$addressfinal <- gsub(" ", "", y$addressfinal)
  y$addressf <- paste(y$postset,y$addressfinal,sep=",")
  
  taba1 <- inner_join(x,y,by="addressf")
  return(taba1)
  
}



link345<-function345(add,epc)


link345<-link345[,..needlist11]


link345u<- uniqueresult(link345)
dim(link345u)


link345d <- doubleresult(link345)
dim(link345d)


dim(epc)
#
epc <- matchleft(epc,link345)
dim(epc)
#2340344      34

#
#################### method 346 ##################

function346<- function(x,y){

   x$bnstreet <-    paste(x$subbuildingname,x$buildingnumber,sep=",")
  x$bnstreet <-    paste(x$bnstreet,x$streetdescription,sep=",")
  x$bnstreet <- paste(x$bnstreet,x$locality,sep=",")
  x$bnstreet <- gsub("/", "", x$bnstreet)
  x$bnstreet <- gsub("[.]", "", x$bnstreet)
  x$bnstreet <- gsub("[']", "", x$bnstreet)
  x$bnstreet <- gsub(" ", "", x$bnstreet)
  x$addressf <-paste(x$postset,x$bnstreet,sep=",")
  
  y$addressfinal <- trimws(y$add)
  y$addressfinal <- gsub("[/]", "", y$addressfinal)
  y$addressfinal <- gsub("[.]", "", y$addressfinal)
  y$addressfinal <- gsub("[']", "", y$addressfinal)
  y$addressfinal <- gsub(" ", "", y$addressfinal)
  y$addressf <- paste(y$postset,y$addressfinal,sep=",")
  
  taba1 <- inner_join(x,y,by="addressf")
  return(taba1)
}




link346<-function346(add,epc)
dim(link346)
# 

link346<-link346[,..needlist11]



link346u<- uniqueresult(link346)
dim(link346u)
# 

link346d <- doubleresult(link346)
dim(link346d)


epc <- matchleft(epc,link346)
dim(epc)
# 2340341      34

#################### method 347 ##################



function347<- function(x,y){
  
 x$bnstreet <-    paste(x$subbuildingname,x$buildingname,sep=" ")
  x$bnstreet <-    paste(x$bnstreet,x$buildingnumber,sep=",")
  x$bnstreet <-    paste(x$bnstreet,x$streetdescription,sep=",")
  x$bnstreet <- gsub("/", "", x$bnstreet)
  x$bnstreet <- gsub("[.]", "", x$bnstreet)
  x$bnstreet <- gsub("[']", "", x$bnstreet)
  x$bnstreet <- gsub(" ", "", x$bnstreet)
  x$addressf <-paste(x$postset,x$bnstreet,sep=",")
  
  y$addressfinal <- trimws(y$add)
  y$addressfinal <- gsub("[/]", "", y$addressfinal)
  y$addressfinal <- gsub("[.]", "", y$addressfinal)
  y$addressfinal <- gsub("[']", "", y$addressfinal)
  y$addressfinal <- gsub(" ", "", y$addressfinal)
  y$addressf <- paste(y$postset,y$addressfinal,sep=",")
  
  taba1 <- inner_join(x,y,by="addressf")
  
  return(taba1)
}

link347<-function347(add,epc)
dim(link347)


link347<-link347[,..needlist11]

link347u<- uniqueresult(link347)
dim(link347u)


link347d <- doubleresult(link347)
dim(link347d)


dim(epc)
# 
epc <- matchleft(epc,link347)
dim(epc)
#2340190      34
#
#################### method 348 ##################

function348<- function(x,y){
  
    x$bnstreet <-    paste(x$buildingname,x$paotext,sep=",")
  
  x$bnstreet <- gsub("/", "", x$bnstreet)
  x$bnstreet <- gsub("[.]", "", x$bnstreet)
  x$bnstreet <- gsub("[']", "", x$bnstreet)
  x$bnstreet <- gsub(" ", "", x$bnstreet)
  x$addressf <-paste(x$postset,x$bnstreet,sep=",")
  
  y$addressfinal <- trimws(y$add) 
  
  y$addressfinal <- gsub("[/]", "", y$addressfinal)
  y$addressfinal <- gsub("[.]", "", y$addressfinal)
  y$addressfinal <- gsub("[']", "", y$addressfinal)
  y$addressfinal <- gsub(" ", "", y$addressfinal)
  y$addressf <- paste(y$postset,y$addressfinal,sep=",")
  
  taba1 <- inner_join(x,y,by="addressf")
  return(taba1)

  
}



link348<-function348(add,epc)
dim(link348)


link348<-link348[,..needlist11]



link348u<- uniqueresult(link348)
dim(link348u)


link348d <- doubleresult(link348)
dim(link348d)


dim(epc)
# 
epc <- matchleft(epc,link348)
dim(epc)
#2349509      34
#################### method 349 ##################
function349<- function(x,y){

  x$bnstreet <-    paste(x$buildingnumber,x$streetdescription,sep=",")
  x$bnstreet <- gsub("/", "", x$bnstreet)
  x$bnstreet <- gsub("[.]", "", x$bnstreet)
  x$bnstreet <- gsub("[']", "", x$bnstreet)
  x$bnstreet <- gsub(" ", "", x$bnstreet)
  x$addressf <-paste(x$postset,x$bnstreet,sep=",")
  
  y$addressfinal <- trimws(y$add)
  y$addressfinal <- gsub("[/]", "", y$addressfinal)
  y$addressfinal <- gsub("[.]", "", y$addressfinal)
  y$addressfinal <- gsub("[']", "", y$addressfinal)
  y$addressfinal <- gsub(" ", "", y$addressfinal)
  y$addressf <- paste(y$postset,y$addressfinal,sep=",")
  
  taba1 <- inner_join(x,y,by="addressf")
  
  
}
link349<-function349(add,epc)

link349<-link349[,..needlist11]


link349u<- uniqueresult(link349)
dim(link349u)


link349d <- doubleresult(link349)
dim(link349d)

dim(epc)

epc <- matchleft(epc,link349)
dim(epc)
# 776564     28



#################### method 350 ##################

function350<- function(x,y){
  x$bnstreet <-    paste(x$saotext,x$paotext,sep=",")
  x$bnstreet <-    paste(x$bnstreet,x$streetdescription,sep=",")
  #x$bnstreet <-    paste(x$saotext,x$paotext,sep=",")
  x$bnstreet <- gsub("/", "", x$bnstreet)
  x$bnstreet <- gsub("[.]", "", x$bnstreet)
  x$bnstreet <- gsub("[']", "", x$bnstreet)
  x$bnstreet <- gsub(" ", "", x$bnstreet)
  x$addressf <-paste(x$postset,x$bnstreet,sep=",")
  
  y$addressfinal <- trimws(y$add) 
  
  y$addressfinal <- gsub("[/]", "", y$addressfinal)
  y$addressfinal <- gsub("[.]", "", y$addressfinal)
  y$addressfinal <- gsub("[']", "", y$addressfinal)
  y$addressfinal <- gsub(" ", "", y$addressfinal)
  y$addressf <- paste(y$postset,y$addressfinal,sep=",")
  
  taba1 <- inner_join(x,y,by="addressf")
  return(taba1) 
  
  
}


link350<-function350(add,epc)

length(unique(link350$lmk_key))
link350<-link350[,..needlist11]


link350u<- uniqueresult(link350)
dim(link350u)


link350d <- doubleresult(link350)
dim(link350d)

dim(epc)
#
epc <- matchleft(epc,link350)
dim(epc)
#2359735      35

#################### method 351 ##################

function351<- function(x,y){
  
  
  x$bnstreet <-    paste(x$saotext,x$paotext,sep=",")
  x$bnstreet <-    paste(x$bnstreet,x$buildingnumber,sep=",")
  
  x$bnstreet <-    paste(x$saotext,x$streetdescription,sep=",")
  
  x$bnstreet <- gsub("/", "", x$bnstreet)
  x$bnstreet <- gsub("[.]", "", x$bnstreet)
  x$bnstreet <- gsub("[']", "", x$bnstreet)
  x$bnstreet <- gsub(" ", "", x$bnstreet)
  x$addressf <-paste(x$postset,x$bnstreet,sep=",")
  
  y$addressfinal <- trimws(y$add) 
  
  y$addressfinal <- gsub("[/]", "", y$addressfinal)
  y$addressfinal <- gsub("[.]", "", y$addressfinal)
  y$addressfinal <- gsub("[']", "", y$addressfinal)
  y$addressfinal <- gsub(" ", "", y$addressfinal)
  y$addressf <- paste(y$postset,y$addressfinal,sep=",")
  
  taba1 <- inner_join(x,y,by="addressf")
  return(taba1) 
  
}


link351<-function351(add,epc)


link351<-link351[,..needlist11]


link351u<- uniqueresult(link351)
dim(link351u)


link351d <- doubleresult(link351)
dim(link351d)

dim(epc)
#
epc <- matchleft(epc,link351)
dim(epc)
#2358669      35


#################### method 352 ##################


function352<- function(x,y){
  
  x$bnstreet <-    paste(x$saotext,x$paotext,sep=",")
  x$bnstreet <-    paste(x$bnstreet,x$buildingnumber,sep=",")
  x$bnstreet <-    paste(x$bnstreet,x$streetdescription,sep=" ")
  x$bnstreet <- gsub("/", "", x$bnstreet)
  x$bnstreet <- gsub("[.]", "", x$bnstreet)
  x$bnstreet <- gsub("[']", "", x$bnstreet)
  x$bnstreet <- gsub(" ", "", x$bnstreet)
  x$addressf <-paste(x$postset,x$bnstreet,sep=",")
  
  y$addressfinal <- trimws(y$add) 
  
  y$addressfinal <- gsub("[/]", "", y$addressfinal)
  y$addressfinal <- gsub("[.]", "", y$addressfinal)
  y$addressfinal <- gsub("[']", "", y$addressfinal)
  y$addressfinal <- gsub(" ", "", y$addressfinal)
  y$addressf <- paste(y$postset,y$addressfinal,sep=",")
  
  taba1 <- inner_join(x,y,by="addressf")
  return(taba1)
  
  
}

link352<-function352(add,epc)

link352<-link352[,..needlist11]


link352u<- uniqueresult(link352)
dim(link352u)


link352d <- doubleresult(link352)
dim(link352d)


dim(epc)
#
epc <- matchleft(epc,link352)
dim(epc)
# 2357648      35



#################### method 353 ##################

function353<- function(x,y){
  
  
  x$bnstreet <-    paste(x$saotext,x$paotext,sep=",")
  x$bnstreet <-    paste(x$bnstreet,x$pp,sep=",")
  x$bnstreet <-    paste(x$bnstreet,x$streetdescription,sep=" ")
  x$bnstreet <- gsub("/", "", x$bnstreet)
  x$bnstreet <- gsub("[.]", "", x$bnstreet)
  x$bnstreet <- gsub("[']", "", x$bnstreet)
  x$bnstreet <- gsub(" ", "", x$bnstreet)
  x$addressf <-paste(x$postset,x$bnstreet,sep=",")
  
  y$addressfinal <- trimws(y$add) 
  
  y$addressfinal <- gsub("[/]", "", y$addressfinal)
  y$addressfinal <- gsub("[.]", "", y$addressfinal)
  y$addressfinal <- gsub("[']", "", y$addressfinal)
  y$addressfinal <- gsub(" ", "", y$addressfinal)
  y$addressf <- paste(y$postset,y$addressfinal,sep=",")
  
  taba1 <- inner_join(x,y,by="addressf")
  return(taba1)
  
}



link353<-function353(add,epc)
dim(link353)
# 
link353<-link353[,..needlist11]

link353u<- uniqueresult(link353)
dim(link353u)
#

link353d <- doubleresult(link353)
dim(link353d)


dim(epc)
#
epc <- matchleft(epc,link353)
dim(epc)
# 779855     28


#################### method 354  ##################

function354<- function(x,y){
  
  
  x$bnstreet <-    paste(x$saotext,x$paotext,sep=",")
  
  x$bnstreet <- gsub("/", "", x$bnstreet)
  x$bnstreet <- gsub("[.]", "", x$bnstreet)
  x$bnstreet <- gsub("[']", "", x$bnstreet)
  x$bnstreet <- gsub(" ", "", x$bnstreet)
  x$addressf <-paste(x$postset,x$bnstreet,sep=",")
  
  y$addressfinal <- trimws(y$add) 
  
  y$addressfinal <- gsub("[/]", "", y$addressfinal)
  y$addressfinal <- gsub("[.]", "", y$addressfinal)
  y$addressfinal <- gsub("[']", "", y$addressfinal)
  y$addressfinal <- gsub(" ", "", y$addressfinal)
  y$addressf <- paste(y$postset,y$addressfinal,sep=",")
  
  
  taba1 <- inner_join(x,y,by="addressf")
  return(taba1)
  
  
  
  
}


link354<-function354(add,epc)



link354<-link354[,..needlist11]


link354u<- uniqueresult(link354)
dim(link354u)


link354d <- doubleresult(link354)
# head(link354[grepl("[']", link354$add),])
# head(link354[grepl("[.]", link354$add),])

dim(epc)
# 
epc<- matchleft(epc,link354)
dim(epc)
#  2353564      35
#################### method 355 check##################

#rm(list=setdiff(ls(), ("epc","add","doubleresult","uniqueresult"))


function355<- function(x,y){
  
  #x$bnstreet <-    paste(x$subbuildingname ,x$buildingname,sep=",")
  x$bnstreet <-    paste(x$saotext,x$paotext,sep=",")
  
  
  x$bnstreet <-    paste(x$bnstreet,x$paostartnumber,sep=",")
  x$bnstreet <-    paste(x$bnstreet,x$paoendnumber,sep="-")
  #x$bnstreet <-    paste(x$bnstreet,x$streetdescription,sep=" ")
  
  #x$bnstreetnew <-    word(x$streetdescription,1,2)
  #x$bnstreet <-    paste(x$bnstreet,x$bnstreetnew,sep=" ")
  x$bnstreet <- gsub("/", "", x$bnstreet)
  x$bnstreet <- gsub("[.]", "", x$bnstreet)
  x$bnstreet <- gsub("[']", "", x$bnstreet)
  x$bnstreet <- gsub(" ", "", x$bnstreet)
  x$addressf <-paste(x$postset,x$bnstreet,sep=",")
  
  
  
  y$addressfinal <- paste(y$add1,y$add2,sep=",")
  y$addressfinal2 <- word(y$add3,1)
  y$addressfinal <- paste(y$addressfinal,y$addressfinal2,sep=",")
  
  y$addressfinal <- gsub("[/]", "", y$addressfinal)
  y$addressfinal <- gsub("[.]", "", y$addressfinal)
  y$addressfinal <- gsub("[']", "", y$addressfinal)
  y$addressfinal <- gsub(" ", "", y$addressfinal)
  y$addressf <- paste(y$postset,y$addressfinal,sep=",")
  
  taba1 <- inner_join(x,y,by="addressf")
  return(taba1)
  
}



link355<-function355(add,epc)


link355<-link355[,..needlist11]


link355u<- uniqueresult(link355)
dim(link355u)


link355d <- doubleresult(link355)
dim(link355d)


dim(epc)
#
epc <- matchleft(epc,link355)
dim(epc)
#2350354      35

#
#################### method 356 postcode ##################

function356<- function(x,y){
 
  x$bnstreet <-    paste(x$saotext,x$streetdescription,sep=" ")
  
  x$bnstreet <- gsub("/", "", x$bnstreet)
  x$bnstreet <- gsub("[.]", "", x$bnstreet)
  x$bnstreet <- gsub("[']", "", x$bnstreet)
  x$bnstreet <- gsub(" ", "", x$bnstreet)
  x$addressf <-paste(x$postset,x$bnstreet,sep=",")
  
  y$addressfinal <- trimws(y$add) 
  
  y$addressfinal <- gsub("[/]", "", y$addressfinal)
  y$addressfinal <- gsub("[.]", "", y$addressfinal)
  y$addressfinal <- gsub("[']", "", y$addressfinal)
  y$addressfinal <- gsub(" ", "", y$addressfinal)
  y$addressf <- paste(y$postset,y$addressfinal,sep=",")
  
  taba1 <- inner_join(x,y,by="addressf")
  return(taba1) 
}




link356<-function356(add,epc)
dim(link356)
# 

link356<-link356[,..needlist1]



link356u<- uniqueresult(link356)
dim(link356u)
# 

link356d <- doubleresult(link356)
dim(link356d)


epc <- matchleft(epc,link356)
dim(epc)
# 2350351      35

#################### method 357 ##################



function357<- function(x,y){
  
  x$bnstreet <-    paste(x$saotext,x$paotext,sep=" ")
  x$bnstreet <- gsub(",", "", x$bnstreet)
  x$bnstreet <- gsub("/", "", x$bnstreet)
  x$bnstreet <- gsub("[.]", "", x$bnstreet)
  x$bnstreet <- gsub("[']", "", x$bnstreet)
  x$bnstreet <- gsub(" ", "", x$bnstreet)
  x$addressf <-paste(x$postset,x$bnstreet,sep=",")
  
  y<-y[y$property_type=="Flat"|y$property_type=="Maisonette",]
  y$addressfinal <-  paste("APARTMENT ",y$add1,sep="")
  y$addressfinal <- gsub(",", "", y$addressfinal)
  y$addressfinal <- gsub("[/]", "", y$addressfinal)
  y$addressfinal <- gsub("[.]", "", y$addressfinal)
  y$addressfinal <- gsub("[']", "", y$addressfinal)
  y$addressfinal <- gsub(" ", "", y$addressfinal)
  y$addressf <- paste(y$postset,y$addressfinal,sep=",")
  
  taba1 <- inner_join(x,y,by="addressf")
  return(taba1)
  
}

link357<-function357(add,epc)
dim(link357)


link357<-link357[,..needlist11]

link357u<- uniqueresult(link357)
dim(link357u)


link357d <- doubleresult(link357)
dim(link357d)


dim(epc)
# 
epc <- matchleft(epc,link357)
dim(epc)
#2350190      35
#
#################### method 358 ##################

function358<- function(x,y){
  
  x$bnstreet <-    paste(x$subbuildingname,x$paotext,sep=",")
  #x$bnstreet <-    paste(x$bnstreet,x$pp,sep=",")
  x$bnstreet <-    paste(x$bnstreet,x$streetdescription,sep=",")
  x$bnstreet <- gsub("/", "", x$bnstreet)
  x$bnstreet <- gsub("[.]", "", x$bnstreet)
  x$bnstreet <- gsub("[']", "", x$bnstreet)
  x$bnstreet <- gsub(" ", "", x$bnstreet)
  x$addressf <-paste(x$postset,x$bnstreet,sep=",")
  
  y$addressfinal <- trimws(y$add) 
  
  y$addressfinal <- gsub("[/]", "", y$addressfinal)
  y$addressfinal <- gsub("[.]", "", y$addressfinal)
  y$addressfinal <- gsub("[']", "", y$addressfinal)
  y$addressfinal <- gsub(" ", "", y$addressfinal)
  y$addressf <- paste(y$postset,y$addressfinal,sep=",")
  
  taba1 <- inner_join(x,y,by="addressf")
  return(taba1)
  
}



link358<-function358(add,epc)
dim(link358)


link358<-link358[,..needlist11]



link358u<- uniqueresult(link358)
dim(link358u)


link358d <- doubleresult(link358)
dim(link358d)


dim(epc)
# 
epc <- matchleft(epc,link358)
dim(epc)
#2359509      35
#################### method 359 ##################
function359<- function(x,y){
  
  x$bnstreet <-    paste("STUDIO ",x$subbuildingname,sep=" ")
  x$bnstreet <-    paste(x$bnstreet,x$paotext,sep=" ")
  x$bnstreet <-    paste(x$bnstreet,x$buildingnumber,sep=" ")
  x$bnstreet <-    paste(x$bnstreet,x$streetdescription,sep=" ")
  x$bnstreet <- gsub(",", "", x$bnstreet)
  x$bnstreet <- gsub("/", "", x$bnstreet)
  x$bnstreet <- gsub("[.]", "", x$bnstreet)
  x$bnstreet <- gsub("[']", "", x$bnstreet)
  x$bnstreet <- gsub(" ", "", x$bnstreet)
  x$addressf <-paste(x$postset,x$bnstreet,sep=",")
  
  y<-y[y$property_type=="Flat"|y$property_type=="Maisonette",]
  y$addressfinal <-  y$add
  
  y$addressfinal <- gsub("-", "", y$addressfinal)
  y$addressfinal <- gsub("[/]", "", y$addressfinal)
  y$addressfinal <- gsub("[.]", "", y$addressfinal)
  y$addressfinal <- gsub("[']", "", y$addressfinal)
  y$addressfinal <- gsub(" ", "", y$addressfinal)
  y$addressf <- paste(y$postset,y$addressfinal,sep=",")
  
  taba1 <- inner_join(x,y,by="addressf")
  return(taba1)
}


link359<-function359(add,epc)

link359<-link359[,..needlist11]


link359u<- uniqueresult(link359)
dim(link359u)


link359d <- doubleresult(link359)
dim(link359d)

dim(epc)
#3549854      35
epc <- matchleft(epc,link359)
dim(epc)
# 761745     28

####sum up#####



link300u$method<-"link300u"
link301u$method<-"link301u"
link302u$method<-"link302u"
link303u$method<-"link303u"
link304u$method<-"link304u"
link305u$method<-"link305u"
link306u$method<-"link306u"
link307u$method<-"link307u"
link308u$method<-"link308u"
link309u$method<-"link309u"
link310u$method<-"link310u"
link311u$method<-"link311u"
link312u$method<-"link312u"
link313u$method<-"link313u"
link314u$method<-"link314u"
link315u$method<-"link315u"
link316u$method<-"link316u"
link317u$method<-"link317u"
link318u$method<-"link318u"
link319u$method<-"link319u"
link320u$method<-"link320u"
link321u$method<-"link321u"
link322u$method<-"link322u"
link323u$method<-"link323u"
link324u$method<-"link324u"
link325u$method<-"link325u"
link326u$method<-"link326u"
link327u$method<-"link327u"

link328u$method<-"link328u"
link329u$method<-"link329u"
link330u$method<-"link330u"
link331u$method<-"link331u"
link332u$method<-"link332u"
link333u$method<-"link333u"
link334u$method<-"link334u"
link335u$method<-"link335u"
link336u$method<-"link336u"
link337u$method<-"link337u"
link338u$method<-"link338u"
link339u$method<-"link339u"








link340u$method<-"link340u"
link341u$method<-"link341u"
link342u$method<-"link342u"
link343u$method<-"link343u"
link344u$method<-"link344u"
link345u$method<-"link345u"
link346u$method<-"link346u"
link347u$method<-"link347u"
link348u$method<-"link348u"
link349u$method<-"link349u"
link350u$method<-"link350u"
link351u$method<-"link351u"
link352u$method<-"link352u"
link353u$method<-"link353u"
link354u$method<-"link354u"
link355u$method<-"link355u"
link356u$method<-"link356u"
link357u$method<-"link357u"
link358u$method<-"link358u"
link359u$method<-"link359u"




link300d$method<-"link300d"
link301d$method<-"link301d"
link302d$method<-"link302d"
link303d$method<-"link303d"
link304d$method<-"link304d"
link305d$method<-"link305d"
link306d$method<-"link306d"
link307d$method<-"link307d"
link308d$method<-"link308d"
link309d$method<-"link309d"
link310d$method<-"link310d"
link311d$method<-"link311d"
link312d$method<-"link312d"
link313d$method<-"link313d"
link314d$method<-"link314d"
link315d$method<-"link315d"
link316d$method<-"link316d"
link317d$method<-"link317d"
link318d$method<-"link318d"
link319d$method<-"link319d"
link320d$method<-"link320d"
link321d$method<-"link321d"
link322d$method<-"link322d"
link323d$method<-"link323d"
link324d$method<-"link324d"
link325d$method<-"link325d"
link326d$method<-"link326d"
link327d$method<-"link327d"

link328d$method<-"link328d"
link329d$method<-"link329d"
link330d$method<-"link330d"
link331d$method<-"link331d"
link332d$method<-"link332d"
link333d$method<-"link333d"
link334d$method<-"link334d"
link335d$method<-"link335d"
link336d$method<-"link336d"
link337d$method<-"link337d"
link338d$method<-"link338d"
link339d$method<-"link339d"







link340d$method<-"link340d"
link341d$method<-"link341d"
link342d$method<-"link342d"
link343d$method<-"link343d"
link344d$method<-"link344d"
link345d$method<-"link345d"
link346d$method<-"link346d"
link347d$method<-"link347d"
link348d$method<-"link348d"
link349d$method<-"link349d"
link350d$method<-"link350d"
link351d$method<-"link351d"
link352d$method<-"link352d"
link353d$method<-"link353d"
link354d$method<-"link354d"
link355d$method<-"link355d"
link356d$method<-"link356d"
link357d$method<-"link357d"
link358d$method<-"link358d"
link359d$method<-"link359d"

#link349u<-link349u[!(link349u$lmk_key %in% link348u$lmk_key),]

dim(link300u)[1]+dim(link301u)[1]+dim(link302u)[1]+dim(link303u)[1]+dim(link304u)[1]+dim(link305u)[1]+dim(link306u)[1]+dim(link307u)[1]+dim(link308u)[1]+dim(link309u)[1]+
  dim(link310u)[1]+dim(link311u)[1]+dim(link312u)[1]+dim(link313u)[1]+dim(link314u)[1]+dim(link315u)[1]+dim(link316u)[1]+dim(link317u)[1]+dim(link318u)[1]+dim(link319u)[1]+
  dim(link320u)[1]+dim(link321u)[1]+dim(link322u)[1]+dim(link323u)[1]+dim(link324u)[1]+dim(link325u)[1]+dim(link326u)[1]+dim(link327u)[1]+dim(link334u)[1]+dim(link335u)[1]+
  dim(link340u)[1]+dim(link341u)[1]+dim(link342u)[1]+dim(link343u)[1]+dim(link344u)[1]+dim(link345u)[1]+dim(link346u)[1]+dim(link347u)[1]+dim(link348u)[1]+dim(link349u)[1]+
  dim(link350u)[1]+dim(link351u)[1]+dim(link352u)[1]+dim(link353u)[1]+dim(link354u)[1]+dim(link355u)[1]+dim(link356u)[1]+dim(link357u)[1]+dim(link358u)[1]+dim(link359u)[1]

# 


l300_359u = list(link300u,link301u,link302u,link303u,link304u,link305u,link306u,link307u,link308u,link309u,
                 link310u,link311u,link312u,link313u,link314u,link315u,link316u,link317u,link318u,link319u,
                 link320u,link321u,link322u,link323u,link324u,link325u,link326u,link327u,link328u,link329u,
                 link330u,link331u,link332u,link333u,link334u,link335u,link336u,link337u,link338u,link339u,
                 link340u,link341u,link342u,link343u,link344u,link345u,link346u,link347u,link348u,link349u,
                 link350u,link351u,link352u,link353u,link354u,link355u,link356u,link357u,link358u,link359u)

#



link300_359u<- rbindlist(l300_359u,use.names=TRUE,fill=T)
dim(link300_359u)
# 38151
# 38480 
length(unique(link300_359u$lmk_key))
#38480 
length(unique(link300_359u$method))
#60
#tt<-doubleresult(link300_359u)
# link300_359u_new<- rbindlist(list(link300_359u,link328u,link329u, link330u,link331u,link332u,link333u,link336u,link337u,link338u,link339u),use.names=TRUE,fill=T)
# 
# dim(link300_359u_new)
# 24572    34
#length(unique(link300_359u_new$lmk_key))


l300_359d = list(link300d,link301d,link302d,link303d,link304d,link305d,link306d,link307d,link308d,link309d,
                 link310d,link311d,link312d,link313d,link314d,link315d,link316d,link317d,link318d,link319d,
                 link320d,link321d,link322d,link323d,link324d,link325d,link326d,link327d,link328d,link329d,
                 link330d,link331d,link332d,link333d,link334d,link335d,link336d,link337d,link338d,link339d,
                 link340d,link341d,link342d,link343d,link344d,link345d,link346d,link347d,link348d,link349d,
                 link350d,link351d,link352d,link353d,link354d,link355d,link356d,link357d,link358d,link359d)

link300_359d<- rbindlist(l300_359d,use.names=TRUE,fill=T)
dim(link300_359d)
# 6722
# link300_359d_new<- rbindlist(list(link300_359d,link328d,link329d, link330d,link331d,link332d,link333d,link336d,link337d,link338d,link339d),use.names=TRUE,fill=T)
# 
# dim(link300_359d_new)
# # 4924   34


dim(link300d)[1]+dim(link301d)[1]+dim(link302d)[1]+dim(link303d)[1]+dim(link304d)[1]+dim(link305d)[1]+dim(link306d)[1]+dim(link307d)[1]+dim(link308d)[1]+dim(link309d)[1]+
  dim(link310d)[1]+dim(link311d)[1]+dim(link312d)[1]+dim(link313d)[1]+dim(link314d)[1]+dim(link315d)[1]+dim(link316d)[1]+dim(link317d)[1]+dim(link318d)[1]+dim(link319d)[1]+
  dim(link320d)[1]+dim(link321d)[1]+dim(link322d)[1]+dim(link323d)[1]+dim(link324d)[1]+dim(link325d)[1]+dim(link326d)[1]+dim(link327d)[1]+dim(link328d)[1]+dim(link329d)[1]+
  dim(link330d)[1]+dim(link331d)[1]+dim(link332d)[1]+dim(link333d)[1]+dim(link334d)[1]+dim(link335d)[1]+dim(link336d)[1]+dim(link337d)[1]+dim(link338d)[1]+dim(link339d)[1]+
  dim(link340d)[1]+dim(link341d)[1]+dim(link342d)[1]+dim(link343d)[1]+dim(link344d)[1]+dim(link345d)[1]+dim(link346d)[1]+dim(link347d)[1]+dim(link348d)[1]+dim(link349d)[1]+
  dim(link350d)[1]+dim(link351d)[1]+dim(link352d)[1]+dim(link353d)[1]+dim(link354d)[1]+dim(link355d)[1]+dim(link356d)[1]+dim(link357d)[1]+dim(link358d)[1]+dim(link359d)[1]
# 6722

dbWriteTable(con, "link300_359dnew1",value =link300_359d, append =  TRUE, row.names = FALSE)
dbWriteTable(con, "link300_359unew1",value =link300_359u, append =  TRUE, row.names = FALSE)


rm(l300_359d,l300_359u)
rm(link300d,link301d,link302d,link303d,link304d,link305d,link306d,link307d,link308d,link309d,
   link310d,link311d,link312d,link313d,link314d,link315d,link316d,link317d,link318d,link319d,
   link320d,link321d,link322d,link323d,link324d,link325d,link326d,link327d,link328d,link329d,
   link330d,link331d,link332d,link333d,link334d,link335d,link336d,link337d,link338d,link339d,
   link340d,link341d,link342d,link343d,link344d,link345d,link346d,link347d,link348d,link349d,
   link350d,link351d,link352d,link353d,link354d,link355d,link356d,link357d,link358d,link359d)

rm(link300u,link301u,link302u,link303u,link304u,link305u,link306u,link307u,link308u,link309u,
   link310u,link311u,link312u,link313u,link314u,link315u,link316u,link317u,link318u,link319u,
   link320u,link321u,link322u,link323u,link324u,link325u,link326u,link327u,link328u,link329u,
   link330u,link331u,link332u,link333u,link334u,link335u,link336u,link337u,link338u,link339u,
   link340u,link341u,link342u,link343u,link344u,link345u,link346u,link347u,link348u,link349u,
   link350u,link351u,link352u,link353u,link354u,link355u,link356u,link357u,link358u,link359u)


rm(link300,link301,link302,link303,link304,link305,link306,link307,link308,link309,
   link310,link311,link312,link313,link314,link315,link316,link317,link318,link319,
   link320,link321,link322,link323,link324,link325,link326,link327,link328,link329,
   link330,link331,link332,link333,link334,link335,link336,link337,link338,link339,
   link340,link341,link342,link343,link344,link345,link346,link347,link348,link349,
   link350,link351,link352,link353,link354,link355,link356,link357,link358,link359)

rm(function300,function301,function302,function303,function304,function305,function306,function307,function308,function309,
   function310,function311,function312,function313,function314,function315,function316,function317,function318,function319,
   function320,function321,function322,function323,function324,function325,function326,function327,function328,function329,
   function330,function331,function332,function333,function334,function335,function336,function337,function338,function339,
   function340,function341,function342,function343,function344,function345,function346,function347,function348,function349,
   function350,function351,function352,function353,function354,function355,function356,function357,function358,function359)



epc1<-matchleft(epc1,link300_359d)
epc1<-matchleft(epc1,link300_359u)
dim(epc1)
#761745     27
#763357     27
#################### method 360 ##################
function360<- function(x,y){
  
 x<-x[x$paotext!="",]
  x$bnstreet <-    paste(x$saotext,x$paotext,sep=" ")
  x$bnstreet <- gsub("/", "", x$bnstreet)
  x$bnstreet <- gsub("[.]", "", x$bnstreet)
  x$bnstreet <- gsub("[']", "", x$bnstreet)
  x$bnstreet <- gsub(" ", "", x$bnstreet)
  x$addressf <-paste(x$postset,x$bnstreet,sep=",")
  
  
  y<-y[y$property_type=="Flat"|y$property_type=="Maisonette",]
  #y$addressfinal <- trimws(y$add1)
  y$addressfinal <- paste("FLAT ",y$add1,sep="")
  y$addressfinal <- gsub(",", "", y$addressfinal)
  y$addressfinal <- gsub("[/]", "", y$addressfinal)
  y$addressfinal <- gsub("[.]", "", y$addressfinal)
  y$addressfinal <- gsub("[']", "", y$addressfinal)
  y$addressfinal <- gsub(" ", "", y$addressfinal)
  y$addressf <- paste(y$postset,y$addressfinal,sep=",")
  
  taba1 <- inner_join(x,y,by="addressf")
  
  return(taba1)
  
}

link360<-function360(add,epc)

length(unique(link360$lmk_key))

link360<-link360[,..needlist11]

link360u<- uniqueresult(link360)

dim(link360u)


link360d <- doubleresult(link360)
dim(link360d)

dim(epc)
#
epc <- matchleft(epc,link360)
dim(epc)
#777396     28

#################### method 361 ##################

function361<- function(x,y){
  

   x$bnstreet <-    paste(x$saotext,x$buildingname,sep=",")
  x$bnstreet <-    paste(x$bnstreet,x$streetdescription,sep=",")
  x$bnstreet <-    paste(x$bnstreet,x$dependentlocality ,sep=",")
  x$bnstreet <- gsub("/", "", x$bnstreet)
  x$bnstreet <- gsub("[.]", "", x$bnstreet)
  x$bnstreet <- gsub("[']", "", x$bnstreet)
  x$bnstreet <- gsub(" ", "", x$bnstreet)
  x$addressf <-paste(x$postset,x$bnstreet,sep=",")
  
  y$addressfinal <- trimws(y$add)
  y$addressfinal <- gsub("[/]", "", y$addressfinal)
  y$addressfinal <- gsub("[.]", "", y$addressfinal)
  y$addressfinal <- gsub("[']", "", y$addressfinal)
  y$addressfinal <- gsub(" ", "", y$addressfinal)
  y$addressf <- paste(y$postset,y$addressfinal,sep=",")
  
  taba1 <- inner_join(x,y,by="addressf")
  return(taba1)
  
}


link361<-function361(add,epc)


link361<-link361[,..needlist11]


link361u<- uniqueresult(link361)
dim(link361u)


link361d <- doubleresult(link361)
dim(link361d)

dim(epc)
#
epc <- matchleft(epc,link361)
dim(epc)
#2368669      36


#################### method 362 ##################


function362<- function(x,y){
  
  x$bnstreet <-    paste(x$subbuildingname,x$buildingnumber,sep=",")
  x$bnstreet <-    paste(x$bnstreet,x$streetdescription,sep=" ")
  x$bnstreet <- paste(x$bnstreet,x$dependentlocality,sep=",")
  x$bnstreet <- gsub("/", "", x$bnstreet)
  x$bnstreet <- gsub("[.]", "", x$bnstreet)
  x$bnstreet <- gsub("[']", "", x$bnstreet)
  x$bnstreet <- gsub(" ", "", x$bnstreet)
  x$addressf <-paste(x$postset,x$bnstreet,sep=",")
  
  y$addressfinal <- trimws(y$add)
  y$addressfinal <- gsub(" ", "", y$addressfinal)
  y$addressf <- paste(y$postset,y$addressfinal,sep=",")
  
  taba1 <- inner_join(x,y,by="addressf")
  return(taba1)
  
}

link362<-function362(add,epc)

link362<-link362[,..needlist11]


link362u<- uniqueresult(link362)
dim(link362u)


link362d <- doubleresult(link362)
dim(link362d)


dim(epc)
#
epc <- matchleft(epc,link362)
dim(epc)
# 2367648      36



#################### method 363 ##################

function363<- function(x,y){
  
  x<-x[x$subbuildingname=="",]
  x$bnstreet <-    paste(x$ss,x$streetdescription,sep=",")
  
  x$bnstreet <- gsub(" ", "", x$bnstreet)
  x$addressf <-paste(x$postset,x$bnstreet,sep=",")
  
  y$addressfinal <- trimws(y$add)
  y$addressfinal <- gsub(" ", "", y$addressfinal)
  y$addressf <- paste(y$postset,y$addressfinal,sep=",")
  
  taba1 <- inner_join(x,y,by="addressf")
  
  return(taba1)
}



link363<-function363(add,epc)
dim(link363)
# 
link363<-link363[,..needlist11]

link363u<- uniqueresult(link363)
dim(link363u)
#

link363d <- doubleresult(link363)
dim(link363d)


dim(epc)
#
epc <- matchleft(epc,link363)
dim(epc)
# 2367636      36


#################### method 364  ##################

function364<- function(x,y){
  
    x$bnstreet <-    paste(x$saotext,x$paotext,sep=",")
  x$bnstreet <-    paste(x$bnstreet,x$paostartnumber,sep=",")
  x$bnstreet <-    paste(x$bnstreet,x$paoendnumber,sep="-")
  x$bnstreet <-    paste(x$bnstreet,x$streetdescription,sep=" ")
  x$bnstreet <- gsub("/", "", x$bnstreet)
  x$bnstreet <- gsub("[.]", "", x$bnstreet)
  x$bnstreet <- gsub("[']", "", x$bnstreet)
  x$bnstreet <- gsub(" ", "", x$bnstreet)
  x$addressf <-paste(x$postset,x$bnstreet,sep=",")
  
  
  y$addressfinal <- trimws(y$add) 
  y$addressfinal <- gsub("[/]", "", y$addressfinal)
  y$addressfinal <- gsub("[.]", "", y$addressfinal)
  y$addressfinal <- gsub("[']", "", y$addressfinal)
  y$addressfinal <- gsub(" ", "", y$addressfinal)
  y$addressf <- paste(y$postset,y$addressfinal,sep=",")
  
  taba1 <- inner_join(x,y,by="addressf")
  return(taba1)  
  
}


link364<-function364(add,epc)



link364<-link364[,..needlist11]


link364u<- uniqueresult(link364)
dim(link364u)


link364d <- doubleresult(link364)

dim(epc)
# 
epc<- matchleft(epc,link364)
dim(epc)
#   777220     28
#################### method 365 ##################


function365<- function(x,y){
   x$bnstreet <-    paste(x$subbuildingname,x$paotext,sep=",")
  x$bnstreet <-    paste(x$bnstreet,x$paostartnumber,sep=",")
  x$bnstreet <-    paste(x$bnstreet,x$paoendnumber,sep="-")
  x$bnstreet <-    paste(x$bnstreet,x$streetdescription,sep=" ")
  x$bnstreet <- gsub("/", "", x$bnstreet)
  x$bnstreet <- gsub("[.]", "", x$bnstreet)
  x$bnstreet <- gsub("[']", "", x$bnstreet)
  x$bnstreet <- gsub(" ", "", x$bnstreet)
  x$addressf <-paste(x$postset,x$bnstreet,sep=",")
  
  
  y$addressfinal <- trimws(y$add) 
  y$addressfinal <- gsub("[/]", "", y$addressfinal)
  y$addressfinal <- gsub("[.]", "", y$addressfinal)
  y$addressfinal <- gsub("[']", "", y$addressfinal)
  y$addressfinal <- gsub(" ", "", y$addressfinal)
  y$addressf <- paste(y$postset,y$addressfinal,sep=",")
  
  taba1 <- inner_join(x,y,by="addressf")
  return(taba1)

  
}



link365<-function365(add,epc)


link365<-link365[,..needlist11]


link365u<- uniqueresult(link365)
dim(link365u)


link365d <- doubleresult(link365)
dim(link365d)


dim(epc)
#
epc <- matchleft(epc,link365)
dim(epc)
#2360364      36

#
#################### method 366 ##################

function366<- function(x,y){

  x$bnstreet <-    paste(x$saotext,"NO",sep=",")
  x$bnstreet <-    paste(x$bnstreet,x$pp,sep=" ")
  x$bnstreet <-    paste(x$bnstreet,x$streetdescription,sep=" ")
  
  x$bnstreet <- gsub("/", "", x$bnstreet)
  x$bnstreet <- gsub("[.]", "", x$bnstreet)
  x$bnstreet <- gsub("[']", "", x$bnstreet)
  x$bnstreet <- gsub(" ", "", x$bnstreet)
  x$addressf <-paste(x$postset,x$bnstreet,sep=",")
  
  y$addressfinal <- y$add
  
  y$addressfinal <- gsub("[/]", "", y$addressfinal)
  y$addressfinal <- gsub("[.]", "", y$addressfinal)
  y$addressfinal <- gsub("[']", "", y$addressfinal)
  y$addressfinal <- gsub(" ", "", y$addressfinal)
  y$addressf <- paste(y$postset,y$addressfinal,sep=",")
  
  taba1 <- inner_join(x,y,by="addressf")
  return(taba1)
}




link366<-function366(add,epc)
dim(link366)
# 

link366<-link366[,..needlist11]



link366u<- uniqueresult(link366)
dim(link366u)
# 

link366d <- doubleresult(link366)
dim(link366d)



epc <- matchleft(epc,link366)
dim(epc)
# 2360361      36

#################### method 367 ##################



function367<- function(x,y){
  x$bnstreet <-    paste(x$saotext,x$pp,sep=",")
  x$bnstreet <-    paste(x$bnstreet,x$streetdescription,sep=" ")
  x$bnstreet <- gsub("/", "", x$bnstreet)
  x$bnstreet <- gsub("[.]", "", x$bnstreet)
  x$bnstreet <- gsub("[']", "", x$bnstreet)
  x$bnstreet <- gsub(" ", "", x$bnstreet)
  x$addressf <-paste(x$postset,x$bnstreet,sep=",")
  
  y$addressfinal <- paste(y$add1,y$add3,sep=",")
  y$addressfinal <- gsub("[/]", "", y$addressfinal)
  y$addressfinal <- gsub("[.]", "", y$addressfinal)
  y$addressfinal <- gsub("[']", "", y$addressfinal)
  y$addressfinal <- gsub(" ", "", y$addressfinal)
  y$addressf <- paste(y$postset,y$addressfinal,sep=",")

  taba1 <- inner_join(x,y,by="addressf")
  
  
  return(taba1)
}

link367<-function367(add,epc)
dim(link367)


link367<-link367[,..needlist11]

link367u<- uniqueresult(link367)
dim(link367u)


link367d <- doubleresult(link367)
dim(link367d)


dim(epc)
# 
epc <- matchleft(epc,link367)
dim(epc)
#2360190      36
#
#################### method 368 ##################

function368<- function(x,y){
  x$bnstreet <-    paste(x$saotext,x$buildingnumber,sep=",")
  x$bnstreet <-    paste(x$bnstreet,x$streetdescription,sep=" ")
  x$bnstreet <- gsub("/", "", x$bnstreet)
  x$bnstreet <- gsub("[.]", "", x$bnstreet)
  x$bnstreet <- gsub("[']", "", x$bnstreet)
  x$bnstreet <- gsub(" ", "", x$bnstreet)
  x$addressf <-paste(x$postset,x$bnstreet,sep=",")
  
  
  
  
  y$addressfinal <- y$add
  
  y$addressfinal <- gsub("[/]", "", y$addressfinal)
  y$addressfinal <- gsub("[.]", "", y$addressfinal)
  y$addressfinal <- gsub("[']", "", y$addressfinal)
  y$addressfinal <- gsub(" ", "", y$addressfinal)
  y$addressf <- paste(y$postset,y$addressfinal,sep=",")
  
  taba1 <- inner_join(x,y,by="addressf")
  return(taba1)
  
  
  
}



link368<-function368(add,epc)
dim(link368)


link368<-link368[,..needlist11]



link368u<- uniqueresult(link368)
dim(link368u)


link368d <- doubleresult(link368)
dim(link368d)


dim(epc)
# 
epc <- matchleft(epc,link368)
dim(epc)
#2369509      36
#################### method 369 ##################
function369<- function(x,y){
  x$bnstreet <-    paste(x$subbuildingname,x$buildingnumber,sep=",")
  x$bnstreet <-    paste(x$bnstreet,x$streetdescription,sep=" ")
  x$bnstreet <- gsub("/", "", x$bnstreet)
  x$bnstreet <- gsub("[.]", "", x$bnstreet)
  x$bnstreet <- gsub("[']", "", x$bnstreet)
  x$bnstreet <- gsub(" ", "", x$bnstreet)
  x$addressf <-paste(x$postset,x$bnstreet,sep=",")
  
  
  
  
  y$addressfinal <- paste(y$add1,y$add2,sep=",")
  
  y$addressfinal <- gsub("[/]", "", y$addressfinal)
  y$addressfinal <- gsub("[.]", "", y$addressfinal)
  y$addressfinal <- gsub("[']", "", y$addressfinal)
  y$addressfinal <- gsub(" ", "", y$addressfinal)
  y$addressf <- paste(y$postset,y$addressfinal,sep=",")
  
  taba1 <- inner_join(x,y,by="addressf")
  return(taba1)

  
}
link369<-function369(add,epc)

link369<-link369[,..needlist11]


link369u<- uniqueresult(link369)
dim(link369u)


link369d <- doubleresult(link369)
dim(link369d)

dim(epc)

epc <- matchleft(epc,link369)
dim(epc)
# 776564     28
#################### method 370 postcode##################
function370<- function(x,y){
  
  x$bnstreet <-    paste(x$saotext,x$pp,sep=",")
  x$bnstreet <-    paste(x$bnstreet,x$streetdescription,sep=" ")
  x$bnstreet <- gsub("/", "", x$bnstreet)
  x$bnstreet <- gsub("[.]", "", x$bnstreet)
  x$bnstreet <- gsub("[']", "", x$bnstreet)
  x$bnstreet <- gsub(" ", "", x$bnstreet)
  x$addressf <-paste(x$postset,x$bnstreet,sep=",")
  
  y[postcode=="SW11 7AY",add:=gsub("THE MODERN, BLOCK D, PLOT", "APARTMENT",add)]
  
  y$addressfinal <- y$add
  
  y$addressfinal <- gsub("[/]", "", y$addressfinal)
  y$addressfinal <- gsub("[.]", "", y$addressfinal)
  y$addressfinal <- gsub("[']", "", y$addressfinal)
  y$addressfinal <- gsub(" ", "", y$addressfinal)
  y$addressf <- paste(y$postset,y$addressfinal,sep=",")
  
  taba1 <- inner_join(x,y,by="addressf")
  
  return(taba1)
  
}

link370<-function370(add,epc)

length(unique(link370$lmk_key))

link370<-link370[,..needlist11]

link370u<- uniqueresult(link370)

dim(link370u)


link370d <- doubleresult(link370)
dim(link370d)

dim(epc)
#
epc <- matchleft(epc,link370)
dim(epc)
#777396     28

#################### method 371 ##################

function371<- function(x,y){
  
  
  
  x$bnstreet <-    paste(x$subbuildingname,x$buildingnumber,sep=",")
  x$bnstreet <-    paste(x$bnstreet,x$streetdescription,sep=" ")
  x$bnstreet <- gsub("/", "", x$bnstreet)
  x$bnstreet <- gsub("[.]", "", x$bnstreet)
  x$bnstreet <- gsub("[']", "", x$bnstreet)
  x$bnstreet <- gsub(" ", "", x$bnstreet)
  x$addressf <-paste(x$postset,x$bnstreet,sep=",")
  
  
  y$addressfinal <- y$add
  
  y$addressfinal <- gsub("[/]", "", y$addressfinal)
  y$addressfinal <- gsub("[.]", "", y$addressfinal)
  y$addressfinal <- gsub("[']", "", y$addressfinal)
  y$addressfinal <- gsub(" ", "", y$addressfinal)
  y$addressf <- paste(y$postset,y$addressfinal,sep=",")
  
  taba1 <- inner_join(x,y,by="addressf")
  
  return(taba1)
  
}


link371<-function371(add,epc)


link371<-link371[,..needlist11]


link371u<- uniqueresult(link371)
dim(link371u)


link371d <- doubleresult(link371)
dim(link371d)

dim(epc)
#
epc <- matchleft(epc,link371)
dim(epc)
#2378669      37


#################### method 372 ##################


function372<- function(x,y){
  
  x$bnstreet <-    paste(x$subbuildingname,x$paotext,sep=" ")
  x$bnstreet <-    paste(x$bnstreet,x$buildingnumber,sep=",")
  x$bnstreet <-    paste(x$bnstreet,x$streetdescription,sep=" ")
  x$bnstreet <- gsub("/", "", x$bnstreet)
  x$bnstreet <- gsub("[.]", "", x$bnstreet)
  x$bnstreet <- gsub("[']", "", x$bnstreet)
  x$bnstreet <- gsub(" ", "", x$bnstreet)
  x$addressf <-paste(x$postset,x$bnstreet,sep=",")
  
  
  y$addressfinal <- y$add
  
  y$addressfinal <- gsub("[/]", "", y$addressfinal)
  y$addressfinal <- gsub("[.]", "", y$addressfinal)
  y$addressfinal <- gsub("[']", "", y$addressfinal)
  y$addressfinal <- gsub(" ", "", y$addressfinal)
  y$addressf <- paste(y$postset,y$addressfinal,sep=",")
  
  taba1 <- inner_join(x,y,by="addressf")
  
  return(taba1) 
  
}

link372<-function372(add,epc)

link372<-link372[,..needlist11]


link372u<- uniqueresult(link372)
dim(link372u)


link372d <- doubleresult(link372)
dim(link372d)


dim(epc)
#
epc <- matchleft(epc,link372)
dim(epc)
# 2377648      37



#################### method 373 ##################

function373<- function(x,y){
  
  x$bnstreet <-    paste(x$saotext,x$paostartnumber,sep=",")
  
  x$bnstreet <-    paste(x$bnstreet,x$paoendnumber,sep="-")
  x$bnstreet <-    paste(x$bnstreet,x$streetdescription,sep=" ")
  x$bnstreet <- gsub("/", "", x$bnstreet)
  x$bnstreet <- gsub("[.]", "", x$bnstreet)
  x$bnstreet <- gsub("[']", "", x$bnstreet)
  x$bnstreet <- gsub(" ", "", x$bnstreet)
  x$addressf <-paste(x$postset,x$bnstreet,sep=",")
  
  
  y$addressfinal <- y$add
  
  y$addressfinal <- gsub("[/]", "", y$addressfinal)
  y$addressfinal <- gsub("[.]", "", y$addressfinal)
  y$addressfinal <- gsub("[']", "", y$addressfinal)
  y$addressfinal <- gsub(" ", "", y$addressfinal)
  y$addressf <- paste(y$postset,y$addressfinal,sep=",")
  
  taba1 <- inner_join(x,y,by="addressf")
  
  return(taba1) 
}



link373<-function373(add,epc)
dim(link373)
# 
link373<-link373[,..needlist11]

link373u<- uniqueresult(link373)
dim(link373u)
#

link373d <- doubleresult(link373)
dim(link373d)


dim(epc)
#
epc <- matchleft(epc,link373)
dim(epc)
# 2377637      37


#################### method 374  ##################

function374<- function(x,y){
  x$bnstreet <-    paste(x$ss,x$paotext,sep=" ")
  
  x$bnstreet <-    paste(x$bnstreet,x$streetdescription,sep=",")
  x$bnstreet <- gsub("/", "", x$bnstreet)
  x$bnstreet <- gsub("[.]", "", x$bnstreet)
  x$bnstreet <- gsub("[']", "", x$bnstreet)
  x$bnstreet <- gsub(" ", "", x$bnstreet)
  x$addressf <-paste(x$postset,x$bnstreet,sep=",")
  
  
  y$addressfinal <- y$add
  
  y$addressfinal <- gsub("[/]", "", y$addressfinal)
  y$addressfinal <- gsub("[.]", "", y$addressfinal)
  y$addressfinal <- gsub("[']", "", y$addressfinal)
  y$addressfinal <- gsub(" ", "", y$addressfinal)
  y$addressf <- paste(y$postset,y$addressfinal,sep=",")
  
  taba1 <- inner_join(x,y,by="addressf")
  
  return(taba1) 
  
}


link374<-function374(add,epc)



link374<-link374[,..needlist11]


link374u<- uniqueresult(link374)
dim(link374u)


link374d <- doubleresult(link374)

dim(epc)
# 
epc<- matchleft(epc,link374)
dim(epc)
#   777220     28
#################### method 375 ##################


function375<- function(x,y){
 
  
  x$bnstreet <-    paste(x$saotext,x$paotext,sep=",")

  x$bnstreet <-    paste(x$bnstreet,x$locality,sep=",")
  x$bnstreet <- gsub("/", "", x$bnstreet)
  x$bnstreet <- gsub("[.]", "", x$bnstreet)
  x$bnstreet <- gsub("[']", "", x$bnstreet)
  x$bnstreet <- gsub(" ", "", x$bnstreet)
  x$addressf <-paste(x$postset,x$bnstreet,sep=",")
  
  
  y$addressfinal <- y$add
 
  
  
  y$addressfinal <- gsub("[/]", "", y$addressfinal)
  y$addressfinal <- gsub("[.]", "", y$addressfinal)
  y$addressfinal <- gsub("[']", "", y$addressfinal)
  y$addressfinal <- gsub(" ", "", y$addressfinal)
  y$addressf <- paste(y$postset,y$addressfinal,sep=",")
  
  taba1 <- inner_join(x,y,by="addressf")
  
  return(taba1)
}



link375<-function375(add,epc)


link375<-link375[,..needlist11]


link375u<- uniqueresult(link375)
dim(link375u)


link375d <- doubleresult(link375)
dim(link375d)


dim(epc)
#
epc <- matchleft(epc,link375)
dim(epc)
#2370374      37

#
#################### method 376 ##################

function376<- function(x,y){
  x$bnstreet <-    paste(x$saotext,x$pp,sep=",")
  
  
  x$bnstreet <-    paste(x$bnstreet,x$streetdescription,sep=" ")
  x$bnstreet <- gsub("/", "", x$bnstreet)
  x$bnstreet <- gsub("[.]", "", x$bnstreet)
  x$bnstreet <- gsub("[']", "", x$bnstreet)
  x$bnstreet <- gsub(" ", "", x$bnstreet)
  x$addressf <-paste(x$postset,x$bnstreet,sep=",")
  
 
  y<-y[y$property_type=="Flat"|y$property_type=="Maisonette",]
  y$addressfinal <- paste("FLAT ",y$add,sep="")
 
  y$addressfinal <- gsub("[/]", "", y$addressfinal)
  y$addressfinal <- gsub("[.]", "", y$addressfinal)
  y$addressfinal <- gsub("[']", "", y$addressfinal)
  y$addressfinal <- gsub(" ", "", y$addressfinal)
  y$addressf <- paste(y$postset,y$addressfinal,sep=",")
  
  taba1 <- inner_join(x,y,by="addressf")
  
  return(taba1)
}




link376<-function376(add,epc)
dim(link376)
# 

link376<-link376[,..needlist11]



link376u<- uniqueresult(link376)
dim(link376u)
# 

link376d <- doubleresult(link376)
dim(link376d)


epc <- matchleft(epc,link376)
dim(epc)
# 2370371      37

#################### method 377 ##################



function377<- function(x,y){
  
  x$bnstreet <-    paste(x$saotext,x$pp,sep=",")
  
  
  x$bnstreet <-    paste(x$bnstreet,x$streetdescription,sep=" ")
  x$bnstreet <- gsub("/", "", x$bnstreet)
  x$bnstreet <- gsub("[.]", "", x$bnstreet)
  x$bnstreet <- gsub("[']", "", x$bnstreet)
  x$bnstreet <- gsub(" ", "", x$bnstreet)
  x$addressf <-paste(x$postset,x$bnstreet,sep=",")
  

  y<-y[y$property_type=="Flat"|y$property_type=="Maisonette",]
  y$addressfinal <- paste("APARTMENT ",y$add,sep="")
  
 
  y$addressfinal <- gsub("[/]", "", y$addressfinal)
  y$addressfinal <- gsub("[.]", "", y$addressfinal)
  y$addressfinal <- gsub("[']", "", y$addressfinal)
  y$addressfinal <- gsub(" ", "", y$addressfinal)
  y$addressf <- paste(y$postset,y$addressfinal,sep=",")
  
  taba1 <- inner_join(x,y,by="addressf")
  
  return(taba1)
}

link377<-function377(add,epc)
dim(link377)


link377<-link377[,..needlist11]

link377u<- uniqueresult(link377)
dim(link377u)


link377d <- doubleresult(link377)
dim(link377d)


dim(epc)
# 
epc <- matchleft(epc,link377)
dim(epc)
#2370190      37
#
#################### method 378 ##################

function378<- function(x,y){
  
  x$bnstreet <-    paste(x$subbuildingname,x$buildingnumber,sep=",")
  x$bnstreet <-    paste(x$bnstreet,x$streetdescription,sep=" ")
  
  x$bnstreet <- gsub("/", "", x$bnstreet)
  x$bnstreet <- gsub("[.]", "", x$bnstreet)
  x$bnstreet <- gsub("[']", "", x$bnstreet)
  x$bnstreet <- gsub(" ", "", x$bnstreet)
  x$addressf <-paste(x$postset,x$bnstreet,sep=",")
  
 
  y$addressfinal <- y$add
  
  y$addressfinal <- gsub("ROOM", "FLAT", y$addressfinal)
  y$addressfinal <- gsub("[/]", "", y$addressfinal)
  y$addressfinal <- gsub("[.]", "", y$addressfinal)
  y$addressfinal <- gsub("[']", "", y$addressfinal)
  y$addressfinal <- gsub(" ", "", y$addressfinal)
  y$addressf <- paste(y$postset,y$addressfinal,sep=",")
  
  taba1 <- inner_join(x,y,by="addressf")
  
  return(taba1)
  
  
}



link378<-function378(add,epc)
dim(link378)


link378<-link378[,..needlist11]



link378u<- uniqueresult(link378)
dim(link378u)


link378d <- doubleresult(link378)
dim(link378d)


dim(epc)
# 
epc <- matchleft(epc,link378)
dim(epc)
#2379509      37
#################### method 379 ##################
function379<- function(x,y){
  
  
  x$bnstreet <-    paste(x$subbuildingname,x$buildingnumber,sep=",")
  x$bnstreet <-    paste(x$bnstreet,x$streetdescription,sep=" ")
 
  x$bnstreet <- gsub("/", "", x$bnstreet)
  x$bnstreet <- gsub("[.]", "", x$bnstreet)
  x$bnstreet <- gsub("[']", "", x$bnstreet)
  x$bnstreet <- gsub(" ", "", x$bnstreet)
  x$addressf <-paste(x$postset,x$bnstreet,sep=",")
  
  
  y$addressfinal <- y$add
  
  y$addressfinal <- gsub( "FLAT","ROOM", y$addressfinal)
  y$addressfinal <- gsub("[/]", "", y$addressfinal)
  y$addressfinal <- gsub("[.]", "", y$addressfinal)
  y$addressfinal <- gsub("[']", "", y$addressfinal)
  y$addressfinal <- gsub(" ", "", y$addressfinal)
  y$addressf <- paste(y$postset,y$addressfinal,sep=",")
  
  taba1 <- inner_join(x,y,by="addressf")
  
  return(taba1)
}
link379<-function379(add,epc)

link379<-link379[,..needlist11]


link379u<- uniqueresult(link379)
dim(link379u)


link379d <- doubleresult(link379)
dim(link379d)

dim(epc)

epc <- matchleft(epc,link379)
dim(epc)

#################### method 380 ##################
function380<- function(x,y){
  
  x$bnstreet <-    paste(x$subbuildingname,x$buildingnumber,sep=",")
  x$bnstreet <-    paste(x$bnstreet,x$streetdescription,sep=" ")
  #bnstreet
  #x$bnstreet <-    paste(x$subbuildingname,x$streetdescription,sep=",")
  x$bnstreet <- gsub("/", "", x$bnstreet)
  x$bnstreet <- gsub("[.]", "", x$bnstreet)
  x$bnstreet <- gsub("[']", "", x$bnstreet)
  x$bnstreet <- gsub(" ", "", x$bnstreet)
  x$addressf <-paste(x$postset,x$bnstreet,sep=",")
  
  
  y$addressfinal <- y$add
  
  y$addressfinal <- gsub( "STUDIO","FLAT", y$addressfinal)
  y$addressfinal <- gsub("[/]", "", y$addressfinal)
  y$addressfinal <- gsub("[.]", "", y$addressfinal)
  y$addressfinal <- gsub("[']", "", y$addressfinal)
  y$addressfinal <- gsub(" ", "", y$addressfinal)
  y$addressf <- paste(y$postset,y$addressfinal,sep=",")
  
  taba1 <- inner_join(x,y,by="addressf")
  
  return(taba1)
  
}

link380<-function380(add,epc)

length(unique(link380$lmk_key))

link380<-link380[,..needlist11]

link380u<- uniqueresult(link380)

dim(link380u)


link380d <- doubleresult(link380)
dim(link380d)

dim(epc)
#
epc <- matchleft(epc,link380)
dim(epc)
#777396     28

#################### method 381 F##################

function381<- function(x,y){
  
  
  x$bnstreet <-    paste("FLAT ",x$subbuildingname,sep="")
  x$bnstreet <-    paste(x$bnstreet,x$paotext,sep=",")
  x$bnstreet <-    paste(x$bnstreet,x$buildingnumber,sep=",")
  
  x$bnstreet <-    paste(x$bnstreet,x$streetdescription,sep=" ")
  
  x$bnstreet <- gsub("/", "", x$bnstreet)
  x$bnstreet <- gsub("[.]", "", x$bnstreet)
  x$bnstreet <- gsub("[']", "", x$bnstreet)
  x$bnstreet <- gsub(" ", "", x$bnstreet)
  x$addressf <-paste(x$postset,x$bnstreet,sep=",")
  
  
  
  y<-y[y$property_type=="Flat"|y$property_type=="Maisonette",]
  y$addressfinal <- y$add
  
 
  y$addressfinal <- gsub("[/]", "", y$addressfinal)
  y$addressfinal <- gsub("[.]", "", y$addressfinal)
  y$addressfinal <- gsub("[']", "", y$addressfinal)
  y$addressfinal <- gsub(" ", "", y$addressfinal)
  y$addressf <- paste(y$postset,y$addressfinal,sep=",")
  
  taba1 <- inner_join(x,y,by="addressf")
  
  return(taba1)
  
}


link381<-function381(add,epc)


link381<-link381[,..needlist11]


link381u<- uniqueresult(link381)
dim(link381u)


link381d <- doubleresult(link381)
dim(link381d)

dim(epc)
#
epc <- matchleft(epc,link381)
dim(epc)
#2388669      38


#################### method 382  ##################


function382<- function(x,y){
  
  x$bnstreet <-    paste(x$saotext,x$buildingname,sep=",")
  x$bnstreet <-    paste(x$bnstreet,x$streetdescription,sep=" ")
  #bnstreet
  #x$bnstreet <-    paste(x$subbuildingname,x$streetdescription,sep=",")
  x$bnstreet <- gsub("/", "", x$bnstreet)
  x$bnstreet <- gsub("[.]", "", x$bnstreet)
  x$bnstreet <- gsub("[']", "", x$bnstreet)
  x$bnstreet <- gsub(" ", "", x$bnstreet)
  x$addressf <-paste(x$postset,x$bnstreet,sep=",")
  
  
  y$addressfinal <- y$add
  
  y$addressfinal <- gsub( "UNIT","FLAT", y$addressfinal)
  y$addressfinal <- gsub("[/]", "", y$addressfinal)
  y$addressfinal <- gsub("[.]", "", y$addressfinal)
  y$addressfinal <- gsub("[']", "", y$addressfinal)
  y$addressfinal <- gsub(" ", "", y$addressfinal)
  y$addressf <- paste(y$postset,y$addressfinal,sep=",")
  
  taba1 <- inner_join(x,y,by="addressf")
  
  return(taba1)
  
  
}

link382<-function382(add,epc)

link382<-link382[,..needlist11]


link382u<- uniqueresult(link382)
dim(link382u)


link382d <- doubleresult(link382)
dim(link382d)


dim(epc)
#
epc <- matchleft(epc,link382)
dim(epc)
# 2387648      38



#################### method 383 ##################

function383<- function(x,y){
  # x$bnstreet <-    paste(x$subbuildingname,x$buildingnumber,sep=",")
  # x$bnstreet <-    paste(x$bnstreet,x$streetdescription,sep=" ")
  # 
  # x$bnstreet <- gsub("/", "", x$bnstreet)
  # x$bnstreet <- gsub("[.]", "", x$bnstreet)
  # x$bnstreet <- gsub("[']", "", x$bnstreet)
  # x$bnstreet <- gsub(" ", "", x$bnstreet)
  # x$addressf <-paste(x$postset,x$bnstreet,sep=",")
  # 
  # 
  # y$addressfinal <- y$add
  # 
  # y$addressfinal <- gsub( "FLAT","ROOM", y$addressfinal)
  # y$addressfinal <- gsub("[/]", "", y$addressfinal)
  # y$addressfinal <- gsub("[.]", "", y$addressfinal)
  # y$addressfinal <- gsub("[']", "", y$addressfinal)
  # y$addressfinal <- gsub(" ", "", y$addressfinal)
  # y$addressf <- paste(y$postset,y$addressfinal,sep=",")
  # 
  # taba1 <- inner_join(x,y,by="addressf")
  # 
  # return(taba1)
  
  
  x$bnstreet <- paste(x$subbuildingname,x$paotext,sep=",")
  x$bnstreet <- paste(x$bnstreet ,x$buildingnumber,sep=",")
  x$bnstreet <- paste(x$bnstreet ,x$streetdescription,sep=" ")
  x$bnstreet <- gsub("[.]", "", x$bnstreet)
  x$bnstreet <- gsub("[/]", "", x$bnstreet)
  x$bnstreet <- gsub("[']", "", x$bnstreet)
  x$bnstreet <- gsub(" ", "", x$bnstreet)
  x$addressf <-paste(x$postset,x$bnstreet,sep=",")
  
  y<-y[y$property_type=="Flat"|y$property_type=="Maisonette",]
  #"FLAT 23"
  y$addressfinal <- y$add
  y$addressfinal <- gsub("STUDIO", "FLAT", y$addressfinal)
  #y$addressfinal <- gsub(",", "", y$addressfinal)
  y$addressfinal <- gsub("/", "", y$addressfinal)
  y$addressfinal <- gsub("[']", "", y$addressfinal)
  y$addressfinal <- gsub("[.]", "", y$addressfinal)
  y$addressfinal <- gsub(" ", "", y$addressfinal)
  y$addressf <- paste(y$postset,y$addressfinal,sep=",")
  
  taba1 <- inner_join(x,y,by="addressf")
  
  return(taba1)
  
}



link383<-function383(add,epc)
dim(link383)
# 
link383<-link383[,..needlist11]

link383u<- uniqueresult(link383)
dim(link383u)
#

link383d <- doubleresult(link383)
dim(link383d)


dim(epc)
#
epc <- matchleft(epc,link383)
dim(epc)
# 2387638      38


#################### method 384  ##################

function384<- function(x,y){
  
  
  x$bnstreet <-    paste(x$subbuildingname,x$buildingnumber,sep=",")
  x$bnstreet <-    paste(x$bnstreet,x$streetdescription,sep=" ")
 
  x$bnstreet <- gsub("/", "", x$bnstreet)
  x$bnstreet <- gsub("[.]", "", x$bnstreet)
  x$bnstreet <- gsub("[']", "", x$bnstreet)
  x$bnstreet <- gsub(" ", "", x$bnstreet)
  x$addressf <-paste(x$postset,x$bnstreet,sep=",")
  
  y[postcode=="E2 8AG",add:=gsub("KINGLAND ROAD", "KINGSLAND ROAD",add)]
  y$addressfinal <- y$add
  
  y$addressfinal <- gsub("[/]", "", y$addressfinal)
  y$addressfinal <- gsub("[.]", "", y$addressfinal)
  y$addressfinal <- gsub("[']", "", y$addressfinal)
  y$addressfinal <- gsub(" ", "", y$addressfinal)
  y$addressf <- paste(y$postset,y$addressfinal,sep=",")
  
  taba1 <- inner_join(x,y,by="addressf")
  
  return(taba1)
}


link384<-function384(add,epc)



link384<-link384[,..needlist11]


link384u<- uniqueresult(link384)
dim(link384u)


link384d <- doubleresult(link384)

dim(epc)
# 
epc<- matchleft(epc,link384)
dim(epc)
#   777220     28
#################### method 385 ##################


function385<- function(x,y){
  
  x$bnstreet <-    paste(x$subbuildingname,x$buildingnumber,sep=",")
  
  x$bnstreet <-    paste(x$bnstreet,x$streetdescription,sep=" ")
  
  x$bnstreet <- gsub("[.]", "", x$bnstreet)
  x$bnstreet <- gsub("[/]", "", x$bnstreet)
  x$bnstreet <- gsub("[']", "", x$bnstreet)
  x$bnstreet <- gsub(" ", "", x$bnstreet)
  x$addressf <-paste(x$postset,x$bnstreet,sep=",")
  
  
  y$addressfinal <-  trimws(y$add)
  y$addressfinal <- gsub("FLAT 01", "FLAT 1", y$addressfinal)
  y$addressfinal <- gsub("FLAT 02", "FLAT 2", y$addressfinal)
  y$addressfinal <- gsub("FLAT 03", "FLAT 3", y$addressfinal)
  y$addressfinal <- gsub("FLAT 04", "FLAT 4", y$addressfinal)
  y$addressfinal <- gsub("FLAT 05", "FLAT 5", y$addressfinal)
  y$addressfinal <- gsub("FLAT 06", "FLAT 6", y$addressfinal)
  y$addressfinal <- gsub("FLAT 07", "FLAT 7", y$addressfinal)
  y$addressfinal <- gsub("FLAT 08", "FLAT 8", y$addressfinal)
  y$addressfinal <- gsub("FLAT 09", "FLAT 9", y$addressfinal)
  
  y$addressfinal <- gsub("[.]", "", y$addressfinal)
  y$addressfinal <- gsub("[/]", "", y$addressfinal)
  
  y$addressfinal <- gsub("[']", "", y$addressfinal)
  y$addressfinal <- gsub(" ", "", y$addressfinal)
  y$addressf <- paste(y$postset,y$addressfinal,sep=",")
  
  taba1 <- inner_join(x,y,by="addressf")
  return(taba1)
  
}



link385<-function385(add,epc)


link385<-link385[,..needlist11]


link385u<- uniqueresult(link385)
dim(link385u)


link385d <- doubleresult(link385)
dim(link385d)


dim(epc)
#
epc <- matchleft(epc,link385)
dim(epc)
#2380384      38

#
#################### method 386 ##################

function386<- function(x,y){
  
  x$bnstreet <-    paste(x$saotext,x$pp,sep=",")
  
  x$bnstreet <-    paste(x$bnstreet,x$streetdescription,sep=" ")
  
  x$bnstreet <- gsub("[.]", "", x$bnstreet)
  x$bnstreet <- gsub("[/]", "", x$bnstreet)
  x$bnstreet <- gsub("[']", "", x$bnstreet)
  x$bnstreet <- gsub(" ", "", x$bnstreet)
  x$addressf <-paste(x$postset,x$bnstreet,sep=",")
  #"FLAT 15"
  
  y$addressfinal <-  trimws(y$add)
  y$addressfinal <- gsub("APARTMENT", "FLAT", y$addressfinal)
  
  
  y$addressfinal <- gsub("[.]", "", y$addressfinal)
  y$addressfinal <- gsub("[/]", "", y$addressfinal)
  
  y$addressfinal <- gsub("[']", "", y$addressfinal)
  y$addressfinal <- gsub(" ", "", y$addressfinal)
  y$addressf <- paste(y$postset,y$addressfinal,sep=",")
  
  taba1 <- inner_join(x,y,by="addressf")
  return(taba1)
}




link386<-function386(add,epc)
dim(link386)
# 

link386<-link386[,..needlist11]



link386u<- uniqueresult(link386)
dim(link386u)
# 

link386d <- doubleresult(link386)
dim(link386d)


epc <- matchleft(epc,link386)
dim(epc)
# 2380381      38

#################### method 387 ##################



function387<- function(x,y){
  x$bnstreet <-    paste(x$ss,x$paotext,sep=",")
  
  x$bnstreet <-    paste(x$bnstreet,x$streetdescription,sep=",")
  
  x$bnstreet <- gsub("[.]", "", x$bnstreet)
  x$bnstreet <- gsub("[/]", "", x$bnstreet)
  x$bnstreet <- gsub("[']", "", x$bnstreet)
  x$bnstreet <- gsub(" ", "", x$bnstreet)
  x$addressf <-paste(x$postset,x$bnstreet,sep=",")
  
  
  y$addressfinal <-  trimws(y$add)
  y$addressfinal <- gsub("[.]", "", y$addressfinal)
  y$addressfinal <- gsub("[/]", "", y$addressfinal)
  
  y$addressfinal <- gsub("[']", "", y$addressfinal)
  y$addressfinal <- gsub(" ", "", y$addressfinal)
  y$addressf <- paste(y$postset,y$addressfinal,sep=",")
  
  taba1 <- inner_join(x,y,by="addressf")
  return(taba1)
}

link387<-function387(add,epc)
dim(link387)


link387<-link387[,..needlist11]

link387u<- uniqueresult(link387)
dim(link387u)


link387d <- doubleresult(link387)
dim(link387d)


dim(epc)
# 
epc <- matchleft(epc,link387)
dim(epc)
#2380190      38
#
#################### method 388 ##################

function388<- function(x,y){
  
  x$bnstreet <- x$buildingname
  
  #x$bnstreet <- paste(x$bnstreet,x$streetdescription,sep=",")
  
  x$bnstreet <- gsub(",", "", x$bnstreet)
  x$bnstreet <- gsub("/", "", x$bnstreet)
  x$bnstreet <- gsub("[.]", "", x$bnstreet)
  x$bnstreet <- gsub("[']", "", x$bnstreet)
  x$bnstreet <- gsub(" ", "", x$bnstreet)
  x$addressf <-paste(x$postset,x$bnstreet,sep=",")
  
  y<-y[y$add!="",]
  y$addressfinal <- y$add
  y$addressfinal <- gsub("[/]", "", y$addressfinal)
  y$addressfinal <- gsub("[.]", "", y$addressfinal)
  y$addressfinal <- gsub("[']", "", y$addressfinal)
  y$addressfinal <- gsub(",", "", y$addressfinal)
  y$addressfinal <- gsub(" ", "", y$addressfinal)
  y$addressf <- paste(y$postset,y$addressfinal,sep=",")
  
  taba1 <- inner_join(x,y,by="addressf")
  
  return(taba1)
  
}



link388<-function388(add,epc)
dim(link388)


link388<-link388[,..needlist1]



link388u<- uniqueresult(link388)
dim(link388u)


link388d <- doubleresult(link388)
dim(link388d)


dim(epc)
# 
epc <- matchleft(epc,link388)
dim(epc)
#2389509      38
#################### method 389 ##################
function389<- function(x,y){
  
  x$bnstreet <-    paste(x$subbuildingname,x$buildingnumber,sep=",")

  x$bnstreet <-    paste(x$bnstreet,x$streetdescription,sep=" ")

  x$bnstreet <- gsub("/", "", x$bnstreet)
  x$bnstreet <- gsub("[.]", "", x$bnstreet)
  x$bnstreet <- gsub("[']", "", x$bnstreet)
  x$bnstreet <- gsub(" ", "", x$bnstreet)
  x$addressf <-paste(x$postset,x$bnstreet,sep=",")
  
  
  
  y<-y[y$property_type=="Flat"|y$property_type=="Maisonette",]
  

  y$addressfinal <- word(y$add1,1)
  y$addressfinal <- gsub(",", "", y$addressfinal)
  y$addressfinal <- paste("FLAT", y$addressfinal,sep="")
  y$addressfinal <- paste(y$addressfinal,y$add2,sep=",")
  
  #y$addressfinal <- gsub( "STUDIO","FLAT", y$addressfinal)
  y$addressfinal <- gsub("[/]", "", y$addressfinal)
  y$addressfinal <- gsub("[.]", "", y$addressfinal)
  y$addressfinal <- gsub("[']", "", y$addressfinal)
  y$addressfinal <- gsub(" ", "", y$addressfinal)
  y$addressf <- paste(y$postset,y$addressfinal,sep=",")
  
  taba1 <- inner_join(x,y,by="addressf")
  
  return(taba1)
  
  
}
link389<-function389(add,epc)

link389<-link389[,..needlist1]


link389u<- uniqueresult(link389)
dim(link389u)


link389d <- doubleresult(link389)
dim(link389d)

dim(epc)

epc <- matchleft(epc,link389)
dim(epc)
# 776564     28

#################### method 390 ##################
function390<- function(x,y){
  
  x$bnstreet <-    paste(x$saotext,x$pp,sep=",")
  
  x$bnstreet <-    paste(x$bnstreet,x$streetdescription,sep=" ")
  
  x$bnstreet <- gsub("[.]", "", x$bnstreet)
  x$bnstreet <- gsub("[/]", "", x$bnstreet)
  x$bnstreet <- gsub("[']", "", x$bnstreet)
  x$bnstreet <- gsub(" ", "", x$bnstreet)
  x$addressf <-paste(x$postset,x$bnstreet,sep=",")
  #"FLAT 15"
  
  y$addressfinal <-  paste(y$add1,y$add3,sep=",")
  y$addressfinal <- gsub("APARTMENT", "FLAT", y$addressfinal)
  
  
  y$addressfinal <- gsub("[.]", "", y$addressfinal)
  y$addressfinal <- gsub("[/]", "", y$addressfinal)
  
  y$addressfinal <- gsub("[']", "", y$addressfinal)
  y$addressfinal <- gsub(" ", "", y$addressfinal)
  y$addressf <- paste(y$postset,y$addressfinal,sep=",")
  
  taba1 <- inner_join(x,y,by="addressf")
  return(taba1)
  
}

link390<-function390(add,epc)

length(unique(link390$lmk_key))

link390<-link390[,..needlist11]

link390u<- uniqueresult(link390)

dim(link390u)


link390d <- doubleresult(link390)
dim(link390d)

dim(epc)
#
epc <- matchleft(epc,link390)
dim(epc)
#777396     28

#################### method 391 ##################

function391<- function(x,y){
  
  x$bnstreet <-    paste(x$subbuildingname,x$paostartnumber,sep=",")
  x$bnstreet <-    paste(x$bnstreet,x$paoendnumber,sep="-")
  
  x$bnstreet <-    paste(x$bnstreet,x$streetdescription,sep=" ")
  
  x$bnstreet <- gsub("[.]", "", x$bnstreet)
  x$bnstreet <- gsub("[/]", "", x$bnstreet)
  x$bnstreet <- gsub("[']", "", x$bnstreet)
  x$bnstreet <- gsub(" ", "", x$bnstreet)
  x$addressf <-paste(x$postset,x$bnstreet,sep=",")
  #"FLAT 15"
  
  y$addressfinal <-  paste(y$add1,y$add3,sep=",")
 
  y$addressfinal <- gsub("[.]", "", y$addressfinal)
  y$addressfinal <- gsub("[/]", "", y$addressfinal)
  
  y$addressfinal <- gsub("[']", "", y$addressfinal)
  y$addressfinal <- gsub(" ", "", y$addressfinal)
  y$addressf <- paste(y$postset,y$addressfinal,sep=",")
  
  taba1 <- inner_join(x,y,by="addressf")
  return(taba1)
  
}


link391<-function391(add,epc)


link391<-link391[,..needlist11]


link391u<- uniqueresult(link391)
dim(link391u)


link391d <- doubleresult(link391)
dim(link391d)

dim(epc)
#
epc <- matchleft(epc,link391)
dim(epc)
#2398669      39


#################### method 392 ##################


function392<- function(x,y){
  
  x$bnstreet <-    paste(x$saotext,x$paotext,sep=" ")
  x$bnstreet <-    paste(x$bnstreet,x$pp,sep=",")
  
  x$bnstreet <-    paste(x$bnstreet,x$streetdescription,sep=" ")
  
  x$bnstreet <- gsub("[.]", "", x$bnstreet)
  x$bnstreet <- gsub("[/]", "", x$bnstreet)
  x$bnstreet <- gsub("[']", "", x$bnstreet)
  x$bnstreet <- gsub(" ", "", x$bnstreet)
  x$addressf <-paste(x$postset,x$bnstreet,sep=",")

  
  y<-y[y$property_type=="Flat"|y$property_type=="Maisonette",]
 
  y$addressfinal <- paste("APARTMENT ", y$add, sep="")
  
  
  y$addressfinal <- gsub("[.]", "", y$addressfinal)
  y$addressfinal <- gsub("[/]", "", y$addressfinal)
  
  y$addressfinal <- gsub("[']", "", y$addressfinal)
  y$addressfinal <- gsub(" ", "", y$addressfinal)
  y$addressf <- paste(y$postset,y$addressfinal,sep=",")
  
  taba1 <- inner_join(x,y,by="addressf")
  return(taba1)
  
}

link392<-function392(add,epc)

link392<-link392[,..needlist11]


link392u<- uniqueresult(link392)
dim(link392u)


link392d <- doubleresult(link392)
dim(link392d)


dim(epc)
#
epc <- matchleft(epc,link392)
dim(epc)
# 2397648      39



#################### method 393 ##################

function393<- function(x,y){
  
  x$bnstreet <-    paste(x$saotext,x$paotext,sep=",")

  x$bnstreet <-    paste(x$bnstreet,x$pp,sep=",")
  x$bnstreet <-    paste(x$bnstreet,x$streetdescription,sep=" ")
  x$bnstreet <- gsub("/", "", x$bnstreet)
  x$bnstreet <- gsub("[.]", "", x$bnstreet)
  x$bnstreet <- gsub("[']", "", x$bnstreet)
  x$bnstreet <- gsub(" ", "", x$bnstreet)
  x$addressf <-paste(x$postset,x$bnstreet,sep=",")
  
  
  
  y$addressfinal <- trimws(y$add)
  y[postset=="B25 8",addressfinal:=gsub("EQUIPPOINT", "EQUIPOINT",addressfinal)]
  
  y$addressfinal <- gsub("[/]", "", y$addressfinal)
  y$addressfinal <- gsub("[.]", "", y$addressfinal)
  y$addressfinal <- gsub("[']", "", y$addressfinal)
  y$addressfinal <- gsub(" ", "", y$addressfinal)
  y$addressf <- paste(y$postset,y$addressfinal,sep=",")
  
  taba1 <- inner_join(x,y,by="addressf")
  return(taba1)
}



link393<-function393(add,epc)
dim(link393)
# 
link393<-link393[,..needlist11]

link393u<- uniqueresult(link393)
dim(link393u)
#

link393d <- doubleresult(link393)
dim(link393d)


dim(epc)
#
epc <- matchleft(epc,link393)
dim(epc)
# 2397639      39


#################### method 394  ##################

function394<- function(x,y){
  
  x$bnstreet <-    paste(x$subbuildingname,x$buildingname,sep=" ")

  x$bnstreet <-    paste(x$bnstreet,x$buildingnumber,sep=",")
  x$bnstreet <-    paste(x$bnstreet,x$streetdescription,sep=",")
  x$bnstreet <- gsub("/", "", x$bnstreet)
  x$bnstreet <- gsub("[.]", "", x$bnstreet)
  x$bnstreet <- gsub("[']", "", x$bnstreet)
  x$bnstreet <- gsub(" ", "", x$bnstreet)
  x$addressf <-paste(x$postset,x$bnstreet,sep=",")
  
  
  
  y$addressfinal <- trimws(y$add)
  y[postset=="S2 2",addressfinal:=gsub("NORFOLK PARK VILLAGE", "NORFOLK PARK STUDENT RESIDENCE",addressfinal)]
  
  y$addressfinal <- gsub("[/]", "", y$addressfinal)
  y$addressfinal <- gsub("[.]", "", y$addressfinal)
  y$addressfinal <- gsub("[']", "", y$addressfinal)
  y$addressfinal <- gsub(" ", "", y$addressfinal)
  y$addressf <- paste(y$postset,y$addressfinal,sep=",")
  
  
  taba1 <- inner_join(x,y,by="addressf")
  return(taba1)
}


link394<-function394(add,epc)



link394<-link394[,..needlist11]


link394u<- uniqueresult(link394)
dim(link394u)


link394d <- doubleresult(link394)

dim(epc)
# 
epc<- matchleft(epc,link394)
dim(epc)
#   777220     28
#################### method 395 ##################


function395<- function(x,y){
  
  x$bnstreet <-    paste(x$subbuildingname,x$paotext,sep=",")
  
  x$bnstreet <-    paste(x$bnstreet,x$pp,sep=",")
  x$bnstreet <-    paste(x$bnstreet,x$streetdescription,sep=",")
  x$bnstreet <- gsub("/", "", x$bnstreet)
  x$bnstreet <- gsub("[.]", "", x$bnstreet)
  x$bnstreet <- gsub("[']", "", x$bnstreet)
  x$bnstreet <- gsub(" ", "", x$bnstreet)
  x$addressf <-paste(x$postset,x$bnstreet,sep=",")
  
  
  
  y$addressfinal <- trimws(y$add)
  
  
  y$addressfinal <- gsub("[/]", "", y$addressfinal)
  y$addressfinal <- gsub("[.]", "", y$addressfinal)
  y$addressfinal <- gsub("[']", "", y$addressfinal)
  y$addressfinal <- gsub(" ", "", y$addressfinal)
  y$addressf <- paste(y$postset,y$addressfinal,sep=",")
  
  
  taba1 <- inner_join(x,y,by="addressf")
  return(taba1)
  
  
}



link395<-function395(add,epc)


link395<-link395[,..needlist11]


link395u<- uniqueresult(link395)
dim(link395u)


link395d <- doubleresult(link395)
dim(link395d)


dim(epc)
#
epc <- matchleft(epc,link395)
dim(epc)
#2390394      39

#
#################### method 396 ##################

function396<- function(x,y){
  
  
  x$bnstreet <-    paste(x$subbuildingname,x$paotext,sep=",")
  
  x$bnstreet <-    paste(x$bnstreet,x$pp,sep=",")
  x$bnstreet <-    paste(x$bnstreet,x$streetdescription,sep=" ")
  x$bnstreet <- gsub("/", "", x$bnstreet)
  x$bnstreet <- gsub("[.]", "", x$bnstreet)
  x$bnstreet <- gsub("[']", "", x$bnstreet)
  x$bnstreet <- gsub(" ", "", x$bnstreet)
  x$addressf <-paste(x$postset,x$bnstreet,sep=",")
  
  
  
  y$addressfinal <- trimws(y$add)
  
  
  y$addressfinal <- gsub("[/]", "", y$addressfinal)
  y$addressfinal <- gsub("[.]", "", y$addressfinal)
  y$addressfinal <- gsub("[']", "", y$addressfinal)
  y$addressfinal <- gsub(" ", "", y$addressfinal)
  y$addressf <- paste(y$postset,y$addressfinal,sep=",")
  
  
  taba1 <- inner_join(x,y,by="addressf")
  return(taba1)
}




link396<-function396(add,epc)
dim(link396)
# 

link396<-link396[,..needlist11]



link396u<- uniqueresult(link396)
dim(link396u)
# 

link396d <- doubleresult(link396)
dim(link396d)


epc <- matchleft(epc,link396)
dim(epc)
# 2390391      39

#################### method 397 ##################



function397<- function(x,y){
  x<-x[x$subbuildingname=="",]
  x$bnstreet <-    paste("FLAT",x$buildingname,sep=" ")
  
  #x$bnstreet <-    paste(x$bnstreet,x$pp,sep=",")
  x$bnstreet <-    paste(x$bnstreet,x$streetdescription,sep=",")
  x$bnstreet <- gsub("/", "", x$bnstreet)
  x$bnstreet <- gsub("[.]", "", x$bnstreet)
  x$bnstreet <- gsub("[']", "", x$bnstreet)
  x$bnstreet <- gsub(" ", "", x$bnstreet)
  x$addressf <-paste(x$postset,x$bnstreet,sep=",")
  
  
  #correct 
  y<-y[y$property_type=="Flat"|y$property_type=="Maisonette",]
  y$addressfinal <- trimws(y$add)
  
  y$addressfinal <- gsub(",GATE 3, HORSHAM GATES", "HORSHAM GATES THREE", y$addressfinal)
  y$addressfinal <- gsub(",GATE 2, HORSHAM GATES", "HORSHAM GATES TWO", y$addressfinal)
  
  y$addressfinal <- gsub("[/]", "", y$addressfinal)
  y$addressfinal <- gsub("[.]", "", y$addressfinal)
  y$addressfinal <- gsub("[']", "", y$addressfinal)
  y$addressfinal <- gsub(" ", "", y$addressfinal)
  y$addressf <- paste(y$postset,y$addressfinal,sep=",")
  
  
  taba1 <- inner_join(x,y,by="addressf")
  return(taba1)
  
}

link397<-function397(add,epc)
dim(link397)


link397<-link397[,..needlist11]

link397u<- uniqueresult(link397)
dim(link397u)


link397d <- doubleresult(link397)
dim(link397d)


dim(epc)
# 
epc <- matchleft(epc,link397)
dim(epc)
#2390190      39
#
#################### method 398 ##################

function398<- function(x,y){
  
  x$bnstreet <-    paste("FLAT",x$subbuildingname,sep=" ")
  x$bnstreet <-    paste(x$bnstreet,x$paostartnumber,sep=",")
  x$bnstreet <-    paste(x$bnstreet,x$paoendnumber,sep="-")
  x$bnstreet <-    paste(x$bnstreet,x$streetdescription,sep=" ")
  
  x$bnstreet <- gsub("[.]", "", x$bnstreet)
  x$bnstreet <- gsub("[/]", "", x$bnstreet)
  x$bnstreet <- gsub("[']", "", x$bnstreet)
  x$bnstreet <- gsub(" ", "", x$bnstreet)
  x$addressf <-paste(x$postset,x$bnstreet,sep=",")
  #"FLAT 15"
  
  y<-y[y$property_type=="Flat"|y$property_type=="Maisonette",]
  y$addressfinal <-  y$add
  
  
  y$addressfinal <- gsub("[.]", "", y$addressfinal)
  y$addressfinal <- gsub("[/]", "", y$addressfinal)
  
  y$addressfinal <- gsub("[']", "", y$addressfinal)
  y$addressfinal <- gsub(" ", "", y$addressfinal)
  y$addressf <- paste(y$postset,y$addressfinal,sep=",")
  
  taba1 <- inner_join(x,y,by="addressf")
  return(taba1)
  
  
}



link398<-function398(add,epc)
dim(link398)


link398<-link398[,..needlist11]



link398u<- uniqueresult(link398)
dim(link398u)


link398d <- doubleresult(link398)
dim(link398d)


dim(epc)
# 
epc <- matchleft(epc,link398)
dim(epc)
#2399509      39
#################### method 399 ##################
function399<- function(x,y){
  
  x$bnstreet <-    paste("FLAT",x$buildingname,sep=" ")
  x$bnstreet <-    paste(x$bnstreet,x$streetdescription,sep=",")
  x$bnstreet <- gsub("/", "", x$bnstreet)
  x$bnstreet <- gsub("[.]", "", x$bnstreet)
  x$bnstreet <- gsub("[']", "", x$bnstreet)
  x$bnstreet <- gsub(",", "", x$bnstreet)
  x$bnstreet <- gsub(" ", "", x$bnstreet)
  x$addressf <-paste(x$postset,x$bnstreet,sep=",")
  
  
  
  
  y<-y[y$property_type=="Flat"|y$property_type=="Maisonette",]
  
  y$addressfinal <- y$add
  y$addressfinal <- gsub("[/]", "", y$addressfinal)
  y$addressfinal <- gsub("[.]", "", y$addressfinal)
  y$addressfinal <- gsub("[']", "", y$addressfinal)
  y$addressfinal <- gsub(",", "", y$addressfinal)

  y$addressfinal <- gsub(" ", "", y$addressfinal)
  y$addressf <- paste(y$postset,y$addressfinal,sep=",")
  
  taba1 <- inner_join(x,y,by="addressf")
  return(taba1)
  
}
link399<-function399(add,epc)

link399<-link399[,..needlist11]


link399u<- uniqueresult(link399)
dim(link399u)


link399d <- doubleresult(link399)
dim(link399d)

dim(epc)

epc <- matchleft(epc,link399)
dim(epc)
# 776564     28
#################### method 400 ##################
function400<- function(x,y){
  
  
  x$bnstreet <-    paste(x$saotext,x$pp,sep=",")
  x$bnstreet <-    paste(x$bnstreet,x$streetdescription,sep=" ")
  x$bnstreet <- gsub("[.]", "", x$bnstreet)
  x$bnstreet <- gsub("[']", "", x$bnstreet)
  x$bnstreet <- gsub("[/]", "", x$bnstreet)
  x$bnstreet <- gsub(" ", "", x$bnstreet)
  x$addressf <-paste(x$postset,x$bnstreet,sep=",")
  
  
  #y<-y[y$property_type=="Flat",]
  
  #y<-y[y$property_type=="Flat"|y$property_type=="Maisonette",]
  
  y$addressfinal <- y$add
  y$addressfinal <- gsub("APARTMENT 01", "APARTMENT 1", y$addressfinal)
  y$addressfinal <- gsub("APARTMENT 02", "APARTMENT 2", y$addressfinal)
  y$addressfinal <- gsub("APARTMENT 03", "APARTMENT 3", y$addressfinal)
  y$addressfinal <- gsub("APARTMENT 04", "APARTMENT 4", y$addressfinal)
  y$addressfinal <- gsub("APARTMENT 05", "APARTMENT 5", y$addressfinal)
  y$addressfinal <- gsub("APARTMENT 06", "APARTMENT 6", y$addressfinal)
  y$addressfinal <- gsub("APARTMENT 07", "APARTMENT 7", y$addressfinal)
  y$addressfinal <- gsub("APARTMENT 08", "APARTMENT 8", y$addressfinal)
  y$addressfinal <- gsub("APARTMENT 09", "APARTMENT 9", y$addressfinal)
  y$addressfinal <- gsub("[/]", "", y$addressfinal)
  
  y$addressfinal <- gsub("[']", "", y$addressfinal)
  
  y$addressfinal <- gsub("[.]", "", y$addressfinal)
  
  y$addressfinal <- gsub(" ", "", y$addressfinal)
  y$addressf <- paste(y$postset,y$addressfinal,sep=",")
  
  taba1 <- inner_join(x,y,by="addressf")
  return(taba1)
  
}

link400<-function400(add,epc)

length(unique(link400$lmk_key))

link400<-link400[,..needlist11]

link400u<- uniqueresult(link400)

dim(link400u)


link400d <- doubleresult(link400)
dim(link400d)

dim(epc)
#
epc <- matchleft(epc,link400)
dim(epc)
#729494     28
#################### summ up ##################




link360u$method<-"link360u"
link361u$method<-"link361u"
link362u$method<-"link362u"
link363u$method<-"link363u"
link364u$method<-"link364u"
link365u$method<-"link365u"
link366u$method<-"link366u"
link367u$method<-"link367u"
link368u$method<-"link368u"
link369u$method<-"link369u"
link370u$method<-"link370u"
link371u$method<-"link371u"
link372u$method<-"link372u"
link373u$method<-"link373u"
link374u$method<-"link374u"
link375u$method<-"link375u"
link376u$method<-"link376u"
link377u$method<-"link377u"
link378u$method<-"link378u"
link379u$method<-"link379u"
link380u$method<-"link380u"
link381u$method<-"link381u"
link382u$method<-"link382u"
link383u$method<-"link383u"
link384u$method<-"link384u"
link385u$method<-"link385u"
link386u$method<-"link386u"
link387u$method<-"link387u"
link388u$method<-"link388u"
link389u$method<-"link389u"

link390u$method<-"link390u"
link391u$method<-"link391u"
link392u$method<-"link392u"
link393u$method<-"link393u"
link394u$method<-"link394u"
link395u$method<-"link395u"
link396u$method<-"link396u"
link397u$method<-"link397u"
link398u$method<-"link398u"
link399u$method<-"link399u"


link400u$method<-"link400u"



link360d$method<-"link360d"
link361d$method<-"link361d"
link362d$method<-"link362d"
link363d$method<-"link363d"
link364d$method<-"link364d"
link365d$method<-"link365d"
link366d$method<-"link366d"
link367d$method<-"link367d"
link368d$method<-"link368d"
link369d$method<-"link369d"
link370d$method<-"link370d"
link371d$method<-"link371d"
link372d$method<-"link372d"
link373d$method<-"link373d"
link374d$method<-"link374d"
link375d$method<-"link375d"
link376d$method<-"link376d"
link377d$method<-"link377d"
link378d$method<-"link378d"
link379d$method<-"link379d"
link380d$method<-"link380d"
link381d$method<-"link381d"
link382d$method<-"link382d"
link383d$method<-"link383d"
link384d$method<-"link384d"
link385d$method<-"link385d"
link386d$method<-"link386d"
link387d$method<-"link387d"
link388d$method<-"link388d"
link389d$method<-"link389d"
link390d$method<-"link390d"
link391d$method<-"link391d"
link392d$method<-"link392d"
link393d$method<-"link393d"
link394d$method<-"link394d"
link395d$method<-"link395d"
link396d$method<-"link396d"
link397d$method<-"link397d"
link398d$method<-"link398d"
link399d$method<-"link399d"
link400d$method<-"link400d"





dim(link360u)[1]+dim(link361u)[1]+dim(link362u)[1]+dim(link363u)[1]+dim(link364u)[1]+dim(link365u)[1]+dim(link366u)[1]+dim(link367u)[1]+dim(link368u)[1]+dim(link369u)[1]+
  dim(link370u)[1]+dim(link371u)[1]+dim(link372u)[1]+dim(link373u)[1]+dim(link374u)[1]+dim(link375u)[1]+dim(link376u)[1]+dim(link377u)[1]+dim(link378u)[1]+dim(link379u)[1]+
  dim(link380u)[1]+dim(link381u)[1]+dim(link382u)[1]+dim(link383u)[1]+dim(link384u)[1]+dim(link385u)[1]+dim(link386u)[1]+dim(link387u)[1]+dim(link388u)[1]+dim(link389u)[1]+
  dim(link390u)[1]+dim(link391u)[1]+dim(link392u)[1]+dim(link393u)[1]+dim(link394u)[1]+dim(link395u)[1]+dim(link396u)[1]+dim(link397u)[1]+dim(link398u)[1]+dim(link399u)[1]+
  dim(link400u)[1]




l360_400u = list(link360u,link361u,link362u,link363u,link364u,link365u,link366u,link367u,link368u,link369u,
                 link370u,link371u,link372u,link373u,link374u,link375u,link376u,link377u,link378u,link379u,
                 link380u,link381u,link382u,link383u,link384u,link385u,link386u,link387u,link388u,link389u,
                 link390u,link391u,link392u,link393u,link394u,link395u,link396u,link397u,link398u,link399u,
                 link400u)

# 31059  



link360_400u <- rbindlist(l360_400u,use.names=TRUE,fill=T)
dim(link360_400u )
# 31056  
#31059
length(unique(link360_400u$lmk_key))
length(unique(link360_400u$method))
#
dim(link360d)[1]+dim(link361d)[1]+dim(link362d)[1]+dim(link363d)[1]+dim(link364d)[1]+dim(link365d)[1]+dim(link366d)[1]+dim(link367d)[1]+dim(link368d)[1]+dim(link369d)[1]+
  dim(link370d)[1]+dim(link371d)[1]+dim(link372d)[1]+dim(link373d)[1]+dim(link374d)[1]+dim(link375d)[1]+dim(link376d)[1]+dim(link377d)[1]+dim(link378d)[1]+dim(link379d)[1]+
  dim(link380d)[1]+dim(link381d)[1]+dim(link382d)[1]+dim(link383d)[1]+dim(link384d)[1]+dim(link385d)[1]+dim(link386d)[1]+dim(link387d)[1]+dim(link388d)[1]+dim(link389d)[1]+
  dim(link390d)[1]+dim(link391d)[1]+dim(link392d)[1]+dim(link393d)[1]+dim(link394d)[1]+dim(link395d)[1]+dim(link396d)[1]+dim(link397d)[1]+dim(link398d)[1]+dim(link399d)[1]+
  dim(link400d)[1]

l360_400d = list(link360d,link361d,link362d,link363d,link364d,link365d,link366d,link367d,link368d,link369d,
                 link370d,link371d,link372d,link373d,link374d,link375d,link376d,link377d,link378d,link379d,
                 link380d,link381d,link382d,link383d,link384d,link385d,link386d,link387d,link388d,link389d,
                 link390d,link391d,link392d,link393d,link394d,link395d,link396d,link397d,link398d,link399d,
                 link400d)


#3952   34
#3956
link360_400d <- rbindlist(l360_400d ,use.names=TRUE,fill=T)
dim(link360_400d )

#3952   34
#3956
dbWriteTable(con, "link360_400dnew1",value =link360_400d, append =  TRUE, row.names = FALSE)
dbWriteTable(con, "link360_400unew1",value =link360_400u, append =  TRUE, row.names = FALSE)


rm(l360_400d,l360_400u)

rm(link360u,link361u,link362u,link363u,link364u,link365u,link366u,link367u,link368u,link369u,
   link370u,link371u,link372u,link373u,link374u,link375u,link376u,link377u,link378u,link379u,
   link380u,link381u,link382u,link383u,link384u,link385u,link386u,link387u,link388u,link389u,
   link390u,link391u,link392u,link393u,link394u,link395u,link396u,link397u,link398u,link399u,
   link400u)





rm(link360d,link361d,link362d,link363d,link364d,link365d,link366d,link367d,link368d,link369d,
   link370d,link371d,link372d,link373d,link374d,link375d,link376d,link377d,link378d,link379d,
   link380d,link381d,link382d,link383d,link384d,link385d,link386d,link387d,link388d,link389d,
   link390d,link391d,link392d,link393d,link394d,link395d,link396d,link397d,link398d,link399d,
   link400d)



rm(function360,function361,function362,function363,function364,function365,function366,function367,function368,function369,
   function370,function371,function372,function373,function374,function375,function376,function377,function378,function379,
   function380,function381,function382,function383,function384,function385,function386,function387,function388,function389,
   function390,function391,function392,function393,function394,function395,function396,function397,function398,function399,
   function400)


rm(link360,link361,link362,link363,link364,link365,link366,link367,link368,link369,
   link370,link371,link372,link373,link374,link375,link376,link377,link378,link379,
   link380,link381,link382,link383,link384,link385,link386,link387,link388,link389,
   link390,link391,link392,link393,link394,link395,link396,link397,link398,link399,
   link400)
epc1<-matchleft(epc1,link360_400u)
epc1<-matchleft(epc1,link360_400d)
#
dim(epc1)
#729494     27
#731101     27
#################### method 401 ##################

function401<- function(x,y){
  
  x$bnstreet <-    paste(x$buildingname,x$subbuildingname,sep=",")
  #x$bnstreet <-    paste(x$bnstreet,x$paostartnumber,sep="-")
  x$bnstreet <-    paste(x$bnstreet,x$pp,sep=",")
  x$bnstreet <-    paste(x$bnstreet,x$streetdescription,sep=" ")
  x$bnstreet <- gsub("/", "", x$bnstreet)
  x$bnstreet <- gsub("[.]", "", x$bnstreet)
  x$bnstreet <- gsub("[']", "", x$bnstreet)
  x$bnstreet <- gsub(" ", "", x$bnstreet)
  x$addressf <-paste(x$postset,x$bnstreet,sep=",")
  
  
  y$addressfinal <- y$add
  y$addressfinal <- gsub("STUDIOS 51", "STUDIO 51", y$addressfinal)
  y$addressfinal <- gsub("APARTMENT", "FLAT", y$addressfinal)
  # STUDIOS 51
  y$addressfinal <- gsub("[/]", "", y$addressfinal)
  y$addressfinal <- gsub("[.]", "", y$addressfinal)
  y$addressfinal <- gsub("[']", "", y$addressfinal)
  y$addressfinal <- gsub(" ", "", y$addressfinal)
  y$addressf <- paste(y$postset,y$addressfinal,sep=",")
  
  taba1 <- inner_join(x,y,by="addressf")
  
  return(taba1) 
  
}


link401<-function401(add,epc)


link401<-link401[,..needlist11]


link401u<- uniqueresult(link401)
dim(link401u)


link401d <- doubleresult(link401)
dim(link401d)

dim(epc)
#
epc <- matchleft(epc,link401)
dim(epc)
#2408669      40


#################### method 402 ##################


function402<- function(x,y){
  x$bnstreet <-    paste(x$subbuildingname,x$buildingname,sep=" ")
  
  #x$bnstreet <-    paste(x$bnstreet,x$pp,sep=",")
  x$bnstreet <-    paste(x$bnstreet,x$streetdescription,sep=",")
  x$bnstreet <- gsub("/", "", x$bnstreet)
  x$bnstreet <- gsub("[.]", "", x$bnstreet)
  x$bnstreet <- gsub("[']", "", x$bnstreet)
  x$bnstreet <- gsub(" ", "", x$bnstreet)
  x$addressf <-paste(x$postset,x$bnstreet,sep=",")
  
  
  
  y$addressfinal <- trimws(y$add)
  
  y$addressfinal <- gsub("FRESHFIELD", "FRESHFIELDS", y$addressfinal)
  y$addressfinal <- gsub("[/]", "", y$addressfinal)
  y$addressfinal <- gsub("[.]", "", y$addressfinal)
  y$addressfinal <- gsub("[']", "", y$addressfinal)
  y$addressfinal <- gsub(" ", "", y$addressfinal)
  y$addressf <- paste(y$postset,y$addressfinal,sep=",")
  
  
  taba1 <- inner_join(x,y,by="addressf")
  return(taba1)
  
  
}

link402<-function402(add,epc)

link402<-link402[,..needlist11]


link402u<- uniqueresult(link402)
dim(link402u)


link402d <- doubleresult(link402)
dim(link402d)


dim(epc)
#
epc <- matchleft(epc,link402)
dim(epc)
# 2407648      40



#################### method 403 ##################

function403<- function(x,y){
  
  x$bnstreet <-    paste(x$subbuildingname,x$paostartnumber,sep=",")
  x$bnstreet <-    paste(x$bnstreet,x$paoendnumber,sep="-")
  #x$bnstreet <-    paste(x$bnstreet,x$pp,sep=",")
  x$bnstreet <-    paste(x$bnstreet,x$streetdescription,sep=",")
  x$bnstreet <- gsub("/", "", x$bnstreet)
  x$bnstreet <- gsub("[.]", "", x$bnstreet)
  x$bnstreet <- gsub("[']", "", x$bnstreet)
  x$bnstreet <- gsub(" ", "", x$bnstreet)
  x$addressf <-paste(x$postset,x$bnstreet,sep=",")
  
  
  
  y$addressfinal <- trimws(y$add)
  
  #y$addressfinal <- gsub("FRESHFIELD", "FRESHFIELDS", y$addressfinal)
  y$addressfinal <- gsub("[/]", "", y$addressfinal)
  y$addressfinal <- gsub("[.]", "", y$addressfinal)
  y$addressfinal <- gsub("[']", "", y$addressfinal)
  y$addressfinal <- gsub(" ", "", y$addressfinal)
  y$addressf <- paste(y$postset,y$addressfinal,sep=",")
  
  
  taba1 <- inner_join(x,y,by="addressf")
  return(taba1)
}



link403<-function403(add,epc)
dim(link403)
# 
link403<-link403[,..needlist11]

link403u<- uniqueresult(link403)
dim(link403u)
#

link403d <- doubleresult(link403)
dim(link403d)


dim(epc)
#
epc <- matchleft(epc,link403)
dim(epc)
# 2407640      40


#################### method 404  ##################

function404<- function(x,y){
  x$bnstreet <-    paste(x$ss,x$paotext,sep=" ")
  x$bnstreet <-    paste(x$bnstreet,x$streetdescription,sep=",")
  #x$bnstreet <-    paste(x$bnstreet,x$streetdescription,sep=" ")
  x$bnstreet <- gsub("[.]", "", x$bnstreet)
  x$bnstreet <- gsub("[/]", "", x$bnstreet)
  x$bnstreet <- gsub("[']", "", x$bnstreet)
  x$bnstreet <- gsub(" ", "", x$bnstreet)
  x$addressf <-paste(x$postset,x$bnstreet,sep=",")
  
  
  # y<-y[y$property_type=="Flat",]
  
  #y<-y[y$property_type=="Flat"|y$property_type=="Maisonette",]
  y$addressfinal <- paste(y$add1,y$add2,sep=",")
  y$addressfinal <- gsub("CLEARWATER", "CLEARWATER VILLAGE", y$addressfinal)
  #y$addressfinal <- gsub("FLAT", "APARTMENT", y$addressfinal)
  y$addressfinal <- gsub("[.]", "", y$addressfinal)
  y$addressfinal <- gsub("[/]", "", y$addressfinal)
  y$addressfinal <- gsub("[']", "", y$addressfinal)
  
  y$addressfinal <- gsub(" ", "", y$addressfinal)
  y$addressf <- paste(y$postset,y$addressfinal,sep=",")
  
  taba1 <- inner_join(x,y,by="addressf")
  return(taba1)
  
  
}


link404<-function404(add,epc)



link404<-link404[,..needlist11]


link404u<- uniqueresult(link404)
dim(link404u)


link404d <- doubleresult(link404)

dim(epc)
# 
epc<- matchleft(epc,link404)
dim(epc)
#   777220     28
#################### method 405 ##################


function405<- function(x,y){
  x$bnstreet <-    paste(x$saotext,x$pp,sep=",")
  x$bnstreet <-    paste(x$bnstreet,x$paotext,sep=" ")
  x$bnstreet <-    paste(x$bnstreet,x$streetdescription,sep=",")
  x$bnstreet <- gsub("[.]", "", x$bnstreet)
  x$bnstreet <- gsub("[/]", "", x$bnstreet)
  x$bnstreet <- gsub("[']", "", x$bnstreet)
  x$bnstreet <- gsub(" ", "", x$bnstreet)
  x$addressf <-paste(x$postcodelocator,x$bnstreet,sep=",")
  
  
  y<-y[y$postcode=="RG1 4ET",]
  
  #y<-y[y$property_type=="Flat"|y$property_type=="Maisonette",]
  y$addressfinal <- y$add
  y$addressfinal <- gsub("FLAT A", "FLAT", y$addressfinal)
  y$addressfinal <- gsub("FLAT A0", "FLAT", y$addressfinal)
  y$addressfinal <- gsub("FLAT B", "FLAT", y$addressfinal)
  y$addressfinal <- gsub("FLAT B0", "FLAT", y$addressfinal)
  
  y$addressfinal <- gsub("[.]", "", y$addressfinal)
  y$addressfinal <- gsub("[/]", "", y$addressfinal)
  y$addressfinal <- gsub("[']", "", y$addressfinal)
  
  y$addressfinal <- gsub(" ", "", y$addressfinal)
  y$addressf <- paste(y$postcode,y$addressfinal,sep=",")
  
  taba1 <- inner_join(x,y,by="addressf")
  return(taba1)
  
  
}



link405<-function405(add,epc)


link405<-link405[,..needlist11]


link405u<- uniqueresult(link405)
dim(link405u)


link405d <- doubleresult(link405)
dim(link405d)


dim(epc)
#
epc <- matchleft(epc,link405)
dim(epc)
#727963     28

#
#################### method 406 ##################

function406<- function(x,y){
  
  x$bnstreet <-    paste(x$subbuildingname,x$buildingname,sep=",")
  x$bnstreet <-    paste(x$bnstreet,x$buildingnumber,sep=",")
  x$bnstreet <-    paste(x$bnstreet,x$streetdescription,sep=" ")
  x$bnstreet <- gsub(",", "", x$bnstreet)
  x$bnstreet <- gsub("-", "", x$bnstreet)
  x$bnstreet <- gsub("[.]", "", x$bnstreet)
  x$bnstreet <- gsub("[/]", "", x$bnstreet)
  x$bnstreet <- gsub("[']", "", x$bnstreet)
  x$bnstreet <- gsub(" ", "", x$bnstreet)
  x$addressf <-paste(x$postset,x$bnstreet,sep=",")
  
  #BD1 2HQ
  
  y$addressfinal <- y$add
  # y$addressfinal <- gsub("CLEARWATER", "CLEARWATER VILLAGE", y$addressfinal)
  # y$addressfinal <- gsub("COLONNADE BUILDING", "COLONNADE HOUSE", y$addressfinal)
  x$addressfinal <- gsub("-", "", x$addressfinal)
  y$addressfinal <- gsub(",", "", y$addressfinal)
  y$addressfinal <- gsub("[.]", "", y$addressfinal)
  y$addressfinal <- gsub("[/]", "", y$addressfinal)
  y$addressfinal <- gsub("[']", "", y$addressfinal)
  
  y$addressfinal <- gsub(" ", "", y$addressfinal)
  y$addressf <- paste(y$postset,y$addressfinal,sep=",")
  
  taba1 <- inner_join(x,y,by="addressf")
  return(taba1)
}




link406<-function406(add,epc)
dim(link406)
# 

link406<-link406[,..needlist11]



link406u<- uniqueresult(link406)
dim(link406u)
# 

link406d <- doubleresult(link406)
dim(link406d)


epc <- matchleft(epc,link406)
dim(epc)
# 2400401      40

#################### method 407##################



function407<- function(x,y){
  x$bnstreet <-    paste(x$subbuildingname,x$buildingname,sep=",")
  #x$bnstreet <-    paste(x$bnstreet,x$buildingnumber,sep=",")
  x$bnstreet <-    paste(x$bnstreet,x$streetdescription,sep=",")
  #x$bnstreet <- gsub(",", "", x$bnstreet)
  #x$bnstreet <- gsub("-", "", x$bnstreet)
  x$bnstreet <- gsub("[.]", "", x$bnstreet)
  x$bnstreet <- gsub("[/]", "", x$bnstreet)
  x$bnstreet <- gsub("[']", "", x$bnstreet)
  x$bnstreet <- gsub(" ", "", x$bnstreet)
  x$addressf <-paste(x$postset,x$bnstreet,sep=",")
  
  #BD1 2HQ
  #"FLAT 47"
  y$addressfinal <- y$add
  # y$addressfinal <- gsub("CLEARWATER", "CLEARWATER VILLAGE", y$addressfinal)
  y$addressfinal <- gsub("STUDIO", "FLAT", y$addressfinal)
  # x$addressfinal <- gsub("-", "", x$addressfinal)
  # y$addressfinal <- gsub(",", "", y$addressfinal)
  y$addressfinal <- gsub("[.]", "", y$addressfinal)
  y$addressfinal <- gsub("[/]", "", y$addressfinal)
  y$addressfinal <- gsub("[']", "", y$addressfinal)
  
  y$addressfinal <- gsub(" ", "", y$addressfinal)
  y$addressf <- paste(y$postset,y$addressfinal,sep=",")
  
  taba1 <- inner_join(x,y,by="addressf")
  return(taba1)
}

link407<-function407(add,epc)
dim(link407)


link407<-link407[,..needlist11]

link407u<- uniqueresult(link407)
dim(link407u)


link407d <- doubleresult(link407)
dim(link407d)


dim(epc)
# 
epc <- matchleft(epc,link407)
dim(epc)
#2400190      40
#
#################### method 408 ##################

function408<- function(x,y){
  
  
  x$bnstreet <-    paste(x$saotext,x$paotext,sep=" ")

  x$bnstreet <-    paste(x$bnstreet,x$streetdescription,sep=",")
  x$bnstreet <-    paste(x$bnstreet,x$locality,sep=",")
  
  x$bnstreet <- gsub("[.]", "", x$bnstreet)
  x$bnstreet <- gsub("[/]", "", x$bnstreet)
  x$bnstreet <- gsub("[']", "", x$bnstreet)
  x$bnstreet <- gsub(" ", "", x$bnstreet)
  x$addressf <-paste(x$postset,x$bnstreet,sep=",")
  
  y<-y[y$property_type=="Flat"|y$property_type=="Maisonette",]

  y$addressfinal <-  paste("FLAT", y$add, sep=" ")
 
  y$addressfinal <- gsub("[.]", "", y$addressfinal)
  y$addressfinal <- gsub("[/]", "", y$addressfinal)
  y$addressfinal <- gsub("[']", "", y$addressfinal)
  
  y$addressfinal <- gsub(" ", "", y$addressfinal)
  y$addressf <- paste(y$postset,y$addressfinal,sep=",")
  
  taba1 <- inner_join(x,y,by="addressf")
  return(taba1)
  
}



link408<-function408(add,epc)
dim(link408)


link408<-link408[,..needlist11]



link408u<- uniqueresult(link408)
dim(link408u)


link408d <- doubleresult(link408)
dim(link408d)


dim(epc)
# 
epc <- matchleft(epc,link408)
dim(epc)
#2409509      40
#################### method 409 ##################
function409<- function(x,y){
  
  x$bnstreet <-    paste(x$subbuildingname,x$buildingname,sep=",")
  #x$bnstreet <-    paste(x$bnstreet,x$buildingnumber,sep=",")
  x$bnstreet <-    paste(x$bnstreet,x$locality,sep=" ")
  x$bnstreet <-    paste(x$bnstreet,x$townname,sep=",")
 
  x$bnstreet <- gsub("[.]", "", x$bnstreet)
  x$bnstreet <- gsub("[/]", "", x$bnstreet)
  x$bnstreet <- gsub("[']", "", x$bnstreet)
  x$bnstreet <- gsub(" ", "", x$bnstreet)
  x$addressf <-paste(x$postset,x$bnstreet,sep=",")
 
  y$addressfinal <-   y$add
  y$addressfinal <- gsub("NO.1", "NUMBER ONE", y$addressfinal)
  
  y$addressfinal <- gsub("[.]", "", y$addressfinal)
  y$addressfinal <- gsub("[/]", "", y$addressfinal)
  y$addressfinal <- gsub("[']", "", y$addressfinal)
  
  y$addressfinal <- gsub(" ", "", y$addressfinal)
  y$addressf <- paste(y$postset,y$addressfinal,sep=",")
  
  taba1 <- inner_join(x,y,by="addressf")
  return(taba1)
  
}
link409<-function409(add,epc)

link409<-link409[,..needlist11]


link409u<- uniqueresult(link409)
dim(link409u)


link409d <- doubleresult(link409)
dim(link409d)

dim(epc)

epc <- matchleft(epc,link409)
dim(epc)
# 776564     28

#################### method 410 check##################
function410<- function(x,y){
  x$bnstreet <-    paste(x$saotext,x$buildingnumber,sep=" ")
  
  x$bnstreet <-    paste(x$bnstreet,x$streetdescription,sep=" ")
  
  x$bnstreet <- gsub("[.]", "", x$bnstreet)
  x$bnstreet <- gsub("[/]", "", x$bnstreet)
  x$bnstreet <- gsub("[']", "", x$bnstreet)
  #x$bnstreet <- gsub(" ", "", x$bnstreet)
  x$addressf <-paste(x$postset,x$bnstreet,sep=",")
  
 
  y$addressfinal <- y$add1
  y$addressfinal <- gsub("WATERLOO", "", y$addressfinal)
  
  y$addressfinal <- gsub("[.]", "", y$addressfinal)
  y$addressfinal <- gsub("[/]", "", y$addressfinal)
  y$addressfinal <- gsub("[']", "", y$addressfinal)
  
  y$addressf <- paste(y$postset,y$addressfinal,sep=",")
  
  taba1 <- inner_join(x,y,by="addressf")
  return(taba1)
  
  
}

link410<-function410(add,epc)

length(unique(link410$lmk_key))

link410<-link410[,..needlist11]

link410u<- uniqueresult(link410)

dim(link410u)


link410d <- doubleresult(link410)
dim(link410d)

dim(epc)
#
epc <- matchleft(epc,link410)
dim(epc)
#777416     28

#################### method 411 ##################

function411<- function(x,y){
  
  x$bnstreet <-    paste(x$subbuildingname,x$pp,sep=",")
  
  x$bnstreet <-    paste(x$bnstreet,x$streetdescription,sep=" ")
  
  x$bnstreet <- gsub("[.]", "", x$bnstreet)
  x$bnstreet <- gsub("[/]", "", x$bnstreet)
  x$bnstreet <- gsub("[']", "", x$bnstreet)
  x$bnstreet <- gsub(" ", "", x$bnstreet)
  x$addressf <-paste(x$postset,x$bnstreet,sep=",")
  
  
  y$addressfinal <- y$add
  #y$addressfinal <- gsub(" WATERLOO", "", y$addressfinal)
  y$addressfinal <- gsub(" ", "", y$addressfinal)
  y$addressfinal <- gsub("[.]", "", y$addressfinal)
  y$addressfinal <- gsub("[/]", "", y$addressfinal)
  y$addressfinal <- gsub("[']", "", y$addressfinal)
  
  y$addressf <- paste(y$postset,y$addressfinal,sep=",")
  
  taba1 <- inner_join(x,y,by="addressf")
  return(taba1)
  
}


link411<-function411(add,epc)


link411<-link411[,..needlist11]


link411u<- uniqueresult(link411)
dim(link411u)


link411d <- doubleresult(link411)
dim(link411d)

dim(epc)
#
epc <- matchleft(epc,link411)
dim(epc)
#2418669      41


#################### method 412 ##################


function412<- function(x,y){
  
  x$bnstreet <-    paste(x$subbuildingname,x$pp,sep=",")
  
  x$bnstreet <-    paste(x$bnstreet,x$streetdescription,sep=" ")
  
  x$bnstreet <- gsub("[.]", "", x$bnstreet)
  x$bnstreet <- gsub("[/]", "", x$bnstreet)
  x$bnstreet <- gsub("[']", "", x$bnstreet)
  x$bnstreet <- gsub(" ", "", x$bnstreet)
  x$addressf <-paste(substring(x$postset,1,3),x$bnstreet,sep=",")
  
  
  y$addressfinal <- y$add
  #y$addressfinal <- gsub(" WATERLOO", "", y$addressfinal)
  y$addressfinal <- gsub(" ", "", y$addressfinal)
  y$addressfinal <- gsub("[.]", "", y$addressfinal)
  y$addressfinal <- gsub("[/]", "", y$addressfinal)
  y$addressfinal <- gsub("[']", "", y$addressfinal)
  
  y$addressf <- paste(substring(y$postset,1,3),y$addressfinal,sep=",")
  
  taba1 <- inner_join(x,y,by="addressf")
  return(taba1)
  
}

link412<-function412(add,epc)

link412<-link412[,..needlist11]


link412u<- uniqueresult(link412)
dim(link412u)


link412d <- doubleresult(link412)
dim(link412d)


dim(epc)
#
epc <- matchleft(epc,link412)
dim(epc)
# 2417648      41



#################### method 413 ##################

function413<- function(x,y){
  
  
  x$bnstreet <-    paste(x$subbuildingname,x$pp,sep=",")
  
  x$bnstreet <-    paste(x$bnstreet,x$streetdescription,sep=" ")
  
  x$bnstreet <- gsub("[.]", "", x$bnstreet)
  x$bnstreet <- gsub("[/]", "", x$bnstreet)
  x$bnstreet <- gsub("[']", "", x$bnstreet)
  x$bnstreet <- gsub(" ", "", x$bnstreet)
  x$addressf <-paste(substring(x$postset,1,2),x$bnstreet,sep=",")
  
  
  y$addressfinal <- y$add
  #y$addressfinal <- gsub(" WATERLOO", "", y$addressfinal)
  y$addressfinal <- gsub(" ", "", y$addressfinal)
  y$addressfinal <- gsub("[.]", "", y$addressfinal)
  y$addressfinal <- gsub("[/]", "", y$addressfinal)
  y$addressfinal <- gsub("[']", "", y$addressfinal)
  
  y$addressf <- paste(substring(y$postset,1,2),y$addressfinal,sep=",")
  
  taba1 <- inner_join(x,y,by="addressf")
  return(taba1)
  
}



link413<-function413(add,epc)
dim(link413)
# 
link413<-link413[,..needlist11]

link413u<- uniqueresult(link413)
dim(link413u)
#

link413d <- doubleresult(link413)
dim(link413d)


dim(epc)
#
epc <- matchleft(epc,link413)
dim(epc)
# 2417641      41


#################### method 414  ##################

function414<- function(x,y){
  
  x$bnstreet <-    paste(x$saotext,x$buildingname,sep=",")
  x$bnstreet <-    paste(x$bnstreet,x$buildingnumber,sep=",")
  x$bnstreet <-    paste(x$bnstreet,x$streetdescription,sep=" ")
  x$bnstreet <- gsub("[.]", "", x$bnstreet)
  x$bnstreet <- gsub("[/]", "", x$bnstreet)
  x$bnstreet <- gsub("[']", "", x$bnstreet)
  x$bnstreet <- gsub(" ", "", x$bnstreet)
  x$addressf <-paste(x$postset,x$bnstreet,sep=",")
  
  
  #y<-y[y$property_type=="Flat",]
  
  #y<-y[y$property_type=="Flat"|y$property_type=="Maisonette",]
  y$addressfinal <- y$add1
  y$addressfinal <- gsub("[-]", "", y$addressfinal)
  y$addressfinal <- gsub("FLAT 01", "FLAT 1", y$addressfinal)
  y$addressfinal <- gsub("FLAT 02", "FLAT 2", y$addressfinal)
  y$addressfinal <- gsub("FLAT 03", "FLAT 3", y$addressfinal)
  y$addressfinal <- gsub("FLAT 04", "FLAT 4", y$addressfinal)
  y$addressfinal <- gsub("FLAT 05", "FLAT 5", y$addressfinal)
  y$addressfinal <- gsub("FLAT 06", "FLAT 6", y$addressfinal)
  y$addressfinal <- gsub("FLAT 07", "FLAT 7", y$addressfinal)
  y$addressfinal <- gsub("FLAT 08", "FLAT 8", y$addressfinal)
  y$addressfinal <- gsub("FLAT 09", "FLAT 9", y$addressfinal)
  
  y$addressfinal <- paste( y$addressfinal ,  y$add2, sep=",")
  y$addressfinal <- paste( y$addressfinal ,  y$add3, sep=",")
  y$addressfinal <- gsub(" ", "", y$addressfinal)
  y$addressfinal <- gsub("[.]", "", y$addressfinal)
  y$addressfinal <- gsub("[/]", "", y$addressfinal)
  y$addressfinal <- gsub("[']", "", y$addressfinal)
  y$addressf <- paste(y$postset,y$addressfinal,sep=",")
  
  taba1 <- inner_join(x,y,by="addressf")
  return(taba1)
}


link414<-function414(add,epc)



link414<-link414[,..needlist11]


link414u<- uniqueresult(link414)
dim(link414u)


link414d <- doubleresult(link414)

dim(epc)
# 
epc<- matchleft(epc,link414)
dim(epc)
#   777220     28
#################### method 415 ##################


function415<- function(x,y){
  x$bnstreet <-    paste(x$saotext,x$pp,sep=",")
  
  x$bnstreet <-    paste(x$bnstreet,x$streetdescription,sep=" ")
  x$bnstreet <- gsub("STUDIO FLAT", "APARTMENT", x$bnstreet)
  x$bnstreet <- gsub("ROOM", "APARTMENT", x$bnstreet)
  x$bnstreet <- gsub("[.]", "", x$bnstreet)
  x$bnstreet <- gsub("[/]", "", x$bnstreet)
  x$bnstreet <- gsub("[']", "", x$bnstreet)
  x$bnstreet <- gsub(" ", "", x$bnstreet)
 
  
  x$addressf <-paste(x$postcodelocator,x$bnstreet,sep=",")
  
  
  
  y$addressfinal <- y$add
  
  y$addressfinal <- gsub(" ", "", y$addressfinal)
  y$addressfinal <- gsub("[.]", "", y$addressfinal)
  y$addressfinal <- gsub("[/]", "", y$addressfinal)
  y$addressfinal <- gsub("[']", "", y$addressfinal)
  y$addressf <- paste(y$postcode,y$addressfinal,sep=",")
  
  taba1 <- inner_join(x,y,by="addressf")
  return(taba1)
  
}



link415<-function415(add,epc)


link415<-link415[,..needlist11]


link415u<- uniqueresult(link415)
dim(link415u)


link415d <- doubleresult(link415)
dim(link415d)


dim(epc)
#
epc <- matchleft(epc,link415)
dim(epc)
#2410414      41

#
#################### method 416 ##################

function416<- function(x,y){
  x$bnstreet <-    paste(x$saotext,x$streetdescription,sep=" ")
  
  x$bnstreet <- gsub("[.]", "", x$bnstreet)
  x$bnstreet <- gsub("[/]", "", x$bnstreet)
  x$bnstreet <- gsub("[']", "", x$bnstreet)
  x$bnstreet <- gsub(" ", "", x$bnstreet)
  #STUDIO FLAT 
  x$addressf <-paste(x$postcodelocator,x$bnstreet,sep=",")
  
  
  y<-y[y$property_type=="Flat"|y$property_type=="Maisonette",]
  y$addressfinal <- paste("FLAT",y$add,sep="")
  
  y$addressfinal <- gsub("PIMPLICO PLACE, 28", "", y$addressfinal)
  y$addressfinal <- gsub(" ", "", y$addressfinal)
  y$addressfinal <- gsub("[.]", "", y$addressfinal)
  y$addressfinal <- gsub("[/]", "", y$addressfinal)
  y$addressfinal <- gsub("[']", "", y$addressfinal)
  y$addressf <- paste(y$postcode,y$addressfinal,sep=",")
  
  taba1 <- inner_join(x,y,by="addressf")
  return(taba1)
  
}




link416<-function416(add,epc)
dim(link416)
# 

link416<-link416[,..needlist11]



link416u<- uniqueresult(link416)
dim(link416u)
# 

link416d <- doubleresult(link416)
dim(link416d)


epc <- matchleft(epc,link416)
dim(epc)
# 2410411      41

#################### method 417 ##################



function417<- function(x,y){
  x$bnstreet <-    paste(x$saotext,x$pp,sep=",")

  x$bnstreet <-    paste(x$bnstreet,x$streetdescription,sep=" ")

  x$bnstreet <- gsub("[.]", "", x$bnstreet)
  x$bnstreet <- gsub("[/]", "", x$bnstreet)
  x$bnstreet <- gsub("[']", "", x$bnstreet)
  x$bnstreet <- gsub(" ", "", x$bnstreet)
  #STUDIO FLAT

  x$addressf <-paste(x$postcodelocator,x$bnstreet,sep=",")
  y[add2=="CORE B, PIMLICO PLACE",add1:=gsub("FLAT", "FLAT B",add1)]
  y[add2=="CORE E, PIMLICO PLACE",add1:=gsub("FLAT", "FLAT E",add1)]
  y[add2=="CORE E",add1:=gsub("FLAT", "FLAT E",add1)]
  y[add2=="CORE B",add1:=gsub("FLAT", "FLAT B",add1)]
  y[add2=="CORE A",add1:=gsub("FLAT", "FLAT A",add1)]
  y[add2=="CORE D",add1:=gsub("FLAT", "FLAT D",add1)]
  y[add2=="CORE F",add1:=gsub("FLAT", "FLAT F",add1)]


  y$addressfinal <- paste(y$add1,y$add3,sep=",")


  y$addressfinal <- gsub(" ", "", y$addressfinal)
  y$addressfinal <- gsub("[.]", "", y$addressfinal)
  y$addressfinal <- gsub("[/]", "", y$addressfinal)
  y$addressfinal <- gsub("[']", "", y$addressfinal)
  y$addressf <- paste(y$postcode,y$addressfinal,sep=",")

  taba1 <- inner_join(x,y,by="addressf")
  return(taba1)
}

link417<-function417(add,epc)
dim(link417)


link417<-link417[,..needlist11]

link417u<- uniqueresult(link417)
dim(link417u)


link417d <- doubleresult(link417)
dim(link417d)


dim(epc)
# 
epc <- matchleft(epc,link417)
dim(epc)
#708495     28
#
#################### method 418 ##################

function418<- function(x,y){
  
  x$bnstreet <- paste("CHALET",x$ss ,sep=" ")
  x$bnstreet <- paste(x$bnstreet,x$paotext ,sep=",")
  x$bnstreet <- paste(x$bnstreet,x$streetdescription ,sep=",")
  x$bnstreet <- paste(x$bnstreet,x$townname ,sep=",")
  #x$bnstreet<- trimws(x$bnstreet)
  x$bnstreet <- gsub("/", "", x$bnstreet)
  x$bnstreet <- gsub("[.]", "", x$bnstreet)
  x$bnstreet <- gsub("[']", "", x$bnstreet)
  x$bnstreet <- gsub(" ", "", x$bnstreet)
  x$addressf <-paste(x$postset,x$bnstreet,sep=",")
  
  
  y$addressfinal <-  trimws(y$add)
  #y$addressfinal <- gsub(",", "", y$addressfinal)
  y$addressfinal <- gsub("[/]", "", y$addressfinal)
  y$addressfinal <- gsub("[.]", "", y$addressfinal)
  y$addressfinal <- gsub("[']", "", y$addressfinal)
  
  y$addressfinal <- gsub(" ", "", y$addressfinal)
  y$addressf <- paste(y$postset,y$addressfinal,sep=",")
  
  taba1 <- inner_join(x,y,by="addressf")
  return(taba1)
  
  
  
}



link418<-function418(add,epc)
dim(link418)


link418<-link418[,..needlist11]



link418u<- uniqueresult(link418)
dim(link418u)


link418d <- doubleresult(link418)
dim(link418d)


dim(epc)
# 
epc <- matchleft(epc,link418)
dim(epc)
#720508     28

# getwd()
# fwrite(epc,"epc_left.csv")
#################### method 419 ##################
function419<- function(x,y){
  x$bnstreet <-    paste(x$pp,x$streetdescription,sep=" ")
  x$bnstreet <- gsub("[.]", "", x$bnstreet)
  x$bnstreet <- gsub("[/]", "", x$bnstreet)
  x$bnstreet <- gsub("[']", "", x$bnstreet)
  x$bnstreet <- gsub(" ", "", x$bnstreet)
  x$addressf <-paste(x$postset,x$bnstreet,sep=",")
  
  #y<-y[y$property_type=="Flat",]
  
  #y<-y[y$property_type=="Flat"|y$property_type=="Maisonette",]
  y<-y[add1=="FLAT",]
  
  y$addressfinal <- y$add2
  
  y$addressfinal <- gsub(" ", "", y$addressfinal)
  y$addressfinal <- gsub("[.]", "", y$addressfinal)
  y$addressfinal <- gsub("[/]", "", y$addressfinal)
  y$addressfinal <- gsub("[']", "", y$addressfinal)
  y$addressf <- paste(y$postset,y$addressfinal,sep=",")
  
  taba1 <- inner_join(x,y,by="addressf")
  return(taba1)
  
  

  
}
link419<-function419(add,epc)

link419<-link419[,..needlist11]


link419u<- uniqueresult(link419)
dim(link419u)


link419d <- doubleresult(link419)
dim(link419d)

dim(epc)

epc <- matchleft(epc,link419)
dim(epc)
# 776564     28



#################### method 420 check##################
function420<- function(x,y){
  x$bnstreet <-    paste("FLAT",x$buildingname,sep=" ")

  x$bnstreet <-    paste(x$bnstreet,x$streetdescription,sep=" ")
  x$bnstreet <- gsub("[.]", "", x$bnstreet)
  x$bnstreet <- gsub("[/]", "", x$bnstreet)
  x$bnstreet <- gsub("[']", "", x$bnstreet)
  x$bnstreet <- gsub(" ", "", x$bnstreet)
  x$addressf <-paste(x$postset,x$bnstreet,sep=",")
  
 
  y<-y[y$property_type=="Flat"|y$property_type=="Maisonette",]
  y<-y[add1!="FLAT",]
  y$addressfinal <- y$add1
  
  y$addressfinal <- gsub(" ", "", y$addressfinal)
  y$addressfinal <- gsub("[.]", "", y$addressfinal)
  y$addressfinal <- gsub("[/]", "", y$addressfinal)
  y$addressfinal <- gsub("[']", "", y$addressfinal)
  y$addressf <- paste(y$postset,y$addressfinal,sep=",")
  
  taba1 <- inner_join(x,y,by="addressf")
  return(taba1)
  
  
  
}

link420<-function420(add,epc)

length(unique(link420$lmk_key))

link420<-link420[,..needlist11]

link420u<- uniqueresult(link420)

dim(link420u)


link420d <- doubleresult(link420)
dim(link420d)

dim(epc)
#
epc <- matchleft(epc,link420)
dim(epc)
#777426     28

#################### method 421 ##################

function421<- function(x,y){
  
  x<-x[x$saotext!="",]
  x<-x[x$paotext!="",]
  x$bnstreet <-    paste(x$saotext,x$paotext,sep=" ")
  #x$bnstreet <-    paste(x$saotext,x$streetdescription,sep=" ")
  x$bnstreet <- gsub(",", "", x$bnstreet)
  x$bnstreet <- gsub("[.]", "", x$bnstreet)
  x$bnstreet <- gsub("[/]", "", x$bnstreet)
  x$bnstreet <- gsub("[']", "", x$bnstreet)
  x$bnstreet <- gsub(" ", "", x$bnstreet)
  x$addressf <-paste(x$postset,x$bnstreet,sep=",")
  
  #y<-y[y$property_type=="Flat"|y$property_type=="Maisonette",]
  y$addressfinal <- y$add1
  y$addressfinal <- gsub("HUMPHREY DAVY HOUSE", "HUMPHRY DAVY HOUSE", y$addressfinal)
  #y$addressfinal <- gsub("FLAT ", "", y$addressfinal)
  y$addressfinal <- gsub(",", "", y$addressfinal)
  y$addressfinal <- gsub(" ", "", y$addressfinal)
  y$addressfinal <- gsub("[.]", "", y$addressfinal)
  y$addressfinal <- gsub("[/]", "", y$addressfinal)
  y$addressfinal <- gsub("[']", "", y$addressfinal)
  y$addressf <- paste(y$postset,y$addressfinal,sep=",")
  
  taba1 <- inner_join(x,y,by="addressf")
  return(taba1)
  
}

epc[add1]

link421<-function421(add,epc)


link421<-link421[,..needlist11]


link421u<- uniqueresult(link421)
dim(link421u)


link421d <- doubleresult(link421)
dim(link421d)

dim(epc)
#
epc <- matchleft(epc,link421)
dim(epc)
#2428669      42


#################### method 422 ##################


function422<- function(x,y){

  # x<-x[x$pp!="",]
  # x$bnstreet <-    paste(x$pp,x$streetdescription,sep=" ")
  # #x$bnstreet <-    paste(v,x$buildingnumber,sep=",")
  # #x$bnstreet <-    paste(x$bnstreet,x$streetdescription,sep="")
  # #x$bnstreet <- gsub(",", "", x$bnstreet)
  # x$bnstreet <- gsub("[.]", "", x$bnstreet)
  # x$bnstreet <- gsub("[/]", "", x$bnstreet)
  # x$bnstreet <- gsub("[']", "", x$bnstreet)
  # x$bnstreet <- gsub(" ", "", x$bnstreet)
  # x$addressf <-paste(x$postset,x$bnstreet,sep=",")
  # 
  # #y<-y[y$property_type=="Flat",]
  # 
  # #y<-y[y$property_type=="Flat"|y$property_type=="Maisonette",]
  # y<-y[add1=="FLAT",]
  # y$addressfinal <- paste(y$add2,y$add3,sep=" ")
  # #y$addressfinal <- gsub(",", "", y$addressfinal)
  # y$addressfinal <- gsub(" ", "", y$addressfinal)
  # y$addressfinal <- gsub("[.]", "", y$addressfinal)
  # y$addressfinal <- gsub("[/]", "", y$addressfinal)
  # y$addressfinal <- gsub("[']", "", y$addressfinal)
  # y$addressf <- paste(y$postset,y$addressfinal,sep=",")
  # 
  # taba1 <- inner_join(x,y,by="addressf")
  # return(taba1)
  
  
  x<-x[x$saotext!="",]
  #x<-x[x$paotext!="",]
  #x$bnstreet <-    paste(x$saotext,x$paotext,sep=" ")
  x$bnstreet <-    paste(x$saotext,x$streetdescription,sep=" ")
  #x$bnstreet <- gsub(",", "", x$bnstreet)
  x$bnstreet <- gsub("[.]", "", x$bnstreet)
  x$bnstreet <- gsub("[/]", "", x$bnstreet)
  x$bnstreet <- gsub("[']", "", x$bnstreet)
  x$bnstreet <- gsub(" ", "", x$bnstreet)
  x$addressf <-paste(x$postset,x$bnstreet,sep=",")
  
  y<-y[!grepl("\\d\\d?[,]\\d\\d?",y$add1) ,]
  y<-y[!grepl("\\d\\d?[,]\\s\\d\\d?",y$add1) ,]
  y$addressfinal <- y$add1
  y$addressfinal <- gsub("HUMPHREY DAVY HOUSE", "HUMPHRY DAVY HOUSE", y$addressfinal)
  #y$addressfinal <- gsub("FLAT ", "", y$addressfinal)
  y$addressfinal <- gsub(",", "", y$addressfinal)
  y$addressfinal <- gsub(" ", "", y$addressfinal)
  y$addressfinal <- gsub("[.]", "", y$addressfinal)
  y$addressfinal <- gsub("[/]", "", y$addressfinal)
  y$addressfinal <- gsub("[']", "", y$addressfinal)
  y$addressf <- paste(y$postset,y$addressfinal,sep=",")
  
  taba1 <- inner_join(x,y,by="addressf")
  return(taba1)
  
}

link422<-function422(add,epc)

link422<-link422[,..needlist11]


link422u<- uniqueresult(link422)
dim(link422u)


link422d <- doubleresult(link422)
dim(link422d)


dim(epc)
#
epc <- matchleft(epc,link422)
dim(epc)
# 2427648      42



#################### method 423 ##################

function423<- function(x,y){
  
  x$bnstreet <- paste("APARTMENT",x$buildingname,sep=" ")
  #x$bnstreet <- paste(x$bnstreet,x$pp,sep=", ")
  x$bnstreet <- paste(x$bnstreet,x$streetdescription,sep=" ")
 
  x$bnstreet <- gsub("/", "", x$bnstreet)
  x$bnstreet <- gsub("[.]", "", x$bnstreet)
  x$bnstreet <- gsub("[']", "", x$bnstreet)
  x$bnstreet <- gsub(" ", "", x$bnstreet)
  x$addressf <-paste(x$postset,x$bnstreet,sep=",")
  
  y<-y[y$property_type=="Flat"|y$property_type=="Maisonette",]
  y$addressfinal <- paste(y$add1,y$add2,sep=" ") 
  y$addressfinal <- paste(y$addressfinal,y$add3,sep=" ") 
 
  y$addressfinal <- gsub("[/]", "", y$addressfinal)
  y$addressfinal <- gsub("[.]", "", y$addressfinal)
  y$addressfinal <- gsub("[']", "", y$addressfinal)
  #y$addressfinal <- gsub(",", "", y$addressfinal)
  y$addressfinal <- gsub(" ", "", y$addressfinal)
  y$addressf <- paste(y$postset,y$addressfinal,sep=",")
  
  taba1 <- inner_join(x,y,by="addressf")
  
  return(taba1)
  
  
}



link423<-function423(add,epc)
dim(link423)
# 
link423<-link423[,..needlist11]

link423u<- uniqueresult(link423)
dim(link423u)
#

link423d <- doubleresult(link423)
dim(link423d)


dim(epc)
#
epc <- matchleft(epc,link423)
dim(epc)
# 2427642      42


#################### method 424  ##################

function424<- function(x,y){
  
  x$bnstreet <- paste(x$pp,x$streetdescription,sep=" ")
  
  x$bnstreet <- gsub(",", "", x$bnstreet)
  x$bnstreet <- gsub("/", "", x$bnstreet)
  x$bnstreet <- gsub("[.]", "", x$bnstreet)
  x$bnstreet <- gsub("[']", "", x$bnstreet)
  x$bnstreet <- gsub(" ", "", x$bnstreet)
  x$addressf <-paste(x$postset,x$bnstreet,sep=",")
  
  y<-y[y$add!="",]
  y$addressfinal <- y$add
  y$addressfinal <- gsub("[/]", "", y$addressfinal)
  y$addressfinal <- gsub("[.]", "", y$addressfinal)
  y$addressfinal <- gsub("[']", "", y$addressfinal)
  y$addressfinal <- gsub(",", "", y$addressfinal)
  y$addressfinal <- gsub(" ", "", y$addressfinal)
  y$addressf <- paste(y$postset,y$addressfinal,sep=",")
  
  taba1 <- inner_join(x,y,by="addressf")
  
  return(taba1)
  
}


link424<-function424(add,epc)



link424<-link424[,..needlist11]


link424u<- uniqueresult(link424)
dim(link424u)


link424d <- doubleresult(link424)

dim(epc)
# 
epc<- matchleft(epc,link424)
dim(epc)
#   777220     28
#################### method 425 ##################


function425<- function(x,y){
  
  x$bnstreet <- paste(x$buildingnumber,x$streetdescription,sep=" ")
  x$bnstreet <- paste(x$bnstreet,x$townname ,sep=" ")
  x$bnstreet <- gsub(",", "", x$bnstreet)
  x$bnstreet <- gsub("/", "", x$bnstreet)
  x$bnstreet <- gsub("[.]", "", x$bnstreet)
  x$bnstreet <- gsub("[']", "", x$bnstreet)
  x$bnstreet <- gsub(" ", "", x$bnstreet)
  x$addressf <-paste(x$postset,x$bnstreet,sep=",")
  
  y<-y[y$add!="",]
  y$addressfinal <- y$add
  y$addressfinal <- gsub("[/]", "", y$addressfinal)
  y$addressfinal <- gsub("[.]", "", y$addressfinal)
  y$addressfinal <- gsub("[']", "", y$addressfinal)
  y$addressfinal <- gsub(",", "", y$addressfinal)
  y$addressfinal <- gsub(" ", "", y$addressfinal)
  y$addressf <- paste(y$postset,y$addressfinal,sep=",")
  
  taba1 <- inner_join(x,y,by="addressf")
  
  return(taba1)  
}



link425<-function425(add,epc)


link425<-link425[,..needlist11]


link425u<- uniqueresult(link425)
dim(link425u)


link425d <- doubleresult(link425)
dim(link425d)


dim(epc)
#
epc <- matchleft(epc,link425)
dim(epc)
#2420424      42

#
#################### method 426 ##################

function426<- function(x,y){
  
  
  x$bnstreet <-    paste(x$saotext,x$paotext,sep=",")
  x$bnstreet <- paste(x$bnstreet,x$pp,sep=",")
  #x$bnstreet <- paste(x$bnstreet,x$paoendnumber,sep=" ")
  x$bnstreet <- paste(x$bnstreet,x$streetdescription,sep=" ")
  x$bnstreet <- gsub(" ", "", x$bnstreet)
  x$addressf <-paste(x$postset,x$bnstreet,sep=",")
  
  y$addressfinal <- gsub(",", "", y$add1)
  y$addressfinal <- paste( y$addressfinal, y$add2, sep=",")
  
  y$addressfinal <- gsub(" ", "", y$addressfinal)
  y$addressf <- paste(y$postset,y$addressfinal,sep=",")
  
  taba1 <- inner_join(x,y,by="addressf")
  return(taba1)
  
}




link426<-function426(add,epc)
dim(link426)
# 

link426<-link426[,..needlist11]



link426u<- uniqueresult(link426)
dim(link426u)
# 

link426d <- doubleresult(link426)
dim(link426d)


epc <- matchleft(epc,link426)
dim(epc)
# 2420421      42

#################### method 427 ##################



function427<- function(x,y){
  x<-x[grepl("^\\d", x$buildingname),]
  x$bnstreet1<-word(x$buildingname,1)
  x$bnstreet2<-char2end(x$buildingname, " ")
  
  x$bnstreet <- paste(x$bnstreet1,x$subbuildingname,sep=" ")
  x$bnstreet <- paste(x$bnstreet,x$bnstreet2,sep=" ")
  x$bnstreet <- paste(x$bnstreet,x$pp,sep=",")
  x$bnstreet <- paste(x$bnstreet,x$streetdescription,sep=",")
  x$bnstreet <- gsub(",", "", x$bnstreet)
  x$bnstreet <- gsub("/", "", x$bnstreet)
  x$bnstreet <- gsub("[.]", "", x$bnstreet)
  x$bnstreet <- gsub("[']", "", x$bnstreet)
  x$bnstreet <- gsub(" ", "", x$bnstreet)
  x$addressf <-paste(x$postset,x$bnstreet,sep=",")
  
  y$addressfinal <- paste(y$add1,y$add2,sep=",")
  
  y$addressfinal <- gsub("[/]", "", y$addressfinal)
  y$addressfinal <- gsub("[.]", "", y$addressfinal)
  y$addressfinal <- gsub("[']", "", y$addressfinal)
  y$addressfinal <- gsub(",", "", y$addressfinal)
  y$addressfinal <- gsub(" ", "", y$addressfinal)
  y$addressf <- paste(y$postset,y$addressfinal,sep=",")
  
  taba1 <- inner_join(x,y,by="addressf")
  
  return(taba1)
}

link427<-function427(add,epc)
dim(link427)


link427<-link427[,..needlist11]

link427u<- uniqueresult(link427)
dim(link427u)


link427d <- doubleresult(link427)
dim(link427d)


dim(epc)
# 
epc <- matchleft(epc,link427)
dim(epc)
#2420190      42
#
#################### method 428 ##################

function428<- function(x,y){
  
  x<-x[grepl("^\\d", x$buildingname),]
  x$bnstreet1<-word(x$buildingname,1)
  
  #x$bnstreet <- paste(x$bnstreet1,x$pp,sep=",")
  x$bnstreet <- paste(x$bnstreet1,x$streetdescription,sep=",")
  x$bnstreet <- gsub(",", "", x$bnstreet)
  x$bnstreet <- gsub("/", "", x$bnstreet)
  x$bnstreet <- gsub("[.]", "", x$bnstreet)
  x$bnstreet <- gsub("[']", "", x$bnstreet)
  x$bnstreet <- gsub(" ", "", x$bnstreet)
  x$addressf <-paste(x$postset,x$bnstreet,sep=",")
  
  
  y$addressfinal <- paste(y$add1,y$add2,sep=",")
  
  y$addressfinal <- gsub("[/]", "", y$addressfinal)
  y$addressfinal <- gsub("[.]", "", y$addressfinal)
  y$addressfinal <- gsub("[']", "", y$addressfinal)
  y$addressfinal <- gsub(",", "", y$addressfinal)
  y$addressfinal <- gsub(" ", "", y$addressfinal)
  y$addressf <- paste(y$postset,y$addressfinal,sep=",")
  
  taba1 <- inner_join(x,y,by="addressf")
  
  return(taba1)
  
  
}



link428<-function428(add,epc)
dim(link428)


link428<-link428[,..needlist11]



link428u<- uniqueresult(link428)
dim(link428u)


link428d <- doubleresult(link428)
dim(link428d)


dim(epc)
# 
epc <- matchleft(epc,link428)
dim(epc)
#

getwd()

#################### method 429 ##################
function429<- function(x,y){
  
  x$bnstreet <- paste(x$buildingname,x$streetdescription,sep=",")
  
  
  #x$bnstreet <- gsub(",", "", x$bnstreet)
  x$bnstreet <- gsub("/", "", x$bnstreet)
  x$bnstreet <- gsub("[.]", "", x$bnstreet)
  x$bnstreet <- gsub("[']", "", x$bnstreet)
  x$bnstreet <- gsub(" ", "", x$bnstreet)
  x$addressf <-paste(x$postset,x$bnstreet,sep=",")
  
  y<-y[y$property_type=="Flat"|y$property_type=="Maisonette",]
  
  y[postset=="M50 1",add:=gsub("CITYLINKS APARTMENTS", "CITY LINK",add)]
  
  y$addressfinal <- y$add
  
  y$addressfinal <- gsub("[/]", "", y$addressfinal)
  y$addressfinal <- gsub("[.]", "", y$addressfinal)
  y$addressfinal <- gsub("[']", "", y$addressfinal)
  #y$addressfinal <- gsub(",", "", y$addressfinal)
  y$addressfinal <- gsub(" ", "", y$addressfinal)
  y$addressf <- paste(y$postset,y$addressfinal,sep=",")
  
  taba1 <- inner_join(x,y,by="addressf")
  
  return(taba1)
  
  
}
link429<-function429(add,epc)

link429<-link429[,..needlist11]


link429u<- uniqueresult(link429)
dim(link429u)


link429d <- doubleresult(link429)
dim(link429d)

dim(epc)

epc <- matchleft(epc,link429)
dim(epc)
# 



#################### method 430 ##################
function430<- function(x,y){
  
  x$bnstreet <-    paste(x$saotext,x$paotext,sep=",")
  x$bnstreet <- paste(x$bnstreet,x$paostartnumber,sep=",")
  x$bnstreet <- paste(x$bnstreet,x$paoendnumber,sep="-")
  x$bnstreet <- paste(x$bnstreet,x$streetdescription,sep=" ")
  x$bnstreet <- gsub(" ", "", x$bnstreet)
  x$addressf <-paste(x$postset,x$bnstreet,sep=",")
  
  y$addressfinal <- gsub(",", "", y$add1)
  y$addressfinal <- paste( y$addressfinal, y$add2, sep=",")
  
  y$addressfinal <- gsub(" ", "", y$addressfinal)
  y$addressf <- paste(y$postset,y$addressfinal,sep=",")
  
  taba1 <- inner_join(x,y,by="addressf")
  return(taba1)
  
  
}

link430<-function430(add,epc)

length(unique(link430$lmk_key))

link430<-link430[,..needlist11]

link430u<- uniqueresult(link430)

dim(link430u)


link430d <- doubleresult(link430)
dim(link430d)

dim(epc)
#
epc <- matchleft(epc,link430)
dim(epc)
#777436     28

#################### method 431 ##################

function431<- function(x,y){

   x<-x[x$saotext!="",]
  x$bnstreet <- paste(x$saotext,x$streetdescription,sep=" ")
  
  x$bnstreet <- gsub("/", "", x$bnstreet)
  x$bnstreet <- gsub("[.]", "", x$bnstreet)
  x$bnstreet <- gsub("[']", "", x$bnstreet)
  x$bnstreet <- gsub(" ", "", x$bnstreet)
  x$addressf <-paste(x$postset,x$bnstreet,sep=",")
  
  
  y$addressfinal <- paste(y$add1,y$add3,sep=" ") 
  
  y$addressfinal <- gsub("[/]", "", y$addressfinal)
  y$addressfinal <- gsub("[.]", "", y$addressfinal)
  y$addressfinal <- gsub("[']", "", y$addressfinal)
  #y$addressfinal <- gsub(",", "", y$addressfinal)
  y$addressfinal <- gsub(" ", "", y$addressfinal)
  y$addressf <- paste(y$postset,y$addressfinal,sep=",")
  
  taba1 <- inner_join(x,y,by="addressf")
  
  return(taba1)
  
}


link431<-function431(add,epc)


link431<-link431[,..needlist11]


link431u<- uniqueresult(link431)
dim(link431u)


link431d <- doubleresult(link431)
dim(link431d)

dim(epc)
#
epc <- matchleft(epc,link431)
dim(epc)
#2438669      43


#################### method 432 change##################


function432<- function(x,y){
  x<-x[grepl("\\d$",x$paotext),]
  
  x$paotext<- trimws(x$paotext)
  x$bnstreet1 <- word(x$paotext,-1)
  x$bnstreet2 <- str_match(x$paotext, "(^.+)\\s")[, 2]
  x$bnstreet <- paste(x$bnstreet1,x$bnstreet2,sep=" ")
  x$bnstreet <- paste(x$bnstreet,x$locality,sep=",")
  
  x$bnstreet <- paste(x$bnstreet,x$townname,sep=",")
  #x$bnstreet <- gsub(",", "", x$bnstreet)
  x$bnstreet <- gsub("/", "", x$bnstreet)
  x$bnstreet <- gsub("[.]", "", x$bnstreet)
  x$bnstreet <- gsub("[']", "", x$bnstreet)
  x$bnstreet <- gsub(" ", "", x$bnstreet)
  x$addressf <-paste(x$postcodelocator,x$bnstreet,sep=",")
  

  y$addressfinal <-y$add1
  
  y$addressfinal <- gsub(",", "", y$addressfinal)
  y[postcode=="TR27 5AF",addressfinal:=gsub("CLONMORE", "CLOMORE",addressfinal)]
  y[postcode=="TR27 5AF",addressfinal:=gsub("TURESTIN","TORESTIN",addressfinal)]
  y[postcode=="TR27 5AF",addressfinal:=gsub("MONT CLAIR", "MONTCLARE",addressfinal)]
  y[postcode=="TR27 5AF",addressfinal:=gsub("VARDO", "VARIDO",addressfinal)]
  y[postcode=="TR27 5AF",addressfinal:=gsub("CHY-AN-TEWENNOW", "CHY AN TEWENNOW",addressfinal)]

  y$addressfinal <- paste(y$addressfinal,y$add2,sep=",")
  y$addressfinal <- paste(y$addressfinal,y$add3,sep=",")
  y$addressfinal <- gsub("[/]", "", y$addressfinal)
  y$addressfinal <- gsub("[.]", "", y$addressfinal)
  y$addressfinal <- gsub("[']", "", y$addressfinal)
  #y$addressfinal <- gsub(",", "", y$addressfinal)
  y$addressfinal <- gsub(" ", "", y$addressfinal)
  y$addressf <- paste(y$postcode,y$addressfinal,sep=",")
  
  taba1 <- inner_join(x,y,by="addressf")
  
  return(taba1)
}

link432<-function432(add,epc)

link432<-link432[,..needlist1]


link432u<- uniqueresult(link432)
dim(link432u)


link432d <- doubleresult(link432)
dim(link432d)


dim(epc)
#
epc <- matchleft(epc,link432)
dim(epc)
# 705431     28



#################### method 433 ##################

function433<- function(x,y){
  x$bnstreet <- paste(x$saotext,x$paotext,sep=" ")
  x$bnstreet <- paste(x$bnstreet,x$streetdescription,sep=" ")
  x$bnstreet <- paste(x$bnstreet,x$townname,sep=" ")
  #x$bnstreet <- paste(x$subbuildingname,x$streetdescription,sep=" ")
  x$bnstreet <- gsub(",", "", x$bnstreet)
  x$bnstreet <- gsub("/", "", x$bnstreet)
  x$bnstreet <- gsub("[.]", "", x$bnstreet)
  x$bnstreet <- gsub("[']", "", x$bnstreet)
  x$bnstreet <- gsub(" ", "", x$bnstreet)
  x$addressf <-paste(x$postcodelocator,x$bnstreet,sep=",")
  
  y<-y[y$property_type=="Flat"|y$property_type=="Maisonette",]
  y$addressfinal <- y$add
  
  y$addressfinal <- gsub("VILLAGE CENTRE", "THE VILLAGE CENTRE", y$addressfinal)
  y$addressfinal <- gsub("[/]", "", y$addressfinal)
  y$addressfinal <- gsub("[.]", "", y$addressfinal)
  y$addressfinal <- gsub("[']", "", y$addressfinal)
  y$addressfinal <- gsub(",", "", y$addressfinal)
  y$addressfinal <- gsub(" ", "", y$addressfinal)
  y$addressf <- paste(y$postcode,y$addressfinal,sep=",")
  
  taba1 <- inner_join(x,y,by="addressf")
  
  return(taba1 ) 
  
 
  
}



link433<-function433(add,epc)
dim(link433)
# 
link433<-link433[,..needlist1]

link433u<- uniqueresult(link433)
dim(link433u)
#

link433d <- doubleresult(link433)
dim(link433d)


dim(epc)
#
epc <- matchleft(epc,link433)
dim(epc)
# 2437643      43


#################### method 434  ##################

function434<- function(x,y){
  
 # x$bnstreet <- paste(x$saotext,x$paotext,sep=" ")
 #  x$bnstreet <- paste(x$bnstreet,x$streetdescription,sep=" ")
 #  x$bnstreet <- paste(x$bnstreet,x$townname,sep=" ")
 # 
 #  x$bnstreet <- gsub(",", "", x$bnstreet)
 #  x$bnstreet <- gsub("/", "", x$bnstreet)
 #  x$bnstreet <- gsub("[.]", "", x$bnstreet)
 #  x$bnstreet <- gsub("[']", "", x$bnstreet)
 #  x$bnstreet <- gsub(" ", "", x$bnstreet)
 #  x$addressf <-paste(x$postcodelocator,x$bnstreet,sep=",")
 # 
 #  #y<-y[y$property_type=="Flat"|y$property_type=="Maisonette",]
 #  y$addressfinal <- paste("APARTMENT",y$add,sep="")
 # 
 # 
 #  y$addressfinal <- gsub("[/]", "", y$addressfinal)
 #  y$addressfinal <- gsub("[.]", "", y$addressfinal)
 #  y$addressfinal <- gsub("[']", "", y$addressfinal)
 #  y$addressfinal <- gsub(",", "", y$addressfinal)
 #  y$addressfinal <- gsub(" ", "", y$addressfinal)
 #  y$addressf <- paste(y$postcode,y$addressfinal,sep=",")
 # 
 #  taba1 <- inner_join(x,y,by="addressf")
 # 
 #  return(taba1)
  x<-x[x$buildingname!="",]
  x$bnstreet1<-word(x$buildingname,1,2)
  x$bnstreet2<-word(x$buildingname,-1)
  
  x$bnstreet <- paste(x$bnstreet2,x$subbuildingname,sep="")
  x$bnstreet <- paste(x$bnstreet,x$bnstreet1,sep="")
  x$bnstreet <- paste(x$bnstreet,x$pp,sep=",")
  x$bnstreet <- paste(x$bnstreet,x$streetdescription,sep=",")
  
  x$bnstreet <- gsub(" ", "", x$bnstreet)
  x$addressf <-paste(x$postcodelocator,x$bnstreet,sep=",")
  
  
  #y$addressfinal <-y$add
  y$addressfinal <- trimws(y$add)
  y$addressfinal <- gsub(" ", "", y$addressfinal)
  y$addressf <- paste(y$postcode,y$addressfinal,sep=",")
  
  taba1 <- inner_join(x,y,by="addressf")
  
  return(taba1)
}


link434<-function434(add,epc)



link434<-link434[,..needlist1]


link434u<- uniqueresult(link434)
dim(link434u)


link434d <- doubleresult(link434)

dim(epc)
# 
epc<- matchleft(epc,link434)
dim(epc)
#   777220     28
#################### method 435 ##################


function435<- function(x,y){
  
  
 x$bnstreet <- paste(x$saotext,x$paotext,sep=",")
  x$bnstreet <- paste(x$bnstreet,x$pp,sep=",")
  x$bnstreet <- paste(x$bnstreet,x$streetdescription,sep=" ")

  x$bnstreet <- gsub(",", "", x$bnstreet)
  x$bnstreet <- gsub("/", "", x$bnstreet)
  x$bnstreet <- gsub("[.]", "", x$bnstreet)
  x$bnstreet <- gsub("[']", "", x$bnstreet)
  x$bnstreet <- gsub(" ", "", x$bnstreet)
  x$addressf <-paste(x$postcodelocator,x$bnstreet,sep=",")
  
  #y<-y[y$property_type=="Flat"|y$property_type=="Maisonette",]
  y$addressfinal <- y$add
  y$addressfinal <- gsub("FLAT", "APARTMENT", y$addressfinal)
  
  y$addressfinal <- gsub("[/]", "", y$addressfinal)
  y$addressfinal <- gsub("[.]", "", y$addressfinal)
  y$addressfinal <- gsub("[']", "", y$addressfinal)
  y$addressfinal <- gsub(",", "", y$addressfinal)
  y$addressfinal <- gsub(" ", "", y$addressfinal)
  y$addressf <- paste(y$postcode,y$addressfinal,sep=",")
  
  taba1 <- inner_join(x,y,by="addressf")
  
  return(taba1)
}

link435<-function435(add,epc)


link435<-link435[,..needlist1]


link435u<- uniqueresult(link435)
dim(link435u)


link435d <- doubleresult(link435)
dim(link435d)


dim(epc)
#
epc <- matchleft(epc,link435)
dim(epc)
#2430434      43

#
#################### method 436 ##################

function436<- function(x,y){
  x$bnstreet <- paste(x$subbuildingname,x$buildingnumber,sep=",")
  
  x$bnstreet <- paste(x$bnstreet,x$streetdescription,sep=",")
  
  x$bnstreet <- gsub(",", "", x$bnstreet)
  x$bnstreet <- gsub("/", "", x$bnstreet)
  x$bnstreet <- gsub("[.]", "", x$bnstreet)
  x$bnstreet <- gsub("[']", "", x$bnstreet)
  x$bnstreet <- gsub(" ", "", x$bnstreet)
  x$addressf <-paste(x$postcodelocator,x$bnstreet,sep=",")
  
  #y<-y[y$property_type=="Flat"|y$property_type=="Maisonette",]
  y$addressfinal <- word(y$add1,1,2)
  # y$addressfinal1 <- word(y$add2,1)
  
  y$addressfinal <-  paste(y$addressfinal,y$add2,sep=",")
  #y$addressfinal <- gsub("FLAT", "APARTMENT", y$addressfinal)
  #y$addressfinal <- gsub("VILLAGE CENTRE", "THE VILLAGE CENTRE", y$addressfinal)
  y$addressfinal <- gsub("[/]", "", y$addressfinal)
  y$addressfinal <- gsub("[.]", "", y$addressfinal)
  y$addressfinal <- gsub("[']", "", y$addressfinal)
  y$addressfinal <- gsub(",", "", y$addressfinal)
  y$addressfinal <- gsub(" ", "", y$addressfinal)
  y$addressf <- paste(y$postcode,y$addressfinal,sep=",")
  
  taba1 <- inner_join(x,y,by="addressf")
  
  return(taba1)

}




link436<-function436(add,epc)
dim(link436)
# 

link436<-link436[,..needlist1]



link436u<- uniqueresult(link436)
dim(link436u)
# 

link436d <- doubleresult(link436)
dim(link436d)


epc <- matchleft(epc,link436)
dim(epc)
# 2430431      43

#################### method 437 ##################



function437<- function(x,y){
   
  x$bnstreet <- paste(x$saotext,x$paotext,sep=",")
  x$bnstreet <- paste(x$bnstreet,x$pp,sep=",")
  #x$bnstreet <- paste(x$bnstreet,x$streetdescription,sep=",")
  
  x$bnstreet <- paste(x$bnstreet,x$streetdescription,sep=" ")
  
  x$bnstreet <- gsub(",", "", x$bnstreet)
  x$bnstreet <- gsub("/", "", x$bnstreet)
  x$bnstreet <- gsub("[.]", "", x$bnstreet)
  x$bnstreet <- gsub("[']", "", x$bnstreet)
  x$bnstreet <- gsub(" ", "", x$bnstreet)
  x$addressf <-paste(x$postcodelocator,x$bnstreet,sep=",")
  
  y<-y[y$property_type=="Flat"|y$property_type=="Maisonette",]
  #y$addressfinal <- word(y$add1,1,2)
  # y$addressfinal1 <- word(y$add2,1)
  
  y$addressfinal <-  paste("FLAT",y$add,sep=" ")
  
  y$addressfinal <- gsub("[/]", "", y$addressfinal)
  y$addressfinal <- gsub("[.]", "", y$addressfinal)
  y$addressfinal <- gsub("[']", "", y$addressfinal)
  y$addressfinal <- gsub(",", "", y$addressfinal)
  y$addressfinal <- gsub(" ", "", y$addressfinal)
  y$addressf <- paste(y$postcode,y$addressfinal,sep=",")
  
  taba1 <- inner_join(x,y,by="addressf")
  
  return(taba1)
}

link437<-function437(add,epc)
dim(link437)


link437<-link437[,..needlist1]

link437u<- uniqueresult(link437)
dim(link437u)


link437d <- doubleresult(link437)
dim(link437d)


dim(epc)
# 
epc <- matchleft(epc,link437)
dim(epc)
#2430190      43
#
#################### method 438 ##################

function438<- function(x,y){
 
  x$bnstreet <-    paste(x$saotext,x$pp,sep=",")
  x$bnstreet <-    paste(x$bnstreet,x$streetdescription,sep=",")
  #x$bnstreet <-    paste(x$buildingnumber,x$streetdescription,sep=",")
  
  x$bnstreet <- gsub("/", "", x$bnstreet)
  x$bnstreet <- gsub("[.]", "", x$bnstreet)
  x$bnstreet <- gsub("[']", "", x$bnstreet)
  x$bnstreet <- gsub(" ", "", x$bnstreet)
  x$addressf <-paste(x$postcodelocator,x$bnstreet,sep=",")
  
  y$addressfinal <- trimws(y$add)
  y$addressfinal <- gsub("FLAT", "ROOM", y$addressfinal)
  y$addressfinal <- gsub("[/]", "", y$addressfinal)
  y$addressfinal <- gsub("[.]", "", y$addressfinal)
  y$addressfinal <- gsub("[']", "", y$addressfinal)
  y$addressfinal <- gsub(" ", "", y$addressfinal)
  y$addressf <- paste(y$postcode,y$addressfinal,sep=",")
  # setkey(x,addressf)
  # setkey(y,addressf)
  # taba1 <-x[y,nomatch=0]
  taba1 <- inner_join(x,y,by="addressf")
  
  
  return(taba1)
}
  




link438<-function438(add,epc)
dim(link438)


link438<-link438[,..needlist1]



link438u<- uniqueresult(link438)
dim(link438u)


link438d <- doubleresult(link438)
dim(link438d)


dim(epc)
# 
epc <- matchleft(epc,link438)
dim(epc)
#

getwd()

#################### method 439 ##################
function439<- function(x,y){
   x$bnstreet <-    paste(x$saotext,x$pp,sep=",")
  x$bnstreet <-    paste(x$bnstreet,x$streetdescription,sep=" ")
  #x$bnstreet <-    paste(x$buildingnumber,x$streetdescription,sep=",")
  
  x$bnstreet <- gsub("/", "", x$bnstreet)
  x$bnstreet <- gsub("[.]", "", x$bnstreet)
  x$bnstreet <- gsub("[']", "", x$bnstreet)
  x$bnstreet <- gsub(" ", "", x$bnstreet)
  x$addressf <-paste(x$postcodelocator,x$bnstreet,sep=",")
  
  #
  y$addressfinal <- trimws(y$add)
  y[postcode=="CV1 1GU",addressfinal:=gsub("CORPOATION STREET", "CORPORATION STREET",addressfinal)]
  
  y$addressfinal <- gsub("FLAT", "ROOM", y$addressfinal)
  y$addressfinal <- gsub("[/]", "", y$addressfinal)
  y$addressfinal <- gsub("[.]", "", y$addressfinal)
  y$addressfinal <- gsub("[']", "", y$addressfinal)
  y$addressfinal <- gsub(" ", "", y$addressfinal)
  y$addressf <- paste(y$postcode,y$addressfinal,sep=",")
  taba1 <- inner_join(x,y,by="addressf")
  
  
  return(taba1)
 
}
link439<-function439(add,epc)

link439<-link439[,..needlist1]


link439u<- uniqueresult(link439)
dim(link439u)


link439d <- doubleresult(link439)
dim(link439d)

dim(epc)

epc <- matchleft(epc,link439)
dim(epc)
# 

#################### method 440 ##################
function440<- function(x,y){
  
  
   x$bnstreet <- paste(x$saotext,x$paotext,sep=",")
  x$bnstreet <- paste(x$bnstreet,x$pp,sep=",")
  #x$bnstreet <- paste(x$bnstreet,x$streetdescription,sep=",")
  
  x$bnstreet <- paste(x$bnstreet,x$streetdescription,sep=" ")
  
  x$bnstreet <- gsub(",", "", x$bnstreet)
  x$bnstreet <- gsub("/", "", x$bnstreet)
  x$bnstreet <- gsub("[.]", "", x$bnstreet)
  x$bnstreet <- gsub("[']", "", x$bnstreet)
  x$bnstreet <- gsub(" ", "", x$bnstreet)
  x$addressf <-paste(x$postcodelocator,x$bnstreet,sep=",")
  
  y<-y[y$property_type=="Flat"|y$property_type=="Maisonette",]
  #y$addressfinal <- word(y$add1,1,2)
  # y$addressfinal1 <- word(y$add2,1)
  
  y$addressfinal <-  y$add
  #y$addressfinal <- gsub("[/]", "", y$addressfinal)
  y$addressfinal <- gsub( "STUDIO", "STUDIO FLAT",y$addressfinal)
  y$addressfinal <- gsub( "CLUSTER","CLUSTER FLAT", y$addressfinal)
  
  y$addressfinal <- gsub("[/]", "", y$addressfinal)
  y$addressfinal <- gsub("[.]", "", y$addressfinal)
  y$addressfinal <- gsub("[']", "", y$addressfinal)
  y$addressfinal <- gsub(",", "", y$addressfinal)
  y$addressfinal <- gsub(" ", "", y$addressfinal)
  y$addressf <- paste(y$postcode,y$addressfinal,sep=",")
  
  taba1 <- inner_join(x,y,by="addressf")
  
  return(taba1)
}

link440<-function440(add,epc)

length(unique(link440$lmk_key))

link440<-link440[,..needlist1]

link440u<- uniqueresult(link440)

dim(link440u)


link440d <- doubleresult(link440)
dim(link440d)

dim(epc)
#
epc <- matchleft(epc,link440)
dim(epc)
#777446     28

#################### method 441 ##################

function441<- function(x,y){
  
   x$bnstreet <- paste(x$saotext,x$paostartnumber,sep=",")
  x$bnstreet <- paste(x$bnstreet,x$paoendnumber,sep="-")
  
  
  x$bnstreet <- paste(x$bnstreet,x$streetdescription,sep=" ")
  
  #x$bnstreet <- gsub(",", "", x$bnstreet)
  x$bnstreet <- gsub("/", "", x$bnstreet)
  x$bnstreet <- gsub("[.]", "", x$bnstreet)
  x$bnstreet <- gsub("[']", "", x$bnstreet)
  x$bnstreet <- gsub(" ", "", x$bnstreet)
  x$addressf <-paste(x$postcodelocator,x$bnstreet,sep=",")
  
  y<-y[y$property_type=="Flat"|y$property_type=="Maisonette",]
  
  #y<-y[ !grepl("\\d",y$add2) ,]
  y$addressfinal <- y$add
  y$addressfinal <- gsub("FLAT", "ROOM", y$addressfinal)
  y$addressfinal <- gsub("[/]", "", y$addressfinal)
  y$addressfinal <- gsub("[.]", "", y$addressfinal)
  y$addressfinal <- gsub("[']", "", y$addressfinal)
  #y$addressfinal <- gsub(",", "", y$addressfinal)
  y$addressfinal <- gsub(" ", "", y$addressfinal)
  y$addressf <- paste(y$postcode,y$addressfinal,sep=",")
  
  taba1 <- inner_join(x,y,by="addressf")
  
  return(taba1)
}


link441<-function441(add,epc)


link441<-link441[,..needlist1]


link441u<- uniqueresult(link441)
dim(link441u)


link441d <- doubleresult(link441)
dim(link441d)

dim(epc)
#
epc <- matchleft(epc,link441)
dim(epc)
#2448669      44


#################### method 442 ##################


function442<- function(x,y){
  
  
 x$bnstreet <- paste(x$saotext,x$paostartnumber,sep=",")
  x$bnstreet <- paste(x$bnstreet,x$paoendnumber,sep="-")
  
  
  x$bnstreet <- paste(x$bnstreet,x$streetdescription,sep=" ")
  
  #x$bnstreet <- gsub(",", "", x$bnstreet)
  x$bnstreet <- gsub("/", "", x$bnstreet)
  x$bnstreet <- gsub("[.]", "", x$bnstreet)
  x$bnstreet <- gsub("[']", "", x$bnstreet)
  x$bnstreet <- gsub(" ", "", x$bnstreet)
  x$addressf <-paste(x$postcodelocator,x$bnstreet,sep=",")
  
  y<-y[y$property_type=="Flat"|y$property_type=="Maisonette",]
  
 
  y$addressfinal <- y$add
  y$addressfinal <- gsub("APARTMENT", "FLAT", y$addressfinal)
  y$addressfinal <- gsub("[/]", "", y$addressfinal)
  y$addressfinal <- gsub("[.]", "", y$addressfinal)
  y$addressfinal <- gsub("[']", "", y$addressfinal)
  #y$addressfinal <- gsub(",", "", y$addressfinal)
  y$addressfinal <- gsub(" ", "", y$addressfinal)
  y$addressf <- paste(y$postcode,y$addressfinal,sep=",")
  
  taba1 <- inner_join(x,y,by="addressf")
  
  return(taba1)
}
link442<-function442(add,epc)

link442<-link442[,..needlist1]


link442u<- uniqueresult(link442)
dim(link442u)


link442d <- doubleresult(link442)
dim(link442d)


dim(epc)
#
epc <- matchleft(epc,link442)
dim(epc)
# 2447648      44



#################### method 443 ##################

function443<- function(x,y){
  
  x$bnstreet <- paste(x$saotext,x$streetdescription,sep=",")
  
  
  x$bnstreet <- gsub("/", "", x$bnstreet)
  x$bnstreet <- gsub("[.]", "", x$bnstreet)
  x$bnstreet <- gsub("[']", "", x$bnstreet)
  x$bnstreet <- gsub(" ", "", x$bnstreet)
  x$addressf <-paste(x$postcodelocator,x$bnstreet,sep=",")
  
  
  y<-y[y$property_type=="Flat"|y$property_type=="Maisonette",]
  
  
  y$addressfinal <- paste( "APARTMENT", y$add1, sep=" ")
  y$addressfinal <- gsub(",", "", y$addressfinal)
  y$addressfinal <- paste( y$addressfinal, y$add2, sep=",")
  
  y$addressfinal <- gsub("[/]", "", y$addressfinal)
  y$addressfinal <- gsub("[.]", "", y$addressfinal)
  y$addressfinal <- gsub("[']", "", y$addressfinal)
  #y$addressfinal <- gsub(",", "", y$addressfinal)
  y$addressfinal <- gsub(" ", "", y$addressfinal)
  y$addressf <- paste(y$postcode,y$addressfinal,sep=",")
  
  taba1 <- inner_join(x,y,by="addressf")
  
  return(taba1)
  
}



link443<-function443(add,epc)
dim(link443)
# 
link443<-link443[,..needlist1]

link443u<- uniqueresult(link443)
dim(link443u)
#

link443d <- doubleresult(link443)
dim(link443d)


dim(epc)
#
epc <- matchleft(epc,link443)
dim(epc)
# 2447644      44


#################### method 444  ##################

function444<- function(x,y){
  x$bnstreet <- paste("FLAT",x$paostartsuffix,sep=" ")
  x$bnstreet <- paste(x$bnstreet,x$paostartnumber,sep=" ")
  
  
  x$bnstreet <- paste(x$bnstreet,x$streetdescription,sep=",")
  
  #x$bnstreet <- gsub(",", "", x$bnstreet)
  x$bnstreet <- gsub("/", "", x$bnstreet)
  x$bnstreet <- gsub("[.]", "", x$bnstreet)
  x$bnstreet <- gsub("[']", "", x$bnstreet)
  x$bnstreet <- gsub(" ", "", x$bnstreet)
  x$addressf <-paste(x$postcodelocator,x$bnstreet,sep=",")
  
  y<-y[y$property_type=="Flat"|y$property_type=="Maisonette",]
  
  y<-y[ !grepl("\\d",y$add2) ,]
  y$addressfinal <- paste(y$add1,y$add3,sep=",")
  
  y$addressfinal <- gsub("[/]", "", y$addressfinal)
  y$addressfinal <- gsub("[.]", "", y$addressfinal)
  y$addressfinal <- gsub("[']", "", y$addressfinal)
  #y$addressfinal <- gsub(",", "", y$addressfinal)
  y$addressfinal <- gsub(" ", "", y$addressfinal)
  y$addressf <- paste(y$postcode,y$addressfinal,sep=",")
  
  taba1 <- inner_join(x,y,by="addressf")
  
  return(taba1)
  
}


link444<-function444(add,epc)



link444<-link444[,..needlist1]


link444u<- uniqueresult(link444)
dim(link444u)


link444d <- doubleresult(link444)

dim(epc)
# 
epc<- matchleft(epc,link444)
dim(epc)
#   697062     28
#################### method 445  ##################


function445<- function(x,y){
  
  x<-x[x$paotext!=""]
  x$bnstreet1 <- word(x$paotext,-1)
  x$bnstreet2 <- word(x$paotext,1,2)
  
  x$bnstreet <- paste(x$bnstreet1,x$townname,sep=",")
  #x$bnstreet <- gsub(",", "", x$bnstreet)
  x$bnstreet <- gsub("/", "", x$bnstreet)
  x$bnstreet <- gsub("[.]", "", x$bnstreet)
  x$bnstreet <- gsub("[']", "", x$bnstreet)
  x$bnstreet <- gsub(" ", "", x$bnstreet)
  x$addressf <-paste(x$postcodelocator,x$bnstreet,sep=",")
  
  #y<-y[y$property_type=="Flat"|y$property_type=="Maisonette",]
  
  #y<-y[ !grepl("\\d",y$add2) ,]
  y$addressfinal <- paste(y$add1,y$add3,sep=",")
  
  y$addressfinal <- gsub("[/]", "", y$addressfinal)
  y$addressfinal <- gsub("[.]", "", y$addressfinal)
  y$addressfinal <- gsub("[']", "", y$addressfinal)
  #y$addressfinal <- gsub(",", "", y$addressfinal)
  y$addressfinal <- gsub(" ", "", y$addressfinal)
  y$addressf <- paste(y$postcode,y$addressfinal,sep=",")
  
  taba1 <- inner_join(x,y,by="addressf")
  
  return(taba1)
  
}


link445<-function445(add,epc)



link445<-link445[,..needlist1]


link445u<- uniqueresult(link445)
dim(link445u)


link445d <- doubleresult(link445)

dim(epc)
epc<- matchleft(epc,link445)
dim(epc)
#################### method 446  ##################

function446<- function(x,y){
  x<-x[x$paotext!=""]
  x$bnstreet1 <- word(x$paotext,-1)
  x$bnstreet2 <- word(x$paotext,1,2)
  
  
  x$bnstreet <- paste(x$bnstreet1,x$bnstreet2,sep=" ")
  x$bnstreet <- paste(x$bnstreet,x$townname,sep=",")
  #x$bnstreet <- gsub(",", "", x$bnstreet)
  x$bnstreet <- gsub("/", "", x$bnstreet)
  x$bnstreet <- gsub("[.]", "", x$bnstreet)
  x$bnstreet <- gsub("[']", "", x$bnstreet)
  x$bnstreet <- gsub(" ", "", x$bnstreet)
  x$addressf <-paste(x$postcodelocator,x$bnstreet,sep=",")
  
  #y<-y[y$property_type=="Flat"|y$property_type=="Maisonette",]
  
  #y<-y[ !grepl("\\d",y$add2) ,]
  y$addressfinal <- paste(y$add1,y$add3,sep=",")
  
  y$addressfinal <- gsub("[/]", "", y$addressfinal)
  y$addressfinal <- gsub("[.]", "", y$addressfinal)
  y$addressfinal <- gsub("[']", "", y$addressfinal)
  #y$addressfinal <- gsub(",", "", y$addressfinal)
  y$addressfinal <- gsub(" ", "", y$addressfinal)
  y$addressf <- paste(y$postcode,y$addressfinal,sep=",")
  
  taba1 <- inner_join(x,y,by="addressf")
  
  return(taba1)
  
}


link446<-function446(add,epc)



link446<-link446[,..needlist1]


link446u<- uniqueresult(link446)
dim(link446u)


link446d <- doubleresult(link446)

dim(epc)
# 
epc<- matchleft(epc,link446)
dim(epc)
#  695376     2
####sum up#####
sta5<-epc[,.(count=.N),by="postcode"]
sta5<-sta5[order(-count)]
pc<-sta5[1,1]
pc<-sta5[12,1]
pc<-sta5[3,1]
pc<-sta5[4,1]
pc<-sta5[55,1]
pc<-sta5[36,1]
pc<-sta5[7,1]
pc<-sta5[8,1]
pc<-sta5[9,1]
c1<-epc[postcode==pc,]
c2<-add[postcodelocator==pc,]

# #
# ####################### end#########################################################

link401u$method<-"link401u"
link402u$method<-"link402u"
link403u$method<-"link403u"
link404u$method<-"link404u"
link405u$method<-"link405u"
link406u$method<-"link406u"
link407u$method<-"link407u"
link408u$method<-"link408u"
link409u$method<-"link409u"
link410u$method<-"link410u"
link411u$method<-"link411u"
link412u$method<-"link412u"
link413u$method<-"link413u"
link414u$method<-"link414u"
link415u$method<-"link415u"
link416u$method<-"link416u"
link417u$method<-"link417u"
link418u$method<-"link418u"
link419u$method<-"link419u"
link420u$method<-"link420u"
link421u$method<-"link421u"
link422u$method<-"link422u"
link423u$method<-"link423u"
link424u$method<-"link424u"
link425u$method<-"link425u"
link426u$method<-"link426u"
link427u$method<-"link427u"
link428u$method<-"link428u"
link429u$method<-"link429u"
link430u$method<-"link430u"
link431u$method<-"link431u"
link432u$method<-"link432u"
link433u$method<-"link433u"
link434u$method<-"link434u"
link435u$method<-"link435u"
link436u$method<-"link436u"
link437u$method<-"link437u"
link438u$method<-"link438u"
link439u$method<-"link439u"
link440u$method<-"link440u"
link441u$method<-"link441u"
link442u$method<-"link442u"
link443u$method<-"link443u"
link444u$method<-"link444u"
link445u$method<-"link445u"
link446u$method<-"link446u"


link401d$method<-"link401d"
link402d$method<-"link402d"
link403d$method<-"link403d"
link404d$method<-"link404d"
link405d$method<-"link405d"
link406d$method<-"link406d"
link407d$method<-"link407d"
link408d$method<-"link408d"
link409d$method<-"link409d"
link410d$method<-"link410d"

link411d$method<-"link411d"
link412d$method<-"link412d"
link413d$method<-"link413d"
link414d$method<-"link414d"
link415d$method<-"link415d"
link416d$method<-"link416d"
link417d$method<-"link417d"
link418d$method<-"link418d"
link419d$method<-"link419d"
link420d$method<-"link420d"
link421d$method<-"link421d"
link422d$method<-"link422d"
link423d$method<-"link423d"
link424d$method<-"link424d"
link425d$method<-"link425d"
link426d$method<-"link426d"
link427d$method<-"link427d"
link428d$method<-"link428d"
link429d$method<-"link429d"
link430d$method<-"link430d"
link431d$method<-"link431d"
link432d$method<-"link432d"
link433d$method<-"link433d"
link434d$method<-"link434d"
link435d$method<-"link435d"
link436d$method<-"link436d"
link437d$method<-"link437d"
link438d$method<-"link438d"
link439d$method<-"link439d"
link440d$method<-"link440d"
link441d$method<-"link441d"
link442d$method<-"link442d"
link443d$method<-"link443d"
link444d$method<-"link444d"
link445d$method<-"link445d"
link446d$method<-"link446d"





dim(link401u)[1]+dim(link402u)[1]+dim(link403u)[1]+dim(link404u)[1]+dim(link405u)[1]+dim(link406u)[1]+dim(link407u)[1]+dim(link408u)[1]+dim(link409u)[1]+
dim(link410u)[1]+dim(link411u)[1]+dim(link412u)[1]+dim(link413u)[1]+dim(link414u)[1]+dim(link415u)[1]+dim(link416u)[1]+dim(link417u)[1]+dim(link418u)[1]+dim(link419u)[1]+
  dim(link420u)[1]+dim(link421u)[1]+dim(link422u)[1]+dim(link423u)[1]+dim(link424u)[1]+dim(link425u)[1]+dim(link426u)[1]+dim(link427u)[1]+dim(link428u)[1]+dim(link429u)[1]+
  dim(link430u)[1]+dim(link431u)[1]+dim(link432u)[1]+dim(link433u)[1]+dim(link434u)[1]+dim(link435u)[1]+dim(link436u)[1]+dim(link437u)[1]+dim(link438u)[1]+dim(link439u)[1]+
  dim(link440u)[1]+dim(link441u)[1]+dim(link442u)[1]+dim(link443u)[1]+dim(link444u)[1]+dim(link445u)[1]+dim(link446u)[1]
l401_446u = list(link401u,link402u,link403u,link404u,link405u,link406u,link407u,link408u,link409u,
                 link410u,link411u,link412u,link413u,link414u,link415u,link416u,link417u,link418u,link419u,
                 link420u,link421u,link422u,link423u,link424u,link425u,link426u,link427u,link428u,link429u,
                 link430u,link431u,link432u,link433u,link434u,link435u,link436u,link437u,link438u,link439u,
                 link440u,link441u,link442u,link443u,link444u,link445u,link446u)

#



link401_446u <- rbindlist(l401_446u , use.names=TRUE, fill=TRUE)
dim(link401_446u)
#33928 

length(unique(link401_446u$method))
#46
length(unique(link401_446u$lmk_key))
#33928 
dim(link401d)[1]+dim(link402d)[1]+dim(link403d)[1]+dim(link404d)[1]+dim(link405d)[1]+dim(link406d)[1]+dim(link407d)[1]+dim(link408d)[1]+dim(link409d)[1]+
  dim(link410d)[1]+dim(link411d)[1]+dim(link412d)[1]+dim(link413d)[1]+dim(link414d)[1]+dim(link415d)[1]+dim(link416d)[1]+dim(link417d)[1]+dim(link418d)[1]+dim(link419d)[1]+
  dim(link420d)[1]+dim(link421d)[1]+dim(link422d)[1]+dim(link423d)[1]+dim(link424d)[1]+dim(link425d)[1]+dim(link426d)[1]+dim(link427d)[1]+dim(link428d)[1]+dim(link429d)[1]+
  dim(link430d)[1]+dim(link431d)[1]+dim(link432d)[1]+dim(link433d)[1]+dim(link434d)[1]+dim(link435d)[1]+dim(link436d)[1]+dim(link437d)[1]+dim(link438d)[1]+dim(link439d)[1]+
  dim(link440d)[1]+dim(link441d)[1]+dim(link442d)[1]+dim(link443d)[1]+dim(link444d)[1]+dim(link445d)[1]+dim(link446d)[1]
l401_446d = list(link401d,link402d,link403d,link404d,link405d,link406d,link407d,link408d,link409d,
                 link410d,link411d,link412d,link413d,link414d,link415d,link416d,link417d,link418d,link419d,
                 link420d,link421d,link422d,link423d,link424d,link425d,link426d,link427d,link428d,link429d,
                 link430d,link431d,link432d,link433d,link434d,link435d,link436d,link437d,link438d,link439d,
                 link440d,link441d,link442d,link443d,link444d,link445d,link446d)

#10462



link401_446d <- rbindlist(l401_446d , use.names=TRUE, fill=TRUE)
dim(link401_446d)
#10462


dbWriteTable(con, "link401_446dnew",value =link401_446d, append = TRUE, row.names = FALSE)
dbWriteTable(con, "link401_446unew",value =link401_446u, append = TRUE, row.names = FALSE)

rm(l401_446d,l401_446u)

rm(link401d,link402d,link403d,link404d,link405d,link406d,link407d,link408d,link409d,
   link410d,link411d,link412d,link413d,link414d,link415d,link416d,link417d,link418d,link419d,
   link420d,link421d,link422d,link423d,link424d,link425d,link426d,link427d,link428d,link429d,
   link430d,link431d,link432d,link433d,link434d,link435d,link436d,link437d,link438d,link439d,
   link440d,link441d,link442d,link443d,link444d,link445d,link446d)


rm(link401u,link402u,link403u,link404u,link405u,link406u,link407u,link408u,link409u,
   link410u,link411u,link412u,link413u,link414u,link415u,link416u,link417u,link418u,link419u,
   link420u,link421u,link422u,link423u,link424u,link425u,link426u,link427u,link428u,link429u,
   link430u,link431u,link432u,link433u,link434u,link435u,link436u,link437u,link438u,link439u,
   link440u,link441u,link442u,link443u,link444u,link445u,link446u)

rm(link401,link402,link403,link404,link405,link406,link407,link408,link409,
   link410,link411,link412,link413,link414,link415,link416,link417,link418,link419,
   link420,link421,link422,link423,link424,link425,link426,link427,link428,link429,
   link430,link431,link432,link433,link434,link435,link436,link437,link438,link439,
   link440,link441,link442,link443,link444,link445,link446)

   
rm(function401,function402,function403,function404,function405,function406,function407,function408,function409,
   function410,function411,function412,function413,function414,function415,function416,function417,function418,function419,
   function420,function421,function422,function423,function424,function425,function426,function427,function428,function429,
   function430,function431,function432,function433,function434,function435,function436,function437,function438,function439,
   function440,function441,function442,function443,function444,function445,function446)  
   
   
#ldouble = list(linkd1,link10_49d,link50_69d,link70_89d,link90_109d,link110_130d,link131_155d)
#lunique = list(link1_9u,link10_49u,link50_69u,link70_89u,link90_109u,link110_130u,link131_155u)
epc1<-matchleft(epc1,link401_446d)
epc1<-matchleft(epc1,link401_446u)
dim(epc1)
#695376     27

# class(link360_400u)
# head(link360_400u)
# length(unique(link360_400u$method))
# length(unique(link360_400u$methou))
# link360_400u[methou=="link390u"|methou=="link391u"|methou=="link392u"|methou=="link393u"|methou=="link394u"|methou=="link395u"|methou=="link396u"|methou=="link397u"|methou=="link398u"| methou=="link399u", method:= methou]
# 
# link360_400u[, methou:=NULL]
###################################to prepare the cleaning process#####################################
length(unique(linku1$method))

length(unique(link300_359u$method))
length(unique(link360_400u$method))
dim(epc)[1]/dim(epcdata)[1]
#0.03181378
linku2<-rbindlist(list(linku1,link300_359u,link360_400u,link401_446u),use.names=TRUE, fill=TRUE)
dim(linku2)
# 21046468       34
length(unique(linku2$lmk_key))

#21046468
#length(unique(linku1$method))
length(unique(linku2$method))
# 446
length(unique(linku2$lmk_key))/dim(epcdata)[1]
#0.9628858
linkd2<-rbindlist(list(linkd1,link300_359d,link360_400d,link401_446d),use.names=TRUE, fill=TRUE)
dim(linkd2)
# 473354     34
length(unique(linkd2$lmk_key))
#115855
length(unique(linkd2$lmk_key))/dim(epcdata)[1]
# 0.005300421
(length(unique(linkd2$lmk_key))+length(unique(linku2$lmk_key)))/dim(epcdata)[1]
#0.9681862

length(unique(linkdcopy$lmk_key))+length(unique(linku2$lmk_key))/dim(epcdata)[1]


dbWriteTable(con, "linku2",value =linku2, append = TRUE, row.names = FALSE)
dbWriteTable(con, "linkd2",value =linkd2, append = TRUE, row.names = FALSE)
rm(linkd1,link300_359d,link360_400d,link401_446d,linku1,link300_359u,link360_400u,link401_446u)


dbWriteTable(con, "epc_left",value =epc, append = TRUE, row.names = FALSE)
rm(epc1)
######################################clean it ############################################################

getwd()

setwd("D:/epc_os/results")
sta5<-epc[,.(count=.N),by="postcode"]
sta5<-sta5[order(-count)]
dim(sta5)
# 205947      2
head(epcdata)
sta6<-epcdata[,.(tcount=.N),by="postcode"]
#sta6<-sta6[order(-count)]

sta7<-merge(sta5,sta6,by="postcode")
#
dim(sta7)
#205835      3

sta7$pro<-sta7$count/sta7$tcount

fwrite(sta7,"D:/epc_os/results/sta7.csv")


####save one example for each method
head(linku2)
setkey(linku2,method)
sample<-linku2[,.SD[1],by = key(linku2)]

#needlist2<-c("method","lmk_key","uprn","postcode.y","property_type","add1","add2","add3","add","postcode.x","postcodelocator","buildingname","buildingnumber","subbuildingname","paostartnumber","paostartsuffix","paoendnumber","paoendsuffix","paotext","saostartnumber","saostartsuffix","saoendnumber","saoendsuffix","saotext","streetdescription","dependentlocality","townname","class","lodgement_date","inspection_date","lodgement_datetime")
#sample<-sample[,..needlist2]
getwd()
fwrite(sample,"D:/epc_os/results/linkage_sample.csv",row.names= F)

rm(epc_0,c1)
rm(add)
############################ clean up ####################################################

sta_matchrateu<-linku2[,.(fcount=.N),by="method"]
sta_matchrated<-linkd2[,.(fcount1=.N),by="method"]

setDT(sta_matchrateu)
setDT(sta_matchrated)

sta_matchrated[,methodid :=substring(method,0,nchar(method)-1)]
sta_matchrateu[,methodid :=substring(method,0,nchar(method)-1)]
head(sta_matchrated)
head(sta_matchrateu)
sta_matchrate<-merge(sta_matchrateu,sta_matchrated,by="methodid",all=T)
sta_matchrate$prou<-round(sta_matchrate$fcount/dim(epcdata)[1],4)
sta_matchrate$prod<-round(sta_matchrate$fcount1/dim(epcdata)[1],4)
head(sta_matchrate)

sta_matchrate[is.na(sta_matchrate)] <- 0
class(sta_matchrate)
sta_matchrate[,id :=substring(methodid,5,nchar(methodid))]



fwrite(sta_matchrate,"D:/epc_os/results/sta_matchrate_original.csv")
############################################################################
# summay up the linkage match rate

head(sta_matchrateu)
head(sta_matchrated) 
setDT(sta_matchrateu)
setDT(sta_matchrated)

sta_matchrate<-merge(sta_matchrateu,sta_matchrated,by="methodid",all=T)
sta_matchrate$prou<-round(sta_matchrate$fcount/dim(epcdata)[1],4)
sta_matchrate$prod<-round(sta_matchrate$fcount1/dim(epcdata)[1],4)
sta_matchrate[is.na(sta_matchrate)] <- 0
fwrite(sta_matchrate,"D:/epc_os/results/sta_matchrate_original_pro.csv")
########################################################### The end##############################################################################################################################

#ble(con, "epc_matchleft",value =epc, append = TRUE, row.names = FALSE)
#rm(list=setdiff(ls(), c("epc", "linku","linkd","epcdata","nspl","sta1","sta2","sta3","sta34","sta34_out","sta34_out_re","sta4","epc_postleft","lauaname")))


link<-rbindlist(list(linku,linkd),use.names=TRUE, fill=TRUE)
linku1<-uniqueresult(link)
dim(linku1)
#20896896
linkd1<-doubleresult(link)
dim(linkd1)
# 759849
dim(linku1[linku1$lmk_key  %in% linku$lmk_key,])
dim(linkd1[linkd1$lmk_key  %in% linkd$lmk_key,])
rm(linku1,linkd1)





####################################
(length(unique(linku$lmk_key))+length(unique(linkd$lmk_key)))/21857699
# 0.9651803                                  
length(unique(linkd$lmk_key))/21857699
#0.009137467
length(unique(linku$lmk_key))/21857699
# 0.9560428
20896896-759849
dim(epcdata)
length(unique(linkd$lmk_key))

length(unique(linku$lmk_key))

epcdata_l<-epcdata[,c("lmk_key","property_type")]
head(epcdata_l)
linku1<-merge(linku,epcdata_l,by="lmk_key")

linkd1<-merge(linkd,epcdata_l,by="lmk_key")

#save one example for each method

class(linku1)
head(linku)
setkey(linku1,method)
sample<-linku1[,.SD[1],by = key(linku1)]
needlist2<-c("method","lmk_key","uprn","postcode.y","property_type","add1","add2","add3","add","postcode.x","postcodelocator","buildingname","buildingnumber","subbuildingname","paostartnumber","paostartsuffix","paoendnumber","paoendsuffix","paotext","saostartnumber","saostartsuffix","saoendnumber","saoendsuffix","saotext","streetdescription","dependentlocality","townname","class","lodgement_date","inspection_date","lodgement_datetime")
sample<-sample[,..needlist2]
getwd()
write.csv(sample,"D:/BINCHI/epc_read/linkage_sample.csv",row.names= F)
epcdata[epcdata$postcode=="GU7 1YG",]
#####check the same part

epc_same<- epcdata[epcdata$lmk_key %in% linku1$lmk_key, ]
class(epc_same)
head(epc_same)
sta5<-epc_same[,.(count=.N),by="uprn_source"]
dim(epcdata[epcdata$uprn_source!="",])
#
head(sta5)
sta5[uprn_source=="",]$uprn_source<-"No UPRN"

sta5$pro<-sta5$count/sum(sta5$count)
head(epcdata)
epcdata_mhclg<-epcdata[,c("lmk_key","uprn_source","uprn")]

mgclg_fail<-epcdata[epcdata$uprn_source=="" ,]
dim(mgclg_fail)[1]/dim(epcdata)[1]
#

bin_s<-linku1[linku1$lmk_key %in% mgclg_fail$lmk_key, ]
dim(bin_s)[1]/dim(epcdata)[1]

epc_same_re<-merge(epcdata_mhclg,linku1,by="lmk_key")
dim(epc_same_re) 

dim(epc_same_re[epc_same_re$uprn_source!="",])

dim(linku1[!(linku1$lmk_key %in% epc_same_re$lmk_key),])
#

c1<-epcdata[(epcdata$uprn_source!="" & !(epcdata$lmk_key %in% linku1$lmk_key)) ,]
dim(c1)
head(c1)














