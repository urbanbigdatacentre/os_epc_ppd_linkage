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
#remove the address1, address2 and address3 fields
epc[, address1:=NULL]
epc[, address2:=NULL]
epc[, address3:=NULL]

#remove whitespace from the left and right sides of the address string and postcode string
epc$add1 <- str_squish(epc$add1)
epc$add2 <- str_squish(epc$add2)
epc$add3 <- str_squish(epc$add3)
epc$add  <- str_squish(epc$add)

epc$add1 <- str_trim(epc$add1)
epc$add2 <- str_trim(epc$add2)
epc$add3 <- str_trim(epc$add3)
epc$add  <- str_trim(epc$add)

#remove multiple commas and trailing commasin add field
epc$add<-gsub("^,*|(?<=,),|,*$", "", epc$add, perl=T)

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
#covert add and epc to a data table format
setDT(add)
setDT(epc)
#prepare the field name list for the linkage process
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
#################################### section 4: data linkage #################################### 
#############################part 1: address matching data linkage############################# 
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
  #match on addressf field
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
  x$bnstreet <- paste(x$subbuildingname,x$buildingnumber,sep=" ")
  x$bnstreet <- paste(x$bnstreet,x$streetdescription,sep=" ")
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
  x$bnstreet <- paste(x$saotext,x$buildingnumber,sep=" ")
  x$bnstreet <- paste(x$bnstreet,x$streetdescription,sep=" ")
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
  x$bnstreet <- paste(x$saotext,x$pp,sep=" ")
  x$bnstreet <- paste(x$bnstreet,x$streetdescription,sep=" ")
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
  x$bnstreet <- x$buildingname
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
  x$bnstreet <- paste(x$ss,x$paotext,sep=" ")
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
  x$bnstreet <- paste(x$pp,x$streetdescription,sep=" ")
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
  x$bnstreet <- paste(x$subbuildingname,x$buildingnumber,sep=",")
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
  x$bnstreet <- paste(x$buildingnumber,x$paotext,sep=",")
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
  x$bnstreet <- paste(x$ss,x$paotext,sep=",")
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
  x$bnstreet <- x$buildingname
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
  x$bnstreet <- x$buildingname
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
  x$bnstreet <- paste(x$ss,x$paotext,sep=",")
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
  x$bnstreet <- paste(x$saotext,x$paotext,sep=" ")
  x$bnstreet <- paste(x$bnstreet,x$paostartnumber,sep=",")
  x$bnstreet <- paste(x$bnstreet,x$paoendnumber,sep="-")
  x$bnstreet <- paste(x$bnstreet,x$streetdescription,sep=" ")
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
  x$bnstreet <- paste(x$buildingnumber,x$paotext,sep=",")
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
  x$bnstreet <- paste(x$ss,x$paotext,sep=" ")
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
  x$bnstreet <- paste(x$subbuildingname,x$paotext,sep=" ")
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
  x$bnstreet <- paste(x$saotext,x$paotext,sep=" ")
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
  x$bnstreet <- paste(x$bnstreet,x$streetdescription,sep=" ")
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
  x$bnstreet <- paste(x$pp,x$streetdescription,sep=" ")
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
  x$bnstreet <- paste(x$ss,x$streetdescription,sep=" ")
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
  x$bnstreet <- paste(x$buildingnumber,x$streetdescription,sep=" ")
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
####################method 48####################
function48<- function(x,y){
  x$bnstreet <- paste(x$subbuildingname,x$buildingname,sep=",")
  x$bnstreet <- paste(x$bnstreet,x$buildingnumber,sep=" ")
  x$bnstreet <- paste(x$bnstreet,x$streetdescription,sep=" ")
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
####################method 49####################
function49<- function(x,y){
  x$bnstreet <- paste(x$saotext,x$buildingname,sep=",")
  x$bnstreet <- paste(x$bnstreet,x$buildingnumber,sep=" ")
  x$bnstreet <- paste(x$bnstreet,x$streetdescription,sep=" ")
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

####################method 50####################
function50<- function(x,y){
  x$bnstreet <- paste(x$subbuildingname,x$buildingname,sep=",")
  x$bnstreet <- paste(x$bnstreet,x$pp,sep=" ")
  x$bnstreet <- paste(x$bnstreet,x$streetdescription,sep=" ")
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
link50d <- doubleresult(link50)

epc <- matchleft(epc,link50)
####################method 51####################
#this is the linkage only for SY16 1Q
function51<- function(x,y){
  x$bnstreet <- x$buildingnumber
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
link51d <- doubleresult(link51)

epc <- matchleft(epc,link51)
####################method 52####################
function52<- function(x,y){
  x$bnstreet <- paste(x$subbuildingname,x$paotext,sep=",")
  x$bnstreet <- paste(x$bnstreet,x$buildingnumber,sep=" ")
  x$bnstreet <- paste(x$bnstreet,x$streetdescription,sep=" ")
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
link52d <- doubleresult(link52)

epc <- matchleft(epc,link52)
####################method 53####################
function53<- function(x,y){
  x$bnstreet <- paste(x$saotext,x$paotext,sep=",")
  x$bnstreet <- paste(x$bnstreet,x$buildingnumber,sep=" ")
  x$bnstreet <- paste(x$bnstreet,x$streetdescription,sep=" ")
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

link53<-link53[,..needlist1]

link53u<- uniqueresult(link53)
link53d <- doubleresult(link53)

epc <- matchleft(epc,link53)
####################method 54####################
function54<- function(x,y){
  x$bnstreet <- paste(x$saotext,x$paotext,sep=",")
  x$bnstreet <- paste(x$bnstreet,x$pp,sep=" ")
  x$bnstreet <- paste(x$bnstreet,x$streetdescription,sep=" ")
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
link54d <- doubleresult(link54)

epc<- matchleft(epc,link54)
####################method 55####################
function55<- function(x,y){
  x$bnstreet <- paste(x$subbuildingname,x$paotext,sep=",")
  x$bnstreet <- paste(x$bnstreet,x$buildingnumber,sep=" ")
  x$bnstreet <- paste(x$bnstreet,x$streetdescription,sep=" ")
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
link55d <- doubleresult(link55)

epc <- matchleft(epc,link55)
####################method 56####################
function56<- function(x,y){
  x$bnstreet <- paste(x$subbuildingname,x$paotext,sep=",")
  x$bnstreet <- paste(x$bnstreet,x$pp,sep=" ")
  x$bnstreet <- paste(x$bnstreet,x$streetdescription,sep=" ")
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
link56<-link56[,..needlist1]

link56u<- uniqueresult(link56)
link56d <- doubleresult(link56)

epc <- matchleft(epc,link56)
####################method 57####################
function57<- function(x,y){
  x$bnstreet <- paste(x$buildingnumber,x$paotext,sep=",")
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
link57<-link57[,..needlist1]

link57u<- uniqueresult(link57)
link57d <- doubleresult(link57)
 
epc <- matchleft(epc,link57)
####################method 58####################
function58<- function(x,y){
  x$bnstreet <- paste(x$saotext,x$pp,sep=",")
  x$bnstreet <- paste(x$bnstreet,x$buildingnumber,sep=" ")
  x$bnstreet <- paste(x$bnstreet,x$streetdescription,sep=" ")
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
link58<-link58[,..needlist1]

link58u<- uniqueresult(link58)
link58d <- doubleresult(link58)

epc <- matchleft(epc,link58)
####################method 59####################
function59<- function(x,y){
  x$bnstreet <- paste(x$subbuildingname,x$buildingname,sep=" ")
  x$addressf <-paste(x$postcodelocator,x$bnstreet,sep=",")
  
  y$addressfinal <- trimws(y$add)
  y$addressf <- paste(y$postcode,y$addressfinal,sep=",")
  
  taba1 <- inner_join(x,y,by="addressf")
  return(taba1) 
}
link59<-function59(add,epc)
link59<-link59[,..needlist1]

link59u<- uniqueresult(link59)
link59d <- doubleresult(link59)

epc <- matchleft(epc,link59)
####################method 60####################
function60<- function(x,y){
  x$bnstreet <- paste(x$paotext,x$buildingname,sep=" ")
  x$addressf <-paste(x$postcodelocator,x$bnstreet,sep=",")
  
  y$addressfinal <- trimws(y$add)
  y$addressf <- paste(y$postcode,y$addressfinal,sep=",")
  
  taba1 <- inner_join(x,y,by="addressf")
  return(taba1) 
}

link60<-function60(add,epc)
link60<-link60[,..needlist1]

link60u<- uniqueresult(link60)
link60d <- doubleresult(link60)

epc <- matchleft(epc,link60)
####################method 61####################
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
link61d <- doubleresult(link61)

epc <- matchleft(epc,link61)
####################method 62####################
function62<- function(x,y){
  x$bnstreet <- paste(x$subbuildingname,x$buildingname,sep=",")
  x$bnstreet <- paste(x$bnstreet,x$buildingnumber,sep=" ")
  x$bnstreet <- paste(x$bnstreet,x$streetdescription,sep=",")
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
link62d <- doubleresult(link62)

epc <- matchleft(epc,link62)
####################method 63####################
function63<- function(x,y){
  x$bnstreet <- paste(x$saotext,x$buildingname,sep=",")
  x$bnstreet <- paste(x$bnstreet,x$buildingnumber,sep=" ")
  x$bnstreet <- paste(x$bnstreet,x$streetdescription,sep=",")
  x$bnstreet <- gsub(" ", "", x$bnstreet)
  x$addressf <-paste(x$postcodelocator,x$bnstreet,sep=",")
  
  y$addressfinal <- trimws(y$add)
  y$addressfinal <- gsub(" ", "", y$addressfinal)
  y$addressf <- paste(y$postcode,y$addressfinal,sep=",")
  
  taba1 <- inner_join(x,y,by="addressf")
  
  return(taba1)
}
link63<-function63(add,epc)
link63<-link63[,..needlist1]

link63u<- uniqueresult(link63)
link63d <- doubleresult(link63)

epc <- matchleft(epc,link63)
####################method 64####################
function64<- function(x,y){
  x$bnstreet <- paste(x$subbuildingname,x$paotext,sep=",")
  x$bnstreet <- paste(x$bnstreet,x$buildingnumber,sep=" ")
  x$bnstreet <- paste(x$bnstreet,x$streetdescription,sep=",")
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
link64d <- doubleresult(link64)

epc<- matchleft(epc,link64)
####################method 65####################
function65<- function(x,y){
  x$bnstreet <- paste(x$subbuildingname,x$paotext,sep=",")
  x$bnstreet <- paste(x$bnstreet,x$pp,sep=" ")
  x$bnstreet <- paste(x$bnstreet,x$streetdescription,sep=",")
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
link65d <- doubleresult(link65)

epc <- matchleft(epc,link65)
####################method 66####################
function66<- function(x,y){
  x$bnstreet <- paste(x$saotext,x$paotext,sep=",")
  x$bnstreet <- paste(x$bnstreet,x$pp,sep=" ")
  x$bnstreet <- paste(x$bnstreet,x$streetdescription,sep=",")
  x$bnstreet <- gsub(" ", "", x$bnstreet)
  x$addressf <-paste(x$postcodelocator,x$bnstreet,sep=",")
  
  y$addressfinal <- trimws(y$add)
  y$addressfinal <- gsub(" ", "", y$addressfinal)
  y$addressf <- paste(y$postcode,y$addressfinal,sep=",")
  
  taba1 <- inner_join(x,y,by="addressf")
  
  return(taba1) 
}
link66<-function66(add,epc)
link66<-link66[,..needlist1]

link66u<- uniqueresult(link66)
link66d <- doubleresult(link66)

epc <- matchleft(epc,link66)
####################method 67####################
function67<- function(x,y){
  x$bnstreet <- paste(x$buildingnumber,x$paotext,sep=" ")
  x$bnstreet <- gsub(" ", "", x$bnstreet)
  x$addressf <-paste(x$postcodelocator,x$bnstreet,sep=",")
  
  y$addressfinal <- trimws(y$add)
  y$addressfinal <- gsub(" ", "", y$addressfinal)
  y$addressf <- paste(y$postcode,y$addressfinal,sep=",")
  
  taba1 <- inner_join(x,y,by="addressf")
  
  return(taba1)
}
link67<-function67(add,epc)
link67<-link67[,..needlist1]

link67u<- uniqueresult(link67)
link67d <- doubleresult(link67)

epc <- matchleft(epc,link67)
####################method 68####################
function68<- function(x,y){
  x$bnstreet <- paste(x$buildingname,x$paotext,sep=" ")
  x$bnstreet <- gsub(" ", "", x$bnstreet)
  x$addressf <-paste(x$postcodelocator,x$bnstreet,sep=",")
  
  y$addressfinal <- trimws(y$add)
  y$addressfinal <- gsub(" ", "", y$addressfinal)
  y$addressf <- paste(y$postcode,y$addressfinal,sep=",")
  
  taba1 <- inner_join(x,y,by="addressf")
  
  return(taba1)
}
link68<-function68(add,epc)
link68<-link68[,..needlist1]

link68u<- uniqueresult(link68)
link68d <- doubleresult(link68)
 
epc <- matchleft(epc,link68)
####################method 69####################
function69<- function(x,y){
  x<-x[x$subbuildingname=="",]
  x$bnstreet <- paste(x$ss,x$streetdescription,sep=",")
  
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
link69d <- doubleresult(link69)

epc <- matchleft(epc,link69)
####################sum up section 3####################
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

l27_69d = list(link27d,link28d,link29d,link30d,link31d,link32d,link33d,link34d,link35d,link36d,link37d,link38d,link39d,link40d,link41d,link42d,link43d,link44d,link45d,link46d,link47d,link48d,link49d,link50d,link51d,link52d,link53d,link54d,link55d,link56d,link57d,link58d,link59d,link60d,link61d,link62d,link63d,link64d,link65d,link66d,link67d,link68d,link69d)
link27_69d<- rbindlist(l27_69d, use.names=TRUE, fill=TRUE)

dbWriteTable(con, "link27_69dnew",value =link27_69d, append = TRUE, row.names = FALSE)
dbWriteTable(con, "link27_69unew",value =link27_69u, append = TRUE, row.names = FALSE)
#delete the data created in the above
rm(link27,link28,link29,link30,link31,link32,link33,link34,link35,link36,link37,link38,link39,link40,link41,link42,link43,link44,link45,link46,link47,link48,link49,link50,link51,link52,link53,link54,link55,link56,link57,link58,link59,link60,link61,link62,link63,link64,link65,link66,link67,link68,link69)
rm(link27u,link28u,link29u,link30u,link31u,link32u,link33u,link34u,link35u,link36u,link37u,link38u,link39u,link40u,link41u,link42u,link43u,link44u,link45u,link46u,link47u,link48u,link49u,link50u,link51u,link52u,link53u,link54u,link55u,link56u,link57u,link58u,link59u,link60u,link61u,link62u,link63u,link64u,link65u,link66u,link67u,link68u,link69u)
rm(link27d,link28d,link29d,link30d,link31d,link32d,link33d,link34d,link35d,link36d,link37d,link38d,link39d,link40d,link41d,link42d,link43d,link44d,link45d,link46d,link47d,link48d,link49d,link50d,link51d,link52d,link53d,link54d,link55d,link56d,link57d,link58d,link59d,link60d,link61d,link62d,link63d,link64d,link65d,link66d,link67d,link68d,link69d)
rm(l27_69u,l27_69d)
rm(function27,function28,function29,function30,function31,function32,function33,function34,function35,function36,function37,function38,function39,function40,function41,function42,function43,function44,function45,function46,function47,function48,function49,function50,function51,function52,function53,function54,function55,function56,function57,function58,function59,function60,function61,function62,function63,function64,function65,function66,function67,function68,function69)
####################method 70####################
function70<- function(x,y){
  x$bnstreet <- paste(x$subbuildingname,x$buildingname,sep=",")
  x$bnstreet <- paste(x$bnstreet,x$buildingnumber,sep=",")
  x$bnstreet <- paste(x$bnstreet,x$streetdescription,sep=",")
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
link70d <- doubleresult(link70)

epc <- matchleft(epc,link70)
####################method 71####################
function71<- function(x,y){
  # x<-x[x$subbuildingname!="",]
  x$bnstreet <- paste(x$saotext,x$paotext,sep=" ")
  x$bnstreet <- paste(x$bnstreet,x$buildingname ,sep=",")
  
  x$bnstreet <- paste(x$bnstreet,x$streetdescription,sep=",")
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
link71d <- doubleresult(link71)

epc <- matchleft(epc,link71)
####################method 72####################
function72<- function(x,y){
  x$bnstreet <- paste(x$subbuildingname,x$paotext,sep=" ")
  x$bnstreet <- paste(x$bnstreet,x$buildingname ,sep=",")
  #x$bnstreet <-    paste(x$bnstreet,x$paoendnumber,sep="-")
  x$bnstreet <- paste(x$bnstreet,x$streetdescription,sep=",")
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
link72d <- doubleresult(link72)

epc <- matchleft(epc,link72)
####################method 73####################
function73<- function(x,y){
  x$bnstreet <- paste(x$buildingname,x$paotext,sep=",")
  x$bnstreet <- paste(x$bnstreet,x$streetdescription,sep=",")
  x$bnstreet <- gsub(" ", "", x$bnstreet)
  x$addressf <-paste(x$postcodelocator,x$bnstreet,sep=",")
  
  y$addressfinal <- trimws(y$add)
  y$addressfinal <- gsub(" ", "", y$addressfinal)
  y$addressf <- paste(y$postcode,y$addressfinal,sep=",")
  
  taba1 <- inner_join(x,y,by="addressf")
  
  return(taba1)
}
link73<-function73(add,epc)
link73<-link73[,..needlist1]

link73u<- uniqueresult(link73)
link73d <- doubleresult(link73)

epc <- matchleft(epc,link73)
####################method 74####################
function74<- function(x,y){
  x$bnstreet <- paste(x$ss,x$paotext,sep=",")
  x$bnstreet <- paste(x$bnstreet,x$streetdescription,sep=",")
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
link74<-link74[,..needlist1]

link74u<- uniqueresult(link74)
link74d <- doubleresult(link74)

epc<- matchleft(epc,link74)
####################method 75####################
function75<- function(x,y){
  x<-x[paostartsuffix=="",]
  x$bnstreet <- paste(x$buildingnumber,x$streetdescription,sep=" ")
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
link75<-function75(add,epc)
link75<-link75[,..needlist1]

link75u<- uniqueresult(link75)
link75d <- doubleresult(link75)

epc <- matchleft(epc,link75)
####################method 76####################
function76<- function(x,y){
  x$bnstreet <- paste(x$buildingnumber,x$paotext,sep=",")
  x$bnstreet <- paste(x$bnstreet,x$streetdescription,sep=",")
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
link76<-link76[,..needlist1]

link76u<- uniqueresult(link76)
link76d <- doubleresult(link76)

epc <- matchleft(epc,link76)
####################method 77####################
function77<- function(x,y){
  x$bnstreet <- paste(x$saotext,x$paotext,sep=",")
  x$bnstreet <- paste(x$bnstreet,x$streetdescription,sep=",")
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
link77<-link77[,..needlist1]

link77u<- uniqueresult(link77)
link77d <- doubleresult(link77)

epc <- matchleft(epc,link77)
####################method 78####################
function78<- function(x,y){
  x$bnstreet <- paste(x$buildingnumber,x$streetdescription,sep=" ")
  x$bnstreet <- paste(x$bnstreet,x$townname,sep=",")
  x$bnstreet <- gsub(" ", "", x$bnstreet)
  x$addressf <-paste(x$postcodelocator,x$bnstreet,sep=",")
  
  y$addressfinal <- trimws(y$add)
  y$addressfinal <- gsub(" ", "", y$addressfinal)
  y$addressf <- paste(y$postcode,y$addressfinal,sep=",")
  
  taba1 <- inner_join(x,y,by="addressf")
  
  return(taba1)
}
link78<-function78(add,epc)
link78<-link78[,..needlist1]

link78u<- uniqueresult(link78)
link78d <- doubleresult(link78)
 
epc <- matchleft(epc,link78)
####################method 79####################
function79<- function(x,y){
  x$bnstreet <- paste(x$pp,x$streetdescription,sep=" ")
  x$bnstreet <- paste(x$bnstreet,x$townname,sep=",")
  x$bnstreet <- gsub(" ", "", x$bnstreet)
  x$addressf <-paste(x$postcodelocator,x$bnstreet,sep=",")
  
  y$addressfinal <- trimws(y$add)
  y$addressfinal <- gsub(" ", "", y$addressfinal)
  y$addressf <- paste(y$postcode,y$addressfinal,sep=",")
  
  taba1 <- inner_join(x,y,by="addressf")
  
  return(taba1)
}
link79<-function79(add,epc)
link79<-link79[,..needlist1]

link79u<- uniqueresult(link79)
link79d <- doubleresult(link79)

epc <- matchleft(epc,link79)
####################method 80####################
function80<- function(x,y){
  x$bnstreet <- paste(x$saotext,x$pp,sep=",")
  x$bnstreet <- paste(x$bnstreet,x$buildingnumber,sep=" ")
  x$bnstreet <- paste(x$bnstreet,x$streetdescription,sep=" ")
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
link80d <- doubleresult(link80)

epc <- matchleft(epc,link80)
####################method 81####################
function81<- function(x,y){
  x$bnstreet <- paste(x$buildingnumber,x$streetdescription,sep=",")
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
link81<-function81(add,epc)
link81<-link81[,..needlist1]

link81u<- uniqueresult(link81)
link81d <- doubleresult(link81)

epc <- matchleft(epc,link81)
####################method 82####################
function82<- function(x,y){
  x$bnstreet <- paste(x$pp,x$streetdescription,sep=",")
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
link82<-function82(add,epc)
link82<-link82[,..needlist1]

link82u<- uniqueresult(link82)
link82d <- doubleresult(link82)

epc <- matchleft(epc,link82)
####################method 83####################
function83<- function(x,y){
  x<-x[x$buildingname=="",]
  x<-x[x$buildingnumber=="",]
  x<-x[x$subbuildingname=="",]
  x<-x[x$paostartsuffix=="",]
  x$bnstreet <- paste(x$saotext,x$paostartnumber,sep=",")
  x$bnstreet <- paste(x$bnstreet,x$streetdescription,sep=",")
  x$bnstreet <- gsub(" ", "", x$bnstreet)
  x$addressf <-paste(x$postcodelocator,x$bnstreet,sep=",")
  
  y$addressfinal <- trimws(y$add)
  y$addressfinal <- gsub(" ", "", y$addressfinal)
  y$addressf <- paste(y$postcode,y$addressfinal,sep=",")
  
  taba1 <- inner_join(x,y,by="addressf")
  
  return(taba1)
}
link83<-function83(add,epc)
link83<-link83[,..needlist1]

link83u<- uniqueresult(link83)
link83d <- doubleresult(link83)

epc <- matchleft(epc,link83)
####################method 84####################
function84<- function(x,y){
  x<-x[x$buildingnumber=="",]
  x<-x[x$paostartsuffix=="",]
  x<-x[x$saotext=="",]
  x<-x[x$paoendnumber=="",]
  x<-x[x$saostartsuffix=="",]
  x$bnstreet <- paste(x$paostartnumber,x$streetdescription,sep=",")
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
link84d <- doubleresult(link84)

epc<- matchleft(epc,link84)
####################method 85####################
function85<- function(x,y){
  x$bnstreet <- paste(x$subbuildingname,x$buildingname,sep=" ")
  x$bnstreet <- paste(x$bnstreet,x$streetdescription,sep=",")
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
link85<-link85[,..needlist1]

link85u<- uniqueresult(link85)
link85d <- doubleresult(link85)

epc <- matchleft(epc,link85)
####################method 86####################
function86<- function(x,y){
  x$bnstreet <- paste(x$saotext,x$buildingname,sep=" ")
  x$bnstreet <- paste(x$bnstreet,x$streetdescription,sep=",")
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
link86<-link86[,..needlist1]

link86u<- uniqueresult(link86)
link86d <- doubleresult(link86)

epc <- matchleft(epc,link86)
####################method 87####################
function87<- function(x,y){
  x$bnstreet <- paste(x$subbuildingname,x$paotext,sep=" ")
  x$bnstreet <- paste(x$bnstreet,x$streetdescription,sep=",")
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
link87<-link87[,..needlist1]

link87u<- uniqueresult(link87)
link87d <- doubleresult(link87)

epc <- matchleft(epc,link87)
####################method 88####################
function88<- function(x,y){
  x$bnstreet <- paste(x$subbuildingname,x$buildingname,sep=" ")
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
link88<-link88[,..needlist1]

link88u<- uniqueresult(link88)
link88d <- doubleresult(link88)
 
epc <- matchleft(epc,link88)
####################method 89####################
function89<- function(x,y){
  x<-x[x$buildingnumber=="",]
  x$bnstreet <-paste(x$saotext,x$pp,sep=",")
  x$bnstreet <- paste(x$bnstreet,x$streetdescription,sep=",")
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
link89<-function89(add,epc)
link89<-link89[,..needlist1]

link89u<- uniqueresult(link89)
link89d <- doubleresult(link89)

epc <- matchleft(epc,link89)
####################method 90####################
function90<- function(x,y){
  x$bnstreet <- paste(x$saotext,x$pp,sep=",")
  x$bnstreet <- paste(x$bnstreet,x$streetdescription,sep=" ")
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
link90<-link90[,..needlist1]

link90u<- uniqueresult(link90)
link90d <- doubleresult(link90)

epc <- matchleft(epc,link90)
####################method 91####################
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
link91d <- doubleresult(link91)

epc <- matchleft(epc,link91)
####################method 92####################
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
link92d <- doubleresult(link92)

epc <- matchleft(epc,link92)
####################method 93####################
function93<- function(x,y){
  x$bnstreet <- paste(x$subbuildingname,x$buildingname,sep=",")
  x$bnstreet <- paste(x$bnstreet,x$buildingnumber,sep=",")
  x$bnstreet <- paste(x$bnstreet,x$streetdescription,sep=" ")
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
link93<-link93[,..needlist1]

link93u<- uniqueresult(link93)
link93d <- doubleresult(link93)

epc <- matchleft(epc,link93)
####################method 94####################
function94<- function(x,y){
  x$bnstreet <- paste(x$subbuildingname,x$buildingname,sep=",")
  x$bnstreet <- paste(x$bnstreet,x$pp,sep=",")
  x$bnstreet <- paste(x$bnstreet,x$streetdescription,sep=" ")
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
link94d <- doubleresult(link94)

epc<- matchleft(epc,link94)
####################method 95####################
function95<- function(x,y){
  x$bnstreet <- paste(x$saotext,x$buildingname,sep=",")
  x$bnstreet <- paste(x$bnstreet,x$pp,sep=",")
  x$bnstreet <- paste(x$bnstreet,x$streetdescription,sep=" ")
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
link95d <- doubleresult(link95)

epc <- matchleft(epc,link95)
#################### method 96 ##################
function96<- function(x,y){
  x$bnstreet <- paste(x$pp,x$townname,sep=",")
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
link96<-link96[,..needlist1]

link96u<- uniqueresult(link96)
link96d <- doubleresult(link96)

epc <- matchleft(epc,link96)
####################method 97####################
function97<- function(x,y){
  x$bnstreet <- paste(x$pp,x$dependentlocality,sep=",")
  x$bnstreet <- gsub("[.]", "", x$bnstreet)
  x$bnstreet <- gsub("[']", "", x$bnstreet)
  x$bnstreet <- gsub(" ", "", x$bnstreet)
  x$addressf <-paste(x$postcodelocator,x$bnstreet,sep=",")
  
  y$addressfinal <- paste(y$add)
  y$bnstreet <- gsub("[.]", "",  y$bnstreet)
  y$bnstreet <- gsub("[']", "",  y$bnstreet)
  y$addressfinal <- gsub(" ", "", y$addressfinal)
  y$addressf <- paste(y$postcode,y$addressfinal,sep=",")
  
  taba1 <- inner_join(x,y,by="addressf")
  
  return(taba1)
}
link97<-function97(add,epc)
link97<-link97[,..needlist1]

link97u<- uniqueresult(link97)
link97d <- doubleresult(link97)
 
epc <- matchleft(epc,link97)
####################method 98####################
function98<- function(x,y){
  x$bnstreet <- paste(x$pp,x$locality,sep=",")
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
link98<-link98[,..needlist1]

link98u<- uniqueresult(link98)
link98d <- doubleresult(link98)

epc <- matchleft(epc,link98)
####################method 99####################
function99<- function(x,y){
  x$bnstreet <- paste(x$saotext,x$buildingnumber ,sep=",")
  #x$bnstreet <-    paste(x$bnstreet,x$paostartsuffix ,sep=" ")
  x$bnstreet <- paste(x$bnstreet,x$streetdescription ,sep=",")
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
link99<-function99(add,epc)
link99<-link99[,..needlist1]

link99u<- uniqueresult(link99)
link99d <- doubleresult(link99)

epc <- matchleft(epc,link99)
####################sum up section####################
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

l70_99u = list(link70u,link71u,link72u,link73u,link74u,link75u,link76u,link77u,link78u,link79u,link80u,link81u,link82u,link83u,link84u,link85u,link86u,link87u,link88u,link89u,link90u,link91u,link92u,link93u,link94u,link95u,link96u,link97u,link98u,link99u)
link70_99u<- rbindlist(l70_99u)

l70_99d = list(link70d,link71d,link72d,link73d,link74d,link75d,link76d,link77d,link78d,link79d,link80d,link81d,link82d,link83d,link84d,link85d,link86d,link87d,link88d,link89d,link90d,link91d,link92d,link93d,link94d,link95d,link96d,link97d,link98d,link99d)
link70_99d<- rbindlist(l70_99d, use.names=TRUE, fill=TRUE)
dbWriteTable(con, "link70_99unew",value =link70_99u, append = TRUE, row.names = FALSE)
dbWriteTable(con, "link70_99dnew",value =link70_99d, append = TRUE, row.names = FALSE)

rm(link70d,link71d,link72d,link73d,link74d,link75d,link76d,link77d,link78d,link79d,link80d,link81d,link82d,link83d,link84d,link85d,link86d,link87d,link88d,link89d,link90d,link91d,link92d,link93d,link94d,link95d,link96d,link97d,link98d,link99d)
rm(link70u,link71u,link72u,link73u,link74u,link75u,link76u,link77u,link78u,link79u,link80u,link81u,link82u,link83u,link84u,link85u,link86u,link87u,link88u,link89u,link90u,link91u,link92u,link93u,link94u,link95u,link96u,link97u,link98u,link99u)
rm(link70,link71,link72,link73,link74,link75,link76,link77,link78,link79,link80,link81,link82,link83,link84,link85,link86,link87,link88,link89,link90,link91,link92,link93,link94,link95,link96,link97,link98,link99)
rm(l70_99u,l70_99d)
rm(function70,function71,function72,function73,function74,function75,function76,function77,function78,function79,function80,function81,function82,function83,function84,function85,function86,function87,function88,function89,function90,function91,function92,function93,function94,function95,function96,function97,function98,function99)
####################method 100####################
function100<- function(x,y){
  x$bnstreet <- paste(x$saotext,x$pp ,sep=",")
  x$bnstreet <- paste(x$bnstreet,x$streetdescription ,sep=",")
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
link100<-link100[,..needlist1]

link100u<- uniqueresult(link100)
link100d <- doubleresult(link100)

epc <- matchleft(epc,link100)
####################method 101####################
function101<- function(x,y){
  x$bnstreet <- paste(x$subbuildingname,x$paostartnumber ,sep=",")
  x$bnstreet <- paste(x$bnstreet,x$paostartsuffix ,sep=" ")
  x$bnstreet <- paste(x$bnstreet,x$streetdescription ,sep=",")
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
link101d<- doubleresult(link101)

epc <- matchleft(epc,link101)
####################method 102####################
function102<- function(x,y){
  x$bnstreet <- paste(x$subbuildingname,x$buildingname ,sep=",")
  x$bnstreet <- paste(x$bnstreet,x$streetdescription ,sep=",")
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
link102d <- doubleresult(link102)

epc <- matchleft(epc,link102)
####################method 103####################
function103<- function(x,y){
  x$bnstreet <- paste(x$buildingnumber,x$paotext,sep=",")
  x$bnstreet <- paste(x$bnstreet,x$locality,sep=",")
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
link103<-link103[,..needlist1]

link103u<- uniqueresult(link103)
link103d <- doubleresult(link103)

epc <- matchleft(epc,link103)
####################method 104####################
function104<- function(x,y){
  x$bnstreet <- paste(x$buildingname,x$streetdescription,sep=" ")
  x$bnstreet <- paste(x$bnstreet,x$locality,sep=",")
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
link104d <- doubleresult(link104)

epc<- matchleft(epc,link104)
####################method 105####################
function105<- function(x,y){
  x$bnstreet <- paste(x$subbuildingname,x$buildingnumber,sep=",")
  x$bnstreet <- paste(x$bnstreet,x$streetdescription,sep=",")
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
link105d <- doubleresult(link105)

epc <- matchleft(epc,link105)
####################method 106####################
function106<- function(x,y){
  x$bnstreet <- paste(x$buildingnumber,x$paotext,sep=",")
  x$bnstreet <- paste(x$bnstreet,x$locality,sep=" ")
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
link106<-link106[,..needlist1]

link106u<- uniqueresult(link106)
link106d <- doubleresult(link106)
epc <- matchleft(epc,link106)
####################method 107####################
function107<- function(x,y){
  x<-x[x$saotext=="",]
  x$bnstreet <- paste(x$pp,x$streetdescription,sep=",")
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
link107<-link107[,..needlist1]

link107u<- uniqueresult(link107)
link107d <- doubleresult(link107)

epc <- matchleft(epc,link107)
####################method 108####################
function108<- function(x,y){
  x$bnstreet <- paste(x$pp,x$streetdescription,sep=" ")
  x$bnstreet <- paste(x$bnstreet,x$townname,sep=",")
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
link108<-link108[,..needlist1]

link108u<- uniqueresult(link108)
link108d <- doubleresult(link108)

epc <- matchleft(epc,link108)
####################method 109####################
function109<- function(x,y){
  x$bnstreet <- paste(x$buildingnumber,x$paotext,sep=",")
  x$bnstreet <- paste(x$bnstreet,x$dependentlocality,sep=",")
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
link109d <- doubleresult(link109)

epc <- matchleft(epc,link109)
####################method 110####################
function110<- function(x,y){
  x$bnstreet <- paste(x$ss,x$paotext,sep=",")
  x$bnstreet <- paste(x$bnstreet,x$dependentlocality,sep=",")
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
link110d <- doubleresult(link110)

epc <- matchleft(epc,link110)
####################method 111####################
function111<- function(x,y){
  x$bnstreet <- paste(x$subbuildingname,x$buildingname,sep=",")
  x$bnstreet <- paste(x$bnstreet,x$paotext,sep=" ")
  x$bnstreet <- paste(x$bnstreet,x$buildingnumber,sep=",")
  x$bnstreet <- paste(x$bnstreet,x$streetdescription,sep=" ")
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
link111d <- doubleresult(link111)

epc <- matchleft(epc,link111)
####################method 112####################
function112<- function(x,y){
  x$bnstreet <- paste(x$ss,x$paotext,sep=" ")
  x$bnstreet <- paste(x$bnstreet,x$dependentlocality,sep=",")
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
link112<-link112[,..needlist1]

link112u<- uniqueresult(link112)
link112d <- doubleresult(link112)

epc <- matchleft(epc,link112)
####################method 113####################
function113<- function(x,y){
  x<-x[x$saostartsuffix=="",]
  x$bnstreet <- paste(x$buildingnumber,x$paotext,sep=" ")
  x$bnstreet <- paste(x$bnstreet,x$dependentlocality,sep=",")
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
link113<-link113[,..needlist1]

link113u<- uniqueresult(link113)
link113d <- doubleresult(link113)

epc <- matchleft(epc,link113)
####################method 114####################
function114<- function(x,y){
  x$bnstreet <- paste(x$buildingnumber,x$buildingname,sep=" ")
  x$bnstreet <- paste(x$bnstreet,x$dependentlocality,sep=",")
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
link114d <- doubleresult(link114)

epc<- matchleft(epc,link114)
####################method 115####################
function115<- function(x,y){
  x$bnstreet <- paste(x$ss,x$paotext,sep=" ")
  x$bnstreet <- paste(x$bnstreet,x$streetdescription,sep=",")
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

link115u<- uniqueresult(link115)
link115d <- doubleresult(link115)

epc <- matchleft(epc,link115)
####################method 116####################
function116<- function(x,y){
  x$bnstreet <- paste(x$ss,x$buildingname,sep=" ")
  x$bnstreet <- paste(x$bnstreet,x$streetdescription,sep=",")
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
link116<-link116[,..needlist1]

link116u<- uniqueresult(link116)
link116d <- doubleresult(link116)

epc <- matchleft(epc,link116)
####################method 117####################
function117<- function(x,y){
  x<-x[x$paostartsuffix=="",]
  x$bnstreet <- paste(x$saotext ,x$paostartnumber,sep=",")
  x$bnstreet <- paste(x$bnstreet ,x$paoendnumber,sep="-")
  x$bnstreet <- paste(x$bnstreet,x$streetdescription,sep=" ")
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
link117<-link117[,..needlist1]

link117u<- uniqueresult(link117)
link117d <- doubleresult(link117)

epc <- matchleft(epc,link117)
####################method 118####################
function118<- function(x,y){
  x$bnstreet <- paste(x$subbuildingname ,x$paostartnumber,sep=",")
  x$bnstreet <- paste(x$bnstreet ,x$paoendnumber,sep="-")
  x$bnstreet <- paste(x$bnstreet,x$streetdescription,sep=" ")
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
link118<-function118(add,epc)
link118<-link118[,..needlist1]

link118u<- uniqueresult(link118)
link118d <- doubleresult(link118)

epc <- matchleft(epc,link118)
####################method 119####################
function119<- function(x,y){
  x$bnstreet <- paste(x$buildingname,x$streetdescription,sep=",")
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
link119<-function119(add,epc)
link119<-link119[,..needlist1]

link119u<- uniqueresult(link119)
link119d <- doubleresult(link119)

epc <- matchleft(epc,link119)
####################method 120####################
function120<- function(x,y){
  x<-x[x$paostartsuffix=="",]
  x$bnstreet <- paste(x$subbuildingname,x$buildingnumber,sep=",")
  x$bnstreet <- paste(x$bnstreet,x$streetdescription,sep=" ")
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
link120<-link120[,..needlist1]

link120u<- uniqueresult(link120)
link120d <- doubleresult(link120)

epc <- matchleft(epc,link120)
####################method 121####################
function121<- function(x,y){
  x$bnstreet <- paste(x$subbuildingname,x$pp,sep=",")
  x$bnstreet <- paste(x$bnstreet,x$streetdescription,sep=" ")
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
link121d <- doubleresult(link121)

epc <- matchleft(epc,link121)
####################method 122####################
function122<- function(x,y){
  x$bnstreet <- paste(x$saotext,x$paotext,sep=" ")
  x$bnstreet <- paste(x$bnstreet,x$paostartnumber,sep=",")
  x$bnstreet <- paste(x$bnstreet,x$paoendnumber,sep="-")
  x$bnstreet <- paste(x$bnstreet,x$streetdescription,sep=",")
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

link122u<- uniqueresult(link122)
link122d <- doubleresult(link122)

epc <- matchleft(epc,link122)
####################method 123####################
function123<- function(x,y){
  x$bnstreet <- paste(x$subbuildingname,x$paotext,sep=" ")
  x$bnstreet <- paste(x$bnstreet,x$paostartnumber,sep=",")
  x$bnstreet <- paste(x$bnstreet,x$paoendnumber,sep="-")
  x$bnstreet <- paste(x$bnstreet,x$streetdescription,sep=",")
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
link123<-link123[,..needlist1]

link123u<- uniqueresult(link123)
link123d <- doubleresult(link123)

epc <- matchleft(epc,link123)
####################method 124####################
function124<- function(x,y){
  x$bnstreet <- paste(x$ss,x$paotext,sep=" ")
  x$bnstreet <- paste(x$bnstreet,x$pp,sep=",")
  x$bnstreet <- paste(x$bnstreet,x$streetdescription,sep=" ")
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
link124d <- doubleresult(link124)

epc <- matchleft(epc,link124)
dim(epc)
####################method 125####################
function125<- function(x,y){
  x$bnstreet <- paste(x$saotext,x$paotext,sep=" ")
  x$bnstreet <- paste(x$bnstreet,x$streetdescription,sep=",")
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
link125<-link125[,..needlist1]

link125u<- uniqueresult(link125)
link125d <- doubleresult(link125)

epc <- matchleft(epc,link125)
####################method 126####################
function126<- function(x,y){
  x$bnstreet <- paste(x$subbuildingname,x$paotext,sep=" ")
  x$bnstreet <- paste(x$bnstreet,x$streetdescription,sep=",")
  x$bnstreet <- gsub("[.]", "", x$bnstreet)
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
link126<-link126[,..needlist1]

link126u<- uniqueresult(link126)
link126d <- doubleresult(link126)

epc <- matchleft(epc,link126)
####################method 127####################
function127<- function(x,y){
  x$bnstreet <- paste(x$subbuildingname,x$buildingname,sep=" ")
  x$bnstreet <- paste(x$bnstreet,x$streetdescription,sep=",")
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
link127<-link127[,..needlist1]

link127u<- uniqueresult(link127)
link127d <- doubleresult(link127)

epc <- matchleft(epc,link127)
####################method 128####################
function128<- function(x,y){
  x$bnstreet <- paste(x$saotext,x$buildingname,sep=" ")
  x$bnstreet <- paste(x$bnstreet,x$streetdescription,sep=",")
  x$bnstreet <- gsub("[.]", "", x$bnstreet)
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
link128<-link128[,..needlist1]

link128u<- uniqueresult(link128)
link128d <- doubleresult(link128)

epc <- matchleft(epc,link128)
####################method 129####################
function129<- function(x,y){

  x$bnstreet <- paste(x$saotext,x$ss,sep=",")
  x$bnstreet <- paste(x$bnstreet,x$paotext,sep=" ")
  x$bnstreet <- paste(x$bnstreet,x$streetdescription,sep=",")
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
link129d <- doubleresult(link129)

epc <- matchleft(epc,link129)
####################method 130####################
function130<- function(x,y){
  x$bnstreet <- paste(x$saotext,x$ss,sep=",")
  x$bnstreet <- paste(x$bnstreet,x$buildingnumber,sep=" ")
  x$bnstreet <- paste(x$bnstreet,x$streetdescription,sep=",")
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
link130<-link130[,..needlist1]

link130u<- uniqueresult(link130)
link130d <- doubleresult(link130)

epc <- matchleft(epc,link130)
####################method 131####################
function131<- function(x,y){
  x$bnstreet <- paste(x$saotext,x$ss,sep=" ")
  x$bnstreet <- paste(x$bnstreet,x$paotext,sep=",")
  x$bnstreet <- paste(x$bnstreet,x$streetdescription,sep=",")
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
link131d <- doubleresult(link131)

epc <- matchleft(epc,link131)
####################method 132####################
function132<- function(x,y){
  x$bnstreet <- paste(x$paotext,x$ss,sep=" ")
  x$bnstreet <- paste(x$bnstreet,x$streetdescription,sep=",")
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
link132d <- doubleresult(link132)

epc <- matchleft(epc,link132)
####################method 133####################
function133<- function(x,y){
  x<-x[x$subbuildingname=="",]
  x$bnstreet <- paste(x$buildingnumber,x$paotext,sep=" ")
  x$bnstreet <- gsub("[.]", "", x$bnstreet)
  x$bnstreet <- gsub("[']", "", x$bnstreet)
  x$addressf <-paste(x$postcodelocator,x$bnstreet,sep=",")
  
  y$addressfinal <- trimws(y$add)
  y$addressfinal <- gsub("[.]", "", y$addressfinal)
  y$addressfinal <- gsub("[']", "", y$addressfinal)
  y$addressf <- paste(y$postcode,y$addressfinal,sep=",")
  
  taba1 <- inner_join(x,y,by="addressf")
  
  return(taba1)
}

link133<-function133(add,epc)
link133<-link133[,..needlist1]

link133u<- uniqueresult(link133)
link133d <- doubleresult(link133)

epc <- matchleft(epc,link133)
####################section sum up####################
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
l100_133d = list(link100d,link101d,link102d,link103d,link104d,link105d,link106d,link107d,link108d,link109d,link110d,link111d,link112d,link113d,link114d,link115d,link116d,link117d,link118d,link119d,link120d,link121d,link122d,link123d,link124d,link125d,link126d,link127d,link128d,link129d,link130d,link131d,link132d,link133d)
link100_133d<- rbindlist(l100_133d, use.names=TRUE, fill=TRUE)

dbWriteTable(con, "link100_133dnew",value =link100_133d, append = TRUE, row.names = FALSE)
dbWriteTable(con, "link100_133unew",value =link100_133u, append = TRUE, row.names = FALSE)

rm(link100u,link101u,link102u,link103u,link104u,link105u,link106u,link107u,link108u,link109u,link110u,link111u,link112u,link113u,link114u,link115u,link116u,link117u,link118u,link119u,link120u,link121u,link122u,link123u,link124u,link125u,link126u,link127u,link128u,link129u,link130u,link131u,link132u,link133u)
rm(link100d,link101d,link102d,link103d,link104d,link105d,link106d,link107d,link108d,link109d,link110d,link111d,link112d,link113d,link114d,link115d,link116d,link117d,link118d,link119d,link120d,link121d,link122d,link123d,link124d,link125d,link126d,link127d,link128d,link129d,link130d,link131d,link132d,link133d)
rm(link100,link101,link102,link103,link104,link105,link106,link107,link108,link109,link110,link111,link112,link113,link114,link115,link116,link117,link118,link119,link120,link121,link122,link123,link124,link125,link126,link127,link128,link129,link130,link131,link132,link133)
rm(l100_133u,l100_133d)
rm(function100,function101,function102,function103,function104,function105,function106,function107,function108,function109,function110,function111,function112,function113,function114,function115,function116,function117,function118,function119,function120,function121,function122,function123,function124,function125,function126,function127,function128,function129,function130,function131,function132,function133)
####################method 134####################
function134<- function(x,y){
  x$bnstreet <- paste(x$subbuildingname,x$buildingnumber,sep=",")
  x$bnstreet <- paste(x$bnstreet,x$streetdescription,sep=" ")
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

link134u<- uniqueresult(link134)
link134d <- doubleresult(link134)

epc<- matchleft(epc,link134)
####################method 135####################
function135<- function(x,y){
  x$bnstreet <- paste(x$saotext,x$paotext,sep=",")
  x$bnstreet <- paste(x$bnstreet,x$pp ,sep=",")
  x$bnstreet <- paste(x$bnstreet,x$streetdescription ,sep=" ")
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

link135u<- uniqueresult(link135)
link135d <- doubleresult(link135)

epc <- matchleft(epc,link135)
####################method 136####################
function136<- function(x,y){
  x$bnstreet <- paste(x$subbuildingname,x$buildingname,sep=",")
  x$bnstreet <- paste(x$bnstreet,x$streetdescription,sep=",")
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
link136<-link136[,..needlist1]

link136u<- uniqueresult(link136)
link136d <- doubleresult(link136)

epc <- matchleft(epc,link136)
####################method 137####################
function137<- function(x,y){
  x$bnstreet <- paste(x$saotext,x$buildingname,sep=",")
  x$bnstreet <- paste(x$bnstreet,x$streetdescription,sep=",")
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
link137<-link137[,..needlist1]

link137u<- uniqueresult(link137)
link137d <- doubleresult(link137)
 
epc <- matchleft(epc,link137)
####################method 138####################
function138<- function(x,y){
  x$bnstreet <-  paste(x$saotext,x$pp,sep=",")
  x$bnstreet <- paste(x$bnstreet,x$streetdescription,sep=",")
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
link138<-link138[,..needlist1]

link138u<- uniqueresult(link138)
link138d <- doubleresult(link138)

epc <- matchleft(epc,link138)
####################method 139####################
function139<- function(x,y){
  x$bnstreet <- paste(x$subbuildingname,x$buildingname,sep=",")
  x$bnstreet <- paste(x$bnstreet,x$ss,sep=",")
  x$bnstreet <- paste(x$bnstreet,x$paotext,sep=" ")
  x$bnstreet <- paste(x$bnstreet,x$streetdescription,sep=",")
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
link139d <- doubleresult(link139)

epc <- matchleft(epc,link139)
####################method 140####################
function140<- function(x,y){
  x$bnstreet <- paste(x$subbuildingname,x$buildingname,sep=",")
  x$bnstreet <- paste(x$bnstreet,x$streetdescription,sep=",")
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
link140<-link140[,..needlist1]

link140u<- uniqueresult(link140)
link140d <- doubleresult(link140)

epc <- matchleft(epc,link140)
####################method 141####################
function141<- function(x,y){
  x<-x[x$saotext=="",]
  x$bnstreet <- paste(x$pp,x$streetdescription,sep=",")
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

link141u<- uniqueresult(link141)

link141d <- doubleresult(link141)

epc <- matchleft(epc,link141)
####################method 142####################
function142<- function(x,y){
  x$bnstreet <- paste(x$buildingnumber,x$streetdescription,sep=",")
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

link142u<- uniqueresult(link142)
link142d <- doubleresult(link142)

epc <- matchleft(epc,link142)
####################method 143####################
function143<- function(x,y){
  x$bnstreet <- x$buildingname
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
link143<-link143[,..needlist1]

link143u<- uniqueresult(link143)
link143d <- doubleresult(link143)

epc <- matchleft(epc,link143)
####################method 144####################
function144<- function(x,y){
  x$bnstreet <- x$buildingname
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
link144<-link144[,..needlist1]

link144u<- uniqueresult(link144)
link144d <- doubleresult(link144)

epc<- matchleft(epc,link144)
####################method 145####################
function145<- function(x,y){
  x$bnstreet <- paste(x$buildingname,x$buildingnumber,sep=" ")
  x$bnstreet <- paste(x$bnstreet,x$streetdescription,sep=" ")
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

link145u<- uniqueresult(link145)
link145d <- doubleresult(link145)

epc <- matchleft(epc,link145)
####################method 146####################
function146<- function(x,y){
  x$bnstreet <- paste(x$buildingname,x$pp,sep=" ")
  x$bnstreet <- paste(x$bnstreet,x$streetdescription,sep=" ")
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
link146<-link146[,..needlist1]

link146u<- uniqueresult(link146)
link146d <- doubleresult(link146)

epc <- matchleft(epc,link146)
####################method 147####################
function147<- function(x,y){
  x$bnstreet <- paste(x$saotext,x$buildingnumber,sep=",")
  x$bnstreet <- paste(x$bnstreet,x$pp,sep=" ")
  x$bnstreet <- paste(x$bnstreet,x$streetdescription,sep=" ")
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
link147<-link147[,..needlist1]

link147u<- uniqueresult(link147)
link147d <- doubleresult(link147)

epc <- matchleft(epc,link147)
####################method 148####################
function148<- function(x,y){
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
link148<-link148[,..needlist1]

link148u<- uniqueresult(link148)
link148d <- doubleresult(link148)

epc <- matchleft(epc,link148)
####################method 149####################
function149<- function(x,y){
  x<-x[x$buildingnumber=="",]
  x$bnstreet <- paste(x$subbuildingname,x$pp,sep=",")
  x$bnstreet <- paste(x$bnstreet,x$streetdescription,sep=" ")
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
link149d <- doubleresult(link149)

epc <- matchleft(epc,link149)
####################method 150####################
function150<- function(x,y){
  x$bnstreet <- paste(x$saotext,x$pp,sep=",")
  x$bnstreet <- paste(x$bnstreet,x$streetdescription,sep=" ")
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
link150<-link150[,..needlist1]

link150u<- uniqueresult(link150)
link150d <- doubleresult(link150)

epc <- matchleft(epc,link150)
####################method 151####################
function151<- function(x,y){
  x$bnstreet <- paste(x$subbuildingname,x$buildingname,sep=",")
  x$bnstreet <- paste(x$bnstreet,x$paotext,sep=" ")
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
link151d <- doubleresult(link151)

epc <- matchleft(epc,link151)
####################method 152####################
function152<- function(x,y){
  x$bnstreet <- paste(x$saotext,x$paostartnumber,sep=",")
  x$bnstreet <- paste(x$bnstreet,x$paoendnumber,sep="-")
  x$bnstreet <- paste(x$bnstreet,x$paotext,sep=",")
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
link152d <- doubleresult(link152)

epc <- matchleft(epc,link152)
####################method 153####################
function153<- function(x,y){
  x$bnstreet <- paste(x$subbuildingname,x$buildingnumber,sep=",")
  x$bnstreet <- paste(x$bnstreet,x$streetdescription,sep=" ")
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
link153<-link153[,..needlist1]

link153u<- uniqueresult(link153)
link153d <- doubleresult(link153)

epc <- matchleft(epc,link153)
####################method 154####################
function154<- function(x,y){
  x$bnstreet <- paste(x$saotext,x$paotext,sep=",")
  x$bnstreet <- paste(x$bnstreet,x$paostartnumber,sep=",")
  x$bnstreet <- paste(x$bnstreet,x$paoendnumber,sep="-")
  x$bnstreet <- paste(x$bnstreet,x$streetdescription,sep=" ")
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
link154<-link154[,..needlist1]

link154u<- uniqueresult(link154)
link154d <- doubleresult(link154)

epc<- matchleft(epc,link154)
####################method 155####################
function155<- function(x,y){
  x<-x[x$subbuildingname=="",]
  x$bnstreet <- paste(x$saotext,x$paotext,sep=",")
  x$bnstreet <- paste(x$bnstreet,x$ss,sep=",")
  x$bnstreet <- paste(x$bnstreet,x$streetdescription,sep=" ")
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
link155<-link155[,..needlist1]

link155u<- uniqueresult(link155)
link155d <- doubleresult(link155)

epc <- matchleft(epc,link155)
####################method 156####################
function156<- function(x,y){
  x$bnstreet <- paste(x$saotext,x$paotext,sep=",")
  x$bnstreet <- paste(x$bnstreet,x$ss,sep=",")
  x$bnstreet <- paste(x$bnstreet,x$streetdescription,sep=" ")
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
link156<-link156[,..needlist1]

link156u<- uniqueresult(link156)
link156d <- doubleresult(link156)

epc <- matchleft(epc,link156)
####################method 157####################
function157<- function(x,y){
  x$bnstreet <- paste(x$saotext,x$paotext,sep=",")
  x$bnstreet <- paste(x$bnstreet,x$paostartnumber,sep=",")
  x$bnstreet <- paste(x$bnstreet,x$paoendnumber,sep="-")
  x$bnstreet <- paste(x$bnstreet,x$streetdescription,sep=" ")
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
link157<-link157[,..needlist1]

link157u<- uniqueresult(link157)
link157d <- doubleresult(link157)

epc <- matchleft(epc,link157)
####################method 158####################
function158<- function(x,y){
  x$bnstreet <- paste(x$saotext,x$paotext,sep=",")
  x$bnstreet <- paste(x$bnstreet,x$paostartnumber,sep=",")
  x$bnstreet <- paste(x$bnstreet,x$paoendnumber,sep="-")
  x$bnstreet <- paste(x$bnstreet,x$streetdescription,sep=" ")
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
link158<-link158[,..needlist1]

link158u<- uniqueresult(link158)
link158d <- doubleresult(link158)

epc <- matchleft(epc,link158)
####################method 159####################
function159<- function(x,y){
  x$bnstreet <- paste(x$saotext,x$paotext,sep=",")
  x$bnstreet <- paste(x$bnstreet,x$pp,sep=",")
  x$bnstreet <- paste(x$bnstreet,x$streetdescription,sep=" ")
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
link159d <- doubleresult(link159)

epc <- matchleft(epc,link159)
####################method 160####################
function160<- function(x,y){
  x$bnstreet <- paste(x$subbuildingname,x$paotext,sep=",")
  x$bnstreet <- paste(x$bnstreet,x$pp,sep=",")
  x$bnstreet <- paste(x$bnstreet,x$streetdescription,sep=" ")
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
link160<-link160[,..needlist1]

link160u<- uniqueresult(link160)
link160d <- doubleresult(link160)

epc <- matchleft(epc,link160)
####################method 161####################
function161<- function(x,y){
  x<-x[x$subbuildingname=="",]
  x$bnstreet <- paste(x$saotext,x$paotext,sep=",")
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
link161d <- doubleresult(link161)

epc <- matchleft(epc,link161)
####################method 162####################
function162<- function(x,y){
  x$bnstreet <- paste(x$saotext,x$paotext,sep=",")
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
link162d <- doubleresult(link162)

epc <- matchleft(epc,link162)
####################method 163####################
function163<- function(x,y){
  x$bnstreet <- paste(x$subbuildingname,x$buildingnumber,sep=",")
  x$bnstreet <- paste(x$bnstreet,x$paotext ,sep=" ")
  x$bnstreet <- gsub("/", "", x$bnstreet)
  x$bnstreet <- gsub("[.]", "", x$bnstreet)
  x$bnstreet <- gsub("[']", "", x$bnstreet)
  x$bnstreet <- gsub(" ", "", x$bnstreet)
  x$addressf <-paste(x$postcodelocator,x$bnstreet,sep=",")
  
  y$addressfinal <- paste(y$add1, y$add2,sep=",")
  y$addressfinal <- gsub("[/]", "", y$addressfinal)
  y$addressfinal <- gsub("[.]", "", y$addressfinal)
  y$addressfinal <- gsub("[']", "", y$addressfinal)
  y$addressfinal <- gsub(" ", "", y$addressfinal)
  y$addressf <- paste(y$postcode,y$addressfinal,sep=",")
  
  taba1 <- inner_join(x,y,by="addressf")
  return(taba1)
}
link163<-function163(add,epc)
link163<-link163[,..needlist1]

link163u<- uniqueresult(link163)
link163d <- doubleresult(link163)

epc <- matchleft(epc,link163)
####################method 164####################
function164<- function(x,y){
  x$bnstreet <- paste(x$saotext,x$paotext,sep=",")
  x$bnstreet <- paste(x$bnstreet,x$streetdescription,sep=",")
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
link164d <- doubleresult(link164)

epc<- matchleft(epc,link164)
####################method 165####################
function165<- function(x,y){
  x$bnstreet <- paste(x$saotext,x$paotext,sep=" ")
  x$bnstreet <- paste(x$bnstreet,x$paostartnumber,sep=",")
  x$bnstreet <- paste(x$bnstreet,x$paoendnumber,sep="-")
  x$bnstreet <- paste(x$bnstreet,x$streetdescription,sep=",")
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
link165d <- doubleresult(link165)

epc <- matchleft(epc,link165)
####################method 166####################
function166<- function(x,y){
  x$bnstreet <- paste(x$subbuildingname,x$paotext,sep=" ")
  x$bnstreet <- paste(x$bnstreet,x$paostartnumber,sep=",")
  x$bnstreet <- paste(x$bnstreet,x$paoendnumber,sep="-")
  x$bnstreet <- paste(x$bnstreet,x$streetdescription,sep=",")
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
link166<-link166[,..needlist1]

link166u<- uniqueresult(link166)
link166d <- doubleresult(link166)

epc <- matchleft(epc,link166)
####################section sum up####################
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

l134_166dl = list(link134d,link135d,link136d,link137d,link138d,link139d,link140d,link141d,link142d,link143d,link144d,link145d,link146d,link147d,link148d,link149d,link150d,link151d,link152d,link153d,link154d,link155d,link156d,link157d,link158d,link159d,link160d,link161d,link162d,link163d,link164d,link165d,link166d)
link134_166d<- rbindlist(l134_166dl, use.names=TRUE, fill=TRUE)

dbWriteTable(con, "link134_166dnew",value =link134_166d, append = TRUE, row.names = FALSE)
dbWriteTable(con, "link134_166unew",value =link134_166u, append = TRUE, row.names = FALSE)

rm(link134u,link135u,link136u,link137u,link138u,link139u,link140u,link141u,link142u,link143u,link144u,link145u,link146u,link147u,link148u,link149u,link150u,link151u,link152u,link153u,link154u,link155u,link156u,link157u,link158u,link159u,link160u,link161u,link162u,link163u,link164u,link165u,link166u)
rm(link134d,link135d,link136d,link137d,link138d,link139d,link140d,link141d,link142d,link143d,link144d,link145d,link146d,link147d,link148d,link149d,link150d,link151d,link152d,link153d,link154d,link155d,link156d,link157d,link158d,link159d,link160d,link161d,link162d,link163d,link164d,link165d,link166d)
rm(link134,link135,link136,link137,link138,link139,link140,link141,link142,link143,link144,link145,link146,link147,link148,link149,link150,link151,link152,link153,link154,link155,link156,link157,link158,link159,link160,link161,link162,link163,link164,link165,link166)
rm(function134,function135,function136,function137,function138,function139,function140,function141,function142,function143,function144,function145,function146,function147,function148,function149,function150,function151,function152,function153,function154,function155,function156,function157,function158,function159,function160,function161,function162,function163,function164,function165,function166)
rm(l134_166l,l134_166dl)
####################method 167####################
function167<- function(x,y){
  x <- x[!grepl("^\\d",x$buildingname),]
  x$bnstreet <- paste(x$subbuildingname,x$buildingname,sep=" ")
  x$bnstreet <- paste(x$bnstreet,x$streetdescription,sep=",")
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
link167<-link167[,..needlist1]

link167u<- uniqueresult(link167)
link167d <- doubleresult(link167)
 
epc <- matchleft(epc,link167)
####################method 168####################
function168<- function(x,y){
  x <- x[!grepl("^\\d",x$buildingname),]
  x$bnstreet <- paste(x$subbuildingname,x$buildingname,sep=" ")
  x$bnstreet <- paste(x$bnstreet,x$streetdescription,sep=",")
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
link168<-link168[,..needlist1]

link168u<- uniqueresult(link168)
link168d <- doubleresult(link168)

epc <- matchleft(epc,link168)
####################method 169####################
function169<- function(x,y){
  x$bnstreet <- x$paotext
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
link169<-link169[,..needlist1]

link169u<- uniqueresult(link169)
link169d <- doubleresult(link169)

epc <- matchleft(epc,link169)
####################method 170####################
function170<- function(x,y){
  x$bnstreet <- paste(x$subbuildingname,x$buildingname,sep=",")
  x$bnstreet <- paste(x$bnstreet,x$streetdescription,sep=",")
  x$bnstreet <- gsub("/", "", x$bnstreet)
  x$bnstreet <- gsub("[.]", "", x$bnstreet)
  x$bnstreet <- gsub("[']", "", x$bnstreet)
  x$bnstreet <- gsub(" ", "", x$bnstreet)
  x$addressf <-paste(x$postcodelocator,x$bnstreet,sep=",")
  
  y$addressfinal <- y$add1
  y$addressfinal <- gsub("[.]", "", y$addressfinal)
  y$addressfinal <- gsub("[']", "", y$addressfinal)
  y$addressfinal <- gsub("/", "", y$addressfinal)
  y$addressfinal <- gsub(" ", "", y$addressfinal)
  y$addressf <- paste(y$postcode,y$addressfinal,sep=",")
  
  taba1 <- inner_join(x,y,by="addressf")
  return(taba1)
}
link170<-function170(add,epc)
link170<-link170[,..needlist1]

link170u<- uniqueresult(link170)

link170d <- doubleresult(link170)

epc <- matchleft(epc,link170)
####################method 171####################
function171<- function(x,y){

  x$bnstreet <- paste(x$saotext ,x$pp,sep=",")
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
link171d <- doubleresult(link171)

epc <- matchleft(epc,link171)
####################method 172####################
function172<- function(x,y){
  #x<-x[x$paostartsuffix=="",]
  x$bnstreet <- paste(x$saotext ,x$pp,sep=",")
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
link172d <- doubleresult(link172)

epc <- matchleft(epc,link172)
####################method 173####################
function173<- function(x,y){
  x<-x[x$paostartsuffix=="",]
  x$bnstreet <- paste(x$buildingnumber,x$streetdescription,sep=" ")
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
link173<-link173[,..needlist1]

link173u<- uniqueresult(link173)
link173d <- doubleresult(link173)

epc <- matchleft(epc,link173)
####################method 174####################
function174<- function(x,y){
  x<-x[x$subbuildingname=="",]
  x$bnstreet <- paste(x$buildingnumber,x$streetdescription,sep=",")
  x$bnstreet <- paste(x$bnstreet,x$locality,sep=",")
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
link174d <- doubleresult(link174)

epc<- matchleft(epc,link174)
####################method 175####################
function175<- function(x,y){
  x<-x[x$subbuildingname=="",]
  x$bnstreet <- paste(x$ss,x$streetdescription,sep=",")
  x$bnstreet <- paste(x$bnstreet,x$locality,sep=",")
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
link175d <- doubleresult(link175)

epc <- matchleft(epc,link175)
####################method 176####################
function176<- function(x,y){
  x$bnstreet <- paste(x$ss,x$paotext,sep=" ")
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
link176<-link176[,..needlist1]

link176u<- uniqueresult(link176)
link176d <- doubleresult(link176)

epc <- matchleft(epc,link176)
####################method 177####################
function177<- function(x,y){
  x$bnstreet <- paste(x$subbuildingname,x$buildingname ,sep=",")
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
link177<-link177[,..needlist1]

link177u<- uniqueresult(link177)
link177d <- doubleresult(link177)

epc <- matchleft(epc,link177)
####################method 178####################
function178<- function(x,y){
  x$bnstreet <- paste(x$buildingnumber,x$streetdescription,sep=" ")
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
link178<-link178[,..needlist1]

link178u<- uniqueresult(link178)
link178d <- doubleresult(link178)

epc <- matchleft(epc,link178)
####################method 179####################
function179<- function(x,y){
  x$bnstreet <- paste(x$subbuildingname,x$pp ,sep=",")
  x$bnstreet <- paste(x$bnstreet,x$paotext ,sep=" ")
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
link179d <- doubleresult(link179)

epc <- matchleft(epc,link179)
####################method 180####################
function180<- function(x,y){
  x <- x[!grepl("^\\d",x$buildingnumber),]
  #buildingname is not a number
  x$bnstreet <- paste(x$saotext,x$paotext,sep=" ")
  x$bnstreet <- paste(x$bnstreet,x$streetdescription,sep=",")
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

link180u<- uniqueresult(link180)
link180d <- doubleresult(link180)

epc <- matchleft(epc,link180)
####################method 181####################
function181<- function(x,y){
  x$bnstreet <- paste(x$subbuildingname,x$buildingnumber,sep=",")
  x$streetdescription <- gsub("-", "", x$streetdescription)
  x$bnstreet <- paste(x$bnstreet,x$streetdescription,sep=",")
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
link181d <- doubleresult(link181)

epc <- matchleft(epc,link181)
####################method 182####################
function182<- function(x,y){
  x$bnstreet <- paste(x$subbuildingname,x$buildingname,sep=" ")
  x$bnstreet <- paste(x$bnstreet,x$buildingnumber,sep=",")
  x$bnstreet <- paste(x$bnstreet,x$streetdescription,sep=" ")
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

link182u<- uniqueresult(link182)
link182d <- doubleresult(link182)

epc <- matchleft(epc,link182)
####################method 183####################
function183<- function(x,y){
  x$bnstreet <- paste(x$subbuildingname,x$buildingnumber,sep=",")
  x$bnstreet <- paste(x$bnstreet,x$paotext,sep=",")
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
link183<-link183[,..needlist1]

link183u<- uniqueresult(link183)
link183d <- doubleresult(link183)

epc <- matchleft(epc,link183)
####################method 184####################
function184<- function(x,y){
  x$bnstreet <- paste(x$saotext,x$buildingnumber,sep=",")
  x$bnstreet <- paste(x$bnstreet,x$paotext,sep=",")
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
link184d <- doubleresult(link184)

epc<- matchleft(epc,link184)
#################### method 185 ##################
function185<- function(x,y){
  x$bnstreet <- paste(x$subbuildingname,x$ss,sep=",")
  x$bnstreet <- paste(x$bnstreet,x$paotext,sep=",")
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
link185d <- doubleresult(link185)

epc <- matchleft(epc,link185)
####################method 186####################
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
link186<-link186[,..needlist1]

link186u<- uniqueresult(link186)
link186d <- doubleresult(link186)

epc <- matchleft(epc,link186)
####################method 187####################
function187<- function(x,y){
  x$bnstreet <- paste(x$saotext,x$ss,sep=",")
  x$bnstreet <- paste(x$bnstreet,x$paotext,sep=" ")
  x$bnstreet <- paste(x$bnstreet,x$pp,sep=",")
  x$bnstreet <- paste(x$bnstreet,x$streetdescription,sep=" ")
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
link187<-link187[,..needlist1]

link187u<- uniqueresult(link187)
link187d <- doubleresult(link187)

epc <- matchleft(epc,link187)
####################method 188####################
function188<- function(x,y){
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
link188<-link188[,..needlist1]

link188u<- uniqueresult(link188)
link188d <- doubleresult(link188)

epc <- matchleft(epc,link188)
####################method 189####################
function189<- function(x,y){
  x$bnstreet <- paste(x$saotext,x$pp,sep=" ")
  x$bnstreet <- paste(x$bnstreet,x$paotext,sep=" ")
  x$bnstreet <- gsub("/", "", x$bnstreet)
  x$bnstreet <- gsub("[.]", "", x$bnstreet)
  x$bnstreet <- gsub("[']", "", x$bnstreet)
  x$addressf <-paste(x$postcodelocator,x$bnstreet,sep=",")
  
  y$addressfinal <-  paste(y$add1,y$add2,sep="")
  y$addressfinal <- gsub("[/]", "", y$addressfinal)
  y$addressfinal <- gsub("[.]", "", y$addressfinal)
  y$addressfinal <- gsub("[']", "", y$addressfinal)
  y$addressf <- paste(y$postcode,y$addressfinal,sep=",")
  
  taba1 <- inner_join(x,y,by="addressf")
  return(taba1) 
}
link189<-function189(add,epc)
link189<-link189[,..needlist1]

link189u<- uniqueresult(link189)
link189d <- doubleresult(link189)

epc <- matchleft(epc,link189)
####################method 190####################
function190<- function(x,y){
  x$bnstreet <- paste(x$buildingnumber,x$dependentlocality,sep=",")
  x$bnstreet <- gsub("[.]", "", x$bnstreet)
  x$bnstreet <- gsub("[']", "", x$bnstreet)
  x$bnstreet <- gsub(" ", "", x$bnstreet)
  x$addressf <-paste(x$postcodelocator,x$bnstreet,sep=",")
  
  y$addressfinal <- paste(y$add)
  y$bnstreet <- gsub("[.]", "",  y$bnstreet)
  y$bnstreet <- gsub("[']", "",  y$bnstreet)
  y$addressfinal <- gsub(" ", "", y$addressfinal)
  y$addressf <- paste(y$postcode,y$addressfinal,sep=",")
  
  taba1 <- inner_join(x,y,by="addressf")
  return(taba1)
}
link190<-function190(add,epc)
link190<-link190[,..needlist1]

link190u<- uniqueresult(link190)
link190d <- doubleresult(link190)

epc <- matchleft(epc,link190)
####################method 191####################
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
link191d <- doubleresult(link191)

epc <- matchleft(epc,link191)
####################method 192####################
function192<- function(x,y){
  x$bnstreet <- paste(x$saotext,x$saostartnumber,sep=",")
  x$bnstreet <- paste(x$bnstreet,x$saoendnumber,sep="-")
  x$bnstreet <- paste(x$bnstreet,x$paotext,sep=" ")
  x$bnstreet <- paste(x$bnstreet,x$streetdescription,sep=",")
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
link192d <- doubleresult(link192)

epc <- matchleft(epc,link192)
####################method 193####################
function193<- function(x,y){
  x$bnstreet <- paste(x$saotext,x$saostartnumber,sep=",")
  x$bnstreet <- paste(x$bnstreet,x$saoendnumber,sep="-")
  x$bnstreet <- paste(x$bnstreet,x$paotext,sep=" ")
  x$bnstreet <- paste(x$bnstreet,x$streetdescription,sep=",")
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
link193<-link193[,..needlist1]

link193u<- uniqueresult(link193)
link193d <- doubleresult(link193)

epc <- matchleft(epc,link193)
####################method 194####################
function194<- function(x,y){
  x$bnstreet <- paste(x$saotext,x$saostartnumber,sep=",")
  x$bnstreet <- paste(x$bnstreet,x$saoendnumber,sep="-")
  x$bnstreet <- paste(x$bnstreet,x$paotext,sep=" ")
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
link194d <- doubleresult(link194)

epc<- matchleft(epc,link194)
####################method 195####################
function195<- function(x,y){
  x$bnstreet <- paste(x$saotext,x$buildingname,sep=",")
  x$bnstreet <- paste(x$bnstreet,x$paotext,sep=" ")
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
link195d <- doubleresult(link195)

epc <- matchleft(epc,link195)
####################method 196####################
function196<- function(x,y){
  x$bnstreet <- paste(x$subbuildingname,x$buildingnumber,sep=",")
  x$bnstreet <- paste(x$bnstreet,x$streetdescription,sep=" ")
  x$bnstreet <- gsub(" ", "", x$bnstreet)
  x$addressf <-paste(x$postcodelocator,x$bnstreet,sep=",")

  y<-y[y$property_type=="Flat",]
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
link196<-link196[,..needlist1]

link196u<- uniqueresult(link196)
link196d <- doubleresult(link196)

epc <- matchleft(epc,link196)
####################method 197####################
function197<- function(x,y){
  x$bnstreet <- paste(x$subbuildingname,x$buildingnumber,sep=",")
  x$bnstreet <- paste(x$bnstreet,x$streetdescription,sep=" ")
  x$bnstreet <- gsub(" ", "", x$bnstreet)
  x$addressf <-paste(x$postcodelocator,x$bnstreet,sep=",")
  
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
link197<-link197[,..needlist1]

link197u<- uniqueresult(link197)
link197d <- doubleresult(link197)
 
epc <- matchleft(epc,link197)
####################method 198####################
function198<- function(x,y){
  x$bnstreet <- paste("FLAT ",x$saostartnumber,sep="")
  x$bnstreet <- paste(x$bnstreet,x$saostartsuffix ,sep="")
  #x$bnstreet <- str_replace_all(x$bnstreet, "NA", "")
  x$bnstreet <- paste(x$bnstreet,x$ paostartnumber,sep=",")
  x$bnstreet <- paste(x$bnstreet,x$ paoendnumber,sep="-")
  x$bnstreet <- paste(x$bnstreet,x$ streetdescription,sep=" ")
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
link198<-link198[,..needlist1]

link198u<- uniqueresult(link198)
link198d <- doubleresult(link198)

epc <- matchleft(epc,link198)
####################method 199####################
function199<- function(x,y){
  x$bnstreet <- paste("FLAT ",x$saostartnumber,sep="")
  x$bnstreet <- paste(x$bnstreet,x$saostartsuffix ,sep="")
  #x$bnstreet <- str_replace_all(x$bnstreet, "NA", "")
  x$bnstreet <- paste(x$bnstreet,x$ paostartnumber,sep=",")
  x$bnstreet <- paste(x$bnstreet,x$ paoendnumber,sep="-")
  x$bnstreet <- paste(x$bnstreet,x$ streetdescription,sep=" ")
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
link199d <- doubleresult(link199)

epc <- matchleft(epc,link199)
####################method 200####################
function200<- function(x,y){
  x$bnstreet <- gsub("FLAT", "APARTMENT", x$subbuildingname) 
  x$bnstreet <- paste(x$bnstreet,x$  buildingname,sep=" ")
  x$bnstreet <- paste(x$bnstreet,x$ streetdescription,sep=",")
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
link200<-link200[,..needlist1]

link200u<- uniqueresult(link200)
link200d <- doubleresult(link200)

epc <- matchleft(epc,link200)
####################method 201####################
function201<- function(x,y){
  x$bnstreet <- paste( "APARTMENT", x$saotext) 
  x$bnstreet <- paste(x$bnstreet,x$ streetdescription,sep=",")
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
link201d <- doubleresult(link201)

epc <- matchleft(epc,link201)
####################method 202####################
function202<- function(x,y){
  x$bnstreet <- beg2char(x$saotext, " ")
  x$bnstreet <- paste("APARTMENT ",x$bnstreet,sep="")
  x$bnstreet <- paste(x$bnstreet,x$streetdescription,sep=",")
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
link202d <- doubleresult(link202)

epc <- matchleft(epc,link202)
####################method 203####################
function203<- function(x,y){
  x<-x[x$paostartsuffix=="",]
  x$bnstreet <- paste(x$subbuildingname,x$paostartnumber,sep=",")
  x$bnstreet <- paste(x$bnstreet,x$streetdescription,sep=" ")
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
link203<-link203[,..needlist1]

link203u<- uniqueresult(link203)
link203d <- doubleresult(link203)

epc <- matchleft(epc,link203)
####################method 204####################
function204<- function(x,y){
  x$bnstreet <- paste(x$subbuildingname,x$buildingname ,sep=" ")
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
link204d <- doubleresult(link204)

epc<- matchleft(epc,link204)
####################method 205####################
function205<- function(x,y){
  x <- x[grepl("FLAT\\s[A-Z]$",x$saotext),]
  x<-x[x$paostartsuffix=="",]
  x$add1ff <- gsub("FLAT ", "", x$saotext)
  x$bnstreet <- paste(x$paostartnumber,x$add1ff,sep="")
  x$bnstreet <- paste(x$bnstreet,x$streetdescription,sep=" ")
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

link205u<- uniqueresult(link205)
link205d <- doubleresult(link205)

epc <- matchleft(epc,link205)
####################method 206####################
function206<- function(x,y){
  x$bnstreet <- paste(x$saotext,x$pp,sep=",")
  x$bnstreet <- paste(x$bnstreet,x$streetdescription,sep=" ")
  x$addressf <-paste(x$postcodelocator,x$bnstreet,sep=",")
  
  y<-y[y$property_type=="Flat"|y$property_type=="Maisonette",]
  y$addressfinal <-  paste("ROOM ",y$add1,sep="")
  y$addressfinal <-  paste(y$addressfinal,y$add3,sep=",")
  y$addressf <- paste(y$postcode,y$addressfinal,sep=",")
  
  taba1 <- inner_join(x,y,by="addressf")
  return(taba1)
}
link206<-function206(add,epc)
link206<-link206[,..needlist1]

link206u<- uniqueresult(link206)
link206d <- doubleresult(link206)

epc <- matchleft(epc,link206)
####################method 207####################
function207<- function(x,y){
  x$bnstreet <- paste(x$subbuildingname,x$buildingname,sep=" ")
  x$bnstreet <- paste(x$bnstreet,x$streetdescription,sep=",")
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
link207<-link207[,..needlist1]

link207u<- uniqueresult(link207)
link207d <- doubleresult(link207)

epc <- matchleft(epc,link207)
#################### method 208 ##################
function208<- function(x,y){
  x$bnstreet <- paste(x$subbuildingname,x$buildingname,sep=" ")
  x$bnstreet <- paste(x$bnstreet,x$streetdescription,sep=",")
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
link208<-link208[,..needlist1]

link208u<- uniqueresult(link208)
link208d <- doubleresult(link208)
 
epc <- matchleft(epc,link208)
#################### method 209 ##################
function209<- function(x,y){
  x$bnstreet <- paste(x$saotext,x$paotext,sep=",")
  x$bnstreet <- paste(x$bnstreet,x$paostartnumber ,sep=",")
  x$bnstreet <- paste(x$bnstreet,x$paostartsuffix ,sep="")
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
link209d <- doubleresult(link209)

epc <- matchleft(epc,link209)
####################method 210####################
function210<- function(x,y){
  x$bnstreet <- paste(x$subbuildingname,x$buildingname,sep=",")
  x$bnstreet <- paste(x$bnstreet,x$streetdescription,sep=",")
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
link210<-link210[,..needlist1]

link210u<- uniqueresult(link210)
link210d <- doubleresult(link210)

epc <- matchleft(epc,link210)
####################method 211####################
function211<- function(x,y){

  x$bnstreet <- paste(x$saotext,x$paotext,sep="")
  x$bnstreet <- paste(x$bnstreet,x$streetdescription,sep=",")
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
link211d <- doubleresult(link211)

epc <- matchleft(epc,link211)
####################method 212####################
function212<- function(x,y){
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
link212d <- doubleresult(link212)

epc <- matchleft(epc,link212)
####################method 213####################
function213<- function(x,y){
  x$bnstreet <- paste(x$saotext,x$pp,sep=",")
  x$bnstreet <- paste(x$bnstreet,x$streetdescription,sep=" ")
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
link213<-link213[,..needlist1]

link213u<- uniqueresult(link213)
link213d <- doubleresult(link213)

epc <- matchleft(epc,link213)
#################### method 214  ##################
function214<- function(x,y){
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
link214<-link214[,..needlist1]

link214u<- uniqueresult(link214)
link214d <- doubleresult(link214)

epc<- matchleft(epc,link214)
####################method 215####################
function215<- function(x,y){
  x$bnstreet <-    paste(x$saotext,x$paotext,sep=" ")
  x$bnstreet <-    paste(x$bnstreet,x$streetdescription,sep=",")
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
link215d <- doubleresult(link215)
epc <- matchleft(epc,link215)
####################method 216####################
function216<- function(x,y){
  x$bnstreet <-    paste(x$subbuildingname,x$buildingnumber,sep=",")
  x$bnstreet <-    paste(x$bnstreet,x$streetdescription,sep=" ")
  x$bnstreet <- gsub("[.]", "", x$bnstreet)
  x$bnstreet <- gsub("[']", "", x$bnstreet)
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
link216<-link216[,..needlist1]

link216u<- uniqueresult(link216)
link216d <- doubleresult(link216)

epc <- matchleft(epc,link216)
####################method 217####################
function217<- function(x,y){
  x$bnstreet <- paste("FLAT ",x$buildingnumber,sep="")
  x$bnstreet <- paste(x$bnstreet,x$streetdescription,sep=",")
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
link217<-link217[,..needlist1]

link217u<- uniqueresult(link217)
link217d <- doubleresult(link217)
 
epc <- matchleft(epc,link217)
####################method 218####################
function218<- function(x,y){
  x<-x[x$subbuildingname!="",]
  x$bnstreet <- paste(x$subbuildingname,x$streetdescription,sep=",")
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
link218<-link218[,..needlist1]

link218u<- uniqueresult(link218)
link218d <- doubleresult(link218)

epc<- matchleft(epc,link218)
####################method 219####################
function219<- function(x,y){
  x<-x[x$saotext!="",]
  x$bnstreet <- paste(x$saotext,x$streetdescription,sep=",")
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
link219d <- doubleresult(link219)

epc <- matchleft(epc,link219)
####################method 220####################
function220<- function(x,y){
  x$bnstreet <- paste(x$subbuildingname,x$buildingnumber,sep=",")
  x$bnstreet <- paste(x$bnstreet,x$streetdescription,sep=" ")
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
link220<-link220[,..needlist1]

link220u<- uniqueresult(link220)
link220d <- doubleresult(link220)

epc <- matchleft(epc,link220)
####################method 221####################
function221<- function(x,y){
  x<-x[x$subbuildingname!="",]
  x$bnstreet <- paste(x$subbuildingname,x$streetdescription,sep=",")
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
link221d <- doubleresult(link221)

epc <- matchleft(epc,link221)
####################method 222####################
function222<- function(x,y){
  x$bnstreet <- paste(x$ss,x$paotext,sep=",")
  x$bnstreet <- paste(x$bnstreet,x$paostartnumber,sep=",")
  x$bnstreet <- paste(x$bnstreet,x$paoendnumber,sep="-")
  x$bnstreet <- paste(x$bnstreet,x$streetdescription,sep=" ")
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

link222u<- uniqueresult(link222)
link222d <- doubleresult(link222)

epc <- matchleft(epc,link222)
####################method 223####################
function223<- function(x,y){
  x$bnstreet <- paste(x$saotext,x$paotext,sep=",")
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
link223<-link223[,..needlist1]

link223u<- uniqueresult(link223)
link223d <- doubleresult(link223)

epc <- matchleft(epc,link223)
####################method 224####################
function224<- function(x,y){
  x$bnstreet <- paste(x$pp,x$streetdescription,sep=",")
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
link224d <- doubleresult(link224)
epc<- matchleft(epc,link224)
####################method 225####################
function225<- function(x,y){
  x$bnstreet <- paste("APARTMENT ",x$ss,sep=" ")
  x$bnstreet <- paste(x$bnstreet,x$paotext,sep=",")
  x$bnstreet <- paste(x$bnstreet,x$pp,sep=",")
  x$bnstreet <- paste(x$bnstreet,x$streetdescription,sep=" ")
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
link225d <- doubleresult(link225)

epc <- matchleft(epc,link225)
####################method 226####################
function226<- function(x,y){
  x <- x[!grepl("^\\d",x$buildingname),]
  x$bnstreet <- paste(x$subbuildingname,x$buildingname,sep=" ")
  x$bnstreet <- paste(x$bnstreet,x$streetdescription,sep=" ")
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
link226<-link226[,..needlist1]

link226u<- uniqueresult(link226)
link226d <- doubleresult(link226)

epc <- matchleft(epc,link226)
####################method 227####################
function227<- function(x,y){
  x$bnstreet <- paste("FLAT ",x$subbuildingname,sep=" ")
  x$bnstreet <- paste(x$bnstreet,x$paotext,sep=",")
  x$bnstreet <- paste(x$bnstreet,x$buildingnumber,sep=",")
  x$bnstreet <- paste(x$bnstreet,x$streetdescription,sep=" ")
  x$bnstreet <- gsub(",", "", x$bnstreet)
  x$bnstreet <- gsub(" ", "", x$bnstreet)
  x$addressf <-paste(x$postcodelocator,x$bnstreet,sep=",")
  
  y<-y[y$property_type=="Flat"|y$property_type=="Maisonette",]
  #add1 is not end with number 
  y <- y[!grepl("\\d$",y$add1),]
  y$addressfinal <-  trimws(y$add)
  y$addressfinal <- gsub(",", "", y$addressfinal)
  y$addressfinal <- gsub(" ", "", y$addressfinal)
  y$addressf <- paste(y$postcode,y$addressfinal,sep=",")
  
  taba1 <- inner_join(x,y,by="addressf")
  
  return(taba1)
}
link227<-function227(add,epc)
link227<-link227[,..needlist1]

link227u<- uniqueresult(link227)
link227d <- doubleresult(link227)

epc <- matchleft(epc,link227)
####################method 228####################
function228<- function(x,y){
  x1<-x[postcodelocator=="M3 6FZ",]
  setDT(x1)
  x1$bnstreet <- gsub("APARTMENT ", "", x1$saotext)
  x1$bnstreet <- paste(x1$buildingname, x1$bnstreet,sep="")
  x1$bnstreet1<- word(x1$bnstreet , 1  , -2)
  x1$bnstreet2<- word(x1$bnstreet,-1)
  x1$bnstreet3 <- paste(x1$bnstreet2,x1$bnstreet1,sep=" ")
  x1$addressf <-paste(x1$postcodelocator,x1$bnstreet3,sep=",")
  
  y<-y[y$property_type=="Flat"|y$property_type=="Maisonette",]
  y$addressfinal <- beg2char(y$add, ",")  
  #y$addressfinal <- gsub(" ", "", y$addressfinal)
  y$addressf <- paste(y$postcode,y$addressfinal,sep=",")
  taba1 <- inner_join(x1,y,by="addressf")
  
  return(taba1)
}
link228<-function228(add,epc)
link228<-link228[,..needlist1]

link228u<- uniqueresult(link228)
link228d <- doubleresult(link228)

epc <- matchleft(epc,link228)
####################method 229####################
function229<- function(x,y){
  x$bnstreet <- paste("FLAT ",x$ss,sep="")
  x$bnstreet <- paste(x$bnstreet,x$pp,sep=" ")
  x$bnstreet <- paste(x$bnstreet,x$streetdescription,sep=" ")
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
link229d <- doubleresult(link229)

epc <- matchleft(epc,link229)
####################method 230####################
function230<- function(x,y){
  x<-x[x$subbuildingname!="",]
  x$bnstreet <- gsub("FLAT", "APT", x$subbuildingname)
  x$bnstreet <- paste(x$bnstreet,x$buildingname,sep=" ")
  x$bnstreet <- paste(x$bnstreet,x$buildingnumber,sep=" ")
  x$bnstreet <- paste(x$bnstreet,x$streetdescription,sep=" ")
  x$addressf <-paste(x$postcodelocator,x$bnstreet,sep=",")
  
  y<-y[y$property_type=="Flat"|y$property_type=="Maisonette",]
  y$addressfinal <-  trimws(y$add)
  y$addressf <- paste(y$postcode,y$addressfinal,sep=",")
  
  taba1 <- inner_join(x,y,by="addressf")
  
  return(taba1)
}
link230<-function230(add,epc)
link230<-link230[,..needlist1]

link230u<- uniqueresult(link230)
link230d <- doubleresult(link230)

epc <- matchleft(epc,link230)
####################method 231####################
function231<- function(x,y){
  #x$bnstreet <- x$subbuildingname
  x$bnstreet <- paste(x$subbuildingname,x$buildingname,sep=" ")
  x$bnstreet <- paste(x$bnstreet,x$buildingnumber,sep=" ")
  x$bnstreet <- paste(x$bnstreet,x$streetdescription,sep=" ")
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
link231<-link231[,..needlist1]

link231u<- uniqueresult(link231)
link231d <- doubleresult(link231)

epc <- matchleft(epc,link231)
####################method 232####################
function232<- function(x,y){
  x<-x[x$buildingname!="",]
  x$bnstreet <- paste(x$buildingname,x$streetdescription,sep=" ")
  x$bnstreet<-  trimws(x$bnstreet) 
  x$addressf <-paste(x$postcodelocator,x$bnstreet,sep=",")
  
  y$addressfinal <-  trimws(y$add)
  y$addressfinal <-  trimws(y$addressfinal) 
  y$addressf <- paste(y$postcode,y$addressfinal,sep=",")
  
  taba1 <- inner_join(x,y,by="addressf")
  return(taba1)
}
link232<-function232(add,epc)
link232<-link232[,..needlist1]

link232u<- uniqueresult(link232)
link232d <- doubleresult(link232)

epc <- matchleft(epc,link232)
####################method 233####################
function233<- function(x,y){
  x$bnstreet <- paste(x$buildingnumber,x$locality,sep=",")
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
link233<-link233[,..needlist1]

link233u<- uniqueresult(link233)
link233d <- doubleresult(link233)

epc <- matchleft(epc,link233)
####################method 234####################
function234<- function(x,y){
  x$bnstreet <- paste(x$paotext,x$townname,sep=",")
  x$bnstreet <- gsub(",", "", x$bnstreet)
  x$bnstreet <- gsub("[.]", "", x$bnstreet)
  x$bnstreet <- gsub("[/]", "", x$bnstreet)
  x$bnstreet <- gsub("[']", "", x$bnstreet)
  x$bnstreet <- gsub(" ", "", x$bnstreet)
  x$addressf <-paste(x$postcodelocator,x$bnstreet,sep=",")
  
 
  y$addressfinal <- trimws(y$add)
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
link234d <- doubleresult(link234)
 
epc<- matchleft(epc,link234)
####################method 235####################
function235<- function(x,y){
  x$bnstreet <- paste(x$buildingname,x$locality,sep=",")
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
link235d <- doubleresult(link235)

epc <- matchleft(epc,link235)
#################### method 236 ##################
function236<- function(x,y){
  x$bnstreet <- paste(x$saotext,x$buildingname,sep=" ")
  x$bnstreet <- paste(x$bnstreet,x$streetdescription,sep=" ")
  x$bnstreet <- paste(x$bnstreet,x$dependentlocality,sep=",")
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
link236<-link236[,..needlist1]

link236u<- uniqueresult(link236)
link236d <- doubleresult(link236)

epc <- matchleft(epc,link236)
####################method 237####################
function237<- function(x,y){
  x$bnstreet <- paste(x$saotext,x$buildingnumber,sep=" ")
  x$bnstreet <- paste(x$bnstreet,x$streetdescription,sep=" ")
  x$bnstreet <- paste(x$bnstreet,x$locality,sep=",")
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
link237<-link237[,..needlist1]

link237u<- uniqueresult(link237)
link237d <- doubleresult(link237)
 
epc <- matchleft(epc,link237)
####################method 238####################
function238<- function(x,y){
  x$bnstreet <- paste(x$buildingnumber,x$streetdescription,sep=",")
  x$bnstreet <- gsub("-", "", x$bnstreet)
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
link238<-link238[,..needlist1]

link238u<- uniqueresult(link238)
link238d <- doubleresult(link238)

epc <- matchleft(epc,link238)
####################method 239####################
function239<- function(x,y){
  x$bnstreet <- paste(x$saotext,x$pp,sep=" ")
  x$bnstreet <- paste(x$bnstreet,x$streetdescription,sep=" ")
  x$bnstreet <- paste(x$bnstreet,x$locality,sep=",")
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

link239u<- uniqueresult(link239)
link239d <- doubleresult(link239)

epc <- matchleft(epc,link239)
####################method 240####################
function240<- function(x,y){
  x$bnstreet <- paste(x$pp,x$streetdescription,sep=" ")
  x$bnstreet <- gsub("-", "", x$bnstreet)
  x$bnstreet <- gsub("[']", "", x$bnstreet)
  x$bnstreet <- gsub("[.]", "", x$bnstreet)
  x$addressf <-paste(x$postcodelocator,x$bnstreet,sep=",")
  
  y<-y[!grepl("\\d+-\\d",y$add),]
  y$addressfinal <- y$add
  y$addressfinal <- gsub("-", "", y$addressfinal)
  y$addressfinal <- gsub(",", "", y$addressfinal)
  y$addressfinal <- gsub("[.]", "", y$addressfinal)
  y$addressfinal <- gsub("[']", "", y$addressfinal)
  y$addressf <- paste(y$postcode,y$addressfinal,sep=",")
  
  taba1 <- inner_join(x,y,by="addressf")
  
  return(taba1)
}
link240<-function240(add,epc)
link240<-link240[,..needlist1]

link240u<- uniqueresult(link240)
link240d <- doubleresult(link240)

epc <- matchleft(epc,link240)
####################method 241####################
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
link241d <- doubleresult(link241)

epc <- matchleft(epc,link241)
####################method 242####################
function242<- function(x,y){
  x$bnstreet <- paste(x$subbuildingname,x$paotext ,sep=" ")
  x$bnstreet <- gsub(" ", "", x$bnstreet)
  x$addressf <-paste(x$postcodelocator,x$bnstreet,sep=",")
  
  y<-y[y$property_type=="Flat"|y$property_type=="Maisonette",]
  y$addressfinal <-  paste("FLAT ",y$add,sep="")
  y$addressfinal <- gsub(" ", "", y$addressfinal)
  y$addressf <- paste(y$postcode,y$addressfinal,sep=",")
  
  taba1 <- inner_join(x,y,by="addressf")
  
  return(taba1) 
}
link242<-function242(add,epc)
link242<-link242[,..needlist1]

link242u<- uniqueresult(link242)
link242d <- doubleresult(link242)

epc <- matchleft(epc,link242)
####################method 243####################
function243<- function(x,y){
  x$bnstreet <- paste("FLAT ",x$subbuildingname,sep=" ")
  x$bnstreet <- paste(x$bnstreet,x$paotext,sep=",")
  x$bnstreet <- paste(x$bnstreet,x$buildingnumber,sep=",")
  x$bnstreet <- paste(x$bnstreet,x$streetdescription,sep=" ")
  x$bnstreet <- gsub(" ", "", x$bnstreet)
  x$addressf <-paste(x$postcodelocator,x$bnstreet,sep=",")
  
  y<-y[y$property_type=="Flat"|y$property_type=="Maisonette",]
  y$addressfinal <-  trimws(y$add)
  y$addressfinal <- gsub(" ", "", y$addressfinal)
  y$addressf <- paste(y$postcode,y$addressfinal,sep=",")
  
  taba1 <- inner_join(x,y,by="addressf")
  
  return(taba1)
}  
link243<-function243(add,epc)
link243<-link243[,..needlist1]

link243u<- uniqueresult(link243)
link243d <- doubleresult(link243)

epc <- matchleft(epc,link243)
####################method 244####################
function244<- function(x,y){
  x$saotext <- gsub("FLAT", "APARTMENT", x$saotext)
  x$bnstreet <- paste(x$saotext,x$paotext ,sep=",")
  x$bnstreet <- paste(x$bnstreet,x$pp,sep=",")
  x$bnstreet <- paste(x$bnstreet,x$streetdescription,sep=" ")
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
link244<-function244(add,epc)
link244<-link244[,..needlist1]

link244u<- uniqueresult(link244)
link244d <- doubleresult(link244)

epc<- matchleft(epc,link244)
####################method 245####################
function245<- function(x,y){
  x$saotext <- gsub("FLAT", "APARTMENT", x$saotext)
  x$bnstreet <- paste(x$saotext,x$paotext ,sep=" ")
  x$bnstreet <- paste(x$bnstreet,x$pp,sep=",")
  x$bnstreet <- paste(x$bnstreet,x$streetdescription,sep=" ")
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
link245d <- doubleresult(link245)

epc <- matchleft(epc,link245)
####################method 246####################
function246<- function(x,y){
  x$bnstreet <- paste(x$saotext,x$paotext,sep=" ")
  x$bnstreet <- gsub(" ", "", x$bnstreet)
  x$addressf <-paste(x$postcodelocator,x$bnstreet,sep=",")
  
  y<-y[y$property_type=="Flat"|y$property_type=="Maisonette",]
  y$addressfinal <- trimws(y$add1)
  y$addressfinal <- paste("FLAT ",y$addressfinal,sep="")
  y$addressfinal <- gsub(" ", "", y$addressfinal)
  y$addressf <- paste(y$postcode,y$addressfinal,sep=",")
  
  taba1 <- inner_join(x,y,by="addressf")
  
  return(taba1)
}
link246<-function246(add,epc)
link246<-link246[,..needlist1]

link246u<- uniqueresult(link246)
link246d <- doubleresult(link246)

epc <- matchleft(epc,link246)
####################method 247####################
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
link247<-link247[,..needlist1]

link247u<- uniqueresult(link247)
link247d <- doubleresult(link247)

epc <- matchleft(epc,link247)
####################method 248####################
function248<- function(x,y){
  x<-x[x$paoendnumber=="",]
  x$bnstreet <- paste(x$saotext,x$pp,sep=",")
  x$bnstreet <- paste(x$bnstreet,x$streetdescription ,sep=" ")
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
link248<-link248[,..needlist1]

link248u<- uniqueresult(link248)
link248d <- doubleresult(link248)

epc <- matchleft(epc,link248)
####################method 249####################
function249<- function(x,y){
  x$bnstreet <- paste(x$saotext,x$pp,sep=" ")
  x$bnstreet <- paste(x$bnstreet,x$streetdescription,sep=" ")
  x$bnstreet <- paste(x$bnstreet,x$dependentlocality,sep=",")
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
link249d <- doubleresult(link249)

epc <- matchleft(epc,link249)
####################section sum up####################
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

l167_249u = list(link167u,link168u,link169u,link170u,link171u,link172u,link173u,link174u,link175u,link176u,link177u,link178u,link179u,
                 link180u,link181u,link182u,link183u,link184u,link185u,link186u,link187u,link188u,link189u,
                 link190u,link191u,link192u,link193u,link194u,link195u,link196u,link197u,link198u,link199u,
                 link200u,link201u,link202u,link203u,link204u,link205u,link206u,link207u,link208u,link209u,
                 link210u,link211u,link212u,link213u,link214u,link215u,link216u,link217u,link218u,link219u,
                 link220u,link221u,link222u,link223u,link224u,link225u,link226u,link227u,link228u,link229u,
                 link230u,link231u,link232u,link233u,link234u,link235u,link236u,link237u,link238u,link239u,
                 link240u,link241u,link242u,link243u,link244u,link245u,link246u,link247u,link248u,link249u)
link167_249u<- rbindlist(l167_249u)

l167_249d = list(link167d,link168d,link169d,link170d,link171d,link172d,link173d,link174d,link175d,link176d,link177d,link178d,link179d,
                 link180d,link181d,link182d,link183d,link184d,link185d,link186d,link187d,link188d,link189d,
                 link190d,link191d,link192d,link193d,link194d,link195d,link196d,link197d,link198d,link199d,
                 link200d,link201d,link202d,link203d,link204d,link205d,link206d,link207d,link208d,link209d,
                 link210d,link211d,link212d,link213d,link214d,link215d,link216d,link217d,link218d,link219d,
                 link220d,link221d,link222d,link223d,link224d,link225d,link226d,link227d,link228d,link229d,
                 link230d,link231d,link232d,link233d,link234d,link235d,link236d,link237d,link238d,link239d,
                 link240d,link241d,link242d,link243d,link244d,link245d,link246d,link247d,link248d,link249d)

link167_249d<- rbindlist(l167_249d)

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
####################method 250####################
function250<- function(x,y){
  x$bnstreet <- paste(x$saotext,x$paotext,sep=",")
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
####################method 251####################
function251<- function(x,y){
  x$bnstreet <- paste(x$subbuildingname,x$paotext,sep=" ")
  x$bnstreet <- paste(x$bnstreet,x$buildingnumber,sep=",")
  x$bnstreet <- paste(x$bnstreet,x$streetdescription,sep=",")
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
####################method 252####################
function252<- function(x,y){
  x$bnstreet <- paste(x$saotext,x$buildingname,sep=" ")
  x$bnstreet <- paste(x$bnstreet,x$streetdescription,sep=" ")
  x$bnstreet <- paste(x$bnstreet,x$locality,sep=",")
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
####################method 253####################
function253<- function(x,y){
  x$bnstreet <- paste(x$saotext,x$pp,sep=",")
  x$bnstreet <- paste(x$bnstreet,x$streetdescription,sep=" ")
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
link253<-link253[,..needlist1]

link253u<- uniqueresult(link253)
link253d <- doubleresult(link253)

epc <- matchleft(epc,link253)
####################method 254####################
function254<- function(x,y){
  x$bnstreet <- paste(x$pp,x$streetdescription,sep=" ")
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
link254d <- doubleresult(link254)
 
epc<- matchleft(epc,link254)
####################method 255####################
function255<- function(x,y){
  x$bnstreet <- paste(x$buildingname,x$streetdescription,sep=" ")
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
link255d <- doubleresult(link255)

epc <- matchleft(epc,link255)
####################method 256####################
function256<- function(x,y){
  x$bnstreet <- paste(x$buildingnumber,x$streetdescription,sep=" ")
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
link256<-link256[,..needlist1]

link256u<- uniqueresult(link256)
link256d <- doubleresult(link256)

epc <- matchleft(epc,link256)
####################method 257####################
function257<- function(x,y){
  x$bnstreet <- paste(x$saotext,x$pp,sep=" ")
  x$bnstreet <- paste(x$bnstreet,x$streetdescription,sep=" ")
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
link257<-link257[,..needlist1]

link257u<- uniqueresult(link257)
link257d <- doubleresult(link257)

epc <- matchleft(epc,link257)
####################method 258####################
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
link258<-link258[,..needlist1]

link258u<- uniqueresult(link258)
link258d <- doubleresult(link258)

epc <- matchleft(epc,link258)
####################method 259####################
epc2<-epc[epc$lmk_key %in% epc$lmk_key,]
add2<-add
setDT(add2)
setDT(epc2)
function259<- function(z,k){
  x<-z
  setDT(x)
  #correct address field before matching
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
  #correct address field before matching
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
link259d <- doubleresult(link259)

epc <- matchleft(epc,link259)
rm(add2,epc2)
####################method 260####################
function260<- function(x,y){
  x$bnstreet <- paste(x$ss,x$streetdescription,sep=",")
  x$bnstreet <- gsub(" ", "", x$bnstreet)
  x$addressf <-paste(x$postcodelocator,x$bnstreet,sep=",")
  
  y$addressfinal <- trimws(y$add)
  y$addressfinal <- gsub(" ", "", y$addressfinal)
  y$addressf <- paste(y$postcode,y$addressfinal,sep=",")
  
  taba1 <- inner_join(x,y,by="addressf")
  
  return(taba1)
}
link260<-function260(add,epc)
link260<-link260[,..needlist1]

link260u<- uniqueresult(link260)
link260d <- doubleresult(link260)

epc <- matchleft(epc,link260)
####################method 261####################
function261<- function(x,y){
  x$bnstreet <- paste(x$buildingnumber,x$paotext,sep=",")
  x$bnstreet<-  trimws(x$bnstreet) 
  x$bnstreet <- gsub(" ", "", x$bnstreet)
  x$addressf <-paste(x$postcodelocator,x$bnstreet,sep=",")
  
  y[postcode=="LN11 0YS",add:=gsub("NORTHOLME COURT","NORTH HOLME COURT", add)]
  y$addressfinal <- y$add
  y$addressfinal <- trimws(y$addressfinal) 
  y$addressfinal <- gsub(" ", "", y$addressfinal)
  y$addressf <- paste(y$postcode,y$addressfinal,sep=",")
  
  taba1 <- inner_join(x,y,by="addressf")
  
  return(taba1)
}
link261<-function261(add,epc)
link261<-link261[,..needlist1]

link261u<- uniqueresult(link261)
link261d <- doubleresult(link261)

epc <- matchleft(epc,link261)
####################method 262####################
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
link262d <- doubleresult(link262)

epc <- matchleft(epc,link262)
####################method 263####################
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
link263<-link263[,..needlist1]

link263u<- uniqueresult(link263)
link263d <- doubleresult(link263)

epc <- matchleft(epc,link263)
####################method 264####################
function264<- function(x,y){
  x$bnstreet <- paste(x$pp,x$streetdescription,sep=" ")
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
link264d <- doubleresult(link264)
 
epc<- matchleft(epc,link264)
####################method 265####################
function265<- function(x,y){
  x$bnstreet <- paste(x$buildingname,x$streetdescription,sep=" ")
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
link265d <- doubleresult(link265)

epc <- matchleft(epc,link265)
####################method 266####################
function266<- function(x,y){
  x<-x[x$subbuildingname!="",]
  x$bnstreet <- paste(x$subbuildingname,x$buildingnumber,sep=" ")
  x$bnstreet <- paste(x$bnstreet,x$streetdescription,sep=" ")
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
  y$addressf <- paste(y$postcode,y$addressfinal,sep=",")
  
  taba1 <- inner_join(x,y,by="addressf")
  
  return(taba1)
}
link266<-function266(add,epc)
link266<-link266[,..needlist1]

link266u<- uniqueresult(link266)
link266d <- doubleresult(link266)

epc <- matchleft(epc,link266)
####################method 267####################
function267<- function(x,y){
  x$bnstreet <- paste(x$subbuildingname,x$paotext,sep=" ")
  x$bnstreet <- paste(x$bnstreet,x$streetdescription,sep=",")
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
link267<-link267[,..needlist1]

link267u<- uniqueresult(link267)
link267d <- doubleresult(link267)

epc <- matchleft(epc,link267)
####################method 268###################
function268<- function(x,y){
  x$bnstreet <- paste(x$subbuildingname,x$buildingname,sep=" ")
  x$bnstreet <- paste(x$bnstreet,x$ buildingnumber,sep=" ")
  x$bnstreet <- paste(x$bnstreet,x$streetdescription,sep=" ")
  x$addressf <-paste(x$postcodelocator,x$bnstreet,sep=",")
  
  y<-y[y$property_type=="Flat"|y$property_type=="Maisonette",]
  y$addressfinal <-  paste("APARTMENT ",y$add,sep="")
  y$addressf <- paste(y$postcode,y$addressfinal,sep=",")
  
  taba1 <- inner_join(x,y,by="addressf")
  
  return(taba1)
}
link268<-function268(add,epc)
link268<-link268[,..needlist1]

link268u<- uniqueresult(link268)
link268d <- doubleresult(link268)

epc <- matchleft(epc,link268)
####################method 269####################
function269<- function(x,y){
  x$bnstreet <- paste(x$subbuildingname,x$buildingname,sep=" ")
  x$bnstreet <- paste(x$bnstreet,x$buildingnumber,sep=",")
  x$bnstreet <- paste(x$bnstreet,x$streetdescription,sep=" ")
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
link269d <- doubleresult(link269)

epc <- matchleft(epc,link269)
####################method 270####################
#only works for "SE1 6SH"
function270<- function(x,k){
  
  x$bnstreet <- paste(x$ss,x$paotext,sep=" ")
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
link270<-link270[,..needlist1]

link270u<- uniqueresult(link270)
link270d <- doubleresult(link270)

epc <- matchleft(epc,link270)
####################method 271####################
function271<- function(x,y){
  x<-x[x$paotext!="",]
  x$bnstreet <- paste(x$saotext,x$paotext,sep=" ")
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
link271<-function271(add,epc)
link271<-link271[,..needlist1]

link271u<- uniqueresult(link271)
link271d <- doubleresult(link271)

epc <- matchleft(epc,link271)
####################method 272####################
function272<- function(x,y){
  x<-x[x$paotext!="",]
  x$bnstreet <- paste(x$subbuildingname,x$paotext,sep=" ")
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
link272d <- doubleresult(link272)

epc <- matchleft(epc,link272)
####################method 273####################
function273<- function(x,y){
  x<-x[x$buildingname!="",]
  x$bnstreet <- paste(x$saotext,x$buildingname,sep=" ")
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

link273<-function273(add,epc)
link273<-link273[,..needlist1]

link273u<- uniqueresult(link273)
link273d <- doubleresult(link273)

epc <- matchleft(epc,link273)
####################method 274####################
function274<- function(x,y){
  x<-x[x$buildingname!="",]
  x$bnstreet <- paste(x$subbuildingname,x$buildingname,sep=" ")
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
link274<-function274(add,epc)
link274<-link274[,..needlist1]

link274u<- uniqueresult(link274)
link274d <- doubleresult(link274)

epc<- matchleft(epc,link274)
####################method 275####################
function275<- function(x,y){
  x$bnstreet <- paste(x$buildingnumber,x$paotext,sep=",")
  x$bnstreet <- paste(x$bnstreet,x$streetdescription,sep=",")
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
link275d <- doubleresult(link275)

epc <- matchleft(epc,link275)
####################method 276####################
#only for PL7 1LJ
function276<- function(x,k){
  x$bnstreet <- paste(x$saotext,x$paotext,sep=",")
  x$bnstreet <- paste(x$bnstreet,x$streetdescription,sep=",")
  x$bnstreet <- gsub("/", "", x$bnstreet)
  x$bnstreet <- gsub("[.]", "", x$bnstreet)
  x$bnstreet <- gsub("[']", "", x$bnstreet)
  x$bnstreet <- gsub(" ", "", x$bnstreet)
  x$addressf <-paste(x$postcodelocator,x$bnstreet,sep=",")

  y<-k
  setDT(y)
  y[postcode=="PL7 1LJ",add:=gsub("POCKLINGTON RISE", "THE RISE",add)]
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
link276<-link276[,..needlist1]

link276u<- uniqueresult(link276)
link276d <- doubleresult(link276)

epc <- matchleft(epc,link276)
####################method 277####################
#only for PL7 1LJ
function277<- function(x,k){
  x$bnstreet <- paste(x$saotext,x$ paotext,sep=" ")
  x$bnstreet <- paste(x$bnstreet,x$streetdescription,sep=",")
  x$bnstreet <- gsub(" ", "", x$bnstreet)
  x$addressf <-paste(x$postcodelocator,x$bnstreet,sep=",")
  
  y<-k
  setDT(y)
  y[postcode=="PL7 1LJ",add:=gsub("POCKLINGTON RISE", "THE RISE",add)]
  y$addressfinal <- trimws(y$add)
  y$addressfinal <- gsub(" ", "", y$addressfinal)
  y$addressf <- paste(y$postcode,y$addressfinal,sep=",")
  
  taba1 <- inner_join(x,y,by="addressf")
  
  return(taba1)
}
link277<-function277(add,epc)
link277<-link277[,..needlist1]

link277u<- uniqueresult(link277)
link277d <- doubleresult(link277)

epc <- matchleft(epc,link277)
####################method 278####################
function278<- function(x,y){
  x$bnstreet <- paste(x$saotext,x$pp,sep=",")
  x$bnstreet <- paste(x$bnstreet,x$streetdescription,sep=" ")
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
link278<-link278[,..needlist1]

link278u<- uniqueresult(link278)
link278d <- doubleresult(link278)

epc <- matchleft(epc,link278)
####################method 279####################
function279<- function(x,y){
  x$bnstreet <- paste(x$subbuildingname,x$buildingname,sep=",")
  x$bnstreet <- paste(x$bnstreet,x$streetdescription,sep=",")
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
link279d <- doubleresult(link279)

epc <- matchleft(epc,link279)
####################method 280####################
function280<- function(x,y){
  x$bnstreet <-    paste(x$subbuildingname,x$buildingname,sep=",")
  x$bnstreet <-    paste(x$bnstreet,x$buildingnumber,sep=" ")
  x$bnstreet <-    paste(x$bnstreet,x$streetdescription,sep=" ")
  x$bnstreet <- gsub("/", "", x$bnstreet)
  x$bnstreet <- gsub("[.]", "", x$bnstreet)
  x$bnstreet <- gsub("[']", "", x$bnstreet)
  x$bnstreet <- gsub(" ", "", x$bnstreet)
  x$addressf <-paste(x$postcodelocator,x$bnstreet,sep=",")
  
  y$addressfinal <- trimws(y$add)
  y$addressfinal <- gsub("[/]", "", y$addressfinal)
  y$addressfinal <- gsub("[.]", "", y$addressfinal)
  y$addressfinal <- gsub("[']", "", y$addressfinal)
  
  y$addressfinal <- gsub(" ", "", y$addressfinal)
  y$addressf <- paste(y$postcode,y$addressfinal,sep=",")
  
  taba1 <- inner_join(x,y,by="addressf")
  
  return(taba1)
}
link280<-function280(add,epc)
link280<-link280[,..needlist1]

link280u<- uniqueresult(link280)
link280d <- doubleresult(link280)

epc <- matchleft(epc,link280)
####################method 281####################
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
link281d <- doubleresult(link281)

epc <- matchleft(epc,link281)
####################method 282####################
function282<- function(x,y){
  x$bnstreet <- paste(x$ss,x$streetdescription,sep=",")
  x$bnstreet <- gsub("/", "", x$bnstreet)
  x$bnstreet <- gsub("[.]", "", x$bnstreet)
  x$bnstreet <- gsub("[']", "", x$bnstreet)
  x$bnstreet <- gsub(" ", "", x$bnstreet)
  x$addressf <-paste(x$postcodelocator,x$bnstreet,sep=",")
  
  y$addressfinal <- trimws(y$add)
  y$addressfinal <- gsub("[/]", "", y$addressfinal)
  y$addressfinal <- gsub("[.]", "", y$addressfinal)
  y$addressfinal <- gsub("[']", "", y$addressfinal)
  y$addressfinal <- gsub(" ", "", y$addressfinal)
  y$addressf <- paste(y$postcode,y$addressfinal,sep=",")
  
  taba1 <- inner_join(x,y,by="addressf")
  
  return(taba1)
}
link282<-function282(add,epc)
link282<-link282[,..needlist1]

link282u<- uniqueresult(link282)
link282d <- doubleresult(link282)

epc <- matchleft(epc,link282)
####################method 283####################
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
  
  x1$bnstreet <- paste(x1$paostartnumber,x1$saotext,sep="")
  x1$bnstreet <- paste(x1$bnstreet,x1$streetdescription,sep=",")
  x1$bnstreet <- gsub(" ", "", x1$bnstreet)
  x1$addressf <-paste(x1$postcodelocator,x1$bnstreet,sep=",")
  
  y$addressfinal <-  trimws(y$add)
  y$addressfinal <- gsub(" ", "", y$addressfinal)
  y$addressf <- paste(y$postcode,y$addressfinal,sep=",")
  
  taba1 <- inner_join(x1,y,by="addressf")
  
  return(taba1)
}
link283<-function283(add,epc)
link283<-link283[,..needlist1]

link283u<- uniqueresult(link283)
link283d <- doubleresult(link283)

epc <- matchleft(epc,link283)
####################method 284####################
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
  
  x1$bnstreet <- paste(x1$paostartnumber,x1$new,sep="")
  x1$bnstreet <- paste(x1$bnstreet,x1$streetdescription,sep=" ")
  x1$addressf <-paste(x1$postcodelocator,x1$bnstreet,sep=",")
  y$addressfinal <-  trimws(y$add)
  y$addressf <- paste(y$postcode,y$addressfinal,sep=",")
  
  taba1 <- inner_join(x1,y,by="addressf")
  
  return(taba1)
}
link284<-function284(add,epc)
link284<-link284[,..needlist1]

link284u<- uniqueresult(link284)
link284d <- doubleresult(link284)

epc<- matchleft(epc,link284)
####################method 285####################
function285<- function(x,y){

  x1<-x
  x1$bnstreet <- paste(x1$pp,x1$saotext,sep="")
  x1$bnstreet <- paste(x1$bnstreet,x1$streetdescription,sep=" ")
  x$bnstreet <- gsub("/", "", x$bnstreet)
  x$bnstreet <- gsub("[.]", "", x$bnstreet)
  x$bnstreet <- gsub("[']", "", x$bnstreet)
  x1$addressf <-paste(x1$postcodelocator,x1$bnstreet,sep=",")
  
  y$addressfinal <-  trimws(y$add)
  y$addressfinal <- gsub("[/]", "", y$addressfinal)
  y$addressfinal <- gsub("[.]", "", y$addressfinal)
  y$addressfinal <- gsub("[']", "", y$addressfinal)
  y$addressf <- paste(y$postcode,y$addressfinal,sep=",")
  
  taba1 <- inner_join(x1,y,by="addressf")
  
  return(taba1)
}
link285<-function285(add,epc)
link285<-link285[,..needlist1]

link285u<- uniqueresult(link285)
link285d <- doubleresult(link285)

epc <- matchleft(epc,link285)
####################method 286####################
function286<- function(x,y){
  
  x<-x[x$paotext!="",]
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
link286<-link286[,..needlist1]

link286u<- uniqueresult(link286)
link286d <- doubleresult(link286)

epc <- matchleft(epc,link286)
####################method 287####################
function287<- function(x,y){
  x<-x[x$paotext!="",]
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
link287<-link287[,..needlist1]

link287u<- uniqueresult(link287)
link287d <- doubleresult(link287)

epc <- matchleft(epc,link287)
####################method 288####################
function288<- function(x,y){
  x$bnstreet <- x$pp
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
link288<-function288(add,epc)
link288<-link288[,..needlist1]

link288u<- uniqueresult(link288)

link288d <- doubleresult(link288)
 
epc <- matchleft(epc,link288)

####################method 289####################
function289<- function(x,y){
  x<- x[x$subbuildingname!="",]
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
link289d <- doubleresult(link289)

epc <- matchleft(epc,link289)
####################method 290####################
function290<- function(x,y){
  x<-x[x$buildingnumber!="",]
  x<- x[grepl("^\\d+",x$buildingnumber),]
  x$bnstreet <- x$buildingnumber 
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
link290<-function290(add,epc)
link290<-link290[,..needlist1]

link290u<- uniqueresult(link290)
link290d <- doubleresult(link290)

epc <- matchleft(epc,link290)
#################### method 291 ##################
function291<- function(x,y){
  x$bnstreet <- paste(x$saostartnumber,x$saostartsuffix, sep=" ")
  x<- x[grepl("^\\d+",x$bnstreet),]
  x$bnstreet <- gsub("/", "", x$bnstreet)
  x$bnstreet <- gsub("[.]", "", x$bnstreet)
  x$bnstreet <- gsub("[']", "", x$bnstreet)
  x$bnstreet <- gsub(" ", "", x$bnstreet)
  x$addressf <-paste(x$postcodelocator,x$bnstreet,sep=",")
  
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
link291d <- doubleresult(link291)

epc <- matchleft(epc,link291)
####################method 292####################
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
link292d <- doubleresult(link292)

epc <- matchleft(epc,link292)
####################method 293####################
function293<- function(x,y){
  x$bnstreet <- paste("APARTMENT ",x$ss,sep="")
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
link293<-link293[,..needlist1]

link293u<- uniqueresult(link293)
link293d <- doubleresult(link293)

epc <- matchleft(epc,link293)
####################method 294####################
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
  taba1<- taba1[!grepl("APARTMENT",taba1$saotext),]
  taba1<- taba1[!grepl("GROUND",taba1$saotext),]

  return(taba1)
}

link294<-function294(add,epc)
link294<-link294[,..needlist1]

link294u<- uniqueresult(link294)
link294d <- doubleresult(link294)

epc<- matchleft(epc,link294)
####################method 295####################
function295<- function(x,y){
  x<-x[x$saostartnumber!="",]
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
link295d <- doubleresult(link295)

epc <- matchleft(epc,link295)
####################method 296####################
function296<- function(x,y){
  x<-x[x$saostartnumber!="",]
  x$bnstreet <- paste("FLAT ",x$ss,sep="")
  x$bnstreet <- paste(x$bnstreet ,x$paotext,sep="")
  x$bnstreet <- gsub(" ", "", x$bnstreet)
  x$addressf <-paste(x$postcodelocator,x$bnstreet,sep=",")
  
  y<-y[y$property_type=="Flat"|y$property_type=="Maisonette",]
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
link296<-link296[,..needlist1]

link296u<- uniqueresult(link296)
link296d <- doubleresult(link296)

epc <- matchleft(epc,link296)
####################method 297####################
function297<- function(x,y){
  x<-x[x$saostartnumber!="",]
  x$bnstreet <- paste("FLAT ",x$ss,sep=" ")
  x$bnstreet <- paste(x$bnstreet ,x$pp,sep=" ")
  x$bnstreet <- paste(x$bnstreet ,x$streetdescription,sep=" ")
  x$bnstreet <- gsub(" ", "", x$bnstreet)
  x$addressf <-paste(x$postcodelocator,x$bnstreet,sep=",")
  
  y<-y[y$property_type=="Flat"|y$property_type=="Maisonette",]
  y$addressfinal <- paste(y$add1,y$add2,sep=" ")
  y$addressfinal <- gsub("[']", "", y$addressfinal)
  y$addressfinal <- gsub("[.]", "", y$addressfinal)
  y$addressfinal <- gsub(" ", "", y$addressfinal)
  y$addressf <- paste(y$postcode,y$addressfinal,sep=",")
  
  taba1 <- inner_join(x,y,by="addressf")
 
  return(taba1)
}
link297<-function297(add,epc)
link297<-link297[,..needlist1]

link297u<- uniqueresult(link297)
link297d <- doubleresult(link297)

epc <- matchleft(epc,link297)
####################method 298####################
function298<- function(x,y){
  x<-x[x$saostartnumber!="",]
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
link298<-link298[,..needlist1]

link298u<- uniqueresult(link298)
link298d <- doubleresult(link298)

epc <- matchleft(epc,link298)
####################method 299####################
function299<- function(x,y){
  x<-x[x$saostartnumber!="",]
  x<-x[x$paostartnumber!="",]
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
  return(taba1)
}
link299<-function299(add,epc)
link299<-link299[,..needlist1]

link299u<- uniqueresult(link299)
link299d <- doubleresult(link299)

epc <- matchleft(epc,link299)
####################sum up####################
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

l250_299u = list(link250u,link251u,link252u,link253u,link254u,link255u,link256u,link257u,link258u,link259u,
                 link260u,link261u,link262u,link263u,link264u,link265u,link266u,link267u,link268u,link269u,
                 link270u,link271u,link272u,link273u,link274u,link275u,link276u,link277u,link278u,link279u,
                 link280u,link281u,link282u,link283u,link284u,link285u,link286u,link287u,link288u,link289u,
                 link290u,link291u,link292u,link293u,link294u,link295u,link296u,link297u,link298u,link299u)

#
link250_299u<- rbindlist(l250_299u)

l250_299d = list(link250d,link251d,link252d,link253d,link254d,link255d,link256d,link257d,link258d,link259d,
                 link260d,link261d,link262d,link263d,link264d,link265d,link266d,link267d,link268d,link269d,
                 link270d,link271d,link272d,link273d,link274d,link275d,link276d,link277d,link278d,link279d,
                 link280d,link281d,link282d,link283d,link284d,link285d,link286d,link287d,link288d,link289d,
                 link290d,link291d,link292d,link293d,link294d,link295d,link296d,link297d,link298d,link299d)

link250_299d<- rbindlist(l250_299d)

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
####################sum up the linked data from first 299 matching rules####################
ldouble1 = list(link1_11d,link12_26d,link27_69d,link70_99d,link100_133d,link134_166d,link167_249d,link250_299d)
lunique1 = list(link1_11u,link12_26u,link27_69u,link70_99u,link100_133u,link134_166u,link167_249u,link250_299u)

linku1<-rbindlist(lunique1,use.names=TRUE, fill=TRUE)
linkd1<-rbindlist(ldouble1,use.names=TRUE, fill=TRUE)

dbWriteTable(con, "linku1new",value =linku1, append =  TRUE, row.names = FALSE)
dbWriteTable(con, "linkd1new",value =linkd1, append =  TRUE, row.names = FALSE)

rm(ldouble1,lunique1)
rm(link1_11d,link12_26d,link27_69d,link70_99d,link100_133d,link134_166d,link167_249d,link250_299d)
rm(link1_11u,link12_26u,link27_69u,link70_99u,link100_133u,link134_166u,link167_249u,link250_299u)
rm(add)
####################read in OS AddressBase###################
add <- dbGetQuery(con,"select * from  addressgb") 
#format the OS addressBase data as before the following linkage process
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

add[nchar(postcodelocator)>=6,postset :=substring(postcodelocator,0,nchar(postcodelocator)-2)]
add$postset  <- str_trim(add$postset)

add<-add[(add$postset  %in% epc$postset), ]

####################method 300####################
function300<- function(x,y){
  x$bnstreet <- paste(x$saotext,x$paotext,sep=",")
  x$bnstreet <- paste(x$bnstreet,x$pp,sep=", ")
  x$bnstreet <- paste(x$bnstreet,x$streetdescription,sep=" ")
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
link300<-link300[,..needlist1]

link300u<- uniqueresult(link300)
link300d <- doubleresult(link300)

epc <- matchleft(epc,link300)
####################method 301####################
function301<- function(x,y){
  x$bnstreet <- paste("STUDIO",x$saotext,sep=" ")
  x$bnstreet <- paste(x$bnstreet,x$pp,sep=",")
  x$bnstreet <- paste(x$bnstreet,x$streetdescription,sep=" ")
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
link301d <- doubleresult(link301)

epc <- matchleft(epc,link301)
####################method 302####################
function302<- function(x,y){
  x$bnstreet <- paste(x$saotext,x$paotext,sep=" ")
  x$bnstreet <- paste(x$bnstreet,x$paostartnumber,sep=",")
  x$bnstreet <- paste(x$bnstreet,x$paoendnumber,sep="-")
  x$bnstreet <- paste(x$bnstreet,x$streetdescription,sep=" ")
  x$bnstreet <- gsub("/", "", x$bnstreet)
  x$bnstreet <- gsub("[.]", "", x$bnstreet)
  x$bnstreet <- gsub("[']", "", x$bnstreet)
  x$bnstreet <- gsub(" ", "", x$bnstreet)
  x$addressf <-paste(x$postcodelocator,x$bnstreet,sep=",")
  
  y<-y[y$property_type=="Flat"|y$property_type=="Maisonette",]
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
link302d <- doubleresult(link302)

epc <- matchleft(epc,link302)
####################method 303####################
function303<- function(x,y){
  x$bnstreet <- paste("APARTMENT ",x$ss,sep=" ")
  x$bnstreet <- paste(x$bnstreet,x$paotext,sep=",")
  x$bnstreet <- paste(x$bnstreet,x$streetdescription,sep=",")
  x$bnstreet <- paste(x$bnstreet,x$townname,sep=",")
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
link303<-link303[,..needlist1]

link303u<- uniqueresult(link303)
link303d <- doubleresult(link303)

epc <- matchleft(epc,link303)
####################method 304####################
function304<- function(x,y){
  x$bnstreet <- paste(x$saotext,x$buildingname,sep=",")
  x$bnstreet <- paste(x$bnstreet,x$buildingnumber,sep=",")
  x$bnstreet <- paste(x$bnstreet,x$streetdescription,sep=" ")
  x$bnstreet <- gsub("[.]", "", x$bnstreet)
  x$bnstreet <- gsub("[/]", "", x$bnstreet)
  x$bnstreet <- gsub("[']", "", x$bnstreet)
  x$bnstreet <- gsub(" ", "", x$bnstreet)
  x$addressf <-paste(x$postcodelocator,x$bnstreet,sep=",")
  
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
link304d <- doubleresult(link304)
 
epc<- matchleft(epc,link304)
####################method 305####################
function305<- function(x,y){
  x$bnstreet <-  paste("FLAT ",x$buildingname,sep=" ")
  x$bnstreet <-  paste(x$bnstreet,x$streetdescription,sep=" ")
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
link305d <- doubleresult(link305)

epc <- matchleft(epc,link305)
####################method 306####################
function306<- function(x,y){
  x$bnstreet <- paste("STUDIO ",x$subbuildingname,sep=" ")
  x$bnstreet <- paste(x$bnstreet,x$paotext,sep=" ")
  x$bnstreet <- paste(x$bnstreet,x$buildingnumber,sep=" ")
  x$bnstreet <- paste(x$bnstreet,x$streetdescription,sep=" ")
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
link306<-link306[,..needlist1]

link306u<- uniqueresult(link306)
link306d <- doubleresult(link306)

epc <- matchleft(epc,link306)
####################method 307####################
function307<- function(x,y){
  x$bnstreet <-  paste(x$subbuildingname,x$paotext,sep=" ")
  x$bnstreet <- paste(x$bnstreet,x$buildingnumber,sep=",")
  x$bnstreet <- paste(x$bnstreet,x$streetdescription,sep=",")
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
link307<-link307[,..needlist1]

link307u<- uniqueresult(link307)
link307d <- doubleresult(link307)

epc <- matchleft(epc,link307)
####################method 308####################
function308<- function(x,y){
  x$bnstreet <- paste(x$subbuildingname,x$buildingname,sep=",")
  x$bnstreet <- paste(x$bnstreet,x$streetdescription,sep=",")
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
link308<-link308[,..needlist1]

link308u<- uniqueresult(link308)
link308d <- doubleresult(link308)
 
epc <- matchleft(epc,link308)
####################method 309####################
function309<- function(x,y){
  x$bnstreet <- paste(x$subbuildingname,x$buildingname,sep=",")
  x$bnstreet <- paste(x$bnstreet,x$streetdescription,sep=" ")
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
link309d <- doubleresult(link309)

epc <- matchleft(epc,link309)
####################method 310####################
function310<- function(x,y){
  x$bnstreet <- paste(x$subbuildingname,x$buildingnumber,sep=",")
  x$bnstreet <-  paste(x$bnstreet,x$streetdescription,sep=" ")
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
link310<-link310[,..needlist1]

link310u<- uniqueresult(link310)
link310d <- doubleresult(link310)

epc <- matchleft(epc,link310)
####################method 311####################
function311<- function(x,y){
  x$bnstreet <- paste(x$subbuildingname,x$pp,sep=",")
  x$bnstreet <- paste(x$bnstreet,x$streetdescription,sep=" ")
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
link311d <- doubleresult(link311)

epc <- matchleft(epc,link311)
####################method 312####################
function312<- function(x,y){
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
link312d <- doubleresult(link312)

epc <- matchleft(epc,link312)
####################method 313####################
function313<- function(x,y){
  x$bnstreet <- paste(x$subbuildingname,x$buildingnumber,sep=",")
  x$bnstreet <- paste(x$bnstreet,x$streetdescription,sep=",")
  x$bnstreet <- gsub(",", "", x$bnstreet)
  x$bnstreet <- gsub("/", "", x$bnstreet)
  x$bnstreet <- gsub("[.]", "", x$bnstreet)
  x$bnstreet <- gsub("[']", "", x$bnstreet)
  x$bnstreet <- gsub(" ", "", x$bnstreet)
  x$addressf <-paste(x$postcodelocator,x$bnstreet,sep=",")
 
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
link313<-link313[,..needlist1]

link313u<- uniqueresult(link313)
link313d <- doubleresult(link313)

epc <- matchleft(epc,link313)
####################method 314####################
function314<- function(x,y){
  x$bnstreet <- paste(x$subbuildingname,x$paotext,sep=" ")
  x$bnstreet <- paste(x$bnstreet,x$buildingnumber,sep=",")
  x$bnstreet <- paste(x$bnstreet,x$streetdescription,sep=",")
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
link314d <- doubleresult(link314)

epc<- matchleft(epc,link314)
####################method 315####################
function315<- function(x,y){
  x$bnstreet <-    paste(x$subbuildingname,x$paotext,sep=" ")
  x$bnstreet <-    paste(x$bnstreet,x$buildingnumber,sep=",")
  x$bnstreet <-    paste(x$bnstreet,x$streetdescription,sep=",")
  x$bnstreet <- gsub("/", "", x$bnstreet)
  x$bnstreet <- gsub("[.]", "", x$bnstreet)
  x$bnstreet <- gsub("[']", "", x$bnstreet)
  x$bnstreet <- gsub(" ", "", x$bnstreet)
  x$addressf <-paste(x$postcodelocator,x$bnstreet,sep=",")
  
  y[postcode=="B3 1PW",add:=gsub("CASPER HOUSE", "CASPAR HOUSE",add)]
  y$addressfinal <-  y$add
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
link315d <- doubleresult(link315)

epc <- matchleft(epc,link315)
####################method 316####################
function316<- function(x,y){
  x$bnstreet <- paste(x$saotext,x$paotext,sep=" ")
  x$bnstreet <- paste(x$bnstreet,x$streetdescription,sep=" ")
  x$bnstreet <- paste(x$bnstreet,x$townname,sep=" ")
  x$bnstreet <- gsub(",", "", x$bnstreet)
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
  y$addressfinal <- gsub(",", "", y$addressfinal)
  y$addressfinal <- gsub(" ", "", y$addressfinal)
  y$addressf <- paste(y$postcode,y$addressfinal,sep=",")
  
  taba1 <- inner_join(x,y,by="addressf")
  
  return(taba1)
}
link316<-function316(add,epc)
link316<-link316[,..needlist1]

link316u<- uniqueresult(link316)
link316d <- doubleresult(link316)

epc <- matchleft(epc,link316)
####################method 317####################
function317<- function(x,y){
  x$bnstreet <- paste(x$saotext,x$pp,sep=",")
  x$bnstreet <- paste(x$bnstreet,x$streetdescription,sep=" ")
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
link317<-link317[,..needlist1]

link317u<- uniqueresult(link317)
link317d <- doubleresult(link317)

epc <- matchleft(epc,link317)
####################method 318####################
function318<- function(x,y){
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
link318<-link318[,..needlist1]

link318u<- uniqueresult(link318)
link318d <- doubleresult(link318)

epc <- matchleft(epc,link318)
####################method 319####################
function319<- function(x,y){
  x$bnstreet <- paste(x$saotext,x$paotext,sep=" ")
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
link319d <- doubleresult(link319)

epc <- matchleft(epc,link319)
####################method 320####################
function320<- function(x,y){
  x$bnstreet <- paste(x$saotext,x$pp,sep=",")
  x$bnstreet <- paste(x$bnstreet,x$streetdescription,sep=" ")
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
link320<-link320[,..needlist1]

link320u<- uniqueresult(link320)
link320d <- doubleresult(link320)

epc <- matchleft(epc,link320)
####################method 321####################
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
link321d <- doubleresult(link321)

epc <- matchleft(epc,link321)
####################method 322####################
function322<- function(x,y){
  x$bnstreet <- paste(x$saotext,x$pp,sep=",")
  x$bnstreet <- paste(x$bnstreet,x$streetdescription,sep=" ")
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
link322<-link322[,..needlist1]

link322u<- uniqueresult(link322)
link322d <- doubleresult(link322)

epc <- matchleft(epc,link322)
####################method 323 ####################
function323<- function(x,y){
  x$bnstreet <- paste(x$pp,x$streetdescription,sep=",")
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
link323<-function323(add,epc)
link323<-link323[,..needlist11]

link323u<- uniqueresult(link323)
link323d <- doubleresult(link323)

epc <- matchleft(epc,link323)
####################method 324####################
function324<- function(x,y){
  x$bnstreet<- paste(x$buildingnumber,x$streetdescription,sep=",")
  x$bnstreet <- paste(x$bnstreet,x$townname,sep=",")
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
link324d <- doubleresult(link324)

epc<- matchleft(epc,link324)
####################method 325####################
function325<- function(x,y){
  x$bnstreet <- paste(x$pp,x$streetdescription,sep=",")
  x$bnstreet <- paste(x$bnstreet,x$townname,sep=",")
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
link325d <- doubleresult(link325)

epc <- matchleft(epc,link325)
####################method 326####################
function326<- function(x,y){
  x$bnstreet <- paste(x$buildingname,x$streetdescription,sep=",")
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
link326<-link326[,..needlist11]

link326u<- uniqueresult(link326)

link326d <- doubleresult(link326)

epc <- matchleft(epc,link326)
####################method 327####################
function327<- function(x,y){
  x$bnstreet <- paste(x$buildingnumber,x$streetdescription,sep=",")
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
link327<-function327(add,epc)
link327<-link327[,..needlist11]

link327u<- uniqueresult(link327)
link327d <- doubleresult(link327)

epc <- matchleft(epc,link327)
####################method 328####################
function328<- function(x,y){
  x$bnstreet <- paste(x$pp,x$streetdescription,sep=",")
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
link328<-function328(add,epc)
link328<-link328[,..needlist11]

link328u<- uniqueresult(link328)
link328d <- doubleresult(link328)

epc <- matchleft(epc,link328)
####################method 329####################
function329<- function(x,y){
  x$bnstreet <- paste(x$buildingnumber,x$streetdescription,sep=",")
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
link329<-function329(add,epc)
link329<-link329[,..needlist11]

link329u<- uniqueresult(link329)
link329d <- doubleresult(link329)

epc <- matchleft(epc,link329)
####################method 330####################
function330<- function(x,y){
  x$bnstreet <- paste(x$pp,x$streetdescription,sep=",")
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
link330<-function330(add,epc)
link330<-link330[,..needlist11]

link330u<- uniqueresult(link330)
link330d <- doubleresult(link330)

epc <- matchleft(epc,link330)
####################method 331 manually####################
function331<- function(x,y){
  x$bnstreet <- paste(x$buildingname,x$streetdescription,sep=",")
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
link331<-function331(add,epc)
link331<-link331[,..needlist11]

link331u<- uniqueresult(link331)
link331d <- doubleresult(link331)

epc <- matchleft(epc,link331)
####################method 332####################
function332<- function(x,y){
  x$bnstreet <- paste(x$buildingname,x$streetdescription,sep=",")
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
link332<-function332(add,epc)
link332<-link332[,..needlist11]

link332u<- uniqueresult(link332)
link332d <- doubleresult(link332)

epc <- matchleft(epc,link332)
####################method 333###################
function333<- function(x,y){
  x$bnstreet <- paste(x$paotext,x$streetdescription,sep=",")
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
link333<-function333(add,epc)
link333<-link333[,..needlist11]

link333u<- uniqueresult(link333)
link333d <- doubleresult(link333)

epc <- matchleft(epc,link333)
####################method 334####################
function334<- function(x,y){
  x$bnstreet <- paste(x$paotext,x$streetdescription,sep=",")
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
link334<-function334(add,epc)
link334<-link334[,..needlist11]

link334u<- uniqueresult(link334)
link334d <- doubleresult(link334)

epc<- matchleft(epc,link334)
####################method 335####################
function335<- function(x,y){
  x$bnstreet <- paste(x$subbuildingname,x$buildingname,sep=",")
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
link335d <- doubleresult(link335)

epc <- matchleft(epc,link335)
####################method 336####################
function336<- function(x,y){
  x$bnstreet <- paste(x$buildingname,x$dependentlocality,sep=",")
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
link336<-link336[,..needlist11]

link336u<- uniqueresult(link336)
link336d <- doubleresult(link336)

epc <- matchleft(epc,link336)
####################method 337####################
function337<- function(x,y){
  x$bnstreet <- paste(x$buildingname,x$locality,sep=",")
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
link337<-link337[,..needlist11]

link337u<- uniqueresult(link337)
link337d <- doubleresult(link337)

epc <- matchleft(epc,link337)
####################method 338####################
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
link338<-link338[,..needlist11]

link338u<- uniqueresult(link338)
link338d <- doubleresult(link338)

epc <- matchleft(epc,link338)
####################method 339####################
function339<- function(x,y){
  x$bnstreet <- paste(x$saotext,x$buildingname,sep=" ")
  x$bnstreet <- paste(x$bnstreet,x$streetdescription,sep=",")
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
link339d <- doubleresult(link339)

epc <- matchleft(epc,link339)
####################method 340####################
function340<- function(x,y){
  x$bnstreet <- paste(x$subbuildingname,x$ss,sep=" ")
  x$bnstreet <- paste(x$bnstreet,x$paotext,sep=",")
  x$bnstreet <- paste(x$bnstreet,x$streetdescription,sep=",")
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

link340_1<-link340[ grepl("^\\d",link340$ss) ,]
link340_2<-link340[grepl("\\d$",link340$subbuildingname) ,]
link340_2<-link340_2[link340_2$lmk_key %in% link340_1$lmk_key,]

link340<-link340[!(link340$lmk_key %in% link340_2$lmk_key),]

rm(link340_1,link340_2)
link340<-link340[,..needlist11]

link340u<- uniqueresult(link340)
link340d <- doubleresult(link340)

epc<- matchleft(epc,link340)
####################method 341 manually####################
function341<- function(x,y){
  x<-x[x$buildingnumber=="",]
  x$bnstreet <- paste(x$paotext,x$streetdescription,sep=",")
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
link341d <- doubleresult(link341)

epc <- matchleft(epc,link341)
####################method 342####################
function342<- function(x,y){
  x$bnstreet <- paste(x$subbuildingname,x$buildingnumber,sep=",")
  x$bnstreet <- paste(x$bnstreet,x$streetdescription,sep=",")
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
link342d <- doubleresult(link342)

epc <- matchleft(epc,link342)
####################method 343####################
function343<- function(x,y){
  x$bnstreet <- paste(x$saotext,x$buildingnumber,sep=",")
  x$bnstreet <- paste(x$bnstreet,x$streetdescription,sep=",")
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
link343<-link343[,..needlist11]

link343u<- uniqueresult(link343)
link343d <- doubleresult(link343)

epc <- matchleft(epc,link343)
####################method 344####################
function344<- function(x,y){
  x$bnstreet <- paste(x$subbuildingname,x$buildingnumber,sep=",")
  x$bnstreet <- paste(x$bnstreet,x$streetdescription,sep=",")
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
link344d <- doubleresult(link344)

epc<- matchleft(epc,link344)
####################method 345####################
function345<- function(x,y){
  x$bnstreet <- paste(x$saotext,x$buildingnumber,sep=",")
  x$bnstreet <- paste(x$bnstreet,x$streetdescription,sep=",")
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
link345d <- doubleresult(link345)

epc <- matchleft(epc,link345)
####################method 346####################
function346<- function(x,y){
  x$bnstreet <- paste(x$subbuildingname,x$buildingnumber,sep=",")
  x$bnstreet <- paste(x$bnstreet,x$streetdescription,sep=",")
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
link346<-link346[,..needlist11]

link346u<- uniqueresult(link346)
link346d <- doubleresult(link346)

epc <- matchleft(epc,link346)
####################method 347####################
function347<- function(x,y){
  
  x$bnstreet <- paste(x$subbuildingname,x$buildingname,sep=" ")
  x$bnstreet <- paste(x$bnstreet,x$buildingnumber,sep=",")
  x$bnstreet <- paste(x$bnstreet,x$streetdescription,sep=",")
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
link347<-link347[,..needlist11]

link347u<- uniqueresult(link347)
link347d <- doubleresult(link347)

epc <- matchleft(epc,link347)
####################method 348####################
function348<- function(x,y){
  
  x$bnstreet <- paste(x$buildingname,x$paotext,sep=",")
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
link348<-link348[,..needlist11]

link348u<- uniqueresult(link348)
link348d <- doubleresult(link348)
 
epc <- matchleft(epc,link348)
####################method 349####################
function349<- function(x,y){
  x$bnstreet <- paste(x$buildingnumber,x$streetdescription,sep=",")
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
link349<-function349(add,epc)
link349<-link349[,..needlist11]

link349u<- uniqueresult(link349)
link349d <- doubleresult(link349)

epc <- matchleft(epc,link349)
####################method 350####################
function350<- function(x,y){
  x$bnstreet <- paste(x$saotext,x$paotext,sep=",")
  x$bnstreet <- paste(x$bnstreet,x$streetdescription,sep=",")
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
link350<-link350[,..needlist11]

link350u<- uniqueresult(link350)
link350d <- doubleresult(link350)

epc <- matchleft(epc,link350)
####################method 351####################
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
link351d <- doubleresult(link351)

epc <- matchleft(epc,link351)
####################method 352####################
function352<- function(x,y){
  x$bnstreet <- paste(x$saotext,x$paotext,sep=",")
  x$bnstreet <- paste(x$bnstreet,x$buildingnumber,sep=",")
  x$bnstreet <- paste(x$bnstreet,x$streetdescription,sep=" ")
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
link352d <- doubleresult(link352)

epc <- matchleft(epc,link352)
####################method 353####################
function353<- function(x,y){
  x$bnstreet <- paste(x$saotext,x$paotext,sep=",")
  x$bnstreet <- paste(x$bnstreet,x$pp,sep=",")
  x$bnstreet <- paste(x$bnstreet,x$streetdescription,sep=" ")
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
link353<-link353[,..needlist11]

link353u<- uniqueresult(link353)
link353d <- doubleresult(link353)

epc <- matchleft(epc,link353)
####################method 354####################
function354<- function(x,y){
  x$bnstreet <- paste(x$saotext,x$paotext,sep=",")
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
link354d <- doubleresult(link354)
 
epc<- matchleft(epc,link354)

####################method 355####################
function355<- function(x,y){
  x$bnstreet <- paste(x$saotext,x$paotext,sep=",")
  x$bnstreet <- paste(x$bnstreet,x$paostartnumber,sep=",")
  x$bnstreet <- paste(x$bnstreet,x$paoendnumber,sep="-")
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
link355d <- doubleresult(link355)

epc <- matchleft(epc,link355)
####################method 356####################
function356<- function(x,y){
  x$bnstreet <- paste(x$saotext,x$streetdescription,sep=" ")
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
link356<-link356[,..needlist1]

link356u<- uniqueresult(link356)
link356d <- doubleresult(link356)

epc <- matchleft(epc,link356)
####################method 357####################
function357<- function(x,y){
  x$bnstreet <- paste(x$saotext,x$paotext,sep=" ")
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
link357<-link357[,..needlist11]

link357u<- uniqueresult(link357)
link357d <- doubleresult(link357)

epc <- matchleft(epc,link357)
####################method 358####################
function358<- function(x,y){
  x$bnstreet <- paste(x$subbuildingname,x$paotext,sep=",")
  x$bnstreet <- paste(x$bnstreet,x$streetdescription,sep=",")
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
link358<-link358[,..needlist11]

link358u<- uniqueresult(link358)
link358d <- doubleresult(link358)

epc <- matchleft(epc,link358)
####################method 359####################
function359<- function(x,y){
  x$bnstreet <- paste("STUDIO ",x$subbuildingname,sep=" ")
  x$bnstreet <- paste(x$bnstreet,x$paotext,sep=" ")
  x$bnstreet <- paste(x$bnstreet,x$buildingnumber,sep=" ")
  x$bnstreet <- paste(x$bnstreet,x$streetdescription,sep=" ")
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
link359d <- doubleresult(link359)

epc <- matchleft(epc,link359)
####################sum up####################
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

l300_359u = list(link300u,link301u,link302u,link303u,link304u,link305u,link306u,link307u,link308u,link309u,
                 link310u,link311u,link312u,link313u,link314u,link315u,link316u,link317u,link318u,link319u,
                 link320u,link321u,link322u,link323u,link324u,link325u,link326u,link327u,link328u,link329u,
                 link330u,link331u,link332u,link333u,link334u,link335u,link336u,link337u,link338u,link339u,
                 link340u,link341u,link342u,link343u,link344u,link345u,link346u,link347u,link348u,link349u,
                 link350u,link351u,link352u,link353u,link354u,link355u,link356u,link357u,link358u,link359u)


link300_359u<- rbindlist(l300_359u,use.names=TRUE,fill=T)



l300_359d = list(link300d,link301d,link302d,link303d,link304d,link305d,link306d,link307d,link308d,link309d,
                 link310d,link311d,link312d,link313d,link314d,link315d,link316d,link317d,link318d,link319d,
                 link320d,link321d,link322d,link323d,link324d,link325d,link326d,link327d,link328d,link329d,
                 link330d,link331d,link332d,link333d,link334d,link335d,link336d,link337d,link338d,link339d,
                 link340d,link341d,link342d,link343d,link344d,link345d,link346d,link347d,link348d,link349d,
                 link350d,link351d,link352d,link353d,link354d,link355d,link356d,link357d,link358d,link359d)

link300_359d<- rbindlist(l300_359d,use.names=TRUE,fill=T)

dbWriteTable(con, "link300_359d",value =link300_359d, append =  TRUE, row.names = FALSE)
dbWriteTable(con, "link300_359u",value =link300_359u, append =  TRUE, row.names = FALSE)


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

####################method 360####################
function360<- function(x,y){
  x<-x[x$paotext!="",]
  x$bnstreet <- paste(x$saotext,x$paotext,sep=" ")
  x$bnstreet <- gsub("/", "", x$bnstreet)
  x$bnstreet <- gsub("[.]", "", x$bnstreet)
  x$bnstreet <- gsub("[']", "", x$bnstreet)
  x$bnstreet <- gsub(" ", "", x$bnstreet)
  x$addressf <-paste(x$postset,x$bnstreet,sep=",")
  
  y<-y[y$property_type=="Flat"|y$property_type=="Maisonette",]
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
link360<-link360[,..needlist11]

link360u<- uniqueresult(link360)
link360d <- doubleresult(link360)

epc <- matchleft(epc,link360)
####################method 361####################
function361<- function(x,y){
  x$bnstreet <- paste(x$saotext,x$buildingname,sep=",")
  x$bnstreet <- paste(x$bnstreet,x$streetdescription,sep=",")
  x$bnstreet <- paste(x$bnstreet,x$dependentlocality ,sep=",")
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
link361d <- doubleresult(link361)

epc <- matchleft(epc,link361)
####################method 362####################
function362<- function(x,y){
  x$bnstreet <- paste(x$subbuildingname,x$buildingnumber,sep=",")
  x$bnstreet <- paste(x$bnstreet,x$streetdescription,sep=" ")
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
link362d <- doubleresult(link362)

epc <- matchleft(epc,link362)
####################method 363####################
function363<- function(x,y){
  x<-x[x$subbuildingname=="",]
  x$bnstreet <- paste(x$ss,x$streetdescription,sep=",")
  x$bnstreet <- gsub(" ", "", x$bnstreet)
  x$addressf <-paste(x$postset,x$bnstreet,sep=",")
  
  y$addressfinal <- trimws(y$add)
  y$addressfinal <- gsub(" ", "", y$addressfinal)
  y$addressf <- paste(y$postset,y$addressfinal,sep=",")
  
  taba1 <- inner_join(x,y,by="addressf")
  
  return(taba1)
}
link363<-function363(add,epc)
link363<-link363[,..needlist11]

link363u<- uniqueresult(link363)
link363d <- doubleresult(link363)

epc <- matchleft(epc,link363)
####################method 364####################
function364<- function(x,y){
  x$bnstreet <- paste(x$saotext,x$paotext,sep=",")
  x$bnstreet <- paste(x$bnstreet,x$paostartnumber,sep=",")
  x$bnstreet <- paste(x$bnstreet,x$paoendnumber,sep="-")
  x$bnstreet <- paste(x$bnstreet,x$streetdescription,sep=" ")
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
link364d <- doubleresult(link364)

epc<- matchleft(epc,link364)
####################method 365####################
function365<- function(x,y){
  x$bnstreet <- paste(x$subbuildingname,x$paotext,sep=",")
  x$bnstreet <- paste(x$bnstreet,x$paostartnumber,sep=",")
  x$bnstreet <- paste(x$bnstreet,x$paoendnumber,sep="-")
  x$bnstreet <- paste(x$bnstreet,x$streetdescription,sep=" ")
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

link365u <- uniqueresult(link365)
link365d <- doubleresult(link365)

epc <- matchleft(epc,link365)

####################method 366####################
function366<- function(x,y){
  x$bnstreet <- paste(x$saotext,"NO",sep=",")
  x$bnstreet <- paste(x$bnstreet,x$pp,sep=" ")
  x$bnstreet <- paste(x$bnstreet,x$streetdescription,sep=" ")
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
link366<-link366[,..needlist11]

link366u<- uniqueresult(link366)
link366d <- doubleresult(link366)

epc <- matchleft(epc,link366)
####################method 367####################
function367<- function(x,y){
  x$bnstreet <- paste(x$saotext,x$pp,sep=",")
  x$bnstreet <- paste(x$bnstreet,x$streetdescription,sep=" ")
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
link367<-link367[,..needlist11]

link367u<- uniqueresult(link367)
link367d <- doubleresult(link367)

epc <- matchleft(epc,link367)
#################### method 368 ##################
function368<- function(x,y){
  x$bnstreet <- paste(x$saotext,x$buildingnumber,sep=",")
  x$bnstreet <- paste(x$bnstreet,x$streetdescription,sep=" ")
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
link368<-link368[,..needlist11]

link368u<- uniqueresult(link368)
link368d <- doubleresult(link368)

epc <- matchleft(epc,link368)
####################method 369####################
function369<- function(x,y){
  x$bnstreet <- paste(x$subbuildingname,x$buildingnumber,sep=",")
  x$bnstreet <- paste(x$bnstreet,x$streetdescription,sep=" ")
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
link369d <- doubleresult(link369)

epc <- matchleft(epc,link369)
####################method 370####################
function370<- function(x,y){
  x$bnstreet <- paste(x$saotext,x$pp,sep=",")
  x$bnstreet <- paste(x$bnstreet,x$streetdescription,sep=" ")
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
link370<-link370[,..needlist11]

link370u<- uniqueresult(link370)
link370d <- doubleresult(link370)

epc <- matchleft(epc,link370)
####################method 371####################
function371<- function(x,y){
  x$bnstreet <- paste(x$subbuildingname,x$buildingnumber,sep=",")
  x$bnstreet <- paste(x$bnstreet,x$streetdescription,sep=" ")
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
link371d <- doubleresult(link371)

epc <- matchleft(epc,link371)
####################method 372####################
function372<- function(x,y){
  x$bnstreet <- paste(x$subbuildingname,x$paotext,sep=" ")
  x$bnstreet <- paste(x$bnstreet,x$buildingnumber,sep=",")
  x$bnstreet <- paste(x$bnstreet,x$streetdescription,sep=" ")
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
link372d <- doubleresult(link372)

epc <- matchleft(epc,link372)
####################method 373####################
function373<- function(x,y){
  x$bnstreet <- paste(x$saotext,x$paostartnumber,sep=",")
  x$bnstreet <- paste(x$bnstreet,x$paoendnumber,sep="-")
  x$bnstreet <- paste(x$bnstreet,x$streetdescription,sep=" ")
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
link373<-link373[,..needlist11]

link373u<- uniqueresult(link373)
link373d <- doubleresult(link373)

epc <- matchleft(epc,link373)
####################method 374####################
function374<- function(x,y){
  x$bnstreet <- paste(x$ss,x$paotext,sep=" ")
  x$bnstreet <- paste(x$bnstreet,x$streetdescription,sep=",")
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
link374d <- doubleresult(link374)
epc<- matchleft(epc,link374)
####################method 375####################
function375<- function(x,y){
  x$bnstreet <- paste(x$saotext,x$paotext,sep=",")
  x$bnstreet <- paste(x$bnstreet,x$locality,sep=",")
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
link375d <- doubleresult(link375)

epc <- matchleft(epc,link375)
####################method 376####################
function376<- function(x,y){
  x$bnstreet <- paste(x$saotext,x$pp,sep=",")
  x$bnstreet <- paste(x$bnstreet,x$streetdescription,sep=" ")
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
link376<-link376[,..needlist11]

link376u<- uniqueresult(link376)
link376d <- doubleresult(link376)

epc <- matchleft(epc,link376)
####################method 377####################
function377<- function(x,y){
  x$bnstreet <- paste(x$saotext,x$pp,sep=",")
  x$bnstreet <- paste(x$bnstreet,x$streetdescription,sep=" ")
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
link377<-link377[,..needlist11]

link377u<- uniqueresult(link377)
link377d <- doubleresult(link377)

epc <- matchleft(epc,link377)
####################method 378####################
function378<- function(x,y){
  x$bnstreet <- paste(x$subbuildingname,x$buildingnumber,sep=",")
  x$bnstreet <- paste(x$bnstreet,x$streetdescription,sep=" ")
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
link378<-link378[,..needlist11]

link378u<- uniqueresult(link378)
link378d <- doubleresult(link378)

epc <- matchleft(epc,link378)
####################method 379####################
function379<- function(x,y){
  x$bnstreet <- paste(x$subbuildingname,x$buildingnumber,sep=",")
  x$bnstreet <- paste(x$bnstreet,x$streetdescription,sep=" ")
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
link379d <- doubleresult(link379)

epc <- matchleft(epc,link379)
####################method 380####################
function380<- function(x,y){
  x$bnstreet <- paste(x$subbuildingname,x$buildingnumber,sep=",")
  x$bnstreet <- paste(x$bnstreet,x$streetdescription,sep=" ")
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
link380<-link380[,..needlist11]

link380u<- uniqueresult(link380)
link380d <- doubleresult(link380)

epc <- matchleft(epc,link380)
####################method 381####################
function381<- function(x,y){
  x$bnstreet <- paste("FLAT ",x$subbuildingname,sep="")
  x$bnstreet <- paste(x$bnstreet,x$paotext,sep=",")
  x$bnstreet <- paste(x$bnstreet,x$buildingnumber,sep=",")
  x$bnstreet <- paste(x$bnstreet,x$streetdescription,sep=" ")
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
link381d <- doubleresult(link381)

epc <- matchleft(epc,link381)
####################method 382###################
function382<- function(x,y){
  x$bnstreet <- paste(x$saotext,x$buildingname,sep=",")
  x$bnstreet <- paste(x$bnstreet,x$streetdescription,sep=" ")
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
link382d <- doubleresult(link382)

epc <- matchleft(epc,link382)
####################method 383####################
function383<- function(x,y){
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
link383<-link383[,..needlist11]

link383u<- uniqueresult(link383)
link383d <- doubleresult(link383)

epc <- matchleft(epc,link383)
####################method 384v
function384<- function(x,y){
  x$bnstreet <-  paste(x$subbuildingname,x$buildingnumber,sep=",")
  x$bnstreet <- paste(x$bnstreet,x$streetdescription,sep=" ")
 
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
link384d <- doubleresult(link384)

epc<- matchleft(epc,link384)
####################method 385####################
function385<- function(x,y){
  x$bnstreet <- paste(x$subbuildingname,x$buildingnumber,sep=",")
  x$bnstreet <- paste(x$bnstreet,x$streetdescription,sep=" ")
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
link385d <- doubleresult(link385)

epc <- matchleft(epc,link385)
####################method 386####################
function386<- function(x,y){
  x$bnstreet <- paste(x$saotext,x$pp,sep=",")
  x$bnstreet <- paste(x$bnstreet,x$streetdescription,sep=" ")
  x$bnstreet <- gsub("[.]", "", x$bnstreet)
  x$bnstreet <- gsub("[/]", "", x$bnstreet)
  x$bnstreet <- gsub("[']", "", x$bnstreet)
  x$bnstreet <- gsub(" ", "", x$bnstreet)
  x$addressf <-paste(x$postset,x$bnstreet,sep=",")
  
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
link386<-link386[,..needlist11]

link386u<- uniqueresult(link386)
link386d <- doubleresult(link386)

epc <- matchleft(epc,link386)
####################method 387####################
function387<- function(x,y){
  x$bnstreet <- paste(x$ss,x$paotext,sep=",")
  x$bnstreet <- paste(x$bnstreet,x$streetdescription,sep=",")
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
link387<-link387[,..needlist11]

link387u<- uniqueresult(link387)
link387d <- doubleresult(link387)

epc <- matchleft(epc,link387)
####################method 388####################
function388<- function(x,y){
  x$bnstreet <- x$buildingname
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
link388<-link388[,..needlist1]

link388u<- uniqueresult(link388)
link388d <- doubleresult(link388)

epc <- matchleft(epc,link388)
####################method 389####################
function389<- function(x,y){
  x$bnstreet <- paste(x$subbuildingname,x$buildingnumber,sep=",")
  x$bnstreet <- paste(x$bnstreet,x$streetdescription,sep=" ")
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
link389d <- doubleresult(link389)

epc <- matchleft(epc,link389)
####################method 390####################
function390<- function(x,y){
  x$bnstreet <- paste(x$saotext,x$pp,sep=",")
  x$bnstreet <- paste(x$bnstreet,x$streetdescription,sep=" ")
  x$bnstreet <- gsub("[.]", "", x$bnstreet)
  x$bnstreet <- gsub("[/]", "", x$bnstreet)
  x$bnstreet <- gsub("[']", "", x$bnstreet)
  x$bnstreet <- gsub(" ", "", x$bnstreet)
  x$addressf <-paste(x$postset,x$bnstreet,sep=",")
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
link390<-link390[,..needlist11]

link390u<- uniqueresult(link390)
link390d <- doubleresult(link390)

epc <- matchleft(epc,link390)
####################method 391####################
function391<- function(x,y){
  x$bnstreet <- paste(x$subbuildingname,x$paostartnumber,sep=",")
  x$bnstreet <- paste(x$bnstreet,x$paoendnumber,sep="-")
  x$bnstreet <- paste(x$bnstreet,x$streetdescription,sep=" ")
  x$bnstreet <- gsub("[.]", "", x$bnstreet)
  x$bnstreet <- gsub("[/]", "", x$bnstreet)
  x$bnstreet <- gsub("[']", "", x$bnstreet)
  x$bnstreet <- gsub(" ", "", x$bnstreet)
  x$addressf <-paste(x$postset,x$bnstreet,sep=",")

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
link391d <- doubleresult(link391)

epc <- matchleft(epc,link391)
####################method 392####################
function392<- function(x,y){
  x$bnstreet <- paste(x$saotext,x$paotext,sep=" ")
  x$bnstreet <- paste(x$bnstreet,x$pp,sep=",")
  x$bnstreet <- paste(x$bnstreet,x$streetdescription,sep=" ")
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
link392d <- doubleresult(link392)

epc <- matchleft(epc,link392)
####################method 393####################
function393<- function(x,y){
  x$bnstreet <- paste(x$saotext,x$paotext,sep=",")
  x$bnstreet <- paste(x$bnstreet,x$pp,sep=",")
  x$bnstreet <- paste(x$bnstreet,x$streetdescription,sep=" ")
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
link393<-link393[,..needlist11]

link393u<- uniqueresult(link393)
link393d <- doubleresult(link393)

epc <- matchleft(epc,link393)
####################method 394####################
function394<- function(x,y){
  x$bnstreet <- paste(x$subbuildingname,x$buildingname,sep=" ")
  x$bnstreet <- paste(x$bnstreet,x$buildingnumber,sep=",")
  x$bnstreet <- paste(x$bnstreet,x$streetdescription,sep=",")
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
link394d <- doubleresult(link394)

epc<- matchleft(epc,link394)
####################method 395####################
function395<- function(x,y){
  x$bnstreet <- paste(x$subbuildingname,x$paotext,sep=",")
  x$bnstreet <- paste(x$bnstreet,x$pp,sep=",")
  x$bnstreet <- paste(x$bnstreet,x$streetdescription,sep=",")
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
link395d <- doubleresult(link395)

epc <- matchleft(epc,link395)
####################method 396####################
function396<- function(x,y){
  x$bnstreet <- paste(x$subbuildingname,x$paotext,sep=",")
  x$bnstreet <- paste(x$bnstreet,x$pp,sep=",")
  x$bnstreet <- paste(x$bnstreet,x$streetdescription,sep=" ")
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
link396<-link396[,..needlist11]

link396u<- uniqueresult(link396)
link396d <- doubleresult(link396)
epc <- matchleft(epc,link396)
####################method 397####################
function397<- function(x,y){
  x<-x[x$subbuildingname=="",]
  x$bnstreet <- paste("FLAT",x$buildingname,sep=" ")
  x$bnstreet <- paste(x$bnstreet,x$streetdescription,sep=",")
  x$bnstreet <- gsub("/", "", x$bnstreet)
  x$bnstreet <- gsub("[.]", "", x$bnstreet)
  x$bnstreet <- gsub("[']", "", x$bnstreet)
  x$bnstreet <- gsub(" ", "", x$bnstreet)
  x$addressf <-paste(x$postset,x$bnstreet,sep=",")
  
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
link397<-link397[,..needlist11]

link397u<- uniqueresult(link397)
link397d <- doubleresult(link397)

epc <- matchleft(epc,link397)
####################method 398####################
function398<- function(x,y){
  x$bnstreet <- paste("FLAT",x$subbuildingname,sep=" ")
  x$bnstreet <- paste(x$bnstreet,x$paostartnumber,sep=",")
  x$bnstreet <- paste(x$bnstreet,x$paoendnumber,sep="-")
  x$bnstreet <- paste(x$bnstreet,x$streetdescription,sep=" ")
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
link398<-link398[,..needlist11]

link398u<- uniqueresult(link398)
link398d <- doubleresult(link398)

epc <- matchleft(epc,link398)
####################method 399####################
function399<- function(x,y){
  x$bnstreet <- paste("FLAT",x$buildingname,sep=" ")
  x$bnstreet <- paste(x$bnstreet,x$streetdescription,sep=",")
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
link399d <- doubleresult(link399)

epc <- matchleft(epc,link399)
####################method 400####################
function400<- function(x,y){
  x$bnstreet <- paste(x$saotext,x$pp,sep=",")
  x$bnstreet <- paste(x$bnstreet,x$streetdescription,sep=" ")
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
link400<-link400[,..needlist11]

link400u<- uniqueresult(link400)
link400d <- doubleresult(link400)

epc <- matchleft(epc,link400)
####################sum up####################
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

l360_400u = list(link360u,link361u,link362u,link363u,link364u,link365u,link366u,link367u,link368u,link369u,
                 link370u,link371u,link372u,link373u,link374u,link375u,link376u,link377u,link378u,link379u,
                 link380u,link381u,link382u,link383u,link384u,link385u,link386u,link387u,link388u,link389u,
                 link390u,link391u,link392u,link393u,link394u,link395u,link396u,link397u,link398u,link399u,
                 link400u)

link360_400u <- rbindlist(l360_400u,use.names=TRUE,fill=T)

l360_400d = list(link360d,link361d,link362d,link363d,link364d,link365d,link366d,link367d,link368d,link369d,
                 link370d,link371d,link372d,link373d,link374d,link375d,link376d,link377d,link378d,link379d,
                 link380d,link381d,link382d,link383d,link384d,link385d,link386d,link387d,link388d,link389d,
                 link390d,link391d,link392d,link393d,link394d,link395d,link396d,link397d,link398d,link399d,
                 link400d)

link360_400d <- rbindlist(l360_400d ,use.names=TRUE,fill=T)

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
####################method 401####################
function401<- function(x,y){
  x$bnstreet <- paste(x$buildingname,x$subbuildingname,sep=",")
  x$bnstreet <- paste(x$bnstreet,x$pp,sep=",")
  x$bnstreet <- paste(x$bnstreet,x$streetdescription,sep=" ")
  x$bnstreet <- gsub("/", "", x$bnstreet)
  x$bnstreet <- gsub("[.]", "", x$bnstreet)
  x$bnstreet <- gsub("[']", "", x$bnstreet)
  x$bnstreet <- gsub(" ", "", x$bnstreet)
  x$addressf <-paste(x$postset,x$bnstreet,sep=",")
  
  y$addressfinal <- y$add
  y$addressfinal <- gsub("STUDIOS 51", "STUDIO 51", y$addressfinal)
  y$addressfinal <- gsub("APARTMENT", "FLAT", y$addressfinal)
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
link401d <- doubleresult(link401)

epc <- matchleft(epc,link401)
####################method 402####################
function402<- function(x,y){
  x$bnstreet <-    paste(x$subbuildingname,x$buildingname,sep=" ")
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
link402d <- doubleresult(link402)

epc <- matchleft(epc,link402)
####################method 403####################
function403<- function(x,y){

  x$bnstreet <- paste(x$subbuildingname,x$paostartnumber,sep=",")
  x$bnstreet <- paste(x$bnstreet,x$paoendnumber,sep="-")
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

link403<-function403(add,epc)
link403<-link403[,..needlist11]

link403u<- uniqueresult(link403)
link403d <- doubleresult(link403)

epc <- matchleft(epc,link403)
####################method 404####################
function404<- function(x,y){
  x$bnstreet <-    paste(x$ss,x$paotext,sep=" ")
  x$bnstreet <-    paste(x$bnstreet,x$streetdescription,sep=",")
  x$bnstreet <- gsub("[.]", "", x$bnstreet)
  x$bnstreet <- gsub("[/]", "", x$bnstreet)
  x$bnstreet <- gsub("[']", "", x$bnstreet)
  x$bnstreet <- gsub(" ", "", x$bnstreet)
  x$addressf <-paste(x$postset,x$bnstreet,sep=",")

  y$addressfinal <- paste(y$add1,y$add2,sep=",")
  y$addressfinal <- gsub("CLEARWATER", "CLEARWATER VILLAGE", y$addressfinal)
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
link404d <- doubleresult(link404)

epc<- matchleft(epc,link404)
####################method 405####################
function405<- function(x,y){
  x$bnstreet <- paste(x$saotext,x$pp,sep=",")
  x$bnstreet <- paste(x$bnstreet,x$paotext,sep=" ")
  x$bnstreet <- paste(x$bnstreet,x$streetdescription,sep=",")
  x$bnstreet <- gsub("[.]", "", x$bnstreet)
  x$bnstreet <- gsub("[/]", "", x$bnstreet)
  x$bnstreet <- gsub("[']", "", x$bnstreet)
  x$bnstreet <- gsub(" ", "", x$bnstreet)
  x$addressf <-paste(x$postcodelocator,x$bnstreet,sep=",")

  y<-y[y$postcode=="RG1 4ET",]
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
link405d <- doubleresult(link405)

epc <- matchleft(epc,link405)
####################method 406####################
function406<- function(x,y){
  x$bnstreet <- paste(x$subbuildingname,x$buildingname,sep=",")
  x$bnstreet <- paste(x$bnstreet,x$buildingnumber,sep=",")
  x$bnstreet <- paste(x$bnstreet,x$streetdescription,sep=" ")
  x$bnstreet <- gsub(",", "", x$bnstreet)
  x$bnstreet <- gsub("-", "", x$bnstreet)
  x$bnstreet <- gsub("[.]", "", x$bnstreet)
  x$bnstreet <- gsub("[/]", "", x$bnstreet)
  x$bnstreet <- gsub("[']", "", x$bnstreet)
  x$bnstreet <- gsub(" ", "", x$bnstreet)
  x$addressf <-paste(x$postset,x$bnstreet,sep=",")

  y$addressfinal <- y$add
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
link406<-link406[,..needlist11]

link406u<- uniqueresult(link406)
link406d <- doubleresult(link406)

epc <- matchleft(epc,link406)
####################method 407####################
function407<- function(x,y){
  x$bnstreet <- paste(x$subbuildingname,x$buildingname,sep=",")
  x$bnstreet <- paste(x$bnstreet,x$streetdescription,sep=",")

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
link407<-link407[,..needlist11]

link407u<- uniqueresult(link407)
link407d <- doubleresult(link407)

epc <- matchleft(epc,link407)
####################method 408####################
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
link408<-link408[,..needlist11]

link408u<- uniqueresult(link408)
link408d <- doubleresult(link408)

epc <- matchleft(epc,link408)
#################### method 409 ##################
function409<- function(x,y){
  x$bnstreet <- paste(x$subbuildingname,x$buildingname,sep=",")
  x$bnstreet <- paste(x$bnstreet,x$locality,sep=" ")
  x$bnstreet <- paste(x$bnstreet,x$townname,sep=",")
  x$bnstreet <- gsub("[.]", "", x$bnstreet)
  x$bnstreet <- gsub("[/]", "", x$bnstreet)
  x$bnstreet <- gsub("[']", "", x$bnstreet)
  x$bnstreet <- gsub(" ", "", x$bnstreet)
  x$addressf <-paste(x$postset,x$bnstreet,sep=",")
 
  y$addressfinal <- y$add
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
link409d <- doubleresult(link409)
epc <- matchleft(epc,link409)
####################method 410####################
function410<- function(x,y){
  x$bnstreet <-    paste(x$saotext,x$buildingnumber,sep=" ")
  x$bnstreet <-    paste(x$bnstreet,x$streetdescription,sep=" ")
  x$bnstreet <- gsub("[.]", "", x$bnstreet)
  x$bnstreet <- gsub("[/]", "", x$bnstreet)
  x$bnstreet <- gsub("[']", "", x$bnstreet)
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
link410<-link410[,..needlist11]

link410u<- uniqueresult(link410)
link410d <- doubleresult(link410)

epc <- matchleft(epc,link410)
####################method 411####################
function411<- function(x,y){
  x$bnstreet <- paste(x$subbuildingname,x$pp,sep=",")
  x$bnstreet <- paste(x$bnstreet,x$streetdescription,sep=" ")
  x$bnstreet <- gsub("[.]", "", x$bnstreet)
  x$bnstreet <- gsub("[/]", "", x$bnstreet)
  x$bnstreet <- gsub("[']", "", x$bnstreet)
  x$bnstreet <- gsub(" ", "", x$bnstreet)
  x$addressf <-paste(x$postset,x$bnstreet,sep=",")
  
  y$addressfinal <- y$add
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
link411d <- doubleresult(link411)

epc <- matchleft(epc,link411)
####################method 412####################
function412<- function(x,y){
  x$bnstreet <- paste(x$subbuildingname,x$pp,sep=",")
  x$bnstreet <- paste(x$bnstreet,x$streetdescription,sep=" ")
  x$bnstreet <- gsub("[.]", "", x$bnstreet)
  x$bnstreet <- gsub("[/]", "", x$bnstreet)
  x$bnstreet <- gsub("[']", "", x$bnstreet)
  x$bnstreet <- gsub(" ", "", x$bnstreet)
  x$addressf <-paste(substring(x$postset,1,3),x$bnstreet,sep=",")
  
  y$addressfinal <- y$add
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
link412d <- doubleresult(link412)

epc <- matchleft(epc,link412)
####################method 413####################
function413<- function(x,y){
  x$bnstreet <- paste(x$subbuildingname,x$pp,sep=",")
  x$bnstreet <- paste(x$bnstreet,x$streetdescription,sep=" ")
  x$bnstreet <- gsub("[.]", "", x$bnstreet)
  x$bnstreet <- gsub("[/]", "", x$bnstreet)
  x$bnstreet <- gsub("[']", "", x$bnstreet)
  x$bnstreet <- gsub(" ", "", x$bnstreet)
  x$addressf <-paste(substring(x$postset,1,2),x$bnstreet,sep=",")
  
  y$addressfinal <- y$add
  y$addressfinal <- gsub(" ", "", y$addressfinal)
  y$addressfinal <- gsub("[.]", "", y$addressfinal)
  y$addressfinal <- gsub("[/]", "", y$addressfinal)
  y$addressfinal <- gsub("[']", "", y$addressfinal)
  y$addressf <- paste(substring(y$postset,1,2),y$addressfinal,sep=",")
  
  taba1 <- inner_join(x,y,by="addressf")
  
  return(taba1)
}
link413<-function413(add,epc)
link413<-link413[,..needlist11]

link413u<- uniqueresult(link413)
link413d <- doubleresult(link413)

epc <- matchleft(epc,link413)
####################method 414####################
function414<- function(x,y){
  x$bnstreet <- paste(x$saotext,x$buildingname,sep=",")
  x$bnstreet <- paste(x$bnstreet,x$buildingnumber,sep=",")
  x$bnstreet <- paste(x$bnstreet,x$streetdescription,sep=" ")
  x$bnstreet <- gsub("[.]", "", x$bnstreet)
  x$bnstreet <- gsub("[/]", "", x$bnstreet)
  x$bnstreet <- gsub("[']", "", x$bnstreet)
  x$bnstreet <- gsub(" ", "", x$bnstreet)
  x$addressf <-paste(x$postset,x$bnstreet,sep=",")
  

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
link414d <- doubleresult(link414)

epc<- matchleft(epc,link414)
####################method 415####################
function415<- function(x,y){
  x$bnstreet <- paste(x$saotext,x$pp,sep=",")
  x$bnstreet <- paste(x$bnstreet,x$streetdescription,sep=" ")
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
link415d <- doubleresult(link415)

epc <- matchleft(epc,link415)
####################method 416####################
function416<- function(x,y){
  x$bnstreet <-    paste(x$saotext,x$streetdescription,sep=" ")
  x$bnstreet <- gsub("[.]", "", x$bnstreet)
  x$bnstreet <- gsub("[/]", "", x$bnstreet)
  x$bnstreet <- gsub("[']", "", x$bnstreet)
  x$bnstreet <- gsub(" ", "", x$bnstreet)
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
link416<-link416[,..needlist11]

link416u<- uniqueresult(link416)
link416d <- doubleresult(link416)

epc <- matchleft(epc,link416)
####################method 417####################
function417<- function(x,y){
  x$bnstreet <-    paste(x$saotext,x$pp,sep=",")
  x$bnstreet <-    paste(x$bnstreet,x$streetdescription,sep=" ")
  x$bnstreet <- gsub("[.]", "", x$bnstreet)
  x$bnstreet <- gsub("[/]", "", x$bnstreet)
  x$bnstreet <- gsub("[']", "", x$bnstreet)
  x$bnstreet <- gsub(" ", "", x$bnstreet)
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
link417<-link417[,..needlist11]

link417u<- uniqueresult(link417)
link417d <- doubleresult(link417)

epc <- matchleft(epc,link417)
####################method 418####################
function418<- function(x,y){
  x$bnstreet <- paste("CHALET",x$ss ,sep=" ")
  x$bnstreet <- paste(x$bnstreet,x$paotext ,sep=",")
  x$bnstreet <- paste(x$bnstreet,x$streetdescription ,sep=",")
  x$bnstreet <- paste(x$bnstreet,x$townname ,sep=",")
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
link418<-function418(add,epc)
link418<-link418[,..needlist11]

link418u<- uniqueresult(link418)
link418d <- doubleresult(link418)

epc <- matchleft(epc,link418)
####################method 419####################
function419<- function(x,y){
  x$bnstreet <- paste(x$pp,x$streetdescription,sep=" ")
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
link419d <- doubleresult(link419)

epc <- matchleft(epc,link419)
####################method 420####################
function420<- function(x,y){
  x$bnstreet <- paste("FLAT",x$buildingname,sep=" ")
  x$bnstreet <- paste(x$bnstreet,x$streetdescription,sep=" ")
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
link420<-link420[,..needlist11]

link420u<- uniqueresult(link420)
link420d <- doubleresult(link420)

epc <- matchleft(epc,link420)
####################method 421####################
function421<- function(x,y){
  x<-x[x$saotext!="",]
  x<-x[x$paotext!="",]
  x$bnstreet <- paste(x$saotext,x$paotext,sep=" ")
  x$bnstreet <- gsub(",", "", x$bnstreet)
  x$bnstreet <- gsub("[.]", "", x$bnstreet)
  x$bnstreet <- gsub("[/]", "", x$bnstreet)
  x$bnstreet <- gsub("[']", "", x$bnstreet)
  x$bnstreet <- gsub(" ", "", x$bnstreet)
  x$addressf <-paste(x$postset,x$bnstreet,sep=",")

  y$addressfinal <- y$add1
  y$addressfinal <- gsub("HUMPHREY DAVY HOUSE", "HUMPHRY DAVY HOUSE", y$addressfinal)
  y$addressfinal <- gsub(",", "", y$addressfinal)
  y$addressfinal <- gsub(" ", "", y$addressfinal)
  y$addressfinal <- gsub("[.]", "", y$addressfinal)
  y$addressfinal <- gsub("[/]", "", y$addressfinal)
  y$addressfinal <- gsub("[']", "", y$addressfinal)
  y$addressf <- paste(y$postset,y$addressfinal,sep=",")
  
  taba1 <- inner_join(x,y,by="addressf")
  
  return(taba1)
}
link421<-function421(add,epc)
link421<-link421[,..needlist11]

link421u<- uniqueresult(link421)
link421d <- doubleresult(link421)

epc <- matchleft(epc,link421)
####################method 422####################
function422<- function(x,y){
  x<-x[x$saotext!="",]
  x$bnstreet <- paste(x$saotext,x$streetdescription,sep=" ")
  x$bnstreet <- gsub("[.]", "", x$bnstreet)
  x$bnstreet <- gsub("[/]", "", x$bnstreet)
  x$bnstreet <- gsub("[']", "", x$bnstreet)
  x$bnstreet <- gsub(" ", "", x$bnstreet)
  x$addressf <-paste(x$postset,x$bnstreet,sep=",")
  
  y<-y[!grepl("\\d\\d?[,]\\d\\d?",y$add1) ,]
  y<-y[!grepl("\\d\\d?[,]\\s\\d\\d?",y$add1) ,]
  y$addressfinal <- y$add1
  y$addressfinal <- gsub("HUMPHREY DAVY HOUSE", "HUMPHRY DAVY HOUSE", y$addressfinal)
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
link422d <- doubleresult(link422)

epc <- matchleft(epc,link422)
####################method 423####################
function423<- function(x,y){
  x$bnstreet <- paste("APARTMENT",x$buildingname,sep=" ")
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
link423<-link423[,..needlist11]

link423u<- uniqueresult(link423)
link423d <- doubleresult(link423)

epc <- matchleft(epc,link423)
####################method 424####################
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
link424d <- doubleresult(link424)

epc<- matchleft(epc,link424)
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
link425d <- doubleresult(link425)

epc <- matchleft(epc,link425)
####################method 426####################
function426<- function(x,y){
  x$bnstreet <- paste(x$saotext,x$paotext,sep=",")
  x$bnstreet <- paste(x$bnstreet,x$pp,sep=",")
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
link426<-link426[,..needlist11]

link426u<- uniqueresult(link426)
link426d <- doubleresult(link426)

epc <- matchleft(epc,link426)
####################method 427####################
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
link427<-link427[,..needlist11]

link427u<- uniqueresult(link427)
link427d <- doubleresult(link427)

epc <- matchleft(epc,link427)
####################method 428####################
function428<- function(x,y){
  x<-x[grepl("^\\d", x$buildingname),]
  x$bnstreet1<-word(x$buildingname,1)
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
link428<-link428[,..needlist11]

link428u<- uniqueresult(link428)
link428d <- doubleresult(link428)

epc <- matchleft(epc,link428)
####################method 429####################
function429<- function(x,y){
  x$bnstreet <- paste(x$buildingname,x$streetdescription,sep=",")
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
  y$addressfinal <- gsub(" ", "", y$addressfinal)
  y$addressf <- paste(y$postset,y$addressfinal,sep=",")
  
  taba1 <- inner_join(x,y,by="addressf")
  
  return(taba1)
}
link429<-function429(add,epc)
link429<-link429[,..needlist11]


link429u<- uniqueresult(link429)
link429d <- doubleresult(link429)

epc <- matchleft(epc,link429)
####################method 430####################
function430<- function(x,y){
  x$bnstreet <- paste(x$saotext,x$paotext,sep=",")
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
link430<-link430[,..needlist11]

link430u<- uniqueresult(link430)
link430d <- doubleresult(link430)

epc <- matchleft(epc,link430)
####################method 431####################
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
  y$addressfinal <- gsub(" ", "", y$addressfinal)
  y$addressf <- paste(y$postset,y$addressfinal,sep=",")
  
  taba1 <- inner_join(x,y,by="addressf")
  
  return(taba1)
}
link431<-function431(add,epc)
link431<-link431[,..needlist11]

link431u<- uniqueresult(link431)
link431d <- doubleresult(link431)

epc <- matchleft(epc,link431)
####################method 432####################
function432<- function(x,y){
  x<-x[grepl("\\d$",x$paotext),]
  x$paotext<- trimws(x$paotext)
  x$bnstreet1 <- word(x$paotext,-1)
  x$bnstreet2 <- str_match(x$paotext, "(^.+)\\s")[, 2]
  x$bnstreet <- paste(x$bnstreet1,x$bnstreet2,sep=" ")
  x$bnstreet <- paste(x$bnstreet,x$locality,sep=",")
  x$bnstreet <- paste(x$bnstreet,x$townname,sep=",")

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
link432d <- doubleresult(link432)

epc <- matchleft(epc,link432)
####################method 433####################
function433<- function(x,y){
  x$bnstreet <- paste(x$saotext,x$paotext,sep=" ")
  x$bnstreet <- paste(x$bnstreet,x$streetdescription,sep=" ")
  x$bnstreet <- paste(x$bnstreet,x$townname,sep=" ")
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
link433<-link433[,..needlist1]

link433u<- uniqueresult(link433)
link433d <- doubleresult(link433)

epc <- matchleft(epc,link433)
####################method 434####################
function434<- function(x,y){
  x<-x[x$buildingname!="",]
  x$bnstreet1<-word(x$buildingname,1,2)
  x$bnstreet2<-word(x$buildingname,-1)
  x$bnstreet <- paste(x$bnstreet2,x$subbuildingname,sep="")
  x$bnstreet <- paste(x$bnstreet,x$bnstreet1,sep="")
  x$bnstreet <- paste(x$bnstreet,x$pp,sep=",")
  x$bnstreet <- paste(x$bnstreet,x$streetdescription,sep=",")
  x$bnstreet <- gsub(" ", "", x$bnstreet)
  x$addressf <-paste(x$postcodelocator,x$bnstreet,sep=",")
  
  y$addressfinal <- trimws(y$add)
  y$addressfinal <- gsub(" ", "", y$addressfinal)
  y$addressf <- paste(y$postcode,y$addressfinal,sep=",")
  
  taba1 <- inner_join(x,y,by="addressf")
  
  return(taba1)
}
link434<-function434(add,epc)
link434<-link434[,..needlist1]

link434u<- uniqueresult(link434)
link434d <- doubleresult(link434)

epc<- matchleft(epc,link434)

####################method 435####################
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
link435d <- doubleresult(link435)

epc <- matchleft(epc,link435)
####################method 436####################
function436<- function(x,y){
  x$bnstreet <- paste(x$subbuildingname,x$buildingnumber,sep=",")
  x$bnstreet <- paste(x$bnstreet,x$streetdescription,sep=",")
  x$bnstreet <- gsub(",", "", x$bnstreet)
  x$bnstreet <- gsub("/", "", x$bnstreet)
  x$bnstreet <- gsub("[.]", "", x$bnstreet)
  x$bnstreet <- gsub("[']", "", x$bnstreet)
  x$bnstreet <- gsub(" ", "", x$bnstreet)
  x$addressf <-paste(x$postcodelocator,x$bnstreet,sep=",")
  

  y$addressfinal <- word(y$add1,1,2)
  y$addressfinal <-  paste(y$addressfinal,y$add2,sep=",")
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
link436<-link436[,..needlist1]

link436u<- uniqueresult(link436)
link436d <- doubleresult(link436)

epc <- matchleft(epc,link436)
####################method 437####################
function437<- function(x,y){
  x$bnstreet <- paste(x$saotext,x$paotext,sep=",")
  x$bnstreet <- paste(x$bnstreet,x$pp,sep=",")
  x$bnstreet <- paste(x$bnstreet,x$streetdescription,sep=" ")
  x$bnstreet <- gsub(",", "", x$bnstreet)
  x$bnstreet <- gsub("/", "", x$bnstreet)
  x$bnstreet <- gsub("[.]", "", x$bnstreet)
  x$bnstreet <- gsub("[']", "", x$bnstreet)
  x$bnstreet <- gsub(" ", "", x$bnstreet)
  x$addressf <-paste(x$postcodelocator,x$bnstreet,sep=",")
  
  y<-y[y$property_type=="Flat"|y$property_type=="Maisonette",]
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
link437<-link437[,..needlist1]

link437u<- uniqueresult(link437)
link437d <- doubleresult(link437)

epc <- matchleft(epc,link437)
####################method 438####################
function438<- function(x,y){
  x$bnstreet <-    paste(x$saotext,x$pp,sep=",")
  x$bnstreet <-    paste(x$bnstreet,x$streetdescription,sep=",")
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

  taba1 <- inner_join(x,y,by="addressf")

  return(taba1)
}
link438<-function438(add,epc)
link438<-link438[,..needlist1]

link438u<- uniqueresult(link438)
link438d <- doubleresult(link438)

epc <- matchleft(epc,link438)
####################method 439####################
function439<- function(x,y){
  x$bnstreet <- paste(x$saotext,x$pp,sep=",")
  x$bnstreet <- paste(x$bnstreet,x$streetdescription,sep=" ")
  x$bnstreet <- gsub("/", "", x$bnstreet)
  x$bnstreet <- gsub("[.]", "", x$bnstreet)
  x$bnstreet <- gsub("[']", "", x$bnstreet)
  x$bnstreet <- gsub(" ", "", x$bnstreet)
  x$addressf <-paste(x$postcodelocator,x$bnstreet,sep=",")

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
link439d <- doubleresult(link439)

epc <- matchleft(epc,link439)
####################method 440####################
function440<- function(x,y){
  x$bnstreet <- paste(x$saotext,x$paotext,sep=",")
  x$bnstreet <- paste(x$bnstreet,x$pp,sep=",")
  x$bnstreet <- paste(x$bnstreet,x$streetdescription,sep=" ")
  x$bnstreet <- gsub(",", "", x$bnstreet)
  x$bnstreet <- gsub("/", "", x$bnstreet)
  x$bnstreet <- gsub("[.]", "", x$bnstreet)
  x$bnstreet <- gsub("[']", "", x$bnstreet)
  x$bnstreet <- gsub(" ", "", x$bnstreet)
  x$addressf <-paste(x$postcodelocator,x$bnstreet,sep=",")
  
  y<-y[y$property_type=="Flat"|y$property_type=="Maisonette",]
  y$addressfinal <-  y$add
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
link440<-link440[,..needlist1]

link440u<- uniqueresult(link440)
link440d <- doubleresult(link440)

epc <- matchleft(epc,link440)
#################### method 441 ##################
function441<- function(x,y){
  x$bnstreet <- paste(x$saotext,x$paostartnumber,sep=",")
  x$bnstreet <- paste(x$bnstreet,x$paoendnumber,sep="-")
  x$bnstreet <- paste(x$bnstreet,x$streetdescription,sep=" ")
  x$bnstreet <- gsub("/", "", x$bnstreet)
  x$bnstreet <- gsub("[.]", "", x$bnstreet)
  x$bnstreet <- gsub("[']", "", x$bnstreet)
  x$bnstreet <- gsub(" ", "", x$bnstreet)
  x$addressf <-paste(x$postcodelocator,x$bnstreet,sep=",")
  
  y<-y[y$property_type=="Flat"|y$property_type=="Maisonette",]
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
link441d <- doubleresult(link441)

epc <- matchleft(epc,link441)
#################### method 442 ##################
function442<- function(x,y){
  x$bnstreet <- paste(x$saotext,x$paostartnumber,sep=",")
  x$bnstreet <- paste(x$bnstreet,x$paoendnumber,sep="-")
  x$bnstreet <- paste(x$bnstreet,x$streetdescription,sep=" ")
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
link442d <- doubleresult(link442)

epc <- matchleft(epc,link442)
####################method 443####################
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
link443<-link443[,..needlist1]

link443u<- uniqueresult(link443)
link443d <- doubleresult(link443)

epc <- matchleft(epc,link443)
####################method 444####################
function444<- function(x,y){
  x$bnstreet <- paste("FLAT",x$paostartsuffix,sep=" ")
  x$bnstreet <- paste(x$bnstreet,x$paostartnumber,sep=" ")
  x$bnstreet <- paste(x$bnstreet,x$streetdescription,sep=",")
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
link444d <- doubleresult(link444)

epc<- matchleft(epc,link444)
####################method 445####################
function445<- function(x,y){
  x<-x[x$paotext!=""]
  x$bnstreet1 <- word(x$paotext,-1)
  x$bnstreet2 <- word(x$paotext,1,2)
  
  x$bnstreet <- paste(x$bnstreet1,x$townname,sep=",")
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
link445<-function445(add,epc)
link445<-link445[,..needlist1]

link445u<- uniqueresult(link445)
link445d <- doubleresult(link445)

epc<- matchleft(epc,link445)

####################method 446####################
function446<- function(x,y){
  x<-x[x$paotext!=""]
  x$bnstreet1 <- word(x$paotext,-1)
  x$bnstreet2 <- word(x$paotext,1,2)
  x$bnstreet <- paste(x$bnstreet1,x$bnstreet2,sep=" ")
  x$bnstreet <- paste(x$bnstreet,x$townname,sep=",")
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
link446<-function446(add,epc)
link446<-link446[,..needlist1]

link446u<- uniqueresult(link446)
link446d <- doubleresult(link446)

epc<- matchleft(epc,link446)
####################sum up####################
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

l401_446u = list(link401u,link402u,link403u,link404u,link405u,link406u,link407u,link408u,link409u,
                 link410u,link411u,link412u,link413u,link414u,link415u,link416u,link417u,link418u,link419u,
                 link420u,link421u,link422u,link423u,link424u,link425u,link426u,link427u,link428u,link429u,
                 link430u,link431u,link432u,link433u,link434u,link435u,link436u,link437u,link438u,link439u,
                 link440u,link441u,link442u,link443u,link444u,link445u,link446u)


link401_446u <- rbindlist(l401_446u , use.names=TRUE, fill=TRUE)


l401_446d = list(link401d,link402d,link403d,link404d,link405d,link406d,link407d,link408d,link409d,
                 link410d,link411d,link412d,link413d,link414d,link415d,link416d,link417d,link418d,link419d,
                 link420d,link421d,link422d,link423d,link424d,link425d,link426d,link427d,link428d,link429d,
                 link430d,link431d,link432d,link433d,link434d,link435d,link436d,link437d,link438d,link439d,
                 link440d,link441d,link442d,link443d,link444d,link445d,link446d)

link401_446d <- rbindlist(l401_446d , use.names=TRUE, fill=TRUE)

dbWriteTable(con, "link401_446dnew",value =link401_446d, append = TRUE, row.names = FALSE)
dbWriteTable(con, "link401_446unew",value =link401_446u, append = TRUE, row.names = FALSE)

#delete data
rm(l401_446d,l401_446u)
#delete data
rm(link401d,link402d,link403d,link404d,link405d,link406d,link407d,link408d,link409d,
   link410d,link411d,link412d,link413d,link414d,link415d,link416d,link417d,link418d,link419d,
   link420d,link421d,link422d,link423d,link424d,link425d,link426d,link427d,link428d,link429d,
   link430d,link431d,link432d,link433d,link434d,link435d,link436d,link437d,link438d,link439d,
   link440d,link441d,link442d,link443d,link444d,link445d,link446d)
#delete data
rm(link401u,link402u,link403u,link404u,link405u,link406u,link407u,link408u,link409u,
   link410u,link411u,link412u,link413u,link414u,link415u,link416u,link417u,link418u,link419u,
   link420u,link421u,link422u,link423u,link424u,link425u,link426u,link427u,link428u,link429u,
   link430u,link431u,link432u,link433u,link434u,link435u,link436u,link437u,link438u,link439u,
   link440u,link441u,link442u,link443u,link444u,link445u,link446u)
#delete data
rm(link401,link402,link403,link404,link405,link406,link407,link408,link409,
   link410,link411,link412,link413,link414,link415,link416,link417,link418,link419,
   link420,link421,link422,link423,link424,link425,link426,link427,link428,link429,
   link430,link431,link432,link433,link434,link435,link436,link437,link438,link439,
   link440,link441,link442,link443,link444,link445,link446)

#delete funciton
rm(function401,function402,function403,function404,function405,function406,function407,function408,function409,
   function410,function411,function412,function413,function414,function415,function416,function417,function418,function419,
   function420,function421,function422,function423,function424,function425,function426,function427,function428,function429,
   function430,function431,function432,function433,function434,function435,function436,function437,function438,function439,
   function440,function441,function442,function443,function444,function445,function446)  
  
####################combined all the linked data####################
linku2<-rbindlist(list(linku1,link300_359u,link360_400u,link401_446u),use.names=TRUE, fill=TRUE)
linkd2<-rbindlist(list(linkd1,link300_359d,link360_400d,link401_446d),use.names=TRUE, fill=TRUE)
#save data in PostGIS
dbWriteTable(con, "linku",value =linku2, append = TRUE, row.names = FALSE)
dbWriteTable(con, "linkd",value =linkd2, append = TRUE, row.names = FALSE)
#delete data
rm(linkd1,link300_359d,link360_400d,link401_446d,linku1,link300_359u,link360_400u,link401_446u)


#############################part 2: linked data cleaning############################# 
