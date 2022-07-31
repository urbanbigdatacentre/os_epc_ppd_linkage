#############load package#######################
library("qdap")
library(data.table)
library("RPostgreSQL")
library("sqldf")
library("dplyr")
library(tidyverse)
library(stringr)
library("dplyr")


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



linkd<-linkd2
linkdcopy<-linkd2


########################cleaning process for the one to many linkage#####################################

####################clean 1 keep the residential UPRN####################
#extract the one to many linkage in the matching rule 1
c2<- linkd[linkd$method=="link1d",]
#select the residential uprn from the mutiple linked UPRN
linkd_1<-linkd[linkd$method=="link1d" & substr(linkd$class,1,1)=="R",]
#remove the linkd_1 from linkd
linkd<-matchleft(linkd,linkd_1)

####################clean 2 ####################
length(unique(linkd$method))
#206
sort(unique(linkd$method))
c2<- linkd[linkd$method=="link2d",]
head(c2)


linkd2_1<-linkd[linkd$method=="link2d" & linkd$saostartnumber=="" & substr(linkd$class,1,1)=="R" ,]
linkd2_1<-uniqueresult(linkd2_1)
dim(linkd2_1)
#6
linkd<-matchleft(linkd,linkd2_1)

c2<- linkd[linkd$method=="link2d",]
head(c2)

linkd2_parent<-linkd[linkd$method=="link2d" & linkd$saostartnumber!="",]

linkd<-matchleft(linkd,linkd2_parent)

c2<- linkd[linkd$method=="link2d",]

linkd<-matchleft(linkd,c2)
rm(c2)
####################clean 3 no one can keep############
c2<- linkd[linkd$method=="link3d",]

linkd<-matchleft(linkd,c2)
rm(c2)
dim(linkd)
#475067     34


####################clean 4############
c2<- linkd[linkd$method=="link4d",]


linkd4_1<-linkd[linkd$method=="link4d" & substr(linkd$class,1,1)=="R" ,]
linkd4_1<-uniqueresult(linkd4_1)
dim(linkd4_1)

linkd<-matchleft(linkd,linkd4_1)
dim(linkd)
# 475047     34
c2<- linkd[linkd$method=="link4d",]



linkd4_parent<-linkd[linkd$method=="link4d" & linkd$subbuildingname!="",]
linkd4_parent1<-keepneed(linkd,linkd4_parent)
rm(linkd4_parent)
linkd<-matchleft(linkd,linkd4_parent1)

c2<- linkd[linkd$method=="link4d",]


linkd4_parent<-c2[grepl("\\d",c2$buildingname),]
linkd4_parent2<-keepneed(linkd,linkd4_parent)
dim(linkd)
linkd<-matchleft(linkd,linkd4_parent2)
dim(linkd)
c2<- linkd[linkd$method=="link4d",]

linkd<-matchleft(linkd,c2)
rm(c2)
dim(linkd)
# 473660     34
linkd4_parent<-rbindlist(list(linkd4_parent1,linkd4_parent2))

rm(linkd4_parent1,linkd4_parent2)
####################clean 5############

c2<- linkd[linkd$method=="link5d",]


linkd5_1<-linkd[linkd$method=="link5d" & substr(linkd$class,1,1)=="R" ,]
linkd5_1<-uniqueresult(linkd5_1)


linkd<-matchleft(linkd,linkd5_1)

c2<- linkd[linkd$method=="link5d",]


linkd5_parent<-linkd[linkd$method=="link5d" & linkd$subbuildingname!="",]

linkd5_parent1<-keepneed(linkd,linkd5_parent)
dim(linkd)
linkd<-matchleft(linkd,linkd5_parent1)
dim(linkd)
rm(linkd5_parent)
c2<- linkd[linkd$method=="link5d",]


linkd5_parent<-c2[grepl("\\d",c2$buildingname),]

linkd5_parent2<-keepneed(linkd,linkd5_parent)

#linkd[linkd$method=="link5d" & linkd$subbuildingname!="",]

dim(linkd)
linkd<-matchleft(linkd,linkd5_parent2)
dim(linkd)

c2<- linkd[linkd$method=="link5d",]


linkd5_parent<-c2[grepl("FLOOR",c2$saotext),]
linkd5_parent3<-keepneed(linkd,linkd5_parent)
rm(linkd5_parent)


dim(linkd)
linkd<-matchleft(linkd,linkd5_parent3)
dim(linkd)
#453184
c2<- linkd[linkd$method=="link5d",]


dim(linkd)
linkd<-matchleft(linkd,c2)
rm(c2)
dim(linkd)
#453149     34

linkd5_parent<-rbindlist(list(linkd5_parent1,linkd5_parent2,linkd5_parent3))
rm(linkd5_parent1,linkd5_parent2,linkd5_parent3)
####################clean 6############

c2<- linkd[linkd$method=="link6d",]



linkd6_1<-linkd[linkd$method=="link6d" & substr(linkd$class,1,1)=="R" ,]
linkd6_1<-uniqueresult(linkd6_1)

linkd<-matchleft(linkd,linkd6_1)


c2<- linkd[linkd$method=="link6d",]


linkd6_parent<-linkd[linkd$method=="link6d" & linkd$saotext!="",]

linkd6_parent1<-keepneed(linkd,linkd6_parent)

linkd<-matchleft(linkd,linkd6_parent1)


c2<- linkd[linkd$method=="link6d",]


linkd6_parent<-linkd[linkd$method=="link6d" & linkd$saostartsuffix!="",]

linkd6_parent2<-keepneed(linkd,linkd6_parent)


linkd<-matchleft(linkd,linkd6_parent2)

c2<- linkd[linkd$method=="link6d",]


linkd6_parent<-linkd[linkd$method=="link6d" & linkd$saostartnumber!="",]

linkd6_parent3<-keepneed(linkd,linkd6_parent)

linkd<-matchleft(linkd,linkd6_parent3)


c2<- linkd[linkd$method=="link6d",]

linkd6_parent<-c2[grepl("FLAT",c2$paotext),]
linkd6_parent4<-keepneed(linkd,linkd6_parent)

linkd<-matchleft(linkd,linkd6_parent4)

c2<- linkd[linkd$method=="link6d",]


linkd6_parent<-c2[grepl("ROOM",c2$paotext),]
linkd6_parent5<-keepneed(linkd,linkd6_parent)

linkd<-matchleft(linkd,linkd6_parent5)

c2<- linkd[linkd$method=="link6d",]


linkd6_parent<-c2[grepl("FLOOR",c2$paotext),]
linkd6_parent6<-keepneed(linkd,linkd6_parent)

linkd<-matchleft(linkd,linkd6_parent6)

c2<- linkd[linkd$method=="link6d",]

linkd<-matchleft(linkd,c2)
rm(c2)

linkd6_parent<-rbindlist(list(linkd6_parent1,linkd6_parent2,linkd6_parent3,linkd6_parent4,linkd6_parent5,linkd6_parent6))
rm(linkd6_parent1,linkd6_parent2,linkd6_parent3,linkd6_parent4,linkd6_parent5,linkd6_parent6)

####################clean 7############
length(unique(linkd$method))
#206
sort(unique(linkd$method))

c2<- linkd[linkd$method=="link7d",]




linkd7_1<-linkd[linkd$method=="link7d" & substr(linkd$class,1,1)=="R" ,]
linkd7_1<-uniqueresult(linkd7_1)

linkd<-matchleft(linkd,linkd7_1)
dim(linkd)
#256095     34

c2<- linkd[linkd$method=="link7d",]


linkd7_parent<-linkd[linkd$method=="link7d" & linkd$subbuildingname!="",]

linkd7_parent1<-keepneed(linkd,linkd7_parent)
rm(linkd7_parent)

dim(linkd)
# 256095     34
linkd<-matchleft(linkd,linkd7_parent1)
dim(linkd)
#254597     34


c2<- linkd[linkd$method=="link7d",]


linkd<-matchleft(linkd,c2)
rm(c2)
dim(linkd)
# 254565     34
linkd7_parent<-linkd7_parent1
rm(linkd7_parent1)
####################clean 8############
c2<- linkd[linkd$method=="link8d",]



linkd8_1<-linkd[linkd$method=="link8d" & substr(linkd$class,1,1)=="R" ,]
linkd8_1<-uniqueresult(linkd8_1)

linkd<-matchleft(linkd,linkd8_1)
dim(linkd)

c2<- linkd[linkd$method=="link8d",]



linkd8_parent<-c2[grepl("\\d",c2$subbuildingname),]

linkd8_parent1<-keepneed(linkd,linkd8_parent)
rm(linkd8_parent)

dim(linkd)
linkd<-matchleft(linkd,linkd8_parent1)
dim(linkd)
#251743     34

c2<- linkd[linkd$method=="link8d",]


linkd8_parent<-c2[grepl("\\d",c2$saotext),]

linkd8_parent2<-keepneed(linkd,linkd8_parent)


dim(linkd)
linkd<-matchleft(linkd,linkd8_parent2)
dim(linkd)
#251620     34


c2<- linkd[linkd$method=="link8d",]


linkd8_parent<-c2[grepl("APARTMENT",c2$saotext),]

linkd8_parent3<-keepneed(linkd,linkd8_parent)

dim(linkd)
#251620     34
linkd<-matchleft(linkd,linkd8_parent3)
dim(linkd)
#251612     34
c2<- linkd[linkd$method=="link8d",]



linkd8_parent<-c2[grepl("FLAT",c2$saotext),]
linkd8_parent4<-keepneed(linkd,linkd8_parent)


dim(linkd)
#251612     34
linkd<-matchleft(linkd,linkd8_parent4)
dim(linkd)
#251589     34

c2<- linkd[linkd$method=="link8d",]


linkd8_parent<-c2[grepl("UNIT ",c2$saotext),]
linkd8_parent5<-keepneed(linkd,linkd8_parent)


dim(linkd)
# 251421     34
linkd<-matchleft(linkd,linkd8_parent5)
dim(linkd)
#251395     34

c2<- linkd[linkd$method=="link8d",]


linkd<-matchleft(linkd,c2)
rm(c2)
dim(linkd)
#248754     34
linkd8_parent<-rbindlist(list(linkd8_parent1,linkd8_parent2,linkd8_parent3,linkd8_parent4,linkd8_parent5))
rm(linkd8_parent1,linkd8_parent2,linkd8_parent3,linkd8_parent4,linkd8_parent5)

####################clean 9############

c2<- linkd[linkd$method=="link9d",]



linkd9_1<-linkd[linkd$method=="link9d" & substr(linkd$class,1,1)=="R" ,]
linkd9_1<-uniqueresult(linkd9_1)

linkd<-matchleft(linkd,linkd9_1)
dim(linkd)
#248725     34

c2<- linkd[linkd$method=="link9d",]


linkd9_parent<-linkd[linkd$method=="link9d" & linkd$buildingnumber!="",]

linkd9_parent1<-keepneed(linkd,linkd9_parent)
rm(linkd9_parent)

dim(linkd)
linkd<-matchleft(linkd,linkd9_parent1)
dim(linkd)
# 248706     34
c2<- linkd[linkd$method=="link9d",]

linkd9_parent<-c2[grepl("UNIT ",c2$saotext),]
linkd9_parent2<-keepneed(linkd,linkd9_parent)
rm(linkd9_parent)

dim(linkd)
# 
linkd<-matchleft(linkd,linkd9_parent2)
dim(linkd)
# 248636     34
c2<- linkd[linkd$method=="link9d",]


linkd9_parent<-c2[c2$saostartnumber!="",]

linkd9_parent3<-keepneed(linkd,linkd9_parent)
rm(linkd9_parent)

dim(linkd)
linkd<-matchleft(linkd,linkd9_parent3)
dim(linkd)
#rm(linkd9_parent)
c2<- linkd[linkd$method=="link9d",]


linkd<-matchleft(linkd,c2)
rm(c2)
dim(linkd)
#248555     34
linkd9_parent<-rbindlist(list(linkd9_parent1,linkd9_parent2,linkd9_parent3))
rm(linkd9_parent1,linkd9_parent2,linkd9_parent3)


####################clean 10############
c2<- linkd[linkd$method=="link10d",]

linkd10_1<-linkd[linkd$method=="link10d" & substr(linkd$class,1,1)=="R" ,]
linkd10_1<-uniqueresult(linkd10_1)
dim(linkd)
linkd<-matchleft(linkd,linkd10_1)
dim(linkd)
#248551     34
####################clean 11############

c2<- linkd[linkd$method=="link11d",]

linkd11_1<-linkd[linkd$method=="link11d" & substr(linkd$class,1,1)=="R" ,]
linkd11_1<-uniqueresult(linkd11_1)
dim(linkd)
linkd<-matchleft(linkd,linkd11_1)
dim(linkd)
#248490     34

c2<- linkd[linkd$method=="link11d",]



linkd11_parent<-c2[grepl("\\d",c2$subbuildingname),]

linkd11_parent1<-keepneed(linkd,linkd11_parent)
rm(linkd11_parent)

dim(linkd)
linkd<-matchleft(linkd,linkd11_parent1)
dim(linkd)

c2<- linkd[linkd$method=="link11d",]



linkd11_parent<-c2[grepl("FLAT",c2$saotext),]
linkd11_parent2<-keepneed(linkd,linkd11_parent)
rm(linkd11_parent)

dim(linkd)
# 
linkd<-matchleft(linkd,linkd11_parent2)
dim(linkd)
#246199     34
c2<- linkd[linkd$method=="link11d",]


linkd11_parent<-c2[grepl("APARTMENT",c2$saotext),]
linkd11_parent3<-keepneed(linkd,linkd11_parent)
rm(linkd11_parent)

dim(linkd)
# 
linkd<-matchleft(linkd,linkd11_parent3)
dim(linkd)

c2<- linkd[linkd$method=="link11d",]


linkd11_parent<-linkd[linkd$method=="link11d" & linkd$saostartnumber!="",]

linkd11_parent4<-keepneed(linkd,linkd11_parent)
rm(linkd11_parent)

dim(linkd)
linkd<-matchleft(linkd,linkd11_parent4)
dim(linkd)


c2<- linkd[linkd$method=="link11d",]



linkd<-matchleft(linkd,c2)
rm(c2)
dim(linkd)
#2245565     34
linkd11_parent<-rbindlist(list(linkd11_parent1,linkd11_parent2,linkd11_parent3,linkd11_parent4))
rm(linkd11_parent1,linkd11_parent2,linkd11_parent3,linkd11_parent4)




####################clean 12############
c2<- linkd[linkd$method=="link12d",]

linkd12_1<-linkd[linkd$method=="link12d" & substr(linkd$class,1,1)=="R" ,]
linkd12_1<-uniqueresult(linkd12_1)
dim(linkd)
linkd<-matchleft(linkd,linkd12_1)
dim(linkd)

c2<- linkd[linkd$method=="link12d",]



linkd12_parent<-c2[grepl("\\d",c2$subbuildingname),]

linkd12_parent1<-keepneed(linkd,linkd12_parent)
rm(linkd12_parent)

dim(linkd)
linkd<-matchleft(linkd,linkd12_parent1)
dim(linkd)
#243793     34


c2<- linkd[linkd$method=="link12d",]


linkd12_parent<-c2[grepl("UNIT",c2$saotext),]
linkd12_parent2<-keepneed(linkd,linkd12_parent)
rm(linkd12_parent)

dim(linkd)
# 
linkd<-matchleft(linkd,linkd12_parent2)
dim(linkd)
c2<- linkd[linkd$method=="link12d",]


linkd12_parent<-c2[grepl("FLOOR",c2$saotext),]
linkd12_parent3<-keepneed(linkd,linkd12_parent)
rm(linkd12_parent)

dim(linkd)
# 
linkd<-matchleft(linkd,linkd12_parent3)
dim(linkd)


c2<- linkd[linkd$method=="link12d",]


linkd<-matchleft(linkd,c2)
rm(c2)
dim(linkd)
#239716     34
linkd12_parent<-rbindlist(list(linkd12_parent1,linkd12_parent2,linkd12_parent3))
rm(linkd12_parent1,linkd12_parent2,linkd12_parent3)

####################clean 13 no ############
c2<- linkd[linkd$method=="link13d",]



####################clean 14 no############
c2<- linkd[linkd$method=="link14d",]


####################clean 15############
c2<- linkd[linkd$method=="link15d",]



linkd15_1<-linkd[linkd$method=="link15d" & substr(linkd$class,1,1)=="R" ,]
linkd15_1<-uniqueresult(linkd15_1)
dim(linkd)
linkd<-matchleft(linkd,linkd15_1)
dim(linkd)
#239706     34
c2<- linkd[linkd$method=="link15d",]


linkd15_parent<-c2[grepl("\\d",c2$subbuildingname),]

linkd15_parent1<-keepneed(linkd,linkd15_parent)
rm(linkd15_parent)

dim(linkd)
linkd<-matchleft(linkd,linkd15_parent1)
dim(linkd)
#239076     34
c2<- linkd[linkd$method=="link15d",]



linkd15_parent<-c2[grepl("FLAT",c2$subbuildingname),]
linkd15_parent2<-keepneed(linkd,linkd15_parent)
rm(linkd15_parent)

dim(linkd)
# 
linkd<-matchleft(linkd,linkd15_parent2)
dim(linkd)
#238994     34
c2<- linkd[linkd$method=="link15d",]



linkd15_parent<-c2[grepl("APARTMENT",c2$subbuildingname),]
linkd15_parent3<-keepneed(linkd,linkd15_parent)
rm(linkd15_parent)

dim(linkd)
# 
linkd<-matchleft(linkd,linkd15_parent3)
dim(linkd)
#238994     34
c2<- linkd[linkd$method=="link15d",]

# 238986     34

linkd15_parent<-c2[grepl("LOWER",c2$subbuildingname),]
linkd15_parent4<-keepneed(linkd,linkd15_parent)
rm(linkd15_parent)

dim(linkd)
# 
linkd<-matchleft(linkd,linkd15_parent4)
dim(linkd)

c2<- linkd[linkd$method=="link15d",]


linkd<-matchleft(linkd,c2)
rm(c2)
dim(linkd)
# 238962     34
linkd15_parent<-rbindlist(list(linkd15_parent1,linkd15_parent2,linkd15_parent3,linkd15_parent4))
rm(linkd15_parent1,linkd15_parent2,linkd15_parent3,linkd15_parent4)

####################clean 16############
c2<- linkd[linkd$method=="link16d",]


linkd<-matchleft(linkd,c2)
rm(c2)
dim(linkd)

# 238938     34



####################clean 17############
c2<- linkd[linkd$method=="link17d",]


####################clean 18############
c2<- linkd[linkd$method=="link18d",]


####################clean 19############
c2<- linkd[linkd$method=="link19d",]



linkd19_parent<-c2[grepl("\\d",c2$saotext),]

linkd19_parent1<-keepneed(linkd,linkd19_parent)
rm(linkd19_parent)

dim(linkd)
linkd<-matchleft(linkd,linkd19_parent1)
dim(linkd)

#231927     34
c2<- linkd[linkd$method=="link19d",]



linkd19_parent<-linkd[linkd$method=="link19d" & linkd$saostartnumber!="",]

linkd19_parent2<-keepneed(linkd,linkd19_parent)
rm(linkd19_parent)

dim(linkd)
linkd<-matchleft(linkd,linkd19_parent2)
dim(linkd)
#230290     34
c2<- linkd[linkd$method=="link19d",]


linkd19_parent<-c2[grepl("FLAT",c2$saotext),]
linkd19_parent3<-keepneed(linkd,linkd19_parent)
rm(linkd19_parent)

dim(linkd)
# 
linkd<-matchleft(linkd,linkd19_parent3)
dim(linkd)
#229830 
c2<- linkd[linkd$method=="link19d",]



linkd19_parent<-c2[grepl("UNIT",c2$saotext),]
linkd19_parent4<-keepneed(linkd,linkd19_parent)
rm(linkd19_parent)

dim(linkd)
# 
linkd<-matchleft(linkd,linkd19_parent4)
dim(linkd)
# 229789     34
c2<- linkd[linkd$method=="link19d",]



linkd19_parent<-c2[grepl("FLOOR",c2$saotext),]
linkd19_parent5<-keepneed(linkd,linkd19_parent)
rm(linkd19_parent)

dim(linkd)
# 
linkd<-matchleft(linkd,linkd19_parent5)
dim(linkd)
#229726     34
c2<- linkd[linkd$method=="link19d",]



linkd<-matchleft(linkd,c2)
rm(c2)
dim(linkd)
#2245565     34
linkd19_parent<-rbindlist(list(linkd19_parent1,linkd19_parent2,linkd19_parent3,linkd19_parent4,linkd19_parent5))
rm(linkd19_parent1,linkd19_parent2,linkd19_parent3,linkd19_parent4,linkd19_parent5)


####################clean 20############
c2<- linkd[linkd$method=="link20d",]


linkd20_1<-linkd[linkd$method=="link20d" & substr(linkd$class,1,1)=="R" ,]
linkd20_1<-uniqueresult(linkd20_1)
dim(linkd)
linkd<-matchleft(linkd,linkd20_1)
dim(linkd)
#228506     34

c2<- linkd[linkd$method=="link20d",]



linkd20_parent<-c2[grepl("\\d",c2$saotext),]

linkd20_parent1<-keepneed(linkd,linkd20_parent)
rm(linkd20_parent)

dim(linkd)
linkd<-matchleft(linkd,linkd20_parent1)
dim(linkd)

# 228294     34

c2<- linkd[linkd$method=="link20d",]


linkd20_parent<-linkd[linkd$method=="link20d" & linkd$saostartnumber!="",]

linkd20_parent2<-keepneed(linkd,linkd20_parent)
rm(linkd20_parent)

dim(linkd)
linkd<-matchleft(linkd,linkd20_parent2)
dim(linkd)
#230290     34
c2<- linkd[linkd$method=="link20d",]


linkd20_parent<-c2[grepl("FLAT",c2$saotext),]
linkd20_parent3<-keepneed(linkd,linkd20_parent)
rm(linkd20_parent)

dim(linkd)
# 
linkd<-matchleft(linkd,linkd20_parent3)
dim(linkd)
#228209     34 
c2<- linkd[linkd$method=="link20d",]


linkd<-matchleft(linkd,c2)
rm(c2)
dim(linkd)
#227957     34
linkd20_parent<-rbindlist(list(linkd20_parent1,linkd20_parent2,linkd20_parent3))
rm(linkd20_parent1,linkd20_parent2,linkd20_parent3)

####################clean 21############
c2<- linkd[linkd$method=="link21d",]
detail_47<-c2
linkd<-matchleft(linkd,detail_47)
dim(linkd)
####################clean 22############
c2<- linkd[linkd$method=="link22d",]



####################clean 23############

c2<- linkd[linkd$method=="link23d",]

linkd23_parent<-linkd[linkd$method=="link23d",]
linkd<-matchleft(linkd,c2)
rm(c2)

####################clean 24############
c2<- linkd[linkd$method=="link24d",]




####################clean 25############
c2<- linkd[linkd$method=="link25d",]

linkd25_1<-linkd[linkd$method=="link25d" & substr(linkd$class,1,1)=="R" ,]
linkd25_1<-uniqueresult(linkd25_1)
dim(linkd)
linkd<-matchleft(linkd,linkd25_1)
dim(linkd)
####################clean 26############
c2<- linkd[linkd$method=="link26d",]



####################clean 27############
c2<- linkd[linkd$method=="link27d",]



####################clean 28############
c2<- linkd[linkd$method=="link28d",]



####################clean 29############
c2<- linkd[linkd$method=="link29d",]

####################clean 30############
c2<- linkd[linkd$method=="link30d",]

linkd30_1<-linkd[linkd$method=="link30d" & substr(linkd$class,1,1)=="R" ,]
linkd30_1<-uniqueresult(linkd30_1)
dim(linkd)
linkd<-matchleft(linkd,linkd30_1)
dim(linkd)
# 224923     34
c2<- linkd[linkd$method=="link30d",]




linkd30_parent<-c2[grepl("\\d",c2$subbuildingname),]

linkd30_parent1<-keepneed(linkd,linkd30_parent)
rm(linkd30_parent)

dim(linkd)
linkd<-matchleft(linkd,linkd30_parent1)
dim(linkd)
#224734     34
c2<- linkd[linkd$method=="link30d",]


linkd30_parent<-c2[grepl("FLOOR",c2$subbuildingname),]

linkd30_parent2<-keepneed(linkd,linkd30_parent)
rm(linkd30_parent)

dim(linkd)
linkd<-matchleft(linkd,linkd30_parent2)
dim(linkd)
#224731
c2<- linkd[linkd$method=="link30d",]


linkd30_parent<-c2[grepl("UPPER",c2$saotext),]
linkd30_parent3<-keepneed(linkd,linkd30_parent)
rm(linkd30_parent)

dim(linkd)
# 
linkd<-matchleft(linkd,linkd30_parent3)
dim(linkd)
#24721     34

c2<- linkd[linkd$method=="link30d",]



linkd<-matchleft(linkd,c2)
rm(c2)
dim(linkd)
#2245565     34
linkd30_parent<-rbindlist(list(linkd30_parent1,linkd30_parent2,linkd30_parent3))
rm(linkd30_parent1,linkd30_parent2,linkd30_parent3)

####################clean 31############
c2<- linkd[linkd$method=="link31d",]


linkd31_parent<-linkd[linkd$method=="link31d",]

linkd<-matchleft(linkd,c2)
rm(c2)
dim(linkd)
#224233     34
####################clean 32############
c2<- linkd[linkd$method=="link32d",]


linkd32_1<-linkd[linkd$method=="link32d" & substr(linkd$class,1,1)=="R" ,]
linkd32_1<-uniqueresult(linkd32_1)
dim(linkd)
linkd<-matchleft(linkd,linkd32_1)
dim(linkd)
#221886     34

c2<- linkd[linkd$method=="link32d",]

linkd32_parent<-linkd[linkd$method=="link32d",]

linkd<-matchleft(linkd,c2)
rm(c2)
dim(linkd)
# 203994     34


####################clean 33############
c2<- linkd[linkd$method=="link33d",]

####################clean 34############
c2<- linkd[linkd$method=="link34d",]



linkd34_1<-linkd[linkd$method=="link34d" & substr(linkd$class,1,1)=="R" ,]
linkd34_1<-uniqueresult(linkd34_1)
dim(linkd)
linkd<-matchleft(linkd,linkd34_1)
dim(linkd)
#203980     34

c2<- linkd[linkd$method=="link34d",]



c2$ss<-paste(c2$saostartnumber,c2$saostartsuffix,sep="")
c2$ss<-gsub(" ", "", c2$ss)
linkd34_2<-c2[c2$buildingnumber== c2$ss,]
linkd34_2<-uniqueresult(linkd34_2)
dim(linkd)
linkd<-matchleft(linkd,linkd34_2)
dim(linkd)
#203978     34
c2<- linkd[linkd$method=="link34d",]

linkd34_parent<-c2


linkd<-matchleft(linkd,c2)
rm(c2)
dim(linkd)
# 203912     34
####################clean 35############

c2<- linkd[linkd$method=="link35d",]

linkd35_1<-linkd[linkd$method=="link35d" & substr(linkd$class,1,1)=="R" ,]
linkd35_1<-uniqueresult(linkd35_1)
dim(linkd)
linkd<-matchleft(linkd,linkd35_1)
dim(linkd)
#203902     34

c2<- linkd[linkd$method=="link35d",]

linkd35_parent<-c2[grepl("\\d",c2$saotext),]

linkd35_parent1<-keepneed(linkd,linkd35_parent)
rm(linkd35_parent)

dim(linkd)
linkd<-matchleft(linkd,linkd35_parent1)
dim(linkd)

c2<- linkd[linkd$method=="link35d",]


linkd35_parent<-c2[grepl("FLAT",c2$saotext),]
linkd35_parent2<-keepneed(linkd,linkd35_parent)
rm(linkd35_parent)

dim(linkd)
# 
linkd<-matchleft(linkd,linkd35_parent2)
dim(linkd)
#203571     34
c2<- linkd[linkd$method=="link35d",]




linkd<-matchleft(linkd,c2)
rm(c2)
dim(linkd)
#2245565     35
linkd35_parent<-rbindlist(list(linkd35_parent1,linkd35_parent2))
rm(linkd35_parent1,linkd35_parent2)



####################clean 36############
c2<- linkd[linkd$method=="link36d",]




####################clean 37############
c2<- linkd[linkd$method=="link37d",]

####################clean 38############


c2<- linkd[linkd$method=="link38d",]


linkd38_1<-linkd[linkd$method=="link38d" & substr(linkd$class,1,1)=="R" ,]
linkd38_1<-uniqueresult(linkd38_1)
dim(linkd)
linkd<-matchleft(linkd,linkd38_1)
dim(linkd)
# 203557     34

c2<- linkd[linkd$method=="link38d",]



linkd38_parent<-c2[grepl("\\d",c2$saotext),]

linkd38_parent1<-keepneed(linkd,linkd38_parent)
rm(linkd38_parent)

dim(linkd)
linkd<-matchleft(linkd,linkd38_parent1)
dim(linkd)

c2<- linkd[linkd$method=="link38d",]




linkd38_parent<-c2[grepl("FLAT",c2$saotext),]
linkd38_parent2<-keepneed(linkd,linkd38_parent)
rm(linkd38_parent)


linkd<-matchleft(linkd,linkd38_parent2)
dim(linkd)
# 
#203481     34
c2<- linkd[linkd$method=="link38d",]



linkd<-matchleft(linkd,c2)
rm(c2)
dim(linkd)
#203465     34
linkd38_parent<-rbindlist(list(linkd38_parent1,linkd38_parent2))
rm(linkd38_parent1,linkd38_parent2)

####################clean 39############
c2<- linkd[linkd$method=="link39d",]



linkd39_parent<-c2[grepl("\\d",c2$saostartnumber),]

linkd39_parent1<-keepneed(linkd,linkd39_parent)
rm(linkd39_parent)

dim(linkd)
linkd<-matchleft(linkd,linkd39_parent1)
dim(linkd)
#203437     34

c2<- linkd[linkd$method=="link39d",]

linkd39_parent<-linkd39_parent1
rm(linkd39_parent1)


####################clean 40############
c2<- linkd[linkd$method=="link40d",]


####################clean 41############
c2<- linkd[linkd$method=="link41d",]

####################clean 42############
c2<- linkd[linkd$method=="link42d",]




linkd42_parent<-c2[grepl("\\d",c2$saotext),]

linkd42_parent1<-keepneed(linkd,linkd42_parent)
rm(linkd42_parent)

dim(linkd)
linkd<-matchleft(linkd,linkd42_parent1)
dim(linkd)

c2<- linkd[linkd$method=="link42d",]


linkd42_parent<-c2[grepl("\\d",c2$buildingname),]

linkd42_parent2<-keepneed(linkd,linkd42_parent)
rm(linkd42_parent)

dim(linkd)
linkd<-matchleft(linkd,linkd42_parent2)
dim(linkd)

c2<- linkd[linkd$method=="link42d",]


linkd42_parent<-c2[grepl("\\d",c2$saostartnumber),]

linkd42_parent3<-keepneed(linkd,linkd42_parent)
rm(linkd42_parent)

dim(linkd)
linkd<-matchleft(linkd,linkd42_parent3)
dim(linkd)

c2<- linkd[linkd$method=="link42d",]

linkd<-matchleft(linkd,c2)
rm(c2)
dim(linkd)
#202420     34
linkd42_parent<-rbindlist(list(linkd42_parent1,linkd42_parent2,linkd42_parent3))
rm(linkd42_parent1,linkd42_parent2,linkd42_parent3)

####################clean 43############
c2<- linkd[linkd$method=="link43d",]


linkd<-matchleft(linkd,c2)
rm(c2)
dim(linkd)
# 202414     34
####################clean 44############

c2<- linkd[linkd$method=="link44d",]


linkd44_1<-linkd[linkd$method=="link44d" & substr(linkd$class,1,1)=="R" ,]
linkd44_1<-uniqueresult(linkd44_1)
dim(linkd)
linkd<-matchleft(linkd,linkd44_1)
dim(linkd)
c2<- linkd[linkd$method=="link44d",]




linkd44_parent<-c2[grepl("\\d",c2$saostartnumber),]

linkd44_parent1<-keepneed(linkd,linkd44_parent)
rm(linkd44_parent)

dim(linkd)
linkd<-matchleft(linkd,linkd44_parent1)
dim(linkd)
#202186     34
c2<- linkd[linkd$method=="link44d",]




linkd44_parent<-c2[grepl("FLAT",c2$saotext),]
linkd44_parent2<-keepneed(linkd,linkd44_parent)
rm(linkd44_parent)

dim(linkd)
# 
linkd<-matchleft(linkd,linkd44_parent2)
dim(linkd)
#202182     34
c2<- linkd[linkd$method=="link44d",]



linkd<-matchleft(linkd,c2)
rm(c2)
dim(linkd)
#2245565     44
linkd44_parent<-rbindlist(list(linkd44_parent1,linkd44_parent2))
rm(linkd44_parent1,linkd44_parent2)


####################clean 45############

c2<- linkd[linkd$method=="link45d",]



linkd45_1<-linkd[linkd$method=="link45d" & substr(linkd$class,1,1)=="R" ,]
linkd45_1<-uniqueresult(linkd45_1)
dim(linkd)
linkd<-matchleft(linkd,linkd45_1)
dim(linkd)
#202171     34

c2<- linkd[linkd$method=="link45d",]


linkd45_parent<-c2[grepl("\\d",c2$saotext),]

linkd45_parent1<-keepneed(linkd,linkd45_parent)
rm(linkd45_parent)

dim(linkd)
linkd<-matchleft(linkd,linkd45_parent1)
dim(linkd)
#202165     34

c2<- linkd[linkd$method=="link45d",]



linkd45_parent<-c2[grepl("FLAT",c2$saotext),]
linkd45_parent2<-keepneed(linkd,linkd45_parent)
rm(linkd45_parent)

dim(linkd)
# 
linkd<-matchleft(linkd,linkd45_parent2)
dim(linkd)

c2<- linkd[linkd$method=="link45d",]


linkd45_parent<-c2[c2$saostartnumber!="",]
linkd45_parent3<-keepneed(linkd,linkd45_parent)
rm(linkd45_parent)

dim(linkd)
# 
linkd<-matchleft(linkd,linkd45_parent3)
dim(linkd)
c2<- linkd[linkd$method=="link45d",]




linkd<-matchleft(linkd,c2)
rm(c2)
dim(linkd)
#202104     34
linkd45_parent<-rbindlist(list(linkd45_parent1,linkd45_parent2,linkd45_parent3))
rm(linkd45_parent1,linkd45_parent2,linkd45_parent3)
####################clean 46 interesting case############

c2<- linkd[linkd$method=="link46d",]


linkd46_1<-linkd[linkd$method=="link46d" & substr(linkd$class,1,1)=="R" ,]
linkd46_1<-uniqueresult(linkd46_1)
dim(linkd)
linkd<-matchleft(linkd,linkd46_1)
dim(linkd)
#202100     34

c2<- linkd[linkd$method=="link46d",]




linkd46_parent<-c2[grepl("\\d",c2$saotext),]

linkd46_parent1<-keepneed(linkd,linkd46_parent)
rm(linkd46_parent)

dim(linkd)
linkd<-matchleft(linkd,linkd46_parent1)
dim(linkd)

c2<- linkd[linkd$method=="link46d",]


linkd<-matchleft(linkd,c2)
rm(c2)
dim(linkd)
#201811     34
linkd46_parent<-linkd46_parent1
rm(linkd46_parent1)


####################clean 47############

c2<- linkd[linkd$method=="link47d",]


linkd47_parent<-c2

linkd<-matchleft(linkd,c2)
rm(c2)
dim(linkd)
# 201760     34
####################clean 48############

c2<- linkd[linkd$method=="link48d",]
####################clean 49############

c2<- linkd[linkd$method=="link49d",]

linkd<-matchleft(linkd,c2)
rm(c2)


####################clean 50############

c2<- linkd[linkd$method=="link50d",]




####################clean 51############

c2<- linkd[linkd$method=="link51d",]

####################clean 52############
c2<- linkd[linkd$method=="link52d",]

####################clean 53############
c2<- linkd[linkd$method=="link53d",]

linkd53_parent<-c2
linkd<-matchleft(linkd,c2)
rm(c2)
####################clean 54 unable to match one of the themn############
c2<- linkd[linkd$method=="link54d",]

linkd<-matchleft(linkd,c2)
rm(c2)
####################clean 55############
c2<- linkd[linkd$method=="link55d",]



####################clean 56############
c2<- linkd[linkd$method=="link56d",]

####################clean 57############
c2<- linkd[linkd$method=="link57d",]

####################clean 58############
c2<- linkd[linkd$method=="link58d",]


linkd58_1<-c2[!grepl("\\d+",c2$buildingnumber),]

linkd58_1<-uniqueresult(linkd58_1)


linkd<-matchleft(linkd,linkd58_1)
dim(linkd)
c2<- linkd[linkd$method=="link58d",]


linkd58_parent<-c2
linkd<-matchleft(linkd,c2)
rm(c2)
dim(linkd)
####################clean 59############
c2<- linkd[linkd$method=="link59d",]


####################clean 60############
c2<- linkd[linkd$method=="link60d",]



linkd60_parent<-c2[grepl("\\d",c2$subbuildingname),]

linkd60_parent1<-keepneed(linkd,linkd60_parent)
rm(linkd60_parent)

dim(linkd)
linkd<-matchleft(linkd,linkd60_parent1)
dim(linkd)

c2<- linkd[linkd$method=="link60d",]



linkd<-matchleft(linkd,c2)
rm(c2)
dim(linkd)
linkd60_parent<-linkd60_parent1
rm(linkd60_parent1)
####################clean 61############
c2<- linkd[linkd$method=="link61d",]




####################clean 62############
c2<- linkd[linkd$method=="link62d",]


####################clean 63############
c2<- linkd[linkd$method=="link63d",]

linkd63_parent<-c2
linkd<-matchleft(linkd,c2)
rm(c2)
dim(linkd)

####################clean 64############
c2<- linkd[linkd$method=="link64d",]

####################clean 65############
c2<- linkd[linkd$method=="link65d",]


linkd65_parent<-c2[c2$buildingname!="",]

linkd65_parent1<-keepneed(linkd,linkd65_parent)
rm(linkd65_parent)

dim(linkd)
linkd<-matchleft(linkd,linkd65_parent1)
dim(linkd)
c2<- linkd[linkd$method=="link65d",]

linkd<-matchleft(linkd,c2)
rm(c2)
dim(linkd)
#201649     34
linkd65_parent<-linkd65_parent1
rm(linkd65_parent1)
####################clean 66############
c2<- linkd[linkd$method=="link66d",]

linkd66_parent<-c2
linkd<-matchleft(linkd,c2)
rm(c2)
dim(linkd)
####################clean 67############
c2<- linkd[linkd$method=="link67d",]




linkd67_parent<-c2[grepl("\\d",c2$subbuildingname),]

linkd67_parent1<-keepneed(linkd,linkd67_parent)
rm(linkd67_parent)

dim(linkd)
linkd<-matchleft(linkd,linkd67_parent1)
dim(linkd)
#201623     34
c2<- linkd[linkd$method=="link67d",]


linkd<-matchleft(linkd,c2)
rm(c2)
dim(linkd)
linkd67_parent<-linkd67_parent1
rm(linkd67_parent1)



####################clean 68############
c2<- linkd[linkd$method=="link68d",]

####################clean 69############
c2<- linkd[linkd$method=="link69d",]




linkd69_parent<-c2[grepl("FLOOR",c2$saotext),]
linkd69_parent1<-keepneed(linkd,linkd69_parent)
rm(linkd69_parent)

dim(linkd)
# 
linkd<-matchleft(linkd,linkd69_parent1)
dim(linkd)

c2<- linkd[linkd$method=="link69d",]




linkd69_parent<-c2[c2$paostartsuffix!="",]
linkd69_parent2<-keepneed(linkd,linkd69_parent)
rm(linkd69_parent)

dim(linkd)
# 
linkd<-matchleft(linkd,linkd69_parent2)
dim(linkd)
#267679     67
c2<- linkd[linkd$method=="link69d",]


linkd<-matchleft(linkd,c2)
rm(c2)
dim(linkd)
#2269569     69
linkd69_parent<-rbindlist(list(linkd69_parent1,linkd69_parent2))
rm(linkd69_parent1,linkd69_parent2)


####################clean 70############
c2<- linkd[linkd$method=="link70d",]

####################clean 71############
c2<- linkd[linkd$method=="link71d",]

####################clean 72############
c2<- linkd[linkd$method=="link72d",]

linkd<-matchleft(linkd,c2)
rm(c2)
dim(linkd)
####################clean 73############
c2<- linkd[linkd$method=="link73d",]


linkd73_1<-linkd[linkd$method=="link73d" & substr(linkd$class,1,1)=="R" ,]
linkd73_1<-uniqueresult(linkd73_1)
dim(linkd)
linkd<-matchleft(linkd,linkd73_1)
dim(linkd)
c2<- linkd[linkd$method=="link73d",]


linkd73_parent<-c2

linkd<-matchleft(linkd,c2)
rm(c2)
dim(linkd)
####################clean 74############
c2<- linkd[linkd$method=="link74d",]


linkd74_1<-linkd[linkd$method=="link74d" & substr(linkd$class,1,1)=="R" ,]
linkd74_1<-uniqueresult(linkd74_1)
dim(linkd)
linkd<-matchleft(linkd,linkd74_1)
dim(linkd)
#201320     34


c2<- linkd[linkd$method=="link74d",]



linkd74_parent<-c2[grepl("\\d",c2$saotext),]

linkd74_parent1<-keepneed(linkd,linkd74_parent)
rm(linkd74_parent)

dim(linkd)
linkd<-matchleft(linkd,linkd74_parent1)
dim(linkd)

c2<- linkd[linkd$method=="link74d",]




linkd74_parent<-c2[grepl("FLAT",c2$saotext),]
linkd74_parent2<-keepneed(linkd,linkd74_parent)
rm(linkd74_parent)

dim(linkd)
# 
linkd<-matchleft(linkd,linkd74_parent2)
dim(linkd)
#200832     34
c2<- linkd[linkd$method=="link74d",]



linkd<-matchleft(linkd,c2)
rm(c2)
dim(linkd)
#200677     34
linkd74_parent<-rbindlist(list(linkd74_parent1,linkd74_parent2))
rm(linkd74_parent1,linkd74_parent2)

####################clean 75############
c2<- linkd[linkd$method=="link75d",]




linkd75_1<-linkd[linkd$method=="link75d" & substr(linkd$class,1,1)=="R" ,]
linkd75_1<-uniqueresult(linkd75_1)
dim(linkd)
linkd<-matchleft(linkd,linkd75_1)
dim(linkd)


c2<- linkd[linkd$method=="link75d",]



linkd75_parent<-c2[grepl("\\d",c2$subbuildingname),]

linkd75_parent1<-keepneed(linkd,linkd75_parent)
rm(linkd75_parent)

dim(linkd)
linkd<-matchleft(linkd,linkd75_parent1)
dim(linkd)
#200200     34
c2<- linkd[linkd$method=="link75d",]



linkd75_parent<-c2[grepl("FLAT",c2$subbuildingname),]
linkd75_parent2<-keepneed(linkd,linkd75_parent)
rm(linkd75_parent)

dim(linkd)
# 
linkd<-matchleft(linkd,linkd75_parent2)
dim(linkd)
#275759     75
c2<- linkd[linkd$method=="link75d",]


linkd75_parent<-c2[grepl("\\d",c2$buildingname),]
linkd75_parent3<-keepneed(linkd,linkd75_parent)
rm(linkd75_parent)

dim(linkd)
# 
linkd<-matchleft(linkd,linkd75_parent3)
dim(linkd)
#200097     34

c2<- linkd[linkd$method=="link75d",]


linkd<-matchleft(linkd,c2)
rm(c2)
dim(linkd)
#2275575     75
linkd75_parent<-rbindlist(list(linkd75_parent1,linkd75_parent2,linkd75_parent3))
rm(linkd75_parent1,linkd75_parent2,linkd75_parent3)



####################clean 76############
c2<- linkd[linkd$method=="link76d",]




linkd76_parent<-c2[grepl("\\d",c2$subbuildingname),]

linkd76_parent1<-keepneed(linkd,linkd76_parent)
rm(linkd76_parent)

dim(linkd)
linkd<-matchleft(linkd,linkd76_parent1)
dim(linkd)

c2<- linkd[linkd$method=="link76d",]



linkd76_parent<-c2[grepl("FLAT",c2$subbuildingname),]
linkd76_parent2<-keepneed(linkd,linkd76_parent)
rm(linkd76_parent)

dim(linkd)
# 
linkd<-matchleft(linkd,linkd76_parent2)
dim(linkd)
#276769     76
c2<- linkd[linkd$method=="link76d",]

linkd<-matchleft(linkd,c2)
rm(c2)
dim(linkd)
#2276576     76
linkd76_parent<-rbindlist(list(linkd76_parent1,linkd76_parent2))
rm(linkd76_parent1,linkd76_parent2)


####################clean 77############
c2<- linkd[linkd$method=="link77d",]






####################clean 78############
c2<- linkd[linkd$method=="link78d",]


linkd78_1<-linkd[linkd$method=="link78d" & substr(linkd$class,1,1)=="R" ,]
linkd78_1<-uniqueresult(linkd78_1)
dim(linkd)

linkd<-matchleft(linkd,linkd78_1)
dim(linkd)
#199750     34

c2<- linkd[linkd$method=="link78d",]



linkd78_parent<-c2[grepl("\\d",c2$subbuildingname),]

linkd78_parent1<-keepneed(linkd,linkd78_parent)
rm(linkd78_parent)

dim(linkd)
linkd<-matchleft(linkd,linkd78_parent1)
dim(linkd)



c2<- linkd[linkd$method=="link78d",]



linkd78_parent<-c2[grepl("FLAT",c2$subbuildingname),]
linkd78_parent2<-keepneed(linkd,linkd78_parent)
rm(linkd78_parent)

dim(linkd)
# 
linkd<-matchleft(linkd,linkd78_parent2)
dim(linkd)

c2<- linkd[linkd$method=="link78d",]



linkd78_parent<-c2[grepl("\\d",c2$paostartnumber) ,]
linkd78_parent<-linkd78_parent[linkd78_parent$buildingnumber=="",]
linkd78_parent3<-keepneed(linkd,linkd78_parent)
rm(linkd78_parent)

dim(linkd)
# 
linkd<-matchleft(linkd,linkd78_parent3)
dim(linkd)

c2<- linkd[linkd$method=="link78d",]


linkd<-matchleft(linkd,c2)
rm(c2)
dim(linkd)
#2278578     78
linkd78_parent<-rbindlist(list(linkd78_parent1,linkd78_parent2,linkd78_parent3))
rm(linkd78_parent1,linkd78_parent2,linkd78_parent3)


####################clean 79############
c2<- linkd[linkd$method=="link79d",]

linkd79_1<-linkd[linkd$method=="link79d" & substr(linkd$class,1,1)=="R" ,]
linkd79_1<-uniqueresult(linkd79_1)
dim(linkd)
linkd<-matchleft(linkd,linkd79_1)
dim(linkd)

c2<- linkd[linkd$method=="link79d",]




linkd79_parent<-c2[grepl("\\d",c2$saotext),]

linkd79_parent1<-keepneed(linkd,linkd79_parent)
rm(linkd79_parent)

dim(linkd)
linkd<-matchleft(linkd,linkd79_parent1)
dim(linkd)

c2<- linkd[linkd$method=="link79d",]



linkd79_parent<-c2[grepl("FLAT",c2$saotext),]
linkd79_parent2<-keepneed(linkd,linkd79_parent)
rm(linkd79_parent)

dim(linkd)
# 
linkd<-matchleft(linkd,linkd79_parent2)
dim(linkd)
#279799     79
c2<- linkd[linkd$method=="link79d",]




linkd79_parent<-c2[grepl("ROOM",c2$saotext),]
linkd79_parent3<-keepneed(linkd,linkd79_parent)
rm(linkd79_parent)

dim(linkd)
# 
linkd<-matchleft(linkd,linkd79_parent3)
dim(linkd)

c2<- linkd[linkd$method=="link79d",]


linkd79_parent<-c2[grepl("FLOOR",c2$saotext),]

linkd79_parent4<-keepneed(linkd,linkd79_parent)
rm(linkd79_parent)

dim(linkd)
linkd<-matchleft(linkd,linkd79_parent4)
dim(linkd)


c2<- linkd[linkd$method=="link79d",]





linkd79_parent<-c2[grepl("ROOM",c2$paotext),]
linkd79_parent5<-keepneed(linkd,linkd79_parent)
rm(linkd79_parent)

dim(linkd)
# 
linkd<-matchleft(linkd,linkd79_parent5)
dim(linkd)


c2<- linkd[linkd$method=="link79d",]





linkd79_parent<-c2[grepl("\\d",c2$paotext),]
linkd79_parent6<-keepneed(linkd,linkd79_parent)
rm(linkd79_parent)

dim(linkd)
# 
linkd<-matchleft(linkd,linkd79_parent6)
dim(linkd)

c2<- linkd[linkd$method=="link79d",]



linkd79_parent<-c2[grepl("MAISONETTE",c2$saotext),]

linkd79_parent7<-keepneed(linkd,linkd79_parent)
rm(linkd79_parent)

dim(linkd)
linkd<-matchleft(linkd,linkd79_parent7)
dim(linkd)


c2<- linkd[linkd$method=="link79d",]



linkd79_parent<-c2[grepl("\\d",c2$saostartnumber),]
linkd79_parent8<-keepneed(linkd,linkd79_parent)
rm(linkd79_parent)

dim(linkd)
# 
linkd<-matchleft(linkd,linkd79_parent8)
dim(linkd)

c2<- linkd[linkd$method=="link79d",]




linkd79_parent<-c2[grepl("FLAT",c2$paotext),]
linkd79_parent9<-keepneed(linkd,linkd79_parent)
rm(linkd79_parent)

dim(linkd)
# 
linkd<-matchleft(linkd,linkd79_parent9)
dim(linkd)
#279799     79
c2<- linkd[linkd$method=="link79d",]





linkd79_parent<-c2[grepl("FLOOR",c2$paotext),]
linkd79_parent10<-keepneed(linkd,linkd79_parent)
rm(linkd79_parent)

dim(linkd)
# 
linkd<-matchleft(linkd,linkd79_parent10)
dim(linkd)
#193044     34
c2<- linkd[linkd$method=="link79d",]

linkd79_parent<-c2[grepl("APARTMENT",c2$saotext),]
linkd79_parent11<-keepneed(linkd,linkd79_parent)
rm(linkd79_parent)

dim(linkd)
# 
linkd<-matchleft(linkd,linkd79_parent11)
dim(linkd)


c2<- linkd[linkd$method=="link79d",]

linkd<-matchleft(linkd,c2)
rm(c2)
dim(linkd)
#2279579     79
linkd79_parent<-rbindlist(list(linkd79_parent1,linkd79_parent2,linkd79_parent3,linkd79_parent4,linkd79_parent5,linkd79_parent6,linkd79_parent7,linkd79_parent8,linkd79_parent9,linkd79_parent10,linkd79_parent11))
rm(linkd79_parent1,linkd79_parent2,linkd79_parent3,linkd79_parent4,linkd79_parent5,linkd79_parent6,linkd79_parent7,linkd79_parent8,linkd79_parent9,linkd79_parent10,linkd79_parent11)

####################clean 80############
c2<- linkd[linkd$method=="link80d",]

####################clean 81############
c2<- linkd[linkd$method=="link81d",]





linkd81_parent<-c2[grepl("\\d",c2$subbuildingname),]

linkd81_parent1<-keepneed(linkd,linkd81_parent)
rm(linkd81_parent)

dim(linkd)
linkd<-matchleft(linkd,linkd81_parent1)
dim(linkd)

c2<- linkd[linkd$method=="link81d",]




linkd81_parent<-c2[grepl("FLAT",c2$subbuildingname),]
linkd81_parent2<-keepneed(linkd,linkd81_parent)
rm(linkd81_parent)

dim(linkd)
# 
linkd<-matchleft(linkd,linkd81_parent2)
dim(linkd)
#281819     81
c2<- linkd[linkd$method=="link81d",]



linkd81_parent<-rbindlist(list(linkd81_parent1,linkd81_parent2))
rm(linkd81_parent1,linkd81_parent2)

####################clean 82############
c2<- linkd[linkd$method=="link82d",]



linkd82_1<-linkd[linkd$method=="link82d" & substr(linkd$class,1,1)=="R" ,]
linkd82_1<-uniqueresult(linkd82_1)
dim(linkd)
linkd<-matchleft(linkd,linkd82_1)
dim(linkd)

c2<- linkd[linkd$method=="link82d",]




linkd82_parent<-c2[grepl("\\d",c2$saotext),]

linkd82_parent1<-keepneed(linkd,linkd82_parent)
rm(linkd82_parent)

dim(linkd)
linkd<-matchleft(linkd,linkd82_parent1)
dim(linkd)

c2<- linkd[linkd$method=="link82d",]


linkd82_parent<-c2[grepl("FLAT",c2$saotext),]
linkd82_parent2<-keepneed(linkd,linkd82_parent)
rm(linkd82_parent)

dim(linkd)
# 
linkd<-matchleft(linkd,linkd82_parent2)
dim(linkd)
#192562     34
c2<- linkd[linkd$method=="link82d",]




linkd82_parent<-c2[grepl("\\d",c2$buildingname),]
linkd82_parent3<-keepneed(linkd,linkd82_parent)
rm(linkd82_parent)

dim(linkd)
# 
linkd<-matchleft(linkd,linkd82_parent3)
dim(linkd)

c2<- linkd[linkd$method=="link82d",]



linkd82_parent<-c2[grepl("\\d",c2$buildingnumber),]

linkd82_parent4<-keepneed(linkd,linkd82_parent)
rm(linkd82_parent)

dim(linkd)
linkd<-matchleft(linkd,linkd82_parent4)
dim(linkd)


c2<- linkd[linkd$method=="link82d",]




linkd82_parent<-c2[grepl("UNIT",c2$saotext),]
linkd82_parent5<-keepneed(linkd,linkd82_parent)
rm(linkd82_parent)

dim(linkd)
# 
linkd<-matchleft(linkd,linkd82_parent5)
dim(linkd)


c2<- linkd[linkd$method=="link82d",]


linkd<-matchleft(linkd,c2)
rm(c2)
dim(linkd)
# 192473     34
linkd82_parent<-rbindlist(list(linkd82_parent1,linkd82_parent2,linkd82_parent3,linkd82_parent4,linkd82_parent5))
rm(linkd82_parent1,linkd82_parent2,linkd82_parent3,linkd82_parent4,linkd82_parent5)



####################clean 83############
c2<- linkd[linkd$method=="link83d",]

linkd<-matchleft(linkd,c2)
rm(c2)
dim(linkd)
#192471     34
####################clean 84############
c2<- linkd[linkd$method=="link84d",]

linkd84_parent<-c2
linkd<-matchleft(linkd,c2)
rm(c2)
dim(linkd)
#192461     34

####################clean 85############
c2<- linkd[linkd$method=="link85d",]


linkd<-matchleft(linkd,c2)
rm(c2)
dim(linkd)
#192453     34
####################clean 86############
c2<- linkd[linkd$method=="link86d",]

####################clean 87############
c2<- linkd[linkd$method=="link87d",]


linkd87_1<-linkd[linkd$method=="link87d" & substr(linkd$class,1,1)=="R" ,]
linkd87_1<-uniqueresult(linkd87_1)
dim(linkd)

linkd<-matchleft(linkd,linkd87_1)
dim(linkd)

c2<- linkd[linkd$method=="link87d",]



linkd87_parent<-c2[grepl("\\d",c2$saostartnumber),]

linkd87_parent1<-keepneed(linkd,linkd87_parent)
rm(linkd87_parent)

dim(linkd)
linkd<-matchleft(linkd,linkd87_parent1)
dim(linkd)

c2<- linkd[linkd$method=="link87d",]



linkd87_parent<-c2[grepl("\\d",c2$buildingname),]
linkd87_parent2<-keepneed(linkd,linkd87_parent)
rm(linkd87_parent)

dim(linkd)
# 
linkd<-matchleft(linkd,linkd87_parent2)
dim(linkd)
#287879     87
c2<- linkd[linkd$method=="link87d",]



linkd87_parent<-c2[grepl("\\d",c2$saotext),]
linkd87_parent3<-keepneed(linkd,linkd87_parent)
rm(linkd87_parent)

dim(linkd)
# 
linkd<-matchleft(linkd,linkd87_parent3)
dim(linkd)

c2<- linkd[linkd$method=="link87d",]


linkd<-matchleft(linkd,c2)
rm(c2)
dim(linkd)
#2287587     87
linkd87_parent<-rbindlist(list(linkd87_parent1,linkd87_parent2,linkd87_parent3))
rm(linkd87_parent1,linkd87_parent2,linkd87_parent3)

####################clean 88############
c2<- linkd[linkd$method=="link88d",]

####################clean 89############
c2<- linkd[linkd$method=="link89d",]


####################clean 90############
c2<- linkd[linkd$method=="link90d",]

linkd<-matchleft(linkd,c2)
rm(c2)
dim(linkd)
####################clean 91############
c2<- linkd[linkd$method=="link91d",]

####################clean 92############
c2<- linkd[linkd$method=="link92d",]

####################clean 93############
c2<- linkd[linkd$method=="link93d",]

####################clean 94############
c2<- linkd[linkd$method=="link94d",]

####################clean 95############
c2<- linkd[linkd$method=="link95d",]

####################clean 96############
c2<- linkd[linkd$method=="link96d",]


linkd96_1<-linkd[linkd$method=="link96d" & substr(linkd$class,1,1)=="R" ,]
linkd96_1<-uniqueresult(linkd96_1)
dim(linkd)

linkd<-matchleft(linkd,linkd96_1)
dim(linkd)

c2<- linkd[linkd$method=="link96d",]

linkd<-matchleft(linkd,c2)
rm(c2)
dim(linkd)
####################clean 97############
c2<- linkd[linkd$method=="link97d",]

linkd97_1<-linkd[linkd$method=="link97d" & substr(linkd$class,1,1)=="R" ,]
linkd97_1<-uniqueresult(linkd97_1)
dim(linkd)
linkd<-matchleft(linkd,linkd97_1)
dim(linkd)

c2<- linkd[linkd$method=="link97d",]


linkd<-matchleft(linkd,c2)
rm(c2)
dim(linkd)
####################clean 98############
c2<- linkd[linkd$method=="link98d",]


linkd98_parent<-c2

linkd<-matchleft(linkd,c2)
rm(c2)
dim(linkd)
####################clean 99############
c2<- linkd[linkd$method=="link99d",]

####################clean 100############
c2<- linkd[linkd$method=="link100d",]

####################clean 101############
c2<- linkd[linkd$method=="link101d",]


linkd101_1<-c2[c2$buildingnumber==c2$paostartnumber,]
linkd101_1<-uniqueresult(linkd101_1)
dim(linkd)
linkd<-matchleft(linkd,linkd101_1)
dim(linkd)
#190474     34


####################clean 102############
c2<- linkd[linkd$method=="link102d",]

####################clean 103############
c2<- linkd[linkd$method=="link103d",]


linkd103_1<-linkd[linkd$method=="link103d" & substr(linkd$class,1,1)=="R" ,]
linkd103_1<-uniqueresult(linkd103_1)
dim(linkd)
linkd<-matchleft(linkd,linkd103_1)
dim(linkd)



####################clean 104############
c2<- linkd[linkd$method=="link104d",]


linkd104_1<-linkd[linkd$method=="link104d" & substr(linkd$class,1,1)=="R" ,]
linkd104_1<-uniqueresult(linkd104_1)
dim(linkd)

linkd<-matchleft(linkd,linkd104_1)
dim(linkd)

c2<- linkd[linkd$method=="link104d",]




linkd104_parent<-c2[grepl("\\d",c2$saotext),]

linkd104_parent1<-keepneed(linkd,linkd104_parent)
rm(linkd104_parent)

dim(linkd)
linkd<-matchleft(linkd,linkd104_parent1)
dim(linkd)

c2<- linkd[linkd$method=="link104d",]



linkd104_parent<-c2[c2$buildingname=="",]
linkd104_parent<-linkd104_parent[grepl("\\d",linkd104_parent$paostartnumber),]
linkd104_parent2<-keepneed(linkd,linkd104_parent)
rm(linkd104_parent)

dim(linkd)
# 
linkd<-matchleft(linkd,linkd104_parent2)
dim(linkd)
#190107     34
c2<- linkd[linkd$method=="link104d",]





linkd104_parent<-c2[grepl("FLAT",c2$saotext),]
linkd104_parent3<-keepneed(linkd,linkd104_parent)
rm(linkd104_parent)

dim(linkd)
# 
linkd<-matchleft(linkd,linkd104_parent3)
dim(linkd)
# 190103     34
c2<- linkd[linkd$method=="link104d",]



linkd<-matchleft(linkd,c2)
rm(c2)
dim(linkd)
#221045104     104
linkd104_parent<-rbindlist(list(linkd104_parent1,linkd104_parent2,linkd104_parent3))
rm(linkd104_parent1,linkd104_parent2,linkd104_parent3)


####################clean 105############
c2<- linkd[linkd$method=="link105d",]

####################clean 106############
c2<- linkd[linkd$method=="link106d",]

####################clean 107############
c2<- linkd[linkd$method=="link107d",]



linkd107_1<-linkd[linkd$method=="link107d" & substr(linkd$class,1,1)=="R" ,]
linkd107_1<-uniqueresult(linkd107_1)
dim(linkd)
linkd<-matchleft(linkd,linkd107_1)
dim(linkd)

c2<- linkd[linkd$method=="link107d",]



linkd<-matchleft(linkd,c2)
rm(c2)

####################clean 108############
c2<- linkd[linkd$method=="link108d",]


linkd108_1<-linkd[linkd$method=="link108d" & substr(linkd$class,1,1)=="R" ,]
linkd108_1<-uniqueresult(linkd108_1)
dim(linkd)
linkd<-matchleft(linkd,linkd108_1)
dim(linkd)
#190070     34

c2<- linkd[linkd$method=="link108d",]



linkd108_parent<-c2[grepl("\\d",c2$saostartnumber),]

linkd108_parent1<-keepneed(linkd,linkd108_parent)
rm(linkd108_parent)

dim(linkd)
linkd<-matchleft(linkd,linkd108_parent1)
dim(linkd)

c2<- linkd[linkd$method=="link108d",]

linkd108_parent<-linkd108_parent1
rm(linkd108_parent1)


####################clean 109############
c2<- linkd[linkd$method=="link109d",]



linkd109_1<-linkd[linkd$method=="link109d" & substr(linkd$class,1,1)=="R" ,]
linkd109_1<-uniqueresult(linkd109_1)
dim(linkd)
linkd<-matchleft(linkd,linkd109_1)
dim(linkd)
#189941     34
c2<- linkd[linkd$method=="link109d",]

linkd109_parent<-c2
linkd<-matchleft(linkd,c2)
rm(c2)
dim(linkd)
#189930     34
####################clean 110############
c2<- linkd[linkd$method=="link110d",]


####################clean 111############
c2<- linkd[linkd$method=="link111d",]

####################clean 112############
c2<- linkd[linkd$method=="link112d",]


linkd112_parent<-c2[grepl("\\d",c2$saotext),]

linkd112_parent1<-keepneed(linkd,linkd112_parent)
rm(linkd112_parent)

dim(linkd)
linkd<-matchleft(linkd,linkd112_parent1)
dim(linkd)

c2<- linkd[linkd$method=="link112d",]




linkd112_parent<-c2[grepl("\\d",c2$buildingname),]
linkd112_parent2<-keepneed(linkd,linkd112_parent)
rm(linkd112_parent)

dim(linkd)
# 
linkd<-matchleft(linkd,linkd112_parent2)
dim(linkd)
#21121129     112
c2<- linkd[linkd$method=="link112d",]



linkd112_parent<-c2[grepl("FLAT",c2$subbuildingname),]
linkd112_parent3<-keepneed(linkd,linkd112_parent)
rm(linkd112_parent)

dim(linkd)
# 
linkd<-matchleft(linkd,linkd112_parent3)
dim(linkd)

c2<- linkd[linkd$method=="link112d",]



linkd<-matchleft(linkd,c2)
rm(c2)
dim(linkd)
#188835     34
linkd112_parent<-rbindlist(list(linkd112_parent1,linkd112_parent2,linkd112_parent3))
rm(linkd112_parent1,linkd112_parent2,linkd112_parent3)


####################clean 113############
c2<- linkd[linkd$method=="link113d",]



linkd113_1<-linkd[linkd$method=="link113d" & substr(linkd$class,1,1)=="R" ,]
linkd113_1<-uniqueresult(linkd113_1)
dim(linkd)
linkd<-matchleft(linkd,linkd113_1)
dim(linkd)
# 188814     34

c2<- linkd[linkd$method=="link113d",]





linkd113_parent<-c2[c2$buildingnumber=="",]
linkd113_parent<-linkd113_parent[grepl("\\d",linkd113_parent$saostartnumber),]
linkd113_parent1<-keepneed(linkd,linkd113_parent)
rm(linkd113_parent)

dim(linkd)
linkd<-matchleft(linkd,linkd113_parent1)
dim(linkd)
#188181     34
c2<- linkd[linkd$method=="link113d",]



linkd113_parent<-c2[grepl("\\d",c2$saostartnumber),]
linkd113_parent2<-keepneed(linkd,linkd113_parent)
rm(linkd113_parent)

dim(linkd)
# 
linkd<-matchleft(linkd,linkd113_parent2)
dim(linkd)
#188161     34
c2<- linkd[linkd$method=="link113d",]



linkd113_parent<-c2[grepl("\\d",c2$subbuildingname),]
linkd113_parent3<-keepneed(linkd,linkd113_parent)
rm(linkd113_parent)

dim(linkd)
# 
linkd<-matchleft(linkd,linkd113_parent3)
dim(linkd)
#188151     34
c2<- linkd[linkd$method=="link113d",]


linkd113_2<-c2[c2$buildingnumber==c2$paostartnumber,]
linkd113_2<-uniqueresult(linkd113_2)
dim(linkd)
linkd<-matchleft(linkd,linkd113_2)
dim(linkd)


linkd113_parent<-rbindlist(list(linkd113_parent1,linkd113_parent2,linkd113_parent3))
rm(linkd113_parent1,linkd113_parent2,linkd113_parent3)

####################clean 114############
c2<- linkd[linkd$method=="link114d",]

####################clean 115############
c2<- linkd[linkd$method=="link115d",]


linkd115_1<-linkd[linkd$method=="link115d" & substr(linkd$class,1,1)=="R" ,]
linkd115_1<-uniqueresult(linkd115_1)
dim(linkd)
linkd<-matchleft(linkd,linkd115_1)
dim(linkd)

c2<- linkd[linkd$method=="link115d",]




linkd115_parent<-c2[grepl("\\d",c2$saotext),]

linkd115_parent1<-keepneed(linkd,linkd115_parent)
rm(linkd115_parent)

dim(linkd)
linkd<-matchleft(linkd,linkd115_parent1)
dim(linkd)

c2<- linkd[linkd$method=="link115d",]



linkd115_parent<-c2[grepl("FLAT",c2$saotext),]

linkd115_parent2<-keepneed(linkd,linkd115_parent)
rm(linkd115_parent)

dim(linkd)
linkd<-matchleft(linkd,linkd115_parent2)
dim(linkd)


c2<- linkd[linkd$method=="link115d",]



linkd115_parent<-c2[grepl("FLOOR",c2$saotext),]

linkd115_parent3<-keepneed(linkd,linkd115_parent)
rm(linkd115_parent)

dim(linkd)
linkd<-matchleft(linkd,linkd115_parent3)
dim(linkd)


c2<- linkd[linkd$method=="link115d",]



linkd115_parent<-c2[grepl("APARTMENT",c2$saotext),]

linkd115_parent4<-keepneed(linkd,linkd115_parent)
rm(linkd115_parent)

dim(linkd)
linkd<-matchleft(linkd,linkd115_parent4)
dim(linkd)


c2<- linkd[linkd$method=="link115d",]


linkd<-matchleft(linkd,c2)
rm(c2)
dim(linkd)
#186897     34
linkd115_parent<-rbindlist(list(linkd115_parent1,linkd115_parent2,linkd115_parent3,linkd115_parent4))
rm(linkd115_parent1,linkd115_parent2,linkd115_parent3,linkd115_parent4)


####################clean 116############
c2<- linkd[linkd$method=="link116d",]


linkd116_parent<-c2

linkd<-matchleft(linkd,c2)
rm(c2)
dim(linkd)



####################clean 117############
c2<- linkd[linkd$method=="link117d",]

####################clean 118############
c2<- linkd[linkd$method=="link118d",]

####################clean 119############
c2<- linkd[linkd$method=="link119d",]


linkd119_parent<-c2

linkd<-matchleft(linkd,c2)
rm(c2)
dim(linkd)



####################clean 120############
c2<- linkd[linkd$method=="link120d",]


####################clean 121############
c2<- linkd[linkd$method=="link121d",]

####################clean 122############
c2<- linkd[linkd$method=="link122d",]


linkd122_parent<-c2

linkd<-matchleft(linkd,c2)
rm(c2)
dim(linkd)
#186856     34




####################clean 123############
c2<- linkd[linkd$method=="link123d",]


linkd123_1<-linkd[linkd$method=="link123d" & substr(linkd$class,1,1)=="R" ,]
linkd123_1<-uniqueresult(linkd123_1)
dim(linkd)



linkd<-matchleft(linkd,linkd123_1)
dim(linkd)

c2<- linkd[linkd$method=="link123d",]




linkd123_parent<-c2


linkd<-matchleft(linkd,c2)
rm(c2)
dim(linkd)
# 186710     34
####################clean 124############
c2<- linkd[linkd$method=="link124d",]



linkd124_1<-linkd[linkd$method=="link124d" & substr(linkd$class,1,1)=="R" ,]
linkd124_1<-uniqueresult(linkd124_1)
dim(linkd)


linkd<-matchleft(linkd,linkd124_1)
dim(linkd)

c2<- linkd[linkd$method=="link124d",]



linkd124_parent<-c2[grepl("\\d",c2$saotext),]

linkd124_parent1<-keepneed(linkd,linkd124_parent)
rm(linkd124_parent)

dim(linkd)
linkd<-matchleft(linkd,linkd124_parent1)
dim(linkd)



c2<- linkd[linkd$method=="link124d",]



linkd124_parent<-c2[grepl("FLAT",c2$saotext),]
linkd124_parent2<-keepneed(linkd,linkd124_parent)
rm(linkd124_parent)

dim(linkd)
# 
linkd<-matchleft(linkd,linkd124_parent2)
dim(linkd)
#21241249     124
c2<- linkd[linkd$method=="link124d",]



linkd124_parent<-c2[grepl("FLOOR",c2$saotext),]

linkd124_parent3<-keepneed(linkd,linkd124_parent)
rm(linkd124_parent)

dim(linkd)
linkd<-matchleft(linkd,linkd124_parent3)
dim(linkd)


c2<- linkd[linkd$method=="link124d",]


linkd<-matchleft(linkd,c2)
rm(c2)
dim(linkd)
#185504     34
linkd124_parent<-rbindlist(list(linkd124_parent1,linkd124_parent2,linkd124_parent3))
rm(linkd124_parent1,linkd124_parent2,linkd124_parent3)



####################clean 125############
c2<- linkd[linkd$method=="link125d",]


linkd125_1<-linkd[linkd$method=="link125d" & substr(linkd$class,1,1)=="R" ,]
linkd125_1<-uniqueresult(linkd125_1)
dim(linkd)
linkd<-matchleft(linkd,linkd125_1)
dim(linkd)

c2<- linkd[linkd$method=="link125d",]


linkd125_parent<-c2[grepl("\\d",c2$saostartnumber),]

linkd125_parent1<-keepneed(linkd,linkd125_parent)
rm(linkd125_parent)

dim(linkd)
linkd<-matchleft(linkd,linkd125_parent1)
dim(linkd)

c2<- linkd[linkd$method=="link125d",]


linkd<-matchleft(linkd,c2)
rm(c2)
dim(linkd)
#184863     34
linkd125_parent<-linkd125_parent1
rm(linkd125_parent1)


####################clean 126############
c2<- linkd[linkd$method=="link126d",]




####################clean 127############
c2<- linkd[linkd$method=="link127d",]




####################clean 128############
c2<- linkd[linkd$method=="link128d",]



####################clean 129############
c2<- linkd[linkd$method=="link129d",]


####################clean 130############
c2<- linkd[linkd$method=="link130d",]


####################clean 131############
c2<- linkd[linkd$method=="link131d",]

####################clean 132############
c2<- linkd[linkd$method=="link132d",]

####################clean 133############
c2<- linkd[linkd$method=="link133d",]

####################clean 134############
c2<- linkd[linkd$method=="link134d",]

####################clean 135############
c2<- linkd[linkd$method=="link135d",]

####################clean 136############
c2<- linkd[linkd$method=="link136d",]

####################clean 137############
c2<- linkd[linkd$method=="link137d",]

####################clean 138############
c2<- linkd[linkd$method=="link138d",]

####################clean 139############
c2<- linkd[linkd$method=="link139d",]

####################clean 140############
c2<- linkd[linkd$method=="link140d",]


linkd<-matchleft(linkd,c2)
rm(c2)
dim(linkd)
# 184861     34
####################clean 141############
c2<- linkd[linkd$method=="link141d",]

####################clean 142############
c2<- linkd[linkd$method=="link142d",]


linkd142_1<-linkd[linkd$method=="link142d" & substr(linkd$class,1,1)=="R" ,]
linkd142_1<-uniqueresult(linkd142_1)
dim(linkd)

linkd<-matchleft(linkd,linkd142_1)
dim(linkd)

c2<- linkd[linkd$method=="link142d",]


linkd142_parent<-c2[grepl("FLAT",c2$subbuildingname),]

linkd142_parent1<-keepneed(linkd,linkd142_parent)
rm(linkd142_parent)

dim(linkd)
linkd<-matchleft(linkd,linkd142_parent1)
dim(linkd)

c2<- linkd[linkd$method=="link142d",]


linkd<-matchleft(linkd,c2)
rm(c2)
dim(linkd)
linkd142_parent<-linkd142_parent1
rm(linkd142_parent1)

####################clean 143############
c2<- linkd[linkd$method=="link143d",]



linkd143<-linkd[linkd$method=="link143d",] 
linkd143$add2<-gsub(",","",linkd143$add2)
linkd143_1<-linkd143[word(linkd143$add2, -1)==word(linkd143$subbuildingname, -1),]


linkd143_1<-uniqueresult(linkd143_1)
dim(linkd)
linkd<-matchleft(linkd,linkd143_1)
dim(linkd)

c2<- linkd[linkd$method=="link143d",]


linkd143<-linkd[linkd$method=="link143d",] 
linkd143$add2<-gsub(",","",linkd143$add2)
linkd143<-linkd143[grepl("^\\d",linkd143$add2),]
linkd143_2<-linkd143[word(linkd143$add2, 1)==word(linkd143$subbuildingname, 1),]


linkd143_2<-uniqueresult(linkd143_2)
dim(linkd)
linkd<-matchleft(linkd,linkd143_2)
dim(linkd)
c2<- linkd[linkd$method=="link143d",]


linkd143<-linkd[linkd$method=="link143d",] 
linkd143$add2<-gsub(",","",linkd143$add2)
linkd143<-linkd143[grepl("^\\d",linkd143$add2),]
linkd143_3<-linkd143[word(linkd143$add2, 1)==word(linkd143$buildingnumber, 1),]


linkd143_3<-uniqueresult(linkd143_3)
dim(linkd)
linkd<-matchleft(linkd,linkd143_3)

c2<- linkd[linkd$method=="link143d",]


linkd143<-linkd[linkd$method=="link143d",] 
linkd143$add2<-gsub(",","",linkd143$add2)
linkd143<-linkd143[grepl("^\\d",linkd143$add2),]
linkd143_4<-linkd143[word(linkd143$add2, 1)==word(linkd143$subbuildingname, -1),]


linkd143_4<-uniqueresult(linkd143_4)
dim(linkd)
linkd<-matchleft(linkd,linkd143_4)

c2<- linkd[linkd$method=="link143d",]

c2<-c2[!grepl("\\d",c2$add2),]


linkd143_parent<-c2[grepl("\\d",c2$subbuildingname),]

linkd143_parent1<-keepneed(linkd,linkd143_parent)
rm(linkd143_parent)

dim(linkd)
linkd<-matchleft(linkd,linkd143_parent1)
dim(linkd)


c2<- linkd[linkd$method=="link143d",]

c2<-c2[!grepl("\\d",c2$add2),]



linkd143_6<-c2[c2$method=="link143d" & substr(c2$class,1,1)=="R" ,]
linkd143_6<-uniqueresult(linkd143_6)
dim(linkd)
linkd<-matchleft(linkd,linkd143_6)
dim(linkd)
c2<- linkd[linkd$method=="link143d",]

c2<-c2[!grepl("\\d",c2$add2),]

linkd<-matchleft(linkd,c2)



c2<- linkd[linkd$method=="link143d",]
c2<-c2[grepl("^\\d",c2$add2),]


linkd143_parent<-c2[grepl("\\d",c2$subbuildingname),]

linkd143_parent2<-keepneed(linkd,linkd143_parent)
rm(linkd143_parent)

dim(linkd)
linkd<-matchleft(linkd,linkd143_parent2)
dim(linkd)

c2<- linkd[linkd$method=="link143d",]
c2<-c2[grepl("^\\d",c2$add2),]


linkd143_parent<-c2[grepl("FLAT",c2$subbuildingname),]

linkd143_parent3<-keepneed(linkd,linkd143_parent)
rm(linkd143_parent)

dim(linkd)
linkd<-matchleft(linkd,linkd143_parent3)
dim(linkd)
c2<- linkd[linkd$method=="link143d",]
c2<-c2[grepl("^\\d",c2$add2),]


fail_1<-c2
linkd<-matchleft(linkd,fail_1)
dim(linkd)

late_1<-linkd[linkd$method=="link143d" & postcode.y=="M1 5AA",]
linkd<-matchleft(linkd,late_1)
dim(linkd)

c2<- linkd[linkd$method=="link143d",]
#c2<-c2[grepl("^\\d",c2$add2),]
c2<-c2[grepl("\\d",c2$add2),]


c2$add2<-gsub(",","",c2$add2)
#linkd143<-linkd143[grepl("^\\d",linkd143$add2),]
linkd143_7<-c2[word(c2$add2,1,3)==paste(c2$subbuildingname,c2$buildingnumber,sep=" "),]


linkd143_7<-uniqueresult(linkd143_7)
dim(linkd)
linkd<-matchleft(linkd,linkd143_7)
dim(linkd)
#164877     34
c2<- linkd[linkd$method=="link143d",]
#c2<-c2[grepl("^\\d",c2$add2),]
c2<-c2[grepl("\\d",c2$add2),]

c2$add2<-gsub(",","",c2$add2)

linkd143_8<-c2[c2$add2==paste(c2$saotext,c2$streetdescription,sep=" "),]


linkd143_8<-uniqueresult(linkd143_8)
dim(linkd)
linkd<-matchleft(linkd,linkd143_8)
dim(linkd)
#164877     34
c2<- linkd[linkd$method=="link143d",]

fail_2<-c2
linkd<-matchleft(linkd,fail_2)
dim(linkd)
# 164754     34

#c2<- linkd[linkd$method=="link143d",]
linkd143_parent<-rbindlist(list(linkd143_parent1,linkd143_parent2,linkd143_parent3))
rm(linkd143_parent1,linkd143_parent2,linkd143_parent3)
####################clean 144############
c2<- linkd[linkd$method=="link144d",]

linkd144_1<-c2[word(c2$add1, -1)==word(c2$subbuildingname, -1),]

linkd144_1<-uniqueresult(linkd144_1)
dim(linkd)
linkd<-matchleft(linkd,linkd144_1)
dim(linkd)
#164754     34

c2<- linkd[linkd$method=="link144d",]



linkd144_parent<-c2[grepl("\\d",c2$subbuildingname),]

linkd144_parent1<-keepneed(linkd,linkd144_parent)
rm(linkd144_parent)

dim(linkd)
linkd<-matchleft(linkd,linkd144_parent1)
dim(linkd)
# 164723     34
c2<- linkd[linkd$method=="link144d",]


linkd<-matchleft(linkd,c2)
rm(c2)
dim(linkd)

linkd144_parent<-linkd144_parent1
rm(linkd144_parent1)


####################clean 145############
c2<- linkd[linkd$method=="link145d",]


linkd145_1<-linkd[linkd$method=="link145d" & substr(linkd$class,1,1)=="R" ,]
linkd145_1<-uniqueresult(linkd145_1)
dim(linkd)

linkd<-matchleft(linkd,linkd145_1)
dim(linkd)

c2<- linkd[linkd$method=="link145d",]


linkd145_parent<-c2[grepl("\\d",c2$saotext),]

linkd145_parent1<-keepneed(linkd,linkd145_parent)
rm(linkd145_parent)

dim(linkd)
linkd<-matchleft(linkd,linkd145_parent1)
dim(linkd)


c2<- linkd[linkd$method=="link145d",]



linkd145_parent<-c2[grepl("FLAT",c2$saotext),]
linkd145_parent2<-keepneed(linkd,linkd145_parent)
rm(linkd145_parent)

dim(linkd)
# 
linkd<-matchleft(linkd,linkd145_parent2)
dim(linkd)
#21451459     145
c2<- linkd[linkd$method=="link145d",]


linkd145_parent<-c2[grepl("FLOOR",c2$saotext),]

linkd145_parent3<-keepneed(linkd,linkd145_parent)
rm(linkd145_parent)

dim(linkd)
linkd<-matchleft(linkd,linkd145_parent3)
dim(linkd)


c2<- linkd[linkd$method=="link145d",]

linkd145_parent<-c2[grepl("\\d",c2$subbuildingname),]

linkd145_parent4<-keepneed(linkd,linkd145_parent)
rm(linkd145_parent)

dim(linkd)
linkd<-matchleft(linkd,linkd145_parent4)
dim(linkd)


c2<- linkd[linkd$method=="link145d",]


linkd<-matchleft(linkd,c2)
rm(c2)
dim(linkd)
#221455145     145
linkd145_parent<-rbindlist(list(linkd145_parent1,linkd145_parent2,linkd145_parent3,linkd145_parent4))
rm(linkd145_parent1,linkd145_parent2,linkd145_parent3,linkd145_parent4)

####################clean 146############
c2<- linkd[linkd$method=="link146d",]




linkd146_1<-linkd[linkd$method=="link146d" & substr(linkd$class,1,1)=="R" ,]
linkd146_1<-uniqueresult(linkd146_1)
dim(linkd)
linkd<-matchleft(linkd,linkd146_1)
dim(linkd)
c2<- linkd[linkd$method=="link146d",]


linkd146_parent<-c2[grepl("\\d",c2$saotext),]

linkd146_parent1<-keepneed(linkd,linkd146_parent)
rm(linkd146_parent)

dim(linkd)
linkd<-matchleft(linkd,linkd146_parent1)
dim(linkd)

c2<- linkd[linkd$method=="link146d",]



linkd146_parent<-c2[grepl("FLAT",c2$saotext),]
linkd146_parent2<-keepneed(linkd,linkd146_parent)
rm(linkd146_parent)

dim(linkd)
# 
linkd<-matchleft(linkd,linkd146_parent2)
dim(linkd)
#21461469     146
c2<- linkd[linkd$method=="link146d",]



linkd146_parent<-c2[grepl("FLOOR",c2$saotext),]
linkd146_parent3<-keepneed(linkd,linkd146_parent)
rm(linkd146_parent)

dim(linkd)
# 
linkd<-matchleft(linkd,linkd146_parent3)
dim(linkd)

c2<- linkd[linkd$method=="link146d",]



linkd146_parent<-c2[grepl("ABOVE",c2$saotext),]

linkd146_parent4<-keepneed(linkd,linkd146_parent)
rm(linkd146_parent)

dim(linkd)
linkd<-matchleft(linkd,linkd146_parent4)
dim(linkd)
#158918     34

c2<- linkd[linkd$method=="link146d",]


linkd146_parent<-c2[grepl("ROOM",c2$paotext),]
linkd146_parent5<-keepneed(linkd,linkd146_parent)
rm(linkd146_parent)

dim(linkd)
# 
linkd<-matchleft(linkd,linkd146_parent5)
dim(linkd)


c2<- linkd[linkd$method=="link146d",]





linkd146_parent<-c2[grepl("\\d",c2$buildingnumber),]
linkd146_parent6<-keepneed(linkd,linkd146_parent)
rm(linkd146_parent)

dim(linkd)
# 
linkd<-matchleft(linkd,linkd146_parent6)
dim(linkd)

c2<- linkd[linkd$method=="link146d",]





linkd<-matchleft(linkd,c2)
rm(c2)
dim(linkd)
#158871     34
linkd146_parent<-rbindlist(list(linkd146_parent1,linkd146_parent2,linkd146_parent3,linkd146_parent4,linkd146_parent5,linkd146_parent6))
rm(linkd146_parent1,linkd146_parent2,linkd146_parent3,linkd146_parent4,linkd146_parent5,linkd146_parent6)

####################clean 147############
c2<- linkd[linkd$method=="link147d",]



linkd147<-linkd[linkd$method=="link147d",] 
linkd147$add2<-gsub(",","",linkd147$add2)
linkd147_1<-linkd147[word(linkd147$add2, 1)==linkd147$paostartnumber,]

linkd147_1<-uniqueresult(linkd147_1)
dim(linkd)
linkd<-matchleft(linkd,linkd147_1)

c2<- linkd[linkd$method=="link147d",]


linkd<-matchleft(linkd,c2)

####################clean 148############
c2<- linkd[linkd$method=="link148d",]

detail_1<-c2

linkd<-matchleft(linkd,c2)

####################clean 149############
c2<- linkd[linkd$method=="link149d",]

####################clean 150############
c2<- linkd[linkd$method=="link150d",]


####################clean 151 ############
c2<- linkd[linkd$method=="link151d",]



linkd151<-linkd[linkd$method=="link151d",] 
linkd151<-linkd151[linkd151$add3!="",]
linkd151$add3<-gsub(",","",linkd151$add3)
linkd151_1<-linkd151[word(linkd151$add3, 1)==linkd151$paostartnumber,]


linkd151_1<-uniqueresult(linkd151_1)
dim(linkd)
linkd<-matchleft(linkd,linkd151_1)
dim(linkd)

c2<- linkd[linkd$method=="link151d",]


detail_2<-c2

linkd<-matchleft(linkd,c2)

####################clean 152############
c2<- linkd[linkd$method=="link152d",]

####################clean 153############
c2<- linkd[linkd$method=="link153d",]

# match based on add2
#add2=ss


linkd153<-linkd[linkd$method=="link153d",] 
linkd153$add2<-gsub(",","",linkd153$add2)
linkd153_1<-linkd153[word(linkd153$add2, 1)==paste(linkd153$saostartnumber,linkd153$saostartsuffix,sep=""),]


linkd153_1<-uniqueresult(linkd153_1)
dim(linkd)
linkd<-matchleft(linkd,linkd153_1)
dim(linkd)
# 158659     34
linkd153<-linkd[linkd$method=="link153d",] 
linkd153$add2<-gsub(",","",linkd153$add2)
linkd153<-linkd153[grepl("^\\d",linkd153$add2),]


fail_3<-linkd153
linkd<-matchleft(linkd,fail_3)
dim(linkd)

linkd153<-linkd[linkd$method=="link153d",] 

housename_diff1<-linkd153



linkd<-matchleft(linkd,housename_diff1)
dim(linkd)

###############161############
c2<- linkd[linkd$method=="link161d",]



linkd161<-linkd[linkd$method=="link161d",] 
linkd161$add2<-gsub(",","",linkd161$add3)
linkd161_1<-linkd161[word(linkd161$add3, 1)==linkd161$saostartnumber,]


linkd161_1<-uniqueresult(linkd161_1)
dim(linkd)
linkd<-matchleft(linkd,linkd161_1)
dim(linkd)
c2<- linkd[linkd$method=="link161d",]

detail_3<-c2
linkd<-matchleft(linkd,detail_3)
dim(linkd)

rm(c2)
####################clean 162############
c2<- linkd[linkd$method=="link162d",]


detail_4<-c2

linkd<-matchleft(linkd,detail_4)
dim(linkd)



####################clean 167############
c2<- linkd[linkd$method=="link167d",]


fail_4<-c2

linkd<-matchleft(linkd,fail_4)
dim(linkd)


####################clean 168############
sort(unique(linkd$method))
c2<- linkd[linkd$method=="link168d",]


detail_5<-c2

linkd<-matchleft(linkd,detail_5)
dim(linkd)

####################clean 169############
length(unique(linkd$method))
#120

##mach based on c2



linkd169<-linkd[linkd$method=="link169d",] 
linkd169$add2<-gsub(",","",linkd169$add2)
linkd169_1<-linkd169[word(linkd169$add2, 1)==paste(linkd169$paostartnumber,linkd169$paostartsuffix,sep=""),]



linkd169_1<-uniqueresult(linkd169_1)
dim(linkd)
linkd<-matchleft(linkd,linkd169_1)
dim(linkd)


c2<- linkd[linkd$method=="link169d",]


linkd169<-linkd[linkd$method=="link169d",] 
linkd169$add2<-gsub(",","",linkd169$add2)
linkd169_2<-linkd169[word(linkd169$add2, -1)==linkd169$saostartnumber,]



linkd169_2<-uniqueresult(linkd169_2)
dim(linkd)
linkd<-matchleft(linkd,linkd169_2)
dim(linkd)
#151344     34



linkd169<-linkd[linkd$method=="link169d",] 
linkd169$add2<-gsub(",","",linkd169$add2)
linkd169_3<-linkd169[word(linkd169$add2, 1)==linkd169$buildingnumber,]



linkd169_3<-uniqueresult(linkd169_3)
dim(linkd)
linkd<-matchleft(linkd,linkd169_3)
dim(linkd)


linkd169<-linkd[linkd$method=="link169d",] 
linkd169$add2<-gsub(",","",linkd169$add2)
linkd169_4<-linkd169[word(linkd169$add2, 1)==linkd169$saostartnumber,]



linkd169_4<-uniqueresult(linkd169_4)
dim(linkd)
linkd<-matchleft(linkd,linkd169_4)
dim(linkd)




linkd169<-linkd[linkd$method=="link169d",] 
linkd169$add2<-gsub(",","",linkd169$add2)
linkd169_5<-linkd169[word(linkd169$add2, 1)==paste(linkd169$saostartnumber,linkd169$saostartsuffix,sep=""),]



linkd169_5<-uniqueresult(linkd169_5)
dim(linkd)
linkd<-matchleft(linkd,linkd169_5)
dim(linkd)

c2<- linkd[linkd$method=="link169d",]

c2<-c2[grepl("\\d",c2$add2),]




linkd169_parent<-c2[grepl("\\d",c2$saotext),]

linkd169_parent1<-keepneed(linkd,linkd169_parent)
rm(linkd169_parent)

dim(linkd)
linkd<-matchleft(linkd,linkd169_parent1)
dim(linkd)

c2<- linkd[linkd$method=="link169d",]

c2<-c2[grepl("\\d",c2$add2),]




linkd169_parent<-c2[grepl("ROOM",c2$saotext),]
linkd169_parent2<-keepneed(linkd,linkd169_parent)
rm(linkd169_parent)

dim(linkd)
# 
linkd<-matchleft(linkd,linkd169_parent2)
dim(linkd)
#21691699     169
c2<- linkd[linkd$method=="link169d",]
c2<-c2[grepl("\\d",c2$add2),]



linkd169_parent<-c2[grepl("FLAT",c2$saotext),]
linkd169_parent3<-keepneed(linkd,linkd169_parent)
rm(linkd169_parent)

dim(linkd)
# 
linkd<-matchleft(linkd,linkd169_parent3)
dim(linkd)

c2<- linkd[linkd$method=="link169d",]
c2<-c2[grepl("\\d",c2$add2),]


linkd169_parent<-c2[grepl("\\d",c2$saostartnumber),]

linkd169_parent4<-keepneed(linkd,linkd169_parent)
rm(linkd169_parent)

dim(linkd)
linkd<-matchleft(linkd,linkd169_parent4)
dim(linkd)

c2<- linkd[linkd$method=="link169d",]
c2<-c2[grepl("\\d",c2$add2),]

detail_6<-c2
linkd<-matchleft(linkd,detail_6)
dim(linkd)

c2<- linkd[linkd$method=="link169d",]



linkd169_parent<-c2[grepl("\\d",c2$saotext),]
linkd169_parent5<-keepneed(linkd,linkd169_parent)
rm(linkd169_parent)

dim(linkd)
# 
linkd<-matchleft(linkd,linkd169_parent5)
dim(linkd)




c2<- linkd[linkd$method=="link169d",]





linkd169_parent<-c2[grepl("\\d",c2$saostartnumber),]
linkd169_parent6<-keepneed(linkd,linkd169_parent)
rm(linkd169_parent)

dim(linkd)
# 
linkd<-matchleft(linkd,linkd169_parent6)
dim(linkd)
c2<- linkd[linkd$method=="link169d",]


detail_7<-c2


linkd<-matchleft(linkd,c2)
rm(c2)
dim(linkd)

linkd169_parent<-rbindlist(list(linkd169_parent1,linkd169_parent2,linkd169_parent3,linkd169_parent4,linkd169_parent5,linkd169_parent6))
rm(linkd169_parent1,linkd169_parent2,linkd169_parent3,linkd169_parent4,linkd169_parent5,linkd169_parent6)


####################clean 170############
c2<- linkd[linkd$method=="link170d",]


####################clean 171############
c2<- linkd[linkd$method=="link171d",]




linkd171<-linkd[linkd$method=="link171d",] 
linkd171$add2<-gsub(",","",linkd171$add2)
linkd171_1<-linkd171[word(linkd171$add2, 1)==paste(linkd171$paostartnumber,linkd171$paoendnumber,sep="-"),]



linkd171_1<-uniqueresult(linkd171_1)
dim(linkd)
linkd<-matchleft(linkd,linkd171_1)
dim(linkd)



####################clean 173############
c2<- linkd[linkd$method=="link173d",]


linkd173<-linkd[linkd$method=="link173d",] 
linkd173$add2<-gsub(",","",linkd173$add2)
linkd173_1<-linkd173[word(linkd173$add2, 1,2)==linkd173$subbuildingname,]



linkd173_1<-uniqueresult(linkd173_1)
dim(linkd)
linkd<-matchleft(linkd,linkd173_1)
dim(linkd)
#85679    34
c2<- linkd[linkd$method=="link173d",]


linkd173<-linkd[linkd$method=="link173d",] 
linkd173$add2<-gsub(",","",linkd173$add2)
linkd173_2<-linkd173[linkd173$add2==paste(linkd173$saostartnumber,linkd173$paotext,sep=" "),]



linkd173_2<-uniqueresult(linkd173_2)
dim(linkd)
linkd<-matchleft(linkd,linkd173_2)
dim(linkd)
#85061    34

c2<- linkd[linkd$method=="link173d",]



linkd173<-linkd[linkd$method=="link173d",] 
linkd173$add2<-gsub(",","",linkd173$add2)
linkd173_3<-linkd173[word(linkd173$add2,-2,-1)==linkd173$subbuildingname,]



linkd173_3<-uniqueresult(linkd173_3)
dim(linkd)
linkd<-matchleft(linkd,linkd173_3)
dim(linkd)



c2<- linkd[linkd$method=="link173d",]




linkd173_parent<-c2[grepl("\\d",c2$subbuildingname),]

linkd173_parent1<-keepneed(linkd,linkd173_parent)
rm(linkd173_parent)

dim(linkd)
linkd<-matchleft(linkd,linkd173_parent1)
dim(linkd)

c2<- linkd[linkd$method=="link173d",]




linkd173_parent<-c2[grepl("FLAT",c2$saotext),]
linkd173_parent2<-keepneed(linkd,linkd173_parent)
rm(linkd173_parent)

dim(linkd)
# 
linkd<-matchleft(linkd,linkd173_parent2)
dim(linkd)
#21731739     173
c2<- linkd[linkd$method=="link173d",]


detail_8<-c2
linkd<-matchleft(linkd,c2)
rm(c2)
dim(linkd)
#221735173     173
linkd173_parent<-rbindlist(list(linkd173_parent1,linkd173_parent2))
rm(linkd173_parent1,linkd173_parent2)


####################clean 174############
c2<- linkd[linkd$method=="link174d",]


linkd<-matchleft(linkd,c2)
rm(c2)
dim(linkd)


####################clean 175############
c2<- linkd[linkd$method=="link175d",]


linkd175_1<-linkd[linkd$method=="link175d" & substr(linkd$class,1,1)=="R" ,]
linkd175_1<-uniqueresult(linkd175_1)
dim(linkd)
linkd<-matchleft(linkd,linkd175_1)
dim(linkd)


####################clean 176############
length(unique(linkd$method))
c2<- linkd[linkd$method=="link176d",]


linkd176_parent<-c2[grepl("\\d",c2$saotext),]

linkd176_parent1<-keepneed(linkd,linkd176_parent)
rm(linkd176_parent)

dim(linkd)
linkd<-matchleft(linkd,linkd176_parent1)
dim(linkd)

c2<- linkd[linkd$method=="link176d",]



linkd176_parent<-c2[grepl("FLAT",c2$saotext),]
linkd176_parent2<-keepneed(linkd,linkd176_parent)
rm(linkd176_parent)

dim(linkd)
# 
linkd<-matchleft(linkd,linkd176_parent2)
dim(linkd)
#21761769     176
c2<- linkd[linkd$method=="link176d",]




linkd176_parent<-c2[grepl("ROOM",c2$saotext),]
linkd176_parent3<-keepneed(linkd,linkd176_parent)
rm(linkd176_parent)

dim(linkd)
# 
linkd<-matchleft(linkd,linkd176_parent3)
dim(linkd)

c2<- linkd[linkd$method=="link176d",]

linkd<-matchleft(linkd,c2)
rm(c2)
dim(linkd)

linkd176_parent<-rbindlist(list(linkd176_parent1,linkd176_parent2,linkd176_parent3))
rm(linkd176_parent1,linkd176_parent2,linkd176_parent3)

####################clean 177############
c2<- linkd[linkd$method=="link177d",]

####################clean 178############
c2<- linkd[linkd$method=="link178d",]

####################clean 179############
c2<- linkd[linkd$method=="link179d",]

####################clean 180############
c2<- linkd[linkd$method=="link180d",]



linkd180<-linkd[linkd$method=="link180d",] 
linkd180<-linkd180[grepl("\\d",linkd180$add2),]
linkd180$add2<-gsub(",","",linkd180$add2)
linkd180_1<-linkd180[word(linkd180$add2, 1)==linkd180$paostartnumber,]


linkd180_1<-uniqueresult(linkd180_1)
dim(linkd)
linkd<-matchleft(linkd,linkd180_1)
dim(linkd)
#84049    34

linkd180<-linkd[linkd$method=="link180d",] 
linkd180$add2<-gsub(",","",linkd180$add2)
linkd180<-linkd180[!grepl("^\\d",linkd180$add2),]

detail_9<-linkd180
linkd<-matchleft(linkd,detail_9)
dim(linkd)

c2<- linkd[linkd$method=="link180d",]



fail_5<-c2

linkd<-matchleft(linkd,fail_5)
dim(linkd)



####################clean 196############
c2<- linkd[linkd$method=="link196d",]


linkd196<-linkd[linkd$method=="link196d",] 
linkd196$add1<-gsub(",","",linkd196$add1)
linkd196$add1c<-str_remove(linkd196$add1, '(\\w+\\s+){1}')


linkd196_1<-linkd196[linkd196$add1c==linkd196$paotext,]


linkd196_1<-uniqueresult(linkd196_1)
dim(linkd)
linkd<-matchleft(linkd,linkd196_1)
dim(linkd)
#82891    34

linkd196<-linkd[linkd$method=="link196d",] 
linkd196$add1<-gsub(",","",linkd196$add1)
linkd196$add1c<-str_remove(linkd196$add1, '(\\w+\\s+){1}')

linkd196_2<-linkd196[word(linkd196$add1c, 1,2)==linkd196$paotext,]


linkd196_2<-uniqueresult(linkd196_2)
dim(linkd)
linkd<-matchleft(linkd,linkd196_2)
dim(linkd)


c2<- linkd[linkd$method=="link196d",]


detail_10<-c2
linkd<-matchleft(linkd,c2)
rm(c2)
dim(linkd)

####################clean 197############
c2<- linkd[linkd$method=="link197d",]



linkd197<-linkd[linkd$method=="link197d",] 
linkd197$add1<-gsub(",","",linkd197$add1)
linkd197$add1c<-str_remove(linkd197$add1, '(\\w+\\s+){1}')


linkd197_1<-linkd197[linkd197$add1c==linkd197$paotext,]


linkd197_1<-uniqueresult(linkd197_1)
dim(linkd)
linkd<-matchleft(linkd,linkd197_1)
dim(linkd)
c2<- linkd[linkd$method=="link197d",]

linkd<-matchleft(linkd,c2)
rm(c2)
dim(linkd)

####################clean 198############
c2<- linkd[linkd$method=="link198d",]


linkd198_parent<-c2

linkd<-matchleft(linkd,c2)
rm(c2)
dim(linkd)
####################clean 199############
c2<- linkd[linkd$method=="link199d",]


linkd199_parent<-c2

linkd<-matchleft(linkd,c2)
rm(c2)
dim(linkd)


####################clean 202############
c2<- linkd[linkd$method=="link202d",]

linkd202<-linkd[linkd$method=="link202d",] 

linkd202[postcode.y=="CV13 0NP",add2:=gsub("GODSON HILL FARM", "GODSONS HILL FARM",add2)]

linkd202_1<-linkd202[linkd202$add2==linkd202$buildingname,]

linkd202_1<-uniqueresult(linkd202_1)

linkd<-matchleft(linkd,linkd202_1)

c2<- linkd[linkd$method=="link202d",]


linkd202<-linkd[linkd$method=="link202d",] 
linkd202$add2<-gsub(",","",linkd202$add2)
#linkd202<-linkd202[grepl("^\\d",linkd202$add2),]
linkd202_2<-linkd202[word(linkd202$add2, 1,3)==word(linkd202$paotext, 1,3),]


linkd202_2<-uniqueresult(linkd202_2)
dim(linkd)
linkd<-matchleft(linkd,linkd202_2)
dim(linkd)
c2<- linkd[linkd$method=="link202d",]


linkd<-matchleft(linkd,c2)
dim(linkd)
# 82420    34
####################clean 203############
c2<- linkd[linkd$method=="link203d",]




linkd203<-linkd[linkd$method=="link203d",] 
linkd203$add2<-gsub(",","",linkd203$add2)
linkd203_1<-linkd203[word(linkd203$add2, 1)==linkd203$buildingnumber,]


linkd203_1<-uniqueresult(linkd203_1)
dim(linkd)
linkd<-matchleft(linkd,linkd203_1)
dim(linkd)
c2<- linkd[linkd$method=="link203d",]

fail_6<-c2
linkd<-matchleft(linkd,c2)
dim(linkd)
rm(c2)

####################clean 205############
c2<- linkd[linkd$method=="link205d",]

detail_46<-c2
linkd<-matchleft(linkd,detail_46)
dim(linkd)

####################clean 211############
length(unique(linkd$method))
c2<- linkd[linkd$method=="link211d",]


detail_11<-c2
linkd<-matchleft(linkd,c2)
dim(linkd)
rm(c2)

####################clean 214############
c2<- linkd[linkd$method=="link214d",]



linkd214<-linkd[linkd$method=="link214d",] 
linkd214$add1<-gsub(",","",linkd214$add1)
linkd214$add1c<-str_remove(linkd214$add1, '(\\w+\\s+){1}')


linkd214_1<-linkd214[linkd214$add1c==linkd214$paotext,]


linkd214_1<-uniqueresult(linkd214_1)
dim(linkd)
linkd<-matchleft(linkd,linkd214_1)
# 81912    34
linkd214<-linkd[linkd$method=="link214d",] 

linkd<-matchleft(linkd,linkd214)
dim(linkd)
rm(c2)


####################clean 217############
c2<- linkd[linkd$method=="link217d",]

linkd217_1<-linkd[linkd$method=="link217d" & substr(linkd$class,1,1)=="R" ,]
linkd217_1<-uniqueresult(linkd217_1)
dim(linkd)
linkd<-matchleft(linkd,linkd217_1)
dim(linkd)


c2<- linkd[linkd$method=="link217d",]



linkd217_parent<-c2
linkd<-matchleft(linkd,c2)
rm(c2)
dim(linkd)
####################clean 218############
c2<- linkd[linkd$method=="link218d",]


detail_12<-c2

linkd217_parent<-c2
linkd<-matchleft(linkd,c2)
rm(c2)
dim(linkd)

####################clean 224############
c2<- linkd[linkd$method=="link224d",]

linkd224_parent<-c2
linkd<-matchleft(linkd,c2)
rm(c2)
dim(linkd)
####################clean 225############
c2<- linkd[linkd$method=="link225d",]


linkd225_parent<-c2
linkd<-matchleft(linkd,c2)
rm(c2)
dim(linkd)


####################clean 226############
c2<- linkd[linkd$method=="link226d",]


detail_12<-c2
linkd<-matchleft(linkd,c2)
rm(c2)
dim(linkd)


####################clean 227############
c2<- linkd[linkd$method=="link227d",]


detail_13<-c2
linkd<-matchleft(linkd,c2)
rm(c2)
dim(linkd)


####################clean 234############
c2<- linkd[linkd$method=="link234d",]


linkd234_1<-linkd[linkd$method=="link234d" & substr(linkd$class,1,1)=="R" ,]
linkd234_1<-uniqueresult(linkd234_1)
dim(linkd)
linkd<-matchleft(linkd,linkd234_1)
dim(linkd)

c2<- linkd[linkd$method=="link234d",]


linkd234_parent<-c2
linkd<-matchleft(linkd,c2)
rm(c2)
dim(linkd)

####################clean 236############
length(unique(linkd$method))
c2<- linkd[linkd$method=="link236d",]

#c2<-c2[c2$saotext=="",]
linkd236_1<-linkd[linkd$method=="link236d" & substr(linkd$class,1,1)=="R" ,]
linkd236_1<-uniqueresult(linkd236_1)
dim(linkd)
linkd<-matchleft(linkd,linkd236_1)
dim(linkd)

c2<- linkd[linkd$method=="link236d",]
c2<-c2[c2$saotext=="",]


linkd236_parent1<-c2

dim(linkd)
linkd<-matchleft(linkd,linkd236_parent1)



dim(linkd)

c2<- linkd[linkd$method=="link236d",]


detail_14<-c2

linkd<-matchleft(linkd,c2)
rm(c2)
dim(linkd)
linkd236_parent<-linkd236_parent1
rm(linkd236_parent1)

####################clean 237############
c2<- linkd[linkd$method=="link237d",]


linkd237_1<-linkd[linkd$method=="link237d" & substr(linkd$class,1,1)=="R" ,]
linkd237_1<-uniqueresult(linkd237_1)
dim(linkd)
linkd<-matchleft(linkd,linkd237_1)
dim(linkd)

c2<- linkd[linkd$method=="link237d",]


linkd237_parent<-c2

linkd<-matchleft(linkd,c2)
rm(c2)
dim(linkd)



####################clean 239############
c2<- linkd[linkd$method=="link239d",]

linkd239_1<-linkd[linkd$method=="link239d" & substr(linkd$class,1,1)=="R" ,]
linkd239_1<-uniqueresult(linkd239_1)
dim(linkd)
linkd<-matchleft(linkd,linkd239_1)
dim(linkd)

c2<- linkd[linkd$method=="link239d",]


linkd239_parent<-c2
linkd<-matchleft(linkd,c2)
rm(c2)
dim(linkd)

###############240############
c2<- linkd[linkd$method=="link240d",]


linkd240_1<-linkd[linkd$method=="link240d" & substr(linkd$class,1,1)=="R" ,]
linkd240_1<-uniqueresult(linkd240_1)
dim(linkd)
linkd<-matchleft(linkd,linkd240_1)
dim(linkd)

c2<- linkd[linkd$method=="link240d",]

linkd240_parent<-c2
linkd<-matchleft(linkd,c2)
rm(c2)
dim(linkd)


####################clean 246############################
length(unique(linkd$method))
#94
c2<- linkd[linkd$method=="link246d",]


##clean based on add2



linkd246<-linkd[linkd$method=="link246d",] 
linkd246$add2<-gsub(",","",linkd246$add2)
linkd246_1<-linkd246[word(linkd246$add2, 1)==linkd246$paostartnumber,]

linkd246_1<-uniqueresult(linkd246_1)
dim(linkd)
linkd<-matchleft(linkd,linkd246_1)
dim(linkd)


c2<- linkd[linkd$method=="link246d",]




linkd246<-linkd[linkd$method=="link246d",] 
linkd246$add2<-gsub(",","",linkd246$add2)
linkd246_2<-linkd246[word(linkd246$add2, 1)==paste(linkd246$paostartnumber,linkd246$paoendnumber,sep="-"),]



linkd246_2<-uniqueresult(linkd246_2)
dim(linkd)
linkd<-matchleft(linkd,linkd246_2)
dim(linkd)
#151344     34


c2<- linkd[linkd$method=="link246d",]



fail_7<-c2[grepl("\\d",c2$add2),]

linkd<-matchleft(linkd,fail_7)
dim(linkd)


c2<- linkd[linkd$method=="link246d",]


detail_15<-c2
linkd<-matchleft(linkd,c2)
rm(c2)
dim(linkd)
####################clean 247############
c2<- linkd[linkd$method=="link247d",]

fail_7<-c2
linkd<-matchleft(linkd,c2)
rm(c2)

####################clean 249############
c2<- linkd[linkd$method=="link249d",]

linkd249_parent<-c2
linkd<-matchleft(linkd,c2)
rm(c2)

####################clean 253############
length(unique(linkd$method))
c2<- linkd[linkd$method=="link253d",]


#addd2=ss


linkd253<-linkd[linkd$method=="link253d",] 
linkd253$add2<-gsub(",","",linkd253$add2)
linkd253_1<-linkd253[word(linkd253$add2, 1)==paste(linkd253$saostartnumber,linkd253$saostartsuffix,sep=""),]



linkd253_1<-uniqueresult(linkd253_1)
dim(linkd)
linkd<-matchleft(linkd,linkd253_1)
dim(linkd)
# 73302    34

#add2=paotext



linkd253<-linkd[linkd$method=="link253d",] 
linkd253$add2<-gsub(",","",linkd253$add2)

linkd253[postcode.y=="TQ2 5PE",add2:=gsub("WALDON HALL", "WALDON HALL APARTMENTS",add2)]

linkd253[postcode.y=="TQ14 8TL",add2:=gsub("LLANSTEPHEN", "LLANSTEPHAN",add2)]
linkd253[postcode.y=="YO30 4XE",add2:=gsub("ST. CHRISTOPHERS HOUSE", "ST CHRISTOPHER HOUSE",add2)]
linkd253[postcode.y=="GU26 6RA",add2:=gsub("MANORMEAD SUPPORTED HOUSING", "MANORMEAD",add2)]

linkd253[postcode.y=="TQ2 6HJ",add2:=gsub("JAMES HOUSE", "JAMES HOUSE BRIDGE VIEW",add2)]
linkd253[postcode.y=="NE1 6PN",add2:=gsub("ACACIA", "ACACIA HOTEL",add2)]


linkd253_2<-linkd253[linkd253$add2==linkd253$paotext,]



linkd253_2<-uniqueresult(linkd253_2)
dim(linkd)
linkd<-matchleft(linkd,linkd253_2)
dim(linkd)

##add2=paotext last three (remove the first one)

linkd253<-linkd[linkd$method=="link253d",] 
linkd253$add2<-gsub(",","",linkd253$add2)
linkd253_3<-linkd253[linkd253$add2==str_remove(linkd253$paotext, '(\\w+\\s+){1}'),]



linkd253_3<-uniqueresult(linkd253_3)
dim(linkd)
linkd<-matchleft(linkd,linkd253_3)
dim(linkd)

c2<- linkd[linkd$method=="link253d",]


c2<-c2[grepl("\\d",c2$add2),]

fail_8<-c2

linkd<-matchleft(linkd,fail_8)
dim(linkd)
c2<- linkd[linkd$method=="link253d",]

noadd2_1<-c2

linkd<-matchleft(linkd,noadd2_1)
dim(linkd)

####################clean 254############
c2<- linkd[linkd$method=="link254d",]

c2<-c2[grepl("\\d",c2$add2),]




##FLAT-2 (FIRST FLOOR)
#add2 first two=saotext


linkd254<-linkd[linkd$method=="link254d",] 
linkd254$add2<-gsub(",","",linkd254$add2)
linkd254$add2<-gsub("-"," ",linkd254$add2)
linkd254_1<-linkd254[word(linkd254$add2, 1,2)==linkd254$saotext,]



linkd254_1<-uniqueresult(linkd254_1)
dim(linkd)
linkd<-matchleft(linkd,linkd254_1)
dim(linkd)

c2<- linkd[linkd$method=="link254d",]

c2<-c2[grepl("\\d",c2$add2),]





c2<- linkd[linkd$method=="link254d",]


linkd254<-linkd[linkd$method=="link254d",] 
linkd254$add2<-gsub(",","",linkd254$add2)
linkd254$add2<-gsub("[(]","",linkd254$add2)
linkd254$add2<-gsub("[])]","",linkd254$add2)
linkd254_2<-linkd254[word(linkd254$add2,-2, -1)==linkd254$saotext,]



linkd254_2<-uniqueresult(linkd254_2)
dim(linkd)
linkd<-matchleft(linkd,linkd254_2)
dim(linkd)

c2<- linkd[linkd$method=="link254d",]

c2<-c2[grepl("\\d",c2$add2),]



noflat_1<-c2
linkd<-matchleft(linkd,noflat_1)
dim(linkd)

c2<- linkd[linkd$method=="link254d",]

linkd254_parent<-c2
linkd<-matchleft(linkd,linkd254_parent)
dim(linkd)


####################clean 255############
length(unique(linkd$method))
c2<- linkd[linkd$method=="link255d",]


##match based on add2


linkd255<-linkd[linkd$method=="link255d",] 
linkd255$add2<-gsub(",","",linkd255$add2)
linkd255_1<-linkd255[word(linkd255$add2, 1)==linkd255$buildingnumber,]



linkd255_1<-uniqueresult(linkd255_1)
dim(linkd)
linkd<-matchleft(linkd,linkd255_1)
dim(linkd)


c2<- linkd[linkd$method=="link255d",]
c2<-c2[grepl("\\d",c2$add2),]


noadd2_2<-c2
linkd<-matchleft(linkd,noadd2_2)
dim(linkd)
#67448    34

c2<- linkd[linkd$method=="link255d",]
#c2<-c2[grepl("\\d",c2$add2),]



linkd255_parent<-c2
linkd<-matchleft(linkd,c2)
dim(linkd)
####################clean 256############
c2<- linkd[linkd$method=="link256d",]

linkd256_parent<-c2
linkd<-matchleft(linkd,c2)
dim(linkd)

####################clean 260############
c2<- linkd[linkd$method=="link260d",]



detail_16<-c2
linkd<-matchleft(linkd,c2)
dim(linkd)

####################clean 264############
c2<- linkd[linkd$method=="link264d",]


linkd264_parent<-c2
linkd<-matchleft(linkd,c2)
dim(linkd)


####################clean 265############
c2<- linkd[linkd$method=="link265d",]


detail_17<-c2
linkd<-matchleft(linkd,c2)
dim(linkd)


###############271############
c2<- linkd[linkd$method=="link271d",]


##add2


linkd271<-linkd[linkd$method=="link271d",] 

linkd271<-linkd271[grepl("\\d",linkd271$add2),]
linkd271$add2<-gsub(",","",linkd271$add2)
linkd271_1<-linkd271[word(linkd271$add2, 1)==linkd271$saostartnumber,]



linkd271_1<-uniqueresult(linkd271_1)
dim(linkd)
linkd<-matchleft(linkd,linkd271_1)
dim(linkd)

#linkd<- linkd[linkd$method=="link271d",]


linkd271<-linkd[linkd$method=="link271d",] 
linkd271<-linkd271[grepl("\\d",linkd271$add2),]
linkd271$add2<-gsub(",","",linkd271$add2)
linkd271_2<-linkd271[word(linkd271$add2, 1)==linkd271$buildingnumber,]



linkd271_2<-uniqueresult(linkd271_2)
dim(linkd)
linkd<-matchleft(linkd,linkd271_2)
dim(linkd)
#54194    34

linkd271<-linkd[linkd$method=="link271d",] 
linkd271<-linkd271[grepl("\\d",linkd271$add2),]


linkd271$add2<-gsub(",","",linkd271$add2)
linkd271_3<-linkd271[word(linkd271$add2, 1)==linkd271$paostartnumber,]



linkd271_3<-uniqueresult(linkd271_3)
dim(linkd)
linkd<-matchleft(linkd,linkd271_3)
dim(linkd)
#54187    34

linkd271<-linkd[linkd$method=="link271d",] 
linkd271<-linkd271[grepl("\\d",linkd271$add2),]
detail_18<-linkd271
linkd<-matchleft(linkd,detail_18)
linkd271<-linkd[linkd$method=="link271d",] 


detail_19<-linkd271
linkd<-matchleft(linkd,detail_19)
dim(linkd)

####################clean 272############
c2<- linkd[linkd$method=="link272d",]



linkd272<-linkd[linkd$method=="link272d",] 
linkd272$add2<-gsub(",","",linkd272$add2)
linkd272_1<-linkd272[word(linkd272$add2, 1)==linkd272$paostartnumber,]



linkd272_1<-uniqueresult(linkd272_1)
dim(linkd)
linkd<-matchleft(linkd,linkd272_1)
dim(linkd)
c2<- linkd[linkd$method=="link272d",]

detail_20<-c2
linkd<-matchleft(linkd,detail_20)
dim(linkd)



####################clean 273############
c2<- linkd[linkd$method=="link273d",]

noadd2_3<-c2

linkd<-matchleft(linkd,c2)

####################clean 274############
c2<- linkd[linkd$method=="link274d",]




noadd2_4<-c2


linkd<-matchleft(linkd,noadd2_4)
dim(linkd)

####################clean 281############
c2<- linkd[linkd$method=="link281d",]



c2$add1c<-str_remove(c2$add1, '(\\w+\\s+){2}')




linkd281<-linkd[linkd$method=="link281d",] 

linkd281<-linkd281[grepl("\\d",linkd281$add2),]

#linkd281$add1c<-str_remove(linkd281$add1, '(\\w+\\s+){2}')

linkd281_1<-linkd281[word(linkd281$add2, 1)==linkd281$saostartnumber,]



linkd281_1<-uniqueresult(linkd281_1)
dim(linkd)
linkd<-matchleft(linkd,linkd281_1)
dim(linkd)
#50969    34
c2<- linkd[linkd$method=="link281d",]
#linkd281<-linkd[linkd$method=="link281d",] 




linkd281<-linkd[linkd$method=="link281d",] 
linkd281$add2<-gsub(",","",linkd281$add2)
linkd281_2<-linkd281[word(linkd281$add2, 1)==linkd281$paostartnumber,]



linkd281_2<-uniqueresult(linkd281_2)
dim(linkd)
linkd<-matchleft(linkd,linkd281_2)
dim(linkd)

c2<- linkd[linkd$method=="link281d",]



linkd281<-linkd[linkd$method=="link281d",] 

linkd281$add2<-gsub(",","",linkd281$add2)
linkd281_3<-linkd281[word(linkd281$add2, 1)==paste(linkd281$saostartnumber,linkd281$saoendnumber,sep="-"),]



linkd281_3<-uniqueresult(linkd281_3)
dim(linkd)
linkd<-matchleft(linkd,linkd281_3)
dim(linkd)


#linkd281<-linkd[linkd$method=="link281d",] 
linkd281<-linkd[linkd$method=="link281d",] 

linkd281<-linkd281[grepl("\\d",linkd281$add2),]

linkd281$add2<-gsub(",","",linkd281$add2)
linkd281_4<-linkd281[word(linkd281$add2, 1)==linkd281$saostartnumber,]



linkd281_4<-uniqueresult(linkd281_4)
dim(linkd)
linkd<-matchleft(linkd,linkd281_4)
dim(linkd)

linkd281<-linkd[linkd$method=="link281d",] 

linkd281<-linkd281[grepl("\\d",linkd281$add2),]



linkd281$add2<-gsub(",","",linkd281$add2)
linkd281_5<-linkd281[word(linkd281$add2, 1)==paste(linkd281$saostartnumber,linkd281$saostartsuffix,sep=""),]



linkd281_5<-uniqueresult(linkd281_5)
dim(linkd)
linkd<-matchleft(linkd,linkd281_5)
dim(linkd)

linkd281<-linkd[linkd$method=="link281d",] 



linkd281$add2<-gsub(",","",linkd281$add2)
linkd281_6<-linkd281[word(linkd281$add2, 1)==paste(linkd281$paostartnumber,linkd281$paoendnumber,sep="-"),]



linkd281_6<-uniqueresult(linkd281_6)
dim(linkd)
linkd<-matchleft(linkd,linkd281_6)
dim(linkd)

linkd281<-linkd[linkd$method=="link281d",] 

# linkd281<-linkd281[grepl("\\d",linkd281$add2),]
# linkd281[postcode.y=="HG1 2JA",add1:=gsub("GRANDVILLE HOUSE", "GRANDVILLE HOUSE",add1)]
# linkd281$add1c<-str_remove(linkd281$add1, '(\\w+\\s+){2}')
# 
# linkd281$add1c<-gsub(",","",linkd281$add1c)
# 
# 


linkd281_7<-linkd281[linkd281$add2==linkd281$paotext,]



linkd281_7<-uniqueresult(linkd281_7)
dim(linkd)
linkd<-matchleft(linkd,linkd281_7)
dim(linkd)
#49756    34
linkd281<-linkd[linkd$method=="link281d",] 

linkd281<-linkd281[grepl("\\d",linkd281$add2),]


linkd281$add1<-gsub(",","",linkd281$add1)
linkd281$add1c<-str_remove(linkd281$add1, '(\\w+\\s+){2}')

linkd281_8<-linkd281[linkd281$add1c==paste(linkd281$saostartnumber,linkd281$paotext,sep=" "),]



linkd281_8<-uniqueresult(linkd281_8)
dim(linkd)
linkd<-matchleft(linkd,linkd281_8)
dim(linkd)

linkd281<-linkd[linkd$method=="link281d",] 


#linkd281<-linkd281[grepl("\\d",linkd281$add2),]



linkd281$add2<-gsub(",","",linkd281$add2)
linkd281_9<-linkd281[word(linkd281$add2, 1)==paste(linkd281$paostartnumber,linkd281$paostartsuffix,sep=""),]



linkd281_9<-uniqueresult(linkd281_9)
dim(linkd)
linkd<-matchleft(linkd,linkd281_9)
dim(linkd)


linkd281<-linkd[linkd$method=="link281d",] 



linkd281$add2<-gsub(",","",linkd281$add2)
linkd281_10<-linkd281[word(linkd281$add2, 1,2)==paste("BLOCK",linkd281$saostartnumber,sep=" "),]



linkd281_10<-uniqueresult(linkd281_10)
dim(linkd)
linkd<-matchleft(linkd,linkd281_10)
dim(linkd)

linkd281<-linkd[linkd$method=="link281d",] 




linkd281$add2<-gsub(",","",linkd281$add2)
linkd281_11<-linkd281[word(linkd281$add2,2)==linkd281$paostartnumber,]



linkd281_11<-uniqueresult(linkd281_11)
dim(linkd)
linkd<-matchleft(linkd,linkd281_11)
dim(linkd)

linkd281<-linkd[linkd$method=="link281d",] 

linkd281<-linkd281[grepl("\\d",linkd281$add2),]

noadd2_5<-linkd281
linkd<-matchleft(linkd,noadd2_5)
dim(linkd)
linkd281<-linkd[linkd$method=="link281d",] 
linkd281<-linkd281[grepl("\\d",linkd281$add3),]

linkd281$add3<-gsub(",","",linkd281$add3)
linkd281_12<-linkd281[word(linkd281$add3, 1)==word(linkd281$paotext, -1),]



linkd281_12<-uniqueresult(linkd281_12)
dim(linkd)
linkd<-matchleft(linkd,linkd281_11)
dim(linkd)
# 42520    34
linkd281<-linkd[linkd$method=="link281d",] 

linkd281<-linkd281[grepl("\\d",linkd281$add3),]
linkd281$add3<-gsub(",","",linkd281$add3)

linkd281_13<-linkd281[word(linkd281$add3, 1)==linkd281$paostartnumber,]



linkd281_13<-uniqueresult(linkd281_13)
dim(linkd)
linkd<-matchleft(linkd,linkd281_13)
dim(linkd)

linkd281<-linkd[linkd$method=="link281d",] 

linkd281<-linkd281[grepl("\\d",linkd281$add3),]
linkd281$add3<-gsub(",","",linkd281$add3)

linkd281_14<-linkd281[word(linkd281$add3, 1)==linkd281$saostartnumber,]



linkd281_14<-uniqueresult(linkd281_14)
dim(linkd)
linkd<-matchleft(linkd,linkd281_14)
dim(linkd)



linkd281<-linkd[linkd$method=="link281d",] 

linkd281<-linkd281[grepl("\\d",linkd281$add3),]
linkd281$add3<-gsub(",","",linkd281$add3)

linkd281_15<-linkd281[word(linkd281$add3, 1)==paste(linkd281$paostartnumber,linkd281$paoendnumber,sep="-"),]



linkd281_15<-uniqueresult(linkd281_15)
dim(linkd)
linkd<-matchleft(linkd,linkd281_15)
dim(linkd)



linkd281<-linkd[linkd$method=="link281d",] 

linkd281<-linkd281[grepl("\\d",linkd281$add3),]
linkd281$add3<-gsub(",","",linkd281$add3)

linkd281_16<-linkd281[word(linkd281$add3, 1)==paste(linkd281$saostartnumber,linkd281$saoendnumber,sep="-"),]



linkd281_16<-uniqueresult(linkd281_16)
dim(linkd)
linkd<-matchleft(linkd,linkd281_16)
dim(linkd)

linkd281<-linkd[linkd$method=="link281d",] 

linkd281<-linkd281[grepl("\\d",linkd281$add3),]
linkd281$add1<-gsub(",","",linkd281$add1)

linkd281$add1c<-str_remove(linkd281$add1, '(\\w+\\s+){2}')

linkd281_17<-linkd281[linkd281$add1c==paste("BLOCK",linkd281$saostartnumber,sep=" "),]



linkd281_17<-uniqueresult(linkd281_17)
dim(linkd)
linkd<-matchleft(linkd,linkd281_17)
dim(linkd)
#42232    34

linkd281<-linkd[linkd$method=="link281d",] 

linkd281<-linkd281[grepl("\\d",linkd281$add3),]

noadd2_6<-linkd281
dim(linkd)
linkd<-matchleft(linkd,noadd2_6)
dim(linkd)


linkd281<-linkd[linkd$method=="link281d",] 
linkd281$add1<-gsub(",","",linkd281$add1)

linkd281$add1c<-str_remove(linkd281$add1, '(\\w+\\s+){2}')

linkd281_18<-linkd281[word(linkd281$add1c, 1)==linkd281$buildingnumber,]



linkd281_18<-uniqueresult(linkd281_18)
dim(linkd)
linkd<-matchleft(linkd,linkd281_18)
dim(linkd)




linkd281<-linkd[linkd$method=="link281d",] 
linkd281$add1<-gsub(",","",linkd281$add1)

linkd281$add1c<-str_remove(linkd281$add1, '(\\w+\\s+){2}')

linkd281_19<-linkd281[word(linkd281$add1c, 1)==paste(linkd281$saostartnumber,linkd281$saostartsuffix,sep=""),]



linkd281_19<-uniqueresult(linkd281_19)
dim(linkd)
linkd<-matchleft(linkd,linkd281_19)
dim(linkd)



linkd281<-linkd[linkd$method=="link281d",] 
linkd281$add1<-gsub(",","",linkd281$add1)

linkd281$add1c<-str_remove(linkd281$add1, '(\\w+\\s+){2}')

linkd281_20<-linkd281[word(linkd281$add1c, 1,2)==word(linkd281$paotext,1,2),]



linkd281_20<-uniqueresult(linkd281_20)
dim(linkd)
linkd<-matchleft(linkd,linkd281_20)
dim(linkd)
#41106    34
linkd281<-linkd[linkd$method=="link281d",] 
linkd281$add1<-gsub(",","",linkd281$add1)

linkd281$add1c<-str_remove(linkd281$add1, '(\\w+\\s+){2}')

linkd281_21<-linkd281[word(linkd281$add1c, 1,2)==word(linkd281$paotext,2,3),]



linkd281_21<-uniqueresult(linkd281_21)
dim(linkd)
linkd<-matchleft(linkd,linkd281_21)
dim(linkd)
# 41065    34

linkd281<-linkd[linkd$method=="link281d",] 
linkd281$add1<-gsub(",","",linkd281$add1)

linkd281$add1c<-str_remove(linkd281$add1, '(\\w+\\s+){2}')

linkd281_22<-linkd281[word(linkd281$add1c, 1)==paste(linkd281$paostartnumber,linkd281$paoendnumber,sep="-"),]



linkd281_22<-uniqueresult(linkd281_22)
dim(linkd)
linkd<-matchleft(linkd,linkd281_22)
dim(linkd)


linkd281<-linkd[linkd$method=="link281d",] 
linkd281$add1<-gsub(",","",linkd281$add1)
linkd281[,add1:=gsub("LLOYDS TSB BANK PLC", "LLOYDS BANK PLC",add1)]
linkd281[,add1:=gsub("ELLIOT HOUSE", "ELIOT HOUSE",add1)]
linkd281[,add1:=gsub("BUXTON COURT", "BUXTON HOUSE",add1)]
linkd281$add1c<-str_remove(linkd281$add1, '(\\w+\\s+){2}')

linkd281_23<-linkd281[word(linkd281$add1c, 1,2)==word(linkd281$paotext,1,2),]



linkd281_23<-uniqueresult(linkd281_23)
dim(linkd)
linkd<-matchleft(linkd,linkd281_23)
dim(linkd)


linkd281<-linkd[linkd$method=="link281d",] 
linkd281$add1<-gsub(",","",linkd281$add1)

linkd281$add1c<-str_remove(linkd281$add1, '(\\w+\\s+){2}')

linkd281_24<-linkd281[word(linkd281$add1c, 1)==word(linkd281$paotext,1),]



linkd281_24<-uniqueresult(linkd281_24)
dim(linkd)
linkd<-matchleft(linkd,linkd281_24)
dim(linkd)
#40896    34

linkd281<-linkd[linkd$method=="link281d",]

noadd2_7<-linkd281
linkd<-matchleft(linkd,noadd2_7)
dim(linkd)


####################clean 284############
c2<- linkd[linkd$method=="link284d",]



linkd284_parent<-c2

linkd<-matchleft(linkd,linkd284_parent)
dim(linkd)

####################clean 286############
c2<- linkd[linkd$method=="link286d",]

detail_21<-c2
linkd<-matchleft(linkd,c2)
dim(linkd)

#####check the one to one part############################
length(unique(linkd$method))
####################clean 288############
c2<- linkd[linkd$method=="link288d",]




linkd288<-linkd[linkd$method=="link288d",] 

#linkd288<-linkd288[grepl("\\d",linkd288$add2),]

linkd288$add1c<-str_remove(linkd288$add1, '(\\w+\\s+){1}')

linkd288_1<-linkd288[linkd288$add1c==paste(linkd288$saotext,linkd288$streetdescription,sep=" "),]



linkd288_1<-uniqueresult(linkd288_1)
dim(linkd)
linkd<-matchleft(linkd,linkd288_1)
dim(linkd)
#39189    34
linkd288<-linkd[linkd$method=="link288d",] 
linkd288$add1<-gsub(",","",linkd288$add1)
linkd288$add1c<-str_remove(linkd288$add1, '(\\w+\\s+){1}')

linkd288_2<-linkd288[linkd288$add1c==paste(linkd288$subbuildingname,linkd288$saotext,sep=" "),]
                                           

linkd288_2<-uniqueresult(linkd288_2)
dim(linkd)
linkd<-matchleft(linkd,linkd288_2)
dim(linkd)
#39149    34



linkd288_parent<-c2[grepl("\\d",c2$saotext),]

linkd288_parent1<-keepneed(linkd,linkd288_parent)
rm(linkd288_parent)

dim(linkd)
linkd<-matchleft(linkd,linkd288_parent1)
c2<- linkd[linkd$method=="link288d",]



linkd288<-linkd[linkd$method=="link288d",] 
linkd288$add1<-gsub(",","",linkd288$add1)
linkd288$add1c<-str_remove(linkd288$add1, '(\\w+\\s+){1}')

linkd288_3<-linkd288[word(linkd288$add1c,1)==paste(linkd288$paostartnumber,linkd288$paoendnumber,sep="-"),]
linkd288_3<-linkd288_3[word(linkd288_3$add1,1)==linkd288_3$saostartnumber,]

linkd288_3<-uniqueresult(linkd288_3)
dim(linkd)
linkd<-matchleft(linkd,linkd288_3)
dim(linkd)
#31114    34


linkd288<-linkd[linkd$method=="link288d",] 
linkd288$add1<-gsub(",","",linkd288$add1)
linkd288$add1c<-str_remove(linkd288$add1, '(\\w+\\s+){1}')

linkd288_4<-linkd288[word(linkd288$add1c,1)==word(linkd288$paotext,1),]
#linkd288_3<-linkd288_3[word(linkd288_3$add1,1)==linkd288_3$saostartnumber,]

linkd288_4<-uniqueresult(linkd288_4)
dim(linkd)
linkd<-matchleft(linkd,linkd288_4)
dim(linkd)
#31114    34

linkd288<-linkd[linkd$method=="link288d",] 


linkd288$add1<-gsub(",","",linkd288$add1)
#linkd288$add1c<-str_remove(linkd288$add1, '(\\w+\\s+){1}')

linkd288_5<-linkd288[word(linkd288$add1,1)==paste(linkd288$saostartnumber,linkd288$saostartsuffix,sep=""),]
#linkd288_3<-linkd288_3[word(linkd288_3$add1,1)==linkd288_3$saostartnumber,]

linkd288_5<-uniqueresult(linkd288_5)
dim(linkd)
linkd<-matchleft(linkd,linkd288_5)
dim(linkd)

linkd288<-linkd[linkd$method=="link288d",] 
linkd288$add1<-gsub(",","",linkd288$add1)
linkd288$add1c<-str_remove(linkd288$add1, '(\\w+\\s+){1}')
linkd288$add1c<-gsub("CAE Y NANT","CAE-Y-NANT",linkd288$add1c)
linkd288$add1c<-gsub("HAZELDEAN COURT","HAZELDENE COURT",linkd288$add1c)
linkd288_6<-linkd288[word(linkd288$add1c,1)==word(linkd288$streetdescription,1),]

linkd288_6<-uniqueresult(linkd288_6)
dim(linkd)
linkd<-matchleft(linkd,linkd288_6)
dim(linkd)



linkd288_7<-linkd[linkd$method=="link288d" & substr(linkd$class,1,1)=="R" ,]
linkd288_7<-uniqueresult(linkd288_7)

linkd<-matchleft(linkd,linkd288_7)
dim(linkd)



linkd288<-linkd[linkd$method=="link288d",] 
linkd288$add1<-gsub(",","",linkd288$add1)
linkd288$add1c<-str_remove(linkd288$add1, '(\\w+\\s+){1}')


linkd288_8<-linkd288[word(linkd288$add1c,1)==word(linkd288$saotext,-1),]

linkd288_8<-uniqueresult(linkd288_8)
dim(linkd)
linkd<-matchleft(linkd,linkd288_8)
dim(linkd)


linkd288<-linkd[linkd$method=="link288d",] 
linkd288$add1<-gsub(",","",linkd288$add1)
linkd288$add1c<-str_remove(linkd288$add1, '(\\w+\\s+){1}')

linkd288_9<-linkd288[word(linkd288$add1c,1,2)==word(linkd288$saotext,-2,-1),]
linkd288_9<-uniqueresult(linkd288_9)
dim(linkd)
linkd<-matchleft(linkd,linkd288_9)
dim(linkd)


linkd288<-linkd[linkd$method=="link288d",] 

linkd288$add1<-gsub(",","",linkd288$add1)
linkd288$add1c<-str_remove(linkd288$add1, '(\\w+\\s+){1}')

linkd288_10<-linkd288[word(linkd288$add1c,1)==word(linkd288$saotext,1),]
linkd288_10<-uniqueresult(linkd288_10)
dim(linkd)
linkd<-matchleft(linkd,linkd288_10)
dim(linkd)


linkd288<-linkd[linkd$method=="link288d",] 

fail_9<-linkd288[grepl("\\d",linkd288$add3),]

linkd<-matchleft(linkd,fail_9)

linkd288<-linkd[linkd$method=="link288d",] 

fail_10<-linkd288[grepl("\\d",linkd288$add2),]

linkd<-matchleft(linkd,fail_10)

linkd288<-linkd[linkd$method=="link288d",] 


linkd<-matchleft(linkd,linkd288)
dim(linkd)
linkd288_parent<-linkd288_parent1
rm(linkd288_parent1)
###############289############
length(unique(linkd$method))
#76


linkd289<-linkd[linkd$method=="link289d",] 



linkd289$add1c<-str_remove(linkd289$add1, '(\\w+\\s+){1}')

linkd289$add1c<-gsub("CHEPLING HOUSE","CHEPING HOUSE",linkd289$add1c)
linkd289_1<-linkd289[word(linkd289$add1c,1,2)==linkd289$buildingname,]



linkd289_1<-uniqueresult(linkd289_1)
dim(linkd)
linkd<-matchleft(linkd,linkd289_1)
dim(linkd)
#28140

linkd289<-linkd[linkd$method=="link289d",] 
linkd289$add1<-gsub(",","",linkd289$add1)
#linkd289$add1c<-str_remove(linkd289$add1, '(\\w+\\s+){1}')

linkd289_2<-linkd289[word(linkd289$add1,1,3)==linkd289$saotext,]


linkd289_2<-uniqueresult(linkd289_2)
dim(linkd)
linkd<-matchleft(linkd,linkd289_2)
dim(linkd)
#28070    34





linkd289<-linkd[linkd$method=="link289d",] 
#linkd289_3<-linkd289[word(linkd289$add1c,1)==paste(linkd289$paostartnumber,linkd289$paoendnumber,sep="-"),]
linkd289_3<-linkd289[linkd289$add2==paste(linkd289$paostartnumber,linkd289$streetdescription,sep=" "),]

linkd289_3<-uniqueresult(linkd289_3)
dim(linkd)
linkd<-matchleft(linkd,linkd289_3)
dim(linkd)




linkd289<-linkd[linkd$method=="link289d",] 
linkd289$add1<-gsub("CLARKES MEWS","CLARKS MEWS",linkd289$add1)
linkd289$add1<-gsub("PORTER MEWS","PORTER HOUSE",linkd289$add1)
linkd289_4<-linkd289[linkd289$add1==paste(linkd289$saostartnumber,linkd289$paotext,sep=" "),]
#linkd289_3<-linkd289_3[word(linkd289_3$add1,1)==linkd289_3$saostartnumber,]

linkd289_4<-uniqueresult(linkd289_4)
dim(linkd)
linkd<-matchleft(linkd,linkd289_4)
dim(linkd)
c2<- linkd[linkd$method=="link289d",]


noadd2_8<-c2
linkd<-matchleft(linkd,noadd2_8)
dim(linkd)

####################clean 290############
c2<- linkd[linkd$method=="link290d",]


linkd290_parent<-c2
linkd<-matchleft(linkd,linkd290_parent)
dim(linkd)
####################clean 291############


linkd291<-linkd[linkd$method=="link291d",] 

linkd291<-linkd291[grepl("\\d",linkd291$add2),]

linkd291$add2<-gsub(",","",linkd291$add2)
linkd291_1<-linkd291[word(linkd291$add2,1)==paste(linkd291$paostartnumber,linkd291$paostartsuffix,sep=""),]



linkd291_1<-uniqueresult(linkd291_1)
dim(linkd)
linkd<-matchleft(linkd,linkd291_1)




linkd291<-linkd[linkd$method=="link291d",] 
linkd291$add1<-gsub(",","",linkd291$add1)
linkd291$add1c<-str_remove(linkd291$add1, '(\\w+\\s+){1}')

linkd291_2<-linkd291[word(linkd291$add1c,1)==linkd291$buildingnumber,]


linkd291_2<-uniqueresult(linkd291_2)
dim(linkd)
linkd<-matchleft(linkd,linkd291_2)
dim(linkd)


linkd291<-linkd[linkd$method=="link291d",] 
linkd291$add1<-gsub(",","",linkd291$add1)
linkd291$add1c<-str_remove(linkd291$add1, '(\\w+\\s+){1}')

linkd291_3<-linkd291[word(linkd291$add1c,1)==paste(linkd291$paostartnumber,linkd291$paoendnumber,sep="-"),]
#linkd291_3<-linkd291_3[word(linkd291_3$add1,1)==linkd291_3$saostartnumber,]

linkd291_3<-uniqueresult(linkd291_3)
dim(linkd)
linkd<-matchleft(linkd,linkd291_3)
dim(linkd)



linkd291<-linkd[linkd$method=="link291d",] 
linkd291$add1<-gsub(",","",linkd291$add1)
linkd291$add1c<-str_remove(linkd291$add1, '(\\w+\\s+){1}')

linkd291_4<-linkd291[word(linkd291$add1c,1)==linkd291$paostartnumber,]
#linkd291_3<-linkd291_3[word(linkd291_3$add1,1)==linkd291_3$saostartnumber,]

linkd291_4<-uniqueresult(linkd291_4)
dim(linkd)
linkd<-matchleft(linkd,linkd291_4)
dim(linkd)

linkd291<-linkd[linkd$method=="link291d",] 
linkd291$add1<-gsub(",","",linkd291$add1)
linkd291$add1c<-str_remove(linkd291$add1, '(\\w+\\s+){1}')


linkd291_5<-linkd291[word(linkd291$add1c,1)==word(linkd291$paotext,2),]

linkd291_5<-uniqueresult(linkd291_5)
dim(linkd)
linkd<-matchleft(linkd,linkd291_5)
dim(linkd)


linkd291<-linkd[linkd$method=="link291d",] 
linkd291$add1<-gsub(",","",linkd291$add1)
linkd291$add1c<-str_remove(linkd291$add1, '(\\w+\\s+){1}')
linkd291_6<-linkd291[word(linkd291$add1c,1)==word(linkd291$paotext,1),]

linkd291_6<-uniqueresult(linkd291_6)
dim(linkd)
linkd<-matchleft(linkd,linkd291_6)
dim(linkd)
#25886    34



linkd291<-linkd[linkd$method=="link291d",] 
linkd291$add2<-gsub(",","",linkd291$add2)

linkd291<-linkd291[grepl("\\d",linkd291$add2),]


linkd291_7<-linkd291[word(linkd291$add2,1)==word(linkd291$paotext,2),]

linkd291_7<-uniqueresult(linkd291_7)
dim(linkd)
linkd<-matchleft(linkd,linkd291_7)
dim(linkd)
#25878    34

linkd291<-linkd[linkd$method=="link291d",] 
linkd291$add1<-gsub(",","",linkd291$add1)
linkd291$add1c<-str_remove(linkd291$add1, '(\\w+\\s+){1}')

#MANOR HOUSE

linkd291_8<-linkd291[linkd291$add1c==linkd291$paotext,]

linkd291_parent<-linkd291_8
rm(linkd291_8)

linkd<-matchleft(linkd,linkd291_parent)
dim(linkd)


linkd291<-linkd[linkd$method=="link291d",] 
linkd291$add1<-gsub(",","",linkd291$add1)
linkd291$add1c<-str_remove(linkd291$add1, '(\\w+\\s+){1}')


linkd291_8<-linkd291[word(linkd291$add1c,1)==word(linkd291$paotext,1),]

linkd291_8<-uniqueresult(linkd291_8)

linkd<-matchleft(linkd,linkd291_parent)
dim(linkd)

linkd291<-linkd[linkd$method=="link291d",] 
linkd291$add1<-gsub(",","",linkd291$add1)
linkd291$add1c<-str_remove(linkd291$add1, '(\\w+\\s+){1}')
linkd291$add1c<-gsub("MANOR HOUSE","THE MANOR HOUSE",linkd291$add1c)

linkd291_9<-linkd291[linkd291$add1c==linkd291$paotext,]

linkd291_9<-uniqueresult(linkd291_9)

linkd<-matchleft(linkd,linkd291_9)
dim(linkd)
# 25829    34


linkd291<-linkd[linkd$method=="link291d",] 
linkd291$add1<-gsub(",","",linkd291$add1)
linkd291$add1c<-str_remove(linkd291$add1, '(\\w+\\s+){1}')
#linkd291$add1c<-gsub("MANOR HOUSE","THE MANOR HOUSE",linkd291$add1c)

linkd291_10<-linkd291[word(linkd291$add1c,1,2)==word(linkd291$paotext,1,2),]

linkd291_10<-uniqueresult(linkd291_10)

linkd<-matchleft(linkd,linkd291_10)
dim(linkd)
# 25395    34



c2<- linkd[linkd$method=="link291d",]


fail_11<-c2
linkd<-matchleft(linkd,fail_11)
dim(linkd)

###############292############



linkd292<-linkd[linkd$method=="link292d",] 

linkd292$add2<-gsub(",","",linkd292$add2)

#linkd292$add1c<-str_remove(linkd292$add1, '(\\w+\\s+){1}')

linkd292_1<-linkd292[linkd292$add2==linkd292$paotext,]


linkd292_1<-uniqueresult(linkd292_1)
dim(linkd)
linkd<-matchleft(linkd,linkd292_1)
dim(linkd)

linkd292<-linkd[linkd$method=="link292d",] 

linkd292$add2<-gsub(",","",linkd292$add2)

linkd292_2<-linkd292[word(linkd292$add2,1,2)==word(linkd292$paotext,1,2),]


linkd292_2<-uniqueresult(linkd292_2)
dim(linkd)
linkd<-matchleft(linkd,linkd292_2)
dim(linkd)

c2<- linkd[linkd$method=="link292d",]

noadd2_9<-c2

linkd<-matchleft(linkd,noadd2_9)
dim(linkd)



####################clean 293############
c2<- linkd[linkd$method=="link293d",]



linkd293<-linkd[linkd$method=="link293d",] 

#linkd293<-linkd293[grepl("\\d",linkd293$add2),]

#linkd293$add1c<-str_remove(linkd293$add1, '(\\w+\\s+){1}')

linkd293_1<-linkd293[word(linkd293$add2,1)==linkd293$paostartnumber,]
linkd293_1<-uniqueresult(linkd293_1)
dim(linkd)
linkd<-matchleft(linkd,linkd293_1)
dim(linkd)
#22042    34


linkd293<-linkd[linkd$method=="link293d",] 
linkd293$add1<-gsub(",","",linkd293$add1)
linkd293$add1c<-str_remove(linkd293$add1, '(\\w+\\s+){2}')

linkd293_2<-linkd293[word(linkd293$add1c,1,2)==word(linkd293$paotext,1,2),]


linkd293_2<-uniqueresult(linkd293_2)
dim(linkd)
linkd<-matchleft(linkd,linkd293_2)
dim(linkd)
# 21988    34
linkd293<-linkd[linkd$method=="link293d",]
linkd293_3<-linkd293[linkd293$add2==linkd293$paotext,]
# 21988    34

linkd293_3<-uniqueresult(linkd293_3)

linkd<-matchleft(linkd,linkd293_3)

linkd293<-linkd[linkd$method=="link293d",] 
linkd293$add2<-gsub(",","",linkd293$add2)
linkd293$pp<-paste(linkd293$paostartnumber,linkd293$paostartsuffix,sep="")
linkd293$pp<- str_trim(linkd293$pp)
linkd293_4<-linkd293[linkd293$add2==paste(linkd293$pp,linkd293$streetdescription,sep=" "),]
 
 
linkd293_4<-uniqueresult(linkd293_4)
dim(linkd)
linkd<-matchleft(linkd,linkd293_4)
dim(linkd)
#  21896    34

linkd293<-linkd[linkd$method=="link293d",]



linkd293<-linkd[linkd$method=="link293d",] 
linkd293$add2<-gsub(",","",linkd293$add2)

linkd293_parent<-linkd293[linkd293$add2==paste(linkd293$saostartnumber,linkd293$paotext,sep=" "),]



linkd<-matchleft(linkd,linkd293_parent)
dim(linkd)
linkd293<-linkd[linkd$method=="link293d",]



linkd293<-linkd[linkd$method=="link293d",] 
fail_12<-linkd293

linkd<-matchleft(linkd,fail_12)
dim(linkd)
####################clean 294############


linkd294_1<-linkd[linkd$method=="link294d" & substr(linkd$class,1,1)=="R" ,]
linkd294_1<-uniqueresult(linkd294_1)

linkd<-matchleft(linkd,linkd294_1)
dim(linkd)


linkd294<-linkd[linkd$method=="link294d",] 
linkd294$add2<-gsub(",","",linkd294$add2)

linkd294$ss<-paste(linkd294$saostartnumber,linkd294$saostartsuffix,sep="")

linkd294_2<-linkd294[word(linkd294$add2,1)==linkd294$ss,]



linkd294_2<-uniqueresult(linkd294_2)
dim(linkd)
linkd<-matchleft(linkd,linkd294_2)
dim(linkd)


linkd294<-linkd[linkd$method=="link294d",] 
inkd294$add2<-gsub(",","",linkd294$add2)

#linkd294$ss<-paste(linkd294$saostartnumber,linkd294$saostartsuffix,sep="")

linkd294_3<-linkd294[word(linkd294$add2,1)==linkd294$paostartnumber,]



linkd294_3<-uniqueresult(linkd294_3)
dim(linkd)
linkd<-matchleft(linkd,linkd294_3)
dim(linkd)
#21420    34

linkd294<-linkd[linkd$method=="link294d",] 

linkd294$add1<-gsub(",","",linkd294$add1)

linkd294$add1c<-str_remove(linkd294$add1, '(\\w+\\s+){1}')

linkd294_4<-linkd294[word(linkd294$add1c,1)==linkd294$saostartnumber,]

linkd294_4<-uniqueresult(linkd294_4)
dim(linkd)
linkd<-matchleft(linkd,linkd294_4)
dim(linkd)
# 21416    34

c2<- linkd[linkd$method=="link294d",]

fail_13<-c2

linkd<-matchleft(linkd,fail_13)
dim(linkd)
####################clean 295############


linkd295<-linkd[linkd$method=="link295d",] 

linkd295<-linkd295[grepl("\\d",linkd295$add2),]
linkd295 <- linkd295[grepl("\\d$",linkd295$add1),]
fail_14<-linkd295

linkd<-matchleft(linkd,fail_14)


linkd295_1<-linkd[linkd$method=="link295d" & substr(linkd$class,1,1)=="R" ,]
linkd295_1<-uniqueresult(linkd295_1)

linkd<-matchleft(linkd,linkd295_1)
dim(linkd)



linkd295<-linkd[linkd$method=="link295d",] 
linkd295$add3<-gsub(",","",linkd295$add3)
linkd295_2<-linkd295[word(linkd295$add3, 1)==linkd295$paostartnumber,]
linkd295_2<-uniqueresult(linkd295_2)

linkd<-matchleft(linkd,linkd295_2)
dim(linkd)


c2<- linkd[linkd$method=="link295d",]


detail_22<-c2

linkd<-matchleft(linkd,detail_22)
dim(linkd)
####################clean 296############
c2<- linkd[linkd$method=="link296d",]


linkd296_1<-linkd[linkd$method=="link296d" & substr(linkd$class,1,1)=="R" ,]
linkd296_1<-uniqueresult(linkd296_1)

linkd<-matchleft(linkd,linkd296_1)
dim(linkd)

c2<- linkd[linkd$method=="link296d",]

detail_23<-c2

linkd<-matchleft(linkd,detail_23)
####################clean 297############
c2<- linkd[linkd$method=="link297d",]


linkd297<-linkd[linkd$method=="link297d",]
linkd297<-linkd297[grepl("\\d",linkd297$add2),]
linkd297 <- linkd297[grepl("\\d$",linkd297$add1),]
linkd297<- linkd297[linkd297$paostartnumber=="",]
fail_15<-linkd297

c2<- linkd[linkd$method=="link297d",]


detail_24<-c2
  
linkd<-matchleft(linkd,detail_24)

###############299############
c2<- linkd[linkd$method=="link299d",]


detail_25<-c2
linkd<-matchleft(linkd,detail_25)

###############301############
c2<- linkd[linkd$method=="link301d",]


linkd301_1<-linkd[linkd$method=="link301d" & word(linkd$add2,1)==word(linkd$paotext,1) ,]
linkd301_1<-uniqueresult(linkd301_1)

linkd<-matchleft(linkd,linkd301_1)
dim(linkd)


c2<- linkd[linkd$method=="link301d",]
detail_26<-c2

linkd<-matchleft(linkd,detail_26)

###############303############
c2<- linkd[linkd$method=="link303d",]


linkd<-matchleft(linkd,c2)
dim(linkd)


###############304############
c2<- linkd[linkd$method=="link304d",]

###############305############
c2<- linkd[linkd$method=="link305d",]



linkd305<-linkd[linkd$method=="link305d",]
linkd305<-linkd305[grepl("\\d",linkd305$add2),]
linkd305 <- linkd305[grepl("\\d$",linkd305$add1),]
fail_15<-linkd305
linkd<-matchleft(linkd,fail_15)
dim(linkd)
c2<- linkd[linkd$method=="link305d",]


linkd305_parent<-c2

linkd<-matchleft(linkd,c2)


###############314############
c2<- linkd[linkd$method=="link314d",]

linkd314_parent<-c2
linkd<-matchleft(linkd,linkd314_parent)

###############318############
c2<- linkd[linkd$method=="link318d",]


linkd318_parent<-c2
linkd<-matchleft(linkd,linkd318_parent)
dim(linkd)


###############319############

linkd319<-linkd[linkd$method=="link319d",] 

linkd319$add2<-gsub("NO.","",linkd319$add2)
#linkd319$add2c<-str_remove(linkd319$add2, '(\\w+\\s+){1}')

#linkd319$add1c<-str_remove(linkd319$add1, '(\\w+\\s+){1}')

linkd319_1<-linkd319[word(linkd319$add2,1)==linkd319$paostartnumber,]



linkd319_1<-uniqueresult(linkd319_1)
dim(linkd)
linkd<-matchleft(linkd,linkd319_1)
dim(linkd)
#20906    34

linkd319<-linkd[linkd$method=="link319d",] 


#linkd319$add2c<-str_remove(linkd319$add2, '(\\w+\\s+){1}')

#linkd319$add1c<-str_remove(linkd319$add1, '(\\w+\\s+){1}')

linkd319_2<-linkd319[word(linkd319$add3,1)==linkd319$buildingnumber,]



linkd319_2<-uniqueresult(linkd319_2)

dim(linkd)
linkd<-matchleft(linkd,linkd319_2)

###############321############

length(unique(linkd$method))

linkd321<-linkd[linkd$method=="link321d",] 

linkd321<-linkd321[grepl("\\d",linkd321$add2),]
linkd321$add2<-gsub(",","",linkd321$add2)


linkd321_1<-linkd321[word(linkd321$add2,1)==linkd321$buildingnumber,]
linkd321_1<-uniqueresult(linkd321_1)
dim(linkd)
linkd<-matchleft(linkd,linkd321_1)
dim(linkd)
#20807    34
linkd321<-linkd[linkd$method=="link321d",] 

linkd321<-linkd321[grepl("\\d",linkd321$add2),]
 linkd321$add2<-gsub(",","",linkd321$add2)


 
linkd321_2<-linkd321[word(linkd321$add2,-1)==linkd321$buildingnumber,]

linkd321_2<-uniqueresult(linkd321_2)
dim(linkd)
linkd<-matchleft(linkd,linkd321_2)
dim(linkd)


linkd321<-linkd[linkd$method=="link321d",] 

linkd321<-linkd321[grepl("\\d",linkd321$add2),]
linkd321$add2<-gsub(",","",linkd321$add2)
noadd2_10<-linkd321
linkd<-matchleft(linkd,noadd2_10)
dim(linkd)

c2<- linkd[linkd$method=="link321d",]


linkd<-matchleft(linkd,c2)

###############322############
c2<- linkd[linkd$method=="link322d",]

noadd2_11<-c2
linkd<-matchleft(linkd,c2)
dim(linkd)
###############323############
c2<- linkd[linkd$method=="link323d",]


linkd323_parent<-c2

linkd<-matchleft(linkd,linkd323_parent)
dim(linkd)
###############324############
c2<- linkd[linkd$method=="link324d",]


linkd324_1<-linkd[linkd$method=="link324d" & substr(linkd$class,1,1)=="R" ,]
linkd324_1<-uniqueresult(linkd324_1)

linkd<-matchleft(linkd,linkd324_1)

c2<- linkd[linkd$method=="link324d",]

linkd324_parent<-c2

linkd<-matchleft(linkd,linkd324_parent)
dim(linkd)
###############325############
c2<- linkd[linkd$method=="link325d",]

linkd325_parent<-c2
linkd<-matchleft(linkd,linkd325_parent)
dim(linkd)


###############326############
c2<- linkd[linkd$method=="link326d",]


linkd326_parent<-c2

linkd<-matchleft(linkd,linkd326_parent)
dim(linkd)

###############327############
c2<- linkd[linkd$method=="link327d",]


linkd327_parent<-c2

linkd<-matchleft(linkd,linkd327_parent)
dim(linkd)

###############329############
c2<- linkd[linkd$method=="link329d",]
linkd329_parent<-c2

linkd<-matchleft(linkd,linkd329_parent)

###############330############
c2<- linkd[linkd$method=="link330d",]
linkd330_parent<-c2

linkd<-matchleft(linkd,linkd330_parent)
dim(linkd)


###############331############
c2<- linkd[linkd$method=="link331d",]

linkd331_parent<-c2

linkd<-matchleft(linkd,c2)

###############333############
c2<- linkd[linkd$method=="link333d",]


linkd333_parent<-c2
linkd<-matchleft(linkd,c2)
dim(linkd)

###############334############
c2<- linkd[linkd$method=="link334d",]

linkd334_parent<-c2
linkd<-matchleft(linkd,c2)

###############336############
c2<- linkd[linkd$method=="link336d",]

linkd336_parent<-c2
linkd<-matchleft(linkd,c2)

###############338############
c2<- linkd[linkd$method=="link338d",]

detail_27<-c2
linkd<-matchleft(linkd,c2)

###############339############
c2<- linkd[linkd$method=="link339d",]

linkd339_parent<-c2
linkd<-matchleft(linkd,c2)
dim(linkd)

###############341############
c2<- linkd[linkd$method=="link341d",]

linkd341_parent<-c2
linkd<-matchleft(linkd,c2)
###############348############
c2<- linkd[linkd$method=="link348d",]

linkd348_1<-linkd[linkd$method=="link348d" & substr(linkd$class,1,1)=="R" ,]
linkd348_1<-uniqueresult(linkd348_1)

linkd<-matchleft(linkd,linkd348_1)

###############349############
c2<- linkd[linkd$method=="link349d",]

linkd349_1<-linkd[linkd$method=="link349d" & substr(linkd$class,1,1)=="R" ,]
linkd349_1<-uniqueresult(linkd349_1)

linkd<-matchleft(linkd,linkd349_1)
dim(linkd)

c2<- linkd[linkd$method=="link349d",]


linkd349_parent<-c2

linkd<-matchleft(linkd,c2)

###############351############
c2<- linkd[linkd$method=="link351d",]
linkd349_parent<-c2
linkd<-matchleft(linkd,c2)

###############356############
c2<- linkd[linkd$method=="link356d",]
linkd356_parent<-c2
linkd<-matchleft(linkd,c2)
dim(linkd)
###############357############

linkd357<-linkd[linkd$method=="link357d",] 
linkd357$add2<-gsub(",","",linkd357$add2)
linkd357_1<-linkd357[word(linkd357$add2, 1)==linkd357$paostartnumber,]



linkd357_1<-uniqueresult(linkd357_1)
dim(linkd)
linkd<-matchleft(linkd,linkd357_1)
dim(linkd)
c2<- linkd[linkd$method=="link357d",]
noadd2_12<-c2
linkd<-matchleft(linkd,c2)
dim(linkd)

###############360############
c2<- linkd[linkd$method=="link360d",]

fail_16<-c2
linkd<-matchleft(linkd,c2)

###############363############
c2<- linkd[linkd$method=="link363d",]



detail_28<-c2
linkd<-matchleft(linkd,c2)
dim(linkd)


###############367############

linkd367<-linkd[linkd$method=="link367d",] 
linkd367$add2<-gsub(",","",linkd367$add2)
linkd367_1<-linkd367[word(linkd367$add2, 1)==paste(linkd367$saostartnumber,linkd367$saostartsuffix,sep=""),]



linkd367_1<-uniqueresult(linkd367_1)
dim(linkd)
linkd<-matchleft(linkd,linkd367_1)
c2<- linkd[linkd$method=="link367d",]



fail_17<-c2
linkd<-matchleft(linkd,c2)
dim(linkd)

####################clean 368############
c2<- linkd[linkd$method=="link368d",]


detail_29<-c2

linkd<-matchleft(linkd,c2)
dim(linkd)
####################clean 369############
c2<- linkd[linkd$method=="link369d",]


detail_30<-c2

linkd<-matchleft(linkd,c2)

####################clean 374############
c2<- linkd[linkd$method=="link374d",]

linkd374_parent<-c2
linkd<-matchleft(linkd,c2)
dim(linkd)

####################clean 376############
c2<- linkd[linkd$method=="link376d",]


detail_31<-c2
linkd<-matchleft(linkd,c2)
dim(linkd)

####################clean 377############
c2<- linkd[linkd$method=="link377d",]

detail_32<-c2
linkd<-matchleft(linkd,c2)
###############381############
c2<- linkd[linkd$method=="link381d",]

linkd381_parent<-c2
linkd<-matchleft(linkd,c2)
dim(linkd)

###############382############
c2<- linkd[linkd$method=="link382d",]

detail_33<-c2
linkd<-matchleft(linkd,c2)

###############388############
c2<- linkd[linkd$method=="link388d",]

detail_34<-c2
linkd<-matchleft(linkd,c2)
dim(linkd)
###############389############
c2<- linkd[linkd$method=="link389d",]
detail_35<-c2
linkd<-matchleft(linkd,c2)
dim(linkd)
###############390############
c2<- linkd[linkd$method=="link390d",]


#add2=paotext


linkd390<-linkd[linkd$method=="link390d",] 
linkd390$add2<-gsub("ST. HELLIER COURT","ST HELIER COURT",linkd390$add2)


linkd390_1<-linkd390[linkd390$add2==linkd390$paotext,]
linkd390_1<-uniqueresult(linkd390_1)

linkd<-matchleft(linkd,linkd390_1)
 dim(linkd)

# 11157    3

linkd390<-linkd[linkd$method=="link390d",] 

linkd390_2<-linkd390[word(linkd390$add2,1)==linkd390$saostartnumber,]

linkd390_2<-uniqueresult(linkd390_2)

linkd<-matchleft(linkd,linkd390_2)
dim(linkd)
linkd390<-linkd[linkd$method=="link390d",] 

noadd2_13<-linkd390

linkd<-matchleft(linkd,linkd390)
dim(linkd)
###############391############

linkd391<-linkd[linkd$method=="link391d",] 

linkd391_1<-linkd391[linkd391$add2==word(linkd391$buildingname,1,2),]



linkd391_1<-uniqueresult(linkd391_1)
dim(linkd)
linkd<-matchleft(linkd,linkd391_1)

###############398############
c2<- linkd[linkd$method=="link398d",]

linkd398_parent<-c2
linkd<-matchleft(linkd,linkd398_parent)
dim(linkd)

###############399############
c2<- linkd[linkd$method=="link399d",]
fail_18<-c2

linkd<-matchleft(linkd,fail_18)
dim(linkd)

###############404############
length(unique(linkd$method))
c2<- linkd[linkd$method=="link404d",]


linkd404_1<-linkd[linkd$method=="link404d" & substr(linkd$class,1,1)=="R" ,]
linkd404_1<-uniqueresult(linkd404_1)

linkd<-matchleft(linkd,linkd404_1)
dim(linkd)

c2<- linkd[linkd$method=="link404d",]

detail_36<-c2

linkd<-matchleft(linkd,detail_36)

###############406############
c2<- linkd[linkd$method=="link406d",]


detail_37<-c2

linkd<-matchleft(linkd,detail_37)


###############412############
c2<- linkd[linkd$method=="link412d",]


detail_38<-c2

linkd<-matchleft(linkd,detail_38)
dim(linkd)

###############413############
c2<- linkd[linkd$method=="link413d",]

detail_39<-c2

linkd<-matchleft(linkd,detail_39)

###############415############
c2<- linkd[linkd$method=="link415d",]


detail_40<-c2

linkd<-matchleft(linkd,detail_40)
dim(linkd)
###############416############
c2<- linkd[linkd$method=="link416d",]
detail_41<-c2

linkd<-matchleft(linkd,detail_41)

###############418############
c2<- linkd[linkd$method=="link418d",]
detail_42<-c2

linkd<-matchleft(linkd,detail_42)
dim(linkd)


###############419############
length(unique(linkd$method))
#18

linkd419_1<-linkd[linkd$method=="link419d" & substr(linkd$class,1,1)=="R" ,]
linkd419_1<-uniqueresult(linkd419_1)

linkd<-matchleft(linkd,linkd419_1)
dim(linkd)

c2<- linkd[linkd$method=="link419d",]

detail_43<-c2

linkd<-matchleft(linkd,detail_43)
dim(linkd)



###############420############
c2<- linkd[linkd$method=="link420d",]
detail_44<-c2

linkd<-matchleft(linkd,detail_44)
dim(linkd)


###############421############
c2<- linkd[linkd$method=="link421d",]


linkd421<-linkd[linkd$method=="link421d",] 
linkd421$add2<-gsub(",","",linkd421$add2)
linkd421_1<-linkd421[word(linkd421$add2, 1)==linkd421$paostartnumber,]



linkd421_1<-uniqueresult(linkd421_1)
dim(linkd)
linkd<-matchleft(linkd,linkd421_1)
dim(linkd)

c2<- linkd[linkd$method=="link421d",]
fail_19<-c2

linkd<-matchleft(linkd,fail_19)
dim(linkd)



###############422############
c2<- linkd[linkd$method=="link422d",]


linkd422_1<-linkd[linkd$method=="link422d" & substr(linkd$class,1,1)=="R" ,]
linkd422_1<-uniqueresult(linkd422_1)

linkd<-matchleft(linkd,linkd422_1)
dim(linkd)

c2<- linkd[linkd$method=="link422d",]

fail_20<-c2

linkd<-matchleft(linkd,fail_20)
dim(linkd)

###############423############
c2<- linkd[linkd$method=="link423d",]

fail_21<-c2


linkd<-matchleft(linkd,fail_21)
dim(linkd)
###############424############
c2<- linkd[linkd$method=="link424d",]

linkd424_parent<-c2
linkd<-matchleft(linkd,linkd424_parent)
dim(linkd)
#8475   34
###############425############
c2<- linkd[linkd$method=="link425d",]


linkd425_parent<-c2[grepl("APARTMENT",c2$subbuildingname),]

linkd425_parent1<-keepneed(linkd,linkd425_parent)
rm(linkd425_parent)

dim(linkd)
linkd<-matchleft(linkd,linkd425_parent1)
dim(linkd)

detail_45<-c2


linkd<-matchleft(linkd,detail_45)
dim(linkd)

linkd425_parent<-linkd425_parent1
rm(linkd425_parent1)

###############428############
c2<- linkd[linkd$method=="link428d",]

linkd428_parent<-c2


linkd<-matchleft(linkd,linkd428_parent)

###############431############

#add2=paotext
#add2(2.3)=paotext(1.2)


linkd431<-linkd[linkd$method=="link431d",] 
#linkd431$add2<-gsub(",","",linkd431$add2)
linkd431_1<-linkd431[linkd431$add2==linkd431$paotext,]



linkd431_1<-uniqueresult(linkd431_1)
dim(linkd)
linkd<-matchleft(linkd,linkd431_1)
dim(linkd)
# 8357   34


linkd431<-linkd[linkd$method=="link431d",] 
#linkd431$add2<-gsub(",","",linkd431$add2)
linkd431_2<-linkd431[word(linkd431$add2, 2,3)==word(linkd431$paotext,1,2),]



linkd431_2<-uniqueresult(linkd431_2)
dim(linkd)
linkd<-matchleft(linkd,linkd431_2)
dim(linkd)

c2<- linkd[linkd$method=="link431d",]

noadd2_14<-c2

linkd<-matchleft(linkd,noadd2_14)


###############436##################################################################
length(unique(linkd$method))



linkd436<-linkd[linkd$method=="link436d",] 
linkd436$add1<-gsub(",","",linkd436$add1)
linkd436$add1c<-str_remove(linkd436$add1, '(\\w+\\s+){2}')


linkd436_1<-linkd436[word(linkd436$add1c,1)==linkd436$paostartnumber,]


linkd436_1<-uniqueresult(linkd436_1)
dim(linkd)
linkd<-matchleft(linkd,linkd436_1)
dim(linkd)

c2<- linkd[linkd$method=="link436d",]

linkd436<-linkd[linkd$method=="link436d",] 
linkd436$add2<-gsub(",","",linkd436$add2)
linkd436$add1<-gsub(",","",linkd436$add1)
#linkd436$add1c<-str_remove(linkd436$add1, '(\\w+\\s+){2}')


linkd436_2<-linkd436[word(linkd436$add1,1,2)==linkd436$subbuildingname & word(linkd436$add2,1)==linkd436$paostartnumber,]


linkd436_2<-uniqueresult(linkd436_2)
dim(linkd)
linkd<-matchleft(linkd,linkd436_2)
dim(linkd)


c2<- linkd[linkd$method=="link436d",]
c2<-c2[grepl("\\d",c2$add2),]

fail_22<-c2
linkd<-matchleft(linkd,fail_22)
dim(linkd)

linkd436<-linkd[linkd$method=="link436d",]
linkd436$add1<-gsub(",","",linkd436$add1)
linkd436$add1c<-str_remove(linkd436$add1, '(\\w+\\s+){2}')
linkd436$add1c<-gsub(",","",linkd436$add1c)
linkd436$add1c<-gsub("FINLEY COURT","FINLAY COURT",linkd436$add1c)
linkd436$add1c<-gsub("AUSTIN FRAIRS","AUSTIN FRIARS",linkd436$add1c)
linkd436$add1c<-gsub("SERENDIPITY HOUSE","SERENDIPITY MEWS",linkd436$add1c)


linkd436_3<-linkd436[linkd436$add1c==linkd436$paotext,]
linkd436_3<-uniqueresult(linkd436_3)
dim(linkd)
linkd<-matchleft(linkd,linkd436_3)
dim(linkd)
c2<- linkd[linkd$method=="link436d",]

noadd2_15<-c2
linkd<-matchleft(linkd,noadd2_15)

###############444############
c2<- linkd[linkd$method=="link444d",]



linkd444_1<-linkd[linkd$method=="link444d" & linkd$add2==linkd$paotext ,]
linkd444_1<-uniqueresult(linkd444_1)

linkd<-matchleft(linkd,linkd444_1)
dim(linkd)


linkd444_2<-linkd[linkd$method=="link444d" & word(linkd$add2,1,2)==word(linkd$paotext,2,3) ,]
linkd444_2<-uniqueresult(linkd444_2)

linkd<-matchleft(linkd,linkd444_2)
dim(linkd)
# 769  34

detail_48<-linkd[linkd$method=="link444d",]
linkd<-matchleft(linkd,detail_48)
dim(linkd)

###############445############
c2<- linkd[linkd$method=="link445d",]


detail_49   <-c2
linkd<-matchleft(linkd,c2)
dim(linkd)
###############446############
c2<- linkd[linkd$method=="link446d",]


linkd446_1<-linkd[linkd$method=="link446d" & substr(linkd$class,1,1)=="R" ,]
linkd446_1<-uniqueresult(linkd446_1)
dim(linkd)
linkd<-matchleft(linkd,linkd446_1)
dim(linkd)


length(unique(linkd$method))

#####sum up the one to many clean process

f_list<-list(fail_1,fail_2,fail_3,fail_4,fail_5,fail_6,fail_7,fail_8,fail_9,fail_10,fail_11,fail_12,fail_13,fail_14,fail_15,fail_16,fail_17,fail_18,fail_19,fail_20,fail_21,fail_22)
linkd_fail<-rbindlist(f_list  , use.names=TRUE, fill=TRUE)
rm(fail_1,fail_2,fail_3,fail_4,fail_5,fail_6,fail_7,fail_8,fail_9,fail_10,fail_11,fail_12,fail_13,fail_14,fail_15,fail_16,fail_17,fail_18,fail_19,fail_20,fail_21,fail_22)
rm(f_list)


detail_list<-list(detail_1,detail_2,detail_3,detail_4,detail_5,detail_6,detail_7,detail_8,detail_9,
                  detail_10,detail_11,detail_12,detail_13,detail_14,detail_15,detail_16,detail_17,detail_18,detail_19,
                  detail_20,detail_21,detail_22,detail_23,detail_24,detail_25,detail_26,detail_27,detail_28,detail_29,
                  detail_30,detail_31,detail_32,detail_33,detail_34,detail_35,detail_36,detail_37,detail_38,detail_39,
                  detail_40,detail_41,detail_42,detail_43,detail_44,detail_45,detail_46,detail_47,detail_48,detail_49)

linkd_needmoredetail<-rbindlist(detail_list, use.names=TRUE, fill=TRUE)


rm(detail_1,detail_2,detail_3,detail_4,detail_5,detail_6,detail_7,detail_8,detail_9,
   detail_10,detail_11,detail_12,detail_13,detail_14,detail_15,detail_16,detail_17,detail_18,detail_19,
   detail_20,detail_21,detail_22,detail_23,detail_24,detail_25,detail_26,detail_27,detail_28,detail_29,
   detail_30,detail_31,detail_32,detail_33,detail_34,detail_35,detail_36,detail_37,detail_38,detail_39,
   detail_40,detail_41,detail_42,detail_43,detail_44,detail_45,detail_46,detail_47,detail_48,detail_49)

rm(detail_list)

linkd_housenamedif<-housename_diff1
rm(housename_diff1)


parent_list<-list(linkd104_parent,linkd108_parent,linkd109_parent,linkd11_parent,linkd112_parent,linkd113_parent,linkd115_parent,linkd116_parent,linkd119_parent,linkd12_parent,
                linkd122_parent,linkd123_parent,linkd124_parent,linkd125_parent,linkd142_parent,linkd143_parent,linkd144_parent,linkd145_parent,linkd146_parent,linkd15_parent,
                linkd169_parent,linkd173_parent,linkd176_parent,linkd19_parent,linkd198_parent,linkd199_parent,linkd2_parent,linkd20_parent,linkd217_parent,linkd224_parent,
                linkd225_parent,linkd23_parent,linkd234_parent,linkd236_parent,linkd237_parent,linkd239_parent,linkd240_parent,linkd249_parent,linkd254_parent,linkd255_parent,
                linkd256_parent,linkd264_parent,linkd284_parent,linkd288_parent,linkd290_parent,linkd291_parent,linkd293_parent,linkd30_parent,linkd305_parent,linkd31_parent,
                linkd314_parent,linkd318_parent,linkd32_parent,linkd323_parent,linkd324_parent,linkd325_parent,linkd326_parent,linkd327_parent,linkd329_parent,linkd330_parent,
                linkd331_parent,linkd333_parent,linkd334_parent,linkd336_parent,linkd339_parent,linkd34_parent,linkd341_parent,linkd349_parent,linkd35_parent,linkd356_parent,
                linkd374_parent,linkd38_parent,linkd381_parent,linkd39_parent,linkd398_parent,linkd4_parent,linkd42_parent,linkd424_parent,linkd425_parent,linkd428_parent,
                linkd44_parent,linkd45_parent,linkd46_parent,linkd47_parent,linkd53_parent,linkd58_parent,linkd6_parent,linkd60_parent,linkd63_parent,linkd65_parent,
                linkd66_parent,linkd67_parent,linkd69_parent,linkd7_parent,linkd73_parent,linkd74_parent,linkd75_parent,linkd76_parent,linkd78_parent,linkd79_parent,
                linkd8_parent,linkd81_parent,linkd82_parent,linkd84_parent,linkd87_parent,linkd9_parent,linkd98_parent
                )

linkd_parent<-rbindlist(parent_list, use.names=TRUE, fill=TRUE)


rm(linkd104_parent,linkd108_parent,linkd109_parent,linkd11_parent,linkd112_parent,linkd113_parent,linkd115_parent,linkd116_parent,linkd119_parent,linkd12_parent,
   linkd122_parent,linkd123_parent,linkd124_parent,linkd125_parent,linkd142_parent,linkd143_parent,linkd144_parent,linkd145_parent,linkd146_parent,linkd15_parent,
   linkd169_parent,linkd173_parent,linkd176_parent,linkd19_parent,linkd198_parent,linkd199_parent,linkd2_parent,linkd20_parent,linkd217_parent,linkd224_parent,
   linkd225_parent,linkd23_parent,linkd234_parent,linkd236_parent,linkd237_parent,linkd239_parent,linkd240_parent,linkd249_parent,linkd254_parent,linkd255_parent,
   linkd256_parent,linkd264_parent,linkd284_parent,linkd288_parent,linkd290_parent,linkd291_parent,linkd293_parent,linkd30_parent,linkd305_parent,linkd31_parent,
   linkd314_parent,linkd318_parent,linkd32_parent,linkd323_parent,linkd324_parent,linkd325_parent,linkd326_parent,linkd327_parent,linkd329_parent,linkd330_parent,
   linkd331_parent,linkd333_parent,linkd334_parent,linkd336_parent,linkd339_parent,linkd34_parent,linkd341_parent,linkd349_parent,linkd35_parent,linkd356_parent,
   linkd374_parent,linkd38_parent,linkd381_parent,linkd39_parent,linkd398_parent,linkd4_parent,linkd42_parent,linkd424_parent,linkd425_parent,linkd428_parent,
   linkd44_parent,linkd45_parent,linkd46_parent,linkd47_parent,linkd53_parent,linkd58_parent,linkd6_parent,linkd60_parent,linkd63_parent,linkd65_parent,
   linkd66_parent,linkd67_parent,linkd69_parent,linkd7_parent,linkd73_parent,linkd74_parent,linkd75_parent,linkd76_parent,linkd78_parent,linkd79_parent,
   linkd8_parent,linkd81_parent,linkd82_parent,linkd84_parent,linkd87_parent,linkd9_parent,linkd98_parent)
rm(parent_list)

clean_list<-list(linkd_1,linkd2_1,linkd4_1,linkd5_1,linkd6_1,linkd7_1,linkd8_1,linkd9_1,
                 linkd10_1,linkd11_1,linkd12_1,linkd15_1,linkd20_1,linkd25_1,linkd30_1,
                 linkd32_1,linkd34_1,linkd35_1,linkd38_1,linkd44_1,linkd45_1,linkd46_1,
                 linkd58_1,linkd73_1,linkd74_1,linkd75_1,linkd78_1,linkd79_1,linkd82_1,
                 linkd87_1,linkd96_1,linkd97_1,linkd101_1,linkd103_1,linkd104_1,linkd107_1,
                 linkd108_1,linkd109_1,linkd113_1,linkd115_1,linkd123_1,linkd124_1,linkd125_1,
                 linkd142_1,linkd143_1,linkd143_2,linkd143_3,linkd143_4,linkd143_6,linkd143_7,
                 linkd143_8,linkd144_1,linkd145_1,linkd146_1,linkd147_1,linkd151_1,linkd153_1,
                 linkd161_1,linkd169_1,linkd169_2,linkd169_3,linkd169_4,linkd169_5,linkd171_1,
                 linkd173_1,linkd173_2,linkd173_3,linkd175_1,linkd180_1,linkd196_1,linkd196_2,
                 linkd197_1,linkd202_1,linkd203_1,linkd214_1,linkd217_1,linkd234_1,linkd236_1,
                 linkd237_1,linkd239_1,linkd240_1,linkd246_1,linkd246_2,linkd253_1,linkd253_2,
                 linkd253_3,linkd254_1,linkd254_2,linkd255_1,linkd271_1,linkd271_2,linkd271_3,
                 linkd272_1,linkd281_1,linkd281_2,linkd281_3,linkd281_4,linkd281_5,linkd281_6,
                 linkd281_7,linkd281_8,linkd281_9,linkd281_10,linkd281_11,linkd281_12,linkd281_13,
                 linkd281_14,linkd281_15,linkd281_16,linkd281_17,linkd281_18,linkd281_19,linkd281_20,
                 linkd281_21,linkd281_22,linkd281_23,linkd281_24,linkd288_1,linkd288_2,linkd288_3,linkd288_4,
                 linkd288_5,linkd288_6,linkd288_7,linkd288_8,linkd288_9,linkd288_10,linkd289_1,linkd289_2,
                 linkd289_3,linkd289_4,linkd291_1,linkd291_2,linkd291_3,linkd291_4,linkd291_5,
                 linkd291_6,linkd291_7,linkd291_8,linkd291_9,linkd292_1,linkd292_2,linkd293_1,
                 linkd291_10,linkd293_2,linkd293_3,linkd293_4,linkd294_1,linkd294_2,linkd294_3,
                 linkd294_4,linkd295_1,linkd295_2,linkd296_1,linkd301_1,linkd319_1,linkd319_2,
                 linkd321_1,linkd321_2,linkd324_1,linkd348_1,linkd349_1,linkd357_1,linkd367_1,
                 linkd390_1,linkd390_2,linkd391_1,linkd404_1,linkd419_1,linkd421_1,linkd422_1,
                 linkd431_1,linkd431_2,linkd436_1,linkd436_2,linkd436_3,linkd444_1,linkd444_2,
                 linkd446_1,linkd34_2,linkd202_2,linkd113_2
)



linkd_clean<-rbindlist(clean_list, use.names=TRUE, fill=TRUE)

rm(linkd_1,linkd2_1,linkd4_1,linkd5_1,linkd6_1,linkd7_1,linkd8_1,linkd9_1,
   linkd10_1,linkd11_1,linkd12_1,linkd15_1,linkd20_1,linkd25_1,linkd30_1,
   linkd32_1,linkd34_1,linkd35_1,linkd38_1,linkd44_1,linkd45_1,linkd46_1,
   linkd58_1,linkd73_1,linkd74_1,linkd75_1,linkd78_1,linkd79_1,linkd82_1,
   linkd87_1,linkd96_1,linkd97_1,linkd101_1,linkd103_1,linkd104_1,linkd107_1,
   linkd108_1,linkd109_1,linkd113_1,linkd115_1,linkd123_1,linkd124_1,linkd125_1,
   linkd142_1,linkd143_1,linkd143_2,linkd143_3,linkd143_4,linkd143_6,linkd143_7,
   linkd143_8,linkd144_1,linkd145_1,linkd146_1,linkd147_1,linkd151_1,linkd153_1,
   linkd161_1,linkd169_1,linkd169_2,linkd169_3,linkd169_4,linkd169_5,linkd171_1,
   linkd173_1,linkd173_2,linkd173_3,linkd175_1,linkd180_1,linkd196_1,linkd196_2,
   linkd197_1,linkd202_1,linkd203_1,linkd214_1,linkd217_1,linkd234_1,linkd236_1,
   linkd237_1,linkd239_1,linkd240_1,linkd246_1,linkd246_2,linkd253_1,linkd253_2,
   linkd253_3,linkd254_1,linkd254_2,linkd255_1,linkd271_1,linkd271_2,linkd271_3,
   linkd272_1,linkd281_1,linkd281_2,linkd281_3,linkd281_4,linkd281_5,linkd281_6,
   linkd281_7,linkd281_8,linkd281_9,linkd281_10,linkd281_11,linkd281_12,linkd281_13,
   linkd281_14,linkd281_15,linkd281_16,linkd281_17,linkd281_18,linkd281_19,linkd281_20,
   linkd281_21,linkd281_22,linkd281_23,linkd281_24,linkd288_1,linkd288_2,linkd288_3,linkd288_4,
   linkd288_5,linkd288_6,linkd288_7,linkd288_8,linkd288_9,linkd288_10,linkd289_1,linkd289_2,
   linkd289_3,linkd289_4,linkd291_1,linkd291_2,linkd291_3,linkd291_4,linkd291_5,
   linkd291_6,linkd291_7,linkd291_8,linkd291_9,linkd292_1,linkd292_2,linkd293_1,
   linkd291_10,linkd293_2,linkd293_3,linkd293_4,linkd294_1,linkd294_2,linkd294_3,
   linkd294_4,linkd295_1,linkd295_2,linkd296_1,linkd301_1,linkd319_1,linkd319_2,
   linkd321_1,linkd321_2,linkd324_1,linkd348_1,linkd349_1,linkd357_1,linkd367_1,
   linkd390_1,linkd390_2,linkd391_1,linkd404_1,linkd419_1,linkd421_1,linkd422_1,
   linkd431_1,linkd431_2,linkd436_1,linkd436_2,linkd436_3,linkd444_1,linkd444_2,
   linkd446_1,linkd34_2,linkd202_2,linkd113_2,clean_list
)


length(unique(linkd_clean$lmk_key))
# 10612


rm(linkd,linkd2,linkd4,linkd5,linkd6,linkd7,linkd8,linkd9,
    linkd10,linkd11,linkd12,linkd15,linkd20,linkd25,linkd30,
    linkd32,linkd34,linkd35,linkd38,linkd44,linkd45,linkd46,
    linkd58,linkd73,linkd74,linkd75,linkd78,linkd79,linkd82,
    linkd87,linkd96,linkd97,linkd101,linkd103,linkd104,linkd107,
    linkd108,linkd109,linkd113,linkd115,linkd123,linkd124,linkd125,
    linkd142,linkd143,linkd144,linkd145,linkd146,linkd147,linkd151,linkd153,
    linkd161,linkd169,linkd171,
    linkd173,linkd175,linkd180,linkd196,
    linkd197,linkd202,linkd203,linkd214,linkd217,linkd234,linkd236,
    linkd237,linkd239,linkd240,linkd246,linkd253,
    linkd254,linkd254_2,linkd255,linkd271,
    linkd272,linkd281,linkd321,linkd324,linkd348,linkd349,linkd357,linkd367,
   linkd390,linkd391,linkd404,linkd419,linkd421,linkd422,
   linkd431,linkd436,linkd444,linkd288,linkd289,linkd291,
   linkd292,linkd294,linkd295,linkd296,linkd301,linkd319,linkd293,linkd305,linkd297,
   linkd446)

dim(linkd_clean)
#10612    37
linkdd_final<-unique(linkd_clean)
dim(linkdd_final)
#10612    37
rm(linkd_clean)
length(unique(linkdd_final$lmk_key))/dim(epcdata)[1]
# 0.000485504
length(unique(linkd_parent$lmk_key))
#79897
length(unique(linkd_parent$lmk_key))/dim(epcdata)[1]
# 0.003655325
length(unique(linkd_fail$lmk_key))
#2560
length(unique(linkd_fail$lmk_key))/dim(epcdata)[1]
# 0.0001171212

length(unique(linkdcopy$lmk_key))-length(unique(linkd_parent$lmk_key))-length(unique(linkd_fail$lmk_key))-length(unique(linkdd_final$lmk_key))
#22786

22786/dim(epcdata)[1]
# 0.00104247
noneed_list<-list(noadd2_1,noadd2_2,noadd2_3,noadd2_4,noadd2_5,noadd2_6,noadd2_7,noadd2_8,noadd2_9,noadd2_10,noadd2_11,noadd2_12,noadd2_13,noadd2_14,noadd2_15)


linkd_noadd2<-rbindlist(noneed_list, use.names=TRUE, fill=TRUE)

rm(noadd2_1,noadd2_2,noadd2_3,noadd2_4,noadd2_5,noadd2_6,noadd2_7,noadd2_8,noadd2_9,noadd2_10,noadd2_11,noadd2_12,noadd2_13,noadd2_14,noadd2_15)

dim(linkdcopy)
#473354     34
length(unique(linkdcopy$lmk_key))
#115855
length(unique(linkd_parent$lmk_key))
#79897
115855-79897
#35958
length(unique(linkd_fail$lmk_key))
# 2560
35958-length(unique(linkd_fail$lmk_key))
# 33398
length(unique(linkd_needmoredetail$lmk_key))
# 6429
33398-6429
#26969


length(unique(linkd_noadd2$lmk_key))
#4938
26969-4938
#22031
  

length(unique(linkdd_final$lmk_key))
#10612
22031-10612
#11419
11472+length(unique(linkd_needmoredetail$lmk_key))+length(unique(linkd_noadd2$lmk_key))



length(unique(linkd$lmk_key))
linkdcopy<- dbGetQuery(con,"select * from  linkd2") 

length(unique(linkdcopy$lmk_key))
# 115855 
length(unique(parent$lmk_key))
length(unique(linkdcopy$lmk_key))
# 115855
linkdcopy<-matchleft(linkdcopy,parent)
length(unique(linkdcopy$lmk_key))
# 30812

linkdcopy<-matchleft(linkdcopy,linkd_fail)
length(unique(linkdcopy$lmk_key))
#  28252
linkdcopy<-matchleft(linkdcopy,linkdd_final)
length(unique(linkdcopy$lmk_key))
#   17640
17640/dim(epcdata)[1]


10612/115855 
#0.09159726
85043/115855 
# 0.7340469
2560/115855 
#0.02209659
17640/115855 
#0.1522593
linkdcopy<-matchleft(linkdcopy,linkd_needmoredetail)
length(unique(linkdcopy$lmk_key))
# 26970

linkdcopy<-matchleft(linkdcopy,linkd_noadd2)
length(unique(linkdcopy$lmk_key))
#22032

linkdcopy<-matchleft(linkdcopy,linkdd_final)
length(unique(linkdcopy$lmk_key))
#11472



linkdcopy5_parent<-linkdcopy[linkdcopy$method=="link5d" & linkdcopy$subbuildingname!="",]

linkdcopy5_parent1<-keepneed(linkdcopy,linkdcopy5_parent)
dim(linkdcopy)
linkdcopy<-matchleft(linkdcopy,linkdcopy5_parent1)
dim(linkdcopy)
rm(linkdcopy5_parent)
c2<- linkdcopy[linkdcopy$method=="link5d",]


linkdcopy5_parent<-c2[grepl("\\d",c2$buildingname),]

linkdcopy5_parent2<-keepneed(linkdcopy,linkdcopy5_parent)

#linkdcopy[linkdcopy$method=="link5d" & linkdcopy$subbuildingname!="",]

dim(linkdcopy)
linkdcopy<-matchleft(linkdcopy,linkdcopy5_parent2)
dim(linkdcopy)

c2<- linkdcopy[linkdcopy$method=="link5d",]


linkdcopy5_parent<-c2[grepl("FLOOR",c2$saotext),]
linkdcopy5_parent3<-keepneed(linkdcopy,linkdcopy5_parent)
rm(linkdcopy5_parent)


dim(linkdcopy)
linkdcopy<-matchleft(linkdcopy,linkdcopy5_parent3)
dim(linkdcopy)
#453184
c2<- linkdcopy[linkdcopy$method=="link5d",]


dim(linkdcopy)
linkdcopy<-matchleft(linkdcopy,c2)
rm(c2)
dim(linkdcopy)
#453149     34

linkdcopy5_parent<-rbindlist(list(linkdcopy5_parent1,linkdcopy5_parent2,linkdcopy5_parent3))
rm(linkdcopy5_parent1,linkdcopy5_parent2,linkdcopy5_parent3)
length(unique(linkdcopy5_parent$lmk_key))
#5146

parent<-rbindlist(list(linkdcopy5_parent,linkd_parent),use.names=TRUE, fill=TRUE)
linkdcopy<-matchleft(linkdcopy,linkdcopy5_parent)
length(unique(linkdcopy$lmk_key))
#11472

  
  
length(unique(parent$lmk_key))/dim(epcdata)[1]
28252/dim(epcdata)[1]



28252+256+85043+10612

########################clean the linku######################
sort(unique(linku2$method))

##################clean the method 58#########################
c1<-linku2[linku2$method=="link58u",]


c1<-c1[grepl("\\d+",c1$buildingnumber),]
c1<-c1[c1$paostartnumber!="",]


dim(linku2)
#21046468       34
linku<-matchleft(linku2,c1)
dim(linku)
#21046385       34

#c1<-linku[linku$method=="link113u",]

c1<-linku[linku$method=="link116u",]
View(c1)
c1<-c1[grepl("^\\d+",c1$buildingname),]
c1<-c1[c1$saostartnumber!="",]
in_1<-c1
dim(linku)
#21046385       34
linku<-matchleft(linku,c1)
dim(linku)
#21046309       34

##143 to solve the add2 issue is not exist in OS addressbase
#c1<-linku[linku$method=="link143u",]
#c1<-linku[linku$method=="link144u",]
#View(c1)


#c1<-linku[linku$method=="link161u",]

#c1<-linku[linku$method=="link167u",]

#c1<-linku[linku$method=="link169u",]
#c1<-c1[grepl("^\\d+",c1$buildingname),]

# c1<-linku[linku$method=="link171u",]
# c1<-linku[linku$method=="link202u",]
# #
# c1<-linku[linku$method=="link211u",]


# c1<-linku[linku$method=="link246u",]
# c1<-linku[linku$method=="link251u",]
# c1<-linku[linku$method=="link254u",]
# c1<-linku[linku$method=="link255u",]

#c1<-linku[linku$method=="link273u",]
#c1<-linku[linku$method=="link274u",]

c1<-linku[linku$method=="link145u",]

c1_1<-c1[grepl("\\d[,]\\s\\d",c1$add1),]


dim(linku)
#21046309       34
linku<-matchleft(linku,c1_1)
dim(linku)
#21046307       34

##same uprn for different adddress

c1<-linku[linku$method=="link281u",]

doubleresult1 <-  function(x){
  
  dt <- as.data.table(x)
  
  esummary<-dt[,.(count=.N),by=uprn]
  
  idd2 <- esummary[esummary$count!=1,]
  
  need1 <- x[x$uprn %in% idd2$uprn,]
  
  return(need1)
}


uniqueresult1 <-  function(x){
  
  dt <- as.data.table(x)
  
  esummary<-dt[,.(count=.N),by=uprn]
  
  idd2 <- esummary[esummary$count==1,]
  
  need1 <- x[x$uprn %in% idd2$uprn,]
  
  return(need1)
}
c11<-doubleresult1(c1)

c12<-c11[,c("add","uprn")]
c12<-unique(c12)

c13<-doubleresult1(c12)




c14<-c1[c1$add %in% c13$add,]
#c14<-c12[!(c12$add %in% c13$add),]

##c14 contain the wrong part
#first remove it and then add in the right part

dim(linku)
#21046309       34
linku<-matchleft(linku,c14)
dim(linku)
#21040785       34


data1<-c14
View(data1)
data1_1<-data1[data1$add2!="",]
data1_1<-data1_1[word(data1_1$add2,1)==paste(data1_1$paostartnumber,data1_1$paostartsuffix,sep=""),]

dim(data1_1)
data1_1<-uniqueresult(data1_1)
dim(data1_1)
#48
dim(data1_1)
# data1_1<-uniqueresult1(data1_1)
# dim(data1_1)

matchleft1 <- function(x,y){
  next0 <- x[!(x$uprn %in% y$uprn),]
  return(next0)
  
}

dim(data1)
#5524   34
data1<-matchleft1(data1,data1_1)

dim(data1)
#5375   34

data1_2<-data1[data1$add2!="",]
data1_2<-data1_2[word(data1_2$add2,1)==paste(data1_2$saostartnumber,data1_2$saostartsuffix,sep=""),]
dim(data1_2)
data1_2<-uniqueresult(data1_2)
dim(data1)
#5375   34
data1<-matchleft1(data1,data1_2)

dim(data1)
# 5307   34
# Two dataset use different street name.

#407452300952017030308312995030576
data1_3<-data1[data1$add2!="",]
data1_3<-data1_3[word(data1_3$add2,1)==data1_3$buildingnumber,]

data1_3<-uniqueresult(data1_3)
dim(data1)
#5375   34
data1<-matchleft1(data1,data1_3)

dim(data1)
#5299   34


data1_4<-data1[data1$add2=="",]

data1_4$add1c<-str_remove(data1_4$add1, '(\\w+\\s+){2}')

data1_4<-data1_4[word(data1_4$add1c,1,2)==word(data1_4$paotext,1,2),]

data1<-matchleft1(data1,data1_4)

dim(data1)
# 5295   34
data1_5<-data1[data1$add2=="",]

data1_5$add1c<-str_remove(data1_5$add1, '(\\w+\\s+){2}')

data1_5<-data1_5[word(data1_5$add1c,1)==paste(data1_5$paostartnumber,data1_5$paostartsuffix,sep=""),]

data1<-matchleft1(data1,data1_5)

dim(data1)
#5293   34
# data1_6<-data1[data1$add2=="",]
# 
# data1_6$add1c<-str_remove(data1_6$add1, '(\\w+\\s+){2}')
# 
# data1_6<-data1_6[word(data1_6$add1c,1)==data1_6$paostartnumber,]

data1_all<-rbindlist(list(data1_1,data1_2,data1_3,data1_4,data1_5),use.names=TRUE, fill=TRUE)
rm(data1_1,data1_2,data1_3,data1_4,data1_5)
head(data1_all)
data1_all[, add1c:=NULL]
dim(linku)
dim(data1_all)

linku_final<-rbindlist(list(linku,data1_all), use.names=TRUE, fill=TRUE)
dim(linku_final)
#21040874       34
c1<-linku_final[linku_final$method=="link288u",]

#pp=add1,1
# add1 contain this 10 B  is wrong

numberstringxtract <- function(string){ 
  str_extract(string, "[0-9]{1,2}\\s[A-Z]")
} 

data2<- c1[grepl("[0-9]{1,2}\\s[A-Z]$",c1$add1),]

dim(linku_final)
#21040876       34
linku_final<-matchleft(linku_final,data2)
dim(linku_final)
# 21040851       34
#these are the wrong and need to redone the linkage.
dim(data2)
#25 34
###unable to solve 

##save out the mark it 
fwrite(data2,"D:/epc_os/results/data2.csv")
c1<-linku_final[linku_final$method=="link295u",]
#maybe no issue
rm(c1)


c1<-linku_final[linku_final$method=="link436u",]

rm(c1)
##
##check random
cc<-linku_final[linku_final$method=="link229u",]
View(cc)
rm(cc)
cc<-linku_final[linku_final$method=="link230u",]
View(cc)
rm(cc)
cc<-linku_final[linku_final$method=="link231u",]
View(cc)
rm(cc)
cc<-linku_final[linku_final$method=="link236u",]
View(cc)
rm(cc)

cc<-linku_final[linku_final$method=="link237u",]
View(cc)
rm(cc)
cc<-linku_final[linku_final$method=="link239u",]
View(cc)
rm(cc)

cc<-linku_final[linku_final$method=="link246u",]
View(cc)
rm(cc)


cc<-linku_final[linku_final$method=="link247u",]
View(cc)
rm(cc)

# dim(linku)
# #21040760       34
# linku<-rbindlist(list(linku,data1_all),use.names=TRUE, fill=TRUE)
# dim(linku)
# ## 21040851       35

#check different address with the same uprn
dim(linku_final)
linku_final<-linku_final[add!="",]
dim(linku_final)
#21040848       34

#################################################clean 2###################################################

c1<-linku_final[linku_final$method=="link167u",]

#y <- y[!grepl("\\d$",y$add1),]


c1<-c1[grepl("\\d$",c1$add1),]
c1<-c1[grepl("^\\d",c1$add2),]

c1_2<-c1[c1$add1!=c1$subbuildingname,]


dim(linku_final)
#21040848       34
linku_final<-matchleft(linku_final,c1_2)
dim(linku_final)
#21040769       34                                                                                                                                                                                                                                                                                                                                                                                                                                       
c1<-linku_final[linku_final$method=="link147u",]
head(c1)
c1<-c1[!grepl("\\d$",c1$saotext),]

c1<-c1[!grepl("^FLAT",c1$saotext),]

c1<-c1[c1$buildingnumber!="",]
c1_11<-c1[c1$paostartnumber!="",]

dim(linku_final)
#21040769       34
linku_final<-matchleft(linku_final,c1_11)
dim(linku_final)
#21040767       34    


21040767/dim(epcdata)[1]

21046468-21040767
#################################################clean 3###################################################

head(linku_final)
c1<-linku_final[,c("add","postcode.y","uprn")]
dim(c1)
#21040767
c1<-unique(c1)
dim(c1)
#16861517        3
length(unique(linku_final$lmk_key))
# 21040767

doubleresult11 <-  function(x){
  
  dt <- as.data.table(x)
  
  esummary<-dt[,.(count=.N),by=uprn]
  
  idd2 <- esummary[esummary$count!=1,]
  
  need1 <- x[x$uprn %in% idd2$uprn,]
  
  return(need1)
}



c11<-doubleresult11(c1)
dim(c11)


#fwrite(c11,"D:/epc_os/results/c11_address_diff.csv")

#same postcode 
head(c11)
c11_1<-c11[,c("uprn","postcode.y")]
dim(c11_1)
c11_1<-unique(c11_1)
dim(c11_1)

issresult11 <-  function(x){
  
  dt <- as.data.table(x)
  
  esummary<-dt[,.(count=.N),by=uprn]
  
  idd2 <- esummary[esummary$count!=1,]
  
  need1 <- x[x$uprn %in% idd2$uprn,]
  
  return(need1)
}

c11_12<-issresult11(c11_1)

issresult12 <-  function(x){
  
  dt <- as.data.table(x)
  
  esummary<-dt[,.(count=.N),by=uprn]
  
  idd2 <- esummary[esummary$count==1,]
  
  need1 <- x[x$uprn %in% idd2$uprn,]
  
  return(need1)
}
c11_13<-issresult12(c11_1)
dim(linku_final[linku_final$uprn %in% c11_12$uprn,])[1]/dim(epcdata)[1]
#  0.002083385
dim(linku_final[linku_final$uprn %in% c11_13$uprn,])[1]/dim(epcdata)[1]
#  0.07390956

#different postcode
data41_d<-c11[c11$uprn %in% c11_12$uprn,]
dim(data41_d)
# 41202 
#same postcode
data42_u<-c11[c11$uprn %in% c11_13$uprn,]
dim(data42_u)
#1462621       3
#perpare for example
fwrite(data42_u,"D:/epc_os/results/data42_u.csv")
fwrite(data41_d,"D:/epc_os/results/data41_d.csv")



##clean the data42_u part
##this is quite interesting,
##ignore the postcode different
#fwrite(data42_u,"D:/epc_os/results/c11_address_diff_samepostcode.csv")
head(data42_u)
data42_u<-c11
head(data42_u)

data42_u$addc<-gsub(",", "", data42_u$add)
data42_u$addc<-gsub("[.]", "", data42_u$addc)
data42_u$addc<-gsub("[']", "", data42_u$addc)
data42_u$addc<-gsub("[/]", "", data42_u$addc)
dim(data42_u)
# data42_u<-unique(data42_u)
# dim(data42_u)


###separate 
head(data42_u)
dim(data42_u)
# 1503823 
dim(c11)
# 1503819       3
length(unique(data42_u$add))
# 1474050
t1<-data42_u[grepl("^\\d",data42_u$addc),]
dim(t1)

clean1<-data42_u[data42_u$uprn %in% t1$uprn,]
dim(clean1)
# 1223145  

class(clean1)
clean2<-data42_u[!(data42_u$uprn %in% clean1$uprn),]
dim(clean2)
# 280674      4
dim(c11)

length(unique(clean1$add))
#1195830
length(unique(clean2$add))
#278224
# 
# ccdif<-c11[!(c11$uprn %in% clean1$uprn), ]
# ccdif1<-clean2[!(clean2$add %in% ccdif$add), ]

###############
#setDT(clean1)
clean1$cout<-str_count(clean1$addc,"\\w+")
clean2$cout<-str_count(clean2$addc,"\\w+")
#if add is 1 
head(clean1)

t1<-clean1[cout==1,]
data6_list<-clean1[clean1$uprn %in% t1$uprn,]

data6<-linku_final[linku_final$uprn %in% data6_list$uprn,]

dim(clean1)
#  1223145       5
clean1<-clean1[!(clean1$uprn %in% data6_list$uprn),]
dim(clean1)
#  1223131       5
dim(c11)
# 1503819       3
c11<-c11[!(c11$uprn %in% data6_list$uprn),]
dim(c11)
#1503805       3



t1<-clean1[cout==2,]

data7_list<-clean1[clean1$uprn %in% t1$uprn,]
data7_list$addc_2<-word(data7_list$addc,1,2)
#


head(data7_list)
tt1<-data7_list[,c("uprn","addc_2")]
dim(tt1)
tt1<-unique(tt1)
dim(tt1)
no_1<-issresult12(tt1)
pp_1<-data42_u[data42_u$uprn %in% no_1$uprn,]
rm(pp)

dim(clean1)
#1223133       5
clean1<-clean1[!(clean1$uprn %in% no_1$uprn),]
dim(clean1)
#1200986       5


dim(c11)
#  1503805       3
c11<-c11[!(c11$uprn %in% no_1$uprn),]
dim(c11)
# 1481660       3

check_1<-data7_list[!(data7_list$uprn %in% no_1$uprn), ]

check_1$addc_1<-gsub(" ", "", check_1$addc)
head(check_1)

summary(nchar(check_1$addc_1))


#check_7<-check_1[nchar(addc_1)<=8,]

check_1[,addc_3 :=substring(addc_1,0,8)]

tt1<-check_1[,c("uprn","addc_3")]
dim(tt1)
tt1<-unique(tt1)
no_1<-issresult12(tt1)

pp<-data42_u[data42_u$uprn %in% no_1$uprn,]
rm(pp)
dim(clean1)
# 1200986       5
clean1<-clean1[!(clean1$uprn %in% no_1$uprn),]
dim(clean1)
# 1200402       5


dim(c11)
# 1481660       3
c11<-c11[!(c11$uprn %in% no_1$uprn),]
dim(c11)
# 1481076       3

t1<-clean1[cout==2,]

data7_list<-clean1[clean1$uprn %in% t1$uprn,]
data7_list$addc_2<-word(data7_list$addc,1,2)
data7_list$addc_1<-gsub(" ", "", data7_list$addc)


data7_list[,addc_3 :=substring(addc_1,0,5)]

tt1<-data7_list[,c("uprn","addc_3")]
dim(tt1)
tt1<-unique(tt1)
dim(tt1)
no_1<-issresult12(tt1)
dim(clean1)
# 1200402       5     
clean1<-clean1[!(clean1$uprn %in% no_1$uprn),]
dim(clean1)
# 1200378       5

dim(c11)
#  1481076       3
c11<-c11[!(c11$uprn %in% no_1$uprn),]
dim(c11)
# 1481052       3



t1<-clean1[cout==2,]

data7_list<-clean1[clean1$uprn %in% t1$uprn,]
#unsolve

unsolve_1<-clean1[clean1$uprn %in% data7_list$uprn,]


t1<-clean1[cout<3,]

data8_list<-clean1[!(clean1$uprn %in% t1$uprn),]
summary(data8_list$cout)
data8_list$addc_2<-word(data8_list$addc,1,3)

head(data8_list)
tt1<-data8_list[,c("uprn","addc_2")]
dim(tt1)
tt1<-unique(tt1)
dim(tt1)

no_1<-issresult12(tt1)

pp_5<-data42_u[data42_u$uprn %in% no_1$uprn,]
rm(pp)

dim(clean1)
# 1200378       5
clean1<-clean1[!(clean1$uprn %in% no_1$uprn),]
dim(clean1)
#153503      5
dim(c11)
# 1481052       3
c11<-c11[!(c11$uprn %in% no_1$uprn),]
dim(c11)
# 434175      3
# 
# pp<-data42_u[data42_u$uprn %in% no_1$uprn,]
# rm(pp)
#datacheck_1<-linku[linku$uprn %in% check_1$uprn, ]

#fwrite(datacheck_1,"D:/epc_os/results/check_1.csv")
t1<-clean1[cout<3,]
data8_list<-clean1[!(clean1$uprn %in% t1$uprn),]
summary(data8_list$cout)
data8_list$addc_2<-word(data8_list$addc,1,3)

#check_1<-data8_list[!(data8_list$uprn %in% no_1$uprn), ]
dim(data8_list)
head(data8_list)

data8_list$addc_1<-gsub(" ", "", data8_list$addc)
# head(data8_list)
# 
# tt1<-data8_list[,c("uprn","addc_1")]
# dim(tt1)
# tt1<-unique(tt1)
# dim(tt1)
# no_1<-issresult12(tt1)
# 
# dim(clean1)
# #
# clean1<-clean1[!(clean1$uprn %in% no_1$uprn),]
# dim(clean1)
# ##

# t1<-clean1[cout<3,]
# data8_list<-clean1[!(clean1$uprn %in% t1$uprn),]
# summary(data8_list$cout)


  
data8_list[,addc_3 :=substring(addc_1,0,8)]






tt1<-data8_list[,c("uprn","addc_3")]
dim(tt1)
tt1<-unique(tt1)
dim(tt1)
no_1<-issresult12(tt1)
dim(clean1)
#   153501      5
clean1<-clean1[!(clean1$uprn %in% no_1$uprn),]
dim(clean1)
# 141178      5
dim(c11)
#  434175      3
c11<-c11[!(c11$uprn %in% no_1$uprn),]
dim(c11)
# 421852      3

t1<-clean1[cout<3,]
data8_list<-clean1[!(clean1$uprn %in% t1$uprn),]
summary(data8_list$cout)
data8_list$addc_2<-word(data8_list$addc,1,3)

#check_1<-data8_list[!(data8_list$uprn %in% no_1$uprn), ]
dim(data8_list)
head(data8_list)

data8_list$addc_1<-gsub(" ", "", data8_list$addc)

data8_list[,addc_3 :=substring(addc_1,0,6)]
tt1<-data8_list[,c("uprn","addc_3")]
dim(tt1)
tt1<-unique(tt1)
dim(tt1)
no_1<-issresult12(tt1)
dim(clean1)
pp_6<-data42_u[data42_u$uprn %in% no_1$uprn,]
rm(pp)

dim(clean1)
#    141178      5
clean1<-clean1[!(clean1$uprn %in% no_1$uprn),]
dim(clean1)
# 138297      5


dim(c11)
# 421852      3
c11<-c11[!(c11$uprn %in% no_1$uprn),]
dim(c11)
# 418971      3
#unable to solve so far
unsolve_2<-clean1

is_2<-linku_final[linku_final$uprn %in% clean1$uprn, ]

###########################################################
#clean2<-data42_u[!(data42_u$uprn %in% clean1$uprn),]
dim(clean2)
# 280674      5

##solve the 4 words

t1<-clean2[cout==1,]

data71_list<-clean2[clean2$uprn %in% t1$uprn,]

data71_list$addc_2<-word(data71_list$addc,1,2)


dim(clean2)
# 280674      5
clean2<-clean2[!(clean2$uprn %in% data71_list$uprn),]
dim(clean2)
# 280492      5

dim(c11)
#  418971      3
c11<-c11[!(c11$uprn %in% data71_list$uprn),]
dim(c11)
# 418789      3


t1<-clean2[cout==2,]

data72_list<-clean2[clean2$uprn %in% t1$uprn,]
data72_list$addc_2<-word(data72_list$addc,1,2)
tt1<-data72_list[,c("uprn","addc_2")]
dim(tt1)
tt1<-unique(tt1)
dim(tt1)
no_1<-issresult12(tt1)

pp<-data42_u[data42_u$uprn %in% no_1$uprn,]
rm(pp)


dim(clean2)
# 280492      5
clean2<-clean2[!(clean2$uprn %in% no_1$uprn),]
dim(clean2)
#279907      5


dim(c11)
#   418789      3
c11<-c11[!(c11$uprn %in% no_1$uprn),]
dim(c11)
# 418204      3

t1<-clean2[cout<3,]


data73_list<-clean2[!(clean2$uprn %in% t1$uprn),]

data73_list$addc_2<-word(data73_list$addc,1,3)
#data7_list$addc_2<-gsub("[/]", "", data7_list$addc_2)
summary(data7_list$cout)


tt1<-data73_list[,c("uprn","addc_2")]
dim(tt1)
tt1<-unique(tt1)
dim(tt1)
no_1<-issresult12(tt1)
pp_7<-data42_u[data42_u$uprn %in% no_1$uprn,]
rm(pp)


dim(clean2)
#  279907      5
clean2<-clean2[!(clean2$uprn %in% no_1$uprn),]
dim(clean2)
# 76088     5

dim(c11)
#   418204      3
c11<-c11[!(c11$uprn %in% no_1$uprn),]
dim(c11)
# 214385      3

t1<-clean2[cout<3,]


data74_list<-clean2[!(clean2$uprn %in% t1$uprn),]

check_1<-data74_list[!(data74_list$uprn %in% no_1$uprn), ]
head(check_1)
dim(check_1)
check_2<-check_1[grepl("^FLAT ",check_1$addc),]

check<-check_1[!(check_1$uprn %in% check_2$uprn), ]

check$addc_1<-gsub(" ", "", check$addc)
head(check)

#check_12<-data42_u[]

check[,addc_3 :=substring(addc_1,0,8)]
#check$addc_2<-word(check$addc,1,3)
tt1<-check[,c("uprn","addc_3")]
dim(tt1)
tt1<-unique(tt1)
dim(tt1)
no_1<-issresult12(tt1)


pp_8<-data42_u[data42_u$uprn %in% no_1$uprn,]
rm(pp)

dim(clean2)
# 76088     5
clean2<-clean2[!(clean2$uprn %in% no_1$uprn),]
dim(clean2)
#62556     5

dim(c11)
#  214385      3
c11<-c11[!(c11$uprn %in% no_1$uprn),]
dim(c11)
#  200853      3
###all the c11 is part could has issue






t1<-clean2[cout<3,]


data75_list<-clean2[!(clean2$uprn %in% t1$uprn),]
head(data75_list)
#check_1<-data75_list[!(data75_list$uprn %in% no_1$uprn), ]

check_2<-data75_list[grepl("FLAT",data75_list$add),]
#
check<-data75_list[!(data75_list$uprn %in% check_2$uprn), ]
#
# check$addc_1<-gsub(" ", "", check$addc)

# check[,addc_3 :=substring(addc_1,0,6)]
# tt1<-check[,c("uprn","addc_3")]
# dim(tt1)
# tt1<-unique(tt1)
# dim(tt1)
# no_1<-issresult12(tt1)



##check1 and check 2 are the left needs to manually correct.

data_iss<-linku_final[linku_final$uprn %in%  c11$uprn,]
dim(data_iss)
#228698     34
dim(c11)
#200853      3
dim(data_iss)[1]
length(unique(data_iss$uprn))
#98032
dim(data_iss)[1]/dim(epcdata)[1]
# 1.05%
##check method from 288u
#288 and 291 could have issues


da_1<-data_iss[method=="link288u",]
da_2<-data_iss[data_iss$uprn %in% da_1$uprn,]
da_3<-da_2[method!="link288u",]
da_3<-da_3[method!="link1u",]
da_3<-da_3[method!="link2u",]

sort(unique(da_3$method))

dim(linku_final)
#21040767       34
linku_final<-matchleft(linku_final,data_iss)
dim(linku_final)
#20812069       34
dim(data_iss)
data_iss[,record:="issue1"]
dim(data_iss)

link_u<-rbindlist(list(linku_final,data_iss), use.names=TRUE, fill=TRUE)
dim(link_u)
### 21040767       35

#####################sum up#######################################
dim(linkdd_final)
#10612    35
dim(linkdd_final)[1]/dim(epcdata)[1]
#0.000485504
head(linkdd_final)
head(link_u)
class(linkdd_final)
linkdd_final[, pp:=NULL]
linkdd_final[, ss:=NULL]
linkdd_final[, add1c:=NULL]
dim(link_u)
link_all<-rbindlist(list(link_u,linkdd_final), use.names=TRUE, fill=TRUE)
#

dim(link_all)
#21051379       35
head(link_all)
unique(link_all$record)
dim(link_all)[1]/dim(epcdata)[1]
#0.9631105
####
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
dbWriteTable(con, "epc_link",value =link_all, append = TRUE, row.names = FALSE)

head(link_all)

class(link_all)

add[nchar(postcodelocator)>=6,postset :=substring(postcodelocator,0,nchar(postcodelocator)-2)]


link_all[,linkid:=substring(method,0,nchar(method)-1)]
length(unique(link_all$linkid))
#446

##matchrate for different linkage
head(link_all)
sta_matchratef<-link_all[,.(fcount=.N),by="linkid"]

sta_matchratef$pro<-sta_matchratef$fcount/dim(epcdata)[1]
sum(sta_matchratef$pro)
#0.9631105
dim(sta_matchratef)
sta_matchratef<-sta_matchratef[order(-pro)]
head(sta_matchratef)

sta_matchratef[, no := .I]


top30<-sta_matchratef[no<=30,]
#
sum(top30$pro)


sum(sta_matchratef[no<=5,]$pro)
sum(sta_matchratef[no<=10,]$pro)
sum(sta_matchratef[no<=20,]$pro)
sum(sta_matchratef[no<=30,]$pro)
sum(sta_matchratef[no<=40,]$pro)
sum(sta_matchratef[no<=50,]$pro)
fwrite(sta_matchratef,"D:/epc_os/results/sta_matchratef.csv")
top20<-sta_matchratef[no<=20,]
head(top20)
fwrite(top20,"D:/plot/epctop20.csv")
## add start with a number, then ###















###################################################
head(link_all)
linkme<-link_all[,c("lmk_key","uprn","postcode.y")]
head(epcdata)
linkpu<-epcdata[,c("lmk_key","postcode","uprn","uprn_source")]
colnames(linkpu)[3]<-"uprn_p"
dim(epcdata)
head(linkpu)
head(linkme)
mgclg_su<-linkpu[linkpu$uprn_source!="" ,]
dim(mgclg_su)

mgclg_fail<-linkpu[linkpu$uprn_source=="" ,]
dim(mgclg_fail)
dim(mgclg_fail)[1]/dim(epcdata)[1]

compare_1<-merge(linkme,mgclg_su,by="lmk_key")
dim(compare_1)
#20070084 
dim(linkme[(linkme$lmk_key %in% mgclg_su$lmk_key),])[1]

#20070084
head(compare_1)
dim(compare_1[uprn==uprn_p,])[1]
unique(compare_1$uprn_source)
dim(compare_1[uprn==uprn_p,])[1]/dim(compare_1)[1]
#19949006
#0.9939672
dim(compare_1[uprn==uprn_p,])[1]/dim(epcdata)[1]
#0.9126764
dim(compare_1[uprn==uprn_p & uprn_source=="Address Matched",])[1]
#18570562
dim(compare_1[uprn==uprn_p & uprn_source=="Address Matched",])[1]/dim(compare_1)[1]
# 0.9252857
dim(compare_1[uprn==uprn_p & uprn_source=="Energy Assessor",])[1]
# 1378444
dim(compare_1[uprn==uprn_p & uprn_source=="Energy Assessor",])[1]/dim(compare_1)[1]
compare_1dife<-compare_1[uprn!=uprn_p & uprn_source=="Energy Assessor",]
compare_1difa<-compare_1[uprn!=uprn_p & uprn_source=="Address Matched",]
dim(compare_1[uprn!=uprn_p,])[1]
#121078
dim(compare_1[uprn!=uprn_p,])[1]/dim(compare_1)[1]

dim(compare_1[uprn!=uprn_p,])[1]/dim(epcdata)[1]
#0.005539375
dim(compare_1[uprn!=uprn_p & uprn_source=="Address Matched",])[1]
#112888
dim(compare_1[uprn!=uprn_p & uprn_source=="Address Matched",])[1]/dim(compare_1[uprn!=uprn_p,])[1]
#  0.9323577
dim(compare_1[uprn!=uprn_p & uprn_source=="Energy Assessor",])[1]
# 8190
dim(compare_1[uprn!=uprn_p & uprn_source=="Energy Assessor",])[1]/dim(compare_1[uprn!=uprn_p,])[1]
#0.06764235


compare_2<-merge(linkme,mgclg_fail,by="lmk_key")
dim(compare_2)
#981295 
dim(linkme[!(linkme$lmk_key %in% mgclg_su$lmk_key),])[1]
#981295 
dim(linkme[!(linkme$lmk_key %in% mgclg_su$lmk_key),])[1]

dim(mgclg_fail[!(mgclg_fail$lmk_key %in% linkme$lmk_key),])[1]
#

dim(mgclg_fail[!(mgclg_fail$lmk_key %in% linkme$lmk_key),])[1]


dim(mgclg_fail[!(mgclg_fail$lmk_key %in% linkme$lmk_key),])[1]/dim(epcdata)[1]

fail_both<-mgclg_fail[!(mgclg_fail$lmk_key %in% linkme$lmk_key),]
dim(fail_both)
#654085
dim(linkme[!(linkme$lmk_key %in% mgclg_su$lmk_key),])[1]
#654085
dim(epcdata[epcdata$uprn_source=="" ,])

linku1<-merge(linku,epcdata_l,by="lmk_key")

epcdata_mhclg<-epcdata[,c("lmk_key","uprn_source","uprn")]


dim(mgclg_fail)[1]/dim(epcdata)[1]
#
compare_1<-merge(linkme,mgclg_su,by="lmk_key")
mhclg_win<-mgclg_su[!(mgclg_su$lmk_key %in% linkme$lmk_key), ]
dim(mhclg_win)
#152235

dim(mhclg_win)[1]/dim(compare_1)[1]
dim(mhclg_win)[1]/dim(epcdata)[1]

dim(mhclg_win[ uprn_source=="Address Matched",])[1]
#144039

dim(mhclg_win[ uprn_source=="Energy Assessor",])[1]
#  8196
###explore the difference

data_check2<-compare_1[uprn!=uprn_p,]
dim(data_check2)
#121078      6
data_check3<-linkme[!(linkme$lmk_key %in% mgclg_su$lmk_key),]
dim(data_check3)
# 981295      3

dim(data_check3)[1]/dim(epcdata)[1]
#0.04489471

head(data_check3)

d3<-link_all[link_all$lmk_key %in% data_check3$lmk_key,]
sort(unique(d3$method))


data_check4<-mhclg_win
dim(data_check4)
#152235      4
#data_check2<-compare_1[uprn!=uprn_p,]
head(data_check2)
pc<-"10024019698"
pc1<-"10004871397"
link_all[uprn==pc,]
add[uprn==pc|uprn==pc1,]

c_plist<-add[,c("uprn","parentuprn")]
head(data_check2)
dim(data_check2)
data_check2<-merge(data_check2,c_plist,by="uprn")
dim(data_check2)
#121078      7


dim(data_check2[parentuprn==uprn_p,])
# 110556   
dim(data_check2[parentuprn==uprn_p,])[1]/dim(data_check2)[1]

data_check2_par<-data_check2[parentuprn==uprn_p,]
data_check2_nopar<-data_check2[parentuprn!=uprn_p,]

head(data_check2_nopar)
head(data_check2_par)
pc<-"368089"
pc1<-"24466"
link_all[uprn==pc,]
add[uprn==pc|uprn==pc1,]
dim(c_plist)
c_plist<-unique(c_plist)
dim(c_plist)
head(c_plist)

count_p<-c_plist[parentuprn!="0",.(fcount=.N),by="parentuprn"]

count_p1<-count_p[fcount==1,]
count_p1[count_p1$parentuprn=="0",]
dim(data_check2_par[data_check2_par$parentuprn %in% count_p1$parentuprn ,])
#99209
dim(data_check2_par[data_check2_par$parentuprn %in% count_p1$parentuprn ,])[1]/dim(data_check2_par)[1]
#

in_2<-data_check2_par[!(data_check2_par$parentuprn %in% count_p1$parentuprn) ,]

head(in_2)
dim(in_2)
data_out1<-link_all[link_all$lmk_key %in% in_2$lmk_key,]
length(unique(data_out$method))
#142
rm(data_out)




head(in_2)
in_2[4,]
pc<-in_2[1,lmk_key]
in_2[lmk_key==pc,]
epcdata[lmk_key==pc,c("address1","address2","address3","address")]
head(epcdata)
epc_size<-epcdata[,c("lmk_key","total_floor_area")]
dim(linkme)
head(linkme)
linkme_size<-merge(linkme,epc_size,by="lmk_key")
dim(linkme_size)
#linkme[uprn==,]
pc<-in_2[1,lmk_key==pc,]$uprn
pc1<-in_2[1,lmk_key==pc,]$uprn_p

linkme_size[uprn=="24141",]

link_all[lmk_key=="297411936352009062813380907210762",]
in_2[lmk_key=="297411936352009062813380907210762",]
add[parentuprn=="24140",]

add[parentuprn=="57409",]
############summary the uprn difference between two methods##############

dim(compare_1[uprn==uprn_p,])[1]/dim(epcdata)[1]
#0.9126764

dim(compare_1[uprn!=uprn_p,])[1]/dim(epcdata)[1]
#0.005539375
dim(mhclg_win)[1]/dim(epcdata)[1]
#0.006964823

dim(mgclg_fail[!(mgclg_fail$lmk_key %in% linkme$lmk_key),])[1]/dim(epcdata)[1]
# 0.0299247
dim(data_check3)[1]/dim(epcdata)[1]
#0.04489471
dim(compare_1[uprn==uprn_p,])[1]+dim(compare_1[uprn!=uprn_p,])[1]+dim(mhclg_win)[1]+
  dim(mgclg_fail[!(mgclg_fail$lmk_key %in% linkme$lmk_key),])[1]+dim(data_check3)[1]
#21857699
dim(epcdata)
#21857699

#################do it autmatic####################

####
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
addall <- dbGetQuery(con,"select * from  osaddc") 

head(addall)

addall1<-addall
head(addall1)

names(addall1) <- toupper(names(addall1))
colnames(addall1)[1]<-"uprn_p"

## check the 
test<-addall[,c("locality","dependentlocality")]

dim(test[test$locality=="",])
#28842021        2
dim(test[test$dependentlocality=="",])
# 27451692        2

28842021-27451692
#
head(in_2)
dim(in_2)
in_2[, postcode.y:=NULL]
in_2[, postcode:=NULL]
in_2[, parentuprn:=NULL]

all_2<-merge(in_2,addall,by="uprn")
all_2<-merge(all_2,addall1,by="uprn_p")
dim(all_2)
head(link_all)
cc<-link_all[,c("lmk_key","property_type","add1","add2","add3","add","method")]
all_2<-merge(all_2,cc,by="lmk_key")

all_2<-merge(all_2,epc_size,by="lmk_key")

fwrite(all_2,"D:/epc_os/results/all_2.csv")


all_2_binright<-all_2[grepl("^FLAT",all_2$add),]
fwrite(all_2_binright,"D:/epc_os/results/all_2_binright.csv")
all_2_LEFT<-all_2[!grepl("^FLAT",all_2$add),]
fwrite(all_2_LEFT,"D:/epc_os/results/all_2_LEFT.csv")

dim(linkdcopy)
length(unique(linkdcopy$lmk_key))
#115855
linkdcopy<-matchleft(linkdcopy,linkd_fail)
length(unique(linkdcopy$lmk_key))
#  28252
linkdcopy<-matchleft(linkdcopy,linkdd_final)
length(unique(linkdcopy$lmk_key))
#102683
####situation three
dim(data_check4)[1]
#152235  
head(data_check4)
round(dim(data_check4)[1]/dim(epcdata)[1],8)
#
all_4<-merge(data_check4,addall1,by="uprn_p")
dim(all_4)
#134137     29
length(unique(all_4$lmk_key))
head(epcdata)
head(all_4)
cc1<-epcdata[,c("lmk_key","property_type","address1","address2","address3","address")]
  
all_4<-merge(all_4,cc1,by="lmk_key")
#134137     34
length(unique(all_4$lmk_key))
# 134137
length(unique(all_4$uprn_p))
# 104095


lost<-data_check4[!(data_check4$uprn_p %in% addall1$uprn_p),]
dim(lost)
#18098
lsot_pare<-all_4[all_4$lmk_key %in% parent$lmk_key,]
dim(all_4[all_4$lmk_key %in% parent$lmk_key,])
# 72456 
dim(lsot_pare)
#72456 
length(unique(lsot_pare$uprn_p))
#54611
length(unique(lsot_pare$lmk_key))
#72456
length(unique(all_4[all_4$lmk_key %in% linkdcopy$lmk_key,]$lmk_key))
#81888
#81888    34


length(unique(all_4$lmk_key))
#134137
lsot_left<-all_4[all_4$lmk_key %in% linkdcopy$lmk_key,]
length(unique(lsot_left$lmk_key))
#81591
length(unique(lsot_left$uprn_p))
#61463
dim(lsot_pare)
lsot_pare<-lsot_left[lsot_left$lmk_key %in% parent$lmk_key,]
dim(lsot_pare)
#72456
head(lsot_pare)

length(unique(lsot_pare$lmk_key))
#72456
#get the parent uprn of the uprn.
head(addall)
c_plist<-addall[,c("uprn","parentuprn")]

dim(parent)
#321711     35
parent_1<-merge(parent,c_plist,by="uprn")
#72456
dim(parent_1)
#321711 
length(unique(parent_1$lmk_key))
#85043
###
fwrite(parent_1,"D:/epc_os/results/linkd_parent.csv")
head(parent_1)


parent_s<-parent_1[,c("lmk_key","parentuprn")]
dim(parent_s)
#321711      2
parent_s<-unique(parent_s)
# 86856     2
colnames(parent_s)[2]<-"parentuprnchi"
head(parent_s)
dim(lsot_pare)
#72456    34
length(unique(lsot_pare$lmk_key))
#72456
head(parent_s)
lsot_pare1<-merge(lsot_pare,parent_s,by="lmk_key")
#73637    35
dim(lsot_pare1)
#73637
length(unique(lsot_pare1$lmk_key))
#72456
tt<-lsot_pare1[uprn_p==parentuprnchi,]
length(unique(tt$lmk_key))
#71449
####

lsot_pare1_no<-lsot_pare[!(lsot_pare$lmk_key %in% lsot_pare1$lmk_key),]
dim(lsot_pare1_no)
#0
##cchekc the data 
tt[,c("lmk_key","address","parentuprnchi","uprn_p")]
addall[,c("uprn","parentuprn")]



pc<-"0013f10070e67dca5dd058104cf6949ec0a67c44c6e13438877cbf9031ae299b"
epcdata[lmk_key==pc,c("address","address1","address2","address3","property_type","postcode","total_floor_area","lodgement_datetime")]
addall[uprn=="1775104582"|parentuprn=="1775104582",]
tt [lmk_key==pc,]

parent_1[lmk_key==pc,]

addpre[uprn=="1775104582"|parent_uprn=="1775104582",]

parent[lmk_key==pc,]

epcdata[lmk_key==pc,]
head(addpre)
setDT(addpre)
##159176650962008101314132802078378
pc<-"159176650962008101314132802078378"
epcdata[lmk_key==pc,c("address","address1","address2","address3","property_type","postcode","total_floor_area","lodgement_datetime")]
addall[uprn=="10008238818"|parentuprn=="10008238818",]
tt [lmk_key==pc,]




####
########
lsot_pare_left<-lsot_left[!(lsot_left$lmk_key %in% parent$lmk_key),]
dim(lsot_pare_left)
# 9135
dim(lsot_pare1)
dim(lsot_pare_left)
#9135
head(lsot_pare_left)
length(unique(lsot_pare_left$lmk_key))
#9135
lsot_pare_dif<-lsot_pare1[!(lsot_pare1$lmk_key %in% tt$lmk_key),]
dim(lsot_pare_dif)
#1269   35
length(unique(lsot_pare_dif$lmk_key))
#1007
head(lsot_pare_dif)
71449+1007=72456
#
length(unique(tt$lmk_key))
#71449
length(unique(lsot_left$lmk_key))
#81591
length(unique(lsot_parent$lmk_key))
#
lsot_pare_dif5<-lsot_pare1[uprn_p!=parentuprnchi,]
#1247   35
dim(lsot_pare_dif5)

length(unique(lsot_pare_dif5$lmk_key))
# 1102

dim(lsot_pare_dif[lsot_pare_dif$lmk_key %in% lsot_pare_dif5$lmk_key,])
#902
head(lsot_pare_dif)

lsot_pare_dif
head(linkdcopy)
cc4<-linkdcopy[,c("lmk_key","uprn")]
length(unique(lsot_pare_dif$lmk_key))
t1<-merge(lsot_pare_dif,cc4,by="lmk_key")
length(unique(t1$lmk_key))  

head(t1)


t2<-t1[(t1$uprn_p==t1$uprn),]
length(unique(t2$lmk_key))
#750

t3<-t1[!(t1$lmk_key %in% t2$lmk_key),]
length(unique(t3$lmk_key))
# 257
length(unique(t1$lmk_key))
#640
length(unique(t2$lmk_key))
#367

dim(lsot_pare_dif5[lsot_pare_dif5$lmk_key %in% lsot_pare_dif$lmk_key,])
#733
dim(lsot_pare_dif)
#1269 
head(lsot_pare_dif)
length(unique(lsot_pare_dif$lmk_key))
#1102
#(unique(lsot_pare_dif$lmk_key))
#1107

head(lsot_pare_dif)
head()
head(addall)
head(lsot_pare_dif)
colnames(lsot_pare_dif)[35]<-"uprn"
lsot_pare_difall<-merge(lsot_pare_dif,addall,by="uprn")
dim(lsot_pare_difall)
#733
cc1<-epcdata[,c("lmk_key","property_type","address1","address2","address3","address","total_floor_area","lodgement_datetime")]
lsot_pare_difall<-merge(lsot_pare_difall,cc1,by="lmk_key")
dim(lsot_pare_difall)
#733
fwrite(lsot_pare_difall,"D:/epc_os/results/lsot_pare_difall.csv")
##############
head(lsot_pare_dif[,c("lmk_key","address","uprn_p","uprn")])

addall[uprn=="24043412"|parentuprn=="24043412",]





#################################
head(lsot_pare_dif)
lsot_pare_dif1<-lsot_pare1[!(lsot_pare1$lmk_key %in% tt$lmk_key), ]
dim(lsot_pare_dif1)
#1269   35
length(unique(lsot_pare_dif1$lmk_key))
#1007

head(lsot_pare_dif1)
#check the 1007 part, can get from the parent but not exactly the same
####15 Kendal Walk 
head(lsot_pare_dif1[,c("lmk_key","uprn_p","postcode","property_type","address","parentuprnchi")],30)
#15 Kendal Walk

pc<-"019e7e8e0ed7b5e04e307a327706abf2d7b38d6b180be8502c571a417842fc00"
PC<-"125567644965201501131332309395053"
t2[t2$lmk_key==pc,]
t3[t3$lmk_key==pc,]
epcdata[lmk_key==pc,c("address","address1","address2","address3","property_type","postcode","total_floor_area","lodgement_datetime")]
addall[uprn=="72263231"|parentuprn=="72263231"|uprn=="72263232",]
lsot_pare_dif1[lsot_pare_dif1$lmk_key==pc,]
#head(linkdcopy)
linkdcopy[linkdcopy$lmk_key==pc,]
lsot_left[lsot_left$lmk_key==pc,]
addpre[uprn=="72263231"|parent_uprn=="72263231",]

#8a Cecil Street
pc<-"0aa08cf1455d448dd17a28c703a67cf1af2d2d6fc9d59bce4c2983a8770190f6"

t2[t2$lmk_key==pc,]
t3[t3$lmk_key==pc,]
epcdata[lmk_key==pc,c("address","address1","address2","address3","property_type","postcode","total_floor_area","lodgement_datetime")]

lsot_pare_dif1[lsot_pare_dif1$lmk_key==pc,]
#head(linkdcopy)
linkdcopy[linkdcopy$lmk_key==pc,]
lsot_left[lsot_left$lmk_key==pc,]
addpre[uprn=="72263231"|parent_uprn=="72263231",]
addall[uprn=="10008853999"|parentuprn=="100081287065"|uprn=="10008853997"|uprn=="100081287065",]

#####Hale Farm, Cucklington

pc<-"1020548479102013100706510215472278"

t1[t1$lmk_key==pc,]
t2[t2$lmk_key==pc,]
epcdata[lmk_key==pc,c("address","address1","address2","address3","property_type","postcode","total_floor_area","lodgement_datetime")]

lsot_pare_dif1[lsot_pare_dif1$lmk_key==pc,]
#head(linkdcopy)
linkdcopy[linkdcopy$lmk_key==pc,]
lsot_left[lsot_left$lmk_key==pc,]
addpre[uprn=="72263231"|parent_uprn=="72263231",]
addall[uprn=="30090040"|parentuprn=="30090040"|uprn=="30075707"|uprn=="30122078",]

## 135 MAIN STREET
head(lsot_pare_dif1[,c("lmk_key","uprn_p","postcode","property_type","address","parentuprnchi")],30)

pc<-"0a8775a67166ef9044ed6e71c065784e35c174583645798f57767f9897c04972"

epcdata[lmk_key==pc,c("address","address1","address2","address3","property_type","postcode","total_floor_area","lodgement_datetime")]

lsot_pare_dif1[lsot_pare_dif1$lmk_key==pc,]
#head(linkdcopy)
linkdcopy[linkdcopy$lmk_key==pc,]
lsot_left[lsot_left$lmk_key==pc,]
addpre[uprn=="72263231"|parent_uprn=="72263231",]
addall[uprn=="3040088147"|uprn=="3040092410",]


#####12 Dinglewood, Ladybrook Road, Bramhall

pc<-"1012024901732013092317260450978203"

t2[t2$lmk_key==pc,]
t3[t3$lmk_key==pc,]
epcdata[lmk_key==pc,c("address","address1","address2","address3","property_type","postcode","total_floor_area","lodgement_datetime")]

lsot_pare_dif1[lsot_pare_dif1$lmk_key==pc,]
#head(linkdcopy)
linkdcopy[linkdcopy$lmk_key==pc,]
lsot_left[lsot_left$lmk_key==pc,]
addpre[uprn=="72263231"|parent_uprn=="72263231",]

addall[uprn=="128002396"|parentuprn=="128002396",]
128002396


addall[uprn=="100012481786"|parentuprn=="100012892071",]
######7, West Pasture

pc<-"1024435679152018031606351290980312"

t2[t2$lmk_key==pc,]
t3[t3$lmk_key==pc,]
epcdata[lmk_key==pc,c("address","address1","address2","address3","property_type","postcode","total_floor_area","lodgement_datetime")]

lsot_pare_dif1[lsot_pare_dif1$lmk_key==pc,]
#head(linkdcopy)
linkdcopy[linkdcopy$lmk_key==pc,]
lsot_left[lsot_left$lmk_key==pc,]
addpre[uprn=="72263231"|parent_uprn=="72263231",]
addall[uprn=="100050454342"|uprn=="10007633302",]


###61, Caroline Street, Irlam

pc<-"1064778819432014010915203835078205"

t2[t2$lmk_key==pc,]
t3[t3$lmk_key==pc,]
epcdata[lmk_key==pc,c("address","address1","address2","address3","property_type","postcode","total_floor_area","lodgement_datetime")]

lsot_pare_dif1[lsot_pare_dif1$lmk_key==pc,]
#head(linkdcopy)
linkdcopy[linkdcopy$lmk_key==pc,]
lsot_left[lsot_left$lmk_key==pc,]
addpre[uprn=="72263231"|parent_uprn=="72263231",]
addall[uprn=="100011351323"|uprn=="10095865938",]

#############72a, Kent Road
pc<-"1306753399042015040310585236450878"


t2[t2$lmk_key==pc,]
t3[t3$lmk_key==pc,]
epcdata[lmk_key==pc,c("address","address1","address2","address3","property_type","postcode","total_floor_area","lodgement_datetime")]

lsot_pare_dif1[lsot_pare_dif1$lmk_key==pc,]
#head(linkdcopy)
linkdcopy[linkdcopy$lmk_key==pc,]
lsot_left[lsot_left$lmk_key==pc,]
addpre[uprn=="72263231"|parent_uprn=="72263231",]
addall[uprn=="100121139484"|uprn=="100121343266"|uprn==  "100121168235",]

#### Flat, 35-37 Dover Road
pc<-"1356937039962015082712113958018975"
t2[t2$lmk_key==pc,]
t3[t3$lmk_key==pc,]
epcdata[lmk_key==pc,c("address","address1","address2","address3","property_type","postcode","total_floor_area","lodgement_datetime")]

lsot_pare_dif1[lsot_pare_dif1$lmk_key==pc,]
#head(linkdcopy)
linkdcopy[linkdcopy$lmk_key==pc,]
lsot_left[lsot_left$lmk_key==pc,]
addpre[uprn=="72263231"|parent_uprn=="72263231",]
addall[uprn=="100060927423"|uprn=="10012028371",]


######## 173, Harvington Road
head(t3)
#173, HARVINGTON ROAD
pc<-"1009001299942013092301050313372208"
t2[t2$lmk_key==pc,]
t3[t3$lmk_key==pc,]
epcdata[lmk_key==pc,c("address","address1","address2","address3","property_type","postcode","total_floor_area","lodgement_datetime")]

lsot_pare_dif1[lsot_pare_dif1$lmk_key==pc,]
#head(linkdcopy)
linkdcopy[linkdcopy$lmk_key==pc,]
lsot_left[lsot_left$lmk_key==pc,]
addpre[uprn=="72263231"|parent_uprn=="72263231",]
addall[uprn=="100070395406"|uprn=="100070395405"|uprn=="100071449881",]

###173, Harvington Road 


PC<-"1014892991732013092616241652278700"
t2[t2$lmk_key==pc,]
t3[t3$lmk_key==pc,]
epcdata[lmk_key==pc,c("address","address1","address2","address3","property_type","postcode","total_floor_area","lodgement_datetime")]

lsot_pare_dif1[lsot_pare_dif1$lmk_key==pc,]
#head(linkdcopy)
linkdcopy[linkdcopy$lmk_key==pc,]
lsot_left[lsot_left$lmk_key==pc,]
addpre[uprn=="72263231"|parent_uprn=="72263231",]
addall[uprn=="100070395406"|uprn=="100070395405"|uprn=="100071449881",]

#####54, Milton Road
pc<-"1101959959042014030616215224040968"

t2[t2$lmk_key==pc,]
t3[t3$lmk_key==pc,]
head(t3)
dim(t3)
epcdata[lmk_key==pc,c("address","address1","address2","address3","property_type","postcode","total_floor_area","lodgement_datetime")]

lsot_pare_dif1[lsot_pare_dif1$lmk_key==pc,]
#head(linkdcopy)
linkdcopy[linkdcopy$lmk_key==pc,]
lsot_left[lsot_left$lmk_key==pc,]
addpre[uprn=="72263231"|parent_uprn=="72263231",]
addall[uprn=="24010621"|uprn=="24153154"|uprn=="24140846"|uprn=="24140845"|uprn=="24140840"|uprn=="24140842"|uprn=="24140841"|uprn=="24140844"|uprn=="24140843"|uprn=="24154141"|uprn=="24154142"|uprn=="24154143",]

####76, Brudenell Road

pc<-"120393280262008100213454148968398"

t2[t2$lmk_key==pc,]
t3[t3$lmk_key==pc,]
epcdata[lmk_key==pc,c("address","address1","address2","address3","property_type","postcode","total_floor_area","lodgement_datetime")]

lsot_pare_dif1[lsot_pare_dif1$lmk_key==pc,]
#head(linkdcopy)
linkdcopy[linkdcopy$lmk_key==pc,]
lsot_left[lsot_left$lmk_key==pc,]
addpre[uprn=="72263231"|parent_uprn=="72263231",]
addall[uprn=="72031851"|uprn=="72772360"|uprn=="72772361"|uprn=="72772359"|uprn=="72772362"|uprn=="72772363"|uprn=="72721807",]


###1 25 CAMERON ROAD, CROYDON
pc<-"e21ae3b3ef3784553e7ba1e3ba6642ff0d6952ac238753814544b6d9472c1b52"
t2[t2$lmk_key==pc,]
t3[t3$lmk_key==pc,]
epcdata[lmk_key==pc,c("address","address1","address2","address3","property_type","postcode","total_floor_area","lodgement_datetime")]

lsot_pare_dif1[lsot_pare_dif1$lmk_key==pc,]
#head(linkdcopy)
linkdcopy[linkdcopy$lmk_key==pc,]
lsot_left[lsot_left$lmk_key==pc,]
addpre[uprn=="72263231"|parent_uprn=="72263231",]
addall[uprn=="100020587225"|uprn=="100020587195"|uprn=="100020587193"|uprn=="100020587192"|uprn=="100020587197"|uprn=="100020587196"|uprn=="100020587194"|uprn=="100023639899"|uprn=="100023639903",]
addall[uprn=="100020587225",]
###
length(unique(lsot_pare_left$lmk_key))
# 9135
length(unique(lsot_pare_left$uprn_p))
#6894
linkdcopy[linkdcopy$lmk_key==pc,]
#288

lsot_left1<-all_4[!(all_4$lmk_key %in% linkdcopy$lmk_key),]
length(unique(lsot_left1$lmk_key))
#52546
length(unique(lsot_left1$uprn_p))
#42999
dim(parent)
head(lsot_left1)

cc1<-epcdata[,c("lmk_key","property_type","address1","address2","address3","address","total_floor_area","lodgement_datetime")]
dim(lsot_left1)
#52546
lsot_left1<-merge(lsot_left1,cc1,by="lmk_key")
dim(lsot_left1)
#52546    41
fwrite(lsot_left1,"D:/epc_os/results/lsot_left1_dluhc.csv")

sta_lsot_left1<-lsot_left1[,.(fcount=.N),by="uprn_source"]
sta_lsot_left1

dim(lsot_left1[(lsot_left1$uprn_p %in% addall$uprn),])
#


parent_1<-merge(parent,c_plist,by="uprn")
#72456
head(parent_1)
dim(parent_1)
parent_s<-parent_1[,c("lmk_key","parentuprn")]
dim(parent_s)
#321711      2
parent_s<-unique(parent_s)
# 86856     2
colnames(parent_s)[2]<-"parentuprnchi"
head(parent_s)
dim(lsot_pare)
#72456    34
length(unique(lsot_pare$lmk_key))
#72456
head(parent_s)
lsot_pare1<-merge(lsot_pare,parent_s,by="lmk_key")
#73637    35
dim(lsot_pare1)
length(unique(lsot_pare1$lmk_key))

head(lsot_pare1)
tt<-lsot_pare1[uprn_p==parentuprnchi,]
length(unique(tt$lmk_key))
#71449

head(tt)
####check tt#######
head(parent)
tt2<-epcdata[epcdata$lmk_key %in% tt$lmk_key,]
#108 STUBBINGTON AVENUE
head(epcdata)
epcdata[lmk_key=="0013f10070e67dca5dd058104cf6949ec0a67c44c6e13438877cbf9031ae299b",c("address","address1","address2","address3","property_type","postcode")]
addall[uprn=="1775104582"|parentuprn=="1775104582",]
parent[lmk_key=="0013f10070e67dca5dd058104cf6949ec0a67c44c6e13438877cbf9031ae299b",]
lsot_pare1 [lmk_key=="0013f10070e67dca5dd058104cf6949ec0a67c44c6e13438877cbf9031ae299b",]

#64A TOLWORTH BROADWAY, TOLWORTH
pc<-"00161daaa3367510f9f08e44980400a2d93ddafc0c8e88824723ae46ce30dd9d"
epcdata[lmk_key==pc,c("address","address1","address2","address3","property_type","postcode")]
addall[uprn=="128002396"|parentuprn=="128002396",]
parent[lmk_key==pc,]
lsot_pare1 [lmk_key==pc,]
epcdata[lmk_key==pc,]
head(addpre)
setDT(addpre)
addpre[uprn=="128002396"|parent_uprn=="128002396",]





######


pc<-"159176650962008101314132802078378"
epcdata[lmk_key==pc,c("address","address1","address2","address3","property_type","postcode")]
addall[uprn=="10008238818"|parentuprn=="10008238818",]
parent[lmk_key==pc,]
lsot_pare1 [lmk_key==pc,]
epcdata[lmk_key==pc,]
length(unique(lsot_pare$lmk_key))
lsot_pare_dif<-lsot_pare1[uprn_p!=parentuprnchi,]
head(lsot_pare_dif)
lsot_pare_dif1<-lsot_pare1[!(lsot_pare1$lmk_key %in% tt$lmk_key), ]
length(unique(lsot_pare_dif1$lmk_key))
#1007

######


pc<-"159176650962008101314132802078378"
epcdata[lmk_key==pc,c("address","address1","address2","address3","property_type","postcode")]
addall[uprn=="10008238818"|parentuprn=="10008238818"|uprn=="10008286620",]
parent[lmk_key==pc,]
lsot_pare1 [lmk_key==pc,]
epcdata[lmk_key==pc,]
length(unique(lsot_pare$lmk_key))
lsot_pare_dif<-lsot_pare1[uprn_p!=parentuprnchi,]
head(lsot_pare_dif)
lsot_pare_dif1<-lsot_pare1[!(lsot_pare1$lmk_key %in% tt$lmk_key), ]
length(unique(lsot_pare_dif1$lmk_key))
#1007
addall[uprn=="10008286620",]
addpre[uprn=="10008238818"|parent_uprn=="10008238818"|uprn=="10008286620",]
#10008286620
### Flat, 44-46 Kenway Road

pc<-"391198150762009103116522119108591"
epcdata[lmk_key==pc,c("address","address1","address2","address3","property_type","postcode")]
addall[uprn=="217046162"|parentuprn=="217046162"|uprn=="217046162",]
parent[lmk_key==pc,]
lsot_pare1 [lmk_key==pc,]
epcdata[lmk_key==pc,]
length(unique(lsot_pare$lmk_key))
lsot_pare_dif<-lsot_pare1[uprn_p!=parentuprnchi,]
head(lsot_pare_dif)
lsot_pare_dif1<-lsot_pare1[!(lsot_pare1$lmk_key %in% tt$lmk_key), ]
length(unique(lsot_pare_dif1$lmk_key))
#1007
addpre[uprn=="217046162"|parent_uprn=="217046162"|uprn=="217046162",]


#First Floor Flat, 101, St. Albans Road

pc<-"1774712532202020010313430461800978"

epcdata[lmk_key==pc,c("address","address1","address2","address3","property_type","postcode")]
addall[uprn=="10090229771"|parentuprn=="10090229771"|uprn=="100081282332",]
parent[lmk_key==pc,]
lsot_pare1 [lmk_key==pc,]
epcdata[lmk_key==pc,]
length(unique(lsot_pare$lmk_key))
lsot_pare_dif<-lsot_pare1[uprn_p!=parentuprnchi,]
head(lsot_pare_dif)
lsot_pare_dif1<-lsot_pare1[!(lsot_pare1$lmk_key %in% tt$lmk_key), ]
length(unique(lsot_pare_dif1$lmk_key))
#1007
addpre[uprn=="10090229771"|parent_uprn=="10090229771"|uprn=="100081282332",]

#################
pc<-"203740151412009050620302401010851"



pc1<-"203740112022020070818214916788980"
epcdata[lmk_key==pc |lmk_key==pc1,c("address","address1","address2","address3","property_type","postcode","total_floor_area","lodgement_datetime")]
addall[uprn=="100091647631"|parentuprn=="100091647631",]
parent[lmk_key==pc |lmk_key==pc1,]
lsot_pare1 [lmk_key==pc |lmk_key==pc1,]
epcdata[lmk_key==pc |lmk_key==pc1,]

head(lsot_pare_dif1)
###########################################################
length(unique(tt$lmk_key))
# 71449
lsot_pare_dif1<-lsot_pare1[!(lsot_pare1$lmk_key %in% tt$lmk_key), ]
dim(lsot_pare_dif)
#2188

linkdcopy<- dbGetQuery(con,"select * from  linkd2") 
dim(linkdcopy)
length(unique(linkdcopy$lmk_key))
#115855

lsot_pare_dif3<-lsot_pare_dif1[lsot_pare_dif1$lmk_key %in% linkdcopy$lmk_key,]
length(unique(lsot_pare_dif3$lmk_key))
#1007
length(unique(lsot_pare_dif3$uprn_p))
#822

lsot_pare_dif4<-lsot_pare_dif1[!(lsot_pare_dif1$lmk_key %in% linkdcopy$lmk_key),]

length(unique(lsot_pare_dif4$lmk_key))
#0
length(unique(lsot_pare_dif4$uprn_p))
#0
rm(lsot_pare_dif4)



dim(lsot_pare_dif1[lsot_pare_dif1$parentuprnchi==0,])
#536 
addpre[uprn=="100091647631"|parent_uprn=="100091647631",]



pc<-"194548370242008122313415551482328"


epcdata[lmk_key==pc,c("address","address1","address2","address3","property_type","postcode")]
addall[uprn=="10024002423"|uprn=="100041195567",]
parent[lmk_key==pc,]
lsot_pare_dif1 [lmk_key==pc,]
epcdata[lmk_key==pc,]
length(unique(lsot_pare$lmk_key))
lsot_pare_dif<-lsot_pare1[uprn_p!=parentuprnchi,]
head(lsot_pare_dif)
lsot_pare_dif1<-lsot_pare1[!(lsot_pare1$lmk_key %in% tt$lmk_key), ]
length(unique(lsot_pare_dif1$lmk_key))
#1007
addall[uprn=="10008286620",]
addpre[uprn=="10008238818"|parent_uprn=="10008238818"|uprn=="10008286620",]
#####
dim(lsot_pare_left)
lenght(unique(lsot_pare_left$lmk_key))
head(lsot_pare_left)
pc<-"1009001299942013092301050313372208"
t2[t2$lmk_key==pc,]
t3[t3$lmk_key==pc,]
epcdata[lmk_key==pc,c("address","address1","address2","address3","property_type","postcode","total_floor_area","lodgement_datetime")]

lsot_pare_dif1[lsot_pare_dif1$lmk_key==pc,]
#head(linkdcopy)
linkdcopy[linkdcopy$lmk_key==pc,]
lsot_left[lsot_left$lmk_key==pc,]
addpre[uprn=="72263231"|parent_uprn=="72263231",]
addall[uprn=="100070395406"|uprn=="100070395405"|uprn=="100071449881",]

###########################lsot_pare_left 9135##########################################
head(lsot_pare_left)
head(linkdcopy)
cd1<-lsot_pare_left[,c("lmk_key","uprn_p")]
cd2<-linkdcopy[,c("lmk_key","uprn")]
setDT(cd1) 
setDT(cd1)
dim(cd1)
dim(cd2)
cd1<-unique(cd1)
cd2<-unique(cd2)
dim(cd1)
#9135    2
dim(cd2)
#23795     2
dim(cd2)
cd2<-cd2[cd2$lmk_key %in% cd1$lmk_key,]
dim(cd2)
#23795 
length(unique(cd2$lmk_key))
#9135
length(unique(cd1$lmk_key))
#9135

cd1_2<-merge(cd1,cd2,by="lmk_key")
dim(cd1_2)
head(cd1_2)
cd1_2same<-cd1_2[cd1_2$uprn==cd1_2$uprn_p,]
length(unique(cd1_2same$lmk_key))
# 1332
cd1_2samedata<-cd1_2[cd1_2$lmk_key %in% cd1_2same$lmk_key,]
cd1_2difdata<-cd1_2[!(cd1_2$lmk_key %in% cd1_2same$lmk_key),]

length(unique(cd1_2samedata$lmk_key))
length(unique(cd1_2difdata$lmk_key))

dim(cd1_2samedata)
dim(cd1_2difdata)
#20809    11
c_plist
cd1_2difdata<-merge(cd1_2difdata,c_plist,by="uprn")
head(c_plist)
dim(cd1_2difdata)

#20809    12
head(cd1_2difdata)
dim(cd1_2difdata)
cd1_2difdata_p<-cd1_2difdata[cd1_2difdata$uprn_p==cd1_2difdata$parentuprn,]
length(unique(cd1_2difdata_p$lmk_key))
#7245
dim(cd1_2difdata_p)
#17971    12

cd1_2difdata_left<-cd1_2difdata[!(cd1_2difdata$lmk_key %in% cd1_2difdata_p$lmk_key), ]
  
#cd1_2difdata[cd1_2difdata$uprn_p!=cd1_2difdata$parentuprn,]
length(unique(cd1_2difdata_left$lmk_key))
# 558

#cd1_2samedata link with other

med<-epcdata[,c("lmk_key","uprn_source","property_type","address1","address2","address3","address","total_floor_area","lodgement_datetime")]
cd1_2samedata<-merge(cd1_2samedata,med,by="lmk_key")
cd1_2difdata<-merge(cd1_2difdata,med,by="lmk_key")


length(unique(cd1_2samedata$lmk_key))
#1332
length(unique(cd1_2difdata$lmk_key))
#7803
###############try the cd1_2samedata part###################
head(
  cd1_2samedata[uprn_source=="Address Matched",],40)
head(
  cd1_2difdata[uprn_source=="Address Matched",],40)
san<- cd1_2samedata[uprn_source=="Address Matched",]
head(san,60)
#####6 SOUTH AVENUE, BUXTON
pc<-"005e4dab10000a91ce790eaeff6dc52b0d9883ba538469385a04426afda85277"

linkdcopy[linkdcopy$lmk_key==pc,]
lsot_left[lsot_left$lmk_key==pc,]


cd1_2samedata[cd1_2samedata$lmk_key==pc,]

lsot_left[lsot_left$lmk_key==pc,]

epcdata[lmk_key==pc,c("address","address1","address2","address3","property_type","postcode","total_floor_area","lodgement_datetime")]

#lsot_pare_dif1[lsot_pare_dif1$lmk_key==pc,]
#head(linkdcopy)
linkdcopy[linkdcopy$lmk_key==pc,]
lsot_left[lsot_left$lmk_key==pc,]
addpre[uprn=="72263231"|parent_uprn=="72263231",]
addall[uprn=="10010715867"|uprn=="10010741684",]


#Plas Bychan, Church Road, Minera
pc<-"100240896732013060509435933068209"


linkdcopy[linkdcopy$lmk_key==pc,]
lsot_left[lsot_left$lmk_key==pc,]
epcdata[lmk_key==pc,c("address","address1","address2","address3","property_type","postcode","total_floor_area","lodgement_datetime")]
addall[uprn=="200001748173"|uprn=="10070386268",]
##Flat Telegraph House, Wolverhampton Road
pc<-"1032107499222013102512325285648967"
linkdcopy[linkdcopy$lmk_key==pc,]
lsot_left[lsot_left$lmk_key==pc,]
epcdata[lmk_key==pc,c("address","address1","address2","address3","property_type","postcode","total_floor_area","lodgement_datetime")]
addall[uprn=="10024534434"|uprn=="10093136635",]
###103577319612012032220361898220742
#11, Adam & Eve Mews
pc<-"103577319612012032220361898220742"

linkdcopy[linkdcopy$lmk_key==pc,]
lsot_left[lsot_left$lmk_key==pc,]
epcdata[lmk_key==pc,c("address","address1","address2","address3","property_type","postcode","total_floor_area","lodgement_datetime")]
addall[uprn=="217101190"|uprn=="217101191",]
#Gravenhunger Hall, Gravenhunger Lane, Woore

pc<-"1037145349262013110412334435448277"
linkdcopy[linkdcopy$lmk_key==pc,]
lsot_left[lsot_left$lmk_key==pc,]
epcdata[lmk_key==pc,c("address","address1","address2","address3","property_type","postcode","total_floor_area","lodgement_datetime")]
addall[uprn=="10013136705"|uprn=="10014524857",]


###############try the cd1_2difdata part###################
# head(cd1_2difdata_p)
#   cd1_2difdata[uprn_source=="Address Matched",],40)

san2<-cd1_2difdata[uprn_source=="Address Matched",]
View(san2)
head(cd1_2difdata)
head(cd1_2difdata)


#OLD PLACE FARM, LITTLE TRODGERS LANE, MAYFIELD
pc<-"0061d682a46dd5610f23a95c73b5b0a5d63e6aa2fbf264331022f4bee7972ddb"
cd1_2difdata[cd1_2difdata$lmk_key==pc,]
linkdcopy[linkdcopy$lmk_key==pc,]
lsot_left[lsot_left$lmk_key==pc,]
epcdata[lmk_key==pc,c("address","address1","address2","address3","property_type","postcode","total_floor_area","lodgement_datetime")]
addall[uprn=="100062555547"|uprn=="100062555549"|uprn=="100062555580",]


# Platt Farm, Whitchurch Road, Prees
#1000671439542014032212243017342408
pc<-"1000671439542014032212243017342408"

cd1_2difdata[cd1_2difdata$lmk_key==pc,]
linkdcopy[linkdcopy$lmk_key==pc,]
lsot_left[lsot_left$lmk_key==pc,]
epcdata[lmk_key==pc,c("address","address1","address2","address3","property_type","postcode","total_floor_area","lodgement_datetime")]
addall[uprn=="10013133206"|uprn=="10014529454"|uprn=="10014538174",]

#Flat, 10 Bellevue Crescent 

pc<-"770466809222018062121300337498138"
linkdcopy[linkdcopy$lmk_key==pc,]
lsot_left[lsot_left$lmk_key==pc,]
epcdata[lmk_key==pc,c("address","address1","address2","address3","property_type","postcode","total_floor_area","lodgement_datetime")]
addall[uprn=="107602"|parentuprn=="107602",]


#6 SOUTH AVENUE, BUXTON
pc<-"005e4dab10000a91ce790eaeff6dc52b0d9883ba538469385a04426afda85277"
t2[t2$lmk_key==pc,]
t3[t3$lmk_key==pc,]
epcdata[lmk_key==pc,c("address","address1","address2","address3","property_type","postcode","total_floor_area","lodgement_datetime")]

lsot_pare_dif1[lsot_pare_dif1$lmk_key==pc,]
#head(linkdcopy)
linkdcopy[linkdcopy$lmk_key==pc,]
lsot_left[lsot_left$lmk_key==pc,]
addpre[uprn=="72263231"|parent_uprn=="72263231",]
addall[uprn=="10010715867"|uprn=="10010741684"|uprn=="100071449881",]

#######cd1_2difdata_left
head(cd1_2difdata_left,50)
unique(cd1_2difdata_lef)
#Flat 8, The Refinery, Jacob Street

pc<-"29443570302008100400153059180898"

epcdata[lmk_key==pc,c("address","address1","address2","address3","property_type","postcode","total_floor_area","lodgement_datetime")]

#lsot_pare_dif1[lsot_pare_dif1$lmk_key==pc,]
#head(linkdcopy)
linkdcopy[linkdcopy$lmk_key==pc,]
lsot_left[lsot_left$lmk_key==pc,]
addall[uprn=="371656"|uprn=="318360"|uprn=="311133",]


#Flat 4, 72 Shoot up Hill
pc<-"1206194951432014100910132064278900"
epcdata[lmk_key==pc,c("address","address1","address2","address3","property_type","postcode","total_floor_area","lodgement_datetime")]

#lsot_pare_dif1[lsot_pare_dif1$lmk_key==pc,]
#head(linkdcopy)
linkdcopy[linkdcopy$lmk_key==pc,]
lsot_left[lsot_left$lmk_key==pc,]
addall[uprn=="5175137"|uprn=="5198126"|uprn=="5003618",]

#Flat 1, Haydon House, 44-46, Uxbridge Road
#method 281 has issue
pc<-"69360479642013090614525541470378"
epcdata[lmk_key==pc,c("address","address1","address2","address3","property_type","postcode","total_floor_area","lodgement_datetime")]

#lsot_pare_dif1[lsot_pare_dif1$lmk_key==pc,]
#head(linkdcopy)
linkdcopy[linkdcopy$lmk_key==pc,]
lsot_left[lsot_left$lmk_key==pc,]
addall[uprn=="12128561"|uprn=="12017152"|uprn=="12001447"|uprn=="12001453"|uprn=="12001459"|uprn=="12001441",]
setDT(linkdcopy)
linkdcopy[uprn=="12017152"|uprn=="12001447"|uprn=="12001453"|uprn=="12001459"|uprn=="12001441",]


#Bruges Place, 12, Baynes Street
pc<-"282366141912019100710395299019365"
epcdata[lmk_key==pc,c("address","address1","address2","address3","property_type","postcode","total_floor_area","lodgement_datetime")]
linkdcopy[linkdcopy$lmk_key==pc,]
lsot_left[lsot_left$lmk_key==pc,]
addall[uprn=="12128561"|uprn=="12017152"|uprn=="12001447"|uprn=="12001453"|uprn=="12001459"|uprn=="12001441",]


linkdcooy_issue<-linkdcopy[linkdcopy$lmk_key %in% cd1_2difdata_left$lmk_key,]
unique(linkdcooy_issue$method)
#
  length( unique(linkdcooy_issue$method))
#43  
#34, Woodstock, Billing Road  
pc<-"278349150602009050523551968110758"  
#1206294161432014100909592574278905
  
epcdata[lmk_key==pc,c("address","address1","address2","address3","property_type","postcode","total_floor_area","lodgement_datetime")]
linkdcopy[linkdcopy$lmk_key==pc,]
lsot_left[lsot_left$lmk_key==pc,]
addall[uprn=="15039065"|uprn=="15134185"|uprn=="15140097"|uprn=="10095796592"|uprn=="15139172"|uprn=="15134186",]


  
#################################################

tt2<-lsot_pare1[uprn_p!=parentuprnchi,]
length(unique(tt2$uprn_p))
head(lsot_pare_left)

lsot_pare_dif2<-lsot_pare_dif1[lsot_pare_dif1$parentuprnchi!=0,]

#BRIDGE MILL FLATS,WORLINGTON ROAD, MILDENHALL
dim(lsot_pare_dif)
#2188
fwrite(lsot_pare_dif,"D:/epc_os/results/lsot_pare_dif.csv")
linkdcopy[linkdcopy$lmk_key==pc,]
#######################################################
dim(data_check3)

ubdc_win<-merge(data_check3, addall   ,by="uprn")

ubdc_win<-merge(ubdc_win, cc   ,by="lmk_key")

ubdc_win<-merge(ubdc_win,epc_size,by="lmk_key")

dim(ubdc_win)
#981295     35

fwrite(ubdc_win,"D:/epc_os/results/ubdc_win.csv")


library(DBI)
library(RPostgreSQL)
#DBI::dbDriver('PostgreSQL')
require(RPostgreSQL)
drv=dbDriver("PostgreSQL")
db <- "os"
host_db <- "localhost"
db_port <- "5432"
db_user <- "postgres"
db_password <- "232323"
con <- dbConnect(RPostgres::Postgres(), dbname = db, host=host_db, port=db_port, user=db_user, password=db_password)
addpre <- dbGetQuery(con,"select * from  abp_blpu") 
head(lost)
dim(lost)
#18098
length(unique(lost$uprn_p))
#14441

add_lost<-addpre[addpre$uprn %in% lost$uprn_p ,]
dim(add_lost)
#14240 
dim(add_lost[!is.na(add_lost$end_date),])
#14023 

length(unique(lost[(lost$uprn_p %in% addpre$uprn),]$postcode))

dim(lost[(lost$uprn_p %in% addpre$uprn),])
#
dim(lost[lost$uprn_p %in% addpre$uprn,])
notexitt<-lost[!(lost$uprn_p %in% addpre$uprn),]
dim(notexitt)
#240
length(unique(notexitt$uprn_p))
#201

osopenuprn<-fread("D:/data/osopenuprn_202202.csv")
head(osopenuprn)
dim(notexitt[notexitt$uprn_p %in% osopenuprn$UPRN,])
#0
dim(notexitt[!(notexitt$uprn_p %in% osopenuprn$UPRN),])
#240
head(notexitt)
str(notexitt)
str(osopenuprn)
head(osopenuprn)
uprn_noexit<-notexitt[!(notexitt$uprn_p %in% osopenuprn$UPRN),]
#
head(uprn_noexit)
dim(uprn_noexit)
uprn_noexit_data<-epcdata[epcdata$lmk_key %in% uprn_noexit$lmk_key,]
dim(uprn_noexit_data)
rm(osopenuprn,uprn_noexit)
fwrite(uprn_noexit_data,"D:/epc_os/results/uprn_noexit_DLUHC.csv")
#osopenuprn[UPRN=="100012426784",]
#map for failed part#
#map for successfully part




unique(link_all$method)
head(link_all[link_all$method=="link181u",],80)
c<-link_all[link_all$method=="link202u",]
c<-link_all[link_all$add3!="",]
c<-epcdata[grepl(",,",  epcdata$address),c("address1","address2","address3","address")]
class(c)
c[,  `:=`(add1 = toupper(address1),
            add2 = toupper(address2),
            add3 = toupper(address3),
            add = toupper(address))]

c<-epcdata[grepl("[.]",  epcdata$address),c("address1","address2","address3","address")]
c<-epcdata[grepl("[']",  epcdata$address),c("address1","address2","address3","address")]
c<-epcdata[grepl("[/]",  epcdata$address),c("address1","address2","address3","address")]

getwd()
fwrite(c,"D:/epc_os/results/c_binfind.csv")

#################match rate for each la############################

nspl<-fread("D:/robin/NSPL_NOV_2021_UK.csv")
head(nspl)
head(epcdata)

nspl<-nspl[,c("pcds","lsoa11","msoa11","laua","ctry","ttwa","imd")]
epcdatal<-epcdata[,c("lmk_key","postcode")]

#unique(nspl$ctry)
#E92000001 = England;
# W92000004 = Wales;
# S92000003 = Scotland;
# N92000002 = Northern Ireland;
# L93000001 = Channel Islands;
# M83000003 = Isle of Man

colnames(nspl)[1]<-"postcode"
epcdata1<-merge(epcdatal,nspl,by="postcode")
dim(epcdata)[1]-dim(epcdata1)[1]
#11769

dim(link_all)
#21051379       35

epc_2<-epcdata1[epcdata1$lmk_key %in% link_all$lmk_key,]



matchre<-epcdata1[, .(count=.N), by=laua]
matchl<-epc_2[, .(countl=.N), by=laua]


head(matchl)
head(matchre)
match_re1<-merge(matchre,matchl,by="laua")
match_re1<-match_re1[match_re1$laua!="",]
head(match_re1)
match_re1$pro<-(match_re1$countl/match_re1$count)

fwrite(match_re1,"D:/epc_os/results/match_re1.csv")

summary(match_re1$pro)


#####get one example for the full report##


