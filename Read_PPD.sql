# ------------------------------------------------
# Read_PPD.sql
# ------------------------------------------------
# Code provided as is and can be used or modified freely. 
# ------------------------------------------------
# Author: BIN CHI
# Urban Big Data Centre at University of Glasgow
# Bin.Chi@glasgow.ac.uk
# Date: 1/4/2022


#Create a pricepaid table
CREATE TABLE pricepaid
(
  transactionid text NOT NULL,
  price bigint,
  dateoftransfer date,
  postcode text,
  propertytype text,
  oldnew text,
  duration text,
  paon text,
  saon text,
  street text,
  locality text,
  towncity text,
  district text,
  county text,
  categorytype text,
  recordstatus text
)

#Read in the Land Registry PPD dataset 
COPY pricepaid FROM 'D:/OS_Data/PPD/pp-complete.csv' DELIMITERS ',' CSV QUOTE '"';

