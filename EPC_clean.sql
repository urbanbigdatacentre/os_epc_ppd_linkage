# ------------------------------------------------
#EPC_clean.sql
# ------------------------------------------------
# Code provided as is and can be used or modified freely. 
# ------------------------------------------------
# Author: BIN CHI
# Urban Big Data Centre at University of Glasgow
# Bin.Chi@glasgow.ac.uk
# Date: 1/4/2022

########clean os addressplus
#create geom field
SELECT AddGeometryColumn ('public', 'osadd', 'geom', 27700, 'POINT', 2);
UPDATE public.osadd SET geom = ST_GeomFromText('POINT(' || xcoordinate || ' ' || ycoordinate || ') ', 27700);

#create a new table only records the address fields for the afterwards address matching
SELECT uprn,parentuprn,postcodelocator,class,postcode,buildingname,buildingnumber,subbuildingname,paostartnumber,paostartsuffix,paoendnumber,paoendsuffix,paotext,saostartnumber,saostartsuffix,saoendnumber,saoendsuffix,saotext,streetdescription,ostopotoid,dependentlocality,locality,townname,administrativearea,posttown,geom INTO osaddc FROM osadd; 

#create index on two fields(uprn and parentuprn)
CREATE UNIQUE INDEX uprn_idx ON osaddc(uprn);
CREATE INDEX puprn_idx ON osaddc(parentuprn);
#create a new table as pare recording the pareuprn which exit in uprn field
SELECT DISTINCT parentuprn INTO pare FROM osaddc WHERE parentuprn in (select uprn from osaddc);
#add a new field in pare and set the value to 1
ALTER TABLE pare ADD exit int;
update pare set exit=1;

#create unique index on parentuprn in table pare
CREATE UNIQUE INDEX puprn_idx1 ON pare(parentuprn);
#change the parentuprn field name to parentuprn1 in pare table
alter table pare rename column parentuprn to parentuprn1;

#identify uprn which is pareuprn in OS AddressBase
select * into addressgb from osaddc left join pare on osaddc.uprn=pare.parentuprn1;
#SELECT 37385807 Query returned successfully in 2 min 43 secs.
#drop the parentuprn1 field
alter table addressgb drop column parentuprn1;

#remove the uprn which is the pareuprn in OS AddressBase Plus dataset
DELETE FROM addressgb WHERE exit=1;

#delete the unuseful field(exit)
alter table  addressgb drop column exit;

#create three new address fields for the afterwards address matching
ALTER TABLE addressgb  ADD bb text;  
update addressgb  set bb=concat(buildingname,', ',buildingnumber); 

ALTER TABLE addressgb  ADD ss text;  
update addressgb  set ss=concat(saostartnumber,saostartsuffix); 

ALTER TABLE addressgb  ADD pp text;  
update addressgb  set pp=concat(paostartnumber,paostartsuffix); 

#addressgb is the data used for address matching