# Linking Domestic Energy Performance Certificates (EPCs) and and Land Registry Price Paid Data (PPD) with UPRN


This repository contains the code for OS short term project. This reserach aims to explore how easy it is to attribute UPRNs to address information, looking at two property datasets in England and Wales, one is Domestic EPCs and the other is Land Registry PPD. This project is licensed under the terms of the [Creative Commons Attribution-NonCommercial (CC-BY-NC)](https://creativecommons.org/licenses/by-nc/4.0/). This whole research is **not** allowed to be used  **commercially**. 


## Repository contents
* [EPC/](EPC/): R files for conducting the address matching bewtween Domestic EPCs and OS AddressBase Plus dataset
* [PPD/](PPD/): R code for conducting the address matching bewtween Land Registry PPD and OS AddressBase Plus dataset
* [pic/](pic/): Additional figures for the readme file in this repositry.

* [EPC_clean.sql](EPC_clean.sql): SQL code for cleaning the EPC data before address matching
* [README.md](README.md): A description of the whole data linkage work.
* [Read_EPC.R](Read_EPC.R): R code for fast reading multiple EPC csv files together and then saveing to PostGIS database
* [Read_OSadd.R](Read_OSadd.R): R code for reading OS AddressBase Plus data and then saveing to PostGIS database
* [Read_PPD.sql](Read_PPD.sql): SQL code for reading the whole Land Registry PPD in PostGIS database
* [lookup_1.csv](lookup_1.csv): Additional csv files for conducting the address matching work


## 1. Set up

The whole rearach are mainly conducted in R, with data inputs and outputs stored in a PostGIS database. Figure 1 displays the whole workflow.

![](pic/f1.png)

**Figure 1.**  The overall workflow of this project

### 1.1 Create a new spatial database in PostgreSQL
This research created a new PostGIS database and named it as **os_ubdc** (https://postgis.net/workshops/postgis-intro/creating_db.html). Here, the password of postgres is assumed to be **654321**.
### 1.2 Set up the working directory for R
The process for setting the working directory is listed below:
- Create a directory named "OS_Data" on your D: drive.
- Create a sub-directory named "e90_ab_plus_csv_gb" in "OS_Data" folder.This e90_ab_plus_csv_gb folder stores OS AdddressBase Plus (AB_Plus_Data.csv).
- Create a sub-directory named "EPC" in "OS_Data" folder. This EPC folder stores whole list of Domestic EPC dataset.
- Create a sub-directory named "PPD" in "OS_Data" folder. This PPD folder stores Land Registry PPD(pp-complete.csv).
### 1.3 Read data in PostGIS database
Three datasets are need to read in PostGIS before conduct the data linakge work.This is achieve by running [Read_EPC.R](Read_EPC.R),[Read_OSadd.R](Read_OSadd.R) and [Read_PPD.sql](Read_PPD.sql).

For OS AddressBase Plus data in the PostGIS database, we further removed the uprn record which is the also a parentuprn in AddressBase dataset.

## 2. Run Data linkage process
The main data linkage process is conducted in R. Below we sepeate introduce the 
### 2.1 Attaching UPRN in Domestic EPCs
The tagging UPRN work for Domestic EPCs   

![](pic/f2.png)
**Figure 2.** A Brief workflow of data linkage between Domestic EPCs and OS AddressBase Plus

### 2.2 Attaching UPRN in Land Registry PPD



![](pic/f4.png)
**Figure 4.** A Brief workflow of data linkage between Land Registry PPD and OS AddressBase Plus


## 3. Acknowledgements
This work was funded by OS.